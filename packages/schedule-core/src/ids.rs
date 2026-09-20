//! Deterministic record ids.
//!
//! Every row the engine creates has an id derived from what it represents, so a
//! duplicate `CREATE` is the idempotency check: two tickers racing the same
//! fire, or a heal pass re-dispatching after a crash between `CREATE job` and
//! `UPDATE step`, both collide on the id instead of double-spawning work.
//!
//! Two hard-won rules shape this module:
//!
//! - **A record id is a (table, key) PAIR, never one string.** SurrealDB's
//!   single-argument `type::record("s:game-sync")` silently truncates at the
//!   hyphen and returns `s:game` — so a schedule named `game-sync` would quietly
//!   read and write the wrong row. Everything here therefore yields a [`Ref`],
//!   and every statement binds `type::record($tb, $key)`.
//! - **Keys the engine mints in USER outbox tables avoid hyphens entirely.**
//!   Those rows are addressed by the job runner, which still uses the
//!   single-argument form, so a hyphen there would hit the same trap from the
//!   other side.

use std::fmt;

use sha2::{Digest, Sha256};

/// Engine-owned table names.
pub const SCHEDULE: &str = "_00_schedule";
pub const SCHEDULE_RUN: &str = "_00_schedule_run";
pub const WORKFLOW_RUN: &str = "_00_workflow_run";
pub const STEP_RUN: &str = "_00_step_run";
pub const RUN_ROLLUP: &str = "_00_run_rollup";
pub const SCHEDULE_KEY: &str = "_00_schedule_key";

/// Longest raw name/key fragment kept verbatim before it is hashed away. Keeps
/// ids readable (`_00_schedule_run:game-sync_1769337000000_9f2a1c4d8b30`)
/// without letting a pathological fan-out key blow up the id length.
const MAX_FRAGMENT: usize = 48;

/// A record id as SurrealDB actually needs it: table and key, separately.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Ref {
    pub table: String,
    pub key: String,
}

impl Ref {
    pub fn new(table: impl Into<String>, key: impl Into<String>) -> Self {
        Self { table: table.into(), key: key.into() }
    }

    /// Split a record id read back from the database.
    ///
    /// Flattened ids arrive with the key backtick-quoted whenever it needs
    /// escaping (`_00_schedule:⁠`game-sync`⁠`), so the quotes come off here —
    /// re-binding them as part of the key would look up a different row.
    pub fn parse(record_id: &str) -> Option<Self> {
        let (table, key) = record_id.split_once(':')?;
        let key = key.trim_matches('`').trim_matches('⟨').trim_matches('⟩');
        if table.is_empty() || key.is_empty() {
            return None;
        }
        Some(Self::new(table, key))
    }

    /// Canonical unquoted `table:key`, for storing in a plain string column
    /// (`_00_schedule_run.job_id`) and for logs.
    pub fn as_string(&self) -> String {
        format!("{}:{}", self.table, self.key)
    }
}

impl fmt::Display for Ref {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.table, self.key)
    }
}

/// Short, collision-resistant digest of an arbitrary string.
pub fn short_hash(input: &str) -> String {
    let digest = Sha256::digest(input.as_bytes());
    digest.iter().take(6).map(|byte| format!("{byte:02x}")).collect()
}

/// Reduce a fragment to `[a-z0-9_-]`, lowercase, length-capped. A fragment that
/// had to be truncated or rewritten gets a hash suffix so distinct inputs stay
/// distinct.
pub fn sanitize(fragment: &str) -> String {
    sanitize_inner(fragment, true)
}

/// Same, but also collapses hyphens to underscores — for keys that land in a
/// user outbox table, which the job runner addresses with the hyphen-truncating
/// single-argument `type::record`.
pub fn sanitize_job(fragment: &str) -> String {
    sanitize_inner(fragment, false)
}

fn sanitize_inner(fragment: &str, allow_hyphen: bool) -> String {
    let mut out = String::with_capacity(fragment.len());
    let mut rewritten = false;
    for ch in fragment.chars() {
        match ch {
            'a'..='z' | '0'..='9' | '_' => out.push(ch),
            '-' if allow_hyphen => out.push(ch),
            '-' => out.push('_'),
            'A'..='Z' => {
                out.push(ch.to_ascii_lowercase());
                rewritten = true;
            }
            _ => {
                out.push('_');
                rewritten = true;
            }
        }
    }
    if out.len() > MAX_FRAGMENT {
        out.truncate(MAX_FRAGMENT);
        rewritten = true;
    }
    if rewritten {
        out.push('_');
        out.push_str(&short_hash(fragment));
    }
    if out.is_empty() {
        out.push_str(&short_hash(fragment));
    }
    out
}

/// Key identifying one fire of one fan-out item: `<schedule>_<fire_ms>_<hash(key)>`.
///
/// The fan-out key is always hashed (never inlined) because it comes from user
/// data — a record id, an email, anything the `forEach` query selected.
pub fn run_key(schedule_name: &str, fire_at_ms: i64, fan_out_key: &str) -> String {
    format!("{}_{}_{}", sanitize(schedule_name), fire_at_ms, short_hash(fan_out_key))
}

pub fn schedule(name: &str) -> Ref {
    Ref::new(SCHEDULE, sanitize(name))
}

/// The per-key failure budget row: `<schedule>_<hash(key)>`.
///
/// The same shape as [`run_key`] minus the fire timestamp, and for the same
/// reason — the fan-out key is user data, so it is hashed rather than inlined.
/// Deterministic, so the gate and the finalize path address the row without a
/// lookup.
pub fn schedule_key(schedule_name: &str, fan_out_key: &str) -> Ref {
    Ref::new(
        SCHEDULE_KEY,
        format!("{}_{}", sanitize(schedule_name), short_hash(fan_out_key)),
    )
}

pub fn schedule_run(run_key: &str) -> Ref {
    Ref::new(SCHEDULE_RUN, run_key)
}

pub fn workflow_run(run_key: &str) -> Ref {
    Ref::new(WORKFLOW_RUN, run_key)
}

pub fn step_run(run_key: &str, step: &str) -> Ref {
    Ref::new(STEP_RUN, format!("{}_{}", run_key, sanitize(step)))
}

/// Outbox job row for a `kind: job` schedule fire.
pub fn job(table: &str, run_key: &str) -> Ref {
    Ref::new(table, format!("sch_{}", sanitize_job(run_key)))
}

/// Outbox job row for one workflow step's dispatch.
///
/// Deterministic so the crash window between creating the job row and stamping
/// `job_id` onto the step row is self-healing: the retry's `CREATE` collides,
/// the engine detects "already dispatched", and stamps the id it would have
/// used anyway.
pub fn step_job(table: &str, run_key: &str, step: &str) -> Ref {
    Ref::new(table, format!("wf_{}_{}", sanitize_job(run_key), sanitize_job(step)))
}

/// Job row for one step's Nth operator retry.
///
/// A retry cannot reuse the step's previous job id: every SSP keeps an in-memory
/// kill flag keyed by job id that only dequeue clears, and `dispatch_step` reads
/// an existing row as "already dispatched". So each attempt gets its own row.
/// Attempt 0 is exactly [`step_job`], so ids minted before retries existed are
/// unchanged.
pub fn step_job_attempt(table: &str, run_key: &str, step: &str, attempt: i64) -> Ref {
    let base = step_job(table, run_key, step);
    if attempt <= 0 {
        base
    } else {
        Ref::new(table, format!("{}_r{}", base.key, attempt))
    }
}

/// Run key for an operator rerun: `<workflow>_<now_ms>_<hash("rerun:<source>")>`.
///
/// The source key goes into the hash so a rerun and a cron fire of the same
/// workflow in the same millisecond cannot collide, and so two reruns of
/// different sources stay distinct.
pub fn rerun_key(workflow_name: &str, fire_at_ms: i64, source_run_key: &str) -> String {
    run_key(workflow_name, fire_at_ms, &format!("rerun:{source_run_key}"))
}

/// Key for one rollup bucket: `<scope>_<name>_<hour>`.
///
/// Deterministic so a fold is a blind `UPSERT ... SET n += x` with no read first:
/// two pruners (or a retried pass) landing on the same bucket accumulate instead of
/// racing a read-modify-write. The name is sanitized rather than hashed so the row
/// stays greppable, and the bucket is an RFC 3339 hour with its punctuation
/// stripped.
pub fn rollup(scope: &str, name: &str, bucket: &str) -> Ref {
    let stamp: String =
        bucket.chars().filter(|c| c.is_ascii_alphanumeric()).collect();
    Ref::new(RUN_ROLLUP, format!("{}_{}_{}", sanitize(scope), sanitize(name), stamp))
}

/// The run key back out of any engine record id. Lets a workflow run find its
/// sibling schedule-run row without storing (or reading back) a link.
pub fn run_key_of(record_id: &str) -> String {
    Ref::parse(record_id).map(|r| r.key).unwrap_or_else(|| record_id.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn run_keys_are_stable_and_distinct() {
        let a = run_key("game-sync", 1_769_337_000_000, "connection:alice");
        assert_eq!(a, run_key("game-sync", 1_769_337_000_000, "connection:alice"));
        // different fire, different key, different schedule → all distinct
        assert_ne!(a, run_key("game-sync", 1_769_337_300_000, "connection:alice"));
        assert_ne!(a, run_key("game-sync", 1_769_337_000_000, "connection:bob"));
        assert_ne!(a, run_key("other-sync", 1_769_337_000_000, "connection:alice"));
    }

    #[test]
    fn no_fan_out_uses_the_empty_key() {
        let key = run_key("nightly", 1_769_337_000_000, "");
        assert!(key.starts_with("nightly_1769337000000_"));
    }

    #[test]
    fn sanitize_keeps_safe_fragments_verbatim() {
        assert_eq!(sanitize("game-sync"), "game-sync");
        assert_eq!(sanitize("extract_orders"), "extract_orders");
    }

    /// Keys the runner addresses must not contain a hyphen: its single-argument
    /// `type::record("job:sch_game-sync_…")` would truncate at the first one.
    #[test]
    fn job_keys_never_contain_a_hyphen() {
        let key = run_key("game-sync", 1_769_337_000_000, "connection:alice");
        let job = job("job", &key);
        assert!(!job.key.contains('-'), "got {}", job.key);
        let step = step_job("job", &key, "extract-orders");
        assert!(!step.key.contains('-'), "got {}", step.key);
        // still unique per step
        assert_ne!(step_job("job", &key, "a"), step_job("job", &key, "b"));
    }

    #[test]
    fn sanitize_disambiguates_rewritten_fragments() {
        // Both would collapse to `a_b` without the hash suffix.
        let dot = sanitize("a.b");
        let slash = sanitize("a/b");
        assert!(dot.starts_with("a_b_"));
        assert!(slash.starts_with("a_b_"));
        assert_ne!(dot, slash);
    }

    #[test]
    fn sanitize_caps_length() {
        let long = "x".repeat(200);
        assert_eq!(sanitize(&long).len(), MAX_FRAGMENT + 1 + 12);
    }

    #[test]
    fn refs_render_and_parse_round_trip() {
        let r = schedule("game-sync");
        assert_eq!(r.table, "_00_schedule");
        assert_eq!(r.as_string(), "_00_schedule:game-sync");
        assert_eq!(Ref::parse(&r.as_string()), Some(r));
    }

    /// SurrealDB hands back keys that need escaping in backticks; leaving them on
    /// would address a different row.
    #[test]
    fn parse_strips_the_quoting_surrealdb_adds() {
        let parsed = Ref::parse("_00_schedule:`game-sync`").unwrap();
        assert_eq!(parsed.key, "game-sync");
        assert_eq!(parsed.table, "_00_schedule");
        assert_eq!(run_key_of("_00_workflow_run:`report_123_abc`"), "report_123_abc");
    }

    #[test]
    fn parse_rejects_malformed_ids() {
        assert!(Ref::parse("no-colon").is_none());
        assert!(Ref::parse(":empty-table").is_none());
        assert!(Ref::parse("empty-key:").is_none());
    }

    #[test]
    fn attempt_zero_is_the_original_step_job_id() {
        let key = run_key("report", 1, "");
        assert_eq!(step_job_attempt("job", &key, "transform", 0), step_job("job", &key, "transform"));
    }

    #[test]
    fn later_attempts_are_distinct_and_still_hyphen_free() {
        let key = run_key("game-sync", 1, "connection:alice");
        let r1 = step_job_attempt("job", &key, "extract-orders", 1);
        let r2 = step_job_attempt("job", &key, "extract-orders", 2);
        assert_ne!(r1, step_job("job", &key, "extract-orders"));
        assert_ne!(r1, r2);
        assert!(r1.key.ends_with("_r1"), "got {}", r1.key);
        assert!(!r1.key.contains('-') && !r2.key.contains('-'));
    }

    /// A rerun landing in the same millisecond as a cron fire must not collide
    /// with it, or the rerun would be read as "already fired".
    #[test]
    fn a_rerun_key_never_collides_with_a_cron_key_at_the_same_instant() {
        let cron = run_key("report", 1_769_337_000_000, "");
        let rerun = rerun_key("report", 1_769_337_000_000, &cron);
        assert_ne!(cron, rerun);
        assert_ne!(rerun, rerun_key("report", 1_769_337_000_000, "other_source"));
    }

    #[test]
    fn step_run_ids_are_unique_per_step() {
        let key = run_key("wf", 1, "");
        assert_ne!(step_run(&key, "a"), step_run(&key, "b"));
    }
}
