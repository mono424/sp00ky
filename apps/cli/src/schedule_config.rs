//! `schedules:` and `workflows:` in sp00ky.yml — the declarative surface for
//! server-side cron jobs and workflow DAGs.
//!
//! This module owns the YAML shape, its validation, and the normalization into
//! the flat form the engine reads out of `_00_schedule`. The engine never sees
//! sugar: `every: 5m` becomes `every_ms`, `backend: api` becomes the app's outbox
//! table, and a step map becomes an ordered step list.
//!
//! Validation runs in two places on purpose. `Sp00kyConfig::validate` does the
//! structural checks that need nothing but the config (cron parses, exactly one
//! cadence, the DAG is acyclic), and `spky lint` / `spky doctor` additionally
//! check things that need the backend route map (does that backend exist, does
//! that route exist on it). Whatever deploys is therefore something the engine
//! can actually plan and run.

use std::collections::{BTreeMap, BTreeSet, VecDeque};

use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};

/// One `schedules:` entry.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ScheduleConfig {
    /// Cron expression, evaluated in `timezone`. Standard 5 fields, with an
    /// optional leading seconds field.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cron: Option<String>,
    /// Fixed interval (`30s`, `5m`, `1h30m`), measured from each fire.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub every: Option<String>,
    /// IANA timezone for `cron`. Defaults to UTC.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timezone: Option<String>,

    /// App name (an `apps:` entry with an outbox method) whose route this calls.
    pub backend: String,
    /// Route path on that backend, e.g. `/syncGames`.
    pub route: String,
    /// Static payload merged into every spawned job's payload.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub payload: Option<serde_yaml::Value>,

    /// Fan out: run this SurrealQL SELECT each fire and spawn one job per row.
    #[serde(default, rename = "forEach", skip_serializing_if = "Option::is_none")]
    pub for_each: Option<ForEachConfig>,
    /// What to do when a fire lands while the previous run is still going.
    #[serde(default)]
    pub concurrency: Concurrency,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry: Option<RetryConfig>,
    /// Per-job HTTP timeout (`120s`). Honoured only when the backend sets
    /// `deploy.timeoutOverridable`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout: Option<String>,

    /// Wall-clock ceiling on ONE run (`30m`). Unset uses the project default from
    /// `retention.runDeadline`.
    ///
    /// Not the same knob as `timeout`: that bounds a single HTTP request to a
    /// backend, this bounds the run as a whole — every step, every job retry, and
    /// every gap in between. A run past it is failed `deadline_exceeded` and its
    /// jobs are killed, which is what stops one stuck run from holding its fan-out
    /// key under `concurrency: skip` indefinitely.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deadline: Option<String>,

    /// Consecutive failures of ONE `forEach` key before the schedule stops
    /// firing that key. Unset (the default) means never.
    ///
    /// The escape hatch for a poison row. A `forEach` query is re-run from
    /// scratch every fire, so a row that always fails is re-selected forever:
    /// `concurrency: skip` only suppresses a fire while the previous run is
    /// still going, and a query that returns a bad row is a perfectly
    /// successful query, so nothing else in the chain notices. One dead row
    /// then costs a job every tick indefinitely and keeps the outbox's failed
    /// bucket permanently non-empty, which is where a real failure would have
    /// shown.
    ///
    /// Only runs that actually executed count: a `skipped` fire moves nothing,
    /// and one success forgets the streak. A quarantined key raises an incident
    /// and records a `quarantined` run carrying the reason; release it with
    /// `spky schedules release <name> <key>` or by raising this number.
    #[serde(default, rename = "quarantineAfter", skip_serializing_if = "Option::is_none")]
    pub quarantine_after: Option<i64>,

    /// `false` deploys the schedule but leaves it inert. Distinct from an
    /// operator `spky schedules pause`, which config never overwrites.
    #[serde(default = "default_true")]
    pub enabled: bool,

    /// Per-schedule run-history retention, overriding the project-level
    /// `retention:`. This is the knob a noisy fan-out needs: a minutely schedule
    /// over 500 rows writes 500 history rows a minute, and almost none of them are
    /// ever read, while a nightly report wants its month.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub history: Option<HistoryConfig>,
}

/// `history:` on one schedule.
///
/// Accepts the shorthand `history: failures-only` as well as the full object, because
/// "never keep a successful run" is the common answer for a wide fan-out and should
/// not require thinking about durations at all.
#[derive(Debug, Clone, Serialize)]
#[serde(untagged)]
pub enum HistoryConfig {
    /// `history: failures-only` (or `all`).
    Mode(HistoryMode),
    /// `history: { mode?, success?, failed? }`.
    Detailed(HistoryDetail),
}

/// Hand-written rather than `#[serde(untagged)]`, the same dual-form idiom as
/// `VersionConfig`. An untagged enum reports a typo inside the object as "data did not
/// match any variant", which names neither the key nor the schedule — and a retention
/// setting that is silently hard to diagnose is one people give up on.
impl<'de> Deserialize<'de> for HistoryConfig {
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        let value = serde_yaml::Value::deserialize(deserializer)?;
        match &value {
            serde_yaml::Value::String(_) => {
                let mode: HistoryMode =
                    serde_yaml::from_value(value).map_err(serde::de::Error::custom)?;
                Ok(HistoryConfig::Mode(mode))
            }
            serde_yaml::Value::Mapping(_) => {
                // `deny_unknown_fields` on HistoryDetail does the work; propagating its
                // error verbatim is what keeps "unknown field `sucess`" readable.
                let detail: HistoryDetail =
                    serde_yaml::from_value(value).map_err(serde::de::Error::custom)?;
                Ok(HistoryConfig::Detailed(detail))
            }
            other => Err(serde::de::Error::custom(format!(
                "`history` must be `failures-only`, `all`, or a mapping of \
                 mode/success/failed — got {other:?}"
            ))),
        }
    }
}

impl HistoryMode {
    pub fn as_str(self) -> &'static str {
        match self {
            HistoryMode::All => "all",
            HistoryMode::FailuresOnly => "failures-only",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum HistoryMode {
    /// Keep every run until its retention window elapses. The default.
    All,
    /// Never persist a successful run: a suppressed tick writes no row at all, and a
    /// run that completes cleanly is deleted as it finalizes. Counts still land in the
    /// rollup, so totals survive.
    FailuresOnly,
}

/// Either window may be omitted, in which case that outcome falls back to the
/// project default.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct HistoryDetail {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mode: Option<HistoryMode>,
    /// How long `success` / `skipped` / `replaced` runs are kept (`15m`, `6h`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub success: Option<String>,
    /// How long `failed` / `killed` runs are kept (`30d`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failed: Option<String>,
}

impl HistoryConfig {
    pub(crate) fn mode(&self) -> Option<HistoryMode> {
        match self {
            HistoryConfig::Mode(m) => Some(*m),
            HistoryConfig::Detailed(d) => d.mode,
        }
    }

    fn windows(&self) -> (Option<&String>, Option<&String>) {
        match self {
            HistoryConfig::Mode(_) => (None, None),
            HistoryConfig::Detailed(d) => (d.success.as_ref(), d.failed.as_ref()),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ForEachConfig {
    /// SurrealQL SELECT. Runs as root, so it can read anything.
    pub query: String,
    /// Row field whose value keys the concurrency check. Defaults to `id`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Concurrency {
    /// Record a suppressed tick and move on. The default: a slow hourly sync
    /// should not pile up behind itself.
    #[default]
    Skip,
    /// Spawn regardless; overlapping runs are fine.
    Allow,
    /// Kill the in-flight run and start the new one.
    Replace,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RetryConfig {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max: Option<i64>,
    /// `linear` or `exponential`, matching the job runner's backoff.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub strategy: Option<String>,
}

/// One `workflows:` entry: a DAG of steps, optionally on a schedule.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct WorkflowConfig {
    /// Omit for a trigger-only workflow (`spky workflows trigger`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schedule: Option<WorkflowTrigger>,
    /// Steps by name. Order here is irrelevant — `dependsOn` decides execution.
    pub steps: BTreeMap<String, WorkflowStep>,
    /// What happens to steps that haven't started when one fails.
    #[serde(default, rename = "onFailure")]
    pub on_failure: OnFailure,
    #[serde(default)]
    pub concurrency: Concurrency,
    #[serde(default, rename = "forEach", skip_serializing_if = "Option::is_none")]
    pub for_each: Option<ForEachConfig>,

    /// Wall-clock ceiling on ONE run (`30m`). Unset uses the project default from
    /// `retention.runDeadline`.
    ///
    /// Not the same knob as `timeout`: that bounds a single HTTP request to a
    /// backend, this bounds the run as a whole — every step, every job retry, and
    /// every gap in between. A run past it is failed `deadline_exceeded` and its
    /// jobs are killed, which is what stops one stuck run from holding its fan-out
    /// key under `concurrency: skip` indefinitely.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deadline: Option<String>,

    /// Consecutive failures of ONE `forEach` key before the schedule stops
    /// firing that key. Unset (the default) means never.
    ///
    /// The escape hatch for a poison row. A `forEach` query is re-run from
    /// scratch every fire, so a row that always fails is re-selected forever:
    /// `concurrency: skip` only suppresses a fire while the previous run is
    /// still going, and a query that returns a bad row is a perfectly
    /// successful query, so nothing else in the chain notices. One dead row
    /// then costs a job every tick indefinitely and keeps the outbox's failed
    /// bucket permanently non-empty, which is where a real failure would have
    /// shown.
    ///
    /// Only runs that actually executed count: a `skipped` fire moves nothing,
    /// and one success forgets the streak. A quarantined key raises an incident
    /// and records a `quarantined` run carrying the reason; release it with
    /// `spky schedules release <name> <key>` or by raising this number.
    #[serde(default, rename = "quarantineAfter", skip_serializing_if = "Option::is_none")]
    pub quarantine_after: Option<i64>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct WorkflowTrigger {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cron: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub every: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timezone: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum OnFailure {
    /// Fail the run and skip every step that hasn't started.
    #[default]
    Halt,
    /// Skip only the branch below the failure; unrelated branches finish. The
    /// run still ends `failed`.
    ContinueIndependent,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct WorkflowStep {
    pub backend: String,
    pub route: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub payload: Option<serde_yaml::Value>,
    /// Steps that must succeed first. Steps with no dependencies are roots and
    /// run in parallel; a step with several is a fan-in join.
    #[serde(default, rename = "dependsOn", alias = "depends_on")]
    pub depends_on: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry: Option<RetryConfig>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout: Option<String>,
}

fn default_true() -> bool {
    true
}

// ── Validation ──────────────────────────────────────────────────────────────

/// Names must be safe to embed in a record id and readable in CLI output.
pub(crate) fn validate_name(kind: &str, name: &str) -> Result<()> {
    let ok = !name.is_empty()
        && name.chars().next().is_some_and(|c| c.is_ascii_lowercase())
        && name.chars().all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-' || c == '_');
    if !ok {
        bail!(
            "{kind} name '{name}' must be lowercase letters, digits, '-' or '_', starting with a letter"
        );
    }
    Ok(())
}

/// Parse a `30s` / `5m` / `1h30m` duration to milliseconds.
pub fn parse_duration_ms(raw: &str) -> Result<i64> {
    let dur = humantime::parse_duration(raw.trim())
        .map_err(|e| anyhow::anyhow!("invalid duration '{raw}': {e}"))?;
    let ms = dur.as_millis();
    if ms == 0 {
        bail!("duration '{raw}' must be greater than zero");
    }
    i64::try_from(ms).map_err(|_| anyhow::anyhow!("duration '{raw}' is too large"))
}

/// The shortest interval a schedule may use. The sweep runs every 5s, so
/// anything below this can't be honoured and would just mislead.
const MIN_INTERVAL_MS: i64 = 10_000;

/// Check a cadence: exactly one of cron/every, and it has to parse.
fn validate_cadence(
    label: &str,
    cron: Option<&str>,
    every: Option<&str>,
    timezone: Option<&str>,
    required: bool,
) -> Result<()> {
    match (cron, every) {
        (Some(_), Some(_)) => bail!("{label} sets both 'cron' and 'every' — pick one"),
        (None, None) if required => bail!("{label} must set either 'cron' or 'every'"),
        (None, None) => return Ok(()),
        _ => {}
    }
    // Parsed by the same code the engine uses, so a cron that lints will plan.
    schedule_core::FireSpec::parse(
        cron,
        every.map(parse_duration_ms).transpose()?,
        timezone,
    )
    .map_err(|e| anyhow::anyhow!("{label}: {e}"))?;
    if let Some(every) = every {
        let ms = parse_duration_ms(every)?;
        if ms < MIN_INTERVAL_MS {
            bail!(
                "{label} interval '{every}' is shorter than the {}s minimum",
                MIN_INTERVAL_MS / 1000
            );
        }
    }
    Ok(())
}

fn validate_for_each(label: &str, for_each: Option<&ForEachConfig>) -> Result<()> {
    let Some(fe) = for_each else { return Ok(()) };
    let q = fe.query.trim();
    if !q.to_ascii_uppercase().starts_with("SELECT") {
        bail!("{label} forEach.query must be a SELECT statement");
    }
    if q.contains(';') {
        bail!("{label} forEach.query must be a single statement (no ';')");
    }
    Ok(())
}

fn validate_retry(label: &str, retry: Option<&RetryConfig>) -> Result<()> {
    let Some(retry) = retry else { return Ok(()) };
    if let Some(max) = retry.max {
        if max < 0 {
            bail!("{label} retry.max cannot be negative");
        }
    }
    if let Some(strategy) = &retry.strategy {
        if !matches!(strategy.as_str(), "linear" | "exponential") {
            bail!("{label} retry.strategy must be 'linear' or 'exponential'");
        }
    }
    Ok(())
}

/// Kahn's algorithm over the step graph, mirroring the docker `dependsOn` check.
/// A deadline must parse and must survive the conversion to whole seconds. `500ms`
/// truncating to `0` would read as "no deadline", which is the opposite of what
/// anyone writing a deadline meant.
fn validate_deadline(label: &str, deadline: Option<&str>) -> Result<()> {
    let Some(raw) = deadline else { return Ok(()) };
    let ms = parse_duration_ms(raw)
        .with_context(|| format!("{label}: `deadline: {raw}` is not a duration"))?;
    if ms / 1000 < 1 {
        bail!(
            "{label}: `deadline: {raw}` is under a second — the engine works in whole \
             seconds, so this would round to 0 and disable the deadline entirely"
        );
    }
    Ok(())
}

fn validate_dag(label: &str, steps: &BTreeMap<String, WorkflowStep>) -> Result<()> {
    if steps.is_empty() {
        bail!("{label} has no steps");
    }
    let names: BTreeSet<&str> = steps.keys().map(String::as_str).collect();
    let mut indegree: BTreeMap<&str, usize> = names.iter().map(|n| (*n, 0usize)).collect();
    let mut dependents: BTreeMap<&str, Vec<&str>> = BTreeMap::new();

    for (name, step) in steps {
        for dep in &step.depends_on {
            if dep == name {
                bail!("{label} step '{name}' cannot depend on itself");
            }
            if !names.contains(dep.as_str()) {
                bail!("{label} step '{name}' depends on unknown step '{dep}'");
            }
            dependents.entry(dep.as_str()).or_default().push(name.as_str());
            *indegree.get_mut(name.as_str()).unwrap() += 1;
        }
    }

    let mut queue: VecDeque<&str> =
        indegree.iter().filter(|(_, d)| **d == 0).map(|(n, _)| *n).collect();
    let mut processed = 0usize;
    while let Some(n) = queue.pop_front() {
        processed += 1;
        for &m in dependents.get(n).map(Vec::as_slice).unwrap_or(&[]) {
            let d = indegree.get_mut(m).unwrap();
            *d -= 1;
            if *d == 0 {
                queue.push_back(m);
            }
        }
    }
    if processed != steps.len() {
        let in_cycle: Vec<&str> =
            indegree.iter().filter(|(_, d)| **d > 0).map(|(n, _)| *n).collect();
        bail!("{label} has a dependsOn cycle among steps: {}", in_cycle.join(", "));
    }
    Ok(())
}

/// Structural validation — everything checkable without the backend route map.
pub fn validate_all(
    schedules: &BTreeMap<String, ScheduleConfig>,
    workflows: &BTreeMap<String, WorkflowConfig>,
) -> Result<()> {
    for (name, s) in schedules {
        validate_name("schedule", name)?;
        let label = format!("schedule '{name}'");
        validate_cadence(&label, s.cron.as_deref(), s.every.as_deref(), s.timezone.as_deref(), true)?;
        validate_for_each(&label, s.for_each.as_ref())?;
        validate_retry(&label, s.retry.as_ref())?;
        if let Some(timeout) = &s.timeout {
            parse_duration_ms(timeout)?;
        }
        validate_deadline(&label, s.deadline.as_deref())?;
        if s.route.is_empty() {
            bail!("{label} has an empty route");
        }
    }

    for (name, w) in workflows {
        validate_name("workflow", name)?;
        let label = format!("workflow '{name}'");
        if let Some(trigger) = &w.schedule {
            validate_cadence(
                &label,
                trigger.cron.as_deref(),
                trigger.every.as_deref(),
                trigger.timezone.as_deref(),
                true,
            )?;
        }
        validate_for_each(&label, w.for_each.as_ref())?;
        validate_deadline(&label, w.deadline.as_deref())?;
        validate_dag(&label, &w.steps)?;
        for (step_name, step) in &w.steps {
            let step_label = format!("{label} step '{step_name}'");
            validate_retry(&step_label, step.retry.as_ref())?;
            if let Some(timeout) = &step.timeout {
                parse_duration_ms(timeout)?;
            }
            if step.route.is_empty() {
                bail!("{step_label} has an empty route");
            }
        }
    }

    // Both kinds share the `_00_schedule` id space, so a collision would make
    // one silently overwrite the other on deploy.
    for name in schedules.keys() {
        if workflows.contains_key(name) {
            bail!("'{name}' is defined as both a schedule and a workflow");
        }
    }
    Ok(())
}

// ── Normalization (config → the row the engine reads) ───────────────────────

/// Normalized spec fields for one `_00_schedule` row, as JSON. Deploy hashes
/// this and UPSERTs it; the engine deserializes it as `ScheduleSpec`.
pub fn normalize_schedule(
    name: &str,
    cfg: &ScheduleConfig,
    outbox_table: &str,
    default_mode: Option<HistoryMode>,
) -> Result<serde_json::Value> {
    let mut row = serde_json::Map::new();
    row.insert("name".into(), serde_json::json!(name));
    row.insert("kind".into(), serde_json::json!("job"));
    row.insert("target_table".into(), serde_json::json!(outbox_table));
    row.insert("path".into(), serde_json::json!(cfg.route));
    insert_cadence(&mut row, cfg.cron.as_deref(), cfg.every.as_deref(), cfg.timezone.as_deref())?;
    if let Some(payload) = &cfg.payload {
        row.insert("payload".into(), yaml_to_json(payload)?);
    }
    insert_for_each(&mut row, cfg.for_each.as_ref());
    row.insert("concurrency".into(), serde_json::json!(concurrency_str(cfg.concurrency)));
    insert_retry(&mut row, cfg.retry.as_ref());
    if let Some(timeout) = &cfg.timeout {
        // The runner takes whole seconds.
        row.insert("timeout".into(), serde_json::json!(parse_duration_ms(timeout)? / 1000));
    }
    insert_deadline(&mut row, cfg.deadline.as_deref())?;
    insert_quarantine(&mut row, cfg.quarantine_after);
    row.insert("config_disabled".into(), serde_json::json!(!cfg.enabled));
    insert_history(&mut row, cfg.history.as_ref(), default_mode)?;
    Ok(serde_json::Value::Object(row))
}

/// The per-key failure budget, omitted rather than nulled when unset so the
/// engine reads NONE and the gate stays off. `option<int>` rejects NULL.
fn insert_quarantine(row: &mut serde_json::Map<String, serde_json::Value>, after: Option<i64>) {
    if let Some(n) = after.filter(|n| *n > 0) {
        row.insert("quarantine_after".into(), serde_json::json!(n));
    }
}

/// Flatten `history:` to the seconds the engine reads. Absent windows are left out
/// of the row entirely rather than nulled, so the engine sees NONE and falls back to
/// the project default.
fn insert_history(
    row: &mut serde_json::Map<String, serde_json::Value>,
    history: Option<&HistoryConfig>,
    default_mode: Option<HistoryMode>,
) -> Result<()> {
    // The schedule's own mode wins; otherwise the project default applies. Resolved here,
    // at deploy time, so the engine reads one field and never has to combine two sources.
    if let Some(mode) = history.and_then(HistoryConfig::mode).or(default_mode) {
        row.insert("history_mode".into(), serde_json::json!(mode.as_str()));
    }
    let Some(history) = history else { return Ok(()) };
    let (success, failed) = history.windows();
    if let Some(success) = success {
        row.insert(
            "history_success_secs".into(),
            serde_json::json!(parse_duration_ms(success)? / 1000),
        );
    }
    if let Some(failed) = failed {
        row.insert(
            "history_failed_secs".into(),
            serde_json::json!(parse_duration_ms(failed)? / 1000),
        );
    }
    Ok(())
}

/// Same, for a workflow: the DAG is flattened to an ordered step list so the
/// engine never has to interpret a map.
pub fn normalize_workflow(
    name: &str,
    cfg: &WorkflowConfig,
    outbox_table: &str,
    step_tables: &BTreeMap<String, String>,
) -> Result<serde_json::Value> {
    let mut row = serde_json::Map::new();
    row.insert("name".into(), serde_json::json!(name));
    row.insert("kind".into(), serde_json::json!("workflow"));
    row.insert("target_table".into(), serde_json::json!(outbox_table));
    if let Some(trigger) = &cfg.schedule {
        insert_cadence(
            &mut row,
            trigger.cron.as_deref(),
            trigger.every.as_deref(),
            trigger.timezone.as_deref(),
        )?;
    }
    insert_for_each(&mut row, cfg.for_each.as_ref());
    row.insert("concurrency".into(), serde_json::json!(concurrency_str(cfg.concurrency)));

    let mut steps = Vec::with_capacity(cfg.steps.len());
    for (step_name, step) in &cfg.steps {
        let mut s = serde_json::Map::new();
        s.insert("name".into(), serde_json::json!(step_name));
        s.insert("path".into(), serde_json::json!(step.route));
        // A step may call a different backend than the workflow's default, so
        // its outbox table travels with it.
        if let Some(table) = step_tables.get(step_name) {
            if table != outbox_table {
                s.insert("table".into(), serde_json::json!(table));
            }
        }
        if let Some(payload) = &step.payload {
            s.insert("payload".into(), yaml_to_json(payload)?);
        }
        s.insert("depends_on".into(), serde_json::json!(step.depends_on));
        if let Some(retry) = &step.retry {
            if let Some(max) = retry.max {
                s.insert("max_retries".into(), serde_json::json!(max));
            }
            if let Some(strategy) = &retry.strategy {
                s.insert("retry_strategy".into(), serde_json::json!(strategy));
            }
        }
        if let Some(timeout) = &step.timeout {
            s.insert("timeout".into(), serde_json::json!(parse_duration_ms(timeout)? / 1000));
        }
        steps.push(serde_json::Value::Object(s));
    }

    row.insert(
        "workflow".into(),
        serde_json::json!({
            "steps": steps,
            "on_failure": match cfg.on_failure {
                OnFailure::Halt => "halt",
                OnFailure::ContinueIndependent => "continue-independent",
            },
        }),
    );
    insert_deadline(&mut row, cfg.deadline.as_deref())?;
    insert_quarantine(&mut row, cfg.quarantine_after);
    row.insert("config_disabled".into(), serde_json::json!(false));
    Ok(serde_json::Value::Object(row))
}

/// Flatten `deadline:` to the seconds the engine reads. Left OUT of the row when
/// unset rather than nulled, so the engine sees NONE and applies the project
/// default — `option<int>` rejects NULL.
fn insert_deadline(
    row: &mut serde_json::Map<String, serde_json::Value>,
    deadline: Option<&str>,
) -> Result<()> {
    let Some(deadline) = deadline else { return Ok(()) };
    row.insert("deadline_secs".into(), serde_json::json!(parse_duration_ms(deadline)? / 1000));
    Ok(())
}

fn insert_cadence(
    row: &mut serde_json::Map<String, serde_json::Value>,
    cron: Option<&str>,
    every: Option<&str>,
    timezone: Option<&str>,
) -> Result<()> {
    if let Some(cron) = cron {
        row.insert("cron".into(), serde_json::json!(cron));
    }
    if let Some(every) = every {
        row.insert("every_ms".into(), serde_json::json!(parse_duration_ms(every)?));
    }
    if let Some(tz) = timezone {
        row.insert("timezone".into(), serde_json::json!(tz));
    }
    Ok(())
}

fn insert_for_each(
    row: &mut serde_json::Map<String, serde_json::Value>,
    for_each: Option<&ForEachConfig>,
) {
    let Some(fe) = for_each else { return };
    row.insert("for_each".into(), serde_json::json!(fe.query));
    if let Some(key) = &fe.key {
        row.insert("for_each_key".into(), serde_json::json!(key));
    }
}

fn insert_retry(
    row: &mut serde_json::Map<String, serde_json::Value>,
    retry: Option<&RetryConfig>,
) {
    let Some(retry) = retry else { return };
    if let Some(max) = retry.max {
        row.insert("max_retries".into(), serde_json::json!(max));
    }
    if let Some(strategy) = &retry.strategy {
        row.insert("retry_strategy".into(), serde_json::json!(strategy));
    }
}

fn concurrency_str(c: Concurrency) -> &'static str {
    match c {
        Concurrency::Skip => "skip",
        Concurrency::Allow => "allow",
        Concurrency::Replace => "replace",
    }
}

fn yaml_to_json(value: &serde_yaml::Value) -> Result<serde_json::Value> {
    Ok(serde_json::to_value(value)?)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn schedule(yaml: &str) -> BTreeMap<String, ScheduleConfig> {
        serde_yaml::from_str(yaml).expect("parses")
    }

    fn workflows(yaml: &str) -> BTreeMap<String, WorkflowConfig> {
        serde_yaml::from_str(yaml).expect("parses")
    }

    /// `history:` becomes the seconds the engine reads, and an omitted window is
    /// LEFT OUT of the row rather than nulled — the engine reads NONE as "fall back
    /// to the project default", and `option<int>` rejects a bound NULL outright.
    #[test]
    fn per_schedule_history_normalizes_to_seconds() {
        let s = schedule(
            "noisy:\n  every: 1m\n  backend: api\n  route: /r\n  \
             history:\n    success: 15m\n    failed: 30d\n",
        );
        let row = normalize_schedule("noisy", &s["noisy"], "job", None).unwrap();
        assert_eq!(row["history_success_secs"], serde_json::json!(900));
        assert_eq!(row["history_failed_secs"], serde_json::json!(2_592_000));

        let partial = schedule(
            "half:\n  every: 1m\n  backend: api\n  route: /r\n  history:\n    success: 1h\n",
        );
        let row = normalize_schedule("half", &partial["half"], "job", None).unwrap();
        assert_eq!(row["history_success_secs"], serde_json::json!(3600));
        assert!(
            row.get("history_failed_secs").is_none(),
            "an unset window must be absent, not null"
        );

        let none = schedule("plain:\n  every: 1m\n  backend: api\n  route: /r\n");
        let row = normalize_schedule("plain", &none["plain"], "job", None).unwrap();
        assert!(row.get("history_success_secs").is_none());
        assert!(row.get("history_failed_secs").is_none());
    }

    /// The shorthand is the point: "never keep a successful run" is the common answer
    /// for a wide fan-out and should not require thinking about durations.
    #[test]
    fn the_failures_only_shorthand_normalizes_to_a_mode() {
        let s = schedule(
            "noisy:\n  every: 1m\n  backend: api\n  route: /r\n  history: failures-only\n",
        );
        let row = normalize_schedule("noisy", &s["noisy"], "job", None).unwrap();
        assert_eq!(row["history_mode"], serde_json::json!("failures-only"));
        // The shorthand carries no windows — the project defaults still apply to the
        // failures it DOES keep.
        assert!(row.get("history_success_secs").is_none());
        assert!(row.get("history_failed_secs").is_none());
    }

    /// The object form can set the mode and the windows together: discard successes,
    /// and keep failures for a specific time.
    #[test]
    fn the_object_form_can_combine_a_mode_with_windows() {
        let s = schedule(
            "noisy:\n  every: 1m\n  backend: api\n  route: /r\n  \
             history:\n    mode: failures-only\n    failed: 7d\n",
        );
        let row = normalize_schedule("noisy", &s["noisy"], "job", None).unwrap();
        assert_eq!(row["history_mode"], serde_json::json!("failures-only"));
        assert_eq!(row["history_failed_secs"], serde_json::json!(604_800));
    }

    /// `all` is the explicit spelling of the default, and must not be confused with
    /// the shorthand.
    #[test]
    fn the_all_mode_round_trips() {
        let s = schedule("calm:\n  every: 1m\n  backend: api\n  route: /r\n  history: all\n");
        let row = normalize_schedule("calm", &s["calm"], "job", None).unwrap();
        assert_eq!(row["history_mode"], serde_json::json!("all"));
    }

    /// An unknown mode must be rejected, not silently treated as `all` — that would
    /// quietly keep every row on a schedule the author meant to be lean.
    #[test]
    fn an_unknown_history_mode_is_rejected() {
        let err = serde_yaml::from_str::<BTreeMap<String, ScheduleConfig>>(
            "s:\n  every: 1m\n  backend: api\n  route: /r\n  history: failures\n",
        )
        .expect_err("unknown mode must not parse");
        assert!(!err.to_string().is_empty());
    }

    /// A typo in `history:` must fail the deploy rather than being ignored —
    /// silently dropping a retention override is how you discover it a month later.
    #[test]
    fn an_unknown_history_key_is_rejected() {
        let err = serde_yaml::from_str::<BTreeMap<String, ScheduleConfig>>(
            "s:\n  every: 1m\n  backend: api\n  route: /r\n  history:\n    sucess: 1h\n",
        )
        .expect_err("unknown key must not parse");
        assert!(err.to_string().contains("sucess"), "got: {err}");
    }

    #[test]
    fn accepts_a_cron_schedule_and_a_fan_out_schedule() {
        let s = schedule(
            r#"
nightly-cleanup:
  cron: "0 3 * * *"
  timezone: Europe/Berlin
  backend: api
  route: /cleanupExpired
  payload: { olderThanDays: 30 }
game-sync:
  every: 5m
  backend: gamesync
  route: /syncGames
  forEach:
    query: SELECT id FROM connection WHERE active = true
    key: id
  concurrency: skip
  retry: { max: 3, strategy: linear }
  timeout: 120s
"#,
        );
        validate_all(&s, &BTreeMap::new()).expect("valid");
        assert_eq!(s["game-sync"].concurrency, Concurrency::Skip);
        assert!(s["nightly-cleanup"].enabled, "enabled defaults to true");
    }

    #[test]
    fn rejects_a_bad_cadence() {
        let both = schedule("x:\n  cron: \"0 3 * * *\"\n  every: 5m\n  backend: api\n  route: /r\n");
        assert!(validate_all(&both, &BTreeMap::new())
            .unwrap_err()
            .to_string()
            .contains("pick one"));

        let neither = schedule("x:\n  backend: api\n  route: /r\n");
        assert!(validate_all(&neither, &BTreeMap::new())
            .unwrap_err()
            .to_string()
            .contains("must set either"));

        let bad_cron = schedule("x:\n  cron: nope\n  backend: api\n  route: /r\n");
        assert!(validate_all(&bad_cron, &BTreeMap::new())
            .unwrap_err()
            .to_string()
            .contains("cron"));

        let too_fast = schedule("x:\n  every: 1s\n  backend: api\n  route: /r\n");
        assert!(validate_all(&too_fast, &BTreeMap::new())
            .unwrap_err()
            .to_string()
            .contains("minimum"));

        let bad_tz =
            schedule("x:\n  cron: \"0 3 * * *\"\n  timezone: Mars/Olympus\n  backend: api\n  route: /r\n");
        assert!(validate_all(&bad_tz, &BTreeMap::new())
            .unwrap_err()
            .to_string()
            .contains("timezone"));
    }

    #[test]
    fn rejects_a_for_each_that_is_not_a_single_select() {
        let not_select = schedule(
            "x:\n  every: 5m\n  backend: api\n  route: /r\n  forEach:\n    query: DELETE user\n",
        );
        assert!(validate_all(&not_select, &BTreeMap::new())
            .unwrap_err()
            .to_string()
            .contains("SELECT"));

        let multi = schedule(
            "x:\n  every: 5m\n  backend: api\n  route: /r\n  forEach:\n    query: \"SELECT id FROM a; DELETE b\"\n",
        );
        assert!(validate_all(&multi, &BTreeMap::new())
            .unwrap_err()
            .to_string()
            .contains("single statement"));
    }

    #[test]
    fn accepts_a_diamond_workflow_and_rejects_a_cycle() {
        let w = workflows(
            r#"
monthly-report:
  schedule: { cron: "0 6 1 * *" }
  steps:
    extract-orders: { backend: api, route: /exportOrders }
    extract-users: { backend: api, route: /exportUsers }
    transform:
      backend: analytics
      route: /buildReport
      dependsOn: [extract-orders, extract-users]
    notify: { backend: notify, route: /postSlack, dependsOn: [transform] }
    archive: { backend: api, route: /archiveReport, dependsOn: [transform] }
  onFailure: halt
"#,
        );
        validate_all(&BTreeMap::new(), &w).expect("valid");
        assert_eq!(w["monthly-report"].on_failure, OnFailure::Halt);

        let cyclic = workflows(
            "w:\n  steps:\n    a: { backend: api, route: /a, dependsOn: [b] }\n    b: { backend: api, route: /b, dependsOn: [a] }\n",
        );
        assert!(validate_all(&BTreeMap::new(), &cyclic)
            .unwrap_err()
            .to_string()
            .contains("cycle"));

        let ghost = workflows("w:\n  steps:\n    a: { backend: api, route: /a, dependsOn: [ghost] }\n");
        assert!(validate_all(&BTreeMap::new(), &ghost)
            .unwrap_err()
            .to_string()
            .contains("unknown step"));

        let selfdep = workflows("w:\n  steps:\n    a: { backend: api, route: /a, dependsOn: [a] }\n");
        assert!(validate_all(&BTreeMap::new(), &selfdep)
            .unwrap_err()
            .to_string()
            .contains("itself"));
    }

    #[test]
    fn rejects_a_name_used_by_both_kinds() {
        let s = schedule("dup:\n  every: 5m\n  backend: api\n  route: /r\n");
        let w = workflows("dup:\n  steps:\n    a: { backend: api, route: /a }\n");
        assert!(validate_all(&s, &w).unwrap_err().to_string().contains("both"));
    }

    #[test]
    fn rejects_unparseable_names() {
        let s = schedule("Bad_Name:\n  every: 5m\n  backend: api\n  route: /r\n");
        assert!(validate_all(&s, &BTreeMap::new())
            .unwrap_err()
            .to_string()
            .contains("lowercase"));
    }

    #[test]
    fn normalizes_sugar_away_for_the_engine() {
        let s = schedule(
            "game-sync:\n  every: 5m\n  backend: gamesync\n  route: /syncGames\n  timeout: 2m\n  \
             retry: { max: 3, strategy: exponential }\n  concurrency: replace\n  \
             forEach:\n    query: SELECT id FROM connection\n    key: id\n  enabled: false\n",
        );
        let row = normalize_schedule("game-sync", &s["game-sync"], "job", None).unwrap();
        assert_eq!(row["kind"], serde_json::json!("job"));
        assert_eq!(row["every_ms"], serde_json::json!(300_000), "`5m` becomes milliseconds");
        assert_eq!(row["target_table"], serde_json::json!("job"), "backend resolved to its table");
        assert_eq!(row["path"], serde_json::json!("/syncGames"));
        assert_eq!(row["timeout"], serde_json::json!(120), "the runner takes seconds");
        assert_eq!(row["max_retries"], serde_json::json!(3));
        assert_eq!(row["retry_strategy"], serde_json::json!("exponential"));
        assert_eq!(row["concurrency"], serde_json::json!("replace"));
        assert_eq!(row["for_each_key"], serde_json::json!("id"));
        assert_eq!(row["config_disabled"], serde_json::json!(true), "`enabled: false`");
        assert!(row.get("cron").is_none(), "no cron on an interval schedule");
    }

    #[test]
    fn normalizes_a_workflow_into_an_ordered_step_list() {
        let w = workflows(
            "report:\n  schedule: { cron: \"0 6 1 * *\" }\n  onFailure: continue-independent\n  steps:\n    \
             extract: { backend: api, route: /extract }\n    \
             load: { backend: warehouse, route: /load, dependsOn: [extract], timeout: 30s }\n",
        );
        let mut tables = BTreeMap::new();
        tables.insert("extract".to_string(), "job".to_string());
        tables.insert("load".to_string(), "warehouse_job".to_string());

        let row = normalize_workflow("report", &w["report"], "job", &tables).unwrap();
        assert_eq!(row["kind"], serde_json::json!("workflow"));
        assert_eq!(row["cron"], serde_json::json!("0 6 1 * *"));
        assert_eq!(row["workflow"]["on_failure"], serde_json::json!("continue-independent"));

        let steps = row["workflow"]["steps"].as_array().unwrap();
        assert_eq!(steps.len(), 2);
        let load = steps.iter().find(|s| s["name"] == serde_json::json!("load")).unwrap();
        assert_eq!(load["depends_on"], serde_json::json!(["extract"]));
        assert_eq!(load["table"], serde_json::json!("warehouse_job"), "a step can use its own table");
        assert_eq!(load["timeout"], serde_json::json!(30));
        let extract = steps.iter().find(|s| s["name"] == serde_json::json!("extract")).unwrap();
        assert!(extract.get("table").is_none(), "a step on the default table omits it");
    }

    /// The engine must be able to read back exactly what deploy writes.
    #[test]
    fn normalized_rows_deserialize_as_engine_specs() {
        let s = schedule("nightly:\n  cron: \"0 3 * * *\"\n  backend: api\n  route: /cleanup\n");
        let row = normalize_schedule("nightly", &s["nightly"], "job", None).unwrap();
        let spec = schedule_core::ScheduleSpec::from_row(&row).expect("engine reads it");
        assert_eq!(spec.name, "nightly");
        assert!(spec.fire_spec().is_ok());

        let w = workflows("wf:\n  steps:\n    a: { backend: api, route: /a }\n    b: { backend: api, route: /b, dependsOn: [a] }\n");
        let row = normalize_workflow("wf", &w["wf"], "job", &BTreeMap::new()).unwrap();
        let spec = schedule_core::ScheduleSpec::from_row(&row).expect("engine reads it");
        let dag = schedule_core::WorkflowDag::validate(spec.workflow.as_ref().unwrap())
            .expect("engine validates the DAG");
        assert_eq!(dag.len(), 2);
    }
}

#[cfg(test)]
mod project_default_mode_tests {
    use super::*;

    fn schedule(yaml: &str) -> BTreeMap<String, ScheduleConfig> {
        serde_yaml::from_str(yaml).expect("parses")
    }

    const PLAIN: &str = "s:\n  every: 1m\n  backend: api\n  route: /r\n";

    /// A schedule with no `history:` inherits the project default. This is what makes
    /// `retention.mode: failures-only` a one-line switch for a whole project.
    #[test]
    fn a_schedule_without_history_inherits_the_project_default() {
        let s = schedule(PLAIN);
        let row =
            normalize_schedule("s", &s["s"], "job", Some(HistoryMode::FailuresOnly)).unwrap();
        assert_eq!(row["history_mode"], serde_json::json!("failures-only"));
    }

    /// And with no project default either, the field stays absent — the engine reads
    /// NONE as "keep everything", so an unconfigured project is unaffected.
    #[test]
    fn no_default_and_no_history_leaves_the_field_unset() {
        let s = schedule(PLAIN);
        let row = normalize_schedule("s", &s["s"], "job", None).unwrap();
        assert!(row.get("history_mode").is_none());
    }

    /// A schedule can opt OUT of a project-wide `failures-only` — the case for an audit
    /// trail that must keep every run.
    #[test]
    fn a_schedule_can_opt_back_out_of_the_project_default() {
        let s = schedule(
            "s:\n  every: 1m\n  backend: api\n  route: /r\n  history: all\n",
        );
        let row =
            normalize_schedule("s", &s["s"], "job", Some(HistoryMode::FailuresOnly)).unwrap();
        assert_eq!(
            row["history_mode"],
            serde_json::json!("all"),
            "the schedule's own mode must win over the project default"
        );
    }

    /// And opt IN when the project default is `all` (or unset).
    #[test]
    fn a_schedule_can_opt_in_against_the_project_default() {
        let s = schedule(
            "s:\n  every: 1m\n  backend: api\n  route: /r\n  history: failures-only\n",
        );
        let row = normalize_schedule("s", &s["s"], "job", Some(HistoryMode::All)).unwrap();
        assert_eq!(row["history_mode"], serde_json::json!("failures-only"));
    }

    /// Setting windows but no mode must not accidentally clear an inherited mode: the
    /// two settings are independent.
    #[test]
    fn per_schedule_windows_do_not_clear_the_inherited_mode() {
        let s = schedule(
            "s:\n  every: 1m\n  backend: api\n  route: /r\n  history:\n    success: 15m\n",
        );
        let row =
            normalize_schedule("s", &s["s"], "job", Some(HistoryMode::FailuresOnly)).unwrap();
        assert_eq!(row["history_mode"], serde_json::json!("failures-only"));
        assert_eq!(row["history_success_secs"], serde_json::json!(900));
    }
}
