//! Tail SurrealDB's native change feed instead of letting `DEFINE EVENT`s
//! `http::post` every mutation out of the user's transaction.
//!
//! # Why
//!
//! The generated `_00_<table>_mutation` events used to call the scheduler's
//! `/ingest` INSIDE the transaction that made the change. That put the
//! scheduler's latency, the SSP's admission and the SSP's locks inside every
//! user write: a slow scheduler stalled writes, a scheduler restart failed
//! them, a transaction that failed at commit had already announced a change
//! that never happened, and a `DELETE _00_query` could deadlock against the
//! SSP's own TTL sweep through `/view/unregister` (2026-09-14).
//!
//! A `CHANGEFEED` clause on a table makes SurrealDB write the change into the
//! same transaction's commit, so `SHOW CHANGES FOR DATABASE SINCE <vs>` is
//! post-commit only, ordered by commit versionstamp, durable and resumable.
//! Verified on 3.1.5: cancelled and failed transactions never appear, a
//! versionstamp is `(commit_unix_ms << 16) | counter` assigned AT commit, and
//! `SINCE` is inclusive.
//!
//! This module is the host-independent half: parsing, cursor arithmetic, gap
//! detection and the tail loop over two small traits. The scheduler and the
//! standalone SSP provide the source (a database handle) and the sink (their
//! ingest path).

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use serde_json::Value;
use tracing::{debug, info, warn};

use crate::db::ReconnectingDb;

/// Table whose row every synced mutation touches inside the same transaction
/// (the generated events CREATE/UPDATE/DELETE a version row per record), so
/// one live query on it is a wake-up signal for every table with no extra
/// write and no hot row.
pub const DOORBELL_TABLE: &str = "_00_version";

/// Meta tables that carry a `CHANGEFEED` clause besides the user's synced
/// tables. `_00_version` rides along only to stamp `_00_rv`; `_00_query`
/// only for its DELETE (view teardown); `_00_heartbeat` for the probe.
pub const CHANGEFEED_META_TABLES: &[&str] = &[
    "_00_version",
    "_00_query",
    "_00_user_feature",
    "_00_app_release",
    "_00_heartbeat",
];

/// Low 16 bits of a versionstamp are a per-millisecond counter.
const STAMP_SHIFT: u32 = 16;

/// Commit time (unix ms) encoded in a versionstamp.
pub fn stamp_ms(versionstamp: u64) -> u64 {
    versionstamp >> STAMP_SHIFT
}

/// The smallest versionstamp a commit at `unix_ms` could carry.
pub fn stamp_from_ms(unix_ms: u64) -> u64 {
    unix_ms << STAMP_SHIFT
}

pub fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Parse a SurrealDB duration literal (`1d`, `12h`, `90m`, `30s`, `1d12h`)
/// into milliseconds. Only the units SurrealDB renders for a `CHANGEFEED`
/// clause are accepted; anything else is `None`.
pub fn parse_duration_ms(text: &str) -> Option<u64> {
    let text = text.trim();
    if text.is_empty() {
        return None;
    }
    let mut total: u64 = 0;
    let mut num = String::new();
    let mut seen_unit = false;
    for c in text.chars() {
        if c.is_ascii_digit() {
            num.push(c);
            continue;
        }
        let n: u64 = num.parse().ok()?;
        num.clear();
        let unit_ms = match c {
            'w' => 7 * 24 * 3_600_000,
            'd' => 24 * 3_600_000,
            'h' => 3_600_000,
            'm' => 60_000,
            's' => 1_000,
            _ => return None,
        };
        total = total.checked_add(n.checked_mul(unit_ms)?)?;
        seen_unit = true;
    }
    if !num.is_empty() || !seen_unit {
        return None;
    }
    Some(total)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChangeOp {
    Create,
    Update,
    Delete,
}

impl ChangeOp {
    pub fn as_str(&self) -> &'static str {
        match self {
            ChangeOp::Create => "CREATE",
            ChangeOp::Update => "UPDATE",
            ChangeOp::Delete => "DELETE",
        }
    }
}

/// One committed row change, ready to become an ingest.
#[derive(Debug, Clone)]
pub struct ChangeRecord {
    pub versionstamp: u64,
    pub table: String,
    /// Full record id, `table:key`.
    pub id: String,
    pub op: ChangeOp,
    /// The row after the change (`_00_rv` stamped when the transaction also
    /// wrote its version row). `None` for a delete: the feed carries only the
    /// id, the sink supplies the before-image if it needs one.
    pub record: Option<Value>,
    pub rv: Option<i64>,
}

/// What one `SHOW CHANGES` result parsed into.
#[derive(Debug, Default)]
pub struct ParsedBatch {
    pub records: Vec<ChangeRecord>,
    /// Number of versionstamp entries (transactions) in the result.
    pub entries: usize,
    /// Highest versionstamp seen; `SINCE` is inclusive so the next cursor is
    /// this plus one.
    pub max_versionstamp: Option<u64>,
    /// Changes dropped: DDL entries, version rows (folded into `rv`), rows the
    /// tail never forwards.
    pub skipped: usize,
}

impl ParsedBatch {
    pub fn next_cursor(&self, current: u64) -> u64 {
        match self.max_versionstamp {
            Some(vs) if vs >= current => vs + 1,
            _ => current,
        }
    }
}

/// Table name of a record id (`game:abc` -> `game`). Escaped ids keep their
/// table prefix, so the first colon is the split point either way.
pub fn table_of(id: &str) -> Option<&str> {
    let (table, _) = id.split_once(':')?;
    if table.is_empty() {
        None
    } else {
        Some(table)
    }
}

fn record_id_of(change: &Value) -> Option<String> {
    change.get("id").and_then(|v| v.as_str()).map(str::to_owned)
}

/// Turn the result array of one `SHOW CHANGES FOR DATABASE` statement into
/// change records, in commit order.
///
/// Entry shapes (SurrealDB 3.1.5, table defined with `INCLUDE ORIGINAL`):
/// - `{"update": {row}}` is a CREATE;
/// - `{"current": {row}, "update": [json-patch back to the original]}` is an
///   UPDATE, `current` is the row after it;
/// - `{"delete": {"id": ..}}` is a DELETE;
/// - `{"define_table": ..}` and any other DDL entry is skipped.
///
/// Within one entry (one transaction) the `_00_version` rows written by the
/// generated events are matched to their record by `record_id`, which stamps
/// `_00_rv` exactly as the HTTP events did, without a lookup and without the
/// race a lookup after the fact would have. `_00_query` rows are forwarded
/// only for their DELETE (view teardown); their UPDATE churn (heartbeats,
/// metrics) is dropped here.
pub fn parse_show_changes(result: &Value) -> ParsedBatch {
    let mut out = ParsedBatch::default();
    let Some(entries) = result.as_array() else {
        return out;
    };
    for entry in entries {
        let Some(vs) = entry.get("versionstamp").and_then(|v| v.as_u64()) else {
            continue;
        };
        out.entries += 1;
        out.max_versionstamp = Some(out.max_versionstamp.map_or(vs, |m| m.max(vs)));
        let Some(changes) = entry.get("changes").and_then(|c| c.as_array()) else {
            continue;
        };

        // First pass: the transaction's version stamps.
        let mut versions: HashMap<String, i64> = HashMap::new();
        for change in changes {
            let Some(row) = change.get("current").or_else(|| change.get("update")) else {
                continue;
            };
            if !row.is_object() {
                continue;
            }
            let Some(id) = record_id_of(row) else {
                continue;
            };
            if table_of(&id) != Some("_00_version") {
                continue;
            }
            if let (Some(record_id), Some(version)) = (
                row.get("record_id").and_then(|v| v.as_str()),
                row.get("version").and_then(|v| v.as_i64()),
            ) {
                versions.insert(record_id.to_string(), version);
            }
        }

        // Second pass: the record changes themselves.
        for change in changes {
            let Some(obj) = change.as_object() else {
                out.skipped += 1;
                continue;
            };
            if let Some(deleted) = obj.get("delete") {
                let Some(id) = record_id_of(deleted) else {
                    out.skipped += 1;
                    continue;
                };
                let Some(table) = table_of(&id).map(str::to_owned) else {
                    out.skipped += 1;
                    continue;
                };
                if table == "_00_version" {
                    out.skipped += 1;
                    continue;
                }
                out.records.push(ChangeRecord {
                    versionstamp: vs,
                    table,
                    id,
                    op: ChangeOp::Delete,
                    record: None,
                    rv: None,
                });
                continue;
            }
            let (op, row) = if let Some(current) = obj.get("current") {
                (ChangeOp::Update, current)
            } else if let Some(update) = obj.get("update") {
                (ChangeOp::Create, update)
            } else {
                // define_table and friends.
                out.skipped += 1;
                continue;
            };
            if !row.is_object() {
                out.skipped += 1;
                continue;
            }
            let Some(id) = record_id_of(row) else {
                out.skipped += 1;
                continue;
            };
            let Some(table) = table_of(&id).map(str::to_owned) else {
                out.skipped += 1;
                continue;
            };
            if table == "_00_version" || table == "_00_query" {
                out.skipped += 1;
                continue;
            }
            let rv = versions.get(&id).copied();
            let mut record = row.clone();
            if let (Some(v), Some(map)) = (rv, record.as_object_mut()) {
                map.insert("_00_rv".to_string(), Value::from(v));
            }
            out.records.push(ChangeRecord {
                versionstamp: vs,
                table,
                id,
                op,
                record: Some(record),
                rv,
            });
        }
    }
    out
}

/// Whether a cursor has fallen behind the feed's retention. SurrealDB answers
/// a `SINCE` older than the retained window with an EMPTY result, not an
/// error, so this arithmetic is the only gap detector there is. `margin`
/// absorbs clock skew between the database and the tailer.
pub fn cursor_gapped(cursor: u64, now_ms: u64, retention_ms: u64, margin_ms: u64) -> bool {
    if cursor == 0 || retention_ms == 0 {
        return false;
    }
    let age = now_ms.saturating_sub(stamp_ms(cursor));
    age > retention_ms.saturating_sub(margin_ms)
}

/// Where a tail starts when nothing persisted says otherwise: a second
/// before now, so a change that commits while the caller is still cloning
/// or bootstrapping is replayed rather than missed. Replays are idempotent
/// downstream (the replica skips a row whose `_00_rv` did not advance).
pub fn fresh_cursor(now_ms: u64) -> u64 {
    stamp_from_ms(now_ms.saturating_sub(1_000))
}

/// Tunables for one tail loop.
#[derive(Debug, Clone)]
pub struct TailerConfig {
    /// Wait after a doorbell wake before polling, so a bulk transaction that
    /// rings once per row costs one poll.
    pub debounce: Duration,
    /// Safety-net poll interval while the doorbell is connected.
    pub fallback: Duration,
    /// Poll interval while the doorbell is down.
    pub fallback_down: Duration,
    /// `LIMIT` per `SHOW CHANGES` (entries, i.e. transactions).
    pub poll_limit: usize,
    /// Deadline for one `SHOW CHANGES`.
    pub poll_timeout: Duration,
    /// The schema's `CHANGEFEED <retention>`.
    pub retention_ms: u64,
    /// Clock-skew allowance subtracted from the retention in the gap check.
    pub gap_margin_ms: u64,
    /// No successful poll for this long is reported as a stall.
    pub stall_after: Duration,
}

impl Default for TailerConfig {
    fn default() -> Self {
        Self {
            debounce: Duration::from_millis(5),
            fallback: Duration::from_secs(2),
            fallback_down: Duration::from_millis(250),
            poll_limit: 500,
            poll_timeout: Duration::from_secs(30),
            retention_ms: 24 * 3_600_000,
            gap_margin_ms: 5 * 60_000,
            stall_after: Duration::from_secs(60),
        }
    }
}

/// Lock-free counters every reader (health, metrics, dashboard) serves from.
#[derive(Debug, Default)]
pub struct TailerStats {
    pub enabled: AtomicBool,
    pub cursor: AtomicU64,
    pub polls: AtomicU64,
    pub last_poll_ms: AtomicU64,
    pub last_success_ms: AtomicU64,
    pub last_entries: AtomicU64,
    pub records_total: AtomicU64,
    pub wakes: AtomicU64,
    pub gap: AtomicBool,
    pub gaps_total: AtomicU64,
    pub stalled: AtomicBool,
    pub doorbell_connected: AtomicBool,
    pub doorbell_reconnects: AtomicU64,
    pub last_error: std::sync::Mutex<Option<String>>,
    /// Bumped by [`TailerStats::reset_cursor`]; a poll that started under an
    /// older generation must not raise the cursor past a reset.
    generation: AtomicU64,
    /// `(minute bucket, count)` for the rolling per-minute figure.
    window: std::sync::Mutex<(u64, u64, u64)>,
}

impl TailerStats {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub fn cursor(&self) -> u64 {
        self.cursor.load(Ordering::Relaxed)
    }

    pub fn set_cursor(&self, cursor: u64) {
        self.cursor.store(cursor, Ordering::Relaxed);
    }

    /// Advance the cursor only forward.
    pub fn raise_cursor(&self, cursor: u64) {
        self.cursor.fetch_max(cursor, Ordering::Relaxed);
    }

    /// Move the cursor (backwards included) and invalidate polls in flight,
    /// so a re-clone that restarts the tail from before its cut cannot be
    /// undone by a poll that was already computing a later cursor.
    pub fn reset_cursor(&self, cursor: u64) {
        self.generation.fetch_add(1, Ordering::Relaxed);
        self.cursor.store(cursor, Ordering::Relaxed);
    }

    pub fn generation(&self) -> u64 {
        self.generation.load(Ordering::Relaxed)
    }

    /// `raise_cursor`, unless the cursor was reset since `generation`.
    pub fn raise_cursor_in(&self, generation: u64, cursor: u64) -> bool {
        if self.generation() != generation {
            return false;
        }
        self.raise_cursor(cursor);
        true
    }

    fn note_records(&self, n: u64, now_ms: u64) {
        self.records_total.fetch_add(n, Ordering::Relaxed);
        if let Ok(mut w) = self.window.lock() {
            let minute = now_ms / 60_000;
            if w.0 != minute {
                *w = (minute, w.1, 0);
                w.1 = 0;
            }
            w.2 += n;
        }
    }

    fn records_last_minute(&self, now_ms: u64) -> u64 {
        self.window
            .lock()
            .map(|w| if w.0 == now_ms / 60_000 { w.2 } else { 0 })
            .unwrap_or(0)
    }

    pub fn set_error(&self, err: Option<String>) {
        if let Ok(mut e) = self.last_error.lock() {
            *e = err;
        }
    }

    /// How far behind the newest commit the tail can be at most: the age of
    /// the cursor, capped by the age of the last successful poll (an empty
    /// poll proves nothing newer than the cursor had committed at that
    /// moment, so an idle feed does not read as a growing lag). `None`
    /// until the first poll.
    pub fn lag_ms(&self, now_ms: u64) -> Option<u64> {
        let cursor = self.cursor();
        if cursor == 0 {
            return None;
        }
        let cursor_age = now_ms.saturating_sub(stamp_ms(cursor));
        let last_ok = self.last_success_ms.load(Ordering::Relaxed);
        if last_ok == 0 {
            return Some(cursor_age);
        }
        Some(cursor_age.min(now_ms.saturating_sub(last_ok)))
    }

    pub fn json(&self, now_ms: u64) -> Value {
        let cursor = self.cursor();
        serde_json::json!({
            "enabled": self.enabled.load(Ordering::Relaxed),
            "cursor": cursor,
            "cursor_ms": if cursor == 0 { Value::Null } else { Value::from(stamp_ms(cursor)) },
            "lag_ms": self.lag_ms(now_ms),
            "polls": self.polls.load(Ordering::Relaxed),
            "last_poll_ms": self.last_poll_ms.load(Ordering::Relaxed),
            "last_success_ms": self.last_success_ms.load(Ordering::Relaxed),
            "records_1m": self.records_last_minute(now_ms),
            "records_total": self.records_total.load(Ordering::Relaxed),
            "wakes": self.wakes.load(Ordering::Relaxed),
            "gap": self.gap.load(Ordering::Relaxed),
            "gaps_total": self.gaps_total.load(Ordering::Relaxed),
            "stalled": self.stalled.load(Ordering::Relaxed),
            "doorbell": if self.doorbell_connected.load(Ordering::Relaxed) { "connected" } else { "reconnecting" },
            "doorbell_reconnects": self.doorbell_reconnects.load(Ordering::Relaxed),
            "last_error": self.last_error.lock().ok().and_then(|e| e.clone()),
        })
    }
}

/// The database side of a tail.
#[async_trait]
pub trait ChangeSource: Send + Sync {
    /// Run `SHOW CHANGES FOR DATABASE SINCE <since> LIMIT <limit>` and return
    /// the statement's result value (an array of entries).
    async fn show_changes(&self, since: u64, limit: usize) -> anyhow::Result<Value>;
    /// A poll that never returned: the connection is presumed dead.
    fn note_stalled(&self);
}

/// Why a batch could not be delivered.
#[derive(Debug)]
pub enum SinkError {
    /// Keep the cursor where it is and try again later (a full backlog, a
    /// WAL write that failed).
    Retry(String),
}

/// The consumer side of a tail: the scheduler's ingest path or the SSP's.
#[async_trait]
pub trait ChangeSink: Send + Sync {
    /// Whether the sink can take records right now (a scheduler still cloning
    /// or restoring says no; the loop waits without moving the cursor).
    fn ready(&self) -> bool;
    /// The row as it was before a DELETE, when the sink keeps one. The feed
    /// carries only the id of a deleted row.
    async fn before_image(&self, table: &str, id: &str) -> Option<Value>;
    /// Deliver one record. On `Retry` the loop stops the batch and keeps the
    /// cursor at this record's transaction.
    async fn deliver(&self, record: ChangeRecord) -> Result<(), SinkError>;
    /// The cursor fell out of the retained window (or a poll answered with a
    /// cursor it cannot serve): rebuild from the source of truth. The loop
    /// resets the cursor to `fresh_cursor(now)` before calling this, so the
    /// rebuild and the tail overlap rather than leave a hole.
    async fn on_gap(&self) -> anyhow::Result<()>;
    /// Persist the cursor after a batch landed. Optional.
    async fn persist_cursor(&self, _cursor: u64) {}
}

/// Run the tail until the task is dropped. Wakes on `notify` (the doorbell,
/// or a host that just wrote something itself), on the fallback timer, and
/// drains until a poll comes back empty.
pub async fn run_tailer(
    source: Arc<dyn ChangeSource>,
    sink: Arc<dyn ChangeSink>,
    cfg: TailerConfig,
    stats: Arc<TailerStats>,
    notify: Arc<tokio::sync::Notify>,
) {
    stats.enabled.store(true, Ordering::Relaxed);
    if stats.cursor() == 0 {
        stats.set_cursor(fresh_cursor(now_ms()));
    }
    info!(
        cursor_ms = stamp_ms(stats.cursor()),
        retention_ms = cfg.retention_ms,
        fallback_ms = cfg.fallback.as_millis() as u64,
        "Changefeed tail started"
    );
    let mut stall_reported = false;
    loop {
        let wait = if stats.doorbell_connected.load(Ordering::Relaxed) {
            cfg.fallback
        } else {
            cfg.fallback_down
        };
        tokio::select! {
            _ = notify.notified() => {
                stats.wakes.fetch_add(1, Ordering::Relaxed);
                // Coalesce a burst: a 200-row transaction rings 200 times
                // within a couple of milliseconds.
                tokio::time::sleep(cfg.debounce).await;
            }
            _ = tokio::time::sleep(wait) => {}
        }

        if !sink.ready() {
            continue;
        }

        let now = now_ms();
        if cursor_gapped(stats.cursor(), now, cfg.retention_ms, cfg.gap_margin_ms) {
            handle_gap(&*sink, &stats, "cursor older than the feed's retention").await;
            continue;
        }

        // Drain: keep polling while a poll returns entries.
        loop {
            let generation = stats.generation();
            let since = stats.cursor();
            let started = std::time::Instant::now();
            stats.polls.fetch_add(1, Ordering::Relaxed);
            let polled =
                tokio::time::timeout(cfg.poll_timeout, source.show_changes(since, cfg.poll_limit))
                    .await;
            let now = now_ms();
            stats.last_poll_ms.store(now, Ordering::Relaxed);
            let result = match polled {
                Ok(Ok(v)) => v,
                Ok(Err(e)) => {
                    warn!(error = %e, "Changefeed poll failed");
                    stats.set_error(Some(e.to_string()));
                    break;
                }
                Err(_) => {
                    warn!(
                        timeout_secs = cfg.poll_timeout.as_secs(),
                        "Changefeed poll timed out; reconnecting"
                    );
                    stats.set_error(Some("poll timed out".into()));
                    source.note_stalled();
                    break;
                }
            };
            let batch = parse_show_changes(&result);
            stats
                .last_entries
                .store(batch.entries as u64, Ordering::Relaxed);
            let poll_ms = started.elapsed().as_millis() as u64;
            if batch.entries > 0 {
                debug!(
                    entries = batch.entries,
                    records = batch.records.len(),
                    skipped = batch.skipped,
                    poll_ms,
                    "Changefeed poll returned changes"
                );
            }

            let next = batch.next_cursor(since);
            let entries = batch.entries;
            let mut delivered: u64 = 0;
            let mut retry = false;
            let mut last_vs = since;
            // The cursor advances per transaction: a `Retry` in the middle of a
            // transaction re-polls from that transaction, and the sink's
            // idempotency (rv-monotonic apply, repeat-tolerant SSP) absorbs the
            // records that already landed.
            for record in batch.records {
                if record.versionstamp > last_vs {
                    stats.raise_cursor_in(generation, last_vs + 1);
                    last_vs = record.versionstamp;
                }
                let mut record = record;
                if record.op == ChangeOp::Delete && record.record.is_none() {
                    record.record = sink.before_image(&record.table, &record.id).await;
                }
                match sink.deliver(record).await {
                    Ok(()) => delivered += 1,
                    Err(SinkError::Retry(reason)) => {
                        debug!(reason, "Changefeed delivery deferred; cursor held");
                        stats.set_error(Some(reason));
                        retry = true;
                        break;
                    }
                }
            }
            if retry {
                // Everything before `last_vs` landed; keep `last_vs` itself.
                stats.raise_cursor_in(generation, last_vs);
                break;
            }
            if !stats.raise_cursor_in(generation, next) {
                debug!("Cursor was reset during the poll; discarding its advance");
                break;
            }
            stats.last_success_ms.store(now, Ordering::Relaxed);
            stats.set_error(None);
            if stall_reported {
                stall_reported = false;
                stats.stalled.store(false, Ordering::Relaxed);
                info!("Changefeed tail recovered");
            }
            if delivered > 0 {
                stats.note_records(delivered, now);
                sink.persist_cursor(stats.cursor()).await;
            }
            if entries < cfg.poll_limit {
                break;
            }
        }

        // Stall detection: no successful poll for a while, with the loop alive.
        let last_ok = stats.last_success_ms.load(Ordering::Relaxed);
        if last_ok > 0
            && now_ms().saturating_sub(last_ok) > cfg.stall_after.as_millis() as u64
            && !stall_reported
        {
            stall_reported = true;
            stats.stalled.store(true, Ordering::Relaxed);
            warn!(
                since_ms = now_ms().saturating_sub(last_ok),
                "Changefeed tail has not completed a poll in a while"
            );
        }
    }
}

async fn handle_gap(sink: &dyn ChangeSink, stats: &TailerStats, why: &str) {
    stats.gap.store(true, Ordering::Relaxed);
    stats.gaps_total.fetch_add(1, Ordering::Relaxed);
    warn!(
        cursor_ms = stamp_ms(stats.cursor()),
        why, "Changefeed gap: the tail cannot resume from its cursor; rebuilding from upstream"
    );
    // Reset BEFORE the rebuild so changes committed during it are replayed.
    stats.reset_cursor(fresh_cursor(now_ms()));
    match sink.on_gap().await {
        Ok(()) => {
            stats.gap.store(false, Ordering::Relaxed);
            stats.set_error(None);
            info!("Changefeed gap closed; tail resumed from a fresh cursor");
        }
        Err(e) => {
            warn!(error = %e, "Changefeed gap rebuild failed; will retry");
            stats.set_error(Some(format!("gap rebuild: {e}")));
            // Keep `gap` set so health shows it; the next wake tries again.
            tokio::time::sleep(Duration::from_secs(5)).await;
        }
    }
}

/// How row changes reach the sync stack: `http` = the generated DB events
/// post to `/ingest` inside the user's transaction; `changefeed` = the host
/// tails SurrealDB's `CHANGEFEED` (this module).
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "lowercase")]
pub enum IngestTransport {
    Http,
    Changefeed,
}

impl IngestTransport {
    pub fn parse(v: &str) -> Option<Self> {
        match v.trim().to_ascii_lowercase().as_str() {
            "http" => Some(Self::Http),
            "changefeed" | "cdc" => Some(Self::Changefeed),
            _ => None,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Http => "http",
            Self::Changefeed => "changefeed",
        }
    }

    /// `SPKY_INGEST_TRANSPORT`, defaulting to `http`.
    pub fn from_env() -> Self {
        match std::env::var("SPKY_INGEST_TRANSPORT") {
            Ok(v) => match Self::parse(&v) {
                Some(t) => t,
                None => {
                    warn!(value = %v, "SPKY_INGEST_TRANSPORT not recognised (http|changefeed); using http");
                    Self::Http
                }
            },
            Err(_) => Self::Http,
        }
    }
}

/// Tunables for the changefeed tail. Every one has an env override so a
/// deployment can be adjusted without a schema change; only `retention`
/// has to agree with the schema's `CHANGEFEED` clause.
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
#[serde(default)]
pub struct ChangefeedSettings {
    /// `LIVE SELECT` on `_00_version` over WebSocket as the wake-up signal.
    /// `false` = fallback timer only. Env `SPKY_CHANGEFEED_DOORBELL`.
    pub doorbell: bool,
    /// Wait after a wake before polling, to coalesce a burst.
    /// Env `SPKY_CHANGEFEED_DEBOUNCE_MS`.
    pub debounce_ms: u64,
    /// Safety-net poll interval while the doorbell is connected.
    /// Env `SPKY_CHANGEFEED_FALLBACK_MS`.
    pub fallback_ms: u64,
    /// Poll interval while the doorbell is down.
    /// Env `SPKY_CHANGEFEED_FALLBACK_DOWN_MS`.
    pub fallback_down_ms: u64,
    /// The schema's `CHANGEFEED` retention as a SurrealDB duration (`1d`).
    /// A cursor older than this minus the margin is a gap: the replica is
    /// re-cloned. Env `SPKY_CHANGEFEED_RETENTION`.
    pub retention: String,
    /// Clock-skew allowance for the gap check. Env
    /// `SPKY_CHANGEFEED_GAP_MARGIN_SECS`.
    pub gap_margin_secs: u64,
    /// Transactions per `SHOW CHANGES`. Env `SPKY_CHANGEFEED_POLL_LIMIT`.
    pub poll_limit: usize,
    /// Deadline for one `SHOW CHANGES`; past it the upstream handle is
    /// replaced. Env `SPKY_CHANGEFEED_POLL_TIMEOUT_SECS`.
    pub poll_timeout_secs: u64,
}

impl Default for ChangefeedSettings {
    fn default() -> Self {
        Self {
            doorbell: true,
            debounce_ms: 5,
            fallback_ms: 2_000,
            fallback_down_ms: 250,
            retention: "1d".to_string(),
            gap_margin_secs: 300,
            poll_limit: 500,
            poll_timeout_secs: 30,
        }
    }
}

impl ChangefeedSettings {
    /// Defaults with every `SPKY_CHANGEFEED_*` override applied.
    pub fn from_env() -> Self {
        let mut s = Self::default();
        s.apply_env();
        s
    }

    pub fn apply_env(&mut self) {
        if let Some(b) = std::env::var("SPKY_CHANGEFEED_DOORBELL")
            .ok()
            .and_then(|v| parse_env_bool(&v))
        {
            self.doorbell = b;
        }
        if let Some(n) = env_u64("SPKY_CHANGEFEED_DEBOUNCE_MS") {
            self.debounce_ms = n;
        }
        if let Some(n) = env_u64("SPKY_CHANGEFEED_FALLBACK_MS").filter(|n| *n > 0) {
            self.fallback_ms = n;
        }
        if let Some(n) = env_u64("SPKY_CHANGEFEED_FALLBACK_DOWN_MS").filter(|n| *n > 0) {
            self.fallback_down_ms = n;
        }
        if let Ok(v) = std::env::var("SPKY_CHANGEFEED_RETENTION") {
            if parse_duration_ms(&v).is_some() {
                self.retention = v.trim().to_string();
            } else {
                warn!(value = %v, "SPKY_CHANGEFEED_RETENTION is not a SurrealDB duration (1d, 12h); keeping the default");
            }
        }
        if let Some(n) = env_u64("SPKY_CHANGEFEED_GAP_MARGIN_SECS") {
            self.gap_margin_secs = n;
        }
        if let Some(n) = env_u64("SPKY_CHANGEFEED_POLL_LIMIT").filter(|n| *n > 0) {
            self.poll_limit = n as usize;
        }
        if let Some(n) = env_u64("SPKY_CHANGEFEED_POLL_TIMEOUT_SECS").filter(|n| *n > 0) {
            self.poll_timeout_secs = n;
        }
    }

    pub fn retention_ms(&self) -> u64 {
        parse_duration_ms(&self.retention).unwrap_or(24 * 3_600_000)
    }

    /// The tail loop's view of these settings.
    pub fn tailer_config(&self) -> TailerConfig {
        TailerConfig {
            debounce: Duration::from_millis(self.debounce_ms),
            fallback: Duration::from_millis(self.fallback_ms),
            fallback_down: Duration::from_millis(self.fallback_down_ms),
            poll_limit: self.poll_limit,
            poll_timeout: Duration::from_secs(self.poll_timeout_secs),
            retention_ms: self.retention_ms(),
            gap_margin_ms: self.gap_margin_secs * 1_000,
            stall_after: Duration::from_millis(
                self.fallback_ms.max(self.fallback_down_ms) * 10 + 30_000,
            ),
        }
    }
}

fn env_u64(key: &str) -> Option<u64> {
    std::env::var(key)
        .ok()
        .and_then(|v| v.trim().parse::<u64>().ok())
}

fn parse_env_bool(v: &str) -> Option<bool> {
    match v.trim().to_ascii_lowercase().as_str() {
        "true" | "1" | "yes" | "on" => Some(true),
        "false" | "0" | "no" | "off" => Some(false),
        _ => None,
    }
}

/// `SHOW CHANGES` through a [`ReconnectingDb`]: the source both hosts use.
pub struct ReconnectingSource {
    pub db: Arc<ReconnectingDb>,
}

#[async_trait]
impl ChangeSource for ReconnectingSource {
    async fn show_changes(&self, since: u64, limit: usize) -> anyhow::Result<Value> {
        let handle = self.db.handle();
        // `since` is a u64 the tail owns, never user input; SurrealDB does not
        // accept a bound parameter in a SINCE clause.
        let sql = format!("SHOW CHANGES FOR DATABASE SINCE {since} LIMIT {limit};");
        let mut response = handle
            .query(sql)
            .await
            .inspect_err(|e| self.db.note_error(&e.to_string()))?;
        let value: surrealdb::types::Value = response.take(0)?;
        Ok(value.into_json_value())
    }

    fn note_stalled(&self) {
        self.db.force_reconnect();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// Shapes captured from surrealdb/surrealdb:v3.1.5 on 2026-09-14.
    fn fixture() -> Value {
        json!([
            {"changes":[{"define_table":{"changefeed":{"expiry":"1h","original":true},"name":"game"}}],"versionstamp":117271319528996864u64},
            {"changes":[{"update":{"id":"_00_version:v1","record_id":"game:a","version":1}},{"update":{"id":"game:a","x":1}}],"versionstamp":117271319530831872u64},
            {"changes":[{"update":{"id":"_00_version:v1","record_id":"game:a","version":2}},{"current":{"id":"game:a","x":2},"update":[{"op":"replace","path":"/x","value":1}]}],"versionstamp":117271319530897408u64},
            {"changes":[{"delete":{"id":"_00_version:v1"}},{"delete":{"id":"game:a"}}],"versionstamp":117271319530962944u64},
            {"changes":[{"update":{"id":"_00_query:abc","surql":"SELECT 1"}},{"delete":{"id":"_00_query:def"}}],"versionstamp":117271319530962945u64},
            {"changes":[{"update":{"id":"plain:m1","x":0}},{"update":{"id":"plain:m2","x":0}}],"versionstamp":117271319530962946u64}
        ])
    }

    #[test]
    fn versionstamp_encodes_commit_time() {
        // Captured on 2026-09-14 from a 3.1.5 container: the stamp's upper
        // bits are the commit's unix milliseconds.
        assert_eq!(stamp_ms(117271319528996864), 1789418327774);
        assert_eq!(stamp_from_ms(1789418327774) >> 16, 1789418327774);
        assert!(stamp_from_ms(1789418327774) <= 117271319528996864);
    }

    #[test]
    fn parses_create_update_delete_with_version_join() {
        let batch = parse_show_changes(&fixture());
        assert_eq!(batch.entries, 6);
        let ops: Vec<(String, ChangeOp, Option<i64>)> = batch
            .records
            .iter()
            .map(|r| (r.id.clone(), r.op, r.rv))
            .collect();
        assert_eq!(
            ops,
            vec![
                ("game:a".into(), ChangeOp::Create, Some(1)),
                ("game:a".into(), ChangeOp::Update, Some(2)),
                ("game:a".into(), ChangeOp::Delete, None),
                ("_00_query:def".into(), ChangeOp::Delete, None),
                ("plain:m1".into(), ChangeOp::Create, None),
                ("plain:m2".into(), ChangeOp::Create, None),
            ]
        );
        // The update carries the row AFTER the change plus the stamped rv.
        let update = &batch.records[1];
        assert_eq!(update.record.as_ref().unwrap()["x"], 2);
        assert_eq!(update.record.as_ref().unwrap()["_00_rv"], 2);
        assert_eq!(update.table, "game");
        // DDL, version rows and _00_query updates are dropped.
        assert_eq!(batch.skipped, 5);
        assert_eq!(batch.max_versionstamp, Some(117271319530962946));
        assert_eq!(batch.next_cursor(0), 117271319530962947);
    }

    #[test]
    fn empty_result_keeps_the_cursor() {
        let batch = parse_show_changes(&json!([]));
        assert_eq!(batch.entries, 0);
        assert_eq!(batch.next_cursor(42), 42);
        assert_eq!(parse_show_changes(&Value::Null).entries, 0);
    }

    #[test]
    fn gap_is_time_based_with_margin() {
        let retention = 24 * 3_600_000;
        let margin = 5 * 60_000;
        let now = 1_789_416_000_000u64;
        assert!(
            !cursor_gapped(0, now, retention, margin),
            "unset cursor is never a gap"
        );
        assert!(!cursor_gapped(
            stamp_from_ms(now - 3_600_000),
            now,
            retention,
            margin
        ));
        assert!(cursor_gapped(
            stamp_from_ms(now - retention + margin - 1),
            now,
            retention,
            margin
        ));
        assert!(cursor_gapped(
            stamp_from_ms(now - 2 * retention),
            now,
            retention,
            margin
        ));
        assert!(
            !cursor_gapped(stamp_from_ms(now), now, 0, margin),
            "no retention = no gap check"
        );
    }

    #[test]
    fn durations_match_surrealdb_rendering() {
        assert_eq!(parse_duration_ms("1d"), Some(86_400_000));
        assert_eq!(parse_duration_ms("12h"), Some(43_200_000));
        assert_eq!(parse_duration_ms("1d12h"), Some(129_600_000));
        assert_eq!(parse_duration_ms("90m"), Some(5_400_000));
        assert_eq!(parse_duration_ms("30s"), Some(30_000));
        assert_eq!(parse_duration_ms("1w"), Some(604_800_000));
        assert_eq!(parse_duration_ms("1x"), None);
        assert_eq!(parse_duration_ms("d"), None);
        assert_eq!(parse_duration_ms(""), None);
    }

    #[test]
    fn table_of_handles_escaped_ids() {
        assert_eq!(table_of("game:abc"), Some("game"));
        assert_eq!(table_of("game:⟨a:b⟩"), Some("game"));
        assert_eq!(table_of("nocolon"), None);
        assert_eq!(table_of(":x"), None);
    }

    #[test]
    fn stats_json_reports_lag_from_cursor() {
        let stats = TailerStats::new();
        let now = 1_789_416_000_000u64;
        assert_eq!(stats.lag_ms(now), None);
        stats.set_cursor(stamp_from_ms(now - 250));
        assert_eq!(stats.lag_ms(now), Some(250));
        stats.raise_cursor(stamp_from_ms(now - 1_000));
        assert_eq!(
            stats.lag_ms(now),
            Some(250),
            "raise never moves the cursor backwards"
        );
        let gen = stats.generation();
        stats.reset_cursor(stamp_from_ms(now - 5_000));
        assert!(
            !stats.raise_cursor_in(gen, stamp_from_ms(now)),
            "a stale generation cannot raise"
        );
        assert_eq!(stats.lag_ms(now), Some(5_000));
        assert!(stats.raise_cursor_in(stats.generation(), stamp_from_ms(now - 100)));
        assert_eq!(stats.lag_ms(now), Some(100));
        // An idle feed: the last poll (empty) was 20 ms ago, so at most 20 ms
        // of commits can be unseen, however old the cursor is.
        stats.last_success_ms.store(now - 20, Ordering::Relaxed);
        assert_eq!(stats.lag_ms(now), Some(20));
        assert_eq!(stats.json(now)["lag_ms"], 20);
        let j = stats.json(now);
        assert_eq!(j["doorbell"], "reconnecting");
        assert_eq!(j["lag_ms"], 20);
    }
}
