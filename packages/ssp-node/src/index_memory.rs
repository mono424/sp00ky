//! Which secondary indexes an SSP holds, kept in the database, so the SSP
//! that replaces it builds the same ones before its clients need them.
//!
//! An SSP builds a mirrored index the first time a view plans over it, and
//! that first build is a pass over the whole table that the registration
//! waits for: 9 s for two 750k-row `game` indexes on WhitePawn (2026-11-17).
//! A process keeps an index for its idle grace after the last reader goes
//! ([`ssp::circuit::circuit::INDEX_IDLE_RELEASE`]), but a new process starts
//! with none, and a recreated SSP (blue/green upgrade, git-linked deploy, a
//! fresh volume) has no local record of what the last one held either. So
//! every deploy put those builds back in front of the first clients.
//!
//! Here the serving SSP writes what it holds (each index, how long it has
//! gone unread, its size) to one record of the private `_00_sync_evidence`
//! table, every [`RECORD_EVERY`] when that changed and on graceful shutdown.
//! A new SSP reads every such record after its bootstrap ([`select`]). A
//! blue/green standby builds before it reports Ready ([`prebuild`]), while
//! its predecessor still serves; any other SSP goes Ready first and builds
//! in the background a short step at a time ([`prebuild_in_background`]),
//! so no restart waits for it. It rebuilds what the mirrored
//! schema still defines and was read within the idle grace, most recently
//! read first, within a time and a memory budget ([`PrebuildLimits`]), and
//! carries each index's idle age over, so a deploy does not keep an index
//! nobody reads alive. Whatever is skipped is logged and still builds lazily
//! on first use, as before.

use std::collections::BTreeMap;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use ssp::circuit::{Circuit, HeldIndex, IndexBuildStats, PrebuildOutcome};
use tokio::sync::RwLock;
use web_time::Instant;

use crate::ports::{Db, DbError};

/// `kind` of every record this module writes to `_00_sync_evidence`.
pub const RECORD_KIND: &str = "ssp_indexes";

/// Layout of [`SspIndexRecord`]. A reader skips records of another format.
pub const RECORD_FORMAT: u32 = 1;

/// How often a serving SSP checks whether what it holds changed.
pub const RECORD_EVERY: Duration = Duration::from_secs(60);

/// A record that still says the same thing is rewritten this often anyway:
/// a reader ages an index that was in use by the record's `at_ms`.
pub const RECORD_REFRESH: Duration = Duration::from_secs(600);

/// Records older than this are deleted by the next write (an SSP id that
/// is gone: every blue/green successor gets a new one).
pub const RECORD_KEEP: Duration = Duration::from_secs(7 * 24 * 3600);

/// How long one step of a background prebuild holds the circuit; the same
/// step a registration takes when it builds an index itself.
pub const BACKGROUND_STEP: Duration = Duration::from_millis(25);

/// Default for `SPKY_SSP_INDEX_PREBUILD_SECS`.
pub const DEFAULT_PREBUILD_TIME: Duration = Duration::from_secs(60);

/// What one SSP held when it last wrote, as stored in
/// `_00_sync_evidence:⟨ssp_indexes:<ssp id>⟩`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SspIndexRecord {
    #[serde(default)]
    pub format: u32,
    pub ssp_id: String,
    /// When it was written, epoch milliseconds.
    pub at_ms: u64,
    #[serde(default)]
    pub indexes: Vec<HeldIndex>,
}

impl SspIndexRecord {
    /// Recorded bytes of every index in it.
    pub fn bytes(&self) -> u64 {
        self.indexes.iter().map(|index| index.bytes as u64).sum()
    }
}

/// The record id one SSP writes. Ids are `ssp-0`, `ssp-0-g<hex>` and the
/// like; anything else is folded to `_` so the id needs no escaping.
pub fn record_id(ssp_id: &str) -> String {
    let safe: String = ssp_id
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.') { c } else { '_' })
        .collect();
    format!("_00_sync_evidence:⟨{RECORD_KIND}:{safe}⟩")
}

/// Write `record` over this SSP's previous one, and delete the records of
/// SSPs that have not written for [`RECORD_KEEP`].
pub async fn write_record(db: &dyn Db, record: &SspIndexRecord) -> Result<(), DbError> {
    let mut content = serde_json::to_value(record).map_err(|e| DbError::Query(e.to_string()))?;
    if let Value::Object(fields) = &mut content {
        fields.insert("kind".into(), Value::from(RECORD_KIND));
    }
    let cutoff = record.at_ms.saturating_sub(RECORD_KEEP.as_millis() as u64);
    // IF NOT EXISTS for a server upgraded before its next CLI deploy, and
    // explicitly private, as for the slow-operation samples.
    let sql = format!(
        "DEFINE TABLE IF NOT EXISTS _00_sync_evidence SCHEMALESS PERMISSIONS NONE; \
         UPSERT {} CONTENT $record RETURN NONE; \
         DELETE _00_sync_evidence WHERE kind = '{RECORD_KIND}' AND at_ms < $cutoff;",
        record_id(&record.ssp_id)
    );
    crate::db_retry::query_retrying(db, &sql, &[("record", content), ("cutoff", Value::from(cutoff))])
        .await
        .map(|_| ())
}

/// Every SSP's record. A database that never had one (no
/// `_00_sync_evidence` table yet) has none; a record of another format, or
/// one that does not read, is left out.
pub async fn read_records(db: &dyn Db) -> Result<Vec<SspIndexRecord>, DbError> {
    let sql = format!("SELECT format, ssp_id, at_ms, indexes FROM _00_sync_evidence WHERE kind = '{RECORD_KIND}';");
    let rows = match db.query(&sql, &[]).await {
        Ok(rows) => rows,
        Err(e) if e.to_string().contains("does not exist") => return Ok(Vec::new()),
        Err(e) => return Err(e),
    };
    let records = match rows.into_iter().next() {
        Some(Value::Array(items)) => items,
        Some(Value::Null) | None => Vec::new(),
        Some(other) => vec![other],
    };
    Ok(records
        .into_iter()
        .filter_map(|item| serde_json::from_value::<SspIndexRecord>(item).ok())
        .filter(|record| record.format == RECORD_FORMAT)
        .collect())
}

/// Writes this SSP's record when what it holds changed, or the last write is
/// [`RECORD_REFRESH`] old. One per process.
pub struct IndexRecorder {
    ssp_id: String,
    last: std::sync::Mutex<Option<Written>>,
}

struct Written {
    fingerprint: Vec<Fingerprint>,
    at_ms: u64,
}

/// What a record says about one index, minus what moves on its own: an
/// index's size grows with every insert, its idle age with the clock.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Fingerprint {
    table: String,
    name: String,
    fields: Vec<String>,
    /// The minute it went idle; `None` while in use.
    idle_since_minute: Option<u64>,
}

fn fingerprint(record: &SspIndexRecord) -> Vec<Fingerprint> {
    record
        .indexes
        .iter()
        .map(|index| Fingerprint {
            table: index.table.clone(),
            name: index.name.clone(),
            fields: index.fields.clone(),
            idle_since_minute: (index.idle_ms > 0).then(|| record.at_ms.saturating_sub(index.idle_ms) / 60_000),
        })
        .collect()
}

impl IndexRecorder {
    pub fn new(ssp_id: impl Into<String>) -> Self {
        Self { ssp_id: ssp_id.into(), last: std::sync::Mutex::new(None) }
    }

    /// The record to write for `held` at `now_ms`, or `None` when the last
    /// one written says the same and is younger than [`RECORD_REFRESH`].
    /// `force` (a shutdown) always writes.
    pub fn due(&self, held: Vec<HeldIndex>, now_ms: u64, force: bool) -> Option<SspIndexRecord> {
        let record = SspIndexRecord { format: RECORD_FORMAT, ssp_id: self.ssp_id.clone(), at_ms: now_ms, indexes: held };
        if force {
            return Some(record);
        }
        let last = self.last.lock().unwrap_or_else(|p| p.into_inner());
        match last.as_ref() {
            Some(written)
                if written.fingerprint == fingerprint(&record)
                    && now_ms.saturating_sub(written.at_ms) < RECORD_REFRESH.as_millis() as u64 =>
            {
                None
            }
            _ => Some(record),
        }
    }

    /// Note that `record` reached the database.
    pub fn written(&self, record: &SspIndexRecord) {
        *self.last.lock().unwrap_or_else(|p| p.into_inner()) =
            Some(Written { fingerprint: fingerprint(record), at_ms: record.at_ms });
    }

    /// Write this SSP's record if it is due (see [`Self::due`]). Returns
    /// whether it wrote. The caller decides whether this SSP may write at
    /// all: only one that serves (not a standby, not retired).
    pub async fn record(&self, db: &dyn Db, processor: &RwLock<Circuit>, force: bool) -> Result<bool, DbError> {
        let held = processor.read().await.held_indexes();
        let Some(record) = self.due(held, crate::now_epoch_ms(), force) else { return Ok(false) };
        write_record(db, &record).await?;
        self.written(&record);
        Ok(true)
    }
}

/// How much a new SSP rebuilds.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PrebuildLimits {
    /// No build starts after this long (one already running finishes).
    /// Zero turns the prebuild from the records off.
    pub time: Duration,
    /// Cap on the recorded bytes of the indexes rebuilt. `None`: as much as
    /// the largest single record held, so a new SSP never takes more index
    /// memory than one SSP before it did (several SSPs' records together
    /// would otherwise add up). Its own line's indexes claim it first.
    pub max_bytes: Option<u64>,
    /// The new circuit's idle grace: an index unread that long would be
    /// released at once, so it is not rebuilt.
    pub idle_grace: Duration,
}

impl PrebuildLimits {
    /// `SPKY_SSP_INDEX_PREBUILD_SECS` (default 60, 0 = off) and
    /// `SPKY_SSP_INDEX_PREBUILD_MAX_MB` (default unset, see
    /// [`Self::max_bytes`]); unparsable values keep the default.
    pub fn from_env(idle_grace: Duration) -> Self {
        Self::parse(
            std::env::var("SPKY_SSP_INDEX_PREBUILD_SECS").ok().as_deref(),
            std::env::var("SPKY_SSP_INDEX_PREBUILD_MAX_MB").ok().as_deref(),
            idle_grace,
        )
    }

    pub fn parse(secs: Option<&str>, max_mb: Option<&str>, idle_grace: Duration) -> Self {
        let time = secs
            .and_then(|v| v.trim().parse::<u64>().ok())
            .map(Duration::from_secs)
            .unwrap_or(DEFAULT_PREBUILD_TIME);
        let max_bytes = max_mb.and_then(|v| v.trim().parse::<u64>().ok()).map(|mb| mb.saturating_mul(1024 * 1024));
        Self { time, max_bytes, idle_grace }
    }
}

/// One index to rebuild.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Wanted {
    pub table: String,
    pub name: String,
    pub fields: Vec<String>,
    /// How long it has gone unread, as of now: carried over to the rebuild.
    pub idle: Duration,
    /// Its recorded size.
    pub bytes: u64,
}

impl Wanted {
    fn label(&self) -> String {
        format!("{}.{}", self.table, self.name)
    }
}

/// Why an index was not rebuilt before Ready.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SkipReason {
    /// Unread for at least the idle grace: it would be released at once.
    IdlePastGrace,
    /// Over [`PrebuildLimits::max_bytes`].
    MemoryBudget,
    /// [`PrebuildLimits::time`] ran out first.
    TimeBudget,
    /// The SSP stopped serving (shutdown, a re-bootstrap) before it got to it.
    Stopped,
    /// The schema does not define it (or its table) any more.
    NotDefined,
    /// The schema defines it over other fields now.
    Redefined,
}

/// What [`select`] picked.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Selection {
    /// Most recently read first.
    pub build: Vec<Wanted>,
    pub skipped: Vec<(Wanted, SkipReason)>,
    /// The byte budget it applied.
    pub byte_budget: u64,
}

/// Which records are a starting SSP's own line: written by itself (an
/// in-place re-bootstrap), by the SSP it replaces (`SPKY_SSP_REPLACES`), or by
/// an SSP of the same `ssp-N` slot (a blue/green successor of `ssp-0` is
/// `ssp-0-g<hex>`). With several SSPs, the others' indexes are what other
/// clients read; [`select`] gives this line's first claim on the budget.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Lineage {
    ssp_id: String,
    replaces: Option<String>,
}

impl Lineage {
    pub fn new(ssp_id: impl Into<String>, replaces: Option<String>) -> Self {
        Self { ssp_id: ssp_id.into(), replaces: replaces.filter(|r| !r.is_empty()) }
    }

    /// Whether the SSP `writer` wrote a record of this line.
    pub fn owns(&self, writer: &str) -> bool {
        if self.ssp_id.is_empty() {
            return false;
        }
        writer == self.ssp_id
            || self.replaces.as_deref() == Some(writer)
            || slot(writer).is_some_and(|slot| Some(slot) == self::slot(&self.ssp_id))
    }
}

/// `ssp-N` of `ssp-N` or `ssp-N-<anything>`; `None` for any other id.
fn slot(id: &str) -> Option<&str> {
    let rest = id.strip_prefix("ssp-")?;
    let digits = rest.bytes().take_while(u8::is_ascii_digit).count();
    let end = "ssp-".len() + digits;
    match (digits, id.as_bytes().get(end)) {
        (0, _) => None,
        (_, None | Some(b'-')) => Some(&id[..end]),
        _ => None,
    }
}

/// Pick what to rebuild from every SSP's record, at `now_ms`.
///
/// An index several records list counts once, as the most recently read of
/// them. Its idle age now is what the record said plus the time since the
/// record was written (an index in use then has been unread at most that
/// long): an SSP that stopped writing (retired, crashed) ages out on its own.
///
/// The indexes of `own` line come first, most recently read first; other
/// SSPs' records only fill what is left of the byte budget, so with several
/// SSPs one does not spend its budget on another's indexes and skip its own.
pub fn select(records: &[SspIndexRecord], now_ms: u64, limits: &PrebuildLimits, own: &Lineage) -> Selection {
    let mut merged: BTreeMap<(String, String), Wanted> = BTreeMap::new();
    let mut own_keys: std::collections::BTreeSet<(String, String)> = Default::default();
    for record in records.iter().filter(|r| r.format == RECORD_FORMAT) {
        let since = now_ms.saturating_sub(record.at_ms);
        let mine = own.owns(&record.ssp_id);
        for index in &record.indexes {
            let wanted = Wanted {
                table: index.table.clone(),
                name: index.name.clone(),
                fields: index.fields.clone(),
                idle: Duration::from_millis(index.idle_ms.saturating_add(since)),
                bytes: index.bytes as u64,
            };
            let key = (wanted.table.clone(), wanted.name.clone());
            if mine {
                own_keys.insert(key.clone());
            }
            merged
                .entry(key)
                .and_modify(|seen| {
                    if wanted.idle < seen.idle {
                        *seen = wanted.clone();
                    }
                })
                .or_insert(wanted);
        }
    }
    let (mut first, mut rest) = (Vec::new(), Vec::new());
    for (key, wanted) in merged {
        if own_keys.contains(&key) { first.push(wanted) } else { rest.push(wanted) }
    }
    let by_recency = |a: &Wanted, b: &Wanted| a.idle.cmp(&b.idle).then_with(|| (&a.table, &a.name).cmp(&(&b.table, &b.name)));
    first.sort_by(by_recency);
    rest.sort_by(by_recency);

    let byte_budget = limits
        .max_bytes
        .unwrap_or_else(|| records.iter().map(SspIndexRecord::bytes).max().unwrap_or(0));
    let mut selection = Selection { byte_budget, ..Default::default() };
    let mut bytes = 0u64;
    for index in first.into_iter().chain(rest) {
        if index.idle >= limits.idle_grace {
            selection.skipped.push((index, SkipReason::IdlePastGrace));
        } else if bytes.saturating_add(index.bytes) > byte_budget {
            selection.skipped.push((index, SkipReason::MemoryBudget));
        } else {
            bytes += index.bytes;
            selection.build.push(index);
        }
    }
    selection
}

/// The indexes a warm restart's local file names (`indexes.json`, no idle
/// age or size), as a [`Selection`]: each with the fields the circuit's
/// schema defines for it now, those it does not define skipped.
pub fn local_selection(circuit: &Circuit, wanted: &BTreeMap<String, Vec<String>>) -> Selection {
    let mut selection = Selection::default();
    for (table, names) in wanted {
        for name in names {
            let fields = circuit.index_def(table, name).map(|def| def.fields.clone());
            let index = Wanted {
                table: table.clone(),
                name: name.clone(),
                fields: fields.clone().unwrap_or_default(),
                idle: Duration::ZERO,
                bytes: 0,
            };
            match fields {
                Some(_) => selection.build.push(index),
                None => selection.skipped.push((index, SkipReason::NotDefined)),
            }
        }
    }
    selection
}

/// What [`prebuild`] did.
#[derive(Debug, Clone, Default)]
pub struct PrebuildReport {
    /// `table.index`, and the build's milliseconds.
    pub built: Vec<(String, f64)>,
    /// Held already (a re-registered view built it).
    pub already_built: usize,
    pub skipped: Vec<(String, SkipReason)>,
    pub stats: IndexBuildStats,
    pub elapsed: Duration,
}

impl PrebuildReport {
    /// `table.index` of every skipped index with the reason, for a log line.
    pub fn skipped_summary(&self) -> String {
        self.skipped
            .iter()
            .map(|(label, reason)| format!("{label} ({})", serde_json::to_value(reason).ok().and_then(|v| v.as_str().map(str::to_owned)).unwrap_or_default()))
            .collect::<Vec<_>>()
            .join(", ")
    }

    pub fn built_summary(&self) -> String {
        self.built.iter().map(|(label, ms)| format!("{label} {ms:.0}ms")).collect::<Vec<_>>().join(", ")
    }
}

/// Build what `selection` picked, most recently read first, until
/// `time_budget` runs out. One read lock per index: writes and every other
/// reader get in between two builds, never during one.
pub async fn prebuild(processor: &RwLock<Circuit>, selection: Selection, time_budget: Duration) -> PrebuildReport {
    let started = Instant::now();
    let mut report = PrebuildReport::default();
    report.skipped = selection.skipped.iter().map(|(index, reason)| (index.label(), *reason)).collect();
    for index in selection.build {
        if started.elapsed() >= time_budget {
            report.skipped.push((index.label(), SkipReason::TimeBudget));
            continue;
        }
        let outcome = processor.read().await.prebuild_held_index(&index.table, &index.name, &index.fields, index.idle);
        match outcome {
            PrebuildOutcome::Built(stats) => {
                report.built.push((index.label(), stats.index_build_ms));
                report.stats += stats;
            }
            PrebuildOutcome::AlreadyBuilt => report.already_built += 1,
            PrebuildOutcome::NotDefined => report.skipped.push((index.label(), SkipReason::NotDefined)),
            PrebuildOutcome::Redefined => report.skipped.push((index.label(), SkipReason::Redefined)),
            // The one-pass build never leaves an index part built.
            PrebuildOutcome::InProgress(stats) => report.stats += stats,
        }
        crate::node::yield_now().await;
    }
    report.elapsed = started.elapsed();
    report
}

/// [`prebuild`] for an SSP that already serves: each index is built `step`
/// at a time, the circuit's read lock held for one step and freed in
/// between, so ingest and registrations keep flowing. A registration that
/// needs an index this is building advances the same part-built index (see
/// [`Circuit::prebuild_held_index_step`]); between them every row is read
/// once. Past `time_budget` no new index starts (the one under way
/// finishes). `should_stop` is asked before every step: once it says so the
/// rest is reported [`SkipReason::Stopped`] and a part-built index is left
/// for whoever needs it next to finish.
pub async fn prebuild_in_background(
    processor: &RwLock<Circuit>,
    selection: Selection,
    time_budget: Duration,
    step: Duration,
    should_stop: impl Fn() -> bool,
) -> PrebuildReport {
    let started = Instant::now();
    let mut report = PrebuildReport::default();
    report.skipped = selection.skipped.iter().map(|(index, reason)| (index.label(), *reason)).collect();
    let mut stopped = false;
    for index in selection.build {
        if stopped || should_stop() {
            stopped = true;
            report.skipped.push((index.label(), SkipReason::Stopped));
            continue;
        }
        if started.elapsed() >= time_budget {
            report.skipped.push((index.label(), SkipReason::TimeBudget));
            continue;
        }
        let mut mine = IndexBuildStats::default();
        loop {
            let outcome = processor
                .read()
                .await
                .prebuild_held_index_step(&index.table, &index.name, &index.fields, index.idle, step);
            match outcome {
                PrebuildOutcome::InProgress(stats) => mine += stats,
                PrebuildOutcome::Built(stats) => {
                    mine += stats;
                    report.built.push((index.label(), mine.index_build_ms));
                    break;
                }
                // Finished by a registration that needed it meanwhile, or
                // built before this started.
                PrebuildOutcome::AlreadyBuilt if mine.rows_indexed > 0 => {
                    report.built.push((index.label(), mine.index_build_ms));
                    break;
                }
                PrebuildOutcome::AlreadyBuilt => {
                    report.already_built += 1;
                    break;
                }
                PrebuildOutcome::NotDefined => {
                    report.skipped.push((index.label(), SkipReason::NotDefined));
                    break;
                }
                PrebuildOutcome::Redefined => {
                    report.skipped.push((index.label(), SkipReason::Redefined));
                    break;
                }
            }
            crate::node::yield_now().await;
            if should_stop() {
                stopped = true;
                report.skipped.push((index.label(), SkipReason::Stopped));
                break;
            }
        }
        report.stats.index_build_ms += mine.index_build_ms;
        report.stats.rows_indexed += mine.rows_indexed;
        crate::node::yield_now().await;
    }
    report.stats.indexes_built = report.built.len();
    report.elapsed = started.elapsed();
    report
}

#[cfg(test)]
mod tests {
    use super::*;

    fn held(table: &str, name: &str, idle_ms: u64, bytes: usize) -> HeldIndex {
        HeldIndex { table: table.into(), name: name.into(), fields: vec!["f".into()], idle_ms, rows: 10, bytes }
    }

    fn record(ssp: &str, at_ms: u64, indexes: Vec<HeldIndex>) -> SspIndexRecord {
        SspIndexRecord { format: RECORD_FORMAT, ssp_id: ssp.into(), at_ms, indexes }
    }

    fn limits(grace_secs: u64, max_bytes: Option<u64>) -> PrebuildLimits {
        PrebuildLimits { time: DEFAULT_PREBUILD_TIME, max_bytes, idle_grace: Duration::from_secs(grace_secs) }
    }

    fn names(list: &[Wanted]) -> Vec<String> {
        list.iter().map(Wanted::label).collect()
    }

    #[test]
    fn select_ages_each_index_from_its_record_and_drops_what_is_past_the_grace() {
        let now: u64 = 1_000_000_000;
        let records = vec![record(
            "ssp-0",
            now - 120_000,
            vec![
                held("game", "in_use", 0, 100),
                held("game", "idle_20m", 20 * 60_000, 100),
                held("game", "idle_59m", 59 * 60_000, 100),
            ],
        )];
        let picked = select(&records, now, &limits(3600, None), &Lineage::default());
        assert_eq!(names(&picked.build), vec!["game.in_use", "game.idle_20m"], "most recently read first");
        assert_eq!(picked.build[0].idle, Duration::from_secs(120), "in use when written two minutes ago");
        assert_eq!(picked.build[1].idle, Duration::from_secs(22 * 60));
        // 59 min + the 2 min since the record was written is past the hour.
        assert_eq!(picked.skipped, vec![(
            Wanted { table: "game".into(), name: "idle_59m".into(), fields: vec!["f".into()], idle: Duration::from_secs(61 * 60), bytes: 100 },
            SkipReason::IdlePastGrace,
        )]);
        // A record nobody refreshed for a day ages out entirely.
        let stale = vec![record("ssp-old", now - 86_400_000, vec![held("game", "in_use", 0, 100)])];
        let picked = select(&stale, now, &limits(3600, None), &Lineage::default());
        assert!(picked.build.is_empty());
        assert_eq!(picked.skipped[0].1, SkipReason::IdlePastGrace);
    }

    #[test]
    fn select_counts_an_index_once_across_records_as_its_most_recent_reader() {
        let now = 10_000_000;
        let blue = record("ssp-0", now - 30_000, vec![held("game", "white", 600_000, 50), held("game", "black", 0, 50)]);
        let retired = record("ssp-0-gold", now - 1_800_000, vec![held("game", "white", 0, 50), held("contact", "rank", 0, 20)]);
        let picked = select(&[blue.clone(), retired.clone()], now, &limits(3600, None), &Lineage::default());
        assert_eq!(names(&picked.build), vec!["game.black", "game.white"]);
        assert_eq!(picked.build[1].idle, Duration::from_secs(630), "blue's age, newer than the retired record's 30 min");
        // Together they list 120 bytes; one SSP never held more than 100.
        assert_eq!(picked.byte_budget, 100, "the larger record's total");
        assert_eq!(picked.skipped.iter().map(|(w, r)| (w.label(), *r)).collect::<Vec<_>>(), vec![("contact.rank".to_string(), SkipReason::MemoryBudget)]);
        let picked = select(&[blue, retired], now, &limits(3600, Some(1 << 20)), &Lineage::default());
        assert_eq!(names(&picked.build), vec!["game.black", "game.white", "contact.rank"]);
    }

    #[test]
    fn select_keeps_to_the_byte_budget_most_recently_read_first() {
        let now = 1_000_000;
        let one = record("a", now, vec![held("t", "fresh", 0, 60), held("t", "older", 1_000, 60), held("t", "small", 2_000, 30)]);
        let other = record("b", now, vec![held("t", "x", 3_000, 10)]);
        let picked = select(&[one.clone(), other.clone()], now, &limits(3600, None), &Lineage::default());
        // Budget = the largest record, 150: fresh (60) + older (60) + small (30).
        assert_eq!(picked.byte_budget, 150);
        assert_eq!(names(&picked.build), vec!["t.fresh", "t.older", "t.small"]);
        assert_eq!(picked.skipped.iter().map(|(w, r)| (w.label(), *r)).collect::<Vec<_>>(), vec![("t.x".to_string(), SkipReason::MemoryBudget)]);
        // An explicit cap skips the big one that does not fit and keeps going.
        let picked = select(&[one, other], now, &limits(3600, Some(100)), &Lineage::default());
        assert_eq!(names(&picked.build), vec!["t.fresh", "t.small", "t.x"]);
        assert_eq!(picked.skipped.iter().map(|(w, r)| (w.label(), *r)).collect::<Vec<_>>(), vec![("t.older".to_string(), SkipReason::MemoryBudget)]);
    }

    #[test]
    fn select_gives_its_own_line_the_budget_first() {
        let now = 1_000_000;
        let ssp0 = record("ssp-0", now - 1_000, vec![held("game", "a", 0, 80), held("game", "b", 0, 80), held("game", "c", 0, 40)]);
        let ssp1 = record("ssp-1", now - 50_000, vec![held("contact", "x", 20_000, 60), held("contact", "y", 30_000, 40)]);
        // Read across all SSPs alike, ssp-0's fresher indexes take the whole
        // budget (200, ssp-0's total) and ssp-1's own are skipped.
        let mixed = select(&[ssp0.clone(), ssp1.clone()], now, &limits(3600, None), &Lineage::default());
        assert_eq!(names(&mixed.build), vec!["game.a", "game.b", "game.c"]);
        // ssp-1's blue/green successor: its own line first, ssp-0's records
        // only fill what is left.
        let own = Lineage::new("ssp-1-g7", Some("ssp-1".into()));
        let picked = select(&[ssp0, ssp1], now, &limits(3600, None), &own);
        assert_eq!(names(&picked.build), vec!["contact.x", "contact.y", "game.a"]);
        assert_eq!(
            picked.skipped.iter().map(|(w, r)| (w.label(), *r)).collect::<Vec<_>>(),
            vec![("game.b".to_string(), SkipReason::MemoryBudget), ("game.c".to_string(), SkipReason::MemoryBudget)]
        );
    }

    #[test]
    fn a_lineage_is_the_ssp_itself_the_one_it_replaces_and_its_slot() {
        let own = Lineage::new("ssp-0-gab12", Some("custom-blue".into()));
        for writer in ["ssp-0-gab12", "custom-blue", "ssp-0", "ssp-0-g99"] {
            assert!(own.owns(writer), "{writer}");
        }
        for writer in ["ssp-1", "ssp-01", "ssp-10-g1", "other", ""] {
            assert!(!own.owns(writer), "{writer}");
        }
        // A generated `ssp-<uuid>` id has no slot: only itself.
        let uuid = Lineage::new("ssp-550e8400-e29b", None);
        assert!(uuid.owns("ssp-550e8400-e29b"));
        assert!(!uuid.owns("ssp-550e8400-ffff"));
        assert!(!uuid.owns("ssp-550"));
        assert!(!Lineage::default().owns(""), "no id, no own line");
    }

    #[test]
    fn select_skips_records_of_another_format_and_a_zero_grace_rebuilds_nothing() {
        let now = 1_000_000;
        let mut future = record("a", now, vec![held("t", "i", 0, 1)]);
        future.format = RECORD_FORMAT + 1;
        assert!(select(&[future], now, &limits(3600, None), &Lineage::default()).build.is_empty());
        let current = record("a", now - 1, vec![held("t", "i", 0, 1)]);
        assert!(select(&[current], now, &limits(0, None), &Lineage::default()).build.is_empty(), "grace 0: nothing outlives its reader");
    }

    #[test]
    fn the_recorder_writes_on_change_and_refreshes_an_unchanged_record() {
        let recorder = IndexRecorder::new("ssp-0");
        let t0 = 100 * 60_000;
        let first = recorder.due(vec![held("t", "i", 0, 10)], t0, false).expect("nothing written yet");
        recorder.written(&first);
        // Size and the clock move on their own: not a change.
        assert!(recorder.due(vec![held("t", "i", 0, 99)], t0 + 60_000, false).is_none());
        // The view went: idle now, a change.
        let idle = recorder.due(vec![held("t", "i", 30_000, 10)], t0 + 120_000, false).expect("went idle");
        recorder.written(&idle);
        // Still idle since the same moment a minute later: unchanged.
        assert!(recorder.due(vec![held("t", "i", 90_000, 10)], t0 + 180_000, false).is_none());
        // ...until the record is RECORD_REFRESH old.
        let later = t0 + 120_000 + RECORD_REFRESH.as_millis() as u64;
        assert!(recorder.due(vec![held("t", "i", 30_000 + RECORD_REFRESH.as_millis() as u64, 10)], later, false).is_some());
        // A new index, or a shutdown, always writes.
        assert!(recorder.due(vec![held("t", "i", 90_000, 10), held("t", "j", 0, 10)], t0 + 180_000, false).is_some());
        assert!(recorder.due(vec![held("t", "i", 90_000, 10)], t0 + 180_000, true).is_some());
    }

    #[test]
    fn limits_read_seconds_and_megabytes() {
        let grace = Duration::from_secs(3600);
        assert_eq!(PrebuildLimits::parse(None, None, grace), PrebuildLimits { time: DEFAULT_PREBUILD_TIME, max_bytes: None, idle_grace: grace });
        let set = PrebuildLimits::parse(Some(" 0 "), Some("512"), grace);
        assert_eq!((set.time, set.max_bytes), (Duration::ZERO, Some(512 * 1024 * 1024)));
        let bad = PrebuildLimits::parse(Some("1m"), Some("lots"), grace);
        assert_eq!((bad.time, bad.max_bytes), (DEFAULT_PREBUILD_TIME, None));
    }

    #[test]
    fn record_ids_need_no_escaping() {
        assert_eq!(record_id("ssp-0-g3fa9"), "_00_sync_evidence:⟨ssp_indexes:ssp-0-g3fa9⟩");
        assert_eq!(record_id("ssp⟩; DELETE x"), "_00_sync_evidence:⟨ssp_indexes:ssp___DELETE_x⟩");
    }
}
