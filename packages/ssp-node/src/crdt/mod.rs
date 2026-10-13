//! Server side of `@crdt` fields.
//!
//! Clients edit a field as a Loro document and append each batch of their own
//! edits as a row of the generated `<parent>__crdt_log` table; everyone
//! receives the rows through ordinary views and merges them locally. The SSP
//! is the only writer of a document's canonical state: it folds the log into
//! `_00_crdt_doc` (snapshot + version) and writes the parent field's plain-text
//! mirror, so no client can ever publish a document that differs from what
//! its log contains.
//!
//! Folding costs CPU, so it runs rarely: when a field has been idle for a
//! while, or its log has grown past a row or byte budget, never more often
//! than a minimum gap, and on a bounded number of workers. Nothing waits on a
//! fold: a late one only delays the snapshot new devices start from and the
//! mirror's freshness.
//!
//! In a cluster every SSP sees every log row, so each tracks every document;
//! a lease on the doc row decides which one folds.

pub mod acceptance;
mod fold;
pub mod hash;
pub mod mirror;
mod sql;

use std::collections::{BTreeMap, HashMap, HashSet};
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use lru::LruCache;
use serde_json::{json, Value};
use tracing::{info, warn};

pub use fold::FoldOutcome;
use fold::Cached;
pub use mirror::Kind;

use crate::ports::{Db, Spawner};

/// Fold tuning. `Default` is what a deployment gets without env overrides.
#[derive(Debug, Clone)]
pub struct FoldConfig {
    /// Hot documents kept in memory between folds. Env `SPKY_CRDT_CACHE_SIZE`.
    pub cache_size: usize,
    /// Fold a field once it has had no new rows for this long.
    pub idle_ms: u64,
    /// ...or once this many rows are pending, whatever the activity.
    pub max_rows: u64,
    /// ...or once this many bytes are pending.
    pub max_bytes: u64,
    /// Never fold one field more often than this.
    pub min_gap_ms: u64,
    /// Folds running at once on this node.
    pub workers: usize,
    /// How often the node looks for due folds (`TimerKind::CrdtFoldTick`).
    pub tick_ms: u64,
    /// Tracker rebuild + orphan and presence cleanup (`TimerKind::CrdtSweep`).
    pub sweep_secs: u64,
    /// Presence rows older than this are removed by the sweep.
    pub presence_ttl_secs: u64,
    /// How long a fold's lease holds before another node may take over.
    pub lease_secs: u64,
    /// Log rows one fold reads at most; the rest wait for the next fold.
    pub rows_per_fold: usize,
    /// Rows per delete statement after a fold.
    pub delete_chunk: usize,
    /// Largest blob the acceptance rule admits (the column's own cap).
    pub max_delta_bytes: usize,
}

impl Default for FoldConfig {
    fn default() -> Self {
        Self {
            cache_size: 1024,
            // A short title edit should reach every list within seconds; folds
            // stay cheap (a delta into a cached document) and `min_gap_ms`
            // still caps each field at one fold per window.
            idle_ms: 5_000,
            max_rows: 200,
            max_bytes: 256 * 1024,
            min_gap_ms: 10_000,
            workers: 2,
            tick_ms: 5_000,
            sweep_secs: 600,
            presence_ttl_secs: 60,
            lease_secs: 30,
            rows_per_fold: 2000,
            delete_chunk: 50,
            max_delta_bytes: 256 * 1024,
        }
    }
}

/// One document field: `(log table, parent record, field)`.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct DocKey {
    pub log: String,
    pub doc: String,
    pub field: String,
}

#[derive(Debug, Default, Clone)]
struct Pending {
    rows: u64,
    bytes: u64,
    last_write_ms: u64,
    last_fold_ms: u64,
    /// Lease lost or fold failed: leave this field alone until then.
    retry_at_ms: u64,
}

impl Pending {
    fn due(&self, cfg: &FoldConfig, now: u64) -> bool {
        self.rows > 0
            && now >= self.retry_at_ms
            && now.saturating_sub(self.last_fold_ms) >= cfg.min_gap_ms
            && (now.saturating_sub(self.last_write_ms) >= cfg.idle_ms
                || self.rows >= cfg.max_rows
                || self.bytes >= cfg.max_bytes)
    }
}

/// Counters surfaced on `/info`.
#[derive(Debug, Default)]
pub struct FoldStats {
    pub folds: AtomicU64,
    pub rows_folded: AtomicU64,
    pub rows_rejected: AtomicU64,
    pub leases_lost: AtomicU64,
    pub errors: AtomicU64,
}

pub struct CrdtFold {
    cfg: FoldConfig,
    /// Lease owner name: this node's SSP id.
    me: String,
    /// Log table -> `field -> kind`, from the tables' `sp00ky:crdt_log` markers.
    tables: std::sync::RwLock<BTreeMap<String, BTreeMap<String, Kind>>>,
    pending: std::sync::Mutex<HashMap<DocKey, Pending>>,
    running: std::sync::Mutex<HashSet<DocKey>>,
    cache: std::sync::Mutex<LruCache<DocKey, Cached>>,
    pub stats: FoldStats,
}

impl CrdtFold {
    pub fn new(cfg: FoldConfig, me: impl Into<String>) -> Self {
        let cap = NonZeroUsize::new(cfg.cache_size.max(1)).expect("non-zero");
        Self {
            cfg,
            me: me.into(),
            tables: Default::default(),
            pending: Default::default(),
            running: Default::default(),
            cache: std::sync::Mutex::new(LruCache::new(cap)),
            stats: FoldStats::default(),
        }
    }

    pub fn config(&self) -> &FoldConfig {
        &self.cfg
    }

    /// Note an ingested log row. Cheap and synchronous: called on the ingest
    /// path for every row of every table.
    pub fn observe(&self, table: &str, op: &str, record: &Value, now_ms: u64) {
        if !table.ends_with(ssp_protocol::CRDT_LOG_SUFFIX) || !op.eq_ignore_ascii_case("CREATE") {
            return;
        }
        let (Some(doc), Some(field)) = (
            record.get("doc").and_then(Value::as_str),
            record.get("field").and_then(Value::as_str),
        ) else {
            return;
        };
        let key = DocKey { log: table.to_string(), doc: doc.to_string(), field: field.to_string() };
        let size = record.get("size").and_then(Value::as_u64).unwrap_or(0);
        let mut pending = self.pending.lock().expect("pending lock");
        let entry = pending.entry(key).or_default();
        entry.rows += 1;
        entry.bytes += size;
        entry.last_write_ms = now_ms;
    }

    /// Fields due for a fold, up to the free worker slots, marked running.
    fn take_due(&self, now_ms: u64) -> Vec<DocKey> {
        let mut running = self.running.lock().expect("running lock");
        let free = self.cfg.workers.saturating_sub(running.len());
        if free == 0 {
            return Vec::new();
        }
        let pending = self.pending.lock().expect("pending lock");
        let mut due: Vec<(&DocKey, &Pending)> = pending
            .iter()
            .filter(|(key, p)| !running.contains(*key) && p.due(&self.cfg, now_ms))
            .collect();
        // Biggest backlog first.
        due.sort_by(|a, b| b.1.rows.cmp(&a.1.rows).then_with(|| a.0.cmp(b.0)));
        // One fold per record at a time: every field's fold writes the record
        // (its mirror and stamp), so two at once conflict and the loser waits
        // out `min_gap_ms`. The other field goes on a later tick.
        let mut records: HashSet<&str> = running.iter().map(|k| k.doc.as_str()).collect();
        let keys: Vec<DocKey> = due
            .into_iter()
            .filter(|(k, _)| records.insert(k.doc.as_str()))
            .take(free)
            .map(|(k, _)| k.clone())
            .collect();
        running.extend(keys.iter().cloned());
        keys
    }

    /// Start the folds that are due (`TimerKind::CrdtFoldTick`).
    pub fn tick(self: &Arc<Self>, db: Arc<dyn Db>, spawner: &dyn Spawner, now_ms: u64) {
        for key in self.take_due(now_ms) {
            let this = Arc::clone(self);
            let db = Arc::clone(&db);
            spawner.spawn(Box::pin(async move {
                this.fold_one(db.as_ref(), key).await;
            }));
        }
    }

    /// Fold one field now and update the tracker. Public for tests and for a
    /// caller that already knows a field needs folding.
    pub async fn fold_one(&self, db: &dyn Db, key: DocKey) -> Option<FoldOutcome> {
        let kind = self.kind_of(db, &key).await;
        let result = match kind {
            Some(kind) => {
                let cached = self.cache.lock().expect("cache lock").pop(&key);
                fold::fold(db, &self.cfg, &self.me, &key, kind, cached, crate::now_epoch_ms() as i64).await
            }
            None => Err(anyhow::anyhow!("`{}` is not a known CRDT log table", key.log)),
        };
        let now = crate::now_epoch_ms();
        let outcome = match result {
            Ok((outcome, cached)) => {
                if let Some(cached) = cached {
                    self.cache.lock().expect("cache lock").put(key.clone(), cached);
                }
                self.record(&key, &outcome, now);
                Some(outcome)
            }
            Err(e) => {
                self.stats.errors.fetch_add(1, Ordering::Relaxed);
                warn!(doc = %key.doc, field = %key.field, error = %e, "CRDT fold failed");
                if let Some(p) = self.pending.lock().expect("pending lock").get_mut(&key) {
                    p.retry_at_ms = now + self.cfg.min_gap_ms;
                }
                None
            }
        };
        self.running.lock().expect("running lock").remove(&key);
        outcome
    }

    fn record(&self, key: &DocKey, outcome: &FoldOutcome, now: u64) {
        let mut pending = self.pending.lock().expect("pending lock");
        let entry = pending.entry(key.clone()).or_default();
        if outcome.lost {
            self.stats.leases_lost.fetch_add(1, Ordering::Relaxed);
            // The holder folds; look again once its lease could have lapsed.
            entry.retry_at_ms = now + self.cfg.lease_secs * 1000 + jitter(&self.me, key);
            return;
        }
        self.stats.folds.fetch_add(1, Ordering::Relaxed);
        self.stats.rows_folded.fetch_add(outcome.folded as u64, Ordering::Relaxed);
        self.stats.rows_rejected.fetch_add(outcome.rejected as u64, Ordering::Relaxed);
        let removed = (outcome.folded + outcome.rejected) as u64;
        entry.rows = entry.rows.saturating_sub(removed);
        entry.bytes = if entry.rows == 0 { 0 } else { entry.bytes };
        entry.last_fold_ms = now;
        entry.retry_at_ms = 0;
        if outcome.more {
            entry.rows = entry.rows.max(self.cfg.max_rows);
        } else if removed == 0 {
            // Only rows waiting on predecessors: nothing a refold could change
            // until new rows arrive (or the sweep recounts).
            entry.rows = 0;
        }
    }

    async fn kind_of(&self, db: &dyn Db, key: &DocKey) -> Option<Kind> {
        if let Some(kind) = self.lookup(key) {
            return Some(kind);
        }
        if let Err(e) = self.refresh_tables(db).await {
            warn!(error = %e, "CRDT: reading log tables failed");
        }
        self.lookup(key)
    }

    fn lookup(&self, key: &DocKey) -> Option<Kind> {
        self.tables.read().expect("tables lock").get(&key.log)?.get(&key.field).copied()
    }

    /// Re-read which log tables exist and their fields' kinds.
    pub async fn refresh_tables(&self, db: &dyn Db) -> anyhow::Result<()> {
        let info = db.query("INFO FOR DB", &[]).await?;
        let mut found = BTreeMap::new();
        if let Some(tables) = info.first().and_then(|i| i.get("tables")).and_then(Value::as_object) {
            for (name, define) in tables {
                let Some(fields) = define.as_str().and_then(ssp_protocol::crdt_log_fields) else {
                    continue;
                };
                let kinds = fields
                    .into_iter()
                    .filter_map(|(field, kind)| Some((field, Kind::parse(&kind)?)))
                    .collect();
                found.insert(name.clone(), kinds);
            }
        }
        *self.tables.write().expect("tables lock") = found;
        Ok(())
    }

    /// Rebuild the tracker from the log tables (after a restart nothing was
    /// observed), drop rows of deleted parents and stale presence
    /// (`TimerKind::CrdtSweep`).
    pub async fn sweep(&self, db: &dyn Db) -> anyhow::Result<()> {
        self.refresh_tables(db).await?;
        let logs: Vec<String> = self.tables.read().expect("tables lock").keys().cloned().collect();
        let now = crate::now_epoch_ms();
        for log in logs {
            db.query(sql::ORPHANS, &[("log", json!(log))]).await?;
            let presence = format!(
                "{}{}",
                log.trim_end_matches(ssp_protocol::CRDT_LOG_SUFFIX),
                ssp_protocol::CRDT_PRESENCE_SUFFIX
            );
            let ttl = format!("{}s", self.cfg.presence_ttl_secs);
            if let Err(e) = db
                .query(sql::STALE_PRESENCE, &[("presence", json!(presence)), ("ttl", json!(ttl))])
                .await
            {
                // A parent without `@cursor` fields has no presence table.
                tracing::debug!(table = %presence, error = %e, "presence sweep skipped");
            }
            let res = db.query(sql::PENDING, &[("log", json!(log))]).await?;
            let Some(Value::Array(groups)) = res.into_iter().next() else { continue };
            let mut pending = self.pending.lock().expect("pending lock");
            for g in groups {
                let (Some(doc), Some(field)) =
                    (g.get("doc").and_then(Value::as_str), g.get("field").and_then(Value::as_str))
                else {
                    continue;
                };
                let key = DocKey { log: log.clone(), doc: doc.to_string(), field: field.to_string() };
                let entry = pending.entry(key).or_default();
                entry.rows = entry.rows.max(g.get("rows").and_then(Value::as_u64).unwrap_or(0));
                entry.bytes = entry.bytes.max(g.get("bytes").and_then(Value::as_u64).unwrap_or(0));
                if entry.last_write_ms == 0 {
                    // Unknown age: treat as written now, so it folds after one idle window.
                    entry.last_write_ms = now;
                }
            }
        }
        let tracked = self.pending.lock().expect("pending lock").values().filter(|p| p.rows > 0).count();
        if tracked > 0 {
            info!(fields = tracked, "CRDT sweep: fields with pending edits");
        }
        Ok(())
    }

    pub fn info(&self) -> Value {
        let pending = self.pending.lock().expect("pending lock");
        json!({
            "pending_fields": pending.values().filter(|p| p.rows > 0).count(),
            "pending_rows": pending.values().map(|p| p.rows).sum::<u64>(),
            "running": self.running.lock().expect("running lock").len(),
            "folds": self.stats.folds.load(Ordering::Relaxed),
            "rows_folded": self.stats.rows_folded.load(Ordering::Relaxed),
            "rows_rejected": self.stats.rows_rejected.load(Ordering::Relaxed),
            "leases_lost": self.stats.leases_lost.load(Ordering::Relaxed),
            "errors": self.stats.errors.load(Ordering::Relaxed),
        })
    }
}

/// Spread retries of nodes that lost the same lease (deterministic per node
/// and field, so no randomness source is needed on wasm).
fn jitter(me: &str, key: &DocKey) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in me.bytes().chain(key.doc.bytes()).chain(key.field.bytes()) {
        h = (h ^ u64::from(b)).wrapping_mul(0x0100_0000_01b3);
    }
    h % 5_000
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key() -> DocKey {
        DocKey { log: "thread__crdt_log".into(), doc: "thread:t1".into(), field: "content".into() }
    }

    fn row(size: u64) -> Value {
        json!({ "doc": "thread:t1", "field": "content", "size": size })
    }

    #[test]
    fn folds_when_idle_or_over_budget_and_never_too_often() {
        let cfg = FoldConfig { idle_ms: 1000, max_rows: 3, min_gap_ms: 500, workers: 4, ..Default::default() };
        let fold = CrdtFold::new(cfg, "ssp-a");
        fold.observe("thread", "CREATE", &row(1), 0); // not a log table
        fold.observe("thread__crdt_log", "DELETE", &row(1), 0); // not a create
        assert!(fold.take_due(10_000).is_empty());

        fold.observe("thread__crdt_log", "CREATE", &row(10), 1_000);
        assert!(fold.take_due(1_500).is_empty(), "still active");
        assert_eq!(fold.take_due(2_000), vec![key()], "idle window passed");
        assert!(fold.take_due(2_000).is_empty(), "already running");
        fold.running.lock().unwrap().clear();

        fold.record(&key(), &FoldOutcome { folded: 1, ..Default::default() }, 2_000);
        for t in [2_100, 2_200, 2_300] {
            fold.observe("thread__crdt_log", "CREATE", &row(10), t);
        }
        assert!(fold.take_due(2_400).is_empty(), "inside the minimum gap");
        assert_eq!(fold.take_due(2_500), vec![key()], "row budget reached while active");
    }

    #[test]
    fn a_lost_lease_backs_off_and_a_stuck_field_stops_refolding() {
        let cfg = FoldConfig { idle_ms: 0, min_gap_ms: 0, lease_secs: 30, workers: 4, ..Default::default() };
        let fold = CrdtFold::new(cfg, "ssp-a");
        fold.observe("thread__crdt_log", "CREATE", &row(1), 0);
        fold.record(&key(), &FoldOutcome { lost: true, ..Default::default() }, 0);
        assert!(fold.take_due(29_000).is_empty());
        assert_eq!(fold.take_due(36_000), vec![key()]);
        fold.running.lock().unwrap().clear();

        fold.record(&key(), &FoldOutcome { kept: 1, ..Default::default() }, 40_000);
        assert!(fold.take_due(100_000).is_empty(), "only rows waiting on predecessors");
    }

    #[test]
    fn workers_bound_concurrent_folds() {
        let cfg = FoldConfig { idle_ms: 0, min_gap_ms: 0, workers: 2, ..Default::default() };
        let fold = CrdtFold::new(cfg, "ssp-a");
        for d in ["a", "b", "c"] {
            fold.observe("thread__crdt_log", "CREATE", &json!({ "doc": format!("thread:{d}"), "field": "content" }), 0);
        }
        assert_eq!(fold.take_due(1).len(), 2);
        assert!(fold.take_due(1).is_empty());
    }

    #[test]
    fn one_fold_per_record_at_a_time() {
        let cfg = FoldConfig { idle_ms: 0, min_gap_ms: 0, workers: 4, ..Default::default() };
        let fold = CrdtFold::new(cfg, "ssp-a");
        for field in ["title", "content"] {
            fold.observe("thread__crdt_log", "CREATE", &json!({ "doc": "thread:a", "field": field }), 0);
        }
        fold.observe("thread__crdt_log", "CREATE", &json!({ "doc": "thread:b", "field": "title" }), 0);
        let first = fold.take_due(1);
        assert_eq!(first.len(), 2, "one field of thread:a and thread:b");
        assert!(fold.take_due(1).is_empty(), "thread:a's other field waits while one runs");
        let a = first.into_iter().find(|k| k.doc == "thread:a").unwrap();
        fold.running.lock().unwrap().remove(&a);
        fold.record(&a, &FoldOutcome { folded: 1, ..Default::default() }, 1);
        let next = fold.take_due(1);
        assert_eq!(next.len(), 1);
        assert_eq!(next[0].doc, "thread:a");
        assert_ne!(next[0].field, a.field);
    }
}
