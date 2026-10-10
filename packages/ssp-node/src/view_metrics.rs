//! Per-view rolling materialization-latency state used by the ingest path
//! to compute and persist p55/p90/p99 plus running counters back onto the
//! `_00_query` row. Counters survive on the row across SSP restarts; the
//! sample window is rebuilt from scratch on boot, matching the client.

use std::collections::HashMap;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::sync::RwLock;

/// Cap on the rolling materialization-sample window kept per view in memory.
/// Mirrors the client-side window in `packages/core/src/types.ts`.
pub const MATERIALIZATION_SAMPLE_WINDOW: usize = 100;

/// Slow registrations are retained independently of a live view's TTL. A
/// single bounded record survives SSP/scheduler restarts without a new store.
pub const SLOW_REGISTRATION_MS: f64 = 250.0;
pub const SLOW_REGISTRATION_HISTORY: usize = 100;

/// Registration stages. `request_ms` includes preparation and
/// metadata I/O, but ends before publication: publication has its own queue
/// age and transaction metrics. Lock waits and holds accumulate preparation,
/// prewarm/read and install/write locks; build, plan and snapshot are subsets.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct RegistrationStages {
    pub request_ms: f64,
    pub prepare_ms: f64,
    pub parse_ms: f64,
    pub lock_wait_ms: f64,
    pub lock_hold_ms: f64,
    pub index_build_ms: f64,
    pub indexes_built: usize,
    pub rows_indexed: usize,
    pub plan_ms: f64,
    pub snapshot_ms: f64,
    pub metadata_db_ms: f64,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct IngestStages {
    pub request_ms: f64,
    pub lock_wait_ms: f64,
    pub lock_hold_ms: f64,
    pub store_apply_ms: f64,
    pub circuit_step_ms: f64,
    pub enqueue_ms: f64,
}

/// Shape masks literals and parameter names before serialization. In
/// particular this record never accepts raw SQL, bindings, auth or session ids.
pub fn slow_registration_evidence(
    shape: &ssp::allowlist::Shape,
    version: &str,
    indexes: &[(String, String)],
    row_count: i64,
    stages: &RegistrationStages,
) -> Value {
    let shape = serde_json::to_value(shape).unwrap_or(Value::Null);
    let shape = if shape.to_string().len() <= 4096 { shape } else { json!({"truncated": true}) };
    json!({
        "at_ms": crate::now_epoch_ms(),
        "kind": "registration",
        "version": version.chars().take(128).collect::<String>(),
        "shape": shape,
        "indexes": indexes.iter().take(16).map(|(t, i)| (
            t.chars().take(128).collect::<String>(), i.chars().take(128).collect::<String>()
        )).collect::<Vec<_>>(),
        "row_count": row_count,
        "stages": stages,
    })
}

pub async fn persist_slow_registration(db: &dyn crate::ports::Db, evidence: Value) -> Result<(), crate::ports::DbError> {
    // IF NOT EXISTS supports a server upgraded before its next CLI deploy;
    // explicitly private even when the upstream default permissions change.
    let sql = format!(
        "DEFINE TABLE IF NOT EXISTS _00_sync_evidence SCHEMALESS PERMISSIONS NONE; \
         UPSERT _00_sync_evidence:slow_operations SET samples = \
         array::slice(array::append(samples ?? [], $sample), -{});",
        SLOW_REGISTRATION_HISTORY
    );
    crate::db_retry::query_retrying(db, &sql, &[("sample", evidence)]).await.map(|_| ())
}

// History is best effort: bound requests in flight as well as stored samples
// so an upstream stall cannot turn diagnostics into an unbounded task queue.
static SLOW_EVIDENCE_WRITERS: tokio::sync::Semaphore = tokio::sync::Semaphore::const_new(4);

pub fn spawn_slow_evidence(platform: &crate::platform::Platform, evidence: Value) -> bool {
    try_spawn_slow_evidence(
        platform.db.clone(), platform.spawner.as_ref(), platform.telemetry.as_ref(), evidence, &SLOW_EVIDENCE_WRITERS,
    )
}

fn try_spawn_slow_evidence(
    db: std::sync::Arc<dyn crate::ports::Db>,
    spawner: &dyn crate::ports::Spawner,
    telemetry: &dyn crate::ports::Telemetry,
    evidence: Value,
    writers: &'static tokio::sync::Semaphore,
) -> bool {
    let Ok(permit) = writers.try_acquire() else {
        telemetry.counter("slow_evidence_dropped", 1);
        return false;
    };
    spawner.spawn(Box::pin(async move {
        let _permit = permit;
        if let Err(e) = persist_slow_registration(db.as_ref(), evidence).await {
            tracing::warn!(target: "ssp::view_metrics", error = %e, "Failed to persist slow sync evidence");
        }
    }));
    true
}

#[derive(Default)]
pub struct ViewMetricsState {
    samples: Vec<f64>,
    /// Ingest increments not yet flushed; the durable row owns the lifetime total.
    pub update_count: u64,
    pub error_count: u64,
    /// Row count of the view at its last ingest.
    pub row_count: usize,
    /// Latency of the last ingest that touched the view.
    pub last_ingest_ms: f64,
    /// Changed since the last flush to `_00_query`. The ingest path only
    /// notes metrics in memory; a timer flushes the dirty ones (see
    /// `SspNode::flush_view_metrics`). Persisting per ingest put one
    /// un-transacted `UPDATE _00_query` per view per ingest on the very rows
    /// the edge transaction and the TTL sweep write, and the conflicts that
    /// caused were what lost edge writes.
    pub dirty: bool,
    pub registration: Option<RegistrationStages>,
    pub ingest: Option<IngestStages>,
}

impl ViewMetricsState {
    /// Record one ingest that touched this view: a sample, the counters, and
    /// the dirty mark that gets it flushed.
    pub fn note_ingest(&mut self, row_count: usize, sample_ms: f64) {
        self.record_sample(sample_ms);
        self.update_count = self.update_count.saturating_add(1);
        self.row_count = row_count;
        self.last_ingest_ms = sample_ms;
        self.dirty = true;
    }

    pub fn record_sample(&mut self, sample_ms: f64) {
        self.samples.push(sample_ms);
        if self.samples.len() > MATERIALIZATION_SAMPLE_WINDOW {
            // Drop the oldest sample. Vec::remove(0) is fine here, the
            // window is small (<=100) so the shift is negligible.
            self.samples.remove(0);
        }
    }

    pub fn percentiles(&self) -> Option<(f64, f64, f64)> {
        if self.samples.is_empty() {
            return None;
        }
        let mut sorted = self.samples.clone();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        let pick = |q: f64| {
            let idx = ((q * sorted.len() as f64).floor() as usize).min(sorted.len() - 1);
            sorted[idx]
        };
        Some((pick(0.55), pick(0.90), pick(0.99)))
    }
}

pub type ViewMetrics = RwLock<HashMap<String, ViewMetricsState>>;

#[cfg(test)]
mod evidence_tests {
    use super::*;
    use std::sync::{Arc, atomic::{AtomicUsize, Ordering}};
    use crate::ports::{Db, DbError, LocalBoxFuture, Spawner, Telemetry};

    struct WaitingDb {
        active: AtomicUsize,
        peak: AtomicUsize,
        gate: tokio::sync::Semaphore,
    }
    #[async_trait::async_trait]
    impl Db for WaitingDb {
        async fn query(&self, _sql: &str, _binds: &[(&str, Value)]) -> Result<Vec<Value>, DbError> {
            let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
            self.peak.fetch_max(active, Ordering::SeqCst);
            self.gate.acquire().await.unwrap().forget();
            self.active.fetch_sub(1, Ordering::SeqCst);
            Err(DbError::Transport("upstream unavailable".into()))
        }
        async fn version(&self) -> Result<String, DbError> { Ok("test".into()) }
    }
    struct Tasks;
    impl Spawner for Tasks {
        fn spawn(&self, future: LocalBoxFuture) { tokio::spawn(future); }
    }
    #[derive(Default)]
    struct Metrics(AtomicUsize);
    impl Telemetry for Metrics {
        fn counter(&self, name: &'static str, value: u64) {
            if name == "slow_evidence_dropped" { self.0.fetch_add(value as usize, Ordering::SeqCst); }
        }
        fn histogram_ms(&self, _: &'static str, _: f64) {}
        fn gauge_add(&self, _: &'static str, _: i64) {}
    }

    #[tokio::test]
    async fn slow_evidence_bounds_inflight_and_releases_failed_writers() {
        // Isolate this limiter so concurrent unrelated tests do not consume
        // the process-wide diagnostic budget used by real request paths.
        let limit = Box::leak(Box::new(tokio::sync::Semaphore::new(4)));
        let db = Arc::new(WaitingDb { active: AtomicUsize::new(0), peak: AtomicUsize::new(0), gate: tokio::sync::Semaphore::new(0) });
        let metrics = Metrics::default();
        for _ in 0..4 { assert!(try_spawn_slow_evidence(db.clone(), &Tasks, &metrics, Value::Null, limit)); }
        assert!(!try_spawn_slow_evidence(db.clone(), &Tasks, &metrics, Value::Null, limit));
        assert_eq!(metrics.0.load(Ordering::SeqCst), 1);
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while db.active.load(Ordering::SeqCst) < 4 { tokio::task::yield_now().await; }
        }).await.unwrap();
        assert_eq!(db.peak.load(Ordering::SeqCst), 4);
        db.gate.add_permits(4);
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while limit.available_permits() < 4 { tokio::task::yield_now().await; }
        }).await.unwrap();
        assert!(try_spawn_slow_evidence(db.clone(), &Tasks, &metrics, Value::Null, limit));
        db.gate.add_permits(1);
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while limit.available_permits() < 4 { tokio::task::yield_now().await; }
        }).await.unwrap();
    }
}
