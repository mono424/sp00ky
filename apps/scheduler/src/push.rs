//! Web Push host: the cluster's single push engine (`push-core`).
//!
//! The engine is built once the shared root handle exists (`Scheduler::start`)
//! and published into a slot, because `/ingest` and the changefeed sink are
//! created before the upstream connection is. Until it lands, rule rows are
//! not pushed and `_00_push_message` rows are left `pending`: the engine's
//! sweep sends a pending message it never saw ingested, so nothing is lost.

use std::sync::{Arc, OnceLock};
use std::time::Duration;

use push_core::{ObservedChange, Op, Origin, PushEngine};
use tokio::sync::Semaphore;
use tracing::{debug, info, warn};

/// Late-bound engine handle shared by every `IngestState` clone.
pub type PushSlot = Arc<OnceLock<Arc<PushEngine>>>;

pub fn new_slot() -> PushSlot {
    Arc::new(OnceLock::new())
}

/// Concurrent `observe` tasks. Saturation drops the push (counted in the log):
/// the ingest path must never wait on a push service.
pub const OBSERVE_PERMITS: usize = 64;

/// How often the engine's periodic work runs (config reload, trailing
/// throttles, retries, scheduled messages, prune). The engine keeps its own
/// clocks per job; this is only the resolution.
const TICK_INTERVAL: Duration = Duration::from_secs(1);

/// Build the engine over the shared root handle and start its ticker.
/// Returns `None` only when the HTTP client cannot be built; a missing key
/// still builds an engine, whose status then says why push is off.
pub fn start(db: Arc<maintenance::db::ReconnectingDb>) -> Option<Arc<PushEngine>> {
    let env = |k: &str| std::env::var(k).ok();
    let mut opts = push_core::EngineOptions::from_env(env);
    opts.sleep = Some(Arc::new(|ms| Box::pin(tokio::time::sleep(Duration::from_millis(ms)))));
    let http = match push_core::ReqwestPushHttp::new(opts.allow_private_endpoints) {
        Ok(h) => h,
        Err(e) => {
            warn!(target: "push", error = %e, "web push disabled: could not build the HTTP client");
            return None;
        }
    };
    let keys = PushEngine::keys_from_env(env);
    if let Some(k) = &keys {
        info!(target: "push", kid = %k.kid(), "web push engine starting");
    }
    let engine = Arc::new(PushEngine::new(
        Arc::new(crate::schedule_engine::SharedDb(db)),
        Arc::new(http),
        keys,
        opts,
    ));
    let ticker = Arc::clone(&engine);
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(TICK_INTERVAL);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            interval.tick().await;
            ticker.tick().await;
        }
    });
    Some(engine)
}

/// Hand one ingested row to the engine if it wants it. Never blocks.
pub fn observe(
    slot: &PushSlot,
    permits: &Arc<Semaphore>,
    table: &str,
    op: &str,
    id: &str,
    record: &serde_json::Value,
    origin: Origin,
) {
    let Some(engine) = slot.get() else { return };
    let Some(op) = Op::parse(op) else { return };
    if !engine.wants(table, op) {
        return;
    }
    let Ok(permit) = Arc::clone(permits).try_acquire_owned() else {
        warn!(target: "push", table, id, "push observers saturated; row not pushed");
        return;
    };
    let engine = Arc::clone(engine);
    let change = ObservedChange {
        table: table.to_string(),
        op,
        id: id.to_string(),
        record: record.clone(),
        origin,
        // Taken here, in ingest order, not inside the spawned task.
        seq: engine.next_seq(),
    };
    tokio::spawn(async move {
        engine.observe(change).await;
        drop(permit);
    });
    debug!(target: "push", "row handed to the push engine");
}

/// `/health`, `/metrics` and the admin overview block.
pub fn status_json(slot: &PushSlot) -> serde_json::Value {
    match slot.get() {
        Some(engine) => serde_json::to_value(engine.status()).unwrap_or(serde_json::Value::Null),
        None => serde_json::json!({ "enabled": false, "reason": "starting" }),
    }
}
