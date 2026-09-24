use super::MaybeSendSync;
use serde::{Deserialize, Serialize};

/// Every scheduled wakeup the node ever needs, as durable data.
///
/// Recurring behavior is NOT a scheduler feature: a periodic task re-arms its
/// own kind inside `on_timer`. That collapses today's five tokio interval
/// loops (job recovery sweep, TTL cleanup, edge flush, backend health, DB
/// re-signin) and two spawn+sleep sites (delayed job enqueue, job retry
/// backoff) into one uniform mechanism that a Durable Object can persist and
/// mux onto its single alarm (see [`crate::TimerMux`]).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum TimerKind {
    /// Standalone job recovery sweep (re-arms every `JOB_RECOVERY_INTERVAL_SECS`).
    JobRecoverySweep,
    /// Standalone schedule sweep: plan/fire declarative schedules, advance
    /// workflow DAGs, heal lost job-completion events (re-arms every
    /// `SCHEDULE_SWEEP_INTERVAL_SECS`). Cluster mode leaves this to the
    /// scheduler service, which is the single ticker there.
    ScheduleSweep,
    /// View TTL cleanup (re-arms every `ttl_cleanup_interval_secs`).
    TtlCleanup,
    /// Flush of the in-memory per-view metrics to `_00_query` (re-arms every
    /// `view_metrics_flush_ms`).
    ViewMetricsFlush,
    /// Follow upstream's table set (`SspNode::refresh_schema`), re-armed every
    /// `schema_poll_secs` while that is non-zero.
    SchemaPoll,
    /// Edge-update batch flush (re-arms every `query_update_throttle_ms`
    /// while a batch is pending).
    EdgeFlush,
    /// Backend health poll (re-arms every `health_check_interval_secs`).
    BackendHealth,
    /// HTTP-engine token refresh (re-arms every `RESIGNIN_INTERVAL_SECS`).
    DbResignin,
    /// Admit backlogged jobs for one outbox table (re-arms every
    /// `JOB_DRAIN_INTERVAL_SECS` while that table still has a known backlog).
    ///
    /// Armed in BOTH standalone and cluster mode, unlike the recovery sweep.
    /// It is the only wakeup that covers the case where the cluster-wide count
    /// is what blocks admission: this node then has free local slots, runs
    /// nothing, and so never completes a job to kick its own drain.
    JobDrain { table: String },
    /// A job created with a delay window, due later.
    DelayedJob { id: String },
    /// Retry backoff for a failed job dispatch.
    JobRetry { id: String },
    /// Bootstrap retry with backoff (tables may not exist yet).
    BootstrapRetry { attempt: u32 },
    /// Periodic circuit checkpoint to the `CircuitStore` (re-arms every
    /// `NodeConfig.checkpoint_interval_secs`). Only armed when a non-noop store
    /// is configured — the VM leaves it disabled (its store is noop).
    CircuitCheckpoint,
}

/// Durable one-shot wakeups + bounded in-request sleep.
///
/// Contract: after `schedule(kind, at)`, the shell invokes the node's
/// `on_timer(kind)` at-or-after `at_epoch_ms`. Scheduling a kind that is
/// already pending REPLACES its deadline. Delivery is at-least-once
/// best-effort — anything that must survive a lost wakeup (crash between
/// schedule and fire) is additionally healed by the recovery sweep, whose
/// deadline checks live in SurrealQL, not host time.
///
/// VM adapter: `tokio::spawn(sleep_until)` → mpsc → shell dispatcher loop.
/// CF adapter: [`crate::TimerMux`] persisted in DO storage, muxed onto the
/// Durable Object's single alarm.
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
pub trait Scheduler: MaybeSendSync {
    async fn schedule(&self, kind: TimerKind, at_epoch_ms: u64);

    async fn cancel(&self, kind: &TimerKind);

    /// Bounded in-request sleep (commit-wait backoff, bootstrap retry pause).
    /// NOT for periodic work — that's `schedule` + re-arm.
    async fn sleep(&self, dur: std::time::Duration);
}
