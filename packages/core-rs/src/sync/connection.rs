//! Connection changes, sync-round outcomes (health) and self-heal.
//!
//! PORT: `packages/spooky_core/lib/src/sync/connection_saga.dart`.

use crate::error::Result;
use crate::kernel::constants::RECONNECT_REFETCH_COOLDOWN_MS;
use crate::kernel::events::RuntimeEvent;
use crate::kernel::Ctx;
use crate::query::env::SagaEnv;
use crate::state::lifecycle::{LifecycleEvent, RemotePhase};
use crate::state::reducers::{self as r, SyncPatch};
use crate::state::selectors::{desired_registrations, pending_mutation_count};
use crate::sync::live::live_invalidate;
use crate::sync::policy::{next_health, self_heal_delay_ms, HealthInput};
use crate::types::{ConnectionState, QueryHash, SyncHealthStatus};

/// Transport state changes. A drop invalidates the live query and arms a
/// resubscribe. The next `connected` restarts LIVE and drains the outbox;
/// outside the burst cooldown it also drops every remote registration so
/// `ensure_registered` rebuilds them behind the `$auth.id` gate.
pub async fn connection_changed(ctx: &Ctx, env: &SagaEnv, state: ConnectionState) -> Result<()> {
    let _ = env;
    let (needs_resubscribe, last_refetch, registered) = ctx.read(|s| {
        let registered: Vec<QueryHash> = s
            .queries
            .values()
            .filter(|e| e.lifecycle.remote != RemotePhase::Unregistered)
            .map(|e| e.def.hash.clone())
            .collect();
        (
            s.sync.needs_resubscribe,
            s.sync.last_reconnect_refetch_at,
            registered,
        )
    })?;
    ctx.update(|s| r::set_connection(s, state))?;
    if matches!(
        state,
        ConnectionState::Disconnected | ConnectionState::Reconnecting
    ) {
        ctx.update(|s| {
            r::patch_sync(
                s,
                SyncPatch {
                    needs_resubscribe: Some(true),
                    ..Default::default()
                },
            )
        })?;
        return live_invalidate(ctx).await;
    }
    if state != ConnectionState::Connected || !needs_resubscribe {
        return Ok(());
    }
    let now = ctx.now()?;
    if last_refetch.is_some_and(|last| now - last < RECONNECT_REFETCH_COOLDOWN_MS as i64) {
        // The cooldown only spares the server a re-registration storm. The
        // views the last refetch registered are still on the server, but LIVE
        // died with the socket and nothing else restarts it: skipping it here
        // left the client on the membership poll until a reconnect outside the
        // window.
        ctx.update(|s| {
            r::patch_sync(
                s,
                SyncPatch {
                    needs_resubscribe: Some(false),
                    ..Default::default()
                },
            )
        })?;
    } else {
        ctx.update(|s| {
            r::patch_sync(
                s,
                SyncPatch {
                    needs_resubscribe: Some(false),
                    last_reconnect_refetch_at: Some(Some(now)),
                    ..Default::default()
                },
            );
            for hash in &registered {
                r::apply_lifecycle(s, hash, LifecycleEvent::RemoteDropped);
            }
        })?;
        ctx.dispatch(RuntimeEvent::EnsureRegistered {
            require_auth: true,
            attempt: 0,
        })?;
    }
    ctx.dispatch(RuntimeEvent::LiveStart)?;
    ctx.dispatch(RuntimeEvent::Drain)
}

/// Fold one sync round's outcome into health; start or stop self-heal.
pub async fn sync_outcome(ctx: &Ctx, env: &SagaEnv, ok: bool, error: Option<String>) -> Result<()> {
    let input = ctx.read(|s| HealthInput {
        health: s.sync.health.clone(),
        consecutive_failures: s.sync.consecutive_failures,
        has_synced_once: s.sync.has_synced_once,
    })?;
    let next = next_health(input, ok, error.as_deref(), env.degrade_after);
    ctx.update(|s| {
        r::set_health(s, next.health.clone());
        r::patch_sync(
            s,
            SyncPatch {
                consecutive_failures: Some(next.consecutive_failures),
                has_synced_once: Some(next.has_synced_once),
                ..Default::default()
            },
        );
    })?;
    if next.degraded_now {
        ctx.update(|s| {
            r::patch_sync(
                s,
                SyncPatch {
                    self_heal_attempts: Some(0),
                    ..Default::default()
                },
            )
        })?;
        ctx.timer_set("heal", self_heal_delay_ms(0), RuntimeEvent::SelfHealTick)?;
    }
    if next.recovered_now {
        ctx.timer_clear("heal")?;
    }
    Ok(())
}

/// While degraded: re-drive whatever can prove the server is back, on a
/// growing backoff. Retry the outbox first, then the registrations (failed
/// ones included, behind the identity gate), else a bare probe.
pub async fn self_heal_tick(ctx: &Ctx, env: &SagaEnv) -> Result<()> {
    let (degraded, attempts, pending, desired, failed) = ctx.read(|s| {
        let failed: Vec<QueryHash> = s
            .queries
            .values()
            .filter(|e| e.lifecycle.remote == RemotePhase::Failed)
            .map(|e| e.def.hash.clone())
            .collect();
        (
            s.sync.health.status == SyncHealthStatus::Degraded,
            s.sync.self_heal_attempts,
            pending_mutation_count(s),
            !desired_registrations(s).is_empty(),
            failed,
        )
    })?;
    if !degraded {
        return Ok(());
    }
    let attempt = attempts + 1;
    ctx.update(|s| {
        r::patch_sync(
            s,
            SyncPatch {
                self_heal_attempts: Some(attempt),
                ..Default::default()
            },
        )
    })?;
    if pending > 0 {
        ctx.dispatch(RuntimeEvent::Drain)?;
    } else if desired || !failed.is_empty() {
        if !failed.is_empty() {
            ctx.update(|s| {
                for hash in &failed {
                    r::apply_lifecycle(s, hash, LifecycleEvent::RemoteDropped);
                }
            })?;
        }
        ctx.dispatch(RuntimeEvent::EnsureRegistered {
            require_auth: true,
            attempt: 0,
        })?;
    } else {
        let outcome = match ctx
            .remote_query("RETURN true", None, Some(env.remote_timeout_ms))
            .await
        {
            Ok(_) => RuntimeEvent::SyncOutcome {
                ok: true,
                error: None,
            },
            Err(error) => RuntimeEvent::SyncOutcome {
                ok: false,
                error: Some(error.to_string()),
            },
        };
        ctx.dispatch(outcome)?;
    }
    ctx.timer_set(
        "heal",
        self_heal_delay_ms(attempt),
        RuntimeEvent::SelfHealTick,
    )
}
