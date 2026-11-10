//! The `_00_list_ref` poll (fallback when LIVE is quiet).
//!
//! PORT: `packages/spooky_core/lib/src/sync/poll_saga.dart`.

use crate::error::Result;
use crate::kernel::events::RuntimeEvent;
use crate::kernel::Ctx;
use crate::query::env::SagaEnv;
use crate::query::membership_saga::read_membership;
use crate::state::reducers::{self as r, SyncPatch};
use crate::state::selectors::{active_hashes, has_acked_writes};
use crate::sync::policy::{list_ref_poll_delay_ms, LIST_REF_POLL_MAX_INTERVAL_MS};

/// The safety net under LIVE. Reads every active query's membership on a
/// backoff that snaps to the base cadence whenever something changed (or an
/// acked write still waits for membership) and coasts up to the cap while
/// quiet. With no queries it probes connectivity, which is also the only
/// health signal an idle client produces.
pub async fn poll_tick(ctx: &Ctx, env: &SagaEnv) -> Result<()> {
    let (hashes, streak, acked) = ctx.read(|s| {
        (
            active_hashes(s),
            s.sync.poll_idle_streak,
            has_acked_writes(s),
        )
    })?;
    let mut changed = false;
    if hashes.is_empty() {
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
    } else {
        changed = read_membership(ctx, env, &hashes, false).await?.changed;
    }
    let idle_streak = if changed || acked { 0 } else { streak + 1 };
    ctx.update(|s| {
        r::patch_sync(
            s,
            SyncPatch {
                poll_idle_streak: Some(idle_streak),
                ..Default::default()
            },
        )
    })?;
    ctx.timer_set(
        "poll",
        list_ref_poll_delay_ms(idle_streak, env.poll_base_ms, LIST_REF_POLL_MAX_INTERVAL_MS),
        RuntimeEvent::PollTick,
    )
}
