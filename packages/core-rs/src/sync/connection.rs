//! Connection changes, sync-round outcomes (health) and self-heal.
//!
//! PORT: `packages/spooky_core/lib/src/sync/connection_saga.dart`.

use crate::error::{Error, Result};
use crate::kernel::Ctx;
use crate::query::env::SagaEnv;
use crate::types::ConnectionState;

pub async fn connection_changed(ctx: &Ctx, env: &SagaEnv, state: ConnectionState) -> Result<()> {
    let _ = (ctx, env, state);
    Err(Error::other("connection_changed: not ported yet"))
}

pub async fn sync_outcome(ctx: &Ctx, env: &SagaEnv, ok: bool, error: Option<String>) -> Result<()> {
    let _ = (ctx, env, ok, error);
    Err(Error::other("sync_outcome: not ported yet"))
}

pub async fn self_heal_tick(ctx: &Ctx, env: &SagaEnv) -> Result<()> {
    let _ = (ctx, env);
    Err(Error::other("self_heal_tick: not ported yet"))
}
