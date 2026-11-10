//! The `_00_list_ref` poll (fallback when LIVE is quiet).
//!
//! PORT: `packages/spooky_core/lib/src/sync/poll_saga.dart`.

use crate::error::{Error, Result};
use crate::kernel::Ctx;
use crate::query::env::SagaEnv;

pub async fn poll_tick(ctx: &Ctx, env: &SagaEnv) -> Result<()> {
    let _ = (ctx, env);
    Err(Error::other("poll_tick: not ported yet"))
}
