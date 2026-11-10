//! Lifecycle tick (evict, heartbeat), ack pruning and orphan collection.
//!
//! PORT: `packages/spooky_core/lib/src/query/lifecycle_saga.dart`.

use crate::error::{Error, Result};
use crate::kernel::Ctx;
use crate::query::env::SagaEnv;

pub async fn lifecycle_tick(ctx: &Ctx, env: &SagaEnv) -> Result<()> {
    let _ = (ctx, env);
    Err(Error::other("lifecycle_tick: not ported yet"))
}

pub async fn evict_query(ctx: &Ctx, hash: &str) -> Result<()> {
    let _ = (ctx, hash);
    Err(Error::other("evict_query: not ported yet"))
}

pub async fn ack_prune(ctx: &Ctx) -> Result<()> {
    let _ = ctx;
    Err(Error::other("ack_prune: not ported yet"))
}

pub async fn gc_tick(ctx: &Ctx) -> Result<()> {
    let _ = ctx;
    Err(Error::other("gc_tick: not ported yet"))
}
