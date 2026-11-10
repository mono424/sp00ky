//! Optimistic writes, debounced flushes and channel peer writes.
//!
//! PORT: `packages/spooky_core/lib/src/mutation/write_saga.dart`.

use crate::error::{Error, Result};
use crate::kernel::Ctx;
use crate::query::env::SagaEnv;

pub async fn flush_write(ctx: &Ctx, env: &SagaEnv, key: &str) -> Result<()> {
    let _ = (ctx, env, key);
    Err(Error::other("flush_write: not ported yet"))
}

pub async fn peer_write_expired(ctx: &Ctx, id: &str) -> Result<()> {
    let _ = (ctx, id);
    Err(Error::other("peer_write_expired: not ported yet"))
}
