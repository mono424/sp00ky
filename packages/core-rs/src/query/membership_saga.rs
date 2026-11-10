//! Membership sagas: apply a server id-set, recover lost views, batched
//! `_00_list_ref` reads.
//!
//! PORT: `packages/spooky_core/lib/src/query/membership_saga.dart`.

use crate::error::{Error, Result};
use crate::kernel::Ctx;
use crate::query::env::SagaEnv;
use crate::types::QueryHash;

pub async fn recover_lost_view(ctx: &Ctx, hash: &str) -> Result<()> {
    let _ = (ctx, hash);
    Err(Error::other("recover_lost_view: not ported yet"))
}

pub async fn read_dirty_membership(ctx: &Ctx, env: &SagaEnv) -> Result<()> {
    let _ = (ctx, env);
    Err(Error::other("read_dirty_membership: not ported yet"))
}

pub async fn read_membership(ctx: &Ctx, env: &SagaEnv, hashes: &[QueryHash], force: bool) -> Result<()> {
    let _ = (ctx, env, hashes, force);
    Err(Error::other("read_membership: not ported yet"))
}

pub async fn mark_membership_dirty(ctx: &Ctx, hashes: &[QueryHash]) -> Result<()> {
    let _ = (ctx, hashes);
    Err(Error::other("mark_membership_dirty: not ported yet"))
}
