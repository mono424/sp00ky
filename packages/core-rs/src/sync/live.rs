//! The session's LIVE subscription on its `_00_list_ref` table.
//!
//! PORT: `packages/spooky_core/lib/src/sync/live_saga.dart`.

use crate::error::{Error, Result};
use crate::kernel::Ctx;
use crate::query::env::SagaEnv;
use crate::types::{InlineRow, QueryHash};

pub async fn live_start(ctx: &Ctx, env: &SagaEnv) -> Result<()> {
    let _ = (ctx, env);
    Err(Error::other("live_start: not ported yet"))
}

pub async fn live_change(ctx: &Ctx, env: &SagaEnv, hashes: Vec<QueryHash>, rows: Option<Vec<InlineRow>>) -> Result<()> {
    let _ = (ctx, env, hashes, rows);
    Err(Error::other("live_change: not ported yet"))
}
