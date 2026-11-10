//! Materialization: render a query's rows from the local store.
//!
//! PORT: `packages/spooky_core/lib/src/query/materialize.dart` and
//! `materialize_saga.dart`.

use crate::error::{Error, Result};
use crate::kernel::ports::StreamUpdate;
use crate::kernel::Ctx;

pub async fn materialize(ctx: &Ctx, hash: &str) -> Result<()> {
    let _ = (ctx, hash);
    Err(Error::other("materialize: not ported yet"))
}

pub async fn stream_update(ctx: &Ctx, update: StreamUpdate) -> Result<()> {
    let _ = (ctx, update);
    Err(Error::other("stream_update: not ported yet"))
}
