//! The unified row fetcher.
//!
//! PORT: `packages/spooky_core/lib/src/query/fetch_saga.dart`.

use crate::error::{Error, Result};
use crate::kernel::Ctx;
use crate::query::env::SagaEnv;

pub async fn fetch_rows(ctx: &Ctx, env: &SagaEnv) -> Result<()> {
    let _ = (ctx, env);
    Err(Error::other("fetch_rows: not ported yet"))
}
