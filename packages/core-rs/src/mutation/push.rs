//! The outbox push (drain) and rollback.
//!
//! PORT: `packages/spooky_core/lib/src/mutation/push_saga.dart`.

use crate::error::{Error, Result};
use crate::kernel::Ctx;
use crate::query::env::SagaEnv;

pub async fn drain(ctx: &Ctx, env: &SagaEnv) -> Result<()> {
    let _ = (ctx, env);
    Err(Error::other("drain: not ported yet"))
}
