//! The signed-in principal changed.
//!
//! PORT: `packages/spooky_core/lib/src/boot/auth_flip_saga.dart`.

use crate::error::{Error, Result};
use crate::kernel::Ctx;
use crate::query::env::SagaEnv;

pub async fn auth_flip(ctx: &Ctx, env: &SagaEnv, user_id: Option<String>) -> Result<()> {
    let _ = (ctx, env, user_id);
    Err(Error::other("auth_flip: not ported yet"))
}
