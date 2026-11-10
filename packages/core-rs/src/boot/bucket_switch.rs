//! Switch the local store to another principal's bucket.
//!
//! PORT: `packages/spooky_core/lib/src/boot/bucket_switch_saga.dart`.

use crate::error::{Error, Result};
use crate::kernel::Ctx;
use crate::query::env::SagaEnv;

pub async fn bucket_switch(ctx: &Ctx, env: &SagaEnv, target: &str) -> Result<()> {
    let _ = (ctx, env, target);
    Err(Error::other("bucket_switch: not ported yet"))
}
