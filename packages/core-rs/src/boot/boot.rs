//! Boot: the local-first half (awaited) and the network half (dispatched).
//!
//! PORT: `packages/spooky_core/lib/src/boot/boot_saga.dart`.

use crate::error::{Error, Result};
use crate::kernel::Ctx;

pub async fn start_remote(ctx: &Ctx) -> Result<()> {
    let _ = ctx;
    Err(Error::other("start_remote: not ported yet"))
}

pub async fn prime_circuit(ctx: &Ctx) -> Result<()> {
    let _ = ctx;
    Err(Error::other("prime_circuit: not ported yet"))
}

pub async fn versions_primed(ctx: &Ctx, entries: Vec<(String, i64)>) -> Result<()> {
    let _ = (ctx, entries);
    Err(Error::other("versions_primed: not ported yet"))
}

pub async fn app_detached(ctx: &Ctx) -> Result<()> {
    let _ = ctx;
    Err(Error::other("app_detached: not ported yet"))
}
