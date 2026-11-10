//! Query registration: local (keys, durable seed, circuit view) and remote
//! (`fn::query::register` plus the membership read-back).
//!
//! PORT: `packages/spooky_core/lib/src/query/register_saga.dart`.

use crate::error::{Error, Result};
use crate::kernel::Ctx;
use crate::modules::query_builder::RelationPlan;
use crate::query::env::SagaEnv;
use crate::types::{QueryHash, QueryTimeToLive};
use crate::value::Object;

/// What `register_local` needs to know about a query.
#[derive(Clone, Debug, PartialEq)]
pub struct RegisterInput {
    pub table_name: String,
    pub surql: String,
    pub params: Object,
    pub ttl: QueryTimeToLive,
    pub has_explicit_order: bool,
    pub relations: Vec<RelationPlan>,
}

/// Register a query locally; returns its hash as soon as the entry exists.
pub async fn register_local(ctx: &Ctx, env: &SagaEnv, input: RegisterInput) -> Result<QueryHash> {
    let _ = (ctx, env, input);
    Err(Error::other("register_local: not ported yet"))
}

/// Start a remote registration for every query that has none.
pub async fn ensure_registered(ctx: &Ctx, env: &SagaEnv, require_auth: bool, attempt: u32) -> Result<()> {
    let _ = (ctx, env, require_auth, attempt);
    Err(Error::other("ensure_registered: not ported yet"))
}

/// One remote registration of `hash`.
pub async fn register_remote(ctx: &Ctx, env: &SagaEnv, hash: &str, retry: bool) -> Result<()> {
    let _ = (ctx, env, hash, retry);
    Err(Error::other("register_remote: not ported yet"))
}
