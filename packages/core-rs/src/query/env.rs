//! Static configuration the sagas read. Plain data, built once by the client
//! from [`crate::Sp00kyConfig`]; sagas never see the config itself.

use std::collections::BTreeMap;
use std::sync::Arc;

use crate::kernel::constants as k;
use crate::modules::ref_tables::{list_ref_table_for, RefMode, ANON_USER_ID};
use crate::state::ClientState;
use crate::types::{ColumnSchema, Schema};

#[derive(Clone, Debug)]
pub struct SagaEnv {
    pub schema: Arc<Schema>,
    pub ref_mode: RefMode,
    pub anon_live: bool,
    pub remote_timeout_ms: u64,
    pub push_timeout_ms: u64,
    pub outbox_batch_size: usize,
    /// Consecutive failures before health degrades; 0 never degrades.
    pub degrade_after: u32,
    pub materialize_debounce_ms: u64,
    pub poll_base_ms: u64,
    pub default_ttl_ms: u64,
    /// False for a purely local client (no endpoint). Gates the network half of
    /// boot and the drain: without it the outbox would drain against nothing,
    /// every push would read as a rejection, and the write would roll back.
    pub has_remote: bool,
}

impl SagaEnv {
    pub fn new(schema: Arc<Schema>) -> Self {
        SagaEnv {
            schema,
            ref_mode: RefMode::Dedicated,
            anon_live: false,
            remote_timeout_ms: 60_000,
            push_timeout_ms: k::PUSH_TIMEOUT_MS,
            outbox_batch_size: k::OUTBOX_BATCH_SIZE,
            degrade_after: k::DEGRADE_AFTER_FAILURES,
            materialize_debounce_ms: k::MATERIALIZE_DEBOUNCE_MS,
            poll_base_ms: k::LIST_REF_POLL_BASE_MS,
            default_ttl_ms: 600_000,
            has_remote: true,
        }
    }

    /// The declared columns of `table` (see [`Schema::columns`]).
    pub fn columns(&self, table: &str) -> Option<&BTreeMap<String, ColumnSchema>> {
        self.schema.columns(table)
    }
}

/// The `_00_list_ref` table this session reads: per-user, anonymous, or global.
pub fn list_ref_table(env: &SagaEnv, s: &ClientState) -> String {
    let user = s
        .user_id
        .as_deref()
        .or(if env.anon_live { Some(ANON_USER_ID) } else { None });
    list_ref_table_for(env.ref_mode, user)
}

/// Whether this session can be sent a view's membership at all. A signed-out
/// session reads edges only from `_00_list_ref_anon`, and only with anonymous
/// live queries on.
pub fn receives_membership(env: &SagaEnv, s: &ClientState) -> bool {
    s.user_id.is_some() || env.anon_live
}
