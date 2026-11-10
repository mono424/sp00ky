//! Native Rust core for sp00ky local-first sync.
//!
//! A port of the Dart (`packages/spooky_core`) and TypeScript
//! (`packages/core`) cores onto the same saga architecture:
//!
//! - [`state`]: one [`state::ClientState`] value, pure reducers and selectors.
//! - [`kernel`]: the effect seam ([`kernel::Ctx`]), the adapter ports, events,
//!   lanes and timing constants.
//! - [`query`], [`mutation`], [`sync`], [`boot`]: the sagas, plain
//!   `async fn(&Ctx, ...)`, holding no adapter and no state.
//! - [`runtime`]: the single-threaded engine that owns the state, runs sagas
//!   on lanes, fires timers and fans out to subscribers.
//! - [`services`], [`surreal`]: the real adapters (sqlite store, in-process
//!   circuit, SurrealDB WebSocket RPC).
//! - [`modules`]: the query builder, auth, buckets, feature flags, releases.
//!
//! The engine is single-threaded, like the Dart isolate it mirrors; the
//! thread-safe client handle runs it on a dedicated thread.

pub mod boot;
pub mod client;
#[cfg(feature = "codegen")]
pub mod codegen;
pub mod error;
pub mod kernel;
pub mod modules;
pub mod mutation;
pub mod query;
pub mod runtime;
pub mod services;
pub mod state;
pub mod surreal;
pub mod sync;
pub mod testing;
pub mod typed;
pub mod types;
pub mod utils;
pub mod value;

pub use error::{Error, Result};
pub use types::*;
pub use value::{from_row, from_value, to_object, to_value, IntoObject, Object, RecordId, RecordKey, Row, SurrealDuration, Value};
pub use client::{
    AppRelease, AuthState, Bucket, Channel, Engine, FeatureFlag, LiveQuery, Push, QueryOptions, Sp00kyChannel,
    Sp00kyClient,
};
