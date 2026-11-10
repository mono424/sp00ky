//! The client: the in-process [`Engine`] and the thread-safe
//! [`Sp00kyClient`] handle.

pub mod engine;
pub mod handle;

pub use engine::{Engine, QueryOptions};
pub use handle::{LiveQuery, Sp00kyClient};
