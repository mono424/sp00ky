//! The client: the in-process [`Engine`] and the thread-safe
//! [`Sp00kyClient`] handle.

pub mod engine;
pub mod handle;
mod push;
mod wiring;

pub use engine::{Engine, QueryOptions};
pub use handle::{AppRelease, Bucket, FeatureFlag, LiveQuery, Sp00kyClient};
pub use push::Push;
