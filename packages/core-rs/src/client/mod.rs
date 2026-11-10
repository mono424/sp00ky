//! The client: the in-process [`Engine`] and the thread-safe
//! [`Sp00kyClient`] handle.

mod channels;
pub mod engine;
pub mod handle;
mod push;
mod wiring;

pub use channels::{Channel, Sp00kyChannel};
pub use engine::{Engine, QueryOptions};
pub use handle::{AppRelease, AuthState, Bucket, FeatureFlag, LiveQuery, Sp00kyClient};
pub use push::Push;
