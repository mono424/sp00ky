//! The crate's one error type.

use std::sync::Arc;

/// Everything that can go wrong in the core.
///
/// `Clone` so one failure can answer every waiter on a shared operation (a
/// dedupe lane, a connect gate).
#[derive(Debug, Clone, thiserror::Error)]
pub enum Error {
    /// The bucket (signed-in account) changed while a saga was running; its
    /// remaining steps were fenced off so they cannot touch the next account's
    /// state. Dart `StateError('Account changed during operation')`.
    #[error("account changed during operation")]
    AccountChanged,
    /// The runtime or client was closed.
    #[error("client is closed")]
    Closed,
    /// No remote endpoint is configured, or the socket is not connected.
    #[error("no remote: {0}")]
    NoRemote(String),
    /// A remote call did not answer in time.
    #[error("remote request timed out after {0}ms")]
    Timeout(u64),
    /// The transport failed (socket closed, refused, TLS, ...).
    #[error("network: {0}")]
    Network(String),
    /// The server answered the RPC with an error.
    #[error("{message}")]
    Rpc { code: i64, message: String },
    /// One statement of a query failed.
    #[error("statement failed: {0}")]
    Statement(String),
    /// Sign-in, sign-up or token verification failed.
    #[error("auth: {0}")]
    Auth(String),
    /// The local store failed.
    #[error("local store: {0}")]
    Local(Arc<str>),
    /// The in-process circuit failed.
    #[error("stream processor: {0}")]
    Ssp(String),
    /// A preload's registration failed, so it can never settle (Dart
    /// `PreloadFailedError`).
    #[error("PreloadFailedError: preload could not settle, the registration of {hash} failed")]
    PreloadFailed { hash: String },
    /// A caller passed something unusable.
    #[error("invalid argument: {0}")]
    InvalidArgument(String),
    /// Anything else, with its message.
    #[error("{0}")]
    Other(String),
}

impl Error {
    pub fn other(message: impl std::fmt::Display) -> Self {
        Error::Other(message.to_string())
    }

    /// A transport-level failure: the request may or may not have reached the
    /// server.
    pub fn is_network(&self) -> bool {
        matches!(self, Error::Network(_) | Error::Timeout(_) | Error::NoRemote(_))
    }

    /// The server refused the credential itself rather than the request: a
    /// bad or revoked token, a token past its `DURATION FOR TOKEN` ("The
    /// token has expired"), or a session past its `DURATION FOR SESSION`.
    /// Retrying with the same token cannot succeed.
    pub fn is_auth_rejection(&self) -> bool {
        let message = self.to_string().to_lowercase();
        [
            "invalid authentication",
            "invalid token",
            "token expired",
            "has expired",
            "authentication failed",
            "problem with authentication",
        ]
        .iter()
        .any(|p| message.contains(p))
    }
}

impl From<rusqlite::Error> for Error {
    fn from(e: rusqlite::Error) -> Self {
        Error::Local(e.to_string().into())
    }
}

impl From<serde_json::Error> for Error {
    fn from(e: serde_json::Error) -> Self {
        Error::InvalidArgument(e.to_string())
    }
}

pub type Result<T, E = Error> = std::result::Result<T, E>;
