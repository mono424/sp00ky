//! Lanes bound concurrency without a queue object per lane.
//!
//! - `Serial`: runs strictly one at a time per key, in arrival order.
//! - `Dedupe`: a request while one run is active joins that run instead of
//!   starting another.

/// Which lane a saga runs on.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum Lane {
    Serial(String),
    Dedupe(String),
}

impl Lane {
    pub fn serial(key: impl Into<String>) -> Self {
        Lane::Serial(key.into())
    }

    pub fn dedupe(key: impl Into<String>) -> Self {
        Lane::Dedupe(key.into())
    }

    pub fn key(&self) -> &str {
        match self {
            Lane::Serial(k) | Lane::Dedupe(k) => k,
        }
    }

    pub fn is_dedupe(&self) -> bool {
        matches!(self, Lane::Dedupe(_))
    }
}
