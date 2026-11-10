//! Sync: connection health, LIVE, polling.

pub mod connection;
pub mod live;
pub mod policy;
pub mod poll;

#[cfg(test)]
mod tests;
