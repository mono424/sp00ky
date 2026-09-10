use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use ssp_node::{Db, DbConnection, DbError};

use crate::SharedDb;

/// How long a single `Db::query` may take before we give up on it.
///
/// The surrealdb HTTP engine has no request timeout of its own, and all SSP
/// database work shares ONE handle whose per-session locks are held across the
/// network await — so one stalled request parks every later one behind it.
/// Unbounded, that turns a slow database into an SSP that hangs `/ingest`,
/// `/view/register` and `/job/recover` indefinitely while `/health` (which
/// reads only memory) still answers `ready`. The scheduler then times out its
/// own POSTs at 30s, marks the SSP `Lagging`, and every client query fails
/// with "No ready SSP available" until the database recovers.
///
/// 15s sits above any healthy statement (a root query answers in under a
/// millisecond, and large edge publishes are chunked) and below the
/// scheduler's 30s client timeout, so we fail and reconnect while the
/// scheduler is still waiting rather than after it has given up.
const DEFAULT_QUERY_TIMEOUT_SECS: u64 = 15;

fn query_timeout() -> Option<Duration> {
    let secs = std::env::var("SPKY_SSP_DB_TIMEOUT_SECS")
        .ok()
        .and_then(|s| s.trim().parse().ok())
        .unwrap_or(DEFAULT_QUERY_TIMEOUT_SECS);
    // 0 is an explicit opt-out, for a deployment that would rather wait than
    // fail a legitimately slow statement.
    (secs > 0).then(|| Duration::from_secs(secs))
}

/// `ssp_node::Db` over the surrealdb SDK's HTTP engine.
pub struct SurrealSdkDb {
    db: SharedDb,
    timeout: Option<Duration>,
    /// Queries that have timed out back to back. Cleared by any success.
    /// Read by `/health` so a wedged connection is visible from outside.
    consecutive_timeouts: AtomicU32,
}

impl SurrealSdkDb {
    pub fn new(db: SharedDb) -> Self {
        Self {
            db,
            timeout: query_timeout(),
            consecutive_timeouts: AtomicU32::new(0),
        }
    }

    /// Apply the call timeout and keep the connection-health counters honest.
    ///
    /// A timeout is reported as `Transport`, not `Query`: the statement may
    /// well still be running server-side, so it is a connection-level failure
    /// the caller may retry — never an application error. Classifying it the
    /// other way is what loses queued writes (see the outbox rollback bug).
    async fn bounded<T, F>(&self, what: &str, fut: F) -> Result<T, DbError>
    where
        // `IntoFuture`, not `Future`: the SDK's builders (`Query`, `Version`)
        // are only awaitable, so they have to be driven into a real future
        // before `timeout` can wrap them.
        F: std::future::IntoFuture<Output = Result<T, surrealdb::Error>>,
    {
        let fut = fut.into_future();
        let result = match self.timeout {
            Some(limit) => match tokio::time::timeout(limit, fut).await {
                Ok(result) => result,
                Err(_) => {
                    let n = self.consecutive_timeouts.fetch_add(1, Ordering::Relaxed) + 1;
                    // The engine cannot tell us a hung session is dead — it
                    // simply never returns — so the timeout IS the signal, and
                    // `force_reconnect` is what actually replaces the handle.
                    self.db.force_reconnect();
                    tracing::warn!(
                        timeout_secs = limit.as_secs(),
                        consecutive = n,
                        statement = what,
                        "SurrealDB call exceeded the SSP timeout; reconnecting"
                    );
                    return Err(DbError::Transport(format!(
                        "SurrealDB call timed out after {}s",
                        limit.as_secs()
                    )));
                }
            },
            None => fut.await,
        };

        match result {
            Ok(value) => {
                self.consecutive_timeouts.store(0, Ordering::Relaxed);
                Ok(value)
            }
            // Every SSP database call funnels through here, so this is the one
            // place that has to tell the connection its session died — that
            // report is what makes a SurrealDB restart heal on the next failed
            // query instead of at the next refresh tick.
            Err(e) => {
                let msg = e.to_string();
                self.db.note_error(&msg);
                Err(DbError::Transport(msg))
            }
        }
    }

    /// Run one statement and flatten the FIRST result to plain JSON.
    ///
    /// `into_json_value()` flattens RecordId/Datetime to plain strings and
    /// unwraps SurrealDB's tagged Value enum into ordinary JSON —
    /// `serde_json::to_value(&val)` would emit the tagged shape
    /// `{"Object": {...}}`, which breaks `.get("tables")`-style access.
    /// Shared by the `Db` impl and `BootstrapSource::Direct`.
    ///
    /// Deliberately NOT time-boxed like `Db::query`: the only caller is
    /// standalone bootstrap paging, where a single page is a WHERE + ORDER BY
    /// + LIMIT pass over a whole table and taking tens of seconds is normal,
    /// not a stall. Bootstrap already has its own budget upstream.
    pub async fn flatten_first(
        db: &SharedDb,
        surql: &str,
    ) -> anyhow::Result<serde_json::Value> {
        use anyhow::Context;
        let handle = db.handle();
        let mut response = handle
            .query(surql)
            .await
            .inspect_err(|e| db.note_error(&e.to_string()))
            .with_context(|| format!("Query failed: {}", surql))?;
        let val: surrealdb::types::Value = response
            .take(0)
            .context("Failed to parse query response")?;
        Ok(val.into_json_value())
    }
}

#[async_trait::async_trait]
impl Db for SurrealSdkDb {
    async fn query(
        &self,
        surql: &str,
        binds: &[(&str, serde_json::Value)],
    ) -> Result<Vec<serde_json::Value>, DbError> {
        let handle = self.db.handle();
        let mut q = handle.query(surql);
        for (name, value) in binds {
            q = q.bind(((*name).to_string(), value.clone()));
        }
        let mut response = self.bounded(surql, q).await?;

        let n = response.num_statements();
        let mut out = Vec::with_capacity(n);
        for i in 0..n {
            let val: surrealdb::types::Value = response
                .take(i)
                .map_err(|e| DbError::Query(e.to_string()))?;
            out.push(val.into_json_value());
        }
        Ok(out)
    }

    async fn version(&self) -> Result<String, DbError> {
        let handle = self.db.handle();
        self.bounded("VERSION", handle.version())
            .await
            .map(|v| v.to_string())
    }

    fn connection(&self) -> DbConnection {
        match self.consecutive_timeouts.load(Ordering::Relaxed) {
            0 => DbConnection::Ok,
            consecutive_timeouts => DbConnection::Stalled {
                consecutive_timeouts,
            },
        }
    }
}
