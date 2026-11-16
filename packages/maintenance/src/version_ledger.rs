//! The background move of the `_00_version` ledger to keyed rows (see
//! `ssp_protocol::version_ledger`), run at boot by the scheduler and by a
//! standalone SSP.
//!
//! The generated events already move a legacy row the first time its record
//! is written, so nothing is wrong while this runs. It is what makes the
//! readers fast for every row: a scan keeps a fallback to the legacy
//! `record_id` lookup while any legacy row remains, and that lookup can cost
//! tens of milliseconds per row on a large ledger.

use std::sync::Arc;
use std::time::{Duration, Instant};

use serde_json::Value;
use ssp_protocol::version_ledger::{self, LedgerDb, MigrationOptions, MigrationProgress};
use tracing::{debug, info, warn};

use crate::db::{DbConfig, ReconnectingDb};

/// [`LedgerDb`] over a [`ReconnectingDb`] session.
struct Ledger(Arc<ReconnectingDb>);

impl LedgerDb for Ledger {
    async fn query_last(&self, sql: &str) -> Result<Value, String> {
        let db = self.0.handle();
        let mut response = match db.query(sql).await {
            Ok(response) => response,
            Err(e) => {
                let e = e.to_string();
                self.0.note_error(&e);
                return Err(e);
            }
        };
        let mut last = Value::Null;
        let mut errors = Vec::new();
        for i in 0..response.num_statements() {
            match response.take::<surrealdb::types::Value>(i) {
                Ok(v) => last = v.into_json_value(),
                Err(e) => errors.push(e.to_string()),
            }
        }
        match version_ledger::cause(&errors) {
            Some(e) => {
                self.0.note_error(&e);
                Err(e)
            }
            None => Ok(last),
        }
    }

    async fn pause(&self, ms: u64) {
        tokio::time::sleep(Duration::from_millis(ms)).await;
    }
}

/// How long to wait before the `attempt`-th retry of a run that failed.
fn backoff(attempt: u32) -> Duration {
    Duration::from_secs((5u64 << attempt.min(6)).min(300))
}

/// Run the migration to the end on `db`, retrying a failed run with backoff.
/// Progress is logged every ten seconds.
pub async fn run_migration(db: Arc<ReconnectingDb>) {
    let ledger = Ledger(db);
    let opts = MigrationOptions::default();
    let started = Instant::now();
    let mut total = MigrationProgress::default();
    let mut announced = false;
    let mut attempt = 0;
    loop {
        let mut last_log = Instant::now();
        let base = total.clone();
        let run = version_ledger::migrate(&ledger, &opts, |p| {
            if !announced && p.moved > 0 {
                announced = true;
                info!("Version ledger: moving legacy rows to keyed ids in the background");
            }
            if last_log.elapsed() >= Duration::from_secs(10) {
                last_log = Instant::now();
                info!(
                    moved = base.moved + p.moved,
                    batches = base.batches + p.batches,
                    conflicts = base.conflicts + p.conflicts,
                    elapsed_s = started.elapsed().as_secs(),
                    "Version ledger migration in progress"
                );
            }
        })
        .await;
        match run {
            Ok(p) => {
                total.moved += p.moved;
                total.batches += p.batches;
                total.conflicts += p.conflicts;
                if p.done {
                    if total.moved > 0 {
                        info!(
                            moved = total.moved,
                            batches = total.batches,
                            conflicts = total.conflicts,
                            elapsed_s = started.elapsed().as_secs(),
                            "Version ledger migrated: every row is keyed by its record"
                        );
                    } else {
                        debug!("Version ledger already keyed");
                    }
                    return;
                }
                warn!(
                    moved = total.moved,
                    "Version ledger: legacy rows keep appearing behind the migration; retrying"
                );
            }
            Err(e) => {
                warn!(error = %e, moved = total.moved, "Version ledger migration failed; retrying");
            }
        }
        tokio::time::sleep(backoff(attempt)).await;
        attempt += 1;
    }
}

/// Open a session of its own (the migration's writes must not queue the
/// host's other requests behind them on a shared one) and run
/// [`run_migration`] on it in the background. The session is re-signed in
/// while the run lasts and dropped when it ends.
pub fn spawn_migration(config: DbConfig) {
    tokio::spawn(async move {
        let mut attempt = 0;
        let db = loop {
            match ReconnectingDb::connect(&config).await {
                Ok(db) => break db,
                Err(e) => {
                    warn!(error = %e, "Version ledger migration: could not open a database session; retrying");
                    tokio::time::sleep(backoff(attempt)).await;
                    attempt += 1;
                }
            }
        };
        let resignin = {
            let db = Arc::clone(&db);
            tokio::spawn(async move {
                let mut interval =
                    tokio::time::interval(Duration::from_secs(crate::db::RESIGNIN_INTERVAL_SECS));
                interval.tick().await;
                loop {
                    interval.tick().await;
                    db.refresh().await;
                }
            })
        };
        run_migration(db).await;
        resignin.abort();
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_grows_and_is_capped() {
        assert_eq!(backoff(0), Duration::from_secs(5));
        assert_eq!(backoff(1), Duration::from_secs(10));
        assert_eq!(backoff(5), Duration::from_secs(160));
        assert_eq!(backoff(6), Duration::from_secs(300));
        assert_eq!(backoff(60), Duration::from_secs(300));
    }
}
