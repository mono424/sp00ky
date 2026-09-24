//! The replica follows upstream's table set while the scheduler runs.
//!
//! Two pieces of replica state come from upstream DDL rather than from rows:
//! which tables sync at all, and which fields of each are opaque (never held,
//! never hashed). Both used to be read only by a clone, and a dropped table
//! was forgotten only at boot. So a running scheduler kept handing a removed
//! or `@nosync` table's hash to every SSP that bootstrapped, which left the
//! table out of its own load and disputed it until the bootstrap breaker
//! re-cloned everything; and a table added after the clone had no opaque set,
//! so under the changefeed transport its hash counted columns every SSP omits.
//!
//! [`SchemaWatch`] runs the shared [`SchemaTracker`] rules (the SSP runs the
//! same ones) on the snapshot updater's tick, split around `drain_lock`:
//! [`SchemaWatch::read`] does the network before the lock is taken,
//! [`SchemaWatch::apply`] changes the replica after the drain, under the lock
//! and never while an SSP bootstraps, so a removed table's buffered events are
//! applied before the table goes.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use ssp_protocol::schema::{SchemaProbe, SchemaStep, SchemaTracker};
use tokio::sync::RwLock;
use tracing::{info, warn};

use crate::replica::Replica;

/// Deadline for one read of upstream's schema. The same reasoning as the drift
/// check's: the read runs inside the snapshot updater's serial tick, and a
/// SurrealDB session that stops answering must not take the drain with it.
const READ_TIMEOUT: Duration = Duration::from_secs(60);

/// Deadline for the probe a bootstrap-verify takes, inside an SSP's request.
const VERIFY_PROBE_TIMEOUT: Duration = Duration::from_secs(5);

/// What upstream syncs, readable without the replica lock (which a drain or a
/// re-clone holds for a long time): the ingest path checks it per event.
#[derive(Debug, Default)]
pub struct SyncedSchema {
    /// Tables upstream syncs, as of the last probe.
    pub synced: BTreeSet<String>,
    /// Tables upstream positively marks `-- @nosync`.
    pub nosync: BTreeSet<String>,
    /// Opaque fields per table, as the replica holds them.
    pub opaque: BTreeMap<String, BTreeSet<String>>,
}

/// A lock-free mirror of [`SyncedSchema`], replaced whole on every change.
pub type SchemaCell = Arc<std::sync::RwLock<Arc<SyncedSchema>>>;

/// The upstream schema read of one tick, taken before `drain_lock`.
pub struct SchemaRead {
    probe: SchemaProbe,
    /// Opaque fields per table, read only when the fingerprint moved.
    opaque: Option<BTreeMap<String, BTreeSet<String>>>,
}

pub struct SchemaWatch {
    db: OnceLock<Arc<maintenance::db::ReconnectingDb>>,
    tracker: tokio::sync::Mutex<SchemaTracker>,
    pub cell: SchemaCell,
}

impl Default for SchemaWatch {
    fn default() -> Self {
        Self::new()
    }
}

impl SchemaWatch {
    pub fn new() -> Self {
        Self {
            db: OnceLock::new(),
            tracker: tokio::sync::Mutex::new(SchemaTracker::default()),
            cell: SchemaCell::default(),
        }
    }

    /// Hand over the upstream handle, once `start()` has one.
    pub fn attach(&self, db: Arc<maintenance::db::ReconnectingDb>) {
        let _ = self.db.set(db);
    }

    /// Whether upstream marks `table` `-- @nosync`. Only positive knowledge:
    /// a table the last probe did not list at all (one a deploy just added)
    /// is not nosync.
    pub fn is_nosync(cell: &SchemaCell, table: &str) -> bool {
        cell.read().map(|s| s.nosync.contains(table)).unwrap_or(false)
    }

    /// Boot: start the tracker from the tables the persisted snapshot hashes,
    /// so a table added while the scheduler was down reads as added, and load
    /// the opaque fields a restart would otherwise leave empty. Runs before
    /// the scheduler is Ready, so nothing else touches the replica.
    pub async fn boot(&self, replica: &Arc<RwLock<Replica>>) -> SchemaStep {
        {
            let hashed = replica.read().await.snapshot_hashes().keys().cloned().collect::<Vec<_>>();
            *self.tracker.lock().await = SchemaTracker::seeded(None, hashed);
        }
        match self.read().await {
            Some(read) => self.apply(read, replica).await,
            None => SchemaStep::default(),
        }
    }

    /// Probe upstream and, when the fingerprint moved since the last load,
    /// read every synced table's opaque fields. `None` when there is no handle
    /// yet, the read failed, or upstream answered without a table list.
    pub async fn read(&self) -> Option<SchemaRead> {
        let db = self.db.get()?;
        let loaded = self.tracker.lock().await.loaded_fingerprint().map(str::to_owned);
        let work = async {
            let handle = db.handle();
            let Some(probe) = Replica::probe_schema(&*handle).await? else {
                return anyhow::Ok(None);
            };
            let opaque = if loaded.as_deref() != Some(probe.fingerprint.as_str()) {
                let tables: Vec<String> = probe.synced.keys().cloned().collect();
                Some(Replica::discover_opaque_fields(&*handle, &tables).await)
            } else {
                None
            };
            Ok(Some(SchemaRead { probe, opaque }))
        };
        match tokio::time::timeout(READ_TIMEOUT, work).await {
            Ok(Ok(read)) => read,
            Ok(Err(e)) => {
                db.note_error(&e.to_string());
                warn!(error = %e, "Schema read failed; the table set is re-checked next tick");
                None
            }
            Err(_) => {
                warn!(timeout_secs = READ_TIMEOUT.as_secs(), "Schema read timed out; reconnecting upstream");
                db.force_reconnect();
                None
            }
        }
    }

    /// Fold a read into the replica: replace the opaque sets (marking a table
    /// whose set moved for a from-content rehash) and drop the tables upstream
    /// no longer syncs. The caller holds `drain_lock` and has made sure no SSP
    /// is bootstrapping. Returns what the probe asked for, `added` included,
    /// so the drift check can backfill new tables.
    pub async fn apply(&self, read: SchemaRead, replica: &Arc<RwLock<Replica>>) -> SchemaStep {
        let mut tracker = self.tracker.lock().await;
        let held = replica.read().await.held_tables();
        if read.probe.synced.is_empty() && !held.is_empty() {
            // A missing or mid-restore database, not a schema: the tracker
            // refuses it too, and its empty opaque map must not replace ours.
            warn!(held = held.len(), "Schema: upstream lists no synced tables; ignoring this read");
            return SchemaStep::default();
        }
        let step = tracker.observe(&read.probe, &held);
        let reloaded = read.opaque.is_some();
        let result = replica.write().await.reconcile_schema(&step.removed, read.opaque).await;
        match result {
            Ok(dropped) => {
                if reloaded {
                    tracker.loaded(&read.probe.fingerprint);
                }
                for table in &dropped {
                    warn!(table = %table, "Schema: table no longer synced upstream (removed or @nosync); dropped from the replica");
                    crate::admin::incidents::emit(
                        "scheduler",
                        "schema_change",
                        "recorded",
                        &format!("Table `{table}` is no longer synced upstream; dropped from the replica in place"),
                        None,
                    );
                }
            }
            Err(e) => warn!(error = %e, "Schema: reconciling the replica failed; retrying next tick"),
        }
        for table in &step.added {
            info!(table = %table, "Schema: table added upstream; now synced");
            crate::admin::incidents::emit(
                "scheduler",
                "schema_change",
                "recorded",
                &format!("Table `{table}` added upstream; synced without a re-clone"),
                None,
            );
        }
        let opaque = replica.read().await.opaque_fields().clone();
        if let Ok(mut cell) = self.cell.write() {
            *cell = Arc::new(SyncedSchema {
                synced: read.probe.tables(),
                nosync: read.probe.nosync.clone(),
                opaque,
            });
        }
        step
    }

    /// The opaque fields of `table` if the last probe listed it, `None` for a
    /// table it has not seen yet.
    pub fn opaque_of(cell: &SchemaCell, table: &str) -> Option<BTreeSet<String>> {
        let schema = cell.read().ok()?;
        schema
            .synced
            .contains(table)
            .then(|| schema.opaque.get(table).cloned().unwrap_or_default())
    }

    /// The tables among `tables` that upstream does not sync, by a fresh
    /// probe. A bootstrap-verify uses it to leave a table the SSP rightly did
    /// not load out of the dispute, instead of counting it toward the breaker
    /// that re-clones everything. Empty whenever the probe gives no clear
    /// answer.
    pub async fn not_synced_upstream(&self, tables: &BTreeSet<String>) -> BTreeSet<String> {
        let Some(db) = self.db.get() else {
            return BTreeSet::new();
        };
        let handle = db.handle();
        let probe = match tokio::time::timeout(VERIFY_PROBE_TIMEOUT, Replica::probe_schema(&*handle)).await {
            Ok(Ok(Some(probe))) if !probe.synced.is_empty() => probe,
            _ => return BTreeSet::new(),
        };
        tables
            .iter()
            .filter(|t| !probe.synced.contains_key(*t))
            .cloned()
            .collect()
    }
}

/// Remove `fields` (names, or dotted paths into nested objects) from a row, so
/// it carries exactly what `SELECT * OMIT <fields>` would: what the clone, the
/// SSP bootstrap and the http transport's event payload all hold.
pub fn strip_opaque(record: &mut serde_json::Value, fields: &BTreeSet<String>) {
    fn strip(value: &mut serde_json::Value, path: &[&str]) {
        match path {
            [] => {}
            [leaf] => {
                if let Some(obj) = value.as_object_mut() {
                    obj.remove(*leaf);
                }
            }
            [head, rest @ ..] => {
                if let Some(next) = value.get_mut(*head) {
                    strip(next, rest);
                }
            }
        }
    }
    for path in fields {
        strip(record, &path.split('.').collect::<Vec<_>>());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::replica::RecordOp;
    use serde_json::json;

    fn read(tables: &[&str], nosync: &[&str]) -> SchemaRead {
        let mut map = serde_json::Map::new();
        for t in tables {
            map.insert(t.to_string(), json!(format!("DEFINE TABLE {t}")));
        }
        for t in nosync {
            map.insert(t.to_string(), json!(format!("DEFINE TABLE {t} COMMENT 'sp00ky:nosync'")));
        }
        SchemaRead {
            probe: SchemaProbe::parse(&json!({ "tables": map }), &serde_json::Value::Null).unwrap(),
            opaque: None,
        }
    }

    async fn replica_with(tables: &[&str]) -> (Arc<RwLock<Replica>>, tempfile::TempDir) {
        let tmp = tempfile::tempdir().unwrap();
        let mut replica = Replica::new(tmp.path().join("replica")).await.unwrap();
        for t in tables {
            replica
                .apply(t, RecordOp::Create, &format!("{t}:1"), Some(json!({ "x": 1, "_00_rv": 1 })))
                .await
                .unwrap();
        }
        replica.set_snapshot_state(1, None).await.unwrap();
        (Arc::new(RwLock::new(replica)), tmp)
    }

    #[tokio::test]
    async fn a_table_turned_nosync_leaves_the_replica_and_stays_out() {
        let (replica, _tmp) = replica_with(&["game", "analysis"]).await;
        let watch = SchemaWatch::new();

        // `analysis` went @nosync: one probe is not enough, the second drops it.
        let step = watch.apply(read(&["game"], &["analysis"]), &replica).await;
        assert!(step.removed.is_empty());
        assert!(replica.read().await.snapshot_hashes().contains_key("analysis"));
        assert!(SchemaWatch::is_nosync(&watch.cell, "analysis"), "ingest drops its events from now on");
        assert!(!SchemaWatch::is_nosync(&watch.cell, "game"));

        let step = watch.apply(read(&["game"], &["analysis"]), &replica).await;
        assert_eq!(step.removed, vec!["analysis".to_string()]);
        let rep = replica.read().await;
        assert!(!rep.snapshot_hashes().contains_key("analysis"));
        assert!(!rep.known_tables().contains("analysis"));
        assert!(rep.snapshot_hashes().contains_key("game"));
    }

    #[tokio::test]
    async fn an_added_table_is_reported_for_backfill() {
        let (replica, _tmp) = replica_with(&["game"]).await;
        let watch = SchemaWatch::new();
        *watch.tracker.lock().await = SchemaTracker::seeded(None, ["game".to_string()]);
        let step = watch.apply(read(&["game", "comment"], &[]), &replica).await;
        assert_eq!(step.added, vec!["comment".to_string()]);
        assert!(step.removed.is_empty());
    }

    #[test]
    fn stripping_matches_an_omit_projection() {
        let mut row = json!({ "id": "doc:1", "title": "t", "body": "crdt", "meta": { "secret": 1, "kept": 2 } });
        let fields: BTreeSet<String> = ["body".to_string(), "meta.secret".to_string(), "absent.path".to_string()].into_iter().collect();
        strip_opaque(&mut row, &fields);
        assert_eq!(row, json!({ "id": "doc:1", "title": "t", "meta": { "kept": 2 } }));
    }

    #[tokio::test]
    async fn an_empty_probe_drops_nothing() {
        let (replica, _tmp) = replica_with(&["game"]).await;
        let watch = SchemaWatch::new();
        for _ in 0..3 {
            watch.apply(read(&[], &[]), &replica).await;
        }
        assert!(replica.read().await.snapshot_hashes().contains_key("game"));
    }
}
