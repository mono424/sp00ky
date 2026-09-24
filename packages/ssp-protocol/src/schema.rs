//! What upstream syncs, and how a running service follows it.
//!
//! The scheduler and the SSP both hold per-table state derived from upstream
//! DDL: the replica its opaque-field sets and one hash per table, the SSP
//! circuit each table's select permission, link targets, opaque fields and
//! columns. Both used to read that DDL once, at a clone or a bootstrap, so a
//! table added or removed afterwards stayed invisible until something rebuilt
//! everything: a view on a new table was default-denied, and a dropped table
//! kept its hash on the scheduler until the bootstrap breaker re-cloned the
//! whole replica.
//!
//! This module is the shared, pure half of following upstream instead:
//! [`SchemaProbe`] reads the answer to [`INFO_FOR_DB`] (plus the CLI's
//! `_00_schema_state` rows) into the set of synced tables and a fingerprint,
//! and [`SchemaTracker`] turns successive probes into what the service has to
//! do. Both services run the same rules, so they agree on the table set.

use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};

use crate::{define_str_is_nosync, table_excluded_from_sync};

/// The DDL half of a probe: every table with its `DEFINE TABLE` string.
pub const INFO_FOR_DB: &str = "INFO FOR DB";

/// The other half: the hashes the CLI records for each schema blob it applies
/// (`apps/cli/src/migrate.rs`). `_00_schema_state:internal` changes whenever
/// the generated per-table events do, and those enumerate every field, so a
/// field added, retyped or marked opaque moves the fingerprint even though
/// `INFO FOR DB` does not list fields. Run on its own: the table does not exist
/// before the first CLI apply, and a failure here only means "no rows".
pub const SCHEMA_STATE_QUERY: &str = "SELECT id, hash FROM _00_schema_state";

/// Consecutive probes a held table has to be missing from before it is
/// removed. One probe is not enough evidence: a restore wipes upstream partway,
/// and a migration that drops and redefines a table can be caught between the
/// two statements.
pub const REMOVE_AFTER_PROBES: u32 = 2;

/// Split an `INFO FOR DB` answer into the tables that sync (name to
/// `DEFINE TABLE` string) and the ones marked `-- @nosync`. `_00_*` runtime
/// tables are in neither. `None` when the answer has no `tables` object at
/// all, which a caller must read as "unknown", never as "no tables".
pub fn sync_table_defs(
    info_for_db: &Value,
) -> Option<(BTreeMap<String, String>, BTreeSet<String>)> {
    let tables = info_for_db.get("tables")?.as_object()?;
    let mut synced = BTreeMap::new();
    let mut nosync = BTreeSet::new();
    for (name, def) in tables {
        if table_excluded_from_sync(name) {
            continue;
        }
        let def = def.as_str().unwrap_or("");
        if define_str_is_nosync(def) {
            nosync.insert(name.clone());
        } else {
            synced.insert(name.clone(), def.to_string());
        }
    }
    Some((synced, nosync))
}

/// One read of upstream's schema.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchemaProbe {
    /// Changes whenever a synced table's `DEFINE TABLE`, the synced or nosync
    /// set, or any `_00_schema_state` hash does.
    pub fingerprint: String,
    /// Synced tables and their `DEFINE TABLE` strings.
    pub synced: BTreeMap<String, String>,
    /// Tables upstream positively marks `-- @nosync`.
    pub nosync: BTreeSet<String>,
}

impl SchemaProbe {
    /// Build a probe from the [`INFO_FOR_DB`] answer and the
    /// [`SCHEMA_STATE_QUERY`] rows (anything but an array counts as none).
    pub fn parse(info_for_db: &Value, schema_state: &Value) -> Option<Self> {
        let (synced, nosync) = sync_table_defs(info_for_db)?;
        let mut state: Vec<(String, String)> = schema_state
            .as_array()
            .map(|rows| {
                rows.iter()
                    .map(|row| {
                        let field = |k: &str| match row.get(k) {
                            Some(Value::String(s)) => s.clone(),
                            Some(v) => v.to_string(),
                            None => String::new(),
                        };
                        (field("id"), field("hash"))
                    })
                    .collect()
            })
            .unwrap_or_default();
        state.sort();

        let mut hasher = blake3::Hasher::new();
        for (name, def) in &synced {
            for part in ["t", name, def] {
                hasher.update(part.as_bytes());
                hasher.update(&[0]);
            }
        }
        for name in &nosync {
            for part in ["n", name] {
                hasher.update(part.as_bytes());
                hasher.update(&[0]);
            }
        }
        for (id, hash) in &state {
            for part in ["s", id, hash] {
                hasher.update(part.as_bytes());
                hasher.update(&[0]);
            }
        }
        let fingerprint = format!("s1:{}", &hasher.finalize().to_hex()[..32]);
        Some(Self {
            fingerprint,
            synced,
            nosync,
        })
    }

    pub fn tables(&self) -> BTreeSet<String> {
        self.synced.keys().cloned().collect()
    }
}

/// What one probe asks of the service.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SchemaStep {
    /// The fingerprint differs from the last one the service loaded: re-read
    /// the per-table metadata (`INFO FOR TABLE`). Stays set until the caller
    /// reports a successful load through [`SchemaTracker::loaded`].
    pub reload: bool,
    /// Tables in this probe that were not in the previous one.
    pub added: Vec<String>,
    /// Tables the service holds that upstream no longer syncs, confirmed by
    /// [`REMOVE_AFTER_PROBES`] probes in a row.
    pub removed: Vec<String>,
}

impl SchemaStep {
    pub fn is_empty(&self) -> bool {
        !self.reload && self.added.is_empty() && self.removed.is_empty()
    }
}

/// Successive probes folded into [`SchemaStep`]s. Pure: the service does the
/// IO and the acting, this only decides.
#[derive(Debug, Clone, Default)]
pub struct SchemaTracker {
    /// Fingerprint of the metadata the service last loaded.
    loaded: Option<String>,
    /// Synced tables as of the previous accepted probe.
    tables: BTreeSet<String>,
    /// Held tables missing from consecutive probes, and for how many.
    absent: BTreeMap<String, u32>,
}

impl SchemaTracker {
    /// Start from what the service loaded at bootstrap or boot. `loaded` is
    /// the fingerprint that load read, or `None` to force a reload on the
    /// first probe.
    pub fn seeded(loaded: Option<String>, tables: impl IntoIterator<Item = String>) -> Self {
        Self {
            loaded,
            tables: tables.into_iter().collect(),
            absent: BTreeMap::new(),
        }
    }

    /// The fingerprint of the last successful metadata load.
    pub fn loaded_fingerprint(&self) -> Option<&str> {
        self.loaded.as_deref()
    }

    /// Record that the metadata of `fingerprint` is now what the service holds.
    pub fn loaded(&mut self, fingerprint: &str) {
        self.loaded = Some(fingerprint.to_string());
    }

    /// Fold one probe in. `held` is the table set the service currently holds
    /// state for; removals are always computed against it, whatever the
    /// fingerprint says, so a table that comes back through late events is
    /// removed again rather than living on.
    pub fn observe(&mut self, probe: &SchemaProbe, held: &BTreeSet<String>) -> SchemaStep {
        // Zero synced tables while we hold some is a failed read (a database
        // that is missing or mid-restore), not a schema. Acting on it would
        // drop everything.
        if probe.synced.is_empty() && !held.is_empty() {
            return SchemaStep::default();
        }
        let reload = self.loaded.as_deref() != Some(probe.fingerprint.as_str());
        let added: Vec<String> = probe
            .synced
            .keys()
            .filter(|t| !self.tables.contains(*t))
            .cloned()
            .collect();

        let missing: BTreeSet<&String> = held
            .iter()
            .filter(|t| !probe.synced.contains_key(*t))
            .collect();
        self.absent.retain(|t, _| missing.contains(t));
        let mut removed = Vec::new();
        for table in missing {
            let streak = self.absent.entry(table.clone()).or_insert(0);
            *streak += 1;
            if *streak >= REMOVE_AFTER_PROBES {
                removed.push(table.clone());
            }
        }

        self.tables = probe.tables();
        SchemaStep {
            reload,
            added,
            removed,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn info(tables: &[(&str, &str)]) -> Value {
        let map: serde_json::Map<String, Value> = tables
            .iter()
            .map(|(n, d)| (n.to_string(), Value::String(d.to_string())))
            .collect();
        json!({ "tables": map })
    }

    fn probe(tables: &[&str]) -> SchemaProbe {
        let defs: Vec<(&str, String)> = tables
            .iter()
            .map(|t| (*t, format!("DEFINE TABLE {t}")))
            .collect();
        let defs: Vec<(&str, &str)> = defs.iter().map(|(t, d)| (*t, d.as_str())).collect();
        SchemaProbe::parse(&info(&defs), &Value::Null).unwrap()
    }

    fn set(tables: &[&str]) -> BTreeSet<String> {
        tables.iter().map(|t| t.to_string()).collect()
    }

    #[test]
    fn a_probe_splits_synced_from_nosync_and_skips_runtime_tables() {
        let p = SchemaProbe::parse(
            &info(&[
                ("game", "DEFINE TABLE game PERMISSIONS FULL"),
                ("secret", "DEFINE TABLE secret COMMENT 'sp00ky:nosync'"),
                ("_00_query", "DEFINE TABLE _00_query"),
                ("_00_user_feature", "DEFINE TABLE _00_user_feature"),
            ]),
            &Value::Null,
        )
        .unwrap();
        assert_eq!(p.tables(), set(&["_00_user_feature", "game"]));
        assert_eq!(p.nosync, set(&["secret"]));
        assert_eq!(p.synced["game"], "DEFINE TABLE game PERMISSIONS FULL");
    }

    #[test]
    fn an_answer_without_tables_is_unknown_not_empty() {
        assert!(SchemaProbe::parse(&Value::Null, &Value::Null).is_none());
        assert!(SchemaProbe::parse(&json!({ "functions": {} }), &Value::Null).is_none());
        assert!(SchemaProbe::parse(&json!({ "tables": {} }), &Value::Null).is_some());
    }

    #[test]
    fn the_fingerprint_follows_ddl_and_schema_state_only() {
        let base = info(&[("game", "DEFINE TABLE game PERMISSIONS FULL")]);
        let state = json!([{ "id": "_00_schema_state:internal", "hash": "a" }]);
        let a = SchemaProbe::parse(&base, &state).unwrap();
        assert_eq!(
            a.fingerprint,
            SchemaProbe::parse(&base, &state).unwrap().fingerprint
        );

        let perm = info(&[("game", "DEFINE TABLE game PERMISSIONS NONE")]);
        assert_ne!(
            a.fingerprint,
            SchemaProbe::parse(&perm, &state).unwrap().fingerprint
        );

        // A field change shows up only in the CLI's internal-schema hash.
        let fields = json!([{ "id": "_00_schema_state:internal", "hash": "b" }]);
        assert_ne!(
            a.fingerprint,
            SchemaProbe::parse(&base, &fields).unwrap().fingerprint
        );

        // Row order of the state query does not matter.
        let two = json!([{ "id": "x", "hash": "1" }, { "id": "y", "hash": "2" }]);
        let two_rev = json!([{ "id": "y", "hash": "2" }, { "id": "x", "hash": "1" }]);
        assert_eq!(
            SchemaProbe::parse(&base, &two).unwrap().fingerprint,
            SchemaProbe::parse(&base, &two_rev).unwrap().fingerprint
        );
    }

    #[test]
    fn added_tables_are_reported_once() {
        let mut t = SchemaTracker::seeded(Some(probe(&["game"]).fingerprint), set(&["game"]));
        let step = t.observe(&probe(&["game", "comment"]), &set(&["game"]));
        assert_eq!(step.added, vec!["comment".to_string()]);
        assert!(step.reload, "the table set moved, so the fingerprint did");
        let again = t.observe(&probe(&["game", "comment"]), &set(&["game", "comment"]));
        assert!(again.added.is_empty());
    }

    #[test]
    fn reload_stays_due_until_the_load_is_reported() {
        let mut t = SchemaTracker::seeded(None, set(&["game"]));
        let p = probe(&["game"]);
        assert!(t.observe(&p, &set(&["game"])).reload);
        assert!(
            t.observe(&p, &set(&["game"])).reload,
            "a failed load is retried"
        );
        t.loaded(&p.fingerprint);
        assert!(!t.observe(&p, &set(&["game"])).reload);
        assert!(t.observe(&p, &set(&["game"])).is_empty());
    }

    #[test]
    fn a_removal_needs_consecutive_probes() {
        let mut t = SchemaTracker::seeded(None, set(&["game", "old"]));
        let held = set(&["game", "old"]);
        assert!(t.observe(&probe(&["game"]), &held).removed.is_empty());
        assert_eq!(
            t.observe(&probe(&["game"]), &held).removed,
            vec!["old".to_string()]
        );

        // Back in between: the streak starts over.
        let mut t = SchemaTracker::seeded(None, set(&["game", "old"]));
        t.observe(&probe(&["game"]), &held);
        t.observe(&probe(&["game", "old"]), &held);
        assert!(t.observe(&probe(&["game"]), &held).removed.is_empty());
    }

    #[test]
    fn a_removed_table_that_comes_back_through_late_events_goes_again() {
        let mut t = SchemaTracker::seeded(None, set(&["game", "old"]));
        t.observe(&probe(&["game"]), &set(&["game", "old"]));
        assert_eq!(
            t.observe(&probe(&["game"]), &set(&["game", "old"])).removed,
            vec!["old".to_string()]
        );
        // The service dropped it...
        assert!(t
            .observe(&probe(&["game"]), &set(&["game"]))
            .removed
            .is_empty());
        // ...and an event that was still in flight re-created it.
        let back = t.observe(&probe(&["game"]), &set(&["game", "old"]));
        assert!(
            back.removed.is_empty(),
            "first sighting of the re-created table"
        );
        assert_eq!(
            t.observe(&probe(&["game"]), &set(&["game", "old"])).removed,
            vec!["old".to_string()]
        );
    }

    #[test]
    fn an_empty_probe_never_removes_what_we_hold() {
        let mut t = SchemaTracker::seeded(None, set(&["game"]));
        for _ in 0..5 {
            assert!(t.observe(&probe(&[]), &set(&["game"])).is_empty());
        }
        // With nothing held, an empty schema is just an empty schema.
        let mut fresh = SchemaTracker::default();
        assert!(fresh
            .observe(&probe(&[]), &BTreeSet::new())
            .removed
            .is_empty());
    }
}
