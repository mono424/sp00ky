//! Cached bodies outlive a schema change on this store, and that loses columns
//! for good (TS `sqlite-column-reset.ts`).
//!
//! A body is landed through `cleanRecord`, which keeps only the columns the
//! running app's schema declares, and is stored at the server's `_00_rv`. The
//! store keeps rows across schema changes (see [`crate::services::migrator`]),
//! so a build that fetched a row after the server added and backfilled a column
//! stored it WITHOUT that column at the new version. The next build knows the
//! column, but its local version equals the server's, so the fetch plan never
//! asks for the row again and the column stays missing until something else
//! bumps the row.
//!
//! So the store remembers each table's column set (`_00_kv`, key
//! [`COLUMN_SETS_KEY`]). On boot, before the prime reads the versions, a table
//! whose set changed, or every table when no set was recorded yet, gets its
//! cached `_00_rv` reset to 0. The bodies stay and keep rendering; any view
//! that names one of those rows sees it as stale and fetches it again, whole.
//! Rows with an unsettled local write keep their version.
//! ([`crate::services::local::LocalDatabaseService::reset_changed_tables`]
//! applies it.)

use std::collections::{BTreeMap, HashSet};

use crate::types::Schema;

/// `_00_kv` key holding the recorded column sets.
pub const COLUMN_SETS_KEY: &str = "_00_column_sets";

/// The column set of every table in `tables`, as `cleanRecord` sees it: the
/// sorted declared column names joined by `,`. Tables the schema does not
/// declare are skipped.
pub fn column_fingerprints<'a>(
    schema: &Schema,
    tables: impl IntoIterator<Item = &'a str>,
) -> BTreeMap<String, String> {
    let mut out = BTreeMap::new();
    for table in tables {
        let Some(entry) = schema.tables.get(table) else {
            continue;
        };
        // BTreeMap keys are already sorted.
        let names: Vec<&str> = entry.columns.keys().map(String::as_str).collect();
        out.insert(table.to_string(), names.join(","));
    }
    out
}

/// The tables of `current` whose rows need their versions reset, given the
/// `stored` sets (`None` when none were recorded) and the tables that have
/// rows.
pub fn changed_tables(
    stored: Option<&BTreeMap<String, String>>,
    current: &BTreeMap<String, String>,
    tables_with_rows: &HashSet<String>,
) -> Vec<String> {
    current
        .iter()
        .filter(|(table, set)| {
            stored.is_none_or(|s| s.get(*table) != Some(*set)) && tables_with_rows.contains(*table)
        })
        .map(|(table, _)| table.clone())
        .collect()
}

/// Whether `stored` already equals `current`.
pub fn same_column_sets(
    stored: Option<&BTreeMap<String, String>>,
    current: &BTreeMap<String, String>,
) -> bool {
    stored == Some(current)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::services::local::LocalDatabaseService;
    use crate::testing::row;
    use crate::types::ColumnSchema;
    use crate::value::Value;
    use serde_json::json;

    // A body landed under an older schema lacks the columns that schema did not
    // know, at the server's version. Without a reset the next build never
    // fetches it again (WhitePawn's puzzles stuck on "Preparing" for want of
    // `layout`).

    fn sets(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    fn old_sets() -> BTreeMap<String, String> {
        sets(&[("puzzle", "title"), ("game", "white")])
    }

    fn new_sets() -> BTreeMap<String, String> {
        sets(&[("puzzle", "layout,title"), ("game", "white")])
    }

    fn setup() -> LocalDatabaseService {
        let db = LocalDatabaseService::open_memory().unwrap();
        db.provision().unwrap();
        // Landed by the old build: no `layout`, at the server's version 6.
        db.create("puzzle:a", row(json!({"title": "A", "_00_rv": 6})))
            .unwrap();
        db.create("puzzle:b", row(json!({"title": "B", "_00_rv": 6})))
            .unwrap();
        db.create("game:1", row(json!({"white": "x", "_00_rv": 3})))
            .unwrap();
        db
    }

    fn versions(db: &LocalDatabaseService, table: &str) -> BTreeMap<String, i64> {
        db.scan_versions(&[table.to_string()]).unwrap()[table]
            .iter()
            .cloned()
            .collect()
    }

    fn keep(ids: &[&str]) -> HashSet<String> {
        ids.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn fingerprints_are_the_sorted_declared_columns() {
        let schema = Schema::default().with_table(
            "puzzle",
            [
                ("title", ColumnSchema::default()),
                (
                    "layout",
                    ColumnSchema {
                        optional: true,
                        ..Default::default()
                    },
                ),
            ],
        );
        assert_eq!(
            column_fingerprints(&schema, ["puzzle", "missing"]),
            sets(&[("puzzle", "layout,title")])
        );
    }

    #[test]
    fn first_boot_with_no_recorded_sets_resets_every_cached_table_once() {
        let db = setup();
        let mut reset = db.reset_changed_tables(&new_sets(), &keep(&[])).unwrap();
        reset.sort();
        assert_eq!(reset, vec!["game".to_string(), "puzzle".to_string()]);
        assert_eq!(
            versions(&db, "puzzle"),
            BTreeMap::from([("puzzle:a".into(), 0), ("puzzle:b".into(), 0)])
        );
        assert_eq!(
            versions(&db, "game"),
            BTreeMap::from([("game:1".into(), 0)])
        );
        // The body stays, and its own `_00_rv` agrees with the column.
        let a = db.get_by_id("puzzle:a").unwrap().unwrap();
        assert_eq!(a["title"], Value::from("A"));
        assert_eq!(a["_00_rv"], Value::Int(0));

        // The next boot of the same build changes nothing.
        db.create(
            "puzzle:a",
            row(json!({"title": "A", "layout": "mirror", "_00_rv": 6})),
        )
        .unwrap();
        assert!(db
            .reset_changed_tables(&new_sets(), &keep(&[]))
            .unwrap()
            .is_empty());
        assert_eq!(versions(&db, "puzzle")["puzzle:a"], 6);
    }

    #[test]
    fn a_new_column_resets_only_that_table() {
        let db = setup();
        db.reset_changed_tables(&old_sets(), &keep(&[])).unwrap();
        db.create("puzzle:a", row(json!({"title": "A", "_00_rv": 6})))
            .unwrap();
        db.create("puzzle:b", row(json!({"title": "B", "_00_rv": 6})))
            .unwrap();
        db.create("game:1", row(json!({"white": "x", "_00_rv": 3})))
            .unwrap();

        assert_eq!(
            db.reset_changed_tables(&new_sets(), &keep(&[])).unwrap(),
            vec!["puzzle".to_string()]
        );
        assert_eq!(
            versions(&db, "puzzle"),
            BTreeMap::from([("puzzle:a".into(), 0), ("puzzle:b".into(), 0)])
        );
        assert_eq!(
            versions(&db, "game"),
            BTreeMap::from([("game:1".into(), 3)])
        );
    }

    #[test]
    fn keeps_the_version_of_a_row_with_an_unsettled_local_write() {
        let db = setup();
        db.reset_changed_tables(&new_sets(), &keep(&["puzzle:b", "game:9"]))
            .unwrap();
        assert_eq!(
            versions(&db, "puzzle"),
            BTreeMap::from([("puzzle:a".into(), 0), ("puzzle:b".into(), 6)])
        );
    }

    #[test]
    fn helpers() {
        let current = new_sets();
        assert!(!same_column_sets(None, &current));
        assert!(same_column_sets(Some(&new_sets()), &current));
        assert!(!same_column_sets(Some(&old_sets()), &current));
        let with_rows = keep(&["puzzle"]);
        assert_eq!(
            changed_tables(Some(&old_sets()), &current, &with_rows),
            vec!["puzzle".to_string()]
        );
        assert_eq!(
            changed_tables(None, &current, &with_rows),
            vec!["puzzle".to_string()]
        );
        assert!(changed_tables(Some(&new_sets()), &current, &with_rows).is_empty());
    }
}
