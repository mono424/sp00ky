//! Exporting the main SurrealDB to a file, surviving a table whose rows
//! SurrealDB can no longer export.
//!
//! SurrealDB 4.0.1 can leave rows in a table that its export refuses: on
//! whitepawn (2026-11-21) a per-user edge table removed and defined again
//! while edge writes were in flight kept rows stamped with a retired layout
//! generation ("names generation 47, outside the table's generations 48 to
//! 48"). `SELECT` still reads them, but the export task fails on them, and it
//! fails by cutting the HTTP stream, so the client sees only "error decoding
//! response body". Every nightly backup failed for eight days on that one
//! table, while the rest of the database exported fine.
//!
//! The tables that can end up like that are sp00ky's own edge tables
//! (`_00_list_ref*`): the SSP is the only writer that removes and defines
//! tables under live writes, and it rebuilds their rows from the registered
//! views whenever it registers again, which a restore makes it do. So when an
//! export fails, each of those tables is exported on its own, the ones
//! SurrealDB refuses are left out of a second full export, and their
//! definitions (fields, permissions) are appended without their rows. A
//! failure anywhere else still fails the export: an application table is
//! never left out of a backup.

use std::path::Path;

use anyhow::{Context, Result};
use surrealdb_rpc::export::{ExcludedTables, TableConfig};
use tokio::io::AsyncWriteExt as _;
use tracing::{info, warn};

use crate::db::HttpDb;

/// The tables an export may leave the rows of out: the SSP's edge tables,
/// which it writes again from the views it serves.
pub const REBUILDABLE_TABLE_PREFIX: &str = "_00_list_ref";

/// Whether `table` is one whose rows the SSP writes again after a restore.
pub fn is_rebuildable(table: &str) -> bool {
    table.starts_with(REBUILDABLE_TABLE_PREFIX)
}

/// Export the database `db` has selected into `path`.
///
/// Returns the tables whose rows were left out because SurrealDB refused to
/// export them (empty for a complete export). Their definitions are in the
/// file.
pub async fn export_database(db: &HttpDb, path: &Path) -> Result<Vec<String>> {
    let first = match db.export(path).await {
        Ok(()) => return Ok(Vec::new()),
        Err(e) => anyhow::Error::new(e),
    };
    warn!(error = %format!("{first:#}"), "Export failed; probing sp00ky's edge tables for rows SurrealDB refuses to export");

    let refused = match refused_rebuildable_tables(db).await {
        Ok(refused) => refused,
        Err(probe) => {
            warn!(error = %format!("{probe:#}"), "Could not probe the edge tables");
            return Err(first);
        }
    };
    if refused.is_empty() {
        return Err(first.context(
            "SurrealDB ended the export early, and none of sp00ky's edge tables is the cause \
             (SurrealDB's own log names what it failed on)",
        ));
    }
    warn!(
        tables = ?refused,
        "SurrealDB refuses to export the rows of these edge tables; exporting without their rows \
         (the SSP writes them again when it registers after a restore)"
    );

    export_without_rows_of(db, path, &refused).await?;
    info!(tables = ?refused, "Export complete without the rows of the refused edge tables");
    Ok(refused)
}

/// Export the database into `path` without the rows of `tables`, keeping
/// their definitions: a restore must still have the tables, with their
/// permissions, before the SSP writes their edges again.
pub async fn export_without_rows_of(db: &HttpDb, path: &Path, tables: &[String]) -> Result<()> {
    db.export(path)
        .with_config()
        .tables(TableConfig::Exclude(ExcludedTables {
            exclude: tables.to_vec(),
        }))
        .await
        .with_context(|| format!("Export without the rows of {tables:?} failed"))?;
    let definitions = tempfile::NamedTempFile::new()
        .context("Failed to create tempfile for table definitions")?;
    definitions_only(db, definitions.path(), tables.to_vec())
        .await
        .with_context(|| format!("Failed to export the definitions of {tables:?}"))?;
    append(definitions.path(), path).await
}

/// The edge tables whose own export SurrealDB refuses, in name order.
async fn refused_rebuildable_tables(db: &HttpDb) -> Result<Vec<String>> {
    let mut response = db
        .query("RETURN object::keys((INFO FOR DB).tables);")
        .await
        .context("Failed to list the database's tables")?;
    let tables: surrealdb::types::Value =
        response.take(0).context("Failed to read the table list")?;
    let mut candidates = rebuildable_tables(&tables.into_json_value());
    candidates.sort();

    let scratch =
        tempfile::NamedTempFile::new().context("Failed to create tempfile for the probe")?;
    let mut refused = Vec::new();
    for table in candidates {
        let probe =
            only_tables(db.export(scratch.path()).with_config(), vec![table.clone()]).records(true);
        if let Err(e) = probe.await {
            warn!(table = %table, error = %e, "SurrealDB refuses to export this table's rows");
            refused.push(table);
        }
    }
    Ok(refused)
}

/// The rebuildable tables in an `object::keys((INFO FOR DB).tables)` answer.
fn rebuildable_tables(tables: &serde_json::Value) -> Vec<String> {
    tables
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|t| t.as_str())
        .filter(|t| is_rebuildable(t))
        .map(str::to_owned)
        .collect()
}

/// The `DEFINE TABLE` / `DEFINE FIELD` / `DEFINE INDEX` statements of
/// `tables`, without rows or anything database-wide.
async fn definitions_only(db: &HttpDb, path: &Path, tables: Vec<String>) -> Result<()> {
    only_tables(db.export(path).with_config(), tables)
        .records(false)
        .await?;
    Ok(())
}

type ExportWithConfig<'r> = surrealdb::method::Export<
    'r,
    surrealdb::engine::remote::http::Client,
    std::path::PathBuf,
    surrealdb::method::ExportConfig,
>;

/// An export of `tables` and nothing database-wide: no database definition,
/// users, accesses, params, functions, analyzers, APIs, buckets, modules,
/// configs or sequences, which the full export already carries.
fn only_tables(export: ExportWithConfig<'_>, tables: Vec<String>) -> ExportWithConfig<'_> {
    export
        .database_definition(false)
        .users(false)
        .accesses(false)
        .params(false)
        .functions(false)
        .analyzers(false)
        .apis(false)
        .buckets(false)
        .modules(false)
        .configs(false)
        .sequences(false)
        .versions(false)
        .tables(tables)
}

/// Append the table definitions at `from` to the export at `to`, without the
/// `OPTION IMPORT;` their own export opens with: the export already set it,
/// and `/import` refuses a second one.
async fn append(from: &Path, to: &Path) -> Result<()> {
    let definitions = tokio::fs::read_to_string(from)
        .await
        .context("Failed to read the table definitions")?;
    let mut dst = tokio::fs::OpenOptions::new()
        .append(true)
        .open(to)
        .await
        .context("Failed to open the export to append to")?;
    dst.write_all(without_import_option(&definitions).as_bytes())
        .await
        .context("Failed to append the table definitions")?;
    dst.flush().await.context("Failed to write the export")?;
    Ok(())
}

fn without_import_option(export: &str) -> String {
    let mut out = String::from("\n");
    for line in export.lines().filter(|l| l.trim() != "OPTION IMPORT;") {
        out.push_str(line);
        out.push('\n');
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn only_sp00kys_edge_tables_are_rebuildable() {
        let tables = json!([
            "game",
            "_00_list_ref",
            "_00_list_ref_anon",
            "_00_list_ref_user_eb0wzu5qq1a3z666tfi9",
            "_00_query",
            "list_ref_notes",
        ]);
        assert_eq!(
            rebuildable_tables(&tables),
            vec![
                "_00_list_ref",
                "_00_list_ref_anon",
                "_00_list_ref_user_eb0wzu5qq1a3z666tfi9"
            ]
        );
        assert!(rebuildable_tables(&json!(null)).is_empty());
    }

    #[test]
    fn appended_definitions_drop_their_import_option() {
        let defs = "-- OPTION\n\nOPTION IMPORT;\n\nDEFINE TABLE t SCHEMALESS;\n";
        let out = without_import_option(defs);
        assert!(!out.contains("OPTION IMPORT;"), "{out}");
        assert!(out.contains("DEFINE TABLE t SCHEMALESS;\n"), "{out}");
    }
}
