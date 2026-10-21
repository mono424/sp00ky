//! Standalone circuit rebuild from the database, over the [`Db`] port.
//!
//! Moved from the VM shell's `self_bootstrap_with_metadata` (the Direct path).
//! The cluster/proxy path stays in `apps/ssp` (it reads rows from the
//! scheduler's HTTP proxy, not the DB). This is the load [`crate::Runtime::bootstrap`]
//! runs on a cold start when the `CircuitStore` has no usable snapshot.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use anyhow::Context;
use serde_json::{json, Value};
use tokio::sync::RwLock;
use tracing::{info, warn};

use ssp::circuit::view::OutputFormat;
use ssp::circuit::{Change, ChangeSet, Circuit, Record, TableMeta};
use ssp_protocol::schema::{SchemaProbe, INFO_FOR_DB, SCHEMA_STATE_QUERY};

use crate::ports::Db;

/// Keyset-paginated page query for the bootstrap scan (ordered by id, never
/// OFFSET — lossy under concurrent writes). It resumes after the id as
/// SurrealDB spelled it ([`ssp_protocol::record_id_literal`]), so a numeric,
/// uuid or escaped key resumes as itself.
///
/// `omit` names fields the circuit must never hold — see
/// [`ssp_protocol::OPAQUE_FIELD_COMMENT`]. Omitting them here is what keeps this
/// scan consistent with the sp00ky ingest payload, which already drops them: a
/// row loaded by bootstrap and the same row loaded by an ingest event must have
/// the same key set, or the circuit's content hash diverges from the scheduler's
/// the moment either happens.
pub fn bootstrap_page_query(
    table: &str,
    page_size: usize,
    after_id: Option<&str>,
    omit: &BTreeSet<String>,
) -> String {
    let omit = ssp_protocol::omit_clause(omit);
    match after_id {
        None => format!("SELECT *{omit} FROM {table} ORDER BY id LIMIT {page_size}"),
        Some(id) => {
            let after = ssp_protocol::record_id_literal(table, id);
            format!("SELECT *{omit} FROM {table} WHERE id > {after} ORDER BY id LIMIT {page_size}")
        }
    }
}

/// Upstream's schema as the circuit needs it.
pub struct LoadedSchema {
    /// The probe the metadata was read under. `None` when upstream answered
    /// without a `tables` object: a database no migration has touched yet.
    pub probe: Option<SchemaProbe>,
    /// Every synced table's metadata.
    pub tables: BTreeMap<String, TableMeta>,
    /// `_00_version` exists upstream, so row versions are durable.
    pub has_versions: bool,
}

/// Read upstream's schema probe ([`INFO_FOR_DB`] plus the CLI's
/// `_00_schema_state` rows). Cheap: two small reads, no `INFO FOR TABLE`.
pub async fn probe_schema(db: &dyn Db) -> anyhow::Result<Option<SchemaProbe>> {
    Ok(read_probe(db).await?.0)
}

pub(crate) async fn read_probe(db: &dyn Db) -> anyhow::Result<(Option<SchemaProbe>, bool)> {
    // One round trip: `_00_schema_state` is only read when it exists, because
    // a statement on a missing table fails the whole request on SurrealDB 3.
    let batched = format!(
        "LET $spky_info = {INFO_FOR_DB}; RETURN $spky_info; \
         IF $spky_info.tables._00_schema_state {{ {SCHEMA_STATE_QUERY} }} ELSE {{ [] }};"
    );
    if let Ok(answers) = db.query(&batched, &[]).await {
        if let (Some(info), Some(state)) = (answers.get(1), answers.get(2)) {
            let has_versions = info.get("tables").and_then(|v| v.get("_00_version")).is_some();
            return Ok((SchemaProbe::parse(info, state), has_versions));
        }
    }
    let info = q1(db, INFO_FOR_DB).await.context("INFO FOR DB")?;
    let has_versions = info.get("tables").and_then(|v| v.get("_00_version")).is_some();
    // Absent until the CLI first applies its schema: that is "no rows".
    let state = q1(db, SCHEMA_STATE_QUERY).await.unwrap_or(Value::Null);
    Ok((SchemaProbe::parse(&info, &state), has_versions))
}

/// One table's metadata: the select permission out of its `DEFINE TABLE`
/// string, link targets, opaque fields and columns out of `INFO FOR TABLE`.
/// A failed `INFO FOR TABLE` keeps the permission and leaves the rest empty
/// rather than failing the load: a missing link map degrades reverse-link
/// edges, it does not corrupt row content.
pub async fn load_table_meta(db: &dyn Db, table: &str, define: &str) -> TableMeta {
    match q1(db, &format!("INFO FOR TABLE {}", table)).await {
        Ok(info) => table_meta_from_info(define, &info),
        Err(e) => {
            warn!(target: "ssp::policy", table = %table, error = %e, "INFO FOR TABLE failed; skipping link map");
            TableMeta { permission: extract_select_permission_text(define), ..TableMeta::default() }
        }
    }
}

/// [`load_table_meta`] from an `INFO FOR TABLE` answer already in hand.
fn table_meta_from_info(define: &str, info: &Value) -> TableMeta {
    let permission = extract_select_permission_text(define);
    let link_targets = info
        .get("fields")
        .and_then(|f| f.as_object())
        .map(|fields| {
            fields
                .iter()
                .filter_map(|(name, def)| {
                    def.as_str()
                        .and_then(parse_link_target)
                        .map(|target| (name.clone(), target))
                })
                .collect()
        })
        .unwrap_or_default();
    TableMeta {
        permission,
        link_targets,
        opaque: ssp_protocol::opaque_fields_from_info(info),
        columns: ssp_protocol::columns_from_info(info),
        indexes: indexes_from_info(info),
    }
}

/// The table's `DEFINE INDEX` statements the circuit can plan over (see
/// [`ssp::circuit::index`]), sorted by name so an unchanged schema reads back
/// equal.
pub fn indexes_from_info(info: &serde_json::Value) -> Vec<ssp::circuit::index::IndexDef> {
    let mut defs: Vec<_> = info
        .get("indexes")
        .and_then(|i| i.as_object())
        .map(|indexes| {
            indexes
                .values()
                .filter_map(|define| define.as_str().and_then(ssp::circuit::index::parse_define_index))
                .collect()
        })
        .unwrap_or_default();
    defs.sort();
    defs
}

/// Probe upstream and read every synced table's metadata. The one loader
/// behind the cold rebuild, the snapshot catch-up, the cluster bootstrap and
/// the live schema refresh, so all four hold the same view of a table.
pub async fn load_schema(db: &dyn Db) -> anyhow::Result<LoadedSchema> {
    let (probe, has_versions) = read_probe(db).await?;
    let mut tables = BTreeMap::new();
    if let Some(probe) = &probe {
        // Every table's `INFO FOR TABLE` in one round trip (a cold start used
        // to pay one per table, sequentially); one by one if the batch fails.
        let surql: String = probe.synced.keys().map(|t| format!("INFO FOR TABLE {t};")).collect();
        let answers = match surql.is_empty() {
            true => Vec::new(),
            false => db.query(&surql, &[]).await.unwrap_or_default(),
        };
        for (i, (table, define)) in probe.synced.iter().enumerate() {
            let meta = match answers.get(i).filter(|info| info.is_object()) {
                Some(info) => table_meta_from_info(define, info),
                None => load_table_meta(db, table, define).await,
            };
            tables.insert(table.clone(), meta);
        }
    }
    Ok(LoadedSchema {
        probe,
        tables,
        has_versions,
    })
}

/// Hand every table's metadata to the circuit, replacing what it held.
pub fn apply_schema(circuit: &mut Circuit, tables: &BTreeMap<String, TableMeta>) {
    for (table, meta) in tables {
        info!(target: "ssp::policy", table = %table, permission = %meta.permission, "registered table permission");
        for (field, target) in &meta.link_targets {
            info!(target: "ssp::policy", table = %table, field = %field, link_target = %target, "registered record-link target");
        }
        if !meta.opaque.is_empty() {
            info!(target: "ssp::policy", table = %table, fields = ?meta.opaque, "omitting opaque fields from row scans");
        }
        for index in &meta.indexes {
            info!(target: "ssp::policy", table = %table, index = %index.name, fields = ?index.fields, "mirrored index");
        }
        let builds = circuit.set_table_meta_timed(table, meta.clone());
        if builds.indexes_built > 0 {
            info!(table = %table, index_build_ms = builds.index_build_ms,
                indexes_built = builds.indexes_built, rows_indexed = builds.rows_indexed,
                "Prewarmed indexes for restored views");
        }
    }
}

/// Target table of a `DEFINE FIELD … TYPE record<X>` link (single, simple X).
pub fn parse_link_target(define_field: &str) -> Option<String> {
    let lower = define_field.to_lowercase();
    let rec_idx = lower.find("record<")?;
    let after = &define_field[rec_idx + "record<".len()..];
    let close = after.find('>')?;
    let inner = after[..close].trim();
    if inner.is_empty()
        || inner.contains('|')
        || !inner.chars().all(|c| c.is_alphanumeric() || c == '_')
    {
        return None;
    }
    Some(inner.to_string())
}

/// Pull `PERMISSIONS FOR select WHERE <expr>` text from a `DEFINE TABLE` string
/// (raw text so `prepare_registration_dbsp` routes it through the same
/// converter as user queries). FULL/absent → `"true"`; NONE/no-select → `"false"`.
pub fn extract_select_permission_text(define_table: &str) -> String {
    let def = define_table.trim().trim_end_matches(';');
    let upper = def.to_uppercase();
    let Some(perm_idx) = upper.find("PERMISSIONS") else {
        return "true".into();
    };
    let perm_section = def[perm_idx + "PERMISSIONS".len()..].trim();
    let perm_upper = perm_section.to_uppercase();
    if perm_upper.starts_with("FULL") {
        return "true".into();
    }
    if perm_upper.starts_with("NONE") {
        return "false".into();
    }

    let lower = perm_section.to_lowercase();
    let mut clause_starts: Vec<usize> = Vec::new();
    for (i, _) in lower.match_indices("for ") {
        if i == 0 || lower.as_bytes()[i - 1].is_ascii_whitespace() {
            clause_starts.push(i);
        }
    }
    if clause_starts.is_empty() {
        warn!(target: "ssp::policy", def = %def, "PERMISSIONS clause has no FOR clauses; denying");
        return "false".into();
    }
    for (idx, &start) in clause_starts.iter().enumerate() {
        let end = clause_starts.get(idx + 1).copied().unwrap_or(perm_section.len());
        let clause = &perm_section[start..end];
        let lower_clause = clause.to_lowercase();
        let where_idx = lower_clause.find("where");
        let header = match where_idx {
            Some(w) => &clause[..w],
            None => clause,
        };
        if !header.to_lowercase().contains("select") {
            continue;
        }
        let Some(w) = where_idx else {
            return "true".into();
        };
        let body = clause[w + "where".len()..]
            .trim()
            .trim_end_matches(',')
            .trim_end_matches(';')
            .trim()
            .to_string();
        if body.is_empty() {
            return "true".into();
        }
        return body;
    }
    "false".into()
}

/// First statement's result as a single flattened `Value`.
async fn q1(db: &dyn Db, surql: &str) -> anyhow::Result<Value> {
    Ok(db
        .query(surql, &[])
        .await
        .with_context(|| format!("query failed: {surql}"))?
        .into_iter()
        .next()
        .unwrap_or(Value::Null))
}

/// Full standalone rebuild: discover tables + permissions + link targets via
/// `INFO FOR DB`/`INFO FOR TABLE`, page every syncable table into the circuit,
/// re-register persisted views from `_00_query`, and reseed catch-up hashes.
/// Everything over the [`Db`] port — no net/fs/env.
pub async fn rebuild_from_db(
    db: &dyn Db,
    processor: &Arc<RwLock<Circuit>>,
    page_size: usize,
) -> anyhow::Result<()> {
    info!("Starting circuit rebuild from DB");

    // 1. Synced tables and their metadata (permissions, link targets, opaque
    //    fields, columns). Skips `_00_*` runtime tables and `@nosync` tables.
    let schema = load_schema(db).await?;
    let has_versions = schema.has_versions;
    info!(count = schema.tables.len(), "Discovered tables: {:?}", schema.tables.keys().collect::<Vec<_>>());
    apply_schema(&mut *processor.write().await, &schema.tables);

    // 2. Page each table's rows into the circuit store.
    for (table, meta) in &schema.tables {
        load_table_rows(db, processor, table, meta, page_size, has_versions).await?;
    }

    // 3. Re-register persisted views from _00_query.
    register_views_from_db(db, processor).await;

    // `Circuit::load` folds every row into the catch-up accumulators as it
    // goes, so nothing is re-seeded here. Debug builds prove it.
    #[cfg(debug_assertions)]
    {
        let mut circuit = processor.write().await;
        let maintained = circuit.compute_catchup_hashes();
        circuit.reseed_catchup_hashes();
        assert_eq!(
            maintained,
            circuit.compute_catchup_hashes(),
            "catch-up accumulators drifted from the rows during the rebuild"
        );
    }
    Ok(())
}

/// Bytes a bootstrap page aims at: enough rows to amortize the round trip,
/// few enough that a page of large rows stays far from a Durable Object's
/// 128 MB (1000 rows of a table holding file bodies overflowed it).
const PAGE_TARGET_BYTES: usize = 1 << 20;
/// Rows of the first page, before the table's row size is known: a `bytes`
/// field arrives as a JSON array of numbers, so one image row can be
/// megabytes.
const FIRST_PAGE_ROWS: usize = 10;
const MIN_PAGE_ROWS: usize = 1;

/// The next page's row count: what fits `PAGE_TARGET_BYTES` at the last
/// page's average row size, within `[MIN_PAGE_ROWS, max_rows]`.
fn next_page_rows(rows: usize, bytes: usize, max_rows: usize) -> usize {
    let per_row = (bytes / rows.max(1)).max(1);
    (PAGE_TARGET_BYTES / per_row).clamp(MIN_PAGE_ROWS, max_rows.max(MIN_PAGE_ROWS))
}

/// Page one table's rows from upstream into the circuit store. Pages are
/// sized by bytes, at most `page_size` rows (SurrealDB 3.1 rescans a table
/// from its first key for every keyset page, so small rows want few, large
/// pages); a page that times out is retried smaller.
pub(crate) async fn load_table_rows(
    db: &dyn Db,
    processor: &Arc<RwLock<Circuit>>,
    table: &str,
    meta: &TableMeta,
    page_size: usize,
    has_versions: bool,
) -> anyhow::Result<usize> {
    let omit = &meta.opaque;
    let mut record_count = 0usize;
    let mut after_id: Option<String> = None;
    let mut limit = FIRST_PAGE_ROWS.min(page_size.max(1));
    loop {
        let query = bootstrap_page_query(table, limit, after_id.as_deref(), omit);
        let query = if has_versions {
            ssp_protocol::with_durable_row_versions(&query)
        } else { query };
        let result = match q1(db, &query).await {
            Ok(result) => result,
            Err(e) if limit > MIN_PAGE_ROWS && format!("{e:#}").contains("timeout") => {
                limit = (limit / 4).max(MIN_PAGE_ROWS);
                warn!(table = %table, rows = limit, "page query timed out; retrying smaller pages");
                continue;
            }
            Err(e) => return Err(e).with_context(|| format!("page-query {table}")),
        };
        let rows: Vec<Value> = match result {
            Value::Array(arr) => arr,
            _ => vec![],
        };
        let n = rows.len();
        if n == 0 {
            break;
        }
        let bytes: usize = rows.iter().map(|row| row.to_string().len()).sum();
        let next_after = rows
            .last()
            .and_then(|row| row.get("id"))
            .and_then(|v| v.as_str())
            .map(str::to_string);
        let records: Vec<Record> = rows
            .into_iter()
            .filter_map(|row| {
                let id = row.get("id")?.as_str()?.to_string();
                Some(Record::new(table, &id, row))
            })
            .collect();
        processor.write().await.load(records);
        record_count += n;
        if n < limit {
            break;
        }
        limit = next_page_rows(n, bytes, page_size);
        match next_after {
            Some(id) => after_id = Some(id),
            None => break,
        }
    }
    info!(table = %table, records = record_count, "Loaded table data");
    Ok(record_count)
}

/// Re-register persisted views from `_00_query` and build the indexes they
/// plan over. A missing table (virgin DB, e.g. a fresh ephemeral host) means
/// "no persisted views", not a bootstrap failure.
pub(crate) async fn register_views_from_db(db: &dyn Db, processor: &Arc<RwLock<Circuit>>) {
    let views = match q1(db, "SELECT * FROM _00_query").await {
        Ok(Value::Array(arr)) => arr,
        Ok(_) => vec![],
        Err(e) => {
            warn!(error = %e, "read _00_query failed (no persisted views?) — skipping view re-registration");
            vec![]
        }
    };
    info!(count = views.len(), "Found persisted views in _00_query");
    for view_row in views {
        let mut circuit = processor.write().await;
        // Initial deltas are deferred here: the host republishes all restored
        // memberships before Ready, once the complete circuit has been loaded
        // and verified.
        register_persisted_view(&mut circuit, &view_row);
    }
    let builds = processor.read().await.prewarm_active_indexes();
    if builds.indexes_built > 0 {
        info!(index_build_ms = builds.index_build_ms, indexes_built = builds.indexes_built,
            rows_indexed = builds.rows_indexed, "Prewarmed remaining active indexes before Ready");
    }
}

/// The `_00_query` columns a persisted view is rebuilt from.
pub const PERSISTED_VIEW_FIELDS: &str = "id, surql, clientId, auth_id, ttl, lastActiveAt, params";

/// The key of a `_00_query` row (`abc` of `_00_query:abc`), the form the
/// circuit and `ViewDelta.query_id` use. `None` without an id.
pub fn persisted_view_key(view_row: &Value) -> Option<String> {
    let view_id = match view_row.get("id")? {
        Value::String(s) => s.clone(),
        v => v.to_string().trim_matches('"').to_string(),
    };
    Some(view_id.strip_prefix("_00_query:").unwrap_or(&view_id).to_string())
}

/// Register one persisted `_00_query` row into `circuit` the way a boot does:
/// prepared against the circuit's current schema, attached to an existing
/// graph when merging finds one computing the same thing, and with no
/// publication (the caller decides what, if anything, to publish). Returns
/// the canonical registration id, or `None` when the row is unusable or its
/// registration is refused (logged).
pub fn register_persisted_view(circuit: &mut Circuit, view_row: &Value) -> Option<String> {
    let raw_id = persisted_view_key(view_row)?;
    let Some(surql) = view_row.get("surql").and_then(|v| v.as_str()) else {
        warn!(view_id = %raw_id, "Skipping view with missing surql");
        return None;
    };
    let get = |k: &str| view_row.get(k).and_then(|v| v.as_str()).unwrap_or("").to_string();
    let auth_id = get("auth_id");
    let payload = json!({
        "id": raw_id,
        "surql": surql,
        "clientId": get("clientId"),
        "authId": auth_id,
        "ttl": view_row.get("ttl").and_then(|v| v.as_str()).unwrap_or("30m"),
        "lastActiveAt": get("lastActiveAt"),
        "params": view_row.get("params").cloned().unwrap_or(json!({})),
    });
    let data = match ssp::service::view::prepare_registration_dbsp(
        payload,
        circuit.permissions(),
        circuit.link_targets(),
        circuit.opaque_fields(),
    ) {
        Ok(data) => data,
        Err(e) => {
            warn!(target: "ssp::policy", view_id = %raw_id, error = %e, "Failed to re-register view");
            return None;
        }
    };
    let query_id = data.plan.id.clone();
    // Merge here as well as on the live path, or every restart un-merges the
    // whole tenant: these rows are exactly the registrations that were
    // sharing graphs before the restart, and rebuilding them one graph apiece
    // is the memory blowup merging exists to prevent.
    let owner = if circuit.merge_views() {
        circuit
            .owner_for_merge_key(&data.merge_key)
            .filter(|owner| *owner != data.plan.id)
            .map(|owner| owner.to_string())
    } else {
        None
    };
    match owner {
        Some(owner) => {
            circuit.attach_subscriber(&owner, data.plan.id.clone(), auth_id.clone());
            info!(view_id = %raw_id, auth_id = %auth_id, owner = %owner, "Re-registered view onto a shared graph");
        }
        None => {
            let merge_key = data.merge_key.clone();
            let (_, timings) = circuit.add_query_with_auth_timed(
                data.plan,
                data.safe_params,
                Some(OutputFormat::Streaming),
                auth_id.clone(),
            );
            if circuit.merge_views() {
                circuit.claim_merge_key(merge_key, query_id.clone());
            }
            info!(view_id = %raw_id, auth_id = %auth_id,
                plan_ms = timings.plan_ms, snapshot_ms = timings.snapshot_ms,
                index_build_ms = timings.index_build_ms, indexes_built = timings.indexes_built,
                rows_indexed = timings.rows_indexed, "Re-registered view");
        }
    }
    Some(ssp::canonical_query_id(&query_id))
}

/// Incremental catch-up after a snapshot restore: for each table load rows
/// whose `_00_rv` is newer than the snapshot's `max_row_version`.
/// A table without a tracked `max_row_version` catches up from `-1` (every row
/// carrying an `_00_rv`). Tables/rows without `_00_rv` are not caught here —
/// the staleness gate + the 503-during-bootstrap window bound that risk, and a
/// too-old snapshot rebuilds in full instead (see [`crate::Runtime::bootstrap`]).
pub async fn catch_up_from_db(
    db: &dyn Db,
    processor: &Arc<RwLock<Circuit>>,
    point: &crate::ports::ResumePoint,
) -> anyhow::Result<()> {
    // Re-read the schema: `Circuit::restore` drops all table metadata, and
    // the schema may have changed since the snapshot was written. A table
    // upstream no longer syncs is stepped out of the restored views and
    // forgotten, exactly as a cold rebuild would never have loaded it.
    let schema = load_schema(db).await.context("schema (catch-up)")?;
    {
        let mut circuit = processor.write().await;
        apply_schema(&mut circuit, &schema.tables);
        let gone: Vec<String> = circuit
            .table_names()
            .into_iter()
            .filter(|t| !ssp_protocol::table_excluded_from_sync(t) && !schema.tables.contains_key(t))
            .collect();
        for table in gone {
            let dropped = circuit.reconcile(&table, &[]).deleted;
            circuit.forget_table(&table);
            info!(table = %table, rows = dropped, "Catch-up: table no longer synced upstream; dropped");
        }
    }

    for (table, meta) in &schema.tables {
        let since = point.max_row_version.get(table).copied().unwrap_or(-1);
        // Must match `bootstrap_page_query`'s projection exactly: a row that
        // arrives via catch-up and the same row via a full rebuild have to carry
        // the same keys or the two produce different content hashes.
        let omit = ssp_protocol::omit_clause(&meta.opaque);
        let q = format!("SELECT *{omit} FROM {table} WHERE _00_rv > {since}");
        let rows: Vec<Value> = match q1(db, &q).await? {
            Value::Array(arr) => arr,
            _ => vec![],
        };
        if rows.is_empty() {
            continue;
        }
        let n = rows.len();
        // Replay through `step` (not bulk `load`) so the RESTORED views update:
        // a row absent from the snapshot is a Create (adds membership), one
        // already present is an Update (content-only, membership unchanged).
        let mut circuit = processor.write().await;
        let changes: Vec<Change> = rows
            .into_iter()
            .filter_map(|row| {
                let id = row.get("id")?.as_str()?.to_string();
                Some(if circuit.contains(table, &id) {
                    Change::update(table, &id, row)
                } else {
                    Change::create(table, &id, row)
                })
            })
            .collect();
        circuit.step(ChangeSet { changes });
        info!(table = %table, caught_up = n, since_rv = since, "Catch-up stepped rows");
    }

    // The restored store re-seeded its accumulators and `step` maintains
    // them, so nothing is re-seeded here.
    let builds = processor.read().await.prewarm_active_indexes();
    if builds.indexes_built > 0 {
        info!(index_build_ms = builds.index_build_ms, indexes_built = builds.indexes_built,
            rows_indexed = builds.rows_indexed, "Prewarmed remaining active indexes before Ready");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn persisted_sparse_view_warms_only_its_chosen_index_before_ready() {
        use ssp::circuit::index::IndexDef;

        let mut circuit = Circuit::new();
        circuit.set_table_meta("game", TableMeta {
            permission: "true".into(),
            indexes: vec![
                IndexDef { name: "game_database_sort".into(), fields: vec!["database".into(), "sort_index".into()] },
                IndexDef { name: "game_owner".into(), fields: vec!["owner".into()] },
            ],
            ..Default::default()
        });
        for i in 0..32 {
            circuit.load([Record::new("game", &format!("g{i:03}"), json!({
                "database": if i < 2 { "game_database:sparse" } else { "game_database:hot" },
                "sort_index": i, "owner": "user:a",
            }))]);
        }
        let id = register_persisted_view(&mut circuit, &json!({
            "id": "_00_query:sparse", "clientId": "tab", "auth_id": "user:a",
            "surql": "SELECT * FROM game WHERE database = $database ORDER BY sort_index LIMIT 50",
            "params": { "database": "game_database:sparse" },
        })).expect("persisted view registers");
        assert_eq!(id, "sparse");
        assert_eq!(circuit.view_keys(&id).len(), 2);
        assert_eq!(circuit.store.get_collection("game").unwrap().built_indexes(), vec!["game_database_sort"]);
        assert_eq!(circuit.prewarm_active_indexes(), ssp::circuit::IndexBuildStats::default(),
            "all selected indexes must already exist before the Ready transition");
    }

    #[test]
    fn page_query_uses_ordered_keyset_not_offset() {
        let none = BTreeSet::new();
        assert_eq!(
            bootstrap_page_query("game", 200, None, &none),
            "SELECT * FROM game ORDER BY id LIMIT 200"
        );
        let next = bootstrap_page_query("game", 200, Some("game:abc"), &none);
        assert_eq!(next, "SELECT * FROM game WHERE id > game:abc ORDER BY id LIMIT 200");
        assert!(!next.contains("START"));
        assert_eq!(
            bootstrap_page_query("game", 200, Some("game:u'0190d5d6-0000-7000-8000-000000000000'"), &none),
            "SELECT * FROM game WHERE id > game:u'0190d5d6-0000-7000-8000-000000000000' ORDER BY id LIMIT 200"
        );
    }

    #[test]
    fn page_query_omits_opaque_fields() {
        let omit: BTreeSet<String> = ["secret_token"].iter().map(|s| s.to_string()).collect();
        assert_eq!(
            bootstrap_page_query("user", 50, None, &omit),
            "SELECT * OMIT secret_token FROM user ORDER BY id LIMIT 50"
        );
    }

    #[test]
    fn permission_extraction() {
        assert_eq!(extract_select_permission_text("DEFINE TABLE t"), "true");
        assert_eq!(extract_select_permission_text("DEFINE TABLE t PERMISSIONS NONE"), "false");
        assert_eq!(extract_select_permission_text("DEFINE TABLE t PERMISSIONS FULL"), "true");
        assert_eq!(
            extract_select_permission_text("DEFINE TABLE t PERMISSIONS FOR select WHERE user = $auth.id"),
            "user = $auth.id"
        );
        assert_eq!(extract_select_permission_text("DEFINE TABLE t PERMISSIONS FOR select"), "true");
        assert_eq!(extract_select_permission_text("DEFINE TABLE t PERMISSIONS FOR update WHERE true"), "false");
    }

    #[test]
    fn link_target_parse() {
        assert_eq!(parse_link_target("DEFINE FIELD owner ON t TYPE record<user>"), Some("user".into()));
        assert_eq!(parse_link_target("DEFINE FIELD x ON t TYPE record<a | b>"), None);
        assert_eq!(parse_link_target("DEFINE FIELD x ON t TYPE string"), None);
    }
}

#[cfg(test)]
mod page_size_tests {
    use super::*;

    #[test]
    fn pages_aim_at_a_megabyte_within_the_row_cap() {
        // 500-byte rows: the cap.
        assert_eq!(next_page_rows(100, 100 * 500, 1000), 1000);
        // 50 KB rows (file bodies): about 20 rows.
        assert_eq!(next_page_rows(100, 100 * 50_000, 1000), 20);
        // Rows over the target go one per page.
        assert_eq!(next_page_rows(10, 10 * 5_000_000, 1000), 1);
        assert_eq!(next_page_rows(0, 0, 1000), 1000);
    }
}
