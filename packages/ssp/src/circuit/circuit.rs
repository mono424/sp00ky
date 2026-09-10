use crate::algebra::{RowKey, ZSet};
use crate::circuit::graph::Graph;
use crate::circuit::store::{Change, ChangeSet, Operation, Record, Store};
use crate::circuit::view::{OutputFormat, View};
use crate::operator::{OperatorPlan, QueryPlan};
use crate::types::{make_key, raw_id, Sp00kyValue};
use std::collections::{BTreeMap, HashMap};
// Portable monotonic clock: std::time on native, performance.now() on wasm32
// (std::time::Instant panics there).
use web_time::Instant;

/// Per-phase processing-time breakdown for a single [`Circuit::step_timed`],
/// surfaced to the binding layer / DevTools. Pure instrumentation — it never
/// affects the delta result.
#[derive(Debug, Clone, Copy, Default)]
pub struct StepTimings {
    /// Milliseconds spent applying changes to the store + building table deltas.
    pub store_apply_ms: f64,
    /// Milliseconds spent stepping the affected queries' operator DAGs.
    pub circuit_step_ms: f64,
}

/// Per-phase timing for one query registration (`add_query_with_auth_timed`).
#[derive(Debug, Clone, Copy, Default)]
pub struct RegTimings {
    /// Milliseconds spent building the operator DAG + view + dependency map.
    pub plan_ms: f64,
    /// Milliseconds spent running the initial snapshot evaluation.
    pub snapshot_ms: f64,
}

/// Elapsed milliseconds since `start`.
fn ms_since(start: Instant) -> f64 {
    start.elapsed().as_secs_f64() * 1000.0
}

/// Operation type for a subquery record delta.
#[derive(Debug, Clone, PartialEq)]
pub enum SubqueryOp {
    Add,
    Update,
    Remove,
}

/// A single subquery record change to be reflected in `_00_list_ref`.
#[derive(Debug, Clone)]
pub struct SubqueryDeltaItem {
    /// The subquery record key (e.g., "comment:abc123").
    pub id: String,
    /// The parent data record key (e.g., "thread:xyz789").
    pub parent_key: String,
    /// The relationship alias (e.g., "comments").
    pub alias: String,
    /// The operation: add, update, or remove.
    pub op: SubqueryOp,
}

/// Output from a materialized view after a step.
#[derive(Debug, Clone)]
pub struct ViewDelta {
    pub query_id: String,
    /// Keys added to the view.
    pub additions: Vec<String>,
    /// Keys removed from the view.
    pub removals: Vec<String>,
    /// Keys whose content changed but remain in the view.
    pub updates: Vec<String>,
    /// All keys currently in the view (for flat/tree modes).
    pub records: Vec<String>,
    /// Hash of the current view state.
    pub result_hash: String,
    /// Subquery record changes (additions/updates/removals for child records).
    pub subquery_items: Vec<SubqueryDeltaItem>,
    /// Authenticated user that owns the originating registration, in
    /// record-id form (e.g. `"user:abc"`). Carried so the SSP server can
    /// route per-user `_00_list_ref_user_<id>` writes without an extra
    /// DB round-trip per delta. Empty when no `$auth.id` was present at
    /// registration time.
    pub auth_id: String,
    /// `true` when this delta is a FULL publish of the view's current
    /// membership (registration snapshot, `snapshot_delta`, subscriber
    /// attach) rather than an increment. The edge writer treats a full
    /// publish as authoritative: it replaces the row's existing edges and
    /// flips `_00_query.state` to `ready` in the same transaction.
    pub initial: bool,
}

/// The DBSP incremental computation circuit.
///
/// Maintains a set of base collections (tables) and registered queries.
/// When input changes arrive via `step()`, the circuit incrementally
/// updates all affected materialized views and returns their deltas.
/// A registration sharing another registration's operator graph.
///
/// Carries exactly the two things a `ViewDelta` needs to be re-pointed at a
/// different client: which `_00_query` row the edges hang off (`query_id`) and
/// which `_00_list_ref*` table they are written to (`auth_id`). Everything else
/// a delta holds is a property of the computation, which is shared by
/// definition, so nothing else needs to be per-subscriber.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Subscriber {
    pub query_id: String,
    pub auth_id: String,
}

pub struct Circuit {
    pub store: Store,
    /// One operator DAG per registered query.
    graphs: HashMap<String, Graph>,
    /// View output state per query.
    views: HashMap<String, View>,
    /// Routing: table_name → [query_id].
    dependency_map: HashMap<String, Vec<String>>,
    /// `merge_key` → the query id that OWNS the graph for that computation.
    ///
    /// Registrations whose `merge_key` matches an existing entry attach as
    /// subscribers instead of building a second identical DAG. See
    /// `crate::merge_key` for why the key is the plan plus the params it
    /// dereferences, and never the plan alone.
    merge_index: HashMap<String, String>,
    /// Owner query id → the OTHER query ids sharing its graph. The owner is not
    /// listed in its own vector; an absent or empty entry means "not shared",
    /// which is what keeps every path below a no-op while merging is disabled.
    subscribers: HashMap<String, Vec<Subscriber>>,
    /// Owners whose own `_00_query` row is gone but whose graph is still
    /// serving subscribers. They stop receiving deltas of their own; edges
    /// pointed at a deleted row would dangle.
    detached_owners: std::collections::HashSet<String>,
    /// Whether registrations computing the same thing may share one graph.
    ///
    /// Deliberately NOT part of the snapshot: it is configuration
    /// (`SPKY_SSP_MERGE_VIEWS`), not state, so a restore must not resurrect the
    /// setting a snapshot happened to be written under. The shell re-applies it
    /// after every circuit construction.
    merge_views: bool,
    /// Server-side row-version floor (`set_monotonic_row_versions`). When on,
    /// a Create/Update of a row already in the store whose body carries no
    /// `_00_rv`, or one not above the stored version, is stamped
    /// `max(stored, floor) + 1` before it lands — see `floor_row_version`.
    /// Configuration like `merge_views`: not snapshotted, re-applied by the
    /// shell after every construction. Off in the browser circuit, whose rows
    /// take their versions from the durable store and must round-trip as-is.
    monotonic_row_versions: bool,
    /// Highest `_00_rv` this circuit has seen or issued while the floor was
    /// on. `0` means not seeded yet; the first synthesis seeds it from
    /// `max_row_versions`, so rows restored from a snapshot count.
    rv_floor: i64,
    /// Versions the floor had to make up. Non-zero means the DB-side stamp
    /// (`_00_version`) is not keeping up; the shell surfaces it as telemetry.
    synthesized_row_versions: u64,
    /// Of those, the ones whose incoming body carried NO `_00_rv` at all
    /// (as opposed to one that simply did not advance). Only the first kind
    /// implicates the DB-side stamp — and therefore the `_00_version` index —
    /// so the two are counted apart rather than pooled into one number that
    /// can only be reported as a guess.
    synthesized_row_versions_missing: u64,
    /// Per-table raw `PERMISSIONS FOR select WHERE <expr>` text, loaded from
    /// SurrealDB at boot. The registration pipeline routes each scan's
    /// permission through the same converter that handles user queries and
    /// AND-folds the result into the scan's filter.
    permissions: HashMap<String, String>,
    /// Per-table record-link field map: `table -> (field -> target_table)`,
    /// loaded from `INFO FOR TABLE` at boot (`DEFINE FIELD ... TYPE record<X>`).
    /// Lets the converter lower a link-traversal permission
    /// (`assigned_to.owner.id = $auth.id`) into a `SemiJoin` — the target table
    /// isn't derivable from the field name. See `converter::LinkMap`.
    link_targets: HashMap<String, HashMap<String, String>>,
    /// Per-table fields whose value this circuit deliberately does NOT hold:
    /// `table -> {field}`, from the `sp00ky:opaque` marker on `DEFINE FIELD`
    /// (`-- @nosync` / `-- @crdt` / `-- @opaque`). Loaded at boot from the same
    /// `INFO FOR TABLE` pass that fills `link_targets`.
    ///
    /// Used to REJECT a registration that tries to filter, order, or join on one
    /// of these fields. Without the check the query registers happily and then
    /// matches nothing, because `resolve_field` returns `None` for the absent key
    /// and the comparison silently evaluates false.
    opaque_fields: HashMap<String, std::collections::BTreeSet<String>>,
    /// Whether base collections keep only the fields registered plans
    /// evaluate (see `Collection::retained`). Off on the server, where the
    /// whole body is the product; on for a browser circuit, which renders from
    /// its own durable store and only needs what predicates, join keys and
    /// sort keys read. Configuration, not state: not part of the snapshot.
    projection: bool,
    /// Fields the most recent registration evaluates that rows ALREADY in the
    /// store were projected without. The caller widens those rows (an
    /// `Operation::Merge` per row with just these fields) or the new view
    /// silently matches nothing on them. Drained by
    /// [`Self::take_missing_fields`].
    missing_fields: BTreeMap<String, std::collections::BTreeSet<String>>,
}

/// What [`Circuit::reconcile`] found when a table's rows were compared against
/// the caller's authoritative `(id, _00_rv)` list.
#[derive(Debug, Default)]
pub struct Reconciled {
    /// Ids (as the caller spelled them) whose row is absent from the store or
    /// stored at a lower `_00_rv`; the caller ingests their bodies.
    pub fetch: Vec<String>,
    /// Rows the store held that the caller's list did not: already stepped
    /// out as deletes.
    pub deleted: usize,
    /// View deltas produced by those deletes.
    pub deltas: Vec<ViewDelta>,
}

/// Compute the full set of subquery records visible through the current view.
///
/// Returns: child_key → (parent_key, alias)
/// This operates as a side-channel alongside the main Z-set pipeline.
fn compute_current_subquery_set(
    store: &Store,
    view: &View,
) -> HashMap<RowKey, (RowKey, String)> {
    let mut result = HashMap::new();

    let subquery_infos = view.plan.root.subquery_projection_info();

    // Pass 1: Root-level subqueries (parent_table = None) — parent is in view.cache
    for (alias, subquery_table, parent_key_opt, parent_table) in &subquery_infos {
        if parent_table.is_some() {
            continue;
        }
        let parent_key = match parent_key_opt {
            Some(pk) => pk,
            None => continue,
        };

        let collection = match store.get_collection(subquery_table) {
            Some(c) => c,
            None => continue,
        };

        // Reverse one-to-one: predicate is `child.id = $parent.<fk>`, so the
        // parent stores the child's id. Iterate parents in the view and follow
        // the FK to the child — the forward iteration below would never match
        // because user.id ("user:xxx") isn't a key in a parent (e.g. thread) cache.
        if parent_key.child_field == "id" {
            for parent_full_key in view.cache.keys() {
                let parent_row = store.get_row_by_key(parent_full_key);
                let child_full_key = match parent_row.get(&parent_key.parent_field).as_str() {
                    Some(v) => v,
                    None => continue,
                };
                let child_raw = raw_id(child_full_key);
                if collection.rows.contains_key(child_raw) {
                    let child_key = make_key(subquery_table, child_raw);
                    result.insert(child_key, (parent_full_key.clone(), alias.clone()));
                }
            }
            continue;
        }

        for (child_raw_id, row_data) in collection.rows.iter() {
            let fk_value = match row_data.get(&parent_key.child_field).as_str() {
                Some(v) => v,
                None => continue,
            };

            if view.cache.contains_key(fk_value) {
                let child_key = make_key(subquery_table, child_raw_id);
                result.insert(child_key, (fk_value.into(), alias.clone()));
            }
        }
    }

    // Pass 2: Nested subqueries (parent_table = Some) — parent is a subquery item
    for (alias, subquery_table, parent_key_opt, parent_table_opt) in &subquery_infos {
        let pt = match parent_table_opt {
            Some(pt) => pt,
            None => continue,
        };
        let parent_key = match parent_key_opt {
            Some(pk) => pk,
            None => continue,
        };

        let collection = match store.get_collection(subquery_table) {
            Some(c) => c,
            None => continue,
        };
        let parent_coll = match store.get_collection(pt) {
            Some(c) => c,
            None => continue,
        };

        // Build index: parent's parent_field value → parent full key
        // Only for parent rows already in the result set (level-1 items)
        let mut parent_field_index: HashMap<String, RowKey> = HashMap::new();
        for (parent_raw_id, parent_row_data) in parent_coll.rows.iter() {
            let parent_full_key = make_key(pt, parent_raw_id);
            if result.contains_key(&parent_full_key) {
                if let Some(val) = parent_row_data.get(&parent_key.parent_field).as_str() {
                    parent_field_index.insert(val.to_string(), parent_full_key);
                }
            }
        }

        // For each child row, check if child.child_field matches a parent's field value
        for (child_raw_id, row_data) in collection.rows.iter() {
            let child_value = match row_data.get(&parent_key.child_field).as_str() {
                Some(v) => v,
                None => continue,
            };
            if let Some(parent_full_key) = parent_field_index.get(child_value) {
                let child_key = make_key(subquery_table, child_raw_id);
                result.insert(child_key, (parent_full_key.clone(), alias.clone()));
            }
        }
    }

    result
}

/// Diff two subquery sets and produce delta items.
fn diff_subquery_sets(
    old: &HashMap<RowKey, (RowKey, String)>,
    new: &HashMap<RowKey, (RowKey, String)>,
    store: &Store,
) -> Vec<SubqueryDeltaItem> {
    let mut items = Vec::new();

    // Additions: in new but not old
    for (key, (parent_key, alias)) in new {
        if !old.contains_key(key) {
            items.push(SubqueryDeltaItem {
                id: key.to_string(),
                parent_key: parent_key.to_string(),
                alias: alias.clone(),
                op: SubqueryOp::Add,
            });
        }
    }

    // Removals: in old but not new
    for (key, (parent_key, alias)) in old {
        if !new.contains_key(key) {
            items.push(SubqueryDeltaItem {
                id: key.to_string(),
                parent_key: parent_key.to_string(),
                alias: alias.clone(),
                op: SubqueryOp::Remove,
            });
        }
    }

    // Updates: in both, check if version changed
    for (key, (parent_key, alias)) in new {
        if old.contains_key(key) {
            // Check if the record version changed
            let old_version = store.get_record_version_by_key(key);
            // We always emit an update for records that exist in both sets
            // when there's any change to subquery tables (the caller determines when to recompute)
            if old_version.is_some() {
                items.push(SubqueryDeltaItem {
                    id: key.to_string(),
                    parent_key: parent_key.to_string(),
                    alias: alias.clone(),
                    op: SubqueryOp::Update,
                });
            }
        }
    }

    items
}

impl Circuit {
    /// Create an empty circuit.
    pub fn new() -> Self {
        Self {
            store: Store::new(),
            graphs: HashMap::new(),
            views: HashMap::new(),
            dependency_map: HashMap::new(),
            merge_index: HashMap::new(),
            subscribers: HashMap::new(),
            detached_owners: std::collections::HashSet::new(),
            merge_views: false,
            monotonic_row_versions: false,
            rv_floor: 0,
            synthesized_row_versions: 0,
            synthesized_row_versions_missing: 0,
            permissions: HashMap::new(),
            link_targets: HashMap::new(),
            opaque_fields: HashMap::new(),
            projection: false,
            missing_fields: BTreeMap::new(),
        }
    }

    /// Read-only access to the per-table permission text (for query rewriting).
    pub fn permissions(&self) -> &HashMap<String, String> {
        &self.permissions
    }

    /// Register a table's raw `PERMISSIONS FOR select WHERE <expr>` text.
    /// Called once per table at boot time after `INFO FOR DB` is parsed.
    pub fn set_permission(&mut self, table: impl Into<String>, where_text: impl Into<String>) {
        self.permissions.insert(table.into(), where_text.into());
    }

    /// Read-only access to the per-table record-link field map (for lowering
    /// link-traversal permission predicates to semi-joins).
    pub fn link_targets(&self) -> &HashMap<String, HashMap<String, String>> {
        &self.link_targets
    }

    /// Register that `table.field` is a record link to `target` table. Called
    /// once per record-typed field at boot time after `INFO FOR TABLE` is parsed.
    pub fn set_link_target(
        &mut self,
        table: impl Into<String>,
        field: impl Into<String>,
        target: impl Into<String>,
    ) {
        self.link_targets
            .entry(table.into())
            .or_default()
            .insert(field.into(), target.into());
    }

    /// Read-only access to the per-table opaque-field map (for rejecting a
    /// registration that would evaluate a field this circuit does not hold).
    pub fn opaque_fields(&self) -> &HashMap<String, std::collections::BTreeSet<String>> {
        &self.opaque_fields
    }

    /// Register the set of opaque fields on `table`, from the `sp00ky:opaque`
    /// markers in its `INFO FOR TABLE` output. Replaces any previous set so a
    /// rediscover after a schema change cannot leave a stale entry behind.
    pub fn set_opaque_fields(
        &mut self,
        table: impl Into<String>,
        fields: std::collections::BTreeSet<String>,
    ) {
        self.opaque_fields.insert(table.into(), fields);
    }

    /// Bulk-load initial data into base collections.
    pub fn load(&mut self, records: impl IntoIterator<Item = Record>) {
        for record in records {
            let coll = self.store.ensure_collection(&record.table);
            let key = make_key(&record.table, &record.id);
            let normalized = crate::types::raw_id(&record.id);
            // Maintain the catch-up XOR accumulator for this fresh insert. `load`
            // is initial bulk data (fresh collections), so there is no prior
            // value to XOR out — same chokepoint guarantee as `apply_mutation`.
            // Digested straight off the value rather than through a throwaway
            // `serde_json::Value` clone, which on a bootstrap meant one full
            // extra copy of every row in the database.
            // One canonicalization, whose digest is then handed straight to
            // the row store to be written into the record header.
            let digest = coll.digest_for(normalized, &record.data);
            ssp_protocol::snapshot_hash::xor_digest(&mut coll.catchup_xor, &digest);
            coll.rows.insert(normalized, &record.data, &digest);
            coll.zset.insert(key, 1);
        }
    }

    /// Register a query. Builds the operator DAG, runs initial evaluation,
    /// and returns the first ViewDelta (if data exists). The owning user
    /// id is left empty; callers that need per-user table routing in
    /// `RefMode::Dedicated` should use [`add_query_with_auth`] instead.
    pub fn add_query(
        &mut self,
        plan: QueryPlan,
        params: Option<serde_json::Value>,
        format: Option<OutputFormat>,
    ) -> Option<ViewDelta> {
        self.add_query_with_auth(plan, params, format, String::new())
    }

    /// Like [`add_query`] but stashes `auth_id` on the resulting `View`
    /// so subsequent `step()` deltas know their owning user, which the
    /// SSP server uses to route `_00_list_ref_user_<id>` writes without
    /// an extra DB lookup per delta.
    pub fn add_query_with_auth(
        &mut self,
        plan: QueryPlan,
        params: Option<serde_json::Value>,
        format: Option<OutputFormat>,
        auth_id: String,
    ) -> Option<ViewDelta> {
        self.add_query_with_auth_timed(plan, params, format, auth_id)
            .0
    }

    /// Like [`add_query_with_auth`] but returns a per-phase timing breakdown
    /// (plan build vs initial snapshot) for the binding layer / DevTools. The
    /// delta result is identical to `add_query_with_auth`.
    pub fn add_query_with_auth_timed(
        &mut self,
        plan: QueryPlan,
        params: Option<serde_json::Value>,
        format: Option<OutputFormat>,
        auth_id: String,
    ) -> (Option<ViewDelta>, RegTimings) {
        let mut timings = RegTimings::default();

        let t_plan = Instant::now();
        // Normalise here rather than trusting callers: registrations arrive
        // spelled `_00_query:<hash>` from a live client and `<hash>` from every
        // DB-derived path, and one query under two keys means two graphs. See
        // [`crate::canonical_query_id`]. The plan carries the canonical id
        // onward so `ViewDelta.query_id` and the snapshot agree with the map.
        let mut plan = plan;
        plan.id = crate::canonical_query_id(&plan.id);
        let query_id = plan.id.clone();
        let referenced_tables = plan.root.referenced_tables();
        let format = format.unwrap_or_default();
        let params_sv = params.map(Sp00kyValue::from);

        // Build the operator DAG
        let graph = Graph::from_plan(&plan.root);

        // Create view state
        let view = View::new(
            query_id.clone(),
            plan.clone(),
            format,
            params_sv,
            referenced_tables.clone(),
            auth_id,
        );

        // Projection bookkeeping BEFORE the graph exists: the fields this plan
        // evaluates that stored rows were projected without are exactly what
        // the initial snapshot below cannot see.
        self.note_missing_fields(&plan.root);

        self.graphs.insert(query_id.clone(), graph);
        self.views.insert(query_id.clone(), view);

        // Update dependency map
        for table in &referenced_tables {
            self.dependency_map
                .entry(table.clone())
                .or_default()
                .push(query_id.clone());
        }
        self.refresh_retained_fields();
        timings.plan_ms = ms_since(t_plan);

        // Run initial snapshot evaluation
        let t_snapshot = Instant::now();
        let delta = self.run_initial_snapshot(&query_id);
        timings.snapshot_ms = ms_since(t_snapshot);

        (delta, timings)
    }

    /// Remove a registered query.
    pub fn remove_query(&mut self, query_id: &str) {
        // Callers reach this from both spellings — `/view/unregister` carries
        // whatever the client sent, the TTL sweep carries the bare key.
        let query_id = &crate::canonical_query_id(query_id);
        self.graphs.remove(query_id);
        self.views.remove(query_id);

        // Clean up dependency map
        for (_, query_ids) in self.dependency_map.iter_mut() {
            query_ids.retain(|id| id != query_id);
        }
        self.dependency_map.retain(|_, ids| !ids.is_empty());
    }

    /// Advance the circuit by one time step.
    pub fn step(&mut self, changes: ChangeSet) -> Vec<ViewDelta> {
        self.step_timed(changes).0
    }

    /// Like [`step`] but also returns a per-phase processing-time breakdown
    /// (store-apply vs circuit-step) so the binding layer can surface it to
    /// DevTools. The delta result is identical to `step`.
    pub fn step_timed(&mut self, changes: ChangeSet) -> (Vec<ViewDelta>, StepTimings) {
        let mut timings = StepTimings::default();
        if changes.changes.is_empty() {
            return (vec![], timings);
        }

        // Phase 1: Apply changes to store and build per-table deltas
        let t_store = Instant::now();
        let mut table_deltas: HashMap<String, ZSet> = HashMap::new();
        let mut changed_tables: Vec<String> = Vec::new();
        // Track content-only updates (Operation::Update has weight 0)
        let mut content_updates: HashMap<String, Vec<RowKey>> = HashMap::new();

        // Consumed by value so the row body moves into the store instead of
        // being deep-copied on the way in. `changes` is not read after this
        // loop.
        for change in changes.changes {
            let Change { table, op, id, data } = change;
            // Capture a row about to be deleted so Filter/Scan predicate
            // evaluation can still read it this step — otherwise the `-1`
            // retraction is tested against a missing row, the predicate fails,
            // and a deleted row lingers in every filtered view. The overlay is
            // cleared after stepping (below); `apply_change` still removes the
            // row from the collection so the subquery/join path sees it gone.
            if op == Operation::Delete {
                self.store.stage_deleted_row(&table, &id);
            }
            let data = if self.monotonic_row_versions
                && matches!(op, Operation::Create | Operation::Update)
            {
                self.floor_row_version(&table, &id, data)
            } else {
                data
            };
            let applied = self.store.apply_owned(&table, op, &id, data);
            // A write whose digest matched what was stored did nothing: no
            // delta, no content update, and the table is not even "changed".
            if !applied.content_changed {
                continue;
            }
            if applied.weight != 0 {
                let delta = table_deltas.entry(table.clone()).or_default();
                *delta.entry(applied.key).or_insert(0) += applied.weight;
            } else {
                // Content changed, membership did not: the store decided that
                // from presence, not from the verb, so a Create that lands on
                // an existing row is tracked here exactly like an Update.
                content_updates
                    .entry(table.clone())
                    .or_default()
                    .push(applied.key);
            }
            if !changed_tables.contains(&table) {
                changed_tables.push(table);
            }
        }

        // Clean up zero weights in deltas
        for delta in table_deltas.values_mut() {
            delta.retain(|_, w| *w != 0);
        }
        timings.store_apply_ms = ms_since(t_store);

        // Phase 2: Determine affected queries
        let mut affected_queries: Vec<String> = Vec::new();
        for table in &changed_tables {
            if let Some(query_ids) = self.dependency_map.get(table) {
                for qid in query_ids {
                    if !affected_queries.contains(qid) {
                        affected_queries.push(qid.clone());
                    }
                }
            }
        }

        // Phase 3: Step each affected query's DAG
        let t_step = Instant::now();
        let mut results = Vec::new();
        for query_id in affected_queries {
            if let Some(delta) = self.step_query(&query_id, &table_deltas, &content_updates) {
                // One computation, one step, then one delta per subscriber.
                // With no subscribers this is `vec![delta]`, so the unmerged
                // path is byte-identical to before.
                results.extend(self.fan_out(delta));
            }
        }
        timings.circuit_step_ms = ms_since(t_step);

        // Drop the per-step deleted-row overlay now that every affected query
        // has stepped (and could read the staged rows for retraction predicates).
        self.store.clear_pending_deleted_rows();

        (results, timings)
    }

    /// Get a reference to a view's state.
    pub fn get_view(&self, query_id: &str) -> Option<&View> {
        self.views
            .get(query_id)
            .or_else(|| self.views.get(&crate::canonical_query_id(query_id)))
    }

    /// Run initial evaluation for a newly registered query.
    ///
    /// Uses `step()` so that stateful operators (TopK, Join, Aggregate,
    /// Distinct) prime their internal buffers. For Scan nodes, the full
    /// collection Z-set is injected as the initial "delta from empty".
    fn run_initial_snapshot(&mut self, query_id: &str) -> Option<ViewDelta> {
        let graph = self.graphs.get_mut(query_id)?;
        let view = self.views.get_mut(query_id)?;

        let num_nodes = graph.node_count();
        let mut node_outputs: Vec<Option<ZSet>> = vec![None; num_nodes];

        let topo_order: Vec<usize> = graph.topo_order().to_vec();

        for &node_id in &topo_order {
            let input_ids = graph.nodes[node_id].inputs.clone();
            let arity = graph.nodes[node_id].operator.arity();

            let output = if arity == 0 {
                // Scan node: inject the full collection as initial delta
                let table_name = graph.nodes[node_id].operator.collections();
                let full_zset = table_name
                    .first()
                    .and_then(|t| self.store.get_collection(t))
                    .map(|c| c.zset.clone())
                    .unwrap_or_default();
                graph.nodes[node_id]
                    .operator
                    .step(&[&full_zset], &self.store, view.params.as_ref())
            } else {
                let inputs: Vec<&ZSet> = input_ids
                    .iter()
                    .map(|&input_id| node_outputs[input_id].as_ref().unwrap())
                    .collect();
                graph.nodes[node_id]
                    .operator
                    .step(&inputs, &self.store, view.params.as_ref())
            };

            node_outputs[node_id] = Some(output);
        }

        let view_output = node_outputs[graph.output_node].take()?;

        if view_output.is_empty() {
            return None;
        }

        // Apply to view cache
        let additions: Vec<String> = view_output
            .iter()
            .filter(|(_, &w)| w > 0)
            .map(|(k, _)| k.to_string())
            .collect();

        view.apply_delta(&view_output);
        view.last_hash = view.compute_hash();

        // Compute initial subquery record set
        let new_subquery_set = compute_current_subquery_set(&self.store, view);
        let subquery_items: Vec<SubqueryDeltaItem> = new_subquery_set
            .iter()
            .map(|(key, (parent_key, alias))| SubqueryDeltaItem {
                id: key.to_string(),
                parent_key: parent_key.to_string(),
                alias: alias.clone(),
                op: SubqueryOp::Add,
            })
            .collect();
        view.subquery_cache = new_subquery_set;

        let records: Vec<String> = view.cache.keys().map(|k| k.to_string()).collect();

        Some(ViewDelta {
            query_id: query_id.to_string(),
            additions,
            removals: vec![],
            updates: vec![],
            records,
            result_hash: view.last_hash.clone(),
            subquery_items,
            auth_id: view.auth_id.clone(),
            initial: true,
        })
    }

    /// Step a single query's operator DAG with table deltas.
    fn step_query(
        &mut self,
        query_id: &str,
        table_deltas: &HashMap<String, ZSet>,
        content_updates: &HashMap<String, Vec<RowKey>>,
    ) -> Option<ViewDelta> {
        let graph = self.graphs.get_mut(query_id)?;
        let view = self.views.get_mut(query_id)?;

        let num_nodes = graph.node_count();
        let mut node_outputs: Vec<Option<ZSet>> = vec![None; num_nodes];
        let empty_delta: ZSet = HashMap::new();

        // Clone topo order to avoid holding an immutable borrow on graph
        // while we mutably access graph.nodes[..].operator.step()
        let topo_order: Vec<usize> = graph.topo_order().to_vec();

        for &node_id in &topo_order {
            let input_ids = graph.nodes[node_id].inputs.clone();
            let arity = graph.nodes[node_id].operator.arity();

            let output = if arity == 0 {
                // Scan node: inject the table delta
                let table_name = graph.nodes[node_id].operator.collections()[0].clone();
                let delta = table_deltas.get(&table_name).unwrap_or(&empty_delta);
                graph.nodes[node_id]
                    .operator
                    .step(&[delta], &self.store, view.params.as_ref())
            } else {
                let inputs: Vec<&ZSet> = input_ids
                    .iter()
                    .map(|&input_id| node_outputs[input_id].as_ref().unwrap())
                    .collect();
                graph.nodes[node_id]
                    .operator
                    .step(&inputs, &self.store, view.params.as_ref())
            };

            node_outputs[node_id] = Some(output);
        }

        let mut view_delta = node_outputs[graph.output_node].take()?;

        // Membership re-evaluation pass for Operation::Update.
        //
        // Operation::Update has weight 0, so the Scan operator emits
        // an empty delta for the table and the Filter never sees the
        // updated row. That silently breaks cross-user realtime sync:
        // when alice publishes a thread, bob's view's permission
        // filter would NOW admit it, but the empty Scan delta means
        // no addition propagates. Symmetrically, an unpublish leaves
        // a stale row in bob's view.cache.
        //
        // Fix: for each content_update key, ask the output operator
        // "does this key satisfy the view right now?". If yes and not
        // in cache → synthesize +1 (will surface as `additions`). If
        // no and in cache → synthesize a weight that zeroes the cache
        // entry (surfaces as `removals`). The existing additions /
        // removals / updates classification below picks these up
        // naturally.
        for (_, keys) in content_updates {
            for key in keys {
                // Walk the DAG in topo order, computing evaluate_key
                // per node so the output's result reflects whether the
                // row's NEW content satisfies the view right now.
                let mut node_evals: Vec<bool> = vec![false; num_nodes];
                let mut reordered: Option<(usize, ZSet)> = None;
                for &node_id in &topo_order {
                    let input_ids = graph.nodes[node_id].inputs.clone();
                    let input_evals: Vec<bool> =
                        input_ids.iter().map(|&i| node_evals[i]).collect();
                    // An order-sensitive operator re-places the key itself:
                    // its answer is a delta, not a bool, and it supersedes the
                    // synthesized +1/-1 below for this key.
                    if reordered.is_none() {
                        let upstream_now = input_evals.first().copied().unwrap_or(true);
                        if let Some(delta) = graph.nodes[node_id].operator.reorder_key(
                            key,
                            upstream_now,
                            &self.store,
                            view.params.as_ref(),
                        ) {
                            reordered = Some((node_id, delta));
                        }
                    }
                    node_evals[node_id] = graph.nodes[node_id].operator.evaluate_key(
                        key,
                        &input_evals,
                        &self.store,
                        view.params.as_ref(),
                    );
                }
                if let Some((from, delta)) = reordered {
                    // Push the re-placement through whatever sits downstream
                    // of the reordering node (a Project, typically) so the
                    // view sees it in output terms.
                    let mut outputs: Vec<Option<ZSet>> = vec![None; num_nodes];
                    outputs[from] = Some(delta);
                    let after = topo_order.iter().position(|&n| n == from).unwrap_or(0);
                    for &node_id in &topo_order[after + 1..] {
                        let input_ids = graph.nodes[node_id].inputs.clone();
                        if !input_ids.iter().any(|&i| outputs[i].is_some()) {
                            continue;
                        }
                        let inputs: Vec<&ZSet> = input_ids
                            .iter()
                            .map(|&i| outputs[i].as_ref().unwrap_or(&empty_delta))
                            .collect();
                        let out = graph.nodes[node_id].operator.step(
                            &inputs,
                            &self.store,
                            view.params.as_ref(),
                        );
                        outputs[node_id] = Some(out);
                    }
                    if let Some(out) = outputs[graph.output_node].take() {
                        for (k, w) in out {
                            *view_delta.entry(k).or_insert(0) += w;
                        }
                        view_delta.retain(|_, w| *w != 0);
                    }
                    continue;
                }
                let now_matches = node_evals[graph.output_node];
                let prev_cached = view.cache.get(&**key).copied().unwrap_or(0);
                let in_cache = prev_cached > 0;
                if now_matches && !in_cache && !view_delta.contains_key(&**key) {
                    view_delta.insert(key.clone().into(), 1);
                } else if !now_matches && in_cache && !view_delta.contains_key(&**key) {
                    view_delta.insert(key.clone().into(), -prev_cached);
                }
            }
        }

        // Identify content-only updates: keys in the view cache whose data changed
        // but membership didn't (Operation::Update with weight 0).
        let mut updates: Vec<RowKey> = content_updates
            .iter()
            .flat_map(|(_, keys)| keys.iter())
            .filter(|key| {
                view.cache.contains_key(&**key) && !view_delta.contains_key(&**key)
            })
            .cloned()
            .collect();

        // Detect subquery table changes: if any table referenced in a subquery
        // projection had changes, all cached parent records need re-fetching.
        if !view.subquery_tables.is_empty() {
            let has_subquery_changes = view.subquery_tables.iter().any(|t| {
                table_deltas.contains_key(t) || content_updates.contains_key(t)
            });
            if has_subquery_changes {
                view.bump_content_generation();
                for key in view.cache.keys() {
                    if !updates.contains(key) {
                        updates.push(key.clone());
                    }
                }
            }
        }

        let has_membership_changes = !view_delta.is_empty();
        let has_content_updates = !updates.is_empty();

        if !has_membership_changes && !has_content_updates {
            return None;
        }

        // Categorize membership changes before applying
        let additions: Vec<String> = view_delta
            .iter()
            .filter(|(k, &w)| w > 0 && !view.cache.contains_key(*k))
            .map(|(k, _)| k.to_string())
            .collect();
        let removals: Vec<String> = view_delta
            .iter()
            .filter(|(k, &w)| {
                w < 0 && view.cache.get(*k).map(|&old| old + w <= 0).unwrap_or(false)
            })
            .map(|(k, _)| k.to_string())
            .collect();

        // Apply delta to view cache
        view.apply_delta(&view_delta);
        let new_hash = view.compute_hash();

        // For content-only updates, the hash won't change (keys unchanged),
        // but we still want to emit the delta so consumers know about data changes.
        if new_hash == view.last_hash && !has_content_updates {
            return None;
        }

        if new_hash != view.last_hash {
            view.last_hash = new_hash.clone();
        }

        // Compute subquery record diffs when relevant tables changed
        let has_subquery_table_changes = view.subquery_tables.iter().any(|t| {
            table_deltas.contains_key(t) || content_updates.contains_key(t)
        });
        let subquery_items = if has_membership_changes || has_subquery_table_changes {
            let new_subquery_set = compute_current_subquery_set(&self.store, view);
            let items = diff_subquery_sets(&view.subquery_cache, &new_subquery_set, &self.store);
            view.subquery_cache = new_subquery_set;
            items
        } else {
            vec![]
        };

        let records: Vec<String> = view.cache.keys().map(|k| k.to_string()).collect();

        Some(ViewDelta {
            query_id: query_id.to_string(),
            additions,
            removals,
            // `ViewDelta` is the wire shape, so shared keys become owned here
            // — at the one boundary that leaves the circuit, rather than
            // throughout it.
            updates: updates.iter().map(|k| k.to_string()).collect(),
            records,
            result_hash: view.last_hash.clone(),
            subquery_items,
            auth_id: view.auth_id.clone(),
            initial: false,
        })
    }
}

// --- Size reporting ---

/// Estimated heap footprint of one base table.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct TableSize {
    pub table: String,
    pub rows: usize,
    /// Row bodies, their ids, and the id index.
    pub rows_bytes: usize,
    /// The id index alone, broken out of `rows_bytes`.
    ///
    /// This is the floor: it is O(rows) and it is anonymous memory, so unlike
    /// the encoded bodies it cannot become reclaimable page cache no matter
    /// where those bodies live. Reported separately so it stays visible.
    pub index_bytes: usize,
    /// Membership Z-set. Separate from `rows_bytes` because its keys are a
    /// second `"table:id"` string per row on top of the raw id in `rows`.
    pub zset_bytes: usize,
    /// Row bytes referenced by a live slot.
    #[serde(default)]
    pub live_bytes: u64,
    /// Row bytes orphaned by updates and deletes, reclaimable by `compact`.
    #[serde(default)]
    pub dead_bytes: u64,
    /// Fields kept per row under projection; `None` when whole bodies are kept.
    #[serde(default)]
    pub retained_fields: Option<Vec<String>>,
}

impl TableSize {
    pub fn total_bytes(&self) -> usize {
        self.rows_bytes + self.zset_bytes
    }

    /// Estimated bytes per row. The headline ratio to watch: it should be a
    /// small multiple of the source JSON, and today it is not.
    pub fn bytes_per_row(&self) -> f64 {
        if self.rows == 0 {
            0.0
        } else {
            self.total_bytes() as f64 / self.rows as f64
        }
    }
}

/// Estimated heap footprint of one registered query.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ViewSize {
    pub query_id: String,
    pub auth_id: String,
    pub cached_records: usize,
    /// The view's own cache and params.
    pub view_bytes: usize,
    /// Z⁻¹ state across the query's operator DAG. Scales with the *table*, not
    /// the result window, so this usually dominates `view_bytes` by orders of
    /// magnitude.
    pub operator_bytes: usize,
}

impl ViewSize {
    pub fn total_bytes(&self) -> usize {
        self.view_bytes + self.operator_bytes
    }
}

/// Attributed heap estimate for the whole circuit, sorted heaviest-first.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SizeReport {
    /// Sum over `tables`: the shared base-table mirror.
    pub store_bytes: usize,
    /// Sum over `views`: per-registered-query state, which is charged once per
    /// query even when many queries read the same table.
    pub query_bytes: usize,
    pub tables: Vec<TableSize>,
    pub views: Vec<ViewSize>,
}

impl SizeReport {
    pub fn total_bytes(&self) -> usize {
        self.store_bytes + self.query_bytes
    }
}

// --- Serialization support ---

use serde::{Deserialize, Serialize};

/// Serializable snapshot of the circuit state.
#[derive(Serialize, Deserialize)]
struct CircuitState {
    store: Store,
    queries: Vec<QueryState>,
}

/// Borrowed mirror of [`CircuitState`], used only for writing.
///
/// Field names and types must stay in lockstep with `CircuitState`: that is
/// what makes a snapshot written through this readable through that. The whole
/// point is to avoid `self.store.clone()`, which duplicated every row in the
/// store just to hand it to the serializer — a full extra copy of the largest
/// structure in the process, on a timer, inside a capped container.
#[derive(Serialize)]
struct CircuitStateRef<'a> {
    store: &'a Store,
    queries: Vec<QueryStateRef<'a>>,
}

/// Borrowed mirror of [`QueryState`]. See [`CircuitStateRef`].
#[derive(Serialize)]
struct QueryStateRef<'a> {
    plan: &'a QueryPlan,
    /// Still converted rather than borrowed: `Sp00kyValue`'s derived
    /// `Serialize` is externally tagged (`{"Int":5}`), so borrowing it here
    /// would silently change the snapshot format. Bind params are per-view and
    /// small, so the conversion is not worth a hand-written serializer.
    params: Option<serde_json::Value>,
    format: OutputFormat,
    cache: &'a ZSet,
    last_hash: &'a str,
    content_generation: u64,
    subquery_cache: &'a HashMap<RowKey, (RowKey, String)>,
    auth_id: &'a str,
    /// Registrations sharing this view's graph. Empty for an unshared view,
    /// which is every view while merging is disabled.
    subscribers: &'a [Subscriber],
    /// This view's own `_00_query` row is gone but its graph still serves
    /// subscribers. Without persisting it, a restart would resume publishing
    /// deltas at a deleted row.
    detached: bool,
    /// The computation this view's graph is registered under, so a
    /// registration arriving after the restart still finds it. Empty when the
    /// view holds no claim (merging was disabled when it registered).
    merge_key: &'a str,
}

/// Serializable snapshot of a single query's state.
#[derive(Serialize, Deserialize)]
struct QueryState {
    plan: QueryPlan,
    #[serde(default)]
    params: Option<serde_json::Value>,
    format: OutputFormat,
    cache: ZSet,
    last_hash: String,
    content_generation: u64,
    #[serde(default)]
    subquery_cache: HashMap<RowKey, (RowKey, String)>,
    /// Owning user record id (e.g. `"user:abc"`). `default` so old
    /// snapshots without this field still deserialize (auth_id falls
    /// back to "" and the SSP routes those views to the global tables).
    #[serde(default)]
    auth_id: String,
    /// Merge bookkeeping. All three default, so a snapshot written before
    /// merging existed restores as an unshared view holding no claim — which
    /// is exactly what it was.
    #[serde(default)]
    subscribers: Vec<Subscriber>,
    #[serde(default)]
    detached: bool,
    #[serde(default)]
    merge_key: String,
}

impl Circuit {
    /// Serialize the circuit state to a JSON string.
    ///
    /// The operator DAG (which contains trait objects) is NOT serialized.
    /// Instead, we serialize the query plans and rebuild graphs on restore.
    pub fn save(&self) -> serde_json::Result<String> {
        // owner → merge_key, inverted once rather than scanned per view. The
        // key is persisted rather than recomputed on restore: recomputing needs
        // the params to round-trip through `Sp00kyValue` unchanged, and a
        // number that came back as a float would silently produce a different
        // key and un-merge the view.
        let claims: HashMap<&str, &str> = self
            .merge_index
            .iter()
            .map(|(key, owner)| (owner.as_str(), key.as_str()))
            .collect();
        const NO_SUBSCRIBERS: &[Subscriber] = &[];

        let queries: Vec<QueryStateRef<'_>> = self
            .views
            .values()
            .map(|view| QueryStateRef {
                subscribers: self
                    .subscribers
                    .get(&view.query_id)
                    .map(|s| s.as_slice())
                    .unwrap_or(NO_SUBSCRIBERS),
                detached: self.detached_owners.contains(&view.query_id),
                merge_key: claims.get(view.query_id.as_str()).copied().unwrap_or(""),
                plan: &view.plan,
                params: view
                    .params
                    .as_ref()
                    .map(|sv| serde_json::Value::from(sv.clone())),
                format: view.format,
                cache: &view.cache,
                last_hash: &view.last_hash,
                content_generation: view.content_generation,
                subquery_cache: &view.subquery_cache,
                auth_id: &view.auth_id,
            })
            .collect();

        let state = CircuitStateRef {
            store: &self.store,
            queries,
        };

        // Reserve up front rather than letting the buffer double its way up:
        // the growth reallocation transiently holds both the old and new
        // buffer, so on a large store that spike lands on top of the store
        // itself. The estimate only has to be the right order of magnitude —
        // a short reserve costs one realloc, not correctness.
        let mut buf = Vec::with_capacity(self.estimated_snapshot_bytes());
        serde_json::to_writer(&mut buf, &state)?;
        // Safe by construction: serde_json only emits UTF-8.
        Ok(String::from_utf8(buf).expect("serde_json emits UTF-8"))
    }

    /// Serialize ONLY the base collections, as bytes.
    ///
    /// For a circuit whose views are re-registered from scratch on every boot
    /// (the browser client mints a fresh session-salted id per query), views
    /// in the snapshot are zombies: stepped on every ingest, never read. The
    /// store is the part worth keeping. Bytes rather than a `String` because
    /// the consumer is a JS host, where a string doubles to UTF-16 on the way
    /// across and the snapshot is the largest single allocation it makes.
    pub fn save_store_only(&self) -> serde_json::Result<Vec<u8>> {
        let state = CircuitStateRef {
            store: &self.store,
            queries: Vec::new(),
        };
        let mut buf = Vec::with_capacity(self.estimated_snapshot_bytes());
        serde_json::to_writer(&mut buf, &state)?;
        Ok(buf)
    }

    /// Read the base collections out of a snapshot written by
    /// [`Self::save_store_only`] (or a full [`Self::save`]; the views are
    /// ignored). The caller installs it with [`Self::replace_store`].
    pub fn restore_store(bytes: &[u8]) -> serde_json::Result<Store> {
        let state: CircuitState = serde_json::from_slice(bytes)?;
        Ok(state.store)
    }

    /// Swap the base collections under the registered views and re-prime
    /// every view against the new rows.
    ///
    /// Keeps everything `restore` drops (permissions, link targets, opaque
    /// fields, projection) and keeps the views themselves: a client loads its
    /// snapshot in the background while queries are already registering, and
    /// a view registered against the empty pre-snapshot store must end up
    /// seeing the restored rows, not be silently discarded. Each view gets a
    /// fresh graph (operator state is a function of the store) and a full
    /// initial snapshot, returned as the deltas to publish. A view that ends
    /// up empty still gets a delta so its consumer drops the stale rows.
    pub fn replace_store(&mut self, store: Store) -> Vec<ViewDelta> {
        self.store = store;
        self.store.clear_pending_deleted_rows();
        self.refresh_retained_fields();

        let ids: Vec<String> = self.views.keys().cloned().collect();
        let mut out = Vec::new();
        for id in ids {
            let (root, previous) = {
                let view = self.views.get_mut(&id).expect("view listed");
                let previous: Vec<String> = view.cache.keys().map(|k| k.to_string()).collect();
                view.cache.clear();
                view.subquery_cache.clear();
                view.last_hash = String::new();
                (view.plan.root.clone(), previous)
            };
            self.graphs.insert(id.clone(), Graph::from_plan(&root));
            match self.run_initial_snapshot(&id) {
                Some(delta) => out.extend(self.fan_out(delta)),
                None => {
                    let view = self.views.get_mut(&id).expect("view listed");
                    view.last_hash = view.compute_hash();
                    let delta = ViewDelta {
                        query_id: id.clone(),
                        additions: vec![],
                        removals: previous,
                        updates: vec![],
                        records: vec![],
                        result_hash: view.last_hash.clone(),
                        subquery_items: vec![],
                        auth_id: view.auth_id.clone(),
                        // The store was replaced and the view came back
                        // empty: this IS the view's whole membership now.
                        initial: true,
                    };
                    out.extend(self.fan_out(delta));
                }
            }
        }
        out
    }

    /// Bring one table in line with the caller's authoritative `(id, rv)`
    /// list, the client-side counterpart of the server's `_00_rv` catch-up.
    ///
    /// Rows the store holds that the list lacks are deleted THROUGH `step`, so
    /// registered views retract them. Rows the list has that the store lacks,
    /// or holds at a lower `_00_rv`, are returned for the caller to ingest;
    /// nothing is fetched here because the bodies live on the caller's side.
    /// Ids may be spelled raw or `table:id`; the returned `fetch` keeps the
    /// caller's spelling.
    pub fn reconcile(&mut self, table: &str, entries: &[(String, i64)]) -> Reconciled {
        let local: HashMap<&str, i64> = entries
            .iter()
            .map(|(id, rv)| (crate::types::raw_id(id), *rv))
            .collect();
        let (fetch, stale): (Vec<String>, Vec<String>) = match self.store.get_collection(table) {
            Some(coll) => {
                let stale = coll
                    .rows
                    .keys()
                    .filter(|id| !local.contains_key(*id))
                    .map(str::to_string)
                    .collect();
                let fetch = entries
                    .iter()
                    .filter(|(id, rv)| match coll.rows.rv_of(crate::types::raw_id(id)) {
                        Some(stored) => stored < *rv,
                        None => true,
                    })
                    .map(|(id, _)| id.clone())
                    .collect();
                (fetch, stale)
            }
            None => (entries.iter().map(|(id, _)| id.clone()).collect(), Vec::new()),
        };
        let deleted = stale.len();
        let deltas = if stale.is_empty() {
            Vec::new()
        } else {
            let changes = stale
                .into_iter()
                .map(|id| Change::delete(table, &id))
                .collect();
            self.step(ChangeSet { changes })
        };
        Reconciled {
            fetch,
            deleted,
            deltas,
        }
    }

    /// Rebuild every table's row bytes without dead space (and re-projected,
    /// when projection is on). Returns the bytes that were dead beforehand.
    /// Holds each table decoded while it rebuilds, so call it from a
    /// checkpoint or idle timer, not from an ingest.
    pub fn compact(&mut self) -> u64 {
        let mut reclaimed = 0;
        for coll in self.store.collections.values_mut() {
            reclaimed += coll.rows.dead_bytes();
            coll.compact();
        }
        reclaimed
    }

    /// Bytes orphaned by updates and deletes across every table.
    pub fn dead_bytes(&self) -> u64 {
        self.store.collections.values().map(|c| c.rows.dead_bytes()).sum()
    }

    /// Bytes referenced by live rows across every table.
    pub fn live_bytes(&self) -> u64 {
        self.store.collections.values().map(|c| c.rows.live_bytes()).sum()
    }

    /// Turn projection on or off. On: every collection keeps only the fields
    /// the registered plans evaluate (recomputed on each registration). Off:
    /// whole bodies. Rows already stored are not rewritten either way until
    /// the next write of each row or a [`Self::compact`].
    pub fn set_projection(&mut self, enabled: bool) {
        self.projection = enabled;
        if enabled {
            self.refresh_retained_fields();
        } else {
            for coll in self.store.collections.values_mut() {
                coll.retained = None;
            }
        }
    }

    pub fn projection(&self) -> bool {
        self.projection
    }

    /// Root fields per table that the registered plans evaluate.
    fn evaluated_fields(&self) -> HashMap<String, std::collections::BTreeSet<String>> {
        let mut out: HashMap<String, std::collections::BTreeSet<String>> = HashMap::new();
        for view in self.views.values() {
            for (table, field) in view.plan.root.evaluated_field_refs() {
                out.entry(table).or_default().insert(field);
            }
        }
        out
    }

    /// Re-derive each collection's retained set from the registered plans.
    /// No-op unless projection is on.
    fn refresh_retained_fields(&mut self) {
        if !self.projection {
            return;
        }
        let wanted = self.evaluated_fields();
        // A table a plan reads but nothing has written yet still needs its
        // projection in place before the first row lands.
        for table in wanted.keys() {
            self.store.ensure_collection(table);
        }
        for (name, coll) in self.store.collections.iter_mut() {
            coll.retained = Some(wanted.get(name).cloned().unwrap_or_default());
        }
    }

    /// Record which fields `root` evaluates that stored rows of its tables
    /// were projected without. Only meaningful with projection on and rows
    /// present: an empty table has nothing to widen.
    fn note_missing_fields(&mut self, root: &OperatorPlan) {
        if !self.projection {
            return;
        }
        for (table, field) in root.evaluated_field_refs() {
            let Some(coll) = self.store.get_collection(&table) else {
                continue;
            };
            if coll.rows.is_empty() {
                continue;
            }
            let held = match &coll.retained {
                Some(kept) => kept.contains(&field) || crate::circuit::store::ALWAYS_RETAINED.contains(&field.as_str()),
                None => true,
            };
            if !held {
                self.missing_fields.entry(table).or_default().insert(field);
            }
        }
    }

    /// Drain the fields noted by the registrations since the last call.
    pub fn take_missing_fields(&mut self) -> BTreeMap<String, std::collections::BTreeSet<String>> {
        std::mem::take(&mut self.missing_fields)
    }

    /// Rough byte estimate for a serialized snapshot, used only to size the
    /// output buffer. Deliberately cheap: it must not walk every row, or
    /// sizing the buffer would cost more than the reallocation it avoids.
    fn estimated_snapshot_bytes(&self) -> usize {
        // ~200 bytes of JSON per row is a mid-range guess for application data;
        // being wrong just means one more realloc.
        const BYTES_PER_ROW_GUESS: usize = 200;
        let rows: usize = self.store.collections.values().map(|c| c.rows.len()).sum();
        rows.saturating_mul(BYTES_PER_ROW_GUESS).max(64 * 1024)
    }

    /// Restore a circuit from a JSON string.
    ///
    /// Rebuilds operator DAGs from the stored query plans and
    /// restores view caches to their saved state.
    pub fn restore(json: &str) -> serde_json::Result<Self> {
        let state: CircuitState = serde_json::from_str(json)?;

        let mut circuit = Self {
            store: state.store,
            graphs: HashMap::new(),
            views: HashMap::new(),
            dependency_map: HashMap::new(),
            merge_index: HashMap::new(),
            subscribers: HashMap::new(),
            detached_owners: std::collections::HashSet::new(),
            merge_views: false,
            monotonic_row_versions: false,
            rv_floor: 0,
            synthesized_row_versions: 0,
            synthesized_row_versions_missing: 0,
            permissions: HashMap::new(),
            // Re-seeded from INFO FOR DB / INFO FOR TABLE after restore, same as
            // `permissions` (none of the three is part of the serialized snapshot).
            link_targets: HashMap::new(),
            opaque_fields: HashMap::new(),
            projection: false,
            missing_fields: BTreeMap::new(),
        };

        for qs in state.queries {
            // Snapshots written before ids were canonicalised hold the
            // `_00_query:<hash>` spelling for anything a live client had
            // registered. Re-key on the way in, or a restored view stays
            // invisible to the sweep and to the client that owns it.
            let mut qs = qs;
            qs.plan.id = crate::canonical_query_id(&qs.plan.id);
            let query_id = qs.plan.id.clone();
            let referenced_tables = qs.plan.root.referenced_tables();
            let params_sv = qs.params.map(Sp00kyValue::from);

            // Rebuild the operator DAG from the plan
            let graph = Graph::from_plan(&qs.plan.root);

            // Restore view state
            let mut view = View::new(
                query_id.clone(),
                qs.plan,
                qs.format,
                params_sv,
                referenced_tables.clone(),
                qs.auth_id,
            );
            view.cache = qs.cache;
            view.last_hash = qs.last_hash;
            view.content_generation = qs.content_generation;
            view.subquery_cache = qs.subquery_cache;

            circuit.graphs.insert(query_id.clone(), graph);
            circuit.views.insert(query_id.clone(), view);

            // Rebuild dependency map
            for table in &referenced_tables {
                circuit
                    .dependency_map
                    .entry(table.clone())
                    .or_default()
                    .push(query_id.clone());
            }

            // Rebuild the merge bookkeeping. Without this a restart silently
            // un-merges: the subscribers vanish (they own no view of their own,
            // so nothing else in the snapshot mentions them) and every one of
            // them stops receiving deltas while its `_00_query` row and
            // heartbeat carry on as if it were live.
            if !qs.subscribers.is_empty() {
                circuit.subscribers.insert(
                    query_id.clone(),
                    qs.subscribers
                        .into_iter()
                        .map(|s| Subscriber {
                            query_id: crate::canonical_query_id(&s.query_id),
                            auth_id: s.auth_id,
                        })
                        .collect(),
                );
            }
            if qs.detached {
                circuit.detached_owners.insert(query_id.clone());
            }
            if !qs.merge_key.is_empty() {
                circuit.merge_index.insert(qs.merge_key, query_id.clone());
            }
        }

        Ok(circuit)
    }

    // --- Accessor methods ---

    /// Number of registered views.
    /// The query id owning the graph for `merge_key`, if one is registered.
    ///
    /// `None` means this computation is not resident and the caller must take
    /// the cold path. Returns the owner even if it has no subscribers yet.
    pub fn owner_for_merge_key(&self, merge_key: &str) -> Option<&str> {
        self.merge_index.get(merge_key).map(|s| s.as_str())
    }

    /// Whether this circuit may share one graph between registrations that
    /// compute the same thing. See the field docs for why it is not snapshotted.
    pub fn merge_views(&self) -> bool {
        self.merge_views
    }

    /// Apply the shell's `SPKY_SSP_MERGE_VIEWS` setting. Must be re-applied
    /// after every `Circuit::new` / `Circuit::restore`, since neither carries
    /// configuration.
    pub fn set_merge_views(&mut self, enabled: bool) {
        self.merge_views = enabled;
    }

    /// Whether Create/Update bodies get a row-version floor (`floor_row_version`).
    pub fn monotonic_row_versions(&self) -> bool {
        self.monotonic_row_versions
    }

    /// Turn the row-version floor on or off. Configuration, like
    /// `set_merge_views`: re-apply after every `Circuit::new` / `Circuit::restore`.
    pub fn set_monotonic_row_versions(&mut self, enabled: bool) {
        self.monotonic_row_versions = enabled;
    }

    /// How many `_00_rv` values the floor has synthesized so far. Monotonic
    /// over the circuit's lifetime; the shell diffs it around a step.
    pub fn synthesized_row_versions(&self) -> u64 {
        self.synthesized_row_versions
    }

    /// Subset of [`Self::synthesized_row_versions`] where the ingested body
    /// carried no `_00_rv` at all, i.e. the DB-side stamp produced nothing.
    /// A non-zero count here is what points at `_00_version`; a zero one says
    /// the stamp is working and the versions merely failed to advance.
    pub fn synthesized_row_versions_missing(&self) -> u64 {
        self.synthesized_row_versions_missing
    }

    /// Stamp `data` with a version subscribers will act on.
    ///
    /// `_00_rv` is the one thing a subscribed client compares against its local
    /// copy (`local < remote` → refetch). The DB-side mutation event stamps it
    /// from `_00_version`; when that lookup comes back NONE the body arrives
    /// with no version, the row is published at 1, and every peer holding it at
    /// >= 1 keeps its stale copy for good. Measured on whitepawn: a partially
    /// built unique index on `_00_version.record_id` (interrupted `DEFINE INDEX
    /// OVERWRITE` rebuilds, see
    /// docs/surrealdb-bugs/unique-index-partial-after-overwrite-rebuild.md) did
    /// that to a third of all rows, and two chat clients each kept rendering
    /// their own last write. A version that does not advance past the stored
    /// one has the same effect.
    ///
    /// So for a row already in the store, a missing or non-advancing version
    /// becomes `max(stored, floor) + 1`, where the floor is the highest version
    /// this circuit has seen or issued: above anything a peer can hold that
    /// came from this server. A row not yet in the store keeps its body as-is
    /// (nobody holds it), and `Merge` is left alone by the caller: it widens a
    /// projected row with fields the store dropped, not a change anyone needs
    /// to refetch. A body whose content matches what is stored still lands
    /// nowhere (`Collection::apply` compares digests with `_00_rv` stripped),
    /// so a replayed event never publishes a bump.
    fn floor_row_version(&mut self, table: &str, id: &str, mut data: Sp00kyValue) -> Sp00kyValue {
        let incoming = match data.get("_00_rv") {
            Some(Sp00kyValue::Int(v)) if *v > 0 => Some(*v),
            _ => None,
        };
        if let Some(v) = incoming {
            self.rv_floor = self.rv_floor.max(v);
        }
        let raw = raw_id(id);
        let stored = match self.store.collections.get(table) {
            Some(coll) if coll.rows.contains_key(raw) => coll.rows.rv_of(raw),
            _ => return data,
        };
        match (incoming, stored) {
            (Some(v), Some(s)) if v > s => return data,
            (Some(_), None) => return data,
            _ => {}
        }
        if self.rv_floor == 0 {
            let seen = self.max_row_versions().into_values().max().unwrap_or(0);
            self.rv_floor = seen.max(0);
        }
        let next = self.rv_floor.max(stored.unwrap_or(0)) + 1;
        self.rv_floor = next;
        self.synthesized_row_versions += 1;
        if incoming.is_none() {
            self.synthesized_row_versions_missing += 1;
        }
        if let Sp00kyValue::Object(map) = &mut data {
            map.insert("_00_rv".to_string(), Sp00kyValue::Int(next));
        }
        data
    }

    /// Claim `merge_key` for `query_id`, so later registrations of the same
    /// computation attach to it instead of building a second graph.
    ///
    /// Call this ONLY after the graph exists (i.e. after `add_query_*`), and
    /// only when merging is enabled: an entry here with no graph behind it
    /// would send every later registration down the attach path to nowhere.
    pub fn claim_merge_key(&mut self, merge_key: String, query_id: String) {
        self.merge_index
            .insert(merge_key, crate::canonical_query_id(&query_id));
    }

    /// Attach `query_id` to `owner`'s graph and return the delta that
    /// publishes the CURRENT membership to the new subscriber.
    ///
    /// The joiner needs a full snapshot rather than the next incremental delta,
    /// because its `_00_list_ref` rows do not exist yet. Everything in the
    /// returned delta is read from the owner's already-computed view, so this
    /// costs a clone rather than a re-evaluation, which is the entire point of
    /// merging.
    ///
    /// Idempotent: re-attaching an existing subscriber refreshes nothing and
    /// returns the snapshot again, which is safe because edge writes are
    /// `RELATE`/`UPDATE` on a deterministic key.
    pub fn attach_subscriber(
        &mut self,
        owner: &str,
        query_id: String,
        auth_id: String,
    ) -> Option<ViewDelta> {
        let owner = crate::canonical_query_id(owner);
        let query_id = crate::canonical_query_id(&query_id);
        let delta = self.snapshot_delta_from(&owner, query_id.clone(), auth_id.clone())?;

        let subs = self.subscribers.entry(owner).or_default();
        if !subs.iter().any(|s| s.query_id == query_id) {
            subs.push(Subscriber { query_id, auth_id });
        }

        Some(delta)
    }

    /// Build a full-membership [`ViewDelta`] for `query_id` out of `source`'s
    /// already-computed view, without touching the graph.
    ///
    /// This is the "publish what this view currently holds" primitive, and it
    /// has two callers by design:
    ///
    /// - [`Circuit::attach_subscriber`], where `source` is another
    ///   registration's graph and the joiner needs a snapshot because its
    ///   `_00_list_ref` rows do not exist yet;
    /// - the SSP's re-registration repair path, where `source` IS `query_id`
    ///   and the rows have gone missing underneath a still-live view (a TTL
    ///   sweep that released the row while another holder kept the graph).
    ///
    /// Both read the cache rather than re-evaluating, so a republish costs a
    /// clone. Crucially it does NOT go through `add_query_with_auth`: that
    /// would `views.insert` over the live view, discard its cache and subquery
    /// state, and push the id onto the dependency map a second time so every
    /// later ingest stepped the graph twice.
    ///
    /// `None` when `source` has no view, including after canonicalization.
    pub fn snapshot_delta_from(
        &self,
        source: &str,
        query_id: String,
        auth_id: String,
    ) -> Option<ViewDelta> {
        let view = self.get_view(source)?;

        let records: Vec<String> = view.cache.keys().map(|k| k.to_string()).collect();
        let subquery_items: Vec<SubqueryDeltaItem> = view
            .subquery_cache
            .iter()
            .map(|(child, (parent, alias))| SubqueryDeltaItem {
                id: child.to_string(),
                parent_key: parent.to_string(),
                alias: alias.clone(),
                op: SubqueryOp::Add,
            })
            .collect();

        Some(ViewDelta {
            query_id: crate::canonical_query_id(&query_id),
            additions: records.clone(),
            removals: vec![],
            updates: vec![],
            records,
            result_hash: view.last_hash.clone(),
            subquery_items,
            auth_id,
            initial: true,
        })
    }

    /// Full current membership of `query_id`'s own view, stamped for itself.
    /// See [`Circuit::snapshot_delta_from`].
    pub fn snapshot_delta(&self, query_id: &str, auth_id: String) -> Option<ViewDelta> {
        self.snapshot_delta_from(query_id, query_id.to_string(), auth_id)
    }

    /// Subscribers sharing `owner`'s graph, excluding the owner itself.
    pub fn subscribers_of(&self, owner: &str) -> &[Subscriber] {
        self.subscribers.get(owner).map(|v| v.as_slice()).unwrap_or(&[])
    }

    /// Owner whose graph `query_id` is attached to, if it is a subscriber.
    fn owner_of(&self, query_id: &str) -> Option<String> {
        self.subscribers
            .iter()
            .find(|(_, subs)| subs.iter().any(|s| s.query_id == query_id))
            .map(|(owner, _)| owner.clone())
    }

    /// Release one registration's claim on a shared graph.
    ///
    /// Returns `true` when this was the LAST holder and the graph was actually
    /// removed, so callers can keep `view_count` and teardown side effects in
    /// step. Returns `false` when others still depend on it.
    ///
    /// This is what makes merging safe to enable. Without it both teardown
    /// paths (`/view/unregister` and the TTL sweep) call `remove_query`
    /// unconditionally, so the first tab to leave would destroy a graph its
    /// siblings are still reading and their lists would go blank.
    ///
    /// An OWNER that leaves does not drop the graph while subscribers remain;
    /// it is marked detached so it stops receiving deltas of its own (its
    /// `_00_query` row is gone, so edges pointed at it would dangle) while its
    /// graph keeps serving everyone else.
    pub fn detach_subscriber(&mut self, query_id: &str) -> bool {
        let query_id = &crate::canonical_query_id(query_id);
        if let Some(owner) = self.owner_of(query_id) {
            if let Some(subs) = self.subscribers.get_mut(&owner) {
                subs.retain(|s| s.query_id != *query_id);
            }
            return self.drop_graph_if_unused(&owner);
        }

        if self.views.contains_key(query_id) {
            self.detached_owners.insert(query_id.to_string());
            return self.drop_graph_if_unused(query_id);
        }

        false
    }

    /// Remove `owner`'s graph once nothing holds it: the owner itself has
    /// detached AND no subscribers remain.
    fn drop_graph_if_unused(&mut self, owner: &str) -> bool {
        let has_subscribers = self
            .subscribers
            .get(owner)
            .map(|s| !s.is_empty())
            .unwrap_or(false);
        if has_subscribers || !self.detached_owners.contains(owner) {
            return false;
        }
        self.remove_query(owner);
        self.subscribers.remove(owner);
        self.detached_owners.remove(owner);
        // Drop the merge-key claim too, or the next registration of this
        // computation attaches to a graph that no longer exists.
        self.merge_index.retain(|_, v| v != owner);
        true
    }

    /// Number of distinct operator graphs. Diverges from `view_count` only
    /// while merging is enabled, and the gap IS the memory saved.
    pub fn graph_count(&self) -> usize {
        self.graphs.len()
    }

    /// Registrations attached to someone else's graph. `graph_count +
    /// subscriber_count` is the number of live registrations, so the gap
    /// against `graph_count` is the sharing actually achieved.
    pub fn subscriber_count(&self) -> usize {
        self.subscribers.values().map(|s| s.len()).sum()
    }

    /// Re-point one computed delta at every subscriber sharing the graph.
    ///
    /// A delta's content is a property of the computation, so subscribers differ
    /// only in `query_id` and `auth_id`. Fanning out HERE rather than in the edge
    /// batcher keeps `build_edge_batch` untouched and, more importantly, covers
    /// the shells that bypass batching entirely (`ssp-cf`, `ssp-portable` drop
    /// the edge receiver and write inline).
    fn fan_out(&self, delta: ViewDelta) -> Vec<ViewDelta> {
        let detached = self.detached_owners.contains(&delta.query_id);
        let subs = match self.subscribers.get(&delta.query_id) {
            Some(s) if !s.is_empty() => s,
            // Not shared: unchanged behaviour, unless the owner itself is gone
            // and we are only keeping the graph alive for nobody (transient,
            // `drop_graph_if_unused` removes it).
            _ => return if detached { vec![] } else { vec![delta] },
        };
        let mut out = Vec::with_capacity(subs.len() + 1);
        for s in subs {
            out.push(ViewDelta {
                query_id: s.query_id.clone(),
                auth_id: s.auth_id.clone(),
                ..delta.clone()
            });
        }
        // A detached owner has no `_00_query` row left to hang edges off.
        if !detached {
            out.push(delta);
        }
        out
    }

    pub fn view_count(&self) -> usize {
        self.views.len()
    }

    /// IDs of all registered views.
    pub fn view_ids(&self) -> Vec<String> {
        self.views.keys().cloned().collect()
    }

    /// Names of all tables in the store.
    pub fn table_names(&self) -> Vec<String> {
        self.store.collections.keys().cloned().collect()
    }

    /// Estimated heap footprint of the circuit, attributed per table and per
    /// view.
    ///
    /// The SSP mirrors every syncable table in RAM for the whole process
    /// lifetime, so this is what decides whether a tenant fits under its
    /// cgroup cap. See [`crate::size`] for why these are estimates and how to
    /// read them (deltas are meaningful; absolutes are indicative).
    ///
    /// Cheap enough to serve from an endpoint — it walks every row once, so
    /// it is O(store), not O(1). Don't call it per-request on a hot path.
    pub fn size_report(&self) -> SizeReport {
        let mut tables: Vec<TableSize> = self
            .store
            .collections
            .iter()
            .map(|(name, coll)| TableSize {
                table: name.clone(),
                rows: coll.rows.len(),
                rows_bytes: coll.rows_bytes(),
                index_bytes: coll.index_bytes(),
                zset_bytes: coll.zset_bytes(),
                live_bytes: coll.rows.live_bytes(),
                dead_bytes: coll.rows.dead_bytes(),
                retained_fields: coll
                    .retained
                    .as_ref()
                    .map(|f| f.iter().cloned().collect()),
            })
            .collect();
        tables.sort_by(|a, b| b.total_bytes().cmp(&a.total_bytes()));

        let mut views: Vec<ViewSize> = self
            .views
            .iter()
            .map(|(query_id, view)| ViewSize {
                query_id: query_id.clone(),
                auth_id: view.auth_id.clone(),
                cached_records: view.cache.len(),
                view_bytes: view.state_bytes(),
                operator_bytes: self.graphs.get(query_id).map_or(0, |g| g.state_bytes()),
            })
            .collect();
        views.sort_by(|a, b| b.total_bytes().cmp(&a.total_bytes()));

        SizeReport {
            store_bytes: tables.iter().map(TableSize::total_bytes).sum(),
            query_bytes: views.iter().map(ViewSize::total_bytes).sum(),
            tables,
            views,
        }
    }

    /// Per-table row counts in the in-memory store. Used by `/info` and
    /// `spky verify` to compare circuit state against the upstream snapshot.
    pub fn table_record_counts(&self) -> Vec<(String, usize)> {
        let mut out: Vec<_> = self.store.collections
            .iter()
            .map(|(name, coll)| (name.clone(), coll.rows.len()))
            .collect();
        out.sort_by(|a, b| a.0.cmp(&b.0));
        out
    }

    /// Per-table content hashes in the in-memory store. Bit-identical to
    /// `Replica::compute_table_hashes` on the scheduler side when the
    /// contents agree — used by SSP self-verify (`self_bootstrap`) and
    /// `spky verify` to detect drift.
    /// Fed row by row through [`TableHasher`] rather than collected into a
    /// `Vec<(String, Value)>` first. The old shape held the entire table as a
    /// parsed `Value` tree *plus* a vec of all of them, at roughly six times
    /// the resident cost of the rows themselves — on a million-row table that
    /// is a multi-gigabyte transient inside a 1 GB container, and `/info`
    /// reaches this code on every request. `TableHasher` reduces each record
    /// to its canonical bytes on `add` and produces byte-identical output by
    /// construction, so peak is now one row's tree plus the accumulated bytes.
    ///
    /// [`TableHasher`]: ssp_protocol::snapshot_hash::TableHasher
    pub fn compute_table_hashes(&self) -> BTreeMap<String, String> {
        self.store
            .collections
            .iter()
            .map(|(name, coll)| {
                let mut hasher = ssp_protocol::snapshot_hash::TableHasher::new();
                for (id, val) in coll.rows.iter() {
                    hasher.add(id, serde_json::Value::from(val.to_owned_value()));
                }
                (name.clone(), hasher.finish())
            })
            .collect()
    }

    /// Whether a record is currently present in the store. Used by catch-up to
    /// pick `Create` (new membership) vs `Update` (content-only) when replaying
    /// post-snapshot rows through [`Self::step`].
    pub fn contains(&self, table: &str, id: &str) -> bool {
        self.store
            .collections
            .get(table)
            .map(|c| c.has_row(id))
            .unwrap_or(false)
    }

    /// Highest `_00_rv` currently folded into each table's rows (`-1` when a
    /// table has no versioned row). This is the resume-point a `CircuitStore`
    /// snapshot carries: on a warm restart, catch-up loads only rows whose
    /// `_00_rv` exceeds the per-table value here (see `bootstrap::catch_up_from_db`).
    /// Reads the field directly instead of converting each row to a
    /// `serde_json::Value` to pull one integer out of it — that conversion
    /// deep-copied every row body in the store, per checkpoint, to look at 8
    /// bytes.
    ///
    /// Matching `Sp00kyValue::Int` specifically is deliberate: `as_i64` also
    /// accepts a whole-valued `Float`, where the `serde_json` path this
    /// replaces returned `None` for `json!(5.0)`. Accepting more here would
    /// raise the resume point past rows that catch-up still needs to replay,
    /// and a skipped row is silent divergence, not a visible failure.
    pub fn max_row_versions(&self) -> BTreeMap<String, i64> {
        self.store
            .collections
            .iter()
            .map(|(name, coll)| {
                let max = coll.rows.max_rv().unwrap_or(-1);
                (name.clone(), max)
            })
            .collect()
    }

    /// Per-table incremental XOR set-hashes (the `catchup_xor` accumulators),
    /// formatted `x3:`. Compared against the scheduler's reconstructed hash at
    /// the catch-up cut to verify a rejoining SSP — see the scheduler's
    /// `verify_catchup_at_m`. Same table set as `compute_table_hashes`.
    pub fn compute_catchup_hashes(&self) -> BTreeMap<String, String> {
        self.store
            .collections
            .iter()
            .map(|(name, coll)| {
                (
                    name.clone(),
                    ssp_protocol::snapshot_hash::xor_acc_to_hex(&coll.catchup_xor),
                )
            })
            .collect()
    }

    /// Re-seed every collection's catch-up XOR accumulator from its rows. Call
    /// once after bootstrap (which bulk-loads via [`Circuit::load`], bypassing
    /// the per-row `apply_mutation` maintenance); steady-state ingest keeps it
    /// current thereafter.
    pub fn reseed_catchup_hashes(&mut self) {
        for coll in self.store.collections.values_mut() {
            coll.reseed_catchup_xor();
        }
    }

    /// Dependency map: table → [query_ids] for debugging.
    pub fn dependency_map_dump(&self) -> &HashMap<String, Vec<String>> {
        &self.dependency_map
    }

    /// Dump a table's circuit rows as `(raw_id, json)` pairs — the exact values
    /// that feed the catch-up XOR set-hash. The scheduler pulls this on a
    /// persistent catch-up mismatch (`/debug/catchup-rows/:table`) to diff its
    /// reconstructed projection row-by-row against the circuit, so an operator
    /// can see the specific diverging (or missing/extra) row instead of guessing
    /// from a one-sided hash. Returns an empty vec for an unknown table.
    /// Rows come back **sorted by raw id and capped at `limit`**, and the
    /// second tuple element reports whether the table had more.
    ///
    /// Both properties matter to the caller. Unbounded, this materialized the
    /// entire table as owned `Value` trees and then serialized that into a
    /// response body — roughly twice the store, at the exact moment (a
    /// persistent catch-up mismatch) when the SSP is least able to afford it.
    ///
    /// But truncating an *unordered* subset would be worse than the memory
    /// cost: the scheduler diffs this against its own projection to name
    /// missing and extra rows, so an arbitrary omission reads as "row missing
    /// on SSP" and sends an operator chasing a divergence that isn't there.
    /// Sorting makes the returned window a well-defined prefix — every id at
    /// or below the last one returned is authoritative, everything past it is
    /// unknown rather than absent — which is a claim the caller can act on.
    pub fn dump_table_rows(
        &self,
        table: &str,
        limit: usize,
    ) -> (Vec<(String, serde_json::Value)>, bool) {
        let Some(coll) = self.store.collections.get(table) else {
            return (Vec::new(), false);
        };
        let mut ids: Vec<&str> = coll.rows.keys().collect();
        ids.sort_unstable();
        let truncated = ids.len() > limit;
        let rows = ids
            .into_iter()
            .take(limit)
            .map(|id| {
                (
                    id.to_string(),
                    serde_json::Value::from(coll.rows.get(id).to_owned_value()),
                )
            })
            .collect();
        (rows, truncated)
    }
}

impl Default for Circuit {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::algebra::ZSetOps;
    use crate::operator::plan::{OperatorPlan, OrderSpec, Projection};
    use crate::types::Path;
    use serde_json::json;

    fn scan_query(id: &str, table: &str) -> QueryPlan {
        QueryPlan {
            id: id.to_string(),
            root: OperatorPlan::Scan {
                table: table.to_string(),
            },
        }
    }

    /// `compute_table_hashes` streams rows through `TableHasher` instead of
    /// collecting the whole table into a `Vec<(String, Value)>` for
    /// `hash_table`. The scheduler compares these hashes against its own and
    /// the SSP `exit(2)`s on mismatch, so the two must agree byte for byte.
    #[test]
    fn streamed_table_hash_matches_hash_table() {
        let mut circuit = Circuit::new();
        circuit.store.ensure_collection("thread");
        for i in 0..64 {
            circuit.store.apply_change(&Change::create(
                "thread",
                &format!("t{i}"),
                json!({
                    "title": format!("title {i}"),
                    "score": i,
                    // Exercise the paths `hash_table` normalizes away: reserved
                    // keys and null-valued keys are both stripped before hashing.
                    "_00_rv": i,
                    "archived_at": serde_json::Value::Null,
                    "nested": { "b": 2, "a": 1 },
                }),
            ));
        }

        let coll = circuit.store.get_collection("thread").unwrap();
        let pairs: Vec<(String, serde_json::Value)> = coll
            .rows
            .iter()
            .map(|(id, val)| (id.to_string(), serde_json::Value::from(val.to_owned_value())))
            .collect();
        let expected = ssp_protocol::snapshot_hash::hash_table(pairs);

        assert_eq!(circuit.compute_table_hashes()["thread"], expected);
    }

    #[test]
    fn empty_table_hash_matches_hash_table() {
        let mut circuit = Circuit::new();
        circuit.store.ensure_collection("thread");
        assert_eq!(
            circuit.compute_table_hashes()["thread"],
            ssp_protocol::snapshot_hash::empty_table_hash()
        );
    }

    /// `max_row_versions` reads `_00_rv` straight off the stored value now
    /// rather than converting each row to `serde_json::Value`. It must keep
    /// accepting *only* integers: `Sp00kyValue::as_i64` would also take a
    /// whole-valued `Float`, but the `serde_json` path it replaces returned
    /// `None` for `json!(5.0)`. Accepting more would raise the resume point
    /// past rows catch-up still has to replay, and a skipped row diverges
    /// silently.
    #[test]
    fn max_row_versions_counts_ints_only() {
        let mut circuit = Circuit::new();
        circuit.store.ensure_collection("thread");
        circuit
            .store
            .apply_change(&Change::create("thread", "a", json!({ "_00_rv": 7 })));
        // A float, a string, and a missing field must all be ignored.
        circuit
            .store
            .apply_change(&Change::create("thread", "b", json!({ "_00_rv": 99.5 })));
        circuit
            .store
            .apply_change(&Change::create("thread", "c", json!({ "_00_rv": "42" })));
        circuit
            .store
            .apply_change(&Change::create("thread", "d", json!({ "title": "no rv" })));

        assert_eq!(circuit.max_row_versions()["thread"], 7);
    }

    #[test]
    fn max_row_versions_defaults_to_minus_one_without_versioned_rows() {
        let mut circuit = Circuit::new();
        circuit.store.ensure_collection("thread");
        circuit
            .store
            .apply_change(&Change::create("thread", "a", json!({ "title": "x" })));
        assert_eq!(circuit.max_row_versions()["thread"], -1);
    }

    /// `save` now serializes through a borrowed `CircuitStateRef` rather than
    /// cloning the whole store. The snapshot format has to be unchanged, or
    /// every warm restart falls back to a full rebuild.
    #[test]
    fn save_through_borrowed_state_restores_identically() {
        let mut circuit = Circuit::new();
        circuit.store.ensure_collection("thread");
        for i in 0..8 {
            circuit.store.apply_change(&Change::create(
                "thread",
                &format!("t{i}"),
                json!({ "title": format!("t{i}"), "score": i, "_00_rv": i }),
            ));
        }
        // Params exercise the one field `CircuitStateRef` still converts
        // rather than borrows.
        circuit.add_query(
            scan_query("q1", "thread"),
            Some(json!({ "auth": { "id": "user:abc" } })),
            None,
        );

        let blob = circuit.save().unwrap();
        let mut restored = Circuit::restore(&blob).unwrap();

        assert_eq!(
            restored.compute_table_hashes(),
            circuit.compute_table_hashes(),
            "restored store must hash identically"
        );
        assert_eq!(restored.max_row_versions(), circuit.max_row_versions());
        assert_eq!(restored.view_count(), circuit.view_count());

        // `catchup_xor` is `#[serde(skip)]`, so a freshly restored circuit
        // carries a zeroed accumulator until it is re-seeded — which is
        // exactly what `Runtime::bootstrap` does after a restore. Assert the
        // documented sequence, since a restore that silently kept a zero
        // accumulator would fail the scheduler's catch-up verification.
        assert_ne!(
            restored.compute_catchup_hashes(),
            circuit.compute_catchup_hashes(),
            "restore alone must not resurrect the skipped accumulator"
        );
        restored.reseed_catchup_hashes();
        assert_eq!(
            restored.compute_catchup_hashes(),
            circuit.compute_catchup_hashes(),
            "re-seeding after restore must reproduce the accumulator"
        );

        // A second round trip must be a fixed point *in content*. Not in
        // bytes: the store is `HashMap`-backed, so JSON key order differs
        // between two circuits holding identical data. The snapshot has never
        // been byte-stable and `restore` does not need it to be — but that
        // also means a snapshot cannot be content-addressed by hashing it.
        let twice = Circuit::restore(&restored.save().unwrap()).unwrap();
        assert_eq!(twice.compute_table_hashes(), circuit.compute_table_hashes());
        assert_eq!(twice.view_count(), circuit.view_count());
    }

    /// A snapshot must carry the merge bookkeeping, not just the views.
    ///
    /// Subscribers own no view of their own — that IS the memory saving — so
    /// nothing else in the snapshot mentions them. Dropping them on restore
    /// would silently un-merge the tenant: each subscriber's `_00_query` row
    /// and heartbeat carry on looking healthy while it never receives another
    /// delta, which reads as a list frozen at whatever it held before the
    /// restart.
    #[test]
    fn a_snapshot_carries_subscribers_the_merge_claim_and_detachment() {
        let mut circuit = Circuit::new();
        circuit.store.ensure_collection("thread");
        circuit
            .store
            .apply_change(&Change::create("thread", "t1", json!({ "title": "a" })));

        circuit.add_query(scan_query("owner", "thread"), None, None);
        circuit.claim_merge_key("mk1:deadbeef".to_string(), "owner".to_string());
        // Two joiners, one of which arrives spelled with its table prefix.
        circuit.attach_subscriber("owner", "sub1".to_string(), "user:alice".to_string());
        circuit.attach_subscriber(
            "owner",
            "_00_query:sub2".to_string(),
            "user:bob".to_string(),
        );

        let restored = Circuit::restore(&circuit.save().unwrap()).unwrap();

        assert_eq!(
            restored.subscribers_of("owner"),
            &[
                Subscriber { query_id: "sub1".into(), auth_id: "user:alice".into() },
                Subscriber { query_id: "sub2".into(), auth_id: "user:bob".into() },
            ],
            "subscribers restore, canonicalised"
        );
        assert_eq!(
            restored.owner_for_merge_key("mk1:deadbeef"),
            Some("owner"),
            "the claim restores, so a later registration of the same \
             computation still attaches instead of building a second graph"
        );
        // Merging is configuration, not state: a restore must not resurrect
        // whatever the snapshot was written under.
        assert!(!restored.merge_views(), "policy is re-applied by the shell");

        // A delta still reaches all three.
        let mut restored = restored;
        let ids: Vec<String> = restored
            .step(ChangeSet {
                changes: vec![Change::create("thread", "t2", json!({ "title": "b" }))],
            })
            .into_iter()
            .map(|d| d.query_id)
            .collect();
        assert_eq!(ids.len(), 3, "owner + 2 subscribers, got {ids:?}");
        for want in ["owner", "sub1", "sub2"] {
            assert!(ids.iter().any(|id| id == want), "{want} missing from {ids:?}");
        }
    }

    /// A detached owner keeps its graph serving subscribers but must not
    /// resume publishing to its own deleted `_00_query` row after a restart.
    #[test]
    fn a_snapshot_keeps_a_detached_owner_detached() {
        let mut circuit = Circuit::new();
        circuit.store.ensure_collection("thread");
        circuit.add_query(scan_query("owner", "thread"), None, None);
        circuit.attach_subscriber("owner", "sub1".to_string(), "user:alice".to_string());
        // The owner's own row went away; the graph stays for sub1.
        assert!(!circuit.detach_subscriber("owner"), "graph outlives its owner");

        let mut restored = Circuit::restore(&circuit.save().unwrap()).unwrap();

        let ids: Vec<String> = restored
            .step(ChangeSet {
                changes: vec![Change::create("thread", "t1", json!({ "title": "a" }))],
            })
            .into_iter()
            .map(|d| d.query_id)
            .collect();
        assert_eq!(ids, vec!["sub1".to_string()], "owner must stay detached");
    }

    /// A snapshot written before the flat row encoding must still restore.
    ///
    /// The rows are stored as flat bytes in memory but serialize to the shape
    /// the old `HashMap<String, Sp00kyValue>` produced, so the encoding never
    /// reaches disk and no snapshot migration is needed.
    #[test]
    fn restores_a_snapshot_written_before_the_flat_encoding() {
        // Captured from the pre-flat-encoding serializer: rows are externally
        // tagged `Sp00kyValue`, and `catchup_xor`/`scratch` were already skipped.
        let legacy = r#"{
            "store": {
                "collections": {
                    "thread": {
                        "name": "thread",
                        "zset": { "thread:t0": 1, "thread:t1": 1 },
                        "rows": {
                            "t0": {"Object":{"title":{"Str":"first"},"_00_rv":{"Int":1}}},
                            "t1": {"Object":{"title":{"Str":"second"},"_00_rv":{"Int":4}}}
                        }
                    }
                }
            },
            "queries": []
        }"#;

        let mut circuit = Circuit::restore(legacy).expect("legacy snapshot must restore");
        assert_eq!(circuit.table_record_counts(), vec![("thread".into(), 2)]);
        assert_eq!(
            circuit.store.get_row_by_key("thread:t0").get("title").as_str(),
            Some("first")
        );
        // `_00_rv` was lifted into the record header on load, not left behind.
        assert_eq!(circuit.max_row_versions()["thread"], 4);

        // The restored rows must hash the same as if they had been ingested.
        let mut fresh = Circuit::new();
        fresh.store.ensure_collection("thread");
        fresh.store.apply_change(&Change::create(
            "thread",
            "t0",
            json!({ "title": "first", "_00_rv": 1 }),
        ));
        fresh.store.apply_change(&Change::create(
            "thread",
            "t1",
            json!({ "title": "second", "_00_rv": 4 }),
        ));
        assert_eq!(circuit.compute_table_hashes(), fresh.compute_table_hashes());

        // And the catch-up accumulator agrees once re-seeded, as bootstrap does.
        circuit.reseed_catchup_hashes();
        assert_eq!(
            circuit.compute_catchup_hashes(),
            fresh.compute_catchup_hashes()
        );
    }

    /// An unreadable snapshot must surface as an error so the runtime falls
    /// back to a full rebuild, rather than silently restoring a partial store.
    #[test]
    fn unreadable_snapshots_error_rather_than_half_restore() {
        for bad in [
            "",
            "not json at all",
            r#"{"store":{"collections":{}}}"#, // missing `queries`
            r#"{"store":{"collections":{"t":{"name":"t","zset":{},"rows":{"a":"not a value"}}}},"queries":[]}"#,
        ] {
            assert!(
                Circuit::restore(bad).is_err(),
                "expected {bad:?} to be rejected so the caller rebuilds"
            );
        }
    }

    /// The dump is capped and sorted so the caller can tell "not examined"
    /// from "absent" — an unordered truncation would read as missing rows.
    #[test]
    fn dump_table_rows_returns_a_sorted_capped_prefix() {
        let mut circuit = Circuit::new();
        circuit.store.ensure_collection("thread");
        for i in 0..10 {
            circuit
                .store
                .apply_change(&Change::create("thread", &format!("t{i}"), json!({ "n": i })));
        }

        let (rows, truncated) = circuit.dump_table_rows("thread", 4);
        assert!(truncated);
        assert_eq!(rows.len(), 4);
        let ids: Vec<&str> = rows.iter().map(|(id, _)| id.as_str()).collect();
        assert_eq!(ids, ["t0", "t1", "t2", "t3"], "must be the sorted prefix");

        let (all, truncated) = circuit.dump_table_rows("thread", 10);
        assert!(!truncated, "an exact fit is not truncated");
        assert_eq!(all.len(), 10);

        let (none, truncated) = circuit.dump_table_rows("nosuchtable", 10);
        assert!(none.is_empty());
        assert!(!truncated);
    }

    /// SELECT * FROM <table> ORDER BY <field> <dir> LIMIT <limit> START <start>
    fn limit_start_query(
        id: &str,
        table: &str,
        limit: usize,
        start: usize,
        order_field: &str,
        dir: &str,
    ) -> QueryPlan {
        QueryPlan {
            id: id.to_string(),
            root: OperatorPlan::Limit {
                input: Box::new(OperatorPlan::Scan {
                    table: table.to_string(),
                }),
                limit,
                start,
                order_by: Some(vec![OrderSpec {
                    field: Path::new(order_field),
                    direction: dir.to_string(),
                }]),
            },
        }
    }

    #[test]
    fn limit_start_window_paginates_by_offset() {
        let mut circuit = Circuit::new();
        circuit.load(vec![
            Record::new("posts", "post:1", json!({"score": 10})),
            Record::new("posts", "post:2", json!({"score": 40})),
            Record::new("posts", "post:3", json!({"score": 30})),
            Record::new("posts", "post:4", json!({"score": 20})),
        ]);

        // ORDER BY score DESC → [40, 30, 20, 10]. LIMIT 2 START 1 → [30, 20].
        circuit.add_query(limit_start_query("p1", "posts", 2, 1, "score", "DESC"), None, None);

        let view = circuit.get_view("p1").unwrap();
        assert!(view.cache.is_present("posts:3")); // score 30
        assert!(view.cache.is_present("posts:4")); // score 20
        assert!(!view.cache.is_present("posts:2")); // score 40 — skipped by START 1
        assert!(!view.cache.is_present("posts:1")); // score 10 — below the window
    }

    #[test]
    fn limit_start_window_shifts_on_insert() {
        let mut circuit = Circuit::new();
        circuit.load(vec![
            Record::new("posts", "post:1", json!({"score": 10})),
            Record::new("posts", "post:2", json!({"score": 40})),
            Record::new("posts", "post:3", json!({"score": 20})),
        ]);

        // [40, 20, 10]. LIMIT 1 START 1 → 2nd row = post:3 (score 20).
        circuit.add_query(limit_start_query("p1", "posts", 1, 1, "score", "DESC"), None, None);
        assert!(circuit.get_view("p1").unwrap().cache.is_present("posts:3"));

        // Insert score 30 → [40, 30, 20, 10]. 2nd row is now post:4 (30); post:3
        // is pushed out of the window.
        let deltas = circuit.step(ChangeSet {
            changes: vec![Change::create("posts", "post:4", json!({"score": 30}))],
        });
        assert_eq!(deltas.len(), 1);
        assert!(deltas[0].additions.contains(&"posts:4".to_string()));
        assert!(deltas[0].removals.contains(&"posts:3".to_string()));

        let view = circuit.get_view("p1").unwrap();
        assert!(view.cache.is_present("posts:4"));
        assert!(!view.cache.is_present("posts:3"));
    }

    #[test]
    fn load_and_add_query_returns_initial_delta() {
        let mut circuit = Circuit::new();
        circuit.load(vec![
            Record::new("users", "user:1", json!({"name": "alice"})),
            Record::new("users", "user:2", json!({"name": "bob"})),
        ]);

        let delta = circuit.add_query(scan_query("q1", "users"), None, None);

        assert!(delta.is_some());
        let d = delta.unwrap();
        assert_eq!(d.additions.len(), 2);
    }

    #[test]
    fn step_returns_delta_for_affected_queries() {
        let mut circuit = Circuit::new();
        circuit.load(vec![Record::new(
            "users",
            "user:1",
            json!({"name": "alice"}),
        )]);
        circuit.add_query(scan_query("q1", "users"), None, None);

        let changes = ChangeSet {
            changes: vec![Change::create("users", "user:2", json!({"name": "bob"}))],
        };
        let deltas = circuit.step(changes);

        assert_eq!(deltas.len(), 1);
        assert!(deltas[0].additions.contains(&"users:2".to_string()));
    }

    #[test]
    fn step_returns_empty_for_unaffected_queries() {
        let mut circuit = Circuit::new();
        circuit.add_query(scan_query("q1", "users"), None, None);

        let changes = ChangeSet {
            changes: vec![Change::create("posts", "post:1", json!({"title": "hi"}))],
        };
        let deltas = circuit.step(changes);

        assert!(deltas.is_empty());
    }

    #[test]
    fn remove_query_stops_producing_deltas() {
        let mut circuit = Circuit::new();
        circuit.add_query(scan_query("q1", "users"), None, None);
        circuit.remove_query("q1");

        let changes = ChangeSet {
            changes: vec![Change::create(
                "users",
                "user:1",
                json!({"name": "alice"}),
            )],
        };
        let deltas = circuit.step(changes);
        assert!(deltas.is_empty());
    }

    #[test]
    fn roundtrip_incremental_equals_snapshot() {
        let mut circuit = Circuit::new();
        circuit.add_query(scan_query("q1", "users"), None, None);

        // Step 1: add alice
        circuit.step(ChangeSet {
            changes: vec![Change::create(
                "users",
                "user:1",
                json!({"name": "alice"}),
            )],
        });

        // Step 2: add bob
        circuit.step(ChangeSet {
            changes: vec![Change::create("users", "user:2", json!({"name": "bob"}))],
        });

        // Step 3: remove alice
        circuit.step(ChangeSet {
            changes: vec![Change::delete("users", "user:1")],
        });

        // Incremental result
        let view = circuit.get_view("q1").unwrap();
        assert!(view.cache.is_present("users:2"));
        assert!(!view.cache.is_present("users:1"));

        // Fresh snapshot should agree
        let mut fresh = Circuit::new();
        fresh.load(vec![Record::new(
            "users",
            "user:2",
            json!({"name": "bob"}),
        )]);
        fresh.add_query(scan_query("q1", "users"), None, None);
        let fresh_view = fresh.get_view("q1").unwrap();

        assert_eq!(view.cache, fresh_view.cache);
    }

    // ── Subquery change detection tests ─────────────────────────────

    /// Helper: build a query with a subquery projection.
    /// SELECT * , (SELECT * FROM child_table) AS children FROM parent_table
    fn subquery_query(id: &str, parent_table: &str, child_table: &str) -> QueryPlan {
        QueryPlan {
            id: id.to_string(),
            root: OperatorPlan::Project {
                input: Box::new(OperatorPlan::Scan {
                    table: parent_table.to_string(),
                }),
                projections: vec![
                    Projection::All,
                    Projection::Subquery {
                        alias: "children".to_string(),
                        plan: Box::new(OperatorPlan::Scan {
                            table: child_table.to_string(),
                        }),
                        parent_key: None,
                    },
                ],
            },
        }
    }

    // A reverse one-to-many subquery (comments-style): child.<fk> = parent.id,
    // with a real parent_key set (unlike `subquery_query`, which leaves it None).
    fn reverse_subquery_query(
        id: &str,
        parent_table: &str,
        child_table: &str,
        child_fk: &str,
    ) -> QueryPlan {
        QueryPlan {
            id: id.to_string(),
            root: OperatorPlan::Project {
                input: Box::new(OperatorPlan::Scan {
                    table: parent_table.to_string(),
                }),
                projections: vec![
                    Projection::All,
                    Projection::Subquery {
                        alias: "children".to_string(),
                        plan: Box::new(OperatorPlan::Scan {
                            table: child_table.to_string(),
                        }),
                        parent_key: Some(crate::operator::plan::SubqueryParentKey {
                            child_field: child_fk.to_string(),
                            parent_field: "id".to_string(),
                        }),
                    },
                ],
            },
        }
    }

    // Regression for the production "comments vanish" bug. A `.related()`
    // reverse one-to-many child (e.g. a thread's comments) must be emitted in
    // the ViewDelta's `subquery_items` — that's what the edge writer turns into
    // a `_00_list_ref` edge the client syncs. The initial registration snapshot
    // (add_query) must include already-present children, or a page reload shows
    // no comments even though they exist.
    #[test]
    fn reverse_subquery_snapshot_emits_child_edges() {
        let mut circuit = Circuit::new();
        circuit.load(vec![
            Record::new("thread", "thread:1", json!({ "title": "Hello" })),
            Record::new(
                "comment",
                "comment:1",
                json!({ "text": "hi", "thread": "thread:1" }),
            ),
        ]);

        let delta = circuit
            .add_query(reverse_subquery_query("q1", "thread", "comment", "thread"), None, None)
            .expect("registration must yield an initial delta");

        assert!(
            delta.subquery_items.iter().any(|it| it.id == "comment:1"),
            "initial snapshot must emit the reverse subquery child as a subquery_item so an \
             _00_list_ref edge is written — else comments never reach the client (prod bug). \
             got subquery_items: {:?}",
            delta.subquery_items
        );
    }

    // ssp-cf (Cloudflare Durable Object) hibernates and rehydrates the circuit
    // via `save()`/`restore()`. This mimics that path: a node with data in its
    // store is saved, restored, and only THEN does a client register the
    // thread-detail query. The reverse `comments` subquery must still emit the
    // child from the restored store. If restore drops the child collection
    // (kept alive only by an active view before), comments never reach clients
    // — the production bug where authors work but comments vanish.
    #[test]
    fn reverse_subquery_emits_after_save_restore() {
        let mut a = Circuit::new();
        a.load(vec![
            Record::new("thread", "thread:1", json!({ "title": "Hello" })),
            Record::new(
                "comment",
                "comment:1",
                json!({ "text": "hi", "thread": "thread:1" }),
            ),
        ]);

        let blob = a.save().expect("save");
        let mut b = Circuit::restore(&blob).expect("restore");

        let delta = b
            .add_query(reverse_subquery_query("q1", "thread", "comment", "thread"), None, None)
            .expect("registration on the restored circuit must yield an initial delta");

        assert!(
            delta.subquery_items.iter().any(|it| it.id == "comment:1"),
            "after save/restore (ssp-cf rehydration), the reverse `comments` subquery must \
             still emit comment:1 — else the restored node writes no comment edge and comments \
             vanish. subquery_items: {:?}",
            delta.subquery_items
        );
    }

    #[test]
    fn subquery_table_create_emits_content_update() {
        let mut circuit = Circuit::new();

        // Load a parent record
        circuit.load(vec![Record::new(
            "thread",
            "thread:1",
            json!({"title": "Hello"}),
        )]);

        // Register a query with a subquery on "comment"
        let delta = circuit.add_query(subquery_query("q1", "thread", "comment"), None, None);
        assert!(delta.is_some());
        let initial_hash = delta.unwrap().result_hash;

        // Create a comment (subquery table change)
        let deltas = circuit.step(ChangeSet {
            changes: vec![Change::create(
                "comment",
                "comment:1",
                json!({"text": "hi", "thread": "thread:1"}),
            )],
        });

        // Must emit a ViewDelta with content updates
        assert_eq!(deltas.len(), 1);
        let d = &deltas[0];
        assert_eq!(d.query_id, "q1");
        assert!(d.additions.is_empty(), "no membership additions");
        assert!(d.removals.is_empty(), "no membership removals");
        assert!(d.updates.contains(&"thread:1".to_string()), "parent record is content-updated");
        assert_ne!(d.result_hash, initial_hash, "hash must change");
    }

    #[test]
    fn subquery_table_delete_emits_content_update() {
        let mut circuit = Circuit::new();

        circuit.load(vec![
            Record::new("thread", "thread:1", json!({"title": "Hello"})),
            Record::new("comment", "comment:1", json!({"text": "hi"})),
        ]);

        circuit.add_query(subquery_query("q1", "thread", "comment"), None, None);

        // Delete the comment
        let deltas = circuit.step(ChangeSet {
            changes: vec![Change::delete("comment", "comment:1")],
        });

        assert_eq!(deltas.len(), 1);
        assert!(deltas[0].updates.contains(&"thread:1".to_string()));
    }

    #[test]
    fn subquery_table_update_emits_content_update() {
        let mut circuit = Circuit::new();

        circuit.load(vec![
            Record::new("thread", "thread:1", json!({"title": "Hello"})),
            Record::new("comment", "comment:1", json!({"text": "hi"})),
        ]);

        circuit.add_query(subquery_query("q1", "thread", "comment"), None, None);

        // Update the comment (Operation::Update has weight 0)
        let deltas = circuit.step(ChangeSet {
            changes: vec![Change::update(
                "comment",
                "comment:1",
                json!({"text": "updated"}),
            )],
        });

        assert_eq!(deltas.len(), 1);
        assert!(deltas[0].updates.contains(&"thread:1".to_string()));
    }

    #[test]
    fn scan_query_emits_content_update_for_in_set_row() {
        // Regression guard for the cross-session title-edit path:
        // when a row that's already in the result set has its content
        // updated (Operation::Update with weight 0), the circuit must
        // emit a ViewDelta with that row's id in `updates`. Otherwise
        // the WasmStreamUpdate.updates is empty and the client never
        // re-queries the local DB, leaving stale data in `useQuery`.
        let mut circuit = Circuit::new();

        circuit.load(vec![Record::new(
            "thread",
            "thread:1",
            json!({"title": "Hello"}),
        )]);

        let initial = circuit.add_query(scan_query("q1", "thread"), None, None);
        let initial_hash = initial.unwrap().result_hash;

        // Content-only update: title changes, row stays in result set.
        let deltas = circuit.step(ChangeSet {
            changes: vec![Change::update(
                "thread",
                "thread:1",
                json!({"title": "Renamed"}),
            )],
        });

        assert_eq!(deltas.len(), 1);
        let d = &deltas[0];
        assert!(d.additions.is_empty(), "no membership additions");
        assert!(d.removals.is_empty(), "no membership removals");
        assert!(
            d.updates.contains(&"thread:1".to_string()),
            "in-set row content update must surface in `updates`, got {:?}",
            d.updates
        );
        // result_hash doesn't change for content-only updates on a plain
        // scan because the cache keys are unchanged; that's expected and
        // is exactly why we need the `updates` field as the change signal.
        assert_eq!(d.result_hash, initial_hash);
    }

    // ── Filter re-evaluation on Operation::Update ─────────────────
    //
    // Regression guards for the cross-user realtime sync fix.
    // Operation::Update has weight 0, so the Scan operator emits an
    // empty delta and the Filter never re-evaluates the predicate.
    // Without a fix, a row that newly matches (publish) or newly
    // fails (unpublish) the filter predicate via a content change is
    // invisible to the circuit's step output.

    /// Build a `Filter(Scan(table), predicate)` plan — minimal shape
    /// for testing predicate re-evaluation on Updates.
    fn filter_scan_query(
        id: &str,
        table: &str,
        predicate: crate::operator::predicate::Predicate,
    ) -> QueryPlan {
        QueryPlan {
            id: id.to_string(),
            root: OperatorPlan::Filter {
                input: Box::new(OperatorPlan::Scan {
                    table: table.to_string(),
                }),
                predicate,
            },
        }
    }

    #[test]
    fn content_update_admits_row_into_filtered_view() {
        use crate::operator::predicate::Predicate;
        use crate::types::Path;

        let mut circuit = Circuit::new();
        circuit.load(vec![Record::new(
            "thread",
            "thread:1",
            json!({"title": "Hello", "published": false}),
        )]);

        // View admits only published threads. Initial snapshot is
        // empty because the only row has published=false.
        let plan = filter_scan_query(
            "q1",
            "thread",
            Predicate::Eq {
                field: Path::new("published"),
                value: json!(true),
            },
        );
        let initial = circuit.add_query(plan, None, None);
        assert!(
            initial.is_none(),
            "filtered view is empty at registration time"
        );

        // Alice flips published to true. Operation::Update has weight 0
        // in the table delta; before the fix, the Scan emitted an empty
        // delta and the Filter never re-evaluated. With the fix,
        // evaluate_key catches the transition.
        let deltas = circuit.step(ChangeSet {
            changes: vec![Change::update(
                "thread",
                "thread:1",
                json!({"title": "Hello", "published": true}),
            )],
        });

        assert_eq!(deltas.len(), 1, "view should be affected by publish");
        let d = &deltas[0];
        assert!(
            d.additions.contains(&"thread:1".to_string()),
            "publish must surface as an addition; got additions={:?}, updates={:?}",
            d.additions,
            d.updates
        );
        assert!(d.removals.is_empty());
    }

    #[test]
    fn content_update_removes_row_from_filtered_view() {
        use crate::operator::predicate::Predicate;
        use crate::types::Path;

        let mut circuit = Circuit::new();
        circuit.load(vec![Record::new(
            "thread",
            "thread:1",
            json!({"title": "Hello", "published": true}),
        )]);

        // View admits only published threads. Initial snapshot has the
        // row.
        let plan = filter_scan_query(
            "q1",
            "thread",
            Predicate::Eq {
                field: Path::new("published"),
                value: json!(true),
            },
        );
        let initial = circuit.add_query(plan, None, None);
        assert!(initial.is_some());

        // Alice unpublishes. Without the fix, the row stays in
        // view.cache forever because Scan emits nothing for Update.
        let deltas = circuit.step(ChangeSet {
            changes: vec![Change::update(
                "thread",
                "thread:1",
                json!({"title": "Hello", "published": false}),
            )],
        });

        assert_eq!(deltas.len(), 1, "view should be affected by unpublish");
        let d = &deltas[0];
        assert!(
            d.removals.contains(&"thread:1".to_string()),
            "unpublish must surface as a removal; got removals={:?}, updates={:?}",
            d.removals,
            d.updates
        );
        assert!(d.additions.is_empty());
    }

    #[test]
    fn filtered_view_drops_row_on_delete() {
        // Reproduces the down-sync delete gap: a record matching a WHERE filter
        // is deleted; the filtered view must emit a removal and drop it from the
        // cache. If the circuit removes the row before the Filter re-evaluates
        // the retraction, the `-1` is silently dropped and the row lingers.
        use crate::operator::predicate::Predicate;
        use crate::types::Path;

        let mut circuit = Circuit::new();
        circuit.load(vec![Record::new(
            "thread",
            "thread:1",
            json!({"title": "Hello", "published": true}),
        )]);

        let plan = filter_scan_query(
            "q1",
            "thread",
            Predicate::Eq {
                field: Path::new("published"),
                value: json!(true),
            },
        );
        circuit.add_query(plan, None, None);
        assert!(
            circuit.get_view("q1").unwrap().cache.contains_key("thread:1"),
            "row should start in the view",
        );

        let deltas = circuit.step(ChangeSet {
            changes: vec![Change::delete("thread", "thread:1")],
        });

        assert_eq!(deltas.len(), 1, "delete should affect the filtered view");
        assert!(
            deltas[0].removals.contains(&"thread:1".to_string()),
            "delete must surface as a removal; got removals={:?}",
            deltas[0].removals,
        );
        assert!(
            !circuit.get_view("q1").unwrap().cache.contains_key("thread:1"),
            "row must be dropped from the view cache after delete",
        );
    }

    /// The whitepawn `stream_presence` permission: owner OR public-broadcast OR
    /// admin-share, the last two as IN-subqueries that lower to SemiJoins.
    const STREAM_PRESENCE_PERM: &str = "( $access = \"account\" AND owner = $auth.id ) OR owner IN (SELECT VALUE owner FROM broadcast WHERE share_visibility = 'public') OR ( $access = \"account\" AND owner IN (SELECT VALUE broadcast.owner FROM broadcast_share WHERE user = $auth.id AND role = 'admin') )";

    /// `SELECT * FROM stream_presence WHERE owner = $owner` with the real
    /// permission injected: `SemiJoin(view, Distinct(Union(...)), on id=id)`.
    fn stream_presence_query(id: &str, params: &serde_json::Value) -> QueryPlan {
        use crate::operator::predicate::Predicate;
        use crate::permission_inject::inject_permissions;

        let mut root = OperatorPlan::Filter {
            input: Box::new(OperatorPlan::Scan {
                table: "stream_presence".into(),
            }),
            predicate: Predicate::Eq {
                field: Path::new("owner"),
                value: json!({"$param": "owner"}),
            },
        };
        let mut perms = HashMap::new();
        perms.insert("stream_presence".to_string(), STREAM_PRESENCE_PERM.to_string());
        inject_permissions(&mut root, &perms, Some(params)).unwrap();
        QueryPlan {
            id: id.to_string(),
            root,
        }
    }

    #[test]
    fn content_update_survives_semijoin_permission_view() {
        // Regression for the stream_presence live-query outage: the relay
        // keep-alive UPSERTs an existing row (Operation::Update, weight 0).
        // The membership re-evaluation pass walks the DAG with evaluate_key;
        // before SemiJoin implemented it, the permission-composed root
        // returned false and the pass synthesized a REMOVAL of the cached
        // row - the dashboard showed the device once, then it vanished on
        // the first keep-alive.
        let params = json!({"auth": {"id": "user:a"}, "access": "account", "owner": "user:a"});
        let mut circuit = Circuit::new();
        circuit.load(vec![Record::new(
            "stream_presence",
            "stream_presence:a",
            json!({"id": "stream_presence:a", "owner": "user:a", "online": true, "fen": "start"}),
        )]);

        let initial = circuit
            .add_query(stream_presence_query("q1", &params), Some(params), None)
            .expect("owner sees their device at registration");
        assert!(initial.additions.contains(&"stream_presence:a".to_string()));

        // Relay keep-alive: same row, new content.
        let deltas = circuit.step(ChangeSet {
            changes: vec![Change::update(
                "stream_presence",
                "stream_presence:a",
                json!({"id": "stream_presence:a", "owner": "user:a", "online": true, "fen": "e2e4"}),
            )],
        });

        assert_eq!(deltas.len(), 1, "keep-alive should reach the view");
        let d = &deltas[0];
        assert!(
            d.removals.is_empty(),
            "keep-alive must not evict the row; got removals={:?}",
            d.removals
        );
        assert!(
            d.updates.contains(&"stream_presence:a".to_string()),
            "keep-alive must surface as a content update; got updates={:?}",
            d.updates
        );
    }

    #[test]
    fn content_update_survives_semijoin_witness_branch() {
        // Same regression through the public-broadcast OR branch: the viewer
        // is NOT the owner, so admission comes from the inner SemiJoin's
        // witness check against its integrated broadcast state (a different
        // key space, where the fresh input eval is meaningless).
        let params = json!({"auth": {"id": "user:viewer"}, "access": "account", "owner": "user:b"});
        let mut circuit = Circuit::new();
        circuit.load(vec![
            Record::new(
                "stream_presence",
                "stream_presence:b",
                json!({"id": "stream_presence:b", "owner": "user:b", "online": true, "fen": "start"}),
            ),
            Record::new(
                "broadcast",
                "broadcast:b",
                json!({"id": "broadcast:b", "owner": "user:b", "share_visibility": "public"}),
            ),
        ]);

        let initial = circuit
            .add_query(stream_presence_query("q1", &params), Some(params), None)
            .expect("public broadcast admits the viewer at registration");
        assert!(initial.additions.contains(&"stream_presence:b".to_string()));

        let deltas = circuit.step(ChangeSet {
            changes: vec![Change::update(
                "stream_presence",
                "stream_presence:b",
                json!({"id": "stream_presence:b", "owner": "user:b", "online": true, "fen": "e2e4"}),
            )],
        });

        assert_eq!(deltas.len(), 1);
        let d = &deltas[0];
        assert!(
            d.removals.is_empty(),
            "public viewer must keep the row on keep-alive; got removals={:?}",
            d.removals
        );
        assert!(d.updates.contains(&"stream_presence:b".to_string()));
    }

    #[test]
    fn content_update_membership_transition_through_semijoin() {
        // evaluate_key must still detect real transitions: the row's owner
        // changes away from the querying user (and has no witness), so the
        // permission now rejects it and the pass must synthesize a removal.
        let params = json!({"auth": {"id": "user:a"}, "access": "account", "owner": "user:a"});
        let mut circuit = Circuit::new();
        circuit.load(vec![Record::new(
            "stream_presence",
            "stream_presence:a",
            json!({"id": "stream_presence:a", "owner": "user:a", "online": true}),
        )]);

        circuit
            .add_query(stream_presence_query("q1", &params), Some(params), None)
            .expect("row visible at registration");

        let deltas = circuit.step(ChangeSet {
            changes: vec![Change::update(
                "stream_presence",
                "stream_presence:a",
                json!({"id": "stream_presence:a", "owner": "user:other", "online": true}),
            )],
        });

        assert_eq!(deltas.len(), 1);
        assert!(
            deltas[0].removals.contains(&"stream_presence:a".to_string()),
            "owner change must surface as a removal; got removals={:?}, updates={:?}",
            deltas[0].removals,
            deltas[0].updates
        );
    }

    #[test]
    fn content_update_no_match_change_still_emits_update() {
        // Companion guard: when an in-cache row's content changes but
        // the predicate result is unchanged (e.g. title rename on an
        // already-published thread), the row should appear in `updates`
        // (the pre-existing content-update path), not `additions` or
        // `removals`.
        use crate::operator::predicate::Predicate;
        use crate::types::Path;

        let mut circuit = Circuit::new();
        circuit.load(vec![Record::new(
            "thread",
            "thread:1",
            json!({"title": "Original", "published": true}),
        )]);

        let plan = filter_scan_query(
            "q1",
            "thread",
            Predicate::Eq {
                field: Path::new("published"),
                value: json!(true),
            },
        );
        circuit.add_query(plan, None, None);

        let deltas = circuit.step(ChangeSet {
            changes: vec![Change::update(
                "thread",
                "thread:1",
                json!({"title": "Renamed", "published": true}),
            )],
        });

        assert_eq!(deltas.len(), 1);
        let d = &deltas[0];
        assert!(d.additions.is_empty(), "no membership change expected");
        assert!(d.removals.is_empty(), "no membership change expected");
        assert!(
            d.updates.contains(&"thread:1".to_string()),
            "in-cache content change should surface in `updates`"
        );
    }

    #[test]
    fn no_subquery_means_no_spurious_updates() {
        let mut circuit = Circuit::new();

        circuit.load(vec![Record::new(
            "thread",
            "thread:1",
            json!({"title": "Hello"}),
        )]);

        // Simple scan query — no subqueries
        circuit.add_query(scan_query("q1", "thread"), None, None);

        // Change to an unrelated table should not affect this query
        let deltas = circuit.step(ChangeSet {
            changes: vec![Change::create("comment", "comment:1", json!({"text": "hi"}))],
        });

        assert!(deltas.is_empty());
    }

    #[test]
    fn empty_view_cache_no_update_on_subquery_change() {
        let mut circuit = Circuit::new();

        // Register query but load NO parent records
        circuit.add_query(subquery_query("q1", "thread", "comment"), None, None);

        // Create a comment — no parent records to update
        let deltas = circuit.step(ChangeSet {
            changes: vec![Change::create("comment", "comment:1", json!({"text": "hi"}))],
        });

        assert!(deltas.is_empty());
    }

    #[test]
    fn self_referencing_subquery_detects_changes() {
        let mut circuit = Circuit::new();

        circuit.load(vec![Record::new(
            "thread",
            "thread:root",
            json!({"title": "Root", "is_root": true}),
        )]);

        // Query: SELECT *, (SELECT * FROM thread WHERE ...) AS children FROM thread
        // "thread" is both primary AND subquery table
        circuit.add_query(subquery_query("q1", "thread", "thread"), None, None);

        // Create a child thread — this is both a membership change (new thread in Scan)
        // AND a subquery table change
        let deltas = circuit.step(ChangeSet {
            changes: vec![Change::create(
                "thread",
                "thread:child",
                json!({"title": "Child", "parent": "thread:root"}),
            )],
        });

        assert_eq!(deltas.len(), 1);
        let d = &deltas[0];
        // Should have both: membership addition (new thread) AND content update (root's subquery changed)
        assert!(d.additions.contains(&"thread:child".to_string()));
        assert!(d.updates.contains(&"thread:root".to_string()));
    }

    // ── Subquery item tracking tests ─────────────────────────────

    use crate::operator::plan::SubqueryParentKey;
    use crate::operator::predicate::Predicate;

    /// Helper: build a query with a subquery projection that has parent_key set.
    /// SELECT *, (SELECT * FROM child_table WHERE child_fk = $parent.id) AS alias FROM parent_table
    fn subquery_query_with_parent_key(
        id: &str,
        parent_table: &str,
        child_table: &str,
        alias: &str,
        child_field: &str,
    ) -> QueryPlan {
        QueryPlan {
            id: id.to_string(),
            root: OperatorPlan::Project {
                input: Box::new(OperatorPlan::Scan {
                    table: parent_table.to_string(),
                }),
                projections: vec![
                    Projection::All,
                    Projection::Subquery {
                        alias: alias.to_string(),
                        plan: Box::new(OperatorPlan::Filter {
                            input: Box::new(OperatorPlan::Scan {
                                table: child_table.to_string(),
                            }),
                            predicate: Predicate::Eq {
                                field: crate::types::Path::new(child_field),
                                value: json!({"$param": "parent.id"}),
                            },
                        }),
                        parent_key: Some(SubqueryParentKey {
                            child_field: child_field.to_string(),
                            parent_field: "id".to_string(),
                        }),
                    },
                ],
            },
        }
    }

    /// Helper: build a reverse one-to-one query (parent stores the child's id).
    /// SELECT *, (SELECT * FROM child_table WHERE id = $parent.<parent_fk> LIMIT 1)[0] AS alias FROM parent_table
    fn reverse_one_to_one_query(
        id: &str,
        parent_table: &str,
        child_table: &str,
        alias: &str,
        parent_fk: &str,
    ) -> QueryPlan {
        QueryPlan {
            id: id.to_string(),
            root: OperatorPlan::Project {
                input: Box::new(OperatorPlan::Scan {
                    table: parent_table.to_string(),
                }),
                projections: vec![
                    Projection::All,
                    Projection::Subquery {
                        alias: alias.to_string(),
                        plan: Box::new(OperatorPlan::Filter {
                            input: Box::new(OperatorPlan::Scan {
                                table: child_table.to_string(),
                            }),
                            predicate: Predicate::Eq {
                                field: crate::types::Path::new("id"),
                                value: json!({ "$param": format!("parent.{parent_fk}") }),
                            },
                        }),
                        parent_key: Some(SubqueryParentKey {
                            child_field: "id".to_string(),
                            parent_field: parent_fk.to_string(),
                        }),
                    },
                ],
            },
        }
    }

    #[test]
    fn initial_snapshot_tracks_reverse_one_to_one_subquery() {
        // Regression: top-level reverse one-to-one (e.g. thread.author -> user)
        // wasn't being tracked because pass 1 only handled child-has-FK-to-parent.
        let mut circuit = Circuit::new();

        circuit.load(vec![
            Record::new("user", "user:khadim", json!({"username": "khadim"})),
            Record::new("user", "user:lisa", json!({"username": "lisa"})),
            Record::new(
                "thread",
                "thread:1",
                json!({"title": "Hello", "author": "user:khadim"}),
            ),
            Record::new(
                "thread",
                "thread:2",
                json!({"title": "World", "author": "user:lisa"}),
            ),
        ]);

        let delta = circuit.add_query(
            reverse_one_to_one_query("q1", "thread", "user", "author", "author"),
            None,
            None,
        );

        let d = delta.expect("expected initial delta");
        assert_eq!(d.additions.len(), 2);

        let mut author_items: Vec<_> = d
            .subquery_items
            .iter()
            .filter(|i| i.op == SubqueryOp::Add && i.alias == "author")
            .collect();
        author_items.sort_by(|a, b| a.id.cmp(&b.id));
        assert_eq!(author_items.len(), 2);
        assert_eq!(author_items[0].id, "user:khadim");
        assert_eq!(author_items[0].parent_key, "thread:1");
        assert_eq!(author_items[1].id, "user:lisa");
        assert_eq!(author_items[1].parent_key, "thread:2");
    }

    #[test]
    fn step_adds_reverse_one_to_one_when_parent_added() {
        let mut circuit = Circuit::new();

        circuit.load(vec![Record::new(
            "user",
            "user:khadim",
            json!({"username": "khadim"}),
        )]);

        circuit.add_query(
            reverse_one_to_one_query("q1", "thread", "user", "author", "author"),
            None,
            None,
        );

        // New thread referencing an existing user — author must show up as Add.
        let deltas = circuit.step(ChangeSet {
            changes: vec![Change::create(
                "thread",
                "thread:1",
                json!({"title": "Hi", "author": "user:khadim"}),
            )],
        });

        assert_eq!(deltas.len(), 1);
        let adds: Vec<_> = deltas[0]
            .subquery_items
            .iter()
            .filter(|i| i.op == SubqueryOp::Add)
            .collect();
        assert_eq!(adds.len(), 1);
        assert_eq!(adds[0].id, "user:khadim");
        assert_eq!(adds[0].parent_key, "thread:1");
        assert_eq!(adds[0].alias, "author");
    }

    #[test]
    fn initial_snapshot_includes_subquery_items() {
        let mut circuit = Circuit::new();

        // Load parent + child records
        circuit.load(vec![
            Record::new("thread", "thread:1", json!({"title": "Hello"})),
            Record::new("comment", "comment:1", json!({"text": "hi", "thread": "thread:1"})),
            Record::new("comment", "comment:2", json!({"text": "yo", "thread": "thread:1"})),
        ]);

        let delta = circuit.add_query(
            subquery_query_with_parent_key("q1", "thread", "comment", "comments", "thread"),
            None,
            None,
        );

        assert!(delta.is_some());
        let d = delta.unwrap();
        assert_eq!(d.additions.len(), 1); // thread:1
        assert_eq!(d.subquery_items.len(), 2); // comment:1, comment:2
        assert!(d.subquery_items.iter().all(|item| item.op == SubqueryOp::Add));
        assert!(d.subquery_items.iter().all(|item| item.parent_key == "thread:1"));
        assert!(d.subquery_items.iter().all(|item| item.alias == "comments"));
    }

    #[test]
    fn step_adds_subquery_items_for_new_child() {
        let mut circuit = Circuit::new();

        circuit.load(vec![
            Record::new("thread", "thread:1", json!({"title": "Hello"})),
        ]);

        circuit.add_query(
            subquery_query_with_parent_key("q1", "thread", "comment", "comments", "thread"),
            None,
            None,
        );

        // Create a comment linked to thread:1
        let deltas = circuit.step(ChangeSet {
            changes: vec![Change::create(
                "comment",
                "comment:1",
                json!({"text": "hi", "thread": "thread:1"}),
            )],
        });

        assert_eq!(deltas.len(), 1);
        let d = &deltas[0];
        let adds: Vec<_> = d.subquery_items.iter().filter(|i| i.op == SubqueryOp::Add).collect();
        assert_eq!(adds.len(), 1);
        assert_eq!(adds[0].id, "comment:1");
        assert_eq!(adds[0].parent_key, "thread:1");
        assert_eq!(adds[0].alias, "comments");
    }

    #[test]
    fn step_removes_subquery_items_when_child_deleted() {
        let mut circuit = Circuit::new();

        circuit.load(vec![
            Record::new("thread", "thread:1", json!({"title": "Hello"})),
            Record::new("comment", "comment:1", json!({"text": "hi", "thread": "thread:1"})),
        ]);

        circuit.add_query(
            subquery_query_with_parent_key("q1", "thread", "comment", "comments", "thread"),
            None,
            None,
        );

        // Delete the comment
        let deltas = circuit.step(ChangeSet {
            changes: vec![Change::delete("comment", "comment:1")],
        });

        assert_eq!(deltas.len(), 1);
        let d = &deltas[0];
        let removes: Vec<_> = d.subquery_items.iter().filter(|i| i.op == SubqueryOp::Remove).collect();
        assert_eq!(removes.len(), 1);
        assert_eq!(removes[0].id, "comment:1");
    }

    #[test]
    fn step_removes_subquery_items_when_parent_removed() {
        let mut circuit = Circuit::new();

        circuit.load(vec![
            Record::new("thread", "thread:1", json!({"title": "Hello"})),
            Record::new("comment", "comment:1", json!({"text": "hi", "thread": "thread:1"})),
        ]);

        circuit.add_query(
            subquery_query_with_parent_key("q1", "thread", "comment", "comments", "thread"),
            None,
            None,
        );

        // Delete the parent thread — all child subquery items should be removed
        let deltas = circuit.step(ChangeSet {
            changes: vec![Change::delete("thread", "thread:1")],
        });

        assert_eq!(deltas.len(), 1);
        let d = &deltas[0];
        assert!(d.removals.contains(&"thread:1".to_string()));
        let removes: Vec<_> = d.subquery_items.iter().filter(|i| i.op == SubqueryOp::Remove).collect();
        assert_eq!(removes.len(), 1);
        assert_eq!(removes[0].id, "comment:1");
    }

    #[test]
    fn no_subquery_items_for_unrelated_child() {
        let mut circuit = Circuit::new();

        circuit.load(vec![
            Record::new("thread", "thread:1", json!({"title": "Hello"})),
        ]);

        circuit.add_query(
            subquery_query_with_parent_key("q1", "thread", "comment", "comments", "thread"),
            None,
            None,
        );

        // Create a comment linked to a non-existent thread
        let deltas = circuit.step(ChangeSet {
            changes: vec![Change::create(
                "comment",
                "comment:1",
                json!({"text": "hi", "thread": "thread:999"}),
            )],
        });

        // Should still emit delta (subquery table change bumps content_generation)
        // but NO subquery items since parent not in view
        assert_eq!(deltas.len(), 1);
        let adds: Vec<_> = deltas[0].subquery_items.iter().filter(|i| i.op == SubqueryOp::Add).collect();
        assert!(adds.is_empty());
    }

    // ── Nested subquery tracking tests ─────────────────────────────

    /// Helper: build a query with a nested subquery projection.
    /// SELECT *, (SELECT *, (SELECT * FROM grandchild_table WHERE id=$parent.gc_field LIMIT 1)[0] AS gc_alias
    ///   FROM child_table WHERE child_fk=$parent.id) AS child_alias FROM parent_table
    fn nested_subquery_query(
        id: &str,
        parent_table: &str,
        child_table: &str,
        child_alias: &str,
        child_fk: &str,
        grandchild_table: &str,
        grandchild_alias: &str,
        grandchild_fk_child: &str,
        grandchild_fk_parent: &str,
    ) -> QueryPlan {
        QueryPlan {
            id: id.to_string(),
            root: OperatorPlan::Project {
                input: Box::new(OperatorPlan::Scan {
                    table: parent_table.to_string(),
                }),
                projections: vec![
                    Projection::All,
                    Projection::Subquery {
                        alias: child_alias.to_string(),
                        plan: Box::new(OperatorPlan::Project {
                            input: Box::new(OperatorPlan::Filter {
                                input: Box::new(OperatorPlan::Scan {
                                    table: child_table.to_string(),
                                }),
                                predicate: Predicate::Eq {
                                    field: crate::types::Path::new(child_fk),
                                    value: json!({"$param": "parent.id"}),
                                },
                            }),
                            projections: vec![
                                Projection::All,
                                Projection::Subquery {
                                    alias: grandchild_alias.to_string(),
                                    plan: Box::new(OperatorPlan::Limit {
                                        input: Box::new(OperatorPlan::Filter {
                                            input: Box::new(OperatorPlan::Scan {
                                                table: grandchild_table.to_string(),
                                            }),
                                            predicate: Predicate::Eq {
                                                field: crate::types::Path::new(grandchild_fk_child),
                                                value: json!({"$param": "parent.id"}),
                                            },
                                        }),
                                        limit: 1,
                                        start: 0,
                                        order_by: None,
                                    }),
                                    parent_key: Some(SubqueryParentKey {
                                        child_field: grandchild_fk_child.to_string(),
                                        parent_field: grandchild_fk_parent.to_string(),
                                    }),
                                },
                            ],
                        }),
                        parent_key: Some(SubqueryParentKey {
                            child_field: child_fk.to_string(),
                            parent_field: "id".to_string(),
                        }),
                    },
                ],
            },
        }
    }

    #[test]
    fn nested_subquery_items_tracked_on_initial_snapshot() {
        let mut circuit = Circuit::new();

        // thread → comment → user (comment.author references user.id)
        circuit.load(vec![
            Record::new("thread", "thread:1", json!({"title": "Hello"})),
            Record::new("comment", "comment:1", json!({"text": "hi", "thread": "thread:1", "author": "user:alice"})),
            Record::new("user", "user:alice", json!({"name": "Alice", "id": "user:alice"})),
        ]);

        let delta = circuit.add_query(
            nested_subquery_query(
                "q1", "thread", "comment", "comments", "thread",
                "user", "author", "id", "author",
            ),
            None,
            None,
        );

        assert!(delta.is_some());
        let d = delta.unwrap();
        // Should have thread:1 in additions
        assert_eq!(d.additions.len(), 1);
        // Should track comment:1 (level-1) and user:alice (level-2) as subquery items
        let adds: Vec<_> = d.subquery_items.iter().filter(|i| i.op == SubqueryOp::Add).collect();
        assert!(adds.iter().any(|i| i.id == "comment:1" && i.alias == "comments"));
        assert!(adds.iter().any(|i| i.id == "user:alice" && i.alias == "author"));
    }

    #[test]
    fn nested_subquery_items_added_on_step() {
        let mut circuit = Circuit::new();

        circuit.load(vec![
            Record::new("thread", "thread:1", json!({"title": "Hello"})),
        ]);

        circuit.add_query(
            nested_subquery_query(
                "q1", "thread", "comment", "comments", "thread",
                "user", "author", "id", "author",
            ),
            None,
            None,
        );

        // First add a comment with an author reference
        circuit.step(ChangeSet {
            changes: vec![Change::create(
                "comment",
                "comment:1",
                json!({"text": "hi", "thread": "thread:1", "author": "user:alice"}),
            )],
        });

        // Then add the user record
        let deltas = circuit.step(ChangeSet {
            changes: vec![Change::create(
                "user",
                "user:alice",
                json!({"name": "Alice", "id": "user:alice"}),
            )],
        });

        assert_eq!(deltas.len(), 1);
        let adds: Vec<_> = deltas[0].subquery_items.iter().filter(|i| i.op == SubqueryOp::Add).collect();
        assert!(adds.iter().any(|i| i.id == "user:alice" && i.alias == "author"));
    }
}

#[cfg(test)]
mod snapshot_and_projection_tests {
    use super::*;
    use crate::operator::plan::{OperatorPlan, OrderSpec};
    use crate::operator::predicate::Predicate;
    use crate::types::Path;
    use serde_json::json;

    fn scan(id: &str, table: &str) -> QueryPlan {
        QueryPlan {
            id: id.to_string(),
            root: OperatorPlan::Scan {
                table: table.to_string(),
            },
        }
    }

    fn top(id: &str, table: &str, limit: usize, field: &str) -> QueryPlan {
        QueryPlan {
            id: id.to_string(),
            root: OperatorPlan::Limit {
                input: Box::new(OperatorPlan::Scan {
                    table: table.to_string(),
                }),
                limit,
                start: 0,
                order_by: Some(vec![OrderSpec {
                    field: Path::new(field),
                    direction: "desc".to_string(),
                }]),
            },
        }
    }

    fn filtered(id: &str, table: &str, field: &str, value: serde_json::Value) -> QueryPlan {
        QueryPlan {
            id: id.to_string(),
            root: OperatorPlan::Filter {
                input: Box::new(OperatorPlan::Scan {
                    table: table.to_string(),
                }),
                predicate: Predicate::Eq {
                    field: Path::new(field),
                    value,
                },
            },
        }
    }

    fn row(i: i64) -> serde_json::Value {
        json!({ "id": format!("thread:t{i}"), "_00_rv": i, "score": i, "published": i % 2 == 0, "pgn": "x".repeat(50) })
    }

    fn seeded(n: i64) -> Circuit {
        let mut c = Circuit::new();
        let changes = (0..n)
            .map(|i| Change::create("thread", &format!("t{i}"), row(i)))
            .collect();
        c.step(ChangeSet { changes });
        c
    }

    fn records(delta: &ViewDelta) -> Vec<String> {
        let mut r = delta.records.clone();
        r.sort();
        r
    }

    #[test]
    fn a_repeated_identical_ingest_produces_no_delta() {
        let mut c = seeded(3);
        c.add_query(scan("q", "thread"), None, None);
        let again = c.step(ChangeSet {
            changes: vec![Change::create("thread", "t1", row(1))],
        });
        assert!(again.is_empty(), "same bytes, no work: {again:?}");
        assert_eq!(c.dead_bytes(), 0, "and no orphaned arena bytes");
    }

    #[test]
    fn save_store_only_round_trips_rows_and_carries_no_views() {
        let mut a = seeded(5);
        a.add_query(scan("q", "thread"), None, None);
        let bytes = a.save_store_only().unwrap();

        let store = Circuit::restore_store(&bytes).unwrap();
        let mut b = Circuit::new();
        let deltas = b.replace_store(store);
        assert!(deltas.is_empty(), "no views registered, nothing to publish");
        assert_eq!(b.view_count(), 0);
        assert_eq!(b.compute_table_hashes(), a.compute_table_hashes());
        assert_eq!(b.max_row_versions()["thread"], 4);

        // A full snapshot's views are ignored on the store-only path too.
        let full = a.save().unwrap();
        let store = Circuit::restore_store(full.as_bytes()).unwrap();
        let mut c = Circuit::new();
        c.replace_store(store);
        assert_eq!(c.view_count(), 0);
    }

    #[test]
    fn replace_store_reprimes_views_registered_before_the_snapshot_landed() {
        let a = seeded(6);
        let bytes = a.save_store_only().unwrap();

        // Boot order on a client: views register against an empty store,
        // THEN the snapshot arrives.
        let mut b = Circuit::new();
        b.set_permission("thread", "true");
        assert!(b.add_query(scan("all", "thread"), None, None).is_none());
        assert!(b.add_query(top("top2", "thread", 2, "score"), None, None).is_none());
        assert!(b
            .add_query(filtered("pub", "thread", "published", json!(true)), None, None)
            .is_none());

        let deltas = b.replace_store(Circuit::restore_store(&bytes).unwrap());
        let by_id: HashMap<String, ViewDelta> =
            deltas.into_iter().map(|d| (d.query_id.clone(), d)).collect();
        assert_eq!(by_id.len(), 3, "every view republished");
        assert_eq!(records(&by_id["all"]).len(), 6);
        assert_eq!(records(&by_id["top2"]), vec!["thread:t4", "thread:t5"]);
        assert_eq!(records(&by_id["pub"]), vec!["thread:t0", "thread:t2", "thread:t4"]);
        assert_eq!(b.permissions()["thread"], "true", "config survives the swap");

        // Operator state was primed from the restored rows: a later row that
        // outranks the window evicts the right one.
        let next = b.step(ChangeSet {
            changes: vec![Change::create("thread", "t9", row(9))],
        });
        let top = next.iter().find(|d| d.query_id == "top2").expect("top2 stepped");
        assert_eq!(records(top), vec!["thread:t5", "thread:t9"]);
        assert_eq!(top.removals, vec!["thread:t4".to_string()]);
    }

    #[test]
    fn replace_store_tells_a_view_that_became_empty() {
        let mut c = seeded(2);
        c.add_query(scan("q", "thread"), None, None);
        let deltas = c.replace_store(Store::new());
        assert_eq!(deltas.len(), 1);
        assert!(deltas[0].records.is_empty());
        let mut removed = deltas[0].removals.clone();
        removed.sort();
        assert_eq!(removed, vec!["thread:t0", "thread:t1"]);
    }

    #[test]
    fn reconcile_deletes_rows_the_caller_lacks_and_lists_what_it_must_fetch() {
        let mut c = seeded(3); // t0 rv0, t1 rv1, t2 rv2
        c.add_query(scan("q", "thread"), None, None);

        let entries = vec![
            ("thread:t0".to_string(), 0), // same
            ("t1".to_string(), 5),        // newer locally, raw spelling
            ("thread:t7".to_string(), 1), // unknown to the store
        ];
        let result = c.reconcile("thread", &entries);
        assert_eq!(result.fetch, vec!["t1".to_string(), "thread:t7".to_string()]);
        assert_eq!(result.deleted, 1, "t2 is gone from the caller's list");
        assert_eq!(result.deltas.len(), 1);
        assert_eq!(result.deltas[0].removals, vec!["thread:t2".to_string()]);
        assert!(!c.contains("thread", "t2"));

        // Unknown table: everything must be fetched, nothing deleted.
        let result = c.reconcile("nope", &entries);
        assert_eq!(result.fetch.len(), 3);
        assert_eq!(result.deleted, 0);
    }

    #[test]
    fn projection_stores_only_evaluated_fields_and_reports_what_a_new_view_lacks() {
        let mut c = Circuit::new();
        c.set_projection(true);
        c.add_query(filtered("pub", "thread", "published", json!(true)), None, None);
        c.step(ChangeSet {
            changes: (0..4)
                .map(|i| Change::create("thread", &format!("t{i}"), row(i)))
                .collect(),
        });
        let stored = c.store.get_row_by_key("thread:t1").to_owned_value();
        assert_eq!(
            stored,
            Sp00kyValue::from(json!({ "id": "thread:t1", "_00_rv": 1, "published": false })),
            "only the predicate field and the identity are kept"
        );
        assert!(c.take_missing_fields().is_empty());

        // A view ordering on `score` cannot see it: registration says so.
        let initial = c.add_query(top("top2", "thread", 2, "score"), None, None);
        assert!(initial.is_some());
        let missing = c.take_missing_fields();
        assert_eq!(
            missing["thread"],
            ["score".to_string()].into_iter().collect::<std::collections::BTreeSet<_>>()
        );
        assert!(c.take_missing_fields().is_empty(), "drained");

        // Widening: merge just that field into each stored row, and the view
        // converges. New rows keep it from now on.
        let widen = c.step(ChangeSet {
            changes: (0..4)
                .map(|i| Change::merge("thread", &format!("t{i}"), json!({ "score": i })))
                .collect(),
        });
        let top = widen.iter().find(|d| d.query_id == "top2").expect("top2 stepped");
        assert_eq!(records(top), vec!["thread:t2", "thread:t3"]);
        let stored = c.store.get_row_by_key("thread:t3").to_owned_value();
        assert_eq!(
            stored,
            Sp00kyValue::from(json!({ "id": "thread:t3", "_00_rv": 3, "published": false, "score": 3 }))
        );

        let report = c.size_report();
        let thread = report.tables.iter().find(|t| t.table == "thread").unwrap();
        let mut kept = thread.retained_fields.clone().unwrap();
        kept.sort();
        assert_eq!(kept, vec!["published", "score"]);
    }

    /// Regression: an UPDATE of a row OUTSIDE a `LIMIT n` window used to grow
    /// the window by one (TopK's `evaluate_key` passed upstream through and
    /// the synthesized `+1` was never evicted). Now content updates re-place
    /// the row: out stays out, a raised score moves in and evicts the tail,
    /// a lowered score moves out and backfills.
    #[test]
    fn a_content_update_re_places_a_row_in_a_top_k_window() {
        let mut c = seeded(10); // scores 0..9
        c.add_query(top("top3", "thread", 3, "score"), None, None);
        let window = |c: &Circuit| {
            let mut r: Vec<String> = c.get_view("top3").unwrap().cache.keys().map(|k| k.to_string()).collect();
            r.sort();
            r
        };
        assert_eq!(window(&c), vec!["thread:t7", "thread:t8", "thread:t9"]);

        // Out stays out.
        let d = c.step(ChangeSet {
            changes: vec![Change::update("thread", "t1", {
                let mut r = row(1);
                r["pgn"] = json!("changed");
                r
            })],
        });
        assert!(d.is_empty(), "no membership change, no delta: {d:?}");
        assert_eq!(window(&c).len(), 3);

        // Raised score moves in and evicts t7.
        let d = c.step(ChangeSet {
            changes: vec![Change::update("thread", "t1", {
                let mut r = row(1);
                r["score"] = json!(100);
                r
            })],
        });
        assert_eq!(d.len(), 1);
        assert_eq!(d[0].additions, vec!["thread:t1".to_string()]);
        assert_eq!(d[0].removals, vec!["thread:t7".to_string()]);
        assert_eq!(window(&c), vec!["thread:t1", "thread:t8", "thread:t9"]);

        // Lowered score moves out and t7 backfills.
        let d = c.step(ChangeSet {
            changes: vec![Change::update("thread", "t1", row(1))],
        });
        assert_eq!(d[0].additions, vec!["thread:t7".to_string()]);
        assert_eq!(d[0].removals, vec!["thread:t1".to_string()]);
        assert_eq!(window(&c), vec!["thread:t7", "thread:t8", "thread:t9"]);

        // In-window content change that keeps the order: content update only.
        let d = c.step(ChangeSet {
            changes: vec![Change::update("thread", "t9", {
                let mut r = row(9);
                r["pgn"] = json!("edited");
                r
            })],
        });
        assert_eq!(d[0].updates, vec!["thread:t9".to_string()]);
        assert!(d[0].additions.is_empty() && d[0].removals.is_empty());
    }

    #[test]
    fn compact_drops_dead_bytes_without_changing_results() {
        let mut c = seeded(10);
        c.add_query(top("top3", "thread", 3, "score"), None, None);
        for i in 0..10 {
            c.step(ChangeSet {
                changes: vec![Change::update("thread", &format!("t{i}"), {
                    let mut r = row(i);
                    r["pgn"] = json!("y".repeat(60));
                    r
                })],
            });
        }
        assert!(c.dead_bytes() > 0);
        let hashes = c.compute_table_hashes();
        let reclaimed = c.compact();
        assert!(reclaimed > 0);
        assert_eq!(c.dead_bytes(), 0);
        assert_eq!(c.compute_table_hashes(), hashes);
        let next = c.step(ChangeSet {
            changes: vec![Change::create("thread", "t99", row(99))],
        });
        assert_eq!(records(&next[0]), vec!["thread:t8", "thread:t9", "thread:t99"]);
    }
}

#[cfg(test)]
mod row_version_floor_tests {
    use super::*;
    use serde_json::json;

    fn rv(circuit: &Circuit, key: &str) -> Option<i64> {
        circuit.store.get_record_version_by_key(key)
    }

    fn step(circuit: &mut Circuit, change: Change) {
        circuit.step(ChangeSet { changes: vec![change] });
    }

    #[test]
    fn off_by_default_and_versions_pass_through_unchanged() {
        let mut circuit = Circuit::new();
        assert!(!circuit.monotonic_row_versions());
        step(&mut circuit, Change::create("thread", "a", json!({ "title": "one", "_00_rv": 5 })));
        step(&mut circuit, Change::update("thread", "a", json!({ "title": "two" })));
        // The browser circuit must keep exactly what it was handed.
        assert_eq!(rv(&circuit, "thread:a"), None);
        assert_eq!(circuit.synthesized_row_versions(), 0);
    }

    #[test]
    fn missing_or_stale_version_on_a_stored_row_advances_past_the_stored_one() {
        let mut circuit = Circuit::new();
        circuit.set_monotonic_row_versions(true);
        step(&mut circuit, Change::create("thread", "a", json!({ "title": "one", "_00_rv": 5 })));
        assert_eq!(rv(&circuit, "thread:a"), Some(5));

        // No version at all (what a NONE `_00_version` lookup stamps).
        step(&mut circuit, Change::update("thread", "a", json!({ "title": "two" })));
        assert_eq!(rv(&circuit, "thread:a"), Some(6));

        // A version that went backwards.
        step(&mut circuit, Change::update("thread", "a", json!({ "title": "three", "_00_rv": 3 })));
        assert_eq!(rv(&circuit, "thread:a"), Some(7));

        // A version equal to the stored one.
        step(&mut circuit, Change::update("thread", "a", json!({ "title": "four", "_00_rv": 7 })));
        assert_eq!(rv(&circuit, "thread:a"), Some(8));

        // A genuine advance is kept verbatim, even past the floor.
        step(&mut circuit, Change::update("thread", "a", json!({ "title": "five", "_00_rv": 20 })));
        assert_eq!(rv(&circuit, "thread:a"), Some(20));
        assert_eq!(circuit.synthesized_row_versions(), 3);
        // Exactly one of those three arrived with no `_00_rv` at all; the
        // other two carried one that failed to advance. Only the first kind
        // says anything about the DB-side stamp.
        assert_eq!(circuit.synthesized_row_versions_missing(), 1);
    }

    /// The two causes of a synthesized version must stay countable apart.
    ///
    /// A missing stamp implicates `_00_version` (and its unique index); a
    /// present-but-repeating one does not. Reporting a single pooled number
    /// made the ingest warning name the index unconditionally, which is how a
    /// healthy index ends up being investigated.
    #[test]
    fn a_repeating_version_is_not_counted_as_a_missing_stamp() {
        let mut circuit = Circuit::new();
        circuit.set_monotonic_row_versions(true);
        step(&mut circuit, Change::create("thread", "a", json!({ "title": "one", "_00_rv": 5 })));

        step(&mut circuit, Change::update("thread", "a", json!({ "title": "two", "_00_rv": 5 })));
        assert_eq!(circuit.synthesized_row_versions(), 1);
        assert_eq!(
            circuit.synthesized_row_versions_missing(),
            0,
            "the stamp produced a version, it just repeated"
        );

        step(&mut circuit, Change::update("thread", "a", json!({ "title": "three" })));
        assert_eq!(circuit.synthesized_row_versions(), 2);
        assert_eq!(
            circuit.synthesized_row_versions_missing(),
            1,
            "this one carried no _00_rv at all"
        );
    }

    #[test]
    fn replayed_event_with_identical_content_publishes_nothing() {
        let mut circuit = Circuit::new();
        circuit.set_monotonic_row_versions(true);
        step(&mut circuit, Change::create("thread", "a", json!({ "title": "one", "_00_rv": 5 })));
        // Same body, no version: the digest matches, the store writes nothing.
        step(&mut circuit, Change::update("thread", "a", json!({ "title": "one" })));
        assert_eq!(rv(&circuit, "thread:a"), Some(5));
    }

    #[test]
    fn floor_is_seeded_from_rows_the_circuit_already_holds() {
        // Rows that predate the flag (a restored snapshot) still raise the
        // floor, so a synthesized version beats every version ever published.
        let mut circuit = Circuit::new();
        step(&mut circuit, Change::create("thread", "old", json!({ "title": "x", "_00_rv": 40 })));
        step(&mut circuit, Change::create("thread", "a", json!({ "title": "one", "_00_rv": 2 })));
        circuit.set_monotonic_row_versions(true);
        step(&mut circuit, Change::update("thread", "a", json!({ "title": "two" })));
        assert_eq!(rv(&circuit, "thread:a"), Some(41));
        // The floor keeps climbing: the next synthesis is above the last one.
        step(&mut circuit, Change::update("thread", "old", json!({ "title": "y" })));
        assert_eq!(rv(&circuit, "thread:old"), Some(42));
    }

    #[test]
    fn rows_without_a_stored_version_keep_a_real_incoming_one() {
        let mut circuit = Circuit::new();
        circuit.set_monotonic_row_versions(true);
        // Bootstrapped from `SELECT *`: no `_00_rv` on the row.
        step(&mut circuit, Change::create("thread", "a", json!({ "title": "one" })));
        assert_eq!(rv(&circuit, "thread:a"), None);
        step(&mut circuit, Change::update("thread", "a", json!({ "title": "two", "_00_rv": 13 })));
        assert_eq!(rv(&circuit, "thread:a"), Some(13));
        // ...and get one made up when the event carries none.
        step(&mut circuit, Change::create("thread", "b", json!({ "title": "one" })));
        step(&mut circuit, Change::update("thread", "b", json!({ "title": "two" })));
        assert_eq!(rv(&circuit, "thread:b"), Some(14));
    }

    #[test]
    fn new_rows_and_merges_are_left_alone() {
        let mut circuit = Circuit::new();
        circuit.set_monotonic_row_versions(true);
        step(&mut circuit, Change::create("thread", "a", json!({ "title": "one" })));
        assert_eq!(rv(&circuit, "thread:a"), None, "a new row carries what it was given");
        step(&mut circuit, Change::create("thread", "b", json!({ "title": "one", "_00_rv": 3 })));
        // Merge widens a projected row; it is not a content change to refetch.
        step(&mut circuit, Change::merge("thread", "b", json!({ "extra": "field" })));
        assert_eq!(rv(&circuit, "thread:b"), Some(3));
        assert_eq!(circuit.synthesized_row_versions(), 0);
    }
}
