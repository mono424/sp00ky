//! Native port of `ssp-wasm`'s `Sp00kyProcessor`.
//!
//! This is a near line-for-line port of `packages/ssp-wasm/src/lib.rs` with
//! `JsValue` / `serde_wasm_bindgen` swapped for `serde_json::Value`, so the
//! same circuit logic is reachable from Dart over a C ABI (see `lib.rs`).

use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use ssp::circuit::{Change, ChangeSet, Circuit, Operation, ViewDelta};
use ssp::eval::normalize_record_id;
use ssp::types::Sp00kyValue;
use std::collections::BTreeMap;
use std::time::Instant;

/// Per-record delta info (id + version).
#[derive(Serialize)]
pub struct WasmDeltaRecord(pub String, pub i64);

/// Granular delta: which records were added, removed, or content-updated.
#[derive(Serialize)]
pub struct WasmDelta {
    pub additions: Vec<WasmDeltaRecord>,
    pub removals: Vec<String>,
    pub updates: Vec<WasmDeltaRecord>,
}

/// Custom DTO mirroring the WASM output (`WasmViewUpdate`).
#[derive(Serialize)]
pub struct WasmViewUpdate {
    pub query_id: String,
    pub result_hash: String,
    pub result_data: Vec<(String, i64)>,
    pub delta: WasmDelta,
    // Per-phase SSP processing time (ms). The ingest path fills
    // store_apply/circuit_step/transform; the register path fills
    // parse/plan/snapshot. The unused side stays 0.
    pub timing_store_apply_ms: f64,
    pub timing_circuit_step_ms: f64,
    pub timing_transform_ms: f64,
    pub timing_parse_ms: f64,
    pub timing_plan_ms: f64,
    pub timing_snapshot_ms: f64,
}

/// One record change on the way in, as `ingest_many` receives it.
#[derive(Deserialize)]
pub struct IngestItem {
    pub table: String,
    pub op: String,
    pub id: String,
    pub record: Value,
}

/// What `reconcile` answers with.
#[derive(Serialize)]
pub struct WasmReconciled {
    pub fetch: Vec<String>,
    pub deleted: usize,
    pub updates: Vec<WasmViewUpdate>,
}

/// What `register_view` answers with: the initial view update plus, under
/// projection, the fields this plan evaluates that stored rows do not hold.
#[derive(Serialize)]
pub struct WasmRegistration {
    #[serde(flatten)]
    pub update: WasmViewUpdate,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub missing_fields: Option<BTreeMap<String, Vec<String>>>,
}

/// Normalize one incoming record into the `Change` the circuit consumes.
/// Shared by `ingest` and `ingest_many`, so a batched row is treated
/// identically to a single one.
fn build_change(table: &str, op: &str, id: &str, record: Value) -> Change {
    let clean_record = ssp::sanitizer::normalize_record(record);
    let clean_sv: Sp00kyValue = clean_record.into();

    let record_id = clean_sv
        .get("id")
        .cloned()
        .map(normalize_record_id)
        .and_then(|v| match v {
            Sp00kyValue::Str(s) => Some(s.to_string()),
            _ => None,
        })
        .unwrap_or_else(|| {
            // Fallback: extract the raw id from the passed `id` param,
            // stripping the table prefix if present ("thread:abc" -> "abc").
            ssp::types::raw_id(id).to_string()
        });

    match Operation::from_str(op).unwrap_or(Operation::Create) {
        Operation::Create => Change::create(table, &record_id, clean_sv),
        Operation::Update => Change::update(table, &record_id, clean_sv),
        Operation::Merge => Change::merge(table, &record_id, clean_sv),
        Operation::Delete => Change::delete(table, &record_id),
    }
}

/// Resolve version for a key from the store (defaults to 1).
fn version_for(circuit: &Circuit, key: &str) -> i64 {
    circuit.store.get_record_version_by_key(key).unwrap_or(1)
}

/// Transform a single ViewDelta to WasmViewUpdate.
fn transform_single_delta(delta: &ViewDelta, circuit: &Circuit) -> WasmViewUpdate {
    let result_data: Vec<(String, i64)> = circuit
        .view_keys(&delta.query_id)
        .into_iter()
        .map(|key| {
            let version = version_for(circuit, &key);
            (key, version)
        })
        .collect();

    let additions: Vec<WasmDeltaRecord> = delta
        .additions
        .iter()
        .map(|key| WasmDeltaRecord(key.clone(), version_for(circuit, key)))
        .collect();

    let removals: Vec<String> = delta.removals.clone();

    let updates: Vec<WasmDeltaRecord> = delta
        .updates
        .iter()
        .map(|key| WasmDeltaRecord(key.clone(), version_for(circuit, key)))
        .collect();

    WasmViewUpdate {
        query_id: delta.query_id.clone(),
        result_hash: delta.result_hash.clone(),
        result_data,
        delta: WasmDelta {
            additions,
            removals,
            updates,
        },
        timing_store_apply_ms: 0.0,
        timing_circuit_step_ms: 0.0,
        timing_transform_ms: 0.0,
        timing_parse_ms: 0.0,
        timing_plan_ms: 0.0,
        timing_snapshot_ms: 0.0,
    }
}

/// Transform a Vec<ViewDelta> to Vec<WasmViewUpdate> with versions from the store.
fn transform_deltas(deltas: &[ViewDelta], circuit: &Circuit) -> Vec<WasmViewUpdate> {
    deltas
        .iter()
        .map(|d| transform_single_delta(d, circuit))
        .collect()
}

pub struct Processor {
    circuit: Circuit,
}

impl Processor {
    pub fn new() -> Processor {
        Processor {
            circuit: Circuit::new(),
        }
    }

    /// Ingest a record change into the stream processor.
    pub fn ingest(
        &mut self,
        table: &str,
        op: &str,
        id: &str,
        record: Value,
    ) -> Result<Vec<WasmViewUpdate>> {
        let changeset = ChangeSet {
            changes: vec![build_change(table, op, id, record)],
        };
        let deltas = self.circuit.step(changeset);
        Ok(transform_deltas(&deltas, &self.circuit))
    }

    /// Ingest many record changes as ONE circuit step, returning the coalesced
    /// deltas for the whole batch. Changes are applied in order, so repeated
    /// ids inside one batch settle last-write-wins, exactly as sequential
    /// [`Processor::ingest`] calls would.
    pub fn ingest_many(&mut self, items: Vec<IngestItem>) -> Result<Vec<WasmViewUpdate>> {
        if items.is_empty() {
            return Ok(vec![]);
        }
        let changes = items
            .into_iter()
            .map(|item| build_change(&item.table, &item.op, &item.id, item.record))
            .collect();

        let (deltas, step_timings) = self.circuit.step_timed(ChangeSet { changes });

        let t_transform = Instant::now();
        let mut updates = transform_deltas(&deltas, &self.circuit);
        let transform_ms = t_transform.elapsed().as_secs_f64() * 1000.0;

        for u in updates.iter_mut() {
            u.timing_store_apply_ms = step_timings.store_apply_ms;
            u.timing_circuit_step_ms = step_timings.circuit_step_ms;
            u.timing_transform_ms = transform_ms;
        }
        Ok(updates)
    }

    /// Register a new materialized view.
    pub fn register_view(&mut self, config: Value) -> Result<WasmRegistration> {
        let data = ssp::service::view::prepare_registration_dbsp(
            config,
            self.circuit.permissions(),
            self.circuit.link_targets(),
            self.circuit.opaque_fields(),
        )
        .map_err(|e| anyhow!("Registration failed: {}", e))?;

        let parse_ms = data.parse_ms;
        let plan_id = data.plan.id.clone();
        // Match `add_query`'s empty auth_id, but capture the plan/snapshot timings.
        let (initial_delta, reg_timings) = self.circuit.add_query_with_auth_timed(
            data.plan,
            data.safe_params,
            data.format,
            String::new(),
        );

        let mut update = match initial_delta {
            Some(ref delta) => transform_single_delta(delta, &self.circuit),
            None => WasmViewUpdate {
                query_id: plan_id,
                result_hash: String::new(),
                result_data: vec![],
                delta: WasmDelta {
                    additions: vec![],
                    removals: vec![],
                    updates: vec![],
                },
                timing_store_apply_ms: 0.0,
                timing_circuit_step_ms: 0.0,
                timing_transform_ms: 0.0,
                timing_parse_ms: 0.0,
                timing_plan_ms: 0.0,
                timing_snapshot_ms: 0.0,
            },
        };
        update.timing_parse_ms = parse_ms;
        update.timing_plan_ms = reg_timings.plan_ms;
        update.timing_snapshot_ms = reg_timings.snapshot_ms;

        let missing = self.circuit.take_missing_fields();
        let missing_fields = (!missing.is_empty()).then(|| {
            missing
                .into_iter()
                .map(|(t, f)| (t, f.into_iter().collect()))
                .collect()
        });
        Ok(WasmRegistration {
            update,
            missing_fields,
        })
    }

    /// Unregister a view by ID.
    pub fn unregister_view(&mut self, id: &str) {
        self.circuit.remove_query(id);
    }

    /// Register a table's raw `PERMISSIONS FOR select WHERE <expr>` text.
    ///
    /// The browser client relies on the deployed circuit already being
    /// permissive; a native Dart client must instead seed permissions from the
    /// schema (the way the SSP server does at boot) so `register_view` does not
    /// hit the circuit's default-deny. See `Circuit::set_permission`.
    pub fn set_permission(&mut self, table: &str, where_text: &str) {
        self.circuit.set_permission(table, where_text);
    }

    /// Save the current circuit state as a JSON string.
    pub fn save_state(&self) -> Result<String> {
        self.circuit
            .save()
            .map_err(|e| anyhow!("Failed to serialize state: {}", e))
    }

    /// Load circuit state from a JSON string.
    pub fn load_state(&mut self, state: &str) -> Result<()> {
        let circuit =
            Circuit::restore(state).map_err(|e| anyhow!("Failed to deserialize state: {}", e))?;
        self.circuit = circuit;
        Ok(())
    }

    /// Snapshot the base collections only, as bytes. Views are deliberately
    /// left out: the client re-registers every query under a fresh session id
    /// on boot, so persisted views would only be stepped and never read. Pair
    /// with [`Processor::load_store_state`].
    pub fn save_store_state(&self) -> Result<Vec<u8>> {
        self.circuit
            .save_store_only()
            .map_err(|e| anyhow!("Failed to serialize store: {}", e))
    }

    /// Install a snapshot written by [`Processor::save_store_state`] UNDER the
    /// views that are already registered, keeping permissions and projection.
    /// Every registered view is re-primed against the restored rows, so a query
    /// that registered against the empty pre-snapshot store catches up.
    pub fn load_store_state(&mut self, bytes: &[u8]) -> Result<Vec<WasmViewUpdate>> {
        let store = Circuit::restore_store(bytes)
            .map_err(|e| anyhow!("Failed to deserialize store: {}", e))?;
        let deltas = self.circuit.replace_store(store);
        Ok(transform_deltas(&deltas, &self.circuit))
    }

    /// Compare one table against the caller's authoritative `[id, rv]` list.
    /// Rows the store holds but the list lacks are deleted (with view updates);
    /// ids the store lacks or holds at a lower `_00_rv` come back in `fetch`
    /// for the caller to ingest.
    pub fn reconcile(&mut self, table: &str, entries: &[(String, i64)]) -> WasmReconciled {
        let result = self.circuit.reconcile(table, entries);
        WasmReconciled {
            fetch: result.fetch,
            deleted: result.deleted,
            updates: transform_deltas(&result.deltas, &self.circuit),
        }
    }

    /// Highest `_00_rv` folded into each table.
    pub fn max_row_versions(&self) -> BTreeMap<String, i64> {
        self.circuit.max_row_versions()
    }

    /// Keep only the fields registered plans evaluate (plus `id`/`_00_rv`) per
    /// stored row. Off by default. Must be set before the first ingest to take
    /// effect on those rows.
    pub fn set_projection(&mut self, enabled: bool) {
        self.circuit.set_projection(enabled);
    }
}
