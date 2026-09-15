use serde::Serialize;
use serde_json::Value;
use ssp::circuit::{Change, ChangeSet, Circuit, Operation, ViewDelta};
use ssp::eval::normalize_record_id;
use ssp::types::Sp00kyValue;
use wasm_bindgen::prelude::*;
use web_time::Instant;

/// Version from Cargo.toml
const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Called when WASM module is loaded
#[wasm_bindgen(start)]
pub fn init() {
    web_sys::console::log_1(&format!("[ssp-wasm] v{} loaded", VERSION).into());
}

#[wasm_bindgen]
pub struct Sp00kyProcessor {
    circuit: Circuit,
}

/// Per-record delta info (id + version).
#[derive(Serialize)]
struct WasmDeltaRecord(String, i64);

/// Granular delta: which records were added, removed, or content-updated.
#[derive(Serialize)]
struct WasmDelta {
    additions: Vec<WasmDeltaRecord>,
    removals: Vec<String>,
    updates: Vec<WasmDeltaRecord>,
}

/// Custom DTO for WASM output.
#[derive(Serialize)]
struct WasmViewUpdate {
    query_id: String,
    result_hash: String,
    result_data: Vec<(String, i64)>,
    delta: WasmDelta,
    // Per-phase SSP processing time (ms). The ingest path fills
    // store_apply/circuit_step/transform; the register path fills
    // parse/plan/snapshot. The unused side stays 0.
    timing_store_apply_ms: f64,
    timing_circuit_step_ms: f64,
    timing_transform_ms: f64,
    timing_parse_ms: f64,
    timing_plan_ms: f64,
    timing_snapshot_ms: f64,
}

/// One record change on the way in, as `ingest_many` receives it (mirrors the
/// `WasmIngestItem` TS interface below).
#[derive(serde::Deserialize)]
struct IngestItem {
    table: String,
    op: String,
    id: String,
    record: Value,
}

/// Normalize one incoming record into the `Change` the circuit consumes. Shared
/// by `ingest` and `ingest_many` so a batched row is treated identically to a
/// single one.
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
            // Fallback: extract raw id from the passed `id` param,
            // stripping the table prefix if present (e.g. "thread:abc" → "abc").
            ssp::types::raw_id(id).to_string()
        });

    match Operation::from_str(op).unwrap_or(Operation::Create) {
        Operation::Create => Change::create(table, &record_id, clean_sv),
        Operation::Update => Change::update(table, &record_id, clean_sv),
        Operation::Merge => Change::merge(table, &record_id, clean_sv),
        Operation::Delete => Change::delete(table, &record_id),
    }
}

/// Serialize circuit output for JS: maps as plain objects, like every other
/// export here.
fn to_js<T: Serialize>(value: &T) -> Result<JsValue, JsValue> {
    let serializer = serde_wasm_bindgen::Serializer::new().serialize_maps_as_objects(true);
    Ok(value.serialize(&serializer)?)
}

/// What `reconcile` returns to JS.
#[derive(Serialize)]
struct WasmReconciled {
    fetch: Vec<String>,
    deleted: usize,
    updates: Vec<WasmViewUpdate>,
}

/// What `register_view` returns: the initial view update plus, under
/// projection, the fields this plan evaluates that stored rows do not hold.
#[derive(Serialize)]
struct WasmRegistration {
    #[serde(flatten)]
    update: WasmViewUpdate,
    #[serde(skip_serializing_if = "Option::is_none")]
    missing_fields: Option<std::collections::BTreeMap<String, Vec<String>>>,
}

/// Transform a Vec<ViewDelta> to Vec<WasmViewUpdate> with versions from the store.
fn transform_deltas(deltas: &[ViewDelta], circuit: &Circuit) -> Vec<WasmViewUpdate> {
    deltas
        .iter()
        .map(|d| transform_single_delta(d, circuit))
        .collect()
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
        // Timings are stamped by the caller (ingest/register); default to 0.
        timing_store_apply_ms: 0.0,
        timing_circuit_step_ms: 0.0,
        timing_transform_ms: 0.0,
        timing_parse_ms: 0.0,
        timing_plan_ms: 0.0,
        timing_snapshot_ms: 0.0,
    }
}

// This is appended to the generated .d.ts file
#[wasm_bindgen(typescript_custom_section)]
const TS_APPEND_CONTENT: &'static str = r#"
export interface WasmViewUpdate {
  query_id: string;
  result_hash: string;
  result_data: [string, number][];
  delta: {
    additions: [string, number][];
    removals: string[];
    updates: [string, number][];
  };
  // Per-phase SSP processing time (ms). Ingest path: store_apply/circuit_step/
  // transform. Register path: parse/plan/snapshot. Unused side is 0.
  timing_store_apply_ms: number;
  timing_circuit_step_ms: number;
  timing_transform_ms: number;
  timing_parse_ms: number;
  timing_plan_ms: number;
  timing_snapshot_ms: number;
}

export interface WasmViewConfig {
  id: string;
  surql: string;
  params?: Record<string, any>;
  clientId: string;
  ttl: string;
  lastActiveAt: string;
  safe_params?: Record<string, any>;
  format?: 'flat' | 'tree' | 'streaming';
}

export interface WasmIngestItem {
  table: string;
  op: string;
  id: string;
  record: any;
}

export interface WasmRegistration extends WasmViewUpdate {
  /** Under projection: fields this plan evaluates that stored rows lack. */
  missing_fields?: Record<string, string[]>;
}

export interface WasmReconciled {
  fetch: string[];
  deleted: number;
  updates: WasmViewUpdate[];
}
"#;

#[wasm_bindgen]
impl Sp00kyProcessor {
    #[wasm_bindgen(constructor)]
    pub fn new() -> Sp00kyProcessor {
        Sp00kyProcessor {
            circuit: Circuit::new(),
        }
    }

    /// Ingest a record into the stream processor
    pub fn ingest(
        &mut self,
        table: String,
        op: String,
        id: String,
        record: JsValue,
    ) -> Result<JsValue, JsValue> {
        let record: Value = serde_wasm_bindgen::from_value(record)
            .map_err(|e| JsValue::from_str(&format!("Failed to parse record: {}", e)))?;

        let change = build_change(&table, &op, &id, record);

        let changeset = ChangeSet {
            changes: vec![change],
        };

        let (deltas, step_timings) = self.circuit.step_timed(changeset);

        // Transform to include versions — this is the "transform/materialize" phase.
        let t_transform = Instant::now();
        let mut wasm_updates = transform_deltas(&deltas, &self.circuit);
        let transform_ms = t_transform.elapsed().as_secs_f64() * 1000.0;

        // Stamp the per-phase ingest timings onto every produced update.
        for u in wasm_updates.iter_mut() {
            u.timing_store_apply_ms = step_timings.store_apply_ms;
            u.timing_circuit_step_ms = step_timings.circuit_step_ms;
            u.timing_transform_ms = transform_ms;
        }

        let serializer = serde_wasm_bindgen::Serializer::new().serialize_maps_as_objects(true);
        Ok(wasm_updates.serialize(&serializer)?)
    }

    /// Ingest MANY record changes as ONE circuit step.
    ///
    /// `ingest` costs one full circuit step per record, and a step walks every
    /// registered view, so a cold sync that lands thousands of rows paid that
    /// fixed cost thousands of times (a ~3.9k-row registry took ~3.4s of circuit
    /// time on a laptop, ~0.85ms a row, nearly all of it per-step overhead).
    /// `ChangeSet` already carries many changes and `step_timed` applies them
    /// all to the store before stepping once, so a batch is a single step with
    /// one set of deltas.
    ///
    /// Same input shape as `ingest`, as an array: `WasmIngestItem[]`. Returns
    /// the coalesced `WasmViewUpdate[]` for the whole batch. Changes are applied
    /// in array order, so repeated ids inside one batch settle last-write-wins,
    /// exactly as sequential `ingest` calls would.
    pub fn ingest_many(&mut self, items: JsValue) -> Result<JsValue, JsValue> {
        let items: Vec<IngestItem> = serde_wasm_bindgen::from_value(items)
            .map_err(|e| JsValue::from_str(&format!("Failed to parse ingest batch: {}", e)))?;

        if items.is_empty() {
            let serializer = serde_wasm_bindgen::Serializer::new().serialize_maps_as_objects(true);
            return Ok(Vec::<WasmViewUpdate>::new().serialize(&serializer)?);
        }

        let changes = items
            .into_iter()
            .map(|item| build_change(&item.table, &item.op, &item.id, item.record))
            .collect();

        let (deltas, step_timings) = self.circuit.step_timed(ChangeSet { changes });

        let t_transform = Instant::now();
        let mut wasm_updates = transform_deltas(&deltas, &self.circuit);
        let transform_ms = t_transform.elapsed().as_secs_f64() * 1000.0;

        for u in wasm_updates.iter_mut() {
            u.timing_store_apply_ms = step_timings.store_apply_ms;
            u.timing_circuit_step_ms = step_timings.circuit_step_ms;
            u.timing_transform_ms = transform_ms;
        }

        let serializer = serde_wasm_bindgen::Serializer::new().serialize_maps_as_objects(true);
        Ok(wasm_updates.serialize(&serializer)?)
    }

    /// Register a new materialized view
    pub fn register_view(&mut self, config: JsValue) -> Result<JsValue, JsValue> {
        let config_val: Value = serde_wasm_bindgen::from_value(config)
            .map_err(|e| JsValue::from_str(&format!("Failed to parse config: {}", e)))?;

        let data = ssp::service::view::prepare_registration_dbsp(
            config_val,
            self.circuit.permissions(),
            self.circuit.link_targets(),
            self.circuit.opaque_fields(),
        )
        .map_err(|e| JsValue::from_str(&format!("Registration failed: {}", e)))?;

        let parse_ms = data.parse_ms;
        let plan_id = data.plan.id.clone();
        // Match `add_query`'s empty auth_id, but capture the plan/snapshot timings.
        let (initial_delta, reg_timings) = self.circuit.add_query_with_auth_timed(
            data.plan,
            data.safe_params,
            data.format,
            String::new(),
        );

        let mut wasm_result = match initial_delta {
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
        wasm_result.timing_parse_ms = parse_ms;
        wasm_result.timing_plan_ms = reg_timings.plan_ms;
        wasm_result.timing_snapshot_ms = reg_timings.snapshot_ms;

        let missing = self.circuit.take_missing_fields();
        let missing_fields = (!missing.is_empty()).then(|| {
            missing
                .into_iter()
                .map(|(t, f)| (t, f.into_iter().collect()))
                .collect()
        });
        to_js(&WasmRegistration {
            update: wasm_result,
            missing_fields,
        })
    }

    /// Unregister a view by ID
    pub fn unregister_view(&mut self, id: String) {
        self.circuit.remove_query(&id);
    }

    /// Seed per-table `select` permission predicates so `register_view` can
    /// inject them (and so non-`_00_` tables aren't default-denied).
    ///
    /// Expects a `{ [table]: whereText }` object, where `whereText` is the raw
    /// `WHERE` expression from the table's `PERMISSIONS FOR select` clause
    /// (e.g. `"true"`, or `"owner = $auth.id"`). Called once at boot after the
    /// schema is parsed — mirrors the native boot path that reads `INFO FOR DB`.
    pub fn set_permissions(&mut self, permissions: JsValue) -> Result<(), JsValue> {
        let map: std::collections::HashMap<String, String> =
            serde_wasm_bindgen::from_value(permissions)
                .map_err(|e| JsValue::from_str(&format!("Failed to parse permissions: {}", e)))?;
        for (table, where_text) in map {
            self.circuit.set_permission(table, where_text);
        }
        Ok(())
    }

    /// Save the current circuit state as a JSON string
    pub fn save_state(&self) -> Result<String, JsValue> {
        self.circuit
            .save()
            .map_err(|e| JsValue::from_str(&format!("Failed to serialize state: {}", e)))
    }

    /// Load circuit state from a JSON string
    pub fn load_state(&mut self, state: String) -> Result<(), JsValue> {
        let mut circuit = Circuit::restore(&state)
            .map_err(|e| JsValue::from_str(&format!("Failed to deserialize state: {}", e)))?;
        // `Circuit::restore` carries state, not configuration: permissions,
        // link targets, opaque fields and projection were seeded on THIS
        // processor and must survive the swap, or every table default-denies.
        for (table, text) in self.circuit.permissions() {
            circuit.set_permission(table.clone(), text.clone());
        }
        for (table, fields) in self.circuit.link_targets() {
            for (field, target) in fields {
                circuit.set_link_target(table.clone(), field.clone(), target.clone());
            }
        }
        for (table, fields) in self.circuit.opaque_fields() {
            circuit.set_opaque_fields(table.clone(), fields.clone());
        }
        circuit.set_projection(self.circuit.projection());
        self.circuit = circuit;
        Ok(())
    }

    /// Snapshot the base collections only, as bytes (a `Uint8Array` in JS).
    /// Views are deliberately left out: the client re-registers every query
    /// under a fresh session id on boot, so persisted views would only be
    /// stepped and never read. Pair with `load_store_state`.
    pub fn save_store_state(&self) -> Result<Vec<u8>, JsValue> {
        self.circuit
            .save_store_only()
            .map_err(|e| JsValue::from_str(&format!("Failed to serialize store: {}", e)))
    }

    /// Install a snapshot written by `save_store_state` UNDER the views that
    /// are already registered, keeping permissions and projection. Every
    /// registered view is re-primed against the restored rows; the returned
    /// `WasmViewUpdate[]` carries their new full results, so a query that
    /// registered against the empty pre-snapshot store catches up.
    pub fn load_store_state(&mut self, bytes: &[u8]) -> Result<JsValue, JsValue> {
        let store = Circuit::restore_store(bytes)
            .map_err(|e| JsValue::from_str(&format!("Failed to deserialize store: {}", e)))?;
        let deltas = self.circuit.replace_store(store);
        to_js(&transform_deltas(&deltas, &self.circuit))
    }

    /// Compare one table against the caller's authoritative `[id, rv][]`.
    /// Rows the store holds but the list lacks are deleted (with view
    /// updates); ids the store lacks or holds at a lower `_00_rv` come back in
    /// `fetch` for the caller to ingest. See `Circuit::reconcile`.
    pub fn reconcile(&mut self, table: String, entries: JsValue) -> Result<JsValue, JsValue> {
        let entries: Vec<(String, i64)> = serde_wasm_bindgen::from_value(entries)
            .map_err(|e| JsValue::from_str(&format!("Failed to parse entries: {}", e)))?;
        let result = self.circuit.reconcile(&table, &entries);
        to_js(&WasmReconciled {
            fetch: result.fetch,
            deleted: result.deleted,
            updates: transform_deltas(&result.deltas, &self.circuit),
        })
    }

    /// Highest `_00_rv` folded into each table, `{ [table]: rv }`.
    pub fn max_row_versions(&self) -> Result<JsValue, JsValue> {
        to_js(&self.circuit.max_row_versions())
    }

    /// Rebuild row storage without the bytes orphaned by updates and deletes.
    /// Returns how many bytes were dead. Costs a decode of every row, so call
    /// it from a checkpoint, never per ingest.
    pub fn compact(&mut self) -> f64 {
        self.circuit.compact() as f64
    }

    /// Bytes of row storage orphaned by updates and deletes.
    pub fn dead_bytes(&self) -> f64 {
        self.circuit.dead_bytes() as f64
    }

    /// Bytes of row storage referenced by live rows.
    pub fn live_bytes(&self) -> f64 {
        self.circuit.live_bytes() as f64
    }

    /// Keep only the fields registered plans evaluate (plus `id`/`_00_rv`)
    /// per stored row. Off by default. Must be set before the first ingest
    /// to take effect on those rows; `compact` re-projects existing ones.
    pub fn set_projection(&mut self, enabled: bool) {
        self.circuit.set_projection(enabled);
    }

    /// Per-table and per-view heap attribution, sorted heaviest first.
    pub fn size_report(&self) -> Result<JsValue, JsValue> {
        to_js(&self.circuit.size_report())
    }
}
