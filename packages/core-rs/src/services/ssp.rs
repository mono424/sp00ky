//! The in-process circuit.
//!
//! - [`Processor`]: the `ssp` crate's circuit driven natively (a port of
//!   `packages/ssp-ffi/src/processor.rs`, which the Dart client reaches over a C
//!   ABI with JSON envelopes; here the calls are direct).
//! - [`StreamProcessorService`]: receiver fan-out, the batching window, the
//!   boot prime, snapshot bookkeeping, permission seeding and the session
//!   identity (Dart `StreamProcessorService`).
//! - [`SspAdapter`]: the [`SspPort`] the runtime drives.
//!
//! Divergence from the browser client: [`StreamProcessorService::seed_permissions_from_schema`]
//! seeds the circuit's per-table select permissions from `schemaSurql` (the
//! browser relies on its deployed circuit already being permissive). Without
//! this, registering a view hits the circuit's default-deny. See
//! [`crate::services::permissions`].

use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, HashSet};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::rc::Rc;
use std::time::Instant;

use chrono::{DateTime, Utc};
use serde_json::Value as Json;
use ssp::circuit::{Change, ChangeSet, Circuit, Operation, ViewDelta as CircuitDelta};
use ssp::types::Sp00kyValue;

use crate::error::{Error, Result};
use crate::kernel::ports::{
    IngestOp, IngestRecord, RegisterPlan, RegisterResult, SspPort, StreamUpdate, ViewDelta,
};
use crate::services::local::{LocalDatabaseService, TableVersions};
use crate::services::permissions::{extract_table_permissions, BUILTIN_SYSTEM_PERMISSIONS};
use crate::types::{RecordVersionArray, RegistrationTimings};
use crate::value::{iso8601, Object, Row, Value};

// ---- normalization ------------------------------------------------------------------

/// A value as the circuit ingests it (TS `normalizeValue`): bytes become
/// `null` (JSON has no binary variant and the SSP cannot filter on opaque
/// bytes; binary CRDT snapshots land here), records their `table:id` string,
/// datetimes ISO-8601, durations their text; maps and lists recurse.
pub fn normalize_value(value: &Value) -> Json {
    match value {
        Value::Bytes(_) => Json::Null,
        Value::Array(a) => Json::Array(a.iter().map(normalize_value).collect()),
        Value::Object(o) => Json::Object(normalize_object(o)),
        other => other.to_json(),
    }
}

fn normalize_object(o: &Object) -> serde_json::Map<String, Json> {
    o.iter()
        .filter(|(_, v)| !v.is_none())
        .map(|(k, v)| (k.clone(), normalize_value(v)))
        .collect()
}

// ---- the processor ------------------------------------------------------------------

/// What [`Processor::register_view`] answers with: the initial view update
/// plus, under projection, the fields this plan evaluates that stored rows do
/// not hold.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Registration {
    pub update: StreamUpdate,
    pub missing_fields: BTreeMap<String, Vec<String>>,
}

/// What [`Processor::reconcile`] answers with.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Reconciled {
    /// Ids (the caller's spelling) whose body the store lacks or holds stale.
    pub fetch: Vec<String>,
    /// Rows deleted because the caller's list did not have them.
    pub deleted: usize,
    /// View updates produced by those deletes.
    pub updates: Vec<StreamUpdate>,
}

/// Run a circuit call, turning a panic into an error the way the C ABI did,
/// so one bad row cannot take the engine thread down.
fn guarded<T>(f: impl FnOnce() -> Result<T>) -> Result<T> {
    catch_unwind(AssertUnwindSafe(f))
        .unwrap_or_else(|_| Err(Error::Ssp("panic in the circuit".into())))
}

/// The native side reports `0` for the phases a call did not run; those are
/// "not measured", not a zero-millisecond phase.
fn measured(ms: f64) -> Option<f64> {
    (ms != 0.0).then_some(ms)
}

/// Normalize one incoming record into the `Change` the circuit consumes.
/// Shared by `ingest` and `ingest_many`, so a batched row is treated
/// identically to a single one.
fn build_change(table: &str, op: IngestOp, id: &str, record: Json) -> Change {
    let clean: Sp00kyValue = ssp::sanitizer::normalize_record(record).into();
    let record_id = clean
        .get("id")
        .cloned()
        .map(ssp::eval::normalize_record_id)
        .and_then(|v| match v {
            Sp00kyValue::Str(s) => Some(s.to_string()),
            _ => None,
        })
        // Fallback: the raw id of the passed id ("thread:abc" -> "abc").
        .unwrap_or_else(|| ssp::types::raw_id(id).to_string());

    match Operation::from_str(op.as_str()).unwrap_or(Operation::Create) {
        Operation::Create => Change::create(table, &record_id, clean),
        Operation::Update => Change::update(table, &record_id, clean),
        Operation::Merge => Change::merge(table, &record_id, clean),
        Operation::Delete => Change::delete(table, &record_id),
    }
}

/// A key's version from the store (defaults to 1).
fn version_for(circuit: &Circuit, key: &str) -> i64 {
    circuit.store.get_record_version_by_key(key).unwrap_or(1)
}

fn versioned(circuit: &Circuit, keys: &[String]) -> RecordVersionArray {
    keys.iter()
        .map(|k| (k.clone(), version_for(circuit, k)))
        .collect()
}

fn to_update(delta: &CircuitDelta, circuit: &Circuit) -> StreamUpdate {
    StreamUpdate {
        query_hash: delta.query_id.clone(),
        local_array: versioned(circuit, &circuit.view_keys(&delta.query_id)),
        result_hash: delta.result_hash.clone(),
        delta: ViewDelta {
            additions: versioned(circuit, &delta.additions),
            removals: delta.removals.clone(),
            updates: versioned(circuit, &delta.updates),
        },
        ..Default::default()
    }
}

fn to_updates(deltas: &[CircuitDelta], circuit: &Circuit) -> Vec<StreamUpdate> {
    deltas.iter().map(|d| to_update(d, circuit)).collect()
}

/// One `ssp` circuit.
pub struct Processor {
    circuit: Circuit,
}

impl Default for Processor {
    fn default() -> Self {
        Self::new()
    }
}

impl Processor {
    pub fn new() -> Self {
        Processor {
            circuit: Circuit::new(),
        }
    }

    /// Ingest one record change, returning the affected view updates (tagged
    /// with `op`).
    pub fn ingest(
        &mut self,
        table: &str,
        op: IngestOp,
        id: &str,
        record: &Row,
    ) -> Result<Vec<StreamUpdate>> {
        guarded(|| {
            let changes = vec![build_change(
                table,
                op,
                id,
                Json::Object(normalize_object(record)),
            )];
            let deltas = self.circuit.step(ChangeSet { changes });
            let mut updates = to_updates(&deltas, &self.circuit);
            for u in &mut updates {
                u.op = Some(op.as_str().to_string());
            }
            Ok(updates)
        })
    }

    /// Ingest many record changes as ONE circuit step, returning the coalesced
    /// updates for the whole batch. Changes are applied in order, so repeated
    /// ids inside one batch settle last-write-wins, exactly as sequential
    /// [`Processor::ingest`] calls would.
    pub fn ingest_many(&mut self, records: &[IngestRecord]) -> Result<Vec<StreamUpdate>> {
        if records.is_empty() {
            return Ok(Vec::new());
        }
        guarded(|| {
            let changes = records
                .iter()
                .map(|r| {
                    build_change(
                        &r.table,
                        r.op,
                        &r.id,
                        Json::Object(normalize_object(&r.record)),
                    )
                })
                .collect();
            let (deltas, timings) = self.circuit.step_timed(ChangeSet { changes });
            let t_transform = Instant::now();
            let mut updates = to_updates(&deltas, &self.circuit);
            let transform_ms = t_transform.elapsed().as_secs_f64() * 1000.0;
            for u in &mut updates {
                u.store_apply_ms = measured(timings.store_apply_ms);
                u.circuit_step_ms = measured(timings.circuit_step_ms);
                u.transform_ms = measured(transform_ms);
            }
            Ok(updates)
        })
    }

    /// Register a materialized view from its config (`id`, `surql`, `params`,
    /// `clientId`, `ttl`, `lastActiveAt`), returning its initial snapshot.
    pub fn register_view(&mut self, config: Json) -> Result<Registration> {
        guarded(|| {
            let data = ssp::service::view::prepare_registration_dbsp(
                config,
                self.circuit.permissions(),
                self.circuit.link_targets(),
                self.circuit.opaque_fields(),
            )
            .map_err(|e| Error::Ssp(format!("Registration failed: {e}")))?;

            let parse_ms = data.parse_ms;
            let plan_id = data.plan.id.clone();
            let (initial, reg) = self.circuit.add_query_with_auth_timed(
                data.plan,
                data.safe_params,
                data.format,
                String::new(),
            );
            let mut update = match initial {
                Some(ref delta) => to_update(delta, &self.circuit),
                None => StreamUpdate {
                    query_hash: plan_id,
                    ..Default::default()
                },
            };
            update.parse_ms = measured(parse_ms);
            update.plan_ms = measured(reg.plan_ms);
            update.snapshot_ms = measured(reg.snapshot_ms);

            let missing_fields = self
                .circuit
                .take_missing_fields()
                .into_iter()
                .map(|(t, f)| (t, f.into_iter().collect()))
                .collect();
            Ok(Registration {
                update,
                missing_fields,
            })
        })
    }

    /// Unregister a view by id.
    pub fn unregister_view(&mut self, id: &str) -> Result<()> {
        guarded(|| {
            self.circuit.remove_query(id);
            Ok(())
        })
    }

    /// Seed a table's `PERMISSIONS FOR select WHERE <expr>` text. Required
    /// before [`Processor::register_view`] for a real table, since the circuit
    /// is default-deny.
    pub fn set_permission(&mut self, table: &str, where_text: &str) {
        self.circuit.set_permission(table, where_text);
    }

    /// The whole circuit as JSON (legacy; only the store snapshot is
    /// persisted now).
    pub fn save_state(&self) -> Result<String> {
        self.circuit
            .save()
            .map_err(|e| Error::Ssp(format!("Failed to serialize state: {e}")))
    }

    /// Replace the circuit with one saved by [`Processor::save_state`].
    pub fn load_state(&mut self, state: &str) -> Result<()> {
        self.circuit = Circuit::restore(state)
            .map_err(|e| Error::Ssp(format!("Failed to deserialize state: {e}")))?;
        Ok(())
    }

    /// Snapshot the base collections only, as bytes. Views are deliberately
    /// left out: the client re-registers every query under a fresh session id
    /// on boot, so persisted views would only be stepped and never read.
    pub fn save_store_state(&self) -> Result<Vec<u8>> {
        guarded(|| {
            self.circuit
                .save_store_image()
                .map_err(|e| Error::Ssp(format!("Failed to serialize store: {e}")))
        })
    }

    /// Install a snapshot written by [`Processor::save_store_state`] UNDER the
    /// views already registered, keeping permissions and projection. Every
    /// registered view is re-primed against the restored rows, so a query that
    /// registered against the empty pre-snapshot store catches up. An empty
    /// snapshot is a no-op.
    pub fn load_store_state(&mut self, bytes: &[u8]) -> Result<Vec<StreamUpdate>> {
        if bytes.is_empty() {
            return Ok(Vec::new());
        }
        guarded(|| {
            let store = Circuit::restore_store(bytes)
                .map_err(|e| Error::Ssp(format!("Failed to deserialize store: {e}")))?;
            let deltas = self.circuit.replace_store(store);
            Ok(to_updates(&deltas, &self.circuit))
        })
    }

    /// Compare one table against the caller's authoritative `(id, rv)` list.
    /// Rows the store holds but the list lacks are stepped out (with view
    /// updates); ids the store lacks or holds at a lower `_00_rv` come back in
    /// `fetch` for the caller to ingest.
    pub fn reconcile(&mut self, table: &str, entries: &[(String, i64)]) -> Result<Reconciled> {
        guarded(|| {
            let result = self.circuit.reconcile(table, entries);
            Ok(Reconciled {
                fetch: result.fetch,
                deleted: result.deleted,
                updates: to_updates(&result.deltas, &self.circuit),
            })
        })
    }

    /// Highest `_00_rv` folded into each table.
    pub fn max_row_versions(&self) -> BTreeMap<String, i64> {
        self.circuit.max_row_versions()
    }

    /// Keep only the fields registered plans evaluate (plus `id`/`_00_rv`) per
    /// stored row. Off by default; must be set before the first ingest to take
    /// effect on those rows.
    pub fn set_projection(&mut self, enabled: bool) {
        self.circuit.set_projection(enabled);
    }
}

// ---- the service --------------------------------------------------------------------

/// Called with every stream update the service dispatches.
pub type StreamUpdateReceiver = Rc<dyn Fn(StreamUpdate)>;

/// What [`StreamProcessorService::register_query_plan`] needs (TS
/// `QueryPlanConfig`).
#[derive(Clone, Debug, PartialEq)]
pub struct QueryPlanConfig {
    pub query_hash: String,
    pub surql: String,
    pub params: Object,
    pub ttl: String,
    pub last_active_at: DateTime<Utc>,
}

/// What a prime did.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PrimeStats {
    /// A snapshot was installed (the rows were reconciled, not all re-read).
    pub restored: bool,
    pub ingested: usize,
    pub deleted: usize,
}

/// Reads a table's rows by id for the prime (in the order asked; ids the
/// store lacks are dropped).
pub type SelectByIds<'a> = &'a dyn Fn(&str, &[String]) -> Result<Vec<Row>>;

/// Gets each primed table's `(id, rv)` pairs.
pub type OnVersions<'a> = &'a dyn Fn(&str, Vec<(String, i64)>);

/// Rows read back per prime ingest.
const PRIME_CHUNK: usize = 500;

/// Enough dirty rows that the client's checkpoint timer writes a snapshot.
pub const CHECKPOINT_FLOOR: usize = 50;

/// Wraps a [`Processor`]: receiver fan-out, the boot prime, snapshot
/// bookkeeping, timed ingest and query-plan registration.
pub struct StreamProcessorService {
    processor: RefCell<Option<Processor>>,
    initialized: Cell<bool>,
    receivers: RefCell<Vec<StreamUpdateReceiver>>,
    /// While set, updates coalesce in `batch_buffer` (one per query, in first
    /// seen order) instead of being dispatched, so a batched ingest updates
    /// the UI once rather than row by row.
    batching: Cell<bool>,
    batch_buffer: RefCell<Vec<StreamUpdate>>,
    /// Snapshot bookkeeping. The circuit is persisted ONLY as a store-only
    /// snapshot ([`Self::save_store_snapshot`]), written by the client's
    /// checkpoint timer and on the way out, never per ingest or per
    /// registration: serializing the circuit walks every row, and on a real
    /// store that is hundreds of milliseconds that used to be paid by every
    /// `queryRaw`.
    dirty: Cell<bool>,
    dirty_rows: Cell<usize>,
    /// The identity used for permission injection. Empty strings (never
    /// absent) match the TS client, which sends `''` when signed out.
    session_auth_id: RefCell<String>,
    session_access: RefCell<String>,
}

impl Default for StreamProcessorService {
    fn default() -> Self {
        Self::new()
    }
}

impl StreamProcessorService {
    pub fn new() -> Self {
        StreamProcessorService {
            processor: RefCell::new(None),
            initialized: Cell::new(false),
            receivers: RefCell::new(Vec::new()),
            batching: Cell::new(false),
            batch_buffer: RefCell::new(Vec::new()),
            dirty: Cell::new(false),
            dirty_rows: Cell::new(0),
            session_auth_id: RefCell::new(String::new()),
            session_access: RefCell::new(String::new()),
        }
    }

    /// A service over an existing processor (tests).
    pub fn with_processor(processor: Processor) -> Self {
        let svc = Self::new();
        *svc.processor.borrow_mut() = Some(processor);
        svc
    }

    /// Run `f` on the processor, `None` when there is none. The borrow ends
    /// before the caller dispatches, so a receiver may call back in.
    fn processor<R>(&self, f: impl FnOnce(&mut Processor) -> R) -> Option<R> {
        self.processor.borrow_mut().as_mut().map(f)
    }

    /// Create the processor. The circuit starts empty; the boot prime fills it
    /// from the local store ([`Self::prime_from_local`]).
    pub fn init(&self) {
        if self.initialized.get() {
            return;
        }
        let mut slot = self.processor.borrow_mut();
        if slot.is_none() {
            *slot = Some(Processor::new());
        }
        self.initialized.set(true);
        tracing::info!("StreamProcessorService initialized");
    }

    pub fn is_initialized(&self) -> bool {
        self.initialized.get()
    }

    // ---- receivers -------------------------------------------------------------

    pub fn add_receiver(&self, receiver: StreamUpdateReceiver) {
        self.receivers.borrow_mut().push(receiver);
    }

    /// Replace every receiver.
    pub fn set_receivers(&self, receivers: Vec<StreamUpdateReceiver>) {
        *self.receivers.borrow_mut() = receivers;
    }

    fn notify(&self, updates: Vec<StreamUpdate>) {
        if !self.batching.get() {
            self.dispatch(updates);
            return;
        }
        // Coalesce by query instead of dispatching. The local array is the
        // full materialized set, so last-write-wins already reflects every
        // prior ingest in the batch. The materialization times are summed so
        // the one recorded sample reflects the batch's total work, and the
        // flush says `CREATE` so the coalesced update takes the immediate
        // (non-debounced) path.
        let mut buffer = self.batch_buffer.borrow_mut();
        for update in updates {
            let pos = buffer
                .iter()
                .position(|u| u.query_hash == update.query_hash);
            let prev_ms = pos
                .and_then(|i| buffer[i].materialization_time_ms)
                .unwrap_or(0.0);
            let coalesced = StreamUpdate {
                materialization_time_ms: Some(
                    prev_ms + update.materialization_time_ms.unwrap_or(0.0),
                ),
                op: Some("CREATE".into()),
                query_hash: update.query_hash,
                local_array: update.local_array,
                result_hash: update.result_hash,
                delta: update.delta,
                ..Default::default()
            };
            match pos {
                Some(i) => buffer[i] = coalesced,
                None => buffer.push(coalesced),
            }
        }
    }

    fn dispatch(&self, updates: Vec<StreamUpdate>) {
        if updates.is_empty() {
            return;
        }
        let receivers = self.receivers.borrow().clone();
        for update in updates {
            for receiver in &receivers {
                receiver(update.clone());
            }
        }
    }

    /// Open a coalescing window: the stream updates of every ingest are
    /// buffered (one per query) until [`Self::end_batch`]. A no-op when one is
    /// already open. Always pair with `end_batch`, or the service stays stuck
    /// buffering.
    pub fn begin_batch(&self) {
        if self.batching.get() {
            return;
        }
        self.batching.set(true);
        self.batch_buffer.borrow_mut().clear();
    }

    /// Close the coalescing window and dispatch one update per buffered query.
    pub fn end_batch(&self) {
        if !self.batching.get() {
            return;
        }
        self.batching.set(false);
        let buffered = std::mem::take(&mut *self.batch_buffer.borrow_mut());
        self.dispatch(buffered);
    }

    // ---- snapshot bookkeeping -------------------------------------------------

    /// True when the circuit holds rows the stored snapshot does not.
    pub fn snapshot_dirty(&self) -> bool {
        self.dirty.get()
    }

    /// How many row changes the stored snapshot is behind by (approximate).
    pub fn dirty_rows(&self) -> usize {
        self.dirty_rows.get()
    }

    fn mark_dirty(&self, rows: usize) {
        if rows == 0 {
            return;
        }
        self.dirty.set(true);
        self.dirty_rows.set(self.dirty_rows.get() + rows);
    }

    /// The snapshot was written: nothing is pending any more.
    pub fn mark_snapshot_clean(&self) {
        self.dirty.set(false);
        self.dirty_rows.set(0);
    }

    /// Snapshot the circuit's base collections for the next boot's prime;
    /// `None` without a processor or when serializing fails.
    pub fn save_store_snapshot(&self) -> Option<Vec<u8>> {
        match self.processor(|p| p.save_store_state())? {
            Ok(bytes) => Some(bytes),
            Err(e) => {
                tracing::warn!("Circuit snapshot failed: {e}");
                None
            }
        }
    }

    /// The client's checkpoint: snapshot a dirty circuit into `db`. Returns
    /// whether a snapshot was written.
    pub fn checkpoint_into(&self, db: &LocalDatabaseService) -> Result<bool> {
        if !self.initialized.get() || !self.dirty.get() {
            return Ok(false);
        }
        let start = Instant::now();
        match self.save_store_snapshot() {
            Some(bytes) if !bytes.is_empty() => {
                db.put_snapshot(&bytes)?;
                self.mark_snapshot_clean();
                tracing::debug!(
                    "Circuit checkpoint: {} bytes in {}ms",
                    bytes.len(),
                    start.elapsed().as_millis()
                );
                Ok(true)
            }
            _ => Ok(false),
        }
    }

    // ---- the prime -----------------------------------------------------------------

    /// Fill the circuit from the LOCAL store.
    ///
    /// With a usable snapshot: install it under whatever views have registered
    /// meanwhile (re-priming them), then reconcile each table against the
    /// store's `(id, rv)` list so rows deleted since the checkpoint are stepped
    /// out and only rows added or changed since are read back and ingested.
    /// Without one: read every row and ingest it, chunked.
    ///
    /// Either way the circuit ends up equal to the local store without touching
    /// the network, so the first sync diff is a real delta rather than "fetch
    /// everything". An unreadable snapshot is not an error: the prime falls
    /// back to the rows. `on_versions` gets each table's primed `(id, rv)`
    /// pairs, minus the rows in `pending_ids`.
    pub fn prime_from_local(
        &self,
        tables: &[String],
        versions: &TableVersions,
        select_by_ids: SelectByIds<'_>,
        snapshot: Option<&[u8]>,
        pending_ids: &HashSet<String>,
        on_versions: Option<OnVersions<'_>>,
    ) -> Result<PrimeStats> {
        if self.processor.borrow().is_none() {
            return Ok(PrimeStats::default());
        }
        let start = Instant::now();
        let mut stats = PrimeStats::default();
        if let Some(bytes) = snapshot.filter(|b| !b.is_empty()) {
            match self.processor(|p| p.load_store_state(bytes)) {
                Some(Ok(updates)) => {
                    self.dispatch(updates);
                    stats.restored = true;
                }
                Some(Err(e)) => {
                    tracing::warn!("Circuit snapshot unreadable; priming from rows: {e}")
                }
                None => {}
            }
        }

        for table in tables {
            let entries = versions.get(table).map(Vec::as_slice).unwrap_or(&[]);
            let to_fetch: Vec<String> = if stats.restored {
                let Some(result) = self.processor(|p| p.reconcile(table, entries)) else {
                    break;
                };
                let result = result?;
                self.dispatch(result.updates);
                stats.deleted += result.deleted;
                result.fetch
            } else {
                entries.iter().map(|(id, _)| id.clone()).collect()
            };
            for chunk in to_fetch.chunks(PRIME_CHUNK) {
                let rows = select_by_ids(table, chunk)?;
                if rows.is_empty() {
                    continue;
                }
                stats.ingested += rows.len();
                let records = rows
                    .into_iter()
                    .map(|row| {
                        let id = row.get("id").cloned().unwrap_or_default().to_id_string();
                        IngestRecord::new(table.as_str(), IngestOp::Create, id, row)
                    })
                    .collect();
                // Dispatches through the usual path (once; the Dart prime
                // dispatched these a second time).
                self.ingest_many(records);
            }
            if let (false, Some(cb)) = (entries.is_empty(), on_versions) {
                // A row with a local write still queued is NOT at the server's
                // version, so it must not be reported as if it were.
                cb(
                    table,
                    entries
                        .iter()
                        .filter(|(id, _)| !pending_ids.contains(id))
                        .cloned()
                        .collect(),
                );
            }
        }
        // A prime that could not restore rebuilt the whole store from rows: the
        // next checkpoint has to write it even if nothing else changes, or every
        // launch pays for the same rebuild. Reconciled ingests were already
        // counted by `ingest_many`; the deletions were not.
        if !stats.restored && stats.ingested > 0 {
            self.mark_dirty(stats.ingested.max(CHECKPOINT_FLOOR));
        }
        self.mark_dirty(stats.deleted);
        tracing::info!(
            "Circuit primed from the local store (restored: {}, ingested: {}, deleted: {}, {}ms)",
            stats.restored,
            stats.ingested,
            stats.deleted,
            start.elapsed().as_millis()
        );
        Ok(stats)
    }

    /// The boot prime over an open store (Dart `ClientServices.sspPrime`):
    /// first reset the versions of tables whose column set changed since their
    /// bodies were cached (see [`crate::services::column_reset`]), then prime
    /// from the store's rows and snapshot.
    pub fn prime_from_store(
        &self,
        db: &LocalDatabaseService,
        tables: &[String],
        column_sets: &BTreeMap<String, String>,
        pending_ids: &HashSet<String>,
        on_versions: Option<OnVersions<'_>>,
    ) -> Result<PrimeStats> {
        // Before anything reads a version: a table whose columns changed since
        // its bodies were cached reads as stale from here on.
        match db.reset_changed_tables(column_sets, pending_ids) {
            Ok(reset) if !reset.is_empty() => {
                tracing::info!("Columns changed since these rows were cached; they will be fetched again: {reset:?}")
            }
            Ok(_) => {}
            Err(e) => tracing::warn!("Column change check failed; cached versions kept: {e}"),
        }
        let versions = db.scan_versions(tables)?;
        let snapshot = db.get_snapshot()?;
        self.prime_from_local(
            tables,
            &versions,
            &|table, ids| db.get_docs(table, ids),
            snapshot.as_deref(),
            pending_ids,
            on_versions,
        )
    }

    // ---- ingest ----------------------------------------------------------------------

    /// Ingest many records as ONE circuit step. Fans the coalesced updates out
    /// and returns them. A failure is logged and yields no updates.
    pub fn ingest_many(&self, records: Vec<IngestRecord>) -> Vec<StreamUpdate> {
        if records.is_empty() {
            return Vec::new();
        }
        match self.processor(|p| p.ingest_many(&records)) {
            None => Vec::new(),
            Some(Err(e)) => {
                tracing::error!("Batch ingest failed: {e}");
                Vec::new()
            }
            Some(Ok(updates)) => {
                if !updates.is_empty() {
                    self.notify(updates.clone());
                }
                self.mark_dirty(records.len());
                updates
            }
        }
    }

    /// Ingest one record change and fan out the resulting updates, stamped
    /// with the ingest's wall time. Returns them unstamped. A failure is logged
    /// and yields no updates.
    pub fn ingest(&self, table: &str, op: IngestOp, id: &str, record: &Row) -> Vec<StreamUpdate> {
        let start = Instant::now();
        let Some(result) = self.processor(|p| p.ingest(table, op, id, record)) else {
            tracing::warn!("Not initialized, skipping ingest");
            return Vec::new();
        };
        let ms = start.elapsed().as_secs_f64() * 1000.0;
        match result {
            Err(e) => {
                tracing::error!("Ingest failed: {e}");
                Vec::new()
            }
            Ok(raw) => {
                if !raw.is_empty() {
                    self.notify(
                        raw.iter()
                            .map(|u| StreamUpdate {
                                materialization_time_ms: Some(ms),
                                ..u.clone()
                            })
                            .collect(),
                    );
                }
                self.mark_dirty(1);
                raw
            }
        }
    }

    // ---- lifecycle ---------------------------------------------------------------------

    /// Throw the circuit away and start from an empty one, keeping the session
    /// identity. Used by a bucket switch: the rows in it belong to the store
    /// that is being replaced. Permissions are seeded again by the switch.
    pub fn reset_circuit(&self) {
        *self.processor.borrow_mut() = Some(Processor::new());
        self.batch_buffer.borrow_mut().clear();
        self.batching.set(false);
        // The new circuit is empty and belongs to the next store, which has its
        // own snapshot: nothing here may be written over it.
        self.mark_snapshot_clean();
    }

    /// Drop the processor.
    pub fn close(&self) {
        self.processor.borrow_mut().take();
        self.initialized.set(false);
    }

    // ---- permissions and session ---------------------------------------------------

    /// Seed per-table `select` permissions from the schema SurrealQL. Call
    /// after [`Self::init`] and before registering real-table queries.
    pub fn seed_permissions_from_schema(&self, schema_surql: &str) {
        let perms = extract_table_permissions(schema_surql);
        let seeded = self.processor(|p| {
            // Built-ins first so a schema-derived permission can override one if
            // the app schema ever does define the table; meta tables absent from
            // the schema keep their built-in seed.
            for (table, text) in BUILTIN_SYSTEM_PERMISSIONS {
                p.set_permission(table, text);
            }
            for (table, text) in &perms {
                p.set_permission(table, text);
            }
        });
        if seeded.is_some() {
            tracing::debug!(
                "Seeded {} table permissions",
                perms.len() + BUILTIN_SYSTEM_PERMISSIONS.len()
            );
        }
    }

    /// Seed one table's permission text directly.
    pub fn set_permission(&self, table: &str, where_text: &str) {
        self.processor(|p| p.set_permission(table, where_text));
    }

    /// Set the signed-in identity for permission injection (TS
    /// `setSessionAuth`). MUST be called before a `$auth`-gated query
    /// registers, and again on every auth change, or the circuit rejects the
    /// registration with "requires $auth but registration params lack it".
    pub fn set_session_auth(&self, auth_id: Option<&str>, access: Option<&str>) {
        *self.session_auth_id.borrow_mut() = auth_id.unwrap_or("").to_string();
        *self.session_access.borrow_mut() = access.unwrap_or("").to_string();
        tracing::debug!(
            "Session auth context updated (authId={}, access={})",
            self.session_auth_id.borrow(),
            self.session_access.borrow()
        );
    }

    /// `(auth id, access)` as registrations see them.
    pub fn session_auth(&self) -> (String, String) {
        (
            self.session_auth_id.borrow().clone(),
            self.session_access.borrow().clone(),
        )
    }

    // ---- registration ----------------------------------------------------------------

    /// Register a query plan and return its initial snapshot update.
    pub fn register_query_plan(&self, plan: &QueryPlanConfig) -> Result<StreamUpdate> {
        // Mirror the server's `fn::query::register`
        // (`object::extend(params, { auth: { id: $auth.id }, access: $access })`).
        // Without these the circuit's permission injection rejects any query
        // whose table permission references $auth with "requires $auth but
        // registration params lack it", which silently breaks every
        // owner-scoped live query.
        let mut params = normalize_object(&plan.params);
        params.insert(
            "auth".into(),
            serde_json::json!({ "id": *self.session_auth_id.borrow() }),
        );
        params.insert(
            "access".into(),
            Json::String(self.session_access.borrow().clone()),
        );
        let config = serde_json::json!({
            "id": plan.query_hash,
            "surql": plan.surql,
            "params": params,
            "clientId": "local",
            "ttl": plan.ttl,
            "lastActiveAt": iso8601(&plan.last_active_at),
        });
        match self.processor(|p| p.register_view(config)) {
            None => Err(Error::Ssp("Stream processor is not initialized".into())),
            Some(result) => Ok(result?.update),
        }
    }

    pub fn unregister_query_plan(&self, query_hash: &str) {
        if let Some(Err(e)) = self.processor(|p| p.unregister_view(query_hash)) {
            tracing::error!("Error unregistering query plan: {e}");
        }
    }

    // ---- pass-through ------------------------------------------------------------------

    /// Highest `_00_rv` folded into each table.
    pub fn max_row_versions(&self) -> BTreeMap<String, i64> {
        self.processor(|p| p.max_row_versions()).unwrap_or_default()
    }

    /// See [`Processor::set_projection`].
    pub fn set_projection(&self, enabled: bool) {
        self.processor(|p| p.set_projection(enabled));
    }
}

// ---- the port --------------------------------------------------------------------------

/// The in-process circuit, as the runtime sees it.
pub struct SspAdapter {
    ssp: Rc<StreamProcessorService>,
}

impl SspAdapter {
    pub fn new(ssp: Rc<StreamProcessorService>) -> Self {
        SspAdapter { ssp }
    }

    pub fn service(&self) -> &Rc<StreamProcessorService> {
        &self.ssp
    }
}

impl SspPort for SspAdapter {
    fn register(&self, plan: RegisterPlan) -> Result<RegisterResult> {
        let update = self.ssp.register_query_plan(&QueryPlanConfig {
            query_hash: plan.query_hash,
            surql: plan.surql,
            params: plan.params,
            ttl: plan.ttl,
            last_active_at: Utc::now(),
        })?;
        Ok(RegisterResult {
            local_array: update.local_array,
            timings: RegistrationTimings {
                parse_ms: update.parse_ms,
                plan_ms: update.plan_ms,
                snapshot_ms: update.snapshot_ms,
                wall_ms: None,
            },
        })
    }

    fn unregister(&self, hash: &str) -> Result<()> {
        self.ssp.unregister_query_plan(hash);
        Ok(())
    }

    fn ingest_many(&self, records: Vec<IngestRecord>) -> Result<()> {
        self.ssp.ingest_many(records);
        Ok(())
    }
}

#[cfg(test)]
mod tests;
