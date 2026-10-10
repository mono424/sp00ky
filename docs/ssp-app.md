# SSP App (Sidecar Server)

The SSP app is a native Rust HTTP server that runs as a sidecar alongside SurrealDB. It receives record mutations via REST API, processes them through the DBSP incremental computation circuit, and writes materialized view edges back to SurrealDB.

**Crate:** `ssp-server` (in `apps/ssp/`)
**Entry point:** `apps/ssp/src/main.rs` -> `run_server()`

---

## Architecture

```
SurrealDB  ──LIVE events──>  Scheduler  ──POST /ingest──>  SSP App
                                                              │
                                                     Circuit (Arc<RwLock>)
                                                     ├─ Store (base tables)
                                                     ├─ Graphs (operator DAGs) ← transient, rebuilt on view registration
                                                     ├─ Views (materialized state) ← transient, recomputed on bootstrap
                                                     └─ dependency_map (table → queries) ← transient
                                                              │
                                                     ViewDelta (additions/removals/updates)
                                                              │
                                                     ──RELATE/UPDATE/DELETE──> SurrealDB edges
```

### Core Components

| Component | Type | Description |
|-----------|------|-------------|
| `Circuit` | `Arc<RwLock<ssp::circuit::Circuit>>` | The DBSP computation engine. Shared across all request handlers via Axum state. |
| `SharedDb` | `Arc<Surreal<Client>>` | SurrealDB WebSocket connection for reading/writing edges and metadata. |
| `SspStatus` | `Arc<RwLock<SspStatus>>` | Bootstrap status (`Bootstrapping` or `Ready`). Exposed via `/health`. |
| `Metrics` | OpenTelemetry | Counters and histograms for ingest throughput, edge operations, active views. |
| `JobRunner` | Tokio task | Optional outbox job processor for configured job tables. |

The server uses Axum with shared `AppState`:

```rust
// apps/ssp/src/lib.rs
pub struct AppState {
    pub db: SharedDb,
    pub processor: Arc<RwLock<Circuit>>,
    pub status: Arc<RwLock<SspStatus>>,
    pub metrics: Arc<Metrics>,
    pub job_config: Arc<JobConfig>,
    pub job_queue_tx: mpsc::Sender<JobEntry>,
}
```

---

## Self-Bootstrap

SurrealDB is the source of truth; what the SSP holds is a cache. On every startup, the SSP self-bootstraps:

1. **Discover tables** — `INFO FOR DB` upstream, filter out system tables (`_00_*`)
2. **Load table data** — standalone: keyset-paged `SELECT *` per table through `ssp_node::bootstrap::rebuild_from_db`, bulk-load via `Circuit::load()`. Cluster: the rows kept as row checkpoints (`src/warm.rs`) are loaded first, then every table is verified against the scheduler's hash at the registration cut; a matching table is kept, a differing one repaired range by range, the rest paged through the scheduler proxy
3. **Re-register views** — `SELECT * FROM _00_query`, rebuild each view via `prepare_registration_dbsp()` + `circuit.add_query()`
4. **Set status to Ready** — `/health` transitions from `"bootstrapping"` to `"ready"`

The bootstrap runs in a spawned Tokio task so the HTTP server is available immediately (the scheduler can poll `/health` to know when the SSP is ready).

---

## Configuration

All configuration is via environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `LISTEN_ADDR` | `0.0.0.0:8667` | HTTP server listen address |
| `SURREALDB_ADDR` | `127.0.0.1:8000` | SurrealDB WebSocket address |
| `SURREALDB_USER` | `root` | SurrealDB username |
| `SURREALDB_PASS` | `root` | SurrealDB password |
| `SURREALDB_NS` | `test` | SurrealDB namespace |
| `SURREALDB_DB` | `test` | SurrealDB database |
| `SP00KY_AUTH_SECRET` | (empty) | Bearer token for auth middleware |
| `SP00KY_CONFIG_PATH` | `sp00ky.yml` | Path to job runner configuration |
| `SCHEDULER_URL` | (none) | Scheduler URL for registration and heartbeats |
| `SSP_ID` | `ssp-<uuid>` | Unique identifier for this SSP instance |
| `HEARTBEAT_INTERVAL_MS` | `5000` | Heartbeat interval to scheduler |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `http://localhost:18888` | OpenTelemetry OTLP endpoint |
| `OTEL_SERVICE_NAME` | `ssp` | OpenTelemetry service name |

---

## HTTP API

All endpoints require `Authorization: Bearer <SP00KY_AUTH_SECRET>` header.

### `POST /ingest`

Process a single record mutation.

**Request:**
```json
{
  "table": "thread",
  "op": "CREATE",
  "id": "thread:abc123",
  "record": { "title": "Hello", "status": "active", "_00_rv": 1 }
}
```

**Behavior:**
1. Parses operation (CREATE/UPDATE/DELETE)
2. Normalizes record via `ssp::sanitizer::normalize_record`
3. If table is a configured job table and op is CREATE with status "pending", queues job
4. Creates a `Change` and runs `circuit.step(ChangeSet { changes: [change] })`
5. For each returned `ViewDelta`, generates SurrealQL to RELATE/UPDATE/DELETE edges in a single transaction

**Response:** `200 OK` (no body)

**Edge operations in SurrealDB:**
- Additions: `RELATE $from->_00_list_ref->$record_id SET version = N, clientId = ...`
- Updates: `UPDATE _00_list_ref SET version = N WHERE in = $from AND out = $record_id`
- Removals: `DELETE $from->_00_list_ref WHERE out = $record_id`

All edge operations are wrapped in `BEGIN TRANSACTION; ... COMMIT TRANSACTION;`.

### `POST /view/register`

Register a materialized view.

**Request:**
```json
{
  "id": "view-abc",
  "surql": "SELECT * FROM thread WHERE status = 'active'",
  "params": {},
  "clientId": "client-1",
  "ttl": "30m",
  "lastActiveAt": "2025-01-01T00:00:00Z"
}
```

**Behavior:**
1. Calls `ssp::service::view::prepare_registration_dbsp(payload)` which parses SurrealQL into an `OperatorPlan` tree
2. Calls `circuit.add_query(plan, params, Some(OutputFormat::Streaming))`
3. Upserts incantation metadata to `_00_query` table in SurrealDB
4. Creates initial edges for any matching records

**Response:** `200 OK`

### `POST /view/unregister`

Remove a materialized view.

**Request:**
```json
{ "id": "view-abc" }
```

**Behavior:** Calls `circuit.remove_query(id)` and deletes all `_00_list_ref` edges from that incantation.

### `POST /reset`

Clear all circuit state and edges.

### `POST /log`

Remote logging endpoint (receives logs from clients).

```json
{ "message": "...", "level": "info", "data": null }
```

### `GET /health`

Returns bootstrap status and circuit summary.

```json
{ "status": "bootstrapping", "views": 0, "tables": 0 }
```

Status values:
- `"bootstrapping"` — SSP is loading data from SurrealDB
- `"ready"` — SSP is fully initialized and accepting ingests

### `GET /version`

```json
{ "version": "0.1.0", "mode": "streaming" }
```

### `GET /debug/view/:view_id`

Returns the internal cache state of a view (for debugging).

```json
{
  "view_id": "view-abc",
  "cache_size": 5,
  "last_hash": "abc123...",
  "format": "Streaming",
  "cache": [{ "key": "thread:1", "weight": 1 }, ...]
}
```

---

## Data Flow

```
1. POST /ingest { table, op, id, record }
         │
2. normalize_record(record)
         │
3. Change::create|update|delete(table, id, clean_record)
         │
4. circuit.step(ChangeSet { changes: [change] })
         │
   ┌─────┴──────────────────────────┐
   │  Store: apply changes to        │
   │  collection rows + zset weights  │
   │                                  │
   │  For each affected query:        │
   │  ├─ Walk operator DAG in topo    │
   │  │  order calling step()         │
   │  ├─ Scan injects table delta     │
   │  ├─ Filter/Map/Join process      │
   │  │  deltas incrementally         │
   │  └─ Output node produces         │
   │     view-level delta             │
   │                                  │
   │  Apply delta to view cache       │
   │  Compute new result hash         │
   └─────┬──────────────────────────┘
         │
5. Vec<ViewDelta> { query_id, additions, removals, updates, records, result_hash }
         │
6. update_all_edges(db, deltas) — single SurrealDB transaction
```

---

## Observability

### Tracing

OpenTelemetry tracing via `tracing-opentelemetry`. Key instrumented spans:
- `ingest_handler` — fields: table, op, id, payload_size_bytes, views_affected, edges_updated
- `register_view_handler` — fields: view_id
- `unregister_view_handler` — fields: view_id
- `update_all_edges` — fields: total_operations

### Metrics

Exported via OTLP every 15 seconds:

| Metric | Type | Description |
|--------|------|-------------|
| `ssp_ingest_total` | Counter | Total ingest operations (by table, op) |
| `ssp_ingest_duration_milliseconds` | Histogram | Ingest handler latency |
| `ssp_views_active` | UpDownCounter | Current number of registered views |
| `ssp_edge_operations_total` | Counter | Edge operations (by create/update/delete) |
| `ssp_ingest_rate_per_minute` | Observable Gauge | Rolling ingestion rate |

---

## Scheduler Integration

When `SCHEDULER_URL` is set:

1. **Registration** — On startup, POST to `{SCHEDULER_URL}/ssp/register` with `{ ssp_id, url }`.
2. **Heartbeat** — Every `HEARTBEAT_INTERVAL_MS`, POST to `{SCHEDULER_URL}/ssp/heartbeat` with `{ ssp_id, timestamp, active_queries, cpu_usage, memory_usage }`.
   - `404` response: needs re-registration
   - `409` response: buffer overflow

The scheduler can poll `GET /health` and wait for `"status": "ready"` before routing ingests to this SSP instance.

---

## Refactoring Notes

### Already done
- The SSP app uses the **new circuit module** (`ssp::circuit::Circuit`) exclusively. All handlers go through `Circuit::step()`, `Circuit::add_query()`, `Circuit::remove_query()`.
- View registration uses `prepare_registration_dbsp()` which produces `operator::plan::QueryPlan` + `circuit::view::OutputFormat`.
- No standalone persistence: the circuit snapshot a standalone SSP used to write every five minutes was never read back, so the VM shell no longer writes it. A cluster SSP keeps row checkpoints (`src/warm.rs`); a standalone one self-bootstraps from SurrealDB on every startup.

### Still needed

1. **Remove the old `engine/` module from `packages/ssp/`** — The SSP app does not use it. The old `engine::circuit::Circuit`, `engine::view::View`, `engine::operators::Operator` (the enum), and all old types (`ViewUpdate`, `ViewResultFormat`, `DeltaEvent`) are dead code from this app's perspective.

2. **Clean up `packages/ssp/src/lib.rs` re-exports** — Currently re-exports old engine types:
   ```rust
   pub use engine::circuit::Circuit;        // dead, should be circuit::Circuit
   pub use engine::view::QueryPlan;         // dead, should be operator::plan::QueryPlan
   pub use engine::update::*;               // dead
   pub use engine::types::*;                // partially dead
   pub use engine::operators::*;            // dead
   ```
   These should be replaced with re-exports from the new modules.

3. **Remove `prepare_registration()` from `service.rs`** — Only `prepare_registration_dbsp()` is used. The old path that produces `engine::operators::Operator` and `engine::view::QueryPlan` can be deleted.

4. **Consolidate service types** — `service.rs` still imports from `crate::engine::*` for the old `RegistrationData` struct. The `DbspRegistrationData` struct should become the only `RegistrationData`.

5. **Align the `converter.rs` output** — The converter produces a `serde_json::Value` which is then deserialized into either the old `Operator` enum or the new `OperatorPlan`. Since only `OperatorPlan` is needed, the converter could return `OperatorPlan` directly instead of going through JSON.
