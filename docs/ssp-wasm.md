# SSP WASM (Browser Module)

The SSP WASM module compiles the DBSP incremental computation circuit to WebAssembly, enabling browser-side materialized view maintenance. It exposes a `Sp00kyProcessor` class to JavaScript/TypeScript that mirrors the native circuit's capabilities: register queries, ingest record changes, and receive view deltas — all without server round-trips.

**Package:** `@sp00ky/ssp-wasm` (in `packages/ssp-wasm/`)
**Build:** `wasm-pack build --target web --out-dir pkg`

---

## Architecture

```
JavaScript / TypeScript
    │
StreamProcessorService          ← packages/core/src/services/stream-processor/index.ts
    │  (normalizes RecordId values, manages lifecycle, persists state)
    │
Sp00kyProcessor                 ← packages/ssp-wasm/src/lib.rs (wasm_bindgen)
    │  (thin wrapper: deserialize JS → Rust, call circuit, serialize back)
    │
ssp::circuit::Circuit           ← packages/ssp/src/circuit/circuit.rs
    │  (DBSP engine: Store + Graphs + Views + dependency_map)
    │
ssp_wasm_bg.wasm                ← compiled binary (~830KB)
```

The WASM module is a thin FFI layer. `Sp00kyProcessor` holds a single `Circuit` instance and translates between JavaScript values and Rust types via `serde-wasm-bindgen`.

---

## Public API

### `Sp00kyProcessor` Class

```typescript
import init, { Sp00kyProcessor } from '@sp00ky/ssp-wasm';

// Initialize WASM module (must be called first)
await init();

const processor = new Sp00kyProcessor();
```

#### `ingest(table, op, id, record) → WasmStreamUpdate[]`

Process a single record mutation through the circuit.

```typescript
const updates = processor.ingest(
  "thread",                              // table name
  "CREATE",                              // operation: CREATE | UPDATE | DELETE
  "thread:abc123",                       // record ID
  { title: "Hello", status: "active" }   // record data (JS object)
);
```

**Internal flow:**
1. Deserialize `record` from `JsValue` to `serde_json::Value`
2. Normalize via `ssp::sanitizer::normalize_record`
3. Convert to `Sp00kyValue`, extract record ID
4. Build a `Change` (create/update/delete) and wrap in `ChangeSet`
5. Call `circuit.step(changeset)` → `Vec<ViewDelta>`
6. Transform each `ViewDelta` into `WasmViewUpdate` with record versions from the store
7. Serialize back to JavaScript via `serde-wasm-bindgen`

#### `register_view(config) → WasmStreamUpdate`

Register a materialized view (query plan).

```typescript
const initialResult = processor.register_view({
  id: "query-hash-abc",
  surql: "SELECT * FROM thread WHERE status = 'active' ORDER BY created_at DESC LIMIT 10",
  params: { status: "active" },
  clientId: "local",
  ttl: "30m",
  lastActiveAt: new Date().toISOString()
});
```

**Internal flow:**
1. Deserialize config from `JsValue`
2. Call `ssp::service::view::prepare_registration_dbsp(config)` which parses SurrealQL into an `OperatorPlan` tree
3. Call `circuit.add_query(plan, params, format)` which builds the operator DAG and runs initial snapshot evaluation
4. Return the initial view state as `WasmViewUpdate`

#### `unregister_view(id)`

Remove a registered view.

```typescript
processor.unregister_view("query-hash-abc");
```

#### `save_state() → string`

Serialize the full circuit state (store + query plans + view caches) as a JSON string. Operator DAGs are not serialized — they are rebuilt from query plans on restore.

```typescript
const stateJson = processor.save_state();
// Persist to database, localStorage, etc.
```

#### `load_state(state: string)`

Restore circuit state from a previously saved JSON string. Rebuilds operator DAGs from the stored query plans. Permissions, link targets, opaque fields and the projection setting of the current processor are carried over (a snapshot holds state, not configuration).

```typescript
processor.load_state(savedStateJson);
```

#### `save_store_state() → Uint8Array` / `load_store_state(bytes) → WasmViewUpdate[]`

The snapshot pair the browser client actually uses. `save_store_state` serializes ONLY the base collections, as bytes: the binary row image (`SPKYSTOR`, one per-table checkpoint image per collection, every row's head and body exactly as the circuit holds them with their digests), so a restore is a copy of bytes and a walk over the heads rather than a decode of every row. An image of the previous layout is converted on load, and a JSON snapshot written by an older build is recognised by its first byte and still loads. `load_store_state` installs those rows UNDER the views that are already registered, keeps permissions and projection, re-primes every view against the restored rows and returns their new full results. That order matters: on a reload, queries register before the snapshot has been read from disk.

```typescript
const bytes = processor.save_store_state();      // checkpoint (leader tab, on a timer)
const updates = processor.load_store_state(bytes); // next boot, after views registered
```

#### `reconcile(table, entries: [id, rv][]) → { fetch, deleted, updates }`

Client-side `_00_rv` catch-up, the counterpart of the server's restore-then-catch-up. Compares one table against the durable store's `(id, rv)` list: rows the circuit holds that the list lacks are stepped out as deletes (with view updates), and ids the circuit lacks or holds at a lower `_00_rv` come back in `fetch` for the caller to read out of its store and `ingest_many`.

#### `set_projection(enabled)`

Keep only the fields registered plans evaluate (filter predicates, join keys, sort keys, subquery parent keys) plus `id`/`_00_rv` per stored row. The circuit never reads anything else and the client renders bodies from its own store. Measured on 7700 rows with 20 KB bodies: wasm heap peak ~500 MB → ~24 MB, snapshot 156 MB → 1.2 MB. `register_view`'s result carries `missing_fields` when a new plan evaluates fields that stored rows were kept without; the caller merges them in with `ingest_many` items whose `op` is `MERGE`.

#### `compact() → number`, `dead_bytes()`, `live_bytes()`, `max_row_versions()`, `size_report()`

Row-arena hygiene and diagnostics. Updates append a fresh copy of the row and orphan the old bytes; `compact()` rebuilds without them (and re-projects) and returns how many bytes were dead. Decodes every row, so checkpoint-time only. Unchanged writes (same canonical digest) never append in the first place.

#### `free()`

Explicitly free the WASM memory for this processor (also available via `Symbol.dispose`).

---

## TypeScript Interfaces

Defined in the generated `pkg/ssp_wasm.d.ts` and mirrored in `packages/core/src/services/stream-processor/wasm-types.ts`:

```typescript
// Output from ingest() and register_view()
interface WasmStreamUpdate {
  query_id: string;             // The registered query's ID
  result_hash: string;          // Blake3 hash of current view membership
  result_data: [string, number][];  // [[record_id, version], ...]
}

// Config for register_view()
interface WasmIncantationConfig {
  id: string;
  sql: string;                  // SurrealQL query string
  params?: Record<string, any>;
  clientId: string;
  ttl: string;
  lastActiveAt: string;
  safe_params?: Record<string, any>;
  format?: 'flat' | 'tree' | 'streaming';
}

// Input for ingest()
interface WasmIngestItem {
  table: string;
  op: string;                   // "CREATE" | "UPDATE" | "DELETE"
  id: string;
  record: any;
}
```

---

## StreamProcessorService (TypeScript Wrapper)

In the browser, `Sp00kyProcessor` is not used directly. It's wrapped by `StreamProcessorService` in `packages/core/src/services/stream-processor/index.ts` which provides:

### Lifecycle management
- `init()` — Initializes WASM module, creates `Sp00kyProcessor`, applies projection
- `primeFromLocal(ctx)` — Fills the circuit from the LOCAL store in the background: restores the snapshot (if any) under the registered views, then `reconcile`s each table against the store's `(id, rv)` list and ingests only what changed; without a snapshot it reads every cached row. `whenPrimed()` gates the sync layer's first diff so a reload never re-downloads the working set.
- `checkpoint()` — Writes a store-only snapshot through the engine (`putSnapshot`), on a timer and when the page goes hidden, never per ingest

### Value normalization
- Recursively converts SurrealDB `RecordId` objects to strings (e.g., `RecordId { table: "user", id: "123" }` → `"user:123"`)
- Handles nested objects and arrays

### Event dispatch
- After each `ingest()`, transforms `WasmStreamUpdate[]` into `StreamUpdate[]` and notifies registered receivers (DataManager, DevTools, etc.)

```typescript
// Usage in the Sp00ky client
const service = new StreamProcessorService(events, db, logger);
await service.init();

// Register a query
const initial = service.registerQueryPlan({
  queryHash: "abc123",
  surql: "SELECT * FROM thread WHERE status = 'active'",
  params: {},
  ttl: "30m",
  lastActiveAt: new Date(),
  localArray: [],
  remoteArray: [],
  meta: { tableName: "thread" }
});

// Process mutations (called on SurrealDB LIVE events)
service.ingest("thread", "CREATE", "thread:xyz", { title: "New post" });

// Listen for view updates
service.addReceiver({
  onStreamUpdate(update) {
    console.log(update.queryHash, update.localArray);
  }
});
```

### State persistence
- Store-only snapshot bytes + a JSON meta row (`formatVersion`, `schemaHash`, `savedAt`, `maxRv`) in the local store's `_00_circuit_snapshot` table (OPFS SQLite; per-bucket by construction, atomic, readable by follower tabs over the port transport). Not available on the SurrealDB/IndexedDB engine, which primes from rows only.
- Written by `checkpoint()` (leader/solo tab only) once at least ~50 rows changed, on the `circuitCheckpointMs` interval and on `visibilitychange: hidden`; `compact()` runs first when dead bytes outweigh live ones.
- Read by `primeFromLocal()` on boot; a snapshot from another format version or schema hash is ignored.

---

## Build

### Build command
```bash
cd packages/ssp-wasm
wasm-pack build --target web --out-dir pkg
```

### Output artifacts (`pkg/`)
| File | Description |
|------|-------------|
| `ssp_wasm.js` | JavaScript wrapper with `Sp00kyProcessor` class and `init()` |
| `ssp_wasm.d.ts` | TypeScript type definitions |
| `ssp_wasm_bg.wasm` | Compiled WebAssembly binary (~830KB) |
| `ssp_wasm_bg.wasm.d.ts` | Low-level WASM interface types |
| `package.json` | Generated NPM package metadata |

### Cargo configuration
```toml
[lib]
crate-type = ["cdylib", "rlib"]   # cdylib = WASM binary, rlib = Rust lib

[dependencies]
wasm-bindgen = "0.2"              # JS/Rust FFI
serde-wasm-bindgen = "0.6"        # Zero-copy JS ↔ Rust serialization
ssp = { path = "../ssp" }         # Core DBSP engine
js-sys = "0.3"                    # JavaScript API bindings
web-sys = { version = "0.3", features = ["console"] }  # Browser APIs
getrandom = { version = "0.2", features = ["js"] }     # WASM-compatible RNG
```

### WASM initialization

**Browser (async):**
```typescript
import init, { Sp00kyProcessor } from '@sp00ky/ssp-wasm';
await init();  // Fetches and instantiates .wasm file
```

**Node.js (sync, for testing):**
```typescript
import { readFileSync } from 'node:fs';
import { initSync, Sp00kyProcessor } from '@sp00ky/ssp-wasm';
initSync({ module: readFileSync('./pkg/ssp_wasm_bg.wasm') });
```

---

## Differences from Native (SSP App)

| Aspect | SSP App (native) | SSP WASM (browser) |
|--------|-------------------|---------------------|
| **Allocator** | mimalloc | Default WASM allocator |
| **Parallelism** | Rayon (multi-threaded) | Single-threaded (JS event loop) |
| **Networking** | Axum HTTP server, SurrealDB client | None — called by JS host |
| **Edge management** | Writes edges to SurrealDB via RELATE/UPDATE/DELETE | Returns deltas to JS — host manages persistence |
| **Persistence** | BackgroundSaver writes JSON to disk | Host calls save_state()/load_state(), stores in DB |
| **Observability** | OpenTelemetry tracing + metrics | Console logging only (`web_sys::console`) |
| **Binary size** | Native executable | ~830KB WASM binary |
| **Scheduler** | Registers with scheduler, heartbeats | N/A |
| **Job runner** | Processes outbox jobs | N/A |

Both share the same core: `ssp::circuit::Circuit` with its `Store`, `Graph`, `View`, and `Operator` implementations.

---

## Data Flow

```
1. JS: service.ingest("thread", "CREATE", "thread:1", { title: "Hi" })
         │
2. StreamProcessorService.normalizeValue(record)
   └─ Converts RecordId objects to "table:id" strings
         │
3. Sp00kyProcessor.ingest(table, op, id, record)   [crosses WASM boundary]
         │
4. serde_wasm_bindgen::from_value(record)  →  serde_json::Value
         │
5. sanitizer::normalize_record(record)     →  serde_json::Value (clean)
         │
6. Sp00kyValue::from(clean_record)
   Operation::from_str(op)
   Change::create|update|delete(table, id, sp00ky_value)
         │
7. circuit.step(ChangeSet { changes: [change] })
   ┌─────┴──────────────────────────┐
   │  Store: apply change to         │
   │  collection rows + zset weights  │
   │                                  │
   │  For each affected query:        │
   │  ├─ Walk operator DAG in topo    │
   │  │  order, calling step()        │
   │  └─ Output node produces         │
   │     view-level delta             │
   │                                  │
   │  Apply delta to view cache       │
   │  Compute result hash             │
   └─────┬──────────────────────────┘
         │
8. Vec<ViewDelta> → transform_deltas() → Vec<WasmViewUpdate>
   └─ Adds record versions from store
         │
9. serde_wasm_bindgen::Serializer → JsValue   [crosses WASM boundary back to JS]
         │
10. StreamProcessorService maps to StreamUpdate[], notifies receivers
         │
11. StreamProcessorService marks the snapshot dirty; the checkpoint timer later runs
    processor.save_store_state() → engine.putSnapshot()
```

---

## Refactoring Notes

### Already done
- The WASM module uses the **new circuit module** (`ssp::circuit::Circuit`) exclusively. It imports `Circuit`, `Change`, `ChangeSet`, `Operation`, `ViewDelta` from `ssp::circuit`.
- View registration uses `prepare_registration_dbsp()` which produces `operator::plan::QueryPlan` + `circuit::view::OutputFormat`.

### Still needed

1. **Remove the old `engine/` module from `packages/ssp/`** — The WASM module does not use it. The old `engine::circuit::Circuit`, `engine::view::View`, `engine::operators::Operator` (the enum), and all old types are dead code.

2. **Clean up `packages/ssp/src/lib.rs` re-exports** — Currently re-exports old engine types:
   ```rust
   pub use engine::circuit::Circuit;        // dead
   pub use engine::view::QueryPlan;         // dead
   pub use engine::update::*;               // dead
   pub use engine::types::*;                // partially dead
   pub use engine::operators::*;            // dead
   ```
   Should be replaced with re-exports from `circuit/` and `operator/` modules.

3. **Remove `prepare_registration()` from `service.rs`** — Only `prepare_registration_dbsp()` is used. The old path that produces `engine::operators::Operator` and `engine::view::QueryPlan` is dead code.

4. **Type the WASM API more strictly** — The generated `.d.ts` types `ingest()` and `register_view()` as returning `any`. The `WasmStreamUpdate` interface is defined in a custom TypeScript section but not linked to the method signatures. Consider using `tsify` or manual `wasm_bindgen` return type annotations.

5. **Remove the `WasmViewUpdate` transform layer** — Currently `transform_deltas()` in `ssp-wasm/src/lib.rs` maps `ViewDelta` to a custom `WasmViewUpdate` struct that pairs record keys with versions from the store. This version lookup (`store.get_record_version_by_key`) is WASM-specific logic leaking into the FFI layer. Consider moving version tracking into the circuit's `ViewDelta` output so both runtimes get versions natively.

6. **Consolidate `StreamProcessorService` type assertions** — The TypeScript wrapper uses `(this.processor as any).save_state()` and `(this.processor as any).load_state()` because `WasmProcessor` interface doesn't include these methods. Add `save_state` and `load_state` to the `WasmProcessor` interface in `wasm-types.ts`.

7. **Align the TypeScript wrapper's `WasmQueryConfig` with the Rust struct** — The TypeScript `WasmQueryConfig` uses `surql` but the Rust custom TS section defines `WasmIncantationConfig` with `sql`. These should use the same field name.
