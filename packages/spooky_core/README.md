# spooky_core

Pure-Dart core for Spooky local-first sync, the twin of `@spooky-sync/core`.
Framework-agnostic: query subscriptions are exposed as Dart `Stream`s, so a
Flutter app consumes them with a `StreamBuilder`.

## Startup and execution

`Sp00kyClient(config)` owns one persistent background isolate. SQLite, native
query processing, network decoding and synchronization execute there. Generated
`AppDb(client)` works unchanged. Callbacks and stream listeners run in the caller
isolate; only subscribed rows and small status updates are mirrored.

`await client.init()` means the saved identity is restored, its local account
store is open and cached queries are readable. It does not wait for connection,
verification, uploads or refreshed membership. Concurrent calls share that work.
Transport failures preserve a restored session. Explicit rejected credentials
or a confirmed missing account clear it; other verification failures can be
inspected through `auth.verificationError` and retried with `wake()`, reconnect,
or the existing connection probe.

Flutter supplies an absolute `localDbPath`, calls `checkpoint()` when hidden and
`wake()` on resume. `close()` checkpoints and releases the worker.

A checkpoint writes the circuit's store (no views) as one snapshot, and the next
boot restores it and reconciles against the rows' `_00_rv`, so startup costs a
restore rather than re-ingesting every cached row. The core also checkpoints on
its own while the circuit is dirty (5 s after boot, then every
`circuitCheckpointMs`, default 30 s), because a swipe-killed app often never
runs its lifecycle hook; `checkpoint()` is a no-op when nothing changed. The
circuit is never serialized per ingest or per registration: that full-state
JSON is gone (older stores have it dropped on boot). A failed worker
rejects outstanding operations and ends subscriptions; it never falls back to
the UI thread. Use `await client.inspectState()` for on-demand diagnostics.

Session routing lives in a sibling `*.session.db` SQLite file. Its atomic row
contains the active token and account. Existing anonymous boot hints and account
tokens are migrated once, including an explicit signed-out marker. Account
SQLite files, memberships and pending writes keep their format. Snapshots are
optional accelerators; SQLite rows remain authoritative.

Custom persistence and injected transports require the explicit advanced client:

```dart
import 'package:spooky_core/advanced.dart';
final client = InProcessSp00kyClient(config, remoteClient: customTransport);
```

This also provides the rollback execution mode without changing account files.
Keep the new session metadata when rolling back execution mode. Downgrading to
an older package that does not understand that metadata is not the same rollback
and can restore its legacy credentials. Direct stores, native handles and
runtime dispatch are available only through the advanced client. Auth is exposed
as `Sp00kyAuth`: use auth operations and live profile queries instead of assigning
`auth.currentUser`.

## Architecture

The engine is **effects-as-data**, the same shape as the TypeScript core, so a
fix on either side lands in the obvious file on the other.

```
Sp00kyClient (worker proxy) -> persistent native isolate
InProcessSp00kyClient  every method runs one saga, reads a selector, or
      │                 attaches a subscriber
      ├─ Runtime        client/runtime.dart - holds the state, runs sagas on
      │                 serial/dedupe lanes, fires timers, fans events out and
      │                 schedules materialization for dirty queries
      ├─ route()        client/router.dart - event -> (saga, lane)
      ├─ sagas          query/ mutation/ sync/ boot/ - pure: they name effects
      │                 and never touch a service, a clock or a Timer
      └─ Interpreter    kernel/interpreter.dart - the ONLY place an effect
                        executes, against the adapters below
```

Under the interpreter: the sqlite local store, the FFI stream processor (the
same Rust DBSP circuit the browser runs through WASM), the SurrealDB WebSocket
client with its connection supervisor, auth, and persistence.

```
lib/src/
  kernel/    effects, the saga contract, lanes, the interpreter, constants
  state/     ClientState, the query lifecycle machine, reducers, selectors
  query/     register, membership, fetch, materialize, lifecycle (+ their sagas)
  mutation/  outbox rows, write, drain, rollback, the failed-writes tray
  sync/      poll, live, connection, health policy
  boot/      boot, auth flip, bucket switch, preload
  client/    the runtime, the router, the adapters
  services/  sqlite store, FFI stream processor, remote socket, supervisor
  modules/   auth, feature flags, app releases, query builder, buckets
  testing/   run_pure (canned-effect saga harness), fakes, state builders
```

### The query lifecycle

`phase` answers "where do this query's rows come from":

- `cold` never resolved on this device: rows come from the SSP's local window,
  and a binding shows its loader because the query is not authoritative.
- `cached` a durable `_00_view` row was found: rows come from that id-set, so a
  relaunch paints from the local store with no network on the paint path.
- `live` a server membership set was accepted this session.
- `viewLost` the server's `_00_query` row vanished while we held membership:
  the rows are KEPT and a re-registration is under way.

An empty result is only empty once the query is authoritative. `_00_query`'s
`rowCount` and `state` are what tell "no rows" apart from "the view has not
published its edges yet"; an empty edge read with no `_00_query` row is a lost
view, never an empty one.

## Deliberate divergences from the browser core

1. **Sagas are functions over an effect context, not generators.** Dart's
   `yield` is one-way and `Iterator` has no `next(value)`, so a saga is
   `Future<R> Function(Ctx)` and yields effects by awaiting them. Every property
   that matters is kept: effects are data, one interpreter executes them, a saga
   holds no adapter reference, and `testing/run_pure.dart` drives it with canned
   results. Effects are generic in their result type, so a saga reads
   `await ctx(Fx.now())` with no cast.
2. **The local store is a document store.** sqlite cannot run SurrealQL, so the
   `local.*` effects address it by `(table, id)` and a transaction is data
   (`LocalTx(List<LocalOp>)`). Materialization resolves the render set by id
   rather than re-running the query; a windowed query re-applies its own
   `ORDER BY` because its ids come from `_00_list_ref`, not the circuit.
   `.related()` projections are resolved from the local cache by
   `query/relation_resolver.dart`.
3. **Permission seeding.** The browser circuit is effectively permissive; the
   native circuit is default-deny. `StreamProcessorService.seedPermissionsFromSchema`
   extracts each table's `PERMISSIONS FOR select` from `schemaSurql` and seeds
   the circuit the way the SSP server does at boot. Without it `registerView`
   fails.
4. **The transport owns its reconnect.** The SurrealDB Dart client has no
   reconnect loop, so `ConnectionSupervisor` covers all three ways a connection
   dies: a closed socket (a revive loop on backoff, forever), a half-open one (a
   heartbeat with a two-failure budget forces the teardown), and a wake signal.
   The core is framework-agnostic, so the app calls `client.wake()` - in Flutter,
   from `AppLifecycleState.resumed`.
5. **Local-only mode.** With no endpoint configured the client never starts the
   network half and never drains the outbox: draining against nothing would fail
   in a way the classifier reads as the server rejecting the write, and a
   rejection rolls the write back.

Not ported (browser-only): shared tabs, the SQLite-WASM worker and its
SurrealQL translator, the DevTools window bridge, the OTel exporter. CRDT
collaborative fields are still deferred (the seam is in place).

The blob cache is ported with files on disk in place of OPFS
(`Sp00kyConfig.blobCache.directory`; in-memory without one). Its manifest is
rebuilt from a directory walk at start rather than kept in a `_00_blob` table,
so there is nothing to migrate; `pin` and `revalidate: 'head'` are not ported.
`bucket().read()` is the cached read, `get()` the raw one; `spooky_flutter`
renders through it with `BucketImage`.

## Native library

The FFI processor needs `libssp_ffi`. Build and stage it with:

```bash
bash packages/ssp-ffi/build-native.sh           # host (macOS/Linux/Windows)
bash packages/ssp-ffi/build-native.sh android   # arm64-v8a + x86_64
bash packages/ssp-ffi/build-native.sh ios       # xcframework
```

`SSP_FFI_PATH` overrides the library location (used in dev and tests).

## Usage

```dart
final client = Sp00kyClient(Sp00kyConfig(
  database: const DatabaseConfig(namespace: 'app', database: 'app'),
  schema: schema,           // { table: { columns: { name: ColumnSchema(...) } } }
  schemaSurql: schemaSurql, // DEFINE TABLE ... PERMISSIONS ...
));
await client.init();

final stream = await client.queryStream('SELECT * FROM thread', {});
stream.listen((records) => print(records)); // or StreamBuilder in Flutter

await client.create('thread:abc', {'title': 'hello'});

// Bucket files, cached on disk after the first read (see blobCache).
final bytes = await client.bucket('covers').read('abc_t.webp');
```

A binding decides when to show a loader from the query's authority, not from its
status:

```dart
final loading = !client.isQueryAuthoritative(hash);   // no server answer yet
final empty = client.isQueryAuthoritative(hash) && rows.isEmpty;
final complete = client.isQuerySettled(hash);         // sized, for a long list
```

Writes the server rejects are undone locally and parked, rather than retried
forever or dropped:

```dart
client.subscribeToFailedMutations((count) => setState(() => failed = count));
for (final row in await client.listFailedMutations()) {
  await client.retryFailedMutation(row.id);   // or discardFailedMutation
}
```

### Compile-time-typed client (codegen)

The dynamic API above (`String` tables, `Map` records) can be wrapped in a fully
typed facade generated from the schema:

```bash
dart run spooky_core:spooky_gen schema.surql \
    --openapi api=api/openapi.yml -o lib/app_db.g.dart
```

```dart
final db = AppDb.open(const DatabaseConfig(namespace: 'app', database: 'app'));
await db.init();

Stream<List<Thread>> s = db.thread.query()
    .where([Thread$.published.eq(true), Thread$.score.gt(10)])
    .orderBy(Thread$.createdAt, desc: true)
    .watch();

await db.thread.create(Thread(id: 'thread:a', title: 'hi', author: uid));
await db.run.api.spookify(id: 'thread:a');
await db.auth.signInAccount(email: 'a@b.c', password: 'pw');
```

Wrong table/field, wrong operand type, or a missing route/auth arg is a **compile
error**. The generated facade wraps `Sp00kyClient`; the typed-query runtime lives
in `package:spooky_core/typed.dart`.

## Tests

The `ssp-ffi` Rust cdylib installs `std`'s signal/stack-overflow handlers when
loaded, which the Dart `test` runner's *secondary* suite isolates do not
tolerate. So run each test file in its own process:

```bash
tool/run_tests.sh                 # unit tests, one process per file
tool/run_tests.sh --integration   # integration tests (need a server)
```

`dart test <single_file>` also works; only the aggregate `dart test` is
affected. Real single-process app usage is fine.

Every saga has a sibling test that drives it through `testing/run_pure.dart`
with canned effect results and asserts the effect log, so a saga that yields a
different sequence than its TypeScript twin fails.

### Integration tests

Tagged `integration` and skipped unless a server is reachable:

```bash
docker run -d --name surreal -p 18011:8000 \
  surrealdb/surrealdb:v2.1.4 start --user root --pass root --allow-all memory
cd packages/spooky_core
SURREAL_IT_ENDPOINT=ws://127.0.0.1:18011 dart test --tags integration
```

Validated against SurrealDB v2.1.4 and v3.1.2.
