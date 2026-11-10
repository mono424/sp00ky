# spooky-core (Rust)

The native Rust client core for [sp00ky](https://sp00ky.cloud) local-first sync:
the same engine as the TypeScript (`@spooky-sync/core`) and Dart (`spooky_core`)
clients, for desktop apps, CLIs and services written in Rust.

- **Local-first.** Rows live in a sqlite store (one file per signed-in
  account). Queries paint from it before the network answers; writes apply
  optimistically and sync through an outbox that survives restarts.
- **Live.** A query is a materialized view: the in-process circuit (the `ssp`
  crate, no FFI) keeps it current as local writes land, and the server's
  membership (`_00_list_ref`, LIVE plus a poll) keeps it in step with every
  other client.
- **Any executor.** The engine runs on its own thread with its own tokio
  runtime. `Sp00kyClient` is `Send + Sync + Clone`; its futures and watch
  channels work from GPUI, smol, tokio or a plain `block_on`.

## Install

```toml
[dependencies]
spooky-core = { git = "https://github.com/mono424/sp00ky", package = "spooky-core" }

[build-dependencies]
spooky-core = { git = "https://github.com/mono424/sp00ky", package = "spooky-core" }
```

Features: `remote` (default, the SurrealDB WebSocket transport; without it the
client is local-only) and `codegen` (default, schema to Rust for build scripts).

## Generate types from the schema

```rust
// build.rs
fn main() {
    let surql = std::fs::read_to_string("schema/src/schema.surql").unwrap();
    let code = spooky_core::codegen::generate_rust(&surql, &[]).unwrap();
    let out = std::path::Path::new(&std::env::var("OUT_DIR").unwrap()).join("db.rs");
    std::fs::write(out, code).unwrap();
    println!("cargo:rerun-if-changed=schema/src/schema.surql");
}
```

```rust
// src/db.rs
include!(concat!(env!("OUT_DIR"), "/db.rs"));
```

The generated file has `schema()`, `SURQL_SCHEMA`, `config(DatabaseConfig)`,
a serde struct and a `Patch` per table, typed field tokens (`Note::rank`),
`AuthApi`, `Backends` and an `AppDb` facade.

## Use it

```rust
use spooky_core::{DatabaseConfig, Sp00kyClient, StoreType};
use serde_json::json;

let client = Sp00kyClient::open(db::config(
    DatabaseConfig::new("main", "app")
        .endpoint("wss://app.example.com/rpc")
        .store(StoreType::File(data_dir.join("spooky.db"))),
))
.await?;

client.sign_in("account", json!({"username": "ada", "password": "..."})).await?;
let me = client.user_id().unwrap();

// A live query: the latest rows, always.
let mut notes = client
    .live(client.query("note").where_eq("owner", me.as_str()).order_by("rank", "desc"))
    .await?;
let rows: Vec<db::Note> = notes.rows_as()?;

// Optimistic writes: visible at once, pushed in the background.
client.create("note:k1", json!({"owner": me, "title": "Hello", "rank": 1})).await?;
client.update("note:k1", json!({"title": "Hello again"})).await?;
client.delete("note:k1").await?;

// Wait for the next change (another device, the web app, a local write).
while let Ok(rows) = notes.changed().await {
    println!("{} notes", rows.len());
}
```

Writes take anything `IntoObject`: a `json!({...})` object, a
`serde_json::Map`, or an `Object`; `spooky_core::to_object(&my_struct)` turns
your own `Serialize` types into one. Writes and the query builder re-type a
`record<..>` field given as its `table:key` string from the schema; the
parameters of a raw SurrealQL query (`live_raw`, `query_raw`) are sent as
given, so pass a `RecordId` there.

A builder from `client.query(..)` is build-only: run it with `client.live`,
`live_typed` or `preload_query` (its own `run`/`subscribe` need the
in-process engine). The generated `AppDb` over `Sp00kyClient` runs on the
caller's executor and its futures are not `Send`; call the plain client
methods from `tokio::spawn` or other `Send`-only contexts. A `record<..>` field may be given as its `table:key` string:
the schema re-types it before it reaches the server.

### In a GPUI app

Hold the client in your app state and pump a `LiveQuery` from a foreground
task:

```rust
let mut live = client.live(client.query("document").where_eq("course", course_id)).await?;
cx.spawn(async move |this, cx| {
    while let Ok(rows) = live.changed().await {
        this.update(cx, |view, cx| {
            view.documents = rows;
            cx.notify();
        })
        .ok();
    }
})
.detach();
```

`LiveQuery::status()` (fetching or idle) and `is_authoritative()` (server
membership known: an empty list really is empty) drive loading states.

### Everything else

| What | API |
| --- | --- |
| Raw SurrealQL query | `live_raw(sql, params, QueryOptions)`, `query_raw`, `preload` |
| One-shot server read, not synced | `query_remote(sql, vars)` |
| Debounced updates (typing) | `update_with(id, data, UpdateOptions::debounced())` |
| Rejected writes | `failed_mutations_watch()`, `list_failed_mutations`, `retry_failed_mutation`, `discard_failed_mutation` |
| Sync indicators | `sync_health_watch()`, `pending_mutations_watch()`, `unsynced_records_watch()`, `fetch_activity_watch()` |
| Auth | `sign_up`, `sign_in`, `sign_out`, `user_watch()`, `authenticate(token)` |
| Files | `bucket(name).put/read/get/delete/list` (reads cached per `BlobCacheConfig`) |
| Feature flags, releases | `feature(key, fallback)`, `app_release(app)` |
| Backend jobs | `run(backend, path, payload, RunOptions)` |
| App lifecycle | `wake()`, `set_offline(bool)`, `checkpoint()`, `detach()`, `close()` |

Call `close().await` before the process exits so the circuit snapshot is
written (dropping the last handle closes in the background).

## In-process engine

`spooky_core::Engine` is the engine itself, single-threaded on a tokio
`LocalSet`, with a similar API returning `!Send` futures and callback
subscriptions (writes take an `Object`; its builder from `engine.query(..)`
can `run` and `subscribe` itself). Use it when you already own a `LocalSet`
(tests, embedding).

## Layout

`state/` (one state value, pure reducers and selectors), `kernel/` (the `Ctx`
effect seam, adapter ports, events, lanes, constants), `query/`, `mutation/`,
`sync/`, `boot/` (the sagas), `runtime/` (the engine), `services/` and
`surreal/` (sqlite, circuit, CBOR WebSocket RPC), `modules/` (query builder,
auth, buckets, flags, releases), `client/` (`Engine`, `Sp00kyClient`),
`codegen/`, `typed`, `testing/` (fakes and `TestRig`). See `AGENTS.md`.

## Tests

```sh
cargo test -p spooky-core
# against a running dev stack (spky dev), see tests/live_stack.rs:
SPOOKY_IT_ENDPOINT=ws://127.0.0.1:8666/rpc SPOOKY_IT_NS=main SPOOKY_IT_DB=rustcore \
  cargo test -p spooky-core --test live_stack
```
