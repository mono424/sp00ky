---
name: spooky-core-rust
description: >-
  Native Rust client for the Sp00ky local-first SurrealDB framework (crate
  spooky-core, lib spooky_core). Use when a Rust app (desktop, GPUI, CLI,
  service) opens a Sp00kyClient, generates types from the schema in build.rs,
  runs live queries, writes optimistically, signs users in, shows sync state,
  reads bucket files, checks feature flags, enqueues backend jobs, or uses
  peer to peer channels or native push.
metadata:
  author: sp00ky-sync
  version: "0.0.1"
---

# Sp00ky Core (Rust)

`spooky-core` is the native Rust client: the same engine as `@spooky-sync/core`
and the Dart `spooky_core`, so Rust, Flutter and web clients sync the same rows.
`Sp00kyClient` is `Send + Sync + Clone` and runs its engine on its own thread
with its own tokio runtime, so it works from any executor.

## Install and codegen

```toml
[dependencies]
spooky-core = { git = "https://github.com/mono424/sp00ky", package = "spooky-core" }

[build-dependencies]
spooky-core = { git = "https://github.com/mono424/sp00ky", package = "spooky-core" }
```

```rust
// build.rs
fn main() {
    let surql = std::fs::read_to_string("schema/src/schema.surql").unwrap();
    let code = spooky_core::codegen::generate_rust(&surql, &[]).unwrap();
    let out = std::path::Path::new(&std::env::var("OUT_DIR").unwrap()).join("db.rs");
    std::fs::write(out, code).unwrap();
    println!("cargo:rerun-if-changed=schema/src/schema.surql");
}

// src/db.rs
include!(concat!(env!("OUT_DIR"), "/db.rs"));
```

Generated: `schema()`, `SURQL_SCHEMA`, `config(DatabaseConfig)`, a serde struct
and `XPatch` per table, field tokens (`Note::rank.gte(3)`), `AuthApi`,
`Backends` (pass `parse_open_api(name, &spec, Some(outbox_table))` results as the
second argument), `AppDb`.

## Open

```rust
use spooky_core::{DatabaseConfig, Sp00kyClient, StoreType};

let client = Sp00kyClient::open(db::config(
    DatabaseConfig::new("main", "app")
        .endpoint("wss://app.example.com/rpc") // none = local-only; spky dev = ws://127.0.0.1:8666/rpc
        .store(StoreType::File(data_dir.join("spooky.db"))),
))
.await?;
```

`open` resolves after the local half of boot, so the next query paints from
cache. Tune `config.blob_cache`, `config.reconnect`, `config.persistence`, etc.
on the `Sp00kyConfig` before opening.

## Auth

```rust
use serde_json::json;

client.sign_in("account", json!({"username": "ada", "password": "..."})).await?;
let me = client.user_id().unwrap(); // "user:..."
let auth = client.auth_state();     // user row, token, access, flags
// A refused or expired token signs out: user_watch() turns to None.
// Sign in again (a desktop app: with its device key, see the Rust guide).
let mut user = client.user_watch(); // every change
client.sign_out().await?;           // drains queued writes first
```

## Live queries

```rust
let mut notes = client
    .live(client.query("note").where_eq("owner", me.as_str()).order_by("rank", "desc").limit(50))
    .await?;

let rows = notes.rows();                    // Arc<Vec<Row>>, Row = BTreeMap<String, Value>
let typed: Vec<db::Note> = notes.rows_as()?;
while let Ok(rows) = notes.changed().await { /* re-render */ }
```

- Keep the `LiveQuery` alive; dropping it unsubscribes.
- `status()` (idle / fetching) and `is_authoritative()` (server membership
  known; only then is an empty list really empty) drive loading states.
- Builder: `where_eq`, `where_op(field, QueryOp::new(">=", v))`, `where_or`,
  `order_by`, `limit`, `offset`, `one`, `select`, `related`, `related_with`
  (`spooky_core::modules::query_builder`). String values for record and
  datetime columns are re-typed from the schema.
- Raw: `client.live_raw(sql, params, QueryOptions::default())`. Raw params are
  NOT re-typed: pass `Value::from(RecordId::parse("user:ada"))` for records.
- `preload_query(q)` prewarms; `query_remote(sql, vars)` is a one-shot read,
  not cached and not live (do not build UI lists on it). `query_interactive`
  is the same for a read a screen waits on: its own pool, never queued behind
  the sync.
- `client.query(..)` has no query host: never call `.run()` / `.subscribe()` on
  it, use `client.live(..)`.

Typed: `client.live_typed(AppDb::new(client.clone()).note().query().where_([Note::rank.gte(3)]).order_by_desc(Note::rank))`.

## Writes

```rust
let id = format!("note:{}", spooky_core::value::generate_id());
client.create(&id, json!({"owner": me, "title": "Hello", "rank": 1})).await?;
client.update(&id, json!({"title": "Hello again"})).await?;
client.update_with(&id, json!({"title": draft}), UpdateOptions::debounced()).await?; // typing
client.delete(&id).await?;
```

Optimistic: visible at once, synced through a persistent outbox. Ids are full
`table:key`. Data is anything `IntoObject` (`json!`, `serde_json::Map`,
`Object`); `spooky_core::to_object(&my_struct)` for `Serialize` types.
`AppDb` writes (`app.note().create(&note)`, `.update(id, &NotePatch { .. })`)
return non-`Send` futures: await them in place, not in `tokio::spawn`.

Rejected writes: `failed_mutations_watch()`, `list_failed_mutations()`,
`retry_failed_mutation(id)`, `discard_failed_mutation(id)`.

## Sync state

Value now plus a `tokio::sync::watch` receiver (works on any executor):
`sync_health()` / `sync_health_watch()`, `pending_mutation_count()` /
`pending_mutations_watch()`, `fetching_query_count()` / `fetch_activity_watch()`,
`failed_mutation_count()` / `failed_mutations_watch()`, `unsynced_record_ids()` /
`unsynced_records_watch()`, `auth_state()` / `auth_watch()`. `events()` for
every engine event.

## Buckets, flags, releases, jobs

```rust
let avatars = client.bucket("avatars");
avatars.put("ada.png", png_bytes).await?;
let bytes: Option<Vec<u8>> = avatars.read("ada.png").await?; // blob cache

let flag = client.feature("new-editor", Some("off")).await?;
if flag.enabled() { /* ... */ }
let release = client.app_release("desktop").await?;
let outdated = release.update_available(env!("CARGO_PKG_VERSION"));

client.run("api", "/spookify", json!({"id": "thread:1"}), RunOptions::default()).await?;
```

Buckets, flags and releases need a remote endpoint (`Error::NoRemote`). `run`
needs the backend in the generated schema.

## Channels and push (experimental)

- `client.channel(&[client.query("call").where_eq("room", "lobby")], ["user:bob"]).await?`
  on `-- @channel` tables; only the `where` counts; both sides must list each
  other. Channel writes take any `IntoObject`, like client writes. The app injects a WebRTC
  stack via `config.channels = Some(ChannelsConfig { rtc: Some(factory), .. })`,
  else the status is `Unsupported`.
- `let push = client.push();` at startup, then
  `push.register(PushToken::new(PushKind::Apns, token, bundle_id), PushRegisterOptions::default())`
  and `push.sync(Some(token), PushSyncOptions::default())` on every launch
  (`spooky_core::modules::push`). The app gets the token from the OS.

## Lifecycle

`wake()` on resume or network back, `set_offline(bool)`, `checkpoint().await`
when backgrounded, `detach().await` when leaving for good, and always
`close().await` before the process exits (dropping the last handle closes in
the background and may lose the circuit snapshot).

## GPUI

```rust
let live = client.live(client.query("note").where_eq("owner", me));
cx.spawn(async move |this, cx| {
    let Ok(mut live) = live.await else { return };
    loop {
        let notes = live.rows_as::<db::Note>().unwrap_or_default();
        if this.update(cx, |view, cx| { view.notes = notes; cx.notify(); }).is_err()
            || live.changed().await.is_err()
        {
            break;
        }
    }
})
.detach();
```

## In-process Engine

`spooky_core::Engine::open(config).await? -> Rc<Engine>`: the same engine on a
tokio `LocalSet` you own, `!Send` futures and callback subscriptions
(`engine.subscribe(&hash, |rows| .., true)`). Writes take `Object`. `AppDb`
drives `Sp00kyClient` only.

## Testing your app

A local-only client (`StoreType::Memory`, `config.persistence =
PersistenceConfig::Memory`, no endpoint) runs the whole engine with no server:
writes land and live queries see them.
