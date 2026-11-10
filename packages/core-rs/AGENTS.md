# `spooky-core` (Rust): agent guide

The native Rust client for sp00ky local-first sync (crate `spooky-core`, lib
`spooky_core`): the same engine as the TypeScript (`packages/core`) and Dart
(`packages/spooky_core`) cores, for desktop apps, CLIs and services. Rows live
in a sqlite store per signed-in account, queries are live materialized views
(in-process circuit, the `ssp` crate), writes are optimistic and drain through
a persistent outbox. User docs: `/docs/guide/rust` on the docs site
(`apps/landing-page/src/pages/docs/guide/rust.mdx`).

The first half of this file is for agents writing an app that USES the crate.
The second half ("Working on the crate") is for agents changing it.

## Setup

- Dependency (not on crates.io yet; pin a `rev`):
  `spooky-core = { git = "https://github.com/mono424/sp00ky", package = "spooky-core" }`
  in both `[dependencies]` and `[build-dependencies]`. Features (default on):
  `remote` (WebSocket transport; off = local-only), `codegen`. MSRV 1.85.
- Codegen in `build.rs`: `spooky_core::codegen::generate_rust(&surql, &backends)`
  writes `$OUT_DIR/db.rs`; `src/db.rs` is
  `include!(concat!(env!("OUT_DIR"), "/db.rs"));`. Typed backend jobs:
  `parse_open_api("<apps key>", &spec_yaml, Some("<method.table>"))` per backend.
- Generated items: `schema()`, `SURQL_SCHEMA`, `config(DatabaseConfig) -> Sp00kyConfig`,
  per table a serde struct (`Note`: `id: String` + fields; `record<..>` = `String`,
  `datetime` = `spooky_core::typed::Datetime`, `option<..>` = `Option`) and
  `NotePatch` (all `Option`), field tokens as associated consts (`Note::rank`:
  `NumField`, `StringField`, `BoolField`, `DateTimeField`, `RecordField`),
  `AuthApi` (`sign_in_<access>(..)`, `sign_up_<access>(..)`, `&str` params),
  `Backends` (`app.run().<backend>().<route>(..)`), `AppDb<C>`
  (`AppDb::new(client.clone()).note()`, `.auth()`, `.run()`).

## Public API (`Sp00kyClient`, the thread-safe handle)

`Send + Sync + Clone`; all clones share one engine running on its own thread
with its own tokio runtime, so its futures and `tokio::sync::watch` receivers
work on any executor (GPUI, smol, tokio, `futures::executor::block_on`).

- Open: `Sp00kyClient::open(db::config(DatabaseConfig::new(ns, db).endpoint("wss://host/rpc").store(StoreType::File(path)))).await?`.
  Resolves after the local half of boot (store, schema, circuit snapshot,
  cached session); network work runs in the background. No `endpoint` =
  local-only. `spky dev`: `ws://127.0.0.1:8666/rpc`. Config fields:
  `database.{store, token, max_concurrent_queries}`, `persistence`
  (`Session` | `Memory`), `blob_cache: BlobCacheConfig { directory, max_bytes, clear_on_sign_out }`,
  `stream_debounce_ms`, `ref_sync_interval_ms`, `enable_anonymous_live_queries`,
  `sync_health`, `live_inline_bodies`, `reconnect: ReconnectConfig`,
  `circuit_checkpoint_ms`, `channels: Option<ChannelsConfig>`.
- Queries: `client.query("note")` -> `QueryBuilder` (`where_eq`, `where_op(field, QueryOp::new(">=", 3))`,
  `where_or([(field, WhereValue::Eq(v))])`, `where_map`, `order_by(field, "asc"|"desc")`,
  `limit`, `offset`, `one`, `select`, `related`, `related_with`; types in
  `spooky_core::modules::query_builder`). Run it with `client.live(q).await?`
  (or `live_with_ttl(q, Some("1h"))`) -> `LiveQuery`, `preload_query(q)`.
  Raw: `live_raw(sql, params: Object, QueryOptions { ttl, .. })`,
  `query_raw` (hash only), `subscribe(hash)`, `preload(sql, params, opts)`.
  One-shot server read, not cached, not live: `query_remote(sql, vars)`;
  `query_interactive(sql, vars)` for one a screen waits on (own pool, never
  behind the sync's registrations and row fetches).
  Also `is_query_settled`, `is_query_authoritative`, `query_timings`,
  `deregister_query`.
- `LiveQuery` (keep it alive; dropping unsubscribes): `rows() -> Arc<Vec<Row>>`,
  `rows_as::<T>()`, `changed().await` (`Err(Error::Closed)` when the client is
  gone), `status()` (`QueryStatus::Idle|Fetching`), `is_authoritative()`,
  `rows_watch()`, `status_watch()`, `authority_watch()`, `into_stream()`, `hash()`.
- Typed: `client.live_typed(app.note().query().where_([Note::owner.eq(me), Note::rank.gte(3)]).order_by_desc(Note::rank).limit(20))`,
  then `rows_as::<Note>()`. `TypedQuery` also has `where_any`, `order_by`,
  `offset`, `related`, `related_with`. Writes: `app.note().create(&note)`,
  `.update(id, &NotePatch { .. })`, `.update_with(id, &patch, opts)`, `.delete(id)`.
- Writes (optimistic, return the row): `create(id, data)`, `update(id, data)`
  (merge), `update_with(id, data, UpdateOptions::debounced())`, `delete(id)`,
  `read_row(id)`. `data: impl IntoObject` = `serde_json::json!({..})`,
  `serde_json::Map`, or `Object`; `spooky_core::to_object(&my_struct)` for
  `Serialize` types. Ids are full `table:key`; `spooky_core::value::generate_id()`.
  Debounce: `UpdateOptions { debounced: Some(DebounceOptions { key: DebounceKey::RecordId | RecordIdXFields, delay_ms: Some(300) }) }`.
- Failed-write tray: `failed_mutation_count()`, `failed_mutations_watch()`,
  `list_failed_mutations() -> Vec<FailedMutationRow>` (`id`, `mutation_type`,
  `record_id`, `table_name`, `data`, `before_record`, `error.{message, kind}`,
  `attempts`, `revert`), `retry_failed_mutation(id)`, `discard_failed_mutation(id)`.
- Sync indicators (value now + watch): `sync_health()` / `sync_health_watch()`
  (`SyncHealth { status, consecutive_failures, kind, error, ever_connected, connection }`,
  `is_degraded()`), `pending_mutation_count()` / `pending_mutations_watch()`,
  `fetching_query_count()` / `fetch_activity_watch()`, `unsynced_record_ids()` /
  `unsynced_records_watch()`. `events()` = broadcast of every `OutEvent`.
- Auth: `sign_up(access, params)`, `sign_in(access, params)` (resolve after the
  account switch), `sign_out()` (drains queued writes first), `user_id()`,
  `user_watch()`, `auth_state()` / `auth_watch()` (`AuthState { user_id, current_user, token, access, is_loading, is_impersonating, verification_error }`),
  `token().await`, `check_auth(None)`, `authenticate(token)`, `deauthenticate()`.
  A refused token (revoked, expired token or session) signs the client out
  (`user_watch()` -> `None`, `Error::is_auth_rejection()`); sign in again.
  Device keys: a record access of their own (`sign_in("device", json!({"key": key}))`),
  re-signed in when the session is gone or `token()`'s `exp` is near.
- Buckets (remote only): `client.bucket("avatars")` -> `Bucket`: `put(path, Vec<u8> | &str)`,
  `read(path) -> Option<Vec<u8>>` (through the blob cache), `get(path)` (server,
  raw `Value`), `exists`, `list(Some(prefix))`, `delete`, `evict`.
  `blob_cache_stats()`.
- Feature flags and releases (remote only, live while held):
  `feature(key, Some("off")).await?` -> `FeatureFlag` (`variant()`, `enabled()`,
  `payload()`, `payload_as::<T>()`, `snapshot()`, `changed().await`, `watch()`);
  `app_release(app).await?` -> `AppRelease` (`update_available(current)`,
  `snapshot()` with `version`, `cache_bust`, `mandatory`, `released_at`, `changed()`).
- Backend jobs: `run(backend, path, payload, RunOptions { assigned_to, max_retries, retry_strategy, timeout, delay })`.
  The backend must be in the generated schema (pass its OpenAPI spec to codegen),
  else `Backend <name> not found`.
- Channels (experimental, `-- @channel` tables): `client.channel(&[query], ["user:bob"]).await?`
  -> `Channel` (`state()` / `state_watch()` / `changed()` with `ChannelState { status, peers }`,
  `set_peers`, `set_queries(..).await`, `create` / `update` / `update_with` /
  `delete` to this channel's peers only, `close()`, closes on drop). Only the
  `where` counts; both sides must list each other. The core has no WebRTC
  stack: set `config.channels = Some(ChannelsConfig { rtc: Some(factory), .. })`
  (`spooky_core::modules::channel`: `RtcPeerConnectionFactory`,
  `RtcPeerConnection`, `RtcDataChannel`, or a custom `transport`), else status
  is `Unsupported`. `ChannelsConfig::accept(table, |op| ..)` for per-table rules.
- Native push (experimental): `let push = client.push();` at startup (installs
  the sign-out unregister hook). `register(PushToken::new(PushKind::Apns, token, bundle_id).with_platform(..).with_environment(..), PushRegisterOptions::default())`,
  `sync(Some(token), PushSyncOptions::default()) -> PushSyncStatus` (never fails),
  `is_registered`, `info(refresh)`, `unsubscribe(all)`, `update`, `devices`,
  `notify`, `cancel`, `test(title, body)`, `unsubscribe_on_sign_out` /
  `set_unsubscribe_on_sign_out`. Errors: `PushError { code, message, cause }`.
  Types in `spooky_core::modules::push`. The app gets the token from the OS.
- Lifecycle: `wake()` (resume / network back; a dead socket is replaced
  within `ReconnectConfig::wake_probe_timeout_ms`, 3 s), `set_offline(bool)`,
  `checkpoint().await` (background), `detach().await` (leaving for good),
  `close().await` (before exit), `inspect_state()`, `is_local_ready()`, `kv_read`, `kv_write`.

## In-process `Engine`

`spooky_core::Engine::open(config).await? -> Rc<Engine>` is the engine without
the thread: single-threaded on a tokio `LocalSet`, `!Send` futures, callback
subscriptions (`engine.subscribe(&hash, |rows| .., true) -> Subscription`, held
until dropped). Its builder is bound: `engine.query("note").run("10m")`,
`.subscribe("10m", cb)`, `TypedQuery::new(engine.query("note")).watch(ttl, cb)`.
Writes take `Object`; `close()` returns `()`; `feature(key, fallback, ttl)`,
`app_release(app, ttl)`, `bucket(name)`, `push()`, `channel(..)` exist too.
The generated `AppDb` drives `Sp00kyClient` only.

## Common gotchas

- **Hold the `LiveQuery`.** Dropping it unsubscribes. In GPUI, move it into a
  `cx.spawn` task that loops on `changed().await` and `this.update(..)`.
- **The client's builder has no query host.** `client.query(..).run(..)` /
  `.subscribe(..)` and `TypedQuery::run` / `watch` on it fail with
  "QueryBuilder has no host (build-only)". Use `client.live(..)`,
  `client.live_typed(..)` or `client.preload_query(..)`.
- **Raw params are not re-typed.** The builder and writes turn `table:id`
  strings into records from the schema; `live_raw` params are sent as given, so
  pass `Value::from(RecordId::parse("user:ada"))` for a `record<..>` column.
- **Empty is not "empty" until authoritative.** Show a loading state while
  `rows()` is empty and `!is_authoritative()`.
- **`AppDb` futures are not `Send`** (`TypedClient` is `async_trait(?Send)`):
  await them in place, never in `tokio::spawn`. Plain client calls are `Send`.
- **Engine writes take an `Object`** (`spooky_core::object_from_json(json!(..))`);
  `Sp00kyClient` and `Channel` writes take any `IntoObject`, `json!` included.
- **Remote-only modules** (`feature`, `app_release`, `bucket`, `check_auth`)
  error with `Error::NoRemote` on a local-only client; `push()` calls answer
  `PushErrorCode::Server` with that error as `cause`.
- **Await `close()` before exit.** Dropping the last handle closes in the
  background, and process exit can kill the engine thread before the circuit
  snapshot is written.
- **Prefer live queries for UI.** `query_remote` is a one-shot read that is
  neither cached nor live.
- **Debounce typing** with `UpdateOptions::debounced()`, not a hand-rolled timer.
- Tests of your own app: a local-only client (`StoreType::Memory`,
  `PersistenceConfig::Memory`, no endpoint) runs the full engine with no server.

---

# Working on the crate

Native Rust port of the Dart core (`packages/spooky_core`), which itself ports
the TypeScript core (`packages/core`). Same saga architecture, same constants,
same SurrealQL, same tables. When behaviour is in doubt, the Dart source is the
spec; the TypeScript source breaks ties.

## Shape

- `state/`: one `ClientState` value. Reducers are plain `fn(&mut ClientState, ..)`
  (`reducers.rs`), reads are selectors (`selectors.rs`). No I/O.
- `kernel/`: `Ctx` (the effect seam), `ports.rs` (adapter traits: `LocalPort`,
  `RemotePort`, `SspPort`, `TimerPort`, `Services`), events, lanes, constants.
- `query/`, `mutation/`, `sync/`, `boot/`: sagas. A saga is
  `pub async fn name(ctx: &Ctx, env: &SagaEnv, ...) -> Result<T>`.
- `runtime/`: the single-threaded engine (`Rc`/`RefCell`, tokio `LocalSet`),
  `router.rs` maps every `RuntimeEvent` to its saga and lane.
- `services/`, `surreal/`: the real adapters. `modules/`: query builder, auth,
  buckets, feature flags, releases.
- `testing/`: fakes for every port plus `TestRig` (a real `Runtime` over fakes,
  with a shared `CallLog`).

## Porting rules

- Effects become `Ctx` calls one to one:
  `Fx.stateRead(f)` -> `ctx.read(|s| ..)?`, `Fx.stateUpdate(r.compose([..]))` ->
  `ctx.update(|s| { r::a(s, ..); r::b(s, ..); })?`, `Fx.stateWait(p)` ->
  `ctx.wait(p).await?`, `Fx.all([...])` -> `futures::future::join_all` over
  `ctx` calls (each item is a `Result`, allSettled semantics),
  `Fx.service(ServiceName.x, args)` -> `ctx.service(|s| async move { s.x(..).await }).await?`
  or `ctx.service_sync(|s| s.x(..))?`, `Fx.remoteWrite` -> `ctx.remote_write`.
- Every `Ctx` call is fenced and returns `Result`; propagate with `?`. A saga
  that must keep going after an error catches it explicitly, as the Dart one
  does with `try`/`catch`. Dart `finally` blocks become an inner async block
  whose result is held while the cleanup runs, then returned.
- Never read the whole state to copy it (`ctx.read(|s| s.clone())`): select
  what the saga needs.
- Rows are `Row = BTreeMap<String, Value>`. Inbound rows hold only JSON shapes
  (records as `table:id` strings, datetimes as ISO strings) plus bytes; typed
  `Value::RecordId`/`Value::Datetime` appear only on the way out, re-typed from
  the schema by `utils::parse_query_params`.
- Keep the Dart doc comments that explain *why* (incident notes included);
  drop ones that only restate the code. No em dashes in new text.
- Port the Dart tests for the module alongside it (`#[cfg(test)] mod tests` or
  `tests/*.rs`), using `crate::testing::TestRig` and `run_local`.
- Format only the files you touched (`rustfmt --edition 2021 <files>`), never a
  bare `cargo fmt` at the repo root.

## Checks

`cargo test -p spooky-core` and `cargo clippy -p spooky-core --all-targets`
must stay clean.
