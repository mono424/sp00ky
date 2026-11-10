# spooky-core (Rust) guidance

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
