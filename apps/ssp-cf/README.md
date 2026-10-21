# ssp-cf — Cloudflare Durable Object shell for the SSP node

The reference **edge** host: the same `ssp_node::SspNode` / `Runtime` as the VM
(`apps/ssp`) and the local reference host (`apps/ssp-portable`), driven by a
Durable Object. Proof that "run anywhere" is an adapter swap, not a fork.

## Shape

| Concern      | Port              | This host's adapter                     |
|--------------|-------------------|-----------------------------------------|
| Ingress      | (push)            | DO `fetch(req)` → `SspNode::route`       |
| DB           | `Db`              | `ssp_node::HttpSqlDb` over `CfHttp`      |
| HTTP out     | `HttpClient`      | `CfHttp` (Workers `fetch`)              |
| Timers       | `Scheduler`       | `CfScheduler` (DO `alarm()` + `TimerMux` persisted in SQLite `ssp_meta`)|
| Persistence  | `CircuitStore`    | `CfCircuitStore` (DO storage)            |
| Spawn        | `Spawner`         | `CfSpawner` (`spawn_local`)              |
| Telemetry    | `Telemetry`       | `NoopTelemetry`                          |

The **DB stays external** (SurrealDB Cloud): `HttpSqlDb` reaches it over HTTP-RPC
via `fetch`, so this host links no surrealdb SDK and needs no SurrealDB-in-wasm.
Cold start / eviction goes through `Runtime::bootstrap` (single flight; a failed
bootstrap retries with 1 s to 60 s backoff through `TimerKind::BootstrapRetry`).

Timers run on demand (`Runtime::on_demand`): a sweep is armed only while it has
work, the alarm is deleted when nothing is pending, and timers survive eviction
in the object's SQLite. An idle project sets no alarm and is evicted. Design and
measurements: [`docs/cloudflare-native.md`](../../docs/cloudflare-native.md) §5.

## Variables

| Name | Kind | Meaning |
|---|---|---|
| `SPKY_DB_URL`, `SPKY_DB_NS`, `SPKY_DB_DB` | var | the external SurrealDB |
| `SPKY_DB_AUTH` | secret | full `Authorization` header value for the DB |
| `SPKY_AUTH_SECRET` | secret | bearer the DB events and the platform use to call this node |
| `SPKY_SSP_ID`, `SPKY_JOB_CONFIG`, `SPKY_REF_MODE`, `SPKY_ANON_LIVE_QUERIES`, `SPKY_QUERY_ALLOWLIST` | var | as on the VM SSP |
| `SPKY_LOG` | var | `error`, `warn`, `info` (default), `debug`, `trace`; JSON lines to the Workers console |
| `SPKY_DO_LOCATION_HINT` | var | optional Durable Object location hint (for example `weur` for a Hetzner-hosted DB); applies when the object is first created |

## Status

Runs in production for free-plan projects (worker 0.8; the control plane
uploads the wasm bundle published by `.github/workflows/ssp-cf-publish.yml`).
It is intentionally **not** a member of the root cargo workspace (its own empty
`[workspace]` in `Cargo.toml` detaches it).

Change notices (`/ingest`, `/ingest/batch`, `/view/unregister`) never fail
because the object is cold, bootstrapping or busy publishing: they are kept in
the object's SQLite (`ssp_inbox`), answered `200 {"status":"queued"}`, and
replayed by their records' current DB state once the node is Ready
(`docs/cloudflare-native.md` §5.3).

## Build / deploy (needs toolchain)

```sh
cargo install worker-build wrangler
# edit wrangler.toml [vars]; set secrets:
wrangler secret put SPKY_DB_AUTH
wrangler secret put SPKY_AUTH_SECRET
wrangler deploy
```

## Open

The remaining phases (chunked snapshots, jobs, schedules, push, doorbell) are tracked in `docs/cloudflare-native.md` §12.
