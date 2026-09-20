# `@spooky-sync/cli` (`spky`) — agent guide

## What this package is

The sp00ky toolchain. A Rust binary (`spky`) plus a thin npm wrapper. It parses `.surql` schemas, emits typed `schema.gen.ts` (and `.dart`), runs migrations, manages buckets and API backends, drives the local dev environment, and orchestrates Sp00ky Cloud deployments.

## Binary

```
spky <subcommand> [flags]
```

Installed via `npx @spooky-sync/cli` or globally as `spky`. The `bin` field in `package.json` is `spky` — *not* `sp00ky`.

## Project layout it expects

```
your-app/
├── sp00ky.yml              # config: schema path, generated outputs, backends, buckets
├── schema/
│   └── schema.surql        # source of truth — your domain model
├── src/
│   └── schema.gen.ts       # GENERATED — never hand-edit
└── migrations/             # GENERATED migrations; modified files are tracked by checksum
```

`spky` finds `sp00ky.yml` in the current directory by default; pass `--config <path>` to override.

## Running it as an agent: always pass `-y`

You are almost certainly running on a **pseudo-terminal**. The CLI sees a terminal, renders its
prompt, and waits forever for a keypress you cannot send. Pass `-y` / `--yes` (any subcommand, any
position; `SPKY_YES=1` also works) and nothing blocks:

- **Consent prompts** ("Apply pending migrations to this PRODUCTION database?", "Restart SurrealDB?",
  "Overwrite?") are answered **yes**, and the answer is printed so your log shows what you agreed to.
- **Preference prompts** ("Initialize git?", "Create a backup before resetting?") take their
  **default**, never a blanket yes.
- **Menus** in `spky dev` (schema drift, pending migrations) take their unattended path: continue
  with drift, auto-apply pending migrations.
- **Guided flows** (login, project picker, billing) do not open. They stop and print the exact
  command or env var to use instead, e.g. `SP00KY_CLOUD_PROJECT` or `slug:` in `sp00ky.yml`.

Two operations are **not** covered by `-y`, on purpose, because they cannot be undone:
`spky project destroy` and `spky backup reset`. They need `--confirm <project-slug>`, spelled
correctly. Do not reach for these unless the user asked for exactly that, by name.

Without `-y` and without a terminal, a consent prompt **refuses** rather than proceeding, and the
error names the flag. If you see "No terminal to ask on", that is this.

## Subcommands an app developer/agent uses most

- **`spky generate` / `spky gen`** — read `sp00ky.yml`, parse all `.surql`, emit `schema.gen.ts` (and Dart equivalents per config). **Run this after every schema edit.**
- **`spky migrate create <name>`** — diff current schema against the last applied migration and emit a new `.surql` migration file.
- **`spky migrate apply`** — apply pending migrations against the configured database. `--fix-checksums` updates stored checksums for legitimately-modified migration files.
- **`spky migrate status`** — show pending vs applied vs modified-but-applied migrations.
- **`spky migrate fix [--fix-checksums]`** — repair schema drift / checksum mismatches.
- **`spky verify [--fix]`** — confirm SSP/scheduler snapshot matches upstream SurrealDB, and print the scheduler's own drift verdict (`/health/snapshot` `drift`). `--fix` re-clones the scheduler replica when its counts are off, otherwise forces every SSP to re-bootstrap. The scheduler runs the same count check itself at startup and after each snapshot drain and auto-reclones by default (`SPKY_DRIFT_AUTO_RECLONE`).
- **`spky lint`** — validate `sp00ky.yml` and referenced files exist.
- **`spky dev [--apply-migrations] [--clean] [--verbose]`** — boots a local SurrealDB + SSP + scheduler stack via Docker. `--clean` wipes SSP/scheduler state but preserves user data in SurrealDB. Startup renders as step lines via `src/ui.rs` (indicatif/console; plain lines when not a TTY); after "ready" only ERROR/crash-looking lines (infra and app streams) show unless `--verbose` / `SPKY_VERBOSE=1`. A bare `logLevel` is scoped to sp00ky crates by `backend::scoped_rust_log`.
- **`spky init`** — scaffold a new sp00ky project in the current directory. (`spky create` is a removed alias that only prints a migration hint.)
- **`spky bucket add`** / **`spky api add`** — append a bucket or backend definition to `sp00ky.yml`.
- **`spky mcp`** — start the bundled `@spooky-sync/devtools-mcp` server (so AI assistants can introspect the running app). Its `token` / `tokens` / `revoke` / `install` subcommands manage credentials for the separate Sp00ky Cloud MCP server. A deployed scheduler also serves its own MCP server for operator actions; see `docs/reference/mcp.mdx`.
- **`spky admin list|add <user>|remove <user>`** — manage the `_00_admin` roster. Roster members can sign in to the scheduler's operator dashboard and administer feature flags. `add` takes an optional `--note`.

## Deployment subcommands

These are top level, not under a `cloud` prefix. `spky cloud ...` was removed and now only
prints a migration hint.

`spky login | logout | deploy | release | status | stats | logs | restart | push | scale | backup | env | domain | link | team | billing | token | project | notice`.
See `spky <command> --help`. Most app code agents touch never need these.

Two that come up when a deployment misbehaves:

- **`spky restart [TARGET...] [--upgrade] [--clean] [--surreal] [--all-backends] [-y]`** —
  recreate containers. Targets are roles (`db`, `scheduler`, `ssp`, `frontend`) or an app
  name; with none it restarts the scheduler and SSPs. `--upgrade` pulls the latest scheduler
  and SSP images, `--clean` wipes the scheduler's volume (replica and WAL, not SurrealDB
  data). `-y` (the global flag, see above) confirms the SurrealDB prompt; without it and without
  a terminal the restart is refused.
- **`spky migrate prod -y`** — apply pending migrations plus the internal Sp00ky schema to the
  deployment, without a full deploy. `-y` confirms the production gate, which otherwise refuses
  when there is no terminal. Set `SPKY_DB_HTTP_TIMEOUT_SECS` if the internal schema step times out.

## Schema annotations the parser recognizes

In your `.surql` source, comment annotations attached to `DEFINE FIELD` / `DEFINE TABLE` change codegen output:

- `-- @crdt text` (above a `DEFINE FIELD`) — marks a field as a Loro CRDT text field. Consumers must use `useCrdtField` to read/write it; plain `useQuery` will see stale or unmerged content.
- `-- @parent` (suffix on `DEFINE FIELD ... TYPE record<...>`) — marks the column as the parent side of a relationship; written automatically from the auth context, never by client code.
- `-- @nosync` (above a `DEFINE TABLE`) — marks a table as server-only: it is omitted from generated types and relations (and any `record<...>` link pointing at it is dropped), no sync events are emitted for it, and the scheduler/SSP exclude it from snapshots and bootstrap. The table still lives in the main DB and is still backed up. The CLI bakes a `COMMENT 'sp00ky:nosync'` marker onto the server-side `DEFINE TABLE` so the runtime services detect it via `INFO FOR DB`. Distinct from `PERMISSIONS FOR select WHERE false`, which only locks reads — a permission-locked table is still synced.
- `-- @nosync` (above a `DEFINE FIELD`) — marks a single field server-only: omitted from generated types and from the client's local cache schema, omitted from sync event payloads, and omitted from the scheduler replica and SSP bootstrap row scans. **Not a read barrier**: a client's down-sync `SELECT` still returns the column over the wire, and it is only discarded on arrival (`cleanRecord`). For real secrecy use `PERMISSIONS FOR select WHERE false` on the field, or move it to a `@nosync` table.
- `-- @opaque` (above a `DEFINE FIELD`) — the field IS synced to the client (it stays in generated types and the local cache, flagged `opaque: true` on the column) but no server-side component stores the value. Intended for large blobs you render but never query on. Because nothing holds the value it cannot be evaluated: using it in `where`/`orderBy`/a join throws in the query builder and is rejected with a 400 at SSP registration, and a schema whose `PERMISSIONS` or `DEFINE INDEX` references one fails to build. Delivery works because sync payloads carry ids + versions, not field values — the client reads the row body straight from SurrealDB.

All three field-level exclusions (`@nosync`, `@crdt`, `@opaque`) get `COMMENT 'sp00ky:opaque'` baked onto the server `DEFINE FIELD` (`schema_builder::add_opaque_field_markers`). The scheduler replica and the SSP bootstrap read that marker from `INFO FOR TABLE` and turn it into a `SELECT * OMIT ...` projection. Both halves are required: skipping a field from the ingest payload while the bootstrap still loads it makes the SSP circuit and the scheduler replica disagree about the row's key set permanently (the replica applies updates with `MERGE`, the circuit replaces the whole row), which shows up as an unfixable `spky verify` mismatch.

Example:
```sql
DEFINE TABLE thread SCHEMAFULL ...;

-- @crdt text
DEFINE FIELD content ON TABLE thread TYPE string ASSERT $value != NONE;

DEFINE FIELD author ON TABLE thread TYPE record<user>; -- @parent

-- @opaque
DEFINE FIELD preview_png ON TABLE thread TYPE option<bytes>;

-- @nosync
DEFINE TABLE audit_log SCHEMALESS;
```

A descriptor must sit directly above its statement (no blank line between). One that attaches to nothing is warned about, not silently dropped (`annotations::warn_unattached_annotations`). A descriptor is the marker alone plus at most one value token (`-- @crdt text`); a longer comment that merely starts with a marker name is prose, not an annotation, and `-- @nosync true` is refused with a warning.

## Docker dev apps (`type: docker`)

Besides `backend`/`frontend` apps, `sp00ky.yml` can declare `type: docker` apps —
containers `spky dev` runs alongside SurrealDB/SSP/scheduler on the
`sp00ky-dev-net` network (each reachable from the others by its app **name**, via
a `--network-alias`). Use `scope: devOnly` for local-only sidecars: never
deployed, and they skip the backend spec/method/deploy validation.

Fields:

- `image` (required) — image to run, e.g. `bluenviron/mediamtx:latest` or `golang:1.22`.
- `ports` — published to the host: `[1935, "8189/udp", "3000:8080"]` (a bare value maps the same port host:container; `/udp` suffix preserved).
- `args` — appended after the image (the container command), e.g. `["go", "run", "."]`.
- `env` — same forms as other apps (inline map / dotenv path / vault). User values **override** the auto-injected `SPKY_*` vars. `${PROJECT_DIR}` (the absolute dir of `sp00ky.yml`) is expanded in values.
- `volumes` — bind/volume mounts (`-v`), e.g. `["/var/run/docker.sock:/var/run/docker.sock", "${PROJECT_DIR}/../..:/src", "gomod:/go"]`. `${PROJECT_DIR}` is expanded in the host portion (Docker normalizes `..`).
- `workdir` — working directory inside the container (`-w`).
- `dependsOn` — names of other docker apps that must be **ready** before this one starts; `spky dev` starts apps in dependency order. Validated at config load — an unknown name, a self-dependency, or a **cycle** is a hard error (`spky lint` reports it).
- `healthcheck` — an HTTP path (e.g. `/health`) polled on the app's first published host port until it returns 200. Lets a dependency signal real readiness so `dependsOn` waits for "up", not just "container started". Without it, a dependency counts as ready once its container is running.

Containers run as `sp00ky-dev-<name>` with `--rm` and are killed on Ctrl-C.
`dependsOn`/`healthcheck` are `spky dev` concerns; the cloud deploy path ignores
them (and `cloudOnly` docker apps are skipped by `spky dev`).

Example — a relay built from source via `go run`, and a publisher that waits for it:
```yaml
apps:
  relay:
    type: docker
    scope: devOnly
    image: golang:1.22
    workdir: /src/apps/relay
    args: ["go", "run", "."]
    ports: [3670]
    healthcheck: /health
    volumes:
      - "${PROJECT_DIR}/../..:/src"
      - "gomod:/go"
  publisher:
    type: docker
    scope: devOnly
    image: golang:1.22
    workdir: /src/apps/publisher
    args: ["go", "run", "."]
    dependsOn: [relay]          # started only after relay's /health returns 200
    volumes:
      - "${PROJECT_DIR}/../..:/src"
      - "gomod:/go"
```

## Sync transport (`sync.transport`)

`sync.transport: changefeed` changes what the schema pipeline emits, in three
places that must agree: `sp00ky::generate_sp00ky_events` keeps the
`_00_version` bookkeeping but drops the `http::post` (`post_ingest`),
`schema_builder::add_changefeed_clauses` puts `CHANGEFEED <retention> INCLUDE
ORIGINAL` on every synced `DEFINE TABLE` in the desired schema (after
`COMMENT`, before `PERMISSIONS`, the order SurrealDB renders, or `schema
diff` sees a phantom change forever), and `migrate::apply_internal_schema`
emits `ALTER TABLE IF EXISTS ... CHANGEFEED` for every synced user table after
the events (a user migration's `DEFINE TABLE OVERWRITE` without the clause
drops the feed; the ALTER puts it back on every deploy) plus `REMOVE EVENT
_00_dbsp_cleanup`. `backend::sync_settings_for(config_path)` is the single
reader of the setting. The scheduler/SSP get `SPKY_INGEST_TRANSPORT` through
`deployment.env` (cloud) or the dev launch specs. Retention must be in
SurrealDB's rendered form (`1d`, not `24h`): `valid_changefeed_retention`.

## Common gotchas

- **`schema.gen.ts` must be regenerated after every `.surql` change.** `spky generate`. CI typically asserts no drift.
- **Migrations are checksum-tracked.** Editing a previously-applied migration file won't silently re-run; `spky migrate status` flags it. Use `--fix-checksums` only when you're sure the change is semantically a no-op.
- **`sp00ky.yml` is the entry point.** The CLI never crawls for `.surql`; everything is wired explicitly through the config.
- **Generation modes matter.** The `--mode` flag (`singlenode`, `cluster`, `surrealism`) changes what the generated client connects to. Default is `singlenode` (HTTP to a single SSP). `surrealism` embeds the WASM stream processor in-browser.
- **Don't commit the bin output.** The Rust binary is built per-platform and shipped via the npm tarball under `dist/`.

## Pointers

- Sync engine the generated client targets: `node_modules/@spooky-sync/core/AGENTS.md`
- Reactive UI bindings: `node_modules/@spooky-sync/client-solid/AGENTS.md`
- Live MCP introspection during dev: `node_modules/@spooky-sync/devtools-mcp/AGENTS.md`
