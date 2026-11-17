# sdb-parity

Every query sp00ky sends to SurrealDB, timed on SurrealDB 3.3.5, 4.0.1 on a
fresh store and 4.0.1 on a 3.3.5 store it converted, at WhitePawn's scale, in
Docker with WhitePawn's production limits. Local only: it never talks to a
deployment. Results and the write-up: `docs/perf/sdb4-parity.md`.

## Run it

Docker running, a WhitePawn checkout at `~/dev/whitepawn-sdk21` (or
`SDB_PARITY_APP=<path to packages/schema>`), from the repository root:

```sh
cargo build --release -p sp00ky-cli -p sdb-parity

# 1. Build the datasets once (about 2 hours; resumable, a loaded table is skipped)
target/release/sdb-parity prepare --spky target/release/spky

# 2. Run the suite: before, then with the fixes, on each server; writes
#    docs/perf/results/<label>.json and docs/perf/sdb4-parity-results.md
target/release/sdb-parity run --targets 335,401f,401c \
  --apply-schema target/release/spky \
  --app-patch docs/perf/whitepawn-events-4x.surql \
  --migrate-ledger --label full

# Re-render the report from saved results
target/release/sdb-parity report --labels full

# Remove the containers (the dataset volumes stay)
target/release/sdb-parity down
```

`--only user,write.game` runs the workloads whose id contains one of the
given strings (add `mixed` to keep the mixed load, `sched.ledger_migration`
to keep the migration), `--skip fn.feature.materialize` leaves workloads
out; `--phases after` skips the before phase; `--reps`,
`--mixed-secs` and `--no-reset` trade accuracy for time. Servers can run side
by side (`--targets 335` and `--targets 401f` in two shells, each with its
own `--label`). `report --labels a,b,c` merges results: a later label
replaces an earlier one workload by workload and phase by phase, so a partial
re-run (`--only mixed`) only replaces what it measured.

On 3.3.5 use `--rekey-ledger` instead of `--migrate-ledger`: the migration
deletes 3M legacy rows at about 11 s per 1,000 (RocksDB deletes grow with the
table), 9-10 hours here. `--rekey-ledger` drops `_00_version` and reloads it
keyed from the generator, the state the migration ends in, in about 5 minutes.

The 4.0.1 conversion (`prepare --targets 401c`) runs the server with the
production block cache: with the 128 MiB cache the cloud upgrade's scratch
copy uses, 4.0.1's startup doc-ID migration re-read SSTable blocks from the
page cache at about 700 MB/s and had not finished after 95 minutes. A
conversion interrupted while that migration runs re-attaches to the running
container.

## What it builds

`prepare` creates one Docker volume per server, `sdb4-parity-335-data`,
`sdb4-parity-401f-data` and `sdb4-parity-401c-data`:

- **Schema:** `spky migrate apply` of the app (WhitePawn's 225 migrations and
  sp00ky's internal schema: meta tables, generated events, functions,
  permissions, indexes, schedules), exactly as a deploy applies it. The JWT
  key of the `account` access is a bench-only Ed25519 pair. Pass an older
  `--spky` to build a dataset with that release's internal schema (the runs
  in `docs/perf` used one built from 1.0.0-beta.4's functions and events).
- **Rows:** 17.3M, generated deterministically (seeded; the same rows on every
  server and every run): analysis 11.45M, `_00_version` 3M (legacy random-id
  rows, WhitePawn's state on 2026-11-16; `--legacy-ledger 0` keys them),
  game 750k, and every other table filled from its field types with
  WhitePawn-shaped distributions (a Zipf over users for library sizes, games in
  their owner's collections, about 40 analysed positions per analysed game,
  outbox job queues, schedule and workflow runs, feature flags). `--scale`
  and `--analysis-rows` shrink it.
- **SSP state:** 1,250 registered views (`_00_query`) and 458k edges in 30
  per-user `_00_list_ref_user_<uid>` tables plus `_00_list_ref_anon`, created
  with the SSP's own DDL; windows over popular offsets, so the top rows of a
  collection are in many views.
- **401c** is the 3.3.5 volume opened by 4.0.1 with
  `SURREAL_EXPERIMENTAL_UPGRADE_FROM_3X=true` (compatibility level 3,
  `SURREAL_STARTUP_OPERATION_TIMEOUT=6h`), then every index removed and
  defined again `CONCURRENTLY`, the replay upgrade's rebuild step.

Rows are written with `OPTION IMPORT` (no events, no changefeed entries).

## What it runs

Each run copies a dataset volume to `sdb4-parity-<server>-run` and starts the
server with 3 vCPUs, 6 GiB, SurrealKV block cache a quarter of that, and the
slow query log at 200 ms. Timestamps of the operational tables are moved to
the current time first. Workloads (ids in the report):

- `user.*`: a signed-in record user's own writes (sp00ky's `remoteBatch`) on
  games they own: the first write on a game with a legacy ledger row, the
  next write, a delete, a create.
- `pages.*`: scheduler clone pages and SSP bootstrap pages with every
  `_00_rv` form (`ssp_protocol::with_durable_row_versions_using`), the beta.3
  forms, the ledger head probe, drift refetches; after the migration, the
  keyed pages. `sched.ledger_migration.step_1000` times the scheduler's
  migration (`ssp_protocol::version_ledger::migrate`).
- `ssp.*`: view registration (cold, warm, stranded probe), the per-user table
  DDL, edge publication built by `ssp_node::edges::build_edge_batch` and
  `plan_transactions` (a bump fanned out to every view holding a record, a
  2,000-operation round, cold publishes of 50 and 3,861 rows), edge drops,
  source probes, metrics flushes, the TTL sweep, the schema probes.
- `scheduler.*`: `ScheduleEngine::tick_pass` (first and steady), every prune
  (the forms before and after this branch), the schedule sweep selects, the
  jobs sampler (`schedule_core::admin::snapshot`), job enqueue, recovery,
  flag sweep, presence, heartbeat, drift counts, and three queries from
  WhitePawn's backends that its slow log showed.
- `jobs.*`: the SSP job runner (`ssp_node::jobs`): assign, claim, complete.
- `functions.*`: `fn::feature::*`, `fn::query::heartbeat`, reads as a record
  user (a game window, counts, notifications, the user's views, list_ref
  membership with `FETCH out`), and a LIVE SELECT on the user's list_ref
  table with `FETCH out` timed from edge write to notification.
- `writes.*`: writes through the generated and app events as backends and
  clients send them (the insights write from the 2026-11-16 slow log, the
  client batch, creates, deletes).
- **Mixed load:** three minutes of everything at once at production cadences
  (schedule tick every 5 s, samplers, flag sweep, job recovery, drift counts,
  changefeed polling, TTL sweep, edge publications, metrics flushes, client
  and backend writes, five record users holding LIVE queries and each
  editing one of their games every 10 s), with the
  server's CPU sampled by `docker stats`, then how long the server stays
  busy after the load stops (statements whose clients gave up keep running).

Queries sp00ky exposes as functions or constants are called directly
(`ssp_node`, `schedule_core`, `ssp_protocol`), so the suite measures what the
code sends today; the scheduler's and SSP's private queries are copied with
their source location, and `cargo test -p sdb-parity` checks the copies
against the files.

## Cleanup

`sdb-parity down` removes the containers. The `*-data` volumes are kept so a
run needs no regeneration; `docker volume rm sdb4-parity-335-run
sdb4-parity-401f-run sdb4-parity-401c-run` frees the run copies.
