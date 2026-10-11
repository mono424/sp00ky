# @spooky-sync/benchmark

Runnable performance benchmarks for the sp00ky scheduler and SSP. The
`whitepawn` suite exercises small live query windows over large chess-game
libraries, inspired by WhitePawn Web. It starts a disposable local SurrealDB
container and fresh scheduler/SSP processes for each dataset. It uses synthetic
data and does not connect to WhitePawn infrastructure.

## Build and run

Run from the repository root with Docker running:

```sh
pnpm install
cargo build --release -p sp00ky-cli -p scheduler -p ssp-server
docker pull surrealdb/surrealdb:v3.1.5
pnpm -F @spooky-sync/benchmark build
pnpm -F @spooky-sync/benchmark test

# Verify the entire path with two 10K-game datasets and short load phases.
node apps/benchmark/dist/index.js whitepawn --smoke

# 100K compact, 1M compact, and 1M PGN-heavy games.
node apps/benchmark/dist/index.js whitepawn --profile standard

# The default profile also attempts 10M compact games if capacity permits.
node apps/benchmark/dist/index.js whitepawn --profile large
```

Pull the selected database image beforehand to keep image-download time out of
startup measurements.

The repository CLI is named `spooky-bench`. It is separate from `spky` and is
not a `spky` subcommand. WhitePawn's server schema is generated from the
benchmark fixture with the local `spky` binary; the suite then applies it to
its disposable database.

## WhitePawn datasets

| Profile                     | Cases, in execution order                           |
| --------------------------- | --------------------------------------------------- |
| `--smoke`                   | 10K compact, 10K PGN-heavy                          |
| `--profile standard`        | 100K compact, 1M compact, 1M PGN-heavy              |
| `--profile large` (default) | 100K compact, 1M compact, 1M PGN-heavy, 10M compact |

Row counts refer to `game` rows. Each case also contains twenty collections and
256 player identities. Eighty percent of games belong to one main collection;
the remainder are distributed over nineteen smaller collections. Every four
games share a sort timestamp, so `ORDER BY sort_index ASC, id ASC` must resolve
ties consistently. Data, ids, collection membership and payloads are
deterministic.

Compact games contain metadata and typed links to their collection and two
players. PGN-heavy games add varied text averaging about 1.2 KiB per game. Both
cases use the same projected list query, which excludes PGN text and folds in
the two players' `id` and `name`. The heavy case therefore measures the effect
of larger source records without increasing the list's requested payload.
Collection filters use bound parameters, preserving typed record comparisons
in the source database and the registered query.

The fixture grants full table permissions to isolate scheduler and SSP work.
This suite does not measure production authorization or browser rendering.
Seeding uses batches of at most 500 records and never constructs the entire
game table in memory. Reports distinguish measured serialized JSON/SQL bytes
from database and replica storage on disk.

## WhitePawn workload and measurements

Each case measures bootstrap, registration and publication over real SurrealDB
writes with `sync.transport: changefeed` and a one-day changefeed retention.

- **Window registration:** fixed 50-row windows, shallow and deep offsets,
  filtered queries and related player names. Registration response latency is
  recorded separately from the time until the published view is ready.
- **Scrolling:** 100 deterministic scroll steps visit shallow, middle and deep
  windows, retain up to twenty registrations, evict old windows and revisit
  them. The smoke run keeps the same scroll trace length.
- **Steady writes:** 50 mutations per second for 60 seconds.
- **Import burst:** 1,000 new games per second for 30 seconds, in batches of
  100 with at most four writes in flight.
- **Isolated propagation probes:** twenty probes per category, run after the
  preceding work has settled, including root-row and related-player changes.
  Smoke uses two probes per category and two-second load phases.

Publication readiness checks materialized root and child references and their
record versions against a SurrealDB snapshot. An HTTP response or a nonempty
view alone does not establish readiness. Correctness checks cover window
membership at tied-key boundaries and related record versions as writes change
the dataset.

Raw output retains phase labels, latency samples, write results, readiness
observations and resource samples. Scheduler/SSP process RSS and CPU are sampled
every 250 ms, scheduler metrics every second, and SSP information every five
seconds. CPU is the operating system's `ps` percentage, where 100% represents
one core and a process can exceed 100%. Process memory summaries report sampled
peaks across each complete case. Separate SSP debug checkpoints between phases
attribute memory to the shared store and query/operator state.
Graphs count operator computations. The raw `subscribers` field counts extra
registrations merged into an existing graph, excluding its owner. Sharing an
identical query should add one merged follower without adding a graph.

The suite verifies sharing during initial registration. Cluster recovery
currently restores persisted registrations independently, so shared
registrations can become separate graphs. Post-import graph/follower counts and
subsequent memory measurements reflect that behavior.

Scroll eviction unregisters the window, waits for committed reference removal,
then deletes its benchmark-owned durable registration metadata so it cannot be
restored. Bounded checks between phases compare the exact retained registration
ids and the expected total of graphs plus merged followers. After final window
verification, each case stops its entire isolated stack; final per-view
unregistration is outside the workload.

Each run writes `run.json` for configuration, environment and case status,
`samples.json` for raw measurements and summaries, and `summary.md` for readable
case outcomes and latency percentiles. These reports are refreshed as phases
finish, so a failed or interrupted run retains the completed measurements.

## Capacity and completion

The suite checks host/Docker memory limits and free disk before each case. It
keeps 5 GiB of disk headroom and limits projected memory use to 80% of the
host memory budget for native processes plus the database, and 80% of Docker's
budget for the database alone. Projections include all populated games: the
initial fixture plus 2,100 load-phase inserts in smoke or 33,000 in full runs.
Initial checks use estimates; completed cases provide measured storage and
memory inputs, scaled by their populated game counts, with a 1.25× safety factor
for later cases.

When a PGN-heavy case has no completed smaller PGN-heavy observation, its
projection uses measured compact-case costs plus a per-game PGN payload
allowance as a floor. For disk, native-process memory and database memory
separately, the suite keeps the larger of that projection and the initial
PGN-heavy estimate. A larger payload therefore cannot appear cheaper merely
because it lacks a matching measured case.

A case whose projected use exceeds a capacity limit is recorded as
`resource-limited`, and the command exits nonzero. This preflight result is a
conservative estimate, not proof of an out-of-memory failure. A failure, timeout
or skipped large dataset is not reported as a successful completed benchmark.
`--startup-timeout-secs` bounds database and backend startup, publication
readiness, isolated propagation probes and load catch-up checks. Each wait has a
3,600-second default deadline; this is not a deadline for the entire run.

## WhitePawn flags

| Flag                         | Purpose                                                                                                         |
| ---------------------------- | --------------------------------------------------------------------------------------------------------------- |
| `--smoke`                    | Two 10K cases, short load phases and two isolated probes per category.                                          |
| `--profile standard\|large`  | Choose the dataset matrix; default `large`.                                                                     |
| `--spky-bin <path>`          | Use a prebuilt schema-generation CLI.                                                                           |
| `--scheduler-bin <path>`     | Use a prebuilt scheduler.                                                                                       |
| `--ssp-bin <path>`           | Use a prebuilt SSP server.                                                                                      |
| `--surreal-image <ref>`      | Override `surrealdb/surrealdb:v3.1.5`.                                                                          |
| `--schema <path>`            | Override the source schema; it must remain compatible with the chess fixture tables and fields.                 |
| `--startup-timeout-secs <n>` | Startup and publication-readiness deadline, including propagation and catch-up checks; default `3600` per wait. |
| `--out <dir>`                | Set the report directory; default is a timestamped directory under `results/`.                                  |
| `--verbose`                  | Stream child-process logs.                                                                                      |

Run `node apps/benchmark/dist/index.js whitepawn --help` for the current flags.
Supply absolute paths for custom binaries when invoking the command from a
different working directory.

## Other suites

These existing suites use the smaller `user`/`thread`/`comment` fixture and
direct HTTP ingestion. Generate their test fixture first:

```sh
pnpm -F @spooky-sync/tests run generate
node apps/benchmark/dist/index.js velocity --smoke
node apps/benchmark/dist/index.js mutation-mix --smoke
```

| Command           | Measurement                                                |
| ----------------- | ---------------------------------------------------------- |
| `velocity`        | Ingestion-rate ramp and latency threshold.                 |
| `mutation-mix`    | Insert, update and delete latency.                         |
| `subquery-depth`  | Propagation cost as subquery nesting grows.                |
| `join-fanout`     | Write amplification as joined fan-out grows.               |
| `join-saturation` | Small active table joined against a larger inactive table. |
| `view-density`    | Memory and latency as registered-view count grows.         |
| `backlog`         | SSP outage and recovery/catch-up.                          |
| `index`           | Roll up existing suite summaries.                          |

Use `<command> --help` for suite-specific options. Existing suite reports contain
`samples.json`, `summary.md` and `run.json`; `--out` selects their destination.
Each workload runs one scheduler and one SSP. Multi-SSP placement is outside
these benchmarks.
