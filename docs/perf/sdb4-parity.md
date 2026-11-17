# sp00ky on SurrealDB 3.3.5 and 4.0.1, side by side

What sp00ky asks of SurrealDB, timed on 3.3.5, on 4.0.1 with a fresh store
and on 4.0.1 with a 3.3.5 store it converted (what WhitePawn runs), at
WhitePawn's scale, before and after the fixes on branch `perf/sdb4-parity`.
Local Docker only, 2026-11-16/17. The suite is `apps/sdb-parity`
(one command, see its README); full tables:
[`sdb4-parity-results.md`](sdb4-parity-results.md).

## Summary

**The 19 s record-user write (the priority).** sp00ky's generated
`_00_game_mutation` event looks up a record's legacy ledger row with
`SELECT ... FROM _00_version WHERE record_id = $sp00ky_rid`. When a record
user's write fires the event, 4.0.1 plans that lookup from the user's session,
and with `_00_version`'s select rule (`WHERE record_id.id != NONE`, not
`FULL`) the plan is a scan of the 3M-row ledger: 24.4 s per record on a
store converted from 3.3.5 (WhitePawn's case), 3.9-4.4 s on a fresh 4.0.1
store (root: 18-34 ms; 3.3.5: 10-50 ms). Every write that reaches a record
without a keyed ledger row pays it, once per such record in a client batch
(fresh store: 1 record 3.9 s, 4 records 15.6 s, 6 records 23.4 s); a
committed write keys the row, so the same record is cheap afterwards. The fix is a correlated lookup that 4.0.1 plans as an index read for
root and record users alike (`LEDGER_LOOKUP_ROWS`): the 4-record batch takes
123 ms with only sp00ky's schema changed. The real cure is the beta.4 ledger
migration, after which no write reads the ledger by `record_id`. Section 1.

**The production CPU saturation.** Reproduced once record users write:
with five signed-in users each editing one of their games every 10 s (one
edit every 2 s in all) on top of the background load at production cadence,
4.0.1 before the fixes ran at CPU p95 339% of its 300% cap (max 376%, mean
132%), each edit taking 4.1 s p50 and 5.2 s p95: one ledger scan per edit is
about a full core. Without those edits the same load ran at mean 67%, p95
187%. The other ingredient is that a timed-out statement keeps running: two
`fn::feature::materialize` calls (released functions) hit the client's 180 s
timeout and kept two cores busy for more than an hour afterwards (still
running when the bench stopped waiting), and on another run an owner's edit
issued meanwhile did not return for 29 minutes.
Retries of the scans this report removes (record users' edits 4 s, an owner's
game delete 130 s, a backend's insights write 4.3 s, materialize) pile up the
same way. After the fixes the same load ran at mean 50%, p95 183%, and the edits took
33.5 ms p50 (91 got through in three minutes instead of 42). On the
converted store, WhitePawn's case, it was worse before (mean 214%, p95 337%,
edits 8.9 s p50 and only 20 of them done) and the same after (mean 38%, p95
189%, edits 47.5 ms). Section 5.

**Before and after, p50 ms** (WhitePawn's dataset on 3.3.5, on 4.0.1 with a
fresh store, and on 4.0.1 with the 3.3.5 store converted, which is
WhitePawn's case; full tables with p95 in
[`sdb4-parity-results.md`](sdb4-parity-results.md)):

| workload | 3.3.5 before | 3.3.5 after | 4.0.1 fresh before | 4.0.1 fresh after | 4.0.1 converted before | 4.0.1 converted after |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| owner edits a game, legacy ledger row (`user.game_merge.first_write`) | 12.6 | 3.9 | 4,171 | 20.9 | 24,417 | 22.7 |
| owner edits the same game again | 3.8 | 3.2 | 30.5 | 18.0 | 174 | 21.7 |
| owner's edits under the mixed load (`user.record_write`) | 14.9 | 8.0 | 4,087 | 33.5 | 8,867 | 47.5 |
| owner deletes a game (`user.game_delete`) | 433 | 69 | 136,442 | 44.6 | over 180,000 | 46.6 |
| backend insights write on a game (`write.game.insights_done`) | 546 | 22.5 | 4,306 | 86.7 | 7,626 | 111 |
| client game delete through the events (`write.game.client_delete`) | 44 | 66 | 122,119 | 46 | over 180,000 | 51 |
| `fn::feature::materialize`, 5,000 users | 647 | 672 | over 180,000 | 214 * | over 180,000 | 411 * |
| `fn::feature::allow` | 3.0 | 3.4 | 55 | 13.2 | 50 | 19.9 |
| workflow prune, nothing due, old form / new | 2.1 / 0.5 | 2.6 / 0.7 | 211 / 5.6 | 266 / 7.6 | 254 / 6.7 | 280 / 8.0 |
| SSP edge publication, a 2,000-operation round | 313 | 362 | 4,206 | 4,284 | 2,849 | 7,183 |
| SSP bootstrap page of 1,000 games, keyed `_00_rv` | | 1,985 | | 1,020 | | 1,784 |
| mixed load with record users' edits, CPU mean / p95 (100% = one core) | 85% / 147% | 100% / 304% ** | 132% / 339% | 50% / 183% | 214% / 337% | 38% / 189% |

("before": the dataset as deployed with sp00ky 1.0.0-beta.4 and WhitePawn's
schema; "after": this branch's internal schema, the WhitePawn patch and the
ledger migrated (3.3.5: rekeyed, section 6). "over 180,000": the client's
180 s timeout; the statement kept running. \* the second call, once the
first had written the assignments that moved (fresh: timed on the
after-phase copy with the last commit's `spky`, 16.1 s with the commit
before it, which still rewrote unchanged rows; converted: 22.0 s first,
411 ms second). \*\* 3.3.5's scheduler spent the last 80 s of that load
deleting the dataset's backlog of finished runs, 40 batches at 4.7 s, in
both phases; section 5.)

**What stays slower on 4.0.1 after the fixes.** Single-record writes that fire
events cost 3-10x more (3-4 ms on 3.3.5 against 15-35 ms: game creates and
edits, job enqueues, feature-row updates); SSP edge publication 7-12x
(`RELATE` and graph-walk updates: a 2,000-operation round 4.3 s against
0.36 s); reads over graph edges with `FETCH` 16x (`list_ref` membership,
563 ms against 34 ms); a record user's notification list 4x (316 ms against
78 ms); the probe of the migrated ledger's first key 256 ms (the migration's
3M deletes leave tombstones until compaction; 3.3.5 here was rekeyed, so it
has none). The converted store is slower than the fresh one again for some
of these (a publication round 7.2 s after the fixes, the notification list
629 ms). Every one is a plan or engine cost inside SurrealDB with no
sp00ky-side shape that changes it; the event and edge costs scale with the
number of events and edges a write touches. Section 7.

**Open risks.** Section 8: the 3.3.5 ledger migration takes hours on big
ledgers; the cloud upgrade's 128 MiB scratch cache makes 4.0.1's startup
migration of a large store crawl; every index build on a busy 4.0.1 database
waits up to 60 s for older writes and then refuses their commits; timed-out
statements keep running; WhitePawn must apply its patch for the delete and
backend-write fixes.

## Setup

- **Schema:** WhitePawn as deployed (whitepawn 1e3e3e22, 225 migrations,
  115 tables, 177 events, 31 functions, about 230 indexes) plus sp00ky's
  internal schema, applied by `spky migrate apply` exactly as a deploy does.
  The datasets were built with the 1.0.0-beta.4 internal schema (keyed
  ledger events, the functions before this branch).
- **Rows:** 17.3M, deterministic: analysis 11.45M, `_00_version` 3M (legacy
  random-id rows, WhitePawn's state during the incident), game 750k, and the
  other tables filled from their field types with WhitePawn-shaped
  distributions; 1,250 registered views and 458k list_ref edges in 30 per-user
  tables.
- **Servers:** 3 vCPUs, 6 GiB, SurrealKV block cache 1.5 GiB (a quarter),
  RocksDB defaults on 3.3.5; the converted store had every index rebuilt
  `CONCURRENTLY` as the replay upgrade does. Load speed (import mode): 3.3.5
  wrote 12k analysis rows/s, 4.0.1 4.3-5.3k/s.
- **Runs:** each server on a fresh copy of its dataset; 3.3.5 and 4.0.1 fresh
  side by side (each capped at 3 vCPUs on a 14-core laptop), so absolute
  numbers carry contention. The `before` phase is the dataset as built; the
  `after` phase, on the same copy, applies this branch's internal schema
  (`spky migrate apply`), the WhitePawn patch
  ([`whitepawn-events-4x.surql`](whitepawn-events-4x.surql)) and the ledger
  migration (3.3.5: rekeyed, section 6). The results merge several runs, by
  label in `docs/perf/results/` (`report --labels` in the order below; a
  later label replaces an earlier one per workload and phase):
  - 4.0.1 fresh: `full-401f` (everything), then `rerun-401f`,
    `clean-before-401f`, `mix-before-401f` and `final-401f`, re-measuring
    the mixed load, the record user's writes and the feature functions
    after bench fixes (a CPU sampler that misread `docker stats`, a
    changefeed poll with a bound `SINCE`, a root token that expired after an
    hour, record users' edits added to the mixed load, and two timed-out
    `materialize` calls that kept running into the next workloads).
  - 3.3.5: `log-335-before` (its before phase, recovered from the run's log
    when the run was stopped in the 9-hour migration), `after-335`,
    `mixed-335-before`, `final-335`, `mix-before-335`, `mix-after-335`.
  - 4.0.1 converted: `log-401c-before` (pages, SSP, scheduler, jobs; from
    the log of a run stopped while two zombie `materialize` calls held two
    cores), `log-401c-rest` (functions, permissions, writes and the user's
    first writes; from the log of a run cut off when the laptop's disk filled
    and the Docker VM stopped), `rest2-401c-before` (the rest of the before
    phase), `full-401c-after`. The converted base was checked after the crash
    (row counts equal to the plan, indexes in use) and every run copies it
    fresh.

## 1. A record user's write: 19 s on WhitePawn (the priority)

A WhitePawn client's game edit, sp00ky's batch
`LET $ok0 = { UPDATE $id0 MERGE $data0; true; }` as the signed-in owner,
took a steady 19 s on 4.0.1; the same edit as root 18 ms. Reproduced on the
4.0.1 dataset with WhitePawn's real permissions and events:

| one game edit | 3.3.5 | 4.0.1 fresh | 4.0.1 converted (WhitePawn's case) |
| --- | ---: | ---: | ---: |
| root, ledger row legacy | 18-33 ms | 27-34 ms | |
| record user, ledger row legacy (rolled back or committed) | 10-50 ms | 3.9-4.4 s | 24.4 s |
| record user, the same record again (keyed by the first write) | 4-11 ms | 30 ms | 174 ms |
| record user, legacy row, **with the fix** | | 21-49 ms | 22.7 ms |

On the converted store, the layout a 3.x upgrade leaves, one edit costs as
much as WhitePawn's whole steady 19 s.

**Cause.** sp00ky's generated `_00_game_mutation` event finds a legacy ledger
row with `LET $sp00ky_old = (SELECT id, version FROM _00_version WHERE
record_id = $sp00ky_rid)`. Event bodies run elevated (checked: they read rows
and write tables the user cannot), but 4.0.1 plans their statements from the
session that fired them, and for a record user reading a table whose select
permission is not `FULL` the plan is a table scan, 3M ledger rows per write.
The same statement as root, or at the top level as the user, is an index
read. Inside the event, fired by the user:

| `_00_version` select rule | root | record user |
| --- | ---: | ---: |
| `WHERE record_id.id != NONE` (sp00ky's) | 18 ms | 3,874 ms |
| same, `WITH INDEX idx_record_id` | 20 ms | 3,794 ms |
| `WHERE true`, `WHERE $auth.id != NONE`, `WHERE record::exists(record_id)` | 18 ms | 3,900-4,021 ms |
| `FULL` | 18 ms | 6 ms |

**Why a steady 19 s.** The cost is per record whose ledger row is still
legacy, and a client batch pays it once for each such record it touches.
On a copy of the fresh 4.0.1 dataset, as the owner, in one transaction
(sp00ky's `remoteBatch` shape):

| records in the batch (none keyed yet) | as deployed | same batch again | sp00ky's fix only |
| --- | ---: | ---: | ---: |
| 1 | 3.9 s | 49 ms | 50 ms |
| 2 | 7.7 s | 60 ms | 66 ms |
| 4 | 15.6 s | 109 ms | 123 ms |
| 6 | 23.4 s | 170 ms | 191 ms |

So on the fresh store a batch over four or five not-yet-keyed games costs
WhitePawn's 19 s, and on the converted store a single one does; every write
that reaches new records pays again until the ledger is migrated.
A committed write moves the record's row (the version carries over), so the
same record is cheap the next time, over HTTP and over WebSocket alike
(first 4.0-6.4 s, then 29-88 ms). A write that is rolled back (a client
timeout, a failed batch) moves nothing and pays again on its retry.

Taking the fixes apart on the same copy (owner's single game edit, first
write to the record, median of three):

| applied, cumulatively | first write | second write | root |
| --- | ---: | ---: | ---: |
| as deployed (beta.4 internal schema, WhitePawn as deployed) | 4,294 ms | 36 ms | 45 ms |
| + WhitePawn: `FOR select FULL` everywhere | 4,003 ms | 32 ms | 36 ms |
| + WhitePawn: event rewrites | 3,951 ms | 30 ms | 40 ms |
| + WhitePawn: single-column indexes | 4,103 ms | 49 ms | 50 ms |
| + sp00ky internal schema from this branch | 31 ms | 39 ms | 41 ms |

All of the edit's cost is sp00ky's ledger lookup; WhitePawn's own game
events (`statistics_enqueue`, `activity_day_delta`, ...) are not what is
slow on an edit (they are on a delete, below).

**Fix (sp00ky).** The lookup is a correlated subquery over a one-field
object, the one shape measured as an index read for root and record users on
both lines (`apps/cli/src/sp00ky.rs`, `LEDGER_LOOKUP_ROWS` / `_IDS`):

```surql
LET $sp00ky_old = (SELECT VALUE (SELECT id, version FROM _00_version WHERE record_id = $parent.r) FROM ONLY { r: $sp00ky_rid });
```

On the 3M legacy ledger: 4.0.1 user 4-70 ms, root 17-20 ms; 3.3.5 3-48 ms.
`cargo test -p sp00ky-cli a_record_users_first_write` signs in a record user
on the embedded 4.0.1 and fails with the old form (862 ms against 74 ms).
And the ledger should not be legacy at all: 1.0.0-beta.4's scheduler moves
every row at boot (373-679 s for 3M rows on 4.0.1 here), after which no
write reads the ledger by `record_id`. On 3.3.5 the same migration is much
slower: about 11 s per 1,000-row step on this dataset (9-10 hours for 3M
rows), because RocksDB deletes cost 0.3-0.9 ms each and grow with the table
(0.22 s per step at 100k rows, 1.05 s at 1M).

**The same planner rule elsewhere.** It hits every lookup an event runs for a
record user's write on a table whose select rule is not `FULL`, including
`FOR select WHERE true`, which means the same as `FULL`. A record user deleting
one of their games on the 4.0.1 dataset:

| owner deletes a game (0 analysis rows) | 4.0.1 |
| --- | ---: |
| as deployed (`DELETE analysis WHERE game = $before.id` in `game_children_cleanup`) | 115 s |
| events rewritten (subquery deletes), `analysis(game)` index added | 37-44 s |
| plus `FOR select FULL` instead of `WHERE true` on the tables involved | 0.17-0.53 s |
| as root, as deployed | 47 ms |

sp00ky now writes `FULL` wherever its generated DDL meant everyone (outbox
platform fields, the `spky add api` template, schema templates,
`_00_app_release`, CRDT tables, the SSP's `_00_list_ref_anon`), and
`spky lint` reports `FOR select WHERE true` with the rewrite. A rule that
does restrict (`_00_version`, `statistics`, `notification`) keeps its
meaning; lookups there need the correlated form or a key.

Write-up: `docs/surrealdb-bugs/v4-record-user-event-scan.md`.

## 2. Statements in events, functions and block expressions

The rules, measured on a 300k-row table (3.3.5 used the index in every
row; full matrix in `docs/surrealdb-bugs/v4-block-param-index-scan.md`):

| inside an event or function body, on 4.0.1 | |
| --- | --- |
| `SELECT ... WHERE f = $after.x` | scan (237 ms) |
| `LET $x = $after.x;` first, then `WHERE f = $x` (also in IF, FOR) | index |
| `UPDATE` / `DELETE t ... WHERE f = $x`, even LET-bound | scan (~1 s) |
| `UPDATE` / `DELETE (SELECT VALUE id FROM t WHERE f = $x)` | index |
| `WHERE f INSIDE $list` | scan, even as a subquery |
| `FOR $v IN $list { ... WHERE f = $v }` | index |
| a lookup only a composite index covers (any prefix, unique or not; `analysis (game, engine, owner, fen)`: 20-32 s on 11.45M rows) | scan; a single-column index is an index read |
| **inside a function that writes and calls another `fn::`** | |
| its lookups (`DELETE (SELECT ...)`, `LET $row = (SELECT ... LIMIT 1)[0]; UPDATE $row`) | scan (15-62 ms at 40k rows, 1 ms without the call) |
| the same lookups and writes in a helper that calls no function | index |
| **in a `{ }` block expression sent by a client or service** | |
| `LET $x = $p;` then `UPDATE t ... WHERE f = $x` | scan (1 s) |
| `FOR $r IN $ids { DELETE (SELECT VALUE id FROM t WHERE f = $r) }` over 500 record ids | scan per id (167 s) |
| a `FOR` loop's lookup on a composite index | scan (57-74 ms at 40k rows) |
| **at the top level** | |
| `DELETE t WHERE f INSIDE $ids` | scan (97 ms on 300k, 0.8 ms on 3.3.5) |
| everything else measured | index |

## 3. What changed on the sp00ky side

| fix | path | before (4.0.1) | after |
| --- | --- | ---: | ---: |
| legacy ledger lookup in the generated events (correlated form) | every record-user write on a not-yet-migrated record | 3.9-4.4 s | 24-49 ms |
| `_00_schema_state` defined before its first row | first `spky migrate apply` on a fresh 4.x database | failed (compat 4 refuses the field change) | applies |
| schedule-core prunes: top-level statements, one LET-bound delete per run instead of `RETURN { ... INSIDE $ids }` | every minute, per status | workflow prune 1.2 s on 300k steps with nothing to prune (prod 2-5 s) | 26-160 ms |
| `fn::feature::assign` / `materialize` / `allow` / `disallow`: find by index, write by id | flag changes, admin | assign 3.2 s at 40k rows (embedded) | flat |
| `_00_user_feature(key)` index | the scheduler's flag sweep, per flag every 30 s | table scan (prod 1.6 s) | index |
| `fn::push::update` / `unsubscribe`: subquery writes | client push settings | `UPDATE ... WHERE` scan | index |
| `_00_crdt_cleanup`, `_00_channel_signal_sweep`: LET + subquery delete | CRDT parent deletes, WebRTC signals | 1.1 s on 300k rows | 14 ms |
| single-column indexes `_00_crdt_doc(doc)`, `_00_channel_signal(sender)` (was `(sender, at)`), `_00_user_feature(user)` | the same events, `fn::feature::assign` | 38-41 ms at 40k rows (composite only) | 1 ms |
| feature functions' reads and writes in helpers that call no other function (`fn::feature::flag`, `put`, `drop_assignments`), the user's row found `WITH INDEX idx_user_feature_user` | flag changes from DevTools or `materialize` | a 5,000-user materialize: over 10 min (bench: 180 s timeout) | 0.85-1.0 s (3.3.5: 1.1-1.3 s, released 0.7-1.2 s) |
| new and changed internal indexes defined `CONCURRENTLY` | every `spky deploy` that adds or changes an index | up to 60 s per index on a busy 4.0.1 database; the apply failed at 120 s | returns at once |
| `FOR select FULL` instead of `WHERE true` in generated DDL | record users' queries and their writes' events | see section 1 | |
| `spky lint` / `spky doctor`: `where-on-event-value`, `write-where`, `inside-param`, `composite-index`, `lookup-write-with-call`, `select-where-true`; `-- spky-lint: allow <rule>` | app schemas | | |

Tests that keep them from regressing: `cargo test -p schedule-core
prunes_with_nothing_due` (40x history, every prune flat),
`cargo test -p sp00ky-cli body_scan` (flag sweep, feature functions, CRDT and
channel events flat at 40x rows; a record user's first write on a legacy
row), `cargo test -p sp00ky-cli sp00kys_own_schema_passes` (the lint over
everything sp00ky deploys).

## 4. WhitePawn's own schema

`spky lint` on WhitePawn finds 25 events with 53 statements in the shapes
above, 79 `FOR select WHERE true` rules (and 55 more in its outbox schemas),
and 16 event lookups that have only a composite index (4 found by reading
the plans, 12 more by the `composite-index` rule); none of its functions
writes and calls another function. The patch
[`whitepawn-schema-4x.patch`](whitepawn-schema-4x.patch) (apply in the
WhitePawn repo) rewrites them in `packages/schema/src` and adds them as a
migration; [`whitepawn-events-4x.surql`](whitepawn-events-4x.surql) is that
migration on its own. Highlights:

- `statistics_enqueue` (every game write): event values bound first; the
  analyzed-count update through a subquery.
- `game_children_cleanup` (every game delete): six subquery deletes and a new
  `analysis(game)` index; with `FOR select FULL` on the child tables, a user's
  game delete went from 115 s to under 0.6 s.
- `statistics_to_profile` (every statistics write) sums a player's rows by
  `owner`, which only `statistics (owner, database)` covered: new index
  `statistics(owner)`.
- `notify_read`, `statistics_cleanup`, `club_*_cleanup`, `skins_user_delete`,
  `collection_*`, `broadcast_children_cleanup`: the same rewrites; indexes
  `statistics(database)`, `notification(recipient)`, `database_players(club)`,
  `collection_share(collection)`, `activity_day(database)`,
  `puzzle_lecture(puzzle)`, `broadcast_share(broadcast)`,
  `broadcast_eval(broadcast)`, `stream_recording_segment(recording)`,
  `stream_recording_part(recording)`, `club_event(club)`, `club_post(club)`,
  `club_post_comment(post)`. `spky lint` on the patched schema reports
  nothing.

Not in the patch: `src/schema.surql` has a `DEFINE INDEX commented_on_out`
pasted inside the `comment_created` event statement (line 5615), which the
migrations do not have; worth fixing by hand.

## 5. The mixed load and the CPU saturation

Three minutes of everything at once at production cadence (scheduler tick
every 5 s, samplers, flag sweep, job recovery, drift counts, changefeed polls,
TTL sweep, edge publications, metrics flushes, client and backend writes, five
record users holding LIVE queries and each editing one of their games every
10 s), CPU from `docker stats` (100% = one core, cap 300%), then how long the
server stays busy after the load stops:

| server | phase | CPU mean | CPU p95 | CPU max | record user's edit p50 / p95 | busy after |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| 3.3.5 | before | 85% | 147% | 232% | 14.9 / 29.2 ms | 8 s |
| 3.3.5 | after | 100% | 304% | 318% | 8.0 / 18.9 ms | 8 s |
| 4.0.1 fresh | before | 132% | 339% | 376% | 4,087 / 5,231 ms | 8 s |
| 4.0.1 fresh | after | 50% | 183% | 321% | 33.5 / 75.4 ms | 8 s |
| 4.0.1 converted | before | 214% | 337% | 502% | 8,867 / 14,060 ms | 8 s |
| 4.0.1 converted | after | 38% | 189% | 267% | 47.5 / 75.3 ms | 8 s |

Without the record users' edits (the load's client writes all as root), 4.0.1
before the fixes ran at mean 67% and p95 187%, and 3.3.5 at 89% and 172%: the
background load alone does not saturate either server. The edits are what
does it on 4.0.1, a ledger scan of about a core-second each.

The scheduler's ticks time out at 60 s on both versions while the dataset's
backlog of finished runs is pruned (4.0.1: one or two per run; 3.3.5: four).
On 3.3.5 that is the backlog itself: RocksDB deletes cost 0.3-0.9 ms each and
grow with the table (section 6), so a prune batch (`DELETE $doomed.id`) took
4.7 s, 40 of them filled the last 80 s of its after-phase load at 305-312%,
and the first tick took 11.6 minutes there (4.0.1: 102 s). The same deletes
ran in sp00ky's older prune forms; this is 3.3.5's cost, not a change.

Under the load, everything else costs 2-3x more on 4.0.1 than on 3.3.5 in
both phases (edge publications 23 ms against 9 ms, metrics flushes 18 against
5, the jobs sampler 166 against 75), the per-statement gap of section 7.

Timed-out statements keep running. Two `fn::feature::materialize` calls
(released functions) hit the client's 180 s timeout on 4.0.1; on the
converted store they kept two of its three cores busy for more than an hour
(the bench stopped waiting at 60 minutes), and on the fresh store an owner's
edit issued meanwhile did not return for 29 minutes. A query timeout cancels
nothing, so any scan above that a client retries adds load until the server
saturates. (The bench now waits for the server to go idle after a timed-out
workload and records how long that took, and the 4.0.1 runs measure the
workloads after `materialize` in separate passes.)

## 6. Upgrading: the ledger migration, the conversion, index builds

- **The beta.4 ledger migration** (`ssp_protocol::version_ledger::migrate`,
  1,000 rows per step): 373-446 s for 3M rows on 4.0.1 fresh (1,249 s in a
  run beside two other servers), 679 s on the converted store. On 3.3.5 it ran at about 11 s per step,
  9-10 hours for 3M rows: RocksDB deletes cost 0.3 ms each without the unique
  index on `record_id` and 0.9 ms with it at 1M rows, and grow with the table
  (one step: 0.22 s at 100k rows, 1.05 s at 1M; creates and upserts 0.16 ms
  regardless). The 3.3.5 after phase therefore rekeyed the ledger directly
  (drop the table, reload it keyed: 283 s), which ends in the same state.
- **Converting a 3.3.5 store to 4.0.1** (`SURREAL_EXPERIMENTAL_UPGRADE_FROM_3X`):
  the startup data migration "retire the type-erased forward doc-ID mappings"
  runs on one core, in 500-entry batches over every table's doc-ID space,
  while the server answers `/health` but refuses queries with 503. With the
  128 MiB block cache that spooky-cloud's upgrade scratch copy uses
  (`cmd/worker/db_upgrade.go` `scratchEnv`), it re-read SSTable blocks from
  the page cache at about 700 MB/s (4.8 TB in 95 minutes) and had not
  finished; with the production cache (a quarter of memory) it finished in
  97 minutes here. WhitePawn's DB VM did it in 6.5 minutes, so Docker on a
  laptop is far slower at this, but the scratch copy's small cache is a
  multiplier on any large store. The rebuild of all 245 indexes
  `CONCURRENTLY` that follows (the replay upgrade's step) took 350 s.
- **Index builds wait for writes.** 4.0.1 makes every index build wait for
  all write transactions that opened before it, up to 60 s
  (`WRITE_BARRIER_WAIT` in `kvs/write_barrier.rs`), then refuses their
  commits. On a scratch server a blocking `DEFINE INDEX` on a one-row table
  waited 31.8 s for an unrelated running write. On the busy dataset, each new
  index in `spky migrate apply` took exactly 60 s, so the internal schema took
  67 s with one changed index and failed at the CLI's 120 s timeout with two.
  `spky` now sends new and changed internal indexes `CONCURRENTLY` (13 ms).
  WhitePawn's patch already defines its indexes `CONCURRENTLY`.

## 7. What remains slower on 4.0.1, and why

From the after phase, 4.0.1 fresh against 3.3.5, p50:

| workload | 3.3.5 | 4.0.1 | | what it is |
| --- | ---: | ---: | ---: | --- |
| `ssp.edge_publish.round_bumps` | 362 | 4,284 | 11.8x | `UPDATE (SELECT VALUE id FROM <record><-<list_ref table> WHERE in = $from)` per bump plus `RELATE`s, 2,000 operations in one transaction |
| `ssp.edge_publish.initial_3861` | 589 | 6,107 | 10.4x | a cold view publish: `DELETE $from->tbl`, 3,861 `RELATE`s |
| `perm.read.list_ref_membership_fetch` | 34 | 563 | 16.4x | `SELECT * FROM <list_ref table> WHERE in = $q FETCH out`, as the user |
| `write.game.client_create` | 3.6 | 34 | 9.5x | a game `CREATE` through WhitePawn's and sp00ky's events |
| `user.game_merge.again` | 3.2 | 22 | 6.9x | an owner's edit through the events, keyed ledger |
| `ssp.job_lifecycle` | 8.9 | 44 | 5.0x | three `UPDATE type::record(...)` on a job row |
| `perm.read.notifications` | 78 | 316 | 4.0x | `... WHERE recipient = $auth.id ORDER BY created_ms DESC LIMIT 100` as the user |
| `sched.job_enqueue` | 2.5 | 9.8 | 4.0x | `CREATE type::record($tb, $key) CONTENT $content` on an outbox table |
| `ssp.ensure_user_tables` | 15 | 68 | 4.4x | the per-user list_ref DDL (10 `DEFINE ... OVERWRITE`) |
| `sched.ledger_head.keyed` | 0.6 | 256 | | `SELECT VALUE id FROM _00_version LIMIT 1` after the migration (tombstones; once per scheduler boot) |
| `app.gamesync_dedup` | 232 | 553 | 2.4x | `SELECT VALUE source_id FROM game WHERE database = $coll AND source = $source` |

On the converted store, after the fixes: the publication round 7,183 ms, a
cold publish of 3,861 rows 10.4 s, the notification list 629 ms, list_ref
membership 548 ms, the ledger head probe 359 ms.

None of these is a table scan sp00ky could rewrite: the plans are index
reads (or have none, for writes), and the shapes that 4.0.1 scans are the
ones sections 1 and 2 removed. The write costs are per event and per
statement inside them; a probe on a scratch server ruled out commit syncing
(the same 14 ms per request under `sync=every`, `sync=1s` and `sync=never`,
and on 3.3.5). The edge costs grow with the edges a publication touches;
that is the SSP's main write path, and the next thing to profile (SurrealDB
side). Faster on 4.0.1: drift counts on game (192 ms against 699 ms), the
collection count (5.6 ms against 463 ms), a record user's count of their
games (1.9 s against 23 s), and the first scheduler tick.

## 8. Open risks

- **Migrating a big ledger on 3.x** takes hours (section 6); the scheduler
  does it in the background, but until it finishes every record user's write
  to a legacy record runs the fallback lookup. Projects staying on 3.3.5
  should expect it.
- **The cloud upgrade's scratch copy** (spooky-cloud `scratchEnv`, 128 MiB
  SurrealKV cache) assumes the copy reads each table once; 4.0.1's startup
  migration does not, and on a large store it crawls. A larger cache for the
  startup phase, or a measured upper bound on store size, would help. Not
  changed here (cloud repo).
- **Write barrier on index builds.** sp00ky's own indexes are now
  `CONCURRENTLY`; an app's migration that defines an index without it, on a
  busy 4.0.1 database, can stall its deploy a minute per index and abort
  writes older than that.
- **Timeouts do not cancel.** A client or proxy timeout leaves the statement
  running; retries multiply it. Worth a server-side `TIMEOUT` on sp00ky's
  heavy statements (prunes, drift counts, materialize) as a follow-up.
- **WhitePawn must apply its patch** (`whitepawn-schema-4x.patch`): the
  record-user write fix is sp00ky's alone, but the delete (130 s), the
  backend insights write (4.3 s) and the statistics events are WhitePawn's
  events.
- **The bench is not production hardware.** Servers ran side by side on one
  laptop (OrbStack), so absolute numbers carry contention; ratios
  between servers in the same phase are the comparison to trust. The 3.3.5
  before phase was recovered from its run log (the run was stopped during
  the 9-hour migration); its p50/p95 are exact, its plans are not recorded.
- **The internal-schema changes of this branch** add three indexes on
  existing tables (`_00_user_feature(user)`, `_00_crdt_doc(doc)`,
  `_00_channel_signal(sender)` replacing `(sender, at)`), built
  `CONCURRENTLY`; until built, the lookups they serve scan as before.

## Reproduce

```sh
cargo build --release -p sp00ky-cli -p sdb-parity
# once: the three datasets (3.3.5 and 4.0.1 fresh about 2 h; the 4.0.1
# conversion of the 3.3.5 store about 2 h more on a laptop)
target/release/sdb-parity prepare --spky <spky of the release to test>
# one command per server (they can run side by side, each with its own label)
target/release/sdb-parity run --targets 401f,401c \
  --apply-schema target/release/spky --app-patch docs/perf/whitepawn-events-4x.surql \
  --migrate-ledger --label full-4
target/release/sdb-parity run --targets 335 \
  --apply-schema target/release/spky --app-patch docs/perf/whitepawn-events-4x.surql \
  --rekey-ledger --label full-335
target/release/sdb-parity report --labels full-335,full-4
```

`--rekey-ledger` on 3.3.5 because its migration takes 9-10 hours (section 6).
The released `fn::feature::materialize` times out on 4.0.1 before the fixes
and keeps the server busy for over an hour; the bench waits for that, or add
`--skip fn.feature.materialize`. Disk: the three datasets take about 47 GB
in Docker's volumes (`sdb4-parity-335-data`, `sdb4-parity-401f-data`,
`sdb4-parity-401c-data`, kept between runs), and each run copies its dataset
first (10-22 GB). See `apps/sdb-parity/README.md`.
