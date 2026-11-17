# SurrealDB 4.0.1 scans for parameters inside blocks, loops and list matches

**Observed on:** SurrealDB server `v4.0.1` (memory and SurrealKV, fresh and
converted from 3.x), 2026-11-16. Compared with `v3.3.5`.

Follows `v4-event-param-index-scan.md`: the same planner gap reaches beyond
`DEFINE EVENT` bodies. Whether 4.0.1 uses an index for a statement depends
on where the statement sits (top level, a `{ ... }` block expression, a
`FOR` loop, an event or function body), on how its parameter got its value
(bound with the request, `LET` at the top level, `LET` inside the block, a
loop variable, an event value), and on the statement kind (`SELECT` versus
`UPDATE` / `DELETE` / `UPSERT ... WHERE`). 3.3.5 used the index in nearly
every shape below.

## Measurements

One table `t` of 300,000 rows with an index on `f` (each `f` matches 3 rows),
a second index on `(s, at)`. Best of three, including a ~11 ms client round
trip ("11 ms" means index read). Script: `apps/sdb-parity` (rules harness in
the report).

| statement | 3.3.5 | 4.0.1 |
| --- | ---: | ---: |
| **inside an event body** | | |
| `SELECT ... WHERE f = $after.x` | 12 ms | 237 ms |
| `LET $x = $after.x;` then `SELECT ... WHERE f = $x` (also in IF, FOR) | 11 ms | 12 ms |
| `UPDATE t SET ... WHERE f = $x`, `$x` LET-bound | 11 ms | 1,006 ms |
| `DELETE t WHERE f = $x`, `$x` LET-bound | 11 ms | 995 ms |
| `UPDATE (SELECT VALUE id FROM t WHERE f = $x) SET ...` | 11 ms | 12 ms |
| `DELETE (SELECT VALUE id FROM t WHERE f = $x)` | 11 ms | 12 ms |
| `UPDATE (SELECT VALUE id FROM t WHERE f = $after.x)` (not bound) | 11 ms | 245 ms |
| `SELECT ... WHERE f INSIDE $x` (50 values, LET-bound) | 15 ms | 280 ms |
| `FOR $v IN $x { DELETE (SELECT VALUE id FROM t WHERE f = $v) }` | 15 ms | 16 ms |
| `LET $x; LET $lo;` then `DELETE (SELECT VALUE id FROM t WHERE f = $x AND g < $lo)` | 12 ms | 14 ms |
| `DELETE t WHERE f = $after.x AND g < 0` | 13 ms | 1,115 ms |
| **inside a function body** | | |
| `SELECT ... WHERE f = $arg` | 11 ms | 11 ms |
| `UPDATE t ... WHERE f = $k`, `DELETE t WHERE f = $k` | 11 ms | 934-944 ms |
| subquery-id forms of the same | 11 ms | 12 ms |
| `SELECT ... WHERE f INSIDE $arr` (50) | 54 ms | 286 ms |
| `FOR $v IN $arr { SELECT ... WHERE f = $v }` | 15 ms | 13 ms |
| **in a top-level block expression** (`RETURN { ... }`, `LET $ok = { ... }`) | | |
| `SELECT ... WHERE f = $p`, `$p` bound with the request | 11 ms | 11 ms |
| `LET $x = $p;` then `UPDATE t ... WHERE f = $x` | 13 ms | 1,013 ms |
| the same as `UPDATE (SELECT VALUE id ...)` | 13 ms | 13 ms |
| `WHERE s = $s AND at < $b ORDER BY at LIMIT 100`, request params | 13 ms | 11 ms |
| the same with `LET $s2 = $s;` inside the block | 15 ms | 248 ms |
| **at the top level** | | |
| `SELECT`, `UPDATE`, `DELETE ... WHERE f = $p` (bound or top-level LET) | 11 ms | 11-14 ms |

And on a 300,000-row `_00_step_run` (sp00ky's workflow steps), 500 run ids
that match nothing, at the top level:

| statement | 3.3.5 | 4.0.1 |
| --- | ---: | ---: |
| `DELETE _00_step_run WHERE workflow_run INSIDE $ids` | 0.8 ms | 97 ms |
| `DELETE (SELECT VALUE id FROM _00_step_run WHERE workflow_run INSIDE $ids)` | 0.8 ms | 102 ms |
| `SELECT VALUE id FROM _00_step_run WHERE workflow_run INSIDE $ids` | 92 ms | 54 ms |
| `FOR $r IN $ids { DELETE (SELECT VALUE id FROM _00_step_run WHERE workflow_run = $r) }` | 7 ms | **167 s** |
| 500 x `LET $wN = type::record($rN); DELETE _00_step_run WHERE workflow_run = $wN;` | 294 ms * | 100 ms * |

\* actually deleting the 1,500 matching steps of 500 existing runs.

So a `FOR` loop over record ids at the top level scans the table once per
iteration on 4.0.1 (inside an event the same loop over strings did not), and
list matches (`INSIDE`, `IN`) scan in every position on 4.0.1 and, for a
`SELECT`, on 3.3.5 too.

## Reproduction

```surql
DEFINE TABLE t SCHEMAFULL;
DEFINE FIELD f ON t TYPE string;
DEFINE INDEX t_f ON t FIELDS f;
LET $rows = array::map(array::range(0, 10000), |$i| { id: type::record('t', $i), f: 'k' + <string>($i % 100000) });
INSERT INTO t $rows RETURN NONE;  -- repeat to 300k rows

-- 1 s on 4.0.1, 11 ms on 3.3.5: an UPDATE ... WHERE in a block
LET $p = 'k7';
LET $ok = { LET $x = $p; UPDATE t SET f = f WHERE f = $x RETURN NONE; true };

-- index read on both
LET $ok = { LET $x = $p; UPDATE (SELECT VALUE id FROM t WHERE f = $x) SET f = f RETURN NONE; true };

-- 97 ms on 4.0.1, under 1 ms on 3.3.5, even at the top level
LET $ids = ['nomatch-a', 'nomatch-b'];
DELETE t WHERE f INSIDE $ids;
```

## What sp00ky does about it

- Queries sp00ky sends are top-level statements, not `{ ... }` blocks, when
  they filter by a parameter (schedule-core's prunes, `sql.rs`), and a match
  against a list is one LET-bound equality per value
  (`delete_workflow_runs`, `delete_durable_instances`).
- Generated functions and events bind event values with `LET` first and write
  through `UPDATE / DELETE (SELECT VALUE id FROM ...)` (`fn::feature::*`,
  `fn::push::*`, `_00_crdt_cleanup`, `_00_channel_signal_sweep`).
- `spky lint` / `spky doctor` flag the shapes in an app's own events and
  functions (`apps/cli/src/query_lint.rs`), and a test runs the lint over
  everything sp00ky deploys.
- Cost tests: `cargo test -p schedule-core prunes_with_nothing_due` and
  `cargo test -p sp00ky-cli generated_functions_and_events_stay_cheap`.

When a fixed SurrealDB ships, nothing needs reverting: every rewrite is an
index read on 3.3.5 too.
