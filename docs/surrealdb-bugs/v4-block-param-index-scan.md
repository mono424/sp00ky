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

### Composite indexes

40,000 rows, a composite index and no single-column one on the matched
fields, memory engine, per call:

| statement | 3.3.5 | 4.0.1 |
| --- | ---: | ---: |
| event: `LET $s = $after.s; SELECT VALUE id FROM u2 WHERE c = $s AND d = $k`, index `(c, d)` | 0.3-3 ms | 37-38 ms |
| event: the same on the leading column only (`WHERE c = $s`) | | 35-38 ms |
| event: `UNIQUE` index `(a, b)`, either prefix | | 35-39 ms |
| event: `DELETE (SELECT VALUE id FROM cdoc WHERE doc = $doc)`, `UNIQUE (doc, field)` (sp00ky's `_00_crdt_cleanup`) | 0.5-1.7 ms | 39-41 ms |
| the same with `WITH INDEX`, the correlated form, both columns, a `FOR` loop variable | | 28-48 ms |
| the same, plus a single-column index on `doc` | | 0.9-1.1 ms |
| function: `RETURN (SELECT VALUE id FROM u2 WHERE c = $s AND d = 'k')`, `$s` an argument | 0.3-0.5 ms | 39 ms |
| top level: `FOR $s IN [user:u15] { SELECT ... WHERE c = $s }` | | 57-74 ms |
| top level: `LET $s = ...; SELECT ... WHERE c = $s` | | 0.5 ms |
| top level, `{ }` block expression, `BEGIN ... COMMIT` | | 0.2-0.6 ms |
| a literal instead of the parameter, in an event | | 1.6-1.8 ms |

Inside an event or function body, and in a `FOR` loop even at the top level,
4.0.1 does not use a composite index for a parameter, whichever of its
columns the WHERE matches; a single-column index on one of them is used.

### Functions that call functions

40,000 rows of `_00_user_feature`, single-column indexes on `user` and
`key`, per call on 4.0.1 (3.3.5, measured for the subquery and LET-bound
deletes with a custom call: 0.1-0.2 ms):

| function body | 4.0.1 |
| --- | ---: |
| `LET $row = (SELECT VALUE id FROM t WHERE user = $user LIMIT 1)[0]; IF $row != NONE { UPDATE $row SET ... };` | 0.8-1.8 ms |
| the same plus `LET $h = fn::noop(1);` (a custom function, before or after) | 15-40 ms |
| `LET $h = fn::noop(1); LET $ids = (SELECT VALUE id FROM t WHERE user = $user); UPDATE $ids SET ...` | 0.6-2 ms |
| `DELETE (SELECT VALUE id FROM t WHERE key = $key)`, no other call | 0.4-0.9 ms |
| the same plus a call to a custom function that never runs (after a `RETURN`) | 39-62 ms |
| the lookup and the write moved into a helper function that calls nothing | 1.7-2.6 ms |
| `SELECT` lookups only (no write) plus a custom call | 0.3-0.8 ms |

A function that both writes and calls another custom function (`fn::...`;
built-ins do not count) gets lookups that read the whole table, depending on
their shape. sp00ky's `fn::feature::assign` (which calls `fn::feature::hash`)
read every row of a flag per user this way, so materializing a flag was
quadratic in users; `fn::feature::materialize` read the table for an unknown
flag. Event bodies are not affected.

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

Composite and calling-function shapes:

```surql
DEFINE TABLE cdoc SCHEMALESS;
DEFINE INDEX cdoc_doc ON cdoc FIELDS doc, field UNIQUE;
-- 40k rows: INSERT INTO cdoc { doc: type::record('thread', 'o' + <string>$i), field: 'summary' } ...
DEFINE TABLE thread SCHEMALESS;
DEFINE EVENT cl ON TABLE thread WHEN $event = 'DELETE' THEN {
    LET $doc = $before.id;
    DELETE (SELECT VALUE id FROM cdoc WHERE doc = $doc);  -- 40 ms on 4.0.1, 1 ms on 3.3.5
};
CREATE thread:d1; DELETE thread:d1;

DEFINE FUNCTION fn::noop($x: any) { RETURN $x; };
DEFINE FUNCTION fn::drop($key: string) {
    DELETE (SELECT VALUE id FROM uf WHERE key = $key);    -- index read
};
DEFINE FUNCTION fn::drop2($key: string) {
    DELETE (SELECT VALUE id FROM uf WHERE key = $key);    -- 40 ms at 40k rows on 4.0.1
    RETURN fn::noop(1);
};
```

## What sp00ky does about it

- Queries sp00ky sends are top-level statements, not `{ ... }` blocks, when
  they filter by a parameter (schedule-core's prunes, `sql.rs`), and a match
  against a list is one LET-bound equality per value
  (`delete_workflow_runs`, `delete_durable_instances`).
- Generated functions and events bind event values with `LET` first and write
  through `UPDATE / DELETE (SELECT VALUE id FROM ...)` (`fn::feature::*`,
  `fn::push::*`, `_00_crdt_cleanup`, `_00_channel_signal_sweep`).
- Every lookup a generated body makes has a single-column index:
  `_00_crdt_doc(doc)`, `_00_channel_signal(sender)` (was `(sender, at)`),
  `_00_user_feature(user)` and `(key)`.
- The feature functions keep their reads and writes in helpers that call no
  other function (`fn::feature::flag`, `put`, `drop_assignments`).
- `spky lint` / `spky doctor` flag the shapes in an app's own events and
  functions (`apps/cli/src/query_lint.rs`: `where-on-event-value`,
  `write-where`, `inside-param`, `composite-index`,
  `lookup-write-with-call`), and a test runs the lint over everything sp00ky
  deploys.
- Cost tests: `cargo test -p schedule-core prunes_with_nothing_due` and
  `cargo test -p sp00ky-cli generated_functions_and_events_stay_cheap`
  (flag sweep, `fn::feature::assign`, `materialize`, `_00_crdt_cleanup`,
  `_00_channel_signal_sweep`, 1,000 against 40,000 rows, within 4x).

When a fixed SurrealDB ships, nothing needs reverting: every rewrite is an
index read on 3.3.5 too.
