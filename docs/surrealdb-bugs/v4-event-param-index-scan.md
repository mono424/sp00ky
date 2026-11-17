# SurrealDB 4.0.1 scans instead of using an index inside an event body

**Observed on:** SurrealDB server `v4.0.1` (memory and SurrealKV, fresh and
converted from 3.x), 2026-11-16. Not on `v3.3.5`.

## Symptom

WhitePawn moved from 3.3.5 to 4.0.1 and the database went to 100% CPU at
once. Every synced write took seconds (about 14 s on a 3M-row `_00_version`),
job claims hung. The generated `_00_<table>_mutation` events found the
record's ledger row with `WHERE record_id = $after.id`, which 4.0.1 runs as a
scan of the whole table when it sits inside a `DEFINE EVENT` body.

## Reproduction

```surql
DEFINE TABLE ledger SCHEMALESS;
DEFINE FIELD record_id ON ledger TYPE record;
DEFINE INDEX idx_record_id ON ledger COLUMNS record_id UNIQUE;
INSERT INTO ledger (array::map(array::range(0, 5000), |$i| { record_id: type::record('other', 'k' + <string>$i), version: 1 })) RETURN NONE;
-- ... repeat to 200k rows
INSERT INTO ledger { record_id: job:a, version: 1 };
DEFINE TABLE job SCHEMALESS;

-- scans: 250-800 ms per write on 4.0.1, about 1 ms on 3.3.5
DEFINE EVENT e ON job WHEN true THEN {
    UPDATE ledger SET version += 1 WHERE record_id = $after.id;
};
UPDATE job:a SET n = 1;

-- the same statement at the top level uses the index
LET $id = job:a; UPDATE ledger SET version += 1 WHERE record_id = $id;
```

Shapes measured inside the event (202k rows): `SELECT ... WHERE record_id =
$after.id` with or without `LIMIT 1`, `IN [$after.id]`, `INSIDE`, `>= AND <=`,
`WITH INDEX idx_record_id`, a block `{ LET $id = $after.id; RETURN (SELECT
...) }` used as a field value, and every `UPDATE` / `DELETE ... WHERE
record_id = <parameter>` all scan. `LET $id = $after.id;` as a statement at the
top of the body followed by `SELECT ... WHERE record_id = $id` uses the index,
as does a key read `type::record('ledger', [$after.id])`.

The same split hits correlated subqueries in a projection, the other way
round from 3.3.5: `{ LET $x = id; RETURN (SELECT ... WHERE record_id = $x) }`
scans on 4.0.1 and seeks on 3.3.5, `(SELECT ... WHERE record_id = $parent.id)`
does the opposite.

## Workaround

Ledger rows are keyed by the record they version (`_00_version:[game:a]`), so
every access is a key read or write, and the readers read the key first. The
remaining `record_id` lookups (rows written before the change, until the
background migration has moved them) use the `LET`-bound shape. See
`docs/version-ledger.md`; code in `apps/cli/src/sp00ky.rs` (`version_bump`,
`version_drop`), `packages/ssp-protocol/src/version_ledger.rs` and
`VersionLookup` in `packages/ssp-protocol/src/lib.rs`.

When a fixed SurrealDB ships, nothing needs reverting: the keyed ledger does
not depend on the planner.
