# SurrealDB 4.0.1 scans inside events fired by a record user

**Observed on:** SurrealDB server `v4.0.1` (SurrealKV), 2026-11-17. Not on
`v3.3.5`.

## Symptom

On WhitePawn (4.0.1, 3M-row `_00_version`) one client write, sp00ky's batch
`LET $ok0 = { UPDATE $id0 MERGE $data0; true; }` on a game, took a steady
18.7 to 19.4 s. The same edit as root took 17 to 18 ms with every event
running. Locally, on a WhitePawn-shaped dataset (750k games, 3M legacy ledger
rows, WhitePawn's real permissions and events, `apps/sdb-parity`):

| the same game edit | 3.3.5 | 4.0.1 |
| --- | ---: | ---: |
| root, ledger row legacy (random id) | 18-33 ms | 27-34 ms |
| record user, ledger row legacy | 10-50 ms | 3,900-4,400 ms |
| record user, ledger row keyed | 4-11 ms | 34-160 ms |

## Cause

The generated `_00_<table>_mutation` event reads a record's legacy ledger row
with `LET $sp00ky_old = (SELECT id, version FROM _00_version WHERE record_id =
$sp00ky_rid)`. Event bodies run elevated (they see rows the user cannot see
and write tables the user cannot write; checked on both lines), but 4.0.1
plans their statements from the session that fired them. When that is a record
user and the table read has a select permission other than `FULL`, the plan
is a table scan, whatever the permission says:

| `_00_version` select permission, lookup inside an event | root | record user |
| --- | ---: | ---: |
| `WHERE record_id.id != NONE` (sp00ky's) | 18 ms | 3,874 ms |
| the same with `WITH INDEX idx_record_id` | 20 ms | 3,794 ms |
| `WHERE true` | 18 ms | 3,905 ms |
| `WHERE $auth.id != NONE` | 18 ms | 3,900 ms |
| `WHERE record::exists(record_id)` | 18 ms | 4,021 ms |
| `FULL` | 18 ms | 6 ms |

The same lookup at the top level, as the same user, uses the index (2-12 ms).

## Reproduction

```surql
DEFINE ACCESS account ON DATABASE TYPE RECORD
  SIGNIN (SELECT * FROM user WHERE name = $name)
  WITH JWT ALGORITHM HS512 KEY 'a-long-test-key-0123456789abcdef0123456789';
CREATE user:alice SET name = 'alice';
DEFINE TABLE ledger SCHEMALESS PERMISSIONS FOR select WHERE true;
DEFINE INDEX ledger_r ON ledger FIELDS r UNIQUE;
-- 1M rows in ledger, one of them { r: t:x }
DEFINE TABLE t SCHEMALESS PERMISSIONS FULL;
DEFINE EVENT e ON t THEN {
    LET $r = $after.id;
    LET $old = (SELECT id FROM ledger WHERE r = $r);
};
-- signed in as alice: the event scans the ledger
UPDATE t:x SET n = 1;
```

## Workaround

A correlated subquery over a one-field object keeps the index for root and
record users on both lines (measured 4-70 ms on the 3M ledger, 3.3.5 and
4.0.1):

```surql
LET $sp00ky_old = (SELECT VALUE (SELECT id, version FROM _00_version WHERE record_id = $parent.r) FROM ONLY { r: $sp00ky_rid });
```

(`... FROM ONLY $sp00ky_rid` with `$parent.id` is fast on 4.0.1 too but
returns nothing on 3.3.5, and needs the record to exist.)

sp00ky's generated events use it (`apps/cli/src/sp00ky.rs`,
`LEDGER_LOOKUP_ROWS` / `LEDGER_LOOKUP_IDS`), and
`cargo test -p sp00ky-cli a_record_users_first_write` fails if a record
user's first write on a legacy row costs more than ten times a keyed one.
The lookup only runs for rows from before the keyed ledger; once the
scheduler's migration (1.0.0-beta.4) has moved them, no write reads the
ledger by `record_id` at all.

The wrapper does not rescue every shape: on a 4-column composite index
(`analysis (game, engine, owner, fen)`), a lookup on the leading column
inside an event scanned 11.45M rows (20-32 s, as root too) in both
spellings; a single-column index on that field made it 19 ms. Events an app
writes itself should keep their lookups on single-column indexes or keys
(see `docs/perf/whitepawn-schema-4x.patch`).
