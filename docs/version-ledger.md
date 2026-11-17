# The `_00_version` ledger: why it is keyed by record

`_00_version` holds the durable version of every synced record. The generated
`_00_<table>_mutation` event bumps it inside the writing transaction and puts
the new value in the ingest payload as `_00_rv`; the delete event drops it. The
SSP (bootstrap, snapshot catch-up, replay) and the scheduler (replica clone,
drift repair) read it back for every row they scan. Clients compare `_00_rv` to
decide whether to refetch a row, so a version that stalls or goes backwards
leaves stale rows on screen.

Since the release after 1.0.0-beta.3, every ledger row is keyed by the record it versions:

```surql
-- the ledger row of game:a
_00_version:[game:a]          -- type::record('_00_version', [$after.id])
{ record_id: game:a, version: 7 }
```

Every read and write of the ledger is then a direct key access. No query
planner is involved, on any SurrealDB version. The `record_id` field and its
UNIQUE index `idx_record_id` stay: the changefeed tail matches ledger rows to
their record by `record_id` (`maintenance::changefeed::parse_show_changes`),
the table's select permission reads it, and the index guarantees one ledger row
per record while legacy rows remain.

Code: `ssp_protocol::version_ledger` (key, migration, probe),
`ssp_protocol::VersionLookup` (the scan projection), `apps/cli/src/sp00ky.rs`
(`version_bump`, `version_drop`), `maintenance::version_ledger` (the
background migration).

## What happened on SurrealDB 4.0.1

Up to 1.0.0-beta.3 ledger rows had random ids and the events found them through the
index:

```surql
CREATE _00_version SET record_id = $after.id, version = 1;
UPDATE _00_version SET version += 1 WHERE record_id = $after.id;
_00_rv: (SELECT VALUE version FROM ONLY _00_version WHERE record_id = $after.id)
DELETE _00_version WHERE record_id = $before.id;
```

WhitePawn (17M rows, 3M ledger rows) moved from 3.3.5 to 4.0.1 on 2026-11-16
and the database went to 100% CPU at once: one write through an event took
about 14 s.

On 4.0.1, inside a `DEFINE EVENT` body, a statement on `_00_version` filtered
by `record_id = <parameter>` does not use the index. It scans the table. The
same statement at the top level of a query uses the index (about 1 ms), and a
literal record id in the event is fast too. It happens on a fresh 4.0.1
database as well as on one converted from 3.x. Measured 2026-11-16 (fresh
4.0.1 servers, ledger of 202k rows in memory, 3M rows on SurrealKV):

| statement inside an event | 4.0.1, 202k | 3.3.5, 202k |
| --- | ---: | ---: |
| `SELECT ... WHERE record_id = $after.r` | 250-800 ms | 1 ms |
| same with `LIMIT 1` | 150-500 ms | 1 ms |
| `... WHERE record_id IN [$after.r]` / `INSIDE` | 0.7-1.2 s | 1 ms |
| `... WITH INDEX idx_record_id WHERE record_id = $after.r` | 200 ms | 1 ms |
| `UPDATE` / `DELETE ... WHERE record_id = <any parameter>` | 800 ms | 1 ms |
| `LET $id = $after.r;` at the top of the body, then `SELECT ... WHERE record_id = $id` | 1 ms | 1 ms |
| `SELECT ... FROM type::record('_00_version', [$after.r])` | 1 ms | 1 ms |

(Times include a 20 ms client round trip; "1 ms" means indistinguishable from
an event that does nothing.)

So the planner's choice depends on the shape of the statement, and the write
statements (`UPDATE` / `DELETE ... WHERE`) scan in every shape tried. A synced
write through the old events, with a 3M-row ledger:

| | 4.0.1 converted from 3.3.5 | 4.0.1 fresh (SurrealKV) | 3.3.5 (RocksDB) |
| --- | ---: | ---: | ---: |
| `UPDATE` | 22-28 s | 19 s | 2-6 ms |
| `CREATE` | 3.4 s | 2.5 s | 4 ms |
| `DELETE` | 14 s | 12.5 s | 2-3 ms |
| same writes, keyed events | 3.5-6.7 ms | 4-6 ms | 1-4 ms |

(The keyed figures include each record's first write, which takes the legacy
fallback described below.)

The scans also hit the readers. A scan reads each row's version with a
correlated subquery, and the two SurrealDB lines plan its two forms in
opposite ways (1000 rows of a 202k ledger):

| projection | 4.0.1 | 3.3.5 |
| --- | ---: | ---: |
| `{ LET $spky_row_id = id; RETURN (SELECT ... WHERE record_id = $spky_row_id) }` | 85 s | 80 ms |
| `(SELECT ... WHERE record_id = $parent.id)` | 162 ms | 84 s |
| `type::record('_00_version', [id]).version` | 13 ms | 25 ms |

The SSP picked one of the first two at bootstrap from `db.version()` and kept
it, so after a server upgrade under a running SSP every scan used the form that
scans the ledger for every row. At 3M rows even the "right" 4.0.1 form costs
45 to 47 ms per row (WhitePawn, and a 3M-row ledger converted from 3.3.5
locally: 100 rows in 4.7 s; the other form 1.4 s per row). Keyed, the same
database answers a 1000-row page in 7.6 ms.

## The events now

```surql
-- _00_<table>_mutation
LET $sp00ky_rid = $after.id;
LET $sp00ky_vk = type::record('_00_version', [$sp00ky_rid]);
LET $sp00ky_ver = IF $event = "CREATE" OR record::exists($sp00ky_vk) {
    (UPSERT ONLY $sp00ky_vk SET record_id = $sp00ky_rid, version += 1 RETURN VALUE version)
} ELSE {
    LET $sp00ky_old = (SELECT id, version FROM _00_version WHERE record_id = $sp00ky_rid);
    DELETE $sp00ky_old.id;
    (CREATE ONLY $sp00ky_vk SET record_id = $sp00ky_rid, version = ($sp00ky_old[0].version ?? 0) + 1 RETURN VALUE version)
};
-- ... _00_rv: $sp00ky_ver

-- _00_<table>_delete
LET $sp00ky_rid = $before.id;
LET $sp00ky_vk = type::record('_00_version', [$sp00ky_rid]);
IF record::exists($sp00ky_vk) {
    DELETE $sp00ky_vk;
} ELSE {
    DELETE (SELECT VALUE id FROM _00_version WHERE record_id = $sp00ky_rid);
};
```

The `ELSE` branches are the transition: a record whose ledger row still has a
random id moves it to the keyed id on its first write, deleting the old row
first so the UNIQUE index never holds two rows for one record. They bind the id
with `LET` at the top of the body, the one shape both lines plan as an index
read (measured 4 to 6 ms on 4.0.1 with 3M legacy rows), and they stop being hit
once the migration below has run. A `CREATE` never takes the fallback: a new
record has no old row. An orphaned legacy row for a re-created record fails the
UNIQUE index, exactly as the old `CREATE _00_version` did, until the migration
has moved it; after that the keyed row simply continues its version.

Steady state with 3M rows, after the migration: 4 to 13 ms per write on 4.0.1
converted from 3.3.5, 2 to 34 ms on a fresh 4.0.1 (SurrealKV compacting the
migration's deletes), 1 to 4 ms on 3.3.5 (RocksDB).

A side effect: the old lookups also failed silently when the UNIQUE index was
missing entries (`docs/surrealdb-bugs/unique-index-partial-after-overwrite-rebuild.md`).
A keyed row does not depend on the index at all.

## The readers now

`ssp_protocol::VersionLookup` has three forms:

- `Keyed`: `type::record('_00_version', [id]).version ?? _00_rv ?? 1`. Used once
  no legacy row remains. The same on every server.
- `KeyedThenBoundId` / `KeyedThenParentId`: the keyed read, then, behind `??`,
  the old lookup in the form the server plans as an index read (3.x / 4.x).
  `??` evaluates its right side only when the left is NONE (checked on both
  lines), so a migrated row never pays for the fallback.

Both inputs are resolved per scan, not per process: the SSP's
`bootstrap::version_lookup` (every `load_schema`, every replay) and the
scheduler's `replica::upstream_version_lookup` (every table page loop, every
drift re-read) probe `SELECT VALUE id FROM _00_version LIMIT 1` and, only while
legacy rows remain, `db.version()`. Record keys sort number, string, uuid,
array, object on 3.x and 4.x, so the first row is a legacy one exactly when
legacy rows remain. A server upgrade or the migration finishing under a running
process takes effect at its next scan.

The probe walks over the keys the migration deleted until the storage engine
compacts them: right after migrating 3M rows it took 0.3 to 1.4 s on SurrealKV
(4.0.1) and 10 to 14 ms on RocksDB (3.3.5). It runs once per scan of a table,
not per row.

While legacy rows remain, a scan pays the legacy lookup for each row not yet
moved: about 28 ms per row on the converted 3M-row database. That is why the
migration runs at boot rather than leaving the rows to the events.

## The migration

`ssp_protocol::version_ledger::migrate`, run in the background at boot by the
scheduler (`maintenance::version_ledger::spawn_migration`) and, in singlenode
mode, by the standalone SSP. Each step is one transaction:

```surql
BEGIN TRANSACTION;
LET $spky_batch = (SELECT id, record_id, version FROM _00_version WHERE id > <cursor> LIMIT 1000);
LET $spky_legacy = $spky_batch.filter(|$r| !type::is_array(record::id($r.id)));
FOR $spky_old IN $spky_legacy {
    DELETE $spky_old.id;
    IF $spky_old.record_id != NONE {
        UPSERT type::record('_00_version', [$spky_old.record_id])
            SET record_id = $spky_old.record_id, version = math::max([version ?? 0, $spky_old.version ?? 0]);
    };
};
COMMIT TRANSACTION;
RETURN { seen: ..., moved: ..., last: ... };
```

- Legacy keys sort before keyed ones, so the steps walk the legacy rows in key
  order and stop at the first keyed row. A final probe of the first key catches
  a legacy row written behind the cursor (only an older release's event writes
  one) and restarts from the top.
- The cursor matters on 4.x, where `WHERE id > <id>` seeks: re-reading from the
  first key instead degraded from 110 ms to 450 ms per 1000 rows (3.7 s at
  worst) as 1.6M deleted keys piled up in front of it on SurrealKV. With the
  cursor a step stays flat. On 3.x a cursor scan starts at the first key
  either way (as does every other form measured), so the step costs the same
  as reading from the top; on RocksDB it stayed flat too.
- An event and the migration moving the same row race on the same keys: one
  transaction fails with a conflict and the loser re-runs against the
  committed state (the migration retries a step up to 50 times; a client
  retries a conflicted write as a network error, see
  `classifySyncError`). `math::max` keeps a version an event already bumped.
  The legacy row is deleted before the keyed one is written, so the UNIQUE
  index never sees two rows for one record.
- Idempotent and resumable: a run over a migrated ledger reads one row and
  stops; a run cut off anywhere is finished by the next one or by the events.
- 20 ms pause between steps; logs every 10 s (`Version ledger migration in
  progress`), and once at the end (`Version ledger migrated: every row is
  keyed by its record`). A failed run is retried with backoff (5 s doubling
  to 5 min).

Measured on 3M-row ledgers (local Docker, 1000-row steps, flat from first to
last):

| | per step | 3M rows |
| --- | ---: | ---: |
| 4.0.1 converted from 3.3.5 (SurrealKV) | 102 ms | 5.1 min |
| 4.0.1 fresh (SurrealKV) | 130-150 ms | about 7 min |
| 3.3.5 (RocksDB) | 354 ms | 17.7 min |

Add the 20 ms pause per step (one minute per 3M rows) and whatever the host is
slower than a laptop. The runner itself, over the SDK's HTTP engine, moved 200k
rows in 16 s on 4.0.1 and 38 s on 3.3.5 (both in memory).

## Tests

`apps/cli/src/version_ledger_tests.rs` builds the schema the CLI generates for
a small app and runs it on an embedded SurrealDB 4.0.1 (kv-mem) by default,
and on 3.3.5 and 4.0.1 servers with Docker:

```sh
cargo test -p sp00ky-cli version_ledger                                   # embedded 4.0.1
cargo test -p sp00ky-cli version_ledger -- --include-ignored --test-threads=1   # + Docker 3.3.5 and 4.0.1
```

- every event path (legacy fallback, keyed update, create, both deletes) costs
  the same on a 100k-row ledger as on a 1000-row one (the old templates: 6.0 s
  against 0.1 s on the embedded engine);
- a 1000-row scan page with `_00_rv` stays within 20 times the same page
  without it, before and after the migration;
- the migration keeps every version, is idempotent, and loses no version bump
  while four writers update records through the events.

The runner the scheduler starts, over the SDK's HTTP engine, against a server
holding a legacy ledger in namespace and database `kv` (root/root):

```sh
SPKY_LEDGER_TEST_URL=http://127.0.0.1:8000 cargo test -p maintenance version_ledger -- --nocapture
```

The host side: `cargo test -p ssp-node --test node_platform bootstrap_reads_keyed`
and `cargo test -p scheduler row_versions_come_from` read versions from keyed,
legacy and missing ledger rows and check the lookup turns `Keyed` once the last
legacy row is gone.
