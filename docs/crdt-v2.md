# CRDT fields (v2): edit log, server fold

Status: implemented in the CLI expansion, the SSP fold and the TS client;
Dart pending. Replaces the inline-snapshot design and the old
`docs/crdt-flow.html`. User-facing docs:
`apps/landing-page/src/pages/docs/client/crdt.mdx`. Keep this note true when
the code deviates.

## Goal

A `-- @crdt <kind>` field is edited by many clients at once as one Loro
document. Only deltas travel, over the existing single SurrealDB socket and
the existing outbox and view pipeline. Clients merge live. The server is the
only writer of canonical state, folds rarely so its CPU stays flat, and no
client can make the remote state inconsistent.

What v1 got wrong, for the record: clients pushed the full snapshot with a raw
`UPDATE` (last push wins, so two typists lost text server-side), wall-clock
echo suppression dropped real concurrent edits, every table with a CRDT field
had its own `LIVE SELECT`, and `/crdt/apply` + `SPKY_CRDT_FIELDS` were dead
code. All of that is removed. There is no migration from v1 state.

## Schema

```sql
-- @crdt prosemirror
-- @cursor
DEFINE FIELD content ON TABLE thread TYPE string;
```

Kinds: `text` (one `LoroText` named after the field; the default when the
value is omitted) and `prosemirror` (a loro-prosemirror tree under root map
`"doc"`). loro-prosemirror owns that root key, so the unit is **one LoroDoc
per (record, field)**. `@cursor` adds presence.

The CLI (`apps/cli/src/crdt_schema.rs::expand`) refuses: a non-`string` type,
a `VALUE`/`ASSERT` on the field, nested fields, relation tables, an unknown
kind, the reserved names (`*__crdt_log`, `*__crdt_presence`, `_00_crdt_doc`,
fields ending `__crdt`), and the `surrealism` deploy mode (no SSP to fold).
The expansion runs in every schema pipeline (server schema, events, internal
schema, client schema), so generated tables get events, markers and
changefeeds like user tables.

### Data model (parent table `P`)

| Object | Shape | Written by | Permissions |
|---|---|---|---|
| `P.<field>` | `TYPE string DEFAULT ''`, guard `VALUE IF $auth.id != NONE AND $before != NONE { $before } ELSE { $value OR '' }` | the creating client (initial text), then the fold (root) | the author's own field `PERMISSIONS` are kept |
| `P.<field>__crdt` | `option<object>`: `{ rev, upto: { vv, frontiers }, state_hash }` | the fold | same guard: record users keep `$before` |
| `P__crdt_log` | `doc record<P>`, `field` (ASSERT in P's crdt fields), `author` (server-stamped), `peer` (decimal string, must be owned by `author` in `_00_crdt_peer`; root is trusted), `data bytes` (`@opaque`, <= 256 KiB), `size` (`bytes::len(data)`), `created_at`; index `(doc, field)`; marker `COMMENT 'sp00ky:crdt_log <field>=<kind>,...'` | clients CREATE, the fold DELETEs | select = P's select rule rebased through `doc`; create = P's update rule rebased, and not an impersonation session; update/delete NONE |
| `P__crdt_presence` (`@cursor` only) | `doc`, `field`, `session`, `author`, `state bytes` (`@opaque`, <= 16 KiB), `at` | each session UPSERTs its own row | select = P's select; create = P's update (no impersonation); update/delete own rows only |
| `_00_crdt_doc` | id `[P:record, field]`; `rev`, `snapshot bytes`, `upto`, `state_hash` (see Verification), `lease_by`, `lease_until`, `folded_at` | the fold only | select = OR of each parent's select rule; writes NONE. Not synced: read one-shot |
| `_00_crdt_peer` | id = peer; `author` (stamped), `at` | clients CREATE their claim | select own; create when signed in and not impersonating; update/delete NONE |
| event `_00_crdt_cleanup` on `P` | DELETE of a record drops its `_00_crdt_doc` rows | | |

Permission rebasing works on the surrealdb-core AST (`rebase_permission`):
bare fields move under `doc` (`published` -> `doc.published`, bare `id` ->
`doc`), `$parent.id` -> `$parent.doc`, fields inside statement subqueries stay
put. So who may read or write a document's edits is exactly who may read or
update the record.

The client schema (`Target::Client`) gets the log/presence tables as plain
`PERMISSIONS FULL` columns and no `_00_crdt_doc`/`_00_crdt_peer`: the local
store only ever holds rows the server already let through.

The mirror is an ordinary synced column (no opaque marker unless the author
adds `@opaque`), so it is filterable and the SSP circuit holds it. The log's
`data` and presence `state` are `@opaque`: they reach clients through body
fetches but never enter the circuit or the scheduler replica.

## Acceptance rule

A pure function of one row, identical in TS
(`packages/core/src/modules/crdt/acceptance.ts`) and Rust
(`packages/ssp-node/src/crdt/acceptance.rs`). Every client and the fold apply
it before importing, so all sides import the same rows (a CRDT never forgets an
op: a row one side took and another refused would split them forever).

1. Non-empty, at most 256 KiB, `peer` parses as a u64.
2. Raw header: magic `loro`, encoding mode (bytes 20..22, big-endian) = 4
   (updates). Checked before any decoding, because Loro's decoders can panic on
   crafted input with a valid checksum.
3. `decode_import_blob_meta` (under `catch_unwind` / `try`) says `Updates`.
4. The peers whose counters advance between `partial_start_vv` and
   `partial_end_vv` are exactly one, and it equals the row's `peer`.

Verdicts: `empty`, `too_large`, `bad_peer`, `header`, `undecodable`,
`not_an_update`, `no_ops`, `foreign_peer`. Since the DB only admits a row whose
peer the author claimed, rule 4 means nobody can author ops under another
user's peer, and rule 2/3 mean nobody can smuggle foreign history in through a
snapshot.

## Peer identity

Each tab draws a random 53-bit peer id (exact as a JS number) per identity
(boot, sign-in, sign-out) and claims it with
`CREATE type::record('_00_crdt_peer', $peer)`. "Already exists" is fine when we
can read it back (only the owner can), otherwise the peer is taken and the tab
draws a new one. A claim that fails (offline, signed out) is retried every 5 s
and holds back sending, not editing.

Deviation from the approved plan: the plan derived the peer as
SHA-256(author || nonce) with a `nonce` column. The shipped design claims a
random peer in `_00_crdt_peer` and stores `peer` on the row, which keeps the
rule a pure function of the row and the DB check a single record lookup.

## Up path (client)

`packages/core/src/modules/crdt/index.ts`, `CrdtManager`.

1. Local edits apply to the tab's `LoroDoc` immediately;
   `subscribeLocalUpdates` schedules a flush and a local persist. The flush
   is a throttle, not a trailing debounce: the first edit after a quiet spell
   goes out `crdtDebounceMs` later (default 50 ms), and while edits keep
   coming one row goes out every `crdtSendIntervalMs` (default 200 ms). An
   armed flush is never pushed back. (The old 300 ms debounce with a 2 s max
   wait meant a peer saw nothing while someone typed, then 2 s at once.) A
   flush asked for while one is running runs right after it. The `enqueued`
   marks reach the local store with the debounced persist, not after every
   send: a persist exports the whole document, and a crash in between only
   re-sends rows.
2. A flush exports, per own peer with unsent ops, only that peer's ops:
   `export({ mode: 'updates-in-range', spans: [{ id: { peer, counter: enqueued }, len }] })`.
   A plain `from: vv` export would include imported foreign ops and fail rule 4.
3. One `db.create('P__crdt_log:<random>', { doc, field, peer, data })` per
   peer, through the ordinary outbox (offline-safe, retried, batched). Then
   `enqueued` advances. Delivery is at-least-once; a duplicate is harmless (the
   import is idempotent), and the outbox treats "already exists" on a retried
   log CREATE as success (`mutation/push.saga.ts::isRepeatedLogCreate`).
4. `closeCrdtField` resolves after a final persist and flush, i.e. once unsent
   edits are in the outbox. Every server wait in open and close is bounded
   (`SERVER_WAIT_MS`, 4 s) and skipped while the socket is not `connected`, so
   an offline form never hangs on a peer claim or a snapshot read; the work
   goes on in the background. A reopen while a close is still saving waits for
   that save, so it starts from the same local state.

Drafts: `createCrdtDraft(table, field)` returns a detached document; passing
it to `openCrdtField(..., draft)` imports its ops and adds the draft's own peer
to the session's peers, so they are claimed and sent under that peer.

## Down path (client)

An open field watches `SELECT * FROM P__crdt_log WHERE doc = $doc AND field = $field`
through the normal query pipeline (`Sp00kyClient` `watch` port: a registered
view, membership via `_00_list_ref`, bodies fetched once). No table LIVE, no
echo filter: own rows import as no-ops. Rows are processed once per id
(`seen`), checked with the acceptance rule, then imported.

A new log row reaches an open field on the edge's LIVE `CREATE`, which core
commits to the view's membership straight away (`applyEdgeDeltas` in
`query/membership.saga.ts`) instead of re-reading the edges; with
`liveInlineBodies` the row's body rides on the same notification, so nothing
is read before it renders. Measured at the hosted demo's round trip from the
US west coast (~320 ms, `example/e2e/bench/crdt-latency`): one edit went from
1.5 s to 0.55 s, the lag behind someone typing from 2.1 s median (3.2 s worst)
to 0.8 s (1.1 s), and a first open from 2.0 s to 1.2 s. What is left is the
write's round trip, the SSP's edge publish window (100 ms on VM SSPs) and the
50 ms materialize debounce.

Cold open (nothing in `_00_crdt_local`): read the published snapshot from
`_00_crdt_doc` once, in parallel with registering the log query (each is a
round trip, and Loro holds ops whose predecessors have not arrived yet); if
the document is still empty, wait (up to 4 s) for the log query to settle
before handing it out. Without the wait two editors binding an empty
document at once each create the prosemirror root and one of them loses its
text. An import that reports pending ops (they depend on rows folded away
before this device saw them) triggers another snapshot read.

The query allowlist admits shapes over `*__crdt_log` / `*__crdt_presence` as
built-ins (`packages/ssp/src/allowlist.rs::is_builtin`).

## Presence

`@cursor` fields: `pushCursorState(bytes)` is throttled to 250 ms and UPSERTs
`P__crdt_presence:p<peer>_<field>_<record key>` directly (no outbox, no offline
queue: latest state only). Other sessions' rows arrive through a second watch
and are handed to `onCursorUpdate` (own session filtered out; a blob that
arrives before the editor attaches is replayed once). Closing the field
deletes the row; the SSP sweep removes rows older than
`SPKY_CRDT_PRESENCE_TTL_SECS` that a crashed tab left behind.

## Client local store

`_00_crdt_local:<record>|<field>` = `{ doc, field, snapshot, rev, peers: { <peer>: { enqueued } } }`.
Persisted on a 400 ms debounce and on close. Another tab may have written it
meanwhile, so persist imports the stored snapshot first and keeps the larger
`enqueued` per peer (no lost update between tabs). On the next open, ops of any
recorded peer beyond its `enqueued` are sent under that original peer. The
table is in the local migrator's `KEPT_TABLES` and the SQLite `SYSTEM_TABLES`,
so a schema-hash change does not drop it. A bucket switch closes fields
without flushing (`closeAll({ flush: false })`): unsent edits wait in the old
bucket's store and are never sent under the next user.

## Server fold (SSP)

`packages/ssp-node/src/crdt/` (portable, builds for wasm32), wired through
`TimerKind::CrdtFoldTick` and `TimerKind::CrdtSweep`; skipped on a standby and
when not Ready.

Tracker: ingest calls `CrdtFold::observe` for every CREATE on a `*__crdt_log`
table (cheap, synchronous) and accumulates per `(log, doc, field)`: rows,
bytes, last write. A field is **due** when it has pending rows, is past its
retry time, at least `min_gap` since its last fold, and either idle for
`idle` or over `max_rows` / `max_bytes`. Each tick starts up to `workers`
due folds, biggest backlog first, and at most one per record: every field's
fold writes the record (mirror and stamp), so two at once conflict and the
loser would wait out `min_gap`. A record's other field goes on a later tick.

One fold (`fold.rs`):

1. **Lease.** `INSERT IGNORE` the doc row, then
   `UPDATE ONLY ... SET lease_by = $me, lease_until = now + 30s WHERE lease free, expired or ours`.
   No row back: another node holds it; back off one lease time plus a
   deterministic per-node jitter.
2. **Load.** Reuse the cached `LoroDoc` (LRU of `SPKY_CRDT_CACHE_SIZE`) if its
   `rev` still matches, else import the stored snapshot. Read up to 2000 log
   rows (`encoding::base64::encode(data)`: bytes cross the JSON `Db` port as
   base64).
3. **Accept.** Acceptance rule per row, then a trial import into a throwaway
   doc, then the real import, each under `catch_unwind`. A row that panics the
   real import: leak the half-updated doc (its drop can abort), delete the row,
   release the lease, fail this fold; the next one starts from the published
   snapshot. Refused rows are deleted. Accepted rows whose ops still depend on
   missing predecessors stay, unless older than 24 h (then refused as
   stranded).
4. **Publish** (only if the oplog changed): one transaction that
   compare-and-swaps `rev` and `lease_by` on `_00_crdt_doc` (snapshot, `rev+1`,
   `upto`, `folded_at`, lease cleared; `THROW "crdt_cas_lost"` on a miss, which
   rolls everything back), then `UPDATE ONLY` the parent with the rendered text
   and `{ rev, upto }`. `UPDATE ONLY` never recreates a parent deleted
   meanwhile. Nothing changed: just release the lease.
5. **Delete** the rows the published doc now covers (`oplog_vv[peer] >= end`)
   and the refused ones, in chunks of 50, one `DELETE` per id so each fires the
   table's delete event and leaves every client's view. A crash between commit
   and delete only re-imports covered rows next time (idempotent), never loses
   ops.

Mirror rendering (`mirror.rs`, TS twin `mirror.ts`): `text` = the LoroText;
`prosemirror` = nodes holding text runs concatenate, nodes holding only nodes
join their children with `\n`, `hard_break`/`hardBreak` = `\n`.

Sweep (10 s after boot, then every 10 min): re-read log tables from their
markers, delete log rows whose parent no longer exists, delete stale presence,
and rebuild the tracker from `GROUP BY doc, field` (after a restart nothing was
observed; unknown age counts as written now).

Cluster safety: every SSP ingests every log row, so all track all documents;
the lease picks one folder and the `rev` CAS makes a lost race harmless (the
loser drops its cache entry). Exactly one node publishes each `rev`.

`/info` exposes `crdt`: `pending_fields`, `pending_rows`, `running`, `folds`,
`rows_folded`, `rows_rejected`, `leases_lost`, `errors`.

## Env knobs (SSP, `apps/ssp/src/lib.rs::crdt_fold_config`)

| Variable | Default |
|---|---|
| `SPKY_CRDT_FOLD_IDLE_SECS` | 5 |
| `SPKY_CRDT_FOLD_MAX_ROWS` | 200 |
| `SPKY_CRDT_FOLD_MAX_BYTES` | 262144 |
| `SPKY_CRDT_FOLD_MIN_GAP_SECS` | 10 |
| `SPKY_CRDT_FOLD_WORKERS` | 2 |
| `SPKY_CRDT_FOLD_TICK_MS` | 5000 |
| `SPKY_CRDT_PRESENCE_TTL_SECS` | 60 |
| `SPKY_CRDT_CACHE_SIZE` | 1024 |

Not configurable: lease 30 s, 2000 rows per fold, delete chunk 50, sweep every
600 s, stranded-row cutoff 24 h. `apps/ssp-cf` uses `FoldConfig::default()`
and reads none of the env vars. For e2e runs, lower idle/min-gap/tick in the
SSP's environment so the mirror lands sooner.

## Verification (state hash)

Every published fold carries `state_hash`: hex SHA-256 over the RFC 8785
canonical JSON of the document (`packages/ssp-node/src/crdt/hash.rs`, mirrored
by `packages/core/src/modules/crdt/hash.ts`). The mapping: keys in UTF-16
order, `JSON.stringify` strings, `Number#toString` numbers (`ryu-js` on the
Rust side; std's shortest digits round ties differently), integers as the
nearest `f64`, NaN and the infinities as `{"$f64": ...}`, bytes as
`{"$bytes": "<padded base64>"}`, a text as its string (marks are not hashed),
and root keys holding `""`, `{}`, `[]` or `0` left out (touching a root
creates it, a document built from updates alone lacks it). The TS side walks
containers into null-prototype objects because `toJSON()` drops a `__proto__`
key. Both sides are checked against
`packages/ssp-node/tests/fixtures/crdt_state_hash.json`.

A client notices a fold when log rows it was holding vanish, waits 1 s, and
reads `{rev, upto, state_hash}` from `_00_crdt_doc`:

- its oplog lacks ops in `upto.vv` (rows folded away before it saw them):
  merge the published snapshot in;
- otherwise fork at `upto.frontiers` (own ops the server never accepted fall
  outside that version) and compare hashes. A mismatch means the same ops gave
  a different state, which merging cannot undo; it is logged as a warning.

Skipped on insecure origins (no WebCrypto).

## Known trade-offs (accepted)

- **Every fold rewrites the parent row** (mirror + stamp), so `VALUE time::now()`
  fields such as `updated_at` move, and the parent re-syncs to every watcher.
- **A new peer per tab and identity** grows the snapshot's version vector
  slowly (one entry per peer that ever edited the field).
- **A non-opaque mirror means SSPs and the scheduler replica hold the full
  text.** `@crdt` + `@opaque` opts out (and gives up filtering on it).
- **The mirror trails live edits** by roughly the idle window, including for
  the author's own list previews.
- **Presence of a crashed tab** lingers until the next sweep (up to TTL + 10 min).
- **Impersonation sessions** cannot write log rows, presence or peer claims.

## Open follow-ups

- **Rebuild on a same-version mismatch.** Verification only logs it. A
  rebuild (published snapshot plus own unsent ops) would have to swap the
  `LoroDoc` under an editor that is bound to it.
- **Value maps and `__proto__`.** A map *value* set with `map.set(k, {...})`
  (not a container) still comes back from `loro-crdt` as a plain object, so a
  `__proto__` key inside it would hash differently in TS and Rust.
- **Optimistic local mirror.** The client renders nothing into the parent
  column before the fold (`mirror.ts` exists for it). Plan: write the rendered
  text local-only, re-apply it when server rows land, drop it once
  `__crdt.rev` catches up.
- **Rejected log rows.** A CREATE the database refuses goes to the failed-writes
  tray like any write; the session does not rewind `enqueued` or re-export
  those ops by itself.
- **Dart.** `packages/spooky_core` still throws `UnimplementedError` for
  `openCrdtField` / `closeCrdtField` (its stub keeps a stale `fallbackText`
  parameter).
- **ssp-cf panic quarantine.** The fold relies on `catch_unwind` to contain
  Loro decoder panics, but wasm32 builds abort on panic, so on the Cloudflare
  SSP a hostile blob that gets past the header check takes the isolate down,
  and the next fold reads the same row again. It needs its own quarantine
  (e.g. mark the row before importing, skip marked rows).
- **Fold env on ssp-cf.** Wire the `SPKY_CRDT_*` knobs there too.
