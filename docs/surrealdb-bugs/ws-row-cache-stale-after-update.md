# Stale row content after a cross-connection UPDATE in our app — RESOLVED

**Observed on:** SurrealDB server `v3.0.5` and `v3.1.0-beta.3`, JS SDK `surrealdb@2.0.3`.

## Update 2026-05-22: RESOLVED via deferred SSP ingest

Fixed in `apps/ssp/src/lib.rs::ingest_handler` by ACKing the schema
event's `http::post` with `200` immediately, then deferring
`update_all_edges` + `persist_view_metrics` into a `tokio::spawn`ed
task. The deferred task waits for `_00_version.version >=
expected_version` on the SSP's own connection before issuing the
`UPDATE _00_list_ref_user_<bob>` — because the `_00_version` bump is
inside alice's parent transaction, it only becomes visible to the
SSP's separate connection AFTER alice's parent commits. Bob's LIVE on
list_ref now fires only once alice's row content is observable
downstream, so his `syncRecords` fetches fresh data.

E2E impact:
- `z-multi-client-sync.spec.ts` "title edits propagate from A to B in
  real-time" — passes (was `test.fail()`).
- `z-multi-user-sync.spec.ts` "title rename on a published thread
  propagates between users" — passes (was `test.fail()`).
- `z-multi-user-sync.spec.ts` "alice publishing…" and "unpublishing
  …" already passed via the #47 evaluate_key fix and continue to.

(Superseded for CRDT: since CRDT v2 edits and cursors are rows of generated
`<table>__crdt_log` / `<table>__crdt_presence` tables that sync like any
other table, and `CrdtManager.applyRow` is gone; see `docs/crdt-v2.md`. The
paragraph below is kept as history.)

For CRDT body / cursor sync over the same race, `sp00ky.ts` now
subscribes the CrdtManager to `syncEngine.events`'s
`SYNC_REMOTE_DATA_INGESTED`, forwarding each ingested parent row
into `CrdtManager.applyRow`. Same-user clients still receive these
rows via the existing per-table `LIVE SELECT *`; this hook is the
cross-user fallback for when permission-LIVE is filtered out on the
parent table. The cross-user CRDT body / cursor e2e tests
(`z-multi-user-sync.spec.ts` lines 214 / 282) still `test.fail()`
because the test premise itself is wrong: the thread schema's
`FOR update` permission only grants writes to the author or members
of `collaborates_on`, so bob's editor renders read-only and the
`[contenteditable="true"]` selector doesn't match. Re-writing those
tests to either invite bob via `collaborates_on` or assert on the
read-only rendering is a separate task.

## Update 2026-05-19 (PM): narrowed root cause to a schema-event transaction race

Adding `[ssp:syncRecords-compare]`, `[ssp:poll-fetch]`, and
`[ssp:localArray-set:*]` instrumentation to the in-app sync path
revealed the actual sequence in test 17 (`title rename` cross-user):

1. Alice does `db.update('thread', id, { title: 'Renamed' })`.
2. Alice's up-queue debounces 200ms then issues `UPDATE thread:<id>
   MERGE { title: 'Renamed' }` upstream.
3. The schema's `DEFINE EVENT OVERWRITE _00_thread_mutation` fires
   inside alice's transaction. It bumps `_00_version` and posts the
   payload to the SSP's `/ingest` endpoint over `http::post(...)`.
4. The SSP processes the change (DBSP step + view-delta computation)
   and issues `UPDATE _00_list_ref_user_<bob> SET version = 3 …` —
   in a SEPARATE transaction over its own root-auth connection.
5. The SSP's list_ref UPDATE commits BEFORE alice's parent
   transaction commits.
6. Bob's WS LIVE on `_00_list_ref_user_<bob>` delivers the v3
   notification immediately.
7. `handleRemoteListRefChange` runs `syncEngine.syncRecords` for the
   bumped row.
8. Bob's `SELECT * FROM thread:<id>` (over both his WS record-user
   session AND the parallel raw-HTTP root-auth diagnostic) returns
   the PRE-rename content. Upstream genuinely has not committed
   alice's UPDATE yet.

Concrete trace excerpt:
```
[test]  t=20946 alice.fill(renamedTitle)
[B:log] [ssp:syncRecords-compare] t=21503
        ids=["thread:32a4…"] sdk=["Cross-User Title Base"]
        raw=["Cross-User Title Base"]
[B:log] [ssp:localArray-set:processStreamUpdate] t=21610
        qh=63d2da la=[["thread:32a4…",3]]
[test]  t=22018 waitForThreadInUpstream returned — upstream HAS renamed
```

Bob's fetch ran at t=21503 and got Base on both the SDK and root-auth
paths. Upstream did not have Renamed until t=22018, 515 ms later.
By then bob had already saved Base + `_00_rv=3` into local DB and
pinned `localArray` to v3. The next list_ref poll sees
`local==remote==3` and never re-fetches the row — bob stays at Base
forever.

So the original "WS per-token row cache" diagnosis is wrong: the SDK
is doing the right thing, and the row is GENUINELY stale upstream
when the LIVE event arrives. The race is:

- SurrealDB executes `DEFINE EVENT` bodies inside the parent
  transaction. The `http::post` runs synchronously, but the SSP
  it calls back into runs the resulting list_ref `UPDATE` in a
  separate transaction over its own connection.
- The SSP's transaction can commit before alice's parent commits.
  When bob's LIVE on list_ref fires, alice's row write is still
  in-flight from the perspective of any other reader.

## Candidate fixes (none implemented yet)

- **SSP-side**: include the row's content (or at least the post-event
  `_00_rv` carried in the ingest payload's `$plain_after`) on the
  list_ref UPDATE statement itself, so bob reads `version` AND
  freshness-validated `_00_rv` from a single record bob is allowed
  to read directly. Then `syncRecords` can verify the fetched row's
  `_00_rv` ≥ the list_ref version and retry on mismatch.
- **Schema-side**: defer the `http::post` until after the parent
  transaction commits (e.g. queue the event into a side table and
  let a different worker fire it). Adds latency but removes the race.
- **Client-side mitigation, content-comparison (TRIED, failed)**: skip
  `cache.saveBatch` when the fetched row's content equals the local
  row's content. Doesn't work because (a) `created_at` is defined with
  `VALUE time::now()` and rewritten on every UPSERT MERGE, and (b) the
  race compounds across UPDATEs — bob's local row already has
  `published=false` (a previous race's stale fetch), so the new fetch
  with `published=true, title=Base` differs from local and gets
  saved, propagating the new stale title.
- **Client-side, time-delay retry**: add a 500-1000 ms delay before
  `syncRecords` runs on a LIVE notification, OR re-fetch once more
  after a delay. Simple but rough; also has to bypass the
  `cache.lookup` skip on the retry.

## Original analysis (2026-05-19 AM): standalone reproduction returns fresh data

The minimal Node.js reproduction at [`repro/ws-row-cache-stale.mjs`](./repro/ws-row-cache-stale.mjs)
exercises every variable I previously suspected, and **all 14 variants
return fresh data**:

```
✓ HTTP record-user, prior fetch primed
✓ HTTP record-user, no prior fetch
✓ HTTP record-user, prior fetch + re-signin
✓ HTTP record-user, fresh connection per fetch
✓ HTTP SDK root-auth, prior fetch primed
✓ Raw HTTP /sql with bob bearer, prior prime
✓ Raw HTTP /sql, fresh signin per fetch
✓ Raw HTTP /sql with bob bearer, NO prior fetch
✓ WS record-user, prior fetch primed
✓ WS record-user, no prior fetch
✓ WS record-user, LIVE + prior fetch primed
✓ WS record-user, BOUND $idsToFetch (in-app shape)
✓ WS record-user, bound + LIVE on _00_list_ref_user_…
✓ WS SDK root-auth, prior fetch primed
```

That includes the exact in-app fetch shape — `SELECT * FROM $idsToFetch`
with `idsToFetch` bound as an array of `RecordId` — over a WS-authenticated
record-user session that had previously primed the row. **No staleness.**

So the original "SurrealDB caches row state per-token" diagnosis is wrong.
SurrealDB and the JS SDK 2.0.3 both return fresh data for this shape.

The staleness is real in the application's e2e tests (13, 17), but its
cause must be somewhere in the application's data path that the standalone
repro doesn't exercise. Candidates still on the table:

- The SurrealDB client's CBOR codec serializes a `RecordId` differently when
  it has been round-tripped through the in-browser SSP's WASM (vs a fresh
  Node-side `new RecordId(...)`). A "different but equal" id might miss a
  server-side row index and return a stale duplicate, or a cached prior
  response object.
- The WS connection in the browser has been alive for ~10s longer and has
  done many other queries (user fetches, comment fetches, registrations).
  Some specific prior operation on the same WS session may install a
  poisoned cache entry that a single-shot Node-side script never primes.
- `RemoteDatabaseService.query` serializes queries through a `queryQueue`
  promise chain; combined with the SDK's own request pipeline a different
  request may be returning stale data tagged with our request id.
- The browser-bundled `surrealdb` (Vite/esbuild output) differs from the
  Node-side build of the same npm version in a way that affects request
  binding or response decoding.

Next step: add targeted in-app instrumentation that runs a parallel raw
HTTP fetch alongside the failing SDK fetch on the same page and compares
the two results. That will tell us whether the staleness is in the SDK
response or in something downstream.

The original symptom report below still stands as the visible behavior in
the application; only the proposed root cause changes.

---

## Symptom

Two record-authenticated sessions, A and B. A `UPDATE`s a column on a row that B can read per the schema's `PERMISSIONS FOR select`. Upstream is committed — `POST /sql` with `Authorization: Basic root:root` returns the new value. B re-issues `SELECT * FROM thread:<id>` over its existing SurrealDB JS SDK session and continues to receive the **pre-UPDATE** value indefinitely.

The staleness is **not** transport-level:

- Same problem over `ws://`.
- Same problem over `http://` reached via the SDK's HttpEngine (a second `Surreal` instance, signed in independently with the same record-level access).
- A direct raw `fetch()` to `/sql` with a record-user bearer token (bypassing the SDK entirely) — same.
- A `curl` with `Authorization: Basic root:root` — returns the **fresh** row.

It tracks the **permission-evaluated, record-level auth context**, not the connection. That points at a server-side per-(token, row) cache or per-permission-evaluation snapshot, not anything the JS SDK is doing wrong.

LIVE notifications on the parent table also miss the UPDATE in many cases — see [`live-permission-cross-session-drop.md`](./live-permission-cross-session-drop.md) — but that's mitigated by the SSP-driven `_00_list_ref_user_<id>` poll. What's broken here is the **read after the notification arrives**: the diff says "version bumped", the client fetches the row, the server hands back the old content.

## Reproduction

```ts
const wsA = new Surreal();
const wsB = new Surreal();
await wsA.connect('ws://localhost:8666/rpc');
await wsB.connect('ws://localhost:8666/rpc');
await wsA.use({ namespace: 'main', database: 'example' });
await wsB.use({ namespace: 'main', database: 'example' });
await wsA.signin({ access: 'user_access', variables: { id: 'user:alice', password: 'p' } });
await wsB.signin({ access: 'user_access', variables: { id: 'user:bob',   password: 'p' } });

await wsB.query('SELECT * FROM thread:1'); // → { title: 'original' }   primes the cache on B
await wsA.query('UPDATE thread:1 SET title = "renamed"');

// Upstream definitely has the new title (curl-as-root verifies).
// But over B's existing session:
await wsB.query('SELECT * FROM thread:1'); // → { title: 'original' }   ❌
```

A brand-new `Surreal` instance signed in fresh as `user:bob` reads the new title correctly. So the staleness is bound to **that authenticated session's history of permission-evaluated reads** on the row, not the connection per se.

## What didn't fix it (negative results)

Each of these was tried in this codebase and dropped:

- Inline `time::now() AS _refresh` cache-bust column.
- Per-query nonce in the WHERE clause.
- Full-table `SELECT id, title FROM thread` instead of a parameter-bound id.
- Second `Surreal` instance over `http://` authenticated via `client.authenticate(token)`.
- Second `Surreal` instance over `http://` doing its **own** `signin(params)` alongside the WS client.
- Raw `fetch()` POST to `/sql` with the bearer token + inlined `LET` vars.

The HTTP / raw-fetch approaches did not return fresh data when authenticated as a record-level user. They DO return fresh data with `Basic root:root`. So this is not the transport, the bind format, or the SDK's query-result cache.

## Open

We don't have a clean client-side workaround. Likely paths forward:

- Wait for a SurrealDB-side fix that invalidates the per-token row cache on UPDATEs visible through the permission rule.
- Have the SSP fan-out include the new row content inline in the `_00_list_ref_user_<id>` UPDATE statement (`SET version = …, content = $row_data`). The client reads the content directly and bypasses the upstream re-fetch entirely. Larger change; out of scope for this round.
- Force a fresh signin on every receiving session after every observed list_ref UPDATE. Expensive and likely racy.

## Affected tests

`example/e2e/tests/z-multi-client-sync.spec.ts` — `'title edits propagate from A to B in real-time'` (test 13) — `test.fail()`.
`example/e2e/tests/z-multi-user-sync.spec.ts` — `'title rename on a published thread propagates between users'` (test 17) — `test.fail()`.
