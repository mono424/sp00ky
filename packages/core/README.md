# @spooky-sync/core

The sync engine behind `Sp00kyClient`. Everything that decides something is a
pure function or a generator saga that yields effect descriptions; the only
effectful code is the adapters under `services/` and the interpreter in
`kernel/interpreter.ts`. State is one immutable `ClientState` value.

## Layout

| Directory | Holds |
| --- | --- |
| `kernel/` | `Effect` union, the saga driver, lanes, constants, the interpreter |
| `state/` | `ClientState`, the query lifecycle machine, reducers, selectors |
| `query/` | keys, membership rules, render set, SQL builders, the query sagas (register, membership, fetch, materialize, lifecycle) |
| `mutation/` | outbox rows, write / push / rollback / tray sagas |
| `sync/` | poll, LIVE, connection health, shared-tabs relay sagas |
| `boot/` | boot, auth flip, bucket switch, preload sagas |
| `client/` | runtime (lanes, timers, subscriptions), router, adapters, the public facade |
| `services/` | adapters: local store engines, remote socket, SSP wasm, tabs broker, blobs |

## Query lifecycle

A query is `cold` (never resolved on this device: rows come from a local
predicate scan, bindings show a loader), `cached` (a durable `_00_view` row
seeded its membership: rows paint from the store at once), `live` (a server
membership set was accepted this session) or `view-lost` (the server dropped
the view; rows are kept while it re-registers).

1. `registerLocal` computes the keys, reads `_00_view`, builds the local SSP
   view and publishes the entry. The runtime materializes it on the next tick.
2. `registerRemote` registers on the server and reads edges + meta in one
   request; `applyMembership` decides (non-empty always; empty only when the
   server vouches for it) and writes `_00_view`.
3. `fetchRows` pulls every body any query is missing, once, in parallel
   chunks, and records versions. `settled` is a selector over that state.
4. Later changes arrive as LIVE edge events (which only mark membership dirty)
   or the poll; both go through the same batched re-read.

Rendered rows are always `(membership ∪ (writes ∩ localView)) − deletes`
where `writes`/`deletes` come from the outbox mirror in state, so unsynced
local mutations show and a rejected one disappears the moment it is rolled
back.

## Mutations

`write` runs one local transaction (row + `_00_pending_mutations` entry with
`beforeRecord`), feeds the circuit and starts `drain`. The drain pushes up to
50 statements per request and judges each on its own: accepted rows are
acked (kept in the overlay until membership names them), rejected rows are
rolled back and moved to `_00_failed_mutations` in the same transaction that
deletes the pending row, a transport cut leaves the tail queued behind a
backoff. The tray is exposed as `failedMutationCount`,
`subscribeToFailedMutations`, `listFailedMutations`, `retryFailedMutation`
and `discardFailedMutation`.

## Testing

Sagas run against canned effect results with `src/testing/run-pure.ts`; the
interpreter and facade run against `src/testing/adapters.ts`. `pnpm test:run`
runs the suite, `pnpm test:coverage` enforces the thresholds in
`vitest.config.ts` (the saga core is held at 100%).
