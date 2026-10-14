# CRDT latency bench (threads example)

Measures keystroke -> other-device latency for `@crdt` fields, cold open of a
thread, and per-character lag while typing, with a stage breakdown from
temporary probes in core.

1. Optional, for the stage breakdown: `git apply example/e2e/bench/crdt-latency/probes.patch`
   and copy `bench-trace.ts` to `packages/core/src/utils/` (probes are no-ops
   unless the page sets `window.__btrace`). Never commit them; the end-to-end
   numbers need neither.
2. Local stack: swap `example/sp00ky.yml` `surrealdb:` to
   `{ namespace: main, database: example }`, then `spky dev -y --clean-db`.
3. `node crdt-bench.mjs` (env: `TRIALS`, `BODY_KB`, `BASE`).
   Hosted-like RTT: `node delay-proxy.mjs 18666 8666 160`, a second app with
   `VITE_DB_ENDPOINT=ws://localhost:18666/rpc vite --port 3007`, then
   `BASE=http://localhost:3007 node crdt-bench.mjs`.
   Experiments: `LIVE_INLINE=1`, `CRDT_DEBOUNCE=<ms>`, `CRDT_MAXWAIT=<ms>`.
4. `node analyze.mjs out/run-*.json` prints the per-edit stage table.

2026-10-14, threads example, author -> collaborator (medians; worst in brackets):

| | edit shows on the other device | lag behind typing | first open |
|---|---|---|---|
| local, before | 536 ms | 1218 ms (2241) | 348 ms |
| local, after | 225 ms | 206 ms (363) | 341 ms |
| 320 ms RTT, before | 1522 ms | 2081 ms (3207) | 2006 ms |
| 320 ms RTT, after | 554 ms | 776 ms (1092) | 1214 ms |
