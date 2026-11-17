# SurrealDB bugs we hit

A folder of known SurrealDB issues (server and JS SDK) that we've worked around in this codebase. One file per bug. Each file has:

1. **Symptom** — what doesn't work, observed externally.
2. **Reproduction** — the smallest possible repro (SurrealQL or a minimal JS snippet) plus the version we observed it on.
3. **Workaround** — what we changed in the code so the rest of the system works despite the bug. Includes the files we touched.

When an upstream fix lands and we bump SurrealDB past the affected version, the workaround note tells us what to revert.

## Affected SurrealDB versions

We currently target `surrealdb/surrealdb:v3.1.0-beta.3` for the server and `surrealdb@2.0.3` for the JS SDK. Most of these bugs were originally found on `v3.0.0` / `v3.0.5`; some still reproduce on `v3.1.0-beta.3`.

## Index

- [http-post-field-stripping.md](./http-post-field-stripping.md) — `http::post` silently drops fields added to the body object.
- [live-permission-cross-session-drop.md](./live-permission-cross-session-drop.md) — LIVE notifications don't fire on permission-gated tables when the inserter and the subscriber are different sessions.
- [select-from-param-subquery-drops-star.md](./select-from-param-subquery-drops-star.md) — `SELECT *, (subquery) FROM $param` drops the `*` fields when `$param` is a bound RecordId.
- [ws-row-cache-stale-after-update.md](./ws-row-cache-stale-after-update.md) — over the WebSocket SDK, `SELECT * FROM record:id` returns a stale row content after another connection UPDATEd it.
- [live-deref-permission-rules.md](./live-deref-permission-rules.md) — LIVE feeds misfire when the permission rule dereferences a related record (upstream issues #3602 and #4026).
- [unique-index-partial-after-overwrite-rebuild.md](./unique-index-partial-after-overwrite-rebuild.md) — a `DEFINE INDEX OVERWRITE` rebuild cut off by a request timeout leaves the unique index partially populated; `_00_version` lookups then stamp `_00_rv` NONE and peers never refetch.
- [v4-event-param-index-scan.md](./v4-event-param-index-scan.md): on 4.0.1 a `WHERE record_id = $after.id` lookup inside an event body scans the table instead of using its index; one synced write cost 14 s on a 3M-row `_00_version`. Fixed by keying ledger rows by their record.
- [v4-block-param-index-scan.md](./v4-block-param-index-scan.md): on 4.0.1 statements inside `{ }` block expressions, `FOR` loops and list matches (`INSIDE $ids`) scan for parameters 3.3.5 served from an index; the workflow prune cost 1.2 s with nothing to prune, a loop over 500 ids 167 s. sp00ky sends top-level statements and one equality per value.
- [v4-record-user-event-scan.md](./v4-record-user-event-scan.md): on 4.0.1 an event fired by a record user's write plans its lookups from the user's session: any select rule other than `FULL` on the table read makes it a scan (a WhitePawn client's game edit: 19 s, 18 ms as root). sp00ky's ledger lookup uses a correlated form; select rules are written `FULL`, not `WHERE true`.
