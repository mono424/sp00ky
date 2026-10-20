# S8: SurrealDB conformance suite (spike results)

Which SurrealDB features sp00ky depends on does a given endpoint support? This
spike answers that per feature, pass / fail / unsupported, so SurrealDB Earth
(or any other endpoint) can be judged on day one. It is a skeleton plus a
report, not a release feature: it lives outside the release workspace and its
lockfile.

- `src/main.rs`: the Rust runner. It talks to SurrealDB through `HttpSqlDb`
  over the `HttpClient` port, JSON-RPC on `/rpc` with `surreal-ns` /
  `surreal-db` headers, which is exactly the transport the Cloudflare SSP uses.
  It applies `apps/cli/src/meta_tables_remote.surql`,
  `functions_remote.surql`, `functions_remote_singlenode.surql` and
  `impersonation_remote.surql` verbatim (placeholders filled the way
  `spky migrate` fills them), copies the per-table events from
  `apps/cli/src/sp00ky.rs`, and builds scans with the production helpers
  (`bootstrap_page_query`, `range_target`, `boundary_query_every`,
  `load_table_meta`, `probe_schema`, impersonation `mint`). A small HTTP sink
  in the same process stands in for the SSP / scheduler, so every
  `http::post` / `http::get` from events and functions is observed on the wire
  (status, delay, body, headers).
- `live.ts`: the WebSocket half (LIVE SELECT ... FETCH out, KILL, record-user
  LIVE permissions), on the surrealdb JS SDK already in the repo. The runner
  starts it after its own probes and folds its rows into the same report.

Outcomes: **pass**; **fail** (feature present, misbehaves); **unsupported**
(parse error, unknown function, capability off); **known-bug** (a regression
probe reproduced a documented defect that sp00ky works around); **skip** (a
prerequisite probe did not pass). Exit code is 1 when any probe fails.

## How to run

Local SurrealDB, the flags `spky dev` uses (`apps/cli/src/dev.rs`):

```bash
docker run -d --name s8-surreal33 --user root -p 18333:8000 -e SURREAL_CAPS_ALLOW_EXPERIMENTAL=surrealism,files surrealdb/surrealdb:v3.3.4 start --bind 0.0.0.0:8000 --allow-all --user root --pass root surrealkv:/data/db
```

```bash
cd spikes/cloudflare/s8-db-conformance && cargo run --release -- --md results.md
```

Defaults: `--url http://127.0.0.1:18333 --ns conformance --user root --pass root`,
a fresh database `run_<unix>` that is removed at the end (`--keep` keeps it),
the sink on a random port reachable from docker as `host.docker.internal`.
`live.ts` needs node 22+ (`--experimental-strip-types`); `--no-live` skips it.
`--only <prefix>` runs a subset (probes that need users or the internal schema
then skip).

Any other endpoint, for example SurrealDB Earth on day one:

```bash
cargo run --release -- --url https://<endpoint> --ns <ns> --db <scratch_db> --token <bearer> --sink-bind 0.0.0.0:8787 --sink-url https://<public tunnel to :8787>
```

- `--token` sends `Authorization: Bearer`, the way the CF SSP authenticates to
  SurrealDB Cloud; `--user/--pass` sends Basic.
- The `http::post` probes need the database to reach the sink: expose it with
  any tunnel (for example `cloudflared tunnel --url http://localhost:8787`) and
  pass the public URL as `--sink-url`. Without it those probes fail with the
  connection error, which itself says whether the endpoint may make outbound
  calls.
- With credentials that cannot create a database, pass an existing empty
  scratch `--db`; it is not removed and keeps the probe tables.
- The probes write (schema, users, rows, buckets). Never point it at a shared
  or production database without the owner's go-ahead. It has only been run
  against the two local containers below.

## Results

Both runs on 2026-10-20 against local docker containers, `surrealkv` storage,
`--allow-all`, `SURREAL_CAPS_ALLOW_EXPERIMENTAL=surrealism,files`. JS SDK
`surrealdb@2.0.3` for the live rows.

- **surrealdb-3.3.4**: 82 probes, 80 pass, 2 known-bug, 0 fail, 0 unsupported.
- **surrealdb-3.1.5** (what staging runs, for comparison): same 82 probes, same
  80 pass and 2 known-bug.

| Group | Probe | 3.3.4 | 3.1.5 | Detail (3.3.4 run) |
|---|---|---|---|---|
| transport | `transport.version` | **pass** | **pass** | GET /version = surrealdb-3.3.4 |
| transport | `transport.httpsqldb_version` | **known-bug** | **known-bug** | sp00ky-side: HttpSqlDb::version() sends `RETURN surrealdb::version()`, which is not a SurrealQL function (query: Parse error: Invalid function/constant path --> [1:8] \| 1 \| RETURN surrealdb::version(); \| ^^...); fix by reading ... |
| transport | `transport.multi_statement` | **pass** | **pass** | one result per statement: [Number(1), String("two"), Null, Number(3)] |
| schema | `schema.internal_apply` | **pass** | **pass** | meta_tables_remote.surql + functions_remote*.surql + DEFINE PARAMs applied verbatim (108 statements) |
| schema | `schema.generated_events` | **pass** | **pass** | SCHEMAFULL table + generated _00_cf_item_mutation / _00_cf_item_delete events defined |
| access | `access.record_signup_signin` | **pass** | **pass** | signin result shape string; JWT alg "HS512"; claims ["AC", "DB", "ID", "NS", "exp", "iat", "iss", "jti", "nbf"]; INFO FOR DB renders: DEFINE ACCESS account ON DATABASE TYPE RECORD SIGNUP { RETURN (CREATE ONLY user SET username = $... |
| access | `access.session_vars` | **pass** | **pass** | over Bearer on /rpc: {"access":"account","auth":"user:1vz3pu4xsw449qrbgfi0","db":"run_1792525687","guard":true,"ns":"conformance","sid":"71167697-c0f1-4cf7-829e-e927204e573c","tok_ac":"account"} |
| access | `access.param_permissions_none` | **pass** | **pass** | root reads it; record user gets Err("query: You don't have permission to view the $sp00ky_secret parameter") |
| access | `access.row_permissions` | **pass** | **pass** | bob cannot read alice's draft: 0 rows; alice reads own draft: 1 rows; bob cannot create as alice: Ok(Array []); bob cannot update: "a"; bob reads once published: 1 row; collaborator update via $parent.id subquery: "collab" |
| access | `access.impersonation_external_jwt` | **pass** | **pass** | as impersonated: {"access":"_00_impersonate","as_access":"account","auth":"user:xwevl63jz26x0gicdgiv","current":{"access":"account","admin":"user:1vz3pu4xsw449qrbgfi0","expires_at":"2026-10-20T20:48:07.918839892Z","reason":"confor... |
| functions | `fn.session_guard` | **pass** | **pass** | session_live() = true; probe row = {"live":true} |
| functions | `fn.query_register` | **pass** | **pass** | missing keys []; params `{ access: 'account', auth: { id: 'user:1vz3pu4xsw449qrbgfi0' }, limit: 10 }`; clientId "a2aa45cb-9edb-4d91-acee-9c7963e68cae"; ownerHash = sha256(owner): true |
| functions | `fn.query_heartbeat` | **pass** | **pass** | 2 heartbeats over HTTP -> 2 subscriber entries (an HTTP request is its own session::id()): [{"id":"f668b70a-8064-4330-9466-f06b624b0c4b","seenAt":"2026-10-20T19:48:07.939617068Z"},{"id":"2244a0d2-2a92-4b60-b0e2-49f686401964","seen... |
| functions | `fn.query_leave_unsubscribe` | **pass** | **pass** | leave -> {"leaving":false,"remaining":2}; unsubscribe -> {"released":false,"remaining":2} |
| functions | `fn.spooky_info_http_get` | **pass** | **pass** | http::get inside a PERMISSIONS FULL function returned {"ok":true,"source":"conformance-sink"} |
| functions | `fn.throw_text` | **pass** | **pass** | client sees: query: An error occurred: There was a problem with authentication: this session has ended, sign in again |
| functions | `api.define_api` | **pass** | **pass** | GET /api/conformance/run_1792525687/spooky -> 200 {"ok":true,"source":"conformance-sink"} |
| builtins | `builtin.crypto_sha256` | **pass** | **pass** | RETURN crypto::sha256('abc') = "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad" |
| builtins | `builtin.crypto_argon2` | **pass** | **pass** | RETURN crypto::argon2::compare(crypto::argon2::generate('pw'), 'pw') = true |
| builtins | `builtin.object_extend` | **pass** | **pass** | RETURN object::extend({ a: 1 }, { b: 2 }) = {"a":1,"b":2} |
| builtins | `builtin.array_filter_closure` | **pass** | **pass** | RETURN array::filter([1, 2, 3], \|$x\| $x > 1) = [2,3] |
| builtins | `builtin.array_clump_map_closure` | **pass** | **pass** | RETURN array::clump([1, 2, 3, 4, 5], 2).map(\|$c\| $c[0]) = [1,3,5] |
| builtins | `builtin.array_append_flatten_len` | **pass** | **pass** | RETURN array::len(array::flatten([array::append([1], 2), [3]])) = 3 |
| builtins | `builtin.duration_arith` | **pass** | **pass** | RETURN <duration>'1h' + 30m = "1h30m" |
| builtins | `builtin.duration_from_column` | **pass** | **pass** | RETURN <duration>(string::concat(<string>1500, 'ms')) = "1s500ms" |
| builtins | `builtin.duration_secs` | **pass** | **pass** | RETURN duration::secs(90s) = 90 |
| builtins | `builtin.datetime_plus_duration` | **pass** | **pass** | RETURN <datetime>'2026-01-01T00:00:00Z' + 1h > <datetime>'2026-01-01T00:30:00Z' = true |
| builtins | `builtin.time_now_minus` | **pass** | **pass** | RETURN time::now() - 1m < time::now() = true |
| builtins | `builtin.type_record_two_args` | **pass** | **pass** | RETURN <string>type::record('_00_query', 'abc') = "_00_query:abc" |
| builtins | `builtin.type_record_one_arg` | **pass** | **pass** | RETURN record::tb(type::record('user:x')) = "user" |
| builtins | `builtin.record_exists` | **pass** | **pass** | RETURN record::exists(type::record('_00_query', 'nobody_here')) = false |
| builtins | `builtin.record_cast_string` | **pass** | **pass** | RETURN <string>user:x = "user:x" |
| builtins | `builtin.contains_notinside` | **pass** | **pass** | RETURN [1, 2] CONTAINS 2 AND 'x' NOTINSIDE ['y'] = true |
| builtins | `builtin.string_fns` | **pass** | **pass** | RETURN string::ends_with('a.jpg', '.jpg') AND string::len(string::trim(' abc ')) = 3 = true |
| builtins | `builtin.null_coalesce` | **pass** | **pass** | RETURN $cf_missing ?? 'fallback' = "fallback" |
| builtins | `builtin.or_fallback` | **pass** | **pass** | RETURN (NONE OR 'x') = "x" |
| builtins | `builtin.if_else_expression` | **pass** | **pass** | RETURN IF 1 > 2 { 'a' } ELSE IF 1 > 0 { 'b' } ELSE { 'c' } = "b" |
| builtins | `builtin.session_fns` | **pass** | **pass** | RETURN session::ns() != NONE AND session::db() != NONE = true |
| events | `event.in_txn_vars` | **pass** | **pass** | $event/$before/$after seen: ["CREATE:null->1", "DELETE:2->null", "UPDATE:1->2"] |
| events | `event.cancelled_txn_discards` | **pass** | **pass** | event side effects of a cancelled txn: [] |
| events | `event.throw_aborts_write` | **pass** | **pass** | error: query: Error while processing event cf_guard: An error occurred: conformance guard: write refused; row exists afterwards: false |
| events | `event.sp00ky_ingest` | **pass** | **pass** | ops ["CREATE", "UPDATE", "DELETE"], _00_rv [Number(1), Number(2), Null], bearer ok true, first payload {"hash":"","id":"cf_item:a","op":"CREATE","record":{"_00_rv":1,"at":"2026-01-01T00:00:00Z","id":"cf_item:a","n":1,"owner":"user... |
| events | `event.outer_let_visible` | **pass** | **pass** | _00_version after UPDATE with $sp00ky_target_version = 7 in the same request: 7 |
| http | `http.post_order` | **pass** | **pass** | arrival order ["cf_item:o1", "cf_item:o2", "cf_item:o3"] |
| http | `http.post_runs_in_txn` | **pass** | **pass** | row visible to another request while http::post is in flight: false; after the write returned: true (in-txn: the post runs before commit) |
| http | `http.non_2xx_aborts_write` | **pass** | **pass** | write refused: query: Error while processing event _00_cf_item_mutation: There was an error processing a remote HTTP request: 500 Internal Server Error; row exists false |
| http | `http.timeout_aborts_write` | **pass** | **pass** | aborted after 1015ms: query: Error while processing event cf_slow: There was an error processing a remote HTTP request: error sending request for url (http://host.docker.internal:63863/p/slow/hold/4000/ingest) |
| http | `http.unreachable_aborts_write` | **pass** | **pass** | write refused: query: Error while processing event _00_cf_item_mutation: There was an error processing a remote HTTP request: error sending request for url... |
| http | `http.get_parsed` | **pass** | **pass** | http::get body decoded as {"ok":true,"source":"conformance-sink"} |
| http | `regress.http_post_field_stripping` | **pass** | **pass** | all keys arrive: [Object {"added": String("invisible"), "existing": String("visible")}, Object {"added": String("extended"), "existing": String("visible")}] (docs/surrealdb-bugs/http-post-field-stripping.md not reproduced) |
| events | `event.async_syntax` | **pass** | **pass** | accepted: `DEFINE EVENT OVERWRITE cf_async ON TABLE cf_async ASYNC WHEN $event = 'CREATE' THEN BODY;`; INFO renders `DEFINE EVENT cf_async ON cf_async ASYNC RETRY 1 MAXDEPTH 3 WHEN $event = 'CREATE' THEN { SELECT * FROM http::post... |
| events | `event.async_post_commit` | **pass** | **pass** | write returned in 13ms (sink holds 1500ms); post arrived 14ms after the write started; row visible during the post: true |
| events | `event.async_non_2xx_no_abort` | **pass** | **pass** | write result ok=true; row exists true; posts received in 4s: 2 (retries visible as >1) |
| events | `event.async_rollback_never_fires` | **pass** | **pass** | posts for cancelled / thrown txns within 3s: 0 |
| events | `event.async_order` | **pass** | **pass** | 8 sequential commits arrive in commit order; rows of one txn arrived in statement order this run (["o0", "o1", "o2", "o3", "o4", "o5", "o6", "o7", "t1", "t2", "t3"]) |
| graph | `graph.relate_edges` | **pass** | **pass** | edge txn as the SSP writes it -> [{"auth_id":"user:1vz3pu4xsw449qrbgfi0","clientId":"conformance","in":"_00_query:cfq3","out":"cf_thread:t1","version":1},{"auth_id":"user:1vz3pu4xsw449qrbgfi0","clientId":"conformance","in":"_00_qu... |
| graph | `graph.traverse_membership` | **pass** | **pass** | alice $in->T: 2 rows, $ins->T: 2 flat rows; bob (edge perms): 0 rows |
| graph | `graph.edge_delete_forms` | **pass** | **pass** | subquery DELETE left 1 edge; flatten DELETE left 0; `DELETE $from->edge WHERE out = x` works (3.0.5 bug gone) |
| info | `info.for_db` | **pass** | **pass** | top-level keys ["accesses", "analyzers", "apis", "buckets", "configs", "functions", "models", "modules", "params", "sequences", "tables", "users"]; SchemaProbe::parse synced tables ["_00_app_release", "_00_user_feature", "cf_async... |
| info | `info.for_table` | **pass** | **pass** | load_table_meta: permission `published = true OR $access = 'account' AND author.id = $auth.id`, links {"author": "user"}, columns {"author", "published", "title"}, indexes ["idx_cf_thread_author(author)"] |
| info | `info.for_index` | **pass** | **pass** | INFO FOR INDEX -> {"building":{"compacting":false,"initial":0,"pending":0,"status":"ready","updated":0}} |
| scan | `scan.keyset_bootstrap` | **pass** | **pass** | bootstrap_page_query pages of 2 -> ["cf_scan:10", "cf_scan:a", "cf_scan:b", "cf_scan:c", "cf_scan:`d-e`", "cf_scan:zz"] |
| scan | `scan.record_range` | **pass** | **pass** | t:⟨b⟩..⟨e⟩ ["cf_rng:b", "cf_rng:c", "cf_rng:d"]; t:⟨b⟩>..⟨e⟩ ["cf_rng:c", "cf_rng:d"]; t:..⟨c⟩ ["cf_rng:a", "cf_rng:b"]; boundary_query ["cf_rng:a", "cf_rng:c", "cf_rng:e"] |
| scan | `scan.order_by_projection` | **pass** | **pass** | projected ORDER BY works; unprojected `SELECT VALUE <string>id ... ORDER BY n`: accepted |
| index | `index.unique` | **pass** | **pass** | duplicate refused: query: Database index `idx_record_id` already contains cf_x:dup, with record `_00_version:nuw9eunf2a1smto6dsln` |
| index | `regress.unique_index_rebuild_complete` | **pass** | **pass** | index lookups after OVERWRITE rebuild of 500 rows [1,1,1,1,1,1,1,1,1]; duplicate refused true; status {"building":{"compacting":false,"initial":500,"pending":0,"status":"ready","updated":0}} |
| index | `index.concurrently` | **pass** | **pass** | trailing CONCURRENTLY accepted (outbox index shape); status {"building":{"compacting":false,"initial":500,"pending":0,"status":"indexing"}} |
| txn | `regress.return_in_txn_commits` | **known-bug** | **known-bug** | RETURN ended the transaction and committed the CREATE before THROW (OK [Null, Array [Object {"id": String("cf_ret:a"), "n": Number(1)}], Number(1), Null]); see memory feedback_surreal_return_commits_txn |
| txn | `regress.select_star_subquery_param` | **pass** | **pass** | `*` kept with a subquery over a bound id: {"comments":[{"id":"cf_spc:c1","text":"first","thread":"cf_sp:hello"}],"id":"cf_sp:hello","title":"hi"} |
| changefeed | `cf.define` | **pass** | **pass** | DEFINE renders `DEFINE TABLE cf_feed TYPE ANY SCHEMALESS CHANGEFEED 1h INCLUDE ORIGINAL PERMISSIONS FULL`; ALTER renders `DEFINE TABLE cf_feed2 TYPE ANY SCHEMALESS CHANGEFEED 1h INCLUDE ORIGINAL PERMISSIONS FULL` |
| changefeed | `cf.show_changes` | **pass** | **pass** | 5 entries for our writes, versionstamps increasing true, vs>>16 ~ unix ms true, change kinds {"current", "define_table", "delete", "update"}; SINCE <datetime> form accepted |
| changefeed | `cf.cancel_excluded` | **pass** | **pass** | BEGIN..CANCEL never reaches SHOW CHANGES |
| changefeed | `cf.throw_excluded` | **pass** | **pass** | THROW txn in feed: false; event-THROW-refused write in feed: false |
| changefeed | `cf.since_inclusive` | **pass** | **pass** | SINCE 117474964236075008 starts at Some(117474964236075008) |
| bucket | `bucket.memory` | **pass** | **pass** | exists true; head {"file":"f\"cf_files:/hello.txt\"","size":5,"updated":"2026-10-20T19:48:19.577952681Z"}; get Ok("\"hello\"") |
| bucket | `bucket.permissions` | **pass** | **pass** | put on $auth bucket: ok true; delete excluded by $action: denied (query: You don't have permission to delete this file in the ...); put on WHERE false: denied (query: You don't have permission to put this file in the `cf...) |
| live | `live.ws_connect` | **pass** | **pass** | root session over ws://127.0.0.1:18333/rpc |
| live | `live.session_id_stable` | **pass** | **pass** | session::id() over one WebSocket: b83d96e6-8751-4377-bbc2-16ab9600f24b then b83d96e6-8751-4377-bbc2-16ab9600f24b |
| live | `live.select_fetch_out` | **pass** | **pass** | 1 notification(s); out = {"author":"user:1vz3pu4xsw449qrbgfi0","id":"cf_thread:t1","published":true,"title":"collab"} |
| live | `live.kill` | **pass** | **pass** | notifications after KILL: 0 |
| live | `live.record_user_cross_session` | **pass** | **pass** | own edge delivered with inline body {"author":"user:1vz3pu4xsw449qrbgfi0","id":"cf_thread:t1","published":true,"title":"collab"}; other user's edge filtered |
| live | `regress.ws_row_fresh_after_update` | **pass** | **pass** | bob read "collab" then "renamed-over-ws" after another session's UPDATE |

## Surprises

1. **`DEFINE EVENT ... ASYNC` already exists, on 3.1.5 as well as 3.3.4.**
   `ASYNC` goes after `ON TABLE`; INFO renders the default as
   `ASYNC RETRY 1 MAXDEPTH 3`. Measured: the write returns in ~13 ms while
   the sink holds the post for 1.5 s; the post starts ~15 ms after the write
   and the row is already visible to other sessions (post-commit); a 500 does
   not abort the write and is retried once (2 posts); cancelled and THROW-failed
   transactions never fire; back-to-back commits arrive in commit order, but
   the rows of ONE transaction arrive in a different order on most runs (both
   versions). That makes ASYNC a candidate third ingest transport next to
   `http` and `changefeed`: no scheduler latency inside user writes and no
   pre-commit race (`docs/surrealdb-bugs/ws-row-cache-stale-after-update.md`),
   at the price of at-most-twice delivery with no durable retry, so it would
   still need the changefeed or drift repair as backstop, and receivers must
   order by `_00_rv`, not arrival. Whether Earth implements ASYNC is the open
   question this probe answers on day one.
2. **sp00ky-side: an ingest timeout or outage reads as an application error to
   the client.** On both versions, the generated `SELECT * FROM http::post(...)
   TIMEOUT 10s` aborts the write on time (TIMEOUT 1s aborted at ~1015 ms) but
   the error is `There was an error processing a remote HTTP request: error
   sending request for url (...)`; a refused connection reads the same.
   `sp00ky.rs::ingest_post` says a timeout reads `"...exceeded the timeout"`,
   which `classifySyncError` (`packages/core/src/utils/error-classification.ts`)
   matches as `network`; this text matches none of its patterns, so a queued
   write that hits a slow or down scheduler is classified `application` and
   rolled back instead of retried. Not verified end to end in the client; worth
   a follow-up (add the remote-HTTP wording to the network patterns, a 5xx from
   `/ingest` reads `...remote HTTP request: 500 Internal Server Error`).
3. **sp00ky-side: `HttpSqlDb::version()` cannot work.** It sends
   `RETURN surrealdb::version()`, which is a parse error ("Invalid
   function/constant path") on 3.1.5 and 3.3.4. Nothing on the CF path calls it
   today (the VM SSP uses the SDK's `version()`), but anything that does gets an
   error. `GET /version` works on both and is what the suite uses.
4. **`RETURN` inside `BEGIN ... COMMIT` commits, on 3.1.5 and 3.3.4 too**, not
   only on the 3.2.4 host where it was first seen: `BEGIN; CREATE ...; RETURN 1;
   THROW 'probe'; COMMIT;` returns OK and the row persists.
5. **Several documented bugs no longer reproduce** in their standalone shape on
   3.1.5 / 3.3.4: `http::post` dropping body fields (literal and
   `object::extend`), `SELECT *, (subquery) FROM $param` dropping `*`,
   `DELETE $from->edge WHERE out = x` failing (3.0.5), cross-session LIVE on a
   permission-gated table (`_00_list_ref` with `auth_id = $auth.id`), and the
   stale WS read after another session's UPDATE. The workarounds can stay (some
   hosts still run 3.0.5), but the probes now tell when they become dead code.
6. **Over HTTP `/rpc` every request is its own `session::id()`.** Two
   `fn::query::heartbeat` calls over HTTP leave two subscriber entries (WS keeps
   one stable id, checked in `live.session_id_stable`). Harmless for WS
   clients and for the root SSP, but anything that heartbeats or registers as a
   user over HTTP piles up entries until the TTL prunes them, and its
   `clientId` never matches a later request.
7. Smaller facts the probes pinned: record-access tokens are HS512 with claims
   `AC DB ID NS exp iat iss jti nbf`, and HTTP-RPC `signin` returns a bare token
   string (not `{ token, refresh }`) for `NS/DB/AC` params; `INFO FOR DB` top
   level keys are `accesses analyzers apis buckets configs functions models
   modules params sequences tables users`; a key like `d-e` comes back as
   ``cf_scan:`d-e` `` (backticks, not angle brackets) and still round-trips
   through `record_id_literal` in keyset paging; `SELECT VALUE <string>id ...
   ORDER BY n` (unprojected) is accepted on both, so the "ORDER BY idiom must be
   projected" rule is not hit in this shape; a `CONCURRENTLY` index reports
   `status: indexing` right after DEFINE; `SHOW CHANGES ... SINCE` accepts a
   datetime as well as a versionstamp; DEFINE API answers on
   `/api/<ns>/<db>/<path>`; a `PERMISSIONS NONE` param read by a record user
   errors ("You don't have permission to view the $sp00ky_secret parameter")
   rather than returning NONE.
8. Ops note: the 3.3.4 image cannot open `surrealkv:/data/...` as its default
   user without a mounted volume ("Permission denied (os error 13)"); `--user
   root` or a volume fixes it.

## Not covered yet

- The shared staging database from the S8 row of the plan: not run, it needs
  the owner's go-ahead (the probes write).
- SurrealDB Earth itself (no endpoint or docs yet). Expected gaps to watch:
  outbound `http::post` from events at all, `ASYNC`, `DEFINE API`, buckets,
  CHANGEFEED, LIVE over WebSocket.
- Throughput and limits (`SHOW CHANGES` LIMIT clamp, big transactions,
  `http::post` concurrency), cluster behaviour, RocksDB/TiKV storage.
- CI wiring and folding the runner into `spky doctor db --url ...` once the
  probe list settles; this crate depends on `ssp-node`, which the CLI does not
  link today.
