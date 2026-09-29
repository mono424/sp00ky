# Web Push in sp00ky - design and implementation contract

Status: being implemented on branch `feat/web-push` (worktree
`/Users/khadim/dev/spooky-push`). This file is the contract every part builds
against. Keep it true when a part has to deviate.

## Goal

Web Push as a platform feature: an app declares `push:` rules in `sp00ky.yml`
("when a row of table X looks like Y, push to the users in field Z"), browsers
subscribe through `db.webPush`, and the platform delivers. No app backend, no
push library, no key management. Powerful defaults, few opinions:

- content pushes (templated title/body/url/actions from the row, plus extra
  context from root SurrealQL) AND content-free nudges (the app's service
  worker renders from its own live data) - the rule decides;
- recipients from row fields or from a root SurrealQL query (fan-out);
- conditions with operators, `once` per state transition, max age, per
  (rule, recipient, topic) throttling with a guaranteed trailing push, topics
  that collapse at the push service;
- direct pushes (`_00_push_message`) from any backend / DEFINE EVENT / the
  CLI, scheduled pushes (`send_at`), self-pushes from clients (reminders, test);
- per-device rule filters, device list, test push;
- a wasm-free live subscriber + service worker bridge for the rendering side.

## What sp00ky can and cannot know (why rules are row rules)

- There is no session / presence table. LIVE queries live inside SurrealDB,
  the SSP is HTTP-only, `_00_query.lastActiveAt + ttl` is ~5 min resolution,
  closed tabs beacon-delete their views. So "push only if the user is not
  connected" is impossible server-side. A matching row pushes to every enabled
  subscription of the recipient; the SERVICE WORKER decides whether to show
  (suppress when a visible client exists).
- Singlenode (the default mode) has no scheduler. The engine is a portable
  crate hosted by the scheduler (cluster) and by the standalone SSP
  (singlenode), like `schedule-core`.
- Ingest is per row. No transaction object, no before-image on UPDATE (the
  changefeed's `INCLUDE ORIGINAL` patch is discarded), DELETE carries the
  before-image read from the replica. `@opaque`/`@crdt`/`@nosync` fields are
  stripped, `@nosync` tables never arrive.
- The same row can be ingested more than once (changefeed look-back, retry
  re-poll, 1 s rewind on restart, WAL replay). Drift repair re-emits old rows
  through `ingest_event(.., 0)`: those must NEVER push (host passes
  `Origin::Repair`).

## Pieces

| Piece | Where |
| --- | --- |
| Config types + validation (`push:` block), wire payload | `packages/push-core/src/config.rs` (DONE, the contract) |
| Templates | `packages/push-core/src/template.rs` |
| Rule matching, recipients, rendering | `packages/push-core/src/rules.rs` (or split) |
| VAPID keys (derivation, JWT) | `packages/push-core/src/vapid.rs` |
| RFC 8291 encryption | `packages/push-core/src/ece.rs` |
| Engine (observe, tick, delivery, limits, bookkeeping) | `packages/push-core/src/engine.rs` |
| DDL + `fn::push::*` | `apps/cli/src/push_tables.surql` (DONE, the contract; fixable) |
| CLI: `push:` in Sp00kyConfig, lint, migrate sync, CHANGEFEED list, http events, `spky push` | `apps/cli`, `packages/maintenance` |
| Scheduler host | `apps/scheduler` |
| Standalone SSP host | `packages/ssp-node`, `apps/ssp` |
| Client: `db.webPush` | `packages/core/src/modules/web-push`, `client-solid(2)` |
| SW side: `@spooky-sync/core/pure`, `/live`, `/sw` | `packages/core` (subpath exports) |
| Docs | `apps/landing-page/src/pages/docs/...`, JSON schema |

Why subpath exports and not a new npm package: OIDC trusted publishing cannot
create a package (see `.github/workflows/npm-publish.yml`), so a new
`@spooky-sync/live` would 404 on its first canary. Separate tsdown entries plus
a bundle check keep wasm out of the SW bundle just as well.

## Storage (see push_tables.surql)

- `_00_push_config:default { spec_json, hash, updated_at }`: the normalized
  `push:` block as JSON (serde of `PushConfig`), written by the CLI at every
  migrate/deploy. Missing row = `PushConfig::default()` (enabled, no rules:
  direct messages still work). A removed `push:` block writes the default.
- `_00_push_subscription`: id = sha256(auth_id + "|" + endpoint).
  `fn::push::subscribe` refuses endpoints that are not `https://` (every
  browser push service is); the engine refuses them too (see Delivery).
- `_00_push_message`: direct / scheduled pushes. `claimed_at` is stamped by
  the claim (status -> `sending`) so the sweep can tell a host that died
  mid-send from one that is sending right now.
- Params published BY THE HOST at boot (root): `$sp00ky_vapid_public_key`,
  `$sp00ky_vapid_kid`, both `PERMISSIONS FULL`. Inline the literal (base64url /
  hex only), do not bind.

Ingest visibility:

- rule tables are ordinary synced tables: they already reach the hosts.
- `_00_push_message` must reach the hosts: add it to
  `maintenance::changefeed::CHANGEFEED_META_TABLES` (changefeed transport) and
  generate an ingest-notify DEFINE EVENT for it under the http transport (same
  shape as `_00_app_release`'s in `apps/cli/src/sp00ky.rs`). Hosts intercept it
  BEFORE WAL/replica/circuit: it is never synced, never fanned out to SSPs.
- `_00_push_config` and `_00_push_subscription` are NOT ingested; the engine
  polls/reads them.

As built (CLI):

- changefeed transport: `_00_push_message` is in both `CHANGEFEED_META_TABLES`
  lists (maintenance and `apps/cli/src/schema_builder.rs`), and
  `schema_builder::push_tables_sql` injects `CHANGEFEED <retention> INCLUDE
  ORIGINAL` into its `DEFINE TABLE OVERWRITE` (in the DEFINE, not an ALTER,
  because the file is re-applied as OVERWRITE). The feed delivers every change
  to the table: the CREATE as `{"update": row}`, the engine's own claim /
  finish UPDATEs, and the 7-day prune DELETEs (record `{}`: excluded tables
  have no before-image). `table_excluded_from_sync` still holds, so drift
  repair never re-emits these rows.
- http transport: `DEFINE EVENT _00_push_message_ingest ... WHEN $event =
  "CREATE"` posts `{ table: '_00_push_message', op: 'CREATE', id, record, hash:
  "" }` to `$sp00ky_endpoint + '/ingest'` inside the usual `SELECT * FROM
  http::post(..) TIMEOUT 10s`. `record` is every DDL field of the row, record
  ids and datetimes as strings, unset optional fields NONE (absent/null). No
  UPDATE/DELETE posts, no `_00_version` row. Under the changefeed transport and
  in surrealism mode the event is REMOVEd (a leftover http event after a switch
  would deliver twice; surrealism has no host, and `mod::dbsp::ingest` would put
  the row in a circuit).
- The http event fires INSIDE the creating transaction, before commit: a host
  that claims the row immediately (`CLAIM_MESSAGE`) sees nothing and the
  message waits for the stale-pending sweep (30 s). Hosts or the engine should
  retry the claim briefly (or defer `observe` for this table by a few hundred
  ms) when the record says `pending` and the claim finds no row.
- `spky push` already existed (bare: schema push for free/Cloudflare
  projects) and keeps that meaning without a subcommand; the Web Push tool is
  `spky push status | send | devices | sync`. `send` takes `--link` for the
  notification URL because `--url` selects the database (shared
  `ConnectionArgs`).
- `push: ./push.yml` links the whole block from a file (whole-section only).

## Keys

- `SPKY_VAPID_PRIVATE_KEY` (base64url raw 32-byte P-256 scalar, the format
  every web-push library prints) wins.
- Else derived from `SPKY_AUTH_SECRET`: HKDF-SHA256(ikm = secret,
  salt = "sp00ky-web-push", info = "vapid-p256-v1") -> 32 bytes; if not a valid
  scalar (0 or >= n), retry with info "vapid-p256-v1/1", "/2", ...
- Neither (or empty secret): push is off, one warning at boot.
- public key = uncompressed SEC1 point (65 bytes), base64url no padding.
- kid = hex of the first 8 bytes of sha256(public key bytes).
- VAPID JWT: ES256, header `{"typ":"JWT","alg":"ES256"}`, claims
  `{aud: <scheme://host[:port] of endpoint>, exp: now + 12h, sub: subject}`;
  header `Authorization: vapid t=<jwt>, k=<public key b64url>`. Cache per
  audience, refresh after 11 h.
- Rotation: a different secret = a different kid. Rows with another kid are
  skipped (`disabled_reason = "vapid_rotated"` is NOT written; they are just
  skipped) and the client's `sync()` re-subscribes on next boot.

## Encryption

RFC 8291 (aes128gcm, RFC 8188 single record): ephemeral P-256 key per message,
16-byte random salt, ECDH with the subscription's `p256dh`, the RFC's two-stage
HKDF with the subscription's 16-byte `auth`, record size 4096, padding
delimiter 0x02. Headers: `Content-Encoding: aes128gcm`,
`Content-Type: application/octet-stream`, `TTL`, `Urgency`, optional `Topic`.
Must reproduce RFC 8291 Appendix A byte for byte given its fixed inputs.
Plaintext cap: 3993 bytes. Over the cap: drop `data`, then truncate
`notification.body`, then drop `notification` (becomes a nudge); log it.

`Topic` header: base64url alphabet only, max 32 chars:
base64url(sha256(topic))[..32] (the readable topic still travels in the payload).

## Engine contract (push-core)

```rust
pub trait PushHttp: MaybeSendSync {
    /// POST `body` to `url` with `headers`. Ok((status, body_prefix)) for any
    /// HTTP answer, Err for transport failures (retried).
    async fn post(&self, url: &str, headers: Vec<(String, String)>, body: Vec<u8>)
        -> Result<(u16, String), String>;
}

pub enum Origin { Live, Replay, Repair }   // Repair never pushes

pub struct ObservedChange {
    pub table: String,
    pub op: Op,               // config::Op
    pub id: String,           // "table:key"
    pub record: serde_json::Value,
    pub origin: Origin,
}

pub type Clock = Arc<dyn Fn() -> u64 + Send + Sync>;   // epoch ms

pub struct EngineOptions {     // Default::default() = the values below
    pub config_reload_ms: u64,        // 30_000
    pub sweep_ms: u64,                // 5_000
    pub prune_ms: u64,                // 1 h
    pub vapid_recheck_ms: u64,        // 5 min, once published
    pub subscription_cache_ms: u64,   // 5_000
    pub dedupe_capacity: usize,       // 100_000
    pub retry_backoff_ms: Vec<u64>,   // [2_000, 10_000, 60_000]
    pub retry_queue_cap: usize,       // 10_000
    pub send_concurrency: usize,      // 8 concurrent POSTs per observe / tick
    pub default_ttl_secs: u64,        // 86_400
    pub last_ok_interval_ms: u64,     // 10 min
    pub jwt_refresh_ms: u64,          // 11 h
    pub stale_pending_ms: u64,        // 30_000
    pub stuck_sending_ms: u64,        // 5 min
    pub sweep_batch: usize,           // 100
    pub message_retention_ms: u64,    // 7 d
    pub disabled_retention_ms: u64,   // 30 d
    pub allow_private_endpoints: bool,// false (tests / local dev only)
    pub clock: Option<Clock>,         // None = web_time::SystemTime
}
// EngineOptions::from_env(get) reads SPKY_PUSH_DEDUPE_CAPACITY,
// SPKY_PUSH_SEND_CONCURRENCY, SPKY_PUSH_CONFIG_RELOAD_SECS,
// SPKY_PUSH_DEFAULT_TTL_SECS.

pub struct PushEngine { .. }   // cheap to share: Arc<PushEngine>

impl PushEngine {
    pub fn new(db: Arc<dyn ScheduleDb>, http: Arc<dyn PushHttp>, keys: Option<VapidKeys>,
               opts: EngineOptions) -> Self;
    /// Keys from env: SPKY_VAPID_PRIVATE_KEY, else derived from SPKY_AUTH_SECRET.
    /// Also honours SPKY_PUSH=off (returns None). Hosts pass std::env::var.
    pub fn keys_from_env(get: impl Fn(&str) -> Option<String>) -> Option<VapidKeys>;
    /// Sync, cheap: does this change need observe()? (enabled + a rule watches
    /// the table + op, or table == "_00_push_message"). Hosts call it inline.
    pub fn wants(&self, table: &str, op: Op) -> bool;
    /// Everything for one row. Hosts spawn it (bounded concurrency).
    pub async fn observe(&self, change: ObservedChange);
    /// Periodic work, call every ~1 s: publish VAPID params until done, reload
    /// config every 30 s (hash short-circuit), flush due trailing throttles,
    /// retry failed sends (backoff 2 s, 10 s, 60 s, then give up), send due
    /// scheduled messages every 5 s, prune every hour.
    pub async fn tick(&self);
    /// For /health, /metrics, admin overview.
    pub fn status(&self) -> PushStatus;   // Serialize
    /// Re-read _00_push_config now (tick does it every 30 s). Ok(true) when
    /// another config became active.
    pub async fn reload_config(&self) -> Result<bool, String>;
    pub fn keys(&self) -> Option<&VapidKeys>;
}

/// Also exported: the endpoint guard (https, public DNS name).
pub fn endpoint_allowed(endpoint: &str, allow_private: bool) -> Result<(), String>;
```

`push_core` re-exports `schedule_core::{ScheduleDb, ScheduleDbError,
MaybeSendSync}` so hosts need one import. The futures `observe` / `tick`
return are `Send` on native (no lock is held across an await; a test pins it).

Boot: hosts call `tick()` right after building the engine. Until the first
successful config read `wants()` sees no rules (rows in that window do not
push); a failing first read is retried every tick instead of every 30 s.

`observe` for a rule row: for every enabled rule on (table, op): skip
`Origin::Repair` (not remembered either, so a later live copy still pushes);
evaluate `when`; `maxAge`; dedupe key in a bounded LRU (100k); resolve
recipients (`to` fields or query, flatten, normalize "user:abc", minus
`except`, cap `limits.maxRecipients`); only then run the `with` queries (a row
nobody receives costs no extra queries) into the template context; per
recipient: throttle (rule, recipient, topic) with trailing send, per-user rate
limit (token bucket per minute), render payload (templates may reference
`recipient`; render per recipient only when one does), deliver to that user's
subscriptions.

Details that are easy to get wrong:

- Conditions: a missing field is not null, except that `x: null` and
  `{exists: false}` match both missing and null. Numbers compare numerically
  (`3 == 3.0`), strings as strings, never across types. Record ids compare
  equal however they were printed (`user:⟨a-b⟩`, `user:a-b`, `{tb, id}`).
  (`{eq: null}` / `{ne: null}` cannot be expressed: serde reads the null as
  "operator absent"; use `x: null` / `{exists: true}`.)
- `maxAge`: the field may be an RFC 3339 datetime, epoch ms, or epoch seconds
  (numbers > 1e11 are ms). A missing or unreadable field PASSES: a typo must
  not mute a rule forever.
- Dedupe key: with `once`, (rule, id, values of the `once` fields): at most
  once per record for each distinct combination while it stays in the LRU.
  Without `once`, (rule, id, op, `_00_rv` or a key-order-independent hash of
  the row): the op keeps a delete (whose before-image equals the last update)
  from being mistaken for that update.
- `$row` in `with` / `to.query` is RE-READ from the table as root
  (`(SELECT * FROM ONLY type::record($tb, $key)) ?? <observed row>`), so record
  links are real records and `SELECT .. FROM ONLY $row.sender` works (a bound
  JSON row would carry them as strings, which match nothing). A deleted row
  falls back to the observed before-image, whose links ARE strings: wrap them
  in `type::record(..)` in delete rules. A query's value is its last
  statement. `to.query` results may be ids, id strings, rows with an `id`, or
  one-field rows (`SELECT user FROM member ..`).
- A failing `with` query renders its name as null (debug log); a failing
  `to.query` skips the row (warn, `last_error`).

`observe` for `_00_push_message` CREATE with status pending: if `send_at` is
unset or due, claim it (`UPDATE .. SET status = 'sending', claimed_at =
time::now() WHERE status = 'pending' AND (send_at = NONE OR send_at <=
time::now())` - the CAS, which also re-checks due-ness on the DB clock),
deliver to `to`, then set `status` sent/failed, `sent_at`, `delivered`,
`error`. Future `send_at`: leave it for the sweep. Other ops (including the
engine's own status writes coming back through ingest) are ignored. Direct
messages bypass rule filters on subscriptions and the subscription cache
(fresh read), but not the per-user limit. The message's notification is shown
as written (no templating) over the rendered `defaults.notification`; `tag`
defaults to its topic.

Outcome: `sent` when at least one subscription accepted it, else `failed`
with `error` = the last failure, or `no recipients` / `rate_limited` /
`no subscriptions`. A message with sends on the retry queue stays `sending`
until they settle, then gets its final status.

Sweep (every 5 s, while enabled), the fallback that makes ingest delivery
best-effort rather than load-bearing:
1. `status = 'sending'` for longer than 5 min (`claimed_at ?? created_at`):
   the host died mid-send; back to `pending`.
2. `status = 'pending' AND ((send_at != NONE AND send_at <= now) OR
   (send_at = NONE AND created_at < now - 30s))` LIMIT 100: due scheduled
   rows, and immediate rows whose ingest was missed (host restarting, engine
   not built yet, a failed http-transport event). Each goes through the same
   claim CAS, so a row the ingest path is handling is never sent twice.

Template context: row fields at top level, plus `row` (the whole row), `id`,
`table`, `op`, `rule`, every `with` name (wins over a row field of the same
name), `recipient` (per-recipient rendering), `now` (epoch ms).

Subscriptions: `SELECT * FROM _00_push_subscription WHERE auth_id IN $ids AND
disabled_at = NONE`, cached per user 5 s (rules only). Skip rows whose `kid` !=
current kid, and rows whose `rules` is set and does not contain the rule name.
Shared endpoints: before sending, if the same endpoint belongs to several
users, only the most recently `updated_at` owner receives; delete the others.

Delivery results: 2xx ok (update `last_ok_at` at most every 10 min per
subscription, or at once when `failures > 0`, resetting `failures` and
`last_error`); 404/410 delete the row; 413 and any other unexpected status
log + give up (row untouched); 400/401/403 set `disabled_at`,
`disabled_reason = "http_<code>: <body prefix, 200 chars>"`;
429/5xx/transport: retry queue, `failures += 1`, `last_error`; after the last
backoff, give up. Unusable keys (`p256dh`/`auth` that do not decode) disable
the row with `bad_keys: ..`.

Endpoint guard: record users choose the endpoint and the engine runs inside
the platform network, so without a check a user could point it at an
internal address and read the answer back through `last_error` /
`disabled_reason` in `fn::push::list()`. Only `https://` endpoints on a public
DNS name are POSTed to (no IP literals, no dotless hosts, no `localhost`,
`.local`, `.internal`, `.lan`, `.home.arpa`); anything else is disabled with
`bad_endpoint: ..` and never contacted. Hosts' HTTP clients should also
refuse names that RESOLVE to private addresses (not checkable here).
`EngineOptions.allow_private_endpoints` lifts it for tests / local dev.

Limits: `limits.perUserPerMinute` (per recipient, counted per push not per
device; token bucket refilled continuously, so 60 = one more per second once
drained), `limits.perMinute` (per HTTP send, retries included). Over a limit:
drop, count, warn at most once a minute. A trailing throttled push takes its
per-user token when it is flushed.

`enabled: false` (or no key): no rule is watched, no message is claimed (they
stay `pending`), queued trailing pushes and retries are dropped so nothing
stale goes out when push comes back. Bookkeeping (VAPID publish, config
reload, prune) continues.

Status (`PushStatus`, serde snake_case): `enabled`, `reason` (only when off),
`kid`, `public_key`, `subject`, `rules` (count), `rule_names`, `config_hash`
(None for "no row"), `config_loaded`, `vapid_published`, `totals` and
`last_minute` (`PushCounters`: matched, messages, sent, failed, dropped,
throttled, retried, deduped, skipped_repair), `queues` (`PushQueues`: retry,
trailing, throttle_slots, messages_in_flight, dedupe_entries, cached_users),
`last_error`.

## Templates (push-core `template.rs`)

`{{ path | filter | filter(arg, ...) }}`; args are literals (quoted strings,
numbers, true/false/null). A missing path renders "" (null in a JSON-typed
position). A name segment on a one-element array reads that element, so a
`with` result `[ { username } ]` is addressable as `{{sender.username}}`.
Record ids render as `table:key` whether they arrive as strings or
`{tb, id}` / `{table, key}` objects. A template that does not parse renders
verbatim (lint rejects it first: `template::check`).

Filters: `default(x)` (null, missing or ""), `truncate(n)` (characters; when
cut, the result is n characters ending in `...`), `upper`, `lower`, `trim`,
`key` (`user:abc` -> `abc`, quoting stripped; element-wise on arrays),
`table`, `json`, `len`, `join(sep = ", ")`, `first`, `last`,
`date(fmt = "%Y-%m-%d %H:%M")` (UTC strftime over a datetime or epoch).
String filters keep null as null so `| default(..)` still applies after them.

Rendering: notification text keys (title, body, icon, badge, image, lang,
dir, tag, url, action ids / titles / icons) render to strings, empty optional
ones are dropped; everything else (`data`, pass-through keys like `vibrate`)
keeps its JSON type when the string is exactly one `{{expr}}`. `data: [paths]`
copies each path under its path string as the key. Built-ins (`row`, `id`,
`table`, `op`, `rule`, `now`) win over row fields of the same name, `with`
names win over both.

## Hosts

As built: `apps/scheduler/src/push.rs` (slot published by `Scheduler::start`,
ticker, bounded observe) and `apps/ssp/src/lib.rs::build_push_engine` +
`SspNode.push_engine` (standalone only). The SSP ticks from a tokio interval
in the shell rather than a `TimerKind`, since only the native shell hosts it.
Both hosts give the engine a tokio sleep (`EngineOptions.sleep`) for the
message-claim retry below, and a `ReqwestPushHttp` (push-core feature
`reqwest`) whose resolver refuses non-public addresses and which never
follows redirects. `_00_push_message` is intercepted before the clone gate in
`ingest_event`, so a scheduler still cloning never refuses (under http:
aborts) the write that created it.

- Scheduler: build the engine after the DB slot is up (`SharedDb` adapter
  over `ReconnectingDb`; reqwest `PushHttp`, 10 s timeout). In `ingest_event`,
  after the gate: `_00_push_message` -> spawn observe, return Ok(0) before
  WAL; otherwise, after the job-terminal observer, `if engine.wants(..)`
  spawn observe bounded by a semaphore (64; saturation drops + counts). Drift
  repair passes Origin::Repair (add an origin parameter to the shared ingest
  path; the changefeed sink and `/ingest` are Live). Tick loop every 1 s.
  `status()` in `/health` (`push` key), `/metrics` gauges, admin overview.
- Standalone SSP (`standalone == true` only): `SspNode.push_engine:
  Option<Arc<PushEngine>>` built by `apps/ssp` (reqwest PushHttp, `PortDb`
  adapter). In `ingest_handler` intercept `_00_push_message` before the
  circuit (like `_00_query_allowlist`), and call the engine for rule tables
  after job routing via the node's `Spawner`. `TimerKind::PushTick` re-armed
  every 1 s. ssp-node must still pass `scripts/check-portability.sh` (wasm32).
- `SPKY_PUSH=off` disables the engine on both hosts.

## Client (`packages/core`)

`db.webPush` (`Sp00kyClient.webPush`, a getter on `SyncedDb` in client-solid
and client-solid2), `packages/core/src/modules/web-push`:
`support()`, `info()`, `subscribe(opts)`, `unsubscribe(opts)`, `sync(opts)`,
`isSubscribed()`, `update(opts)`, `devices()`, `notify(msg)`, `cancel(id)`,
`test(opts)`, `bridge(registration?)`, `onMessage(cb)`. All server calls are
`fn::push::*` through the remote connection (no codegen for `_00_` tables).
Typed `WebPushError` codes: unsupported, signed-out, impersonating, disabled,
permission-denied, permission-default, no-registration, not-subscribed,
subscribe-failed, server. `sync()` never throws and returns a status
(ok, resubscribed, registered, unsupported, signed-out, impersonating,
permission-default, permission-denied, not-subscribed, disabled,
no-registration, error).

- `subscribe()`: the permission request is the FIRST await (the user gesture
  must survive); a browser subscription made with another
  `applicationServerKey` is unsubscribed first; `rules: []` is sent as NONE
  (every rule), matching `fn::push::update`.
- Persisted under `sp00ky_push_endpoint` (PersistenceClient):
  `{ endpoint, userId, kid, publicKey, at }`. `sync()` only acts for a user
  whose record this is, unless `autoResubscribe`.
- `sync()` reads `fn::push::list()` once, then: browser subscription missing,
  made with another key (or, when the browser does not expose the key, stored
  kid != current kid), or its row disabled -> unsubscribe + resubscribe
  (`resubscribed`, label/rules/meta carried over, the old endpoint's row
  removed); row missing / `current: false` -> `fn::push::subscribe` again
  (`registered`).
- Config `webPush?: { serviceWorkerUrl, autoSync (true), autoResubscribe
  (false), unsubscribeOnSignOut (true), bridge (true) }`. Auto-sync runs
  1.5 s after the supervisor reports `connected` with a signed-in user, once
  per user, and again when the worker posts `sp00ky:subscriptionchange`.
- Sign-out: `AuthService.onBeforeSignOut(hook)` (new) runs every hook at the
  start of `signOut()`, in parallel, capped at 2 s. The web push hook posts
  `sp00ky:signout` to the active worker and, unless impersonating, calls
  `fn::push::unsubscribe($endpoint)` (1.5 s timeout) then drops the stored
  endpoint. While impersonating the server row cannot be removed (the table
  denies `_00_impersonate`); the admin's row stays until the push service
  reports it gone or the endpoint's next owner subscribes.
- Bridge message: `{ type: 'sp00ky:token', token, userId, endpoint,
  namespace, database, publicKey?, pushEndpoint? }` (`endpoint` = the
  SurrealDB endpoint; `publicKey` lets the worker re-subscribe on
  `pushsubscriptionchange`). The automatic bridge posts only while
  notification permission is granted; `bridge()` posts regardless. Never an
  impersonation token.

Subpath exports (a second tsdown config: entries `pure`, `live`, `sw`, chunks
prefixed `worker-`, so `dist/index.js` stays flat and nothing is shared with
the wasm graph; `scripts/check-worker-entries.mjs` runs in `build` and fails
on any wasm/pino/blurhash import, any `window`/`document`/`localStorage`
reference, a `surrealdb` import from `/sw`, or an entry that throws when
imported in Node):
- `@spooky-sync/core/pure`: side-effect-free builders (query sql/hash,
  `queryKey`, ref-tables, membership parsing, edge decoding, surql utils,
  `parseQueryParams`, `decodeTokenClaims`, sha256 helper, push wire types).
- `@spooky-sync/core/live`: `createLiveFeed({ endpoint, namespace, database,
  token, refMode?, ttl?, sessionId?, schema?, onUnauthorized?, onError?,
  timeoutMs?, surreal? })` -> `subscribe(query, { onSet, onChange?, onError? })`,
  `query(surql, vars)`, `idle({ timeoutMs, quietMs })`, `connect()`,
  `close()`. Same server contract as the full client: `registerSelect` /
  `registerVars` (config `{ id: _00_query:<sha256({surql, params,
  sessionId})>, surql, params, ttl }`, params typed with `parseQueryParams`
  from the builder's schema), bodies `SELECT * FROM $ids`, one
  `LIVE SELECT * FROM <user list_ref> FETCH out` per feed (started after the
  first registration, followed by one membership re-read so nothing between
  the read-back and LIVE is lost; bare-id `out` -> body read; null `out` ->
  membership re-read), heartbeat at half the ttl (reclaimed view ->
  re-register + diff), `fn::query::unsubscribe` per view on unsubscribe/close,
  LIVE again + re-read on reconnect. Rows are unordered and `.related()`
  children are not joined (subquery edges are ignored).
- `@spooky-sync/core/sw`: `installPushHandlers(options)` for the service
  worker: parses the payload (`PushPayload`, `v: 1`; anything else ->
  placeholder), `onPush` first, suppresses when a visible client exists
  (posts `sp00ky:push` to it instead), shows content pushes (tag defaults to
  topic; `data` = notification data + `url` + `payload` + per-action urls;
  unknown keys pass through), hands nudges to `onNudge`, else `render`, else
  the `live` renderer (`feed(bridgedSession)`, `query(userId, payload)`,
  `visible(row)`, `render(row)`; stale `sp00kyLive` tags closed; placeholder
  when nothing is left on screen), placeholder on failure (Chrome needs a
  visible notification per push), `notificationclick` (onClick, then
  focus + navigate, or `postMessage sp00ky:navigate` with `clickMode:
  'message'` or for an uncontrolled window, else `openWindow`),
  `notificationclose`, token bridge in IndexedDB (`sp00ky-push` / `kv`,
  `getBridgedToken()`, expired JWT = none), sign-out wipe (token + every
  shown notification), `pushsubscriptionchange` (re-subscribe with the old
  or bridged key, move the device through a feed's `fn::push::subscribe`
  keeping label/rules/meta, then post `sp00ky:subscriptionchange`).

## Payload (config::PushPayload)

```json
{ "v": 1, "kind": "rule", "rule": "new-message", "table": "message",
  "id": "message:abc", "op": "create", "topic": "dm:xyz",
  "notification": { "title": "...", "body": "...", "tag": "dm:xyz",
                    "url": "/m/xyz", "actions": [], "data": {} },
  "data": { "conversation": "conversation:xyz" }, "ts": 1727000000000 }
```

`kind: "message"` carries `message` (the `_00_push_message` id) instead of
rule/table/id/op.

## Verified end to end (2026-09-29)

A real Chrome (Playwright, system Chrome channel, persistent profile, since
Chrome refuses the Push API in incognito) subscribed through FCM against a
`spky dev` stack built from this branch, in both modes:

- singlenode + http transport (standalone SSP hosts the engine) and
- cluster + changefeed transport (scheduler hosts it).

Covered: `fn::push::info/subscribe/list/test`, a content rule with `with`,
`except`, templated topic/url (80-250 ms from the CREATE to the service
worker), `fn::push::test` and `spky push send` direct messages (~90-190 ms),
throttling (a burst collapses into one trailing push carrying the latest row),
and a nudge rule rendered by `installPushHandlers({ live })` over
`createLiveFeed` against the real SSP (register, list_ref, `LIVE ... FETCH
out`), including closing the notification when the row stops being visible.

Found and fixed by that run:

- Under the http transport the ingest event fires inside the creating
  transaction, so the engine's claim of a `_00_push_message` could run before
  the commit and leave the message to the 30 s sweep. The claim now retries
  (10 ms doubling to 250 ms, 5 s budget, host-supplied sleep) while the row is
  absent or pending-and-due.
- Under the changefeed transport `_00_push_message` writes do not touch
  `_00_version`, so they waited for the 2 s fallback poll. The doorbell now
  also holds a best-effort `LIVE SELECT id FROM _00_push_message`
  (`DOORBELL_EXTRA_TABLES`).
- FCM answers `410 push subscription has unsubscribed or expired` for a
  subscription made a few seconds earlier; the same endpoint accepts pushes
  shortly after (observed up to ~12 s). A 404/410 within
  `fresh_subscription_grace_ms` (2 min) of the row's `updated_at` is retried
  through the backoff instead of deleting the subscription.
- A live-rendered nudge whose only effect was closing notifications showed the
  placeholder ("New notification") in their place. It now shows nothing
  (`live.silentClose`, default true).
