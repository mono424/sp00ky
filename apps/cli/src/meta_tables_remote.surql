-- ==================================================
-- SPOOKY INCANTATION
-- The Registry of active Live Queries (Incantations).
--
-- Scoped per USER, not per session — one row is SHARED by every tab, window
-- and device of that user. This mirrors `_00_list_ref` below, which was moved
-- off session scoping for the same reason; `_00_query` was the straggler.
--
-- Session scoping (`clientId = session::id()`) actively broke a user with two
-- tabs open. The second tab's registration overwrites `clientId`, after which
-- the first tab can no longer read its own row: its `rowCount` probe returns
-- NONE, so it cannot tell "no results" from "edges still flushing" and latches
-- an empty list; and its `fn::query::heartbeat` UPDATE matches zero rows and
-- returns success, so the TTL refresh becomes a SILENT no-op. When the winning
-- tab closes, nothing refreshes `lastActiveAt`, the sweep deletes the row AND
-- every edge, and the surviving tab goes blank.
--
-- The anon branch mirrors `_00_list_ref_anon` (see ssp-node/src/tables.rs): a
-- logged-out session has `$auth = NONE` and cannot be gated on `$auth.id`, and
-- an anon row only ever describes a query whose results are already reachable
-- through that world-readable table.
--
-- `$auth.id != NONE` guards the cast so the rule evaluates cleanly for an
-- anonymous caller instead of erroring.
--
-- `delete` is granted deliberately: the row DELETE is the only trigger wired
-- to `/view/unregister` (see the _00_dbsp_cleanup event below), so without it
-- the sole working teardown is the TTL sweep. The refcount that decides
-- whether a delete is safe lives in `fn::query::unsubscribe`, not here.
--
-- The SSP runs as root and bypasses all of this.
-- ==================================================

DEFINE TABLE OVERWRITE _00_query SCHEMALESS
    PERMISSIONS
      FOR select, create, update, delete
        WHERE auth_id = 'anon'
           OR ($auth.id != NONE AND auth_id = <string>$auth.id);

-- The raw query string (for re-hydration/debugging)
DEFINE FIELD OVERWRITE surql ON TABLE _00_query TYPE option<string>;

-- The session that registered most recently. No longer a permission key (see
-- the table rule above) — kept because every `_00_list_ref` edge copies it off
-- this row, and for observability.
DEFINE FIELD OVERWRITE clientId ON TABLE _00_query TYPE option<string>;

-- Who is currently watching this shared incantation: one entry per WebSocket
-- session, `{ id: <session::id()>, seenAt: <datetime> }`.
--
-- ADVISORY, not authoritative. It gates only the EAGER release path
-- (`fn::query::unsubscribe`): the last subscriber to leave deletes the row and
-- with it the view and every edge. `lastActiveAt` below remains the SOLE
-- authority for the TTL sweep, because a client that crashes never removes its
-- entry — gating expiry on an empty array would pin a view forever.
--
-- Self-pruning: every writer drops entries older than this row's own `ttl` in
-- the same statement that appends its own, so the array is bounded by the
-- sessions active within one TTL window rather than by reconnect count.
--
-- Deliberately NOT `subscribers[*] ... FLEXIBLE`: SurrealDB rejects FLEXIBLE
-- on a SCHEMALESS table, and this one is SCHEMALESS. A bare array<object>
-- already accepts arbitrary keys here — pinned by
-- tests/surrealdb/shared_query_semantics.test.ts.
DEFINE FIELD OVERWRITE subscribers ON TABLE _00_query TYPE array<object> DEFAULT [];

-- The auth user id that registered this query. The SSP propagates it
-- onto each `_00_list_ref` row so live-query delivery can be gated on
-- the user (stable across re-auth / reconnect) instead of the per-
-- connection `session::id()`. Empty string means "no auth recorded",
-- which is fine for SSP-only registrations and unreadable to any
-- real authenticated user via the list_ref permission below.
DEFINE FIELD OVERWRITE auth_id ON TABLE _00_query TYPE string DEFAULT '';

-- For garbage collection (Heartbeat)
DEFINE FIELD OVERWRITE lastActiveAt ON TABLE _00_query TYPE datetime DEFAULT time::now();

-- How long this Incantation stays alive without activity
DEFINE FIELD OVERWRITE ttl ON TABLE _00_query TYPE duration;

-- Metrics: lifecycle and per-query observability. The SSP keeps a rolling
-- window of materialization samples per view and writes the persisted
-- percentiles on each ingest, so consumers can read them without
-- re-running the query.
DEFINE FIELD OVERWRITE createdAt ON TABLE _00_query TYPE datetime DEFAULT time::now() READONLY;
DEFINE FIELD OVERWRITE registrationTime ON TABLE _00_query TYPE option<float>;
DEFINE FIELD OVERWRITE materializationP55 ON TABLE _00_query TYPE option<float>;
DEFINE FIELD OVERWRITE materializationP90 ON TABLE _00_query TYPE option<float>;
DEFINE FIELD OVERWRITE materializationP99 ON TABLE _00_query TYPE option<float>;
DEFINE FIELD OVERWRITE lastIngestLatency ON TABLE _00_query TYPE option<float>;
DEFINE FIELD OVERWRITE updateCount ON TABLE _00_query TYPE int DEFAULT 0;
DEFINE FIELD OVERWRITE rowCount ON TABLE _00_query TYPE int DEFAULT 0;
-- Publish state of the view behind this row, written by the SSP:
--   'materializing'  the SSP has (re)computed the set and its edges are in
--                    flight to `_00_list_ref*`;
--   'ready'          the edges are committed (flipped inside the same
--                    transaction that writes them), so "no edges" now really
--                    means "no rows".
-- NONE = written by a server that predates the marker; clients then fall
-- back to `rowCount` alone. No DEFAULT on purpose.
DEFINE FIELD OVERWRITE state ON TABLE _00_query TYPE option<string>
    ASSERT $value == NONE OR $value IN ['materializing', 'ready'];
DEFINE FIELD OVERWRITE errorCount ON TABLE _00_query TYPE int DEFAULT 0;

-- ==================================================
-- SPOOKY LIST REF
-- The current list of results for a query/incantation.
-- Read-only to record-token callers — only the SSP (root) writes here, so
-- clients can never forge entries into their own view. Scoped by
-- `session::id()`: the SSP propagates the originating session's id from
-- _00_query.clientId onto each list-ref row, so a token-bearer only sees
-- its own list refs.
-- ==================================================

-- Scoped per user, not per session. The originating session's
-- `auth.id` is copied here from `_00_query.auth_id` at write time, so
-- every session of the same user receives LIVE updates for queries
-- that user has registered, regardless of which connection wrote
-- them. The client still filters in-memory by registered queryId
-- (see `Sp00kySync.handleRemoteListRefChange`), so cross-session
-- entries it doesn't care about are dropped after delivery.
DEFINE TABLE OVERWRITE _00_list_ref SCHEMALESS
    PERMISSIONS FOR select WHERE auth_id = <string>$auth.id;

DEFINE FIELD OVERWRITE in ON TABLE _00_list_ref TYPE record;

DEFINE FIELD OVERWRITE out ON TABLE _00_list_ref TYPE record;

DEFINE FIELD OVERWRITE clientId ON TABLE _00_list_ref TYPE string;

DEFINE FIELD OVERWRITE auth_id ON TABLE _00_list_ref TYPE string DEFAULT '';

DEFINE FIELD OVERWRITE version ON TABLE _00_list_ref TYPE int;

DEFINE FIELD OVERWRITE updatedAt ON TABLE _00_list_ref TYPE datetime VALUE time::now() READONLY;

DEFINE FIELD OVERWRITE parent ON TABLE _00_list_ref TYPE option<record<_00_list_ref>>;

DEFINE FIELD OVERWRITE parent_rel ON TABLE _00_list_ref TYPE option<string>;

-- ==================================================
-- SPOOKY STREAM PROCESSOR STATE
-- Stores the local state of the stream processor (DBSP)
-- ==================================================

-- Remote-only meta tables (not synced to client). Stays FULL because only the
-- root token (used by the SSP and the scheduler) ever reaches this table —
-- record-token callers can't address it.
DEFINE TABLE OVERWRITE _00_module_state SCHEMALESS PERMISSIONS FULL;

-- Unregister from DBSP on deletion
DEFINE EVENT OVERWRITE _00_dbsp_cleanup ON TABLE _00_query WHEN $event = "DELETE" THEN {
    let $result = mod::dbsp::unregister_view(<string>$before.id);
};

-- Note: Registration is now handled explicitly by fn::query::register calling mod::dbsp::register_view
-- The previous _00_dbsp_register event is removed to prevent recursion and double-registration.

-- ==================================================
-- SPOOKY VERSION
-- Tracks the version of every record for sync.
-- Permission inherits from the linked record: dereferencing record_id under
-- the caller's permission context returns NONE when the user can't read the
-- target row, which makes the version row invisible too.
-- ==================================================
DEFINE TABLE OVERWRITE _00_version SCHEMALESS
    PERMISSIONS FOR select WHERE record_id.id != NONE;
DEFINE FIELD OVERWRITE record_id ON TABLE _00_version TYPE record;
DEFINE FIELD OVERWRITE version ON TABLE _00_version TYPE int;
DEFINE INDEX OVERWRITE idx_record_id ON TABLE _00_version COLUMNS record_id UNIQUE;

-- CRDT and cursor state live inline on the parent record itself —
-- `@crdt`-only fields hold a base64 LoroDoc snapshot directly; `@crdt
-- @cursor` fields hold an `option<object> FLEXIBLE` of shape `{ state,
-- cursors }` (the schema-builder rewrites the user's TYPE clause).
-- No sidecar tables. Sync-down naturally carries CRDT state with the
-- parent row; cross-browser delivery rides the parent's existing LIVE
-- feed without a follow-up subquery.

-- ==================================================
-- SPOOKY ADMIN (operator roster)
-- The `user` table belongs to the app, not to sp00ky, so we can't gate
-- on a `role` field that may not exist — and a self-writable role field
-- would be a privilege-escalation hole anyway. This table is the whole
-- admin concept: a row here means "may edit feature flags from the
-- DevTools panel".
--
-- Root-written ONLY, via `spky admin add|remove|list`. create/update/
-- delete are denied unconditionally, so a record token cannot insert its
-- own row even with a forged mutation. Nobody self-promotes.
--
-- SELECT is scoped to the caller's OWN row (same shape as
-- _00_user_feature below). That lets the panel answer "am I an admin?"
-- with one query while the roster itself stays unreadable: a non-admin
-- gets [], an admin gets exactly one row — their own.
--
-- The _00_feature_flag / _00_user_feature rules below read this table
-- from INSIDE a PERMISSIONS clause. SurrealDB computes permission
-- predicates with permission checks disabled on the nested Options
-- (surrealdb-core `doc/check.rs::process_permissions` does
-- `opt.new_with_perms(false)`), so that subquery sees every row
-- regardless of the self-scoped rule here. The membership test works
-- without the roster ever being readable by a non-admin.
--
-- MUST NEVER join `ssp_protocol::SYNCED_META_TABLES`: the SSP injects NO
-- permission filter for `_00_*` tables (packages/ssp/src/
-- permission_inject.rs returns Ok(None) for them), so syncing this would
-- publish the whole roster unfiltered to any client that registers a
-- query on it.
-- ==================================================
DEFINE TABLE OVERWRITE _00_admin SCHEMAFULL
    PERMISSIONS
      FOR select WHERE user = $auth.id
      FOR create, update, delete NONE;

DEFINE FIELD OVERWRITE user ON TABLE _00_admin TYPE record;
DEFINE FIELD OVERWRITE note ON TABLE _00_admin TYPE option<string>;
DEFINE FIELD OVERWRITE added_at ON TABLE _00_admin TYPE datetime DEFAULT time::now() READONLY;
DEFINE INDEX OVERWRITE idx_admin_user ON TABLE _00_admin COLUMNS user UNIQUE;

-- ==================================================
-- SPOOKY FEATURE FLAG (definitions)
-- Readable and updatable by admins (see _00_admin above), invisible to
-- everyone else — a non-admin's SELECT silently returns zero rows and
-- never learns the table exists, so targeting rules (including
-- allowlisted user records) still never reach an ordinary client.
--
-- create/delete stay root-only: `spky flag create|delete` owns the
-- lifecycle. The DevTools panel only flips `enabled` and edits `rules`,
-- both of which are UPDATEs.
--
-- The membership test is an indexed point lookup on _00_admin
-- (idx_admin_user). The `$auth.id != NONE` guard keeps anonymous live
-- queries out. Keep this predicate byte-identical to the one on
-- _00_user_feature and on the fn::feature::* functions below — if the
-- four ever drift, one gate opens wider than the others.
-- ==================================================
DEFINE TABLE OVERWRITE _00_feature_flag SCHEMAFULL
    PERMISSIONS
      FOR select, update
        WHERE $auth.id != NONE
          AND array::len((SELECT VALUE id FROM _00_admin WHERE user = $auth.id LIMIT 1)) > 0
      FOR create, delete NONE;

DEFINE FIELD OVERWRITE key ON TABLE _00_feature_flag TYPE string;
DEFINE FIELD OVERWRITE description ON TABLE _00_feature_flag TYPE option<string>;
DEFINE FIELD OVERWRITE variants ON TABLE _00_feature_flag TYPE array<string> DEFAULT ['off', 'on'];
DEFINE FIELD OVERWRITE default_variant ON TABLE _00_feature_flag TYPE string DEFAULT 'off';
-- FLEXIBLE: arbitrary per-variant payload keys (set by the CLI / evaluator).
DEFINE FIELD OVERWRITE payloads ON TABLE _00_feature_flag TYPE option<object> FLEXIBLE;
-- Each rule (written by `spky flag`): allowlist =
--   { kind: 'allowlist', variant, users: array<record>, priority }
-- rollout = { kind: 'rollout', variant, percent: int 0..100, priority }.
-- The element is FLEXIBLE so these arbitrary keys are accepted on a SCHEMAFULL
-- table; without it SurrealDB rejects `rules[*].kind` as an unknown field.
DEFINE FIELD OVERWRITE rules ON TABLE _00_feature_flag TYPE array<object> DEFAULT [];
DEFINE FIELD OVERWRITE rules[*] ON TABLE _00_feature_flag TYPE object FLEXIBLE;
DEFINE FIELD OVERWRITE enabled ON TABLE _00_feature_flag TYPE bool DEFAULT true;
DEFINE FIELD OVERWRITE created_at ON TABLE _00_feature_flag TYPE datetime DEFAULT time::now() READONLY;
DEFINE FIELD OVERWRITE updated_at ON TABLE _00_feature_flag TYPE datetime VALUE time::now();
DEFINE INDEX OVERWRITE idx_feature_flag_key ON TABLE _00_feature_flag COLUMNS key UNIQUE;

-- ==================================================
-- SPOOKY USER FEATURE (per-user materialized assignments)
-- Record-token callers can SELECT only their own rows: an ordinary
-- client cannot self-enable a flag, and cannot see anyone else's.
--
-- Writes are open to ADMINS (and root), not to ordinary users. This is
-- load-bearing and must not be "tightened" back to NONE: a SurrealDB
-- custom function is NOT security-definer — `fn::feature::materialize`
-- below runs its UPSERTs under the CALLER's permissions (surrealdb-core
-- computes a function body with the caller's Options; the AuthLimit it
-- applies is a cap, never a grant). With `FOR create, update, delete
-- NONE` an admin calling materialize from the browser would fail on
-- every row.
--
-- An admin being able to write assignment rows is inside the trust they
-- already hold: they can flip the flag those rows are derived from.
-- ==================================================
DEFINE TABLE OVERWRITE _00_user_feature SCHEMAFULL
    PERMISSIONS
      FOR select WHERE user = $auth.id
      FOR create, update, delete
        WHERE $auth.id != NONE
          AND array::len((SELECT VALUE id FROM _00_admin WHERE user = $auth.id LIMIT 1)) > 0;

DEFINE FIELD OVERWRITE user ON TABLE _00_user_feature TYPE record;
DEFINE FIELD OVERWRITE key ON TABLE _00_user_feature TYPE string;
DEFINE FIELD OVERWRITE variant ON TABLE _00_user_feature TYPE string;
DEFINE FIELD OVERWRITE payload ON TABLE _00_user_feature TYPE option<object> FLEXIBLE;
DEFINE FIELD OVERWRITE evaluated_at ON TABLE _00_user_feature TYPE datetime VALUE time::now();
DEFINE INDEX OVERWRITE idx_user_feature_user_key ON TABLE _00_user_feature COLUMNS user, key UNIQUE;

-- Stable hash for percentage rollouts. Combines flag key and user id
-- so the same (flag, user) pair always lands in the same bucket 0..99
-- across deploys. Implementation matches the Rust evaluator in
-- `apps/cli/src/flag.rs::rollout_hash`: SHA-256, then read the first 8
-- hex chars as an unsigned 32-bit integer modulo 100.
--
-- The hex digits are folded by hand rather than cast. SurrealDB 3 removed
-- hex support from the int cast — `<int>'0x5f7bdee4'` fails with "Could
-- not cast into `int`", and `0x5f7bdee4` is no longer even a valid token —
-- so the original one-liner errors on every call. Nothing exercised it
-- before (both Rust evaluators hash themselves and the CLI never called
-- it), which is why it went unnoticed; `fn::feature::materialize` below
-- does call it, so every rollout rule depends on this being correct.
-- `string::split(s, '')` yields empty strings at both ends, hence the
-- filter. The golden vectors in `flag.rs::rollout_hash_matches_golden_vectors`
-- are asserted against this implementation.
DEFINE FUNCTION OVERWRITE fn::feature::hash($key: string, $user_id: string) -> int {
  LET $hex = string::slice(crypto::sha256($key + ':' + $user_id), 0, 8);
  LET $digits = ['0','1','2','3','4','5','6','7','8','9','a','b','c','d','e','f'];
  LET $chars = array::filter(string::split($hex, ''), |$c| $c != '');
  RETURN array::fold($chars, 0, |$acc, $c| $acc * 16 + array::find_index($digits, $c)) % 100;
};

-- ==================================================
-- ADMIN-CALLABLE FEATURE FLAG MUTATIONS
--
-- These exist so the DevTools panel can flip a flag from the browser.
-- `spky flag` does the same work in Rust and does NOT call them.
--
-- Two SurrealDB facts drive the shape of everything below:
--
--   1. DEFINE FUNCTION defaults to PERMISSIONS FULL. Omitting the clause
--      does NOT make a function root-only — it ships a self-service flag
--      editor to every signed-in user. Each function here therefore
--      carries an explicit PERMISSIONS clause, identical to the one on
--      _00_feature_flag / _00_user_feature. (The `fn::job::*` functions
--      in functions_remote_singlenode.surql claim otherwise in a comment;
--      that comment is wrong and is tracked separately.)
--
--   2. A custom function is NOT security-definer: its body runs under the
--      CALLER's permissions. That is why _00_user_feature grants writes to
--      admins, and why the `SELECT VALUE id FROM user` below is subject to
--      the app's own `user` table SELECT rule — see the guard in
--      fn::feature::materialize.
--
-- Root bypasses permission checks entirely, so the CLI and the scheduler
-- are unaffected by the clauses.
-- ==================================================

-- Re-evaluate ONE flag for EVERY user and refresh `_00_user_feature`.
-- Faithful port of the Rust evaluator (`apps/cli/src/flag.rs::evaluate_one`
-- and `apps/scheduler/src/feature_flags.rs::evaluate_one`); the golden
-- vectors pinned in both Rust test suites are asserted against this
-- implementation too, so drift fails a test rather than silently
-- assigning different variants depending on who flipped the flag.
DEFINE FUNCTION OVERWRITE fn::feature::materialize($key: string) -> object {
    LET $flag = (SELECT * FROM ONLY _00_feature_flag WHERE key = $key LIMIT 1);

    -- Unknown/deleted flag: drop the orphaned assignments, mirroring the
    -- load_flag-failed branch of `flag.rs::materialize`.
    IF $flag = NONE {
        DELETE _00_user_feature WHERE key = $key;
        RETURN { key: $key, found: false, users: 0 };
    };

    LET $default  = $flag.default_variant ?? 'off';
    LET $payloads = $flag.payloads ?? {};
    -- `enabled = false` => everyone gets default_variant, rules ignored.
    LET $rules    = IF ($flag.enabled ?? true) { $flag.rules ?? [] } ELSE { [] };

    -- Normalise the rules ONCE (they don't depend on the user).
    --   rank  — priority ascending, original position as tiebreak, because
    --           Rust sorts with the stable `sort_by_key`. 100 is the
    --           missing-priority default; the CLI writes 10 (allowlist)
    --           and 50 (rollout).
    --   users — `rules[].users` holds STRINGS on disk (`flag.rs` pushes
    --           Value::String and interpolates the JSON verbatim), so a
    --           record-vs-array compare would never match. Cast both
    --           sides to string and a hand-written record element works
    --           too.
    LET $ranked = array::map($rules, |$r, $i| {
        RETURN {
            rank: (<int>($r.priority ?? 100)) * 100000 + $i,
            kind: $r.kind,
            variant: $r.variant,
            percent: <int>($r.percent ?? 0),
            users: array::map($r.users ?? [], |$x| <string>$x)
        };
    });

    LET $users = (SELECT VALUE id FROM user);

    -- The SELECT above runs under the CALLER's permissions (fact 2). Every
    -- shipped schema template uses `FOR create, select WHERE true` on
    -- `user`, but an app that scopes user selects to `id = $auth.id` would
    -- return just the admin — materializing one row and leaving everyone
    -- else on the old variant, with no error. The scheduler stamps the
    -- root-enumerated count each sweep precisely so we can catch that.
    LET $expected = _00_module_state:feature_user_count.count ?? 0;
    IF array::len($users) < $expected {
        THROW "fn::feature::materialize can only see " + <string>array::len($users) + " of "
            + <string>$expected + " users, so it would leave most of them on a stale variant. "
            + "The `user` table's SELECT permission is hiding rows from this admin — widen it, "
            + "or change the flag with `spky flag` instead.";
    };

    FOR $u IN $users {
        LET $uid = <string>$u;

        LET $matched = array::filter($ranked, |$c| {
            RETURN IF $c.variant = NONE { false }
                ELSE IF $c.kind = 'allowlist' { $uid IN $c.users }
                ELSE IF $c.kind = 'rollout' {
                    $c.percent > 0 AND fn::feature::hash($key, $uid) < $c.percent
                }
                ELSE { false };
        });

        -- First match wins = lowest rank. Avoids `SELECT … ORDER BY` over an
        -- array, which SurrealDB 3 rejects when the ORDER BY field isn't in
        -- the projection.
        LET $best    = array::min(array::map($matched, |$c| $c.rank));
        LET $win     = array::first(array::filter($matched, |$c| $c.rank = $best));
        LET $variant = IF $win = NONE { $default } ELSE { $win.variant };

        UPSERT _00_user_feature
            SET user = $u, key = $key, variant = $variant, payload = $payloads[$variant]
            WHERE user = $u AND key = $key;
    };

    RETURN { key: $key, found: true, users: array::len($users) };
}
PERMISSIONS WHERE $auth.id != NONE
    AND array::len((SELECT VALUE id FROM _00_admin WHERE user = $auth.id LIMIT 1)) > 0;

-- Add `$user` to `$key`'s allowlist for `$variant`, then re-materialize.
-- Rule shape and priority (10) must stay identical to
-- `apps/cli/src/flag.rs::upsert_allowlist` / PRIORITY_ALLOWLIST. The rule
-- object is rebuilt field-by-field rather than merged, because an
-- allowlist rule only ever carries these four keys.
--
-- This is a read-modify-write over the whole `rules` array (the same shape
-- `spky flag set` uses). Two admins acting on one flag at the same moment
-- therefore collide — but SurrealDB fails the loser with "Transaction
-- conflict ... can be retried" instead of dropping the write, so no entry is
-- silently lost. The DevTools bridge retries on that error
-- (`FlagsAdminService.mutate`); a caller using these functions directly
-- should do the same.
DEFINE FUNCTION OVERWRITE fn::feature::allow($key: string, $variant: string, $user: record) -> object {
    LET $flag = (SELECT * FROM ONLY _00_feature_flag WHERE key = $key LIMIT 1);
    IF $flag = NONE { THROW "Unknown flag: " + $key };
    IF !($variant IN ($flag.variants ?? [])) {
        THROW "Unknown variant '" + $variant + "' for flag '" + $key + "'";
    };

    LET $uid   = <string>$user;
    LET $rules = $flag.rules ?? [];
    LET $has   = array::len(array::filter($rules, |$r| {
        RETURN $r.kind = 'allowlist' AND $r.variant = $variant;
    })) > 0;

    LET $next = IF $has {
        array::map($rules, |$r| {
            RETURN IF $r.kind = 'allowlist' AND $r.variant = $variant {
                {
                    kind: 'allowlist',
                    variant: $r.variant,
                    users: array::distinct(array::append(array::map($r.users ?? [], |$x| <string>$x), $uid)),
                    priority: $r.priority ?? 10
                }
            } ELSE { $r };
        })
    } ELSE {
        array::append($rules, { kind: 'allowlist', variant: $variant, users: [$uid], priority: 10 })
    };

    UPDATE _00_feature_flag SET rules = $next WHERE key = $key;
    RETURN fn::feature::materialize($key);
}
PERMISSIONS WHERE $auth.id != NONE
    AND array::len((SELECT VALUE id FROM _00_admin WHERE user = $auth.id LIMIT 1)) > 0;

-- Remove `$user` from every allowlist rule on `$key`, then re-materialize.
-- Mirrors `spky flag unset --for-user`: the rule is kept (possibly with an
-- empty `users`), only the membership goes.
DEFINE FUNCTION OVERWRITE fn::feature::disallow($key: string, $user: record) -> object {
    LET $flag = (SELECT * FROM ONLY _00_feature_flag WHERE key = $key LIMIT 1);
    IF $flag = NONE { THROW "Unknown flag: " + $key };

    LET $uid  = <string>$user;
    LET $next = array::map($flag.rules ?? [], |$r| {
        RETURN IF $r.kind = 'allowlist' {
            {
                kind: 'allowlist',
                variant: $r.variant,
                users: array::filter(array::map($r.users ?? [], |$x| <string>$x), |$x| $x != $uid),
                priority: $r.priority ?? 10
            }
        } ELSE { $r };
    });

    UPDATE _00_feature_flag SET rules = $next WHERE key = $key;
    RETURN fn::feature::materialize($key);
}
PERMISSIONS WHERE $auth.id != NONE
    AND array::len((SELECT VALUE id FROM _00_admin WHERE user = $auth.id LIMIT 1)) > 0;

-- ==================================================
-- SPOOKY APP RELEASE (per-frontend "current version" announcements)
-- One row per frontend app, keyed by name (_00_app_release:<app>). Written
-- root-only: `spky deploy` UPSERTs it after every successful frontend deploy
-- (version = the frontend's package.json version), the git-linked builder
-- mirrors that, and `spky release` sets it manually. Readable by everyone
-- (incl. anonymous live queries) so running clients can compare against their
-- built-in version and offer/force a reload. cache_bust asks the client to
-- clear caches on that reload; mandatory means reload/update without asking.
-- Synced to clients via the explicit ingest-notify events in sp00ky.rs (the
-- _00_user_feature path) - _00_ tables are excluded from the snapshot clone.
-- ==================================================
DEFINE TABLE OVERWRITE _00_app_release SCHEMAFULL
    PERMISSIONS
      FOR select WHERE true
      FOR create, update, delete NONE;

DEFINE FIELD OVERWRITE app ON TABLE _00_app_release TYPE string;
DEFINE FIELD OVERWRITE version ON TABLE _00_app_release TYPE string;
DEFINE FIELD OVERWRITE cache_bust ON TABLE _00_app_release TYPE option<bool>;
DEFINE FIELD OVERWRITE mandatory ON TABLE _00_app_release TYPE option<bool>;
DEFINE FIELD OVERWRITE released_at ON TABLE _00_app_release TYPE datetime DEFAULT time::now();

-- ==================================================
-- QUERY ALLOWLIST
-- One row per (app, version): the query shapes that release's query module
-- produces, generated by `spky generate` from `clientTypes[].queries` and
-- published by `spky deploy` / `spky release` / `spky dev`. The SSP (root)
-- reads the union of every row and refuses (or, under `warn`, logs) a view
-- registration whose shape is not in it; see `sync.queryAllowlist`. The
-- writer keeps the current and the previous version per app so a rollout
-- never refuses the release still loaded in open tabs. Root-only: nothing a
-- client ever reads, and not a synced meta table (it never enters the
-- circuit; its ingest-notify event only tells the SSPs to reload).
-- ==================================================
DEFINE TABLE OVERWRITE _00_query_allowlist SCHEMAFULL PERMISSIONS NONE;

DEFINE FIELD OVERWRITE app ON TABLE _00_query_allowlist TYPE string;
DEFINE FIELD OVERWRITE version ON TABLE _00_query_allowlist TYPE string;
DEFINE FIELD OVERWRITE source_hash ON TABLE _00_query_allowlist TYPE string;
DEFINE FIELD OVERWRITE entries ON TABLE _00_query_allowlist TYPE array<object>;
DEFINE FIELD OVERWRITE entries.*.name ON TABLE _00_query_allowlist TYPE string;
DEFINE FIELD OVERWRITE entries.*.surql ON TABLE _00_query_allowlist TYPE string;
DEFINE FIELD OVERWRITE entries.*.whereMode ON TABLE _00_query_allowlist TYPE string
    ASSERT $value IN ['static', 'any'];
DEFINE FIELD OVERWRITE released_at ON TABLE _00_query_allowlist TYPE datetime DEFAULT time::now();

-- ==================================================
-- SPOOKY SCHEMA STATE
-- Stores a hash of the last-applied assembled internal schema and remote
-- functions blob, so spky migrate/deploy can skip re-applying them when the
-- desired state is byte-identical to what is already on the server. Rows are
-- keyed by id: internal (meta tables + per-table events) and remote_fns (the
-- remote-functions blob). See apply_internal_schema and
-- apply_remote_functions_if_changed in apps/cli/src/migrate.rs. Mirrors the
-- client-side _00_schema/local-migrator.ts hash-skip pattern.
-- ==================================================
DEFINE TABLE OVERWRITE _00_schema_state SCHEMAFULL PERMISSIONS NONE;
DEFINE FIELD OVERWRITE hash ON TABLE _00_schema_state TYPE string;
DEFINE FIELD OVERWRITE updated_at ON TABLE _00_schema_state TYPE datetime VALUE time::now();

-- ==================================================
-- SPOOKY HEARTBEAT (e2e sync-pipeline probe)
-- One row (_00_heartbeat:probe), UPSERTed by the scheduler's heartbeat loop.
-- Its hand-written mutation event in sp00ky.rs posts to /ingest, so every
-- probe write exercises the full pipeline: DB event -> scheduler ingest ->
-- WAL -> broadcast -> SSP circuit step. The SSP records the last hb_seq it
-- saw and the scheduler polls that back to measure end-to-end latency.
-- NOT in SYNCED_META_TABLES: excluded from snapshot clones and bootstrap
-- hashes by design (adding it would force a lockstep scheduler+SSP deploy).
-- ==================================================
DEFINE TABLE OVERWRITE _00_heartbeat SCHEMAFULL PERMISSIONS NONE;
DEFINE FIELD OVERWRITE hb_seq ON TABLE _00_heartbeat TYPE int DEFAULT 0;
DEFINE FIELD OVERWRITE sent_at ON TABLE _00_heartbeat TYPE datetime VALUE time::now();

-- ==================================================
-- SPOOKY HEARTBEAT ROLLUP (bounded latency time series)
-- One row per hour (_00_heartbeat_rollup:[<epoch-hour>]), advanced with
-- atomic `+=` UPSERTs from the scheduler's heartbeat loop — 24 rows/day,
-- mirrors the _00_run_rollup pattern. No mutation events (plain _00_ table),
-- so writing results here cannot re-trigger the probe's own ingest.
-- avg = sum_ms / ok; failures leave last_e2e_ms untouched. The probe loop
-- prunes rows older than 90d once a day.
-- ==================================================
DEFINE TABLE OVERWRITE _00_heartbeat_rollup SCHEMAFULL PERMISSIONS NONE;
DEFINE FIELD OVERWRITE bucket ON TABLE _00_heartbeat_rollup TYPE datetime;
DEFINE FIELD OVERWRITE count ON TABLE _00_heartbeat_rollup TYPE int DEFAULT 0;
DEFINE FIELD OVERWRITE ok ON TABLE _00_heartbeat_rollup TYPE int DEFAULT 0;
DEFINE FIELD OVERWRITE sum_ms ON TABLE _00_heartbeat_rollup TYPE int DEFAULT 0;
DEFINE FIELD OVERWRITE max_ms ON TABLE _00_heartbeat_rollup TYPE int DEFAULT 0;
DEFINE FIELD OVERWRITE last_e2e_ms ON TABLE _00_heartbeat_rollup TYPE option<int>;
DEFINE FIELD OVERWRITE last_ok_at ON TABLE _00_heartbeat_rollup TYPE option<datetime>;
DEFINE FIELD OVERWRITE updated_at ON TABLE _00_heartbeat_rollup TYPE datetime VALUE time::now();
DEFINE INDEX OVERWRITE idx_heartbeat_bucket ON TABLE _00_heartbeat_rollup COLUMNS bucket;
