-- ==================================================
-- CLIENT-SIDE META TABLES
--
-- These tables live in the per-device local SurrealKV cache, which is
-- single-tenant by construction (one user, one device). The codegen step in
-- apps/cli/src/codegen.rs rewrites every PERMISSIONS clause to `WHERE true`
-- when emitting the client schema, because there's no $session / $auth on
-- the local DB to scope against. For multi-tenant scoping, see the
-- corresponding rules in meta_tables_remote.surql which run on the server.
-- ==================================================

-- ==================================================
-- SPOOKY SCHEMA
-- The provisioned schema state for the database. Currently only used in local cache.
-- Used in local-migrator.ts
-- ==================================================

DEFINE TABLE IF NOT EXISTS _00_schema SCHEMAFULL;
DEFINE FIELD IF NOT EXISTS id ON _00_schema TYPE string;
DEFINE FIELD IF NOT EXISTS hash ON _00_schema TYPE string;
DEFINE FIELD IF NOT EXISTS created_at ON _00_schema TYPE datetime VALUE time::now();
DEFINE INDEX IF NOT EXISTS unique_hash ON _00_schema FIELDS hash UNIQUE;

-- ==================================================
-- SPOOKY STREAM PROCESSOR STATE
-- Stores the local state of the stream processor (DBSP)
-- ==================================================

DEFINE TABLE IF NOT EXISTS _00_stream_processor_state SCHEMALESS
    PERMISSIONS FOR select, create, update, delete WHERE true;

DEFINE FIELD IF NOT EXISTS id ON _00_stream_processor_state TYPE string;
DEFINE FIELD IF NOT EXISTS state ON _00_stream_processor_state TYPE string;
DEFINE FIELD IF NOT EXISTS updated_at ON _00_stream_processor_state TYPE datetime VALUE time::now();

-- ==================================================
-- SPOOKY INCANTATION
-- The Registry of active Live Queries (Incantations).
-- ==================================================

DEFINE TABLE _00_query SCHEMALESS
    PERMISSIONS FOR select, create, update WHERE true;

DEFINE FIELD surql ON TABLE _00_query TYPE string
    PERMISSIONS FOR select, create, update WHERE true;

DEFINE FIELD localArray ON TABLE _00_query TYPE any
    PERMISSIONS FOR select, create, update WHERE true;

DEFINE FIELD remoteArray ON TABLE _00_query TYPE any
    PERMISSIONS FOR select, create, update WHERE true;

DEFINE FIELD lastActiveAt ON TABLE _00_query TYPE datetime DEFAULT time::now()
    PERMISSIONS FOR select, create, update WHERE true;

DEFINE FIELD ttl ON TABLE _00_query TYPE string
    PERMISSIONS FOR select, create, update WHERE true;

DEFINE FIELD tableName ON TABLE _00_query TYPE string
    PERMISSIONS FOR select, create, update WHERE true;

-- Metrics: lifecycle and per-query observability. Rolling-window percentiles
-- are computed in the client (DataModule) from the last N materialization
-- samples and persisted on each ingest so devtools can read them without
-- re-running the query.

DEFINE FIELD createdAt ON TABLE _00_query TYPE datetime DEFAULT time::now() READONLY
    PERMISSIONS FOR select, create, update WHERE true;

DEFINE FIELD registrationTime ON TABLE _00_query TYPE option<float>
    PERMISSIONS FOR select, create, update WHERE true;

DEFINE FIELD materializationP55 ON TABLE _00_query TYPE option<float>
    PERMISSIONS FOR select, create, update WHERE true;

DEFINE FIELD materializationP90 ON TABLE _00_query TYPE option<float>
    PERMISSIONS FOR select, create, update WHERE true;

DEFINE FIELD materializationP99 ON TABLE _00_query TYPE option<float>
    PERMISSIONS FOR select, create, update WHERE true;

DEFINE FIELD lastIngestLatency ON TABLE _00_query TYPE option<float>
    PERMISSIONS FOR select, create, update WHERE true;

DEFINE FIELD updateCount ON TABLE _00_query TYPE int DEFAULT 0
    PERMISSIONS FOR select, create, update WHERE true;

DEFINE FIELD rowCount ON TABLE _00_query TYPE int DEFAULT 0
    PERMISSIONS FOR select, create, update WHERE true;

DEFINE FIELD errorCount ON TABLE _00_query TYPE int DEFAULT 0
    PERMISSIONS FOR select, create, update WHERE true;

-- ==================================================
-- SPOOKY EVENTS
-- The local outbox: one row per create, update and delete not yet pushed.
--
-- SCHEMALESS on purpose. The row shape belongs to the client
-- (packages/core/src/mutation/rows.ts, `pendingInsert`), which reads every
-- shape this table ever had. This used to be SCHEMAFULL with only
-- id/mutationType/recordId/data, and when the client started writing
-- `tableName`, `createdAt`, `v` and `beforeRecord`, SurrealDB rejected the
-- unknown fields and failed the whole local write transaction: on the
-- SurrealDB local engine no create, update or delete could be stored.
-- ==================================================

DEFINE TABLE _00_pending_mutations SCHEMALESS
    PERMISSIONS FOR select, create, update, delete WHERE true;

-- CRDT state lives inline on the parent record itself (the `@crdt`-
-- annotated field). `@crdt`-only fields hold the base64 LoroDoc
-- snapshot directly; `@crdt @cursor` fields hold an
-- `option<object> FLEXIBLE` of shape `{ state, cursors }` (rewritten
-- by the schema-builder). Same shape on client and server.

-- ==================================================
-- SPOOKY USER FEATURE (per-user materialized flag assignments)
--
-- Server-materialized, root-only writes (see meta_tables_remote.surql for the
-- scoped `select WHERE user = $auth.id` rule). Synced DOWN read-only so the
-- feature-flag resolver can run its one shared live query
-- (`SELECT key, variant, payload FROM _00_user_feature`) on the client. This
-- table MUST be declared here, or the synced rows have no local table to land
-- in and every flag silently falls back to its default (a flag gated fail-open
-- then shows for everyone). SCHEMALESS so a synced row is never dropped on a
-- field/type mismatch; the local cache is single-user, so permissions are
-- WHERE true (the codegen rewrites them anyway).
-- ==================================================

DEFINE TABLE _00_user_feature SCHEMALESS
    PERMISSIONS FOR select, create, update, delete WHERE true;

-- ==================================================
-- SPOOKY APP RELEASE (per-frontend current-version announcements)
--
-- Server-written (root-only) release rows, synced DOWN read-only so the
-- app-release resolver can run its one shared live query
-- (`SELECT * FROM _00_app_release`) on the client. Must be declared here or
-- the synced rows have no local table to land in (same trap as
-- _00_user_feature above). SCHEMALESS so a synced row is never dropped on a
-- field/type mismatch.
-- ==================================================

DEFINE TABLE _00_app_release SCHEMALESS
    PERMISSIONS FOR select, create, update, delete WHERE true;
