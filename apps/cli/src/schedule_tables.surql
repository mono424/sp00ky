-- ==================================================
-- SPOOKY SCHEDULING TABLES
-- Server-side cron / workflow definitions and their run history. Split out of
-- meta_tables_remote.surql so `packages/schedule-core` can include_str! exactly
-- the DDL that ships and run its engine tests against it — the rest of the
-- internal schema needs CLI-time substitution (the surrealism `mod::` call) and
-- won't parse standalone.
-- Applied by the same paths as the other meta tables: migrate::apply_internal_schema
-- (VM/cloud) and schema_builder::build_server_schema (free/Cloudflare push).
-- ==================================================

-- ==================================================
-- SPOOKY SCHEDULE (declarative server-side cron / workflow definitions)
-- One row per `schedules:` / `workflows:` entry in sp00ky.yml, keyed by name
-- (_00_schedule:<name>). Root-only: a schedule can fan out over arbitrary
-- SurrealQL and spawn jobs against any backend, so it must never be
-- client-reachable.
--
-- Three writers, three disjoint field groups, and they must stay disjoint:
--   deploy   (`spky deploy` / `spky dev`) writes the SPEC fields + spec_hash,
--            and resets next_fire_at to NONE only when spec_hash changed.
--            It NEVER writes paused — an operator pause must survive a deploy.
--   operator (`spky schedules pause|resume|trigger`) writes paused and
--            trigger_requested_at, and nothing else.
--   engine   (the schedule sweep, singlenode SSP or cluster scheduler) writes
--            next_fire_at / last_fire_at / last_error, and nothing else.
-- trigger_requested_at is its own field rather than "just set next_fire_at to
-- now" precisely so an operator trigger can never collide with the engine's
-- compare-and-swap on the cron clock.
-- ==================================================
DEFINE TABLE OVERWRITE _00_schedule SCHEMAFULL PERMISSIONS NONE;

-- Spec: written by deploy only.
DEFINE FIELD OVERWRITE name ON TABLE _00_schedule TYPE string;
DEFINE FIELD OVERWRITE kind ON TABLE _00_schedule TYPE string
    ASSERT $value INSIDE ['job', 'workflow'];
-- Exactly one of cron / every_ms is set (the CLI validates; the engine records
-- a last_error and stays inert if both or neither arrive).
DEFINE FIELD OVERWRITE cron ON TABLE _00_schedule TYPE option<string>;
DEFINE FIELD OVERWRITE every_ms ON TABLE _00_schedule TYPE option<int>;
-- IANA name; NONE means UTC. Cron is evaluated in this zone, so "daily at
-- 03:00" stays 03:00 local across DST transitions.
DEFINE FIELD OVERWRITE timezone ON TABLE _00_schedule TYPE option<string>;
-- Outbox table the spawned job rows are created in (resolved from the app's
-- `method.table` at deploy time, so the engine never reads sp00ky.yml).
DEFINE FIELD OVERWRITE target_table ON TABLE _00_schedule TYPE option<string>;
DEFINE FIELD OVERWRITE path ON TABLE _00_schedule TYPE option<string>;
DEFINE FIELD OVERWRITE payload ON TABLE _00_schedule TYPE option<object> FLEXIBLE;
-- Frozen DAG definition for kind = 'workflow': { steps: [...], on_failure }.
DEFINE FIELD OVERWRITE workflow ON TABLE _00_schedule TYPE option<object> FLEXIBLE;
-- Fan-out: a SurrealQL SELECT run on every fire; one run is spawned per row.
DEFINE FIELD OVERWRITE for_each ON TABLE _00_schedule TYPE option<string>;
-- Row field whose value keys the per-item concurrency check. Defaults to `id`.
DEFINE FIELD OVERWRITE for_each_key ON TABLE _00_schedule TYPE option<string>;
DEFINE FIELD OVERWRITE concurrency ON TABLE _00_schedule TYPE string DEFAULT 'skip'
    ASSERT $value INSIDE ['skip', 'allow', 'replace'];
DEFINE FIELD OVERWRITE max_retries ON TABLE _00_schedule TYPE option<int>;
DEFINE FIELD OVERWRITE retry_strategy ON TABLE _00_schedule TYPE option<string>;
DEFINE FIELD OVERWRITE timeout ON TABLE _00_schedule TYPE option<int>;
-- Wall-clock ceiling on ONE run of this schedule, in seconds. Unset means "use the
-- project default from _00_retention.run_deadline_secs".
--
-- Not the same thing as `timeout` above: that bounds a single HTTP request to a
-- backend, while this bounds the run as a whole -- every step, every job retry, and
-- every gap in between. Without it a run that stops making progress for a reason no
-- job-level timeout can see (a step that lost its dispatch, a step job row deleted
-- underneath it) stays `running` forever, and with the default `concurrency: skip`
-- that holds its fan-out key forever too.
DEFINE FIELD OVERWRITE deadline_secs ON TABLE _00_schedule TYPE option<int>;
-- Hash of the spec fields. Deploy skips the UPSERT when unchanged, and resets
-- next_fire_at when it moves so the engine replans against the new cadence.
DEFINE FIELD OVERWRITE spec_hash ON TABLE _00_schedule TYPE option<string>;
-- `enabled: false` in config. Distinct from `paused` so config intent and
-- operator intent can't overwrite each other.
DEFINE FIELD OVERWRITE config_disabled ON TABLE _00_schedule TYPE bool DEFAULT false;
-- Per-schedule retention override (`history:` in sp00ky.yml), in seconds. Unset
-- means "use the project default from _00_retention". A noisy minutely fan-out
-- wants minutes of history; a nightly report wants a month.
DEFINE FIELD OVERWRITE history_success_secs ON TABLE _00_schedule TYPE option<int>;
DEFINE FIELD OVERWRITE history_failed_secs ON TABLE _00_schedule TYPE option<int>;
-- `history: failures-only` in sp00ky.yml. A successful run then never persists at
-- all: a suppressed tick writes no row, and a run that completes successfully is
-- deleted as it finalizes. Its counts still land in _00_run_rollup, so the totals
-- survive even though no row ever does. NONE means 'all' (keep everything until
-- its retention window elapses).
DEFINE FIELD OVERWRITE history_mode ON TABLE _00_schedule TYPE option<string>
    ASSERT $value INSIDE ['all', 'failures-only'];

-- Consecutive failures of one forEach key before the gate stops firing it.
-- NONE (the default) means never: a schedule opts in, because turning a firing
-- behaviour on implicitly at upgrade is not a change anyone asked for. The
-- budget is read at the gate, never frozen onto a key, so retuning it applies
-- on the next fire.
DEFINE FIELD OVERWRITE quarantine_after ON TABLE _00_schedule TYPE option<int>
    ASSERT $value = NONE OR $value > 0;

-- Operator: written by the CLI only.
DEFINE FIELD OVERWRITE paused ON TABLE _00_schedule TYPE bool DEFAULT false;
DEFINE FIELD OVERWRITE trigger_requested_at ON TABLE _00_schedule TYPE option<datetime>;

-- Engine: written by the schedule sweep only.
DEFINE FIELD OVERWRITE next_fire_at ON TABLE _00_schedule TYPE option<datetime>;
DEFINE FIELD OVERWRITE last_fire_at ON TABLE _00_schedule TYPE option<datetime>;
-- Why the schedule couldn't be planned or fanned out (bad cron, failing
-- forEach query). Surfaced by `spky schedules list`.
DEFINE FIELD OVERWRITE last_error ON TABLE _00_schedule TYPE option<string>;
-- Outcome of the most recent run, denormalized off _00_schedule_run.
--
-- `spky schedules list` used to derive this by scanning the run table ordered by
-- fire_at with a LIMIT across ALL schedules, so one wide fan-out schedule pushed
-- every other schedule's last run out of the window within a minute. Keeping it
-- here makes the lookup O(1) and, more importantly, makes it survive retention:
-- the answer no longer depends on the run row still existing.
DEFINE FIELD OVERWRITE last_run_status ON TABLE _00_schedule TYPE option<string>;
DEFINE FIELD OVERWRITE last_run_at ON TABLE _00_schedule TYPE option<datetime>;

DEFINE FIELD OVERWRITE created_at ON TABLE _00_schedule TYPE datetime DEFAULT time::now() READONLY;
DEFINE FIELD OVERWRITE updated_at ON TABLE _00_schedule TYPE datetime VALUE time::now();

-- ==================================================
-- SPOOKY SCHEDULE RUN (history + per-key concurrency state)
-- One row per fire per fan-out item. The record id is derived from
-- (schedule, fire time, hashed key), which is what makes firing idempotent:
-- two tickers racing the same fire collide on the id instead of double-
-- spawning, and a duplicate CREATE is read as "already fired".
--
-- This table IS the concurrency state — `skip` asks it whether a run for this
-- key is still 'running' rather than consulting in-memory state, so the policy
-- survives restarts and works identically in singlenode and cluster mode.
-- schedule_name is denormalized so history outlives a deleted schedule.
-- ==================================================
DEFINE TABLE OVERWRITE _00_schedule_run SCHEMAFULL PERMISSIONS NONE;

DEFINE FIELD OVERWRITE schedule ON TABLE _00_schedule_run TYPE option<record<_00_schedule>>;
DEFINE FIELD OVERWRITE schedule_name ON TABLE _00_schedule_run TYPE string;
-- '' when the schedule doesn't fan out.
DEFINE FIELD OVERWRITE key ON TABLE _00_schedule_run TYPE string DEFAULT '';
DEFINE FIELD OVERWRITE fire_at ON TABLE _00_schedule_run TYPE datetime;
DEFINE FIELD OVERWRITE kind ON TABLE _00_schedule_run TYPE string
    ASSERT $value INSIDE ['job', 'workflow'];
DEFINE FIELD OVERWRITE status ON TABLE _00_schedule_run TYPE string DEFAULT 'running'
    ASSERT $value INSIDE ['running', 'success', 'failed', 'skipped', 'replaced', 'killed'];
-- 'cron' | 'manual' — how this fire was initiated.
DEFINE FIELD OVERWRITE trigger ON TABLE _00_schedule_run TYPE option<string>;
-- Spawned outbox job (kind = 'job'). A string, not a record link: the outbox
-- table varies per schedule, so a typed link isn't expressible here.
DEFINE FIELD OVERWRITE job_id ON TABLE _00_schedule_run TYPE option<string>;
DEFINE FIELD OVERWRITE workflow_run ON TABLE _00_schedule_run TYPE option<record<_00_workflow_run>>;
-- Snapshot of the forEach row this run was spawned for. NO LONGER WRITTEN: the
-- same row is on the spawned job's `payload.row` (and on `_00_workflow_run.input`
-- for workflow kinds), which is where every reader looks. A second copy here was
-- write-only and, at high fan-out, most of this table's bytes. Kept defined so
-- rows written by older deploys stay readable.
DEFINE FIELD OVERWRITE row ON TABLE _00_schedule_run TYPE option<object> FLEXIBLE;
DEFINE FIELD OVERWRITE error ON TABLE _00_schedule_run TYPE option<object> FLEXIBLE;
-- Copy of the schedule's `history_mode`, frozen at spawn like the workflow `dag`.
-- Finalizing a run has to know whether to keep it, and reading the schedule row
-- back for every completion would be a point read per fan-out item; this makes it
-- free, and means changing the config never changes the fate of a run already in
-- flight.
DEFINE FIELD OVERWRITE history_mode ON TABLE _00_schedule_run TYPE option<string>;
-- Resolved run deadline in seconds, frozen at spawn for the same reason as
-- `history_mode` above: the reaper has to bound a run without a point read of the
-- schedule row per in-flight run, and retuning (or deleting) the schedule must not
-- move the ceiling of a run already in flight. NONE means the project default
-- applies, which is what rows written before this field carry.
DEFINE FIELD OVERWRITE deadline_secs ON TABLE _00_schedule_run TYPE option<int>;
DEFINE FIELD OVERWRITE created_at ON TABLE _00_schedule_run TYPE datetime DEFAULT time::now() READONLY;
DEFINE FIELD OVERWRITE updated_at ON TABLE _00_schedule_run TYPE datetime VALUE time::now();
DEFINE FIELD OVERWRITE finished_at ON TABLE _00_schedule_run TYPE option<datetime>;

-- Backs the per-key concurrency count on every fire of a fanned-out schedule.
DEFINE INDEX OVERWRITE idx_srun_active ON TABLE _00_schedule_run
    COLUMNS schedule_name, key, status;
-- Reverse lookup for the job-terminal observer.
DEFINE INDEX OVERWRITE idx_srun_job ON TABLE _00_schedule_run COLUMNS job_id;
-- Backs the retention prune. Column order matters: the prune filters one status
-- by equality (an IndexScan) and then orders by finished_at, which this index
-- serves without a Sort. A range predicate on finished_at is NOT pushed into the
-- scan by SurrealDB, so the ordering is what makes the prune bounded rather than
-- a whole-partition filter.
DEFINE INDEX OVERWRITE idx_srun_retention ON TABLE _00_schedule_run
    COLUMNS status, finished_at;
-- `spky schedules list` / `get` / `runs` all sort by fire_at. Unindexed this was
-- a whole-table sort on every invocation.
DEFINE INDEX OVERWRITE idx_srun_fire ON TABLE _00_schedule_run COLUMNS fire_at;

-- ==================================================
-- SPOOKY WORKFLOW RUN (one DAG execution)
-- `dag` is a FROZEN copy of the definition taken at spawn, so redeploying a
-- workflow mid-run can never change the semantics of runs already in flight.
-- kill_requested is the operator's channel: the engine consumes it, sets the
-- run terminal, and kills the dispatched steps' jobs — the CLI never writes
-- run status itself.
-- ==================================================
DEFINE TABLE OVERWRITE _00_workflow_run SCHEMAFULL PERMISSIONS NONE;

DEFINE FIELD OVERWRITE schedule ON TABLE _00_workflow_run TYPE option<record<_00_schedule>>;
DEFINE FIELD OVERWRITE schedule_name ON TABLE _00_workflow_run TYPE option<string>;
DEFINE FIELD OVERWRITE workflow_name ON TABLE _00_workflow_run TYPE string;
DEFINE FIELD OVERWRITE dag ON TABLE _00_workflow_run TYPE object FLEXIBLE;
-- Default outbox table for steps that don't name one. Copied from the schedule at
-- spawn (rather than read back through the link) so deleting the schedule can't
-- strand a run mid-flight.
DEFINE FIELD OVERWRITE target_table ON TABLE _00_workflow_run TYPE option<string>;
-- forEach row or manual trigger payload; reachable from every step's payload.
DEFINE FIELD OVERWRITE input ON TABLE _00_workflow_run TYPE option<object> FLEXIBLE;
DEFINE FIELD OVERWRITE status ON TABLE _00_workflow_run TYPE string DEFAULT 'running'
    ASSERT $value INSIDE ['running', 'success', 'failed', 'killed'];
DEFINE FIELD OVERWRITE kill_requested ON TABLE _00_workflow_run TYPE bool DEFAULT false;
DEFINE FIELD OVERWRITE error ON TABLE _00_workflow_run TYPE option<object> FLEXIBLE;
-- Copy of the schedule's `history_mode`, frozen at spawn like the `dag` above. A
-- successful run is discarded as it finalizes, and by then its owning
-- _00_schedule_run may already be gone -- so the mode cannot be read back from
-- there and has to live here.
DEFINE FIELD OVERWRITE history_mode ON TABLE _00_workflow_run TYPE option<string>;
-- Frozen resolved run deadline, as on `_00_schedule_run`. NONE = project default.
DEFINE FIELD OVERWRITE deadline_secs ON TABLE _00_workflow_run TYPE option<int>;
-- How this run was started: 'cron' for a schedule fire, 'manual' for an operator
-- rerun. NONE on rows written before the field existed; readers treat that as
-- 'cron'.
DEFINE FIELD OVERWRITE trigger ON TABLE _00_workflow_run TYPE option<string>;
-- The run an operator reran to produce this one. A link rather than a copy: the
-- source may be pruned later, and a dangling link is harmless.
DEFINE FIELD OVERWRITE rerun_of ON TABLE _00_workflow_run TYPE option<record<_00_workflow_run>>;
-- Operator retries applied to this run. Doubles as the attempt number baked into
-- step job ids, so a retried step never collides with its previous job row (an
-- SSP keeps kill flags keyed by job id, so re-creating the same id would fail on
-- arrival).
--
-- `option<int>`, not `int`: SurrealDB applies DEFAULT only on CREATE and
-- re-validates the whole row on every UPDATE, so a non-option field makes every
-- write to a run created before the field existed fail (cancel, the kill flag,
-- the engine's own finalize). Readers treat NONE as 0.
DEFINE FIELD OVERWRITE retry_count ON TABLE _00_workflow_run TYPE option<int> DEFAULT 0;
DEFINE FIELD OVERWRITE last_retry_at ON TABLE _00_workflow_run TYPE option<datetime>;
-- Backfill rows from before `retry_count` existed. This file is applied by every
-- `spky migrate` / `spky deploy`, so this runs once per table state and is a
-- no-op afterwards.
UPDATE _00_workflow_run SET retry_count = 0 WHERE retry_count = NONE;
DEFINE FIELD OVERWRITE created_at ON TABLE _00_workflow_run TYPE datetime DEFAULT time::now() READONLY;
DEFINE FIELD OVERWRITE updated_at ON TABLE _00_workflow_run TYPE datetime VALUE time::now();
DEFINE FIELD OVERWRITE finished_at ON TABLE _00_workflow_run TYPE option<datetime>;

DEFINE INDEX OVERWRITE idx_wfrun_status ON TABLE _00_workflow_run COLUMNS status;
DEFINE INDEX OVERWRITE idx_wfrun_sched ON TABLE _00_workflow_run COLUMNS schedule_name;
-- Retention prune, same shape as idx_srun_retention: status equality then
-- finished_at ordering. Supersedes idx_wfrun_status for the prune, which is kept
-- for the heal pass's `status = 'running'` lookup.
DEFINE INDEX OVERWRITE idx_wfrun_retention ON TABLE _00_workflow_run
    COLUMNS status, finished_at;
-- The reaper's scan: `status = 'running'` by equality, then oldest-first by
-- `created_at`. Same column-order reasoning as idx_srun_retention above -- a range
-- predicate on the second column is not pushed into the scan, so the ordering is
-- what keeps this an IndexScan with a LIMIT rather than a whole-partition filter.
DEFINE INDEX OVERWRITE idx_wfrun_deadline ON TABLE _00_workflow_run
    COLUMNS status, created_at;

-- ==================================================
-- SPOOKY STEP RUN (one node of one workflow run)
-- status drives the whole DAG: a step goes blocked -> ready -> dispatched ->
-- success/failed, or straight to skipped when an ancestor fails. Advancement
-- is idempotent because the blocked -> ready promotion is a compare-and-swap:
-- whichever pass wins it is the one that creates the step's job, so two
-- concurrent advancements can't double-dispatch.
-- depends_on is frozen from the DAG so readiness is a cheap local check.
-- ==================================================
DEFINE TABLE OVERWRITE _00_step_run SCHEMAFULL PERMISSIONS NONE;

DEFINE FIELD OVERWRITE workflow_run ON TABLE _00_step_run TYPE record<_00_workflow_run>;
DEFINE FIELD OVERWRITE step ON TABLE _00_step_run TYPE string;
DEFINE FIELD OVERWRITE depends_on ON TABLE _00_step_run TYPE array<string> DEFAULT [];
DEFINE FIELD OVERWRITE status ON TABLE _00_step_run TYPE string DEFAULT 'blocked'
    ASSERT $value INSIDE ['blocked', 'ready', 'dispatched', 'success', 'failed', 'skipped'];
DEFINE FIELD OVERWRITE job_id ON TABLE _00_step_run TYPE option<string>;
-- Dispatch attempts made for this step, opened by the `blocked -> ready`
-- promotion and incremented by every recovery re-dispatch. It is the give-up
-- counter for a step that is past `blocked` but has no job to show for it:
-- nothing else in the engine can rescue such a step (`ready_steps` yields only
-- `blocked` steps and CLAIM_STEP_READY guards on `blocked`), so without a bound a
-- lost dispatch either sits forever or retries forever.
--
-- `option<int>`, not `int`, and the same for `started_at` below -- see the note on
-- `_00_workflow_run.retry_count`: DEFAULT applies only on CREATE while every
-- UPDATE re-validates the whole row, so a non-option field breaks every write to a
-- step row created before the field existed. Readers treat NONE as 0.
DEFINE FIELD OVERWRITE dispatch_attempts ON TABLE _00_step_run TYPE option<int> DEFAULT 0;
-- When the step's job was created. `created_at` is when the step ROW was made,
-- which for a step deep in a DAG is the run's own start -- so it cannot answer
-- "how long has this step been running".
DEFINE FIELD OVERWRITE started_at ON TABLE _00_step_run TYPE option<datetime>;
-- The step job's captured `result`, handed to dependent steps' payloads.
-- `any` rather than `option<any>`: `any` already admits NONE.
DEFINE FIELD OVERWRITE output ON TABLE _00_step_run TYPE any;
DEFINE FIELD OVERWRITE error ON TABLE _00_step_run TYPE option<object> FLEXIBLE;
DEFINE FIELD OVERWRITE created_at ON TABLE _00_step_run TYPE datetime DEFAULT time::now() READONLY;
DEFINE FIELD OVERWRITE updated_at ON TABLE _00_step_run TYPE datetime VALUE time::now();
DEFINE FIELD OVERWRITE finished_at ON TABLE _00_step_run TYPE option<datetime>;

-- Backfill rows from before the two fields above existed. Idempotent, and this
-- file is re-applied by every `spky migrate` / `spky deploy`.
UPDATE _00_step_run SET dispatch_attempts = 0 WHERE dispatch_attempts = NONE;

DEFINE INDEX OVERWRITE idx_step_wf ON TABLE _00_step_run COLUMNS workflow_run, step UNIQUE;
DEFINE INDEX OVERWRITE idx_step_job ON TABLE _00_step_run COLUMNS job_id;
DEFINE INDEX OVERWRITE idx_step_status ON TABLE _00_step_run COLUMNS workflow_run, status;

-- ==================================================
-- SPOOKY RETENTION (one singleton row: retention policy + prune targets)
-- Written by deploy, read by the engine's prune pass.
--
-- The policy lives in the database rather than in an env var so that singlenode,
-- cluster, and Cloudflare deployments all read the identical thing without a new
-- variable threaded through three shells (and through the separate cloud control
-- plane, which assembles the SSP's environment server-side). It also means an
-- operator can retune retention with one UPDATE and no redeploy, the same way
-- `spky schedules pause` works.
--
-- Retention is asymmetric by outcome, not by table: a successful run is read once
-- (if at all) and then never again, while a failure is the thing you came to look
-- for. `skipped` and `replaced` count as successes here — with the default
-- `concurrency: skip` a slow wide fan-out writes one `skipped` row per item per
-- fire, which is the highest-volume, lowest-information row in the system.
--
-- Pending and processing rows are NEVER pruned at any age.
-- ==================================================
DEFINE TABLE OVERWRITE _00_retention SCHEMAFULL PERMISSIONS NONE;

-- Outbox job rows, keyed off `updated_at` (a terminal job's last write IS its
-- terminalization). Deliberately not `created_at`: that field is only the
-- creation time, so a job that waited on a delay or burned retries would be
-- judged by when it was queued rather than when it finished.
DEFINE FIELD OVERWRITE success_secs ON TABLE _00_retention TYPE int DEFAULT 21600;
DEFINE FIELD OVERWRITE failed_secs ON TABLE _00_retention TYPE int DEFAULT 1209600;

-- `_00_schedule_run` / `_00_workflow_run` history, keyed off `finished_at`.
-- The failed window matches the previous hardcoded 30 days, so upgrading can
-- never drop failure history a project already had.
DEFINE FIELD OVERWRITE run_success_secs ON TABLE _00_retention TYPE int DEFAULT 86400;
DEFINE FIELD OVERWRITE run_failed_secs ON TABLE _00_retention TYPE int DEFAULT 2592000;

-- Wall-clock ceiling on ONE run, in seconds, for schedules that do not set their
-- own `deadline:`. A run past this is failed `deadline_exceeded` by the reaper and
-- its jobs are killed.
--
-- One hour is deliberately generous: this is a backstop against a run that is
-- STUCK, not a service-level objective. A schedule with genuinely long runs sets
-- its own `deadline:` rather than raising this, because raising this also raises
-- how long one wedged key can hold up a whole fan-out under `concurrency: skip`.
--
-- 0 disables the reaper entirely. That is a supported answer -- it is the behaviour
-- every deployment had before this field existed -- and a bad default.
DEFINE FIELD OVERWRITE run_deadline_secs ON TABLE _00_retention TYPE int DEFAULT 3600;

-- Hard cap on rows in a trimmable (success-shaped) status, per target. 0 = off.
-- The windows above already bound volume at rate x window; this is the valve for
-- the pathological case where even a short window holds more than the deployment
-- can carry. Checked on a slow multiple of the prune cadence because it has to
-- COUNT, and it never trims a failure — a volume cap must not be the reason a
-- failure disappears.
DEFINE FIELD OVERWRITE max_rows ON TABLE _00_retention TYPE int DEFAULT 0;

-- Outbox tables the prune pass may sweep, written by deploy from each app's
-- `method.table`. The engine therefore never reads sp00ky.yml or SPKY_JOB_CONFIG
-- to find them, and a table that is not listed here is never touched.
DEFINE FIELD OVERWRITE job_tables ON TABLE _00_retention TYPE array<string> DEFAULT [];
DEFINE FIELD OVERWRITE job_tables[*] ON TABLE _00_retention TYPE string;

DEFINE FIELD OVERWRITE updated_at ON TABLE _00_retention TYPE datetime VALUE time::now();

-- ==================================================
-- SPOOKY RUN ROLLUP (permanent hourly counters for what retention deleted)
-- One row per (scope, name, hour). Written by the prune pass immediately before
-- it deletes a batch, so the counts are a by-product of work already being done
-- rather than a second pass over the data.
--
-- This is what makes short retention windows acceptable: the raw rows go, but
-- "how much ran, and how much of it failed" survives forever at a few rows a day.
-- Bounded by construction — 24 rows per day per (scope, name).
--
-- KNOWN GAP: only the prune pass folds into this table. Rows removed by
-- `spky jobs clear`, by an application's own DELETE, or by a build that predates
-- this table are not counted, so a rollup total is a lower bound on what really
-- ran. It is not an audit log.
-- ==================================================
DEFINE TABLE OVERWRITE _00_run_rollup SCHEMAFULL PERMISSIONS NONE;

-- 'schedule' counts _00_schedule_run / _00_workflow_run rows and `name` is the
-- schedule name; 'job' counts outbox rows and `name` is the table name.
DEFINE FIELD OVERWRITE scope ON TABLE _00_run_rollup TYPE string
    ASSERT $value INSIDE ['schedule', 'job'];
DEFINE FIELD OVERWRITE name ON TABLE _00_run_rollup TYPE string;
-- Start of the hour the pruned rows finished in.
DEFINE FIELD OVERWRITE bucket ON TABLE _00_run_rollup TYPE datetime;

-- One named int per outcome rather than a map: `+=` on a named field is a single
-- atomic SET, whereas incrementing a computed key inside an object is not
-- expressible in one statement — and a blind atomic UPSERT is what lets two
-- pruners fold concurrently without a read-modify-write race.
DEFINE FIELD OVERWRITE success ON TABLE _00_run_rollup TYPE int DEFAULT 0;
DEFINE FIELD OVERWRITE failed ON TABLE _00_run_rollup TYPE int DEFAULT 0;
DEFINE FIELD OVERWRITE skipped ON TABLE _00_run_rollup TYPE int DEFAULT 0;
DEFINE FIELD OVERWRITE replaced ON TABLE _00_run_rollup TYPE int DEFAULT 0;
DEFINE FIELD OVERWRITE killed ON TABLE _00_run_rollup TYPE int DEFAULT 0;

DEFINE FIELD OVERWRITE updated_at ON TABLE _00_run_rollup TYPE datetime VALUE time::now();

DEFINE INDEX OVERWRITE idx_rollup_lookup ON TABLE _00_run_rollup
    COLUMNS scope, name, bucket;

-- ==================================================
-- SPOOKY SCHEDULE KEY (per-forEach-key failure budget)
-- One row per (schedule, forEach key), id
-- `_00_schedule_key:<schedule>_<hash(key)>`.
--
-- The engine holds no state across fires, which is usually the right call: a
-- fire is decided entirely from the schedule row and the forEach query. But it
-- means a forEach row that ALWAYS fails is re-selected forever. Nothing in the
-- chain accumulates evidence: `concurrency: skip` only blocks on a run still
-- `running`, `last_error` is only written when the fan-out query itself errors,
-- and a query returning a poison row is a perfectly successful query. So one
-- dead row costs a job every tick, indefinitely, and keeps the outbox's failed
-- bucket permanently non-empty, which is where a real failure would have shown.
--
-- This is the one piece of per-key memory, and it holds exactly one number.
-- Whether that number means "quarantined" is DERIVED at the gate from the
-- schedule's current `quarantine_after`, so raising or lowering the budget in
-- `sp00ky.yml` takes effect on the next fire with nothing to migrate.
--
-- `skipped` and `replaced` touch neither counter: a suppressed fire is evidence
-- of nothing. Only a run that actually executed moves it.
-- ==================================================
DEFINE TABLE OVERWRITE _00_schedule_key SCHEMAFULL PERMISSIONS NONE;

DEFINE FIELD OVERWRITE schedule_name ON TABLE _00_schedule_key TYPE string;
DEFINE FIELD OVERWRITE key ON TABLE _00_schedule_key TYPE string DEFAULT '';
DEFINE FIELD OVERWRITE consecutive_failures ON TABLE _00_schedule_key TYPE int DEFAULT 0;
-- 'success' | 'failed' — the last outcome that actually executed, or 'probe'
-- while a quarantined key's probe fire is out. That write also restarts
-- `updated_at`, which is the clock the probe backoff is measured on.
DEFINE FIELD OVERWRITE last_status ON TABLE _00_schedule_key TYPE option<string>;
-- Set when the gate first suppresses this key, cleared when it lets it through
-- again under budget (after a passing probe or a release zeroed the count). It
-- is not what makes a key quarantined (the count and the budget are);
-- it is the record that an operator has already been told, so the incident
-- opens and closes once rather than on every tick.
DEFINE FIELD OVERWRITE quarantined_at ON TABLE _00_schedule_key TYPE option<datetime>;
DEFINE FIELD OVERWRITE updated_at ON TABLE _00_schedule_key TYPE datetime VALUE time::now();

-- `spky schedules get` lists a schedule's quarantined keys.
DEFINE INDEX OVERWRITE idx_skey_schedule ON TABLE _00_schedule_key
    COLUMNS schedule_name, quarantined_at;

-- ==================================================
-- SPOOKY JOB POLICY (per-outbox-table execution limits)
-- One row per outbox table, id `_00_job_policy:<table>`, written by deploy from
-- each app's `method.concurrency`.
--
-- Lives in the database for the same reason `_00_retention` does: singlenode,
-- cluster, and Cloudflare all read the identical thing without another variable
-- threaded through three shells and the separate cloud control plane, which
-- assembles the SSP's environment server-side. And a throttle is exactly the
-- setting you want to retune live — the reason you reach for one is that a
-- backend is falling over right now, not next deploy.
--
-- `concurrency` bounds how many of this table's jobs are `processing` at once.
-- Everything above the bound stays `pending` in the outbox and is admitted in
-- `created_at` order as slots free: the queue is the table itself, so a backlog
-- survives a restart and needs no new status. Absent row => 1, which is what the
-- runner did before this existed.
--
-- Enforced per SSP process and, in cluster mode, best-effort across processes by
-- counting non-stale `processing` rows. A single-node deployment is therefore
-- exact.
-- ==================================================
DEFINE TABLE OVERWRITE _00_job_policy SCHEMAFULL PERMISSIONS NONE;

-- `> 0`, not `>= 0`: `0` would read as "pause this table", and there is no
-- unpause UX to go with it. Pausing a backend is `spky schedules pause` or a
-- deploy, not a silently wedged outbox.
DEFINE FIELD OVERWRITE concurrency ON TABLE _00_job_policy TYPE int DEFAULT 1
    ASSERT $value > 0;

DEFINE FIELD OVERWRITE updated_at ON TABLE _00_job_policy TYPE datetime VALUE time::now();
