-- ==================================================
-- SPOOKY MACHINE POOLS
-- Jobs that run on their own, autoscaled machines. Split into its own file for
-- the same reason schedule_tables.surql is: `packages/pool-core` include_str!s
-- exactly the DDL that ships and runs its engine tests against it.
-- Applied by the same paths as the other meta tables.
--
-- Two tables, and what is deliberately NOT here matters as much as what is:
-- there is no job-to-machine binding table. A pool job is bound to a machine by
-- the outbox row itself (`status = 'processing'` AND `assignee = <machine id>`,
-- under the row's existing lease + fencing token). Occupancy, free slots and
-- "which jobs does this machine own" are all derived from the outbox rows, so
-- there is no second copy of the truth to drift or to repair after a crash.
-- ==================================================

-- ==================================================
-- SPOOKY POOL (one row per `pools:` entry in sp00ky.yml, id `_00_pool:<name>`)
--
-- Three writers, three disjoint field groups (same discipline as _00_schedule):
--   deploy   writes the SPEC fields + spec_hash. It never writes `paused`.
--   operator (`spky pools pause|resume`) writes `paused`, and nothing else.
--   engine   (the pool sweep in the cluster scheduler) writes boot_failures /
--            breaker_until / last_error, and nothing else.
-- ==================================================
DEFINE TABLE OVERWRITE _00_pool SCHEMAFULL PERMISSIONS NONE;

-- Spec: written by deploy only.
DEFINE FIELD OVERWRITE name ON TABLE _00_pool TYPE string;
-- Where machines come from. `docker`: containers on the scheduler's own docker
-- host (dev, and light jobs). `hetzner`: a VM per machine, created by the cloud
-- control plane on the tenant's behalf (the tenant never holds cloud credentials).
DEFINE FIELD OVERWRITE provider ON TABLE _00_pool TYPE string
    ASSERT $value INSIDE ['docker', 'hetzner'];
DEFINE FIELD OVERWRITE machine_type ON TABLE _00_pool TYPE option<string>;
-- Ordered fallbacks; empty means "the provider's default".
DEFINE FIELD OVERWRITE locations ON TABLE _00_pool TYPE array<string> DEFAULT [];
-- Concurrent jobs per machine. 1 = a job owns its machine.
DEFINE FIELD OVERWRITE slots ON TABLE _00_pool TYPE int DEFAULT 1 ASSERT $value > 0;
-- Baseline that always runs. With autoscale off this IS the pool size.
DEFINE FIELD OVERWRITE min ON TABLE _00_pool TYPE int DEFAULT 0 ASSERT $value >= 0;
DEFINE FIELD OVERWRITE autoscale ON TABLE _00_pool TYPE bool DEFAULT false;
-- Hard ceiling on live machines. Required by the CLI when autoscale is on.
DEFINE FIELD OVERWRITE max ON TABLE _00_pool TYPE option<int>;
-- Ready, idle machines kept ABOVE what is in use. Ignored with autoscale off.
DEFINE FIELD OVERWRITE buffer ON TABLE _00_pool TYPE int DEFAULT 0 ASSERT $value >= 0;
-- How long surplus idle capacity may linger before it is drained.
DEFINE FIELD OVERWRITE idle_timeout_secs ON TABLE _00_pool TYPE int DEFAULT 600;
-- `job`: the agent restarts the backend process after every job. `never`: reuse it.
DEFINE FIELD OVERWRITE recycle ON TABLE _00_pool TYPE string DEFAULT 'job'
    ASSERT $value INSIDE ['job', 'never'];
-- Hard stop for ONE attempt, and for one machine.
DEFINE FIELD OVERWRITE max_job_duration_secs ON TABLE _00_pool TYPE int DEFAULT 28800;
DEFINE FIELD OVERWRITE max_lifetime_secs ON TABLE _00_pool TYPE int DEFAULT 86400;
-- Job lease, renewed by every agent poll. This is the failover knob: a machine
-- that stops polling loses its jobs after this long. The CLI enforces a sane
-- floor; the DDL only rejects nonsense so tests can use short leases.
DEFINE FIELD OVERWRITE lease_secs ON TABLE _00_pool TYPE int DEFAULT 90 ASSERT $value > 0;
-- How long a machine may take from creation to its agent reporting ready.
DEFINE FIELD OVERWRITE boot_timeout_secs ON TABLE _00_pool TYPE int DEFAULT 300;
-- The one backend this pool runs (v1: one backend per pool), its outbox table,
-- and how to run it: { image, cmd, port, healthcheck, env, workdir }.
DEFINE FIELD OVERWRITE backend ON TABLE _00_pool TYPE string;
DEFINE FIELD OVERWRITE target_table ON TABLE _00_pool TYPE string;
DEFINE FIELD OVERWRITE container ON TABLE _00_pool TYPE object FLEXIBLE DEFAULT {};
-- Hash of everything above. A machine remembers the hash it was created under;
-- a mismatch is how the engine knows to roll it after a deploy.
DEFINE FIELD OVERWRITE spec_hash ON TABLE _00_pool TYPE string;

-- Operator: stop assigning and stop scaling up. Running jobs finish.
DEFINE FIELD OVERWRITE paused ON TABLE _00_pool TYPE bool DEFAULT false;

-- Engine: the boot-failure circuit breaker. A pool whose machines keep dying
-- before they are ready stops spawning for a while instead of burning money.
DEFINE FIELD OVERWRITE boot_failures ON TABLE _00_pool TYPE int DEFAULT 0;
DEFINE FIELD OVERWRITE breaker_until ON TABLE _00_pool TYPE option<datetime>;
DEFINE FIELD OVERWRITE last_error ON TABLE _00_pool TYPE option<string>;
DEFINE FIELD OVERWRITE updated_at ON TABLE _00_pool TYPE datetime VALUE time::now();

-- ==================================================
-- SPOOKY MACHINE (one row per machine ever requested, id `_00_machine:<id>`)
--
--   requested   row written, provider create not confirmed yet. The id doubles
--               as the provider idempotency key, so a crash here re-creates the
--               SAME machine instead of a second one.
--   booting     the provider has it; waiting for the agent to report ready.
--   ready       agent is polling; accepts jobs while it has free slots.
--   draining    accepts no new jobs; terminated as soon as it is empty.
--   terminating destroy requested at the provider.
--   gone        destroyed (terminal).
--   failed      never became ready, or stopped polling (terminal).
--
-- "busy" is not a state: it is `ready` with at least one bound job. busy_slots
-- is a cache of that count for the admin UI, recomputed every sweep.
-- ==================================================
DEFINE TABLE OVERWRITE _00_machine SCHEMAFULL PERMISSIONS NONE;

DEFINE FIELD OVERWRITE pool ON TABLE _00_machine TYPE string;
DEFINE FIELD OVERWRITE state ON TABLE _00_machine TYPE string DEFAULT 'requested'
    ASSERT $value INSIDE ['requested', 'booting', 'ready', 'draining', 'terminating', 'gone', 'failed'];
DEFINE FIELD OVERWRITE provider ON TABLE _00_machine TYPE string;
DEFINE FIELD OVERWRITE provider_id ON TABLE _00_machine TYPE option<string>;
DEFINE FIELD OVERWRITE slots ON TABLE _00_machine TYPE int DEFAULT 1;
DEFINE FIELD OVERWRITE busy_slots ON TABLE _00_machine TYPE int DEFAULT 0;
-- The pool spec_hash this machine was created under.
DEFINE FIELD OVERWRITE spec_hash ON TABLE _00_machine TYPE string;
DEFINE FIELD OVERWRITE created_at ON TABLE _00_machine TYPE datetime DEFAULT time::now();
DEFINE FIELD OVERWRITE ready_at ON TABLE _00_machine TYPE option<datetime>;
-- Agent heartbeat, stamped with the DATABASE clock on every poll.
DEFINE FIELD OVERWRITE last_seen ON TABLE _00_machine TYPE option<datetime>;
-- Set when the machine last became empty, cleared while it holds a job. The
-- scale-down clock.
DEFINE FIELD OVERWRITE idle_since ON TABLE _00_machine TYPE option<datetime>;
DEFINE FIELD OVERWRITE ended_at ON TABLE _00_machine TYPE option<datetime>;
-- Why it is being (or was) taken away, and whether that was a failure. A lost or
-- never-booted machine still has to be destroyed at the provider, so it passes
-- through `terminating` like any other and only then lands in `failed`: the
-- destroy is retried until it sticks, and `failure` remembers how to file it.
DEFINE FIELD OVERWRITE reason ON TABLE _00_machine TYPE option<string>;
DEFINE FIELD OVERWRITE failure ON TABLE _00_machine TYPE bool DEFAULT false;
-- Whatever the agent reports about itself: version, cpu, memory, net counters.
DEFINE FIELD OVERWRITE agent ON TABLE _00_machine TYPE option<object> FLEXIBLE;
DEFINE FIELD OVERWRITE updated_at ON TABLE _00_machine TYPE datetime VALUE time::now();

-- The sweep reads "this pool's live machines" every pass; retention reads
-- terminal rows by age.
DEFINE INDEX OVERWRITE idx_machine_pool_state ON TABLE _00_machine COLUMNS pool, state;
DEFINE INDEX OVERWRITE idx_machine_retention ON TABLE _00_machine COLUMNS state, ended_at;
