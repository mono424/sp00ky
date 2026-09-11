-- ##################################################################
-- API OUTBOX TABLE
-- ##################################################################

DEFINE TABLE job SCHEMAFULL
PERMISSIONS
  FOR select, create, update, delete WHERE $access = "account" AND assigned_to.author.id = $auth.id;

-- The thread this job belongs to, used by the table permission above. OPTIONAL
-- on purpose: the `schedules:` entries in sp00ky.yml spawn system-initiated jobs
-- that belong to no single thread, so the scheduler creates rows without it.
-- Requiring it makes every scheduled fire fail to spawn with
-- "Expected `record<thread>` but found `NONE`". Such rows are also invisible to
-- clients, which is what you want for system work.
DEFINE FIELD assigned_to ON TABLE job TYPE option<record<thread>>
PERMISSIONS
  FOR create, select WHERE true
  FOR update WHERE false;

DEFINE FIELD path ON TABLE job TYPE string
PERMISSIONS
  FOR create, select WHERE true
  FOR update WHERE false;

DEFINE FIELD payload ON TABLE job TYPE any
PERMISSIONS
  FOR create, select WHERE true
  FOR update WHERE false;

DEFINE FIELD retries ON TABLE job TYPE int DEFAULT ALWAYS 0
PERMISSIONS
  FOR create, select WHERE true
  FOR update WHERE false;

DEFINE FIELD max_retries ON TABLE job TYPE int DEFAULT ALWAYS 3;

-- Minimum time (milliseconds) the job stays pending before it is eligible to run.
DEFINE FIELD delay ON TABLE job TYPE int DEFAULT ALWAYS 0
PERMISSIONS
  FOR create, select WHERE true
  FOR update WHERE false;

DEFINE FIELD retry_strategy ON TABLE job TYPE string DEFAULT ALWAYS "linear"
ASSERT $value IN ["linear", "exponential"]
PERMISSIONS
  FOR create, select WHERE true
  FOR update WHERE false;

DEFINE FIELD status ON TABLE job TYPE string DEFAULT ALWAYS "pending"
ASSERT $value IN ["pending", "processing", "success", "failed"]
PERMISSIONS
  FOR create, select WHERE true
  FOR update WHERE false;

-- Owning SSP node id (ssp_id) once a job is accepted; used by cluster recovery
-- to detect orphaned jobs. Written server-side (root) only, never by clients.
DEFINE FIELD assignee ON TABLE job TYPE option<string>
PERMISSIONS
  FOR select WHERE true
  FOR create, update WHERE false;

-- Lease expiry for the attempt holding this row, and the fencing token that
-- attempt presents to write its outcome. `assignee` says WHO claimed it; these say
-- UNTIL WHEN, which is the half that makes a stuck job recoverable -- reclaim keys
-- off `lease_until` alone, never off whether the assignee still looks alive.
-- `lease_epoch` is bumped on every claim and every reclaim, and the runner's writes
-- are guarded on the epoch they claimed under, so a reclaimed job's original
-- attempt cannot overwrite the new one when it wakes up.
-- Written server-side (root) only.
DEFINE FIELD lease_until ON TABLE job TYPE option<datetime>
PERMISSIONS
  FOR select WHERE true
  FOR create, update WHERE false;

DEFINE FIELD lease_epoch ON TABLE job TYPE option<int>
PERMISSIONS
  FOR select WHERE true
  FOR create, update WHERE false;

DEFINE FIELD errors[*] ON TABLE job TYPE object FLEXIBLE;
DEFINE FIELD errors ON TABLE job TYPE array<object> DEFAULT ALWAYS []
PERMISSIONS
  FOR create WHERE true
  FOR select, update WHERE false;

-- Set on create, and thereafter only when a writer sets it explicitly. The
-- recovery sweeps' staleness clocks read it, and the SSP's assignee stamp
-- deliberately leaves it alone, so this must not become `VALUE time::now()`.
DEFINE FIELD updated_at ON TABLE job TYPE datetime
DEFAULT ALWAYS time::now()
PERMISSIONS
  FOR create, select WHERE true
  FOR update WHERE false;

-- `DEFAULT`, deliberately NOT `VALUE`: a `VALUE` field is recomputed on every
-- UPDATE, which would make `created_at` mean "last modified" and slide the
-- `created_at + delay` job-due clause forward on any write to a pending row.
DEFINE FIELD created_at ON TABLE job TYPE datetime
DEFAULT time::now()
PERMISSIONS
  FOR create, select WHERE true
  FOR update WHERE false;

-- Per-job HTTP timeout in SECONDS, overriding the backend default. Set by
-- `db.run(..., { timeout })` and by a schedule's `timeout:` (the CLI converts that
-- duration to seconds at deploy time). Seconds, not milliseconds: every consumer
-- reads it through `Duration::from_secs`, and it also sizes the reclaim lease, so a
-- value written as ms would make the row unreclaimable for months.
-- The field must exist:
-- the scheduler puts it in the job content whenever the schedule declares one, and
-- a SCHEMAFULL table without it rejects the whole CREATE with
-- "Found field 'timeout', but no such field exists".
DEFINE FIELD timeout ON TABLE job TYPE option<int>
PERMISSIONS
  FOR create, select WHERE true
  FOR update WHERE false;

-- ── Indexes ────────────────────────────────────────────────────────────────
--
-- `(status, created_at)` is the shape the dispatcher's drain asks for on EVERY
-- job completion (`SELECT ... WHERE status = 'pending' ORDER BY created_at`),
-- and the shape the retention prune and the admin plane's queue counts ask for
-- too. Without it all three are full scans of a table that, on a busy project,
-- is the largest one there is.
--
-- `updated_at` carries the other two: the prune ages terminal rows by their last
-- write, and the Jobs page lists by it.
--
-- `IF NOT EXISTS`, never `OVERWRITE`: an `OVERWRITE` here would rebuild both
-- indexes on every single `spky deploy`.
DEFINE INDEX IF NOT EXISTS idx_job_dispatch ON TABLE job COLUMNS status, created_at;
DEFINE INDEX IF NOT EXISTS idx_job_activity ON TABLE job COLUMNS updated_at;
