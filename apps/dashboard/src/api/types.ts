/** Wire types for the scheduler's `/admin/api` surface. */

export type SessionMode = 'roster' | 'breakglass' | 'mcp';

/** What a session may do. Roster and break-glass sessions are always `full`. */
export type SessionScope = 'read' | 'full';

export interface ServerConfig {
  scheduler_id: string;
  version: string;
  /** Whether `SPKY_ADMIN_PASSWORD` is set, i.e. password-only login is offered. */
  breakglass_available: boolean;
  /**
   * Whether this scheduler can reach the Sp00ky Cloud control plane. Off for a
   * self-hosted scheduler, and for a cloud tenant that has not been restarted
   * since the link was introduced. Cloud-only actions are offered disabled,
   * with that reason, rather than hidden.
   */
  cloud_linked?: boolean;
  /** The project slug this scheduler serves; names the MCP server entry. */
  project_slug?: string;
  /** Whether tokens outlive the process (true when `SPKY_AUTH_SECRET` is set). */
  sessions_persistent?: boolean;
  /**
   * Whether something will relaunch the process after it exits. A scheduler
   * run from a checkout is not supervised, and a restart from the dashboard
   * would simply stop it.
   */
  supervised?: boolean;
}

export interface LoginResponse {
  token: string;
  mode: SessionMode;
  subject: string;
  label: string;
  expires_in_secs: number;
  scope?: SessionScope;
}

export interface MeResponse {
  subject: string;
  label: string;
  mode: SessionMode;
  scope?: SessionScope;
}

/** `POST /admin/api/tokens`: a long-lived MCP token, shown exactly once. */
export interface TokenResponse {
  token: string;
  label: string;
  scope: SessionScope;
  expires_at: string;
  /** Path of the MCP endpoint on this scheduler, e.g. `/admin/api/mcp`. */
  endpoint: string;
}

/** One entry of the MCP server's `tools/list`. */
export interface McpTool {
  name: string;
  description: string;
  inputSchema: unknown;
  annotations?: {
    title?: string;
    readOnlyHint?: boolean;
    destructiveHint?: boolean;
  };
}

/** One end-to-end heartbeat probe cycle. `ms` is null for a failed cycle. */
export interface HeartbeatSample {
  ts: number;
  ms: number | null;
  ok: boolean;
}

export interface HeartbeatInfo {
  enabled?: boolean;
  /** Seconds between probes. */
  interval_secs?: number;
  last_e2e_ms?: number | null;
  last_ok_epoch_ms?: number | null;
  last_attempt_epoch_ms?: number | null;
  consecutive_failures?: number;
  stale?: boolean;
  blocked_reason?: string | null;
  samples?: HeartbeatSample[];
}

export type SchedulerStatus =
  | 'cloning'
  | 'ready'
  | 'frozen'
  | 'updating'
  | 'restoring';

export interface SchedulerEntity {
  entity: 'scheduler';
  id: string;
  ip: string | null;
  status: SchedulerStatus;
  views: number;
  version: string;
  surrealdb_version: string;
  uptime_seconds: number;
  pending_events: number;
  snapshot_seq: number;
  latest_seq: number;
  lag: number;
  heartbeat: HeartbeatInfo;
  env: Record<string, string>;
}

export type SspStatus = 'bootstrapping' | 'replaying' | 'ready' | 'unknown';

/** Populated only while an SSP is loading its circuit. */
export interface BootstrapProgress {
  tables_done: number;
  tables_total: number;
  rows_loaded: number;
  current_table?: string | null;
}

export interface SspEntity {
  entity: 'ssp';
  id: string;
  ip: string | null;
  status: SspStatus;
  views: number;
  version: string;
  uptime_seconds: number;
  last_heartbeat_seconds_ago: number;
  /** Seconds in the current phase. Null when the scheduler has no record. */
  state_seconds: number | null;
  buffered_events: number;
  bootstrap: BootstrapProgress | null;
  env: Record<string, string> | null;
}

export type BackendStatus = 'healthy' | 'unhealthy' | 'unreachable' | 'unknown';

export interface BackendSummary {
  /**
   * The overview serialises the registry entity, which carries `id`; the
   * backends routes answer with `name`. Read whichever is present.
   */
  name: string;
  id?: string;
  url: string;
  ip: string | null;
  port: number | null;
  healthcheck: string;
  healthcheck_url: string;
  status: BackendStatus;
  response_time_ms: number | null;
  last_checked: string | null;
  last_healthy: string | null;
}

export interface HealthSample {
  at: number;
  ms: number;
  status: BackendStatus;
  ok: boolean;
}

export interface BackendDetail extends BackendSummary {
  history: HealthSample[];
  env: Record<string, string> | null;
  check_interval_secs: number;
  logs_available: boolean;
}

export interface Overview {
  scheduler: SchedulerEntity | null;
  ssps: SspEntity[];
  backends: BackendSummary[];
  totals: {
    ssps: number;
    ssps_ready: number;
    backends: number;
    backends_healthy: number;
  };
  bootstrap_timeout_secs: number;
  server_time_ms: number;
  /** Operations still running. Finished ones live on `/operations`. */
  operations?: Operation[];
  /**
   * Live users / sessions / views, folded in from the presence sampler's
   * memory. It rides this poll rather than a request of its own, so the
   * sidebar count and the overview tile are free.
   */
  presence?: PresenceBlock;
  /**
   * Outbox queue totals, folded in from the job sampler's memory for the same
   * reason presence is: every job aggregate is a scan of a user table, so it is
   * paid for once by the sampler rather than once per open tab.
   */
  jobs?: JobTotals;
}

/* ------------------------------------------------------------------ */
/* Presence: who is connected and what they have registered.            */
/* ------------------------------------------------------------------ */

export interface PresenceTotals {
  /** Distinct authenticated identities with at least one live view. */
  users: number;
  /** Live sessions that never signed in. Not folded into `users`. */
  anon_sessions: number;
  /** Distinct `clientId`s — live tabs, authenticated or not. */
  sessions: number;
  views: number;
  shared_views: number;
  slow_views: number;
  errored_views: number;
  /** Views holding at least `large_view_rows` rows (see Presence). */
  large_views: number;
}

export interface PresenceSample {
  /** Epoch ms. */
  ts: number;
  users: number;
  sessions: number;
  views: number;
}

export interface PresenceBlock {
  totals: PresenceTotals;
  samples: PresenceSample[];
  sample_interval_secs: number;
  taken_at_ms: number | null;
  /** The sampler's row cap was hit, so every figure is a floor. */
  truncated: boolean;
  /** Last sampling error. The totals are then the last good ones. */
  error: string | null;
  /** False until the sampler has run once. Distinct from "nobody is here". */
  ready: boolean;
}

export interface Presence extends PresenceBlock {
  slow_ms: number;
  /** Row count from which a view is flagged large. */
  large_view_rows: number;
  top_users: { auth_id: string; views: number; sessions: number }[];
  by_ssp: { ssp_id: string; views: number }[];
}

/** A registered live query, as the list projects it. */
export interface ViewSummary {
  /** `_00_query:<key>`. */
  id: string;
  /** Just the key, which is what the detail route takes. */
  key: string;
  auth_id: string;
  client_id: string;
  surql: string | null;
  subscriber_count: number;
  shared: boolean;
  ssp_id: string | null;
  row_count: number;
  update_count: number;
  error_count: number;
  registration_ms: number | null;
  p55: number | null;
  p90: number | null;
  p99: number | null;
  last_ingest_ms: number | null;
  ttl_secs: number;
  created_at: string | null;
  last_active_at: string | null;
  expires_at: string | null;
  expires_at_ms: number | null;
  /** Past its TTL, awaiting the SSP's sweep. */
  expired: boolean;
}

export interface ViewsList {
  views: ViewSummary[];
  returned: number;
  /** Counts the database filter only; an SSP filter narrows the page. */
  total: number;
  ssp_filtered: boolean;
  limit: number;
  sort: string;
  slow_ms: number;
  large_view_rows: number;
  server_time_ms: number;
}

export interface ViewSubscriber {
  id: string;
  seen_at_ms: number | null;
  age_secs: number | null;
  /** Older than the row's own TTL, so no longer watching. */
  stale: boolean;
}

export interface ViewDetailData {
  view: ViewSummary & {
    params: unknown;
    subscribers: ViewSubscriber[];
  };
  /** Null when the SSP is unreachable or the view is unassigned. */
  ssp: {
    ssp_id: string;
    view: {
      query_id: string;
      auth_id: string;
      cached_records: number;
      view_bytes: number;
      operator_bytes: number;
      total_bytes: number;
    } | null;
    /** Whether graph-level merging is enabled on that SSP at all. */
    merging: boolean;
    /** Operator DAGs against registrations: the merge win, measured. */
    graphs: number;
    subscribers: number;
    total_bytes: number;
  } | null;
  siblings: {
    /** Includes this view, so 1 reads as "only you". */
    sessions: number;
    users: number;
    truncated: boolean;
    rows: {
      id: string;
      key: string;
      auth_id: string;
      client_id: string;
      row_count: number;
      last_active_at: string | null;
    }[];
  };
  slow_ms: number;
  server_time_ms: number;
}

/* ------------------------------------------------------------------ */
/* Operations: the scheduler's log of operator actions in flight.       */
/* ------------------------------------------------------------------ */

export type OpKind =
  | 'ssp_restart'
  | 'ssp_clean'
  | 'ssp_reload'
  | 'rolling_restart'
  | 'scheduler_restart'
  | 'reclone'
  | 'rehash'
  | 'cloud_restart'
  | 'backup_create'
  | 'backup_restore';

export type OpStatus = 'running' | 'done' | 'failed';

export interface Operation {
  id: string;
  kind: OpKind;
  /** SSP id, run id, backup id: whatever the action was aimed at. */
  target: string | null;
  requested_by: string;
  /** Epoch ms. */
  started_at: number;
  finished_at: number | null;
  status: OpStatus;
  message: string | null;
  /**
   * Free-form progress. Known shapes: rolling_restart `{done, total, current}`,
   * ssp_restart `{ssp_version}`, backup_create `{backup_id, status, size_bytes}`,
   * backup_restore `{restore_id, stage}`.
   */
  detail: Record<string, unknown>;
}

/* ------------------------------------------------------------------ */
/* Actions                                                              */
/* ------------------------------------------------------------------ */

export type SspRestartMode = 'restart' | 'clean' | 'reload';
export type SchedulerRestartMode = 'restart' | 'reclone' | 'rehash';

export interface CloudRestartRequest {
  roles?: string[];
  upgrade: boolean;
  clean: boolean;
  surreal: boolean;
}

export interface OperationResponse {
  operation: Operation;
}

export interface CancelResponse {
  run: string;
  status: 'killed' | 'kill_requested';
}

export interface RerunResponse {
  run: string;
  rerun_of: string;
}

export interface RetryResponse {
  run: string;
  retry_count: number;
  reset: string[];
  kept: string[];
}

export interface JobKillResponse {
  id: string;
  dispatched: number;
  ssps: number;
}

export interface JobRetryResponse {
  id: string;
  status: string;
  assigned_to: string;
}

/* ------------------------------------------------------------------ */
/* Backups                                                              */
/* ------------------------------------------------------------------ */

export type BackupJobStatus = 'queued' | 'running' | 'completed' | 'failed';

/** The scheduler's own record of a backup it executed. In memory only. */
export interface BackupJobState {
  backup_id: string;
  project_slug: string;
  status: BackupJobStatus;
  enqueued_at: string;
  started_at: string | null;
  finished_at: string | null;
  /** Compressed size of the export. */
  size_bytes: number | null;
  snapshot_seq: number | null;
  storage_path: string | null;
  error: string | null;
}

/**
 * The scheduler's own record of a restore. The three booleans are the real
 * stages; `replica_restored` is the wire name for host state, kept for the
 * control plane which reads it.
 */
export interface RestoreJobState {
  restore_id: string;
  backup_id: string;
  project_slug: string;
  storage_path: string;
  status: BackupJobStatus;
  enqueued_at: string;
  started_at: string | null;
  finished_at: string | null;
  snapshot_seq: number | null;
  pending_cleared: number | null;
  main_db_restored: boolean;
  replica_restored: boolean;
  ssps_evicted: number | null;
  error: string | null;
}

export type CatalogStatus =
  | 'pending'
  | 'in_progress'
  | 'completed'
  | 'failed'
  | 'deleted';

/** One backup as the catalog knows it, joined with the local job if any. */
export interface CatalogEntry {
  id: string;
  name: string | null;
  status: CatalogStatus;
  size_bytes: number;
  storage_path: string | null;
  snapshot_seq: number | null;
  created_at: string;
  completed_at: string | null;
  error: string | null;
  /** `cloud` from the control plane's table, `s3` from a bucket listing. */
  source: 'cloud' | 's3';
  local: BackupJobState | null;
}

export interface BackupConfig {
  enabled: boolean;
  schedule: string | null;
  retention: number | null;
  next_run_at: string | null;
  last_scheduled_at: string | null;
}

export interface BackupsData {
  linked: boolean;
  s3: { configured: boolean; endpoint: string | null; bucket: string | null };
  project_slug: string;
  scheduler_status: string;
  local: {
    current_running: BackupJobState | null;
    queue_len: number;
    recent: BackupJobState[];
  };
  catalog: CatalogEntry[];
  restores: RestoreJobState[];
  /** Null when not linked: schedules are run by Sp00ky Cloud. */
  config: BackupConfig | null;
}

export type RestoreStage =
  | 'queued'
  | 'running'
  | 'main_db'
  | 'replica'
  | 'done'
  | 'failed';

export interface RestoreStatus {
  cloud: {
    id: string;
    status: string;
    created_at: string;
    completed_at: string | null;
    error: string | null;
  } | null;
  local: RestoreJobState | null;
  stage: RestoreStage;
}

export type RunStatus =
  | 'running'
  | 'success'
  | 'failed'
  | 'skipped'
  | 'replaced'
  | 'killed';

/**
 * A recorded cause, as every table in `schedule-core` writes one: a stable `code`
 * for the machine, a sentence for the reader, and whatever ids make the sentence
 * actionable. Extra keys are normal — the renderer shows them rather than
 * requiring each one to be declared here.
 */
export interface RunError {
  code?: string;
  reason?: string;
  [key: string]: unknown;
}

export interface WorkflowRun {
  id: string;
  workflow_name: string;
  schedule_name: string | null;
  status: RunStatus;
  kill_requested: boolean;
  error: RunError | null;
  created_at: string;
  updated_at: string | null;
  finished_at: string | null;
  /** `cron` for a scheduled fire, `manual` for an operator rerun. Null on old rows. */
  trigger?: string | null;
  /** Operator retries applied to this run. Each mints new `_r<n>` step job ids. */
  retry_count?: number;
  /** The run this one was rerun from, when it was. */
  rerun_of?: string | null;
}

export interface WorkflowRunDetail extends WorkflowRun {
  dag: unknown;
  input: unknown;
  target_table: string | null;
}

/* ------------------------------------------------------------------ */
/* Jobs: the outbox plane.                                              */
/* ------------------------------------------------------------------ */

/**
 * Where a job came from, derived by the scheduler from the key
 * `schedule-core` minted for it.
 *
 * `app` is the class with no other surface anywhere in the dashboard: a job the
 * application created itself, belonging to no schedule and no workflow.
 */
export type OriginKind = 'schedule' | 'workflow' | 'app';

/**
 * The origin block on every job row. `kind` is always present; the rest is the
 * resolved back-link, which is absent once the owning run has been pruned on
 * its own retention window.
 */
export interface JobOrigin {
  kind: OriginKind;
  /** `schedule` only. */
  schedule?: string | null;
  schedule_run?: string | null;
  fire_at?: string | null;
  key?: string | null;
  /** `workflow` only. */
  workflow_run?: string | null;
  step?: string | null;
}

export interface JobStatusCounts {
  pending: number;
  processing: number;
  success: number;
  failed: number;
  /** A status the table's own ASSERT does not allow. Should always be zero. */
  other: number;
}

/** One point of the queue-depth sparkline. */
export interface JobSample {
  t: number;
  pending: number;
  processing: number;
  failed: number;
  throughput_1m: number;
}

/**
 * The queue as a whole. Counts cover in-flight work plus the last hour of
 * terminal work, never all of history: an all-time ratio reports a fail rate
 * that climbs as successes age out of retention.
 */
export interface JobTotals {
  counts: JobStatusCounts;
  /** `processing` rows whose lease is still live: work really in flight. */
  in_flight: number;
  /**
   * `processing` rows whose lease has expired. Nobody is working on these; a
   * recovery sweep may reclaim them. The state the schema has no name for.
   */
  stalled: number;
  throughput_1m: number;
  oldest_pending: string | null;
  tables: number;
  /** Every outbox table refused to answer, so zeros here mean nothing. */
  blind: boolean;
  samples: JobSample[];
  taken_at_ms?: number | null;
  sample_interval_secs?: number;
  /** False when the background sampler is switched off entirely. */
  enabled?: boolean;
  /**
   * The sampler has completed a pass. False means "not measured yet", which is
   * not the same as an empty queue, and the UI must not draw it as one.
   */
  ready: boolean;
}

/** Per-table depth, plus the ceiling the dispatcher admits against. */
export interface JobTableStat {
  table: string;
  counts: JobStatusCounts;
  in_flight: number;
  stalled: number;
  /** `_00_job_policy` concurrency, or the dispatcher default of 1. */
  concurrency: number;
  throughput_1m: number;
  oldest_pending: string | null;
  error: string | null;
}

/**
 * One row of the jobs list. Without `payload` and `result`, which are uncapped
 * and up to 64 KiB; both are on the detail.
 */
export interface JobSummary {
  id: string;
  /** The bare record key, which is what carries the origin prefix. */
  key: string;
  table: string;
  status: string;
  path: string | null;
  retries: number | null;
  max_retries: number | null;
  retry_strategy: string | null;
  assignee: string | null;
  timeout: number | null;
  delay: number | null;
  lease_until: string | null;
  created_at: string | null;
  updated_at: string | null;
  origin: JobOrigin;
  /** The last recorded attempt. The whole history is on the detail. */
  last_error: RunError | null;
  attempts: number;
}

export interface JobsListResponse {
  jobs: JobSummary[];
  returned: number;
  limit: number;
  totals: JobTotals;
  tables: JobTableStat[];
  sampled_at_ms?: number;
  /** True when this page came from the sampler rather than a fresh query. */
  live?: boolean;
  filtered?: boolean;
  table_errors?: { table: string; error: string | null }[];
}

export interface JobsClearResponse {
  cleared: Record<string, number>;
  total: number;
  /** The batch ceiling was hit; running it again picks up where it stopped. */
  more: boolean;
}

/**
 * One outbox job, from `GET /admin/api/jobs/:id`.
 *
 * `errors` is the whole attempt history, which is the part a step row cannot
 * carry: `_00_step_run.error` holds only the attempt that ended the job.
 */
export interface JobDetail {
  id: string;
  status: string;
  path: string;
  payload: unknown;
  result: unknown;
  errors: RunError[] | null;
  retries: number | null;
  max_retries: number | null;
  retry_strategy: string | null;
  /** SSP instance holding the row, when one has claimed it. */
  assignee: string | null;
  /** Per-attempt HTTP ceiling in seconds, when the schedule sets one. */
  timeout: number | null;
  /** One-shot delay in ms: the job is due at `created_at + delay`. */
  delay: number | null;
  lease_until: string | null;
  created_at: string | null;
  updated_at: string | null;
  /** Which outbox table the row lives in. */
  table?: string;
  key?: string;
  /** Where it came from, and the run it belongs to when that still exists. */
  origin?: JobOrigin;
}

export interface StepRun {
  step: string;
  depends_on: string[];
  status: string;
  job_id: string | null;
  output: unknown;
  error: RunError | null;
  /** When the step ROW was made — the run's own start for every step in a DAG. */
  created_at: string | null;
  /**
   * When the step's job was actually created. `null` on runs recorded before
   * the engine started stamping it, which is why the timeline still needs a
   * fallback rather than trusting this outright.
   */
  started_at: string | null;
  finished_at: string | null;
  /** Dispatch attempts, including recovery re-dispatches. */
  dispatch_attempts?: number | null;
}

export interface Schedule {
  name: string;
  kind: string;
  cron: string | null;
  every_ms: number | null;
  timezone: string | null;
  paused: boolean;
  config_disabled: boolean;
  concurrency: string;
  max_retries: number | null;
  retry_strategy: string | null;
  /** Seconds. */
  timeout: number | null;
  target_table: string | null;
  path: string | null;
  for_each: string | null;
  for_each_key: string | null;
  history_mode: string | null;
  last_run_status: string | null;
  next_fire_at: string | null;
  last_fire_at: string | null;
  last_run_at: string | null;
  created_at: string | null;
  updated_at: string | null;
  last_error: string | null;
}

/** One fire of a schedule. */
export interface ScheduleRun {
  id: string;
  schedule_name: string;
  key: string;
  kind: string;
  status: RunStatus;
  trigger: string | null;
  job_id: string | null;
  /** Record id of the workflow run this fire produced, when it produced one. */
  workflow_run: string | null;
  error: RunError | null;
  fire_at: string | null;
  created_at: string | null;
  finished_at: string | null;
}

/**
 * Hourly outcome buckets from `_00_run_rollup`. These survive retention, so a
 * tally built from them does not shrink as run rows are pruned.
 */
export interface RunRollup {
  bucket: string;
  success: number;
  failed: number;
  skipped: number;
  replaced: number;
  killed: number;
}

export interface ScheduleDetailData {
  schedule: Schedule;
  runs: ScheduleRun[];
  rollup: RunRollup[];
}

export interface LogLine {
  ts: number;
  level: string;
  target: string;
  message: string;
  fields?: string;
}
