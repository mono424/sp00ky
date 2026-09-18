/**
 * The scheduler the dashboard screenshots are taken against.
 *
 * Nothing here is read from a real deployment: it is one invented project
 * ("acme") with a plausible cluster behind it. `capture.mjs` answers every
 * `/admin/api/*` request from this file, so the dashboard runs unmodified with
 * no scheduler, no database and no network.
 *
 * Shapes mirror `src/api/types.ts`. When a screen renders a dash, an empty
 * section or `NaN`, this file and the types have drifted and the types are
 * right.
 */

/** Pinned so a regenerated screenshot is identical when nothing changed. */
export const NOW = Date.UTC(2026, 8, 17, 9, 41, 0);
const ago = (ms) => NOW - ms;
const iso = (ms) => new Date(NOW - ms).toISOString();

export const TOKEN = 'scr-demo-token';

const heartbeatSamples = () => {
  const out = [];
  for (let i = 39; i >= 0; i--) {
    const base = [58, 61, 64, 57, 72, 63, 59, 66, 61, 55][i % 10];
    out.push({ ts: ago(i * 15_000), ms: i === 14 ? null : base + (i % 3) * 4, ok: i !== 14 });
  }
  return out;
};

const SCHEDULER = {
  entity: 'scheduler',
  id: 'scheduler-0',
  ip: '10.0.3.11',
  status: 'ready',
  views: 72,
  version: '0.0.1-canary.265',
  surrealdb_version: '3.1.5',
  uptime_seconds: 149_820,
  pending_events: 0,
  snapshot_seq: 884_512,
  latest_seq: 884_512,
  lag: 0,
  heartbeat: {
    enabled: true,
    interval_secs: 15,
    last_e2e_ms: 61,
    last_ok_epoch_ms: ago(9_000),
    last_attempt_epoch_ms: ago(9_000),
    consecutive_failures: 0,
    stale: false,
    samples: heartbeatSamples(),
  },
  env: { SPKY_PROJECT: 'acme', RUST_LOG: 'info' },
};

const SSPS = [
  {
    entity: 'ssp',
    id: 'ssp-0',
    ip: '10.0.3.21',
    status: 'ready',
    views: 38,
    version: '0.0.1-canary.265',
    uptime_seconds: 149_640,
    last_heartbeat_seconds_ago: 2,
    state_seconds: 149_600,
    buffered_events: 0,
    bootstrap: null,
    env: null,
    publication: {
      pending_batches: 0,
      pending_operations: 0,
      pending_bytes: 0,
      oldest_age_ms: 0,
      parked_batches: 0,
      last_success_at_ms: ago(1_400),
      overload_total: 0,
      connection: { generation: 1, last_reconnect_duration_ms: null, reconnect_failures: 0 },
      worst_views: [],
    },
  },
  {
    entity: 'ssp',
    id: 'ssp-1',
    ip: '10.0.3.22',
    status: 'ready',
    views: 34,
    version: '0.0.1-canary.265',
    uptime_seconds: 86_400,
    last_heartbeat_seconds_ago: 3,
    state_seconds: 86_330,
    buffered_events: 12,
    bootstrap: null,
    env: null,
    publication: {
      pending_batches: 1,
      pending_operations: 34,
      pending_bytes: 18_944,
      oldest_age_ms: 240,
      parked_batches: 0,
      last_success_at_ms: ago(900),
      overload_total: 0,
      connection: { generation: 2, last_reconnect_duration_ms: 812, reconnect_failures: 0 },
      worst_views: [
        {
          query_id: '814233901',
          pending_operations: 22,
          pending_bytes: 12_288,
          oldest_age_ms: 240,
        },
      ],
    },
  },
];

const BACKENDS = [
  {
    name: 'api',
    id: 'api',
    url: 'http://api:3660',
    ip: '10.0.3.31',
    port: 3660,
    healthcheck: '/health',
    healthcheck_url: 'http://api:3660/health',
    status: 'healthy',
    response_time_ms: 8,
    last_checked: iso(4_000),
    last_healthy: iso(4_000),
  },
  {
    name: 'worker',
    id: 'worker',
    url: 'http://worker:3661',
    ip: '10.0.3.32',
    port: 3661,
    healthcheck: '/health',
    healthcheck_url: 'http://worker:3661/health',
    status: 'healthy',
    response_time_ms: 14,
    last_checked: iso(4_000),
    last_healthy: iso(4_000),
  },
];

const PRESENCE = {
  totals: {
    users: 148,
    anon_sessions: 24,
    sessions: 212,
    views: 72,
    shared_views: 31,
    slow_views: 2,
    errored_views: 0,
    large_views: 1,
  },
  samples: Array.from({ length: 40 }, (_, i) => ({
    ts: ago((39 - i) * 30_000),
    users: 120 + ((i * 7) % 40),
    sessions: 180 + ((i * 11) % 60),
    views: 66 + ((i * 3) % 12),
  })),
  sample_interval_secs: 30,
  taken_at_ms: ago(11_000),
  truncated: false,
  error: null,
  ready: true,
};

const JOB_TOTALS = {
  counts: { pending: 3, processing: 1, success: 1_284, failed: 2, other: 0 },
  in_flight: 1,
  stalled: 0,
  throughput_1m: 42,
  oldest_pending: iso(2_400),
  tables: 4,
  blind: false,
  samples: Array.from({ length: 40 }, (_, i) => ({
    t: ago((39 - i) * 30_000),
    pending: 2 + ((i * 3) % 7),
    processing: (i % 3) + 1,
    failed: i === 22 ? 2 : 0,
    throughput_1m: 30 + ((i * 5) % 25),
  })),
  taken_at_ms: ago(11_000),
  sample_interval_secs: 30,
  enabled: true,
  ready: true,
};

/* ------------------------------------------------------------------ */
/* Incidents                                                            */
/* ------------------------------------------------------------------ */

const incidentEvent = (at, component, kind, state, summary) => ({
  at,
  component,
  kind,
  state,
  summary,
  operation_id: null,
  version: '0.0.1-canary.265',
});

const INCIDENTS = [
  {
    id: 'inc-7f21c4',
    component: 'ssp-1',
    kind: 'ingest_lag',
    severity: 'warning',
    state: 'recovered',
    started_at: ago(3 * 3_600_000),
    ended_at: ago(3 * 3_600_000 - 412_000),
    max_buffered_events: 18_442,
    max_publication_operations: 2_140,
    max_publication_bytes: 1_310_720,
    max_publication_age_ms: 41_000,
    event_count: 4,
    events: [
      incidentEvent(
        ago(3 * 3_600_000),
        'ssp-1',
        'ingest_lag',
        'open',
        'Buffered events crossed the lag threshold (12,004).'
      ),
      incidentEvent(
        ago(3 * 3_600_000 - 90_000),
        'ssp-1',
        'ingest_lag',
        'open',
        'Backlog still growing: 18,442 events buffered.'
      ),
      incidentEvent(
        ago(3 * 3_600_000 - 300_000),
        'ssp-1',
        'ingest_lag',
        'open',
        'Replay started; backlog draining.'
      ),
      incidentEvent(
        ago(3 * 3_600_000 - 412_000),
        'ssp-1',
        'ingest_lag',
        'recovered',
        'Backlog drained; the circuit is live again.'
      ),
    ],
  },
  {
    id: 'inc-5ad098',
    component: 'scheduler',
    kind: 'heartbeat_failed',
    severity: 'warning',
    state: 'recovered',
    started_at: ago(9 * 3_600_000),
    ended_at: ago(9 * 3_600_000 - 120_000),
    max_buffered_events: 0,
    event_count: 2,
    events: [
      incidentEvent(
        ago(9 * 3_600_000),
        'scheduler',
        'heartbeat_failed',
        'open',
        'Two consecutive end-to-end probes timed out.'
      ),
      incidentEvent(
        ago(9 * 3_600_000 - 120_000),
        'scheduler',
        'heartbeat_failed',
        'recovered',
        'Probe answered in 64ms.'
      ),
    ],
  },
  {
    id: 'inc-31bb02',
    component: 'ssp-0',
    kind: 'registered_again',
    severity: 'info',
    state: 'recorded',
    started_at: ago(26 * 3_600_000),
    ended_at: ago(26 * 3_600_000),
    max_buffered_events: 0,
    event_count: 1,
    events: [
      incidentEvent(
        ago(26 * 3_600_000),
        'ssp-0',
        'registered_again',
        'recorded',
        'SSP re-registered after a deploy.'
      ),
    ],
  },
  {
    id: 'inc-22c7de',
    component: 'scheduler',
    kind: 'operator_action',
    severity: 'info',
    state: 'recorded',
    started_at: ago(30 * 3_600_000),
    ended_at: ago(30 * 3_600_000),
    max_buffered_events: 0,
    event_count: 1,
    events: [
      incidentEvent(
        ago(30 * 3_600_000),
        'scheduler',
        'operator_action',
        'recorded',
        'Operator requested a scheduler restart.'
      ),
    ],
  },
];

const brief = ({ id, component, kind, severity, state, started_at, ended_at }) => ({
  id,
  component,
  kind,
  severity,
  state,
  started_at,
  ended_at,
});

const INCIDENT_SUMMARY = {
  open: 0,
  total: INCIDENTS.length,
  last_24h: 2,
  recent: INCIDENTS.slice(0, 2).map(brief),
  latest: brief(INCIDENTS[0]),
  retention_days: 30,
  storage_error: null,
  server_time_ms: NOW,
};

export const INCIDENT_LIST = {
  incidents: INCIDENTS,
  total: INCIDENTS.length,
  offset: 0,
  limit: 400,
  storage_error: null,
  server_time_ms: NOW,
};

export const OVERVIEW = {
  scheduler: SCHEDULER,
  ssps: SSPS,
  backends: BACKENDS,
  totals: { ssps: 2, ssps_ready: 2, backends: 2, backends_healthy: 2 },
  bootstrap_timeout_secs: 900,
  server_time_ms: NOW,
  operations: [],
  presence: PRESENCE,
  jobs: JOB_TOTALS,
  incidents: INCIDENT_SUMMARY,
};

export const CONFIG = {
  scheduler_id: 'scheduler-0',
  version: '0.0.1-canary.265',
  breakglass_available: true,
  cloud_linked: true,
  project_slug: 'acme',
  sessions_persistent: true,
  supervised: true,
};

export const ME = {
  subject: 'user:mira',
  label: 'Mira Oyelaran',
  mode: 'roster',
  scope: 'full',
};

/* ------------------------------------------------------------------ */
/* Views                                                                */
/* ------------------------------------------------------------------ */

const view = (o) => ({
  id: `_00_query:${o.key}`,
  key: o.key,
  auth_id: o.auth_id,
  client_id: o.client_id,
  surql: o.surql,
  subscriber_count: o.subscribers,
  shared: o.subscribers > 1,
  ssp_id: o.ssp,
  row_count: o.rows,
  update_count: o.updates,
  error_count: o.errors ?? 0,
  registration_ms: o.registration,
  p55: o.p55,
  p90: o.p90,
  p99: o.p99,
  last_ingest_ms: o.p55,
  ttl_secs: 600,
  created_at: iso(o.age),
  last_active_at: iso(o.active),
  expires_at: new Date(NOW + 600_000).toISOString(),
  expires_at_ms: NOW + 600_000,
  expired: false,
});

export const VIEWS = {
  views: [
    view({
      key: '814233901',
      auth_id: 'user:mira',
      client_id: 'tab-7f21c4',
      surql: 'SELECT * FROM message WHERE channel = $channel ORDER BY created_at DESC LIMIT 50',
      subscribers: 6,
      ssp: 'ssp-0',
      rows: 50,
      updates: 1_284,
      registration: 26.4,
      p55: 3.8,
      p90: 7.1,
      p99: 19.4,
      age: 412_000,
      active: 3_100,
    }),
    view({
      key: '512004773',
      auth_id: 'user:tomas',
      client_id: 'tab-2b9910',
      surql: 'SELECT * FROM channel WHERE workspace = $workspace ORDER BY name',
      subscribers: 3,
      ssp: 'ssp-0',
      rows: 14,
      updates: 402,
      registration: 8.9,
      p55: 2.4,
      p90: 4.0,
      p99: 9.2,
      age: 398_000,
      active: 900,
    }),
    view({
      key: '907461255',
      auth_id: 'user:iris',
      client_id: 'tab-ce0043',
      surql: 'SELECT * FROM task WHERE assigned_to = $auth.id AND done = false ORDER BY due_at',
      subscribers: 1,
      ssp: 'ssp-1',
      rows: 231,
      updates: 96,
      errors: 1,
      registration: 44.2,
      p55: 27.7,
      p90: 62.9,
      p99: 118.5,
      age: 122_000,
      active: 11_400,
    }),
    view({
      key: '331988402',
      auth_id: 'user:nadia',
      client_id: 'tab-118ba7',
      surql: 'SELECT * FROM user WHERE id = $auth.id',
      subscribers: 4,
      ssp: 'ssp-1',
      rows: 1,
      updates: 18,
      registration: 4.1,
      p55: 1.0,
      p90: 1.6,
      p99: 2.9,
      age: 401_000,
      active: 64_000,
    }),
    view({
      key: '140772318',
      auth_id: 'anonymous',
      client_id: 'tab-9d4410',
      surql: 'SELECT count() FROM message WHERE channel = $channel GROUP ALL',
      subscribers: 1,
      ssp: 'ssp-0',
      rows: 1,
      updates: 44,
      registration: 6.2,
      p55: 1.7,
      p90: 2.8,
      p99: 5.0,
      age: 96_000,
      active: 29_000,
    }),
    view({
      key: '205913664',
      auth_id: 'user:mira',
      client_id: 'tab-7f21c4',
      surql: 'SELECT * FROM membership WHERE channel = $channel FETCH user',
      subscribers: 2,
      ssp: 'ssp-1',
      rows: 412,
      updates: 6,
      registration: 51.8,
      p55: 12.2,
      p90: 24.4,
      p99: 48.0,
      age: 1_800,
      active: 1_800,
    }),
  ],
  returned: 6,
  total: 72,
  ssp_filtered: false,
  limit: 50,
  sort: 'active',
  slow_ms: 50,
  large_view_rows: 400,
  server_time_ms: NOW,
};

export const PRESENCE_FULL = {
  ...PRESENCE,
  slow_ms: 50,
  large_view_rows: 400,
  top_users: [
    { auth_id: 'user:mira', views: 9, sessions: 3 },
    { auth_id: 'user:iris', views: 7, sessions: 2 },
    { auth_id: 'user:tomas', views: 5, sessions: 2 },
  ],
  by_ssp: [
    { ssp_id: 'ssp-0', views: 38 },
    { ssp_id: 'ssp-1', views: 34 },
  ],
};

/* ------------------------------------------------------------------ */
/* Schedules                                                            */
/* ------------------------------------------------------------------ */

const schedule = (o) => ({
  name: o.name,
  kind: o.kind ?? 'job',
  cron: o.cron ?? null,
  every_ms: o.every_ms ?? null,
  timezone: 'UTC',
  paused: o.paused ?? false,
  config_disabled: false,
  concurrency: o.concurrency ?? 'skip',
  max_retries: 3,
  retry_strategy: 'exponential',
  timeout: 300,
  target_table: o.table ?? null,
  path: o.path ?? null,
  for_each: o.for_each ?? null,
  for_each_key: null,
  history_mode: 'all',
  last_run_status: o.last_status ?? 'success',
  next_fire_at: new Date(NOW + (o.next ?? 600_000)).toISOString(),
  last_fire_at: iso(o.last ?? 1_800_000),
  last_run_at: iso(o.last ?? 1_800_000),
  created_at: iso(30 * 86_400_000),
  updated_at: iso(2 * 86_400_000),
  last_error: o.last_error ?? null,
});

export const SCHEDULES = {
  schedules: [
    schedule({
      name: 'nightly-digest',
      kind: 'workflow',
      cron: '0 6 * * *',
      path: '/jobs/digest',
      next: 5 * 3_600_000,
      last: 19 * 3_600_000,
    }),
    schedule({
      name: 'refresh-search-index',
      cron: '*/15 * * * *',
      path: '/jobs/reindex',
      table: 'search_job',
      next: 420_000,
      last: 480_000,
    }),
    schedule({
      name: 'prune-expired-invites',
      cron: '0 * * * *',
      path: '/jobs/prune',
      table: 'maintenance_job',
      next: 1_140_000,
      last: 2_460_000,
    }),
    schedule({
      name: 'per-workspace-rollup',
      kind: 'workflow',
      every_ms: 3_600_000,
      for_each: 'SELECT id FROM workspace',
      next: 900_000,
      last: 2_700_000,
      concurrency: 'queue',
    }),
    schedule({
      name: 'weekly-export',
      cron: '0 3 * * 1',
      paused: true,
      path: '/jobs/export',
      next: 4 * 86_400_000,
      last: 3 * 86_400_000,
      last_status: 'failed',
      last_error: 'backend returned 503',
    }),
  ],
};

/* ------------------------------------------------------------------ */
/* Workflows                                                            */
/* ------------------------------------------------------------------ */

const run = (o) => ({
  id: o.id,
  workflow_name: o.workflow,
  schedule_name: o.schedule ?? null,
  status: o.status,
  kill_requested: false,
  error: o.error ?? null,
  created_at: iso(o.started),
  updated_at: iso(o.updated ?? o.started),
  finished_at: o.finished === undefined ? null : iso(o.finished),
  trigger: o.trigger ?? 'cron',
  retry_count: o.retries ?? 0,
  rerun_of: null,
});

export const WORKFLOW_RUNS = {
  runs: [
    run({
      id: 'run-9f2c1a',
      workflow: 'nightly-digest',
      schedule: 'nightly-digest',
      status: 'running',
      started: 240_000,
      updated: 4_000,
    }),
    run({
      id: 'run-7b04de',
      workflow: 'per-workspace-rollup',
      schedule: 'per-workspace-rollup',
      status: 'success',
      started: 3_600_000,
      finished: 3_540_000,
    }),
    run({
      id: 'run-22aa90',
      workflow: 'per-workspace-rollup',
      schedule: 'per-workspace-rollup',
      status: 'skipped',
      started: 7_200_000,
      finished: 7_200_000,
    }),
    run({
      id: 'run-5c18b3',
      workflow: 'weekly-export',
      schedule: 'weekly-export',
      status: 'failed',
      started: 3 * 86_400_000,
      finished: 3 * 86_400_000 - 86_000,
      error: { code: 'backend_unreachable', reason: 'backend returned 503 for /jobs/export' },
    }),
    run({
      id: 'run-e04771',
      workflow: 'nightly-digest',
      schedule: 'nightly-digest',
      status: 'success',
      started: 24 * 3_600_000,
      finished: 24 * 3_600_000 - 142_000,
    }),
    run({
      id: 'run-1ab5cf',
      workflow: 'reindex-tenant',
      status: 'success',
      trigger: 'manual',
      started: 48 * 3_600_000,
      finished: 48 * 3_600_000 - 61_000,
      retries: 1,
    }),
  ],
};

/* ------------------------------------------------------------------ */
/* Jobs                                                                 */
/* ------------------------------------------------------------------ */

const job = (o) => ({
  id: `${o.table}:${o.key}`,
  key: o.key,
  table: o.table,
  status: o.status,
  path: o.path,
  retries: o.retries ?? 0,
  max_retries: 3,
  retry_strategy: 'exponential',
  assignee: o.assignee ?? null,
  timeout: 300,
  delay: null,
  lease_until: o.assignee ? new Date(NOW + 120_000).toISOString() : null,
  created_at: iso(o.age),
  updated_at: iso(o.updated ?? o.age),
  origin: o.origin,
  last_error: o.error ?? null,
  attempts: o.attempts ?? 1,
});

const JOB_TABLES = [
  {
    table: 'search_job',
    counts: { pending: 2, processing: 1, success: 812, failed: 1, other: 0 },
    in_flight: 1,
    stalled: 0,
    concurrency: 4,
    throughput_1m: 28,
    oldest_pending: iso(2_400),
    error: null,
  },
  {
    table: 'maintenance_job',
    counts: { pending: 1, processing: 0, success: 204, failed: 0, other: 0 },
    in_flight: 0,
    stalled: 0,
    concurrency: 1,
    throughput_1m: 6,
    oldest_pending: iso(41_000),
    error: null,
  },
  {
    table: 'digest_job',
    counts: { pending: 0, processing: 0, success: 240, failed: 1, other: 0 },
    in_flight: 0,
    stalled: 0,
    concurrency: 2,
    throughput_1m: 8,
    oldest_pending: null,
    error: null,
  },
  {
    table: 'export_job',
    counts: { pending: 0, processing: 0, success: 28, failed: 0, other: 0 },
    in_flight: 0,
    stalled: 0,
    concurrency: 1,
    throughput_1m: 0,
    oldest_pending: null,
    error: null,
  },
];

export const JOBS = {
  jobs: [
    job({
      table: 'search_job',
      key: 'wf_run-9f2c1a_reindex',
      status: 'processing',
      path: '/jobs/reindex',
      assignee: 'ssp-0',
      age: 44_000,
      updated: 4_000,
      origin: { kind: 'workflow', workflow_run: 'run-9f2c1a', step: 'reindex' },
    }),
    job({
      table: 'search_job',
      key: 'sch_refresh-search-index_1789',
      status: 'pending',
      path: '/jobs/reindex',
      age: 2_400,
      origin: {
        kind: 'schedule',
        schedule: 'refresh-search-index',
        schedule_run: 'srun-4410',
        fire_at: iso(2_400),
      },
    }),
    job({
      table: 'search_job',
      key: 'sch_refresh-search-index_1790',
      status: 'pending',
      path: '/jobs/reindex',
      age: 1_200,
      origin: {
        kind: 'schedule',
        schedule: 'refresh-search-index',
        schedule_run: 'srun-4411',
        fire_at: iso(1_200),
      },
    }),
    job({
      table: 'maintenance_job',
      key: 'sch_prune-expired-invites_882',
      status: 'pending',
      path: '/jobs/prune',
      age: 41_000,
      origin: { kind: 'schedule', schedule: 'prune-expired-invites', schedule_run: 'srun-4402' },
    }),
    job({
      table: 'digest_job',
      key: 'app_welcome_88de20',
      status: 'failed',
      path: '/jobs/welcome-mail',
      retries: 3,
      attempts: 4,
      age: 6 * 3_600_000,
      updated: 5 * 3_600_000,
      origin: { kind: 'app' },
      error: { code: 'http_503', reason: 'backend returned 503' },
    }),
    job({
      table: 'search_job',
      key: 'app_backfill_3f90ac',
      status: 'success',
      path: '/jobs/reindex',
      age: 9 * 3_600_000,
      updated: 9 * 3_600_000 - 4_000,
      origin: { kind: 'app' },
    }),
  ],
  returned: 6,
  limit: 50,
  totals: JOB_TOTALS,
  tables: JOB_TABLES,
  sampled_at_ms: ago(11_000),
  live: true,
  filtered: false,
  table_errors: [],
};

/* ------------------------------------------------------------------ */
/* Backups                                                              */
/* ------------------------------------------------------------------ */

const catalogEntry = (o) => ({
  id: o.id,
  name: o.name ?? null,
  status: o.status ?? 'completed',
  size_bytes: o.size,
  storage_path: `s3://acme-backups/${o.id}.surql.gz`,
  snapshot_seq: o.seq,
  created_at: iso(o.age),
  completed_at: iso(o.age - 90_000),
  error: null,
  source: 'cloud',
  local: null,
});

export const BACKUPS = {
  linked: true,
  s3: { configured: true, endpoint: 's3.eu-central-1.amazonaws.com', bucket: 'acme-backups' },
  project_slug: 'acme',
  scheduler_status: 'ready',
  local: { current_running: null, queue_len: 0, recent: [] },
  catalog: [
    catalogEntry({
      id: 'bkp-9f2c1a',
      name: 'nightly',
      size: 412_140_032,
      seq: 884_012,
      age: 8 * 3_600_000,
    }),
    catalogEntry({
      id: 'bkp-7b04de',
      name: 'nightly',
      size: 409_993_216,
      seq: 871_204,
      age: 32 * 3_600_000,
    }),
    catalogEntry({
      id: 'bkp-22aa90',
      name: 'before upgrade',
      size: 402_653_184,
      seq: 858_770,
      age: 54 * 3_600_000,
    }),
    catalogEntry({
      id: 'bkp-5c18b3',
      name: 'nightly',
      size: 398_458_880,
      seq: 844_119,
      age: 80 * 3_600_000,
    }),
  ],
  restores: [],
  config: {
    enabled: true,
    schedule: '0 2 * * *',
    retention: 14,
    next_run_at: new Date(NOW + 16 * 3_600_000).toISOString(),
    last_scheduled_at: iso(8 * 3_600_000),
  },
};

/* ------------------------------------------------------------------ */
/* Logs and MCP                                                         */
/* ------------------------------------------------------------------ */

const LOG_LINES = [
  {
    level: 'INFO',
    target: 'scheduler::ingest',
    message: 'applied 142 events in 38ms (seq 884,512)',
  },
  { level: 'INFO', target: 'scheduler::heartbeat', message: 'e2e probe ok in 61ms' },
  {
    level: 'DEBUG',
    target: 'scheduler::publication',
    message: 'ssp-1 drained 1 batch (34 ops, 18.5 KiB)',
  },
  {
    level: 'INFO',
    target: 'scheduler::jobs',
    message: 'dispatched search_job:sch_refresh-search-index_1789 to ssp-0',
  },
  {
    level: 'WARN',
    target: 'scheduler::backends',
    message: 'worker health check took 214ms (threshold 200ms)',
  },
  {
    level: 'INFO',
    target: 'scheduler::views',
    message: 'registered _00_query:205913664 for user:mira on ssp-1',
  },
  {
    level: 'INFO',
    target: 'scheduler::ingest',
    message: 'applied 96 events in 24ms (seq 884,608)',
  },
  {
    level: 'INFO',
    target: 'scheduler::schedules',
    message: 'fired refresh-search-index (next in 15m)',
  },
  {
    level: 'DEBUG',
    target: 'scheduler::presence',
    message: 'presence sample: 148 users, 212 sessions, 72 views',
  },
  { level: 'INFO', target: 'scheduler::heartbeat', message: 'e2e probe ok in 58ms' },
  { level: 'INFO', target: 'scheduler::workflows', message: 'run-9f2c1a step reindex dispatched' },
  {
    level: 'INFO',
    target: 'scheduler::ingest',
    message: 'applied 214 events in 51ms (seq 884,822)',
  },
];

/** `text/event-stream` bodies, delivered in one go and then closed. */
export const STREAMS = {
  'GET /jobs/stream': `event: jobs
data: ${JSON.stringify(JOBS)}

`,
  'GET /workflows/stream': `event: runs
data: ${JSON.stringify(WORKFLOW_RUNS)}

`,
  'GET /logs': LOG_LINES.map(
    (line, i) =>
      `event: line
data: ${JSON.stringify({ ...line, ts: ago((LOG_LINES.length - i) * 4_000), fields: '' })}

`
  ).join(''),
};

const MCP_TOOLS = [
  {
    name: 'overview',
    description: 'Cluster health: scheduler, SSPs, backends, presence and jobs.',
  },
  { name: 'views_list', description: 'Registered live queries, with their cost and subscribers.' },
  { name: 'jobs_list', description: 'The outbox queue, filterable by table and status.' },
  { name: 'job_retry', description: 'Re-enqueue a failed job.' },
  { name: 'schedules_list', description: 'Cron schedules and their next fire.' },
  { name: 'schedule_trigger', description: 'Fire a schedule now.' },
  { name: 'workflow_runs_list', description: 'Workflow runs and their status.' },
  { name: 'incidents_list', description: 'Lag, heartbeat and restart episodes.' },
  { name: 'logs_recent', description: 'Recent log lines from any component.' },
  { name: 'ssp_restart', description: 'Restart one SSP, optionally cleaning its state.' },
];

export const MCP_TOOLS_RESULT = {
  jsonrpc: '2.0',
  id: 1,
  result: { tools: MCP_TOOLS },
};

/**
 * `METHOD /path` (without the `/admin/api` prefix) to the JSON answered.
 * A path not listed here is reported by `capture.mjs` rather than silently
 * 404ing, so a screen that grows a request cannot quietly lose its data.
 */
export const ROUTES = {
  'GET /config': CONFIG,
  'GET /me': ME,
  'GET /overview': OVERVIEW,
  'GET /presence': PRESENCE_FULL,
  'GET /views': VIEWS,
  'GET /schedules': SCHEDULES,
  'GET /backends': { backends: BACKENDS, check_interval_secs: 10 },
  'GET /incidents': INCIDENT_LIST,
  'GET /backups': BACKUPS,
  'POST /mcp': MCP_TOOLS_RESULT,
};
