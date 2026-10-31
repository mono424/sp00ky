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

/** What `GET /backends/:name` adds: the last few minutes of checks and the env. */
const backendDetail = (b, base, swing) => ({
  ...b,
  check_interval_secs: 10,
  logs_available: false,
  env: { PORT: String(b.port), NODE_ENV: 'production', DATABASE_URL: 'ws://surrealdb:8000/rpc' },
  history: Array.from({ length: 36 }, (_, i) => ({
    at: ago((35 - i) * 10_000),
    ms: Math.round(base + swing * Math.sin(i / 3) + ((i * 7) % 5)),
    status: 'healthy',
    ok: true,
  })),
});
const BACKEND_DETAILS = {
  api: backendDetail(BACKENDS[0], 9, 3),
  worker: backendDetail(BACKENDS[1], 15, 5),
};

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
    hot_views: 1,
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
    id: 'inc-c90e17',
    component: 'views',
    kind: 'hot_views',
    severity: 'warning',
    state: 'open',
    started_at: ago(140_000),
    ended_at: null,
    max_buffered_events: 0,
    event_count: 1,
    events: [
      incidentEvent(
        ago(140_000),
        'views',
        'hot_views',
        'open',
        '1 view updating at least 5.0/s; hottest 907461255 at 104/s by user:iris'
      ),
    ],
  },
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
  // A week of history behind the last day, so the page has something to show.
  ...[
    ['inc-9b40aa', 'ssp-1', 'publication_backlog', 'warning', 'recovered', 50, 9, 'Publication backlog crossed 2,000 operations.', 'Backlog published; queue empty.'],
    ['inc-8e12f3', 'api', 'backend_unhealthy', 'error', 'failed', 61, 23, 'Health check returned 503 three times in a row.', 'Gave up after 23m; the backend was redeployed.'],
    ['inc-77c5d1', 'ssp-0', 'bootstrap_slow', 'warning', 'recovered', 74, 14, 'Bootstrap still loading after 10m (52 of 70 tables).', 'Bootstrap finished; the SSP is ready.'],
    ['inc-6d0e9c', 'views', 'slow_views', 'warning', 'recovered', 98, 6, '3 views over the slow threshold (p99 > 100ms).', 'All views back under the threshold.'],
    ['inc-5c7a28', 'scheduler', 'ingest_stall', 'warning', 'interrupted', 121, 4, 'Ingest made no progress for 60s.', 'The scheduler restarted before recovery was seen.'],
    ['inc-4b19e0', 'ssp-1', 'ingest_lag', 'warning', 'recovered', 140, 3, 'Buffered events crossed the lag threshold (12,004).', 'Backlog drained; the circuit is live again.'],
    ['inc-3a88c4', 'worker', 'backend_unhealthy', 'warning', 'recovered', 152, 2, 'Health check timed out after 5s.', 'Health check answered in 18ms.'],
  ].map(([id, component, kind, severity, state, hoursAgo, minutes, first, last]) => ({
    id,
    component,
    kind,
    severity,
    state,
    started_at: ago(hoursAgo * 3_600_000),
    ended_at: ago(hoursAgo * 3_600_000 - minutes * 60_000),
    max_buffered_events: kind === 'ingest_lag' ? 12_004 : 0,
    event_count: 2,
    events: [
      incidentEvent(ago(hoursAgo * 3_600_000), component, kind, 'open', first),
      incidentEvent(ago(hoursAgo * 3_600_000 - minutes * 60_000), component, kind, state, last),
    ],
  })),
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
  cloud_linked: true,
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
  update_rate: o.rate === undefined ? 0 : o.rate,
  update_rate_peak: o.peak?.[0] ?? null,
  update_rate_peak_at_ms: o.peak ? ago(o.peak[1]) : null,
  hot: (o.rate ?? 0) >= 5,
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
      rate: 0.6,
      peak: [3.2, 260_000],
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
      // Mid-import: a CSV of tasks landing in the project iris has open.
      updates: 14_208,
      rate: 118.4,
      peak: [131.2, 45_000],
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
      rate: 0.6,
      peak: [1.4, 80_000],
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
      // Seen by one presence sample so far: no rate yet.
      rate: null,
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
  hot_view_rate: 5,
  server_time_ms: NOW,
};

const HOT_VIEW = VIEWS.views.find((v) => v.key === '907461255');

export const VIEW_DETAIL = {
  view: {
    ...HOT_VIEW,
    params: { 'auth.id': 'user:iris', access: 'account' },
    subscribers: [{ id: 'tab-ce0043', seen_at_ms: ago(11_400), age_secs: 11, stale: false }],
  },
  ssp: {
    ssp_id: 'ssp-1',
    view: {
      query_id: `_00_query:${HOT_VIEW.key}`,
      auth_id: 'user:iris',
      cached_records: 231,
      view_bytes: 182_400,
      operator_bytes: 96_300,
      total_bytes: 278_700,
    },
    merging: false,
    graphs: 34,
    subscribers: 34,
    total_bytes: 41_200_000,
  },
  siblings: { sessions: 1, users: 1, truncated: false, rows: [] },
  slow_ms: 50,
  large_view_rows: 400,
  hot_view_rate: 5,
  server_time_ms: NOW,
};

export const PRESENCE_FULL = {
  ...PRESENCE,
  slow_ms: 50,
  large_view_rows: 400,
  hot_view_rate: 5,
  hot: [{ key: '907461255', auth_id: 'user:iris', ssp_id: 'ssp-1', per_sec: 118.4 }],
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
  s3: { configured: true, endpoint: 'https://spooky-t-4c1e9a7b2f8d4e6a9b3c5d7e1f2a4b6c.fsn1.your-objectstorage.com', bucket: 'spooky-t-4c1e9a7b2f8d4e6a9b3c5d7e1f2a4b6c' },
  project_slug: 'acme',
  scheduler_status: 'ready',
  local: { current_running: null, queue_len: 0, recent: [] },
  // Two weeks of nightlies, one taken by hand before an upgrade, and one
  // night that failed: the page's history needs all three to look like one.
  catalog: [
    ...Array.from({ length: 13 }, (_, i) => catalogEntry({
      id: `bkp-${(0x9f2c1a - i * 4099).toString(16)}`,
      name: 'nightly',
      size: Math.round(412_140_032 - i * 3_100_000 - (i % 3) * 900_000),
      seq: 884_012 - i * 12_800,
      age: (8 + i * 24) * 3_600_000,
    })).filter((_, i) => i !== 4),
    { ...catalogEntry({ id: 'bkp-31e0f7', name: 'nightly', size: 0, seq: null, age: (8 + 4 * 24) * 3_600_000 }),
      status: 'failed', completed_at: null, error: 'upload to acme-backups failed: SlowDown (503) after 3 attempts' },
    catalogEntry({ id: 'bkp-22aa90', name: 'before upgrade', size: 402_653_184, seq: 858_770, age: 54 * 3_600_000 }),
  ].sort((x, y) => y.created_at.localeCompare(x.created_at)),
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

/**
 * Twenty minutes of tail: the lines above on a loop, a line every few seconds,
 * with one bad stretch (a backend timing out, then a job failing) so the page
 * has levels to tell apart.
 */
const LOG_TAIL = (() => {
  const out = [];
  const span = 20 * 60_000;
  let t = span;
  let i = 0;
  while (t > 0) {
    const base = LOG_LINES[i % LOG_LINES.length];
    const minute = Math.floor((span - t) / 60_000);
    let line = { ...base, fields: '' };
    if (minute >= 11 && minute <= 13 && i % 3 === 0) {
      line = {
        level: 'WARN',
        target: 'scheduler::backends',
        message: `worker health check took ${180 + (i % 7) * 31}ms (threshold 200ms)`,
        fields: 'backend=worker attempt=1',
      };
    }
    if (minute === 12 && i % 5 === 0) {
      line = {
        level: 'ERROR',
        target: 'scheduler::jobs',
        message: 'job /jobs/welcome-mail failed: backend returned 503',
        fields: 'table=digest_job attempt=4 max=4',
      };
    }
    out.push({ ...line, ts: ago(t) });
    t -= 3_000 + ((i * 1_700) % 4_000);
    i++;
  }
  return out;
})();

/** `text/event-stream` bodies, delivered in one go and then closed. */
export const STREAMS = {
  'GET /jobs/stream': `event: jobs
data: ${JSON.stringify(JOBS)}

`,
  'GET /workflows/stream': `event: runs
data: ${JSON.stringify(WORKFLOW_RUNS)}

`,
  'GET /logs': LOG_TAIL.map((line) => `event: line\ndata: ${JSON.stringify(line)}\n\n`).join(''),
};

/**
 * The scheduler's MCP tools as `tools/list` answers them: the real names, each
 * marked read-only or destructive the way the server annotates them.
 */
const tool = (name, description, kind = 'write') => ({
  name,
  description,
  inputSchema: { type: 'object' },
  annotations: { readOnlyHint: kind === 'read', destructiveHint: kind === 'destructive' },
});
const MCP_TOOLS = [
  tool('overview', 'Cluster health: scheduler, SSPs, backends, presence and jobs.', 'read'),
  tool('admin_config', 'What this scheduler is: version, project, cloud link.', 'read'),
  tool('operations_list', 'Restarts, backups and restores in flight or recent.', 'read'),
  tool('incidents_list', 'Lag, heartbeat and restart episodes.', 'read'),
  tool('incident_get', 'One incident with its event timeline.', 'read'),
  tool('logs_recent', 'Recent log lines from the scheduler or an SSP.', 'read'),
  tool('ssp_restart', 'Restart one SSP, optionally cleaning its state.'),
  tool('ssps_restart_all', 'Restart every SSP, rolling or all at once.', 'destructive'),
  tool('scheduler_restart', 'Restart the scheduler process.', 'destructive'),
  tool('backends_list', 'Every backend and its last health check.', 'read'),
  tool('backend_get', 'One backend with its recent checks.', 'read'),
  tool('views_list', 'Registered live queries, with their cost and subscribers.', 'read'),
  tool('view_get', 'One live query, its owner and its siblings.', 'read'),
  tool('view_history', 'How one live query has updated over time.', 'read'),
  tool('presence', 'Who is connected: users, tabs and views.', 'read'),
  tool('jobs_list', 'The outbox queue, filterable by table and status.', 'read'),
  tool('job_get', 'One job with its payload and result.', 'read'),
  tool('job_retry', 'Re-enqueue a failed job.'),
  tool('job_kill', 'Stop a running job.', 'destructive'),
  tool('jobs_clear', 'Delete finished jobs from the outbox.', 'destructive'),
  tool('schedules_list', 'Cron schedules and their next fire.', 'read'),
  tool('schedule_get', 'One schedule with its recent fires.', 'read'),
  tool('schedule_trigger', 'Fire a schedule now.'),
  tool('schedule_pause', 'Stop a schedule from firing.'),
  tool('schedule_resume', 'Let a paused schedule fire again.'),
  tool('schedule_release', 'Release a quarantined schedule key.'),
  tool('workflow_runs_list', 'Workflow runs and their status.', 'read'),
  tool('workflow_run_get', 'One run with its steps.', 'read'),
  tool('workflow_run_retry', 'Retry a failed run from its failed step.'),
  tool('workflow_run_rerun', 'Run a workflow again from the start.'),
  tool('workflow_run_cancel', 'Cancel a running workflow.', 'destructive'),
  tool('durable_instances_list', 'Durable workflow instances.', 'read'),
  tool('durable_instance_get', 'One durable instance and its history.', 'read'),
  tool('durable_instance_retry', 'Retry a failed durable instance.'),
  tool('durable_instance_cancel', 'Cancel a durable instance.', 'destructive'),
  tool('backups_list', 'The backup catalog.', 'read'),
  tool('backup_create', 'Take a backup now.'),
  tool('backups_configure', 'Set the backup schedule and retention.'),
  tool('backup_restore', 'Replace the database with a backup.', 'destructive'),
  tool('backup_restore_status', 'Where a restore has got to.', 'read'),
  tool('backup_delete', 'Delete a backup from storage.', 'destructive'),
  tool('cloud_stats', 'CPU, memory and disk of every container.', 'read'),
  tool('cloud_deployment', 'The current deployment on Sp00ky Cloud.', 'read'),
  tool('cloud_restart', 'Restart containers through Sp00ky Cloud.', 'destructive'),
  tool('pools_list', 'Machine pools and their state.', 'read'),
  tool('pool_machines', 'The machines in one pool.', 'read'),
  tool('pool_pause', 'Stop a pool from starting machines.'),
  tool('pool_resume', 'Let a paused pool start machines again.'),
  tool('machine_drain', 'Drain and stop one dedicated machine.', 'destructive'),
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
/** Resources (`GET /cloud/stats`): the acme stack as Sp00ky Cloud measures it. */
const statSeries = (base, swing, n = 60) => {
  const out = [];
  for (let i = 0; i < n; i++) out.push(Math.round((base + swing * Math.sin(i / 5) + (i % 7) * swing * 0.08) * 10) / 10);
  return out;
};
const memSeries = (mb, swing, n = 60) =>
  statSeries(mb, swing, n).map((v) => Math.round(v * 1024 * 1024));
const service = (id, name, role, vcpus, memoryMb, cpu, cpuSwing, memMb, extra = {}) => {
  const cpuS = statSeries(cpu, cpuSwing);
  const memS = memSeries(memMb, memMb * 0.04);
  return {
    id, name, role, status: 'running', vcpus, memory_mb: memoryMb, since: iso(3 * 86_400_000 + id.length * 3_600_000),
    sample: {
      ts: iso(9_000), cpu_pct: cpuS[cpuS.length - 1], mem_bytes: memS[memS.length - 1], mem_limit_bytes: memoryMb * 1024 * 1024,
      disk_read_bps: 0, disk_write_bps: 18_000, net_rx_bps: Math.round(cpu * 2_400), net_tx_bps: Math.round(cpu * 5_100),
    },
    cpu_series: cpuS, mem_series: memS, ...extra,
  };
};
/** A container's volumes on the host disk (160 GB, 62 GB of it free). */
const disk = (usedBytes) => ({ disk: { used_bytes: usedBytes, fs_bytes: 171_798_691_840, free_bytes: 66_571_993_088, measured_at: iso(2 * 60_000) } });
const CLOUD_STATS = {
  deployment: { id: 'dep_7f2c', version: 142, status: 'running', deployed_at: iso(2 * 3_600_000) },
  services: [
    service('vm-db', 'surrealdb', 'surrealdb', 3, 3072, 38, 14, 1740, disk(4_617_089_843)),
    service('vm-sched', 'scheduler', 'scheduler', 1, 3072, 11, 4, 1210, disk(1_288_490_189)),
    service('vm-ssp0', 'ssp-0', 'ssp', 1, 2048, 24, 9, 1380, disk(402_653_184)),
    service('vm-api', 'api', 'backend', 1, 512, 6, 3, 212, { image: 'acme/api' }),
    service('vm-mail', 'email', 'backend', 1, 512, 1.2, 0.6, 64, { image: 'acme/email' }),
    service('vm-render', 'renderer', 'backend', 1, 64, 0.4, 0.2, 9, { image: 'nginx:1.27-alpine', machine: 'media' }),
    service('vm-web', 'web', 'frontend', 1, 512, 0.3, 0.2, 18, { image: 'acme/web' }),
  ],
  metrics_available: true,
  machines: [
    { id: 'm1', name: 'media', backend: 'renderer', generation: 3, status: 'running', phase: 'serving', ip: '116.203.20.41',
      server_type: 'cx33', location: 'fsn1', serving: true, healthy_at: iso(26 * 3_600_000), created_at: iso(26 * 3_600_000) },
  ],
  pool_machines: [
    { pool: 'thumbs', machine_id: 'thumbs-4k2p', status: 'running', server_type: 'cx23', location: 'nbg1', created_at: iso(14 * 60_000) },
  ],
  bucket_volume: {
    size_gb: 100,
    provisioned_gb: 100,
    usage: { fs_bytes: 105_089_261_568, used_bytes: 31_457_280_000, free_bytes: 73_631_981_568, measured_at: iso(3 * 60_000) },
  },
  server_time: new Date(NOW).toISOString(),
};

/* ------------------------------------------------------------------ */
/* Run, job and schedule details: what the Runs page's sheet opens        */
/* ------------------------------------------------------------------ */

const step = (name, deps, status, startedAgo, finishedAgo, extra = {}) => ({
  step: name,
  depends_on: deps,
  status,
  job_id: status === 'pending' ? null : `search_job:wf_${extra.run ?? 'run'}_${name}`,
  output: status === 'success' ? { ok: true } : null,
  error: extra.error ?? null,
  created_at: iso(extra.runAgo ?? startedAgo),
  started_at: startedAgo == null ? null : iso(startedAgo),
  finished_at: finishedAgo == null ? null : iso(finishedAgo),
  dispatch_attempts: extra.attempts ?? 1,
});

const RUN_DETAILS = {
  'run-9f2c1a': [
    step('collect', [], 'success', 240_000, 200_000, { run: 'run-9f2c1a', runAgo: 240_000 }),
    step('render', ['collect'], 'success', 199_000, 70_000, { run: 'run-9f2c1a', runAgo: 240_000 }),
    step('reindex', ['collect'], 'processing', 198_000, null, { run: 'run-9f2c1a', runAgo: 240_000 }),
    step('send', ['render', 'reindex'], 'pending', null, null, { run: 'run-9f2c1a', runAgo: 240_000 }),
  ],
  'run-5c18b3': [
    step('snapshot', [], 'success', 30 * 3_600_000, 30 * 3_600_000 - 40_000, { run: 'run-5c18b3', runAgo: 30 * 3_600_000 }),
    step('export', ['snapshot'], 'failed', 30 * 3_600_000 - 39_000, 30 * 3_600_000 - 70_000, {
      run: 'run-5c18b3', runAgo: 30 * 3_600_000, attempts: 4,
      error: { code: 'backend_status', reason: 'backend returned 503 for /jobs/export', status: 503 },
    }),
    step('notify', ['export'], 'skipped', null, null, { run: 'run-5c18b3', runAgo: 30 * 3_600_000 }),
  ],
};

const runDetail = (id) => {
  const r = WORKFLOW_RUNS.runs.find((x) => x.id === id);
  const steps = RUN_DETAILS[id] ?? [
    step('main', [], r.status === 'running' ? 'processing' : r.status, NOW - Date.parse(r.created_at), r.finished_at ? NOW - Date.parse(r.finished_at) : null, { run: id, runAgo: NOW - Date.parse(r.created_at) }),
  ];
  // Line the steps up with the run's own start, whatever hours they were written at.
  const starts = steps.map((x) => Date.parse(x.created_at));
  const shift = Date.parse(r.created_at) - Math.min(...starts);
  const move = (v) => (v ? new Date(Date.parse(v) + shift).toISOString() : v);
  const aligned = steps.map((x) => ({ ...x, created_at: move(x.created_at), started_at: move(x.started_at), finished_at: move(x.finished_at) }));
  const end = aligned.map((x) => x.finished_at).filter(Boolean).sort().at(-1);
  const run = r.status === 'running' ? r : { ...r, finished_at: end ?? r.finished_at };
  return { run: { ...run, dag: null, input: { tenant: 'acme', window: '24h' }, target_table: 'search_job' }, steps: aligned };
};

const jobDetail = (j) => ({
  job: {
    id: j.id, status: j.status, path: j.path, table: j.table, key: j.key, origin: j.origin,
    payload: { tenant: 'acme', id: j.key }, result: j.status === 'success' ? { indexed: 1284 } : null,
    errors: j.status === 'failed'
      ? [1, 2, 3, 4].map((n) => ({ code: 'backend_status', reason: 'backend returned 503', attempt: n }))
      : null,
    retries: j.retries, max_retries: j.max_retries, retry_strategy: j.retry_strategy,
    assignee: j.assignee, timeout: 30, delay: null, lease_until: j.lease_until,
    created_at: j.created_at, updated_at: j.updated_at,
  },
});

const scheduleDetail = (sc) => {
  const runs = Array.from({ length: 12 }, (_, i) => {
    const failed = sc.last_run_status === 'failed' ? i % 3 === 0 : i === 5;
    const at = NOW - (i + 1) * (sc.every_ms ?? 3_600_000);
    return {
      id: `srun-${sc.name}-${i}`, schedule_name: sc.name, key: sc.name, kind: sc.kind,
      status: failed ? 'failed' : 'success', trigger: 'cron',
      job_id: sc.kind === 'job' ? `${sc.target_table}:sch_${sc.name}_${1700 - i}` : null,
      workflow_run: sc.kind === 'workflow' ? `run-${(0x7b04de + i).toString(16)}` : null,
      error: failed ? { code: 'backend_status', reason: 'backend returned 503' } : null,
      fire_at: new Date(at).toISOString(), created_at: new Date(at).toISOString(),
      finished_at: new Date(at + 42_000).toISOString(),
    };
  });
  const rollup = Array.from({ length: 24 }, (_, h) => ({
    bucket: new Date(NOW - (23 - h) * 3_600_000).toISOString(),
    success: 3 + (h % 4), failed: sc.last_run_status === 'failed' && h % 6 === 0 ? 1 : 0, skipped: 0, replaced: 0, killed: 0,
  }));
  return {
    schedule: sc, runs, rollup,
    quarantined: sc.for_each ? [{ key: 'workspace:7f20', consecutive_failures: 5, quarantined_at: iso(2 * 3_600_000) }] : [],
  };
};

export const ROUTES = {
  'GET /config': CONFIG,
  'GET /me': ME,
  'GET /overview': OVERVIEW,
  'GET /presence': PRESENCE_FULL,
  'GET /views': VIEWS,
  'GET /views/907461255': VIEW_DETAIL,
  'GET /schedules': SCHEDULES,
  'GET /jobs': JOBS,
  'GET /workflows/runs': WORKFLOW_RUNS,
  ...Object.fromEntries(WORKFLOW_RUNS.runs.map((r) => [`GET /workflows/runs/${r.id}`, runDetail(r.id)])),
  ...Object.fromEntries(JOBS.jobs.map((j) => [`GET /jobs/${encodeURIComponent(j.id)}`, jobDetail(j)])),
  ...Object.fromEntries(SCHEDULES.schedules.map((sc) => [`GET /schedules/${sc.name}`, scheduleDetail(sc)])),
  // The sign-in form, so the fixture server can be signed into and out of.
  'POST /session': { token: TOKEN, mode: 'roster', subject: ME.subject, label: ME.label, expires_in_secs: 43_200 },
  'POST /logout': { ok: true },
  'POST /tokens': {
    token: 'spky_mcp_7Hq2vT9xKcN4pLw8RzYb3mFa6sJd1uGe',
    label: 'claude-code on my laptop',
    scope: 'read',
    expires_at: new Date(NOW + 90 * 86_400_000).toISOString(),
    endpoint: '/admin/api/mcp',
  },
  'DELETE /tokens': { ok: true },
  // Starting a backup answers with the operation that runs it.
  'POST /backups': {
    backup_id: 'bkp-a1c3f0',
    operation: {
      id: 'op-a1c3f0', kind: 'backup_create', target: 'bkp-a1c3f0', requested_by: 'user:mira',
      started_at: NOW, finished_at: null, status: 'running', message: 'queued', detail: { backup_id: 'bkp-a1c3f0', status: 'queued' },
    },
  },
  'GET /backends': { backends: BACKENDS, check_interval_secs: 10 },
  ...Object.fromEntries(Object.entries(BACKEND_DETAILS).map(([name, detail]) => [`GET /backends/${name}`, detail])),
  'GET /incidents': INCIDENT_LIST,
  ...Object.fromEntries(INCIDENTS.map((incident) => [`GET /incidents/${incident.id}`, { incident }])),
  'GET /backups': BACKUPS,
  'GET /cloud/stats': CLOUD_STATS,
  'POST /mcp': MCP_TOOLS_RESULT,
};
