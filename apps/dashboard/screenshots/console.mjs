/**
 * Serve the console build against fixtures, for a browser.
 *
 *   pnpm --filter @spooky-sync/dashboard build:console
 *   node apps/dashboard/screenshots/console.mjs        # http://localhost:4320/
 *
 * Stands in for both halves of the console: Sp00ky Cloud's `/v1` (a signed-in
 * account with two workspaces, deployments, env, members...) and a project's
 * admin API, answered under `/fx/admin/api/*` from the same `fixtures.mjs` the
 * scheduler dashboard is photographed with. `?signedout` shows the landing.
 *
 * The workspace `nadia` has no projects, so it opens on the first-project page.
 * `GET /__onboarding?connected=1&cli=1&config=1&project=pending|building|running&repo=configured`
 * moves that page's world on, the way a terminal elsewhere would: it updates by
 * itself, and `?reset=1` starts over.
 *
 * Settings > Database shows one SurrealDB upgrade scenario at a time: open any
 * page with `?upgrade=soak` (see UPGRADES below for the names; `?upgrade=` goes
 * back to each project's own offer). The buttons move it on as the API would.
 */
import { createServer } from 'node:http';
import { readFile } from 'node:fs/promises';
import { dirname, extname, join, normalize, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { ROUTES, STREAMS } from './fixtures.mjs';

const HERE = dirname(fileURLToPath(import.meta.url));
const DIST = resolve(HERE, '..', 'dist-console');
const PORT = Number(process.env.PORT ?? 4320);
const ORIGIN = `http://localhost:${PORT}`;
const REAL_ADMIN = process.env.FX_ADMIN;
// The policy console.sp00ky.cloud serves (spooky-cloud internal/console/static.go),
// so a page that needs more than it allows fails here first.
const CSP = [
  "default-src 'self'",
  "script-src 'self' 'wasm-unsafe-eval'",
  "worker-src 'self'",
  "style-src 'self' 'unsafe-inline'",
  "img-src 'self' data: https://avatars.githubusercontent.com",
  "font-src 'self'",
  "connect-src 'self'",
  "frame-ancestors 'none'",
  "base-uri 'none'",
  "form-action 'self'",
  "object-src 'none'",
].join('; ');
let signedOut = false;

const TYPES = { '.html': 'text/html; charset=utf-8', '.js': 'text/javascript', '.css': 'text/css', '.woff2': 'font/woff2', '.wasm': 'application/wasm', '.svg': 'image/svg+xml' };

const now = Date.now();
const iso = (msAgo) => new Date(now - msAgo).toISOString();
const H = 3600_000;

const WORKSPACES = [
  {
    id: 'ten-acme', name: 'acme', slug: 'acme', type: 'organization', role: 'admin',
    projects: [
      { id: 'prj-wp', slug: 'whitepawn', plan: 'pro', platform: 'hetzner', status: 'active', deployment_status: 'provisioning', deployment_version: 43, admin: 'scheduler' },
      { id: 'prj-td', slug: 'threads-demo', plan: 'payg', platform: 'cloudflare', status: 'active', deployment_status: 'running', deployment_version: 18, admin: 'ssp' },
      { id: 'prj-pa', slug: 'pawn-analytics', plan: 'starter', platform: 'hetzner', status: 'active', deployment_status: 'degraded', deployment_version: 5, admin: 'scheduler' },
      { id: 'prj-sl', slug: 'staging-lab', plan: 'starter', platform: 'hetzner', status: 'suspended', deployment_status: 'superseded', deployment_version: 2, admin: 'scheduler' },
    ],
  },
  { id: 'ten-mira', name: 'mira', slug: 'mira', type: 'personal', role: 'admin', projects: [{ id: 'prj-n', slug: 'notes', plan: 'free', platform: 'cloudflare', status: 'active', deployment_status: 'running', deployment_version: 3, admin: 'ssp' }] },
];

const FRESH = { id: 'ten-nadia', name: 'nadia', slug: 'nadia', type: 'personal', role: 'admin', projects: [] };
WORKSPACES.push(FRESH);

// The first-project page's world, moved on through `/__onboarding`.
const ONBOARDING = { connected: false, cli: false, config: false, repo: 'plain', project: null, linked: false };
const NEW_PROJECT = (project) => ({
  id: 'prj-new', slug: 'tideline', plan: 'payg', platform: 'cloudflare', admin: 'ssp',
  status: project === 'pending' ? 'pending_payment' : 'active',
  deployment_status: project === 'pending' || project === 'paid' ? null : project === 'building' ? 'provisioning' : project,
  deployment_version: project === 'running' ? 1 : null,
});
const repo = (owner, name, language, description, days, priv = true) => ({
  owner, name, full_name: `${owner}/${name}`, private: priv, default_branch: 'main', language, description, pushed_at: iso(days * 24 * H),
});
const GITHUB = {
  configured: true,
  install_url: 'https://github.com/apps/sp00ky-cloud/installations/new?state=fixture',
  installations: [
    {
      id: 7001, account: 'nadia-k', type: 'User', selection: 'all', manage_url: 'https://github.com/settings/installations/7001', avatar_url: 'https://avatars.githubusercontent.com/u/9919?v=4',
      repos: [
        repo('nadia-k', 'tideline', 'TypeScript', 'Tide tables and harbour alerts for small boats', 0.1),
        repo('nadia-k', 'fieldnotes', 'TypeScript', 'Shared notebooks for survey crews, offline first', 3),
        repo('nadia-k', 'harbour-api', 'Go', 'Port schedules as a small API', 11),
        repo('nadia-k', 'dotfiles', 'Shell', null, 40, false),
        repo('nadia-k', 'chart-render', 'Rust', 'Vector nautical charts to tiles', 120, false),
        repo('nadia-k', 'sailing-log', 'Dart', 'Logbook app', 300),
      ],
    },
    { id: 7002, account: 'northsound', type: 'Organization', selection: 'selected', manage_url: 'https://github.com/organizations/northsound/settings/installations/7002', avatar_url: 'https://avatars.githubusercontent.com/u/9892?v=4', repos: [repo('northsound', 'dock-booking', 'TypeScript', 'Berth bookings', 2)] },
  ],
};
const inspection = () => {
  const configured = ONBOARDING.repo === 'configured';
  return {
    repo: 'nadia-k/tideline', branch: 'main', sha: ONBOARDING.config ? '7c41e09ab2d' : 'a3f9c1e7d2b', private: true, language: 'TypeScript',
    description: 'Tide tables and harbour alerts for small boats', url: 'https://github.com/nadia-k/tideline',
    empty: false, files: 212, truncated: false,
    config: configured
      ? { path: 'sp00ky.yml', valid: true, slug: 'tideline', mode: 'singlenode', schema: './schema', apps: [{ name: 'web', type: 'frontend' }] }
      : ONBOARDING.config ? { path: 'spooky/sp00ky.yml', valid: true, mode: 'singlenode', schema: '.', apps: [] } : null,
    package_manager: 'pnpm', monorepo: true,
    apps: [
      { path: 'apps/web', name: '@tideline/web', kind: 'frontend', framework: 'Solid' },
      { path: 'services/alerts', name: '@tideline/alerts', kind: 'backend', framework: 'Hono' },
    ],
    schema_dir: configured ? 'schema' : ONBOARDING.config ? 'spooky' : undefined, schema_files: configured ? 6 : ONBOARDING.config ? 1 : 0,
    dockerfiles: 1, uses_spooky: configured, has_cli: false,
  };
};

const DEPLOYMENTS = {
  deployments: [
    { id: 'd43', version: 43, status: 'provisioning', error: null, live: false, live_from: null, live_until: null, source: 'git', actor: null, rollback_of: null, images: { scheduler: '0.0.1-canary.370', ssp: '0.0.1-canary.370', surrealdb: 'v3.1.5' }, created_at: iso(130_000), deployed_at: null, updated_at: iso(5000), build: { commit_sha: 'a3f9c1e7d2', commit_message: 'fix(game): pending moves survive a reload', commit_author: 'mono424', status: 'deploying' } },
    { id: 'd42', version: 42, status: 'superseded', error: null, live: true, live_from: iso(52 * H), live_until: null, source: 'git', actor: null, rollback_of: null, images: { scheduler: '0.0.1-canary.369', ssp: '0.0.1-canary.369', surrealdb: 'v3.1.5' }, created_at: iso(52 * H + 222_000), deployed_at: iso(52 * H), updated_at: iso(130_000), build: { commit_sha: '9b2e07d1aa', commit_message: 'feat(puzzles): daily set survives a reload', commit_author: 'mono424', status: 'running' } },
    { id: 'd41', version: 41, status: 'superseded', error: null, live: false, live_from: iso(70 * H), live_until: iso(52 * H), source: 'cli', actor: 'mira', rollback_of: null, images: {}, created_at: iso(70 * H + 241_000), deployed_at: iso(70 * H), updated_at: iso(52 * H), build: null },
    { id: 'd40', version: 40, status: 'failed', error: 'migration timed out on game_analysis', live: false, live_from: null, live_until: null, source: 'git', actor: null, rollback_of: null, images: {}, created_at: iso(80 * H), deployed_at: null, updated_at: iso(80 * H), build: { commit_sha: '51c0aa3b9e', commit_message: 'feat(analysis): engine depth per plan', commit_author: 'mono424', status: 'failed' } },
    { id: 'd39', version: 39, status: 'superseded', error: null, live: false, live_from: iso(96 * H), live_until: iso(70 * H), source: 'rollback', actor: 'mira', rollback_of: 37, images: {}, created_at: iso(96 * H + 58_000), deployed_at: iso(96 * H), updated_at: iso(70 * H), build: null },
  ],
  builds: [],
};

const CLOUD = {
  'GET /console/me': { account: { id: 'acct-1', github_login: 'mono424', email: 'mira@acme.dev', created_at: iso(400 * 24 * H) }, session: 'sess-1', grants: true },
  'GET /console/workspaces': { workspaces: WORKSPACES },
  'GET /console/sessions': [
    { id: 'sess-1', ip: '84.150.12.9', user_agent: 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) Chrome/131', created_at: iso(3 * H), last_seen_at: iso(60_000), expires_at: iso(-6 * 24 * H), current: true },
    { id: 'sess-2', ip: '84.150.12.9', user_agent: 'Mozilla/5.0 (iPhone; CPU iPhone OS 18_0) Safari/604.1', created_at: iso(40 * H), last_seen_at: iso(20 * H), expires_at: iso(-5 * 24 * H), current: false },
  ],
  'GET /vault/status': { initialized: true },
  'GET /vault/ci-access': { enabled: false },
  'GET /vault/key-params': { salt: '00112233445566778899aabbccddeeff', params: { time: 1, memory: 65536, threads: 4 } },
  'GET /billing/usage': [
    { project: 'whitepawn', metric: 'ssp_hours', total: 2160 },
    { project: 'whitepawn', metric: 'bucket_storage_gb', total: 12.4 },
    { project: 'threads-demo', metric: 'requests', total: 1840220 },
  ],
  'GET /auth/keys': null,
  'GET /mcp/tokens': [{ id: 'mt1', name: 'claude on my laptop', prefix: 'mcp_live_3fa1', scopes: ['mcp:full'], created_at: iso(9 * 24 * H), last_used_at: iso(2 * H) }],
};

// The Database tab's world: `/projects/{pid}/database/upgrade`, one scenario at a time.
const MIN = 60_000;
const SCHEDULER_SDK = { code: 'scheduler_sdk', message: 'Projects with a scheduler stay on v3.3.5 for now: its SurrealDB SDK refuses 4.x servers.' };
const BUSY = { code: 'busy', message: 'An upgrade is already running.' };
const offer = (over) => ({ current: 'v3.1.5', beta: false, one_way: false, up_to_date: false, blockers: [], ...over });
const OFFER_3 = { target: 'v3.3.5', kind: 'swap' };
const OFFER_4 = { current: 'v3.3.5', target: 'v4.0.0-beta.1', kind: 'replay', beta: true, one_way: true };
const replay = (over) => ({
  id: 'up-7', kind: 'replay', state: 'sync', active: true, from_version: 'v3.3.5', to_version: 'v4.0.0-beta.1', reverse: false, hold_at_ready: false, abort_requested: false,
  backup_id: 'bk-2291', stats: { frozen_ms: 180, open_ms: 41_300, rebuilt_indexes: 6, applied_entries: 8112, applied_rows: 1_284_120, lag_ms: 2400 }, created_at: iso(38 * MIN), ...over,
});
const swap = (over) => ({
  id: 'up-6', kind: 'swap', state: 'rehearse', active: true, from_version: 'v3.1.5', to_version: 'v3.3.5', reverse: false, hold_at_ready: false, abort_requested: false,
  backup_id: 'bk-2288', stats: {}, created_at: iso(4 * MIN), ...over,
});
const CUT = { hold_ms: 1240, verified_tables: 42, verified_rows: 3_418_907 };
const UPGRADES = {
  // What is on offer.
  offer3: () => offer(OFFER_3),
  offer4: () => offer(OFFER_4),
  blocked: () => offer({ ...OFFER_4, blockers: [SCHEDULER_SDK] }),
  uptodate: () => offer({ current: 'v4.0.0-beta.1', up_to_date: true }),
  nodb: () => offer({ blockers: [{ code: 'no_database', message: 'This project does not run on a database of its own here.' }] }),
  // A move inside 3.x.
  swap: () => offer({ ...OFFER_3, blockers: [BUSY], upgrade: swap() }),
  swapready: () => offer({ ...OFFER_3, blockers: [BUSY], upgrade: swap({ state: 'ready', hold_at_ready: true, stats: { frozen_ms: 90, open_ms: 2140 } }) }),
  swapflip: () => offer({ ...OFFER_3, blockers: [BUSY], upgrade: swap({ state: 'flip_start', flip_at: iso(4000), stats: { frozen_ms: 90, open_ms: 2140 } }) }),
  swapdone: () => offer({ current: 'v3.3.5', ...OFFER_4, blockers: [SCHEDULER_SDK], upgrade: swap({ state: 'done', active: false, flip_at: iso(2 * H), rollback_until: iso(-166 * H), finished_at: iso(2 * H - MIN), stats: { frozen_ms: 90, open_ms: 2140, hold_ms: 4310 } }) }),
  // The step to 4.x.
  clone: () => offer({ ...OFFER_4, blockers: [BUSY], upgrade: replay({ state: 'clone', created_at: iso(6 * MIN), stats: { frozen_ms: 180 } }) }),
  sync: () => offer({ ...OFFER_4, blockers: [BUSY], upgrade: replay() }),
  verify: () => offer({ ...OFFER_4, blockers: [BUSY], upgrade: replay({ state: 'verify', stats: { ...replay().stats, lag_ms: 800, verified_rows: 912_004, detail: 'comparing game_analysis' } }) }),
  ready: () => offer({ ...OFFER_4, blockers: [BUSY], upgrade: replay({ state: 'ready', hold_at_ready: true, stats: { ...replay().stats, lag_ms: undefined, ...CUT, hold_ms: undefined } }) }),
  fence: () => offer({ ...OFFER_4, blockers: [BUSY], upgrade: replay({ state: 'fence', stats: { ...replay().stats, lag_ms: undefined, ...CUT, hold_ms: undefined } }) }),
  soak: () => offer({ current: 'v4.0.0-beta.1', up_to_date: true, blockers: [BUSY], upgrade: replay({ state: 'soak', flip_at: iso(18 * MIN), soak_until: iso(-42 * MIN), stats: { ...replay().stats, lag_ms: undefined, ...CUT } }) }),
  soakwarn: () => offer({ current: 'v4.0.0-beta.1', up_to_date: true, blockers: [BUSY], upgrade: replay({ state: 'soak', flip_at: iso(18 * MIN), soak_until: iso(-42 * MIN), warning: 'The abort failed and the new version is still serving: the old database did not open its data.', stats: { ...replay().stats, lag_ms: undefined, ...CUT } }) }),
  reverse: () => offer({ current: 'v4.0.0-beta.1', up_to_date: true, blockers: [BUSY], upgrade: replay({ state: 'sync', reverse: true, abort_requested: true, flip_at: iso(24 * MIN), stats: { open_ms: 3800, frozen_ms: 180, applied_rows: 2204, lag_ms: 600, hold_ms: 1240 } }) }),
  rollback: () => offer({ current: 'v4.0.0-beta.1', up_to_date: true, blockers: [BUSY], upgrade: replay({ state: 'rollback', flip_at: iso(24 * MIN), stats: { ...CUT } }) }),
  undoing: () => offer({ ...OFFER_4, blockers: [BUSY], upgrade: replay({ state: 'abort', abort_requested: true, stats: { frozen_ms: 180 } }) }),
  // How it ended.
  done: () => offer({ current: 'v4.0.0-beta.1', up_to_date: true, upgrade: replay({ state: 'done', active: false, flip_at: iso(3 * H), rollback_until: iso(-165 * H), finished_at: iso(2 * H), stats: { ...replay().stats, lag_ms: undefined, ...CUT } }) }),
  closed: () => offer({ current: 'v4.0.0-beta.1', up_to_date: true, upgrade: replay({ state: 'done', active: false, flip_at: iso(200 * H), created_at: iso(201 * H), finished_at: iso(199 * H), stats: { ...replay().stats, lag_ms: undefined, ...CUT } }) }),
  failed: () => offer({ ...OFFER_4, upgrade: replay({ state: 'failed', active: false, error: 'the two databases differ: table game_analysis, row game_analysis:9f2a has a different value for depth', finished_at: iso(6 * MIN), stats: { ...replay().stats, lag_ms: undefined, verified_rows: 912_004 } }) }),
  failedflip: () => offer({ current: 'v3.1.5', ...OFFER_3, upgrade: swap({ state: 'failed', active: false, flip_at: iso(30 * MIN), error: 'The old database could not be started again: container exited with code 137', finished_at: iso(20 * MIN), stats: { frozen_ms: 90, open_ms: 2140 } }) }),
  aborted: () => offer({ ...OFFER_4, upgrade: replay({ state: 'aborted', active: false, reverse: true, flip_at: iso(50 * MIN), finished_at: iso(9 * MIN), stats: { open_ms: 3800, applied_rows: 2204, hold_ms: 1180 } }) }),
  cancelled: () => offer({ ...OFFER_4, upgrade: replay({ state: 'aborted', active: false, finished_at: iso(9 * MIN), stats: { frozen_ms: 180 } }) }),
  rolled_back: () => offer({ ...OFFER_4, upgrade: replay({ state: 'rolled_back', active: false, flip_at: iso(70 * MIN), finished_at: iso(61 * MIN), stats: { ...CUT } }) }),
};
// What each button leads to, from where it can be pressed.
const UPGRADE_MOVES = {
  cutover: { ready: 'fence', swapready: 'swapflip' },
  cancel: { soak: 'reverse', soakwarn: 'reverse', swap: 'cancelled', swapready: 'cancelled', clone: 'cancelled', sync: 'cancelled', verify: 'cancelled', ready: 'cancelled' },
  finish: { soak: 'done', soakwarn: 'done' },
  rollback: { soak: 'rolled_back', soakwarn: 'rolled_back', done: 'rolled_back', swapdone: 'rolled_back' },
};
// Null is each project's own: a project with a scheduler is held on 3.x, Cloudflare goes on to 4.x.
let upgradeScenario = null;
const upgradeOf = (projectId) => upgradeScenario ?? (projectId === 'prj-td' ? 'offer4' : 'offer3');

function upgradeRoute(method, projectId, action, body) {
  const at = upgradeOf(projectId);
  const shown = UPGRADES[at]();
  if (method === 'GET') return [200, shown];
  if (!action) {
    if (shown.blockers.length) return [409, { code: shown.blockers[0].code, error: shown.blockers[0].message }];
    if (body?.to !== shown.target) return [409, { code: 'wrong_step', error: `the next step from ${shown.current} is ${shown.target}, not ${body?.to}` }];
    upgradeScenario = shown.kind === 'swap' ? (body.no_cutover ? 'swapready' : 'swap') : body.no_cutover ? 'ready' : 'clone';
    return [202, UPGRADES[upgradeScenario]().upgrade];
  }
  const next = UPGRADE_MOVES[action]?.[at];
  if (!next) return [409, { code: action === 'rollback' ? 'no_rollback' : 'no_db_upgrade', error: `nothing to ${action} from ${at}` }];
  upgradeScenario = next;
  return [200, UPGRADES[next]().upgrade];
}

function cloudRoute(method, path, url, body) {
  FRESH.projects = ONBOARDING.project ? [NEW_PROJECT(ONBOARDING.project)] : [];
  if (path === '/console/me') {
    const me = CLOUD['GET /console/me'];
    return [200, { ...me, account: { ...me.account, cli_login_at: ONBOARDING.cli ? iso(4000) : null }, api_url: 'https://api-stg.sp00ky.cloud' }];
  }
  if (path === '/github') return [200, ONBOARDING.connected ? GITHUB : { ...GITHUB, installations: [] }];
  if (path === '/github/inspect') {
    return url.searchParams.get('repo') === 'nadia-k/tideline' ? [200, inspection()] : [404, { error: 'the app cannot see that repository' }];
  }
  if (path === '/projects/prj-new/link') {
    if (method === 'PATCH') ONBOARDING.linked = true;
    return [200, ONBOARDING.linked ? { status: 'linked', repo_owner: 'nadia-k', repo_name: 'tideline', branch: 'main', auto_deploy: true, config_path: 'sp00ky.yml' } : { status: 'not_linked' }];
  }
  const exact = CLOUD[`${method} ${path}`];
  if (exact !== undefined) return [200, exact];
  let m;
  if ((m = path.match(/^\/console\/projects\/([^/]+)\/grant$/)) && method === 'POST') {
    const p = WORKSPACES.flatMap((w) => w.projects).find((x) => x.id === m[1]);
    if (p?.platform === 'cloudflare') return [200, { grant: 'fx', admin_url: `${ORIGIN}/cf`, admin: 'ssp', scope: 'full', expires_at: Math.floor(now / 1000) + 60 }];
    return [200, { grant: 'fixture-grant', admin_url: `${ORIGIN}/fx`, admin: 'scheduler', scope: 'full', expires_at: Math.floor(now / 1000) + 60 }];
  }
  if ((m = path.match(/^\/projects\/([^/]+)\/database\/upgrade(?:\/(cutover|cancel|finish|rollback))?$/))) return upgradeRoute(method, m[1], m[2], body);
  if (/^\/console\/projects\/[^/]+\/deployments$/.test(path)) return [200, DEPLOYMENTS];
  if (/^\/console\/workspaces\/[^/]+\/activity$/.test(path)) {
    return [200, { events: [
      { id: 3, at: iso(60_000), actor: 'mono424', action: 'console.grant', project: 'whitepawn', ip: '84.150.12.9', detail: {} },
      { id: 2, at: iso(3 * H), actor: 'mira', action: 'console.grant', project: 'pawn-analytics', ip: '91.2.33.4', detail: {} },
    ] }];
  }
  if (/^\/projects\/[^/]+\/deployment$/.test(path)) {
    const backends = ['comments', 'chat', 'games', 'server', 'iroh-relay', 'livekit', 'play', 'record', 'relay', 'surreal', 'video', 'call'];
    return [200, { deployment: { id: 'd42', version: 42, status: 'superseded' }, vms: [
      { role: 'scheduler', status: 'running', name: 'scheduler' }, { role: 'ssp', status: 'running', name: 'ssp-0' }, { role: 'ssp', status: 'running', name: 'ssp-1' },
      { role: 'surrealdb', status: 'running', name: 'surrealdb' }, { role: 'frontend', status: 'running', name: 'app' },
      ...backends.map((name) => ({ role: 'backend', status: name === 'livekit' ? 'starting' : 'running', name })),
    ], urls: { app: 'https://whitepawn-app.stg.spky.cloud', ...Object.fromEntries(backends.map((n) => [n, `https://whitepawn-${n}.stg.spky.cloud`])), surrealdb: 'https://whitepawn-db.stg.spky.cloud', admin: 'https://whitepawn-admin.stg.spky.cloud/admin' } }];
  }
  if (/^\/projects\/[^/]+\/link$/.test(path) && method === 'GET') return [200, { status: 'linked', repo_owner: 'mono424', repo_name: 'whitepawn', branch: 'main', auto_deploy: true, config_path: 'sp00ky.yml' }];
  if (/^\/projects\/[^/]+\/link\/runs$/.test(path)) return [200, [
    { id: 'r1', commit_sha: 'a3f9c1e7d2', commit_message: 'fix(game): pending moves survive a reload', commit_author: 'mono424', status: 'deploying', created_at: iso(130_000) },
    { id: 'r0', commit_sha: '9b2e07d1aa', commit_message: 'feat(puzzles): daily set survives a reload', commit_author: 'mono424', status: 'running', created_at: iso(52 * H) },
  ]];
  if (/^\/projects\/[^/]+\/envs$/.test(path)) return [200, [
    { name: 'LICHESS_TOKEN', has_dev: true, has_prod: true, updated_at: iso(9 * 24 * H) },
    { name: 'RESEND_API_KEY', has_dev: false, has_prod: true, updated_at: iso(28 * 24 * H) },
    { name: 'STOCKFISH_THREADS', has_dev: true, has_prod: true, updated_at: iso(32 * 24 * H) },
  ]];
  if (/^\/projects\/[^/]+\/envs\/load$/.test(path)) return [200, { variables: { LICHESS_TOKEN: 'lip_fixture_9f2', RESEND_API_KEY: 're_fixture_x1', STOCKFISH_THREADS: '4' } }];
  if (/^\/projects\/[^/]+\/domains$/.test(path)) return [200, [
    { id: 'dm1', domain: 'whitepawn.com', target_kind: 'frontend', target_name: 'app', status: 'active', dns_target: 'connect.stg.spky.cloud', created_at: iso(30 * 24 * H) },
    { id: 'dm2', domain: 'api.whitepawn.com', target_kind: 'backend', target_name: 'server', status: 'pending_dns', dns_target: 'connect.stg.spky.cloud', created_at: iso(2 * H) },
  ]];
  if (/^\/tenants\/[^/]+\/members$/.test(path)) return [200, [
    { id: 'm1', account_id: 'acct-1', role: 'admin', email: 'mira@acme.dev', github_login: 'mono424', created_at: iso(300 * 24 * H) },
    { id: 'm2', account_id: 'acct-2', role: 'member', email: 'sam@acme.dev', github_login: 'samk', created_at: iso(40 * 24 * H) },
  ]];
  if ((m = path.match(/^\/tenants\/([^/]+)$/))) {
    const w = WORKSPACES.find((x) => x.id === m[1]);
    if (!w) return [404, { error: 'tenant not found' }];
    return [200, { id: w.id, name: w.name, slug: w.slug, type: w.type, created_at: iso(400 * 24 * H) }];
  }
  if (/^\/tenants\/[^/]+\/invitations$/.test(path)) return [200, [{ id: 'i1', email: 'jo@acme.dev', role: 'member', expires_at: iso(-5 * 24 * H) }]];
  if (method !== 'GET') return [200, { status: 'ok' }];
  return [404, { error: `no fixture for ${method} ${path}` }];
}

async function sendFile(res, path) {
  const body = await readFile(path);
  res.writeHead(200, { 'content-type': TYPES[extname(path)] ?? 'application/octet-stream', 'content-security-policy': CSP });
  res.end(body);
}

createServer(async (req, res) => {
  const url = new URL(req.url ?? '/', ORIGIN);
  const json = (status, body) => {
    res.writeHead(status, { 'content-type': 'application/json', 'access-control-allow-origin': '*', 'access-control-allow-headers': '*' });
    res.end(JSON.stringify(body));
  };
  if (url.pathname === '/__onboarding') {
    if (url.searchParams.has('reset')) Object.assign(ONBOARDING, { connected: false, cli: false, config: false, repo: 'plain', project: null, linked: false });
    for (const key of ['connected', 'cli', 'config']) if (url.searchParams.has(key)) ONBOARDING[key] = url.searchParams.get(key) === '1';
    if (url.searchParams.has('repo')) ONBOARDING.repo = url.searchParams.get('repo');
    if (url.searchParams.has('project')) ONBOARDING.project = url.searchParams.get('project') || null;
    return json(200, ONBOARDING);
  }
  if (url.searchParams.has('signedout')) signedOut = true;
  if (url.searchParams.has('signedin')) signedOut = false;
  if (url.searchParams.has('upgrade')) upgradeScenario = url.searchParams.get('upgrade') || null;
  if (url.pathname.startsWith('/v1/')) {
    if (signedOut) return json(401, { error: 'missing authorization header', code: 'unauthorized' });
    const chunks = [];
    for await (const chunk of req) chunks.push(chunk);
    const sent = chunks.length ? JSON.parse(Buffer.concat(chunks).toString()) : undefined;
    const [status, body] = cloudRoute(req.method, url.pathname.slice(3), url, sent);
    return json(status, body);
  }
  // A Cloudflare project whose SSP predates console grants.
  if (url.pathname.startsWith('/cf/admin/api/config')) return json(200, { ...ROUTES['GET /config'], host: 'ssp', cloud_login: undefined });
  if (url.pathname.startsWith('/fx/admin/api') && REAL_ADMIN) {
    if (req.method === 'OPTIONS') return json(204, {});
    const chunks = [];
    for await (const chunk of req) chunks.push(chunk);
    const session = url.pathname.endsWith('/session');
    const upstream = await fetch(`${REAL_ADMIN}${url.pathname.replace(/^\/fx/, '')}${url.search}`, {
      method: req.method,
      headers: { 'content-type': req.headers['content-type'] ?? 'application/json', ...(req.headers.authorization ? { authorization: req.headers.authorization } : {}) },
      body: session ? JSON.stringify({ password: process.env.FX_ADMIN_PASSWORD }) : chunks.length ? Buffer.concat(chunks) : undefined,
    });
    const text = await upstream.text();
    // No socket through this little proxy: the pages poll.
    if (url.pathname.endsWith('/config')) return json(200, { ...JSON.parse(text), cloud_login: true, live: undefined });
    res.writeHead(upstream.status, { 'content-type': upstream.headers.get('content-type') ?? 'application/json', 'access-control-allow-origin': '*' });
    return res.end(text);
  }
  if (url.pathname.startsWith('/fx/admin/api')) {
    if (req.method === 'OPTIONS') return json(204, {});
    const key = `${req.method} ${url.pathname.replace(/^\/fx\/admin\/api/, '') || '/'}`;
    if (key === 'GET /config') return json(200, { ...ROUTES['GET /config'], cloud_login: true, project_slug: 'whitepawn' });
    if (key === 'POST /session') return json(200, { token: 'fixture-session', mode: 'cloud', subject: 'cloud:acct-1', label: 'mono424', expires_in_secs: 900 });
    if (STREAMS[key] !== undefined) {
      res.writeHead(200, { 'content-type': 'text/event-stream', 'access-control-allow-origin': '*' });
      return res.end(STREAMS[key]);
    }
    const body = ROUTES[key];
    return json(body === undefined ? 404 : 200, body ?? { error: `no fixture for ${key}` });
  }
  const rel = normalize(url.pathname);
  const file = join(DIST, rel);
  try {
    if (!file.startsWith(DIST) || rel.endsWith('/')) throw new Error('index');
    await sendFile(res, file);
  } catch {
    await sendFile(res, join(DIST, 'index.html')).catch(() => {
      res.writeHead(500);
      res.end('dist-console/ is missing: run `pnpm build:console` first');
    });
  }
}).listen(PORT, () => console.log(`console fixtures on ${ORIGIN}/`));
