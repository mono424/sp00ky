// Run with `pnpm test` (`node --test`; Node 22.18+ strips the types itself).
import { test } from 'node:test';
import assert from 'node:assert/strict';
import {
  branchNote,
  draftFrom,
  moves,
  patchBody,
  problem,
  saveConfirm,
  savedMessage,
  unlinkConfirm,
  validBranch,
  validConfigPath,
  type Link,
} from '../src/console/linkPlan.ts';

const linked: Link = {
  status: 'linked',
  installation_id: 7,
  repo_owner: 'acme',
  repo_name: 'web',
  branch: 'main',
  auto_deploy: true,
  config_path: 'sp00ky.yml',
};

test('an edit starts from the link, or from nothing on the first account', () => {
  assert.deepEqual(draftFrom(linked), { installation: 7, repo: 'acme/web', branch: 'main', configPath: 'sp00ky.yml', autoDeploy: true });
  assert.deepEqual(draftFrom({ status: 'not_linked' }, 9), { installation: 9, repo: '', branch: '', configPath: 'sp00ky.yml', autoDeploy: true });
  // An older row without a manifest path reads as the default.
  assert.equal(draftFrom({ ...linked, config_path: '' }).configPath, 'sp00ky.yml');
});

test('an unchanged draft saves nothing', () => {
  assert.equal(patchBody(linked, draftFrom(linked)), null);
  // Whitespace is not a change.
  assert.equal(patchBody(linked, { ...draftFrom(linked), branch: ' main ' }), null);
});

test('a branch or manifest change sends only that', () => {
  assert.deepEqual(patchBody(linked, { ...draftFrom(linked), branch: 'release' }), { branch: 'release' });
  assert.deepEqual(patchBody(linked, { ...draftFrom(linked), configPath: 'apps/api/sp00ky.yml' }), { config_path: 'apps/api/sp00ky.yml' });
  // A linked project keeps its own deploy-on-push setting.
  assert.equal('auto_deploy' in (patchBody(linked, { ...draftFrom(linked), branch: 'x', autoDeploy: false }) ?? {}), false);
});

test('another repository or account sends the whole link', () => {
  const d = { ...draftFrom(linked), repo: 'acme/api', branch: 'trunk' };
  assert.equal(moves(linked, d), true);
  assert.deepEqual(patchBody(linked, d), { repo_owner: 'acme', repo_name: 'api', branch: 'trunk', config_path: 'sp00ky.yml', installation_id: 7 });
  // Same repository, read through another account: still a move.
  assert.equal(moves(linked, { ...draftFrom(linked), installation: 8 }), true);
  assert.equal(patchBody(linked, { ...draftFrom(linked), installation: 8 })?.installation_id, 8);
});

test('connecting sends deploy on push too', () => {
  const d = { ...draftFrom({ status: 'not_linked' }, 7), repo: 'acme/web', branch: 'main', autoDeploy: false };
  assert.deepEqual(patchBody({ status: 'not_linked' }, d), {
    repo_owner: 'acme',
    repo_name: 'web',
    branch: 'main',
    config_path: 'sp00ky.yml',
    installation_id: 7,
    auto_deploy: false,
  });
});

test('problems come before the server sees them', () => {
  const none: Link = { status: 'not_linked' };
  assert.equal(problem(none, { ...draftFrom(none), installation: undefined }, undefined), 'Pick a GitHub account');
  assert.equal(problem(none, draftFrom(none, 7), []), 'Pick a repository');
  assert.equal(problem(none, { ...draftFrom(none, 7), repo: 'acme/secret', branch: 'main' }, ['acme/web']), 'The app cannot see acme/secret on that account');
  assert.equal(problem(none, { ...draftFrom(none, 7), repo: 'acme/web', branch: '' }, ['acme/web']), 'Name a branch');
  assert.equal(problem(none, { ...draftFrom(none, 7), repo: 'acme/web', branch: 'a b' }, ['acme/web']), '"a b" is not a branch name');
  assert.match(problem(none, { ...draftFrom(none, 7), repo: 'acme/web', branch: 'main', configPath: '../x.yml' }, ['acme/web']) ?? '', /inside the repository/);
  assert.equal(problem(none, { ...draftFrom(none, 7), repo: 'acme/web', branch: 'main' }, ['acme/web']), null);
  // Unknown while the account's repositories load: not judged.
  assert.equal(problem(none, { ...draftFrom(none, 7), repo: 'acme/web', branch: 'main' }, undefined), null);
  // A link made through someone else's account: its branch can still change.
  assert.equal(problem(linked, { ...draftFrom(linked), branch: 'release' }, undefined), null);
});

test('branch names follow git, as the control plane does', () => {
  for (const ok of ['main', 'feature/login', 'release-1.2', 'dependabot/npm_and_yarn/vite-5.4.1', 'ü']) assert.equal(validBranch(ok), true, ok);
  for (const bad of ['', ' main', 'my branch', '-x', 'a..b', 'a/', '/a', 'a//b', 'a.lock', 'a.', '.hidden', 'a/.b', 'a~1', 'a^', 'a:b', 'a?', 'a*', 'a[b', 'a\\b', '@', 'a@{1}', 'tab\tb']) {
    assert.equal(validBranch(bad), false, JSON.stringify(bad));
  }
  for (const ok of ['sp00ky.yml', 'apps/api/sp00ky.yml']) assert.equal(validConfigPath(ok), true, ok);
  for (const bad of ['/etc/passwd', '../sp00ky.yml', 'apps/../../x', 'a\\b', '']) assert.equal(validConfigPath(bad), false, bad);
});

test('the branch note says when a branch is not on GitHub', () => {
  const list = { default_branch: 'main', branches: ['main', 'dev'] };
  assert.deepEqual(branchNote('main', list), { tone: 'dim', text: 'The default branch' });
  assert.equal(branchNote('dev', list), null);
  assert.equal(branchNote('next', list)?.tone, 'warn');
  // A listing cut short cannot say a branch is missing.
  assert.equal(branchNote('next', { ...list, truncated: true }), null);
  assert.equal(branchNote('next', undefined), null);
});

test('only moving to another repository asks first', () => {
  assert.equal(saveConfirm(linked, { ...draftFrom(linked), branch: 'dev' }, 'shop'), null);
  assert.equal(saveConfirm({ status: 'not_linked' }, { ...draftFrom(undefined, 7), repo: 'acme/web', branch: 'main' }, 'shop'), null);
  const spec = saveConfirm(linked, { ...draftFrom(linked), repo: 'acme/api', branch: 'trunk' }, 'shop')!;
  assert.equal(spec.title, 'Deploy shop from acme/api');
  assert.deepEqual(spec.consequences.slice(0, 2), ['Pushes to acme/web (main) stop deploying shop.', 'Pushes to acme/api (trunk) deploy shop.']);
  const off = saveConfirm({ ...linked, auto_deploy: false }, { ...draftFrom(linked), repo: 'acme/api' }, 'shop')!;
  assert.match(off.consequences[1], /build nothing until deploy on push is turned on/);
  assert.match(unlinkConfirm(linked, 'shop').consequences.join(' '), /build history is deleted/);
});

test('the toast names what changed', () => {
  const d = { ...draftFrom(linked), branch: 'dev', configPath: 'apps/x/sp00ky.yml' };
  assert.equal(savedMessage(linked, d, patchBody(linked, d)!), 'Following dev, reading apps/x/sp00ky.yml');
  const moved = { ...draftFrom(linked), repo: 'acme/api' };
  assert.equal(savedMessage(linked, moved, patchBody(linked, moved)!), 'Now deploying from acme/api');
  const fresh = { ...draftFrom(undefined, 7), repo: 'acme/web', branch: 'main' };
  assert.equal(savedMessage({ status: 'not_linked' }, fresh, patchBody({ status: 'not_linked' }, fresh)!), 'Linked to acme/web');
});
