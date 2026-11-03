// Run with `pnpm test` (`node --test`; Node 22.18+ strips the types itself).
import { test } from 'node:test';
import assert from 'node:assert/strict';
import {
  canUpgrade,
  emptyNote,
  humanize,
  kindLabel,
  statusChip,
  upgradeConfirm,
  upgradeNote,
  upgrading,
  type HostStatus,
  type HostView,
} from '../src/console/hostPlan.ts';

const host = (over: Partial<HostView> = {}): HostView => ({
  kind: 'app',
  status: 'ready',
  server_type: 'cx23',
  location: 'fsn1',
  target_type: 'cx33',
  target_available: true,
  upgrade_pending: true,
  ...over,
});

const WINDOW = '03:30 to 05:30 UTC';

test('kinds are named for what they are', () => {
  assert.equal(kindLabel('app'), 'Project host');
  assert.equal(kindLabel('db'), 'Database VM');
  assert.equal(kindLabel('other'), 'other');
});

test('the status chip', () => {
  assert.deepEqual(statusChip(host()), { label: 'Ready', tone: 'live' });
  assert.deepEqual(statusChip(host({ status: 'failed' })), { label: 'Failed', tone: 'fail' });
  assert.deepEqual(statusChip(host({ status: 'provisioning' })), { label: 'Provisioning', tone: 'run' });
  assert.equal(humanize('resize_waiting'), 'Resize waiting');
  assert.equal(humanize(''), '');
});

test('only an admin can ask for an upgrade, and only for a project host with one waiting', () => {
  assert.equal(canUpgrade(host(), 'admin'), true);
  assert.equal(canUpgrade(host(), 'member'), false);
  assert.equal(canUpgrade(host({ upgrade_pending: false }), 'admin'), false);
  assert.equal(canUpgrade(host({ kind: 'db' }), 'admin'), false);
});

test('an upgrade already asked for takes the button away', () => {
  const asked = host({ upgrade_state: 'requested' });
  assert.equal(upgrading(asked), true);
  assert.equal(canUpgrade(asked, 'admin'), false);
  assert.equal(upgrading(host()), false);
});

test('the note under a machine', () => {
  assert.equal(
    upgradeNote(host(), WINDOW, 'admin'),
    'It upgrades to cx33 by itself in the quiet window, 03:30 to 05:30 UTC.',
  );
  assert.equal(
    upgradeNote(host(), WINDOW, 'member'),
    'It upgrades to cx33 by itself in the quiet window, 03:30 to 05:30 UTC. Only a project admin can upgrade it sooner.',
  );
  assert.equal(
    upgradeNote(host({ target_available: false }), WINDOW, 'admin'),
    'It upgrades to cx33 by itself in the quiet window, 03:30 to 05:30 UTC. cx33 is not available in fsn1 right now.',
  );
  assert.equal(upgradeNote(host({ upgrade_state: 'requested' }), WINDOW, 'admin'), 'Upgrading to cx33: requested.');
  assert.equal(upgradeNote(host({ upgrade_pending: false }), WINDOW, 'admin'), undefined);
});

test('the confirm states the downtime and that the database is left alone', () => {
  const spec = upgradeConfirm(host());
  assert.equal(spec.verb, 'Upgrade');
  assert.match(spec.title, /cx33/);
  assert.ok(spec.consequences.some((c) => c.includes('about two minutes')));
  assert.ok(spec.consequences.some((c) => c.includes('database VM is not touched')));
  assert.ok(spec.consequences.some((c) => c.includes('from cx23 to cx33')));
});

test('a project with no machines says why', () => {
  const none = (placement: string): HostStatus => ({ placement, hosts: [], quiet_window: WINDOW, note: '' });
  assert.match(emptyNote(none('')), /no VMs of its own/);
  assert.match(emptyNote(none('preparing')), /being prepared/);
});
