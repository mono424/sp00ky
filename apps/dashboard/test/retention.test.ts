// Run with `pnpm test` (`node --test`; Node 22.18+ strips the types itself).
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { EMPTY_POLICY, cadence, pastRuns, periodName, retain, type Policy } from '../src/lib/retention.ts';

const RECOMMENDED: Policy = { ...EMPTY_POLICY, daily: 3, weekly: 5, quarterly: 4 };

/** `n` nightly backups at 03:00 UTC, newest first, the newest on `last`. */
const nightly = (last: string, n: number) =>
  Array.from({ length: n }, (_, i) => Date.parse(`${last}T03:00:00Z`) - i * 86_400_000);
const days = (marks: { at: number }[]) => marks.map((m) => new Date(m.at).toISOString().slice(0, 10));

// The same vector as TestRecommendedPolicyOnNightlyBackups in spooky-cloud.
test('recommended policy on nightly backups matches the cloud', () => {
  const r = retain(RECOMMENDED, nightly('2026-11-14', 500));
  assert.deepEqual(days(r.kept), [
    '2026-11-14', '2026-11-13', '2026-11-12',
    '2026-11-08', '2026-11-01', '2026-10-25', '2026-10-18', '2026-10-11',
    '2026-09-30', '2026-06-30', '2026-03-31', '2025-12-31',
  ]);
});

test('newest stacks in front of the other tiers', () => {
  const r = retain({ ...RECOMMENDED, last: 3 }, nightly('2026-11-15', 500));
  assert.equal(r.kept.length, 15);
  assert.deepEqual(days(r.kept.filter((k) => k.tier === 'last')), ['2026-11-15', '2026-11-14', '2026-11-13']);
  assert.deepEqual(days(r.kept.filter((k) => k.tier === 'daily')), ['2026-11-12', '2026-11-11', '2026-11-10']);
  // The current ISO week (9 to 15 Nov) is already held, so the first weekly is 8 Nov.
  assert.equal(days(r.kept.filter((k) => k.tier === 'weekly'))[0], '2026-11-08');
});

test('a policy of all zeros prunes nothing', () => {
  const r = retain(EMPTY_POLICY, nightly('2026-11-14', 30));
  assert.equal(r.prunes, false);
  assert.equal(r.kept.length, 30);
});

test('cadence reads the shapes the panel can draw', () => {
  const now = Date.parse('2026-11-15T14:41:00Z');
  assert.deepEqual(cadence('0 3 * * *', now), { stepMs: 86_400_000, last: Date.parse('2026-11-15T03:00:00Z') });
  assert.deepEqual(cadence('30 * * * *', now), { stepMs: 3_600_000, last: Date.parse('2026-11-15T14:30:00Z') });
  assert.deepEqual(cadence('45 * * * *', now), { stepMs: 3_600_000, last: Date.parse('2026-11-15T13:45:00Z') });
  assert.deepEqual(cadence('0 */6 * * *', now), { stepMs: 6 * 3_600_000, last: Date.parse('2026-11-15T12:00:00Z') });
  // 15 Nov 2026 is a Sunday; the last Monday run was the 9th.
  assert.deepEqual(cadence('0 3 * * 1', now), { stepMs: 7 * 86_400_000, last: Date.parse('2026-11-09T03:00:00Z') });
  assert.equal(cadence('0 3 1 * *', now), null);
  assert.equal(cadence('nonsense', now), null);
});

test('past runs reach far enough for every tier', () => {
  const c = cadence('0 3 * * *', Date.parse('2026-11-15T14:41:00Z'))!;
  const runs = pastRuns(c, { ...EMPTY_POLICY, yearly: 3 });
  const r = retain({ ...EMPTY_POLICY, yearly: 3 }, runs);
  assert.deepEqual(days(r.kept), ['2026-11-15', '2025-12-31', '2024-12-31']);
});

test('periods are named in their own unit', () => {
  const now = Date.parse('2026-11-15T14:41:00Z');
  const t = (iso: string) => Date.parse(iso);
  assert.equal(periodName('hourly', t('2026-11-15T13:00:00Z'), now), '13:00');
  assert.equal(periodName('hourly', t('2026-11-14T22:00:00Z'), now), '14 Nov 22:00');
  assert.equal(periodName('daily', t('2026-11-12T03:00:00Z'), now), '12 Nov');
  assert.equal(periodName('monthly', t('2025-12-31T03:00:00Z'), now), 'Dec 2025');
  assert.equal(periodName('quarterly', t('2025-12-31T03:00:00Z'), now), 'Q4 2025');
  assert.equal(periodName('quarterly', t('2026-09-30T03:00:00Z'), now), 'Q3');
  assert.equal(periodName('yearly', t('2024-12-31T03:00:00Z'), now), '2024');
});
