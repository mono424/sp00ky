// Run with `pnpm test` (`node --test`; Node 22.18+ strips the types itself).
import { test } from 'node:test';
import assert from 'node:assert/strict';
import {
  STEP_MS,
  cellLabel,
  changePct,
  countTicks,
  dayTicks,
  formatCount,
  heatLevel,
  heatMax,
  nearestIndex,
  onlineNow,
  people,
  segments,
  trailingAverage,
  viewsPerPerson,
  windowCells,
  windowLabel,
  type ActivityPoint,
  type LivePresence,
} from '../src/console/audienceStats.ts';

const T0 = Date.UTC(2026, 9, 1);
const point = (minutes: number, over: Partial<ActivityPoint> = {}): ActivityPoint => ({
  at: new Date(T0 + minutes * 60_000).toISOString(),
  users_avg: 3,
  users_max: 5,
  anon_avg: 1,
  anon_max: 2,
  views_avg: 20,
  views_max: 30,
  ...over,
});

test('people are signed-in users and signed-out sessions together', () => {
  assert.equal(people(point(0)), 4);
});

const live = (over: Partial<LivePresence> = {}): LivePresence => ({
  totals: { users: 3, anon_sessions: 2 },
  taken_at_ms: T0,
  error: null,
  ready: true,
  ...over,
});

test('online now is the live count while it is current', () => {
  assert.deepEqual(onlineNow(live(), point(0), T0 + 15_000), { live: true, people: 5, signedIn: 3, signedOut: 2 });
});

test('nobody online live is a real zero, not a reason to fall back', () => {
  const now = onlineNow(live({ totals: { users: 0, anon_sessions: 0 } }), point(0), T0 + 15_000);
  assert.equal(now.live, true);
  assert.equal(now.people, 0);
});

test('without a usable live reading it falls back to the last bucket', () => {
  const bucket = point(0);
  const expected = { live: false, people: 7, signedIn: 5, signedOut: 2 };
  assert.deepEqual(onlineNow(undefined, bucket, T0), expected);
  assert.deepEqual(onlineNow(live({ ready: false }), bucket, T0), expected);
  assert.deepEqual(onlineNow(live({ error: 'db down' }), bucket, T0), expected);
  assert.deepEqual(onlineNow(live({ taken_at_ms: null }), bucket, T0), expected);
  assert.deepEqual(onlineNow(live(), bucket, T0 + 61_000), expected, 'a frozen feed is not live');
});

test('a short visit survives the fallback and its parts add up', () => {
  const visit = point(0, { users_avg: 0.37, users_max: 1, anon_avg: 0.38, anon_max: 1 });
  const now = onlineNow(undefined, visit, T0);
  assert.equal(now.people, 2);
  assert.equal(now.signedIn + now.signedOut, now.people);
});

test('views per person is unknown with nobody there, not zero', () => {
  assert.equal(viewsPerPerson(point(0)), 5);
  assert.equal(viewsPerPerson(point(0, { users_avg: 0, anon_avg: 0 })), null);
});

test('counts read naturally', () => {
  assert.equal(formatCount(0), '0');
  assert.equal(formatCount(0.42), '0.4');
  assert.equal(formatCount(12), '12');
  assert.equal(formatCount(123.4), '123');
  assert.equal(formatCount(3200), '3.2K');
  assert.equal(formatCount(45_000), '45K');
});

test('a change needs an earlier period to compare with', () => {
  assert.equal(changePct(15, 10, 96), 0.5);
  assert.equal(changePct(15, 0, 96), null);
  assert.equal(changePct(15, 10, 0), null);
});

test('a bucket nothing measured breaks the line', () => {
  const runs = segments([point(0), point(15), point(30), point(120), point(135)], STEP_MS['15m']);
  assert.deepEqual(runs.map((r) => r.length), [3, 2]);
  assert.deepEqual(segments([], STEP_MS['15m']), []);
});

test('one missed bucket does not break it', () => {
  assert.equal(segments([point(0), point(30)], STEP_MS['15m']).length, 1);
});

test('the trailing average forgets what left its window', () => {
  const points = [point(0, { views_avg: 10 }), point(15, { views_avg: 20 }), point(30, { views_avg: 60 })];
  assert.deepEqual(trailingAverage(points, (p) => p.views_avg, 15 * 60_000), [null, 20, 60]);
  assert.deepEqual(trailingAverage(points, (p) => p.views_avg, 20 * 60_000), [null, null, 40]);
});

test('an unknown value neither counts nor gets a trend', () => {
  const points = [point(0, { views_avg: 10 }), point(15, { views_avg: 99 }), point(30, { views_avg: 20 }), point(45, { views_avg: 30 })];
  const value = (p: ActivityPoint) => (p.views_avg === 99 ? null : p.views_avg);
  assert.deepEqual(trailingAverage(points, value, 45 * 60_000), [null, null, null, 25]);
});

test('a trend waits for a full window, so it cannot draw a fall that never happened', () => {
  const points = [point(0, { views_avg: 100 }), point(15, { views_avg: 1 }), point(30, { views_avg: 1 })];
  assert.deepEqual(trailingAverage(points, (p) => p.views_avg, 3_600_000), [null, null, null]);
});

test('the pointer finds the closest bucket', () => {
  const points = [point(0), point(15), point(30)];
  assert.equal(nearestIndex(points, T0 + 8 * 60_000), 1);
  assert.equal(nearestIndex(points, T0 + 6 * 60_000), 0);
  assert.equal(nearestIndex(points, T0 + 90 * 60_000), 2);
  assert.equal(nearestIndex([], T0), -1);
});

test('count ticks are whole numbers that cover the top', () => {
  assert.deepEqual(countTicks(11), [0, 5, 10, 15]);
  assert.deepEqual(countTicks(0), [0, 1]);
  assert.deepEqual(countTicks(2), [0, 1, 2]);
  for (const t of countTicks(0.4)) assert.ok(Number.isInteger(t));
});

test('day ticks are midnights and thin out', () => {
  const from = new Date(2026, 9, 1, 15).getTime();
  const to = new Date(2026, 9, 8, 15).getTime();
  const ticks = dayTicks(from, to, 10);
  assert.equal(ticks.length, 7);
  assert.equal(new Date(ticks[0]).getHours(), 0);
  assert.ok(dayTicks(from, new Date(2026, 11, 1).getTime(), 6).length <= 7);
});

test('quiet windows read as a stretch of the week', () => {
  assert.equal(cellLabel(2, 3), 'Tue 03:00');
  assert.equal(windowLabel({ day: 2, hour: 3, hours: 2, people: 0 }), 'Tue 03:00 to 05:00');
  assert.equal(windowLabel({ day: 2, hour: 23, hours: 2, people: 0 }), 'Tue 23:00 to Wed 01:00');
  assert.equal(windowLabel({ day: 7, hour: 23, hours: 2, people: 0 }), 'Sun 23:00 to Mon 01:00');
});

test('a quiet window covers its hours, across the end of the week', () => {
  assert.deepEqual(windowCells({ day: 7, hour: 23, hours: 2, people: 0 }), ['7-23', '1-0']);
});

test('one spike does not wash out the heatmap', () => {
  const cells = Array.from({ length: 100 }, (_, i) => ({ day: 1, hour: 0, people: i === 99 ? 1000 : 10, views: 0 }));
  assert.equal(heatMax(cells), 10);
  assert.equal(heatLevel(40, 10), 1);
  assert.equal(heatLevel(0, 10), 0);
  assert.ok(heatLevel(0.1, 10) > 0.05, 'a quiet hour reads apart from an empty one');
  assert.equal(heatMax([]), 1);
});
