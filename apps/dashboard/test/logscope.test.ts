// Run with `pnpm test` (`node --test`; Node 22.18+ strips the types itself).
process.env.TZ = 'UTC';

import { test } from 'node:test';
import assert from 'node:assert/strict';
import {
  appendNew,
  count,
  frameEnd,
  frameFor,
  lasting,
  lowerBound,
  parseFields,
  prependOlder,
  rangeLabel,
  shape,
  span,
  type Line,
} from '../src/lib/logscope.ts';

const L = (ts: number, message = `m${ts}`, level = 'INFO'): Line => ({ ts, level, target: 't', message });

test('frameFor picks the finest round step that fits', () => {
  const f = frameFor({ t0: 0, t1: 20 * 60_000 }, 100, null);
  assert.equal(f.step, 15_000);
  assert.equal(f.cols, 80);
  assert.equal(f.t0, 0);
});

test('an aligned frame keeps its columns while the tail grows', () => {
  const a = frameFor({ t0: 7_300, t1: 61_000 }, 60, 0);
  const b = frameFor({ t0: 7_300, t1: 61_900 }, 60, 0);
  assert.equal(a.step, 1_000);
  assert.equal(a.t0, 7_000);
  assert.deepEqual([b.t0, b.step], [a.t0, a.step]);
  assert.ok(frameEnd(b) > 61_900);
});

test('an aligned frame never exceeds its column budget', () => {
  for (let t1 = 1_000; t1 < 5_000_000; t1 += 77_777) {
    const f = frameFor({ t0: 333, t1 }, 90, 3_600_000);
    assert.ok(f.cols <= 90, `cols ${f.cols} for t1 ${t1}`);
    assert.ok(f.t0 <= 333 && frameEnd(f) >= t1);
  }
});

test('lowerBound and span cut a window out of a sorted buffer', () => {
  const xs = [L(1), L(3), L(3), L(5), L(9)];
  assert.equal(lowerBound(xs, 0), 0);
  assert.equal(lowerBound(xs, 3), 1);
  assert.equal(lowerBound(xs, 4), 3);
  assert.equal(lowerBound(xs, 10), 5);
  assert.deepEqual(span(xs, { t0: 3, t1: 9 }), [1, 4]);
  assert.deepEqual(span(xs, null), [0, 5]);
});

test('count puts each line in its column, warnings and errors apart', () => {
  const xs = [L(0), L(5, 'a', 'WARN'), L(10, 'b', 'ERROR'), L(19), L(25)];
  const c = count(xs, 0, xs.length, { t0: 0, step: 10, cols: 3 });
  assert.deepEqual([...c.all], [2, 2, 1]);
  assert.deepEqual([...c.warn], [1, 0, 0]);
  assert.deepEqual([...c.error], [0, 1, 0]);
  assert.equal(c.hi, 2);
});

test('appendNew skips what the socket resends and keeps real same-instant twins', () => {
  const buf = [L(1), L(2, 'x'), L(2, 'y')];
  // the socket's whole tail again, plus a new line, plus a twin of x at 2
  const added = appendNew(buf, [L(1), L(2, 'x'), L(2, 'y'), L(2, 'x'), L(4)]);
  assert.equal(added, 2);
  assert.deepEqual(buf.map((l) => `${l.ts}${l.message}`), ['1m1', '2x', '2y', '2x', '4m4']);
  assert.equal(appendNew(buf, [L(2, 'x'), L(4)]), 0);
});

test('appendNew puts a line a millisecond out of order in its place', () => {
  const buf = [L(10)];
  appendNew(buf, [L(12), L(11)]);
  assert.deepEqual(buf.map((l) => l.ts), [10, 11, 12]);
});

test('prependOlder adds only the history before the buffer', () => {
  const buf = [L(5, 'a'), L(6)];
  const out = prependOlder(buf, [L(3), L(4), L(5, 'a'), L(5, 'b'), L(6)]);
  assert.deepEqual(out.map((l) => `${l.ts}${l.message}`), ['3m3', '4m4', '5b', '5a', '6m6']);
  assert.equal(prependOlder(buf, [L(6)]), buf);
});

test('parseFields splits key=value pairs whose values hold spaces', () => {
  assert.deepEqual(parseFields('backend=worker attempt=1'), [
    ['backend', 'worker'],
    ['attempt', '1'],
  ]);
  assert.deepEqual(parseFields('error=connection refused (os error 61) url=http://x/a=b'), [
    ['error', 'connection refused (os error 61)'],
    ['url', 'http://x/a=b'],
  ]);
  assert.deepEqual(parseFields('name="a b"'), [['name', 'a b']]);
  assert.deepEqual(parseFields(''), []);
});

test('shape reads two lines of one kind as the same', () => {
  assert.equal(shape('applied 96 events in 24ms (seq 884,608)'), shape('applied 142 events in 38ms (seq 884,512)'));
  assert.equal(shape('run-9f2c1a step reindex dispatched'), 'run-# step reindex dispatched');
});

test('labels read in the unit the window calls for', () => {
  const t0 = Date.UTC(2026, 10, 5, 9, 21, 40);
  assert.equal(rangeLabel({ t0, t1: t0 + 5 * 60_000 + 30_000 }), '09:21:40 – 09:27:09');
  assert.equal(rangeLabel({ t0, t1: t0 + 2_500 }), '09:21:40.000 – 09:21:42.499');
  assert.equal(rangeLabel({ t0, t1: t0 + 20 * 3_600_000 }), 'Nov 5 09:21 – Nov 6 05:21');
  assert.equal(lasting(850), '850ms');
  assert.equal(lasting(19 * 60_000 + 55_000), '19m 55s');
  assert.equal(lasting(2 * 3_600_000 + 5 * 60_000), '2h 05m');
});
