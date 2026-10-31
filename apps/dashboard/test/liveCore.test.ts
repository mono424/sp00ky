// Run with `pnpm test` (`node --test`; Node 22.18+ strips the types itself).
//
// The golden vectors are the Rust host's (`ssp_protocol::admin_live`): every
// patch it produces for them must, applied here, reproduce its next state.
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import {
  ApplyError,
  applyPatch,
  canonical,
  jsonEqual,
  mergeApply,
  topicKey,
  type Json,
  type Shape,
} from '../src/api/liveCore.ts';

interface Vector {
  name: string;
  shape: Shape;
  key?: string;
  prev: Json;
  next: Json;
  patch: Json | 'reset';
}

const vectors: Vector[] = JSON.parse(
  readFileSync(
    fileURLToPath(new URL('../../../packages/ssp-protocol/tests/admin_live_vectors.json', import.meta.url)),
    'utf8',
  ),
);

test('golden vectors: applying the host patch reproduces the next state', () => {
  assert.ok(vectors.length > 0);
  for (const v of vectors) {
    if (v.patch === 'reset' || v.patch === null) continue;
    const got = applyPatch(v.shape, v.key ?? 'id', canonical(v.prev), v.patch);
    assert.ok(jsonEqual(got, canonical(v.next)), `${v.name}: got ${JSON.stringify(got)}`);
  }
});

test('golden vectors: unchanged states really are equal', () => {
  for (const v of vectors) {
    if (v.patch !== null) continue;
    assert.ok(jsonEqual(canonical(v.prev), canonical(v.next)), v.name);
  }
});

test('merge patch deletes on null and replaces non-objects', () => {
  assert.deepEqual(mergeApply({ a: 1, b: { c: 1 } }, { b: { c: null, d: 2 } }), { a: 1, b: { d: 2 } });
  assert.deepEqual(mergeApply({ a: [1, 2] }, { a: [3] }), { a: [3] });
  assert.deepEqual(mergeApply(5, { a: 1 }), { a: 1 });
});

test('apply never mutates its input', () => {
  const state = { rows: [{ id: 'a', n: 1 }], meta: { total: 1 } };
  const before = JSON.stringify(state);
  applyPatch('window', 'id', state, { up: [{ id: 'a', n: 2 }], meta: { total: 2 } });
  assert.equal(JSON.stringify(state), before);
});

test('a backwards series append is refused', () => {
  const state = { items: [{ t: 5 }], keep: 10, meta: {} };
  assert.throws(() => applyPatch('series', 't', state, { append: [{ t: 4 }] }), ApplyError);
});

test('an order naming unknown rows is refused', () => {
  const state = { rows: [{ id: 'a' }], meta: {} };
  assert.throws(() => applyPatch('window', 'id', state, { order: ['a', 'b'] }), ApplyError);
});

test('topic keys match the Rust spelling', () => {
  assert.equal(topicKey('overview'), 'overview');
  assert.equal(topicKey('overview', {}), 'overview');
  assert.equal(topicKey('metrics', { name: 'sync' }), 'metrics?{"name":"sync"}');
  assert.equal(topicKey('x', { b: 1, a: [2, { d: 1, c: 2 }] }), 'x?{"a":[2,{"c":2,"d":1}],"b":1}');
});
