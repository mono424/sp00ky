// Run with `pnpm test` (`node --test`; Node 22.18+ strips the types itself).
import { test } from 'node:test';
import assert from 'node:assert/strict';
import {
  actionsFor,
  cancelConfirm,
  cutOverConfirm,
  doing,
  facts,
  finishConfirm,
  goBackConfirm,
  outcome,
  phases,
  rollbackConfirm,
  runChip,
  startConfirm,
  stateNote,
  stepWord,
  versionLine,
  type UpgradeRun,
  type UpgradeStatus,
} from '../src/console/dbUpgradePlan.ts';

const NOW = Date.parse('2026-11-02T12:00:00Z');
const LATER = '2026-11-05T12:00:00Z';
const EARLIER = '2026-10-30T12:00:00Z';

const run = (over: Partial<UpgradeRun> = {}): UpgradeRun => ({
  id: 'up',
  kind: 'replay',
  state: 'sync',
  active: true,
  from_version: 'v3.3.5',
  to_version: 'v4.0.0-beta.1',
  reverse: false,
  hold_at_ready: false,
  abort_requested: false,
  stats: {},
  created_at: EARLIER,
  ...over,
});
const status = (over: Partial<UpgradeStatus> = {}): UpgradeStatus => ({
  current: 'v3.3.5',
  target: 'v4.0.0-beta.1',
  kind: 'replay',
  beta: true,
  one_way: true,
  up_to_date: false,
  blockers: [],
  ...over,
});
const on = (r: UpgradeRun) => Object.entries(actionsFor(r, NOW)).filter(([, v]) => v).map(([k]) => k);

test('a phase is done, current or waiting, and the phases follow the kind', () => {
  const at = (r: UpgradeRun) => phases(r).map((p) => `${p.title}:${p.at}`);
  assert.deepEqual(at(run({ state: 'sync' })), ['Prepare:done', 'Catch up:sync', 'Cut over:waiting', 'Watch:waiting']);
  assert.deepEqual(at(run({ state: 'preflight' })), ['Prepare:preflight', 'Catch up:waiting', 'Cut over:waiting', 'Watch:waiting']);
  assert.deepEqual(at(run({ state: 'finalize' })), ['Prepare:done', 'Catch up:done', 'Cut over:done', 'Watch:finalize']);
  assert.deepEqual(at(run({ kind: 'swap', state: 'rehearse' })), ['Prepare:rehearse', 'Cut over:waiting']);
  assert.deepEqual(at(run({ kind: 'swap', state: 'smoke' })), ['Prepare:done', 'Cut over:smoke']);
});

test('an abort from the watch window runs its own, shorter track', () => {
  const r = (state: string) => phases(run({ state, reverse: true })).map((p) => p.at);
  assert.deepEqual(r('convert'), ['convert', 'waiting', 'waiting']);
  assert.deepEqual(r('fence'), ['done', 'fence', 'waiting']);
  assert.deepEqual(r('finalize'), ['done', 'done', 'finalize']);
  assert.ok(!phases(run({ reverse: true })).flatMap((p) => p.steps).includes('capture'));
});

test('undoing and states this console does not know still show', () => {
  assert.deepEqual(phases(run({ state: 'abort' })), [{ title: 'Undoing', steps: ['abort'], at: 'abort' }]);
  assert.deepEqual(phases(run({ state: 'revert', flip_at: EARLIER })), [{ title: 'Undoing', steps: ['revert', 'abort'], at: 'revert' }]);
  assert.deepEqual(phases(run({ state: 'rollback' })), [{ title: 'Rolling back', steps: ['rollback'], at: 'rollback' }]);
  assert.deepEqual(phases(run({ state: 'new_thing' })), [{ title: 'Upgrade', steps: ['new_thing'], at: 'new_thing' }]);
  assert.equal(stepWord('new_thing'), 'new thing');
});

test('Cancel is offered until the fence, and only there', () => {
  for (const state of ['preflight', 'backup', 'capture', 'clone', 'convert', 'rebuild', 'sync', 'verify', 'ready']) {
    assert.deepEqual(on(run({ state })), ['cancel'], state);
  }
  assert.deepEqual(on(run({ kind: 'swap', state: 'rehearse' })), ['cancel']);
  for (const state of ['fence', 'flip_stop', 'flip_start', 'reconnect', 'smoke', 'finalize', 'abort', 'revert', 'rollback', 'failed', 'aborted', 'rolled_back']) {
    assert.deepEqual(on(run({ state })), [], state);
  }
});

test('the go waits at ready, only while held', () => {
  assert.deepEqual(on(run({ state: 'ready', hold_at_ready: true })), ['cancel', 'cutOver']);
  assert.deepEqual(on(run({ kind: 'swap', state: 'ready', hold_at_ready: true })), ['cancel', 'cutOver']);
  assert.deepEqual(on(run({ state: 'ready' })), ['cancel']);
});

test('the watch window offers every way out; an abort under way takes them away', () => {
  assert.deepEqual(on(run({ state: 'soak' })), ['goBack', 'finish', 'rollBack']);
  assert.deepEqual(on(run({ state: 'soak', abort_requested: true })), []);
  assert.deepEqual(on(run({ state: 'sync', abort_requested: true })), []);
  for (const state of ['convert', 'sync', 'verify', 'ready', 'fence', 'flip_stop', 'smoke', 'finalize']) {
    assert.deepEqual(on(run({ state, reverse: true, hold_at_ready: true })), [], state);
  }
});

test('a done run can roll back only while rollback_until is ahead', () => {
  assert.deepEqual(on(run({ state: 'done', active: false, rollback_until: LATER })), ['rollBack']);
  assert.deepEqual(on(run({ state: 'done', active: false, rollback_until: EARLIER })), []);
  assert.deepEqual(on(run({ state: 'done', active: false })), []);
});

test('the version line follows the run while one is active', () => {
  assert.deepEqual(versionLine(status()), { from: 'v3.3.5', to: 'v4.0.0-beta.1', beta: true });
  assert.deepEqual(versionLine(status({ target: undefined, beta: false, up_to_date: true })), { from: 'v3.3.5', to: undefined, beta: false });
  const soak = status({ current: 'v4.0.0-beta.1', target: undefined, beta: false, up_to_date: true });
  assert.deepEqual(versionLine({ ...soak, upgrade: run({ state: 'soak' }) }), { from: 'v3.3.5', to: 'v4.0.0-beta.1', beta: true });
  assert.deepEqual(versionLine({ ...soak, upgrade: run({ state: 'sync', reverse: true }) }), { from: 'v4.0.0-beta.1', to: 'v3.3.5', beta: false });
  assert.deepEqual(versionLine({ ...status(), upgrade: run({ state: 'failed', active: false }) }), { from: 'v3.3.5', to: 'v4.0.0-beta.1', beta: true });
});

test('a failed run says the database is serving, unless the run says it is not', () => {
  const before = outcome(run({ state: 'failed', active: false, error: 'the two databases differ' }));
  assert.match(before.text, /unchanged and serving on v3\.3\.5/);
  assert.ok(!before.caution);
  // A flip that failed and brought the old one back left things as they were.
  const reverted = outcome(run({ state: 'failed', active: false, flip_at: EARLIER, error: 'The new version did not come up' }));
  assert.match(reverted.text, /unchanged and serving on v3\.3\.5/);
  const down = outcome(run({ state: 'failed', active: false, flip_at: EARLIER, error: 'The old database could not be started again', stats: { not_serving: true } }));
  assert.ok(down.caution);
  assert.match(down.text, /not serving/);
  assert.doesNotMatch(down.text, /unchanged|serving on/);
});

test('an aborted run says the database is serving, and whether it ever moved', () => {
  assert.match(outcome(run({ state: 'aborted', active: false })).text, /unchanged and serving/);
  assert.match(outcome(run({ state: 'aborted', active: false, reverse: true, flip_at: EARLIER })).text, /back on v3\.3\.5 and serving\. Nothing was lost/);
  assert.match(outcome(run({ state: 'rolled_back', active: false })).text, /dropped/);
});

test('the numbers appear once known, and an absent lag means none past the first catch-up', () => {
  assert.deepEqual(facts(run({ stats: null, state: 'clone' })), []);
  assert.deepEqual(facts(run({ kind: 'swap', state: 'ready', stats: {} })), []);
  const lag = (r: UpgradeRun) => facts(r).find((f) => f.label === 'Lag')?.value;
  assert.equal(lag(run({ state: 'sync', stats: { lag_ms: 2400 } })), '2.4 s');
  assert.equal(lag(run({ state: 'sync' })), 'Measuring');
  assert.equal(lag(run({ state: 'verify' })), 'None');
  assert.equal(lag(run({ state: 'soak' })), undefined);
  const all = facts(run({ state: 'soak', stats: { applied_rows: 1_284_120, verified_rows: 912_004, detail: 'comparing game_analysis', hold_ms: 1240, frozen_ms: 180, open_ms: 41_300 } }));
  assert.deepEqual(all, [
    { label: 'Rows applied', value: '1,284,120' },
    { label: 'Rows verified', value: '912,004', note: 'comparing game_analysis' },
    { label: 'Converted in', value: '41.3 s' },
    { label: 'Frozen for the copy', value: '180 ms' },
    { label: 'Cutover', value: 'Writes refused for 1.2 s' },
  ]);
  assert.equal(facts(run({ kind: 'swap', state: 'ready', stats: { open_ms: 2140 } }))[0].label, 'Rehearsed open');
});

test('the step to 4.x asks for the slug and says all of it plainly', () => {
  const spec = startConfirm(status(), false, 'whitepawn');
  assert.equal(spec.typeToConfirm, 'whitepawn');
  for (const part of [/beta release of SurrealDB/, /stays up while the new version catches up/, /few seconds of refused writes at the cutover/, /one hour after/, /without losing anything/, /kept for 7 days/, /drops newer writes/, /JS SDK 2\.1\.0/, /Dart canary\.363/]) {
    assert.match(spec.warning ?? '', part);
  }
  assert.match(startConfirm(status(), true, 'whitepawn').consequences.join(' '), /Stops before the cutover/);
});

test('a step inside 3.x confirms without typing, and warns of downtime', () => {
  const spec = startConfirm(status({ target: 'v3.3.5', kind: 'swap', beta: false, one_way: false, current: 'v3.1.5' }), false, 'whitepawn');
  assert.equal(spec.typeToConfirm, undefined);
  for (const part of [/fresh backup/, /rehearsal on a copy/, /few seconds of downtime/, /drops writes made since/]) assert.match(spec.warning ?? '', part);
});

test('rolling back asks for the slug and says how long ago the cutover was', () => {
  const spec = rollbackConfirm(run({ state: 'soak' }), 'whitepawn', '18.0m ago');
  assert.equal(spec.typeToConfirm, 'whitepawn');
  assert.match(spec.warning ?? '', /write since the cutover is dropped\. The cutover was 18\.0m ago\./);
  assert.doesNotMatch(rollbackConfirm(run({ state: 'done' }), 'whitepawn', undefined).warning ?? '', /was/);
  assert.match(goBackConfirm(run({ state: 'soak' })).consequences.join(' '), /Nothing is lost/);
});

test('no em dash in anything a person reads', () => {
  const states = ['preflight', 'backup', 'rehearse', 'capture', 'clone', 'convert', 'rebuild', 'sync', 'verify', 'ready', 'fence', 'flip_stop', 'flip_start', 'reconnect', 'smoke', 'soak', 'finalize', 'abort', 'revert', 'rollback', 'done', 'failed', 'aborted', 'rolled_back'];
  const copy: unknown[] = [startConfirm(status(), true, 's'), startConfirm(status({ one_way: false }), false, 's'), finishConfirm()];
  for (const state of states) {
    for (const r of [run({ state }), run({ state, reverse: true }), run({ state, kind: 'swap', hold_at_ready: true }), run({ state, abort_requested: true, flip_at: EARLIER })]) {
      copy.push(doing(r), stateNote(r), runChip(r), outcome(r), facts(r), phases(r).map((p) => [p.title, p.steps.map(stepWord)]), cancelConfirm(r), goBackConfirm(r), cutOverConfirm(r), rollbackConfirm(r, 's', 'just now'));
    }
  }
  assert.doesNotMatch(JSON.stringify(copy), /—/);
});
