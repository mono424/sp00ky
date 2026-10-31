// The TanStack DB adapter (`src/api/collections.ts`) driven by a fake socket:
// what a screen would see in a collection after snapshots, patches, gaps and
// trims. Built by `build-adapter-test.mjs`, run by `pnpm test`.
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { TopicCollections, TOPICS, type TopicSpec } from '../src/api/collections';
import type { LiveClient, TopicSink } from '../src/api/live';
import type { Json } from '../src/api/liveCore';

class FakeClient {
  sinks = new Map<string, TopicSink>();
  subscribe(topic: string, _args: Json | undefined, sink: TopicSink) {
    this.sinks.set(topic, sink);
    return () => this.sinks.delete(topic);
  }
  sink(topic: string): TopicSink {
    const s = this.sinks.get(topic);
    assert.ok(s, `no reader for ${topic}`);
    return s;
  }
}

async function setup(spec: TopicSpec) {
  const client = new FakeClient();
  const topics = new TopicCollections(client as unknown as LiveClient, 'test', null);
  const collection = topics.get(spec);
  const sub = collection.subscribeChanges(() => {}, { includeInitialState: true });
  await Promise.resolve();
  return { client, topics, collection, sub };
}

/** Rows without TanStack DB's virtual `$…` props. */
const rows = (c: { values(): Iterable<unknown> }) =>
  [...c.values()].map((r) =>
    Object.fromEntries(Object.entries(r as Record<string, unknown>).filter(([k]) => !k.startsWith('$'))),
  );

test('window: snapshot, patch, removal and meta', async () => {
  const { client, collection } = await setup(TOPICS.operations);
  const sink = client.sink('operations');
  sink.snap('b1', 10, { rows: [{ id: 'a', s: 'running' }, { id: 'b', s: 'running' }], meta: { total: 2 } });
  await collection.toArrayWhenReady();
  assert.equal(collection.size, 2);
  assert.equal(sink.since('b1'), 10);
  assert.equal(sink.since('other-boot'), undefined, 'a cursor from another host process is never offered');

  assert.equal(sink.diff('b1', 10, 11, { up: [{ id: 'a', s: 'done' }, { id: 'c', s: 'running' }], rm: ['b'], meta: { total: 3 } }), true);
  assert.deepEqual(
    rows(collection).sort((x, y) => String(x.id).localeCompare(String(y.id))),
    [{ id: 'a', s: 'done' }, { id: 'c', s: 'running' }],
  );
  assert.deepEqual(collection.utils.meta(), { total: 3 });
  assert.equal(sink.since('b1'), 11);
});

test('a patch that does not chain is refused and changes nothing', async () => {
  const { client, collection } = await setup(TOPICS.incidents);
  const sink = client.sink('incidents');
  sink.snap('b1', 5, { rows: [{ id: 'x', state: 'open' }], meta: {} });
  assert.equal(sink.diff('b1', 4, 6, { up: [{ id: 'x', state: 'recovered' }] }), false);
  assert.equal(sink.diff('b2', 5, 6, { up: [{ id: 'x', state: 'recovered' }] }), false, 'another boot is a gap');
  assert.deepEqual(rows(collection), [{ id: 'x', state: 'open' }]);
  assert.equal(sink.since('b1'), 5);
});

test('a snapshot replaces every row', async () => {
  const { client, collection } = await setup(TOPICS.runs);
  const sink = client.sink('runs');
  sink.snap('b1', 1, { rows: [{ id: 'old' }], meta: {} });
  sink.snap('b2', 900, { rows: [{ id: 'new' }], meta: {} });
  assert.deepEqual(rows(collection), [{ id: 'new' }]);
  assert.equal(sink.since('b2'), 900);
});

test('doc: one row, merge-patched', async () => {
  const { client, collection } = await setup(TOPICS.overview);
  const sink = client.sink('overview');
  sink.snap('b1', 1, { totals: { ssps: 2 }, ssps: { 'ssp-0': { status: 'ready' }, 'ssp-1': { status: 'ready' } } });
  assert.equal(sink.diff('b1', 1, 2, { ssps: { 'ssp-0': { status: 'lagging' }, 'ssp-1': null } }), true);
  const doc = collection.get('doc') as unknown as { data: Record<string, unknown> };
  assert.deepEqual(doc.data, { totals: { ssps: 2 }, ssps: { 'ssp-0': { status: 'lagging' } } });
});

test('series: appends and trims to keep, oldest first', async () => {
  const { client, collection } = await setup(TOPICS.jobMetrics);
  const sink = client.sink('metrics');
  sink.snap('b1', 1, { items: [{ t: 1 }, { t: 2 }, { t: 3 }], keep: 3, meta: {} });
  assert.equal(sink.diff('b1', 1, 2, { append: [{ t: 4 }, { t: 5 }] }), true);
  assert.deepEqual(rows(collection).map((r) => r.t).sort(), [3, 4, 5]);
  // More than `keep` in one patch: only the newest land.
  assert.equal(sink.diff('b1', 2, 3, { append: [{ t: 6 }, { t: 7 }, { t: 8 }, { t: 9 }] }), true);
  assert.deepEqual(rows(collection).map((r) => r.t).sort(), [7, 8, 9]);
});

test('the same topic is one collection per host', async () => {
  const client = new FakeClient();
  const topics = new TopicCollections(client as unknown as LiveClient, 'test', null);
  assert.equal(topics.get(TOPICS.jobs), topics.get(TOPICS.jobs));
  await topics.dispose();
});

test('page views: jobs, runs and incidents read like the REST payloads they replace', async () => {
  const { createRoot } = await import('solid-js');
  const { liveIncidentsView, liveJobsView, liveRunsView } = await import('../src/api/liveStack');
  const client = new FakeClient();
  const topics = new TopicCollections(client as unknown as LiveClient, 'test', null);
  await createRoot(async (dispose) => {
    const jobs = liveJobsView(topics);
    const runs = liveRunsView(topics);
    const incidents = liveIncidentsView(topics);
    await Promise.resolve();
    assert.equal(jobs(), undefined, 'no snapshot yet: the page keeps its fallback data');

    client.sink('jobs').snap('b', 1, {
      rows: [{ id: 'job:a', created_at: '2026-10-31T10:00:00Z' }, { id: 'job:b', created_at: '2026-10-31T11:00:00Z' }],
      meta: { totals: { counts: { pending: 1 } }, limit: 50 },
    });
    client.sink('metrics').snap('b', 1, { items: [{ t: 2 }, { t: 1 }], keep: 120, meta: {} });
    const page = jobs()!;
    assert.deepEqual(page.jobs.map((j) => j.id), ['job:b', 'job:a'], 'newest first');
    assert.deepEqual((page.totals as unknown as { samples: { t: number }[] }).samples.map((s) => s.t), [1, 2]);

    client.sink('runs').snap('b', 1, { rows: [{ id: 'r1', created_at: '2026-10-30T00:00:00Z' }, { id: 'r2', created_at: '2026-10-31T00:00:00Z' }], meta: {} });
    assert.deepEqual(runs().map((r) => r.id), ['r2', 'r1']);

    client.sink('incidents').snap('b', 1, { rows: [{ id: 'i1', started_at: 1 }, { id: 'i2', started_at: 5 }], meta: { total: 9, open: 1 } });
    assert.equal(incidents().total, 9);
    assert.deepEqual(incidents().incidents.map((i) => (i as unknown as { id: string }).id), ['i2', 'i1']);
    dispose();
  });
});
