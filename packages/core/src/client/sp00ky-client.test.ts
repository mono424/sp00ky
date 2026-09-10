import { describe, expect, it } from 'vitest';
import { RecordId } from 'surrealdb';
import { Sp00kyClient } from './sp00ky-client';
import { Runtime } from './runtime';
import { fakeAdapters } from '../testing/adapters';
import { fakeServiceBundle } from '../testing/fake-services';
import { defaultEnv } from '../query/env';
import type { StatementResult } from '../kernel/effects';

const schema = { tables: [{ name: 'thing', columns: { a: {} } }], backends: { api: { outboxTable: 'thing', routes: { go: { args: {} } } } } } as any;
const config = { database: { namespace: 'n', database: 'd' }, schema, schemaSurql: '', logLevel: 'silent' } as any;
const ok = (result: unknown): StatementResult => ({ status: 'OK', result });
const tick = () => new Promise((r) => setTimeout(r, 0));
const flush = async (n = 6) => {
  for (let i = 0; i < n; i++) await tick();
};

function makeClient(over: Parameters<typeof fakeAdapters>[0] = {}) {
  const services = fakeServiceBundle<any>();
  const a = fakeAdapters({
    services: {
      'hint.read': () => 'u1',
      'local.usesSurqlSchema': () => false,
      'auth.restoreSession': async () => null,
      'auth.sessionAuthId': () => null,
      'remote.connect': () => new Promise(() => {}),
      ...over.services,
    },
    ...over,
  });
  const runtime = new Runtime({ env: defaultEnv(schema, { materializeDebounceMs: 1 }), adapters: a.adapters, logger: services.logger, tabId: 'tab-test' });
  const client = new Sp00kyClient<any>(config, { services, runtime });
  return { client, a, services, runtime };
}

describe('Sp00kyClient facade', () => {
  it('init resolves from the local store while the remote connect never does', async () => {
    const { client, a } = makeClient();
    expect(client.isLocalReady()).toBe(false);
    await client.init();
    expect(client.isLocalReady()).toBe(true);
    await flush();
    expect(a.names()).toContain('service.remote.connect');
    expect(a.names().indexOf('service.local.connect')).toBeLessThan(a.names().indexOf('service.remote.connect'));
    expect(client.syncHealth.status).toBe('healthy');
    expect(client.storageHealth).toEqual({ status: 'unknown', fallback: false });
    expect(client.tabRole).toBeNull();
    expect(client.liveRetryCount).toBe(0);
  });

  it('queryRaw registers locally and returns the hash before any remote work; subscribers paint from the local store', async () => {
    const { client, a } = makeClient({ local: { query: (async (sql: string) => (sql.startsWith('SELECT * FROM thing') ? [[{ id: 'thing:1' }]] : [])) as any } });
    await client.init();
    const hash = await client.queryRaw('SELECT * FROM thing', {}, '10m');
    expect(client.state.queries.get(hash)!.lifecycle.phase).toBe('cold');
    const rows: unknown[] = [];
    const off = await client.subscribe(hash, (r) => rows.push(r), { immediate: true });
    a.timers.fire(`mat:${hash}`);
    await flush();
    expect(rows.at(-1)).toEqual([{ id: 'thing:1' }]);
    const statuses: string[] = [];
    client.subscribeQueryStatus(hash, (s) => statuses.push(s), { immediate: true });
    const auth: boolean[] = [];
    client.subscribeQueryAuthority(hash, (k) => auth.push(k), { immediate: true });
    expect(statuses[0]).toBeDefined();
    expect(auth).toEqual([false]);
    client.reportFrontendTiming(hash, 3);
    client.reportFrontendTiming(hash, Number.NaN);
    expect(client.state.queries.get(hash)!.telemetry.phaseLast.frontend).toBe(3);
    off();
    client.deregisterQuery(hash);
    await flush();
    expect(client.state.queries.has(hash)).toBe(false);
    client.deregisterQuery('missing');
  });

  it('create writes locally, mirrors the outbox and reports activity; the tray API reads state', async () => {
    const { client } = makeClient({
      local: {
        execute: (async () => ({ id: new RecordId('thing', '1'), a: 1 })) as any,
        // The drain reads the pending rows back; hand it delete rows so the (empty) push answer keeps them queued.
        query: (async (sql: string, vars: any) =>
          sql === 'SELECT * FROM $ids' && String(vars.ids[0]).startsWith('_00_pending_mutations')
            ? [vars.ids.map((id: unknown) => ({ id: String(id), mutationType: 'delete', recordId: 'thing:1' }))]
            : []) as any,
      },
    });
    await client.init();
    const counts: number[] = [];
    const off = client.subscribeToPendingMutations((n) => counts.push(n));
    const created = await client.create('thing:1', { a: 1 });
    expect(created).toEqual({ id: new RecordId('thing', '1'), a: 1 });
    expect(client.pendingMutationCount).toBe(1);
    expect(counts).toEqual([0, 1]);
    off();
    const updated = await client.update('thing', 'thing:1', { a: 2 });
    expect(updated).toEqual({ a: 2 });
    await client.delete('thing', 'thing:1');
    await client.run('api', 'go', {} as never);
    expect(client.state.outbox).toHaveLength(4);
    const fetching: number[] = [];
    client.subscribeToFetchActivity((n) => fetching.push(n));
    expect(fetching).toEqual([0]);
    const tray: number[] = [];
    client.subscribeToFailedMutations((n) => tray.push(n));
    expect(tray).toEqual([0]);
    expect(client.failedMutationCount).toBe(0);
    expect(await client.listFailedMutations()).toEqual([]);
    expect(await client.retryFailedMutation('nope')).toBe(false);
    expect(await client.discardFailedMutation('nope')).toBe(false);
    const health: unknown[] = [];
    client.subscribeToSyncHealth((h) => health.push(h.status));
    expect(health).toEqual([client.syncHealth.status]);
    let stored = 0;
    client.subscribeToStorageHealth(() => stored++);
    expect(stored).toBe(1);
  });

  it('preload: cold waits for membership + bodies; cached returns at once; abort rejects', async () => {
    const edges = [{ out: new RecordId('thing', '1'), version: 1 }];
    const { client, a } = makeClient({
      remote: {
        queryResponses: async (sql: string) => {
          if (sql.startsWith('fn::query::register')) return [ok(null), ok(edges), ok({ rowCount: 1, state: 'ready' }), ok([])];
          if (sql.startsWith('SELECT * FROM $ids')) return [ok([{ id: new RecordId('thing', '1'), a: 1 }])];
          return [ok(null)];
        },
      },
      local: { select: (async () => [{ id: 'thing:1' }]) as any },
    });
    await client.init();
    a.timers.fire('lifecycle');
    const q = { innerQuery: { tableName: 'thing', selectQuery: { query: 'SELECT * FROM thing', vars: {}, plan: { table: 'thing' } } } } as any;
    const done = client.preload(q).then(() => 'settled');
    await flush(10);
    for (const key of [...a.timers.pending.keys()]) if (key.startsWith('mat:')) a.timers.fire(key);
    await flush(10);
    expect(await done).toBe('settled');
    const hash = [...client.state.queries.keys()][0];
    expect(client.state.queries.get(hash)!.lifecycle.phase).toBe('live');
    await expect(client.preload(q)).resolves.toBeUndefined();
    const ctrl = new AbortController();
    const q2 = { innerQuery: { tableName: 'thing', selectQuery: { query: 'SELECT * FROM thing WHERE a = 1', vars: {} } } } as any;
    const aborted = client.preload(q2, { signal: ctrl.signal });
    await flush(4);
    ctrl.abort();
    await expect(aborted).rejects.toThrow('aborted');
  });

  it('adapter callbacks become runtime events; tabs relays reach the hub/forwarder; close tears down', async () => {
    const { client, services, runtime } = makeClient();
    await client.init();
    const seen: string[] = [];
    runtime.on('*', (e) => seen.push(e.type));
    services.receivers.forEach((r: any) => r.onStreamUpdate?.({ queryHash: 'zz', localArray: [] }));
    services.connectionListeners.forEach((cb) => cb('connected'));
    services.authListeners.forEach((cb) => cb(null));
    await flush();
    const sent: unknown[] = [];
    (client as any).hub = { relayIngest: (t: unknown) => sent.push(['relay', t]), broadcast: (m: unknown) => sent.push(['bcast', m]), sendTo: (id: string, m: unknown) => sent.push(['to', id, m]) };
    runtime.emit({ type: 'tabs:broadcast', message: { type: 'ingest', records: [] } });
    runtime.emit({ type: 'tabs:broadcast', message: { type: 'mutation-settled' } });
    runtime.emit({ type: 'tabs:sendTo', tabId: 'x', message: { type: 'mutation-rolled-back' } });
    (client as any).hub = null;
    (client as any).forwarder = { ingest: (t: unknown) => sent.push(['f-ingest', t]), mutationEnqueued: (id: string) => sent.push(['f-enq', id]) };
    runtime.emit({ type: 'tabs:broadcast', message: { type: 'ingest', records: [] } });
    runtime.emit({ type: 'tabs:broadcast', message: { type: 'outbox-changed', mutationId: 'm' } });
    runtime.emit({ type: 'tabs:broadcast', message: { type: 'membership-dirty', hashes: [] } });
    expect(sent).toEqual([['relay', []], ['bcast', { type: 'mutation-settled' }], ['to', 'x', { type: 'mutation-rolled-back' }], ['f-ingest', []], ['f-enq', 'm']]);
    runtime.emit({ type: 'query:status', hash: 'h', status: 'idle' });
    runtime.emit({ type: 'query:authority', hash: 'h', known: true });
    runtime.emit({ type: 'query:view-lost', hash: 'h' });
    runtime.emit({ type: 'mutation:event', event: { type: 'create', record_id: new RecordId('thing', '1'), data: {} } });
    runtime.emit({ type: 'devtools', name: 'X', data: 1 });
    expect(await client.authenticate('tok')).toBe('tok');
    await client.deauthenticate();
    expect(await client.useRemote(async () => 'r')).toBe('r');
    expect(await client.remoteQuery('RETURN 1')).toEqual(['remote']);
    expect(client.remoteClient).toBeDefined();
    expect(client.localClient).toBe('local-client');
    expect(client.getBlobCacheStats()).toEqual({ entries: 0 });
    expect(client.bucket('files')).toBeDefined();
    expect(await client.openCrdtField('t', 'r', 'f')).toBe('field');
    client.closeCrdtField('t', 'r', 'f');
    expect(client.getFeatureOverrides()).toEqual({});
    client.setFeatureOverride('k', 'v');
    client.clearFeatureOverrides();
    expect(client.feature('k')).toBeDefined();
    expect(client.appRelease('web')).toBeDefined();
    await client.close();
    expect(client.state.localReady).toBe(true);
  });
});

describe('Sp00kyClient facade (builder, devtools source, event fan-out)', () => {
  it('query() builds through the QueryBuilder and registers locally; dispatch feeds the engine; DevTools reads state', async () => {
    const { client, runtime } = makeClient();
    await client.init();
    const { hash } = await (client.query('thing', {} as any) as any).build().run();
    expect(client.state.queries.has(hash)).toBe(true);
    const source = (client as any).devTools.dataManager;
    expect(source.getActiveQueries().map((q: any) => q.config.surql)).toEqual(['SELECT * FROM thing;']);
    const entry = client.state.queries.get(hash)!;
    expect(source.getQueryById(entry.def.id)!.config.membershipKey).toBe(entry.def.viewKey);
    expect(source.getQueryById({ id: 'nope', table: '_00_query' } as any)).toBeUndefined();
    expect(source.phaseTimings(source.getActiveQueries()[0]).updateCount).toBe(0);
    expect(source.phaseTimings({ config: { id: { id: 'nope' } } } as any)).toEqual({});
    await client.dispatch({ type: 'PollTick' });
    await expect((client.query('nope' as any, {} as any) as any).build().run()).rejects.toThrow('Table nope not found');
    const tray: number[] = [];
    const fetching: number[] = [];
    const health: string[] = [];
    client.subscribeToFailedMutations((n) => tray.push(n));
    client.subscribeToFetchActivity((n) => fetching.push(n));
    client.subscribeToSyncHealth((h) => health.push(h.status));
    runtime.emit({ type: 'tray:changed', count: 2 });
    runtime.emit({ type: 'activity:changed', fetching: 3, pending: 0 });
    runtime.emit({ type: 'health:changed', health: { ...client.syncHealth, status: 'degraded' } });
    runtime.emit({ type: 'query:evicted', hash });
    expect(tray).toEqual([0, 2]);
    expect(fetching).toEqual([0, 3]);
    expect(health).toEqual(['healthy', 'degraded']);
    const pending: number[] = [];
    client.subscribeToPendingMutations((n) => pending.push(n));
    runtime.emit({ type: 'activity:changed', fetching: 0, pending: 4 });
    expect(pending).toEqual([0, 4]);
  });
  it('honours config knobs when building the saga env', () => {
    const services = fakeServiceBundle<any>();
    const a = fakeAdapters();
    const runtime = new Runtime({ env: defaultEnv(schema), adapters: a.adapters, logger: services.logger, tabId: 't' });
    const tuned = new Sp00kyClient<any>(
      { ...config, database: { ...config.database, queryTimeoutMs: 5 }, syncHealth: false, pushTimeoutMs: 7, refSyncIntervalMs: 9, streamDebounceTime: 11, enableAnonymousLiveQueries: true, sharedTabs: true } as any,
      { services, runtime, env: { defaultTtlMs: 1 } }
    );
    expect((tuned as any).env).toMatchObject({ remoteTimeoutMs: 5, degradeAfter: 0, pushTimeoutMs: 7, pollBaseMs: 9, materializeDebounceMs: 11, anonLive: true, defaultTtlMs: 1 });
    const degraded = new Sp00kyClient<any>({ ...config, syncHealth: { degradeAfterConsecutiveFailures: 2 } } as any, { services, runtime });
    expect((degraded as any).env.degradeAfter).toBe(2);
    expect(tuned.tabRole).toBeNull();
  });
});
