import { describe, expect, it } from 'vitest';
import { RecordId } from 'surrealdb';
import { runPure } from '../testing/run-pure';
import { fakeServices } from '../testing/services';
import { buildEntry, buildOutboxItem, buildState } from '../testing/build';
import * as R from '../state/reducers';
import { defaultEnv } from '../query/env';
import { boot, migrateWindowToView, pageHide, primeCircuit, startRemote, versionsPrimed, warmBlobs } from './boot.saga';

const env = defaultEnv({ tables: [] } as any);

describe('boot', () => {
  it('solo: opens the hinted bucket, provisions, starts the SSP, restores the session, mirrors the outbox, flips localReady, hands off to the network', async () => {
    const svc = fakeServices({
      'hint.read': () => 'u1',
      'local.usesSurqlSchema': () => true,
      'auth.restoreSession': async () => 'user:u1',
      'auth.sessionAuthId': () => 'user:u1',
      'auth.access': () => 'account',
    });
    const out = await runPure(boot(env, { sharedTabs: false }), {
      handlers: { service: svc.handler, 'local.query': (e: any) => (e.sql.startsWith('SELECT count()') ? [[{ count: 1 }]] : []) },
    });
    expect(svc.names()).toEqual([
      'hint.read',
      'local.connect',
      'local.usesSurqlSchema',
      'migrator.provision',
      'ssp.init',
      'ssp.setPermissions',
      'auth.restoreSession',
      'auth.sessionAuthId',
      'crdt.setSessionId',
      'auth.access',
      'ssp.setSessionAuth',
      'window.attach',
      'features.init',
      'releases.init',
    ]);
    expect(svc.calls[1]).toEqual(['local.connect', ['u1']]);
    expect(out.state).toMatchObject({ bucketId: 'u1', userId: 'user:u1', saltUserId: 'user:u1', sessionId: 'salt-1', localReady: true });
    expect(out.dispatched.map((d) => d.type)).toEqual(['WarmBlobs', 'PrimeCircuit', 'LifecycleTick', 'GcTick', 'StartRemote']);
    expect(svc.names()).not.toContain('remote.connect');
  });
  it('shared tabs: takes the role from the coordinator, or falls back to solo; schemaless engines skip provisioning; anonymous skips session auth', async () => {
    const leader = fakeServices({ 'hint.read': () => null, 'tabs.start': async () => 'leader', 'local.usesSurqlSchema': () => false, 'auth.restoreSession': async () => null, 'auth.sessionAuthId': () => null });
    const out = await runPure(boot(env, { sharedTabs: true }), { handlers: { service: leader.handler, 'local.query': () => [] } });
    expect(out.state.tabRole).toBe('leader');
    expect(out.state.bucketId).toBe('anon');
    expect(leader.names()).not.toContain('local.connect');
    expect(leader.names()).not.toContain('migrator.provision');
    expect(leader.names()).not.toContain('ssp.setSessionAuth');
    const fallback = fakeServices({
      'hint.read': () => 'x',
      'tabs.start': async () => {
        throw new Error('no broker');
      },
      'local.usesSurqlSchema': () => false,
      'auth.restoreSession': async () => null,
      'auth.sessionAuthId': () => null,
    });
    const fb = await runPure(boot(env, { sharedTabs: true }), { handlers: { service: fallback.handler, 'local.query': () => [] } });
    expect(fallback.calls).toContainEqual(['local.connect', ['x']]);
    expect(fb.emitted).toContainEqual(expect.objectContaining({ level: 'warn' }));
  });
});

describe('migrateWindowToView', () => {
  it('copies legacy rows once, skips when _00_view has rows, tolerates errors and junk', async () => {
    const upserts: any[] = [];
    await runPure(migrateWindowToView(), {
      handlers: {
        'local.query': (e: any) =>
          e.sql.startsWith('SELECT count()')
            ? [[]]
            : [[{ id: '_00_window:k1', ids: [['t:1', 1]], confirmed: true, updatedAt: 5 }, { id: new RecordId('_00_window', 'k2'), ids: [] }, { id: 'x', ids: 'bad' }, { ids: [] }]],
        'local.upsert': (e: any) => void upserts.push(e),
      },
    });
    expect(upserts).toEqual([
      { kind: 'local.upsert', table: '_00_view', id: new RecordId('_00_view', 'k1'), data: { ids: [['t:1', 1]], confirmed: true, updatedAt: 5 }, mode: 'replace' },
      { kind: 'local.upsert', table: '_00_view', id: new RecordId('_00_view', 'k2'), data: { ids: [], confirmed: false, updatedAt: 0 }, mode: 'replace' },
    ]);
    const skipped = await runPure(migrateWindowToView(), { handlers: { 'local.query': () => [[{ count: 3 }]] } });
    expect(skipped.log.filter((e) => e.kind === 'local.query')).toHaveLength(1);
    const failing = await runPure(migrateWindowToView(), {
      handlers: {
        'local.query': () => {
          throw new Error('no table');
        },
      },
    });
    expect(failing.emitted).toEqual([expect.objectContaining({ level: 'debug' })]);
  });
});

describe('startRemote / primeCircuit / versionsPrimed / pageHide / warmBlobs', () => {
  it('startRemote tolerates connect and auth failures and starts the loops', async () => {
    const svc = fakeServices({
      'remote.connect': async () => {
        throw new Error('offline');
      },
      'auth.init': async () => {
        throw new Error('401');
      },
    });
    const out = await runPure(startRemote(), { handlers: { service: svc.handler } });
    expect(svc.names()).toEqual(['remote.connect', 'supervisor.start', 'auth.init']);
    expect(out.emitted.filter((e) => e.type === 'log')).toHaveLength(2);
    expect(out.dispatched.map((d) => d.type)).toEqual(['EnsureRegistered', 'LiveStart', 'PollTick', 'Drain']);
    const ok = await runPure(startRemote(), { handlers: { service: fakeServices().handler } });
    expect(ok.emitted).toEqual([]);
  });
  it('primeCircuit passes outbox ids and always flips primed', async () => {
    const svc = fakeServices({ 'ssp.prime': async () => undefined });
    const s = buildState([], R.outboxReplace([buildOutboxItem({ recordId: 'thing:1' })]));
    const out = await runPure(primeCircuit(), { state: s, handlers: { service: svc.handler } });
    expect(svc.calls).toEqual([['ssp.prime', [['thing:1']]]]);
    expect(out.state.primed).toBe(true);
    const failing = fakeServices({
      'ssp.prime': async () => {
        throw new Error('snapshot');
      },
    });
    const f = await runPure(primeCircuit(), { state: buildState(), handlers: { service: failing.handler } });
    expect(f.state.primed).toBe(true);
    expect(f.emitted).toEqual([expect.objectContaining({ level: 'warn' })]);
  });
  it('versionsPrimed records versions; pageHide releases registered views; warmBlobs is best-effort', async () => {
    const v = await runPure(versionsPrimed([['thing:1', 3]]), { state: buildState() });
    expect(v.state.versions.get('thing:1')).toBe(3);
    const svc = fakeServices();
    const s = buildState([buildEntry({ def: { hash: 'a' }, lifecycle: { remote: 'registered' } }), buildEntry({ def: { hash: 'b' } })]);
    await runPure(pageHide(), { state: s, handlers: { service: svc.handler } });
    expect(svc.calls).toEqual([['remote.releaseViews', [[new RecordId('_00_query', 'a')]]]]);
    const none = fakeServices();
    await runPure(pageHide(), { state: buildState(), handlers: { service: none.handler } });
    expect(none.calls).toEqual([]);
    const blobs = fakeServices({
      'blobs.start': async () => {
        throw new Error('opfs');
      },
    });
    const w = await runPure(warmBlobs(), { state: { ...buildState(), bucketId: 'u9' }, handlers: { service: blobs.handler } });
    expect(blobs.calls).toEqual([['blobs.start', ['u9']]]);
    expect(w.emitted).toEqual([expect.objectContaining({ level: 'warn' })]);
    const anon = fakeServices({ 'blobs.start': async () => undefined });
    await runPure(warmBlobs(), { state: buildState(), handlers: { service: anon.handler } });
    expect(anon.calls).toEqual([['blobs.start', ['anon']]]);
  });
});
