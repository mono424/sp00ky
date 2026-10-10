import { describe, expect, it } from 'vitest';
import { RecordId } from 'surrealdb';
import { runPure } from '../testing/run-pure';
import { fakeServices } from '../testing/services';
import { buildEntry, buildState } from '../testing/build';
import * as R from '../state/reducers';
import { defaultEnv } from '../query/env';
import { authFlip, persistVerifiedUser } from './auth-flip.saga';
import { queryHashInput } from '../query/hash';
import { sha256Hex } from '../utils/sha256';

const env = defaultEnv({ tables: [{ name: 'user', columns: { id: {}, email_verified: {} } }] } as any);

describe('authFlip', () => {
  it('sets identity and $auth, rotates the salt for a new principal before the bucket work, writes the hint, switches, persists the user row', async () => {
    const released: string[] = [];
    const svc = fakeServices({
      'auth.sessionAuthId': () => 'user:abc',
      'auth.access': () => 'account',
      'local.currentBucketId': () => 'anon',
      'local.beginSwitch': () => () => void released.push('gate'),
      'local.usesSurqlSchema': () => false,
      'auth.token': () => 'jwt',
      'auth.currentUser': () => ({ id: new RecordId('user', 'abc'), email_verified: true }),
    });
    const state = { ...buildState(), sessionId: 'old', saltUserId: null };
    const out = await runPure(authFlip(env, 'user:abc'), {
      state,
      handlers: { service: svc.handler, 'local.query': () => [], 'local.execute': () => undefined, 'ssp.ingest': () => undefined },
    });
    const names = svc.names();
    expect(names.slice(0, 6)).toEqual(['auth.sessionAuthId', 'auth.access', 'ssp.setSessionAuth', 'crdt.setSessionId', 'hint.write', 'local.currentBucketId']);
    expect(svc.calls[3]).toEqual(['crdt.setSessionId', ['salt-1']]);
    expect(svc.calls[4]).toEqual(['hint.write', ['abc']]);
    expect(names).toContain('local.switchStore');
    expect(released).toEqual(['gate']);
    expect(out.state).toMatchObject({ userId: 'user:abc', saltUserId: 'user:abc', sessionId: 'salt-1', bucketId: 'abc' });
    expect(out.state.versions.get('user:abc')).toBe(1);
  });
  it("re-keys active queries under the new principal's salt, so no remote id names the previous principal's view", async () => {
    const svc = fakeServices({
      'auth.sessionAuthId': () => 'user:target',
      'auth.access': () => 'account',
      'local.currentBucketId': () => 'admin',
      'local.beginSwitch': () => () => undefined,
      'local.usesSurqlSchema': () => false,
      'auth.token': () => 'jwt',
      'auth.currentUser': () => null,
    });
    const def = { surql: 'SELECT * FROM notification', params: {} };
    const before = await sha256Hex(queryHashInput(def, 'admin-salt'));
    const state = R.putQuery(
      buildEntry({ def: { hash: 'q', ...def, id: new RecordId('_00_query', before) }, lifecycle: { phase: 'live', remote: 'registered' } })
    )({ ...buildState(), sessionId: 'admin-salt', saltUserId: 'user:admin', bucketId: 'admin' });
    const out = await runPure(authFlip(env, 'user:target'), {
      state,
      handlers: {
        service: svc.handler,
        'local.getById': () => null,
        'ssp.register': () => ({ localArray: [], timings: {} }),
        'local.query': () => [],
      },
    });
    const id = String(out.state.queries.get('q')!.def.id.id);
    expect(out.state.sessionId).toBe('salt-1');
    expect(id).toBe(await sha256Hex(queryHashInput(def, 'salt-1')));
    expect(id).not.toBe(before);
  });
  it('a new principal on the same bucket still re-keys its queries', async () => {
    const svc = fakeServices({
      'auth.sessionAuthId': () => 'user:b',
      'auth.access': () => 'account',
      'local.currentBucketId': () => 'anon',
      'auth.currentUser': () => null,
    });
    const def = { surql: 'SELECT * FROM thing', params: {} };
    const state = R.putQuery(buildEntry({ def: { hash: 'q', ...def }, lifecycle: { phase: 'live', remote: 'registered' } }))({
      ...buildState(),
      sessionId: 'old',
      saltUserId: 'user:a',
      bucketId: 'anon',
    });
    const out = await runPure(authFlip(env, null), {
      state,
      handlers: { service: svc.handler, 'local.getById': () => null, 'ssp.register': () => ({ localArray: [], timings: {} }) },
    });
    expect(svc.names()).not.toContain('local.switchStore');
    expect(String(out.state.queries.get('q')!.def.id.id)).toBe(await sha256Hex(queryHashInput(def, 'salt-1')));
    expect(out.dispatched.map((d) => d.type)).toContain('EnsureRegistered');
  });
  it('same principal on the same bucket: no switch, no salt rotation', async () => {
    const svc = fakeServices({
      'auth.sessionAuthId': () => 'user:abc',
      'auth.access': () => 'account',
      'local.currentBucketId': () => 'abc',
      'auth.currentUser': () => null,
    });
    const state = { ...buildState(), sessionId: 'keep', saltUserId: 'user:abc', bucketId: 'abc' };
    const out = await runPure(authFlip(env, 'user:abc'), { state, handlers: { service: svc.handler } });
    expect(svc.names()).not.toContain('local.switchStore');
    expect(svc.names()).not.toContain('local.beginSwitch');
    expect(out.state.sessionId).toBe('keep');
  });
});

describe('persistVerifiedUser', () => {
  it('skips when there is no verified row, an unknown table, or a bare id; writes otherwise and logs failures', async () => {
    for (const row of [null, { id: 'user:x', a: 1 }, { id: new RecordId('user', 'x') }, { id: new RecordId('nope', 'x'), a: 1 }]) {
      const svc = fakeServices({ 'auth.currentUser': () => row as any });
      const out = await runPure(persistVerifiedUser(env), { state: buildState(), handlers: { service: svc.handler } });
      expect(out.log.filter((e) => e.kind === 'local.execute')).toHaveLength(0);
    }
    const svc = fakeServices({ 'auth.currentUser': () => ({ id: new RecordId('user', 'x'), email_verified: true, junk: 1 }) });
    const s = buildState([buildEntry({ def: { hash: 'q', tableName: 'user' } })], R.setVersions([['user:x', 4]]));
    const out = await runPure(persistVerifiedUser(env), {
      state: s,
      handlers: {
        service: svc.handler,
        'local.execute': (e: any) => expect(e.vars.content0).toEqual({ email_verified: true, _00_rv: 4 }),
        'ssp.ingest': (e: any) => expect(e.records[0].record.junk).toBeUndefined(),
      },
    });
    expect(out.state.dirty.has('q')).toBe(true);
    const failing = await runPure(persistVerifiedUser(env), {
      state: s,
      handlers: {
        service: svc.handler,
        'local.execute': () => {
          throw new Error('locked');
        },
      },
    });
    expect(failing.emitted).toEqual([expect.objectContaining({ level: 'warn' })]);
  });
});
