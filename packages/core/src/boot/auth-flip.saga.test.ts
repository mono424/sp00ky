import { describe, expect, it } from 'vitest';
import { RecordId } from 'surrealdb';
import { runPure } from '../testing/run-pure';
import { fakeServices } from '../testing/services';
import { buildEntry, buildState } from '../testing/build';
import * as R from '../state/reducers';
import { defaultEnv } from '../query/env';
import { authFlip, persistVerifiedUser } from './auth-flip.saga';

const env = defaultEnv({ tables: [{ name: 'user', columns: { id: {}, email_verified: {} } }] } as any);

describe('authFlip', () => {
  it('sets identity and $auth before the bucket work, writes the hint, switches, rotates the salt for a new principal, persists the user row', async () => {
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
    expect(names.slice(0, 5)).toEqual(['auth.sessionAuthId', 'auth.access', 'ssp.setSessionAuth', 'hint.write', 'local.currentBucketId']);
    expect(svc.calls[3]).toEqual(['hint.write', ['abc']]);
    expect(names).toContain('local.switchStore');
    expect(names).toContain('crdt.setSessionId');
    expect(released).toEqual(['gate']);
    expect(out.state).toMatchObject({ userId: 'user:abc', saltUserId: 'user:abc', sessionId: 'salt-1', bucketId: 'abc' });
    expect(out.state.versions.get('user:abc')).toBe(1);
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
