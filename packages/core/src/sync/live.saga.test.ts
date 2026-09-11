import { describe, expect, it } from 'vitest';
import { RecordId } from 'surrealdb';
import { runPure } from '../testing/run-pure';
import { buildEntry, buildState } from '../testing/build';
import * as R from '../state/reducers';
import { defaultEnv } from '../query/env';
import { liveChange, liveInvalidate, liveStart } from './live.saga';
import { rowOfEdge } from '../client/services';

const env = defaultEnv({ tables: [] } as any);

describe('liveStart', () => {
  it('subscribes on the session table and records the uuid; no-op when already on it; followers never subscribe', async () => {
    const out = await runPure(liveStart(env), { state: { ...buildState(), userId: 'user:abc' }, handlers: { 'remote.live': (e: any) => `uuid-${e.table}` } });
    expect(out.state.sync).toMatchObject({ liveUuid: 'uuid-_00_list_ref_user_abc', liveTable: '_00_list_ref_user_abc' });
    const same = await runPure(liveStart(env), { state: out.state });
    expect(same.log.filter((e) => e.kind === 'remote.live')).toHaveLength(0);
    const follower = await runPure(liveStart(env), { state: { ...buildState(), tabRole: 'follower' } });
    expect(follower.log.filter((e) => e.kind === 'remote.live')).toHaveLength(0);
  });
  it('kills the previous subscription when connected (ignoring kill errors), tolerates a failing subscribe', async () => {
    const prev = R.compose(R.patchSync({ liveUuid: 'old', liveTable: '_00_list_ref' }), R.setConnection('connected'))({ ...buildState(), userId: 'user:x' });
    const killed: string[] = [];
    const out = await runPure(liveStart(env), {
      state: prev,
      handlers: {
        'remote.kill': (e: any) => {
          killed.push(e.uuid);
          throw new Error('gone');
        },
        'remote.live': () => 'new',
      },
    });
    expect(killed).toEqual(['old']);
    expect(out.state.sync.liveUuid).toBe('new');
    const clean = await runPure(liveStart(env), { state: prev, handlers: { 'remote.kill': () => undefined, 'remote.live': () => 'new2' } });
    expect(clean.state.sync.liveUuid).toBe('new2');
    expect(clean.emitted).toEqual([]);
    const offline = R.patchSync({ liveUuid: 'old', liveTable: '_00_list_ref' })({ ...buildState(), userId: 'user:x' });
    const noKill = await runPure(liveStart(env), {
      state: offline,
      handlers: {
        'remote.live': () => {
          throw new Error('not connected');
        },
      },
    });
    expect(noKill.log.filter((e) => e.kind === 'remote.kill')).toHaveLength(0);
    expect(noKill.state.sync.liveUuid).toBeNull();
    expect(noKill.emitted).toEqual([expect.objectContaining({ level: 'warn' })]);
  });
  it('liveInvalidate clears the bookkeeping', async () => {
    const out = await runPure(liveInvalidate(), { state: R.patchSync({ liveUuid: 'u', liveTable: 't' })(buildState()) });
    expect(out.state.sync).toMatchObject({ liveUuid: null, liveTable: null });
  });
});

describe('liveChange', () => {
  it('marks known hashes dirty, resets the poll streak, relays as leader; ignores unknown hashes', async () => {
    const s = R.patchSync({ pollIdleStreak: 3 })(buildState([buildEntry({ def: { hash: 'a' } })]));
    const out = await runPure(liveChange(env, ['a', 'zz']), { state: { ...s, tabRole: 'leader' } });
    expect([...out.state.membershipDirty]).toEqual(['a']);
    expect(out.state.sync.pollIdleStreak).toBe(0);
    expect(out.timers.get('membership')).toEqual({ ms: 50, event: { type: 'ReadDirtyMembership' } });
    expect(out.emitted).toEqual([{ type: 'tabs:broadcast', message: { type: 'membership-dirty', hashes: ['a'] } }]);
    const solo = await runPure(liveChange(env, ['a']), { state: s });
    expect(solo.emitted).toEqual([]);
    const none = await runPure(liveChange(env, ['zz']), { state: s });
    expect(none.timers.size).toBe(0);
  });
});

describe('rowOfEdge', () => {
  const rid = new RecordId('game', 'g1');

  it('reads the joined body off the notification', () => {
    expect(rowOfEdge({ in: new RecordId('_00_query', 'a'), out: { id: rid, result: '1-0' }, version: 7 })).toEqual({
      id: 'game:g1',
      version: 7,
      record: { id: rid, result: '1-0' },
    });
  });

  it('yields nothing when there is no body to read', () => {
    // No FETCH clause: `out` is still just the record id.
    expect(rowOfEdge({ out: rid, version: 7 })).toBeNull();
    // The session may not read the row, or its target is gone.
    expect(rowOfEdge({ out: null, version: 7 })).toBeNull();
    // A body with no version cannot be placed against what we already hold.
    expect(rowOfEdge({ out: { id: rid }, version: undefined })).toBeNull();
    // `landChunk` keys off a real RecordId; a string id would record the
    // version for a body it never wrote.
    expect(rowOfEdge({ out: { id: 'game:g1' }, version: 7 })).toBeNull();
    expect(rowOfEdge(null)).toBeNull();
  });
});

describe('liveChange with the body joined on', () => {
  const rid = new RecordId('game', 'g1');
  const row = (version: number) => ({ id: 'game:g1', version, record: { id: rid, result: '1-0' } });
  const handlers = { 'local.execute': () => undefined, 'ssp.ingest': () => undefined };

  it('lands the pushed body so the fetch plan has nothing left to pull', async () => {
    const s = buildState([buildEntry({ def: { hash: 'a' } })]);
    const out = await runPure(liveChange(env, ['a'], [row(7)]), { state: s, handlers });

    // Recorded at the edge's version: this is what makes `planFetch` skip it.
    expect(out.state.versions.get('game:g1')).toBe(7);
    // Written to the store and ingested into the circuit, exactly as a fetch would.
    expect(out.log.filter((e) => e.kind === 'local.execute')).toHaveLength(1);
    expect(out.log.filter((e) => e.kind === 'ssp.ingest')).toHaveLength(1);
    // Still a doorbell: membership is re-read regardless.
    expect([...out.state.membershipDirty]).toEqual(['a']);
  });

  it('ignores a body we already hold at that version or newer', async () => {
    const s = R.setVersions([['game:g1', 7]])(buildState([buildEntry({ def: { hash: 'a' } })]));
    const out = await runPure(liveChange(env, ['a'], [row(7)]), { state: s, handlers });
    expect(out.log.filter((e) => e.kind === 'local.execute')).toHaveLength(0);
    expect([...out.state.membershipDirty]).toEqual(['a']);
  });

  it('writes one copy when the same row arrives for several views', async () => {
    const s = buildState([buildEntry({ def: { hash: 'a' } })]);
    const out = await runPure(liveChange(env, ['a'], [row(7), row(7), row(8)]), { state: s, handlers });
    expect(out.log.filter((e) => e.kind === 'local.execute')).toHaveLength(1);
    expect(out.state.versions.get('game:g1')).toBe(8);
  });

  it('behaves exactly as before when no body rode along', async () => {
    const s = buildState([buildEntry({ def: { hash: 'a' } })]);
    const out = await runPure(liveChange(env, ['a']), { state: s, handlers });
    expect(out.log.filter((e) => e.kind === 'local.execute')).toHaveLength(0);
    expect(out.state.versions.size).toBe(0);
    expect([...out.state.membershipDirty]).toEqual(['a']);
  });
});
