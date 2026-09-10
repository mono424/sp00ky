import { describe, expect, it } from 'vitest';
import { RecordId } from 'surrealdb';
import {
  decideMembershipOutcome,
  dedupeRecordVersions,
  isResolvedBefore,
  metaFromRow,
  parseViewRow,
  snapshotFromSingle,
  snapshotsFromBatch,
  suspectHashes,
} from './membership';

describe('durable view row', () => {
  it('parses and gates resolved-before', () => {
    expect(parseViewRow(null)).toBeNull();
    expect(parseViewRow({ ids: 'nope' })).toBeNull();
    expect(parseViewRow({ ids: [['t:1', 1]], confirmed: true })).toEqual({ ids: [['t:1', 1]], confirmed: true });
    expect(parseViewRow({ ids: [] })).toEqual({ ids: [], confirmed: false });
    expect(isResolvedBefore(null)).toBe(false);
    expect(isResolvedBefore({ ids: [], confirmed: false })).toBe(false);
    expect(isResolvedBefore({ ids: [], confirmed: true })).toBe(true);
    expect(isResolvedBefore({ ids: [['t:1', 1]], confirmed: false })).toBe(true);
  });
});

describe('metaFromRow / dedupe', () => {
  it('folds the row-count select', () => {
    expect(metaFromRow(null)).toEqual({ present: false, rowCount: null, state: null });
    expect(metaFromRow({ rowCount: 3, state: 'ready' })).toEqual({ present: true, rowCount: 3, state: 'ready' });
    expect(metaFromRow({ rowCount: null, state: 'weird' })).toEqual({ present: true, rowCount: null, state: null });
    expect(metaFromRow({ state: 'materializing' })).toEqual({ present: true, rowCount: null, state: 'materializing' });
  });
  it('keeps the highest version per id', () => {
    const single: [string, number][] = [['t:1', 1]];
    expect(dedupeRecordVersions(single)).toBe(single);
    const clean: [string, number][] = [['t:1', 1], ['t:2', 1]];
    expect(dedupeRecordVersions(clean)).toBe(clean);
    expect(dedupeRecordVersions([['t:1', 1], ['t:1', 3], ['t:1', 2]])).toEqual([['t:1', 3]]);
  });
});

describe('decideMembershipOutcome', () => {
  const ready0 = { present: true, rowCount: 0, state: 'ready' as const };
  it('non-empty or verified removal is always applied', () => {
    expect(decideMembershipOutcome({ phase: 'cold', held: 0, remoteArray: [['t:1', 1]] })).toBe('applied');
    expect(decideMembershipOutcome({ phase: 'live', held: 2, remoteArray: [], verifiedRemoval: true })).toBe('applied');
  });
  it('empty with no readable row: view-lost when holding, ignored when cold and empty', () => {
    expect(decideMembershipOutcome({ phase: 'cold', held: 0, remoteArray: [] })).toBe('ignored');
    expect(decideMembershipOutcome({ phase: 'cold', held: 0, remoteArray: [], meta: { present: false, rowCount: null, state: null } })).toBe('ignored');
    expect(decideMembershipOutcome({ phase: 'cold', held: 2, remoteArray: [] })).toBe('view-lost');
    expect(decideMembershipOutcome({ phase: 'cached', held: 0, remoteArray: [] })).toBe('view-lost');
    expect(decideMembershipOutcome({ phase: 'live', held: 1, remoteArray: [], meta: { present: false, rowCount: null, state: null } })).toBe('view-lost');
  });
  it('empty with a present row: applied only when ready and zero rows', () => {
    expect(decideMembershipOutcome({ phase: 'cold', held: 0, remoteArray: [], meta: ready0 })).toBe('applied');
    expect(decideMembershipOutcome({ phase: 'cold', held: 0, remoteArray: [], meta: { present: true, rowCount: 0, state: null } })).toBe('applied');
    expect(decideMembershipOutcome({ phase: 'cold', held: 0, remoteArray: [], meta: { present: true, rowCount: 0, state: 'materializing' } })).toBe('ignored');
    expect(decideMembershipOutcome({ phase: 'live', held: 3, remoteArray: [], meta: { present: true, rowCount: 3, state: 'ready' } })).toBe('ignored');
    expect(decideMembershipOutcome({ phase: 'live', held: 3, remoteArray: [], meta: { present: true, rowCount: null, state: 'ready' } })).toBe('ignored');
  });
});

describe('snapshots', () => {
  const q1 = new RecordId('_00_query', 'a');
  const q2 = new RecordId('_00_query', 'b');
  const out = (id: string) => new RecordId('thing', id);
  it('single: edges, meta, children; null when the primary select is not an array', () => {
    expect(snapshotFromSingle(null, null, null)).toBeNull();
    const snap = snapshotFromSingle(
      [{ out: out('1'), version: 1 }, { out: out('1'), version: 2 }],
      { rowCount: 1, state: 'ready' },
      [{ out: out('c'), version: 1 }]
    );
    expect(snap).toEqual({
      primary: [['thing:1', 2]],
      subquery: [['thing:c', 1]],
      meta: { present: true, rowCount: 1, state: 'ready' },
    });
    expect(snapshotFromSingle([], null, null)!.subquery).toEqual([]);
  });
  it('batch: splits by hash and parent, fills meta, marks missing rows absent', () => {
    const hashById = new Map([['_00_query:a', 'ha'], ['_00_query:b', 'hb']]);
    expect(snapshotsFromBatch(null, null, hashById).size).toBe(0);
    const snaps = snapshotsFromBatch(
      [
        { in: q1, out: out('1'), version: 1 },
        { in: q1, out: out('1'), version: 1 },
        { in: q1, out: out('c'), version: 1, parent: q1 },
        { in: new RecordId('_00_query', 'zz'), out: out('9'), version: 1 },
      ],
      [{ id: q1, rowCount: 1, state: 'ready' }, null, { rowCount: 5 }, { id: new RecordId('_00_query', 'zz') }],
      hashById
    );
    expect(snaps.get('ha')).toEqual({
      primary: [['thing:1', 1]],
      subquery: [['thing:c', 1]],
      meta: { present: true, rowCount: 1, state: 'ready' },
    });
    expect(snaps.get('hb')).toEqual({ primary: [], subquery: [], meta: { present: false, rowCount: null, state: null } });
    expect(snapshotsFromBatch([], null, hashById).get('ha')!.meta.present).toBe(false);
  });
  it('suspectHashes flags held queries with a missing row or a silently empty batch', () => {
    const snaps = new Map([
      ['missing', { primary: [], subquery: [], meta: { present: false, rowCount: null, state: null } }],
      ['empty', { primary: [], subquery: [], meta: { present: true, rowCount: 4, state: 'ready' as const } }],
      ['fine', { primary: [], subquery: [], meta: { present: true, rowCount: 0, state: 'ready' as const } }],
      ['unheld', { primary: [], subquery: [], meta: { present: false, rowCount: null, state: null } }],
    ]);
    const held = new Map([['missing', 2], ['empty', 4], ['fine', 1], ['unheld', 0]]);
    expect(suspectHashes(snaps, held, 0)).toEqual(['missing', 'empty']);
    expect(suspectHashes(snaps, held, 1)).toEqual(['missing']);
    const nullCount = new Map([['q', { primary: [], subquery: [], meta: { present: true, rowCount: null, state: 'ready' as const } }]]);
    expect(suspectHashes(nullCount, new Map([['q', 3]]), 0)).toEqual([]);
    expect(suspectHashes(nullCount, new Map(), 0)).toEqual([]);
  });
});
