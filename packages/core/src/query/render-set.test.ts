import { describe, expect, it } from 'vitest';
import { buildRenderIds, resolveMembership } from './render-set';

const ov = (writes: string[] = [], deletes: string[] = []) => ({ writes: new Set(writes), deletes: new Set(deletes) });

describe('resolveMembership', () => {
  it('cold scans, cold window uses the local view, anything else uses membership', () => {
    const remoteArray: [string, number][] = [['t:1', 1]];
    const localArray: [string, number][] = [['t:2', 1]];
    expect(resolveMembership({ phase: 'cold', remoteArray, localArray, isWindow: false })).toBeNull();
    expect(resolveMembership({ phase: 'cold', remoteArray, localArray, isWindow: true })).toBe(localArray);
    expect(resolveMembership({ phase: 'cached', remoteArray, localArray, isWindow: false })).toBe(remoteArray);
    expect(resolveMembership({ phase: 'view-lost', remoteArray, localArray, isWindow: true })).toBe(remoteArray);
  });
});

describe('buildRenderIds', () => {
  const opts = { hasExplicitOrder: false, isWindow: false };
  it('membership minus deletes, sorted', () => {
    expect(buildRenderIds([['t:b', 1], ['t:a', 1], ['t:c', 1]], [], ov([], ['t:c']), opts)).toEqual(['t:a', 't:b']);
  });
  it('adds pending writes the local view admits, never twice, never deleted', () => {
    const ids = buildRenderIds(
      [['t:1', 1]],
      [['t:1', 1], ['t:2', 1], ['t:3', 1], ['t:4', 1]],
      ov(['t:1', 't:2', 't:3', 't:9'], ['t:3']),
      opts
    );
    expect(ids).toEqual(['t:1', 't:2']);
  });
  it('keeps server order for windows and explicit orderBy; dedupes membership', () => {
    const m: [string, number][] = [['t:b', 1], ['t:a', 1], ['t:b', 1]];
    expect(buildRenderIds(m, [], ov(), { hasExplicitOrder: true, isWindow: false })).toEqual(['t:b', 't:a']);
    expect(buildRenderIds(m, [], ov(), { hasExplicitOrder: false, isWindow: true })).toEqual(['t:b', 't:a']);
  });
});
