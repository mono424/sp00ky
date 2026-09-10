import { describe, it, expect } from 'vitest';
import { RecordId } from 'surrealdb';
import {
  diffRecordVersionArray,
  applyRecordVersionDiff,
  createDiffFromDbOp,
  ArraySyncer,
  resolveListRefPollInterval,
  DEFAULT_LIST_REF_POLL_INTERVAL_MS,
  buildListRefSelect,
  buildSubqueryListRefSelect,
  buildListRefBatchSelect,
  buildQueryRowCountBatchSelect,
  planListRefPollChunks,
  nextPollDelayMs,
  listRefPollDelayMs,
  LIST_REF_POLL_MAX_INTERVAL_MS,
  recordVersionArraysEqual,
} from './policy';
import type { RecordVersionArray, RecordVersionDiff } from '../types';
import { encodeRecordId } from '../utils/index';

describe('ArraySyncer.update', () => {
  it('planListRefPollChunks keeps duplicate hashes with equal ages stable', () => {
    const now = 1000;
    const chunks = planListRefPollChunks(
      [
        { hash: 'x', rows: 1, lastPolledAt: 0 },
        { hash: 'x', rows: 1, lastPolledAt: 0 },
        { hash: 'a', rows: 1, lastPolledAt: 0 },
      ],
      { now }
    );
    expect(chunks).toEqual([['a', 'x', 'x']]);
  });
  it('nextPollDelayMs treats a future live event as no event', () => {
    expect(nextPollDelayMs({ now: 100, lastLiveEventAt: 200, baseIntervalMs: 500 })).toBe(500);
  });
  it('bumps only the matching record and re-sorts on the next diff', () => {
    const syncer = new ArraySyncer([['t:b', 1], ['t:a', 1]], [['t:a', 2], ['t:b', 1]]);
    syncer.update('t:a', 2);
    expect(syncer.nextSet()).toEqual({ added: [], updated: [], removed: [] });
    syncer.insert('t:c', 1);
    syncer.delete('t:b');
    const diff = syncer.nextSet();
    expect(diff!.added.map((a) => encodeRecordId(a.id))).toEqual(['t:b']);
    expect(diff!.removed.map((r) => encodeRecordId(r))).toEqual(['t:c']);
  });
});

function rid(table: string, id: string): RecordId<string> {
  return new RecordId(table, id);
}

describe('diffRecordVersionArray', () => {
  it('detects added records (in remote, not local)', () => {
    const local: RecordVersionArray = [['user:1', 1]];
    const remote: RecordVersionArray = [
      ['user:1', 1],
      ['user:2', 1],
    ];
    const diff = diffRecordVersionArray(local, remote);

    expect(diff.added).toHaveLength(1);
    expect(encodeRecordId(diff.added[0].id)).toBe('user:2');
    expect(diff.added[0].version).toBe(1);
    expect(diff.updated).toHaveLength(0);
    expect(diff.removed).toHaveLength(0);
  });

  it('detects updated records (remote version > local version)', () => {
    const local: RecordVersionArray = [['user:1', 1]];
    const remote: RecordVersionArray = [['user:1', 3]];
    const diff = diffRecordVersionArray(local, remote);

    expect(diff.updated).toHaveLength(1);
    expect(encodeRecordId(diff.updated[0].id)).toBe('user:1');
    expect(diff.updated[0].version).toBe(3);
    expect(diff.added).toHaveLength(0);
    expect(diff.removed).toHaveLength(0);
  });

  it('detects removed records (in local, not remote)', () => {
    const local: RecordVersionArray = [
      ['user:1', 1],
      ['user:2', 1],
    ];
    const remote: RecordVersionArray = [['user:1', 1]];
    const diff = diffRecordVersionArray(local, remote);

    expect(diff.removed).toHaveLength(1);
    expect(encodeRecordId(diff.removed[0])).toBe('user:2');
    expect(diff.added).toHaveLength(0);
    expect(diff.updated).toHaveLength(0);
  });

  it('handles null arrays', () => {
    const diff = diffRecordVersionArray(null, null);
    expect(diff.added).toHaveLength(0);
    expect(diff.updated).toHaveLength(0);
    expect(diff.removed).toHaveLength(0);
  });

  it('handles empty arrays', () => {
    const diff = diffRecordVersionArray([], []);
    expect(diff.added).toHaveLength(0);
    expect(diff.updated).toHaveLength(0);
    expect(diff.removed).toHaveLength(0);
  });

  it('no diff when arrays match', () => {
    const arr: RecordVersionArray = [
      ['user:1', 1],
      ['user:2', 2],
    ];
    const diff = diffRecordVersionArray(arr, arr);
    expect(diff.added).toHaveLength(0);
    expect(diff.updated).toHaveLength(0);
    expect(diff.removed).toHaveLength(0);
  });

  it('handles mixed adds/updates/removes', () => {
    const local: RecordVersionArray = [
      ['user:1', 1],
      ['user:2', 1],
      ['user:3', 1],
    ];
    const remote: RecordVersionArray = [
      ['user:1', 1], // same
      ['user:2', 3], // updated
      // user:3 removed
      ['user:4', 1], // added
    ];
    const diff = diffRecordVersionArray(local, remote);

    expect(diff.added).toHaveLength(1);
    expect(encodeRecordId(diff.added[0].id)).toBe('user:4');
    expect(diff.updated).toHaveLength(1);
    expect(encodeRecordId(diff.updated[0].id)).toBe('user:2');
    expect(diff.removed).toHaveLength(1);
    expect(encodeRecordId(diff.removed[0])).toBe('user:3');
  });
});

describe('applyRecordVersionDiff', () => {
  it('applies additions', () => {
    const current: RecordVersionArray = [['user:1', 1]];
    const diff: RecordVersionDiff = {
      added: [{ id: rid('user', '2'), version: 1 }],
      updated: [],
      removed: [],
    };
    const result = applyRecordVersionDiff(current, diff);
    expect(result).toEqual([
      ['user:1', 1],
      ['user:2', 1],
    ]);
  });

  it('applies updates', () => {
    const current: RecordVersionArray = [['user:1', 1]];
    const diff: RecordVersionDiff = {
      added: [],
      updated: [{ id: rid('user', '1'), version: 5 }],
      removed: [],
    };
    const result = applyRecordVersionDiff(current, diff);
    expect(result).toEqual([['user:1', 5]]);
  });

  it('applies removals', () => {
    const current: RecordVersionArray = [
      ['user:1', 1],
      ['user:2', 2],
    ];
    const diff: RecordVersionDiff = {
      added: [],
      updated: [],
      removed: [rid('user', '1')],
    };
    const result = applyRecordVersionDiff(current, diff);
    expect(result).toEqual([['user:2', 2]]);
  });

  it('result is sorted by record ID', () => {
    const current: RecordVersionArray = [['user:c', 1]];
    const diff: RecordVersionDiff = {
      added: [
        { id: rid('user', 'a'), version: 1 },
        { id: rid('user', 'z'), version: 1 },
      ],
      updated: [],
      removed: [],
    };
    const result = applyRecordVersionDiff(current, diff);
    expect(result).toEqual([
      ['user:a', 1],
      ['user:c', 1],
      ['user:z', 1],
    ]);
  });

  it('empty diff returns original (sorted)', () => {
    const current: RecordVersionArray = [
      ['user:b', 2],
      ['user:a', 1],
    ];
    const diff: RecordVersionDiff = { added: [], updated: [], removed: [] };
    const result = applyRecordVersionDiff(current, diff);
    expect(result).toEqual([
      ['user:a', 1],
      ['user:b', 2],
    ]);
  });
});

describe('createDiffFromDbOp', () => {
  it('CREATE populates added array', () => {
    const recordId = rid('user', '1');
    const diff = createDiffFromDbOp('CREATE', recordId, 1);
    expect(diff.added).toHaveLength(1);
    expect(diff.added[0].id).toBe(recordId);
    expect(diff.added[0].version).toBe(1);
    expect(diff.updated).toHaveLength(0);
    expect(diff.removed).toHaveLength(0);
  });

  it('UPDATE populates updated array', () => {
    const recordId = rid('user', '1');
    const diff = createDiffFromDbOp('UPDATE', recordId, 2);
    expect(diff.updated).toHaveLength(1);
    expect(diff.updated[0].id).toBe(recordId);
    expect(diff.updated[0].version).toBe(2);
    expect(diff.added).toHaveLength(0);
    expect(diff.removed).toHaveLength(0);
  });

  it('DELETE populates removed array', () => {
    const recordId = rid('user', '1');
    const diff = createDiffFromDbOp('DELETE', recordId, 1);
    expect(diff.removed).toHaveLength(1);
    expect(diff.removed[0]).toBe(recordId);
    expect(diff.added).toHaveLength(0);
    expect(diff.updated).toHaveLength(0);
  });

  it('skips if existing version >= new version', () => {
    const recordId = rid('user', '1');
    const versions: RecordVersionArray = [['user:1', 5]];
    const diff = createDiffFromDbOp('UPDATE', recordId, 3, versions);
    expect(diff.added).toHaveLength(0);
    expect(diff.updated).toHaveLength(0);
    expect(diff.removed).toHaveLength(0);
  });

  it('applies if existing version < new version', () => {
    const recordId = rid('user', '1');
    const versions: RecordVersionArray = [['user:1', 2]];
    const diff = createDiffFromDbOp('UPDATE', recordId, 5, versions);
    expect(diff.updated).toHaveLength(1);
    expect(diff.updated[0].version).toBe(5);
  });
});

describe('ArraySyncer', () => {
  it('insert adds to local array', () => {
    const syncer = new ArraySyncer(
      [['user:1', 1]],
      [['user:1', 1]]
    );

    syncer.insert('user:2', 1);
    const diff = syncer.nextSet();
    expect(diff).not.toBeNull();
    // local now has user:2 which remote does not → user:2 is in local removed from remote perspective
    // Actually: local=[user:1, user:2], remote=[user:1] → user:2 is "removed" (in local, not remote)
    expect(diff!.removed).toHaveLength(1);
    expect(encodeRecordId(diff!.removed[0])).toBe('user:2');
  });

  it('update modifies version in local array', () => {
    const syncer = new ArraySyncer(
      [['user:1', 1]],
      [['user:1', 1]]
    );

    syncer.update('user:1', 5);
    const diff = syncer.nextSet();
    // local version (5) > remote version (1), so no "updated" from diff perspective
    // diff finds remote additions/updates relative to local; local version higher means no remote update
    expect(diff).not.toBeNull();
    expect(diff!.added).toHaveLength(0);
    expect(diff!.updated).toHaveLength(0);
    expect(diff!.removed).toHaveLength(0);
  });

  it('delete removes from local array', () => {
    const syncer = new ArraySyncer(
      [
        ['user:1', 1],
        ['user:2', 1],
      ],
      [
        ['user:1', 1],
        ['user:2', 1],
      ]
    );

    syncer.delete('user:2');
    const diff = syncer.nextSet();
    expect(diff).not.toBeNull();
    // remote has user:2, local does not → added from remote perspective
    expect(diff!.added).toHaveLength(1);
    expect(encodeRecordId(diff!.added[0].id)).toBe('user:2');
  });

  it('nextSet returns correct diff against remote', () => {
    const syncer = new ArraySyncer(
      [['user:1', 1]],
      [
        ['user:1', 1],
        ['user:2', 1],
      ]
    );

    const diff = syncer.nextSet();
    expect(diff).not.toBeNull();
    expect(diff!.added).toHaveLength(1);
    expect(encodeRecordId(diff!.added[0].id)).toBe('user:2');
  });

  it('maintains sorting after mutations', () => {
    const syncer = new ArraySyncer(
      [['user:c', 1]],
      []
    );

    syncer.insert('user:a', 1);
    syncer.insert('user:z', 1);

    // nextSet triggers sort
    const diff = syncer.nextSet();
    // All 3 are in local but not remote → 3 removed items
    expect(diff!.removed).toHaveLength(3);
    // Check they come in sorted order
    const removedIds = diff!.removed.map((r) => encodeRecordId(r));
    const sorted = [...removedIds].toSorted();
    expect(removedIds).toEqual(sorted);
  });
});

describe('resolveListRefPollInterval', () => {
  it('returns the default when no option is provided', () => {
    expect(resolveListRefPollInterval()).toBe(DEFAULT_LIST_REF_POLL_INTERVAL_MS);
    expect(resolveListRefPollInterval(undefined)).toBe(
      DEFAULT_LIST_REF_POLL_INTERVAL_MS
    );
  });

  it('respects a positive override', () => {
    expect(resolveListRefPollInterval(1000)).toBe(1000);
    expect(resolveListRefPollInterval(5000)).toBe(5000);
  });

  it('falls back to default for zero, negative, NaN, or Infinity', () => {
    // Accepting these would either silently disable polling or busy-loop
    // the event loop, so they fall back to the default.
    expect(resolveListRefPollInterval(0)).toBe(DEFAULT_LIST_REF_POLL_INTERVAL_MS);
    expect(resolveListRefPollInterval(-500)).toBe(
      DEFAULT_LIST_REF_POLL_INTERVAL_MS
    );
    expect(resolveListRefPollInterval(NaN)).toBe(
      DEFAULT_LIST_REF_POLL_INTERVAL_MS
    );
    expect(resolveListRefPollInterval(Infinity)).toBe(
      DEFAULT_LIST_REF_POLL_INTERVAL_MS
    );
  });

  it('locks the default at 500ms so accidental tuning trips this test', () => {
    expect(DEFAULT_LIST_REF_POLL_INTERVAL_MS).toBe(500);
  });
});

describe('buildListRefSelect', () => {
  it('substitutes the table name', () => {
    expect(buildListRefSelect('_00_list_ref')).toContain('FROM _00_list_ref ');
    expect(buildListRefSelect('_00_list_ref_user_abc')).toContain(
      'FROM _00_list_ref_user_abc '
    );
  });

  it('filters by the bound query id', () => {
    expect(buildListRefSelect('_00_list_ref')).toContain('WHERE in = $in');
  });

  it('excludes subquery rows via parent IS NONE', () => {
    // Without this predicate the poll would surface child records (the
    // subquery-projection rows the SSP also writes into list_ref) as
    // spurious "added" diffs against `localArray` every tick.
    expect(buildListRefSelect('_00_list_ref')).toContain('parent IS NONE');
  });

  it('selects only `out` and `version`', () => {
    // The diff only needs the record id and its version; pulling more
    // fields would bloat the per-tick query traffic.
    const sql = buildListRefSelect('_00_list_ref');
    expect(sql.startsWith('SELECT out, version FROM ')).toBe(true);
  });
});

describe('buildSubqueryListRefSelect', () => {
  it('substitutes the table name', () => {
    expect(buildSubqueryListRefSelect('_00_list_ref')).toContain('FROM _00_list_ref ');
    expect(buildSubqueryListRefSelect('_00_list_ref_user_abc')).toContain(
      'FROM _00_list_ref_user_abc '
    );
  });

  it('filters by the bound query id', () => {
    expect(buildSubqueryListRefSelect('_00_list_ref')).toContain('WHERE in = $in');
  });

  it('selects ONLY subquery child rows via parent IS NOT NONE', () => {
    // The mirror of `buildListRefSelect`'s `parent IS NONE`: the SSP writes
    // `.related()` child rows into list_ref tagged with `parent`/`parent_rel`;
    // this select pulls exactly those so their bodies can be synced into the
    // local cache (separately from the primary window) and the correlated
    // surql re-materializes with related data on a cold reload.
    const sql = buildSubqueryListRefSelect('_00_list_ref');
    expect(sql).toContain('parent IS NOT NONE');
    expect(sql).not.toContain('parent IS NONE ');
  });

  it('selects only `out` and `version`', () => {
    const sql = buildSubqueryListRefSelect('_00_list_ref');
    expect(sql.startsWith('SELECT out, version FROM ')).toBe(true);
  });
});

describe('nextPollDelayMs', () => {
  it('returns the base interval when LIVE has never delivered', () => {
    expect(
      nextPollDelayMs({
        now: 10_000,
        lastLiveEventAt: null,
        baseIntervalMs: 500,
      })
    ).toBe(500);
  });

  it('returns the healthy interval when a LIVE event fired within the cooldown', () => {
    expect(
      nextPollDelayMs({
        now: 10_000,
        lastLiveEventAt: 8_000, // 2s ago, well within 5s cooldown
        baseIntervalMs: 500,
      })
    ).toBe(5_000);
  });

  it('snaps back to the base interval after the cooldown elapses', () => {
    expect(
      nextPollDelayMs({
        now: 20_000,
        lastLiveEventAt: 10_000, // 10s ago, well past cooldown
        baseIntervalMs: 500,
      })
    ).toBe(500);
  });

  it('treats the cooldown boundary as expired (>= cooldownMs returns base)', () => {
    // Exactly at the boundary, the LIVE feed is considered cold so we
    // resume the aggressive cadence. Picks the same direction as
    // `if (sinceLive >= cooldownMs)`.
    expect(
      nextPollDelayMs({
        now: 15_000,
        lastLiveEventAt: 10_000, // exactly 5s ago
        baseIntervalMs: 500,
        cooldownMs: 5_000,
      })
    ).toBe(500);
  });

  it('clamps a clock-skew negative sinceLive to the base interval', () => {
    // If a stale timestamp puts the LIVE event in the future, we
    // should not interpret that as "LIVE healthy" — treat it as
    // unknown and use the conservative aggressive cadence.
    expect(
      nextPollDelayMs({
        now: 10_000,
        lastLiveEventAt: 20_000, // future timestamp
        baseIntervalMs: 500,
      })
    ).toBe(500);
  });

  it('never widens below an aggressively-configured base interval', () => {
    // If the caller picked a faster base (e.g. 100ms) on purpose,
    // the healthy path should not silently slow things back to 5s.
    // The healthy interval is clamped to at least base.
    expect(
      nextPollDelayMs({
        now: 10_000,
        lastLiveEventAt: 9_500,
        baseIntervalMs: 8_000,
      })
    ).toBe(8_000);
  });

  it('respects custom cooldown and healthy-interval values', () => {
    expect(
      nextPollDelayMs({
        now: 10_000,
        lastLiveEventAt: 9_500, // 0.5s ago
        baseIntervalMs: 250,
        cooldownMs: 1_000,
        healthyIntervalMs: 2_000,
      })
    ).toBe(2_000);
    expect(
      nextPollDelayMs({
        now: 10_000,
        lastLiveEventAt: 8_500, // 1.5s ago > cooldown
        baseIntervalMs: 250,
        cooldownMs: 1_000,
        healthyIntervalMs: 2_000,
      })
    ).toBe(250);
  });
});

describe('listRefPollDelayMs', () => {
  it('returns the base interval at idle streak 0 (something just happened)', () => {
    expect(listRefPollDelayMs({ idleStreak: 0, baseIntervalMs: 500 })).toBe(500);
    // Negative streak is defensively treated as "active" too.
    expect(listRefPollDelayMs({ idleStreak: -3, baseIntervalMs: 500 })).toBe(500);
  });

  it('doubles per idle streak (exponential backoff)', () => {
    expect(listRefPollDelayMs({ idleStreak: 1, baseIntervalMs: 500 })).toBe(1_000);
    expect(listRefPollDelayMs({ idleStreak: 2, baseIntervalMs: 500 })).toBe(2_000);
    expect(listRefPollDelayMs({ idleStreak: 3, baseIntervalMs: 500 })).toBe(4_000);
  });

  it('caps at LIST_REF_POLL_MAX_INTERVAL_MS (5s) once the doubling exceeds it', () => {
    // streak 4 would be 8000ms uncapped → clamped to 5000.
    expect(listRefPollDelayMs({ idleStreak: 4, baseIntervalMs: 500 })).toBe(5_000);
    expect(listRefPollDelayMs({ idleStreak: 50, baseIntervalMs: 500 })).toBe(5_000);
    expect(LIST_REF_POLL_MAX_INTERVAL_MS).toBe(5_000);
  });

  it('never returns below the configured base, even when base exceeds the cap', () => {
    // An aggressively-large base must not be implicitly shrunk by the cap.
    expect(listRefPollDelayMs({ idleStreak: 0, baseIntervalMs: 8_000 })).toBe(8_000);
    expect(listRefPollDelayMs({ idleStreak: 5, baseIntervalMs: 8_000 })).toBe(8_000);
  });

  it('respects a custom max interval', () => {
    expect(
      listRefPollDelayMs({ idleStreak: 10, baseIntervalMs: 500, maxIntervalMs: 3_000 })
    ).toBe(3_000);
  });

  it('does not overflow for a very long idle streak', () => {
    // 2^1000 would be Infinity; the exponent clamp keeps it finite and capped.
    expect(listRefPollDelayMs({ idleStreak: 1_000, baseIntervalMs: 500 })).toBe(5_000);
  });
});

describe('recordVersionArraysEqual', () => {
  it('treats the same reference and identical contents as equal', () => {
    const a: RecordVersionArray = [['game:1', 1], ['game:2', 3]];
    expect(recordVersionArraysEqual(a, a)).toBe(true);
    expect(recordVersionArraysEqual(a, [['game:1', 1], ['game:2', 3]])).toBe(true);
  });

  it('is order-insensitive (the list_ref SELECT has no ORDER BY)', () => {
    expect(
      recordVersionArraysEqual(
        [['game:1', 1], ['game:2', 3]],
        [['game:2', 3], ['game:1', 1]]
      )
    ).toBe(true);
  });

  it('returns false when a version differs for the same id', () => {
    expect(
      recordVersionArraysEqual([['game:1', 1]], [['game:1', 2]])
    ).toBe(false);
  });

  it('returns false on length mismatch', () => {
    expect(
      recordVersionArraysEqual([['game:1', 1]], [['game:1', 1], ['game:2', 1]])
    ).toBe(false);
  });

  it('returns false when an id is present in one but not the other', () => {
    expect(
      recordVersionArraysEqual([['game:1', 1]], [['game:2', 1]])
    ).toBe(false);
  });

  it('treats two empty arrays as equal', () => {
    expect(recordVersionArraysEqual([], [])).toBe(true);
  });
});

describe('list_ref poll batching', () => {
  it('reads many queries in one statement and their rowCounts keyed by id', () => {
    expect(buildListRefBatchSelect('_00_list_ref_user_abc')).toBe(
      'SELECT in, out, version, parent FROM _00_list_ref_user_abc WHERE in IN $ins'
    );
    expect(buildQueryRowCountBatchSelect()).toBe(
      'SELECT VALUE { id: id, rowCount: rowCount, state: state } FROM $ins'
    );
  });

  it('packs due queries oldest-first under the row budget', () => {
    const chunks = planListRefPollChunks(
      [
        { hash: 'a', rows: 600, lastPolledAt: 300 },
        { hash: 'b', rows: 600, lastPolledAt: 100 },
        { hash: 'c', rows: 0, lastPolledAt: 0 },
        { hash: 'd', rows: 900, lastPolledAt: 200 },
      ],
      { now: 1_000, rowBudget: 1_400 }
    );
    // c (never polled) first, then b, d, a by age; d does not fit next to b.
    expect(chunks).toEqual([['c', 'b'], ['d'], ['a']]);
  });

  it('never splits below one query and always refreshes a lone huge view', () => {
    expect(
      planListRefPollChunks([{ hash: 'big', rows: 10_000, lastPolledAt: 0 }], {
        now: 100_000,
        rowBudget: 100,
      })
    ).toEqual([['big']]);
  });

  it('defers large views until their minimum age has passed', () => {
    const large = { hash: 'large', rows: 5_000, lastPolledAt: 990_000 };
    const small = { hash: 'small', rows: 10, lastPolledAt: 990_000 };
    expect(
      planListRefPollChunks([large, small], { now: 1_000_000, largeViewRows: 1_000, largeViewMinAgeMs: 15_000 })
    ).toEqual([['small']]);
    expect(
      planListRefPollChunks([large, small], { now: 1_006_000, largeViewRows: 1_000, largeViewMinAgeMs: 15_000 })
    // Same age: hash order breaks the tie, and the large view fills its own chunk.
    ).toEqual([['large'], ['small']]);
  });

  it('returns no chunks when nothing is due', () => {
    expect(planListRefPollChunks([], { now: 1 })).toEqual([]);
  });
});
