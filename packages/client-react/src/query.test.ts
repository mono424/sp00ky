import { describe, expect, it } from 'vitest';
import { initialQueryState, reduceQuery, sameValue, share, type QueryState } from './query';

class FakeRecordId {
  constructor(
    readonly tb: string,
    readonly id: string
  ) {}
  toString() {
    return `${this.tb}:${this.id}`;
  }
}

describe('share', () => {
  it('returns prev when the emission is deep-equal, with fresh RecordId and Date instances', () => {
    const prev = [
      { id: new FakeRecordId('post', 'a'), at: new Date(1), tags: ['x'], author: { name: 'n' } },
    ];
    const next = [
      { id: new FakeRecordId('post', 'a'), at: new Date(1), tags: ['x'], author: { name: 'n' } },
    ];
    expect(share(prev, next)).toBe(prev);
  });

  it('keeps unchanged rows by id across a reorder and an insert', () => {
    const a = { id: 'a', n: 1 };
    const b = { id: 'b', n: 2 };
    const out = share(
      [a, b],
      [
        { id: 'c', n: 3 },
        { id: 'b', n: 2 },
        { id: 'a', n: 1 },
      ]
    );
    expect(out[1]).toBe(b);
    expect(out[2]).toBe(a);
    expect(out[0]).toEqual({ id: 'c', n: 3 });
  });

  it('replaces only the changed row and the changed nested branch', () => {
    const author = { name: 'n' };
    const a = { id: 'a', title: 't', author };
    const b = { id: 'b', title: 'u', author: { name: 'm' } };
    const out = share(
      [a, b],
      [
        { id: 'a', title: 't2', author: { name: 'n' } },
        { id: 'b', title: 'u', author: { name: 'm' } },
      ]
    );
    expect(out[0]).not.toBe(a);
    expect(out[0].author).toBe(author);
    expect(out[1]).toBe(b);
  });

  it('notices a dropped field', () => {
    const a = { id: 'a', x: 1, y: 2 };
    expect(share([a], [{ id: 'a', x: 1 }])[0]).toEqual({ id: 'a', x: 1 });
  });

  it('never mutates its inputs', () => {
    const prev = [{ id: 'a', n: 1 }];
    const next = [{ id: 'a', n: 2 }];
    share(prev, next);
    expect(prev[0].n).toBe(1);
    expect(next[0].n).toBe(2);
  });
});

describe('sameValue', () => {
  it('compares same-class wrappers by their printed value, not opaque ones', () => {
    expect(sameValue(new FakeRecordId('t', '1'), new FakeRecordId('t', '1'))).toBe(true);
    expect(sameValue(new FakeRecordId('t', '1'), new FakeRecordId('t', '2'))).toBe(false);
    expect(sameValue(new ArrayBuffer(1), new ArrayBuffer(1))).toBe(false);
    expect(sameValue(new Uint8Array([1]), new Uint8Array([1]))).toBe(true);
  });
});

describe('reduceQuery', () => {
  const run = (...events: Parameters<typeof reduceQuery>[1][]): QueryState =>
    events.reduce(reduceQuery, initialQueryState);

  it('an empty emission before the server answers is not a paint', () => {
    const s = run({ type: 'rows', rows: [], isOne: false });
    expect(s.hasPainted).toBe(false);
    expect(s.data).toEqual([]);
  });

  it('a cached paint, then authority, then a bucket switch', () => {
    let s = run({ type: 'rows', rows: [{ id: 'a' }], isOne: false });
    expect(s.hasPainted).toBe(true);
    s = reduceQuery(s, { type: 'authority', known: true });
    expect(s.isAuthoritative).toBe(true);
    s = reduceQuery(s, { type: 'authority', known: false });
    expect(s.isAuthoritative).toBe(false);
    expect(s.hasPainted).toBe(false);
  });

  it('reset keeps data but clears the identity flags', () => {
    let s = run(
      { type: 'rows', rows: [{ id: 'a' }], isOne: false },
      { type: 'authority', known: true },
      { type: 'status', fetching: true }
    );
    const data = s.data;
    s = reduceQuery(s, { type: 'reset' });
    expect(s.data).toBe(data);
    expect(s).toMatchObject({ isAuthoritative: false, hasPainted: false, isFetching: false });
  });

  it('returns the same state for a no-op event', () => {
    const s = run({ type: 'rows', rows: [{ id: 'a', n: 1 }], isOne: false });
    expect(reduceQuery(s, { type: 'rows', rows: [{ id: 'a', n: 1 }], isOne: false })).toBe(s);
    expect(reduceQuery(s, { type: 'status', fetching: false })).toBe(s);
  });

  it('one() takes the first row or null', () => {
    expect(run({ type: 'rows', rows: [{ id: 'a' }], isOne: true }).data).toEqual({ id: 'a' });
    const empty = run({ type: 'rows', rows: [], isOne: true });
    expect(empty.data).toBeNull();
    expect(empty.hasPainted).toBe(false);
  });
});
