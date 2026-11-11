import { describe, expect, it } from 'vitest';
import { RecordId } from 'surrealdb';
import { sameValue, share } from '@spooky-sync/client-angular';

describe('share', () => {
  it('returns prev for a deep-equal emission, RecordId and Date by value', () => {
    const prev = [{ id: new RecordId('post', 'a'), at: new Date(1), tags: ['x'] }];
    const next = [{ id: new RecordId('post', 'a'), at: new Date(1), tags: ['x'] }];
    expect(share(prev, next)).toBe(prev);
  });

  it('keeps unchanged rows by id across reorder, insert and delete', () => {
    const a = { id: 'post:a', title: 'A' };
    const b = { id: 'post:b', title: 'B' };
    const c = { id: 'post:c', title: 'C' };
    const out = share(
      [a, b, c],
      [
        { id: 'post:c', title: 'C' },
        { id: 'post:d', title: 'D' },
        { id: 'post:a', title: 'A!' },
      ]
    );
    expect(out[0]).toBe(c);
    expect(out[1]).toEqual({ id: 'post:d', title: 'D' });
    expect(out[2]).not.toBe(a);
    expect(out[2]).toEqual({ id: 'post:a', title: 'A!' });
  });

  it('keeps an unchanged nested related row when a sibling field changes', () => {
    const author = { id: 'user:1', name: 'Ann' };
    const prev = { id: 'post:a', title: 'A', author };
    const out = share(prev, { id: 'post:a', title: 'B', author: { id: 'user:1', name: 'Ann' } });
    expect(out).not.toBe(prev);
    expect(out.author).toBe(author);
  });

  it('sameValue never equates plain objects or default-toString classes', () => {
    expect(sameValue({}, {})).toBe(false);
    expect(sameValue(new ArrayBuffer(1), new ArrayBuffer(1))).toBe(false);
    expect(sameValue(new RecordId('a', '1'), new RecordId('a', '2'))).toBe(false);
  });
});
