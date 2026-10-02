import { describe, expect, it } from 'vitest';
import { RecordId } from '@spooky-sync/query-builder';
import { absentOptionalColumns, parseParams, parseQueryParams } from './parser';

const columns = {
  white: { recordId: true },
  black: { recordId: true },
  owner: { recordId: true },
  created_at: { dateTime: true },
  result: {},
} as any;

describe('parseQueryParams', () => {
  // The regression this exists for: an `_or` group binds under synthetic names,
  // and dropping them left `(white = $white__or0 OR black = $black__or1)` with
  // nothing bound, so every OR query registered and matched no rows at all.
  it('keeps an _or branch param and types it through its field', () => {
    const parsed = parseQueryParams(columns, {
      owner: 'user:abc',
      white__or0: 'player_name:PN_1',
      black__or1: 'player_name:PN_1',
    });

    expect(Object.keys(parsed).sort()).toEqual(['black__or1', 'owner', 'white__or0']);
    expect(parsed.white__or0).toBeInstanceOf(RecordId);
    expect(String(parsed.white__or0)).toBe('player_name:PN_1');
    expect(parsed.black__or1).toBeInstanceOf(RecordId);
  });

  it('passes a param it cannot resolve through untouched instead of dropping it', () => {
    const parsed = parseQueryParams(columns, { in: 'anything', limit: 30 });
    expect(parsed).toEqual({ in: 'anything', limit: 30 });
  });

  it('still types and keeps plain column params, and skips undefined', () => {
    const parsed = parseQueryParams(columns, {
      owner: 'user:abc',
      created_at: '2026-01-01T00:00:00.000Z',
      result: undefined,
    });
    expect(parsed.owner).toBeInstanceOf(RecordId);
    expect(parsed.created_at).toBeInstanceOf(Date);
    expect('result' in parsed).toBe(false);
  });
});

describe('parseParams', () => {
  // Records keep the old behaviour: a field the schema does not know is dropped
  // rather than written.
  it('drops a field that is not a column', () => {
    const parsed = parseParams(columns, { result: '1-0', nonsense: 'x', white__or0: 'y' });
    expect(parsed).toEqual({ result: '1-0' });
  });
});

describe('absentOptionalColumns', () => {
  const schema = {
    title: { type: 'string', optional: false },
    club: { type: 'string', recordId: true, optional: true },
    note: { type: 'string', optional: true },
    _00_local: { type: 'string', optional: true },
    id: { type: 'string', optional: true },
  } as any;

  // The bug: `UPDATE broadcast:x UNSET club` leaves no `club` key in the body,
  // and a MERGE that does not name it keeps the stale local value.
  it('names every optional column the row lacks, as undefined', () => {
    const cleared = absentOptionalColumns(schema, { id: 'broadcast:x', title: 't', note: 'n' });
    expect(cleared).toStrictEqual({ club: undefined });
    expect(Object.prototype.hasOwnProperty.call(cleared, 'club')).toBe(true);
  });

  it('treats an explicit undefined (a decoded NONE) as absent', () => {
    expect(absentOptionalColumns(schema, { title: 't', club: undefined, note: 'n' })).toStrictEqual(
      { club: undefined }
    );
  });

  it('never names a required column, id, or _00_* bookkeeping', () => {
    expect(absentOptionalColumns(schema, {})).toStrictEqual({ club: undefined, note: undefined });
  });

  it('keeps a present value, even a falsy one', () => {
    expect(absentOptionalColumns(schema, { club: null, note: '' })).toStrictEqual({});
  });

  it('is empty without a schema', () => {
    expect(absentOptionalColumns(undefined, { a: 1 })).toStrictEqual({});
  });
});
