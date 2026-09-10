import { describe, expect, it } from 'vitest';
import { RecordId } from 'surrealdb';
import { isWindowed, materializeEffect, rowsEqual, rowsFromResult } from './materialize';

describe('materializeEffect', () => {
  const plan = { table: 'thing', where: [{ field: 'a', op: '=', value: 1 }] } as any;
  it('plan + ids: id-set plan select without where/limit/offset', () => {
    const e = materializeEffect({ surql: 'SELECT * FROM thing', params: { a: 1 }, plan: { ...plan, limit: 5, offset: 2 } }, ['thing:1']);
    expect(e.kind).toBe('local.select');
    if (e.kind !== 'local.select') throw new Error();
    expect(e.plan.ids).toEqual([new RecordId('thing', '1')]);
    expect(e.plan.where).toBeUndefined();
    expect(e.plan.limit).toBeUndefined();
    expect(e.params).toEqual({ a: 1 });
  });
  it('surql + ids: id-set rewrite bound to __win; unparseable surql falls back to the raw query', () => {
    const e = materializeEffect({ surql: 'SELECT * FROM thing WHERE a = $a ORDER BY b LIMIT 3', params: { a: 1 } }, ['thing:1']);
    expect(e).toMatchObject({ kind: 'local.query', sql: 'SELECT * FROM $__win ORDER BY b' });
    if (e.kind !== 'local.query') throw new Error();
    expect(e.vars).toEqual({ a: 1, __win: [new RecordId('thing', '1')] });
    const raw = materializeEffect({ surql: 'RETURN 1', params: {} }, ['thing:1']);
    expect(raw).toMatchObject({ kind: 'local.query', sql: 'RETURN 1' });
  });
  it('null ids: predicate scan via plan or raw surql', () => {
    expect(materializeEffect({ surql: 's', params: {}, plan }, null)).toMatchObject({ kind: 'local.select', plan });
    expect(materializeEffect({ surql: 'SELECT * FROM thing', params: { x: 1 } }, null)).toMatchObject({
      kind: 'local.query',
      sql: 'SELECT * FROM thing',
      vars: { x: 1 },
    });
  });
  it('rowsFromResult reads both effect shapes; rowsEqual compares structurally', () => {
    const sel = materializeEffect({ surql: 's', params: {}, plan }, null);
    const q = materializeEffect({ surql: 'SELECT * FROM thing', params: {} }, null);
    expect(rowsFromResult(sel, [{ id: 1 }])).toEqual([{ id: 1 }]);
    expect(rowsFromResult(sel, undefined)).toEqual([]);
    expect(rowsFromResult(q, [[{ id: 1 }]])).toEqual([{ id: 1 }]);
    expect(rowsFromResult(q, [null])).toEqual([]);
    expect(rowsFromResult(q, 'x')).toEqual([]);
    const rows = [{ id: 1 }];
    expect(rowsEqual(rows, rows)).toBe(true);
    expect(rowsEqual(rows, [{ id: 1 }])).toBe(true);
    expect(rowsEqual(rows, [{ id: 2 }])).toBe(false);
    expect(rowsEqual(rows, [])).toBe(false);
  });
  it('isWindowed', () => {
    expect(isWindowed('SELECT * FROM t LIMIT 10 START 10')).toBe(true);
    expect(isWindowed('SELECT * FROM t LIMIT 10')).toBe(false);
  });
});
