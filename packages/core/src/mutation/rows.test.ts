import { describe, expect, it } from 'vitest';
import { RecordId } from 'surrealdb';
import * as rows from './rows';

const rid = (t: string, i: string) => new RecordId(t, i);

describe('stored ids', () => {
  it('strips ⟨⟩ escaping and reads the timestamp prefix', () => {
    expect(rows.parseStoredRecordId('_00_pending_mutations:⟨1700000000000_0001_tab⟩')).toEqual(
      rid('_00_pending_mutations', '1700000000000_0001_tab')
    );
    expect(rows.storedIdString(rid('_00_pending_mutations', 'x'))).toBe('_00_pending_mutations:x');
    expect(rows.storedIdString('_00_pending_mutations:⟨x⟩')).toBe('_00_pending_mutations:x');
    expect(rows.createdAtFromId('_00_pending_mutations:⟨1700000000000_0001_tab⟩')).toBe(1700000000000);
    expect(rows.createdAtFromId('1700000000000_0001_tab')).toBe(1700000000000);
    expect(rows.createdAtFromId('_00_pending_mutations:abc')).toBeNull();
  });
});

describe('parsePendingRow', () => {
  it('reads v2 rows', () => {
    const row = rows.parsePendingRow({
      id: '_00_pending_mutations:⟨1700000000000_0001_tab⟩',
      mutationType: 'update',
      recordId: 'thing:1',
      tableName: 'thing',
      data: { a: 1 },
      beforeRecord: { id: 'thing:1', a: 0 },
      createdAt: 5,
      v: 2,
    });
    expect(row).toEqual({
      id: '_00_pending_mutations:1700000000000_0001_tab',
      mutationType: 'update',
      recordId: 'thing:1',
      tableName: 'thing',
      data: { a: 1 },
      beforeRecord: { id: 'thing:1', a: 0 },
      createdAt: 5,
      v: 2,
    });
  });
  it('tolerates legacy rows and rejects unreplayable ones', () => {
    const legacy = rows.parsePendingRow({
      id: '_00_pending_mutations:1700000000000_0001_tab',
      mutationType: 'delete',
      recordId: rid('thing', '1'),
    });
    expect(legacy).toMatchObject({ tableName: 'thing', createdAt: 1700000000000, v: 1, beforeRecord: undefined });
    expect(rows.parsePendingRow({ id: '_00_pending_mutations:1', mutationType: 'create', recordId: 'thing:1' })).toBeNull();
    expect(rows.parsePendingRow({ id: '_00_pending_mutations:abc', mutationType: 'delete', recordId: 'thing:1' })!.createdAt).toBe(0);
    expect(rows.parsePendingRow(null)).toBeNull();
    expect(rows.parsePendingRow({ mutationType: 'bogus', recordId: 'x' })).toBeNull();
    expect(rows.parsePendingRow({ mutationType: 'delete', recordId: 5 })).toBeNull();
    expect(rows.toOutboxItem(legacy!)).toEqual({
      id: legacy!.id,
      type: 'delete',
      recordId: 'thing:1',
      table: 'thing',
      status: 'pending',
      ackedAt: null,
      attempts: 0,
    });
    expect(rows.loadPendingRows()).toBe('SELECT * FROM _00_pending_mutations ORDER BY id ASC');
    expect(rows.loadFailedRows()).toBe('SELECT * FROM _00_failed_mutations ORDER BY failedAt ASC');
  });
});

describe('local write transactions', () => {
  const base = { recordId: rid('thing', '1'), mutationId: rid('_00_pending_mutations', 'm'), table: 'thing', now: 7 };
  it('create: row + v2 outbox entry, result index 0', () => {
    const { query, vars } = rows.planCreateTx({ ...base, data: { a: 1, b: 'x' } });
    expect(query.sql).toBe(
      "BEGIN TRANSACTION;\nCREATE ONLY $id SET a = $data_a, b = $data_b;CREATE ONLY $mid SET mutationType = 'create', recordId = $id, tableName = $table, createdAt = $createdAt, v = 2, data = $data;\nCOMMIT TRANSACTION;"
    );
    expect(vars).toEqual({ id: base.recordId, mid: base.mutationId, table: 'thing', createdAt: 7, data: { a: 1, b: 'x' }, data_a: 1, data_b: 'x' });
    expect(query.extract([null, 'row', 'outbox'])).toBe('row');
    expect(rows.planCreateTx(base).vars.data).toEqual({});
  });
  it('update: rv bump, merge, outbox with beforeRecord, RETURN target', () => {
    const { query, vars } = rows.planUpdateTx({ ...base, data: { a: 2 }, before: { id: 'thing:1', a: 1 } });
    expect(query.sql).toContain('UPDATE $id SET _00_rv += 1;LET $updated = (UPDATE ONLY $id MERGE $data);');
    expect(query.sql).toContain("mutationType = 'update', recordId = $id, tableName = $table, createdAt = $createdAt, v = 2, data = $data, beforeRecord = $before;RETURN {target: $updated}");
    expect(vars.before).toEqual({ id: 'thing:1', a: 1 });
    expect(rows.planUpdateTx(base).vars).toMatchObject({ data: {}, before: null });
  });
  it('delete: DELETE + outbox with beforeRecord', () => {
    const { query, vars } = rows.planDeleteTx({ ...base, before: { id: 'thing:1' } });
    expect(query.sql).toBe(
      "BEGIN TRANSACTION;\nDELETE $id;CREATE ONLY $mid SET mutationType = 'delete', recordId = $id, tableName = $table, createdAt = $createdAt, v = 2, beforeRecord = $before;\nCOMMIT TRANSACTION;"
    );
    expect(vars.before).toEqual({ id: 'thing:1' });
    expect(rows.planDeleteTx(base).vars.before).toBeNull();
  });
});

describe('debounced update transactions', () => {
  it('local-only update and deferred outbox row', () => {
    const local = rows.planLocalOnlyUpdateTx({ recordId: rid('thing', '1'), data: { a: 1 } });
    expect(local.query.sql).toBe('BEGIN TRANSACTION;\nUPDATE $id SET _00_rv += 1;LET $updated = (UPDATE ONLY $id MERGE $data);RETURN {target: $updated};\nCOMMIT TRANSACTION;');
    expect(local.query.extract([null, 1, 2, { target: 'x' }])).toEqual({ target: 'x' });
    const deferred = rows.planDeferredOutboxRowTx({ recordId: rid('thing', '1'), mutationId: rid('_00_pending_mutations', 'm'), table: 'thing', data: { a: 1 }, before: { a: 0 }, now: 3 });
    expect(deferred.query.sql).toBe("BEGIN TRANSACTION;\nCREATE ONLY $mid SET mutationType = 'update', recordId = $id, tableName = $table, createdAt = $createdAt, v = 2, data = $data, beforeRecord = $before;\nCOMMIT TRANSACTION;");
    expect(rows.planDeferredOutboxRowTx({ recordId: rid('thing', '1'), mutationId: rid('_00_pending_mutations', 'm'), table: 'thing', now: 3 }).vars).toMatchObject({ data: {}, before: null });
  });
});

describe('remoteBatch', () => {
  it('one statement per row, indexed vars, mixed types', () => {
    const { sql, vars } = rows.remoteBatch([
      { id: 'm1', mutationType: 'create', recordId: 'thing:1', tableName: 'thing', data: { a: 1 }, createdAt: 0, v: 2 },
      { id: 'm2', mutationType: 'update', recordId: 'thing:2', tableName: 'thing', data: { b: 2 }, createdAt: 0, v: 2 },
      { id: 'm3', mutationType: 'delete', recordId: 'thing:3', tableName: 'thing', createdAt: 0, v: 2 },
      { id: 'm4', mutationType: 'update', recordId: 'thing:4', tableName: 'thing', createdAt: 0, v: 2 },
    ]);
    expect(sql).toBe('CREATE ONLY $id0 SET a = $d0_a;\nUPDATE $id1 MERGE $data1;\nDELETE $id2;\nUPDATE $id3 MERGE $data3');
    expect(vars).toEqual({
      id0: rid('thing', '1'),
      d0_a: 1,
      id1: rid('thing', '2'),
      data1: { b: 2 },
      id2: rid('thing', '3'),
      id3: rid('thing', '4'),
      data3: {},
    });
    expect(rows.remoteBatch([{ id: 'm', mutationType: 'create', recordId: 'thing:9', tableName: 'thing', createdAt: 0, v: 2 }]).sql).toBe('CREATE ONLY $id0 SET ');
  });
});

describe('rollback plans', () => {
  const pending = (type: rows.PendingMutationRow['mutationType']): rows.PendingMutationRow => ({
    id: '_00_pending_mutations:m',
    mutationType: type,
    recordId: 'thing:1',
    tableName: 'thing',
    data: { a: 1 },
    createdAt: 1,
    v: 2,
  });
  it('create reverts by delete', () => {
    const plan = rows.planRevert(pending('create'), null);
    expect(plan.revert).toBe('full');
    expect(plan.tx!.query.sql).toBe('BEGIN TRANSACTION;\nDELETE $id;\nCOMMIT TRANSACTION;');
    expect(plan.circuit).toEqual({ table: 'thing', op: 'DELETE', id: 'thing:1', record: { a: 1 } });
    expect(rows.planRevert({ ...pending('create'), data: undefined }, null).circuit.record).toEqual({});
  });
  it('update / delete restore beforeRecord, or are partial without it', () => {
    const before = { id: 'thing:1', a: 0, _00_rv: 3 };
    const upd = rows.planRevert(pending('update'), before);
    expect(upd.tx!.query.sql).toBe('BEGIN TRANSACTION;\nUPSERT ONLY $id REPLACE $content;\nCOMMIT TRANSACTION;');
    expect(upd.tx!.vars).toEqual({ id: rid('thing', '1'), content: { a: 0, _00_rv: 3 } });
    expect(upd.circuit).toMatchObject({ op: 'UPDATE', record: { a: 0, _00_rv: 3, id: rid('thing', '1') } });
    const del = rows.planRevert(pending('delete'), before);
    expect(del.circuit.op).toBe('CREATE');
    const partial = rows.planRevert(pending('update'), null);
    expect(partial).toEqual({ tx: null, circuit: { table: 'thing', op: 'UPDATE', id: 'thing:1', record: {} }, revert: 'partial' });
  });
  it('failed rows: build, move tx, delete statements, parse', () => {
    const failed = rows.buildFailedRow(pending('update'), { message: 'denied', kind: 'application' }, { id: 'thing:1' }, 2, 99, 'full');
    expect(failed).toMatchObject({ id: '_00_pending_mutations:m', attempts: 2, failedAt: 99, revert: 'full', beforeRecord: { id: 'thing:1' } });
    const { query, vars } = rows.moveToFailedTx(failed);
    expect(query.sql).toBe('BEGIN TRANSACTION;\nCREATE ONLY $fid CONTENT $failed;DELETE $mid;\nCOMMIT TRANSACTION;');
    expect(vars.fid).toEqual(rid('_00_failed_mutations', 'm'));
    expect(vars.mid).toEqual(rid('_00_pending_mutations', 'm'));
    expect((vars.failed as Record<string, unknown>).id).toBeUndefined();
    expect(rows.deletePendingRow('_00_pending_mutations:m')).toEqual({ sql: 'DELETE $mid', vars: { mid: rid('_00_pending_mutations', 'm') } });
    expect(rows.deleteFailedRow('_00_pending_mutations:m')).toEqual({ sql: 'DELETE $fid', vars: { fid: rid('_00_failed_mutations', 'm') } });
    const parsed = rows.parseFailedRow({ ...failed, id: '_00_failed_mutations:⟨m⟩' });
    expect(parsed).toMatchObject({ id: '_00_pending_mutations:m', mutationType: 'update', error: { message: 'denied', kind: 'application' }, revert: 'full' });
    expect(rows.parseFailedRow({ id: '_00_failed_mutations:m', mutationType: 'delete', recordId: 'thing:2', error: { kind: 'unreplayable' }, revert: 'partial' })).toMatchObject({
      tableName: 'thing',
      error: { message: 'unknown', kind: 'unreplayable' },
      attempts: 0,
      createdAt: 0,
      failedAt: 0,
      revert: 'partial',
      beforeRecord: null,
      data: undefined,
    });
    expect(rows.parseFailedRow(null)).toBeNull();
    expect(rows.parseFailedRow({ id: '_00_failed_mutations:m', mutationType: 'create', recordId: 'thing:1' })!.error).toEqual({ message: 'unknown', kind: 'application' });
    expect(rows.parseFailedRow({ mutationType: 'nope', recordId: 'x' })).toBeNull();
    expect(rows.parseFailedRow({ mutationType: 'delete', recordId: 1 })).toBeNull();
  });
});

describe('hydrateRowData', () => {
  const columns = { owner: { recordId: true }, at: { dateTime: true }, n: {} } as any;
  const base: rows.PendingMutationRow = { id: 'm', mutationType: 'create', recordId: 'thing:1', tableName: 'thing', createdAt: 0, v: 2 };
  it('re-types record and datetime fields from the schema, keeps unknown keys, leaves rows without data or schema alone', () => {
    const out = rows.hydrateRowData({ ...base, data: { owner: 'user:u1', at: '2026-01-02T00:00:00.000Z', n: 1, extra: 'x' } }, columns);
    expect(out.data).toEqual({ owner: rid('user', 'u1'), at: new Date('2026-01-02T00:00:00.000Z'), n: 1, extra: 'x' });
    expect(rows.hydrateRowData(base, columns)).toBe(base);
    const noSchema = { ...base, data: { owner: 'user:u1' } };
    expect(rows.hydrateRowData(noSchema, null)).toBe(noSchema);
  });
  it('sends a payload the schema rejects as it is', () => {
    const bad = { ...base, data: { owner: 42 } };
    expect(rows.hydrateRowData(bad, columns)).toBe(bad);
  });
});
