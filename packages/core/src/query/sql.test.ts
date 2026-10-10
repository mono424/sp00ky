import { describe, expect, it } from 'vitest';
import { RecordId } from 'surrealdb';
import * as sql from './sql';
import {
  buildListRefBatchSelect,
  buildListRefSelect,
  buildQueryRowCountBatchSelect,
  buildQueryRowCountSelect,
  buildSubqueryListRefSelect,
} from '../sync/policy';

describe('query sql builders (golden against the previous builders)', () => {
  it('edge and meta selects are byte-identical to the old helpers', () => {
    expect(sql.listRefSelect('_00_list_ref_user_x')).toBe(buildListRefSelect('_00_list_ref_user_x'));
    expect(sql.subqueryListRefSelect('t')).toBe(buildSubqueryListRefSelect('t'));
    expect(sql.queryRowCountSelect()).toBe(buildQueryRowCountSelect());
    expect(sql.listRefBatchSelect('t')).toBe(buildListRefBatchSelect('t'));
    expect(sql.queryRowCountBatchSelect()).toBe(buildQueryRowCountBatchSelect());
  });
  it('composes the single, batch and register requests', () => {
    expect(sql.singleSnapshotSelect('t').split(';\n')).toEqual([
      sql.listRefSelect('t'),
      sql.queryRowCountSelect(),
      sql.subqueryListRefSelect('t'),
    ]);
    expect(sql.batchSnapshotSelect('t')).toBe(`${sql.listRefBatchSelect('t')};\n${sql.queryRowCountBatchSelect()}`);
    expect(sql.registerSelect('t').startsWith('fn::query::register($config);\n')).toBe(true);
    const id = new RecordId('_00_query', 'h');
    expect(sql.registerVars({ id, surql: 's', params: { a: 1 }, ttl: '10m' })).toEqual({
      config: { id, surql: 's', params: { a: 1 }, ttl: '10m' },
      in: id,
    });
  });
  it('view rows and ids', () => {
    expect(sql.viewRecordId('k')).toEqual(new RecordId('_00_view', 'k'));
    expect(sql.viewRow([['t:1', 1]], [['c:1', 2]], true, 5)).toEqual({ ids: [['t:1', 1]], children: [['c:1', 2]], confirmed: true, updatedAt: 5 });
    expect(sql.readViewRows()).toBe('SELECT * FROM _00_view');
    expect(sql.readLegacyViewRows()).toBe('SELECT * FROM _00_window');
    expect(sql.countViewRows()).toContain('_00_view');
    expect(sql.bodySelect()).toBe('SELECT * FROM $ids');
  });
  it('heartbeat batch answers per index and detects reclaimed rows', () => {
    const a = new RecordId('_00_query', 'a');
    const b = new RecordId('_00_query', 'b');
    expect(sql.heartbeatBatch([a, b])).toEqual({
      sql: 'fn::query::heartbeat($id0);\nfn::query::heartbeat($id1)',
      vars: { id0: a, id1: b },
    });
    expect(sql.heartbeatRowGone([])).toBe(true);
    expect(sql.heartbeatRowGone([{ id: a }])).toBe(false);
    expect(sql.heartbeatRowGone(undefined)).toBe(false);
  });
  it('the pagehide beacon names the page sessions and inlines only shapes it minted', () => {
    const a = new RecordId('_00_query', 'a'.repeat(64));
    const b = new RecordId('_00_query', 'b'.repeat(64));
    const s1 = 'de81a067-f4bc-4351-a2a3-1ecf06b6305a';
    const s2 = '8A14ED5A-32B5-4B4E-973D-D2B88860932F';
    expect(sql.leaveViewsBeacon([a, b], [s1, s2])).toBe(
      `FOR $id IN [${a}, ${b}] { LET $_left = fn::query::leave($id, ['${s1}', '${s2}']); };`
    );
    const digits = new RecordId('_00_query', '1'.repeat(64));
    expect(sql.leaveViewsBeacon([digits], [s1])).toBe(`FOR $id IN [${digits}] { LET $_left = fn::query::leave($id, ['${s1}']); };`);
    // Anything else is dropped rather than interpolated.
    expect(sql.leaveViewsBeacon([a, new RecordId('_00_query', "x']; DELETE user; --"), 'user:1'], [s1, "x'; DELETE user; --"])).toBe(
      `FOR $id IN [${a}] { LET $_left = fn::query::leave($id, ['${s1}']); };`
    );
    // No session ids: the server could not tell this page's entries from a reload's, so send nothing.
    expect(sql.leaveViewsBeacon([a], [])).toBeNull();
    expect(sql.leaveViewsBeacon([], [s1])).toBeNull();
  });
  it('upsertBodiesTx MERGEs each body inside one transaction', () => {
    const { query, vars } = sql.upsertBodiesTx([
      { id: 'thing:1', content: { a: 1 } },
      { id: 'thing:2', content: { b: 2 } },
    ]);
    expect(query.sql).toBe(
      'BEGIN TRANSACTION;\nUPSERT ONLY $id0 MERGE $content0;UPSERT ONLY $id1 MERGE $content1;\nCOMMIT TRANSACTION;'
    );
    expect(vars).toEqual({ id0: 'thing:1', content0: { a: 1 }, id1: 'thing:2', content1: { b: 2 } });
  });
});
