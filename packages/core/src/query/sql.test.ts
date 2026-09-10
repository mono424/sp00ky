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
    expect(sql.viewRow([['t:1', 1]], true, 5)).toEqual({ ids: [['t:1', 1]], confirmed: true, updatedAt: 5 });
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
