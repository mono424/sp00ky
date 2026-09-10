import { describe, expect, it } from 'vitest';
import { columnsFor, defaultEnv, listRefTable } from './env';
import { emptyState } from '../state/client-state';

const schema = { tables: [{ name: 'thing', columns: { a: { type: 'string' } } }] } as any;

describe('SagaEnv', () => {
  it('defaultEnv applies overrides', () => {
    expect(defaultEnv(schema).outboxBatchSize).toBe(50);
    expect(defaultEnv(schema, { outboxBatchSize: 5 }).outboxBatchSize).toBe(5);
  });
  it('listRefTable: per-user, anonymous when enabled, global otherwise', () => {
    const s = emptyState({ tabId: 't' });
    expect(listRefTable(defaultEnv(schema), { ...s, userId: 'user:abc' })).toBe('_00_list_ref_user_abc');
    expect(listRefTable(defaultEnv(schema, { anonLive: true }), s)).toBe('_00_list_ref_anon');
    expect(listRefTable(defaultEnv(schema), s)).toBe('_00_list_ref');
  });
  it('columnsFor: schema table, framework table, unknown', () => {
    expect(columnsFor(defaultEnv(schema), 'thing')).toEqual({ a: { type: 'string' } });
    expect(columnsFor(defaultEnv(schema), '_00_feature_flag')).toEqual({});
    expect(columnsFor(defaultEnv(schema), 'nope')).toBeNull();
  });
});
