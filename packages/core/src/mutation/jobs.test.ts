import { describe, expect, it } from 'vitest';
import { buildJobRecord } from './jobs';

const schema = {
  tables: [],
  backends: {
    api: { outboxTable: '_00_job_api', routes: { hello: { args: { name: { optional: false }, tone: { optional: true } } } } },
    bare: { routes: { x: { args: {} } } },
  },
} as any;

describe('buildJobRecord', () => {
  it('builds a pending job row with the route payload and option overrides', () => {
    const { tableName, record } = buildJobRecord(schema, 'api', 'hello', { name: 'a', extra: 1 }, { max_retries: 5, retry_strategy: 'exponential', timeout: 9, delay: 100, assignedTo: 'w1' });
    expect(tableName).toBe('_00_job_api');
    expect(record).toEqual({ path: 'hello', payload: JSON.stringify({ name: 'a', tone: undefined }), status: 'pending', max_retries: 5, retry_strategy: 'exponential', timeout: 9, delay: 100, assigned_to: 'w1' });
    expect(buildJobRecord(schema, 'api', 'hello', { name: 'a' }).record).toMatchObject({ max_retries: 3, retry_strategy: 'linear' });
  });
  it('rejects unknown routes, missing outbox tables and missing required args', () => {
    expect(() => buildJobRecord(schema, 'api', 'nope', {})).toThrow('Route api.nope not found');
    expect(() => buildJobRecord(schema, 'bare', 'x', {})).toThrow('Outbox table for backend bare not found');
    expect(() => buildJobRecord(schema, 'api', 'hello', {})).toThrow('Missing required argument name');
    expect(() => buildJobRecord({ tables: [] } as any, 'api', 'hello', {})).toThrow('Route api.hello not found');
  });
});
