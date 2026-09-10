import type { SchemaStructure } from '@spooky-sync/query-builder';
import type { RunOptions } from '../types';

/**
 * Build the outbox job record for a backend route and resolve its table.
 * Every job is a single execution; recurring work is declared server-side.
 */
export function buildJobRecord(
  schema: SchemaStructure,
  backend: string,
  path: string,
  data: Record<string, unknown>,
  options?: RunOptions
): { tableName: string; record: Record<string, unknown> } {
  const backends = (schema as { backends?: Record<string, { outboxTable?: string; routes?: Record<string, { args: Record<string, { optional: boolean }> }> }> }).backends;
  const route = backends?.[backend]?.routes?.[path];
  if (!route) throw new Error(`Route ${backend}.${path} not found`);
  const tableName = backends?.[backend]?.outboxTable;
  if (!tableName) throw new Error(`Outbox table for backend ${backend} not found`);

  const payload: Record<string, unknown> = {};
  for (const argName of Object.keys(route.args)) {
    const arg = route.args[argName];
    if (data[argName] === undefined && arg.optional === false) throw new Error(`Missing required argument ${argName}`);
    payload[argName] = data[argName];
  }

  const record: Record<string, unknown> = {
    path,
    payload: JSON.stringify(payload),
    // Explicit: the schema's server-side DEFAULT does not apply to an
    // optimistic local create, and in-flight indicators key on `pending`.
    status: 'pending',
    max_retries: options?.max_retries ?? 3,
    retry_strategy: options?.retry_strategy ?? 'linear',
  };
  if (options?.timeout != null) record.timeout = options.timeout;
  if (options?.delay != null) record.delay = options.delay;
  if (options?.assignedTo) record.assigned_to = options.assignedTo;
  return { tableName, record };
}
