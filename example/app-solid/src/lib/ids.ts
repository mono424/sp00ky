import { RecordId } from '@spooky-sync/client-solid';

/**
 * `table:key` for a record id or its string form. Not `RecordId.toString()`:
 * the SDK brackets a key that starts with a digit (`user:⟨5ffa…⟩`), which then
 * never equals the plain form, and parsed back keeps the brackets in the key.
 */
export const idString = (v: unknown): string =>
  typeof v === 'string' ? v : v instanceof RecordId ? `${v.table.name}:${v.id}` : v ? String(v) : '';

/** The `RecordId` of a `table:key` string, for binding in raw SurrealQL. */
export const toRecordId = (id: string): RecordId => {
  const colon = id.indexOf(':');
  return new RecordId(id.slice(0, colon), id.slice(colon + 1));
};
