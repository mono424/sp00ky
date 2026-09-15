import { RecordId } from 'surrealdb';

export function toRecordId(full: string): RecordId {
  const s = String(full);
  const i = s.indexOf(':');
  return i === -1 ? new RecordId(s, '') : new RecordId(s.slice(0, i), s.slice(i + 1));
}
