import { RecordId } from 'surrealdb';
import type { InlineRow } from '../types';
import { encodeRecordId } from './index';

/**
 * Reading `_00_list_ref` edges as a LIVE notification delivers them. Pure, so
 * both the full client and the wasm-free live feed (`@spooky-sync/core/live`)
 * decode the same wire shape the same way.
 */

/** The hash a `_00_list_ref` edge belongs to: the id part of its `in` (`_00_query:<hash>`). */
export function hashOfEdge(value: unknown): string | null {
  const inId = (value as { in?: unknown } | null)?.in;
  if (!inId) return null;
  const str = inId instanceof RecordId ? String(inId.id) : String(inId).replace(/^_00_query:/, '');
  return str.length > 0 ? str : null;
}

/**
 * The row an edge notification carries, when the subscription joined it on.
 *
 * Without `FETCH out` the edge's `out` is a record id and there is nothing to
 * land, so this returns null and the caller falls back to fetching the body.
 * It also returns null for a row the session may not read (the join yields
 * `out: null`) and for an edge whose target has been deleted.
 */
export function rowOfEdge(value: unknown): InlineRow | null {
  const edge = value as { out?: unknown; version?: unknown } | null;
  const out = edge?.out;
  if (!out || typeof out !== 'object' || Array.isArray(out) || out instanceof RecordId) return null;
  // Must be a real RecordId: the landing path keys off `row.id` being one, and
  // would otherwise record the version for a body it never wrote.
  const rid = (out as { id?: unknown }).id;
  if (!(rid instanceof RecordId)) return null;
  const id = encodeRecordId(rid);
  const version = typeof edge?.version === 'number' ? edge.version : null;
  if (version === null) return null;
  return { id, version, record: out as Record<string, unknown> };
}
