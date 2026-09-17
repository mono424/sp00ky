import type { RecordId } from 'surrealdb';
import type { MembershipOutcome, RecordVersionArray, ServerViewMeta } from '../types';
import type { QueryPhase } from '../state/lifecycle';
import { encodeRecordId } from '../utils/index';

/** The durable `_00_view` row: the last server-confirmed membership. */
export interface DurableView {
  ids: RecordVersionArray;
  confirmed: boolean;
}

export function parseViewRow(row: unknown): DurableView | null {
  if (!row || typeof row !== 'object') return null;
  const ids = (row as { ids?: unknown }).ids;
  if (!Array.isArray(ids)) return null;
  return { ids: ids as RecordVersionArray, confirmed: (row as { confirmed?: unknown }).confirmed === true };
}

/**
 * "Resolved before": the server answered this query on this device at some
 * point. An unconfirmed empty row cannot be told apart from one written before
 * the marker existed, so it does not count.
 */
export const isResolvedBefore = (view: DurableView | null): boolean =>
  view !== null && (view.ids.length > 0 || view.confirmed);

/** The `{ rowCount, state }` object the row-count selects return per row. */
export interface QueryMetaRow {
  id?: RecordId<string>;
  rowCount?: number | null;
  state?: string | null;
}

export function metaFromRow(row: QueryMetaRow | null | undefined): ServerViewMeta {
  if (!row || typeof row !== 'object') return { present: false, rowCount: null, state: null };
  const state = row.state === 'materializing' || row.state === 'ready' ? row.state : null;
  return { present: true, rowCount: typeof row.rowCount === 'number' ? row.rowCount : null, state };
}

/** Collapse duplicate `(id, version)` pairs to one per id, keeping the highest version. */
export function dedupeRecordVersions(pairs: RecordVersionArray): RecordVersionArray {
  if (pairs.length < 2) return pairs;
  const best = new Map<string, number>();
  for (const [id, version] of pairs) {
    const prev = best.get(id);
    if (prev === undefined || version > prev) best.set(id, version);
  }
  if (best.size === pairs.length) return pairs;
  return Array.from(best.entries());
}

export interface MembershipDecisionInput {
  phase: QueryPhase;
  /** Number of ids currently held as membership. */
  held: number;
  remoteArray: RecordVersionArray;
  meta?: ServerViewMeta;
  verifiedRemoval?: boolean;
}

/**
 * Whether a server id-set may be applied. A NON-EMPTY set is always the
 * server's answer. An EMPTY set is believed only when the server stands
 * behind it: the `_00_query` row is present, `ready` (or pre-`state`), and
 * reports zero rows. A missing row while we hold membership is a lost view.
 */
export function decideMembershipOutcome(input: MembershipDecisionInput): MembershipOutcome {
  const { phase, held, remoteArray, meta, verifiedRemoval } = input;
  if (remoteArray.length > 0 || verifiedRemoval) return 'applied';
  if (!meta || !meta.present) {
    return held > 0 || phase !== 'cold' ? 'view-lost' : 'ignored';
  }
  const knownEmpty = meta.rowCount === 0 && meta.state !== 'materializing';
  return knownEmpty ? 'applied' : 'ignored';
}

// ---- list_ref snapshots ------------------------------------------------------

/**
 * A statement that did not answer (an ERR, or no result at all). Kept apart
 * from a `null` result, which is the server saying "no such row": reading a
 * failed statement as a missing row turned every timeout under load into a
 * lost view and re-registered it, over and over (whitepawn, 2026-09-16).
 */
export const FAILED: unique symbol = Symbol('statement failed');
export type Answer<T> = T | null | typeof FAILED;

export interface ListRefEdgeRow {
  in: RecordId<string>;
  out: RecordId<string>;
  version: number;
  parent?: unknown;
}

export interface ListRefSnapshot {
  primary: RecordVersionArray;
  subquery: RecordVersionArray;
  meta: ServerViewMeta;
}

const toPairs = (rows: Array<{ out: RecordId<string>; version: number }> | null | undefined): RecordVersionArray =>
  dedupeRecordVersions(Array.isArray(rows) ? rows.map((r) => [encodeRecordId(r.out), r.version]) : []);

/**
 * Fold the single-query statement batch (edges, meta, children) into a
 * snapshot. `null` when any of the three did not answer: half an answer is
 * not one, and a missing meta would read as a lost view.
 */
export function snapshotFromSingle(
  items: Answer<Array<{ out: RecordId<string>; version: number }>>,
  metaRow: Answer<QueryMetaRow>,
  children: Answer<Array<{ out: RecordId<string>; version: number }>>
): ListRefSnapshot | null {
  if (!Array.isArray(items) || metaRow === FAILED || children === FAILED) return null;
  return { primary: toPairs(items), subquery: toPairs(children), meta: metaFromRow(metaRow) };
}

/**
 * Fold the many-query statement batch into one snapshot per hash. Every hash
 * in `hashById` gets a snapshot; a query whose row did not come back reads
 * `present: false`. Empty when either statement did not answer.
 */
export function snapshotsFromBatch(
  edges: Answer<ListRefEdgeRow[]>,
  counts: Answer<Array<QueryMetaRow | null>>,
  hashById: ReadonlyMap<string, string>
): Map<string, ListRefSnapshot> {
  const out = new Map<string, ListRefSnapshot>();
  if (!Array.isArray(edges) || counts === FAILED) return out;
  for (const hash of hashById.values()) {
    out.set(hash, { primary: [], subquery: [], meta: { present: false, rowCount: null, state: null } });
  }
  for (const row of edges) {
    const hash = hashById.get(encodeRecordId(row.in));
    if (!hash) continue;
    const snapshot = out.get(hash)!;
    const pair: [string, number] = [encodeRecordId(row.out), row.version];
    if (row.parent == null) snapshot.primary.push(pair);
    else snapshot.subquery.push(pair);
  }
  for (const snapshot of out.values()) {
    snapshot.primary = dedupeRecordVersions(snapshot.primary);
    snapshot.subquery = dedupeRecordVersions(snapshot.subquery);
  }
  for (const count of Array.isArray(counts) ? counts : []) {
    if (!count || !count.id) continue;
    const hash = hashById.get(encodeRecordId(count.id));
    if (!hash) continue;
    out.get(hash)!.meta = metaFromRow(count);
  }
  return out;
}

/**
 * A batch that must not be believed at face value: a query we hold rows for
 * whose `_00_query` row did not come back, or no edge at all while the server
 * still reports rows for a held query. Such hashes are re-read one at a time.
 */
export function suspectHashes(
  snapshots: ReadonlyMap<string, ListRefSnapshot>,
  heldCounts: ReadonlyMap<string, number>,
  edgeCount: number
): string[] {
  const out: string[] = [];
  for (const [hash, snapshot] of snapshots) {
    const held = heldCounts.get(hash) ?? 0;
    if (held === 0) continue;
    if (!snapshot.meta.present || (edgeCount === 0 && (snapshot.meta.rowCount ?? 0) > 0)) out.push(hash);
  }
  return out;
}
