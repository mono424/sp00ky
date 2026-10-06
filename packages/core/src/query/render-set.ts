import type { RecordVersionArray } from '../types';
import type { QueryPhase } from '../state/lifecycle';
import type { Overlay } from '../state/selectors';

export interface RenderInput {
  phase: QueryPhase;
  remoteArray: RecordVersionArray;
  localArray: RecordVersionArray;
  isWindow: boolean;
}

/**
 * The id-set a query renders from, or `null` when it must fall back to a
 * local predicate scan (a cold query that was never resolved on this device).
 * A cold window has no usable scan (re-applying `START m` against the shared
 * store returns the wrong rows), so it renders the SSP's local window instead.
 */
export function resolveMembership(input: RenderInput): RecordVersionArray | null {
  if (input.phase !== 'cold') return input.remoteArray;
  if (input.isWindow) return input.localArray;
  return null;
}

/**
 * Whether a body may paint before the server has answered (`cold`). One no
 * durable view vouches for and no optimistic write touches is an orphan:
 * deleted upstream while no view watched it, or dropped from every view since.
 * The predicate scan would otherwise resurrect it until the server answers,
 * and indefinitely offline.
 */
export function paintsCold(id: string, vouched: ReadonlySet<string>, overlay: Overlay): boolean {
  return id.startsWith('_00_') || vouched.has(id) || overlay.writes.has(id);
}

export interface RenderOptions {
  hasExplicitOrder: boolean;
  isWindow: boolean;
}

/**
 *   render = (membership ∪ (writes ∩ localView)) − deletes
 *
 * `writes`/`deletes` are the outbox overlay (pending and acked items). The
 * middle term keeps optimistic writes visible without re-admitting stale rows:
 * `localView` (the SSP's local id-set) says whether the written row matches
 * the query per local truth. Sorted unless the query orders itself or is a
 * window (whose id order is the slice order).
 */
export function buildRenderIds(
  membership: RecordVersionArray,
  localView: RecordVersionArray,
  overlay: Overlay,
  opts: RenderOptions
): string[] {
  const ordered: string[] = [];
  const seen = new Set<string>();
  for (const [id] of membership) {
    if (overlay.deletes.has(id) || seen.has(id)) continue;
    seen.add(id);
    ordered.push(id);
  }
  if (overlay.writes.size > 0) {
    for (const [id] of localView) {
      if (!overlay.writes.has(id) || overlay.deletes.has(id) || seen.has(id)) continue;
      seen.add(id);
      ordered.push(id);
    }
  }
  if (!opts.hasExplicitOrder && !opts.isWindow) ordered.sort();
  return ordered;
}
