/**
 * Outbox mutation ids. Previously `_00_pending_mutations:${Date.now()}`, which
 * (a) collides when two mutations land in the same millisecond, a real case
 * once multiple tabs share one store, and (b) never actually ordered the
 * drain, because `loadFromDatabase` sorted by a `created_at` field that does
 * not exist on the table. The new shape is sortable AND collision-free:
 *
 *   <13-digit zero-padded ms timestamp>_<4-digit base36 seq>_<tabId>
 *
 * Lexicographic order == chronological order (per tab; cross-tab within-ms
 * ordering is arbitrary, which matches reality), so `ORDER BY id` in the
 * drain is now meaningful. The tabId suffix also routes rollbacks back to the
 * tab that owns the mutation in shared-tabs mode.
 */

let seq = 0;

/** Tab identity for mutation ids in SOLO mode, minted once per session. In
 *  shared-tabs mode the coordinator's tabId is passed in instead. */
const FALLBACK_TAB_ID = Math.random().toString(36).slice(2, 8);

export function mintMutationId(tabId?: string): string {
  const ts = Date.now().toString().padStart(13, '0');
  const n = (seq = (seq + 1) % 36 ** 4).toString(36).padStart(4, '0');
  return `_00_pending_mutations:${ts}_${n}_${tabId ?? FALLBACK_TAB_ID}`;
}

/** The tab that created the mutation, or null for a legacy numeric id. */
export function mutationOwnerTabId(mutationId: string): string | null {
  const raw = mutationId.startsWith('_00_pending_mutations:')
    ? mutationId.slice('_00_pending_mutations:'.length)
    : mutationId;
  const parts = raw.split('_');
  return parts.length >= 3 ? parts[parts.length - 1] : null;
}
