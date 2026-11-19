/**
 * Capability gate for shared-tabs mode. Any missing piece routes the tab to
 * solo mode, which is exactly the pre-shared-tabs behavior (first tab gets
 * OPFS, later tabs fall back to memory and warn via StorageHealth).
 */
import type { Sp00kyConfig } from '../../types';

export type SharedTabsUnsupportedReason =
  | 'flag-off'
  | 'not-browser'
  | 'no-shared-worker'
  | 'no-web-locks'
  | 'no-message-channel'
  | 'engine-not-sqlite';

export type SharedTabsSupport =
  | { supported: true }
  | { supported: false; reason: SharedTabsUnsupportedReason };

export function detectSharedTabsSupport(config: Sp00kyConfig<any>): SharedTabsSupport {
  if (config.sharedTabs !== true) return { supported: false, reason: 'flag-off' };
  // Shared-tabs only makes sense for the engines whose OPFS files are
  // single-holder: SQLite and the row store. The SurrealDB engine and custom
  // engines keep their existing per-tab behavior.
  if (config.localEngine !== 'sqlite' && config.localEngine !== 'rows') {
    return { supported: false, reason: 'engine-not-sqlite' };
  }
  if (typeof window === 'undefined') return { supported: false, reason: 'not-browser' };
  if (typeof SharedWorker === 'undefined') return { supported: false, reason: 'no-shared-worker' };
  if (typeof MessageChannel === 'undefined') {
    return { supported: false, reason: 'no-message-channel' };
  }
  const locks = (navigator as { locks?: LockManager }).locks;
  if (!locks || typeof locks.request !== 'function') {
    return { supported: false, reason: 'no-web-locks' };
  }
  return { supported: true };
}
