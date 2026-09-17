import { createSignal, onCleanup, type Accessor } from 'solid-js';
import { useDb } from './context';
import type { ImpersonationInfo } from '@spooky-sync/core';

export interface UseImpersonation {
  /** The active impersonation, or `null`. */
  impersonation: Accessor<ImpersonationInfo | null>;
  isImpersonating: Accessor<boolean>;
  /** Return to the admin session. */
  stop: () => Promise<void>;
}

/**
 * Observe admin impersonation (started from the DevTools). The client already
 * shows its own warning bar with a Stop button; use this for anything extra,
 * such as hiding actions an admin should not take on someone's behalf.
 * Must be used within a `<Sp00kyProvider>`.
 */
export function useImpersonation(): UseImpersonation {
  const db = useDb();
  const [impersonation, setImpersonation] = createSignal<ImpersonationInfo | null>(
    db.auth.impersonation
  );
  onCleanup(db.auth.subscribeImpersonation(setImpersonation));
  return {
    impersonation,
    isImpersonating: () => impersonation() !== null,
    stop: () => db.auth.stopImpersonating(),
  };
}
