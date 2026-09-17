import { createEffect, createSignal, onCleanup, type Accessor } from 'solid-js';
import { useDb } from './context';
import type { ImpersonationInfo } from '@spooky-sync/core';

export interface UseImpersonationOptions {
  /**
   * This component renders the app's own impersonation banner. Required with
   * `impersonationBanner: { mode: 'custom' }`: it acknowledges the banner, so
   * the client does not fall back to its built-in one.
   */
  rendersBanner?: boolean;
}

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
export function useImpersonation(options: UseImpersonationOptions = {}): UseImpersonation {
  const db = useDb();
  const [impersonation, setImpersonation] = createSignal<ImpersonationInfo | null>(
    db.auth.impersonation
  );
  onCleanup(db.auth.subscribeImpersonation(setImpersonation));
  // Re-acknowledged per session: a new impersonation restarts the client's
  // fallback timer, and this component is what cancels it.
  createEffect(() => {
    if (options.rendersBanner && impersonation()) db.acknowledgeImpersonationBanner();
  });
  return {
    impersonation,
    isImpersonating: () => impersonation() !== null,
    stop: () => db.auth.stopImpersonating(),
  };
}
