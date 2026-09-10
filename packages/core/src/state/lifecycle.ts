import type { QueryStatus } from '../types';

/**
 * The query lifecycle as one explicit machine.
 *
 * `phase` answers "where do this query's rows come from":
 * - `cold`      never resolved on this device: rows come from a local
 *               predicate scan (or the SSP local window), bindings show a loader.
 * - `cached`    a durable `_00_view` row was found: rows come from that id-set.
 * - `live`      a server membership set was accepted this session.
 * - `view-lost` the server's `_00_query` row vanished while we held
 *               membership; rows are kept, a re-registration is under way.
 *
 * `remote` tracks the server-side registration, `fetchDepth` is the refcount
 * behind `status: 'fetching'`, `notified` says whether subscribers received
 * at least one materialization this registration.
 */
export type QueryPhase = 'cold' | 'cached' | 'live' | 'view-lost';
export type RemotePhase = 'unregistered' | 'registering' | 'registered' | 'failed';

export interface QueryLifecycle {
  readonly phase: QueryPhase;
  readonly remote: RemotePhase;
  readonly fetchDepth: number;
  readonly notified: boolean;
}

export type LifecycleEvent =
  | { type: 'seed'; resolvedBefore: boolean }
  | { type: 'membership-applied'; present: boolean }
  | { type: 'row-missing' }
  | { type: 'remote-registering' }
  | { type: 'remote-registered' }
  | { type: 'remote-failed' }
  | { type: 'remote-dropped' }
  | { type: 'fetch-begin' }
  | { type: 'fetch-end' }
  | { type: 'notified' }
  | { type: 'bucket-switch'; resolvedBefore: boolean };

export class LifecycleError extends Error {
  constructor(lifecycle: QueryLifecycle, event: LifecycleEvent) {
    super(`Impossible lifecycle transition: ${lifecycle.phase}/${lifecycle.remote} + ${event.type}`);
    this.name = 'LifecycleError';
  }
}

export function seedLifecycle(resolvedBefore: boolean): QueryLifecycle {
  return { phase: resolvedBefore ? 'cached' : 'cold', remote: 'unregistered', fetchDepth: 0, notified: false };
}

/** Total on every (lifecycle, event) pair the sagas can produce; throws on the rest. */
export function transition(l: QueryLifecycle, ev: LifecycleEvent): QueryLifecycle {
  switch (ev.type) {
    case 'seed':
      return seedLifecycle(ev.resolvedBefore);
    case 'membership-applied':
      if (l.phase === 'view-lost' && !ev.present) throw new LifecycleError(l, ev);
      return { ...l, phase: 'live' };
    case 'row-missing':
      // A cold query holds nothing the server could have lost: the outcome
      // is `ignored` upstream and the phase does not move.
      return l.phase === 'cold' ? l : { ...l, phase: 'view-lost' };
    case 'remote-registering':
      return { ...l, remote: 'registering' };
    case 'remote-registered':
      return { ...l, remote: 'registered' };
    case 'remote-failed':
      return { ...l, remote: 'failed' };
    case 'remote-dropped':
      return { ...l, remote: 'unregistered', notified: false };
    case 'fetch-begin':
      return { ...l, fetchDepth: l.fetchDepth + 1 };
    case 'fetch-end':
      return l.fetchDepth === 0 ? l : { ...l, fetchDepth: l.fetchDepth - 1 };
    case 'notified':
      return l.notified ? l : { ...l, notified: true };
    case 'bucket-switch':
      return { ...seedLifecycle(ev.resolvedBefore), fetchDepth: 0 };
  }
}

export const isAuthoritative = (l: QueryLifecycle): boolean => l.phase !== 'cold';
export const hasServerMembership = (l: QueryLifecycle): boolean =>
  l.phase === 'live' || l.phase === 'view-lost';
export const deriveStatus = (l: QueryLifecycle): QueryStatus => (l.fetchDepth > 0 ? 'fetching' : 'idle');
