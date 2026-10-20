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
 *
 * `answered` says subscribers have been handed the rows of a known membership
 * (the server's, or the durable one of an earlier session) at least once:
 * a materialization ran past `cold`, with no body of a server set still to
 * download, and the membership it drew was still the one in state when it
 * committed. Only a re-seed (a new entry, a bucket switch) clears it, so a
 * reconnect or a lost view does not send a painted query back to loading.
 * It is what the bindings' `isAuthoritative()` mirrors (see `isAnswered`):
 * a `cached` seed or a freshly applied server set alone is not enough,
 * because `records` is still empty (or stale) until `materialize` paints, and
 * "known and empty" read in that beat made apps create rows that existed.
 *
 * `backgroundDepth` is the part of `fetchDepth` that re-verifies rows the
 * query already shows: a registration of a query that is not `cold`. It still
 * holds `status: 'fetching'` (results are not yet confirmed), but it is not
 * traffic the user waits on, so the activity indicator leaves it out.
 */
export type QueryPhase = 'cold' | 'cached' | 'live' | 'view-lost';
export type RemotePhase = 'unregistered' | 'registering' | 'registered' | 'failed';

export interface QueryLifecycle {
  readonly phase: QueryPhase;
  readonly remote: RemotePhase;
  readonly fetchDepth: number;
  readonly backgroundDepth: number;
  readonly notified: boolean;
  readonly answered: boolean;
}

export type LifecycleEvent =
  | { type: 'seed'; resolvedBefore: boolean }
  | { type: 'membership-applied'; present: boolean }
  | { type: 'row-missing' }
  | { type: 'remote-registering' }
  | { type: 'remote-registered' }
  | { type: 'remote-failed' }
  | { type: 'remote-dropped' }
  | { type: 'fetch-begin'; background?: boolean }
  | { type: 'fetch-end'; background?: boolean }
  | { type: 'notified'; answered?: boolean }
  | { type: 'bucket-switch'; resolvedBefore: boolean };

export class LifecycleError extends Error {
  constructor(lifecycle: QueryLifecycle, event: LifecycleEvent) {
    super(`Impossible lifecycle transition: ${lifecycle.phase}/${lifecycle.remote} + ${event.type}`);
    this.name = 'LifecycleError';
  }
}

export function seedLifecycle(resolvedBefore: boolean): QueryLifecycle {
  return {
    phase: resolvedBefore ? 'cached' : 'cold',
    remote: 'unregistered',
    fetchDepth: 0,
    backgroundDepth: 0,
    notified: false,
    answered: false,
  };
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
      return { ...l, fetchDepth: l.fetchDepth + 1, backgroundDepth: l.backgroundDepth + (ev.background ? 1 : 0) };
    case 'fetch-end': {
      if (l.fetchDepth === 0) return l;
      const fetchDepth = l.fetchDepth - 1;
      // Never more background cycles than cycles: an unbalanced end clamps.
      const backgroundDepth = Math.min(fetchDepth, l.backgroundDepth - (ev.background && l.backgroundDepth > 0 ? 1 : 0));
      return { ...l, fetchDepth, backgroundDepth };
    }
    case 'notified': {
      // A cold render is a guess from the local store, never an answer.
      const answered = l.answered || (ev.answered === true && l.phase !== 'cold');
      return l.notified && answered === l.answered ? l : { ...l, notified: true, answered };
    }
    case 'bucket-switch':
      return { ...seedLifecycle(ev.resolvedBefore), fetchDepth: 0 };
  }
}

/** Membership is known (server set or durable seed); its rows may not be painted yet. */
export const isAuthoritative = (l: QueryLifecycle): boolean => l.phase !== 'cold';
/**
 * Subscribers hold the rows of a known membership: what the authority channel
 * (`subscribeQueryAuthority`, the bindings' `isAuthoritative()`) reports.
 */
export const isAnswered = (l: QueryLifecycle): boolean => l.answered;
export const hasServerMembership = (l: QueryLifecycle): boolean =>
  l.phase === 'live' || l.phase === 'view-lost';
export const deriveStatus = (l: QueryLifecycle): QueryStatus => (l.fetchDepth > 0 ? 'fetching' : 'idle');
/** A fetch cycle the user is waiting on: rows to download, or a first registration. */
export const isForegroundFetching = (l: QueryLifecycle): boolean => l.fetchDepth > l.backgroundDepth;
