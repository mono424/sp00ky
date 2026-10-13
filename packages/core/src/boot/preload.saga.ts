import type { QueryHash } from '../types';
import type { Saga } from '../kernel/saga';
import { fx } from '../kernel/effects';
import { settleFailed, settled } from '../state/selectors';
import type { SagaEnv } from '../query/env';
import { receivesMembership } from '../query/env';
import type { RegisterInput } from '../query/register.saga';
import { registerLocal } from '../query/register.saga';

export class PreloadFailedError extends Error {
  constructor(public readonly hash: QueryHash) {
    super(`Preload could not settle: the registration of ${hash} failed`);
    this.name = 'PreloadFailedError';
  }
}

/**
 * Preload = a registered query nobody subscribes to. Resolved before on this
 * device: returns as soon as the entry exists (its rows paint from cache).
 * Never resolved: blocks until the server's membership and every body are
 * local and the first materialization ran. The entry is evicted like any
 * other query a ttl after it was last watched (or registered).
 *
 * A session that cannot be sent membership (signed out, without anonymous
 * live queries) does not wait either: a view with rows would never settle,
 * and an app that gates its first paint on a preload showed its loading
 * screen forever, signed-out landing page included.
 */
export function* preload(env: SagaEnv, input: RegisterInput): Saga<{ hash: QueryHash; waited: boolean }> {
  const hash = yield* registerLocal(env, input);
  const [phase, reachable] = (yield fx.state.read((s) => [s.queries.get(hash)?.lifecycle.phase, receivesMembership(env, s)])) as [
    string | undefined,
    boolean,
  ];
  if (phase !== 'cold' || !reachable) return { hash, waited: false };
  yield fx.state.wait((s) => settled(s, hash) || settleFailed(s, hash));
  const failed = (yield fx.state.read((s) => settleFailed(s, hash))) as boolean;
  if (failed) throw new PreloadFailedError(hash);
  return { hash, waited: true };
}
