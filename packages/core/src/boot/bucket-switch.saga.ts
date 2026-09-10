import { RecordId } from 'surrealdb';
import type { Saga } from '../kernel/saga';
import type { RegisterResult } from '../kernel/effects';
import { fx } from '../kernel/effects';
import type { ClientState, QueryEntry } from '../state/client-state';
import { seedLifecycle, isAuthoritative } from '../state/lifecycle';
import * as R from '../state/reducers';
import { ANON_USER_ID } from '../modules/ref-tables';
import type { SagaEnv } from '../query/env';
import { queryHashInput } from '../query/hash';
import { isResolvedBefore, parseViewRow } from '../query/membership';
import * as sql from '../query/sql';
import { loadOutbox } from '../mutation/push.saga';

export interface BucketSwitchOptions {
  clearBlobsOnSignOut: boolean;
}

/**
 * Move the client to another user's local store: drain, swap, rebind.
 * Runs on the serial `bucket` lane; a switch that wakes up to a newer
 * `pendingBucket` steps aside. Every active query keeps its hash and is
 * re-seeded from the new store's `_00_view` rows, then re-registered.
 */
export function* bucketSwitch(env: SagaEnv, target: string, release: (() => void) | null = null, opts: BucketSwitchOptions = { clearBlobsOnSignOut: false }): Saga<void> {
  const state = (yield fx.state.read((s) => s)) as ClientState;
  const current = (yield fx.service('local.currentBucketId')) as string;
  if (state.pendingBucket !== target || current === target) {
    release?.();
    return;
  }
  const gate = release ?? ((yield fx.service('local.beginSwitch')) as () => void);
  for (const key of ['poll', 'outbox', 'membership', 'fetch', 'ack-prune']) yield fx.timer.clear(key);
  yield fx.state.update(R.clearBucketState());
  yield fx.service('crdt.closeAll', false);
  try {
    if (state.tabRole !== 'solo') {
      try {
        const role = (yield fx.service('tabs.moveToBucket', target)) as ClientState['tabRole'];
        yield fx.state.update(R.setTabRole(role));
      } catch (error) {
        yield fx.emit({ type: 'log', level: 'warn', message: 'shared-tabs bucket move failed; switching solo', data: { error } });
        yield fx.state.update(R.setTabRole('solo'));
        yield fx.service('ssp.setPersistence', true);
        yield fx.service('local.switchStore', target);
      }
    } else {
      yield fx.service('local.switchStore', target);
      if ((yield fx.service('local.usesSurqlSchema')) as boolean) yield fx.service('migrator.provision');
    }
    try {
      if (opts.clearBlobsOnSignOut && target === ANON_USER_ID && current !== ANON_USER_ID) yield fx.service('blobs.clear');
      yield fx.service('blobs.setNamespace', target);
    } catch (error) {
      yield fx.emit({ type: 'log', level: 'warn', message: 'blob cache bucket switch failed', data: { error } });
    }
    yield fx.service('ssp.reset');
    yield fx.service('ssp.setPermissions');
    yield fx.service('ssp.setSessionAuth', (yield fx.service('auth.sessionAuthId')) as string | null, (yield fx.service('auth.access')) as string | null);
    yield fx.state.update(R.setIdentity({ bucketId: target }));
    yield fx.dispatch({ type: 'PrimeCircuit' });
  } finally {
    gate();
  }
  const token = (yield fx.service('auth.token')) as string | null;
  if (token) {
    try {
      yield fx.service('persistence.set', 'sp00ky_auth_token', token);
    } catch (error) {
      yield fx.emit({ type: 'log', level: 'warn', message: 'failed to re-persist the auth token', data: { error } });
    }
  }
  yield* rebindQueries();
  yield* loadOutbox(env);
  yield fx.dispatch({ type: 'EnsureRegistered' });
  yield fx.dispatch({ type: 'LiveStart' });
  // The switch cleared the poll timer, and only the tick itself re-arms it: a
  // sign-in (auth flip -> bucket switch) otherwise left the client running on
  // LIVE alone for the rest of the session, so any membership change LIVE did
  // not deliver was never noticed.
  yield fx.dispatch({ type: 'PollTick' });
  yield fx.dispatch({ type: 'Drain' });
}

/** Re-seed every active query from the new store and rebuild its SSP view. */
export function* rebindQueries(): Saga<void> {
  const [entries, sessionId] = (yield fx.state.read((s) => [[...s.queries.values()], s.sessionId])) as [QueryEntry[], string | null];
  for (const entry of entries) {
    let view: ReturnType<typeof parseViewRow> = null;
    try {
      view = parseViewRow(yield fx.local.getById(sql.VIEW_TABLE, sql.viewRecordId(entry.def.viewKey)));
    } catch {
      view = null;
    }
    const hash = (yield fx.hash(queryHashInput({ surql: entry.def.surql, params: entry.def.params as Record<string, unknown> }, sessionId))) as string;
    let localArray: QueryEntry['localArray'] = [];
    try {
      const reg = (yield fx.ssp.register({
        queryHash: entry.def.hash,
        surql: entry.def.surql,
        params: entry.def.params as Record<string, unknown>,
        ttl: entry.def.ttl,
        tableName: entry.def.tableName,
      })) as RegisterResult;
      localArray = reg.localArray;
    } catch (error) {
      yield fx.emit({ type: 'log', level: 'warn', message: 'local view rebuild failed after bucket switch', data: { hash: entry.def.hash, error } });
    }
    const lifecycle = seedLifecycle(isResolvedBefore(view));
    yield fx.state.update(R.rebindQuery(entry.def.hash, { id: new RecordId('_00_query', hash), lifecycle, remoteArray: view?.ids ?? [], localArray }));
    if (isAuthoritative(entry.lifecycle) !== isAuthoritative(lifecycle)) {
      yield fx.emit({ type: 'query:authority', hash: entry.def.hash, known: isAuthoritative(lifecycle) });
    }
  }
}
