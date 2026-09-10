import type { Saga } from '../kernel/saga';
import { fx } from '../kernel/effects';
import type { ClientState } from '../state/client-state';
import * as R from '../state/reducers';
import { ANON_USER_ID } from '../modules/ref-tables';
import type { SagaEnv } from '../query/env';
import * as sql from '../query/sql';
import { loadOutbox } from '../mutation/push.saga';

export interface BootOptions {
  /** A shared-tabs coordinator is available: ask it for a role instead of opening the store alone. */
  sharedTabs: boolean;
}

/**
 * Local boot. Everything awaited here is network-free: the store opens,
 * the schema provisions, the SSP starts, the session is restored from the
 * cached token, the outbox is mirrored. `localReady` flips at the end and
 * the network half runs in the background (`startRemote`).
 */
export function* boot(env: SagaEnv, opts: BootOptions): Saga<void> {
  const bucket = ((yield fx.service('hint.read')) as string | null) ?? ANON_USER_ID;
  let shared = false;
  if (opts.sharedTabs) {
    try {
      const role = (yield fx.service('tabs.start', bucket)) as ClientState['tabRole'];
      yield fx.state.update(R.setTabRole(role));
      shared = true;
    } catch (error) {
      yield fx.emit({ type: 'log', level: 'warn', message: 'shared tabs unavailable; booting solo', data: { error } });
    }
  }
  if (!shared) yield fx.service('local.connect', bucket);
  yield fx.state.update(R.setIdentity({ bucketId: bucket }));
  if ((yield fx.service('local.usesSurqlSchema')) as boolean) yield fx.service('migrator.provision');
  yield* migrateWindowToView();
  yield fx.dispatch({ type: 'WarmBlobs' });
  yield fx.service('ssp.init');
  yield fx.service('ssp.setPermissions');
  yield fx.dispatch({ type: 'PrimeCircuit' });

  const restoredUserId = (yield fx.service('auth.restoreSession')) as string | null;
  const salt = (yield fx.id('salt')) as string;
  const authId = (yield fx.service('auth.sessionAuthId')) as string | null;
  yield fx.state.update(R.setIdentity({ sessionId: salt, userId: restoredUserId, saltUserId: authId }));
  yield fx.service('crdt.setSessionId', salt);
  if (restoredUserId) yield fx.service('ssp.setSessionAuth', authId, (yield fx.service('auth.access')) as string | null);

  yield* loadOutbox(env);
  yield fx.service('window.attach');
  yield fx.service('features.init');
  yield fx.service('releases.init');
  yield fx.dispatch({ type: 'LifecycleTick' });
  yield fx.dispatch({ type: 'GcTick' });
  yield fx.state.update(R.setIdentity({ localReady: true }));
  yield fx.dispatch({ type: 'StartRemote' });
}

/** One-time copy of the legacy `_00_window` rows into `_00_view`. */
export function* migrateWindowToView(): Saga<void> {
  try {
    const countRes = (yield fx.local.query(sql.countViewRows())) as unknown[];
    const countRow = Array.isArray(countRes?.[0]) ? (countRes[0] as Array<{ count?: number }>)[0] : undefined;
    if ((countRow?.count ?? 0) > 0) return;
    const legacy = (yield fx.local.query(sql.readLegacyViewRows())) as unknown[];
    const rows = Array.isArray(legacy?.[0]) ? (legacy[0] as Array<Record<string, unknown>>) : [];
    for (const row of rows) {
      const key = typeof row.id === 'string' ? row.id.slice(row.id.indexOf(':') + 1) : String((row.id as { id?: unknown })?.id ?? '');
      if (!key || !Array.isArray(row.ids)) continue;
      yield fx.local.upsert(sql.VIEW_TABLE, sql.viewRecordId(key), { ids: row.ids, confirmed: row.confirmed === true, updatedAt: row.updatedAt ?? 0 }, 'replace');
    }
  } catch (error) {
    yield fx.emit({ type: 'log', level: 'debug', message: 'window -> view migration skipped', data: { error } });
  }
}

/**
 * The network half of boot: connect, verify the restored session, then let
 * membership, LIVE, the poll and the outbox start. Every step is best-effort.
 */
export function* startRemote(): Saga<void> {
  try {
    yield fx.service('remote.connect');
  } catch (error) {
    yield fx.emit({ type: 'log', level: 'warn', message: 'remote connect failed; running from the local store', data: { error } });
  }
  yield fx.service('supervisor.start');
  try {
    yield fx.service('auth.init');
  } catch (error) {
    yield fx.emit({ type: 'log', level: 'warn', message: 'auth verification failed; keeping the restored session', data: { error } });
  }
  yield fx.dispatch({ type: 'EnsureRegistered' });
  yield fx.dispatch({ type: 'LiveStart' });
  yield fx.dispatch({ type: 'PollTick' });
  yield fx.dispatch({ type: 'Drain' });
}

/** Fill the circuit from the local store; `primed` flips even on failure so fetches never hang. */
export function* primeCircuit(): Saga<void> {
  const pending = (yield fx.state.read((s) => s.outbox.map((i) => i.recordId))) as string[];
  try {
    yield fx.service('ssp.prime', pending);
  } catch (error) {
    yield fx.emit({ type: 'log', level: 'warn', message: 'circuit prime failed; starting empty', data: { error } });
  } finally {
    yield fx.state.update(R.setIdentity({ primed: true }));
  }
}

export function* versionsPrimed(entries: Array<readonly [string, number]>): Saga<void> {
  yield fx.state.update(R.setVersions(entries));
}

/** Hand this tab's views back to the server when the page goes away for good. */
export function* pageHide(): Saga<void> {
  const ids = (yield fx.state.read((s) => [...s.queries.values()].filter((e) => e.lifecycle.remote === 'registered').map((e) => e.def.id))) as unknown[];
  if (ids.length > 0) yield fx.service('remote.releaseViews', ids);
}

export function* warmBlobs(): Saga<void> {
  const bucket = ((yield fx.state.read((s) => s.bucketId)) as string | null) ?? ANON_USER_ID;
  try {
    yield fx.service('blobs.start', bucket);
  } catch (error) {
    yield fx.emit({ type: 'log', level: 'warn', message: 'blob cache failed to start', data: { error } });
  }
}
