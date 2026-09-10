import type { QueryPlan } from '@spooky-sync/query-builder';
import { RecordId } from 'surrealdb';
import type { QueryHash, QueryTimeToLive } from '../types';
import type { Saga } from '../kernel/saga';
import type { RegisterResult, StatementResult } from '../kernel/effects';
import { fx } from '../kernel/effects';
import { AUTH_READY_MAX_ATTEMPTS, AUTH_READY_RETRY_MS, REGISTER_MAX_RETRIES, backoffMs } from '../kernel/constants';
import type { ClientState, QueryEntry } from '../state/client-state';
import { emptyTelemetry } from '../state/client-state';
import { seedLifecycle } from '../state/lifecycle';
import * as R from '../state/reducers';
import { desiredRegistrations } from '../state/selectors';
import { parseDuration } from '../utils/index';
import type { SagaEnv } from './env';
import { listRefTable } from './env';
import { queryHashInput, viewKeyInput } from './hash';
import { isResolvedBefore, parseViewRow, snapshotFromSingle } from './membership';
import { applyMembership, applySubqueryChildren } from './membership.saga';
import * as sql from './sql';

export interface RegisterInput {
  tableName: string;
  surql: string;
  params: Record<string, unknown>;
  ttl: QueryTimeToLive;
  plan?: QueryPlan;
}

/**
 * Register a query locally: compute its keys, read the durable `_00_view`
 * row, build the SSP local view and publish the entry. Returns as soon as the
 * entry exists (the first paint comes from `materialize`); the remote
 * registration is dispatched, never awaited.
 */
export function* registerLocal(env: SagaEnv, input: RegisterInput): Saga<QueryHash> {
  const sessionId = (yield fx.state.read((s) => s.sessionId)) as string | null;
  const hash = (yield fx.hash(queryHashInput(input, sessionId))) as string;
  const status = (yield fx.state.read((s) => (s.queries.has(hash) ? 'active' : s.registering.has(hash) ? 'pending' : 'new'))) as
    | 'active'
    | 'pending'
    | 'new';
  if (status === 'active') return hash;
  if (status === 'pending') {
    yield fx.state.wait((s) => !s.registering.has(hash));
    return hash;
  }
  yield fx.state.update(R.beginRegistering(hash));
  try {
    const viewKey = (yield fx.hash(viewKeyInput(input))) as string;
    let viewRow: unknown = null;
    try {
      viewRow = yield fx.local.getById(sql.VIEW_TABLE, sql.viewRecordId(viewKey));
    } catch {
      viewRow = null;
    }
    const view = parseViewRow(viewRow);
    const now = (yield fx.now()) as number;
    const reg = (yield fx.ssp.register({
      queryHash: hash,
      surql: input.surql,
      params: input.params,
      ttl: input.ttl,
      tableName: input.tableName,
    })) as RegisterResult;
    const entry: QueryEntry = {
      def: {
        id: new RecordId('_00_query', hash),
        hash,
        viewKey,
        surql: input.surql,
        params: input.params,
        plan: input.plan,
        ttl: input.ttl,
        ttlMs: parseDuration(input.ttl),
        tableName: input.tableName,
        createdAt: now,
      },
      lifecycle: seedLifecycle(isResolvedBefore(view)),
      remoteArray: view?.ids ?? [],
      localArray: reg.localArray,
      subqueryRemoteArray: [],
      records: [],
      serverState: null,
      subscribers: 0,
      lastSubscriberLeftAt: now,
      lastHeartbeatAt: null,
      lastPolledAt: null,
      registerAttempts: 0,
      telemetry: { ...emptyTelemetry(), registrationTimings: reg.timings },
    };
    yield fx.state.update(R.putQuery(entry));
    yield fx.dispatch({ type: 'EnsureRegistered' });
    return hash;
  } finally {
    yield fx.state.update(R.endRegistering(hash));
  }
}

/**
 * Make the remote match the desired set: start a registration for every
 * query that has none. Idempotent. With `requireAuth` (after a reconnect or
 * while degraded) it first waits for `$auth.id` to be visible on the socket:
 * a register issued before that stamps the view with an empty identity.
 */
export function* ensureRegistered(env: SagaEnv, opts: { requireAuth?: boolean; attempt?: number } = {}): Saga<void> {
  const [hashes, userId] = (yield fx.state.read((s) => [desiredRegistrations(s), s.userId])) as [QueryHash[], string | null];
  if (hashes.length === 0) return;
  if (opts.requireAuth && userId !== null) {
    let authed = false;
    try {
      const res = (yield fx.remote.query('RETURN $auth.id', undefined, env.remoteTimeoutMs)) as StatementResult[];
      authed = res[0]?.status === 'OK' && res[0].result != null;
    } catch {
      authed = false;
    }
    if (!authed) {
      const attempt = (opts.attempt ?? 0) + 1;
      if (attempt >= AUTH_READY_MAX_ATTEMPTS) {
        yield fx.emit({ type: 'log', level: 'warn', message: 'auth identity never became visible; not registering', data: { attempt } });
        return;
      }
      yield fx.timer.set('ensure-registered', AUTH_READY_RETRY_MS, { type: 'EnsureRegistered', requireAuth: true, attempt });
      return;
    }
  }
  for (const hash of hashes) yield fx.dispatch({ type: 'RegisterRemote', hash });
}

const stmt = (results: StatementResult[], i: number): StatementResult | undefined => results[i];

/**
 * One remote registration: `fn::query::register` plus the edge/meta/children
 * read in ONE request, then membership application. Concurrent with every
 * other registration; retried on its own backoff; stops when the entry is
 * gone.
 */
export function* registerRemote(env: SagaEnv, hash: QueryHash, retry = false): Saga<void> {
  const state = (yield fx.state.read((s) => s)) as ClientState;
  const entry = state.queries.get(hash);
  if (!entry) return;
  const { remote } = entry.lifecycle;
  if (remote === 'registered' || remote === 'failed' || (remote === 'registering' && !retry)) return;
  yield fx.state.update(
    R.compose(R.applyLifecycle(hash, { type: 'remote-registering' }), R.applyLifecycle(hash, { type: 'fetch-begin' }))
  );
  try {
    const table = listRefTable(env, state);
    const results = (yield fx.remote.query(
      sql.registerSelect(table),
      sql.registerVars({ id: entry.def.id, surql: entry.def.surql, params: { ...entry.def.params }, ttl: entry.def.ttl }),
      env.remoteTimeoutMs
    )) as StatementResult[];
    const stillThere = (yield fx.state.read((s) => s.queries.has(hash))) as boolean;
    if (!stillThere) return;
    const register = stmt(results, 0);
    if (!register || register.status === 'ERR') throw new Error(register?.status === 'ERR' ? register.error : 'register returned nothing');
    const edges = stmt(results, 1);
    const meta = stmt(results, 2);
    const children = stmt(results, 3);
    const snap = snapshotFromSingle(
      edges?.status === 'OK' ? (edges.result as never) : null,
      meta?.status === 'OK' ? (meta.result as never) : null,
      children?.status === 'OK' ? (children.result as never) : null
    );
    if (!snap) throw new Error(edges?.status === 'ERR' ? edges.error : 'edge read returned no array');
    const outcome = yield* applyMembership(hash, snap.primary, snap.meta);
    if (outcome === 'ignored' && snap.meta.present && snap.meta.state === 'materializing') {
      yield fx.state.update(R.compose(R.markMembershipDirty([hash]), R.setMembershipReread(hash, 1)));
      yield fx.timer.set('membership', 150, { type: 'ReadDirtyMembership' });
    }
    yield* applySubqueryChildren(hash, snap.subquery);
    yield fx.state.update(R.compose(R.applyLifecycle(hash, { type: 'remote-registered' }), R.resetRegisterAttempts(hash)));
    yield fx.dispatch({ type: 'SyncOutcome', ok: true });
  } catch (error) {
    const current = (yield fx.state.read((s) => s.queries.get(hash))) as QueryEntry | undefined;
    if (!current) return;
    const attempts = current.registerAttempts + 1;
    yield fx.state.update(R.bumpRegisterAttempts(hash));
    yield fx.emit({ type: 'log', level: 'warn', message: 'remote registration failed', data: { hash, attempts, error } });
    yield fx.dispatch({ type: 'SyncOutcome', ok: false, error });
    if (attempts >= REGISTER_MAX_RETRIES) {
      yield fx.state.update(R.applyLifecycle(hash, { type: 'remote-failed' }));
    } else {
      yield fx.timer.set(`register:${hash}`, backoffMs(attempts), { type: 'RegisterRemote', hash, retry: true });
    }
  } finally {
    const stillThere = (yield fx.state.read((s) => s.queries.has(hash))) as boolean;
    if (stillThere) yield fx.state.update(R.applyLifecycle(hash, { type: 'fetch-end' }));
  }
}
