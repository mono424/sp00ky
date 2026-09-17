import '../kernel/constants.dart';
import '../kernel/effects.dart';
import '../kernel/events.dart';
import '../kernel/saga.dart';
import '../modules/query_builder.dart' show RelationPlan;
import '../state/client_state.dart';
import '../state/lifecycle.dart';
import '../state/reducers.dart' as r;
import '../state/selectors.dart' show desiredRegistrations;
import '../surreal/value.dart';
import '../types.dart';
import '../utils/duration_utils.dart';
import 'env.dart';
import 'hash.dart';
import 'membership.dart';
import 'membership_saga.dart';
import 'sql.dart' as sql;

class RegisterInput {
  const RegisterInput({
    required this.tableName,
    required this.surql,
    required this.params,
    required this.ttl,
    this.hasExplicitOrder = false,
    this.relations = const [],
  });

  final String tableName;
  final String surql;
  final Map<String, dynamic> params;
  final QueryTimeToLive ttl;
  final bool hasExplicitOrder;
  final List<RelationPlan> relations;
}

/// Register a query locally: compute its keys, read the durable `_00_view`
/// row, build the SSP local view and publish the entry. Returns as soon as the
/// entry exists (the first paint comes from `materialize`); the remote
/// registration is dispatched, never awaited.
Future<QueryHash> registerLocal(
    Ctx ctx, SagaEnv env, RegisterInput input) async {
  final key = QueryKeyInput(surql: input.surql, params: input.params);
  final sessionId = await ctx(Fx.stateRead((s) => s.sessionId));
  final hash = await ctx(Fx.hash(queryHashInput(key, sessionId)));
  final status = await ctx(Fx.stateRead((s) => s.queries.containsKey(hash)
      ? 'active'
      : s.registering.contains(hash)
          ? 'pending'
          : 'new'));
  if (status == 'active') return hash;
  if (status == 'pending') {
    await ctx(Fx.stateWait((s) => !s.registering.contains(hash)));
    return hash;
  }
  await ctx(Fx.stateUpdate(r.beginRegistering(hash)));
  try {
    final viewKey = await ctx(Fx.hash(viewKeyInput(key)));
    Map<String, dynamic>? viewRow;
    try {
      viewRow = await ctx(Fx.localGet(sql.viewTable, sql.viewRecordId(viewKey)));
    } catch (_) {
      viewRow = null;
    }
    final view = parseViewRow(viewRow);
    final now = await ctx(Fx.now());
    final reg = await ctx(Fx.sspRegister(RegisterPlan(
      queryHash: hash,
      surql: input.surql,
      params: input.params,
      ttl: input.ttl,
      tableName: input.tableName,
    )));
    final entry = QueryEntry(
      def: QueryDefinition(
        id: RecordId('_00_query', hash),
        hash: hash,
        viewKey: viewKey,
        surql: input.surql,
        params: input.params,
        ttl: input.ttl,
        ttlMs: parseDuration(input.ttl),
        tableName: input.tableName,
        createdAt: now,
        relations: input.relations,
        hasExplicitOrder: input.hasExplicitOrder,
      ),
      lifecycle: seedLifecycle(isResolvedBefore(view)),
      remoteArray: view?.ids ?? const [],
      localArray: reg.localArray,
      subqueryRemoteArray: const [],
      records: const [],
      serverState: null,
      subscribers: 0,
      lastSubscriberLeftAt: now,
      lastHeartbeatAt: null,
      lastPolledAt: null,
      registerAttempts: 0,
      telemetry: emptyTelemetry().copyWith(registrationTimings: reg.timings),
    );
    await ctx(Fx.stateUpdate(r.putQuery(entry)));
    await ctx(Fx.dispatch(const EnsureRegistered()));
    return hash;
  } finally {
    await ctx(Fx.stateUpdate(r.endRegistering(hash)));
  }
}

/// Make the remote match the desired set: start a registration for every query
/// that has none. Idempotent. With [requireAuth] (after a reconnect or while
/// degraded) it first waits for `$auth.id` to be visible on the socket: a
/// register issued before that stamps the view with an empty identity.
Future<void> ensureRegistered(
  Ctx ctx,
  SagaEnv env, {
  bool requireAuth = false,
  int attempt = 0,
}) async {
  final hashes = await ctx(Fx.stateRead(desiredRegistrations));
  final userId = await ctx(Fx.stateRead((s) => s.userId));
  if (hashes.isEmpty) return;
  if (requireAuth && userId != null) {
    var authed = false;
    try {
      final res = await ctx(Fx.remoteQuery('RETURN \$auth.id',
          timeoutMs: env.remoteTimeoutMs));
      authed = res.isNotEmpty && res.first.isOk && res.first.result != null;
    } catch (_) {
      authed = false;
    }
    if (!authed) {
      final next = attempt + 1;
      if (next >= authReadyMaxAttempts) {
        await ctx(Fx.log(LogLevel.warn,
            'auth identity never became visible; not registering',
            {'attempt': next}));
        return;
      }
      await ctx(Fx.timerSet('ensure-registered', authReadyRetryMs,
          EnsureRegistered(requireAuth: true, attempt: next)));
      return;
    }
  }
  for (final hash in hashes) {
    await ctx(Fx.dispatch(RegisterRemote(hash)));
  }
}

/// One remote registration: `fn::query::register` plus the edge/meta/children
/// read in ONE request, then membership application. Concurrent with every
/// other registration; retried on its own backoff; stops when the entry is
/// gone.
Future<void> registerRemote(Ctx ctx, SagaEnv env, QueryHash hash,
    {bool retry = false}) async {
  final state = await ctx(Fx.stateRead((s) => s));
  final entry = state.queries[hash];
  if (entry == null) return;
  final remote = entry.lifecycle.remote;
  if (remote == RemotePhase.registered ||
      remote == RemotePhase.failed ||
      (remote == RemotePhase.registering && !retry)) {
    return;
  }
  await ctx(Fx.stateUpdate(r.compose([
    r.applyLifecycle(hash, const RemoteRegisteringEvent()),
    r.applyLifecycle(hash, const FetchBeginEvent()),
  ])));
  try {
    final table = listRefTable(env, state);
    final results = await ctx(Fx.remoteQuery(
      sql.registerSelect(table),
      vars: sql.registerVars(sql.RegisterPayload(
        id: entry.def.id,
        surql: entry.def.surql,
        params: {...entry.def.params},
        ttl: entry.def.ttl,
      )),
      timeoutMs: env.remoteTimeoutMs,
    ));
    final stillThere = await ctx(Fx.stateRead((s) => s.queries.containsKey(hash)));
    if (!stillThere) return;
    final register = sql.stmt(results, 0);
    if (register == null || !register.isOk) {
      throw StateError(register?.error ?? 'register returned nothing');
    }
    Object? answer(StatementResult? res) =>
        res != null && res.isOk ? res.result : statementFailed;
    final snap = snapshotFromSingle(
      answer(sql.stmt(results, 1)),
      answer(sql.stmt(results, 2)),
      answer(sql.stmt(results, 3)),
    );
    if (snap == null) {
      // Registered, but the read-back did not answer. That says nothing about
      // the row, so read membership again rather than guess: a guess of
      // "gone" re-registered the query, and under load the next read-back
      // failed the same way.
      await ctx(Fx.stateUpdate(r.compose([
        r.applyLifecycle(hash, const RemoteRegisteredEvent()),
        r.resetRegisterAttempts(hash),
      ])));
      await ctx(Fx.log(LogLevel.debug,
          'registration read-back did not answer; re-reading membership',
          {'hash': hash}));
      await markMembershipDirty(ctx, [hash]);
      await ctx(Fx.dispatch(
          const SyncOutcome(false, 'registration read-back failed')));
      return;
    }
    final outcome =
        await applyMembership(ctx, hash, snap.primary, meta: snap.meta);
    if (outcome == MembershipOutcome.ignored &&
        snap.meta.present &&
        snap.meta.state == 'materializing') {
      await ctx(Fx.stateUpdate(r.compose([
        r.markMembershipDirty([hash]),
        r.setMembershipReread(hash, 1),
      ])));
      await ctx(Fx.timerSet('membership', 150, const ReadDirtyMembership()));
    }
    await applySubqueryChildren(ctx, hash, snap.subquery);
    await ctx(Fx.stateUpdate(r.compose([
      r.applyLifecycle(hash, const RemoteRegisteredEvent()),
      r.resetRegisterAttempts(hash),
    ])));
    await ctx(Fx.dispatch(const SyncOutcome(true)));
  } catch (error) {
    final current = await ctx(Fx.stateRead((s) => s.queries[hash]));
    if (current == null) return;
    final attempts = current.registerAttempts + 1;
    await ctx(Fx.stateUpdate(r.bumpRegisterAttempts(hash)));
    await ctx(Fx.log(LogLevel.warn, 'remote registration failed',
        {'hash': hash, 'attempts': attempts, 'error': error}));
    await ctx(Fx.dispatch(SyncOutcome(false, error)));
    if (attempts >= registerMaxRetries) {
      await ctx(Fx.stateUpdate(r.applyLifecycle(hash, const RemoteFailedEvent())));
    } else {
      await ctx(Fx.timerSet('register:$hash', backoffMs(attempts),
          RegisterRemote(hash, retry: true)));
    }
  } finally {
    final stillThere = await ctx(Fx.stateRead((s) => s.queries.containsKey(hash)));
    if (stillThere) {
      await ctx(Fx.stateUpdate(r.applyLifecycle(hash, const FetchEndEvent())));
    }
  }
}
