import 'boot_saga.dart' show primeCircuit;
import '../kernel/effects.dart';
import '../kernel/events.dart';
import '../kernel/saga.dart';
import '../mutation/push_saga.dart' show loadOutbox;
import '../query/env.dart';
import '../query/hash.dart';
import '../query/membership.dart';
import '../query/sql.dart' as sql;
import '../state/lifecycle.dart';
import '../state/reducers.dart' as r;
import '../surreal/value.dart';
import '../types.dart';

/// Move the client to another principal's local store: swap and rebind.
///
/// Runs on the serial `bucket` lane; a switch that wakes up to a newer
/// `pendingBucket` steps aside. Every active query keeps its hash and is
/// re-seeded from the new store's `_00_view` rows, then re-registered.
Future<void> bucketSwitch(Ctx ctx, SagaEnv env, String target) async {
  final state = await ctx(Fx.stateRead((s) => s));
  final current =
      await ctx(Fx.service<String>(ServiceName.localCurrentBucketId));
  if (state.pendingBucket != target || current == target) return;

  for (final key in const [
    'poll',
    'outbox',
    'membership',
    'fetch',
    'ack-prune'
  ]) {
    await ctx(Fx.timerClear(key));
  }
  await ctx(Fx.stateUpdate(r.clearBucketState()));
  await ctx(Fx.service<void>(ServiceName.crdtCloseAll, [false]));

  await ctx(Fx.service<void>(ServiceName.localSwitchStore, [target]));
  await ctx(Fx.service<void>(ServiceName.migratorProvision));
  await ctx(Fx.service<void>(ServiceName.sspReset));
  await ctx(Fx.service<void>(ServiceName.sspSetPermissions));
  await ctx(Fx.service<void>(ServiceName.sspSetSessionAuth, [
    await ctx(Fx.service<String?>(ServiceName.authSessionAuthId)),
    await ctx(Fx.service<String?>(ServiceName.authAccess)),
  ]));
  await ctx(Fx.stateUpdate(r.setIdentity(bucketId: target)));
  await loadOutbox(ctx, env);
  await primeCircuit(ctx);

  final token = await ctx(Fx.service<String?>(ServiceName.authToken));
  if (token != null) {
    try {
      await ctx(Fx.service<void>(
          ServiceName.persistenceSet, ['sp00ky_auth_token', token]));
    } catch (error) {
      await ctx(Fx.log(LogLevel.warn, 'failed to re-persist the auth token',
          {'error': error}));
    }
  }
  await rebindQueries(ctx);
  await ctx(Fx.dispatch(const EnsureRegistered()));
  await ctx(Fx.dispatch(const LiveStart()));
  // The switch cleared the poll timer, and only the tick itself re-arms it: a
  // sign-in (auth flip -> bucket switch) otherwise left the client running on
  // LIVE alone for the rest of the session, so any membership change LIVE did
  // not deliver was never noticed.
  await ctx(Fx.dispatch(const PollTick()));
  await ctx(Fx.dispatch(const Drain()));
}

/// Re-seed every active query from the new store and rebuild its SSP view.
Future<void> rebindQueries(Ctx ctx) async {
  final entries = await ctx(Fx.stateRead((s) => s.queries.values.toList()));
  final sessionId = await ctx(Fx.stateRead((s) => s.sessionId));
  for (final entry in entries) {
    DurableView? view;
    try {
      view = parseViewRow(await ctx(
          Fx.localGet(sql.viewTable, sql.viewRecordId(entry.def.viewKey))));
    } catch (_) {
      view = null;
    }
    final hash = await ctx(Fx.hash(queryHashInput(
        QueryKeyInput(surql: entry.def.surql, params: entry.def.params),
        sessionId)));
    RecordVersionArray localArray = const [];
    try {
      final reg = await ctx(Fx.sspRegister(RegisterPlan(
        queryHash: entry.def.hash,
        surql: entry.def.surql,
        params: entry.def.params,
        ttl: entry.def.ttl,
        tableName: entry.def.tableName,
      )));
      localArray = reg.localArray;
    } catch (error) {
      await ctx(Fx.log(
          LogLevel.warn,
          'local view rebuild failed after bucket switch',
          {'hash': entry.def.hash, 'error': error}));
    }
    final lifecycle = seedLifecycle(isResolvedBefore(view));
    await ctx(Fx.stateUpdate(r.rebindQuery(
      entry.def.hash,
      id: RecordId('_00_query', hash),
      lifecycle: lifecycle,
      remoteArray: view?.ids ?? const [],
      localArray: localArray,
    )));
    if (isAuthoritative(entry.lifecycle) != isAuthoritative(lifecycle)) {
      await ctx(Fx.emit(
          QueryAuthorityEvent(entry.def.hash, isAuthoritative(lifecycle))));
    }
  }
}
