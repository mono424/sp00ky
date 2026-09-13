import '../kernel/effects.dart';
import '../kernel/events.dart';
import '../kernel/saga.dart';
import '../modules/ref_tables.dart' show anonUserId;
import '../mutation/push_saga.dart' show loadOutbox;
import '../query/env.dart';
import '../query/sql.dart' as sql;
import '../state/lifecycle.dart' show RemotePhase;
import '../state/reducers.dart' as r;

/// Local boot. Everything awaited here is network-free: the store opens, the
/// schema provisions, the SSP starts, the session is restored from the cached
/// token, the outbox is mirrored. `localReady` flips at the end and the network
/// half runs in the background ([startRemote]).
Future<void> boot(Ctx ctx, SagaEnv env) async {
  final bucket =
      await ctx(Fx.service<String?>(ServiceName.hintRead)) ?? anonUserId;
  await ctx(Fx.service<void>(ServiceName.localConnect, [bucket]));
  await ctx(Fx.stateUpdate(r.setIdentity(bucketId: bucket)));
  await ctx(Fx.service<void>(ServiceName.migratorProvision));
  await migrateWindowToView(ctx);
  await ctx(Fx.service<void>(ServiceName.sspInit));
  await ctx(Fx.service<void>(ServiceName.sspSetPermissions));
  await ctx(Fx.dispatch(const PrimeCircuit()));

  final restoredUserId =
      await ctx(Fx.service<String?>(ServiceName.authRestoreSession));
  final salt = await ctx(Fx.id(IdScope.salt));
  final authId = await ctx(Fx.service<String?>(ServiceName.authSessionAuthId));
  await ctx(Fx.stateUpdate(r.setIdentity(
    sessionId: salt,
    userId: restoredUserId,
    saltUserId: authId,
  )));
  await ctx(Fx.service<void>(ServiceName.crdtSetSessionId, [salt]));
  if (restoredUserId != null) {
    await ctx(Fx.service<void>(ServiceName.sspSetSessionAuth, [
      authId,
      await ctx(Fx.service<String?>(ServiceName.authAccess)),
    ]));
  }

  await loadOutbox(ctx, env);
  await ctx(Fx.service<void>(ServiceName.featuresInit));
  await ctx(Fx.service<void>(ServiceName.releasesInit));
  await ctx(Fx.dispatch(const LifecycleTick()));
  await ctx(Fx.dispatch(const GcTick()));
  await ctx(Fx.stateUpdate(r.setIdentity(localReady: true)));
  await ctx(Fx.dispatch(const StartRemote()));
}

/// One-time copy of the legacy `_00_window` rows into `_00_view`.
Future<void> migrateWindowToView(Ctx ctx) async {
  try {
    final existing = await ctx(Fx.localGetAll(sql.viewTable));
    if (existing.isNotEmpty) return;
    final legacy = await ctx(Fx.localGetAll(sql.legacyViewTable));
    for (final row in legacy) {
      final id = row['id']?.toString();
      if (id == null || row['ids'] is! List) continue;
      final key = id.substring(id.indexOf(':') + 1);
      await ctx(Fx.localPut(sql.viewTable, sql.viewRecordId(key), {
        'ids': row['ids'],
        'confirmed': row['confirmed'] == true,
        'updatedAt': row['updatedAt'] ?? 0,
      }));
    }
  } catch (error) {
    await ctx(Fx.log(
        LogLevel.debug, 'window -> view migration skipped', {'error': error}));
  }
}

/// The network half of boot: connect, verify the restored session, then let
/// membership, LIVE, the poll and the outbox start. Every step is best-effort.
Future<void> startRemote(Ctx ctx) async {
  try {
    await ctx(Fx.service<void>(ServiceName.remoteConnect));
  } catch (error) {
    await ctx(Fx.log(
        LogLevel.warn,
        'remote connect failed; running from the local store',
        {'error': error}));
  }
  await ctx(Fx.service<void>(ServiceName.supervisorStart));
  try {
    await ctx(Fx.service<void>(ServiceName.authInit));
  } catch (error) {
    await ctx(Fx.log(
        LogLevel.warn,
        'auth verification failed; keeping the restored session',
        {'error': error}));
  }
  await ctx(Fx.dispatch(const EnsureRegistered()));
  await ctx(Fx.dispatch(const LiveStart()));
  await ctx(Fx.dispatch(const PollTick()));
  await ctx(Fx.dispatch(const Drain()));
}

/// Fill the circuit from the local store; `primed` flips even on failure so
/// fetches never hang.
Future<void> primeCircuit(Ctx ctx) async {
  final pending =
      await ctx(Fx.stateRead((s) => [for (final i in s.outbox) i.recordId]));
  try {
    await ctx(Fx.service<void>(ServiceName.sspPrime, [pending]));
  } catch (error) {
    await ctx(Fx.log(LogLevel.warn, 'circuit prime failed; starting empty',
        {'error': error}));
  } finally {
    await ctx(Fx.stateUpdate(r.setIdentity(primed: true)));
  }
}

Future<void> versionsPrimed(Ctx ctx, List<(String, int)> entries) async {
  await ctx(Fx.stateUpdate(r.setVersions(entries)));
}

/// Hand this client's views back to the server when the app goes away for good.
Future<void> appDetached(Ctx ctx) async {
  final ids = await ctx(Fx.stateRead((s) => [
        for (final e in s.queries.values)
          if (e.lifecycle.remote == RemotePhase.registered) e.def.id
      ]));
  if (ids.isNotEmpty) {
    await ctx(Fx.service<void>(ServiceName.remoteReleaseViews, [ids]));
  }
}
