import 'dart:async';

import '../services/stream_processor/stream_processor_service.dart'
    show IngestRecord;
import '../state/client_state.dart';
import '../types.dart';
import 'effects.dart';
import 'events.dart';
import 'saga.dart';

/// The local document store, as the interpreter sees it.
abstract interface class LocalPort {
  Map<String, dynamic>? get(String table, String id);
  List<Map<String, dynamic>> getMany(String table, List<String> ids);
  List<Map<String, dynamic>> getAll(String table);
  void put(String table, String id, Map<String, dynamic> data, WriteMode mode);
  void delete(String table, String id);

  /// Apply [ops] atomically.
  void tx(List<LocalOp> ops);

  /// Bumped when the store is replaced; a write fenced with a stale value is
  /// dropped rather than landing in the wrong bucket.
  int get epoch;
}

abstract interface class RemotePort {
  /// Per-statement outcomes, in order. Never throws for a failed statement.
  Future<List<StatementResult>> queryStatements(String sql,
      [Map<String, dynamic>? vars]);

  /// Subscribe to `LIVE SELECT * FROM <table>`; the callback receives the
  /// changed query hashes, plus the changed rows themselves when the
  /// subscription was opened with the body joined on.
  Future<String> live(
      String table, void Function(List<String> hashes, List<InlineRow>? rows) onChange);

  Future<void> kill(String uuid);
}

abstract interface class SspPort {
  RegisterResult register(RegisterPlan plan);
  void unregister(String hash);
  void ingestMany(List<IngestRecord> records);
}

abstract interface class TimerPort {
  void set(String key, int ms, void Function() fire);
  void clear(String key);
}

/// Calls into the services that are adapters, not state. A saga names the call
/// through [ServiceEffect]; this binds it to the real service.
abstract interface class Services {
  /// The bucket this process last used, so boot can open the right store before
  /// the network answers.
  Future<String?> hintRead();
  Future<void> hintWrite(String bucketId);

  Future<void> localConnect(String bucketId);
  Future<void> localSwitchStore(String bucketId);
  String localCurrentBucketId();

  Future<void> migratorProvision();

  Future<void> sspInit();
  void sspSetPermissions();
  void sspSetSessionAuth(String? authId, String? access);
  Future<void> sspPrime(List<String> pendingIds);
  Future<void> sspReset();
  void sspSetPersistence(bool enabled);

  Future<String?> authRestoreSession();
  Future<void> authInit();
  String? authSessionAuthId();
  String? authAccess();
  String? authToken();
  Map<String, dynamic>? authCurrentUser();

  Future<void> remoteConnect();
  void remoteReleaseViews(List<Object?> ids);
  void supervisorStart();

  void crdtSetSessionId(String sessionId);
  void crdtCloseAll(bool flush);

  Future<void> persistenceSet(String key, Object? value);
  void featuresInit();
  void releasesInit();
}

/// The five adapters plus the pure ports.
class Adapters {
  const Adapters({
    required this.local,
    required this.remote,
    required this.ssp,
    required this.timers,
    required this.now,
    required this.mutationId,
    required this.saltId,
    required this.hash,
    required this.services,
  });

  final LocalPort local;
  final RemotePort remote;
  final SspPort ssp;
  final TimerPort timers;
  final int Function() now;
  final String Function() mutationId;
  final String Function() saltId;
  final String Function(String) hash;
  final Services services;
}

/// What the interpreter needs from the runtime: state, waiting, emitting,
/// dispatching.
abstract interface class InterpreterHost {
  ClientState getState();
  void setState(ClientState next);
  Future<void> waitFor(bool Function(ClientState) until);
  void emit(OutEvent event);
  void dispatch(RuntimeEvent event);
}

/// Effects become adapter calls here and nowhere else.
class Interpreter implements Ctx {
  Interpreter(this._adapters, this._host);

  final Adapters _adapters;
  final InterpreterHost _host;

  @override
  Future<R> call<R>(Effect<R> effect) async {
    switch (effect) {
      case LocalGet(:final table, :final id):
        return _adapters.local.get(table, id) as R;
      case LocalGetMany(:final table, :final ids):
        return _adapters.local.getMany(table, ids) as R;
      case LocalGetAll(:final table):
        return _adapters.local.getAll(table) as R;
      case LocalPut(:final table, :final id, :final data, :final mode, :final epoch):
        if (_fenced(epoch)) return null as R;
        _adapters.local.put(table, id, data, mode);
        return null as R;
      case LocalDelete(:final table, :final id, :final epoch):
        if (_fenced(epoch)) return null as R;
        _adapters.local.delete(table, id);
        return null as R;
      case LocalTx(:final ops, :final epoch):
        if (_fenced(epoch)) return null as R;
        _adapters.local.tx(ops);
        return null as R;
      case LocalEpoch():
        return _adapters.local.epoch as R;
      case RemoteQuery(:final sql, :final vars, :final timeoutMs):
        final pending = _adapters.remote.queryStatements(sql, vars);
        if (timeoutMs == null) return await pending as R;
        return await pending.timeout(
          Duration(milliseconds: timeoutMs),
          onTimeout: () =>
              throw TimeoutException('Remote request timed out after ${timeoutMs}ms'),
        ) as R;
      case RemoteLive(:final table):
        return await _adapters.remote.live(
          table,
          (hashes, rows) => _host.dispatch(LiveChange(hashes, rows: rows)),
        ) as R;
      case RemoteKill(:final uuid):
        await _adapters.remote.kill(uuid);
        return null as R;
      case SspRegister(:final plan):
        return _adapters.ssp.register(plan) as R;
      case SspUnregister(:final hash):
        _adapters.ssp.unregister(hash);
        return null as R;
      case SspIngest(:final records):
        _adapters.ssp.ingestMany(records);
        return null as R;
      case TimerSet(:final key, :final ms, :final event):
        _adapters.timers.set(key, ms, () => _host.dispatch(event));
        return null as R;
      case TimerClear(:final key):
        _adapters.timers.clear(key);
        return null as R;
      case StateRead(:final select):
        return select(_host.getState());
      case StateUpdate(:final fn):
        final next = fn(_host.getState());
        _host.setState(next);
        return next as R;
      case StateWait(:final until):
        await _host.waitFor(until);
        return null as R;
      case NowEffect():
        return _adapters.now() as R;
      case IdEffect(:final scope):
        return (scope == IdScope.mutation
            ? _adapters.mutationId()
            : _adapters.saltId()) as R;
      case HashEffect(:final input):
        return _adapters.hash(input) as R;
      case EmitEffect(:final event):
        _host.emit(event);
        return null as R;
      case DispatchEffect(:final event):
        _host.dispatch(event);
        return null as R;
      case AllEffect(:final effects):
        final settled = await Future.wait([
          for (final inner in effects)
            call<Object?>(inner).then<Settled<Object?>>(
              (value) => Settled.ok(value),
              onError: (Object error) => Settled<Object?>.err(error),
            )
        ]);
        return settled as R;
      case ServiceEffect(:final name, :final args):
        return await _service(name, args) as R;
    }
  }

  /// A write planned against an older store is dropped: the bucket switched
  /// under it and the row belongs to a database that is no longer open.
  bool _fenced(int? epoch) => epoch != null && epoch != _adapters.local.epoch;

  Future<Object?> _service(ServiceName name, List<Object?> args) async {
    final s = _adapters.services;
    switch (name) {
      case ServiceName.hintRead:
        return s.hintRead();
      case ServiceName.hintWrite:
        await s.hintWrite(args[0] as String);
      case ServiceName.localConnect:
        await s.localConnect(args[0] as String);
      case ServiceName.localSwitchStore:
        await s.localSwitchStore(args[0] as String);
      case ServiceName.localCurrentBucketId:
        return s.localCurrentBucketId();
      case ServiceName.migratorProvision:
        await s.migratorProvision();
      case ServiceName.sspInit:
        await s.sspInit();
      case ServiceName.sspSetPermissions:
        s.sspSetPermissions();
      case ServiceName.sspSetSessionAuth:
        s.sspSetSessionAuth(args[0] as String?, args[1] as String?);
      case ServiceName.sspPrime:
        await s.sspPrime((args[0] as List).cast<String>());
      case ServiceName.sspReset:
        await s.sspReset();
      case ServiceName.sspSetPersistence:
        s.sspSetPersistence(args[0] as bool);
      case ServiceName.authRestoreSession:
        return s.authRestoreSession();
      case ServiceName.authInit:
        await s.authInit();
      case ServiceName.authSessionAuthId:
        return s.authSessionAuthId();
      case ServiceName.authAccess:
        return s.authAccess();
      case ServiceName.authToken:
        return s.authToken();
      case ServiceName.authCurrentUser:
        return s.authCurrentUser();
      case ServiceName.remoteConnect:
        await s.remoteConnect();
      case ServiceName.remoteReleaseViews:
        s.remoteReleaseViews((args[0] as List).cast<Object?>());
      case ServiceName.supervisorStart:
        s.supervisorStart();
      case ServiceName.crdtSetSessionId:
        s.crdtSetSessionId(args[0] as String);
      case ServiceName.crdtCloseAll:
        s.crdtCloseAll(args[0] as bool);
      case ServiceName.persistenceSet:
        await s.persistenceSet(args[0] as String, args[1]);
      case ServiceName.featuresInit:
        s.featuresInit();
      case ServiceName.releasesInit:
        s.releasesInit();
    }
    return null;
  }
}
