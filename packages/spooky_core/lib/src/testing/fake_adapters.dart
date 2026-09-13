import 'dart:async';

import '../kernel/effects.dart';
import '../kernel/events.dart';
import '../kernel/interpreter.dart';
import '../services/stream_processor/stream_processor_service.dart'
    show IngestRecord;
import '../state/client_state.dart';
import '../types.dart';
import 'run_pure.dart' show sha256Hex;

/// One recorded adapter call.
typedef AdapterCall = (String name, List<Object?> args);

class FakeTimers implements TimerPort {
  final Map<String, ({int ms, void Function() fire})> pending = {};

  @override
  void set(String key, int ms, void Function() fire) =>
      pending[key] = (ms: ms, fire: fire);

  @override
  void clear(String key) => pending.remove(key);

  void fire(String key) {
    final t = pending.remove(key);
    t?.fire();
  }

  void fireAll() {
    for (final key in [...pending.keys]) {
      fire(key);
    }
  }
}

class FakeLocal implements LocalPort {
  FakeLocal(this._log, {Map<String, Map<String, Map<String, dynamic>>>? rows})
      : tables = rows ?? {};

  final void Function(String, List<Object?>) _log;

  /// `table -> id -> document`.
  final Map<String, Map<String, Map<String, dynamic>>> tables;

  int _epoch = 0;
  @override
  int get epoch => _epoch;
  void bumpEpoch() => _epoch++;

  @override
  Map<String, dynamic>? get(String table, String id) {
    _log('local.get', [table, id]);
    return tables[table]?[id];
  }

  @override
  List<Map<String, dynamic>> getMany(String table, List<String> ids) {
    _log('local.getMany', [table, ids]);
    final t = tables[table] ?? const {};
    return [
      for (final id in ids)
        if (t[id] != null) t[id]!
    ];
  }

  @override
  List<Map<String, dynamic>> getAll(String table) {
    _log('local.getAll', [table]);
    return (tables[table] ?? const {}).values.toList();
  }

  @override
  void put(String table, String id, Map<String, dynamic> data, WriteMode mode) {
    _log('local.put', [table, id, data, mode]);
    final t = tables.putIfAbsent(table, () => {});
    t[id] = mode == WriteMode.merge ? {...?t[id], ...data} : {...data};
  }

  @override
  void delete(String table, String id) {
    _log('local.delete', [table, id]);
    tables[table]?.remove(id);
  }

  @override
  void tx(List<LocalOp> ops) {
    _log('local.tx', [ops]);
    for (final op in ops) {
      switch (op) {
        case PutOp(:final table, :final id, :final data, :final mode):
          final t = tables.putIfAbsent(table, () => {});
          t[id] = mode == WriteMode.merge ? {...?t[id], ...data} : {...data};
        case DeleteOp(:final table, :final id):
          tables[table]?.remove(id);
        case BumpRvOp(:final table, :final id):
          final row = tables[table]?[id];
          if (row != null) {
            row['_00_rv'] = ((row['_00_rv'] as num?) ?? 0).toInt() + 1;
          }
      }
    }
  }
}

class FakeRemotePort implements RemotePort {
  FakeRemotePort(this._log, {this.answer});

  final void Function(String, List<Object?>) _log;

  /// Scripted answer per request; the default is an empty response.
  final FutureOr<List<StatementResult>> Function(String sql, Map<String, dynamic>? vars)?
      answer;

  void Function(List<String> hashes, List<InlineRow>? rows)? onLiveChange;
  final List<String> killed = [];

  @override
  Future<List<StatementResult>> queryStatements(String sql,
      [Map<String, dynamic>? vars]) async {
    _log('remote.query', [sql, vars]);
    return answer == null ? const [] : await answer!(sql, vars);
  }

  @override
  Future<String> live(String table,
      void Function(List<String>, List<InlineRow>?) onChange) async {
    _log('remote.live', [table]);
    onLiveChange = onChange;
    return 'live-uuid';
  }

  @override
  Future<void> kill(String uuid) async {
    _log('remote.kill', [uuid]);
    killed.add(uuid);
  }
}

class FakeSsp implements SspPort {
  FakeSsp(this._log, {this.localArrayFor});

  final void Function(String, List<Object?>) _log;
  final RecordVersionArray Function(String hash)? localArrayFor;
  final List<IngestRecord> ingested = [];
  final List<String> unregistered = [];

  @override
  RegisterResult register(RegisterPlan plan) {
    _log('ssp.register', [plan.queryHash]);
    return RegisterResult(
      localArray: localArrayFor?.call(plan.queryHash) ?? const [],
      timings: RegistrationTimings.empty,
    );
  }

  @override
  void unregister(String hash) {
    _log('ssp.unregister', [hash]);
    unregistered.add(hash);
  }

  @override
  void ingestMany(List<IngestRecord> records) {
    _log('ssp.ingest', [records.length]);
    ingested.addAll(records);
  }
}

/// Records every service call and answers from [answers], so a saga test can
/// script one call without stubbing the other twenty-five.
class FakeServices implements Services {
  FakeServices(this._log, [this.answers = const {}]);

  final void Function(String, List<Object?>) _log;
  final Map<ServiceName, Object? Function(List<Object?> args)> answers;

  Object? _call(ServiceName name, [List<Object?> args = const []]) {
    _log('service.${name.name}', args);
    return answers[name]?.call(args);
  }

  @override
  String? hintRead() => _call(ServiceName.hintRead) as String?;
  @override
  void hintWrite(String bucketId) => _call(ServiceName.hintWrite, [bucketId]);
  @override
  Future<void> localConnect(String bucketId) async =>
      _call(ServiceName.localConnect, [bucketId]);
  @override
  Future<void> localSwitchStore(String bucketId) async =>
      _call(ServiceName.localSwitchStore, [bucketId]);
  @override
  String localCurrentBucketId() =>
      _call(ServiceName.localCurrentBucketId) as String? ?? 'anon';
  @override
  Future<void> migratorProvision() async =>
      _call(ServiceName.migratorProvision);
  @override
  Future<void> sspInit() async => _call(ServiceName.sspInit);
  @override
  void sspSetPermissions() => _call(ServiceName.sspSetPermissions);
  @override
  void sspSetSessionAuth(String? authId, String? access) =>
      _call(ServiceName.sspSetSessionAuth, [authId, access]);
  @override
  Future<void> sspPrime(List<String> pendingIds) async =>
      _call(ServiceName.sspPrime, [pendingIds]);
  @override
  Future<void> sspReset() async => _call(ServiceName.sspReset);
  @override
  void sspSetPersistence(bool enabled) =>
      _call(ServiceName.sspSetPersistence, [enabled]);
  @override
  Future<String?> authRestoreSession() async =>
      _call(ServiceName.authRestoreSession) as String?;
  @override
  Future<void> authInit() async => _call(ServiceName.authInit);
  @override
  String? authSessionAuthId() =>
      _call(ServiceName.authSessionAuthId) as String?;
  @override
  String? authAccess() => _call(ServiceName.authAccess) as String?;
  @override
  String? authToken() => _call(ServiceName.authToken) as String?;
  @override
  Map<String, dynamic>? authCurrentUser() =>
      _call(ServiceName.authCurrentUser) as Map<String, dynamic>?;
  @override
  Future<void> remoteConnect() async => _call(ServiceName.remoteConnect);
  @override
  void remoteReleaseViews(List<Object?> ids) =>
      _call(ServiceName.remoteReleaseViews, [ids]);
  @override
  void supervisorStart() => _call(ServiceName.supervisorStart);
  @override
  void crdtSetSessionId(String sessionId) =>
      _call(ServiceName.crdtSetSessionId, [sessionId]);
  @override
  void crdtCloseAll(bool flush) => _call(ServiceName.crdtCloseAll, [flush]);
  @override
  Future<void> persistenceSet(String key, Object? value) async =>
      _call(ServiceName.persistenceSet, [key, value]);
  @override
  void featuresInit() => _call(ServiceName.featuresInit);
  @override
  void releasesInit() => _call(ServiceName.releasesInit);
}

/// Recording adapters for interpreter, runtime and facade tests. Every call is
/// logged; behaviour is scripted by passing the port you care about.
class FakeAdapters {
  FakeAdapters({
    FakeLocal? local,
    FakeRemotePort? remote,
    FakeSsp? ssp,
    Map<ServiceName, Object? Function(List<Object?>)> services = const {},
    int now = 1700000000000,
  }) : _now = now {
    this.local = local ?? FakeLocal(_log);
    this.remote = remote ?? FakeRemotePort(_log);
    this.ssp = ssp ?? FakeSsp(_log);
    this.services = FakeServices(_log, services);
  }

  final List<AdapterCall> calls = [];
  final FakeTimers timers = FakeTimers();
  late final FakeLocal local;
  late final FakeRemotePort remote;
  late final FakeSsp ssp;
  late final FakeServices services;
  int _now;
  int _ids = 0;

  void _log(String name, List<Object?> args) => calls.add((name, args));

  List<String> names() => [for (final c in calls) c.$1];

  void advance(int ms) => _now += ms;

  Adapters build() => Adapters(
        local: local,
        remote: remote,
        ssp: ssp,
        timers: timers,
        now: () => _now,
        mutationId: () =>
            '_00_pending_mutations:${(++_ids).toString().padLeft(13, '0')}_0001_tab',
        saltId: () => 'salt-${++_ids}',
        hash: sha256Hex,
        services: services,
      );
}

/// A minimal host: holds state and resolves waits when the predicate holds
/// after an update.
class FakeHost implements InterpreterHost {
  FakeHost([ClientState? initial])
      : _state = initial ?? emptyState(tabId: 'tab-a');

  ClientState _state;
  final List<OutEvent> emitted = [];
  final List<RuntimeEvent> dispatched = [];
  final Set<({bool Function(ClientState) until, Completer<void> done})>
      waiters = {};

  ClientState get state => _state;

  @override
  ClientState getState() => _state;

  @override
  void setState(ClientState next) {
    _state = next;
    for (final w in [...waiters]) {
      if (w.until(_state)) {
        waiters.remove(w);
        w.done.complete();
      }
    }
  }

  @override
  Future<void> waitFor(bool Function(ClientState) until) {
    if (until(_state)) return Future.value();
    final done = Completer<void>();
    waiters.add((until: until, done: done));
    return done.future;
  }

  @override
  void emit(OutEvent event) => emitted.add(event);

  @override
  void dispatch(RuntimeEvent event) => dispatched.add(event);
}
