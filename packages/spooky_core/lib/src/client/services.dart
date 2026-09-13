import 'dart:async';

import '../kernel/effects.dart';
import '../kernel/interpreter.dart';
import '../modules/auth/auth_service.dart';
import '../services/database/connection_supervisor.dart';
import '../services/database/local_database_service.dart';
import '../services/database/local_migrator.dart';
import '../services/database/remote_database_service.dart';
import '../services/logger/logger.dart';
import '../services/stream_processor/stream_processor_service.dart';
import '../surreal/remote_client.dart';
import '../surreal/value.dart' show generateId;
import '../testing/run_pure.dart' show sha256Hex;
import '../types.dart';
import '../utils/record_id_utils.dart';
import 'runtime.dart';

/// Holds the local store so a bucket switch can replace it under the adapters
/// without rebuilding them.
class LocalStoreHolder {
  LocalStoreHolder(this._logger, this._config);

  final SpookyLogger _logger;
  final DatabaseConfig _config;

  LocalDatabaseService? _db;
  String _bucketId = '';
  int _epoch = 0;

  LocalDatabaseService get db {
    final open = _db;
    if (open == null) throw StateError('The local store is not open yet');
    return open;
  }

  String get bucketId => _bucketId;
  int get epoch => _epoch;

  /// The file a bucket's store lives in.
  ///
  /// Every bucket gets its own file, including the anonymous one: a single
  /// shared file is how one user's cached rows end up readable by the next.
  /// An in-memory store needs no separation - reopening it IS a fresh database.
  String? pathFor(String bucketId) {
    if (_config.store == StoreType.memory) return null;
    final base = _config.localDbPath ?? 'spooky.db';
    final dot = base.lastIndexOf('.');
    return dot <= 0
        ? '$base.$bucketId'
        : '${base.substring(0, dot)}.$bucketId${base.substring(dot)}';
  }

  void connect(String bucketId) {
    _db?.close();
    _bucketId = bucketId;
    _db = LocalDatabaseService.open(_logger,
        store: _config.store, path: pathFor(bucketId));
    _db!.provision();
    _epoch++;
    _logger.debug('Local store open for bucket $bucketId');
  }
}

/// The local document store, as the interpreter sees it.
class LocalStoreAdapter implements LocalPort {
  LocalStoreAdapter(this._holder);
  final LocalStoreHolder _holder;

  @override
  int get epoch => _holder.epoch;

  @override
  Map<String, dynamic>? get(String table, String id) =>
      _holder.db.getDoc(table, id);

  @override
  List<Map<String, dynamic>> getMany(String table, List<String> ids) =>
      _holder.db.getDocs(table, ids);

  @override
  List<Map<String, dynamic>> getAll(String table) =>
      _holder.db.getAllDocs(table);

  @override
  void put(String table, String id, Map<String, dynamic> data, WriteMode mode) =>
      _holder.db.putDoc(table, id, data, merge: mode == WriteMode.merge);

  @override
  void delete(String table, String id) => _holder.db.deleteDoc(table, id);

  @override
  void tx(List<LocalOp> ops) {
    final db = _holder.db;
    db.tx(() {
      for (final op in ops) {
        switch (op) {
          case PutOp(:final table, :final id, :final data, :final mode):
            db.putDoc(table, id, data, merge: mode == WriteMode.merge);
          case DeleteOp(:final table, :final id):
            db.deleteDoc(table, id);
          case BumpRvOp(:final table, :final id):
            db.bumpRv(table, id);
        }
      }
    });
  }
}

/// The remote socket, as the interpreter sees it.
class RemoteAdapter implements RemotePort {
  RemoteAdapter(this._remote, this._logger, {required this.inlineBodies});

  final RemoteDatabaseService _remote;
  final SpookyLogger _logger;
  final bool inlineBodies;

  StreamSubscription<LiveMessage>? _liveSub;

  @override
  Future<List<StatementResult>> queryStatements(String sql,
          [Map<String, dynamic>? vars]) =>
      _remote.queryStatements(sql, vars);

  @override
  Future<String> live(String table,
      void Function(List<String>, List<InlineRow>?) onChange) async {
    // `FETCH out` resolves the edge's target into the notification, so the body
    // arrives with the doorbell instead of costing a second round trip. The
    // clause is only ever added, never relied on: a server that ignores it, or
    // a row the session cannot read, simply yields no inline row and the fetch
    // path takes over.
    final join = inlineBodies ? ' FETCH out' : '';
    final (uuid, stream) =
        await _remote.live('LIVE SELECT * FROM $table$join');
    await _liveSub?.cancel();
    _liveSub = stream.listen((message) {
      if (message.action == 'KILLED') return;
      final hash = hashOfEdge(message.value);
      if (hash == null) return;
      // Not on DELETE: that edge is a row leaving the view, and for a row that
      // was actually deleted the join yields nothing anyway. Landing a body
      // there would only ever write back what is going away.
      final row = message.action == 'DELETE' ? null : rowOfEdge(message.value);
      onChange([hash], row == null ? null : [row]);
    }, onError: (Object e) => _logger.debug('live stream error: $e'));
    return uuid;
  }

  @override
  Future<void> kill(String uuid) async {
    await _liveSub?.cancel();
    _liveSub = null;
    await _remote.kill(uuid);
  }

  Future<void> dispose() async {
    await _liveSub?.cancel();
    _liveSub = null;
  }
}

/// The hash a `_00_list_ref` edge belongs to: the id part of its `in`
/// (`_00_query:<hash>`).
String? hashOfEdge(Map<String, dynamic> value) {
  final inId = value['in'];
  if (inId == null) return null;
  final str = extractIdPart(inId);
  return str.isEmpty ? null : str;
}

/// The row an edge notification carries, when the subscription joined it on.
///
/// Without `FETCH out` the edge's `out` is a record-id string and there is
/// nothing to land, so this returns null and the caller falls back to fetching
/// the body. It also returns null for a row the session may not read (the join
/// yields `out: null`) and for an edge whose target has been deleted.
InlineRow? rowOfEdge(Map<String, dynamic> value) {
  final out = value['out'];
  if (out is! Map) return null;
  final id = out['id'];
  // The landing path keys off `row.id`; without one it would record a version
  // for a body it never wrote.
  if (id == null) return null;
  final version = value['version'];
  if (version is! num) return null;
  return InlineRow(
    id: id.toString(),
    version: version.toInt(),
    record: Map<String, dynamic>.from(out),
  );
}

/// The in-process SSP, as the interpreter sees it.
class SspAdapter implements SspPort {
  SspAdapter(this._ssp);
  final StreamProcessorService _ssp;

  @override
  RegisterResult register(RegisterPlan plan) {
    final update = _ssp.registerQueryPlan(QueryPlanConfig(
      queryHash: plan.queryHash,
      surql: plan.surql,
      params: plan.params,
      ttl: plan.ttl,
      lastActiveAt: DateTime.now().toUtc(),
    ));
    if (update == null) throw StateError('Stream processor is not initialized');
    return RegisterResult(
      localArray: update.localArray,
      timings: RegistrationTimings(
        parseMs: update.parseMs,
        planMs: update.planMs,
        snapshotMs: update.snapshotMs,
      ),
    );
  }

  @override
  void unregister(String hash) => _ssp.unregisterQueryPlan(hash);

  @override
  void ingestMany(List<IngestRecord> records) => _ssp.ingestMany(records);
}

/// Everything the `service` effect can name, bound to the real services.
class ClientServices implements Services {
  ClientServices({
    required this.holder,
    required this.ssp,
    required this.persistence,
    required this.schemaSurql,
    required this.logger,
    required this.lateModules,
    this.remote,
    this.auth,
    this.supervisor,
  });

  final LocalStoreHolder holder;
  final StreamProcessorService ssp;
  final PersistenceClient persistence;
  final String schemaSurql;
  final SpookyLogger logger;

  /// Modules whose live query must start once boot has finished.
  final List<void Function()> Function() lateModules;

  RemoteDatabaseService? remote;
  AuthService? auth;
  ConnectionSupervisor? supervisor;

  static const _hintKey = 'sp00ky_boot_bucket';

  @override
  Future<String?> hintRead() => persistence.get<String>(_hintKey);

  @override
  Future<void> hintWrite(String bucketId) =>
      persistence.set(_hintKey, bucketId);

  @override
  Future<void> localConnect(String bucketId) async => holder.connect(bucketId);

  @override
  Future<void> localSwitchStore(String bucketId) async =>
      holder.connect(bucketId);

  @override
  String localCurrentBucketId() => holder.bucketId;

  @override
  Future<void> migratorProvision() =>
      LocalMigrator(holder.db, logger).provision(schemaSurql);

  @override
  Future<void> sspInit() => ssp.init();

  @override
  void sspSetPermissions() => ssp.seedPermissionsFromSchema(schemaSurql);

  @override
  void sspSetSessionAuth(String? authId, String? access) =>
      ssp.setSessionAuth(authId, access);

  @override
  Future<void> sspPrime(List<String> pendingIds) async {
    final tables = schemaTables;
    final db = holder.db;
    await ssp.primeFromLocal(
      tables: tables,
      versions: db.scanVersions(tables),
      selectByIds: db.getDocs,
      snapshot: db.getSnapshot(),
      pendingIds: pendingIds.toSet(),
      onVersions: onVersions,
    );
  }

  /// Tables the prime walks, set by the client from its schema.
  List<String> schemaTables = const [];

  /// Where the primed `(id, rv)` pairs go: the runtime's `VersionsPrimed`.
  void Function(String table, List<(String, int)> entries)? onVersions;

  @override
  Future<void> sspReset() async {
    await ssp.resetCircuit();
    holder.db.clearSnapshot();
  }

  @override
  void sspSetPersistence(bool enabled) {}

  @override
  Future<String?> authRestoreSession() async =>
      auth?.restoreSessionFromToken();

  @override
  Future<void> authInit() async => auth?.init();

  @override
  String? authSessionAuthId() => auth?.currentUser?['id']?.toString();

  @override
  String? authAccess() => auth?.access;

  @override
  String? authToken() => auth?.token;

  @override
  Map<String, dynamic>? authCurrentUser() => auth?.currentUser;

  @override
  Future<void> remoteConnect() async => remote?.connect();

  @override
  void remoteReleaseViews(List<Object?> ids) {
    final r = remote;
    if (r == null) return;
    final valid = [
      for (final id in ids)
        if (_viewId.hasMatch(id.toString())) id.toString()
    ];
    if (valid.isEmpty) return;
    // Best-effort and unawaited: the process is going away.
    final list = valid.join(', ');
    unawaited(r
        .query(
            'FOR \$id IN [$list] { LET \$_released = fn::query::unsubscribe(\$id); };')
        .catchError((Object _) => const <dynamic>[]));
  }

  static final _viewId = RegExp(r'^_00_query:[0-9a-f]{64}$');

  @override
  void supervisorStart() => supervisor?.start();

  @override
  void crdtSetSessionId(String sessionId) {}

  @override
  void crdtCloseAll(bool flush) {}

  @override
  Future<void> persistenceSet(String key, Object? value) =>
      persistence.set(key, value);

  @override
  void featuresInit() {
    for (final start in lateModules()) {
      start();
    }
  }

  @override
  void releasesInit() {}
}

/// Build the adapters the interpreter drives.
Adapters createAdapters({
  required LocalPort local,
  required RemotePort remote,
  required SspPort ssp,
  required Services services,
  required String Function() mutationId,
  TimerPort? timers,
}) =>
    Adapters(
      local: local,
      remote: remote,
      ssp: ssp,
      timers: timers ?? RealTimers(),
      now: () => DateTime.now().millisecondsSinceEpoch,
      mutationId: mutationId,
      saltId: generateId,
      hash: sha256Hex,
      services: services,
    );
