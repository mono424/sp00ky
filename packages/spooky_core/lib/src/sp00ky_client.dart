import 'dart:async';
import 'client/worker_client.dart';
import 'modules/auth/sp00ky_auth.dart';
import 'modules/query_builder.dart';
import 'modules/query_host.dart';
import 'modules/bucket.dart';
import 'modules/feature_flag/feature_flag.dart';
import 'modules/app_release/app_release.dart';
import 'mutation/rows.dart';
import 'services/logger/logger.dart';
import 'state/client_state.dart';
import 'types.dart';

/// Native local-first client. SQLite, the circuit and sync live in one worker.
/// Use the advanced InProcessSp00kyClient only when you own its scheduling.
abstract class Sp00kyClient {
  factory Sp00kyClient(Sp00kyConfig config) = WorkerSp00kyClient;
  Sp00kyClient.internal();
  Sp00kyConfig get config;
  Sp00kyAuth get auth;
  bool get isLocalReady;
  Future<void> init();
  Future<void> close();
  Future<void> checkpoint();
  void wake();
  Future<void> detach();
  Future<ClientState> inspectState();
  Future<String> queryRaw(String sql, Map<String, dynamic> params,
      {QueryTimeToLive ttl = defaultTtl,
      List<RelationPlan> relations = const []});
  Future<List<dynamic>> queryRemote(String sql, [Map<String, dynamic>? vars]);
  Future<void> preload(String sql, Map<String, dynamic> params,
      {QueryTimeToLive ttl = defaultTtl});
  Stream<List<Map<String, dynamic>>> subscribeStream(String hash,
      {bool immediate = true});
  void Function() subscribe(String hash, QueryUpdateCallback callback,
      {bool immediate = false}) {
    final sub = subscribeStream(hash, immediate: immediate).listen(callback);
    return () => unawaited(sub.cancel());
  }

  void Function() subscribeQueryStatus(
      String hash, QueryStatusCallback callback,
      {bool immediate = false});
  Stream<QueryStatus> queryStatusStream(String hash, {bool immediate = true}) =>
      broadcast((cb) => subscribeQueryStatus(hash, cb, immediate: immediate));
  void Function() subscribeQueryAuthority(
      String hash, QueryAuthorityCallback callback,
      {bool immediate = false});
  Stream<bool> queryAuthorityStream(String hash, {bool immediate = true}) =>
      broadcast(
          (cb) => subscribeQueryAuthority(hash, cb, immediate: immediate));
  bool isQueryAuthoritative(String hash);
  bool isQuerySettled(String hash);
  QueryTimings? queryTimings(String hash);
  void reportFrontendTiming(String hash, double ms);
  void deregisterQuery(String hash);
  QueryBuilder query(String table) => QueryBuilder(table,
      schema: config.schema,
      registrar: (sql, params, ttl, relations) =>
          queryRaw(sql, params, ttl: ttl, relations: relations),
      subscriber: subscribeStream);
  Future<Stream<List<Map<String, dynamic>>>> queryStream(
          String sql, Map<String, dynamic> params,
          {QueryTimeToLive ttl = defaultTtl}) async =>
      subscribeStream(await queryRaw(sql, params, ttl: ttl));
  Future<Map<String, dynamic>> create(String id, Map<String, dynamic> data);
  Future<Map<String, dynamic>> update(
      String table, String id, Map<String, dynamic> data,
      {UpdateOptions? options});
  Future<void> delete(String table, String id);
  Future<void> run(String backend, String path, Map<String, dynamic> payload,
      {RunOptions? options});
  int get failedMutationCount;
  int get pendingMutationCount;
  int get fetchingQueryCount;
  Set<String> get unsyncedRecordIds;
  SyncHealth get syncHealth;
  void Function() subscribeToFailedMutations(void Function(int) cb);
  void Function() subscribeToPendingMutations(void Function(int) cb);
  void Function() subscribeToFetchActivity(void Function(int) cb);
  void Function() subscribeToUnsyncedRecords(void Function(Set<String>) cb);
  void Function() subscribeToSyncHealth(void Function(SyncHealth) cb);
  Stream<SyncHealth> syncHealthStream() => broadcast(subscribeToSyncHealth);
  Future<List<FailedMutationRow>> listFailedMutations();
  Future<bool> retryFailedMutation(String id);
  Future<bool> discardFailedMutation(String id);
  Future<dynamic> authenticate(String token);
  Future<void> deauthenticate();

  FeatureFlagModule? _flags;
  AppReleaseModule? _releases;
  FeatureFlagHandle feature(String key,
      {String? fallback, QueryTimeToLive? ttl}) {
    final flags = _flags ??= FeatureFlagModule(
        host: _Host(this), auth: auth, logger: SpookyLogger.root())
      ..init();
    return flags.feature(key, fallback: fallback, ttl: ttl);
  }

  AppReleaseHandle appRelease(String app, {QueryTimeToLive? ttl}) {
    final releases = _releases ??= AppReleaseModule(
        host: _Host(this), auth: auth, logger: SpookyLogger.root())
      ..init();
    return releases.release(app, ttl: ttl);
  }

  void closeModules() {
    _flags?.closeAll();
    _releases?.closeAll();
  }

  BucketHandle bucket(String name) => BucketHandle.withQuery(name, queryRemote);
  Future<Never> openCrdtField(String table, String recordId, String field,
          [String? fallbackText]) =>
      throw UnimplementedError('CRDT deferred');
  void closeCrdtField(String table, String recordId, String field) =>
      throw UnimplementedError('CRDT deferred');
}

Stream<T> broadcast<T>(void Function() Function(void Function(T)) attach) {
  late StreamController<T> controller;
  void Function()? off;
  controller = StreamController<T>.broadcast(
      onListen: () => off = attach(controller.add),
      onCancel: () {
        off?.call();
        off = null;
      });
  return controller.stream;
}

class _Host implements QueryHost {
  _Host(this.client);
  final Sp00kyClient client;
  @override
  Future<String> registerQuery(String table, String sql,
          Map<String, dynamic> params, QueryTimeToLive ttl) =>
      client.queryRaw(sql, params, ttl: ttl);
  @override
  void Function() subscribe(String hash, QueryUpdateCallback cb,
          {bool immediate = false}) =>
      client.subscribe(hash, cb, immediate: immediate);
}
