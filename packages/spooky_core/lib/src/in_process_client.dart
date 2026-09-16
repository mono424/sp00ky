import 'dart:async';
import 'sp00ky_client.dart';
import 'services/persistence/session_persistence.dart';

import 'boot/auth_flip_saga.dart' show authFlip;
import 'boot/boot_saga.dart' as boot_saga;
import 'boot/preload_saga.dart' as preload_saga;
import 'client/services.dart';
import 'client/runtime.dart';
import 'kernel/effects.dart';
import 'kernel/events.dart';
import 'kernel/interpreter.dart';
import 'kernel/saga.dart';
import 'modules/app_release/app_release.dart';
import 'modules/auth/auth_service.dart';
import 'modules/bucket.dart';
import 'modules/feature_flag/feature_flag.dart';
import 'modules/query_host.dart';
import 'modules/query_builder.dart';
import 'modules/ref_tables.dart' show RefMode, anonUserId;
import 'mutation/jobs.dart';
import 'mutation/mutation_id.dart';
import 'mutation/rows.dart';
import 'mutation/tray_saga.dart' as tray_saga;
import 'mutation/write_saga.dart' as write_saga;
import 'query/env.dart';
import 'query/lifecycle_saga.dart' show evictQuery;
import 'query/register_saga.dart';
import 'services/database/connection_supervisor.dart';
import 'services/database/local_database_service.dart';
import 'services/database/remote_database_service.dart';
import 'services/logger/logger.dart';
import 'services/persistence/memory_persistence.dart';
import 'services/stream_processor/stream_processor_service.dart';
import 'state/client_state.dart';
import 'state/lifecycle.dart' show isAuthoritative;
import 'state/reducers.dart' as r;
import 'state/selectors.dart' as sel;
import 'surreal/remote_client.dart';
import 'surreal/value.dart';
import 'types.dart';

/// Main entry point of the pure-Dart Spooky core (TS `Sp00kyClient`).
///
/// A thin facade over the saga [Runtime]: every method here either runs one
/// saga, reads a selector, or attaches a subscriber. Live queries are exposed as
/// Dart `Stream`s (consume with a Flutter `StreamBuilder`).
class InProcessSp00kyClient extends Sp00kyClient {
  InProcessSp00kyClient(
    this.config, {
    SpookyLogger? logger,
    RemoteSurrealClient? remoteClient,
  })  : _logger = logger ?? SpookyLogger.root(),
        _remoteClientOverride = remoteClient,
        super.internal();

  final Sp00kyConfig config;
  final SpookyLogger _logger;
  final RemoteSurrealClient? _remoteClientOverride;

  late final LocalStoreHolder _holder;
  late final StreamProcessorService _streamProcessor;
  late final PersistenceClient _persistence;
  late final ClientServices _services;
  RemoteAdapter? _remoteAdapter;
  late final Runtime _runtime;
  late final SagaEnv _env;

  RemoteDatabaseService? _remote;
  AuthService? _auth;
  ConnectionSupervisor? _supervisor;
  FeatureFlagModule? _featureFlags;
  AppReleaseModule? _appReleases;
  void Function()? _authUnsubscribe;

  bool _initialized = false;
  bool sessionTransitioning = false;
  bool _closed = false;
  Future<void>? _initializing;
  Future<void>? _checkpoint;
  Future<void>? _closing;
  SessionPersistence? _sessionPersistence;

  @override
  Future<ClientState> inspectState() async => state;

  StreamProcessorService get streamProcessor => _streamProcessor;

  /// The open local store. A diagnostics and test seam; the engine addresses it
  /// through effects, never directly.
  LocalDatabaseService get localStore => _holder.db;

  /// The engine state, read-only. For tests and diagnostics.
  ClientState get state => _runtime.state;
  int get storeEpoch => _holder.epoch;

  /// Feed an event into the engine. For adapters, tests and diagnostics.
  Future<void> dispatch(RuntimeEvent event) => _runtime.dispatchAsync(event);

  /// The auth service. Throws if no remote endpoint is configured.
  AuthService get auth {
    final a = _auth;
    if (a == null) throw StateError('Auth requires a remote endpoint');
    return a;
  }

  // ==================== LIFECYCLE ====================

  /// Initialize the client.
  ///
  /// Local boot is network-free and awaited: the store opens, the schema
  /// provisions, the circuit primes from the local rows and the session is
  /// restored from the cached token. The network half runs in the background,
  /// so a query registered right after this paints from cache immediately.
  Future<void> init() {
    if (_closed) return Future.error(StateError('Client is closed'));
    return _initializing ??= _init();
  }

  Future<void> _init() async {
    if (_initialized) return;
    _holder = LocalStoreHolder(_logger, config.database);
    // The store has to exist before the persistence client can read the boot
    // hint out of it; the bucket the hint names replaces it a moment later.
    _holder.connect(anonUserId);
    _persistence = _resolvePersistence();
    _streamProcessor = StreamProcessorService(_persistence, _logger);

    final hasRemote =
        config.database.endpoint != null || _remoteClientOverride != null;

    _services = ClientServices(
      holder: _holder,
      ssp: _streamProcessor,
      persistence: _persistence,
      schemaSurql: config.schemaSurql,
      logger: _logger,
      lateModules: () => [
        if (_featureFlags != null) _featureFlags!.init,
        if (_appReleases != null) _appReleases!.init,
      ],
    )
      ..schemaTables = _syncedTables()
      ..onVersions =
          (table, entries) => _runtime.dispatch(VersionsPrimed(entries));

    _env = SagaEnv(
      schema: config.schema,
      refMode: RefMode.dedicated,
      anonLive: config.enableAnonymousLiveQueries,
      materializeDebounceMs: config.streamDebounceTime,
      pollBaseMs: config.refSyncIntervalMs,
      degradeAfter: hasRemote
          ? (config.syncHealth?.degradeAfterConsecutiveFailures ?? 0)
          : 0,
      hasRemote: hasRemote,
    );

    if (hasRemote) {
      final client = _remoteClientOverride ?? WebSocketSurrealClient();
      if (client is WebSocketSurrealClient) {
        client.connectTimeout =
            Duration(milliseconds: config.reconnect.connectTimeoutMs);
      }
      final remote = RemoteDatabaseService(config.database, client, _logger);
      // Armed before boot: boot is local-first and returns before the network
      // half has run, so an app that calls `signUp` the instant `init()`
      // resolves would otherwise reach the server before `use(ns, db)`.
      remote.armConnectGate();
      _remote = remote;
      final auth = AuthService(config.schema, remote, _persistence, _logger);
      // Push what is already queued while the session is still valid. Bounded:
      // an unreachable server must not hold sign-out open. Anything still
      // pending stays in that user's store and drains on their next sign-in.
      auth.onBeforeSignOut = () => _runtime
          .dispatchAsync(const Drain())
          .timeout(Duration(milliseconds: config.reconnect.connectTimeoutMs));
      _auth = auth;
      _services
        ..remote = remote
        ..auth = auth;
      _remoteAdapter =
          RemoteAdapter(remote, _logger, inlineBodies: config.liveInlineBodies);
      _supervisor = ConnectionSupervisor(
        reconnect: remote.connect,
        probe: () async {
          await remote.query('RETURN true');
          if (auth.needsVerification) {
            try {
              await auth.check();
            } catch (error) {
              _logger.warn('Auth retry deferred: $error');
            }
          }
        },
        forceClose: remote.forceClose,
        isConnected: () => remote.isConnected,
        onConnected: client.onConnected,
        onDisconnected: client.onDisconnected,
        logger: _logger,
        config: config.reconnect,
      );
      _services.supervisor = _supervisor;
      remote.onHandshake = () => unawaited(auth.check().catchError(
          (Object e) => _logger.warn('Auth verification deferred: $e')));
    } else {
      _remoteAdapter = null;
    }

    _runtime = Runtime(
      env: _env,
      adapters: createAdapters(
        local: LocalStoreAdapter(_holder),
        remote: _remoteAdapter ?? _OfflineRemote(),
        ssp: SspAdapter(_streamProcessor),
        services: _services,
        mutationId: () => mintMutationId(_clientId),
      ),
      logger: _logger,
      clientId: _clientId,
    );

    // The circuit's updates are what dirty a query, so they have to reach the
    // runtime before anything registers.
    _streamProcessor.addReceiver(_StreamUpdateBridge(_runtime));

    if (hasRemote) {
      _supervisor!
          .subscribe((state) => _runtime.dispatch(ConnectionChanged(state)));
      _featureFlags = FeatureFlagModule(
        host: _QueryHost(this),
        auth: _auth!,
        logger: _logger,
      );
      _appReleases = AppReleaseModule(
        host: _QueryHost(this),
        auth: _auth!,
        logger: _logger,
      );
      // The signed-in principal drives routing, the local `$auth`, the bucket
      // and the salt: all of that is one saga.
      _auth!.onSessionChanged = (userId) async {
        if (_auth!.currentUser?['id']?.toString() != userId) return;
        sessionTransitioning = true;
        try {
          await _runtime.run((ctx) => authFlip(ctx, _env, userId),
              lane: const Lane.serial('bucket'), allowsAccountChange: true);
        } finally {
          sessionTransitioning = false;
        }
      };
    }

    await _runtime.run((ctx) => boot_saga.boot(ctx, _env),
        allowsAccountChange: true);
    _initialized = true;
    await _auth?.publishSession();
    if (_env.hasRemote) _runtime.dispatch(const StartRemote());
    _logger.info('Sp00kyClient initialized');
  }

  /// True once the local half of boot has finished, which is when a query can
  /// paint from cache.
  bool get isLocalReady => _runtime.state.localReady;

  /// The host came back to the foreground, or the network returned.
  ///
  /// The core is framework-agnostic, so the app calls this: in Flutter, from
  /// `AppLifecycleState.resumed`. It resets the reconnect backoff, probes a
  /// possibly half-open socket and beats the TTL heartbeat.
  void wake() {
    _supervisor?.wake('resumed');
    if (_auth != null)
      unawaited(_auth!
          .check()
          .catchError((Object e) => _logger.warn('Auth retry deferred: $e')));
    _runtime.dispatch(const HeartbeatNow());
  }

  /// The host is going away for good: hand this client's views back rather than
  /// leaving them to expire by TTL.
  Future<void> detach() => _runtime.dispatchAsync(const AppDetached());

  Future<void> close() => _closing ??= _close();

  Future<void> _close() async {
    _closed = true;
    if (_initializing == null) return;
    try {
      await _initializing;
    } catch (_) {}
    _remote?.onHandshake = null;
    _authUnsubscribe?.call();
    _auth?.dispose();
    _featureFlags?.closeAll();
    _appReleases?.closeAll();
    closeModules();
    try {
      _runtime.dispose();
    } catch (_) {}
    _checkpointCircuit();
    await _supervisor?.dispose();
    await _remoteAdapter?.dispose();
    await _remote?.close();
    try {
      await _streamProcessor.close();
    } catch (_) {}
    try {
      _holder.db.close();
    } catch (_) {}
    _sessionPersistence?.close();
    _initialized = false;
  }

  // ==================== QUERIES ====================

  /// One-shot direct remote query, bypassing the sync layer (TS
  /// `useRemote(r => r.query(...))`). Results are NOT synced into the local
  /// cache. Throws without a remote endpoint.
  Future<List<dynamic>> queryRemote(String sql, [Map<String, dynamic>? vars]) {
    final remote = _remote;
    if (remote == null) {
      throw StateError('queryRemote requires a remote endpoint');
    }
    return remote.query(sql, vars);
  }

  /// Register a raw SURQL query and return its hash. The table is parsed from
  /// the first `FROM <table>`.
  ///
  /// Returns as soon as the LOCAL registration completes, so a `StreamBuilder`
  /// paints from the local store with no network on the paint path. The remote
  /// registration is dispatched, never awaited.
  Future<String> queryRaw(
    String sql,
    Map<String, dynamic> params, {
    QueryTimeToLive ttl = defaultTtl,
    List<RelationPlan> relations = const [],
  }) =>
      _runtime.run((ctx) => registerLocal(
            ctx,
            _env,
            RegisterInput(
              tableName: parseTableFromSurql(sql),
              surql: sql,
              params: params,
              ttl: ttl,
              hasExplicitOrder: _hasOrderBy.hasMatch(sql),
              relations: relations,
            ),
          ));

  static final _hasOrderBy = RegExp(r'\bORDER\s+BY\b', caseSensitive: false);

  /// Prewarm a query without subscribing to it.
  ///
  /// Resolved before on this device: returns at once, and its rows paint from
  /// cache. Never resolved: resolves once the server's membership and every
  /// body are local. The entry is evicted like any other query a ttl after it
  /// was registered, unless a view mounts the same query meanwhile.
  Future<void> preload(
    String sql,
    Map<String, dynamic> params, {
    QueryTimeToLive ttl = defaultTtl,
  }) async {
    await _runtime.run((ctx) => preload_saga.preload(
          ctx,
          _env,
          RegisterInput(
            tableName: parseTableFromSurql(sql),
            surql: sql,
            params: params,
            ttl: ttl,
            hasExplicitOrder: _hasOrderBy.hasMatch(sql),
          ),
        ));
  }

  /// Subscribe to a registered query as a broadcast [Stream]. Multiple
  /// listeners share one internal registration; [immediate] replays the current
  /// result set on first listen (so a `StreamBuilder` renders immediately).
  Stream<List<Map<String, dynamic>>> subscribeStream(
    String queryHash, {
    bool immediate = true,
  }) =>
      _broadcast<List<Map<String, dynamic>>>(
          (add) => _runtime.subscribe(queryHash, add, immediate: immediate));

  /// Faithful callback-based subscribe (TS `subscribe`).
  void Function() subscribe(
    String hash,
    QueryUpdateCallback callback, {
    bool immediate = false,
  }) =>
      _runtime.subscribe(hash, callback, immediate: immediate);

  /// A query's fetch status (idle/fetching) via callback.
  void Function() subscribeQueryStatus(
    String queryHash,
    QueryStatusCallback callback, {
    bool immediate = false,
  }) =>
      _runtime.subscribeStatus(queryHash, callback, immediate: immediate);

  Stream<QueryStatus> queryStatusStream(String queryHash,
          {bool immediate = true}) =>
      _broadcast<QueryStatus>((add) =>
          _runtime.subscribeStatus(queryHash, add, immediate: immediate));

  /// A query's authority: true once server membership is known for it (a
  /// registration, a poll, or the durable `_00_view` seed), false when a bucket
  /// switch resets it.
  ///
  /// This is what tells "no rows yet" apart from "no rows": a binding shows its
  /// loader while a query is not authoritative, and an empty result only means
  /// empty once it is.
  void Function() subscribeQueryAuthority(
    String queryHash,
    QueryAuthorityCallback callback, {
    bool immediate = false,
  }) =>
      _runtime.subscribeAuthority(queryHash, callback, immediate: immediate);

  Stream<bool> queryAuthorityStream(String queryHash,
          {bool immediate = true}) =>
      _broadcast<bool>((add) =>
          _runtime.subscribeAuthority(queryHash, add, immediate: immediate));

  bool isQueryAuthoritative(String queryHash) {
    final entry = _runtime.state.queries[queryHash];
    return entry != null && isAuthoritative(entry.lifecycle);
  }

  /// "This query's rows are authoritative and complete": server membership
  /// accepted, every body local, nothing left to re-render. What a virtualized
  /// list gates its end detection on.
  bool isQuerySettled(String queryHash) =>
      sel.settled(_runtime.state, queryHash);

  /// A fluent query builder for [table].
  QueryBuilder query(String table) => QueryBuilder(
        table,
        registrar: (sql, vars, ttl, relations) =>
            queryRaw(sql, vars, ttl: ttl, relations: relations),
        subscriber: subscribeStream,
        schema: config.schema,
        logger: _logger,
      );

  /// Register a query and return a result [Stream] in one call.
  Future<Stream<List<Map<String, dynamic>>>> queryStream(
    String sql,
    Map<String, dynamic> params, {
    QueryTimeToLive ttl = defaultTtl,
  }) async =>
      subscribeStream(await queryRaw(sql, params, ttl: ttl));

  /// Report the UI reconcile time (ms) for a query. Call this after applying an
  /// update in a widget to attribute build and paint time to the query.
  void reportFrontendTiming(String queryHash, double ms) {
    if (ms.isFinite) {
      _runtime.update(r.recordPhase(queryHash, TimingPhase.frontend, ms));
    }
  }

  /// Per-query processing-time breakdown.
  QueryTimings? queryTimings(String queryHash) {
    final entry = _runtime.state.queries[queryHash];
    return entry == null ? null : sel.phaseTimings(entry);
  }

  /// Opt-in eager teardown of a query whose last subscriber has left: frees the
  /// local view and forgets the query. No-op while any subscriber remains. Most
  /// queries should NOT call this - the default keep-alive avoids
  /// re-registration churn on navigation.
  void deregisterQuery(String queryHash) {
    final entry = _runtime.state.queries[queryHash];
    if (entry == null || entry.subscribers > 0) return;
    unawaited(_runtime.run((ctx) => evictQuery(ctx, queryHash),
        lane: Lane.serial('mat:$queryHash')));
  }

  // ==================== MUTATIONS ====================

  Future<Map<String, dynamic>> create(
      String id, Map<String, dynamic> data) async {
    final out = await _write(write_saga.WriteInput(
        kind: MutationEventType.create, recordId: id, data: data));
    return out.record ?? {...data, 'id': id};
  }

  Future<Map<String, dynamic>> update(
    String table,
    String id,
    Map<String, dynamic> data, {
    UpdateOptions? options,
  }) async {
    final out = await _write(write_saga.WriteInput(
      kind: MutationEventType.update,
      recordId: id,
      data: data,
      options: options,
    ));
    return out.record ?? {...data, 'id': id};
  }

  Future<void> delete(String table, String id) async {
    await _write(
        write_saga.WriteInput(kind: MutationEventType.delete, recordId: id));
  }

  Future<write_saga.WriteResult> _write(write_saga.WriteInput input) =>
      _runtime.run((ctx) => write_saga.write(ctx, _env, input),
          lane: const Lane.serial('write'));

  /// Enqueue a backend job.
  Future<void> run(
    String backend,
    String path,
    Map<String, dynamic> payload, {
    RunOptions? options,
  }) async {
    final job =
        buildJobRecord(config.schema, backend, path, payload, options: options);
    await create('${job.tableName}:${generateId()}', job.record);
  }

  // ==================== FAILED WRITES ====================

  /// Writes the server rejected. They are undone locally and parked here rather
  /// than retried forever, so an app can show them and let the user decide.
  int get failedMutationCount => _runtime.state.failedCount;

  void Function() subscribeToFailedMutations(void Function(int count) cb) {
    cb(_runtime.state.failedCount);
    return _runtime.on(
        'tray:changed', (e) => cb((e as TrayChangedEvent).count));
  }

  Future<List<FailedMutationRow>> listFailedMutations() =>
      _runtime.run(tray_saga.listFailed);

  /// Re-apply a rejected mutation as a new optimistic write.
  Future<bool> retryFailedMutation(String mutationId) =>
      _runtime.run((ctx) => tray_saga.retryFailed(ctx, _env, mutationId),
          lane: const Lane.serial('tray'));

  Future<bool> discardFailedMutation(String mutationId) =>
      _runtime.run((ctx) => tray_saga.discardFailed(ctx, mutationId),
          lane: const Lane.serial('tray'));

  // ==================== OBSERVABILITY ====================

  int get pendingMutationCount => sel.pendingMutationCount(_runtime.state);

  void Function() subscribeToPendingMutations(void Function(int count) cb) {
    cb(pendingMutationCount);
    return _runtime.on(
        'activity:changed', (e) => cb((e as ActivityChangedEvent).pending));
  }

  /// Record ids with a local write the server has not acknowledged yet (queued
  /// in the outbox, or a debounced patch not flushed to it). The per-record
  /// counterpart of [pendingMutationCount]: what a "sending" versus "synced"
  /// indicator on one row reads.
  Set<String> get unsyncedRecordIds => sel.unsyncedRecordIds(_runtime.state);

  /// Observe [unsyncedRecordIds]. Fires immediately and again whenever the set
  /// changes, including a write acked in the same moment another is queued.
  void Function() subscribeToUnsyncedRecords(
      void Function(Set<String> recordIds) cb) {
    cb(unsyncedRecordIds);
    return _runtime.on(
        'unsynced:changed', (e) => cb((e as UnsyncedChangedEvent).recordIds));
  }

  /// How many queries are pulling rows right now: what a global spinner reads.
  int get fetchingQueryCount => sel.fetchingQueryCount(_runtime.state);

  void Function() subscribeToFetchActivity(void Function(int fetching) cb) {
    cb(fetchingQueryCount);
    return _runtime.on(
        'activity:changed', (e) => cb((e as ActivityChangedEvent).fetching));
  }

  SyncHealth get syncHealth => _runtime.state.sync.health;

  /// Observe sync health. Fires immediately with the current snapshot and again
  /// on every transition, transport changes included.
  void Function() subscribeToSyncHealth(void Function(SyncHealth health) cb) {
    cb(syncHealth);
    return _runtime.on(
        'health:changed', (e) => cb((e as HealthChangedEvent).health));
  }

  Stream<SyncHealth> syncHealthStream() =>
      _broadcast<SyncHealth>(subscribeToSyncHealth);

  // ==================== AUTH ====================

  /// Authenticate the remote connection with a raw token. Bypasses
  /// [AuthService]: use this when a token comes from outside the client.
  Future<dynamic> authenticate(String token) {
    final remote = _remote;
    if (remote == null) {
      throw StateError('authenticate() requires a remote endpoint');
    }
    remote.setAuthToken(token);
    return remote.authenticate(token);
  }

  Future<void> deauthenticate() {
    final remote = _remote;
    if (remote == null) {
      throw StateError('deauthenticate() requires a remote endpoint');
    }
    remote.setAuthToken(null);
    return remote.invalidate();
  }

  // ==================== FEATURE FLAGS & RELEASES ====================

  /// A reactive handle for feature flag [key]. Requires a remote endpoint.
  FeatureFlagHandle feature(String key,
      {String? fallback, QueryTimeToLive? ttl}) {
    final ff = _featureFlags;
    if (ff == null) throw StateError('feature() requires a remote endpoint');
    return ff.feature(key, fallback: fallback, ttl: ttl);
  }

  /// A reactive handle for app [app]'s latest announced release. Requires a
  /// remote endpoint.
  AppReleaseHandle appRelease(String app, {QueryTimeToLive? ttl}) {
    final releases = _appReleases;
    if (releases == null) {
      throw StateError('appRelease() requires a remote endpoint');
    }
    return releases.release(app, ttl: ttl);
  }

  // ==================== BUCKETS & CRDT ====================

  /// A handle to a storage bucket. Requires a remote endpoint.
  BucketHandle bucket(String name) {
    final remote = _remote;
    if (remote == null) throw StateError('bucket() requires a remote endpoint');
    return BucketHandle(name, remote,
        blobs: blobCache, namespace: blobNamespace);
  }

  Future<Never> openCrdtField(String table, String recordId, String field,
          [String? fallbackText]) =>
      throw UnimplementedError('CRDT deferred');

  void closeCrdtField(String table, String recordId, String field) =>
      throw UnimplementedError('CRDT deferred');

  // ==================== INTERNALS ====================

  /// Snapshot the circuit's rows so the next boot restores them instead of
  /// re-reading every row out of sqlite.
  ///
  /// Also taken on close and before changing accounts. A process that dies
  /// without one still primes, just
  /// from the rows: the snapshot is a shortcut, never the source of truth.
  Future<void> checkpoint() {
    if (!_initialized || _closed) return Future.value();
    return _checkpoint ??= _runtime.run((ctx) async {
      _checkpointCircuit();
    }, lane: const Lane.serial('bucket')).whenComplete(
        () => _checkpoint = null);
  }

  void _checkpointCircuit() {
    if (!_initialized) return;
    try {
      final bytes = _streamProcessor.saveStoreSnapshot();
      if (bytes != null && bytes.isNotEmpty) _holder.db.putSnapshot(bytes);
    } catch (error) {
      _logger.warn('Circuit checkpoint failed: $error');
    }
  }

  late final String _clientId = generateId().substring(0, 8);

  /// The tables the circuit primes from: every schema table the client syncs.
  List<String> _syncedTables() => [
        for (final key in config.schema.keys)
          if (key != 'access' && key != 'backends' && key != 'relationships')
            key
      ];

  PersistenceClient _resolvePersistence() {
    final pc = config.persistenceClient;
    if (pc is PersistenceClient) return pc;
    if (pc == 'memory') return MemoryPersistenceClient();
    // Default to sqlite-backed persistence so the circuit state and the auth
    // token survive restarts (with a file-backed store).
    return _sessionPersistence =
        SessionPersistence.open(config.database, _logger, () => _holder.db);
  }

  /// A broadcast stream over a callback subscription, so a `StreamBuilder` can
  /// consume anything the callback API exposes.
  Stream<T> _broadcast<T>(void Function() Function(void Function(T)) attach) {
    late StreamController<T> controller;
    void Function()? off;
    controller = StreamController<T>.broadcast(
      onListen: () => off = attach(controller.add),
      onCancel: () {
        off?.call();
        off = null;
      },
    );
    return controller.stream;
  }
}

/// Parse the target table from a `SELECT ... FROM <table>` query.
String parseTableFromSurql(String sql) {
  final match = RegExp(r'\bFROM\s+(?:ONLY\s+)?([A-Za-z_][A-Za-z0-9_]*)',
          caseSensitive: false)
      .firstMatch(sql);
  if (match == null) {
    throw ArgumentError('Could not parse table from query: $sql');
  }
  return match.group(1)!;
}

/// Feeds the circuit's view updates into the runtime.
class _StreamUpdateBridge implements StreamUpdateReceiver {
  _StreamUpdateBridge(this._runtime);
  final Runtime _runtime;

  @override
  void onStreamUpdate(update) => _runtime.dispatch(StreamUpdateEvent(update));
}

/// The remote port of a local-only client: every call fails the way an
/// unreachable server would, so the sagas take their offline paths.
class _OfflineRemote implements RemotePort {
  @override
  Future<List<StatementResult>> queryStatements(String sql,
          [Map<String, dynamic>? vars]) =>
      Future.error(StateError('No remote endpoint is configured'));

  @override
  Future<String> live(String table,
          void Function(List<String>, List<InlineRow>?) onChange) =>
      Future.error(StateError('No remote endpoint is configured'));

  @override
  Future<void> kill(String uuid) async {}
}

/// What the feature-flag and app-release modules need from the client: register
/// a shared query and subscribe to it.
class _QueryHost implements QueryHost {
  _QueryHost(this._client);
  final Sp00kyClient _client;

  @override
  Future<String> registerQuery(String table, String surql,
          Map<String, dynamic> params, QueryTimeToLive ttl) =>
      _client.queryRaw(surql, params, ttl: ttl);

  @override
  void Function() subscribe(String hash, QueryUpdateCallback cb,
          {bool immediate = false}) =>
      _client.subscribe(hash, cb, immediate: immediate);
}

/// Re-export so callers can build ids without importing the surreal layer.
String recordIdString(String table, Object id) => RecordId(table, id).encode();
