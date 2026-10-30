import 'dart:async';
import 'dart:io';
import 'package:meta/meta.dart';
import 'client/worker_client.dart';
import 'modules/channel/channel.dart';
import 'modules/channel/webrtc.dart';
import 'modules/auth/sp00ky_auth.dart';
import 'modules/query_builder.dart';
import 'modules/query_host.dart';
import 'modules/bucket.dart';
import 'modules/feature_flag/feature_flag.dart';
import 'modules/app_release/app_release.dart';
import 'modules/push/push.dart';
import 'modules/ref_tables.dart' show anonUserId, bucketIdForUser;
import 'mutation/rows.dart';
import 'services/blobs/blob_cache.dart';
import 'services/logger/logger.dart';
import 'state/client_state.dart';
import 'typed/typed_query.dart' show TypedQuery;
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
  void setOffline(bool offline);
  Future<void> detach();
  Future<ClientState> inspectState();
  Future<String> queryRaw(String sql, Map<String, dynamic> params,
      {QueryTimeToLive ttl = defaultTtl,
      List<RelationPlan> relations = const []});
  Future<List<dynamic>> queryRemote(String sql, [Map<String, dynamic>? vars]);
  Future<void> preload(String sql, Map<String, dynamic> params,
      {QueryTimeToLive ttl = defaultTtl});
  /// A registered query's rows as a broadcast [Stream]. With `@channel` tables
  /// in the schema, channel peers' values are folded in (output only, never the
  /// store).
  Stream<List<Map<String, dynamic>>> subscribeStream(String hash,
      {bool immediate = true}) {
    final raw = subscribeRecords(hash, immediate: immediate);
    final channels = activeChannels;
    if (channels == null) return raw;
    late StreamController<List<Map<String, dynamic>>> controller;
    StreamSubscription<List<Map<String, dynamic>>>? sub;
    void Function()? stop;
    controller = StreamController.broadcast(onListen: () {
      final tap = channels.tap(controller.add);
      stop = tap.stop;
      sub = raw.listen(tap.feed,
          onError: controller.addError, onDone: controller.close);
    }, onCancel: () {
      stop?.call();
      stop = null;
      unawaited(sub?.cancel());
      sub = null;
    });
    return controller.stream;
  }

  /// The rows as the engine renders them, before any channel overlay.
  @protected
  Stream<List<Map<String, dynamic>>> subscribeRecords(String hash,
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
  Future<Map<String, dynamic>> create(String id, Map<String, dynamic> data) =>
      _writeCreate(id, data, null);
  Future<Map<String, dynamic>> update(
          String table, String id, Map<String, dynamic> data,
          {UpdateOptions? options}) =>
      _writeUpdate(table, id, data, options, null);
  Future<void> delete(String table, String id) => _writeDelete(table, id, null);

  /// The optimistic write itself, without channel peers.
  @protected
  Future<Map<String, dynamic>> createRecord(
      String id, Map<String, dynamic> data);
  @protected
  Future<Map<String, dynamic>> updateRecord(
      String table, String id, Map<String, dynamic> data,
      {UpdateOptions? options});
  @protected
  Future<void> deleteRecord(String table, String id);
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

  /// One key of the client's persistence, for modules that keep per-device
  /// state (the account's bucket under session persistence). `null` removes.
  Future<Object?> kvRead(String key);
  Future<void> kvWrite(String key, Object? value);

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

  PushModule? _push;

  /// Native push registration for this device (APNs / FCM). The token comes
  /// from the platform (`spooky_push` in Flutter). The sign-out hook is
  /// installed on first use, so touch it early.
  PushModule get push => _push ??= PushModule(
      remote: queryRemote,
      auth: auth,
      storage: _KvStorage(this),
      logger: SpookyLogger.root())
    ..attach();

  // ---- peer to peer channels ------------------------------------------------

  /// Open a peer to peer channel to [peers] (`table:id` strings or
  /// [RecordId]s; your own id is skipped, and a connection forms only when
  /// both sides list each other) for the rows [queries] match: a
  /// [QueryBuilder] or [TypedQuery] on a `-- @channel` table, or a list of
  /// them. Only the `where` counts, evaluated against each row; the query is
  /// never registered. Writes to matching rows (plain or the channel's own)
  /// reach the peers, and theirs show in this client's query results before
  /// the database has them. Close it when done.
  Sp00kyChannel channel(Object queries, Iterable<Object?> peers) {
    if (!hostsChannels) {
      throw StateError('Channels are opened on the client the app holds.');
    }
    final channels = _channelManager ??= _buildChannels();
    final handle = channels.open(_channelScopes(channels, queries), peers);
    _bindChannelUser(channels);
    return Sp00kyChannel._(this, channels, handle);
  }

  /// False for the client a sync worker runs: channels live on the caller's
  /// side of the isolate boundary.
  @protected
  bool get hostsChannels => true;

  /// The channel manager when the schema has `@channel` tables, else null.
  @protected
  ChannelManager? get activeChannels {
    if (!hostsChannels) return null;
    final channels = _channelManager ??= _buildChannels();
    return channels.enabled ? channels : null;
  }

  /// A channel peer created a row: into the store and the circuit unconfirmed
  /// for [ttlMs] (the `peerCreate` saga). False when refused.
  @internal
  Future<bool> peerCreate(String id, Map<String, dynamic> row, int ttlMs);

  /// A channel peer deleted a row: hidden until confirmed (the `peerDelete`
  /// saga). False when there was nothing to do.
  @internal
  Future<bool> peerDelete(String id, int ttlMs);

  /// The row as the local store holds it, or null.
  @internal
  Future<Map<String, dynamic>?> readRow(String id);

  /// Where WebRTC signals go: rows of `_00_channel_signal`.
  @internal
  SignalPort get channelSignal;

  ChannelManager? _channelManager;
  bool _channelUserBound = false;
  void Function()? _channelAuthOff;

  ChannelManager _buildChannels() {
    final cfg = config.channels;
    final logger = SpookyLogger.root('spooky.channel');
    return ChannelManager(
      config.schema,
      config: cfg,
      logger: logger,
      rows: ChannelRows(
        create: (_, id, row, ttlMs) => peerCreate(id, row, ttlMs),
        delete: (_, id, ttlMs) => peerDelete(id, ttlMs),
        read: readRow,
      ),
      transport: cfg?.transport ??
          (hooks) => webrtcTransport(
              hooks,
              WebRtcOptions(
                signal: channelSignal,
                rtc: cfg?.rtc,
                iceServers: cfg?.iceServers,
                logger: logger,
              )),
    );
  }

  /// Follow the signed-in user once a channel is open: who this device is to
  /// its peers.
  void _bindChannelUser(ChannelManager channels) {
    if (_channelUserBound) return;
    _channelUserBound = true;
    unawaited(init().then((_) {
      if (_channelAuthOff != null || !identical(_channelManager, channels)) {
        return;
      }
      _channelAuthOff = auth.subscribe(channels.setUser);
    }).catchError((Object error) {
      SpookyLogger.root('spooky.channel')
          .warn('channels have no signed-in user: $error');
    }));
  }

  List<ChannelScope> _channelScopes(ChannelManager channels, Object queries) {
    final list = queries is Iterable ? queries.toList() : [queries];
    return [
      for (final q in list)
        channels.scope(switch (q) {
          QueryBuilder b => b,
          TypedQuery t => t.builder,
          _ => throw ArgumentError(
              "channel(): pass a query, e.g. client.query('message')"
              ".where({'conversation': id})"),
        }),
    ];
  }

  // Peers go first in all three: the local write costs a transaction they
  // need not wait for. `@ephemeral` fields never reach the database.

  Future<Map<String, dynamic>> _writeCreate(
      String id, Map<String, dynamic> data, ChannelHandle? scope) async {
    final channels = activeChannels;
    if (channels == null) return createRecord(id, data);
    final table = _tableOf(id);
    final (:persistent, :ephemeral) = channels.split(table, data);
    channels.localCreate(table, id, data, scope);
    final out = await createRecord(id, persistent);
    return {...out, ...?ephemeral};
  }

  Future<Map<String, dynamic>> _writeUpdate(
      String table,
      String id,
      Map<String, dynamic> data,
      UpdateOptions? options,
      ChannelHandle? scope) async {
    final channels = activeChannels;
    if (channels == null) {
      return updateRecord(table, id, data, options: options);
    }
    final tableName = table.isEmpty ? _tableOf(id) : table;
    final (:persistent, :ephemeral) = channels.split(tableName, data);
    channels.local(tableName, id, persistent, ephemeral, scope);
    if (ephemeral != null && persistent.isEmpty) return {...data, 'id': id};
    final out = await updateRecord(table, id, persistent, options: options);
    return {...out, ...?ephemeral};
  }

  Future<void> _writeDelete(
      String table, String id, ChannelHandle? scope) async {
    final channels = activeChannels;
    // Awaited: the filter is checked against the row, which the delete removes.
    await channels?.localDelete(table.isEmpty ? _tableOf(id) : table, id, scope);
    await deleteRecord(table, id);
  }

  void closeModules() {
    _channelAuthOff?.call();
    _channelAuthOff = null;
    _channelManager?.close();
    _flags?.closeAll();
    _releases?.closeAll();
    _push?.dispose();
    _blobsAuthUnsubscribe?.call();
    _blobsAuthUnsubscribe = null;
  }

  BlobCache? _blobs;
  void Function()? _blobsAuthUnsubscribe;

  /// The one blob cache for this client, built on first use from
  /// [Sp00kyConfig.blobCache]. Misses read the bucket through [queryRemote];
  /// the raw handle used for that has no cache of its own.
  BlobCache get blobCache => _blobs ??= _createBlobCache();

  BlobCache _createBlobCache() {
    final cfg = config.blobCache;
    final dir = cfg.directory;
    final cache = BlobCache(
      store: dir == null ? MemoryBlobStore() : FileBlobStore(Directory(dir)),
      fetchRemote: (key) async => bucketContentToBytes(
          await BucketHandle.withQuery(key.bucket, queryRemote).get(key.path)),
      logger: SpookyLogger.root(),
      maxBytes: cfg.maxBytes,
      namespace: isLocalReady ? blobNamespace() : anonUserId,
    );
    if (cfg.clearOnSignOut && config.database.endpoint != null) {
      String? lastUser = auth.currentUser?['id']?.toString();
      _blobsAuthUnsubscribe = auth.subscribe((userId) {
        final previous = lastUser;
        lastUser = userId;
        if (userId == null && previous != null) {
          unawaited(cache.clear(bucketIdForUser(previous)));
        }
      });
    }
    return cache;
  }

  /// The cache namespace for the signed-in user: the same per-user bucket id
  /// the local store is split by, so two accounts on one device never see
  /// each other's files.
  String blobNamespace() {
    if (config.database.endpoint == null) return anonUserId;
    try {
      return bucketIdForUser(auth.currentUser?['id']);
    } on StateError {
      // Auth not wired yet (an in-process client before init): anonymous.
      return anonUserId;
    }
  }

  /// Snapshot of the blob cache's counters, for diagnostics.
  BlobCacheStats get blobCacheStats => blobCache.stats;

  BucketHandle bucket(String name) => BucketHandle.withQuery(name, queryRemote,
      blobs: blobCache, namespace: blobNamespaceWhenReady);

  /// [blobNamespace] once the local boot has restored the session. Before
  /// that the user is not known yet, and a cover read would be filed (and
  /// fetched) under the anonymous namespace.
  Future<String> blobNamespaceWhenReady() async {
    try {
      await init();
    } catch (_) {
      // A client that cannot boot still serves what it can.
    }
    return blobNamespace();
  }

  Future<Never> openCrdtField(String table, String recordId, String field,
          [String? fallbackText]) =>
      throw UnimplementedError('CRDT deferred');
  void closeCrdtField(String table, String recordId, String field) =>
      throw UnimplementedError('CRDT deferred');
}

String _tableOf(String id) {
  final i = id.indexOf(':');
  return i < 0 ? id : id.substring(0, i);
}

/// An open peer to peer channel (`client.channel(queries, peers)`): its state,
/// plus writes that go to ITS peers only (when the row matches its filter).
/// Plain `create` / `update` / `delete` reach the peers of every open channel
/// whose filter the row matches.
class Sp00kyChannel {
  Sp00kyChannel._(this._client, this._manager, this._handle);

  final Sp00kyClient _client;
  final ChannelManager _manager;
  final ChannelHandle _handle;

  /// The tables its queries read.
  List<String> get tables => _handle.tables;

  ChannelState get state => _handle.state;

  /// The current state at once, then every change.
  Stream<ChannelState> get states => broadcast(_handle.subscribe);

  /// Called at once with the current state, then on every change.
  void Function() subscribe(void Function(ChannelState) cb) =>
      _handle.subscribe(cb);

  /// Replace who it connects to (your own id is skipped).
  void setPeers(Iterable<Object?> peers) => _handle.setPeers(peers);

  /// Replace the queries (their filters changed).
  void setQueries(Object queries) =>
      _handle.setScopes(_client._channelScopes(_manager, queries));

  bool get isClosed => _handle.isClosed;

  void close() => _handle.close();

  ChannelHandle _scoped(String table) {
    _manager.assertCarries(_handle, table);
    return _handle;
  }

  Future<Map<String, dynamic>> update(
      String table, String id, Map<String, dynamic> data,
      {UpdateOptions? options}) async {
    final scope = _scoped(table.isEmpty ? _tableOf(id) : table);
    return _client._writeUpdate(table, id, data, options, scope);
  }

  Future<Map<String, dynamic>> create(
      String id, Map<String, dynamic> data) async {
    final scope = _scoped(_tableOf(id));
    return _client._writeCreate(id, data, scope);
  }

  Future<void> delete(String table, String id) async {
    final scope = _scoped(table.isEmpty ? _tableOf(id) : table);
    return _client._writeDelete(table, id, scope);
  }
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

class _KvStorage implements PersistenceClient {
  _KvStorage(this.client);
  final Sp00kyClient client;
  @override
  Future<void> set(String key, dynamic value) => client.kvWrite(key, value);
  @override
  Future<T?> get<T>(String key) async => await client.kvRead(key) as T?;
  @override
  Future<void> remove(String key) => client.kvWrite(key, null);
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
