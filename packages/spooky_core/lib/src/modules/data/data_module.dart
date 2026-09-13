import 'dart:async';
import 'dart:convert';

import 'package:crypto/crypto.dart';

import '../../events/event_system.dart' show PushEventOptions, DebouncedConfig;
import '../../ffi/stream_update.dart';
import '../../services/database/local_database_service.dart';
import '../../services/logger/logger.dart';
import '../../services/stream_processor/stream_processor_service.dart';
import '../../surreal/value.dart';
import '../../types.dart';
import '../../utils/duration_utils.dart';
import '../../utils/parser.dart';
import '../../utils/record_id_utils.dart';
import '../../utils/sort_rows.dart';
import '../cache/cache_module.dart';
import '../query_builder.dart' show RelationPlan;
import '../sync/queue/queue_up.dart';
import 'relation_resolver.dart';
import '../../query/window_query.dart';

/// Unified query and mutation management (TS `DataModule`).
///
/// Read-path divergence from the JS core: the JS re-runs the registered SURQL
/// against the local SurrealDB after each stream update to fetch record bodies.
/// sqlite can't run SurrealQL, so this resolves the result set from the DBSP
/// `localArray` (`[id, version]` pairs) via [LocalDatabaseService.getById].
class DataModule {
  DataModule(
    this._cache,
    this._local,
    this._schema,
    SpookyLogger logger, {
    int streamDebounceTime = 100,
    void Function(String hash)? onHeartbeat,
    void Function(String hash)? onDeregister,
    bool bornFetching = true,
  })  : _logger = logger.child('DataModule'),
        _streamDebounceTime = streamDebounceTime,
        _onHeartbeat = onHeartbeat,
        _onDeregister = onDeregister,
        _bornFetching = bornFetching;

  final CacheModule _cache;
  final LocalDatabaseService _local;
  final Map<String, dynamic>
      _schema; // table -> { columns: {name: ColumnSchema} }
  final SpookyLogger _logger;
  final int _streamDebounceTime;

  /// Called when a query's TTL heartbeat fires (TS leaves this a TODO; here it
  /// is wired so the client can re-register/heartbeat the query before the
  /// server expires it). Null in local-only mode (timer just reschedules).
  final void Function(String hash)? _onHeartbeat;

  /// Fired by [deregisterQuery] when an opt-in query's last subscriber leaves,
  /// so the client can enqueue the remote `_00_query` cleanup. Null in
  /// local-only mode (then [deregisterQuery] is a no-op, matching the TS core).
  final void Function(String hash)? _onDeregister;

  /// Whether a new query starts [QueryStatus.fetching]. True with a remote
  /// (a `register` down-event follows every registration and settles it);
  /// false in local-only mode, where nothing would ever settle it.
  final bool _bornFetching;

  final Map<String, QueryState> _activeQueries = {};
  final Map<String, Future<String>> _pendingQueries = {};
  final Map<String, Set<QueryUpdateCallback>> _subscriptions = {};
  final Map<String, Set<QueryStatusCallback>> _statusSubscriptions = {};
  final Set<MutationCallback> _mutationCallbacks = {};
  final Map<String, Timer> _debounceTimers = {};

  /// Nesting depth of overlapping fetch cycles per query; see [beginFetching].
  final Map<String, int> _fetchDepth = {};

  /// The debounced stream update awaiting its trailing-edge flush, per query.
  /// Kept so [flushPendingStreamUpdate] can land it early, before a status flip.
  final Map<String, StreamUpdate> _pendingStreamUpdates = {};

  String _sessionId = '';
  String? _currentUserId;

  Future<void> init(String sessionId) async {
    _sessionId = sessionId;
  }

  void setSessionId(String sessionId) => _sessionId = sessionId;
  void setCurrentUserId(String? userId) => _currentUserId = userId;
  String? getCurrentUserId() => _currentUserId;

  // ==================== QUERY MANAGEMENT ====================

  /// Register a query and return its hash (TS `query`).
  Future<String> query(
    String tableName,
    String surqlString,
    Map<String, dynamic> params,
    QueryTimeToLive ttl, {
    List<RelationPlan> relations = const [],
  }) async {
    final hash = calculateHash({'surql': surqlString, 'params': params});
    final recordId = RecordId('_00_query', hash);

    if (_activeQueries.containsKey(hash)) return hash;

    final pending = _pendingQueries[hash];
    if (pending != null) {
      await pending;
      return hash;
    }

    final promise = _createAndRegisterQuery(
        hash, recordId, surqlString, params, ttl, tableName, relations);
    _pendingQueries[hash] = promise;
    try {
      await promise;
    } finally {
      _pendingQueries.remove(hash);
    }
    return hash;
  }

  /// Subscribe to query updates (TS `subscribe`). Returns an unsubscribe fn.
  void Function() subscribe(
    String queryHash,
    QueryUpdateCallback callback, {
    bool immediate = false,
  }) {
    final subs = _subscriptions.putIfAbsent(queryHash, () => {});
    subs.add(callback);

    if (immediate) {
      final query = _activeQueries[queryHash];
      if (query != null) callback(query.records);
    }

    return () {
      final s = _subscriptions[queryHash];
      if (s != null) {
        s.remove(callback);
        if (s.isEmpty) _subscriptions.remove(queryHash);
      }
    };
  }

  /// Subscribe to a query's fetch-status changes (idle/fetching) (TS
  /// `subscribeStatus`). With [immediate], fires synchronously with the current
  /// status (defaults to [QueryStatus.idle] if the query isn't registered yet).
  /// Returns an unsubscribe fn.
  void Function() subscribeStatus(
    String queryHash,
    QueryStatusCallback callback, {
    bool immediate = false,
  }) {
    final subs = _statusSubscriptions.putIfAbsent(queryHash, () => {});
    subs.add(callback);

    if (immediate) {
      callback(_activeQueries[queryHash]?.status ?? QueryStatus.idle);
    }

    return () {
      final s = _statusSubscriptions[queryHash];
      if (s != null) {
        s.remove(callback);
        if (s.isEmpty) _statusSubscriptions.remove(queryHash);
      }
    };
  }

  /// Set a query's fetch status and notify status subscribers (TS
  /// `setQueryStatus`). No-op when the status is unchanged or the query is
  /// unknown. (The TS `onQueryStatusChange` DevTools observer is omitted here:
  /// DevTools is not ported to the Dart core.)
  void setQueryStatus(String queryHash, QueryStatus status) {
    final qs = _activeQueries[queryHash];
    if (qs == null || qs.status == status) return;
    qs.status = status;

    final subs = _statusSubscriptions[queryHash];
    if (subs == null) return;
    for (final callback in subs.toList()) {
      callback(status);
    }
  }

  /// Enter a fetch cycle for a query (TS `beginFetching`). Refcounted:
  /// registration and concurrent poll/LIVE sync rounds can overlap on the same
  /// hash, and only the OUTERMOST cycle may flip the status. 0 -> 1 emits
  /// `fetching`. Always pair with [endFetching] in a `finally`.
  void beginFetching(String queryHash) {
    final depth = _fetchDepth[queryHash] ?? 0;
    _fetchDepth[queryHash] = depth + 1;
    if (depth == 0) setQueryStatus(queryHash, QueryStatus.fetching);
  }

  /// Leave a fetch cycle started with [beginFetching]; the last exit emits
  /// `idle` (TS `endFetching`).
  void endFetching(String queryHash) {
    final depth = _fetchDepth[queryHash] ?? 0;
    if (depth <= 1) {
      _fetchDepth.remove(queryHash);
      setQueryStatus(queryHash, QueryStatus.idle);
      return;
    }
    _fetchDepth[queryHash] = depth - 1;
  }

  /// Subscribe to mutations (TS `onMutation`).
  void Function() onMutation(MutationCallback callback) {
    _mutationCallbacks.add(callback);
    return () => _mutationCallbacks.remove(callback);
  }

  /// Handle a stream update from DBSP (TS `onStreamUpdate`). UPDATE is
  /// debounced; CREATE/DELETE propagate immediately.
  Future<void> onStreamUpdate(StreamUpdate update) async {
    final hash = update.queryHash;
    if (update.op == 'UPDATE') {
      _debounceTimers[hash]?.cancel();
      _pendingStreamUpdates[hash] = update;
      _debounceTimers[hash] =
          Timer(Duration(milliseconds: _streamDebounceTime), () {
        _debounceTimers.remove(hash);
        _pendingStreamUpdates.remove(hash);
        _processStreamUpdate(update);
      });
      return;
    }
    // An immediate update carries the full latest localArray, so any coalesced
    // UPDATE it supersedes is already reflected — drop the pending entry so the
    // flush path can't replay it.
    _debounceTimers.remove(hash)?.cancel();
    _pendingStreamUpdates.remove(hash);
    await _processStreamUpdate(update);
  }

  /// Process a query's pending (debounced) stream update NOW instead of on the
  /// trailing edge (TS `flushPendingStreamUpdate`). Called by sync before it
  /// flips a query back to `idle`, so the status change never races ahead of the
  /// rows it fetched. No-op when nothing is pending. The pending entry is
  /// removed before the await, so a concurrently-firing timer can't process it
  /// twice.
  Future<void> flushPendingStreamUpdate(String queryHash) async {
    _debounceTimers.remove(queryHash)?.cancel();
    final pending = _pendingStreamUpdates.remove(queryHash);
    if (pending == null) return;
    await _processStreamUpdate(pending);
  }

  Future<void> _processStreamUpdate(StreamUpdate update) async {
    final queryState = _activeQueries[update.queryHash];
    if (queryState == null) {
      _logger.warn('Update for unknown query ${update.queryHash}; skipping');
      return;
    }

    final ms = update.materializationTimeMs;
    if (ms != null) {
      queryState.materializationSamples.add(ms);
      if (queryState.materializationSamples.length >
          materializationSampleWindow) {
        queryState.materializationSamples.removeAt(0);
      }
      queryState.lastIngestLatencyMs = ms;
    }

    try {
      // Materialize against the incoming array BEFORE it is cached, so a
      // windowed query can prefer the authoritative remoteArray and still fall
      // back to this update's own view.
      final newRecords = _materialize(queryState, sspArray: update.localArray);
      queryState.config.localArray = update.localArray;

      final prevJson = jsonEncode(queryState.records);
      final newJson = jsonEncode(newRecords);
      queryState.records = newRecords;
      final recordsChanged = prevJson != newJson;
      if (recordsChanged) queryState.updateCount++;

      _persistQueryStats(queryState);

      if (!recordsChanged) return;
      _notify(update.queryHash, queryState.records);
    } catch (err) {
      queryState.errorCount++;
      _logger.error('Failed to materialize stream update', err);
      _local.patchQueryConfig(encodeRecordId(queryState.config.id),
          {'errorCount': queryState.errorCount});
    }
  }

  /// Resolve a query's result rows from local sqlite.
  ///
  /// Normal queries materialize the DBSP view's `localArray` in circuit order.
  /// A windowed query (`LIMIT n START m`, m>0) instead takes its id-set from the
  /// server's authoritative `_00_list_ref` (`remoteArray`) when it has one,
  /// falling back to the incoming stream update's array and then the cached
  /// `localArray`, and re-applies the query's `ORDER BY` in Dart. Deriving page 2
  /// from whatever rows happen to be resident locally returns the wrong page (or
  /// none at all). Mirrors TS `materializeRecords`.
  List<Map<String, dynamic>> _materialize(
    QueryState qs, {
    RecordVersionArray? sspArray,
  }) {
    final window = buildWindowMaterialization(qs.config.surql);
    final RecordVersionArray ids;
    if (window == null) {
      // The circuit's view IS the result set for a normal query: prefer the
      // array from the update being processed over the cached one, which is
      // still the previous view at this point.
      ids = sspArray ?? qs.config.localArray;
    } else if (qs.config.remoteArray.isNotEmpty) {
      ids = qs.config.remoteArray;
    } else if (sspArray != null && sspArray.isNotEmpty) {
      ids = sspArray;
    } else {
      ids = qs.config.localArray;
    }

    final watch = Stopwatch()..start();
    var out = <Map<String, dynamic>>[];
    for (final pair in ids) {
      final record = _local.getById(pair.$1);
      if (record != null) out.add(record);
    }
    if (window != null) out = sortRows(out, window.orderBy);

    if (qs.config.relations.isNotEmpty) {
      // Copy each row before attaching aliases: the maps come straight from
      // `getById`, and a caller holding an earlier result must not see relation
      // keys appear on it retroactively.
      out = [for (final row in out) {...row}];
      try {
        resolveRelations(out, qs.config.relations, _relationFetcher);
      } catch (err) {
        _logger.error('Failed to resolve query relations', err);
      }
    }

    _recordPhase(qs, TimingPhase.localFetch, watch.elapsedMicroseconds / 1000);
    return out;
  }

  late final RelationFetcher _relationFetcher = LocalRelationFetcher(_local);

  void _persistQueryStats(QueryState qs) {
    final p = _computePercentiles(qs.materializationSamples);
    _local.patchQueryConfig(encodeRecordId(qs.config.id), {
      'localArray': _encodeVersionArray(qs.config.localArray),
      'rowCount': qs.config.localArray.length,
      'updateCount': qs.updateCount,
      'lastIngestLatency': qs.lastIngestLatencyMs,
      'materializationP55': p.$1,
      'materializationP90': p.$2,
      'materializationP99': p.$3,
    });
  }

  (double?, double?, double?) _computePercentiles(List<double> samples) {
    if (samples.isEmpty) return (null, null, null);
    final sorted = [...samples]..sort();
    double pick(double q) {
      final idx = (q * sorted.length).floor();
      return sorted[idx >= sorted.length ? sorted.length - 1 : idx];
    }

    return (pick(0.55), pick(0.90), pick(0.99));
  }

  /// Record a per-phase timing sample (ms) on a query's rolling window
  /// (TS `recordPhase`). Non-finite samples are ignored.
  void _recordPhase(QueryState qs, String phase, double ms) {
    if (!ms.isFinite) return;
    final samples = qs.phaseSamples.putIfAbsent(phase, () => <double>[]);
    samples.add(ms);
    if (samples.length > materializationSampleWindow) samples.removeAt(0);
    qs.phaseLast[phase] = ms;
  }

  /// Record the remote record-fetch time (ms) for a query (TS
  /// `recordRemoteFetch`). Called by [Sp00kySync] after a record-body fetch.
  void recordRemoteFetch(String hash, double ms) {
    final qs = _activeQueries[hash];
    if (qs != null) _recordPhase(qs, TimingPhase.remoteFetch, ms);
  }

  /// Record the UI reconcile time (ms) for a query (TS `recordFrontendTiming`).
  /// Called from a Flutter widget via `Sp00kyClient.reportFrontendTiming` after
  /// it applies an update.
  void recordFrontendTiming(String hash, double ms) {
    final qs = _activeQueries[hash];
    if (qs != null) _recordPhase(qs, TimingPhase.frontend, ms);
  }

  /// Percentile summary for one timed [phase] of a query (TS `phaseStatOf`).
  /// The `ssp` native-ingest phase lives in [QueryState.materializationSamples];
  /// pass [TimingPhase] values for the rest.
  PhaseStat phaseStat(String hash, String phase) {
    final qs = _activeQueries[hash];
    if (qs == null) return const PhaseStat();
    return _phaseStatOf(
        qs.phaseSamples[phase] ?? const [], qs.phaseLast[phase]);
  }

  PhaseStat _phaseStatOf(List<double> samples, double? lastMs) {
    if (samples.isEmpty) return PhaseStat(lastMs: lastMs);
    final sorted = [...samples]..sort();
    double pick(double q) {
      final idx = (q * sorted.length).floor();
      return sorted[idx >= sorted.length ? sorted.length - 1 : idx];
    }

    return PhaseStat(
      lastMs: lastMs,
      p50: pick(0.5),
      p90: pick(0.9),
      p99: pick(0.99),
      count: samples.length,
    );
  }

  QueryState? getQueryByHash(String hash) => _activeQueries[hash];
  QueryState? getQueryById(RecordId id) => _activeQueries[extractIdPart(id)];
  List<QueryState> getActiveQueries() => _activeQueries.values.toList();
  List<String> getActiveQueryHashes() => _activeQueries.keys.toList();

  /// Persist a query's local version array (TS `updateQueryLocalArray`).
  Future<void> updateQueryLocalArray(
      String id, RecordVersionArray localArray) async {
    final qs = _activeQueries[id];
    if (qs == null) return;
    qs.config.localArray = localArray;
    _local.patchQueryConfig(encodeRecordId(qs.config.id),
        {'localArray': _encodeVersionArray(localArray)});
  }

  /// Persist a query's remote version array (TS `updateQueryRemoteArray`).
  Future<void> updateQueryRemoteArray(
      String hash, RecordVersionArray remoteArray) async {
    final qs = getQueryByHash(hash);
    if (qs == null) return;
    qs.config.remoteArray = remoteArray;
    _local.patchQueryConfig(encodeRecordId(qs.config.id),
        {'remoteArray': _encodeVersionArray(remoteArray)});
  }

  /// Roll back a failed optimistic create (TS `rollbackCreate`).
  Future<void> rollbackCreate(RecordId recordId, String tableName) async {
    final id = encodeRecordId(recordId);
    _local.delete(id);
    await _cache.delete(tableName, id, skipDbDelete: true);
    _removeRecordFromQueries(recordId);
  }

  /// Roll back a failed optimistic update by restoring [beforeRecord]
  /// (TS `rollbackUpdate`).
  Future<void> rollbackUpdate(
    RecordId recordId,
    String tableName,
    Map<String, dynamic> beforeRecord,
  ) async {
    final id = encodeRecordId(recordId);
    final content = {...beforeRecord}..remove('id');
    _local.replace(id, content);
    await _cache.save(
      CacheRecord(
        table: tableName,
        op: 'UPDATE',
        record: beforeRecord,
        version: ((beforeRecord['_00_rv'] as num?) ?? 1).toInt(),
      ),
      skipDbInsert: true,
    );
    _replaceRecordInQueries(beforeRecord);
  }

  void _removeRecordFromQueries(RecordId recordId) {
    final encodedId = encodeRecordId(recordId);
    for (final entry in _activeQueries.entries) {
      final records = entry.value.records;
      final index = records.indexWhere((r) {
        final rid = r['id'] is RecordId
            ? encodeRecordId(r['id'] as RecordId)
            : r['id'].toString();
        return rid == encodedId;
      });
      if (index != -1) {
        records.removeAt(index);
        _notify(entry.key, records);
      }
    }
  }

  /// Notify subscribers after a query's initial sync (TS `notifyQuerySynced`).
  /// Emits on first sync even with no results, so a `StreamBuilder` stops
  /// loading on an empty set.
  ///
  /// The emit gate is the ephemeral [QueryState.syncNotified] flag rather than a
  /// persisted counter: a re-registered query (new subscriber on a hash whose
  /// `updateCount` survived in `_00_query`) must still emit once, otherwise an
  /// unchanged empty window leaves the consumer loading forever.
  Future<void> notifyQuerySynced(String queryHash) async {
    final qs = _activeQueries[queryHash];
    if (qs == null) return;
    final newRecords = _materialize(qs);
    final changed = jsonEncode(qs.records) != jsonEncode(newRecords);
    qs.records = newRecords;
    if (changed || !qs.syncNotified) {
      qs.syncNotified = true;
      qs.updateCount++;
      _notify(queryHash, qs.records);
    }
  }

  // ==================== HYDRATION & PRELOAD ====================

  /// Cold-query guard for instant-hydrate (TS `isCold`): true when the query
  /// exists, hasn't hydrated, and has NOT yet fetched its server result
  /// (`remoteArray` empty).
  ///
  /// Gated on `remoteArray`, not on local records: a windowed query is often
  /// partially pre-seeded from the circuit but still hasn't loaded its own window
  /// from the server, so it should still hydrate. A warm re-subscribe
  /// (remoteArray already populated) is skipped.
  bool isCold(String hash) {
    final qs = _activeQueries[hash];
    return qs != null && !qs.hydrated && qs.config.remoteArray.isEmpty;
  }

  /// Instant-hydrate (TS `applyHydration`): ingest rows fetched one-shot from the
  /// remote so the query displays immediately while the realtime registration
  /// proceeds. Ingests with versions (`_00_rv`) so the later record-body sync
  /// skips unchanged rows, and seeds `remoteArray` so a windowed query
  /// materializes the correct window.
  ///
  /// Sets [QueryState.hydrated] before any early return, so [isCold] — the guard
  /// the caller checks — closes for good even when the remote returned nothing.
  Future<void> applyHydration(
      String hash, List<Map<String, dynamic>> rows) async {
    final qs = _activeQueries[hash];
    if (qs == null) return;
    qs.hydrated = true; // run-once, even when the remote returned nothing
    if (rows.isEmpty) return;

    await _buildAndSaveCacheBatch(qs.config.tableName, rows);

    // Prime remoteArray from the hydrated id/version pairs: materialization
    // prefers it for windowed queries and it feeds the version dedup.
    // Registration later overwrites it with the authoritative `_00_list_ref`.
    qs.config.remoteArray = [
      for (final row in rows)
        (row['id'].toString(), ((row['_00_rv'] as num?) ?? 1).toInt()),
    ];

    qs.records = _materialize(qs);
    _notify(hash, qs.records);
  }

  /// Preload/prewarm (TS `persistSnapshot`): persist one-shot rows (and their
  /// embedded related children) into the local cache WITHOUT registering a query
  /// — no active-query entry, no `_00_query` view, no TTL heartbeat. The rows
  /// live in sqlite as ordinary bodies so a later query seeds its first paint
  /// from them instantly, then registers a live view to freshen.
  Future<void> persistSnapshot(
      String tableName, List<Map<String, dynamic>> rows) async {
    if (rows.isEmpty) return;
    await _buildAndSaveCacheBatch(tableName, rows);
  }

  /// Build and persist the cache batch for a set of one-shot rows
  /// (TS `buildAndSaveCacheBatch`). Maps each row to a `CREATE` on its own table
  /// and extracts EMBEDDED related children (at any depth) as standalone records,
  /// so a later correlated re-materialization finds them. Shared by
  /// [applyHydration] and [persistSnapshot].
  Future<void> _buildAndSaveCacheBatch(
      String tableName, List<Map<String, dynamic>> rows) async {
    final batch = <CacheRecord>[
      for (final record in rows)
        CacheRecord(
          table: tableName,
          op: 'CREATE',
          record: flattenRelationsForStorage(record, tableName),
          version: ((record['_00_rv'] as num?) ?? 1).toInt(),
        ),
    ];

    final seen = {for (final r in rows) r['id'].toString()};
    for (final record in rows) {
      collectEmbeddedChildren(record, batch, seen);
    }

    await _cache.saveBatch(batch);
  }

  /// Collect EMBEDDED related children of [record] as their own cache records
  /// (TS `collectEmbeddedChildren`).
  ///
  /// A child is any field value that is itself a record — a Map whose `id` reads
  /// as a record id — or a list of such records (one-to-many vs one-to-one). A
  /// bare record-id reference is skipped, so a foreign-key column is never
  /// mistaken for an embedded body. [seen] dedupes within the batch.
  void collectEmbeddedChildren(
    Map<String, dynamic> record,
    List<CacheRecord> batch,
    Set<String> seen,
  ) {
    for (final value in record.values) {
      final children = <Map<String, dynamic>>[];
      if (value is List) {
        for (final v in value) {
          if (_isEmbeddedRecord(v)) children.add((v as Map).cast());
        }
      } else if (_isEmbeddedRecord(value)) {
        children.add((value as Map).cast());
      }
      for (final child in children) {
        final key = child['id'].toString();
        if (!seen.add(key)) continue;
        // Recurse FIRST so nested grandchildren are captured before this child's
        // own alias fields are flattened away.
        collectEmbeddedChildren(child, batch, seen);
        final table = extractTablePart(key);
        batch.add(CacheRecord(
          table: table,
          op: 'CREATE',
          record: flattenRelationsForStorage(child, table),
          version: ((child['_00_rv'] as num?) ?? 1).toInt(),
        ));
      }
    }
  }

  /// Prepare a subquery-bearing row (preload / hydration) for the local store
  /// (TS `flattenRelationsForStorage`): replace an embedded FORWARD-relation
  /// object (`author = {id, …}`) with its record id, and DROP reverse-subquery
  /// ARRAYS (`comments = [ … ]`) whose rows are cached separately as their own
  /// bodies. A flat record — as the live `SELECT * FROM $ids` sync returns, with
  /// relations already ids — passes through unchanged.
  Map<String, dynamic> flattenRelationsForStorage(
      Map<String, dynamic> record, String tableName) {
    final flat = <String, dynamic>{};
    record.forEach((key, value) {
      if (_isEmbeddedRecord(value)) {
        flat[key] = (value as Map)['id'];
      } else if (value is List && value.any(_isEmbeddedRecord)) {
        return; // reverse subquery: cached as its own rows
      } else {
        flat[key] = value;
      }
    });
    final columns = _schema.containsKey(tableName) ? _columnsFor(tableName) : null;
    return columns != null ? cleanRecord(columns, flat) : flat;
  }

  /// Whether [value] is an embedded record body rather than a reference: a Map
  /// carrying an `id` that reads as a record id.
  bool _isEmbeddedRecord(Object? value) {
    if (value is! Map) return false;
    final id = value['id'];
    if (id is RecordId) return true;
    return id is String && id.contains(':');
  }

  /// Durable preload freshness marker for [hash], or null when this query was
  /// never preloaded (TS `getPreloadMarker`). Stored as an ordinary
  /// `_00_preload` document alongside the cached rows, so a local reset that
  /// clears the data clears the marker too — a stale marker can never claim
  /// "warm" when the rows are gone. Any read error reads as cold.
  ({int fetchedAt, int rowCount})? getPreloadMarker(String hash) {
    try {
      final row = _local.getById('_00_preload:$hash');
      if (row == null) return null;
      return (
        fetchedAt: ((row['fetchedAt'] as num?) ?? 0).toInt(),
        rowCount: ((row['rowCount'] as num?) ?? 0).toInt(),
      );
    } catch (err) {
      _logger.debug('Preload marker read failed; treating as cold: $err');
      return null;
    }
  }

  /// Stamp the preload freshness marker after a successful snapshot fetch
  /// (TS `writePreloadMarker`).
  void writePreloadMarker(String hash, int rowCount) {
    _local.replace('_00_preload:$hash', {
      'fetchedAt': DateTime.now().millisecondsSinceEpoch,
      'rowCount': rowCount,
    });
  }

  // ==================== BACKEND RUN ====================

  /// Enqueue a backend job by writing an outbox record (TS `run`). Validates
  /// the route's declared args, then reuses [create] (local write + sync).
  Future<void> run(
    String backend,
    String path,
    Map<String, dynamic> data, {
    RunOptions? options,
  }) async {
    final backends = _schema['backends'];
    final backendDef = (backends is Map ? backends[backend] : null) as Map?;
    if (backendDef == null) throw ArgumentError('Backend $backend not found');
    final route = (backendDef['routes'] as Map?)?[path] as Map?;
    if (route == null) throw ArgumentError('Route $backend.$path not found');
    final tableName = backendDef['outboxTable'] as String?;
    if (tableName == null) {
      throw ArgumentError('Outbox table for backend $backend not found');
    }

    final args = (route['args'] as Map?) ?? const {};
    final payload = <String, dynamic>{};
    args.forEach((argName, argDef) {
      final optional = argDef is Map && argDef['optional'] == true;
      if (!data.containsKey(argName) && !optional) {
        throw ArgumentError('Missing required argument $argName');
      }
      payload[argName as String] = data[argName];
    });

    final record = <String, dynamic>{
      'path': path,
      'payload': jsonEncode(payload),
      // Seed the status locally so an optimistic job row reads as `pending`
      // before the server touches it. Without it the row materializes with no
      // status and a jobs list can't tell it apart from a finished job.
      'status': 'pending',
      'max_retries': options?.maxRetries ?? 3,
      'retry_strategy': options?.retryStrategy ?? 'linear',
    };
    if (options?.timeout != null) record['timeout'] = options!.timeout;
    if (options?.delay != null) record['delay'] = options!.delay;
    if (options?.assignedTo != null)
      record['assigned_to'] = options!.assignedTo;

    await create('$tableName:${generateId()}', record);
  }

  // ==================== MUTATIONS ====================

  /// Create a record (TS `create`). Optimistic local write + DBSP ingest +
  /// mutation emission.
  Future<Map<String, dynamic>> create(
      String id, Map<String, dynamic> data) async {
    final tableName = extractTablePart(id);
    final columns = _columnsFor(tableName);
    final rid = parseRecordIdString(id);
    final params = parseParams(columns, data);
    final mutationId = RecordId('_00_pending_mutations', _nowStamp());

    final target = {...params, 'id': id, '_00_rv': 1};
    _local.tx(() {
      // Seed `_00_rv = 1` locally (the JS local schema defaults this column),
      // so a later UPDATE's `_00_rv += 1` reaches 2.
      _local.create(id, {...params, '_00_rv': 1});
      _local.putMutation(encodeRecordId(mutationId), {
        'mutationType': 'create',
        'recordId': id,
        'created_at': DateTime.now().toUtc().toIso8601String(),
      });
    });

    await _cache.save(
      CacheRecord(table: tableName, op: 'CREATE', record: target, version: 1),
      skipDbInsert: true,
    );

    final event = CreateEvent(
      mutationId: mutationId,
      recordId: rid,
      data: params,
      record: target,
      tableName: tableName,
    );
    _emitMutation([event]);
    return target;
  }

  /// Update a record (TS `update`).
  Future<Map<String, dynamic>> update(
    String table,
    String id,
    Map<String, dynamic> data, {
    UpdateOptions? options,
  }) async {
    final tableName = extractTablePart(id);
    final columns = _columnsFor(tableName);
    final rid = parseRecordIdString(id);
    final params = parseParams(columns, data);
    final mutationId = RecordId('_00_pending_mutations', _nowStamp());

    final beforeRecord = _local.getById(id);

    late Map<String, dynamic> target;
    _local.tx(() {
      _local.incrementRv(id);
      _local.updateMerge(id, params);
      _local.putMutation(encodeRecordId(mutationId), {
        'mutationType': 'update',
        'recordId': id,
        'data': _jsonSafe(params),
        'created_at': DateTime.now().toUtc().toIso8601String(),
      });
      target = _local.getById(id) ?? {'id': id, ...params};
    });

    // Build a partial record with only the fields the user changed.
    final updatedFields = <String, dynamic>{'id': target['id']};
    for (final key in data.keys) {
      if (target.containsKey(key)) updatedFields[key] = target[key];
    }
    if (target.containsKey('_00_rv'))
      updatedFields['_00_rv'] = target['_00_rv'];
    _replaceRecordInQueries(updatedFields);

    await _cache.save(
      CacheRecord(
        table: table,
        op: 'UPDATE',
        record: target,
        version: ((target['_00_rv'] as num?) ?? 1).toInt(),
      ),
      skipDbInsert: true,
    );

    final event = UpdateEvent(
      mutationId: mutationId,
      recordId: rid,
      data: params,
      record: target,
      beforeRecord: beforeRecord,
      options: parseUpdateOptions(id, data, options),
    );
    _emitMutation([event]);
    return target;
  }

  /// Delete a record (TS `delete`).
  Future<void> delete(String table, String id) async {
    final tableName = extractTablePart(id);
    _columnsFor(tableName); // validate table exists
    final rid = parseRecordIdString(id);
    final mutationId = RecordId('_00_pending_mutations', _nowStamp());

    final beforeRecord = _local.getById(id) ?? <String, dynamic>{};

    _local.tx(() {
      _local.delete(id);
      _local.putMutation(encodeRecordId(mutationId), {
        'mutationType': 'delete',
        'recordId': id,
        'created_at': DateTime.now().toUtc().toIso8601String(),
      });
    });

    await _cache.delete(table, id,
        skipDbDelete: true, recordData: beforeRecord);

    // DBSP may not emit view updates for DELETE; notify queries on this table.
    for (final entry in _activeQueries.entries) {
      if (entry.value.config.tableName == tableName) {
        await notifyQuerySynced(entry.key);
      }
    }

    _emitMutation([DeleteEvent(mutationId: mutationId, recordId: rid)]);
  }

  // ==================== INTERNALS ====================

  Future<String> _createAndRegisterQuery(
    String hash,
    RecordId recordId,
    String surqlString,
    Map<String, dynamic> params,
    QueryTimeToLive ttl,
    String tableName,
    List<RelationPlan> relations,
  ) async {
    final queryState =
        _createNewQuery(recordId, surqlString, params, ttl, tableName, relations);

    final localArray = _cache.registerQuery(QueryPlanConfig(
      queryHash: hash,
      surql: surqlString,
      params: params,
      ttl: ttl,
      lastActiveAt: DateTime.now(),
    ));

    queryState.config.localArray = localArray;
    queryState.records = _materialize(queryState);

    _local.patchQueryConfig(encodeRecordId(recordId), {
      'localArray': _encodeVersionArray(localArray),
      'rowCount': localArray.length,
    });

    _activeQueries[hash] = queryState;
    _startTTLHeartbeat(hash, queryState);
    return hash;
  }

  /// Self-rescheduling timer at 90% of the query's TTL (TS `startTTLHeartbeat`).
  /// Faithful to the JS lifecycle; additionally invokes [_onHeartbeat] so the
  /// client can keep the server-side registration alive.
  void _startTTLHeartbeat(String hash, QueryState queryState) {
    if (queryState.ttlTimer != null) return;
    final heartbeatTime = (queryState.ttlDurationMs * 0.9).floor();
    queryState.ttlTimer = Timer(Duration(milliseconds: heartbeatTime), () {
      queryState.ttlTimer = null;
      _onHeartbeat?.call(hash);
      _startTTLHeartbeat(hash, queryState);
    });
  }

  void _stopTTLHeartbeat(QueryState queryState) {
    queryState.ttlTimer?.cancel();
    queryState.ttlTimer = null;
  }

  /// Opt-in eager teardown for a query whose last subscriber just left (TS
  /// `deregisterQuery`). No-op while any subscriber remains (refcount) or if the
  /// query isn't active. Only fires the remote-cleanup hook; the local DBSP view
  /// and in-memory state are freed in [finalizeDeregister] after the remote
  /// delete, so a re-subscribe in between heals it.
  void deregisterQuery(String hash) {
    if (_subscriptions[hash]?.isNotEmpty ?? false) return;
    if (!_activeQueries.containsKey(hash)) return;
    _onDeregister?.call(hash);
  }

  /// Final local teardown after the remote `_00_query` row was deleted (TS
  /// `finalizeDeregister`): stop the heartbeat + debounce timers, free the DBSP
  /// view, and drop in-memory query state and subscriptions. Invoked by
  /// [Sp00kySync] once the remote delete completes.
  void finalizeDeregister(String hash) {
    final qs = _activeQueries[hash];
    if (qs != null) _stopTTLHeartbeat(qs);
    _debounceTimers.remove(hash)?.cancel();
    _pendingStreamUpdates.remove(hash);
    _fetchDepth.remove(hash);
    _cache.unregisterQuery(hash);
    _activeQueries.remove(hash);
    _subscriptions.remove(hash);
    _statusSubscriptions.remove(hash);
  }

  /// Cancel all heartbeat and debounce timers (call on client shutdown).
  void dispose() {
    for (final qs in _activeQueries.values) {
      _stopTTLHeartbeat(qs);
    }
    for (final timer in _debounceTimers.values) {
      timer.cancel();
    }
    _debounceTimers.clear();
    _pendingStreamUpdates.clear();
  }

  QueryState _createNewQuery(
    RecordId recordId,
    String surqlString,
    Map<String, dynamic> params,
    QueryTimeToLive ttl,
    String tableName,
    List<RelationPlan> relations,
  ) {
    // Validate the table exists in the schema so a typo'd table name fails
    // fast. `_00_`-prefixed system/meta tables (e.g. `_00_user_feature`, the
    // feature-flag assignments) are server-provisioned and intentionally absent
    // from the generated client schema, but are still queryable + live-synced —
    // skip the check for them (matching SyncEngine, which cleans them through).
    if (!tableName.startsWith('_00_')) {
      _columnsFor(tableName); // validate table exists
    }

    final id = encodeRecordId(recordId);
    var configRecord = _local.getQueryConfig(id);
    if (configRecord == null) {
      configRecord = {
        'surql': surqlString,
        'params': _jsonSafe(params),
        'localArray': <dynamic>[],
        'remoteArray': <dynamic>[],
        'lastActiveAt': DateTime.now().toUtc().toIso8601String(),
        'createdAt': DateTime.now().toUtc().toIso8601String(),
        'ttl': ttl,
        'tableName': tableName,
        'updateCount': 0,
        'rowCount': 0,
        'errorCount': 0,
      };
      _local.putQueryConfig(id, configRecord);
    }

    final config = QueryConfig(
      id: recordId,
      surql: surqlString,
      params: params,
      localArray: _decodeVersionArray(configRecord['localArray']),
      remoteArray: _decodeVersionArray(configRecord['remoteArray']),
      ttl: ttl,
      lastActiveAt: DateTime.now(),
      tableName: tableName,
      relations: relations,
    );

    return QueryState(
      config: config,
      records: [],
      ttlDurationMs: parseDuration(ttl),
      updateCount: ((configRecord['updateCount'] as num?) ?? 0).toInt(),
      errorCount: ((configRecord['errorCount'] as num?) ?? 0).toInt(),
      status: _bornFetching ? QueryStatus.fetching : QueryStatus.idle,
    );
  }

  /// SHA-256 of `{...data, sessionId}` as lowercase hex (TS `calculateHash`).
  ///
  /// Key order matches the JS object literal (`surql`, `params`, then
  /// `sessionId`) so the hash agrees with the JS client for a shared
  /// `_00_query` table.
  String calculateHash(Map<String, dynamic> data) {
    // Sanitize RecordId/DateTime params (jsonEncode can't encode them) the same
    // way they serialize on the wire, so the hash is stable.
    final content =
        jsonEncode({..._jsonSafe(data) as Map, 'sessionId': _sessionId});
    return sha256.convert(utf8.encode(content)).toString();
  }

  void _replaceRecordInQueries(Map<String, dynamic> record) {
    for (final entry in _activeQueries.entries) {
      final records = entry.value.records;
      final index = records.indexWhere((r) => r['id'] == record['id']);
      if (index != -1) {
        records[index] = {...records[index], ...record};
        _notify(entry.key, records);
      }
    }
  }

  void _notify(String queryHash, List<Map<String, dynamic>> records) {
    final subs = _subscriptions[queryHash];
    if (subs == null) return;
    for (final callback in subs.toList()) {
      callback(records);
    }
  }

  void _emitMutation(List<UpEvent> events) {
    for (final callback in _mutationCallbacks.toList()) {
      callback(events);
    }
  }

  Map<String, ColumnSchema> _columnsFor(String tableName) {
    final table = _schema[tableName];
    if (table == null) {
      throw ArgumentError('Table $tableName not found');
    }
    final columns = (table is Map && table['columns'] is Map)
        ? table['columns'] as Map
        : (table as Map);
    return columns
        .map((key, value) => MapEntry(key as String, value as ColumnSchema));
  }

  String _nowStamp() => DateTime.now().microsecondsSinceEpoch.toString();

  /// Strip non-JSON values (e.g. [RecordId], [DateTime]) for storage.
  dynamic _jsonSafe(dynamic value) {
    if (value is RecordId) return value.encode();
    if (value is DateTime) return value.toUtc().toIso8601String();
    if (value is Map) {
      return value.map((k, v) => MapEntry(k.toString(), _jsonSafe(v)));
    }
    if (value is List) return value.map(_jsonSafe).toList();
    return value;
  }

  List<dynamic> _encodeVersionArray(RecordVersionArray array) =>
      array.map((e) => [e.$1, e.$2]).toList();

  RecordVersionArray _decodeVersionArray(dynamic raw) {
    if (raw is! List) return [];
    return raw
        .map<RecordVersion>(
            (e) => ((e as List)[0] as String, (e[1] as num).toInt()))
        .toList();
  }
}

/// Build push-event debounce options from [UpdateOptions] (TS `parseUpdateOptions`).
PushEventOptions parseUpdateOptions(
  String id,
  Map<String, dynamic> data,
  UpdateOptions? options,
) {
  final debounced = options?.debounced;
  if (debounced == null || debounced == false) {
    return const PushEventOptions();
  }
  final opts = debounced is DebounceOptions ? debounced : null;
  final delay = opts?.delay ?? 200;
  final isFieldsKey = opts?.key == DebounceKey.recordIdXFields;
  final key =
      isFieldsKey ? '$id::${(data.keys.toList()..sort()).join('#')}' : id;
  return PushEventOptions(debounced: DebouncedConfig(key: key, delay: delay));
}
