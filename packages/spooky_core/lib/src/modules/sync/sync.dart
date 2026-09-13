import 'dart:async';

import 'package:meta/meta.dart';

import '../../events/event_system.dart';
import '../../services/database/local_database_service.dart';
import '../../services/database/remote_database_service.dart';
import '../../services/logger/logger.dart';
import '../../surreal/remote_client.dart';
import '../../surreal/value.dart';
import '../../types.dart';
import '../../utils/error_classification.dart';
import '../../utils/parser.dart';
import '../../utils/record_id_utils.dart';
import '../../utils/surql.dart';
import '../cache/cache_module.dart';
import '../data/data_module.dart';
import '../ref_tables.dart';
import 'engine.dart';
import 'queue/queue_down.dart';
import 'queue/queue_up.dart';
import 'scheduler.dart';
import 'sync_events.dart';
import 'sync_utils.dart';

/// Tunables for [Sp00kySync].
class Sp00kySyncOptions {
  const Sp00kySyncOptions({
    this.refSyncIntervalMs,
    this.anonymousLiveQueries,
    this.degradeAfterConsecutiveFailures,
    this.selfHealBaseMs,
  });
  final int? refSyncIntervalMs;

  /// Run the `_00_list_ref` poll + LIVE against the shared `_00_list_ref_anon`
  /// table even with no authenticated user, so a signed-out client syncs live
  /// over world-readable tables. Requires the server deployed with
  /// `anonymousLiveQueries: true`. See [Sp00kyConfig.enableAnonymousLiveQueries].
  final bool? anonymousLiveQueries;

  /// Consecutive failed sync rounds before health flips to `degraded`; 0
  /// disables reporting. Defaults to 3. See [Sp00kyConfig.syncHealth].
  final int? degradeAfterConsecutiveFailures;

  /// Base delay of the self-heal backoff (ms). Defaults to
  /// [Sp00kySync.selfHealDefaultBaseMs]; overridable so tests don't wait
  /// seconds for a retry.
  final int? selfHealBaseMs;
}

/// Bidirectional sync engine (TS `Sp00kySync`): up-queue (local -> remote),
/// down-queue (query registration/sync), a LIVE subscription on
/// `_00_list_ref[_user_<id>]`, and a self-rescheduling poll fallback.
class Sp00kySync {
  Sp00kySync(
    this._local,
    this._remote,
    this._cache,
    this._dataModule,
    this._schema,
    SpookyLogger logger, {
    Sp00kySyncOptions? options,
  }) : _logger = logger.child('Sp00kySync') {
    _upQueue = UpQueue(_local, _logger);
    _downQueue = DownQueue(_logger);
    _syncEngine = SyncEngine(_remote, _cache, _schema, _logger);
    _scheduler = SyncScheduler(
      _upQueue,
      _downQueue,
      _processUpEvent,
      _processDownEvent,
      _logger,
      onRollback: _handleRollback,
      onSyncOutcome: _recordSyncOutcome,
    );
    refSyncIntervalMs = resolveListRefPollInterval(options?.refSyncIntervalMs);
    _anonLiveEnabled = options?.anonymousLiveQueries ?? false;
    final degrade = options?.degradeAfterConsecutiveFailures ?? 3;
    _degradeAfterFailures = degrade > 0 ? degrade : 0;
    _selfHealBaseMs = options?.selfHealBaseMs ?? selfHealDefaultBaseMs;
  }

  final LocalDatabaseService _local;
  final RemoteDatabaseService _remote;
  final CacheModule _cache;
  final DataModule _dataModule;
  final Map<String, dynamic> _schema;
  final SpookyLogger _logger;

  late final UpQueue _upQueue;
  late final DownQueue _downQueue;
  late final SyncEngine _syncEngine;
  late final SyncScheduler _scheduler;
  late final int refSyncIntervalMs;

  final EventSystem events = createSyncEventSystem();
  EventSystem get engineEvents => _syncEngine.events;

  bool _isInit = false;
  bool _closed = false;
  bool _wasDisconnected = false;
  final RefMode _refMode = defaultRefMode;
  late final bool _anonLiveEnabled;

  String? _currentLiveQueryId;
  StreamSubscription<LiveMessage>? _liveSub;

  Timer? _listRefPollTimer;
  bool _listRefPollRunning = false;

  /// Wall-clock timestamp (ms) of the most recent LIVE event delivered through
  /// [_handleRemoteListRefChange]. A liveness diagnostic; the poll cadence is
  /// driven by [_listRefIdleStreak] (TS `lastLiveEventAt`).
  int? _lastLiveEventAt;
  int? get lastLiveEventAt => _lastLiveEventAt;

  /// Consecutive poll cycles that observed no `_00_list_ref` change. Drives the
  /// adaptive backoff; reset by a poll-detected change or an incoming LIVE event.
  int _listRefIdleStreak = 0;

  /// The in-flight poll cycle, so [close] can await it instead of tearing the
  /// remote out from under a request.
  Future<void>? _listRefPollInFlight;

  int _liveRetryCount = 0;
  int get liveRetryCount => _liveRetryCount;

  int get pendingMutationCount => _upQueue.size;
  bool get isSyncing => _scheduler.isSyncing;

  void Function() subscribeToPendingMutations(void Function(int count) cb) {
    final id1 = _upQueue.events.subscribe(SyncQueueEventTypes.mutationEnqueued,
        (e) => cb((e.payload as Map)['queueSize'] as int));
    final id2 = _upQueue.events.subscribe(SyncQueueEventTypes.mutationDequeued,
        (e) => cb((e.payload as Map)['queueSize'] as int));
    return () {
      _upQueue.events.unsubscribe(id1);
      _upQueue.events.unsubscribe(id2);
    };
  }

  // ---- sync health ----------------------------------------------------------

  /// Default base delay of the self-heal backoff (ms).
  static const int selfHealDefaultBaseMs = 2000;

  /// Cap on the self-heal backoff (ms), so a long outage doesn't busy-loop.
  static const int selfHealMaxMs = 30000;

  /// Consecutive failed rounds before reporting `degraded`; 0 disables.
  late final int _degradeAfterFailures;
  late final int _selfHealBaseMs;

  int _consecutiveSyncFailures = 0;
  SyncHealthStatus _syncHealthStatus = SyncHealthStatus.healthy;
  String? _lastSyncErrorKind;
  String? _lastSyncErrorMessage;

  /// Latched true on the first successful sync round; never reset. Lets a UI
  /// tell a cold-start "connecting" phase apart from a lost connection after a
  /// working session.
  bool _hasSyncedOnce = false;

  Timer? _selfHealTimer;
  int _selfHealAttempts = 0;

  /// Current sync-health snapshot (TS `syncHealth`).
  SyncHealth get syncHealth => SyncHealth(
        status: _syncHealthStatus,
        consecutiveFailures: _consecutiveSyncFailures,
        kind: _syncHealthStatus == SyncHealthStatus.degraded
            ? _lastSyncErrorKind
            : null,
        error: _syncHealthStatus == SyncHealthStatus.degraded
            ? _lastSyncErrorMessage
            : null,
        everConnected: _hasSyncedOnce,
      );

  /// Observe sync health (TS `subscribeToSyncHealth`). The callback fires
  /// immediately with the current snapshot and again on every
  /// healthy<->degraded transition. Returns an unsubscribe fn.
  void Function() subscribeToSyncHealth(void Function(SyncHealth health) cb) {
    cb(syncHealth);
    final id = events.subscribe(SyncEventTypes.syncHealthChanged,
        (event) => cb(event.payload as SyncHealth));
    return () => events.unsubscribe(id);
  }

  void _emitSyncHealth() =>
      events.emit(SyncEventTypes.syncHealthChanged, syncHealth);

  /// Fed by the scheduler once per drained sync round, and by the idle
  /// `_00_list_ref` poll (TS `recordSyncOutcome`). Individual failures are
  /// absorbed by the queue's retry; only a run of [_degradeAfterFailures]
  /// consecutive failures flips the status to `degraded`, and the next clean
  /// round flips it back. No-op when reporting is disabled.
  void _recordSyncOutcome(bool ok, [Object? error]) {
    if (_degradeAfterFailures <= 0) return;
    if (ok) {
      // Latch the first-ever success before the early return, so a clean cold
      // start (0 prior failures) still drops a UI's connecting phase.
      _hasSyncedOnce = true;
      if (_consecutiveSyncFailures == 0) return;
      _consecutiveSyncFailures = 0;
      if (_syncHealthStatus != SyncHealthStatus.healthy) {
        _syncHealthStatus = SyncHealthStatus.healthy;
        _lastSyncErrorKind = null;
        _lastSyncErrorMessage = null;
        _stopSelfHeal();
        _logger.info('Sync recovered; health back to healthy');
        _emitSyncHealth();
      }
      return;
    }

    _consecutiveSyncFailures++;
    _lastSyncErrorKind = classifySyncError(error);
    _lastSyncErrorMessage = error?.toString();
    if (_syncHealthStatus != SyncHealthStatus.degraded &&
        _consecutiveSyncFailures >= _degradeAfterFailures) {
      _syncHealthStatus = SyncHealthStatus.degraded;
      _logger.warn(
          'Sync degraded after $_consecutiveSyncFailures consecutive failures ($_lastSyncErrorKind): $error');
      _emitSyncHealth();
      _startSelfHeal();
    }
  }

  /// Begin self-heal retries (no-op if already running). While degraded,
  /// re-drive sync on an exponential backoff so the app recovers on its own —
  /// even when the socket never actually dropped, in which case no `connected`
  /// event fires and this re-probe is the ONLY thing that reaches the server.
  void _startSelfHeal() {
    if (_selfHealTimer != null) return;
    _selfHealAttempts = 0;
    _scheduleSelfHeal();
  }

  void _scheduleSelfHeal() {
    final backoff = _selfHealBaseMs * (1 << _selfHealAttempts.clamp(0, 30));
    final delay = backoff < selfHealMaxMs ? backoff : selfHealMaxMs;
    _selfHealTimer = Timer(Duration(milliseconds: delay), () async {
      _selfHealTimer = null;
      if (_closed) return;
      if (_syncHealthStatus != SyncHealthStatus.degraded) return;
      _selfHealAttempts++;
      _logger.debug(
          'Self-heal attempt $_selfHealAttempts (delay ${delay}ms): re-driving sync while degraded');
      try {
        // Retry whatever is still queued first: the failing op was re-queued by
        // the queue, so this re-probes the server and reports its outcome
        // through the scheduler -> _recordSyncOutcome.
        if (_upQueue.size > 0) {
          await _scheduler.syncUp();
        } else if (_downQueue.size > 0) {
          await _scheduler.syncDown();
        } else {
          // Nothing queued (e.g. the failing op was rolled back and dropped):
          // re-register active queries, mirroring the reconnect handler, so
          // there's a concrete op whose success flips health. With no active
          // queries either, probe connectivity directly.
          final hashes = _dataModule.getActiveQueryHashes();
          if (hashes.isNotEmpty) {
            for (final hash in hashes) {
              _scheduler.enqueueDownEvent(RegisterEvent(hash));
            }
            await _scheduler.syncDown();
          } else {
            await _remote.query('RETURN true');
            _recordSyncOutcome(true);
          }
        }
      } catch (err) {
        // Only the direct probe can throw here (syncUp/syncDown swallow and
        // self-report); treat a probe failure as another failed round.
        _recordSyncOutcome(false, err);
      }
      // Keep retrying until recovery. A successful outcome calls _stopSelfHeal,
      // so only continue while still degraded.
      if (!_closed && _syncHealthStatus == SyncHealthStatus.degraded) {
        _scheduleSelfHeal();
      }
    });
  }

  void _stopSelfHeal() {
    _selfHealTimer?.cancel();
    _selfHealTimer = null;
    _selfHealAttempts = 0;
  }

  Future<void> init() async {
    if (_isInit) throw StateError('Sp00kySync is already initialized');
    _isInit = true;
    await _scheduler.init();
    _subscribeToReconnect();
    unawaited(_scheduler.syncUp());
    unawaited(_scheduler.syncDown());
    // No initial LIVE subscription: wait for setCurrentUserId from auth — in
    // dedicated mode the table name depends on the authenticated user, and an
    // unauthenticated subscription wouldn't match any per-user table anyway.
    //
    // Exception: with anonymous live queries enabled, start realtime now
    // against the shared `_00_list_ref_anon` table so a signed-out client syncs
    // immediately. `setCurrentUserId` re-points LIVE to the per-user table on
    // sign-in. Guard on the current user id because the auth callback can fire
    // (and authenticate) before init() runs — don't clobber that back to anon.
    if (_anonLiveEnabled && _currentUserIdMirror == null) {
      _startListRefPoll();
      _restartRefLiveQuery().catchError((Object err) {
        _logger.debug(
            'Anonymous ref LIVE start failed; relying on poll fallback: $err');
      });
    }
  }

  /// Push the authenticated user id; (re)starts the `_00_list_ref` LIVE under
  /// the new auth context with backoff, and the poll fallback. Null on sign-out.
  Future<void> setCurrentUserId(String? userId) async {
    _currentUserIdMirror ??= userId;
    if (_currentUserIdMirror == userId && _currentLiveQueryId != null) return;
    _currentUserIdMirror = userId;

    if (userId == null) {
      if (_anonLiveEnabled) {
        // Signed out but anonymous realtime is on: keep the poll running and
        // re-point LIVE from the (now stale) per-user table to the shared
        // `_00_list_ref_anon`. `_startListRefPoll` is idempotent; the poll
        // re-resolves `listRefTable()` each tick so it follows automatically.
        _startListRefPoll();
        await _restartRefLiveQuery().catchError((Object err) {
          _logger.debug(
              'Anonymous ref LIVE restart failed; relying on poll fallback: $err');
        });
        return;
      }
      await _killRefLiveQuery();
      _stopListRefPoll();
      return;
    }

    _startListRefPoll();
    const attemptDelays = [0, 250, 500, 1000, 2000];
    for (var i = 0; i < attemptDelays.length; i++) {
      if (attemptDelays[i] > 0) {
        _liveRetryCount++;
        await Future<void>.delayed(Duration(milliseconds: attemptDelays[i]));
      }
      if (_closed) return; // bail if torn down mid-backoff
      try {
        await _restartRefLiveQuery();
        return;
      } catch (err) {
        _logger.debug(
            'Ref LIVE start failed (attempt ${i + 1}); poll fallback active');
      }
    }
  }

  String? _currentUserIdMirror;

  void enqueueDownEvent(DownEvent event) => _scheduler.enqueueDownEvent(event);
  Future<void> enqueueMutation(List<UpEvent> mutations) async =>
      _scheduler.enqueueMutation(mutations);

  /// Resolve the active `_00_list_ref` table name (TS `listRefTable`). Reads
  /// the user id from [DataModule] (set synchronously from the auth callback).
  /// Unauthenticated with anonymous live queries on → the shared
  /// `_00_list_ref_anon` table.
  String listRefTable() {
    final userId = _dataModule.getCurrentUserId();
    if (userId == null && _anonLiveEnabled) {
      return listRefTableFor(_refMode, anonUserId);
    }
    return listRefTableFor(_refMode, userId);
  }

  // ---- poll fallback --------------------------------------------------------

  /// Periodic re-poll of `_00_list_ref` as a safety net for missed LIVE
  /// notifications (TS `startListRefPoll`). SurrealDB v3 occasionally drops LIVE
  /// deliveries across sessions even when the row matches the permission rule,
  /// so the LIVE subscription alone can leave a query stale until reload; this
  /// catches those. Self-rescheduling (not a fixed interval) so each tick picks
  /// its own delay via [listRefPollDelayMs] — an idle client coasts toward the
  /// slow cap and any observed change snaps it back to the fast base cadence.
  void _startListRefPoll() {
    if (_listRefPollRunning) return;
    _listRefPollRunning = true;
    _logger.debug('list_ref poll loop started (base ${refSyncIntervalMs}ms)');
    void schedule(int delayMs) {
      _listRefPollTimer = Timer(Duration(milliseconds: delayMs), () async {
        if (!_listRefPollRunning) return;
        var changed = false;
        final tick = _pollListRefForActiveQueries().then((c) => changed = c);
        _listRefPollInFlight = tick.then((_) {}).catchError((Object _) {});
        try {
          await tick;
        } finally {
          _listRefPollInFlight = null;
          if (_listRefPollRunning) {
            // Reset the idle streak on any observed change so the poll snaps
            // back to the base cadence; otherwise grow it so a quiet client
            // backs off toward the cap. (A LIVE event resets it too, in
            // [_handleRemoteListRefChange].)
            _listRefIdleStreak = changed ? 0 : _listRefIdleStreak + 1;
            schedule(listRefPollDelayMs(
              idleStreak: _listRefIdleStreak,
              baseIntervalMs: refSyncIntervalMs,
            ));
          }
        }
      });
    }

    schedule(refSyncIntervalMs);
  }

  void _stopListRefPoll() {
    _listRefPollRunning = false;
    _listRefPollTimer?.cancel();
    _listRefPollTimer = null;
  }

  /// Create the remote view for [queryHash] and pull its primary window plus any
  /// subquery child bodies. The registration path normally drives this through
  /// the down-queue; exposed so tests can step it without a scheduler.
  @visibleForTesting
  Future<void> registerRemoteQueryForTest(String queryHash) =>
      _createRemoteQuery(queryHash);

  /// Run exactly one `_00_list_ref` poll cycle, returning whether anything
  /// changed. The loop normally drives this on its own timer; exposed so tests
  /// can step it deterministically.
  @visibleForTesting
  Future<bool> pollListRefOnce() => _pollListRefForActiveQueries();

  /// One poll cycle: refetch `_00_list_ref` for every active query. Returns
  /// whether ANY query's remoteArray actually changed, which drives the adaptive
  /// idle backoff (TS `pollListRefForActiveQueries`).
  ///
  /// Also the ONLY health signal that runs while the client is idle. Sync health
  /// is otherwise activity-driven (mutations/registrations via the scheduler,
  /// reconnect re-registration, self-heal), so on a quiet client a stale
  /// `degraded` would linger until the next mutation and a genuine idle drop
  /// would be invisible. Folding the cycle's aggregate reachability into
  /// [_recordSyncOutcome] makes idle health self-recover (and self-degrade) with
  /// no user action. A clean cycle is free when already healthy
  /// ([_recordSyncOutcome] early-returns at zero consecutive failures).
  Future<bool> _pollListRefForActiveQueries() async {
    final hashes = _dataModule.getActiveQueryHashes();
    if (hashes.isEmpty) {
      // No active queries to piggyback on, but health still needs a heartbeat:
      // probe connectivity directly so an idle client with no live queries
      // doesn't go blind. Cheap, and gated by the same adaptive backoff.
      try {
        await _remote.query('RETURN true');
        _recordSyncOutcome(true);
      } catch (err) {
        _recordSyncOutcome(false, err);
      }
      return false;
    }

    var anyChanged = false;
    // `reached` = the server answered at least once this cycle (a success, or an
    // application error, which still proves reachability). `firstNetworkErr`
    // holds the first network-classified failure. A cycle that only produced
    // network errors reports a down round; a mixed/app cycle counts as reached;
    // an all-application cycle reports nothing (a query-shape fault owned by the
    // registration path, not a reachability signal).
    var reached = false;
    Object? firstNetworkErr;
    for (final hash in hashes) {
      try {
        if (await _refetchListRefForQuery(hash)) anyChanged = true;
        reached = true;
      } catch (err) {
        if (classifySyncError(err) == 'network') {
          firstNetworkErr ??= err;
        } else {
          reached = true;
        }
        _logger.debug('Per-query list_ref poll failed for $hash: $err');
      }
    }
    // Record directly rather than routing through the scheduler: the scheduler
    // only reports rounds that drained a queue item, and this isn't one.
    if (reached) {
      _recordSyncOutcome(true);
    } else if (firstNetworkErr != null) {
      _recordSyncOutcome(false, firstNetworkErr);
    }
    return anyChanged;
  }

  /// Pull the upstream list_ref entries for [queryHash], persist them only when
  /// they actually changed, then sync record bodies. Returns whether the cached
  /// `remoteArray` changed (TS `refetchListRefForQuery`).
  Future<bool> _refetchListRefForQuery(String queryHash) async {
    final queryState = _dataModule.getQueryByHash(queryHash);
    if (queryState == null) return false;
    final fresh = await _fetchListRef(queryState.config.id);

    // Idempotent poll: only persist when the array actually changed. The poll
    // runs continuously as a LIVE fallback, so on a quiet client `fresh` equals
    // the cached array every tick, and re-writing it (an `UPDATE _00_query` per
    // active query per cycle) was pure churn and the bulk of idle traffic.
    // Order-insensitive because the list_ref SELECT has no `ORDER BY`.
    final changed =
        !recordVersionArraysEqual(fresh, queryState.config.remoteArray);
    if (changed) {
      await _dataModule.updateQueryRemoteArray(queryHash, fresh);
    }

    // Run syncQuery every tick regardless: it's a no-op once localArray has
    // caught up (issues no query), but it covers the rare case where
    // remoteArray is stable yet localArray is behind (a prior record fetch
    // failed), so a missed row still gets retried.
    try {
      await syncQuery(queryHash);
    } catch (err) {
      _logger.info('syncQuery failed during poll: $err');
    }
    return changed;
  }

  // ---- LIVE -----------------------------------------------------------------

  Future<void> _killRefLiveQuery() async {
    await _liveSub?.cancel();
    _liveSub = null;
    if (_currentLiveQueryId != null) {
      try {
        await _remote.getClient().kill(_currentLiveQueryId!);
      } catch (err) {
        _logger.debug('Prior LIVE KILL failed; continuing: $err');
      }
      _currentLiveQueryId = null;
    }
  }

  Future<void> _restartRefLiveQuery() async {
    await _killRefLiveQuery();
    await _startRefLiveQueries();
  }

  void _subscribeToReconnect() {
    final client = _remote.getClient();
    client.onDisconnected.listen((_) {
      _wasDisconnected = true;
      _logger.info('Remote disconnected');
    });
    client.onConnected.listen((_) {
      if (!_wasDisconnected) return;
      _wasDisconnected = false;
      _logger.info('Remote reconnected, refetching active queries');
      for (final hash in _dataModule.getActiveQueryHashes()) {
        _scheduler.enqueueDownEvent(RegisterEvent(hash));
      }
      // The WS reconnect leaves the server-side LIVE subscription dead — the
      // re-enqueued register events only re-fetch initial state, they don't
      // re-subscribe. Without this, LIVE never recovers after a reconnect and
      // the poll silently becomes the sole sync path. Authenticated → per-user
      // table; signed-out with anon live on → the shared `_00_list_ref_anon`.
      if (_currentUserIdMirror != null || _anonLiveEnabled) {
        _restartRefLiveQuery().catchError((Object err) {
          _logger.debug(
              'LIVE restart after reconnect failed; poll fallback active: $err');
        });
      }
    });
  }

  Future<void> _startRefLiveQueries() async {
    final tableName = listRefTable();
    final (liveId, stream) =
        await _remote.getClient().live('LIVE SELECT * FROM $tableName');
    _currentLiveQueryId = liveId;
    _liveSub = stream.listen((message) {
      if (message.action == 'KILLED') return;
      final value = message.value;
      _handleRemoteListRefChange(
        message.action,
        parseRecordIdString(value['in'].toString()),
        parseRecordIdString(value['out'].toString()),
        ((value['version'] as num?) ?? 0).toInt(),
      ).catchError((Object err) {
        _logger.error('Error handling remote list ref change', err);
      });
    });
  }

  Future<void> _handleRemoteListRefChange(
    String action,
    RecordId queryId,
    RecordId recordId,
    int version,
  ) async {
    _lastLiveEventAt = DateTime.now().millisecondsSinceEpoch;
    // Activity: snap the poll back to its base cadence.
    _listRefIdleStreak = 0;

    final existing = _dataModule.getQueryById(queryId);
    if (existing == null) {
      _logger.warn('Received remote update for unknown local query $queryId');
      return;
    }

    // DELETE is handled like CREATE/UPDATE (mirrors the TS core). When a record
    // leaves a query's window — e.g. it's deleted on the website or another
    // device — the SSP removes its `_00_list_ref` edge and LIVE delivers a
    // DELETE here; turn that into a `removed` diff so the row drops in realtime.
    // (Previously this returned early, so removals only caught up on the slow
    // poll — i.e. realtime deletion appeared not to work.) A delete
    // notification's `version` isn't reliable, so bypass the version guard;
    // SyncEngine re-verifies the record is gone upstream before deleting
    // locally, so a forced removal is safe.
    final diff = action == 'DELETE'
        ? RecordVersionDiff(added: [], updated: [], removed: [recordId])
        : createDiffFromDbOp(
            action, recordId, version, existing.config.localArray);
    await _runSyncForQuery(extractIdPart(queryId), diff);
  }

  // ---- up/down processing ---------------------------------------------------

  Future<void> _processUpEvent(UpEvent event) async {
    switch (event) {
      case CreateEvent(:final recordId, :final data):
        final setItems =
            data.keys.map((k) => SetItem.keyVar(k, 'data_$k')).toList();
        final vars = <String, dynamic>{'id': recordId};
        for (final k in data.keys) {
          vars['data_$k'] = data[k];
        }
        await _remote.query(surql.seal(surql.createSet('id', setItems)), vars);
      case UpdateEvent(:final recordId, :final data):
        await _remote
            .query('UPDATE \$id MERGE \$data', {'id': recordId, 'data': data});
      case DeleteEvent(:final recordId):
        await _remote.query('DELETE \$id', {'id': recordId});
    }
  }

  Future<void> _handleRollback(UpEvent event, Object error) async {
    final recordIdStr = encodeRecordId(event.recordId);
    final tableName = event is CreateEvent && event.tableName != null
        ? event.tableName!
        : extractTablePart(recordIdStr);

    switch (event) {
      case CreateEvent():
        await _dataModule.rollbackCreate(event.recordId, tableName);
      case UpdateEvent(:final beforeRecord):
        if (beforeRecord != null) {
          await _dataModule.rollbackUpdate(
              event.recordId, tableName, beforeRecord);
        } else {
          _logger.warn(
              'Cannot rollback update: no beforeRecord. Down-sync will reconcile.');
        }
      case DeleteEvent():
        _logger
            .warn('Delete rollback not implemented. Down-sync will reconcile.');
    }

    events.emit(SyncEventTypes.mutationRolledBack, {
      'eventType': event.runtimeType.toString(),
      'recordId': recordIdStr,
      'error': error.toString(),
    });
  }

  Future<void> _processDownEvent(DownEvent event) async {
    switch (event) {
      case RegisterEvent(:final hash):
        return _registerQuery(hash);
      case SyncDownEvent(:final hash):
        return syncQuery(hash);
      case HeartbeatEvent(:final hash):
        return _heartbeatQuery(hash);
      case CleanupEvent(:final hash):
        return _cleanupQuery(hash);
    }
  }

  Future<void> syncQuery(String hash) async {
    final queryState = _dataModule.getQueryByHash(hash);
    if (queryState == null) return;
    final diff =
        ArraySyncer(queryState.config.localArray, queryState.config.remoteArray)
            .nextSet();
    if (diff == null) return;
    return _runSyncForQuery(hash, diff);
  }

  /// Number of consecutive rounds an id must stay "removed from list_ref but
  /// still upstream" before its `localArray` entry is dropped. See
  /// [_runSyncForQuery].
  static const int _convergeAfter = 3;

  /// Per `hash:recordId`, how many consecutive rounds the id has been
  /// still-remote.
  final Map<String, int> _stillRemoteStreaks = {};

  /// Fetch the diff's record bodies via [SyncEngine] while reflecting the
  /// query's fetch status (TS `runSyncForQuery`). Marks the query `fetching` for
  /// the duration when the diff actually pulls records, then settles it in a
  /// `finally` so a failed sync never leaves a query stuck `fetching`.
  Future<void> _runSyncForQuery(String hash, RecordVersionDiff diff) async {
    // Don't let sync re-add a record the user just deleted locally. The remote
    // delete is queued in the outbox, so until it's processed the server's
    // `_00_list_ref` still lists the record; the diff then classifies it as
    // `added` (present remotely, absent locally) and re-inserts it, so deleted
    // rows reappear a few seconds later. Once the remote delete lands, the
    // pending row clears and the server drops the edge, so the guard stops
    // applying on its own.
    if (diff.added.isNotEmpty || diff.updated.isNotEmpty) {
      final pendingDeletes = _pendingDeleteIds();
      if (pendingDeletes.isNotEmpty) {
        diff = RecordVersionDiff(
          added: diff.added
              .where((r) => !pendingDeletes.contains(encodeRecordId(r.id)))
              .toList(),
          updated: diff.updated
              .where((r) => !pendingDeletes.contains(encodeRecordId(r.id)))
              .toList(),
          removed: diff.removed,
        );
      }
    }

    final fetching = diff.added.isNotEmpty || diff.updated.isNotEmpty;
    if (fetching) _dataModule.beginFetching(hash);
    try {
      final result = await _syncEngine.syncRecords(diff);
      if (fetching) {
        _dataModule.recordRemoteFetch(hash, result.remoteFetchMs);
      }
      await _convergeStillRemote(hash, result.stillRemoteIds);
    } finally {
      if (fetching) {
        // Land the coalesced result BEFORE flipping to idle: the final stream
        // update sits on a debounce timer, and an `idle` racing ahead of it
        // would let a consumer treat a partially filled window as complete.
        try {
          await _dataModule.flushPendingStreamUpdate(hash);
        } catch (err) {
          _logger
              .warn('Failed to flush pending stream update before idle: $err');
        }
        _dataModule.endFetching(hash);
      }
    }
  }

  /// Converge `localArray` to the authoritative `remoteArray` for ids that left
  /// the server's list_ref but still exist upstream — a view-membership change,
  /// not a delete — so the poll's diff stops re-flagging them every tick.
  ///
  /// Only converges after an id has been still-remote for [_convergeAfter]
  /// CONSECUTIVE rounds. A record that is merely mid-deletion reads as
  /// still-remote for about one round (its delete hasn't committed when the
  /// existence check races it) and is gone the next, so it never reaches the
  /// threshold and gets deleted normally instead of being stranded here.
  Future<void> _convergeStillRemote(
      String hash, List<String> stillRemoteIds) async {
    if (stillRemoteIds.isEmpty) return;
    final toConverge = <String>[];
    for (final id in stillRemoteIds) {
      final key = '$hash:$id';
      final n = (_stillRemoteStreaks[key] ?? 0) + 1;
      if (n >= _convergeAfter) {
        _stillRemoteStreaks.remove(key);
        toConverge.add(id);
      } else {
        _stillRemoteStreaks[key] = n;
      }
    }
    if (toConverge.isEmpty) return;

    final local = _dataModule.getQueryByHash(hash)?.config.localArray;
    if (local == null || local.isEmpty) return;
    final next = local.where((e) => !toConverge.contains(e.$1)).toList();
    if (next.length != local.length) {
      await _dataModule.updateQueryLocalArray(hash, next);
    }
  }

  /// Record ids with a pending local DELETE in the outbox
  /// (TS `getPendingDeleteIds`). Reads the sqlite outbox synchronously.
  Set<String> _pendingDeleteIds() {
    try {
      return _local
          .getAllMutations()
          .where((m) => m['mutationType'] == 'delete')
          .map((m) => m['recordId'].toString())
          .toSet();
    } catch (err) {
      _logger.warn(
          'Failed to read pending deletes; sync may briefly resurrect a just-deleted record: $err');
      return const {};
    }
  }

  Future<void> _registerQuery(String queryHash) async {
    // Hold `fetching` across the WHOLE registration (remote view creation +
    // initial sync + post-sync notify). A query is born `fetching`; this
    // refcounted cycle is what settles it, so a consumer never sees an idle
    // query whose window is still empty or partially materialized.
    _dataModule.beginFetching(queryHash);
    try {
      await _createRemoteQuery(queryHash);
      await syncQuery(queryHash);
      // Land any still-debounced stream result, then always notify — this covers
      // empty result sets, where no stream update fires but the consumer still
      // needs to stop loading.
      await _dataModule.flushPendingStreamUpdate(queryHash);
      await _dataModule.notifyQuerySynced(queryHash);
    } finally {
      _dataModule.endFetching(queryHash);
    }
  }

  Future<void> _createRemoteQuery(String queryHash) async {
    final queryState = _dataModule.getQueryByHash(queryHash);
    if (queryState == null) throw StateError('Query to register not found');

    await _remote.query('fn::query::register(\$config)', {
      'config': {
        'id': queryState.config.id,
        'surql': queryState.config.surql,
        'params': queryState.config.params,
        'ttl': queryState.config.ttl,
      },
    });

    final array = await _fetchListRef(queryState.config.id);
    await _dataModule.updateQueryRemoteArray(queryHash, array);

    await _syncSubqueryChildren(queryHash).catchError((Object err) {
      _logger.info(
          'Subquery child sync failed during registration; poll will retry: $err');
    });
  }

  /// Sync the BODIES of a `.related()` query's subquery child rows into the
  /// local cache, separately from the primary window (TS `syncSubqueryChildren`).
  ///
  /// The SSP writes each matched child as a `_00_list_ref` edge tagged
  /// `parent`/`parent_rel`; [subqueryListRefSelect] pulls those pairs at any
  /// nesting depth. Added/updated bodies are fetched through [SyncEngine], which
  /// saves them into sqlite AND the circuit — the parent view then re-emits (its
  /// subquery depends on the child table), so the next materialization resolves
  /// the relation from real local rows.
  ///
  /// Deletion safety: `removed` is deliberately empty. A child body can be shared
  /// with other queries, and deleting one that merely left THIS query's child set
  /// would clobber data another query still shows. Real deletes flow through the
  /// normal delete path; a lingering orphan body is invisible, because the
  /// correlated match stops finding it.
  ///
  /// Kept off [_runSyncForQuery] on purpose, so child fetches never flip the
  /// query to `fetching` or skew its timings.
  Future<void> _syncSubqueryChildren(String queryHash) async {
    final queryState = _dataModule.getQueryByHash(queryHash);
    if (queryState == null) return;
    if (queryState.config.relations.isEmpty) return;

    final results = await _remote.query(
        subqueryListRefSelect(listRefTable()), {'in': queryState.config.id});
    final fresh = _parseListRefRows(results);
    final prev = queryState.config.subqueryRemoteArray ?? const [];
    if (recordVersionArraysEqual(fresh, prev)) return; // nothing new

    final diff = diffRecordVersionArray(prev, fresh);
    if (diff.added.isNotEmpty || diff.updated.isNotEmpty) {
      await _syncEngine.syncRecords(RecordVersionDiff(
        added: diff.added,
        updated: diff.updated,
        removed: const [], // never delete child bodies here — see the doc above
      ));
    }
    queryState.config.subqueryRemoteArray = fresh;
  }

  Future<RecordVersionArray> _fetchListRef(RecordId queryId) async {
    final results = await _remote
        .query(listRefSelect(listRefTable()), {'in': queryId});
    return _parseListRefRows(results);
  }

  /// `[out, version]` pairs from a `_00_list_ref` select result.
  RecordVersionArray _parseListRefRows(List<dynamic> results) => [
        for (final item in firstRows(results))
          (
            (item as Map)['out'].toString(),
            ((item['version'] as num?) ?? 0).toInt(),
          ),
      ];

  Future<void> _heartbeatQuery(String queryHash) async {
    final queryState = _dataModule.getQueryByHash(queryHash);
    if (queryState == null) throw StateError('Query to register not found');
    await _remote
        .query('fn::query::heartbeat(\$id)', {'id': queryState.config.id});
  }

  Future<void> _cleanupQuery(String queryHash) async {
    final queryState = _dataModule.getQueryByHash(queryHash);
    if (queryState == null) throw StateError('Query to register not found');
    // Eager remote release is DISABLED, matching TS `cleanupQuery` (see the
    // long note there). The TTL sweep reclaims the row and its edges on
    // `lastActiveAt + ttl`, which is the only reclamation that has ever
    // actually run: before canary.190 `_00_query` granted no delete permission,
    // so the bare `DELETE $id` this used to issue affected zero rows.
    //
    // Dart is the riskier client to re-enable this on, because it has none of
    // the TS guards - no `hasSubscribers` check before or after the release,
    // and no `serverRowCount`/`emptyReads` protection - so a cleanup firing on
    // a query still in view would delete the row with nothing to catch it.
    //
    // Only free the local view + in-memory state (TS `finalizeDeregister`).
    _dataModule.finalizeDeregister(queryHash);
  }

  Future<void> close() async {
    _closed = true;
    _stopSelfHeal();
    _stillRemoteStreaks.clear();
    _stopListRefPoll();
    // Let an in-flight poll cycle finish so its remote round-trip isn't torn
    // out from under it while we close the connection.
    final inFlight = _listRefPollInFlight;
    if (inFlight != null) await inFlight;
    await _killRefLiveQuery();
  }
}
