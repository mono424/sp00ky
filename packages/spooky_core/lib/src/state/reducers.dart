import '../kernel/constants.dart';
import '../surreal/value.dart';
import '../types.dart';
import 'client_state.dart';
import 'lifecycle.dart';

/// Pure state updates. Every reducer returns a new [ClientState]; sagas apply
/// them through `Fx.stateUpdate`. Reducers that change a query's render inputs
/// add it to `dirty` themselves, which is what drives materialization.
typedef Reducer = ClientState Function(ClientState);

Set<T> _addAll<T>(Set<T> set, Iterable<T> items) => {...set, ...items};

Set<T> _withoutAll<T>(Set<T> set, Iterable<T> items) {
  final next = {...set};
  next.removeAll(items);
  return next;
}

ClientState _withEntry(
    ClientState s, QueryHash hash, QueryEntry Function(QueryEntry) fn) {
  final entry = s.queries[hash];
  if (entry == null) return s;
  final next = fn(entry);
  if (identical(next, entry)) return s;
  return s.copyWith(queries: {...s.queries, hash: next});
}

List<QueryHash> _hashesForTable(ClientState s, String table) => [
      for (final e in s.queries.entries)
        if (e.value.def.tableName == table) e.key,
    ];

List<double> _capped(List<double> prev, double ms) {
  final next = [...prev, ms];
  return next.length <= telemetrySampleWindow
      ? next
      : next.sublist(next.length - telemetrySampleWindow);
}

// ---- queries ---------------------------------------------------------------

Reducer putQuery(QueryEntry entry) => (s) => s.copyWith(
      queries: {...s.queries, entry.def.hash: entry},
      dirty: _addAll(s.dirty, [entry.def.hash]),
    );

Reducer removeQuery(QueryHash hash) => (s) {
      if (!s.queries.containsKey(hash)) return s;
      return s.copyWith(
        queries: {...s.queries}..remove(hash),
        membershipReread: {...s.membershipReread}..remove(hash),
        dirty: _withoutAll(s.dirty, [hash]),
        membershipDirty: _withoutAll(s.membershipDirty, [hash]),
      );
    };

Reducer applyLifecycle(QueryHash hash, LifecycleEvent ev) => (s) =>
    _withEntry(s, hash, (e) => e.copyWith(lifecycle: transition(e.lifecycle, ev)));

Reducer setServerState(QueryHash hash, ServerViewState? serverState) => (s) =>
    _withEntry(
        s,
        hash,
        (e) => e.serverState == serverState
            ? e
            : e.copyWith(
                serverState: serverState, clearServerState: serverState == null));

/// Accept a server membership set. Also releases every acked outbox item the
/// set names: membership has caught up with the write, so the overlay's job is
/// done.
Reducer commitMembership(
        QueryHash hash, RecordVersionArray remoteArray, bool present) =>
    (s) {
      final withArray = _withEntry(
        s,
        hash,
        (e) => e.copyWith(
          remoteArray: remoteArray,
          lifecycle: transition(e.lifecycle, MembershipAppliedEvent(present)),
        ),
      );
      if (identical(withArray, s)) return s;
      final named = {for (final rv in remoteArray) rv.$1};
      final outbox = withArray.outbox
          .where((i) =>
              !(i.status == OutboxStatus.acked && named.contains(i.recordId)))
          .toList();
      return withArray.copyWith(
        outbox: outbox,
        dirty: _addAll(withArray.dirty, [hash]),
      );
    };

Reducer setLocalArray(QueryHash hash, RecordVersionArray localArray) => (s) {
      final next = _withEntry(s, hash, (e) => e.copyWith(localArray: localArray));
      return identical(next, s)
          ? s
          : next.copyWith(dirty: _addAll(next.dirty, [hash]));
    };

Reducer setSubqueryRemoteArray(QueryHash hash, RecordVersionArray array) =>
    (s) => _withEntry(s, hash, (e) => e.copyWith(subqueryRemoteArray: array));

Reducer setRecords(
        QueryHash hash, List<Row> records, bool changed, double? materializeMs) =>
    (s) {
      final next = _withEntry(s, hash, (e) {
        final prev = e.telemetry.phaseSamples[TimingPhase.localFetch] ?? const [];
        final telemetry = e.telemetry.copyWith(
          updateCount: changed
              ? e.telemetry.updateCount + 1
              : e.telemetry.updateCount,
          phaseSamples: materializeMs == null
              ? e.telemetry.phaseSamples
              : {
                  ...e.telemetry.phaseSamples,
                  TimingPhase.localFetch: _capped(prev, materializeMs),
                },
          phaseLast: materializeMs == null
              ? e.telemetry.phaseLast
              : {...e.telemetry.phaseLast, TimingPhase.localFetch: materializeMs},
        );
        return e.copyWith(
          records: changed ? records : e.records,
          telemetry: telemetry,
          lifecycle: transition(e.lifecycle, const NotifiedEvent()),
        );
      });
      return identical(next, s)
          ? s
          : next.copyWith(dirty: _withoutAll(next.dirty, [hash]));
    };

Reducer stampUpdated(QueryHash hash, int now) => (s) => _withEntry(
    s, hash, (e) => e.copyWith(telemetry: e.telemetry.copyWith(lastUpdatedAt: now)));

Reducer recordPhase(QueryHash hash, String phase, double ms) => (s) =>
    _withEntry(s, hash, (e) {
      final prev = e.telemetry.phaseSamples[phase] ?? const [];
      return e.copyWith(
        telemetry: e.telemetry.copyWith(
          phaseSamples: {...e.telemetry.phaseSamples, phase: _capped(prev, ms)},
          phaseLast: {...e.telemetry.phaseLast, phase: ms},
        ),
      );
    });

/// SSP ingest wall time for one update (the `ssp` phase).
Reducer recordIngest(QueryHash hash, double ms) => (s) => _withEntry(
      s,
      hash,
      (e) => e.copyWith(
        telemetry: e.telemetry.copyWith(
          lastIngestLatencyMs: ms,
          materializationSamples:
              _capped(e.telemetry.materializationSamples, ms),
        ),
      ),
    );

Reducer recordError(QueryHash hash) => (s) => _withEntry(
      s,
      hash,
      (e) => e.copyWith(
          telemetry: e.telemetry.copyWith(errorCount: e.telemetry.errorCount + 1)),
    );

Reducer setRegistrationTimings(QueryHash hash, RegistrationTimings timings) =>
    (s) => _withEntry(s, hash,
        (e) => e.copyWith(telemetry: e.telemetry.copyWith(registrationTimings: timings)));

Reducer bumpRegisterAttempts(QueryHash hash) => (s) => _withEntry(
    s, hash, (e) => e.copyWith(registerAttempts: e.registerAttempts + 1));

/// View-lost recovery pacing; see `QueryEntry.viewLostCount`.
Reducer setViewLost(QueryHash hash, int count, int? retryAt) =>
    (s) => _withEntry(
        s,
        hash,
        (e) => e.viewLostCount == count && e.viewLostRetryAt == retryAt
            ? e
            : e.copyWith(
                viewLostCount: count,
                viewLostRetryAt: retryAt,
                clearViewLostRetryAt: retryAt == null,
              ));

Reducer resetRegisterAttempts(QueryHash hash) => (s) => _withEntry(s, hash,
    (e) => e.registerAttempts == 0 ? e : e.copyWith(registerAttempts: 0));

Reducer stampHeartbeat(Iterable<QueryHash> hashes, int now) => (s) {
      var next = s;
      for (final hash in hashes) {
        next = _withEntry(next, hash, (e) => e.copyWith(lastHeartbeatAt: now));
      }
      return next;
    };

Reducer beginRegistering(QueryHash hash) =>
    (s) => s.copyWith(registering: _addAll(s.registering, [hash]));

Reducer endRegistering(QueryHash hash) => (s) => s.registering.contains(hash)
    ? s.copyWith(registering: _withoutAll(s.registering, [hash]))
    : s;

Reducer setMembershipReread(QueryHash hash, int? attempt) => (s) {
      if (attempt == null) {
        if (!s.membershipReread.containsKey(hash)) return s;
        return s.copyWith(membershipReread: {...s.membershipReread}..remove(hash));
      }
      return s.copyWith(
          membershipReread: {...s.membershipReread, hash: attempt});
    };

Reducer stampPolled(Iterable<QueryHash> hashes, int now) => (s) {
      var next = s;
      for (final hash in hashes) {
        next = _withEntry(next, hash, (e) => e.copyWith(lastPolledAt: now));
      }
      return next;
    };

// ---- subscribers -----------------------------------------------------------

Reducer subscribeQuery(QueryHash hash) => (s) => _withEntry(
      s,
      hash,
      (e) => e.copyWith(
          subscribers: e.subscribers + 1, clearLastSubscriberLeftAt: true),
    );

Reducer unsubscribeQuery(QueryHash hash, int now) => (s) => _withEntry(s, hash, (e) {
      final subscribers = e.subscribers - 1 < 0 ? 0 : e.subscribers - 1;
      return e.copyWith(
        subscribers: subscribers,
        lastSubscriberLeftAt:
            subscribers == 0 ? now : e.lastSubscriberLeftAt,
      );
    });

// ---- dirt ------------------------------------------------------------------

Reducer markDirty(Iterable<QueryHash> hashes) => (s) {
      final dirty = _addAll(s.dirty, hashes);
      return dirty.length == s.dirty.length ? s : s.copyWith(dirty: dirty);
    };

Reducer markTableDirty(String table) =>
    (s) => markDirty(_hashesForTable(s, table))(s);

Reducer clearDirty(QueryHash hash) => (s) =>
    s.dirty.contains(hash) ? s.copyWith(dirty: _withoutAll(s.dirty, [hash])) : s;

Reducer markMembershipDirty(Iterable<QueryHash> hashes) => (s) {
      final live = hashes.where(s.queries.containsKey).toList();
      if (live.isEmpty) return s;
      return s.copyWith(membershipDirty: _addAll(s.membershipDirty, live));
    };

Reducer clearMembershipDirty(Iterable<QueryHash> hashes) =>
    (s) => s.copyWith(membershipDirty: _withoutAll(s.membershipDirty, hashes));

// ---- versions --------------------------------------------------------------

/// Record local body versions; dirties every query whose membership names one
/// of them.
Reducer setVersions(List<(String, int)> entries) => (s) {
      if (entries.isEmpty) return s;
      final versions = {...s.versions};
      final changed = <String>{};
      for (final (id, v) in entries) {
        if (versions[id] != v) {
          versions[id] = v;
          changed.add(id);
        }
      }
      if (changed.isEmpty) return s;
      final dirty = <QueryHash>[];
      for (final e in s.queries.entries) {
        final names = e.value.remoteArray.any((rv) => changed.contains(rv.$1)) ||
            e.value.localArray.any((rv) => changed.contains(rv.$1));
        if (names) dirty.add(e.key);
      }
      return s.copyWith(versions: versions, dirty: _addAll(s.dirty, dirty));
    };

Reducer deleteVersions(Iterable<String> ids) => (s) {
      final versions = {...s.versions};
      var touched = false;
      for (final id in ids) {
        touched = versions.remove(id) != null || touched;
      }
      return touched ? s.copyWith(versions: versions) : s;
    };

// ---- outbox ----------------------------------------------------------------

Reducer outboxReplace(List<OutboxItem> items) => (s) {
      final tables = {...s.outbox.map((i) => i.table), ...items.map((i) => i.table)};
      var next = s.copyWith(outbox: [...items]);
      for (final table in tables) {
        next = markTableDirty(table)(next);
      }
      return next;
    };

Reducer outboxPush(OutboxItem item) =>
    (s) => markTableDirty(item.table)(s.copyWith(outbox: [...s.outbox, item]));

Reducer outboxAck(String id, int now) => (s) {
      final idx = s.outbox.indexWhere((i) => i.id == id);
      if (idx < 0) return s;
      final outbox = [...s.outbox];
      outbox[idx] = outbox[idx].copyWith(status: OutboxStatus.acked, ackedAt: now);
      return s.copyWith(outbox: outbox);
    };

Reducer outboxRemove(String id) => (s) {
      final idx = s.outbox.indexWhere((i) => i.id == id);
      if (idx < 0) return s;
      final table = s.outbox[idx].table;
      return markTableDirty(table)(
          s.copyWith(outbox: s.outbox.where((i) => i.id != id).toList()));
    };

Reducer outboxBumpAttempts(String id) => (s) {
      final idx = s.outbox.indexWhere((i) => i.id == id);
      if (idx < 0) return s;
      final outbox = [...s.outbox];
      outbox[idx] = outbox[idx].copyWith(attempts: outbox[idx].attempts + 1);
      return s.copyWith(outbox: outbox);
    };

/// Drop acked items older than [graceMs] (membership never named them).
Reducer outboxPruneAcked(int now, int graceMs) => (s) {
      final stale = s.outbox
          .where((i) =>
              i.status == OutboxStatus.acked &&
              i.ackedAt != null &&
              now - i.ackedAt! >= graceMs)
          .toList();
      if (stale.isEmpty) return s;
      final staleIds = {for (final i in stale) i.id};
      var next =
          s.copyWith(outbox: s.outbox.where((i) => !staleIds.contains(i.id)).toList());
      for (final table in {for (final i in stale) i.table}) {
        next = markTableDirty(table)(next);
      }
      return next;
    };

/// Merge a debounced update into its pending write (the first `before` wins).
Reducer mergePendingWrite(PendingWrite write) => (s) {
      final prev = s.pendingWrites[write.key];
      final merged = prev == null
          ? write
          : PendingWrite(
              key: prev.key,
              table: prev.table,
              recordId: prev.recordId,
              data: {...prev.data, ...write.data},
              before: prev.before,
              firstAt: prev.firstAt,
            );
      return s.copyWith(pendingWrites: {...s.pendingWrites, write.key: merged});
    };

Reducer clearPendingWrite(String key) => (s) =>
    s.pendingWrites.containsKey(key)
        ? s.copyWith(pendingWrites: {...s.pendingWrites}..remove(key))
        : s;

Reducer setFailedCount(int failedCount) =>
    (s) => s.failedCount == failedCount ? s : s.copyWith(failedCount: failedCount);

// ---- identity / connection --------------------------------------------------

/// Sentinel for "this field was not named", so a reducer can tell "leave
/// `userId` alone" from "set `userId` to null".
const Object _unset = Object();

Reducer setIdentity({
  Object? sessionId = _unset,
  Object? userId = _unset,
  Object? saltUserId = _unset,
  Object? pendingBucket = _unset,
  Object? bucketId = _unset,
  TabRole? tabRole,
  bool? localReady,
  bool? primed,
}) =>
    (s) => s.copyWith(
          sessionId: identical(sessionId, _unset) ? null : sessionId as String?,
          clearSessionId: !identical(sessionId, _unset) && sessionId == null,
          userId: identical(userId, _unset) ? null : userId as String?,
          clearUserId: !identical(userId, _unset) && userId == null,
          saltUserId:
              identical(saltUserId, _unset) ? null : saltUserId as String?,
          clearSaltUserId: !identical(saltUserId, _unset) && saltUserId == null,
          pendingBucket:
              identical(pendingBucket, _unset) ? null : pendingBucket as String?,
          clearPendingBucket:
              !identical(pendingBucket, _unset) && pendingBucket == null,
          bucketId: identical(bucketId, _unset) ? null : bucketId as String?,
          clearBucketId: !identical(bucketId, _unset) && bucketId == null,
          tabRole: tabRole,
          localReady: localReady,
          primed: primed,
        );

/// Re-home a query after a bucket switch: new store, same hash, fresh sync
/// state.
Reducer rebindQuery(
  QueryHash hash, {
  required RecordId id,
  required QueryLifecycle lifecycle,
  required RecordVersionArray remoteArray,
  required RecordVersionArray localArray,
}) =>
    (s) {
      final rebound = _withEntry(
        s,
        hash,
        (e) => QueryEntry(
          def: QueryDefinition(
            id: id,
            hash: e.def.hash,
            viewKey: e.def.viewKey,
            surql: e.def.surql,
            params: e.def.params,
            ttl: e.def.ttl,
            ttlMs: e.def.ttlMs,
            tableName: e.def.tableName,
            createdAt: e.def.createdAt,
            relations: e.def.relations,
            hasExplicitOrder: e.def.hasExplicitOrder,
          ),
          lifecycle: lifecycle,
          remoteArray: remoteArray,
          localArray: localArray,
          subqueryRemoteArray: const [],
          records: const [],
          serverState: null,
          subscribers: e.subscribers,
          lastSubscriberLeftAt: e.lastSubscriberLeftAt,
          lastHeartbeatAt: null,
          lastPolledAt: null,
          registerAttempts: 0,
          telemetry: e.telemetry,
        ),
      );
      return identical(rebound, s)
          ? s
          : rebound.copyWith(dirty: _addAll(rebound.dirty, [hash]));
    };

/// Everything a bucket switch invalidates in one go.
Reducer clearBucketState() => (s) => s.copyWith(
      versions: const {},
      outbox: const [],
      pendingWrites: const {},
      dirty: const {},
      membershipDirty: const {},
      membershipReread: const {},
      primed: false,
    );

Reducer setTabRole(TabRole tabRole) =>
    (s) => s.tabRole == tabRole ? s : s.copyWith(tabRole: tabRole);

Reducer setConnection(ConnectionState connection) => (s) =>
    s.sync.health.connection == connection
        ? s
        : s.copyWith(
            sync: s.sync.copyWith(
                health: s.sync.health.copyWith(connection: connection)));

/// Value equality over [SyncHealth]. `nextHealth` rebuilds the snapshot on
/// every sync round, so identity alone would report a change each time; the
/// runtime uses identity to decide whether to notify subscribers, and this is
/// what keeps that identity stable while nothing actually moved.
bool sameHealth(SyncHealth a, SyncHealth b) => a == b;

Reducer setHealth(SyncHealth health) => (s) => sameHealth(s.sync.health, health)
    ? s
    : s.copyWith(sync: s.sync.copyWith(health: health));

Reducer patchSync({
  int? consecutiveFailures,
  bool? hasSyncedOnce,
  int? selfHealAttempts,
  int? pollIdleStreak,
  int? lastReconnectRefetchAt,
  bool clearLastReconnectRefetchAt = false,
  bool? needsResubscribe,
  int? fetchAttempts,
  String? liveUuid,
  bool clearLiveUuid = false,
  String? liveTable,
  bool clearLiveTable = false,
}) =>
    (s) => s.copyWith(
          sync: s.sync.copyWith(
            consecutiveFailures: consecutiveFailures,
            hasSyncedOnce: hasSyncedOnce,
            selfHealAttempts: selfHealAttempts,
            pollIdleStreak: pollIdleStreak,
            lastReconnectRefetchAt: lastReconnectRefetchAt,
            clearLastReconnectRefetchAt: clearLastReconnectRefetchAt,
            needsResubscribe: needsResubscribe,
            fetchAttempts: fetchAttempts,
            liveUuid: liveUuid,
            clearLiveUuid: clearLiveUuid,
            liveTable: liveTable,
            clearLiveTable: clearLiveTable,
          ),
        );

/// Identity reducer; the `? :` arm of a conditional compose.
ClientState noop(ClientState s) => s;

/// Compose reducers left to right.
Reducer compose(List<Reducer> reducers) =>
    (s) => reducers.fold(s, (acc, r) => r(acc));
