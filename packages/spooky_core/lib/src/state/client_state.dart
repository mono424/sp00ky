import '../kernel/saga.dart' show LaneState, emptyLanes;
import '../surreal/value.dart';
import '../types.dart';
import 'lifecycle.dart';

typedef Row = Map<String, dynamic>;

/// Everything a query is, minus anything that changes after registration.
class QueryDefinition {
  const QueryDefinition({
    required this.id,
    required this.hash,
    required this.viewKey,
    required this.surql,
    required this.params,
    required this.ttl,
    required this.ttlMs,
    required this.tableName,
    required this.createdAt,
    this.relations = const [],
    this.hasExplicitOrder = false,
  });

  /// Session-salted `_00_query:<hash>` record id: the remote view id.
  final RecordId id;
  final QueryHash hash;

  /// Unsalted `sha256({surql, params})`: key of the durable `_00_view` row.
  final String viewKey;
  final String surql;
  final Map<String, dynamic> params;
  final QueryTimeToLive ttl;
  final int ttlMs;
  final String tableName;
  final int createdAt;

  /// The query's `.related()` plan, resolved from the local cache on every
  /// materialization. Not persisted: the surql, which is, already encodes the
  /// same relation shape.
  final List<Object> relations;

  /// The query orders itself, so the render set must not be re-sorted by id.
  final bool hasExplicitOrder;
}

/// What the server's `_00_query` row last said about this view.
enum ServerViewState { materializing, ready }

class QueryTelemetry {
  const QueryTelemetry({
    required this.updateCount,
    required this.errorCount,
    required this.lastUpdatedAt,
    required this.materializationSamples,
    required this.lastIngestLatencyMs,
    required this.phaseSamples,
    required this.phaseLast,
    required this.registrationTimings,
  });

  final int updateCount;
  final int errorCount;
  final int? lastUpdatedAt;
  final List<double> materializationSamples;
  final double? lastIngestLatencyMs;
  final Map<String, List<double>> phaseSamples;
  final Map<String, double?> phaseLast;
  final RegistrationTimings registrationTimings;

  QueryTelemetry copyWith({
    int? updateCount,
    int? errorCount,
    int? lastUpdatedAt,
    List<double>? materializationSamples,
    double? lastIngestLatencyMs,
    Map<String, List<double>>? phaseSamples,
    Map<String, double?>? phaseLast,
    RegistrationTimings? registrationTimings,
  }) =>
      QueryTelemetry(
        updateCount: updateCount ?? this.updateCount,
        errorCount: errorCount ?? this.errorCount,
        lastUpdatedAt: lastUpdatedAt ?? this.lastUpdatedAt,
        materializationSamples:
            materializationSamples ?? this.materializationSamples,
        lastIngestLatencyMs: lastIngestLatencyMs ?? this.lastIngestLatencyMs,
        phaseSamples: phaseSamples ?? this.phaseSamples,
        phaseLast: phaseLast ?? this.phaseLast,
        registrationTimings: registrationTimings ?? this.registrationTimings,
      );
}

QueryTelemetry emptyTelemetry() => const QueryTelemetry(
      updateCount: 0,
      errorCount: 0,
      lastUpdatedAt: null,
      materializationSamples: [],
      lastIngestLatencyMs: null,
      phaseSamples: {},
      phaseLast: {},
      registrationTimings: RegistrationTimings.empty,
    );

class QueryEntry {
  const QueryEntry({
    required this.def,
    required this.lifecycle,
    required this.remoteArray,
    required this.localArray,
    required this.subqueryRemoteArray,
    required this.records,
    required this.serverState,
    required this.subscribers,
    required this.lastSubscriberLeftAt,
    required this.lastHeartbeatAt,
    required this.lastPolledAt,
    required this.registerAttempts,
    required this.telemetry,
  });

  final QueryDefinition def;
  final QueryLifecycle lifecycle;

  /// Server membership (ids + versions) or the durable seed.
  final RecordVersionArray remoteArray;

  /// The in-process SSP view's id-set: local truth for "does this row match".
  final RecordVersionArray localArray;
  final RecordVersionArray subqueryRemoteArray;
  final List<Row> records;
  final ServerViewState? serverState;
  final int subscribers;
  final int? lastSubscriberLeftAt;
  final int? lastHeartbeatAt;
  final int? lastPolledAt;
  final int registerAttempts;
  final QueryTelemetry telemetry;

  QueryEntry copyWith({
    QueryDefinition? def,
    QueryLifecycle? lifecycle,
    RecordVersionArray? remoteArray,
    RecordVersionArray? localArray,
    RecordVersionArray? subqueryRemoteArray,
    List<Row>? records,
    ServerViewState? serverState,
    bool clearServerState = false,
    int? subscribers,
    int? lastSubscriberLeftAt,
    bool clearLastSubscriberLeftAt = false,
    int? lastHeartbeatAt,
    int? lastPolledAt,
    int? registerAttempts,
    QueryTelemetry? telemetry,
  }) =>
      QueryEntry(
        def: def ?? this.def,
        lifecycle: lifecycle ?? this.lifecycle,
        remoteArray: remoteArray ?? this.remoteArray,
        localArray: localArray ?? this.localArray,
        subqueryRemoteArray: subqueryRemoteArray ?? this.subqueryRemoteArray,
        records: records ?? this.records,
        serverState:
            clearServerState ? null : (serverState ?? this.serverState),
        subscribers: subscribers ?? this.subscribers,
        lastSubscriberLeftAt: clearLastSubscriberLeftAt
            ? null
            : (lastSubscriberLeftAt ?? this.lastSubscriberLeftAt),
        lastHeartbeatAt: lastHeartbeatAt ?? this.lastHeartbeatAt,
        lastPolledAt: lastPolledAt ?? this.lastPolledAt,
        registerAttempts: registerAttempts ?? this.registerAttempts,
        telemetry: telemetry ?? this.telemetry,
      );
}

enum OutboxStatus { pending, acked }

/// In-memory mirror of one `_00_pending_mutations` row plus its push progress.
class OutboxItem {
  const OutboxItem({
    required this.id,
    required this.type,
    required this.recordId,
    required this.table,
    required this.status,
    required this.ackedAt,
    required this.attempts,
  });

  final String id;
  final MutationEventType type;
  final String recordId;
  final String table;
  final OutboxStatus status;
  final int? ackedAt;
  final int attempts;

  OutboxItem copyWith({
    OutboxStatus? status,
    int? ackedAt,
    int? attempts,
  }) =>
      OutboxItem(
        id: id,
        type: type,
        recordId: recordId,
        table: table,
        status: status ?? this.status,
        ackedAt: ackedAt ?? this.ackedAt,
        attempts: attempts ?? this.attempts,
      );
}

/// Kept for shape parity with the TypeScript core. A Flutter app is one
/// process, so the Dart runtime never leaves [TabRole.solo].
enum TabRole { solo, leader, follower }

/// A debounced update accumulating locally until its flush writes the outbox
/// row.
class PendingWrite {
  const PendingWrite({
    required this.key,
    required this.table,
    required this.recordId,
    required this.data,
    required this.before,
    required this.firstAt,
  });

  final String key;
  final String table;
  final String recordId;
  final Map<String, dynamic> data;
  final Map<String, dynamic>? before;
  final int firstAt;
}

class SyncSlice {
  const SyncSlice({
    required this.health,
    required this.consecutiveFailures,
    required this.hasSyncedOnce,
    required this.selfHealAttempts,
    required this.pollIdleStreak,
    required this.lastReconnectRefetchAt,
    required this.needsResubscribe,
    required this.fetchAttempts,
    required this.liveUuid,
    required this.liveTable,
    required this.lanes,
  });

  final SyncHealth health;
  final int consecutiveFailures;
  final bool hasSyncedOnce;
  final int selfHealAttempts;
  final int pollIdleStreak;
  final int? lastReconnectRefetchAt;
  final bool needsResubscribe;
  final int fetchAttempts;
  final String? liveUuid;
  final String? liveTable;
  final LaneState lanes;

  SyncSlice copyWith({
    SyncHealth? health,
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
    LaneState? lanes,
  }) =>
      SyncSlice(
        health: health ?? this.health,
        consecutiveFailures: consecutiveFailures ?? this.consecutiveFailures,
        hasSyncedOnce: hasSyncedOnce ?? this.hasSyncedOnce,
        selfHealAttempts: selfHealAttempts ?? this.selfHealAttempts,
        pollIdleStreak: pollIdleStreak ?? this.pollIdleStreak,
        lastReconnectRefetchAt: clearLastReconnectRefetchAt
            ? null
            : (lastReconnectRefetchAt ?? this.lastReconnectRefetchAt),
        needsResubscribe: needsResubscribe ?? this.needsResubscribe,
        fetchAttempts: fetchAttempts ?? this.fetchAttempts,
        liveUuid: clearLiveUuid ? null : (liveUuid ?? this.liveUuid),
        liveTable: clearLiveTable ? null : (liveTable ?? this.liveTable),
        lanes: lanes ?? this.lanes,
      );
}

class ClientState {
  const ClientState({
    required this.sessionId,
    required this.userId,
    required this.saltUserId,
    required this.pendingBucket,
    required this.tabId,
    required this.tabRole,
    required this.bucketId,
    required this.localReady,
    required this.primed,
    required this.queries,
    required this.registering,
    required this.membershipReread,
    required this.versions,
    required this.outbox,
    required this.pendingWrites,
    required this.failedCount,
    required this.dirty,
    required this.membershipDirty,
    required this.sync,
  });

  final String? sessionId;
  final String? userId;

  /// The principal the current session salt was minted for.
  final String? saltUserId;

  /// Latest bucket-switch target; an older switch that wakes up to a newer
  /// target skips.
  final String? pendingBucket;
  final String tabId;
  final TabRole tabRole;
  final String? bucketId;
  final bool localReady;
  final bool primed;
  final Map<QueryHash, QueryEntry> queries;

  /// Hashes whose local registration is in flight (dedupes concurrent `query()`
  /// calls).
  final Set<QueryHash> registering;

  /// Re-read attempts per hash while the server reports a view as
  /// `materializing`.
  final Map<QueryHash, int> membershipReread;

  /// Local body versions (`_00_rv`) by encoded record id.
  final Map<String, int> versions;
  final List<OutboxItem> outbox;
  final Map<String, PendingWrite> pendingWrites;
  final int failedCount;

  /// Queries whose render inputs changed since their last materialization.
  final Set<QueryHash> dirty;

  /// Queries whose server membership must be re-read.
  final Set<QueryHash> membershipDirty;
  final SyncSlice sync;

  ClientState copyWith({
    String? sessionId,
    bool clearSessionId = false,
    String? userId,
    bool clearUserId = false,
    String? saltUserId,
    bool clearSaltUserId = false,
    String? pendingBucket,
    bool clearPendingBucket = false,
    String? tabId,
    TabRole? tabRole,
    String? bucketId,
    bool clearBucketId = false,
    bool? localReady,
    bool? primed,
    Map<QueryHash, QueryEntry>? queries,
    Set<QueryHash>? registering,
    Map<QueryHash, int>? membershipReread,
    Map<String, int>? versions,
    List<OutboxItem>? outbox,
    Map<String, PendingWrite>? pendingWrites,
    int? failedCount,
    Set<QueryHash>? dirty,
    Set<QueryHash>? membershipDirty,
    SyncSlice? sync,
  }) =>
      ClientState(
        sessionId: clearSessionId ? null : (sessionId ?? this.sessionId),
        userId: clearUserId ? null : (userId ?? this.userId),
        saltUserId: clearSaltUserId ? null : (saltUserId ?? this.saltUserId),
        pendingBucket:
            clearPendingBucket ? null : (pendingBucket ?? this.pendingBucket),
        tabId: tabId ?? this.tabId,
        tabRole: tabRole ?? this.tabRole,
        bucketId: clearBucketId ? null : (bucketId ?? this.bucketId),
        localReady: localReady ?? this.localReady,
        primed: primed ?? this.primed,
        queries: queries ?? this.queries,
        registering: registering ?? this.registering,
        membershipReread: membershipReread ?? this.membershipReread,
        versions: versions ?? this.versions,
        outbox: outbox ?? this.outbox,
        pendingWrites: pendingWrites ?? this.pendingWrites,
        failedCount: failedCount ?? this.failedCount,
        dirty: dirty ?? this.dirty,
        membershipDirty: membershipDirty ?? this.membershipDirty,
        sync: sync ?? this.sync,
      );
}

SyncHealth initialHealth(
        [ConnectionState connection = ConnectionState.disconnected]) =>
    SyncHealth(
      status: SyncHealthStatus.healthy,
      consecutiveFailures: 0,
      everConnected: false,
      connection: connection,
    );

ClientState emptyState({required String tabId}) => ClientState(
      sessionId: null,
      userId: null,
      saltUserId: null,
      pendingBucket: null,
      tabId: tabId,
      tabRole: TabRole.solo,
      bucketId: null,
      localReady: false,
      primed: false,
      queries: const {},
      registering: const {},
      membershipReread: const {},
      versions: const {},
      outbox: const [],
      pendingWrites: const {},
      failedCount: 0,
      dirty: const {},
      membershipDirty: const {},
      sync: SyncSlice(
        health: initialHealth(),
        consecutiveFailures: 0,
        hasSyncedOnce: false,
        selfHealAttempts: 0,
        pollIdleStreak: 0,
        lastReconnectRefetchAt: null,
        needsResubscribe: false,
        fetchAttempts: 0,
        liveUuid: null,
        liveTable: null,
        lanes: emptyLanes(),
      ),
    );
