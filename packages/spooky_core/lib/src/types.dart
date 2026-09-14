import 'dart:async';

import 'events/event_system.dart';
import 'ffi/stream_update.dart' show RecordVersionArray;
import 'modules/query_builder.dart' show RelationPlan;
import 'surreal/value.dart';
import 'utils/duration_utils.dart';

export 'ffi/stream_update.dart' show RecordVersion, RecordVersionArray;
export 'utils/duration_utils.dart' show QueryTimeToLive, defaultTtl;

/// Local store backend (TS `StoreType`).
enum StoreType { memory, indexeddb }

/// Custom storage backend interface (TS `PersistenceClient`).
abstract class PersistenceClient {
  Future<void> set(String key, dynamic value);
  Future<T?> get<T>(String key);
  Future<void> remove(String key);
}

/// Result of registering/executing a query (TS `Sp00kyQueryResult`).
class Sp00kyQueryResult {
  const Sp00kyQueryResult(this.hash);
  final String hash;
}

/// Database connection configuration (TS `Sp00kyConfig.database`).
class DatabaseConfig {
  const DatabaseConfig({
    this.endpoint,
    required this.namespace,
    required this.database,
    this.store = StoreType.memory,
    this.localDbPath,
    this.token,
  });

  final String? endpoint;
  final String namespace;
  final String database;
  final StoreType store;

  /// Filesystem path for the local sqlite store when [store] is not
  /// [StoreType.memory]. When null, a non-memory store falls back to the
  /// default relative path (`spooky.db`). Ignored for [StoreType.memory].
  final String? localDbPath;

  final String? token;
}

/// Configuration for the Sp00ky client (TS `Sp00kyConfig`).
class Sp00kyConfig {
  const Sp00kyConfig({
    required this.database,
    required this.schema,
    required this.schemaSurql,
    this.logLevel = 'info',
    this.persistenceClient,
    this.streamDebounceTime = 50,
    this.crdtDebounceMs = 500,
    this.refSyncIntervalMs = 500,
    this.enableAnonymousLiveQueries = false,
    this.syncHealth = const SyncHealthConfig(),
    this.liveInlineBodies = false,
    this.reconnect = const ReconnectConfig(),
    this.circuitCheckpointMs = 30000,
  });

  final DatabaseConfig database;

  /// The schema definition: table name -> column name -> [ColumnSchema].
  /// (The TS generic `S extends SchemaStructure` collapses to runtime data.)
  final Map<String, dynamic> schema;

  /// The compiled SURQL schema string (used for provisioning + permissions).
  final String schemaSurql;
  final String logLevel;
  final dynamic persistenceClient;
  /// Trailing coalesce (ms) before a dirty query re-materializes. Matches the
  /// browser core's `MATERIALIZE_DEBOUNCE_MS`, so the two clients repaint on
  /// the same cadence.
  final int streamDebounceTime;
  final int crdtDebounceMs;
  final int refSyncIntervalMs;

  /// Enable realtime sync while signed out. When true, the client runs its
  /// `_00_list_ref` poll + LIVE subscription against the shared
  /// `_00_list_ref_anon` table even with no authenticated user, so a logged-out
  /// client gets live `query().stream()` updates over world-readable tables.
  /// Requires the server deployed with `anonymousLiveQueries: true` in
  /// `sp00ky.yml` (this flag must match it). Defaults to false: anonymous
  /// clients can read one-shot (`queryRemote`) but never sync live.
  final bool enableAnonymousLiveQueries;

  /// Surface sustained sync failures as a `degraded` health status the app can
  /// observe via `Sp00kyClient.subscribeToSyncHealth` / `syncHealthStream` to
  /// render a "can't reach the server" banner.
  ///
  /// Individual failures (a transient remote 500 on query registration, a
  /// dropped WebSocket) are always swallowed and retried; they never throw at
  /// the app. This only controls when a *run* of consecutive failures is
  /// reported. Status flips back to `healthy` on the next successful round.
  /// Defaults to `SyncHealthConfig()` (degrade after 3); pass
  /// [SyncHealthConfig.disabled] to never report degraded.
  final SyncHealthConfig? syncHealth;

  /// Ask the LIVE subscription to join the changed row onto its notification
  /// (`FETCH out`), so a body arrives with the doorbell instead of costing a
  /// second round trip.
  ///
  /// Only ever an optimisation: a server that ignores the clause, or a row the
  /// session may not read, simply yields no inline row and the fetch path takes
  /// over. Requires a server that publishes readable `out` targets.
  final bool liveInlineBodies;

  /// Transport supervision knobs: the reconnect backoff cap and the liveness
  /// probe that detects a half-open socket.
  final ReconnectConfig reconnect;

  /// How often (ms) the circuit's rows are snapshotted to the local store
  /// while they are dirty, so the next boot restores them instead of
  /// re-reading every row. Matches the browser core's `circuitCheckpointMs`.
  /// `0` disables the interval; `checkpoint()`, close and account switches
  /// still snapshot.
  final int circuitCheckpointMs;
}

/// Tunables for sync-health reporting (TS `SyncHealthConfig`).
/// See [Sp00kyConfig.syncHealth].
class SyncHealthConfig {
  const SyncHealthConfig({this.degradeAfterConsecutiveFailures = 3});

  /// Never report `degraded` (TS `syncHealth: false`).
  const SyncHealthConfig.disabled() : degradeAfterConsecutiveFailures = 0;

  /// Consecutive failed sync rounds (up or down) before the status flips from
  /// `healthy` to `degraded`. A single transient failure is absorbed by the
  /// retry; only a sustained run trips the banner. `0` disables reporting.
  final int degradeAfterConsecutiveFailures;
}

/// Sync-health state (TS `SyncHealthStatus`).
enum SyncHealthStatus { healthy, degraded }

/// Snapshot of sync health delivered to `subscribeToSyncHealth` subscribers
/// (TS `SyncHealth`).
class SyncHealth {
  const SyncHealth({
    required this.status,
    required this.consecutiveFailures,
    this.kind,
    this.error,
    required this.everConnected,
    this.connection = ConnectionState.disconnected,
  });

  /// [SyncHealthStatus.degraded] once consecutive failures cross the threshold.
  final SyncHealthStatus status;

  /// Consecutive failed sync rounds at the moment of this report.
  final int consecutiveFailures;

  /// Classification of the most recent failure (`network` / `application`).
  /// Only set while degraded.
  final String? kind;

  /// Message of the most recent failure. Only set while degraded.
  final String? error;

  /// True once at least one sync round has succeeded this session. Lets a UI
  /// tell a first-time "connecting" phase (never reached the server, so a
  /// cold-start failure run is expected) apart from a real lost connection
  /// after a working session. Never resets once set.
  final bool everConnected;

  /// Transport state of the socket, independent of [status]: a `connected`
  /// socket can still be degraded, and a `reconnecting` one is usually still
  /// healthy for the first few seconds.
  final ConnectionState connection;

  bool get isDegraded => status == SyncHealthStatus.degraded;

  SyncHealth copyWith({
    SyncHealthStatus? status,
    int? consecutiveFailures,
    String? kind,
    String? error,
    bool? everConnected,
    ConnectionState? connection,
  }) =>
      SyncHealth(
        status: status ?? this.status,
        consecutiveFailures: consecutiveFailures ?? this.consecutiveFailures,
        kind: kind ?? this.kind,
        error: error ?? this.error,
        everConnected: everConnected ?? this.everConnected,
        connection: connection ?? this.connection,
      );

  @override
  bool operator ==(Object other) =>
      other is SyncHealth &&
      other.status == status &&
      other.consecutiveFailures == consecutiveFailures &&
      other.kind == kind &&
      other.error == error &&
      other.everConnected == everConnected &&
      other.connection == connection;

  @override
  int get hashCode => Object.hash(
      status, consecutiveFailures, kind, error, everConnected, connection);
}

typedef QueryHash = String;

/// Difference between two record-version sets (TS `RecordVersionDiff`).
class RecordVersionDiff {
  RecordVersionDiff({
    required this.added,
    required this.updated,
    required this.removed,
  });

  final List<({RecordId id, int version})> added;
  final List<({RecordId id, int version})> updated;
  final List<RecordId> removed;
}

/// Configuration for a specific query instance (TS `QueryConfig`).
///
/// Fields are mutable: `processStreamUpdate` and sync mutate them in place.
class QueryConfig {
  QueryConfig({
    required this.id,
    required this.surql,
    required this.params,
    required this.localArray,
    required this.remoteArray,
    required this.ttl,
    required this.lastActiveAt,
    required this.tableName,
    this.relations = const [],
  });

  final RecordId id;
  String surql;
  Map<String, dynamic> params;
  RecordVersionArray localArray;
  RecordVersionArray remoteArray;
  QueryTimeToLive ttl;
  DateTime lastActiveAt;
  String tableName;

  /// The query's `.related()` plan, resolved from the local cache on every
  /// materialization. Not persisted: it is rebuilt from the caller's builder on
  /// each registration (the surql, which IS persisted, already encodes the same
  /// relation shape).
  List<RelationPlan> relations;

  /// Version array of the query's SUBQUERY child edges, diffed to fetch joined
  /// bodies. In-memory only: child rows must never enter the persisted primary
  /// [remoteArray], which is the query's own window.
  RecordVersionArray? subqueryRemoteArray;
}

/// Cap on the rolling materialization-sample window per query
/// (TS `MATERIALIZATION_SAMPLE_WINDOW`).
const int materializationSampleWindow = 100;

/// Timed processing phases surfaced per query (TS `TimingPhase`). `ssp` is the
/// native-ingest wall time tracked separately in
/// [QueryState.materializationSamples]; these are the rolling windows the core
/// records around its own work.
class TimingPhase {
  static const sspStoreApply = 'sspStoreApply';
  static const sspCircuitStep = 'sspCircuitStep';
  static const sspTransform = 'sspTransform';
  static const localFetch = 'localFetch';
  static const remoteFetch = 'remoteFetch';
  static const frontend = 'frontend';
}

/// Percentile summary for one timed phase (TS `PhaseStat`).
class PhaseStat {
  const PhaseStat({
    this.lastMs,
    this.p50,
    this.p90,
    this.p99,
    this.count = 0,
  });

  final double? lastMs;
  final double? p50;
  final double? p90;
  final double? p99;
  final int count;
}

/// Internal state of a live query (TS `QueryState`).
class QueryState {
  QueryState({
    required this.config,
    List<Map<String, dynamic>>? records,
    this.ttlTimer,
    this.ttlDurationMs = 0,
    this.updateCount = 0,
    List<double>? materializationSamples,
    this.lastIngestLatencyMs,
    this.errorCount = 0,
    this.status = QueryStatus.idle,
    Map<String, List<double>>? phaseSamples,
    Map<String, double>? phaseLast,
  })  : records = records ?? [],
        materializationSamples = materializationSamples ?? [],
        phaseSamples = phaseSamples ?? {},
        phaseLast = phaseLast ?? {};

  QueryConfig config;
  List<Map<String, dynamic>> records;
  Timer? ttlTimer;
  int ttlDurationMs;
  int updateCount;
  List<double> materializationSamples;
  double? lastIngestLatencyMs;
  int errorCount;

  /// Current fetch status (idle/fetching). Driven by the refcounted
  /// `DataModule.beginFetching`/`endFetching` pair around registration and
  /// per-query record fetches; observed via `DataModule.subscribeStatus`.
  ///
  /// With a remote configured, queries are born [QueryStatus.fetching] (TS
  /// `createNewQuery`): a freshly mounted query is loading until its
  /// registration settles, so a consumer never sees a spurious `idle` before
  /// the first result lands. Divergence from the TS core: in local-only mode
  /// nothing ever registers the query remotely, so there is no lifecycle to
  /// resolve the status and queries start [QueryStatus.idle] instead.
  QueryStatus status;

  /// Rolling per-phase timing windows (TS `QueryState.phaseSamples`), capped at
  /// [materializationSampleWindow] samples each. Keys are [TimingPhase] values.
  final Map<String, List<double>> phaseSamples;

  /// Most recent sample per phase (TS `QueryState.phaseLast`).
  final Map<String, double> phaseLast;

  /// Set once `applyHydration` has run for this query, so the cold
  /// instant-hydrate path fires at most once (TS `QueryState.hydrated`).
  bool hydrated = false;

  /// Set once `notifyQuerySynced` has emitted for this query. Ephemeral (not
  /// persisted): a re-registered query re-emits even when its result set is
  /// unchanged, so an empty window still stops a consumer's loading state
  /// (TS `QueryState.syncNotified`).
  bool syncNotified = false;
}

/// Notified with the latest result set for a query (TS `QueryUpdateCallback`).
typedef QueryUpdateCallback = void Function(List<Map<String, dynamic>> records);

/// A query's fetch status (TS `QueryStatus`): `idle` when settled, `fetching`
/// while record bodies are being pulled from the remote.
enum QueryStatus { idle, fetching }

/// Notified when a query's fetch status changes (TS `QueryStatusCallback`).
typedef QueryStatusCallback = void Function(QueryStatus status);

/// Mutation kind (TS `MutationEventType`).
enum MutationEventType { create, update, delete }

/// A mutation to be synchronized (TS `MutationEvent`).
class MutationEvent {
  MutationEvent({
    required this.type,
    required this.mutationId,
    required this.recordId,
    this.data,
    this.record,
    this.options,
    required this.createdAt,
  });

  final MutationEventType type;
  final RecordId mutationId;
  final RecordId recordId;
  final dynamic data;
  final dynamic record;
  final PushEventOptions? options;
  final DateTime createdAt;
}

/// Options for `run` operations (TS `RunOptions`).
class RunOptions {
  const RunOptions({
    this.assignedTo,
    this.maxRetries,
    this.retryStrategy,
    this.timeout,
    this.delay,
  });

  final String? assignedTo;
  final int? maxRetries;
  final String? retryStrategy;
  final int? timeout;

  /// Minimum delay in milliseconds before the job is eligible to run. While
  /// delayed the job stays pending (enqueued) and can still be killed.
  final int? delay;
}

/// Debounce key strategy for updates (TS `DebounceOptions.key`).
enum DebounceKey { recordId, recordIdXFields }

/// Configuration for debouncing updates (TS `DebounceOptions`).
class DebounceOptions {
  const DebounceOptions({this.key, this.delay});
  final DebounceKey? key;
  final int? delay;
}

/// Options for update operations (TS `UpdateOptions`).
///
/// `debounced` is either a bool (default behavior) or a [DebounceOptions].
class UpdateOptions {
  const UpdateOptions({this.debounced});
  final Object? debounced;
}

/// Transport state of the remote socket, independent of sync health (TS
/// `ConnectionState`). A `connected` socket can still be `degraded` (the server
/// is erroring), and a `reconnecting` socket is usually still healthy for the
/// first few seconds.
enum ConnectionState { connecting, connected, reconnecting, disconnected }

/// A row that rode in on a LIVE notification because the subscription was
/// opened with the body joined on (TS `InlineRow`). `version` is the edge's
/// version, the same number a membership read would report for this row.
class InlineRow {
  const InlineRow({
    required this.id,
    required this.version,
    required this.record,
  });

  final String id;
  final int version;
  final Map<String, dynamic> record;
}

/// What the server's `_00_query` row says about a view, read alongside its
/// `_00_list_ref` edges (TS `ServerViewMeta`).
///
/// - [present] false: the row is not there (or not readable). The SSP no longer
///   has this view: a TTL sweep, an SSP reset, a scheduler wipe. An empty edge
///   set read at the same time is NOT "no rows", it is "no view".
/// - [state] `'materializing'`: the SSP has (re)computed the set and its edges
///   are in flight; an empty read is "not landed yet".
/// - [state] `'ready'`: the edges are committed in the same transaction, so a
///   [rowCount] of 0 with no edges is a real empty result.
class ServerViewMeta {
  const ServerViewMeta({
    required this.present,
    this.rowCount,
    this.state,
  });

  static const absent = ServerViewMeta(present: false);

  final bool present;
  final int? rowCount;
  final String? state;
}

/// One-shot registration timings (ms), captured once when a query registers
/// (TS `RegistrationTimings`).
class RegistrationTimings {
  const RegistrationTimings({
    this.parseMs,
    this.planMs,
    this.snapshotMs,
    this.wallMs,
  });

  static const empty = RegistrationTimings();

  /// SSP surql -> plan parse + permission injection.
  final double? parseMs;

  /// SSP operator-DAG build.
  final double? planMs;

  /// SSP initial snapshot evaluation.
  final double? snapshotMs;

  /// Wall time of the `register_view` round trip.
  final double? wallMs;
}

/// Observer for a query's authority flips (TS `QueryAuthorityCallback`): true
/// once server membership is known for it (registration, poll, durable seed),
/// false when it is reset by a bucket switch. What `isAuthoritative()` mirrors.
typedef QueryAuthorityCallback = void Function(bool known);

/// Per-query processing-time breakdown (TS `QueryTimings`).
class QueryTimings {
  const QueryTimings({
    required this.ssp,
    required this.sspStoreApply,
    required this.sspCircuitStep,
    required this.sspTransform,
    required this.localFetch,
    required this.remoteFetch,
    required this.frontend,
    required this.registration,
    required this.updateCount,
    required this.errorCount,
  });

  /// End-to-end native ingest wall time.
  final PhaseStat ssp;
  final PhaseStat sspStoreApply;
  final PhaseStat sspCircuitStep;
  final PhaseStat sspTransform;
  final PhaseStat localFetch;
  final PhaseStat remoteFetch;
  final PhaseStat frontend;
  final RegistrationTimings registration;
  final int updateCount;
  final int errorCount;
}

/// Transport supervision knobs (TS `ReconnectConfig`).
class ReconnectConfig {
  const ReconnectConfig({
    this.retryDelayMaxMs = 15000,
    this.heartbeatIntervalMs = 20000,
    this.heartbeatTimeoutMs = 10000,
    this.connectTimeoutMs = 10000,
  });

  /// Cap on the supervisor's exponential backoff between reconnect attempts.
  final int retryDelayMaxMs;

  /// Cadence of the application-level liveness probe (`RETURN true`) that
  /// detects a half-open socket the transport never reports as closed. `0`
  /// disables the heartbeat.
  final int heartbeatIntervalMs;

  /// Deadline for a heartbeat response. Exceeding it means the socket is dead
  /// regardless of what it claims, so the connection is torn down and rebuilt.
  final int heartbeatTimeoutMs;

  /// Deadline for opening a socket.
  ///
  /// A refused port fails fast, but a black-holed one (a captive portal, a
  /// dropped route, a firewall that drops instead of rejecting) never answers
  /// at all. Without this the revive loop parks inside a single attempt
  /// forever: it never retries, and never reports the connection as gone.
  final int connectTimeoutMs;
}
