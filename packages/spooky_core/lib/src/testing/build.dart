import '../state/client_state.dart';
import '../state/lifecycle.dart';
import '../state/reducers.dart';
import '../surreal/value.dart';
import '../types.dart';

/// Builders for tests over [ClientState]. Mirrors
/// `packages/core/src/testing/build.ts`.

QueryDefinition buildDefinition({
  String hash = 'h1',
  RecordId? id,
  String? viewKey,
  String surql = 'SELECT * FROM thing',
  Map<String, dynamic> params = const {},
  QueryTimeToLive ttl = '10m',
  int ttlMs = 600000,
  String tableName = 'thing',
  int createdAt = 1700000000000,
  bool hasExplicitOrder = false,
}) =>
    QueryDefinition(
      id: id ?? RecordId('_00_query', hash),
      hash: hash,
      viewKey: viewKey ?? 'view-$hash',
      surql: surql,
      params: params,
      ttl: ttl,
      ttlMs: ttlMs,
      tableName: tableName,
      createdAt: createdAt,
      hasExplicitOrder: hasExplicitOrder,
    );

QueryEntry buildEntry({
  QueryDefinition? def,
  QueryLifecycle? lifecycle,
  RecordVersionArray remoteArray = const [],
  RecordVersionArray localArray = const [],
  RecordVersionArray subqueryRemoteArray = const [],
  List<Row> records = const [],
  ServerViewState? serverState,
  int subscribers = 0,
  int? lastSubscriberLeftAt,
  int? lastHeartbeatAt,
  int? lastPolledAt,
  int registerAttempts = 0,
  QueryTelemetry? telemetry,
}) =>
    QueryEntry(
      def: def ?? buildDefinition(),
      lifecycle: lifecycle ?? seedLifecycle(false),
      remoteArray: remoteArray,
      localArray: localArray,
      subqueryRemoteArray: subqueryRemoteArray,
      records: records,
      serverState: serverState,
      subscribers: subscribers,
      lastSubscriberLeftAt: lastSubscriberLeftAt,
      lastHeartbeatAt: lastHeartbeatAt,
      lastPolledAt: lastPolledAt,
      registerAttempts: registerAttempts,
      telemetry: telemetry ?? emptyTelemetry(),
    );

OutboxItem buildOutboxItem({
  String id = 'm1',
  MutationEventType type = MutationEventType.create,
  String recordId = 'thing:1',
  String table = 'thing',
  OutboxStatus status = OutboxStatus.pending,
  int? ackedAt,
  int attempts = 0,
}) =>
    OutboxItem(
      id: id,
      type: type,
      recordId: recordId,
      table: table,
      status: status,
      ackedAt: ackedAt,
      attempts: attempts,
    );

/// A state holding [entries], with `dirty` cleared (as after materialization).
ClientState buildState([
  List<QueryEntry> entries = const [],
  List<Reducer> extra = const [],
]) {
  final base = compose([for (final e in entries) putQuery(e)])(
      emptyState(tabId: 'tab-a'));
  return compose(extra)(base.copyWith(dirty: const {}));
}
