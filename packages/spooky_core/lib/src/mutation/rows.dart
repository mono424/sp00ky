import '../kernel/effects.dart';
import '../services/stream_processor/stream_processor_service.dart'
    show IngestOp, IngestRecord;
import '../state/client_state.dart';
import '../surreal/value.dart';
import '../types.dart';
import '../utils/parser.dart';
import '../utils/record_id_utils.dart';
import 'mutation_id.dart' show pendingTable;

const String failedTable = '_00_failed_mutations';
const int pendingRowVersion = 2;

/// How much of a rejected mutation could be undone locally.
enum RevertKind { full, partial, none }

enum FailedErrorKind { application, unreplayable }

/// One `_00_pending_mutations` row as this client writes it (v2).
class PendingMutationRow {
  const PendingMutationRow({
    required this.id,
    required this.mutationType,
    required this.recordId,
    required this.tableName,
    this.data,
    this.beforeRecord,
    required this.createdAt,
    required this.v,
  });

  final String id;
  final MutationEventType mutationType;
  final String recordId;
  final String tableName;
  final Map<String, dynamic>? data;
  final Map<String, dynamic>? beforeRecord;
  final int createdAt;
  final int v;

  Map<String, dynamic> toJson() => {
        'mutationType': mutationType.name,
        'recordId': recordId,
        'tableName': tableName,
        if (data != null) 'data': data,
        if (beforeRecord != null) 'beforeRecord': beforeRecord,
        'createdAt': createdAt,
        'v': v,
      };

  PendingMutationRow withData(Map<String, dynamic>? next) => PendingMutationRow(
        id: id,
        mutationType: mutationType,
        recordId: recordId,
        tableName: tableName,
        data: next,
        beforeRecord: beforeRecord,
        createdAt: createdAt,
        v: v,
      );
}

class FailedMutationError {
  const FailedMutationError({required this.message, required this.kind});
  final String message;
  final FailedErrorKind kind;

  Map<String, dynamic> toJson() => {'message': message, 'kind': kind.name};
}

class FailedMutationRow {
  const FailedMutationRow({
    required this.id,
    required this.mutationType,
    required this.recordId,
    required this.tableName,
    this.data,
    this.beforeRecord,
    required this.error,
    required this.attempts,
    required this.createdAt,
    required this.failedAt,
    required this.revert,
  });

  final String id;
  final MutationEventType mutationType;
  final String recordId;
  final String tableName;
  final Map<String, dynamic>? data;
  final Map<String, dynamic>? beforeRecord;
  final FailedMutationError error;
  final int attempts;
  final int createdAt;
  final int failedAt;
  final RevertKind revert;

  Map<String, dynamic> toJson() => {
        'mutationType': mutationType.name,
        'recordId': recordId,
        'tableName': tableName,
        if (data != null) 'data': data,
        'beforeRecord': beforeRecord,
        'error': error.toJson(),
        'attempts': attempts,
        'createdAt': createdAt,
        'failedAt': failedAt,
        'revert': revert.name,
      };

  /// The pending row this tray entry can be replayed as.
  PendingMutationRow toPending() => PendingMutationRow(
        id: id,
        mutationType: mutationType,
        recordId: recordId,
        tableName: tableName,
        data: data,
        beforeRecord: beforeRecord,
        createdAt: createdAt,
        v: pendingRowVersion,
      );
}

MutationEventType? _parseType(Object? raw) => switch (raw) {
      'create' => MutationEventType.create,
      'update' => MutationEventType.update,
      'delete' => MutationEventType.delete,
      _ => null,
    };

/// Stable `table:id` string for an id read from the store, SurrealDB's `⟨⟩`
/// escaping removed (outbox ids contain `_`, so a SurrealDB-written row comes
/// back escaped).
String storedIdString(Object? id) {
  if (id is RecordId) return '${id.table}:${id.id}';
  final raw = id.toString();
  final colon = raw.indexOf(':');
  if (colon < 0) return raw;
  var rest = raw.substring(colon + 1);
  if (rest.startsWith('⟨') && rest.endsWith('⟩')) {
    rest = rest.substring(1, rest.length - 1);
  }
  return '${raw.substring(0, colon)}:$rest';
}

final _tsPrefix = RegExp(r'^⟨?(\d{13})');

/// Timestamp prefix of a v1/v2 mutation id, or null for a legacy numeric id.
int? createdAtFromId(String id) {
  final colon = id.indexOf(':');
  final raw = colon >= 0 ? id.substring(colon + 1) : id;
  final m = _tsPrefix.firstMatch(raw);
  return m == null ? null : int.tryParse(m.group(1)!);
}

/// Read any row shape this table ever had. Legacy rows (`v` missing) lack
/// `tableName`, `createdAt` and `beforeRecord`. A create without `data` is not
/// replayable and returns null.
PendingMutationRow? parsePendingRow(Object? row) {
  if (row is! Map) return null;
  final type = _parseType(row['mutationType']);
  if (type == null) return null;
  final rawRecordId = row['recordId'];
  if (rawRecordId is! String && rawRecordId is! RecordId) return null;
  final id = storedIdString(row['id']);
  final recordId =
      rawRecordId is String ? rawRecordId : storedIdString(rawRecordId);
  final data = row['data'] is Map
      ? Map<String, dynamic>.from(row['data'] as Map)
      : null;
  if (type == MutationEventType.create && data == null) return null;
  return PendingMutationRow(
    id: id,
    mutationType: type,
    recordId: recordId,
    tableName: row['tableName'] is String
        ? row['tableName'] as String
        : extractTablePart(recordId),
    data: data,
    beforeRecord: row['beforeRecord'] is Map
        ? Map<String, dynamic>.from(row['beforeRecord'] as Map)
        : null,
    createdAt: row['createdAt'] is num
        ? (row['createdAt'] as num).toInt()
        : (createdAtFromId(id) ?? 0),
    v: row['v'] is num ? (row['v'] as num).toInt() : 1,
  );
}

FailedMutationRow? parseFailedRow(Object? row) {
  if (row is! Map) return null;
  final type = _parseType(row['mutationType']);
  if (type == null) return null;
  final recordId = row['recordId'];
  if (recordId is! String) return null;
  final err = row['error'] is Map ? row['error'] as Map : const {};
  return FailedMutationRow(
    id: storedIdString(row['id']).replaceFirst('$failedTable:', '$pendingTable:'),
    mutationType: type,
    recordId: recordId,
    tableName: row['tableName'] is String
        ? row['tableName'] as String
        : extractTablePart(recordId),
    data: row['data'] is Map
        ? Map<String, dynamic>.from(row['data'] as Map)
        : null,
    beforeRecord: row['beforeRecord'] is Map
        ? Map<String, dynamic>.from(row['beforeRecord'] as Map)
        : null,
    error: FailedMutationError(
      message: err['message'] is String ? err['message'] as String : 'unknown',
      kind: err['kind'] == 'unreplayable'
          ? FailedErrorKind.unreplayable
          : FailedErrorKind.application,
    ),
    attempts: row['attempts'] is num ? (row['attempts'] as num).toInt() : 0,
    createdAt: row['createdAt'] is num ? (row['createdAt'] as num).toInt() : 0,
    failedAt: row['failedAt'] is num ? (row['failedAt'] as num).toInt() : 0,
    revert: switch (row['revert']) {
      'partial' => RevertKind.partial,
      'none' => RevertKind.none,
      _ => RevertKind.full,
    },
  );
}

/// Re-type a stored row's payload from the schema before it is pushed. The
/// store keeps JSON, so a `record<user>` field comes back as `'user:x'` and a
/// datetime as a string; the server coerces neither. A payload the schema
/// rejects is sent raw, so the server's answer decides its fate rather than a
/// local throw.
PendingMutationRow hydrateRowData(
    PendingMutationRow row, Map<String, ColumnSchema>? columns) {
  if (row.data == null || columns == null) return row;
  try {
    return row.withData(parseQueryParams(columns, row.data!));
  } catch (_) {
    return row;
  }
}

OutboxItem toOutboxItem(PendingMutationRow row) => OutboxItem(
      id: row.id,
      type: row.mutationType,
      recordId: row.recordId,
      table: row.tableName,
      status: OutboxStatus.pending,
      ackedAt: null,
      attempts: 0,
    );

/// Outbox rows in drain order. Lexicographic id order is chronological order.
List<PendingMutationRow> sortPendingRows(List<PendingMutationRow> rows) =>
    [...rows]..sort((a, b) => a.id.compareTo(b.id));

List<FailedMutationRow> sortFailedRows(List<FailedMutationRow> rows) =>
    [...rows]..sort((a, b) => a.failedAt.compareTo(b.failedAt));

// ---- local write transactions ------------------------------------------------

class WritePlanInput {
  const WritePlanInput({
    required this.recordId,
    required this.mutationId,
    required this.table,
    this.data,
    this.before,
    required this.now,
  });

  final String recordId;
  final String mutationId;
  final String table;
  final Map<String, dynamic>? data;
  final Map<String, dynamic>? before;
  final int now;
}

PendingMutationRow _pendingRow(
  WritePlanInput input,
  MutationEventType type, {
  bool withData = false,
  bool withBefore = false,
}) =>
    PendingMutationRow(
      id: input.mutationId,
      mutationType: type,
      recordId: input.recordId,
      tableName: input.table,
      data: withData ? (input.data ?? const {}) : null,
      beforeRecord: withBefore ? input.before : null,
      createdAt: input.now,
      v: pendingRowVersion,
    );

PutOp _outboxOp(PendingMutationRow row) =>
    PutOp(pendingTable, row.id, row.toJson());

/// CREATE the row and its outbox entry in one transaction.
///
/// `_00_rv` is seeded to 1 because the local schema's DEFAULT does not apply to
/// a document write, so a later update's `_00_rv += 1` reaches 2.
List<LocalOp> planCreateTx(WritePlanInput input) => [
      PutOp(input.table, input.recordId, {
        ...?input.data,
        'id': input.recordId,
        '_00_rv': 1,
      }),
      _outboxOp(_pendingRow(input, MutationEventType.create, withData: true)),
    ];

/// Bump `_00_rv`, merge the patch, write the outbox row with its `beforeRecord`.
List<LocalOp> planUpdateTx(WritePlanInput input) => [
      BumpRvOp(input.table, input.recordId),
      PutOp(input.table, input.recordId, input.data ?? const {},
          mode: WriteMode.merge),
      _outboxOp(_pendingRow(input, MutationEventType.update,
          withData: true, withBefore: true)),
    ];

/// DELETE the row and write the outbox entry with its `beforeRecord`.
List<LocalOp> planDeleteTx(WritePlanInput input) => [
      DeleteOp(input.table, input.recordId),
      _outboxOp(
          _pendingRow(input, MutationEventType.delete, withBefore: true)),
    ];

/// Debounced update: bump `_00_rv` and merge now; the outbox row comes later.
List<LocalOp> planLocalOnlyUpdateTx(
        String table, String recordId, Map<String, dynamic> data) =>
    [
      BumpRvOp(table, recordId),
      PutOp(table, recordId, data, mode: WriteMode.merge),
    ];

/// The outbox row of a flushed debounced update, on its own.
List<LocalOp> planDeferredOutboxRowTx(WritePlanInput input) => [
      _outboxOp(_pendingRow(input, MutationEventType.update,
          withData: true, withBefore: true)),
    ];

// ---- remote push -------------------------------------------------------------

/// Many outbox rows as ONE multi-statement request. Statement `i` answers row
/// `i`. Each statement is its own transaction on the server.
({String sql, Map<String, dynamic> vars}) remoteBatch(
    List<PendingMutationRow> rows) {
  final vars = <String, dynamic>{};
  final stmts = <String>[];
  for (var i = 0; i < rows.length; i++) {
    final row = rows[i];
    vars['id$i'] = parseRecordIdString(row.recordId);
    switch (row.mutationType) {
      case MutationEventType.create:
        final data = row.data ?? const <String, dynamic>{};
        final sets = <String>[];
        for (final key in data.keys) {
          vars['d${i}_$key'] = data[key];
          sets.add('$key = \$d${i}_$key');
        }
        stmts.add('CREATE ONLY \$id$i SET ${sets.join(', ')}');
      case MutationEventType.update:
        vars['data$i'] = row.data ?? const <String, dynamic>{};
        stmts.add('UPDATE \$id$i MERGE \$data$i');
      case MutationEventType.delete:
        stmts.add('DELETE \$id$i');
    }
  }
  return (sql: stmts.join(';\n'), vars: vars);
}

// ---- rollback ----------------------------------------------------------------

class RevertPlan {
  const RevertPlan({
    required this.ops,
    required this.circuit,
    required this.revert,
  });

  /// Local writes that undo the mutation; empty when nothing can be restored.
  final List<LocalOp> ops;
  final IngestRecord circuit;
  final RevertKind revert;
}

/// How to undo a rejected mutation locally. A create is deleted; an update or
/// delete is restored from `beforeRecord` (replace, so the pre-bump `_00_rv`
/// comes back). Without a `beforeRecord` nothing can be restored.
RevertPlan planRevert(
    PendingMutationRow row, Map<String, dynamic>? before) {
  if (row.mutationType == MutationEventType.create) {
    return RevertPlan(
      ops: [DeleteOp(row.tableName, row.recordId)],
      circuit: IngestRecord(
        table: row.tableName,
        op: IngestOp.delete,
        id: row.recordId,
        record: row.data ?? const {},
      ),
      revert: RevertKind.full,
    );
  }
  if (before == null) {
    return RevertPlan(
      ops: const [],
      circuit: IngestRecord(
        table: row.tableName,
        op: IngestOp.update,
        id: row.recordId,
        record: const {},
      ),
      revert: RevertKind.partial,
    );
  }
  return RevertPlan(
    ops: [
      PutOp(row.tableName, row.recordId, {...before, 'id': row.recordId}),
    ],
    circuit: IngestRecord(
      table: row.tableName,
      op: row.mutationType == MutationEventType.delete
          ? IngestOp.create
          : IngestOp.update,
      id: row.recordId,
      record: {...before, 'id': row.recordId},
    ),
    revert: RevertKind.full,
  );
}

String failedRecordId(String mutationId) =>
    '$failedTable:${mutationId.substring(mutationId.indexOf(':') + 1)}';

FailedMutationRow buildFailedRow(
  PendingMutationRow row,
  FailedMutationError error,
  Map<String, dynamic>? before,
  int attempts,
  int now,
  RevertKind revert,
) =>
    FailedMutationRow(
      id: row.id,
      mutationType: row.mutationType,
      recordId: row.recordId,
      tableName: row.tableName,
      data: row.data,
      beforeRecord: before,
      error: error,
      attempts: attempts,
      createdAt: row.createdAt,
      failedAt: now,
      revert: revert,
    );

/// ONE transaction: write the tray row, delete the pending row.
List<LocalOp> moveToFailedTx(FailedMutationRow failed) => [
      PutOp(failedTable, failedRecordId(failed.id), failed.toJson()),
      DeleteOp(pendingTable, failed.id),
    ];

LocalOp deletePendingRow(String mutationId) =>
    DeleteOp(pendingTable, mutationId);

LocalOp deleteFailedRow(String mutationId) =>
    DeleteOp(failedTable, failedRecordId(mutationId));
