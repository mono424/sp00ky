import '../kernel/effects.dart';
import '../kernel/events.dart';
import '../kernel/saga.dart';
import '../query/env.dart';
import '../services/stream_processor/stream_processor_service.dart'
    show IngestOp, IngestRecord;
import '../state/client_state.dart';
import '../state/reducers.dart' as r;
import '../surreal/value.dart';
import '../types.dart';
import '../utils/parser.dart';
import '../utils/record_id_utils.dart';
import 'mutation_id.dart' show pendingTable;
import 'rows.dart';

class WriteInput {
  const WriteInput({
    required this.kind,
    required this.recordId,
    this.data,
    this.options,
  });

  final MutationEventType kind;
  final String recordId;
  final Map<String, dynamic>? data;
  final UpdateOptions? options;
}

class WriteResult {
  const WriteResult({required this.mutationId, required this.record});
  final String mutationId;
  final Map<String, dynamic>? record;
}

const int _defaultDebounceMs = 300;

String? _debounceKey(WriteInput input) {
  final d = input.options?.debounced;
  if (d == null || d == false) return null;
  final opts = d is DebounceOptions ? d : const DebounceOptions();
  final mode = opts.key ?? DebounceKey.recordIdXFields;
  if (mode == DebounceKey.recordId) return input.recordId;
  final fields = (input.data?.keys.toList() ?? <String>[])..sort();
  return '${input.recordId}::${fields.join(',')}';
}

int _debounceDelay(WriteInput input) {
  final d = input.options?.debounced;
  return d is DebounceOptions ? (d.delay ?? _defaultDebounceMs) : _defaultDebounceMs;
}

/// One optimistic write: read the previous row (update/delete), run the local
/// transaction that writes the row AND its outbox entry, publish the outbox
/// item (which puts the id into the overlay and dirties the table's queries),
/// feed the circuit, and start a drain. A debounced update applies locally at
/// once but accumulates into one outbox row written on flush.
Future<WriteResult> write(Ctx ctx, SagaEnv env, WriteInput input) async {
  final table = extractTablePart(input.recordId);
  final columns = columnsFor(env, table);
  if (columns == null) throw ArgumentError('Table $table not found');
  final params =
      input.data == null ? null : parseQueryParams(columns, input.data!);
  final key =
      input.kind == MutationEventType.update ? _debounceKey(input) : null;
  if (key != null) {
    return _debouncedUpdate(ctx, input, key, table, params ?? const {});
  }
  final mutationId = await ctx(Fx.id(IdScope.mutation));
  final now = await ctx(Fx.now());
  Map<String, dynamic>? before;
  if (input.kind != MutationEventType.create) {
    before = await ctx(Fx.localGet(table, input.recordId));
  }
  final plan = WritePlanInput(
    recordId: input.recordId,
    mutationId: mutationId,
    table: table,
    data: params,
    before: before,
    now: now,
  );
  final ops = switch (input.kind) {
    MutationEventType.create => planCreateTx(plan),
    MutationEventType.update => planUpdateTx(plan),
    MutationEventType.delete => planDeleteTx(plan),
  };
  await ctx(Fx.localTx(ops));

  // The local store has no RETURN clause, so the written row is read back
  // rather than carried out of the transaction.
  final record = input.kind == MutationEventType.delete
      ? null
      : await ctx(Fx.localGet(table, input.recordId));

  await ctx(Fx.stateUpdate(r.outboxPush(OutboxItem(
    id: mutationId,
    type: input.kind,
    recordId: input.recordId,
    table: table,
    status: OutboxStatus.pending,
    ackedAt: null,
    attempts: 0,
  ))));

  final version = switch (input.kind) {
    MutationEventType.create => 1,
    _ => (record?['_00_rv'] as num?)?.toInt() ??
        (((before?['_00_rv'] as num?) ?? 0).toInt() + 1),
  };
  final circuitRecord = input.kind == MutationEventType.delete
      ? (before ?? const <String, dynamic>{})
      : {...?record, '_00_rv': version};
  final circuit = IngestRecord(
    table: table,
    op: switch (input.kind) {
      MutationEventType.create => IngestOp.create,
      MutationEventType.update => IngestOp.update,
      MutationEventType.delete => IngestOp.delete,
    },
    id: input.recordId,
    record: circuitRecord,
  );
  try {
    await ctx(Fx.sspIngest([circuit]));
    if (input.kind == MutationEventType.delete) {
      await ctx(Fx.stateUpdate(r.deleteVersions([input.recordId])));
    } else {
      await ctx(Fx.stateUpdate(r.setVersions([(input.recordId, version)])));
    }
  } catch (error) {
    await ctx(Fx.log(LogLevel.error, 'circuit ingest failed after a local write',
        {'recordId': input.recordId, 'error': error}));
  }
  await ctx(Fx.emit(MutationEmittedEvent(MutationEvent(
    type: input.kind,
    mutationId: RecordId.parse(mutationId),
    recordId: parseRecordIdString(input.recordId),
    data: params,
    record: record,
    createdAt: DateTime.fromMillisecondsSinceEpoch(now),
  ))));
  await ctx(Fx.dispatch(const Drain()));
  return WriteResult(mutationId: mutationId, record: record);
}

/// Apply locally now, remember the merged patch, write the outbox row on flush.
Future<WriteResult> _debouncedUpdate(
  Ctx ctx,
  WriteInput input,
  String key,
  String table,
  Map<String, dynamic> params,
) async {
  final existing = await ctx(Fx.stateRead((s) => s.pendingWrites[key]));
  final now = await ctx(Fx.now());
  final before = existing != null
      ? existing.before
      : await ctx(Fx.localGet(table, input.recordId));
  await ctx(Fx.localTx(planLocalOnlyUpdateTx(table, input.recordId, params)));
  final record = await ctx(Fx.localGet(table, input.recordId));
  await ctx(Fx.stateUpdate(r.mergePendingWrite(PendingWrite(
    key: key,
    table: table,
    recordId: input.recordId,
    data: params,
    before: before,
    firstAt: existing?.firstAt ?? now,
  ))));
  await ctx(Fx.stateUpdate(r.markTableDirty(table)));
  final version = (record?['_00_rv'] as num?)?.toInt() ?? 0;
  final circuit = IngestRecord(
    table: table,
    op: IngestOp.update,
    id: input.recordId,
    record: {...?record, '_00_rv': version},
  );
  try {
    await ctx(Fx.sspIngest([circuit]));
    await ctx(Fx.stateUpdate(r.setVersions([(input.recordId, version)])));
  } catch (error) {
    await ctx(Fx.log(LogLevel.error, 'circuit ingest failed after a local write',
        {'recordId': input.recordId, 'error': error}));
  }
  await ctx(Fx.timerSet(
      'debounce:$key', _debounceDelay(input), FlushWrite(key)));
  return WriteResult(mutationId: '', record: record);
}

/// Timer target: turn the accumulated patch into one outbox row and drain.
Future<void> flushWrite(Ctx ctx, SagaEnv env, String key) async {
  final pending = await ctx(Fx.stateRead((s) => s.pendingWrites[key]));
  if (pending == null) return;
  await ctx(Fx.stateUpdate(r.clearPendingWrite(key)));
  final mutationId = await ctx(Fx.id(IdScope.mutation));
  final now = await ctx(Fx.now());
  await ctx(Fx.localTx(planDeferredOutboxRowTx(WritePlanInput(
    recordId: pending.recordId,
    mutationId: mutationId,
    table: pending.table,
    data: {...pending.data},
    before: pending.before,
    now: now,
  ))));
  await ctx(Fx.stateUpdate(r.outboxPush(OutboxItem(
    id: mutationId,
    type: MutationEventType.update,
    recordId: pending.recordId,
    table: pending.table,
    status: OutboxStatus.pending,
    ackedAt: null,
    attempts: 0,
  ))));
  await ctx(Fx.emit(MutationEmittedEvent(MutationEvent(
    type: MutationEventType.update,
    mutationId: RecordId.parse(mutationId),
    recordId: parseRecordIdString(pending.recordId),
    data: {...pending.data},
    createdAt: DateTime.fromMillisecondsSinceEpoch(now),
  ))));
  await ctx(Fx.dispatch(const Drain()));
}

/// The local table an outbox row lives in. Exposed so the drain can read rows
/// back through the same effect family the write used.
const String outboxTable = pendingTable;
