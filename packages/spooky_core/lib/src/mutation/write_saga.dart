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

// ---- channel peers' creates and deletes -------------------------------------

/// The row as the local store holds it, or null: what a channel checks its
/// filter against.
Future<Map<String, dynamic>?> readRow(Ctx ctx, String recordId) =>
    ctx(Fx.localGet(extractTablePart(recordId), recordId));

class PeerWriteInput {
  const PeerWriteInput({required this.recordId, this.data, required this.ttlMs});

  final String recordId;

  /// The row's fields, for a create.
  final Map<String, dynamic>? data;

  /// How long it shows without the server confirming it.
  final int ttlMs;
}

/// A channel peer created a row: put it in the store and the circuit like an
/// own optimistic create, minus the outbox entry (the peer pushes it), and show
/// it through the overlay. `_00_rv` and [ClientState.versions] stay unset on
/// purpose: the server's body (with its computed fields) still downloads and
/// replaces this one. Refuses an id this client already holds a confirmed row
/// or an own pending write for, so a peer can never overwrite real data.
/// Returns whether it applied.
Future<bool> peerCreate(Ctx ctx, SagaEnv env, PeerWriteInput input) async {
  final table = extractTablePart(input.recordId);
  final columns = columnsFor(env, table);
  if (columns == null) return false;
  final (confirmed, ownPending, peerPending) = await ctx(Fx.stateRead((s) => (
        s.versions.containsKey(input.recordId),
        s.outbox.any((i) => i.recordId == input.recordId),
        s.peerWrites.containsKey(input.recordId),
      )));
  if (confirmed || ownPending || peerPending) return false;
  var record = await ctx(Fx.localGet(table, input.recordId));
  if (record == null) {
    final params = parseQueryParams(columns, input.data ?? const {});
    try {
      await ctx(
          Fx.localTx(planPeerCreateTx(table, input.recordId, params)));
      record = await ctx(Fx.localGet(table, input.recordId));
    } catch (error) {
      await ctx(Fx.log(LogLevel.debug, 'peer create refused by the local store',
          {'recordId': input.recordId, 'error': error}));
      return false;
    }
  }
  if (record == null) return false;
  try {
    await ctx(Fx.sspIngest([
      IngestRecord(
        table: table,
        op: IngestOp.create,
        id: input.recordId,
        record: {...record, '_00_rv': 0},
      )
    ]));
  } catch (error) {
    await ctx(Fx.log(LogLevel.error, 'circuit ingest failed after a peer create',
        {'recordId': input.recordId, 'error': error}));
  }
  final now = await ctx(Fx.now());
  await ctx(Fx.stateUpdate(r.setPeerWrite(
      input.recordId,
      PeerWrite(
          kind: PeerWriteKind.create,
          table: table,
          expiresAt: now + input.ttlMs))));
  await ctx(Fx.timerSet('peer:${input.recordId}', input.ttlMs,
      PeerWriteExpired(input.recordId)));
  return true;
}

/// A channel peer deleted a row: hide it through the overlay, keep it in the
/// store. Undoing is then just forgetting the entry; a confirmed delete has
/// taken the row out of the server's membership by the time it expires.
Future<bool> peerDelete(Ctx ctx, SagaEnv env, PeerWriteInput input) async {
  final table = extractTablePart(input.recordId);
  if (columnsFor(env, table) == null) return false;
  final (known, confirmed) = await ctx(Fx.stateRead((s) => (
        s.peerWrites[input.recordId],
        s.versions.containsKey(input.recordId),
      )));
  if (known?.kind == PeerWriteKind.create && !confirmed) {
    // Deleting a row the same peer created and nobody confirmed: just undo it.
    await ctx(Fx.timerClear('peer:${input.recordId}'));
    await _undoPeerCreate(ctx, table, input.recordId);
    return true;
  }
  final exists = await ctx(Fx.localGet(table, input.recordId)) != null;
  if (!exists) return false;
  final now = await ctx(Fx.now());
  await ctx(Fx.stateUpdate(r.setPeerWrite(
      input.recordId,
      PeerWrite(
          kind: PeerWriteKind.delete,
          table: table,
          expiresAt: now + input.ttlMs))));
  await ctx(Fx.timerSet('peer:${input.recordId}', input.ttlMs,
      PeerWriteExpired(input.recordId)));
  return true;
}

/// Timer target: a peer's write stops showing as unconfirmed.
Future<void> peerWriteExpired(Ctx ctx, String id) async {
  final (write, confirmed, ownPending) = await ctx(Fx.stateRead((s) => (
        s.peerWrites[id],
        s.versions.containsKey(id),
        s.outbox.any((i) => i.recordId == id),
      )));
  if (write == null) return;
  if (write.kind == PeerWriteKind.create && !confirmed && !ownPending) {
    await _undoPeerCreate(ctx, write.table, id);
    return;
  }
  await ctx(Fx.stateUpdate(r.clearPeerWrite(id)));
}

Future<void> _undoPeerCreate(Ctx ctx, String table, String id) async {
  final before = await ctx(Fx.localGet(table, id));
  try {
    await ctx(Fx.localTx(planPeerUndoTx(table, id)));
    if (before != null) {
      await ctx(Fx.sspIngest([
        IngestRecord(table: table, op: IngestOp.delete, id: id, record: before)
      ]));
    }
  } catch (error) {
    await ctx(Fx.log(LogLevel.warn, 'could not undo an unconfirmed peer create',
        {'recordId': id, 'error': error}));
  }
  await ctx(Fx.stateUpdate(r.clearPeerWrite(id)));
}
