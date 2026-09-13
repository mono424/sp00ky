import '../kernel/constants.dart';
import '../kernel/effects.dart';
import '../kernel/events.dart';
import '../kernel/saga.dart';
import '../services/stream_processor/stream_processor_service.dart'
    show IngestOp, IngestRecord;
import '../state/client_state.dart';
import '../state/lifecycle.dart';
import '../state/reducers.dart' as r;
import '../state/selectors.dart';
import '../utils/parser.dart';
import '../utils/record_id_utils.dart';
import 'env.dart';
import 'sql.dart' as sql;

/// The unified row fetcher. Runs on the serial `fetch` lane: computes the
/// cross-query set of bodies missing or stale locally, pulls them in parallel
/// chunks, writes and ingests them, and loops until the plan is empty. A row
/// shared by N queries is fetched once. Bodies are never deleted here; ids that
/// left membership simply stop rendering.
Future<void> fetchRows(Ctx ctx, SagaEnv env) async {
  await ctx(Fx.stateWait((s) => s.primed));
  for (;;) {
    final state = await ctx(Fx.stateRead((s) => s));
    final plan = planFetch(state);
    if (plan.chunks.isEmpty) {
      if (state.sync.fetchAttempts != 0) {
        await ctx(Fx.stateUpdate(r.patchSync(fetchAttempts: 0)));
      }
      return;
    }
    // The store's epoch, not a mirrored counter: it also moves on a bucket
    // switch, and a stale value makes the store drop every body write.
    final epoch = await ctx(Fx.localEpoch());
    await ctx(Fx.stateUpdate(r.compose([
      for (final h in plan.hashes) r.applyLifecycle(h, const FetchBeginEvent())
    ])));
    var failed = 0;
    try {
      final results = await ctx(Fx.all([
        for (final ids in plan.chunks)
          Fx.remoteQuery(
            sql.bodySelect(),
            vars: {'ids': [for (final id in ids) parseRecordIdString(id)]},
            timeoutMs: env.remoteTimeoutMs,
          )
      ]));
      for (var i = 0; i < results.length; i++) {
        final res = results[i];
        final statements = res.ok ? res.value as List<StatementResult> : null;
        final first = statements == null || statements.isEmpty
            ? null
            : statements.first;
        if (first == null || !first.isOk || first.result is! List) {
          failed++;
          continue;
        }
        final rows = [
          for (final row in first.result as List)
            if (row is Map) Map<String, dynamic>.from(row)
        ];
        final landed = await landChunk(
            ctx, env, plan.chunks[i], rows, plan.versions, state, epoch);
        if (!landed) failed++;
      }
    } finally {
      await ctx(Fx.stateUpdate(r.compose([
        for (final h in plan.hashes) r.applyLifecycle(h, const FetchEndEvent())
      ])));
    }
    await ctx(Fx.dispatch(
        SyncOutcome(failed == 0, failed > 0 ? 'body fetch failed' : null)));
    if (failed > 0) {
      final attempt = await ctx(Fx.stateRead((s) => s.sync.fetchAttempts));
      await ctx(Fx.stateUpdate(r.patchSync(fetchAttempts: attempt + 1)));
      await ctx(Fx.timerSet('fetch', backoffMs(attempt), const FetchRows()));
      return;
    }
  }
}

/// Write one chunk's bodies to the store and the circuit; record their
/// versions.
///
/// Shared with the LIVE path: a body that arrives on a notification is landed
/// through here so it is indistinguishable from a fetched one, which is what
/// makes `planFetch` stop asking for it.
Future<bool> landChunk(
  Ctx ctx,
  SagaEnv env,
  List<String> requested,
  List<Map<String, dynamic>> rows,
  Map<String, int> versions,
  ClientState state,
  int epoch,
) async {
  final ops = <LocalOp>[];
  final ingest = <IngestRecord>[];
  for (final row in rows) {
    final rawId = row['id'];
    if (rawId == null) continue;
    final id = rawId.toString();
    final table = extractTablePart(id);
    final version = versions[id] ?? 0;
    final columns = columnsFor(env, table);
    final cleaned = columns == null || columns.isEmpty
        ? row
        : cleanRecord(columns, row);
    final body = {...cleaned, 'id': id, '_00_rv': version};
    ops.add(PutOp(table, id, body, mode: WriteMode.merge));
    ingest.add(IngestRecord(
      table: table,
      op: state.versions.containsKey(id) ? IngestOp.update : IngestOp.create,
      id: id,
      record: body,
    ));
  }
  if (ops.isNotEmpty) {
    try {
      await ctx(Fx.localTx(ops, epoch: epoch));
    } catch (e) {
      await ctx(Fx.log(LogLevel.warn, 'body write failed', {'error': e}));
      return false;
    }
    try {
      await ctx(Fx.sspIngest(ingest));
    } catch (e) {
      await ctx(Fx.log(LogLevel.warn, 'circuit ingest failed', {'error': e}));
    }
  }
  // Ids the server did not return (deleted or unreadable upstream) count as
  // known at the requested version, so the plan stops asking for them; their
  // absence from the store is what the render shows.
  await ctx(Fx.stateUpdate(r.setVersions([
    for (final id in requested) (id, versions[id] ?? 0)
  ])));
  return true;
}
