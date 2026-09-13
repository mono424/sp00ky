import '../ffi/stream_update.dart' show StreamUpdate;
import '../kernel/effects.dart';
import '../kernel/events.dart';
import '../kernel/saga.dart';
import '../state/reducers.dart' as r;
import '../state/selectors.dart' show overlay;
import '../types.dart';
import 'materialize.dart';
import 'render_set.dart';

/// Render one query from state: membership (or the SSP's local window), the
/// outbox overlay, one local read, then notify subscribers if the rows changed.
/// The only writer of `records`. Runs on the `mat:<hash>` lane whenever the
/// query is dirty.
Future<void> materialize(Ctx ctx, QueryHash hash) async {
  final entry = await ctx(Fx.stateRead((s) => s.queries[hash]));
  if (entry == null) return;
  final ov = await ctx(Fx.stateRead(overlay));
  final t0 = await ctx(Fx.now());
  final isWindow = isWindowed(entry.def.surql);
  final membership = resolveMembership(RenderInput(
    phase: entry.lifecycle.phase,
    remoteArray: entry.remoteArray,
    localArray: entry.localArray,
  ));
  final ids = buildRenderIds(
    membership,
    entry.localArray,
    ov,
    RenderOptions(
      hasExplicitOrder: entry.def.hasExplicitOrder,
      isWindow: isWindow,
    ),
  );
  List<Map<String, dynamic>> rows;
  try {
    rows = await ctx(
        materializeEffect(tableOfIds(ids, entry.def.tableName), ids));
    if (isWindow) rows = applyWindowOrder(entry.def.surql, rows);
  } catch (e) {
    await ctx(Fx.stateUpdate(
        r.compose([r.recordError(hash), r.clearDirty(hash)])));
    await ctx(Fx.log(
        LogLevel.warn, 'materialize failed', {'hash': hash, 'error': e}));
    return;
  }
  final current = await ctx(Fx.stateRead((s) => s.queries[hash]));
  if (current == null) return;
  final t1 = await ctx(Fx.now());
  final changed = !rowsEqual(rows, current.records);
  await ctx(Fx.stateUpdate(r.compose([
    r.setRecords(hash, rows, changed, (t1 - t0).toDouble()),
    if (changed) r.stampUpdated(hash, t1) else r.noop,
  ])));
  if (changed) await ctx(Fx.emit(QueryRecordsEvent(hash, rows)));
}

/// A view update from the in-process SSP: the query's local id-set moved.
/// State takes the array (which dirties the query); the ingest timings feed
/// telemetry.
Future<void> streamUpdate(Ctx ctx, StreamUpdate update) async {
  final hash = update.queryHash;
  final exists = await ctx(Fx.stateRead((s) => s.queries.containsKey(hash)));
  if (!exists) return;
  final phases = <(String, double?)>[
    (TimingPhase.sspStoreApply, update.storeApplyMs),
    (TimingPhase.sspCircuitStep, update.circuitStepMs),
    (TimingPhase.sspTransform, update.transformMs),
  ];
  await ctx(Fx.stateUpdate(r.compose([
    r.setLocalArray(hash, update.localArray),
    if (update.materializationTimeMs != null)
      r.recordIngest(hash, update.materializationTimeMs!)
    else
      r.noop,
    for (final (phase, ms) in phases)
      if (ms != null) r.recordPhase(hash, phase, ms),
  ])));
}
