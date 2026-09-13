import '../state/lifecycle.dart' show QueryPhase;
import '../state/selectors.dart' show Overlay;
import '../types.dart';

class RenderInput {
  const RenderInput({
    required this.phase,
    required this.remoteArray,
    required this.localArray,
  });

  final QueryPhase phase;
  final RecordVersionArray remoteArray;
  final RecordVersionArray localArray;
}

/// The id-set a query renders from.
///
/// Divergence from `packages/core/src/query/render-set.ts`: there, a cold
/// non-window query falls back to a local predicate scan (`null`), re-running
/// its own SurrealQL against the local SurrealDB. sqlite cannot run SurrealQL,
/// so a cold Dart query renders the SSP's local window instead - which is the
/// same set once the circuit has been primed from the local store. A query that
/// renders from the local window is still `cold`, so `isAuthoritative` is false
/// and a binding shows its loader until the server answers.
RecordVersionArray resolveMembership(RenderInput input) =>
    input.phase == QueryPhase.cold ? input.localArray : input.remoteArray;

class RenderOptions {
  const RenderOptions({
    required this.hasExplicitOrder,
    required this.isWindow,
  });

  final bool hasExplicitOrder;
  final bool isWindow;
}

/// ```text
/// render = (membership ∪ (writes ∩ localView)) − deletes
/// ```
///
/// `writes`/`deletes` are the outbox overlay (pending and acked items). The
/// middle term keeps optimistic writes visible without re-admitting stale rows:
/// `localView` (the SSP's local id-set) says whether the written row matches the
/// query per local truth. Sorted unless the query orders itself or is a window,
/// whose id order is the slice order.
List<String> buildRenderIds(
  RecordVersionArray membership,
  RecordVersionArray localView,
  Overlay overlay,
  RenderOptions opts,
) {
  final ordered = <String>[];
  final seen = <String>{};
  for (final (id, _) in membership) {
    if (overlay.deletes.contains(id) || seen.contains(id)) continue;
    seen.add(id);
    ordered.add(id);
  }
  if (overlay.writes.isNotEmpty) {
    for (final (id, _) in localView) {
      if (!overlay.writes.contains(id) ||
          overlay.deletes.contains(id) ||
          seen.contains(id)) {
        continue;
      }
      seen.add(id);
      ordered.add(id);
    }
  }
  if (!opts.hasExplicitOrder && !opts.isWindow) ordered.sort();
  return ordered;
}
