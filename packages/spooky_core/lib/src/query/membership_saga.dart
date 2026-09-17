import '../kernel/constants.dart';
import '../kernel/effects.dart';
import '../kernel/events.dart';
import '../kernel/saga.dart';
import '../state/client_state.dart';
import '../state/lifecycle.dart';
import '../state/reducers.dart' as r;
import '../sync/policy.dart' show recordVersionArraysEqual, planListRefPollChunks, ListRefPollCandidate;
import '../types.dart';
import 'env.dart';
import 'membership.dart';
import 'sql.dart' as sql;

/// Accept (or refuse) a server id-set for one query. The only writer of
/// `remoteArray`, the `_00_view` row and the cold -> live transition.
Future<MembershipOutcome> applyMembership(
  Ctx ctx,
  QueryHash hash,
  RecordVersionArray remoteArray, {
  ServerViewMeta? meta,
  bool verifiedRemoval = false,
}) async {
  final entry = await ctx(Fx.stateRead((s) => s.queries[hash]));
  if (entry == null) return MembershipOutcome.ignored;
  if (meta != null) {
    await ctx(Fx.stateUpdate(r.setServerState(
        hash,
        meta.present
            ? switch (meta.state) {
                'materializing' => ServerViewState.materializing,
                'ready' => ServerViewState.ready,
                _ => null,
              }
            : null)));
  }
  // The row is there: whatever made it read as gone has passed.
  if (meta != null &&
      meta.present &&
      (entry.viewLostCount > 0 || entry.viewLostRetryAt != null)) {
    await ctx(Fx.stateUpdate(r.setViewLost(hash, 0, null)));
  }
  final outcome = decideMembershipOutcome(MembershipDecisionInput(
    phase: entry.lifecycle.phase,
    held: entry.remoteArray.length,
    remoteArray: remoteArray,
    meta: meta,
    verifiedRemoval: verifiedRemoval,
  ));
  if (outcome == MembershipOutcome.ignored) return outcome;
  if (outcome == MembershipOutcome.viewLost) {
    if (entry.lifecycle.phase != QueryPhase.viewLost) {
      await ctx(Fx.stateUpdate(r.applyLifecycle(hash, const RowMissingEvent())));
      await ctx(Fx.emit(QueryViewLostEvent(hash)));
    }
    // A recovery is already scheduled: every read in the meantime would
    // otherwise re-register again.
    if (entry.viewLostRetryAt != null) return outcome;
    if (entry.viewLostCount == 0) {
      await ctx(Fx.stateUpdate(r.setViewLost(hash, 1, null)));
      if (entry.lifecycle.remote != RemotePhase.unregistered) {
        await ctx(Fx.stateUpdate(
            r.applyLifecycle(hash, const RemoteDroppedEvent())));
      }
      await ctx(Fx.dispatch(const EnsureRegistered()));
      return outcome;
    }
    // Re-registered already and the row still reads as gone. Under load that
    // repeated every read, each one costing the server a registration while it
    // was still publishing the last, so from here on it is paced.
    final delay = backoffMs(entry.viewLostCount - 1,
        base: viewLostRetryBaseMs, max: viewLostRetryMaxMs);
    final now = await ctx(Fx.now());
    await ctx(Fx.stateUpdate(
        r.setViewLost(hash, entry.viewLostCount + 1, now + delay)));
    await ctx(Fx.log(LogLevel.debug,
        'view still lost; re-registering after a delay', {
      'hash': hash,
      'attempt': entry.viewLostCount + 1,
      'delayMs': delay,
    }));
    await ctx(Fx.timerSet('view-lost:$hash', delay, RecoverLostView(hash)));
    return outcome;
  }
  final wasAuthoritative = isAuthoritative(entry.lifecycle);
  await ctx(Fx.stateUpdate(
      r.commitMembership(hash, remoteArray, meta?.present ?? true)));
  if (!wasAuthoritative) {
    await ctx(Fx.emit(QueryAuthorityEvent(hash, true)));
  }
  final now = await ctx(Fx.now());
  try {
    await ctx(Fx.localPut(sql.viewTable, sql.viewRecordId(entry.def.viewKey),
        sql.viewRow(remoteArray, true, now)));
  } catch (e) {
    await ctx(Fx.log(LogLevel.debug, 'view row write failed',
        {'hash': hash, 'error': e}));
  }
  await ctx(Fx.dispatch(const FetchRows()));
  return outcome;
}

/// The paced view-lost re-registration. A no-op when the row has been seen
/// since (the pacing was reset) or the query recovered or went away.
Future<void> recoverLostView(Ctx ctx, QueryHash hash) async {
  final entry = await ctx(Fx.stateRead((s) => s.queries[hash]));
  if (entry == null || entry.viewLostRetryAt == null) return;
  await ctx(Fx.stateUpdate(r.setViewLost(hash, entry.viewLostCount, null)));
  if (entry.lifecycle.phase != QueryPhase.viewLost) return;
  if (entry.lifecycle.remote != RemotePhase.unregistered) {
    await ctx(
        Fx.stateUpdate(r.applyLifecycle(hash, const RemoteDroppedEvent())));
  }
  await ctx(Fx.dispatch(RegisterRemote(hash)));
}

/// Replace the subquery child set; bodies follow through the fetch plan.
Future<void> applySubqueryChildren(
    Ctx ctx, QueryHash hash, RecordVersionArray children) async {
  final entry = await ctx(Fx.stateRead((s) => s.queries[hash]));
  if (entry == null ||
      recordVersionArraysEqual(entry.subqueryRemoteArray, children)) {
    return;
  }
  await ctx(Fx.stateUpdate(r.setSubqueryRemoteArray(hash, children)));
  await ctx(Fx.dispatch(const FetchRows()));
}

/// The result of one membership read.
typedef MembershipRead = ({bool changed, bool failed});

/// One statement's result, or [statementFailed] when it did not answer.
Object? _stmt(Object? results, int i) {
  if (results is! List<StatementResult>) return statementFailed;
  final r = sql.stmt(results, i);
  return r != null && r.isOk ? r.result : statementFailed;
}

/// Read the server membership of many queries in as few round trips as the row
/// budget allows, then apply each changed set. This is the ONLY place a server
/// id-set enters state: registration, the poll, LIVE dirt and view-lost
/// recovery all come through here.
Future<MembershipRead> readMembership(
  Ctx ctx,
  SagaEnv env,
  List<QueryHash> hashes, {
  bool force = false,
}) async {
  final state = await ctx(Fx.stateRead((s) => s));
  final entries = [
    for (final h in hashes)
      if (state.queries[h] != null) state.queries[h]!
  ];
  if (entries.isEmpty) return (changed: false, failed: false);
  final table = listRefTable(env, state);
  final now = await ctx(Fx.now());
  final chunks = planListRefPollChunks(
    [
      for (final e in entries)
        ListRefPollCandidate(
          hash: e.def.hash,
          rows: e.remoteArray.length,
          lastPolledAt: force ? 0 : (e.lastPolledAt ?? 0),
        )
    ],
    now: now,
  );
  final byHash = {for (final e in entries) e.def.hash: e};

  Effect<List<StatementResult>> single(QueryEntry e) => Fx.remoteQuery(
        sql.singleSnapshotSelect(table),
        vars: {'in': e.def.id},
        timeoutMs: env.remoteTimeoutMs,
      );

  final requests = [
    for (final chunk in chunks)
      if (chunk.length == 1)
        single(byHash[chunk.first]!)
      else
        Fx.remoteQuery(
          sql.batchSnapshotSelect(table),
          vars: {'ins': [for (final h in chunk) byHash[h]!.def.id]},
          timeoutMs: env.remoteTimeoutMs,
        )
  ];
  final results = await ctx(Fx.all(requests));

  final snapshots = <QueryHash, ListRefSnapshot>{};
  var failed = false;
  final rereads = <QueryEntry>[];
  for (var i = 0; i < chunks.length; i++) {
    final chunk = chunks[i];
    final res = results[i];
    if (!res.ok) {
      failed = true;
      continue;
    }
    if (chunk.length == 1) {
      final snap = snapshotFromSingle(_stmt(res.value, 0), _stmt(res.value, 1),
          _stmt(res.value, 2));
      if (snap != null) {
        snapshots[chunk.first] = snap;
      } else {
        failed = true;
      }
      continue;
    }
    final hashById = {
      for (final h in chunk) byHash[h]!.def.id.encode(): h,
    };
    final edges = _stmt(res.value, 0);
    final counts = _stmt(res.value, 1);
    if (edges is! List || identical(counts, statementFailed)) {
      failed = true;
      continue;
    }
    final batch = snapshotsFromBatch(edges, counts, hashById);
    final held = {for (final h in chunk) h: byHash[h]!.remoteArray.length};
    final suspect = suspectHashes(
        batch, held, edges.length).toSet();
    for (final e in batch.entries) {
      if (suspect.contains(e.key)) {
        rereads.add(byHash[e.key]!);
      } else {
        snapshots[e.key] = e.value;
      }
    }
  }
  if (rereads.isNotEmpty) {
    final again = await ctx(Fx.all([for (final e in rereads) single(e)]));
    for (var i = 0; i < again.length; i++) {
      final res = again[i];
      if (!res.ok) {
        failed = true;
        continue;
      }
      final snap = snapshotFromSingle(_stmt(res.value, 0), _stmt(res.value, 1),
          _stmt(res.value, 2));
      if (snap != null) {
        snapshots[rereads[i].def.hash] = snap;
      } else {
        failed = true;
      }
    }
  }

  var changed = false;
  final stillMaterializing = <QueryHash>[];
  for (final e in snapshots.entries) {
    final hash = e.key;
    final snap = e.value;
    final current = await ctx(Fx.stateRead((s) => s.queries[hash]));
    if (current == null) continue;
    final serverState = snap.meta.present ? snap.meta.state : null;
    final metaChanged = current.serverState?.name != serverState;
    if (!recordVersionArraysEqual(snap.primary, current.remoteArray) ||
        metaChanged ||
        current.lifecycle.phase == QueryPhase.cold ||
        current.lifecycle.phase == QueryPhase.cached) {
      final outcome =
          await applyMembership(ctx, hash, snap.primary, meta: snap.meta);
      if (outcome == MembershipOutcome.applied) changed = true;
      if (outcome == MembershipOutcome.ignored &&
          snap.meta.present &&
          snap.meta.state == 'materializing') {
        stillMaterializing.add(hash);
      }
    }
    await applySubqueryChildren(ctx, hash, snap.subquery);
  }
  await ctx(Fx.stateUpdate(r.compose([
    r.clearMembershipDirty(snapshots.keys),
    r.stampPolled(snapshots.keys, now),
  ])));

  // A view whose edges are still in flight: re-read on a short ladder rather
  // than waiting for a backed-off poll tick.
  for (final hash in stillMaterializing) {
    final attempt =
        await ctx(Fx.stateRead((s) => s.membershipReread[hash] ?? 0));
    if (attempt >= materializingRereadLadderMs.length) {
      await ctx(Fx.stateUpdate(r.setMembershipReread(hash, null)));
      continue;
    }
    await ctx(Fx.stateUpdate(r.compose([
      r.setMembershipReread(hash, attempt + 1),
      r.markMembershipDirty([hash]),
    ])));
    await ctx(Fx.timerSet('membership', materializingRereadLadderMs[attempt],
        const ReadDirtyMembership()));
  }
  for (final hash in snapshots.keys) {
    if (!stillMaterializing.contains(hash)) {
      await ctx(Fx.stateUpdate(r.setMembershipReread(hash, null)));
    }
  }
  await ctx(Fx.dispatch(
      SyncOutcome(!failed, failed ? 'membership read failed' : null)));
  return (changed: changed, failed: failed);
}

/// LIVE handler: mark dirty and coalesce into one batched re-read.
///
/// The window is armed ONCE per burst, when the dirty set goes from empty to
/// non-empty. `timer.set` replaces a pending timer, so re-arming on every event
/// would push the read out for as long as events keep arriving: on a busy
/// `_00_list_ref` table (registrations, an import, another client syncing) the
/// events never stop for a whole 50 ms and the read simply never ran, leaving
/// membership frozen until a poll tick happened to cover it.
Future<void> markMembershipDirty(Ctx ctx, List<QueryHash> hashes) async {
  final armed = await ctx(Fx.stateRead((s) => s.membershipDirty.isNotEmpty));
  await ctx(Fx.stateUpdate(r.markMembershipDirty(hashes)));
  if (!armed) {
    await ctx(Fx.timerSet(
        'membership', membershipCoalesceMs, const ReadDirtyMembership()));
  }
}

Future<void> readDirtyMembership(Ctx ctx, SagaEnv env) async {
  final dirty = await ctx(Fx.stateRead((s) => s.membershipDirty.toList()));
  if (dirty.isEmpty) return;
  await readMembership(ctx, env, dirty, force: true);
  // Dirt that arrived while the read was in flight, or a hash the read could
  // not answer, holds the set non-empty - and with the window armed only on the
  // empty -> non-empty edge, nothing else would arm it again.
  final left = await ctx(Fx.stateRead((s) => s.membershipDirty.length));
  if (left > 0) {
    await ctx(Fx.timerSet(
        'membership', membershipCoalesceMs, const ReadDirtyMembership()));
  }
}
