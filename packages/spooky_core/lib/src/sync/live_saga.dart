import '../kernel/effects.dart';
import '../kernel/events.dart';
import '../kernel/saga.dart';
import '../query/env.dart';
import '../query/fetch_saga.dart' show landChunk;
import '../query/membership_saga.dart' show markMembershipDirty;
import '../state/reducers.dart' as r;
import '../types.dart';

/// LIVE on the session's `_00_list_ref` table. An event marks its query's
/// membership dirty and the batched re-read decides.
///
/// When the subscription was opened with the body joined on, the event ALSO
/// carries the changed row. That is an optimisation on top, never a source of
/// truth: the row is landed exactly as a fetched body would be, so a
/// notification the server drops costs nothing but the round trip it would
/// have saved.
Future<void> liveStart(Ctx ctx, SagaEnv env) async {
  final state = await ctx(Fx.stateRead((s) => s));
  final table = listRefTable(env, state);
  if (state.sync.liveUuid != null && state.sync.liveTable == table) return;
  if (state.sync.liveUuid != null &&
      state.sync.health.connection == ConnectionState.connected) {
    try {
      await ctx(Fx.remoteKill(state.sync.liveUuid!));
    } catch (error) {
      await ctx(Fx.log(LogLevel.debug,
          'kill of the previous live query failed', {'error': error}));
    }
  }
  await ctx(Fx.stateUpdate(
      r.patchSync(clearLiveUuid: true, clearLiveTable: true)));
  try {
    final uuid = await ctx(Fx.remoteLive(table));
    await ctx(Fx.stateUpdate(r.patchSync(liveUuid: uuid, liveTable: table)));
  } catch (error) {
    await ctx(Fx.log(
        LogLevel.warn,
        'live subscription failed; the poll covers membership',
        {'table': table, 'error': error}));
  }
}

/// The socket dropped: the server-side live query is gone with it.
Future<void> liveInvalidate(Ctx ctx) async {
  await ctx(Fx.stateUpdate(
      r.patchSync(clearLiveUuid: true, clearLiveTable: true)));
}

/// One or more edges of these queries changed on the server.
Future<void> liveChange(
    Ctx ctx, SagaEnv env, List<QueryHash> hashes, List<InlineRow>? rows) async {
  final known = await ctx(Fx.stateRead(
      (s) => [for (final h in hashes) if (s.queries.containsKey(h)) h]));
  if (known.isEmpty) return;
  final streak = await ctx(Fx.stateRead((s) => s.sync.pollIdleStreak));
  // Zeroing the streak only decides the delay the NEXT tick picks: `pollTick`
  // arms its timer at the end of a tick, so a client that had coasted up to the
  // 5s cap still waited out that armed timer. When the poll is the thing that
  // catches a change (a notification the server dropped, a row LIVE never
  // carried) that made the worst case 5s even though LIVE had just proved the
  // client was active. Re-arm at the base cadence too, so the safety net is
  // back under us immediately.
  //
  // Only when the poll had actually backed off. A `set` on the same key
  // replaces the pending timer, so re-arming on every event while LIVE is
  // delivering faster than `pollBaseMs` would push the poll out forever and
  // starve the full-membership reconciliation it exists to provide. A streak of
  // 0 means the cadence is already base and there is nothing to correct.
  if (streak > 0) {
    await ctx(Fx.stateUpdate(r.patchSync(pollIdleStreak: 0)));
    await ctx(Fx.timerSet('poll', env.pollBaseMs, const PollTick()));
  }
  // Before marking membership dirty: recording the version here is what makes
  // the re-read's `planFetch` find nothing left to pull for this row.
  if (rows != null && rows.isNotEmpty) await _landInlineRows(ctx, env, rows);
  await markMembershipDirty(ctx, known);
}

/// Land bodies that rode in on the notification. Rows already held at this
/// version or newer are dropped: one edit bumps the edge in every view holding
/// the row, so the same body arrives once per view.
Future<void> _landInlineRows(
    Ctx ctx, SagaEnv env, List<InlineRow> rows) async {
  final state = await ctx(Fx.stateRead((s) => s));
  final fresh = <String, InlineRow>{};
  for (final row in rows) {
    if ((state.versions[row.id] ?? -1) >= row.version) continue;
    final held = fresh[row.id];
    if (held == null || held.version < row.version) fresh[row.id] = row;
  }
  if (fresh.isEmpty) return;
  final picked = fresh.values.toList();
  final epoch = await ctx(Fx.localEpoch());
  final landed = await landChunk(
    ctx,
    env,
    [for (final r in picked) r.id],
    [for (final r in picked) r.record],
    {for (final r in picked) r.id: r.version},
    state,
    epoch,
  );
  if (!landed) {
    await ctx(Fx.log(
        LogLevel.debug,
        'inline live body not landed; the fetch path will pull it',
        {'ids': [for (final r in picked) r.id]}));
  }
}
