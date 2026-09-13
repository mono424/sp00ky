import '../services/stream_processor/stream_processor_service.dart'
    show IngestRecord;
import '../state/client_state.dart';
import '../types.dart';
import 'events.dart';

/// What the in-process SSP needs to build a local view.
class RegisterPlan {
  const RegisterPlan({
    required this.queryHash,
    required this.surql,
    required this.params,
    required this.ttl,
    required this.tableName,
  });

  final String queryHash;
  final String surql;
  final Map<String, dynamic> params;
  final QueryTimeToLive ttl;
  final String tableName;
}

class RegisterResult {
  const RegisterResult({required this.localArray, required this.timings});
  final RecordVersionArray localArray;
  final RegistrationTimings timings;
}

/// One statement's outcome of a multi-statement remote request.
class StatementResult {
  const StatementResult.ok(this.result)
      : status = 'OK',
        error = null;
  const StatementResult.err(this.error)
      : status = 'ERR',
        result = null;

  final String status;
  final Object? result;
  final String? error;

  bool get isOk => status == 'OK';
}

/// One entry of an [AllEffect] fan-out result (allSettled semantics).
class Settled<T> {
  const Settled.ok(this.value)
      : ok = true,
        error = null;
  const Settled.err(this.error)
      : ok = false,
        value = null;

  final bool ok;
  final T? value;
  final Object? error;
}

/// How a document write lands on an existing row.
enum WriteMode {
  /// Replace the stored document wholesale.
  replace,

  /// Deep-merge into the stored document, preserving keys the patch omits
  /// (`_00_crdt`, `_00_cursor`).
  merge,
}

/// One step of a [LocalTx]. A transaction is data, not a closure, so a saga can
/// describe a multi-write commit without reaching for the store.
sealed class LocalOp {
  const LocalOp();
}

class PutOp extends LocalOp {
  const PutOp(this.table, this.id, this.data, {this.mode = WriteMode.replace});
  final String table;
  final String id;
  final Map<String, dynamic> data;
  final WriteMode mode;
}

class DeleteOp extends LocalOp {
  const DeleteOp(this.table, this.id);
  final String table;
  final String id;
}

/// `_00_rv += 1` on a stored row.
class BumpRvOp extends LocalOp {
  const BumpRvOp(this.table, this.id);
  final String table;
  final String id;
}

/// Effects are data. A saga awaits one through its [Ctx] and gets the result
/// back; the interpreter is the only place they execute.
///
/// The `local.*` family is the one deliberate divergence from
/// `packages/core/src/kernel/effects.ts`: sqlite cannot run SurrealQL, so the
/// local store is addressed as a document store by `(table, id)` instead of by
/// a SurrealQL string or a rendered `QueryPlan`. Every other effect matches its
/// TypeScript twin one-for-one.
sealed class Effect<R> {
  const Effect();

  /// Stable discriminator used by the interpreter, `runPure` and test
  /// assertions on the effect log.
  String get kind;
}

// ---- local ------------------------------------------------------------------

class LocalGet extends Effect<Map<String, dynamic>?> {
  const LocalGet(this.table, this.id);
  final String table;
  final String id;
  @override
  String get kind => 'local.get';
}

/// Resolve many rows by id, in the order asked. Ids the store does not hold are
/// dropped rather than yielding nulls: this is the materialization read.
class LocalGetMany extends Effect<List<Map<String, dynamic>>> {
  const LocalGetMany(this.table, this.ids);
  final String table;
  final List<String> ids;
  @override
  String get kind => 'local.getMany';
}

class LocalGetAll extends Effect<List<Map<String, dynamic>>> {
  const LocalGetAll(this.table);
  final String table;
  @override
  String get kind => 'local.getAll';
}

class LocalPut extends Effect<void> {
  const LocalPut(this.table, this.id, this.data, {this.mode = WriteMode.replace, this.epoch});
  final String table;
  final String id;
  final Map<String, dynamic> data;
  final WriteMode mode;

  /// Store epoch this write was planned against; a write fenced with a stale
  /// epoch is dropped rather than landing in the wrong bucket.
  final int? epoch;
  @override
  String get kind => 'local.put';
}

class LocalDelete extends Effect<void> {
  const LocalDelete(this.table, this.id, {this.epoch});
  final String table;
  final String id;
  final int? epoch;
  @override
  String get kind => 'local.delete';
}

class LocalTx extends Effect<void> {
  const LocalTx(this.ops, {this.epoch});
  final List<LocalOp> ops;
  final int? epoch;
  @override
  String get kind => 'local.tx';
}

/// The store's current epoch; a write fenced with it is dropped after a bucket
/// switch.
class LocalEpoch extends Effect<int> {
  const LocalEpoch();
  @override
  String get kind => 'local.epoch';
}

// ---- remote -----------------------------------------------------------------

class RemoteQuery extends Effect<List<StatementResult>> {
  const RemoteQuery(this.sql, {this.vars, this.timeoutMs});
  final String sql;
  final Map<String, dynamic>? vars;
  final int? timeoutMs;
  @override
  String get kind => 'remote.query';
}

/// Subscribe to `LIVE SELECT * FROM <table>`; resolves to the live uuid.
class RemoteLive extends Effect<String> {
  const RemoteLive(this.table);
  final String table;
  @override
  String get kind => 'remote.live';
}

class RemoteKill extends Effect<void> {
  const RemoteKill(this.uuid);
  final String uuid;
  @override
  String get kind => 'remote.kill';
}

// ---- ssp --------------------------------------------------------------------

class SspRegister extends Effect<RegisterResult> {
  const SspRegister(this.plan);
  final RegisterPlan plan;
  @override
  String get kind => 'ssp.register';
}

class SspUnregister extends Effect<void> {
  const SspUnregister(this.hash);
  final String hash;
  @override
  String get kind => 'ssp.unregister';
}

class SspIngest extends Effect<void> {
  const SspIngest(this.records);
  final List<IngestRecord> records;
  @override
  String get kind => 'ssp.ingest';
}

// ---- timers -----------------------------------------------------------------

class TimerSet extends Effect<void> {
  const TimerSet(this.key, this.ms, this.event);
  final String key;
  final int ms;
  final RuntimeEvent event;
  @override
  String get kind => 'timer.set';
}

class TimerClear extends Effect<void> {
  const TimerClear(this.key);
  final String key;
  @override
  String get kind => 'timer.clear';
}

// ---- state ------------------------------------------------------------------

class StateRead<T> extends Effect<T> {
  const StateRead(this.select);
  final T Function(ClientState) select;
  @override
  String get kind => 'state.read';
}

class StateUpdate extends Effect<ClientState> {
  const StateUpdate(this.fn);
  final ClientState Function(ClientState) fn;
  @override
  String get kind => 'state.update';
}

/// Suspend until [until] holds (checked after every [StateUpdate]).
class StateWait extends Effect<void> {
  const StateWait(this.until);
  final bool Function(ClientState) until;
  @override
  String get kind => 'state.wait';
}

// ---- pure ports -------------------------------------------------------------

class NowEffect extends Effect<int> {
  const NowEffect();
  @override
  String get kind => 'now';
}

enum IdScope { mutation, salt }

class IdEffect extends Effect<String> {
  const IdEffect(this.scope);
  final IdScope scope;
  @override
  String get kind => 'id';
}

class HashEffect extends Effect<String> {
  const HashEffect(this.input);
  final String input;
  @override
  String get kind => 'hash';
}

class EmitEffect extends Effect<void> {
  const EmitEffect(this.event);
  final OutEvent event;
  @override
  String get kind => 'emit';
}

class DispatchEffect extends Effect<void> {
  const DispatchEffect(this.event);
  final RuntimeEvent event;
  @override
  String get kind => 'dispatch';
}

/// Fan out, allSettled semantics: one [Settled] per inner effect, in order.
class AllEffect extends Effect<List<Settled<Object?>>> {
  const AllEffect(this.effects);
  final List<Effect<Object?>> effects;
  @override
  String get kind => 'all';
}

/// Calls into the services that are adapters, not state: auth, the migrator,
/// persistence, the SSP lifecycle, the remote socket, the supervisor. A saga
/// names the call; the runtime binds it to the real service.
///
/// Mirrors the `ServiceCalls` table at
/// `packages/core/src/kernel/effects.ts:70`, minus the browser-only entries
/// (shared tabs, OPFS blobs, the DevTools window bridge).
enum ServiceName {
  hintRead,
  hintWrite,
  localConnect,
  localSwitchStore,
  localCurrentBucketId,
  migratorProvision,
  sspInit,
  sspSetPermissions,
  sspSetSessionAuth,
  sspPrime,
  sspReset,
  sspSetPersistence,
  authRestoreSession,
  authInit,
  authSessionAuthId,
  authAccess,
  authToken,
  authCurrentUser,
  remoteConnect,
  remoteReleaseViews,
  supervisorStart,
  crdtSetSessionId,
  crdtCloseAll,
  persistenceSet,
  featuresInit,
  releasesInit,
}

class ServiceEffect<R> extends Effect<R> {
  const ServiceEffect(this.name, [this.args = const []]);
  final ServiceName name;
  final List<Object?> args;
  @override
  String get kind => 'service';
}

/// Typed constructors. Each is one line so a saga reads like the plan.
abstract final class Fx {
  // local
  static Effect<Map<String, dynamic>?> localGet(String table, String id) =>
      LocalGet(table, id);
  static Effect<List<Map<String, dynamic>>> localGetMany(
          String table, List<String> ids) =>
      LocalGetMany(table, ids);
  static Effect<List<Map<String, dynamic>>> localGetAll(String table) =>
      LocalGetAll(table);
  static Effect<void> localPut(String table, String id, Map<String, dynamic> data,
          {WriteMode mode = WriteMode.replace, int? epoch}) =>
      LocalPut(table, id, data, mode: mode, epoch: epoch);
  static Effect<void> localDelete(String table, String id, {int? epoch}) =>
      LocalDelete(table, id, epoch: epoch);
  static Effect<void> localTx(List<LocalOp> ops, {int? epoch}) =>
      LocalTx(ops, epoch: epoch);
  static Effect<int> localEpoch() => const LocalEpoch();

  // remote
  static Effect<List<StatementResult>> remoteQuery(String sql,
          {Map<String, dynamic>? vars, int? timeoutMs}) =>
      RemoteQuery(sql, vars: vars, timeoutMs: timeoutMs);
  static Effect<String> remoteLive(String table) => RemoteLive(table);
  static Effect<void> remoteKill(String uuid) => RemoteKill(uuid);

  // ssp
  static Effect<RegisterResult> sspRegister(RegisterPlan plan) =>
      SspRegister(plan);
  static Effect<void> sspUnregister(String hash) => SspUnregister(hash);
  static Effect<void> sspIngest(List<IngestRecord> records) =>
      SspIngest(records);

  // timers
  static Effect<void> timerSet(String key, int ms, RuntimeEvent event) =>
      TimerSet(key, ms, event);
  static Effect<void> timerClear(String key) => TimerClear(key);

  // state
  static Effect<T> stateRead<T>(T Function(ClientState) select) =>
      StateRead<T>(select);
  static Effect<ClientState> stateUpdate(ClientState Function(ClientState) fn) =>
      StateUpdate(fn);
  static Effect<void> stateWait(bool Function(ClientState) until) =>
      StateWait(until);

  // pure ports
  static Effect<int> now() => const NowEffect();
  static Effect<String> id(IdScope scope) => IdEffect(scope);
  static Effect<String> hash(String input) => HashEffect(input);
  static Effect<void> emit(OutEvent event) => EmitEffect(event);
  static Effect<void> log(LogLevel level, String message,
          [Map<String, Object?>? data]) =>
      EmitEffect(LogEvent(level, message, data));
  static Effect<void> dispatch(RuntimeEvent event) => DispatchEffect(event);
  static Effect<List<Settled<Object?>>> all(List<Effect<Object?>> effects) =>
      AllEffect(effects);
  static Effect<R> service<R>(ServiceName name, [List<Object?> args = const []]) =>
      ServiceEffect<R>(name, args);
}
