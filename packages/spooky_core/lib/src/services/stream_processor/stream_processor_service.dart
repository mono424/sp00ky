import 'dart:typed_data';

import '../../ffi/stream_processor.dart';
import '../../ffi/stream_update.dart';
import '../../surreal/value.dart';
import '../../types.dart';
import '../logger/logger.dart';
import 'permission_extractor.dart';

/// Config passed to [StreamProcessorService.registerQueryPlan]
/// (TS `QueryPlanConfig`).
class QueryPlanConfig {
  QueryPlanConfig({
    required this.queryHash,
    required this.surql,
    required this.params,
    required this.ttl,
    required this.lastActiveAt,
  });

  final String queryHash;
  final String surql;
  final Map<String, dynamic> params;
  final QueryTimeToLive ttl;
  final DateTime lastActiveAt;
}

/// Circuit ingest operation (TS `IngestRecord.op`). `merge` overlays the given
/// fields on the stored row, which is what projection widening needs.
enum IngestOp { create, update, delete, merge }

/// One record handed to the circuit (TS `IngestRecord`).
class IngestRecord {
  const IngestRecord({
    required this.table,
    required this.op,
    required this.id,
    required this.record,
  });

  final String table;
  final IngestOp op;
  final String id;
  final Map<String, dynamic> record;

  /// The wire spelling the native circuit expects.
  String get opName => switch (op) {
        IngestOp.create => 'CREATE',
        IngestOp.update => 'UPDATE',
        IngestOp.delete => 'DELETE',
        IngestOp.merge => 'MERGE',
      };
}

/// Implemented by anything that wants raw stream updates (TS
/// `StreamUpdateReceiver`). [CacheModule] and (later) DevTools implement it.
abstract class StreamUpdateReceiver {
  void onStreamUpdate(StreamUpdate update);
}

/// Wraps the native FFI [StreamProcessor], mirroring the TS
/// `StreamProcessorService`: receiver fan-out, state load/save, timed ingest,
/// and query-plan registration.
///
/// Divergence from the browser client: [seedPermissionsFromSchema] seeds the
/// circuit's per-table select permissions from `schemaSurql` (the browser
/// relies on its deployed circuit already being permissive). Without this,
/// `registerView` hits the circuit's default-deny. See [permission_extractor].
class StreamProcessorService {
  StreamProcessorService(this._persistence, SpookyLogger logger,
      {StreamProcessor? processor})
      : _logger = logger.child('StreamProcessorService'),
        _processor = processor;

  final PersistenceClient _persistence;
  final SpookyLogger _logger;
  StreamProcessor? _processor;
  bool _initialized = false;
  final List<StreamUpdateReceiver> _receivers = [];

  /// When true, [_notifyUpdates] coalesces updates into [_batchBuffer] (keyed
  /// by queryHash) instead of dispatching them. Used to collapse the per-record
  /// stream updates produced by a batched ingest into a single notification per
  /// query, so the UI updates once after the whole batch rather than row-by-row.
  bool _batching = false;
  final Map<String, StreamUpdate> _batchBuffer = {};

  static const _stateKey = '_00_stream_processor_state';

  /// Built-in `select` permissions for server-provisioned meta tables the client
  /// reads through the local view but that never appear in an app's `schemaSurql`
  /// (they live in `apps/cli/src/meta_tables_remote.surql`, not the app schema),
  /// so [seedPermissionsFromSchema] can't reach them.
  ///
  /// A circuit built with default-deny (see the class divergence note) would
  /// otherwise reject the view for these tables. We seed them explicitly so the
  /// view is permitted regardless of the circuit's default — the same reason the
  /// FFI tests always `setPermission` before `registerView`.
  ///
  /// `_00_user_feature` is scoped server-side to `user = $auth.id` and only the
  /// user's own rows ever sync down (server permission + per-user `_00_list_ref`),
  /// so `'true'` here is the same "trust the server filtered" model as every other
  /// synced table (e.g. `thread` seeds `'true'`). Extend this map for any future
  /// client-readable meta table.
  /// `_00_app_release` is world-readable server-side (root-only writes), so
  /// `'true'` is exact rather than a trust assumption.
  static const Map<String, String> _builtinSystemPermissions = {
    '_00_user_feature': 'true',
    '_00_app_release': 'true',
  };

  void addReceiver(StreamUpdateReceiver receiver) => _receivers.add(receiver);

  void _notifyUpdates(List<StreamUpdate> updates) {
    if (_batching) {
      // Coalesce by queryHash instead of dispatching. The FFI `result_data`
      // (localArray) is the full materialized array, so last-write-wins already
      // reflects every prior ingest in the batch. We sum the materialization
      // times so the single recorded sample reflects the batch's total work,
      // and emit `op: 'CREATE'` on flush so the coalesced update takes
      // DataModule's immediate (non-debounced) path.
      for (final update in updates) {
        final prev = _batchBuffer[update.queryHash];
        final summedTime = (prev?.materializationTimeMs ?? 0) +
            (update.materializationTimeMs ?? 0);
        _batchBuffer[update.queryHash] = StreamUpdate(
          queryHash: update.queryHash,
          localArray: update.localArray,
          resultHash: update.resultHash,
          delta: update.delta,
          op: 'CREATE',
          materializationTimeMs: summedTime,
        );
      }
      return;
    }
    _dispatchUpdates(updates);
  }

  void _dispatchUpdates(List<StreamUpdate> updates) {
    for (final update in updates) {
      for (final receiver in _receivers) {
        receiver.onStreamUpdate(update);
      }
    }
  }

  /// Open a coalescing window. While open, the per-record stream updates emitted
  /// by [ingest] are buffered (one entry per queryHash) instead of dispatched.
  /// Pair with [endBatch] in a try/finally so the window always closes,
  /// otherwise the processor stays stuck buffering forever.
  ///
  /// No-op if a batch is already open (nested batches aren't expected here).
  void beginBatch() {
    if (_batching) return;
    _batching = true;
    _batchBuffer.clear();
  }

  /// Close the coalescing window and flush: dispatch one coalesced
  /// [StreamUpdate] per buffered queryHash, then persist processor state once
  /// for the whole batch (instead of once per ingest).
  void endBatch() {
    if (!_batching) return;
    _batching = false;
    final buffered = _batchBuffer.values.toList();
    _batchBuffer.clear();
    if (buffered.isNotEmpty) {
      _dispatchUpdates(buffered);
    }
    // The processor state after the last ingest is cumulative, so a single
    // snapshot covers the whole batch. Kept fire-and-forget like the per-ingest
    // call it replaces.
    saveState();
  }

  /// Initialize the native processor and load any persisted circuit state.
  Future<void> init() async {
    if (_initialized) return;
    _processor ??= StreamProcessor.create();
    await loadState();
    _initialized = true;
    _logger.info('Initialized');
  }

  /// Seed per-table `select` permissions from the schema SURQL. Call after
  /// [init] and before registering real-table queries.
  void seedPermissionsFromSchema(String schemaSurql) {
    final processor = _processor;
    if (processor == null) return;
    // Built-ins first so a schema-derived permission can override one if the app
    // schema ever does define the table (forward-compat); meta tables absent from
    // the schema keep their built-in seed.
    _builtinSystemPermissions.forEach(processor.setPermission);
    final perms = extractTablePermissions(schemaSurql);
    perms.forEach(processor.setPermission);
    _logger.debug('Seeded ${perms.length + _builtinSystemPermissions.length} '
        'table permissions');
  }

  Future<void> loadState() async {
    final processor = _processor;
    if (processor == null) return;
    try {
      final state = await _persistence.get<String>(_stateKey);
      if (state != null && state.isNotEmpty) {
        processor.loadState(state);
        _logger.info('Loaded state from persistence');
      } else {
        _logger.info('No saved state found');
      }
    } catch (e) {
      _logger.error('Failed to load state', e);
    }
  }

  Future<void> saveState() async {
    final processor = _processor;
    if (processor == null) return;
    try {
      final state = processor.saveState();
      if (state.isNotEmpty) {
        await _persistence.set(_stateKey, state);
      }
    } catch (e) {
      _logger.error('Failed to save state', e);
    }
  }

  /// Ingest a record change, fan out resulting updates, and persist state.
  /// Mirrors the TS `ingest` (sync FFI call, then async state save).
  List<StreamUpdate> ingest(
      String table, String op, String id, Map<String, dynamic> record) {
    final processor = _processor;
    if (processor == null) {
      _logger.warn('Not initialized, skipping ingest');
      return [];
    }
    try {
      final normalized = _normalizeValue(record) as Map<String, dynamic>;
      final sw = Stopwatch()..start();
      final rawUpdates = processor.ingest(table, op, id, normalized);
      final ms = sw.elapsedMicroseconds / 1000.0;
      if (rawUpdates.isNotEmpty) {
        final updates = rawUpdates
            .map((u) => StreamUpdate(
                  queryHash: u.queryHash,
                  localArray: u.localArray,
                  resultHash: u.resultHash,
                  delta: u.delta,
                  op: op,
                  materializationTimeMs: ms,
                ))
            .toList();
        _notifyUpdates(updates);
      }
      // While batching, `endBatch` persists once for the whole batch, so skip
      // the redundant per-record snapshot here.
      if (!_batching) {
        saveState();
      }
      return rawUpdates;
    } catch (e) {
      _logger.error('Ingest failed', e);
      return [];
    }
  }

  /// Register a query plan and return its initial snapshot update.
  StreamUpdate? registerQueryPlan(QueryPlanConfig plan) {
    final processor = _processor;
    if (processor == null) {
      _logger.warn('Not initialized, skipping registration');
      return null;
    }
    final normalizedParams =
        _normalizeValue(plan.params) as Map<String, dynamic>;
    // Mirror the server's `fn::query::register`
    // (`object::extend(params, { auth: { id: $auth.id }, access: $access })`).
    // Without these, the SSP's permission_inject rejects any query whose table
    // permission references $auth with "requires $auth but registration params
    // lack it" - which silently breaks every owner-scoped live query.
    final paramsWithAuth = <String, dynamic>{
      ...normalizedParams,
      'auth': {'id': _sessionAuthId},
      'access': _sessionAccess,
    };
    final initial = processor.registerView({
      'id': plan.queryHash,
      'surql': plan.surql,
      'params': paramsWithAuth,
      'clientId': 'local',
      'ttl': plan.ttl.toString(),
      'lastActiveAt': plan.lastActiveAt.toUtc().toIso8601String(),
    });
    if (initial == null) {
      throw StateError('Failed to register query plan');
    }
    saveState();
    return initial.update;
  }

  /// Current session identity used for permission injection. Empty strings
  /// (never null) match the TS client, which sends `''` when signed out.
  String _sessionAuthId = '';
  String _sessionAccess = '';

  /// Set the signed-in identity for permission injection, mirroring the TS
  /// `setSessionAuth`. MUST be called before a `$auth`-gated query registers,
  /// and re-called on every auth change, or the SSP rejects the registration
  /// with "requires $auth but registration params lack it".
  void setSessionAuth(String? authId, String? access) {
    _sessionAuthId = authId ?? '';
    _sessionAccess = access ?? '';
    _logger.debug(
      'Session auth context updated (authId=$_sessionAuthId, '
      'access=$_sessionAccess)',
    );
  }

  void unregisterQueryPlan(String queryHash) {
    final processor = _processor;
    if (processor == null) return;
    try {
      processor.unregisterView(queryHash);
      saveState();
    } catch (e) {
      _logger.error('Error unregistering query plan', e);
    }
  }

  Future<void> close() async {
    _processor?.dispose();
    _processor = null;
    _initialized = false;
  }

  /// Recursively normalize a value for ingest. Mirrors the TS `normalizeValue`:
  /// binary blobs become `null` (JSON has no binary variant and the SSP can't
  /// filter on opaque bytes), [RecordId] becomes its `table:id` string, and
  /// maps/lists recurse.
  dynamic _normalizeValue(dynamic value) {
    if (value == null) return null;

    // Binary CRDT snapshots -> null (see TS comment / project_ssp_byte_ingest_gap).
    if (value is TypedData) return null;

    if (value is RecordId) return value.toString();

    // schema-coerced temporal types -> canonical strings (JSON has no native
    // form, and the SSP filters on the string representation).
    if (value is DateTime) return value.toUtc().toIso8601String();
    if (value is SurrealDuration) return value.toString();

    if (value is Map) {
      final out = <String, dynamic>{};
      value.forEach((k, v) {
        out[k.toString()] = _normalizeValue(v);
      });
      return out;
    }

    if (value is List) {
      return value.map(_normalizeValue).toList();
    }

    return value;
  }
}
