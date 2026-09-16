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
/// `StreamProcessorService`: receiver fan-out, the boot prime, snapshot
/// bookkeeping, timed ingest and query-plan registration.
///
/// Divergence from the browser client: [seedPermissionsFromSchema] seeds the
/// circuit's per-table select permissions from `schemaSurql` (the browser
/// relies on its deployed circuit already being permissive). Without this,
/// `registerView` hits the circuit's default-deny. See [permission_extractor].
class StreamProcessorService {
  StreamProcessorService(SpookyLogger logger, {StreamProcessor? processor})
      : _logger = logger.child('StreamProcessorService'),
        _processor = processor;

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

  /// Snapshot bookkeeping. The circuit is persisted ONLY as a store-only
  /// snapshot ([saveStoreSnapshot]), written by the client's checkpoint timer
  /// and on the way out, never per ingest or per registration: serializing
  /// the circuit walks every row, and on a real store that is hundreds of
  /// milliseconds that used to be paid by every `queryRaw`.
  bool _dirty = false;
  int _dirtyRows = 0;

  /// True when the circuit holds rows the stored snapshot does not.
  bool get snapshotDirty => _dirty;

  /// How many row changes the stored snapshot is behind by (approximate).
  int get dirtyRows => _dirtyRows;

  void _markDirty(int rows) {
    if (rows <= 0) return;
    _dirty = true;
    _dirtyRows += rows;
  }

  /// The snapshot was written: nothing is pending any more.
  void markSnapshotClean() {
    _dirty = false;
    _dirtyRows = 0;
  }

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
  }

  /// Initialize the native processor. The circuit starts empty; the boot
  /// prime fills it from the local store ([primeFromLocal]).
  Future<void> init() async {
    if (_initialized) return;
    _processor ??= StreamProcessor.create();
    _initialized = true;
    _logger.info('Initialized');
  }

  /// Fill the circuit from the LOCAL store.
  ///
  /// With a usable snapshot: install it under whatever views have registered
  /// meanwhile (`loadStoreState` re-primes them), then `reconcile` each table
  /// against the store's `(id, rv)` list so rows deleted since the checkpoint
  /// are stepped out and only rows added or changed since are read back and
  /// ingested. Without one: read every row and ingest it, chunked.
  ///
  /// Either way the circuit ends up equal to the local store without touching
  /// the network, so the first sync diff is a real delta rather than "fetch
  /// everything". Never throws: a failed prime just means the circuit fills
  /// from sync instead.
  Future<void> primeFromLocal({
    required List<String> tables,
    required Map<String, List<(String, int)>> versions,
    required List<Map<String, dynamic>> Function(String table, List<String> ids)
        selectByIds,
    Uint8List? snapshot,
    Set<String> pendingIds = const {},
    void Function(String table, List<(String, int)> entries)? onVersions,
  }) async {
    final processor = _processor;
    if (processor == null) return;
    final sw = Stopwatch()..start();
    var restored = false;
    if (snapshot != null && snapshot.isNotEmpty) {
      try {
        _dispatchUpdates(processor.loadStoreState(snapshot));
        restored = true;
      } catch (e) {
        _logger.warn('Circuit snapshot unreadable; priming from rows: $e');
      }
    }

    var ingested = 0;
    var deleted = 0;
    for (final table in tables) {
      final entries = versions[table] ?? const <(String, int)>[];
      List<String> toFetch;
      if (restored) {
        final result = processor.reconcile(table, entries);
        _dispatchUpdates(result.updates);
        deleted += result.deleted;
        toFetch = result.fetch;
      } else {
        toFetch = [for (final (id, _) in entries) id];
      }
      for (var i = 0; i < toFetch.length; i += _primeChunk) {
        final chunk = toFetch.sublist(
            i,
            i + _primeChunk > toFetch.length
                ? toFetch.length
                : i + _primeChunk);
        final rows = selectByIds(table, chunk);
        if (rows.isEmpty) continue;
        _dispatchUpdates(ingestMany([
          for (final row in rows)
            IngestRecord(
              table: table,
              op: IngestOp.create,
              id: row['id'].toString(),
              record: row,
            )
        ]));
        ingested += rows.length;
      }
      if (entries.isNotEmpty && onVersions != null) {
        // A row with a local write still queued is NOT at the server's version,
        // so it must not be reported as if it were.
        onVersions(table, [
          for (final e in entries)
            if (!pendingIds.contains(e.$1)) e
        ]);
      }
    }
    // A prime that could not restore rebuilt the whole store from rows: the
    // next checkpoint has to write it even if nothing else changes, or every
    // launch pays for the same rebuild. Reconciled ingests were already
    // counted by [ingestMany]; the deletions were not.
    if (!restored && ingested > 0) {
      _markDirty(ingested < _checkpointFloor ? _checkpointFloor : ingested);
    }
    _markDirty(deleted);
    _logger.info('Circuit primed from the local store '
        '(restored: $restored, ingested: $ingested, deleted: $deleted, '
        '${sw.elapsedMilliseconds}ms)');
  }

  static const int _primeChunk = 500;

  /// Enough dirty rows that the client's checkpoint timer writes a snapshot.
  static const int _checkpointFloor = 50;

  /// Ingest many records as ONE circuit step. Returns the coalesced updates and
  /// fans them out, exactly as [ingest] does for a single record.
  List<StreamUpdate> ingestMany(List<IngestRecord> records) {
    final processor = _processor;
    if (processor == null || records.isEmpty) return const [];
    try {
      final updates = processor.ingestMany([
        for (final r in records)
          {
            'table': r.table,
            'op': r.opName,
            'id': r.id,
            'record': _normalizeValue(r.record),
          }
      ]);
      if (updates.isNotEmpty) _notifyUpdates(updates);
      _markDirty(records.length);
      return updates;
    } catch (e) {
      _logger.error('Batch ingest failed', e);
      return const [];
    }
  }

  /// Throw the circuit away and start from an empty one, keeping the
  /// permissions and the session identity. Used by a bucket switch: the rows in
  /// it belong to the store that is being replaced.
  Future<void> resetCircuit() async {
    _processor?.dispose();
    _processor = StreamProcessor.create();
    _batchBuffer.clear();
    _batching = false;
    // The new circuit is empty and belongs to the next store, which has its
    // own snapshot: nothing here may be written over it.
    markSnapshotClean();
  }

  /// Snapshot the circuit's base collections for the next boot's prime.
  Uint8List? saveStoreSnapshot() {
    final processor = _processor;
    if (processor == null) return null;
    try {
      return processor.saveStoreState();
    } catch (e) {
      _logger.warn('Circuit snapshot failed: $e');
      return null;
    }
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

  /// Ingest a record change and fan out resulting updates. Mirrors the TS
  /// `ingest`.
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
      _markDirty(1);
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
