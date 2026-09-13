/// A `(recordId, version)` pair, mirroring the WASM `[string, number]` tuple.
typedef RecordVersion = (String, int);

/// In-memory representation of a query result set as id/version pairs.
///
/// Mirrors the TS `RecordVersionArray = Array<[string, number]>`.
typedef RecordVersionArray = List<RecordVersion>;

/// A single record's id + version (matches `WasmDeltaRecord`).
class DeltaRecord {
  const DeltaRecord(this.id, this.version);

  final String id;
  final int version;

  factory DeltaRecord.fromJson(List<dynamic> json) =>
      DeltaRecord(json[0] as String, (json[1] as num).toInt());
}

/// Granular delta describing what changed (matches `WasmDelta`).
class ViewDelta {
  const ViewDelta({
    required this.additions,
    required this.removals,
    required this.updates,
  });

  final List<DeltaRecord> additions;
  final List<String> removals;
  final List<DeltaRecord> updates;

  factory ViewDelta.fromJson(Map<String, dynamic> json) => ViewDelta(
        additions: (json['additions'] as List<dynamic>)
            .map((e) => DeltaRecord.fromJson(e as List<dynamic>))
            .toList(),
        removals: (json['removals'] as List<dynamic>).cast<String>(),
        updates: (json['updates'] as List<dynamic>)
            .map((e) => DeltaRecord.fromJson(e as List<dynamic>))
            .toList(),
      );

  static const empty = ViewDelta(additions: [], removals: [], updates: []);
}

/// A materialized-view update emitted by the stream processor.
///
/// Mirrors the TS `StreamUpdate` (the local store path consumes [localArray];
/// [delta] and [resultHash] are carried for receivers that want them).
class StreamUpdate {
  const StreamUpdate({
    required this.queryHash,
    required this.localArray,
    this.resultHash = '',
    this.delta = ViewDelta.empty,
    this.op,
    this.materializationTimeMs,
    this.storeApplyMs,
    this.circuitStepMs,
    this.transformMs,
    this.parseMs,
    this.planMs,
    this.snapshotMs,
  });

  /// The query id this update is for (WASM `query_id`).
  final String queryHash;

  /// The full result set as id/version pairs (WASM `result_data`).
  final RecordVersionArray localArray;

  final String resultHash;
  final ViewDelta delta;

  /// Operation that produced this update; null for the register snapshot.
  final String? op;

  /// End-to-end ingest latency for the FFI call that produced this update.
  final double? materializationTimeMs;

  /// Per-phase circuit time (ms). The ingest path fills
  /// [storeApplyMs]/[circuitStepMs]/[transformMs]; the register path fills
  /// [parseMs]/[planMs]/[snapshotMs]. The unused side stays null.
  final double? storeApplyMs;
  final double? circuitStepMs;
  final double? transformMs;
  final double? parseMs;
  final double? planMs;
  final double? snapshotMs;

  /// Build from the decoded `WasmViewUpdate` JSON.
  factory StreamUpdate.fromWasm(
    Map<String, dynamic> json, {
    String? op,
    double? materializationTimeMs,
  }) {
    final resultData = (json['result_data'] as List<dynamic>)
        .map<RecordVersion>(
            (e) => ((e as List<dynamic>)[0] as String, (e[1] as num).toInt()))
        .toList();
    return StreamUpdate(
      queryHash: json['query_id'] as String,
      localArray: resultData,
      resultHash: (json['result_hash'] as String?) ?? '',
      delta: json['delta'] != null
          ? ViewDelta.fromJson(json['delta'] as Map<String, dynamic>)
          : ViewDelta.empty,
      op: op,
      materializationTimeMs: materializationTimeMs,
      storeApplyMs: _ms(json['timing_store_apply_ms']),
      circuitStepMs: _ms(json['timing_circuit_step_ms']),
      transformMs: _ms(json['timing_transform_ms']),
      parseMs: _ms(json['timing_parse_ms']),
      planMs: _ms(json['timing_plan_ms']),
      snapshotMs: _ms(json['timing_snapshot_ms']),
    );
  }

  /// The native side writes `0` for the phases a call did not run; report those
  /// as "not measured" rather than as a zero-millisecond phase.
  static double? _ms(Object? raw) {
    if (raw is! num) return null;
    final v = raw.toDouble();
    return v == 0 ? null : v;
  }
}

/// Thrown when the native processor returns an `{"err": ...}` envelope.
class SspException implements Exception {
  SspException(this.message);
  final String message;
  @override
  String toString() => 'SspException: $message';
}

/// What `registerView` answers with: the initial view update plus, under
/// projection, the fields this plan evaluates that stored rows do not hold.
class Registration {
  const Registration({required this.update, this.missingFields = const {}});

  final StreamUpdate update;
  final Map<String, List<String>> missingFields;
}

/// What `reconcile` answers with.
class Reconciled {
  const Reconciled({
    required this.fetch,
    required this.deleted,
    required this.updates,
  });

  /// Ids (the caller's spelling) whose body the store lacks or holds stale.
  final List<String> fetch;

  /// Rows deleted because the caller's list did not have them.
  final int deleted;

  /// View updates produced by those deletes.
  final List<StreamUpdate> updates;
}
