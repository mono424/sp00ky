import '../kernel/constants.dart' as k;
import '../modules/ref_tables.dart';
import '../state/client_state.dart';
import '../utils/parser.dart' show ColumnSchema;

/// Static configuration the sagas read. Plain data, built once by the runtime
/// from `Sp00kyConfig`; sagas never see the config object itself.
class SagaEnv {
  const SagaEnv({
    required this.schema,
    this.refMode = RefMode.dedicated,
    this.anonLive = false,
    this.remoteTimeoutMs = 60000,
    this.pushTimeoutMs = k.pushTimeoutMs,
    this.outboxBatchSize = k.outboxBatchSize,
    this.degradeAfter = k.degradeAfterFailures,
    this.materializeDebounceMs = k.materializeDebounceMs,
    this.pollBaseMs = k.listRefPollBaseMs,
    this.defaultTtlMs = 600000,
  });

  /// `{ table: { columns: { field: ColumnSchema } } }` plus `backends`.
  final Map<String, dynamic> schema;
  final RefMode refMode;
  final bool anonLive;
  final int remoteTimeoutMs;
  final int pushTimeoutMs;
  final int outboxBatchSize;
  final int degradeAfter;
  final int materializeDebounceMs;
  final int pollBaseMs;
  final int defaultTtlMs;
}

/// The `_00_list_ref` table this session reads: per-user, anonymous, or global.
String listRefTable(SagaEnv env, ClientState s) {
  final userId = s.userId ?? (env.anonLive ? anonUserId : null);
  return listRefTableFor(env.refMode, userId);
}

/// The declared columns of [table], or null when the schema does not know it.
/// Internal `_00_*` tables are known-and-empty: their rows are stored whole.
Map<String, ColumnSchema>? columnsFor(SagaEnv env, String table) {
  final entry = env.schema[table];
  if (entry == null) {
    return table.startsWith('_00_') ? const {} : null;
  }
  final columns = (entry is Map && entry['columns'] is Map)
      ? entry['columns'] as Map
      : (entry as Map);
  return {
    for (final e in columns.entries)
      if (e.value is ColumnSchema) e.key as String: e.value as ColumnSchema
  };
}
