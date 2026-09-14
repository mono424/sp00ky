import 'package:spooky_core/src/kernel/effects.dart';
import 'package:spooky_core/src/query/env.dart';
import 'package:spooky_core/src/testing/run_pure.dart';
import 'package:spooky_core/src/types.dart';
import 'package:spooky_core/src/utils/parser.dart' show ColumnSchema;

/// A schema with one domain table, which is all most saga tests need.
const testSchema = <String, dynamic>{
  'thing': {
    'columns': <String, ColumnSchema>{
      'title': ColumnSchema(type: 'string'),
      'owner': ColumnSchema(recordId: true, type: 'record<user>'),
    }
  },
  'child': {'columns': <String, ColumnSchema>{}},
};

SagaEnv env({
  Map<String, dynamic> schema = testSchema,
  int outboxBatchSize = 50,
  int degradeAfter = 3,
  int pollBaseMs = 500,
}) =>
    SagaEnv(
      schema: schema,
      outboxBatchSize: outboxBatchSize,
      degradeAfter: degradeAfter,
      pollBaseMs: pollBaseMs,
    );

/// Answers the adapter effects a test does not care about, so each test scripts
/// only the ones it is actually about. Anything still unhandled throws, exactly
/// as it would in the TypeScript harness.
Map<String, EffectHandler> defaults({
  Map<String, EffectHandler> over = const {},
  RecordVersionArrayFn? sspLocalArray,
}) =>
    {
      'local.get': (_, __) => null,
      'local.getMany': (_, __) => <Map<String, dynamic>>[],
      'local.getAll': (_, __) => <Map<String, dynamic>>[],
      'local.put': (_, __) => null,
      'local.delete': (_, __) => null,
      'local.tx': (_, __) => null,
      'remote.query': (_, __) => <StatementResult>[],
      'remote.live': (_, __) => 'live-uuid',
      'remote.kill': (_, __) => null,
      'ssp.register': (e, __) => RegisterResult(
            localArray:
                sspLocalArray?.call((e as SspRegister).plan.queryHash) ??
                    const [],
            timings: const RegistrationTimings(parseMs: 1),
          ),
      'ssp.unregister': (_, __) => null,
      'ssp.ingest': (_, __) => null,
      'service': (_, __) => null,
      ...over,
    };

typedef RecordVersionArrayFn = List<(String, int)> Function(String hash);

/// Build a `[{out, version}, ...]` edge statement result.
List<Map<String, dynamic>> edges(List<(String, int)> pairs) => [
      for (final (id, v) in pairs) {'out': id, 'version': v}
    ];

/// The three-statement answer `singleSnapshotSelect` expects.
List<StatementResult> snapshot({
  List<(String, int)> primary = const [],
  Object? meta,
  List<(String, int)> children = const [],
}) =>
    [
      StatementResult.ok(edges(primary)),
      StatementResult.ok(meta),
      StatementResult.ok(edges(children)),
    ];

/// The four-statement answer `registerSelect` expects.
List<StatementResult> registerAnswer({
  List<(String, int)> primary = const [],
  Object? meta,
  List<(String, int)> children = const [],
}) =>
    [
      const StatementResult.ok(null),
      ...snapshot(primary: primary, meta: meta, children: children),
    ];

Map<String, dynamic> readyMeta(int rowCount) =>
    {'rowCount': rowCount, 'state': 'ready'};
