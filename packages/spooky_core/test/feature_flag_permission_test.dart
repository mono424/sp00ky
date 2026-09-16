import 'package:spooky_core/src/ffi/stream_processor.dart';
import 'package:spooky_core/src/ffi/stream_update.dart';
import 'package:spooky_core/src/services/logger/logger.dart';
import 'package:spooky_core/src/services/stream_processor/stream_processor_service.dart';
import 'package:test/test.dart';

/// Covers the client-circuit permission for the feature-flag meta table.
///
/// `_00_user_feature` is a server-provisioned meta table absent from any app
/// `schemaSurql`, so `seedPermissionsFromSchema` never derives a permission for
/// it. [StreamProcessorService] seeds a built-in `_00_user_feature -> 'true'` so
/// the view is permitted regardless of the circuit's default (a default-deny
/// circuit would otherwise reject it — see the service's divergence note). These
/// tests lock in that the meta table is permitted and that permission control is
/// genuinely active (an explicit `false` is rejected).
void main() {
  final logger = SpookyLogger.root('test');

  const featureQuery = 'SELECT key, variant, payload FROM _00_user_feature';
  Map<String, dynamic> featureRow(String key, String variant) => {
        'id': '_00_user_feature:$key',
        'user': 'user:alice',
        'key': key,
        'variant': variant,
      };
  Map<String, dynamic> viewConfig() => {
        'id': 'ff',
        'surql': featureQuery,
        'params': <String, dynamic>{},
        'clientId': 'local',
        'ttl': '10m',
        'lastActiveAt': '2026-01-01T00:00:00.000Z',
      };

  group('feature-flag circuit permission (FFI level)', () {
    late StreamProcessor sp;
    setUp(() => sp = StreamProcessor.create());
    tearDown(() => sp.dispose());

    test('with the permission seeded the assignment materializes', () {
      sp.setPermission('_00_user_feature', 'true');
      sp.registerView(viewConfig());
      final updates = sp.ingest('_00_user_feature', 'CREATE',
          '_00_user_feature:demo', featureRow('demo', 'on'));

      final u = updates.firstWhere((e) => e.queryHash == 'ff');
      expect(u.localArray.map((e) => e.$1), contains('_00_user_feature:demo'));
    });

    test('an explicit deny is enforced (permission control is active)', () {
      // Proves the circuit actually gates on the permission text: a `false`
      // permission rejects the view outright. This is why the built-in `'true'`
      // seed matters on a default-deny circuit.
      sp.setPermission('_00_user_feature', 'false');
      expect(() => sp.registerView(viewConfig()), throwsA(isA<SspException>()));
    });
  });

  group('StreamProcessorService built-in feature-flag seed', () {
    late StreamProcessorService svc;
    setUp(() async {
      svc = StreamProcessorService(logger);
      await svc.init();
    });
    tearDown(() => svc.close());

    test(
        'seedPermissionsFromSchema permits _00_user_feature even when the schema '
        'omits it', () {
      // A realistic app schema knows nothing about the meta table; the built-in
      // seed must still permit the feature view.
      svc.seedPermissionsFromSchema(
          'DEFINE TABLE thread SCHEMAFULL PERMISSIONS FOR select WHERE true;');

      svc.registerQueryPlan(QueryPlanConfig(
        queryHash: 'ff',
        surql: featureQuery,
        params: const {},
        ttl: '10m',
        lastActiveAt: DateTime.utc(2026),
      ));

      final updates = svc.ingest('_00_user_feature', 'CREATE',
          '_00_user_feature:demo', featureRow('demo', 'on'));

      final u = updates.firstWhere((e) => e.queryHash == 'ff');
      expect(u.localArray.map((e) => e.$1), contains('_00_user_feature:demo'),
          reason: 'built-in seed must let the assignment through the view');
    });
  });
}
