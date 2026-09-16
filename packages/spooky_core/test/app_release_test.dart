import 'package:spooky_core/spooky_core.dart';
import 'package:spooky_core/src/ffi/stream_processor.dart';
import 'package:spooky_core/src/modules/app_release/app_release.dart';
import 'package:spooky_core/src/services/logger/logger.dart';
import 'package:spooky_core/src/services/stream_processor/stream_processor_service.dart';
import 'package:test/test.dart';

import 'fake_query_host.dart';

/// `AppReleaseModule` runs ONE shared live query over the world-readable
/// `_00_app_release` table and fans per-app snapshots out to handles, so an app
/// can prompt (or force) an update when the deployed version passes the running
/// build.
void main() {
  final logger = SpookyLogger.root('test');
  const schemaSurql =
      'DEFINE TABLE thread SCHEMAFULL PERMISSIONS FOR select WHERE true;';

  Map<String, dynamic> releaseRow(
    String app,
    String version, {
    bool? cacheBust,
    bool? mandatory,
    String? releasedAt,
  }) =>
      {
        'id': '_00_app_release:$app',
        'app': app,
        'version': version,
        'cache_bust': cacheBust,
        'mandatory': mandatory,
        'released_at': releasedAt,
      };

  group('module behavior', () {
    late FakeQueryHost host;
    late AppReleaseModule releases;
    late FakeAuth auth;

    setUp(() {
      host = FakeQueryHost();
      auth = FakeAuth();
      releases = AppReleaseModule(host: host, auth: auth, logger: logger);
    });
    tearDown(() => releases.closeAll());

    /// Deliver release rows the way the shared query would.
    Future<void> push(List<Map<String, dynamic>> rows) async {
      await _tick(); // let the module's registration land
      host.emit(rows);
    }

    test('registers ONE shared, unfiltered query for many apps', () async {
      releases.release('web');
      releases.release('admin');
      await _tick();

      expect(host.registrations, hasLength(1));
      final surql = host.registrations.single.surql;
      expect(surql, contains('FROM _00_app_release'));
      expect(surql, isNot(contains('WHERE')));
    });

    test('fans the shared result out to each handle by app', () async {
      final web = releases.release('web');
      final missing = releases.release('missing');
      await push([releaseRow('web', '1.2.0', cacheBust: true)]);

      expect(web.version(), '1.2.0');
      expect(web.cacheBust, isTrue);
      expect(web.mandatory, isFalse, reason: 'a null flag reads as false');
      expect(missing.version(), isNull);
      expect(missing.updateAvailable('1.0.0'), isFalse);
    });

    test('updateAvailable compares semver against the running build', () async {
      final web = releases.release('web');
      await push([releaseRow('web', '1.2.0')]);

      expect(web.updateAvailable('1.1.9'), isTrue);
      expect(web.updateAvailable('1.2.0'), isFalse);
      expect(web.updateAvailable('1.3.0'), isFalse);
      expect(web.updateAvailable('garbage'), isFalse);
    });

    test('observes row updates live without re-registering', () async {
      final web = releases.release('web');
      await push([releaseRow('web', '1.0.0')]);
      expect(web.updateAvailable('1.0.0'), isFalse);

      await push([releaseRow('web', '1.0.1', mandatory: true)]);
      expect(web.updateAvailable('1.0.0'), isTrue);
      expect(web.mandatory, isTrue);
      expect(host.registrations, hasLength(1));
    });

    test('seeds a late handle from the already-loaded snapshot', () async {
      releases.release('web');
      await push([releaseRow('web', '2.0.0')]);

      final late = releases.release('web');
      expect(late.version(), '2.0.0',
          reason: 'a late handle must not flash empty');
    });

    test('subscribe fires immediately and on each change', () async {
      final web = releases.release('web');
      final seen = <String?>[];
      web.subscribe((s) => seen.add(s.version));
      expect(seen, [null], reason: 'immediate fire with the empty snapshot');

      await push([releaseRow('web', '1.0.0')]);
      expect(seen.last, '1.0.0');
    });

    test('a closed handle stops receiving snapshots', () async {
      final web = releases.release('web');
      final seen = <String?>[];
      web.subscribe((s) => seen.add(s.version));
      web.close();
      await push([releaseRow('web', '9.9.9')]);
      expect(seen, [null]);
    });

    test('re-registers on a user change', () async {
      releases.init();
      final web = releases.release('web');
      await push([releaseRow('web', '1.0.0')]);
      expect(web.version(), '1.0.0');

      auth.emit('user:other');
      await _tick();
      // Re-observed under the new session: one registration per observation,
      // and the previous subscription released.
      expect(host.registrations, hasLength(2));
      expect(host.liveSubscriptions, 1);
    });

    test('rows without an app or version are ignored', () async {
      final web = releases.release('web');
      await push([
        {'id': '_00_app_release:x', 'app': 'web'}, // no version
      ]);
      expect(web.version(), isNull);
    });
  });

  group('circuit permission', () {
    // `_00_app_release` is server-provisioned and absent from any app
    // schemaSurql, so seedPermissionsFromSchema can't derive a permission. The
    // service seeds a built-in `'true'`, else the default-deny circuit rejects
    // the view and releases silently never arrive.
    Map<String, dynamic> viewConfig() => {
          'id': 'rel',
          'surql': releaseQuery,
          'params': <String, dynamic>{},
          'clientId': 'local',
          'ttl': '10m',
          'lastActiveAt': '2026-01-01T00:00:00.000Z',
        };

    test('an explicit deny is enforced (permission control is active)', () {
      final sp = StreamProcessor.create();
      addTearDown(sp.dispose);
      sp.setPermission('_00_app_release', 'false');
      expect(() => sp.registerView(viewConfig()), throwsA(isA<SspException>()));
    });

    test('the built-in seed permits the view when the schema omits the table',
        () async {
      final svc = StreamProcessorService(logger);
      await svc.init();
      addTearDown(svc.close);
      svc.seedPermissionsFromSchema(schemaSurql);

      svc.registerQueryPlan(QueryPlanConfig(
        queryHash: 'rel',
        surql: releaseQuery,
        params: const {},
        ttl: '10m',
        lastActiveAt: DateTime.utc(2026),
      ));

      final updates = svc.ingest('_00_app_release', 'CREATE',
          '_00_app_release:web', releaseRow('web', '1.0.0'));
      final u = updates.firstWhere((e) => e.queryHash == 'rel');
      expect(u.localArray.map((e) => e.$1), contains('_00_app_release:web'));
    });
  });
}

Future<void> _tick() => Future<void>.delayed(const Duration(milliseconds: 20));
