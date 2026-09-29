import 'dart:async';

import 'package:fake_async/fake_async.dart';
import 'package:spooky_core/spooky_core.dart';
import 'package:spooky_core/src/services/logger/logger.dart';
import 'package:spooky_core/src/services/persistence/memory_persistence.dart';
import 'package:test/test.dart';

/// `db.push`: which device belongs to whom, told to the server through
/// `fn::push::*`. The sync decision is a pure function of state; the module
/// only executes it.
void main() {
  const apns = PushToken(
      kind: PushKind.apns,
      token: 'AB12',
      appId: 'im.app',
      platform: PushPlatform.ios,
      environment: ApnsEnvironment.sandbox);
  const fcm = PushToken(
      kind: PushKind.fcm,
      token: 'fcm-tok',
      appId: 'im.app',
      platform: PushPlatform.android);
  const info = PushInfo(providers: {PushKind.apns, PushKind.fcm});
  PushDevice row(String endpoint,
          {bool disabled = false,
          ApnsEnvironment? env = ApnsEnvironment.sandbox,
          String app = 'im.app',
          String? label}) =>
      PushDevice(
          id: '_00_push_subscription:x',
          endpoint: endpoint,
          kind: PushKind.apns,
          appId: app,
          environment: env,
          label: label,
          rules: label == null ? null : ['dm'],
          disabledAt: disabled ? DateTime(2026) : null);

  PushSyncDecision decide({
    String? user = 'user:a',
    bool impersonating = false,
    PushPermission permission = PushPermission.granted,
    PushToken? token = apns,
    PushRecord? record,
    bool autoRegister = false,
    PushInfo? info,
    List<PushDevice>? devices,
  }) =>
      decidePushSync(PushSyncInput(
          userId: user,
          impersonating: impersonating,
          permission: permission,
          token: token,
          record: record,
          autoRegister: autoRegister,
          info: info,
          devices: devices));

  const optedIn =
      PushRecord(userId: 'user:a', enabled: true, endpoint: 'apns:ab12');

  group('decidePushSync', () {
    test('answers that need no server', () {
      expect(decide(user: null).status, PushSyncStatus.signedOut);
      expect(decide(impersonating: true).status, PushSyncStatus.impersonating);
      expect(decide(token: null, record: optedIn).status,
          PushSyncStatus.noRegistration);
      expect(decide().status, PushSyncStatus.notSubscribed,
          reason: 'never opted in');
      expect(
          decide(
                  record: const PushRecord(userId: 'user:a', enabled: false),
                  autoRegister: true)
              .status,
          PushSyncStatus.notSubscribed,
          reason: 'an explicit opt-out beats autoRegister');
      expect(decide(record: optedIn), isA<PushSyncNeedsServer>());
      expect(decide(autoRegister: true), isA<PushSyncNeedsServer>());
    });

    test('a permission that went away drops the rows but keeps the choice', () {
      final d = decide(
          permission: PushPermission.denied, record: optedIn, token: fcm);
      expect(d, isA<PushSyncDropRows>());
      expect(d.status, PushSyncStatus.permissionDenied);
      expect((d as PushSyncDropRows).endpoints, ['apns:ab12', 'fcm:fcm-tok']);
      expect(decide(permission: PushPermission.notDetermined).status,
          PushSyncStatus.permissionDefault);
      expect(decide(permission: PushPermission.provisional, record: optedIn),
          isA<PushSyncNeedsServer>());
    });

    test('with the server: ok, registered, resubscribed, disabled', () {
      expect(
          decide(
              record: optedIn,
              info: const PushInfo(providers: {PushKind.web}),
              devices: []).status,
          PushSyncStatus.disabled);
      expect(
          decide(record: optedIn, info: info, devices: [row('apns:ab12')])
              .status,
          PushSyncStatus.ok);

      for (final (why, devices) in [
        ('no row', <PushDevice>[]),
        ('disabled', [row('apns:ab12', disabled: true)]),
        (
          'other environment',
          [row('apns:ab12', env: ApnsEnvironment.production)]
        ),
        ('other app', [row('apns:ab12', app: 'im.other')]),
      ]) {
        final d = decide(record: optedIn, info: info, devices: devices);
        expect(d, isA<PushSyncRegister>(), reason: why);
        expect(d.status, PushSyncStatus.registered, reason: why);
      }

      // The token rotated: register the new one, carry label/rules over,
      // remove the old one.
      final d = decide(
          record: const PushRecord(
              userId: 'user:a', enabled: true, endpoint: 'apns:old'),
          info: info,
          devices: [row('apns:old', label: 'iPhone')]) as PushSyncRegister;
      expect(d.status, PushSyncStatus.resubscribed);
      expect(d.remove, 'apns:old');
      expect(d.label, 'iPhone');
      expect(d.rules, ['dm']);
    });
  });

  group('PushModule', () {
    late _Remote remote;
    late _Auth auth;
    late MemoryPersistenceClient storage;
    late PushModule push;

    setUp(() {
      remote = _Remote();
      auth = _Auth()..user = 'user:a';
      storage = MemoryPersistenceClient();
      push = PushModule(
          remote: remote.call,
          auth: auth,
          storage: storage,
          logger: SpookyLogger.root('test'))
        ..attach();
      remote.answers['fn::push::info'] = {
        'enabled': false,
        'providers': ['apns', 'fcm'],
        'android': {
          'projectId': 'p',
          'appId': '1:2:android:3',
          'apiKey': 'k',
          'senderId': 42
        },
      };
      remote.answers['fn::push::register'] = {
        'id': '_00_push_subscription:n',
        'endpoint': 'apns:ab12',
        'kind': 'apns'
      };
      remote.answers['fn::push::list'] = <Object?>[];
      remote.answers['fn::push::unsubscribe'] = 1;
    });

    test('info parses providers and the Firebase client config, and caches',
        () async {
      remote.fail = true;
      await expectLater(push.info(), throwsA(isA<PushError>()));
      remote.fail = false;
      final i = await push.info();
      expect(i.supports(PushKind.fcm), isTrue);
      expect(i.supports(PushKind.web), isFalse);
      expect(i.android?.senderId, '42');
      await push.info();
      expect(remote.calls.where((c) => c.$1.contains('info')), hasLength(2),
          reason: 'the failed read was not cached, the good one is');
    });

    test(
        'register sends the device, remembers the choice, removes the previous endpoint',
        () async {
      await storage.set(pushEndpointKey,
          '{"userId":"user:a","enabled":true,"endpoint":"apns:old"}');
      final d = await push.register(apns, label: 'iPhone', rules: const []);
      expect(d.thisDevice, isTrue);
      final reg = remote.calls.firstWhere((c) => c.$1.contains('register'));
      expect(reg.$2!['device'], {
        'kind': 'apns',
        'token': 'AB12',
        'appId': 'im.app',
        'platform': 'ios',
        'environment': 'sandbox',
      });
      expect(reg.$2!['opts'], {'label': 'iPhone'},
          reason: 'empty rules mean every rule: not sent');
      final unsub = remote.calls.lastWhere((c) => c.$1.contains('unsubscribe'));
      expect(unsub.$2, {'e': 'apns:old'});
      expect(await push.isRegistered(), isTrue);
      expect(await storage.get<String>(pushEndpointKey),
          contains('"endpoint":"apns:ab12"'));
    });

    test('refusals make no call', () async {
      auth.access = impersonationAccess;
      await expectLater(
          push.register(apns),
          throwsA(isA<PushError>()
              .having((e) => e.code, 'code', PushErrorCode.impersonating)));
      expect(await push.sync(apns), PushSyncStatus.impersonating);
      auth
        ..access = 'account'
        ..user = null;
      await expectLater(
          push.register(apns),
          throwsA(isA<PushError>()
              .having((e) => e.code, 'code', PushErrorCode.signedOut)));
      expect(remote.calls, isEmpty);
      auth.user = 'user:a';
      remote.answers['fn::push::info'] = {
        'providers': ['web']
      };
      await expectLater(
          push.register(fcm),
          throwsA(isA<PushError>()
              .having((e) => e.code, 'code', PushErrorCode.disabled)));
    });

    test('sync registers an opted-in device once, then reports ok', () async {
      expect(await push.sync(apns), PushSyncStatus.notSubscribed);
      await storage.set(pushEndpointKey,
          '{"userId":"user:a","enabled":true,"endpoint":"apns:ab12"}');
      expect(await push.sync(apns), PushSyncStatus.registered);
      remote.answers['fn::push::list'] = [
        {
          'id': 'x',
          'endpoint': 'apns:ab12',
          'kind': 'apns',
          'app_id': 'im.app',
          'environment': 'sandbox',
          'current': true
        }
      ];
      final before = remote.calls.length;
      expect(await push.sync(apns), PushSyncStatus.ok);
      expect(remote.calls.skip(before).map((c) => c.$1),
          everyElement(isNot(contains('register'))));
      remote.fail = true;
      expect(await push.sync(apns), PushSyncStatus.error,
          reason: 'never throws');
    });

    test('unsubscribe is remembered; autoRegister does not undo it', () async {
      await push.register(fcm);
      expect(await push.unsubscribe(), 1);
      expect(remote.calls.last.$2, {'e': 'fcm:fcm-tok'});
      expect(await push.sync(fcm, autoRegister: true),
          PushSyncStatus.notSubscribed);
      await push.unsubscribe(all: true);
      expect(remote.calls.last.$1, contains('unsubscribe(NONE)'));
    });

    test('sign-out removes this device but keeps the choice', () async {
      await push.register(fcm);
      await auth.runHooks();
      expect(remote.calls.last.$1, contains('unsubscribe'));
      expect(remote.calls.last.$2, {'e': 'fcm:fcm-tok'});
      expect(await push.isRegistered(), isTrue,
          reason: 'the user signs back in to push');

      final before = remote.calls.length;
      await auth.runHooks(impersonating: true);
      push.unsubscribeOnSignOut = false;
      await auth.runHooks();
      expect(remote.calls.length, before);
    });

    test('sign-out never waits more than 1.5 s for the server', () async {
      await push.register(fcm);
      fakeAsync((clock) {
        remote.hang = true;
        var done = false;
        auth.runHooks().then((_) => done = true);
        clock.elapse(const Duration(milliseconds: 1499));
        expect(done, isFalse);
        clock.elapse(const Duration(milliseconds: 2));
        expect(done, isTrue);
      });
    });

    test('devices, update, notify, cancel and test', () async {
      await push.register(fcm);
      remote.answers['fn::push::list'] = [
        {
          'id': '_00_push_subscription:1',
          'endpoint': 'fcm:fcm-tok',
          'kind': 'fcm',
          'platform': 'android',
          'created_at': '2026-09-29T10:00:00Z',
          'current': true,
        },
        {
          'id': '_00_push_subscription:2',
          'endpoint': 'https://push.example/x',
          'current': false
        },
      ];
      final list = await push.devices();
      expect(list.first.thisDevice, isTrue);
      expect(list.first.platform, PushPlatform.android);
      expect(list.first.createdAt, DateTime.utc(2026, 9, 29, 10));
      expect(list.last.kind, PushKind.web,
          reason: 'rows without a kind are web');
      expect(list.last.current, isFalse);

      remote.answers['fn::push::update'] = [
        {
          'id': '_00_push_subscription:1',
          'endpoint': 'fcm:fcm-tok',
          'label': 'Pixel'
        }
      ];
      final updated = await push.update(label: 'Pixel', rules: const []);
      expect(updated?.label, 'Pixel');
      expect(remote.calls.last.$2, {
        'e': 'fcm:fcm-tok',
        'o': {'label': 'Pixel', 'rules': <String>[]}
      });

      remote.answers['fn::push::notify'] = {
        'id': '_00_push_message:m',
        'status': 'pending'
      };
      final m = await push.notify(PushMessageInput(
          notification: const {'title': 'Later'},
          sendAt: DateTime.utc(2030),
          ttl: const Duration(minutes: 5)));
      expect(m.id, '_00_push_message:m');
      expect(remote.calls.last.$2!['m'], {
        'notification': {'title': 'Later'},
        'ttl': 300,
        'sendAt': '2030-01-01T00:00:00.000Z'
      });

      remote.answers['fn::push::cancel'] = true;
      expect(await push.cancel('_00_push_message:m'), isTrue);
      expect(
          remote.calls.last.$2!['id'], const RecordId('_00_push_message', 'm'));

      remote.answers['fn::push::test'] = {
        'id': '_00_push_message:t',
        'status': 'pending'
      };
      await push.test(title: 'Hi');
      expect(remote.calls.last.$2, {
        'o': {'title': 'Hi'}
      });
    });
  });

  group('PushPayload.tryParse', () {
    test('reads an iOS map and an FCM JSON string, refuses anything else', () {
      final ios = PushPayload.tryParse({
        'v': 1,
        'kind': 'rule',
        'rule': 'dm',
        'id': 'message:1',
        'notification': {'url': '/m/1', 'tag': 'dm:1'},
        'ts': 5,
      });
      expect(ios?.rule, 'dm');
      expect(ios?.url, '/m/1');
      expect(ios?.isNudge, isFalse);
      final android = PushPayload.tryParse(
          '{"v":1,"kind":"message","message":"_00_push_message:m","data":{"k":1}}');
      expect(android?.message, '_00_push_message:m');
      expect(android?.isNudge, isTrue);
      expect(PushPayload.tryParse({'v': 2, 'kind': 'rule'}), isNull);
      expect(PushPayload.tryParse('not json'), isNull);
      expect(PushPayload.tryParse(null), isNull);
    });
  });
}

class _Remote {
  final answers = <String, Object?>{};
  final calls = <(String, Map<String, dynamic>?)>[];
  bool fail = false;
  bool hang = false;

  Future<List<dynamic>> call(String sql, [Map<String, dynamic>? vars]) async {
    calls.add((sql, vars));
    if (hang) return Completer<List<dynamic>>().future;
    if (fail) throw StateError('offline');
    final fn = RegExp(r'fn::push::[a-z]+').firstMatch(sql)?.group(0);
    return [answers[fn]];
  }
}

class _Auth implements Sp00kyAuth {
  String? user;
  @override
  String? access = 'account';
  final _hooks = <SignOutHook>[];

  Future<void> runHooks({bool impersonating = false}) => Future.wait([
        for (final h in _hooks)
          h(SignOutContext(
              userId: user, token: 't', impersonating: impersonating))
      ]);

  @override
  Map<String, dynamic>? get currentUser => user == null ? null : {'id': user};
  @override
  bool get isAuthenticated => user != null;
  @override
  void Function() onBeforeSignOut(SignOutHook hook) {
    _hooks.add(hook);
    return () => _hooks.remove(hook);
  }

  @override
  dynamic noSuchMethod(Invocation invocation) => super.noSuchMethod(invocation);
}
