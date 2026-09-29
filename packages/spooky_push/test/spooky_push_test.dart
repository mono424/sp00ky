import 'dart:convert';

import 'package:fake_async/fake_async.dart';
import 'package:flutter/services.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:spooky_core/spooky_core.dart';
import 'package:spooky_core/src/services/logger/logger.dart';
import 'package:spooky_core/src/services/persistence/memory_persistence.dart';
import 'package:spooky_push/spooky_push.dart';

/// The Dart half of the plugin against a scripted OS side and a real
/// `PushModule` over a scripted server.
void main() {
  TestWidgetsFlutterBinding.ensureInitialized();
  final messenger =
      TestDefaultBinaryMessengerBinding.instance.defaultBinaryMessenger;

  late _Os os;
  late _Server server;
  late _Host host;

  setUp(() {
    os = _Os();
    server = _Server();
    host = _Host(server);
    messenger.setMockMethodCallHandler(Sp00kyPush.channel, os.handle);
  });
  tearDown(() => messenger.setMockMethodCallHandler(Sp00kyPush.channel, null));

  Future<void> fromOs(String method, Object? args) async {
    final data = const StandardMethodCodec().encodeMethodCall(
      MethodCall(method, args),
    );
    await messenger.handlePlatformMessage(
      Sp00kyPush.channel.name,
      data,
      (_) {},
    );
  }

  Sp00kyPush plugin({
    bool autoRegister = false,
    Duration debounce = Duration.zero,
  }) => Sp00kyPush.withHost(
    host,
    autoRegister: autoRegister,
    authDebounce: debounce,
  );

  test(
    'initialize gets the options; the launching tap comes back once',
    () async {
      os.initial = {
        'raw': {
          'sp00ky': {
            'v': 1,
            'kind': 'rule',
            'rule': 'dm',
            'notification': {'url': '/m/1'},
          },
          'aps': {
            'alert': {'title': 'Ann', 'body': 'hi'},
          },
        },
      };
      final p = Sp00kyPush.withHost(
        host,
        foreground: ForegroundPresentation.show,
        apnsEnvironment: ApnsEnvironment.sandbox,
      );
      await p.start();
      await p.start();
      final init = os.calls.where((c) => c.method == 'initialize').toList();
      expect(init, hasLength(1));
      expect(init.single.arguments, {
        'foreground': 'show',
        'channel': {
          'id': 'sp00ky_default',
          'name': 'Notifications',
          'importance': 4,
        },
        'signedIn': false,
        'apnsEnvironment': 'sandbox',
      });
      final first = await p.initialMessage();
      expect(first?.url, '/m/1');
      expect(first?.title, 'Ann');
      expect(first?.payload?.rule, 'dm');
      expect(await p.initialMessage(), isNull);
    },
  );

  test('iOS: enable prompts first, then registers the APNs token', () async {
    os.platform = 'ios';
    os.token = 'AABB';
    host.auth.user = 'user:a';
    final p = plugin();
    await p.start();
    os.calls.clear();
    final device = await p.enable(label: 'iPhone');
    expect(os.calls.first.method, 'requestPermission');
    expect(device.thisDevice, isTrue);
    final reg = server.calls.firstWhere((c) => c.$1.contains('register'));
    expect(reg.$2!['device'], {
      'kind': 'apns',
      'token': 'AABB',
      'appId': 'im.app',
      'platform': 'ios',
      'environment': 'sandbox',
    });
    expect((reg.$2!['opts'] as Map)['label'], 'iPhone');
    expect((reg.$2!['opts'] as Map)['userAgent'], 'iOS 18.0 ; iPhone15,2');
  });

  test('enable fails typed when the OS says no', () async {
    os.platform = 'ios';
    os.token = null;
    host.auth.user = 'user:a';
    final p = plugin();
    os.permission = 'denied';
    await expectLater(
      p.enable(),
      throwsA(
        isA<PushError>().having(
          (e) => e.code,
          'code',
          PushErrorCode.permissionDenied,
        ),
      ),
    );
    os.permission = 'granted';
    await expectLater(
      p.enable(),
      throwsA(
        isA<PushError>().having(
          (e) => e.code,
          'code',
          PushErrorCode.noRegistration,
        ),
      ),
    );
    expect(server.calls.where((c) => c.$1.contains('register')), isEmpty);
  });

  test(
    'Android: Firebase is configured from fn::push::info before the token is asked for',
    () async {
      final p = plugin();
      await p.start();
      os.calls.clear();
      final t = await p.token();
      expect(os.calls.map((c) => c.method), ['configureFirebase', 'getToken']);
      expect(os.calls.first.arguments, {
        'projectId': 'p',
        'appId': '1:2:android:3',
        'apiKey': 'k',
        'senderId': '42',
      });
      expect(t?.kind, PushKind.fcm);
      expect(t?.platform, PushPlatform.android);
      expect(t?.environment, isNull);
      os.calls.clear();
      await p.token();
      expect(os.calls, isEmpty, reason: 'configured once, token cached');
    },
  );

  test(
    'Android without push.fcm.android gets no token and never touches Firebase',
    () async {
      server.info = {
        'providers': ['fcm'],
      };
      final p = plugin();
      await p.start();
      os.calls.clear();
      expect(await p.token(), isNull);
      expect(os.calls, isEmpty);
    },
  );

  test(
    'sync follows the user: after sign-in (debounced), not again for the same state',
    () {
      fakeAsync((clock) {
        os.platform = 'ios';
        os.token = 'AABB';
        final p = plugin(
          autoRegister: true,
          debounce: const Duration(milliseconds: 1500),
        );
        p.start();
        clock.flushMicrotasks();
        expect(server.calls, isEmpty, reason: 'signed out: nothing to sync');
        host.auth.emit('user:a');
        clock.elapse(const Duration(milliseconds: 1499));
        expect(server.calls, isEmpty);
        clock.elapse(const Duration(milliseconds: 2));
        clock.flushMicrotasks();
        expect(
          server.calls.map((c) => c.$1),
          contains(contains('fn::push::register')),
        );
        expect(
          os.calls.where((c) => c.method == 'setSignedIn').last.arguments,
          {'signedIn': true},
        );
        final before = server.calls.length;
        p.sync();
        clock.flushMicrotasks();
        expect(
          server.calls.length,
          before,
          reason: 'same user, token and permission',
        );
      });
    },
  );

  test('Android sign-out drops the FCM token; a new token re-syncs', () async {
    host.auth.user = 'user:a';
    final p = plugin(autoRegister: true);
    await p.start();
    await p.sync();
    host.auth.emit(null);
    await Future<void>.delayed(Duration.zero);
    expect(os.calls.map((c) => c.method), contains('deleteToken'));

    host.auth.emit('user:a');
    await Future<void>.delayed(Duration.zero);
    server.calls.clear();
    os.nextToken = 'fcm-new-token-0123456789';
    await fromOs('onToken', 'fcm-new-token-0123456789');
    await Future<void>.delayed(const Duration(milliseconds: 10));
    final reg = server.calls.lastWhere((c) => c.$1.contains('register'));
    expect((reg.$2!['device'] as Map)['token'], 'fcm-new-token-0123456789');
  });

  test(
    'a silent push wakes the client and finishes the iOS background task',
    () async {
      os.platform = 'ios';
      final p = plugin();
      await p.start();
      final got = <PushEvent>[];
      p.onMessage.listen(got.add);
      await fromOs('onMessage', {
        'raw': {
          'sp00ky': {'v': 1, 'kind': 'rule', 'rule': 'sync'},
          'aps': {'content-available': 1},
        },
        'foreground': false,
        'id': 'bg-1',
      });
      await Future<void>.delayed(const Duration(milliseconds: 10));
      expect(got.single.isNudge, isTrue);
      expect(host.woken, 1);
      expect(os.calls.last.method, 'completeBackground');
      expect(os.calls.last.arguments, {'id': 'bg-1', 'result': 'newData'});
    },
  );

  test('taps arrive on onOpened; Android payloads are JSON strings', () async {
    final p = plugin();
    await p.start();
    final taps = <PushEvent>[];
    p.onOpened.listen(taps.add);
    await fromOs('onOpened', {
      'raw': {
        'sp00ky': jsonEncode({
          'v': 1,
          'kind': 'message',
          'message': '_00_push_message:m',
          'notification': {'url': '/x'},
        }),
        'notification': {'title': 'T', 'body': 'B'},
      },
      'action': 'reply',
    });
    await Future<void>.delayed(Duration.zero);
    expect(taps.single.url, '/x');
    expect(taps.single.action, 'reply');
    expect(taps.single.title, 'T');
    expect(taps.single.payload?.message, '_00_push_message:m');
  });

  test('permission strings map to PushPermission', () async {
    final p = plugin();
    for (final (raw, want) in [
      ('granted', PushPermission.granted),
      ('provisional', PushPermission.provisional),
      ('denied', PushPermission.denied),
      ('notDetermined', PushPermission.notDetermined),
      ('weird', PushPermission.notDetermined),
    ]) {
      os.permission = raw;
      expect(await p.permission(), want);
    }
  });
}

class _Os {
  String platform = 'android';
  String permission = 'granted';
  String? token = 'fcm-token-0123456789';
  String? nextToken;
  bool firebaseReady = false;
  Map<String, Object?>? initial;
  final calls = <MethodCall>[];

  Future<Object?> handle(MethodCall call) async {
    calls.add(call);
    switch (call.method) {
      case 'initialize':
        return {
          'platform': platform,
          'appId': 'im.app',
          'permission': permission,
          'model': platform == 'ios' ? 'iPhone15,2' : 'Pixel 8',
          'osVersion': platform == 'ios' ? '18.0' : '15',
          if (platform == 'ios') 'environment': 'sandbox',
          'firebaseReady': firebaseReady,
          'initial': initial,
        };
      case 'permission':
      case 'requestPermission':
        return permission;
      case 'configureFirebase':
        firebaseReady = true;
        return {'ready': true, 'restartRequired': false};
      case 'getToken':
        final t = nextToken ?? token;
        if (t == null) {
          throw PlatformException(code: 'no-token', message: 'none');
        }
        return t;
      default:
        return null;
    }
  }
}

class _Server {
  Object? info = {
    'enabled': false,
    'providers': ['apns', 'fcm'],
    'android': {
      'projectId': 'p',
      'appId': '1:2:android:3',
      'apiKey': 'k',
      'senderId': 42,
    },
  };
  final calls = <(String, Map<String, dynamic>?)>[];

  Future<List<dynamic>> call(String sql, [Map<String, dynamic>? vars]) async {
    calls.add((sql, vars));
    final fn = RegExp(r'fn::push::[a-z]+').firstMatch(sql)?.group(0);
    return [
      switch (fn) {
        'fn::push::info' => info,
        'fn::push::list' => <Object?>[],
        'fn::push::register' => {
          'id': '_00_push_subscription:n',
          'endpoint':
              '${(vars!['device'] as Map)['kind']}:${(vars['device'] as Map)['token']}'
                  .toLowerCase(),
        },
        'fn::push::unsubscribe' => 1,
        _ => null,
      },
    ];
  }
}

class _Auth implements Sp00kyAuth {
  String? user;
  final _listeners = <void Function(String?)>[];
  @override
  String? access = 'account';

  void emit(String? next) {
    user = next;
    for (final l in [..._listeners]) {
      l(next);
    }
  }

  @override
  Map<String, dynamic>? get currentUser => user == null ? null : {'id': user};
  @override
  bool get isAuthenticated => user != null;
  @override
  void Function() subscribe(void Function(String?) cb) {
    _listeners.add(cb);
    cb(user);
    return () => _listeners.remove(cb);
  }

  @override
  void Function() onBeforeSignOut(SignOutHook hook) => () {};

  @override
  dynamic noSuchMethod(Invocation invocation) => super.noSuchMethod(invocation);
}

class _Host implements PushHost {
  _Host(_Server server) : auth = _Auth(), _server = server;
  final _Server _server;
  @override
  final _Auth auth;
  int woken = 0;
  late final PushModule _push = PushModule(
    remote: _server.call,
    auth: auth,
    storage: MemoryPersistenceClient(),
    logger: SpookyLogger.root('test'),
  )..attach();
  @override
  PushModule get push => _push;
  @override
  void Function() subscribeToSyncHealth(void Function(SyncHealth) cb) => () {};
  @override
  void Function() subscribeToFetchActivity(void Function(int) cb) => () {};
  @override
  int get fetchingQueryCount => 0;
  @override
  void wake() => woken++;
}
