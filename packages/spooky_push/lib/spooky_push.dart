/// Native push for sp00ky apps: APNs on iOS, FCM on Android.
///
/// The OS half of `db.push` (spooky_core): asks for the notification
/// permission, gets the device token, keeps it registered for the signed-in
/// user, and reports what arrives and what the user taps. What is pushed is
/// decided server-side by the `push:` rules in sp00ky.yml.
///
/// ```dart
/// final push = Sp00kyPush(db);
/// await push.start();                       // after runApp
/// push.onOpened.listen((e) => router.go(e.url ?? '/'));
/// // From a settings toggle (user gesture):
/// await push.enable(label: 'My phone');
/// ```
///
/// Android needs no google-services.json: the Firebase client config comes
/// from `push.fcm.android` in sp00ky.yml through `fn::push::info()`.
library;

import 'dart:async';

import 'package:flutter/services.dart';
import 'package:flutter/widgets.dart';
import 'package:spooky_core/spooky_core.dart';

export 'package:spooky_core/spooky_core.dart'
    show
        ApnsEnvironment,
        FirebaseClientConfig,
        PushDevice,
        PushError,
        PushErrorCode,
        PushKind,
        PushPayload,
        PushPermission,
        PushPlatform,
        PushSyncStatus,
        PushToken;

/// Whether a push that arrives while the app is open is shown by the OS.
enum ForegroundPresentation {
  /// The app shows it (or not) from [Sp00kyPush.onMessage].
  hide,
  show,
}

/// An Android notification channel (Android 8+). The server picks one per
/// rule (`native.android.channelId`); this one is the default.
@immutable
class AndroidChannel {
  const AndroidChannel({
    required this.id,
    required this.name,
    this.description,
    this.importance = 4,
  });

  static const defaults = AndroidChannel(
    id: 'sp00ky_default',
    name: 'Notifications',
  );

  final String id;
  final String name;
  final String? description;

  /// `NotificationManager.IMPORTANCE_*`: 4 = high (heads-up), 3 = default.
  final int importance;

  Map<String, Object?> toMap() => {
    'id': id,
    'name': name,
    if (description != null) 'description': description,
    'importance': importance,
  };
}

/// A push that arrived, or one the user tapped.
@immutable
class PushEvent {
  const PushEvent(this.raw, {this.action, this.foreground = false, this.id});

  /// iOS: the notification's `userInfo`. Android: the data fields, plus
  /// `notification` (`title`, `body`, `tag`, `channelId`) when there is one.
  final Map<String, Object?> raw;

  /// The action button tapped, `null` for the notification itself.
  final String? action;

  /// Arrived while the app was in the foreground.
  final bool foreground;

  /// iOS silent push: the id to finish it with (the plugin does).
  final String? id;

  /// The sp00ky part: rule / message, record id, url. `null` for a push
  /// something else sent.
  PushPayload? get payload => PushPayload.tryParse(raw['sp00ky']);

  /// A silent push: refresh, show nothing.
  bool get isNudge => payload?.isNudge ?? false;

  String? get url => payload?.url;

  Map<String, Object?>? get _alert {
    final aps = raw['aps'];
    if (aps is Map && aps['alert'] is Map) {
      return (aps['alert'] as Map).cast<String, Object?>();
    }
    final n = raw['notification'];
    return n is Map ? n.cast<String, Object?>() : null;
  }

  String? get title => _alert?['title'] as String?;
  String? get body => _alert?['body'] as String?;
}

/// What [Sp00kyPush] needs from a client. [Sp00kyClient] is one; tests
/// supply their own.
abstract interface class PushHost {
  PushModule get push;
  Sp00kyAuth get auth;
  void Function() subscribeToSyncHealth(void Function(SyncHealth) cb);
  void Function() subscribeToFetchActivity(void Function(int) cb);
  int get fetchingQueryCount;
  void wake();
}

class _ClientHost implements PushHost {
  _ClientHost(this.db);
  final Sp00kyClient db;
  @override
  PushModule get push => db.push;
  @override
  Sp00kyAuth get auth => db.auth;
  @override
  void Function() subscribeToSyncHealth(void Function(SyncHealth) cb) =>
      db.subscribeToSyncHealth(cb);
  @override
  void Function() subscribeToFetchActivity(void Function(int) cb) =>
      db.subscribeToFetchActivity(cb);
  @override
  int get fetchingQueryCount => db.fetchingQueryCount;
  @override
  void wake() => db.wake();
}

PushPermission _permission(Object? v) => switch (v) {
  'granted' => PushPermission.granted,
  'provisional' => PushPermission.provisional,
  'denied' => PushPermission.denied,
  _ => PushPermission.notDetermined,
};

Map<String, Object?> _map(Object? v) =>
    v is Map ? v.cast<String, Object?>() : const {};

class Sp00kyPush with WidgetsBindingObserver {
  /// [autoRegister]: register as soon as the permission is granted, without
  /// an explicit [enable]. Off by default: an app that asks for notification
  /// permission for something else must not opt users into pushes.
  ///
  /// [apnsEnvironment]: which APNs host this build's token belongs to. By
  /// default read from the app's provisioning profile (development builds:
  /// sandbox, TestFlight / App Store: production).
  Sp00kyPush(
    Sp00kyClient db, {
    bool autoRegister = false,
    ForegroundPresentation foreground = ForegroundPresentation.hide,
    ApnsEnvironment? apnsEnvironment,
    AndroidChannel androidChannel = AndroidChannel.defaults,
    bool rotateTokenOnSignOut = true,
    Future<void> Function(PushEvent event)? onNudge,
    String? label,
  }) : this.withHost(
         _ClientHost(db),
         autoRegister: autoRegister,
         foreground: foreground,
         apnsEnvironment: apnsEnvironment,
         androidChannel: androidChannel,
         rotateTokenOnSignOut: rotateTokenOnSignOut,
         onNudge: onNudge,
         label: label,
       );

  @visibleForTesting
  Sp00kyPush.withHost(
    this._host, {
    this.autoRegister = false,
    this.foreground = ForegroundPresentation.hide,
    this.apnsEnvironment,
    this.androidChannel = AndroidChannel.defaults,
    this.rotateTokenOnSignOut = true,
    this.onNudge,
    this.label,
    @visibleForTesting
    Duration authDebounce = const Duration(milliseconds: 1500),
  }) : _authDebounce = authDebounce;

  static const channel = MethodChannel('dev.sp00ky/push');

  final PushHost _host;
  final bool autoRegister;
  final ForegroundPresentation foreground;
  final ApnsEnvironment? apnsEnvironment;
  final AndroidChannel androidChannel;

  /// Android: drop the FCM token on sign-out, so pushes for the previous
  /// user stop even if the server never heard about the sign-out.
  final bool rotateTokenOnSignOut;

  /// Handle a silent push (the app is woken for it). Default: wake the
  /// client and wait up to 10 s for its queries to settle.
  final Future<void> Function(PushEvent event)? onNudge;

  /// Device label for the settings list. Default: the device model.
  final String? label;
  final Duration _authDebounce;

  final _messages = StreamController<PushEvent>.broadcast();
  final _opened = StreamController<PushEvent>.broadcast();
  final _offs = <void Function()>[];

  Future<void>? _starting;
  String _platform = '';
  String _appId = '';
  ApnsEnvironment? _environment;
  String? _model;
  String? _osVersion;
  String? _nativeToken;
  bool _firebaseReady = false;
  PushEvent? _initial;
  PushPermission _lastPermission = PushPermission.notDetermined;
  String? _user;
  Timer? _debounce;
  (String?, String?, PushPermission)? _lastSynced;
  PushSyncStatus? _lastStatus;
  bool _disposed = false;

  /// Received while the app runs (foreground, or woken for a silent push).
  Stream<PushEvent> get onMessage => _messages.stream;

  /// Notifications the user tapped after [start]. The tap that launched
  /// the app is [initialMessage].
  Stream<PushEvent> get onOpened => _opened.stream;

  bool get _isIos => _platform == 'ios';

  /// Hand-shake with the OS side, then keep the device registered: after
  /// sign-in, on reconnect, when the app returns with a changed permission,
  /// when the OS rotates the token. Idempotent.
  Future<void> start() => _starting ??= _start();

  Future<void> _start() async {
    channel.setMethodCallHandler(_onNative);
    final user = _host.auth.currentUser?['id']?.toString();
    final init = _map(
      await channel.invokeMethod<Object?>('initialize', {
        'foreground': foreground.name,
        'channel': androidChannel.toMap(),
        'signedIn': user != null,
        if (apnsEnvironment != null) 'apnsEnvironment': apnsEnvironment!.name,
      }),
    );
    _platform = (init['platform'] ?? '') as String;
    _appId = (init['appId'] ?? '') as String;
    _environment = switch (init['environment']) {
      'sandbox' => ApnsEnvironment.sandbox,
      'production' => ApnsEnvironment.production,
      _ => null,
    };
    _model = init['model'] as String?;
    _osVersion = init['osVersion'] as String?;
    _nativeToken = init['token'] as String?;
    _firebaseReady = init['firebaseReady'] == true;
    _lastPermission = _permission(init['permission']);
    final initial = init['initial'];
    if (initial is Map) {
      _initial = PushEvent(
        _map(initial['raw']),
        action: initial['action'] as String?,
      );
    }

    // Touching the module installs its sign-out hook.
    _host.push;
    _user = user;
    _offs.add(_host.auth.subscribe(_onUser));
    _offs.add(
      _host.subscribeToSyncHealth((h) {
        // By name: Flutter's own ConnectionState shadows spooky_core's.
        if (h.connection.name == 'connected' &&
            _lastStatus == PushSyncStatus.error) {
          _schedule(Duration.zero);
        }
      }),
    );
    WidgetsBinding.instance.addObserver(this);
    _offs.add(() => WidgetsBinding.instance.removeObserver(this));
    if (user != null) await _syncNow();
  }

  void _onUser(String? user) {
    final previous = _user;
    if (user == previous) return;
    _user = user;
    unawaited(
      channel.invokeMethod<void>('setSignedIn', {'signedIn': user != null}),
    );
    if (user != null) {
      _schedule(_authDebounce);
    } else if (previous != null) {
      _debounce?.cancel();
      _lastSynced = null;
      if (!_isIos && rotateTokenOnSignOut) {
        _nativeToken = null;
        unawaited(channel.invokeMethod<void>('deleteToken').catchError((_) {}));
      }
    }
  }

  void _schedule(Duration after) {
    _debounce?.cancel();
    _debounce = Timer(after, () => unawaited(sync()));
  }

  @override
  void didChangeAppLifecycleState(AppLifecycleState state) {
    if (state != AppLifecycleState.resumed) return;
    unawaited(
      permission()
          .then((p) {
            if (p != _lastPermission) _schedule(Duration.zero);
          })
          .catchError((_) {}),
    );
  }

  Future<void> dispose() async {
    _disposed = true;
    _debounce?.cancel();
    for (final off in _offs) {
      off();
    }
    _offs.clear();
    channel.setMethodCallHandler(null);
    await _messages.close();
    await _opened.close();
  }

  Future<PushPermission> permission() async {
    final p = _permission(await channel.invokeMethod<Object?>('permission'));
    _lastPermission = p;
    return p;
  }

  /// Ask the OS. Call it from a user gesture. iOS `provisional`: no prompt,
  /// quiet delivery to the notification center.
  Future<PushPermission> requestPermission({bool provisional = false}) async {
    final p = _permission(
      await channel.invokeMethod<Object?>('requestPermission', {
        'provisional': provisional,
      }),
    );
    _lastPermission = p;
    return p;
  }

  /// The device token, or `null` when the OS gave none (no permission to
  /// register on iOS, no Play services, no network, no FCM config).
  Future<PushToken?> token() async {
    if (_platform.isEmpty) await start();
    var raw = _nativeToken;
    if (raw == null) {
      if (!_isIos && !_firebaseReady) {
        final info = await _host.push.info();
        final config = info.android;
        if (config == null) return null;
        final r = _map(
          await channel.invokeMethod<Object?>(
            'configureFirebase',
            config.toJson(),
          ),
        );
        _firebaseReady = r['ready'] == true;
        if (!_firebaseReady) {
          debugPrint(
            'spooky_push: Firebase not ready (${r['conflict'] ?? r['error'] ?? 'unknown'})',
          );
          return null;
        }
      }
      try {
        raw = await channel.invokeMethod<String>('getToken', {
          'timeoutMs': 10000,
        });
      } on PlatformException catch (e) {
        debugPrint('spooky_push: no device token (${e.code}: ${e.message})');
        return null;
      }
      _nativeToken = raw;
    }
    if (raw == null || raw.isEmpty) return null;
    return PushToken(
      kind: _isIos ? PushKind.apns : PushKind.fcm,
      token: raw,
      appId: _appId,
      platform: _isIos ? PushPlatform.ios : PushPlatform.android,
      environment: _isIos
          ? (apnsEnvironment ?? _environment ?? ApnsEnvironment.production)
          : null,
    );
  }

  String get _userAgent => [
    if (_isIos) 'iOS' else 'Android',
    if (_osVersion != null) _osVersion!,
    if (_model != null) '; ${_model!}',
  ].join(' ');

  /// Ask for permission, get the token, register this device.
  Future<PushDevice> enable({
    String? label,
    List<String>? rules,
    Map<String, Object?>? meta,
  }) async {
    await start();
    final p = await requestPermission();
    if (!p.allowsDelivery) {
      throw const PushError(
        PushErrorCode.permissionDenied,
        'notifications are not allowed',
      );
    }
    final t = await token();
    if (t == null) {
      throw const PushError(
        PushErrorCode.noRegistration,
        'the OS gave no device token',
      );
    }
    final device = await _host.push.register(
      t,
      label: label ?? this.label ?? _model,
      rules: rules,
      meta: meta,
      userAgent: _userAgent,
    );
    _lastSynced = (_user, t.endpoint, p);
    _lastStatus = PushSyncStatus.registered;
    return device;
  }

  /// Stop pushes to this device (`all`: every device of the user).
  Future<int> disable({bool all = false}) async {
    final n = await _host.push.unsubscribe(all: all);
    _lastSynced = null;
    return n;
  }

  /// Reconcile now. Skipped when nothing changed since the last success.
  Future<PushSyncStatus> sync() async {
    if (_disposed) return PushSyncStatus.error;
    await start();
    return _syncNow();
  }

  Future<PushSyncStatus> _syncNow() async {
    final user = _host.auth.currentUser?['id']?.toString();
    PushPermission p;
    try {
      p = await permission();
    } catch (_) {
      return _lastStatus = PushSyncStatus.error;
    }
    final t = await token().catchError((_) => null);
    final key = (user, t?.endpoint, p);
    if (key == _lastSynced && _lastStatus != null) return _lastStatus!;
    final status = await _host.push.sync(
      t,
      permission: p,
      autoRegister: autoRegister,
      userAgent: _userAgent,
    );
    _lastStatus = status;
    _lastSynced = status == PushSyncStatus.error ? null : key;
    return status;
  }

  /// The tap that launched the app, once.
  Future<PushEvent?> initialMessage() async {
    await start();
    final e = _initial;
    _initial = null;
    return e;
  }

  Future<void> setBadge(int count) =>
      channel.invokeMethod<void>('setBadge', {'count': count});

  Future<bool> openSettings() async =>
      await channel.invokeMethod<bool>('openSettings') ?? false;

  /// Android: another channel a rule can name in `native.android.channelId`.
  Future<void> createChannel(AndroidChannel channel) =>
      Sp00kyPush.channel.invokeMethod<void>('createChannel', channel.toMap());

  Future<Object?> _onNative(MethodCall call) async {
    switch (call.method) {
      case 'onToken':
        final t = call.arguments as String?;
        if (t != null && t != _nativeToken) {
          _nativeToken = t;
          if (_user != null) _schedule(Duration.zero);
        }
      case 'onTokenError':
        debugPrint('spooky_push: token error: ${call.arguments}');
      case 'onMessage':
        final a = _map(call.arguments);
        final event = PushEvent(
          _map(a['raw']),
          foreground: a['foreground'] == true,
          id: a['id'] as String?,
        );
        if (!_messages.isClosed) _messages.add(event);
        if (event.isNudge || event.id != null) unawaited(_nudge(event));
      case 'onOpened':
        final a = _map(call.arguments);
        final event = PushEvent(_map(a['raw']), action: a['action'] as String?);
        if (!_opened.isClosed) _opened.add(event);
    }
    return null;
  }

  /// A silent push woke the app: refresh, then tell iOS it may suspend.
  Future<void> _nudge(PushEvent event) async {
    var result = 'noData';
    try {
      final custom = onNudge;
      if (custom != null) {
        await custom(event);
      } else {
        _host.wake();
        await _settled(const Duration(seconds: 10));
      }
      result = 'newData';
    } catch (_) {
      result = 'failed';
    } finally {
      if (event.id != null) {
        unawaited(
          channel
              .invokeMethod<void>('completeBackground', {
                'id': event.id,
                'result': result,
              })
              .catchError((_) {}),
        );
      }
    }
  }

  Future<void> _settled(Duration timeout) {
    if (_host.fetchingQueryCount == 0) return Future.value();
    final done = Completer<void>();
    late void Function() off;
    off = _host.subscribeToFetchActivity((n) {
      if (n == 0 && !done.isCompleted) done.complete();
    });
    return done.future
        .timeout(timeout, onTimeout: () {})
        .whenComplete(() => off());
  }
}
