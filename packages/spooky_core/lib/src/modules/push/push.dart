import 'dart:async';
import 'dart:convert';

import 'package:meta/meta.dart';

import '../../services/logger/logger.dart';
import '../../surreal/value.dart';
import '../../types.dart';
import '../auth/sp00ky_auth.dart';

/// Native push (APNs, FCM) for a sp00ky client: `db.push`.
///
/// The Dart side of the platform's push feature. Rules in `sp00ky.yml`
/// (`push:`) decide what is sent; this module only tells the server which
/// device belongs to the signed-in user, through the `fn::push::*` functions
/// (`_00_` tables have no client codegen). It does not talk to the OS: the
/// token comes from the caller (`spooky_push` in Flutter, or any other
/// source), so this stays pure Dart.
///
/// Mirrors `db.webPush` in `@spooky-sync/core` minus the browser parts.

/// How a device is reached (`_00_push_subscription.kind`).
enum PushKind { web, apns, fcm }

enum PushPlatform { web, ios, android, macos }

/// Which of Apple's hosts an APNs token belongs to. Development builds get
/// sandbox tokens, TestFlight and the App Store production ones.
enum ApnsEnvironment { sandbox, production }

enum PushPermission {
  granted,

  /// iOS "deliver quietly": delivered to the notification center only.
  provisional,
  denied,
  notDetermined;

  bool get allowsDelivery =>
      this == PushPermission.granted || this == PushPermission.provisional;
}

T? _enumByName<T extends Enum>(List<T> values, Object? name) {
  for (final v in values) {
    if (v.name == name) return v;
  }
  return null;
}

/// What the OS gave this install.
@immutable
class PushToken {
  const PushToken({
    required this.kind,
    required this.token,
    required this.appId,
    this.platform,
    this.environment,
  });

  final PushKind kind;
  final String token;

  /// Bundle id / package name: the APNs topic.
  final String appId;
  final PushPlatform? platform;

  /// APNs only; the server assumes production when absent.
  final ApnsEnvironment? environment;

  /// The endpoint the server stores for this device (`<kind>:<token>`).
  String get endpoint =>
      '${kind.name}:${kind == PushKind.apns ? token.toLowerCase() : token}';

  Map<String, Object?> toJson() => {
        'kind': kind.name,
        'token': token,
        'appId': appId,
        if (platform != null) 'platform': platform!.name,
        if (kind == PushKind.apns && environment != null)
          'environment': environment!.name,
      };

  @override
  bool operator ==(Object other) =>
      other is PushToken &&
      other.kind == kind &&
      other.token == token &&
      other.appId == appId &&
      other.platform == platform &&
      other.environment == environment;

  @override
  int get hashCode => Object.hash(kind, token, appId, platform, environment);
}

/// The Firebase client config an Android app initialises FCM with, served
/// by `fn::push::info()` so the app ships no google-services.json.
@immutable
class FirebaseClientConfig {
  const FirebaseClientConfig({
    required this.projectId,
    required this.appId,
    required this.apiKey,
    required this.senderId,
  });

  final String projectId;
  final String appId;
  final String apiKey;
  final String senderId;

  static FirebaseClientConfig? fromJson(Object? v) {
    if (v is! Map) return null;
    final p = v['projectId'],
        a = v['appId'],
        k = v['apiKey'],
        s = v['senderId'];
    if (p is! String || a is! String || k is! String || s == null) return null;
    return FirebaseClientConfig(
        projectId: p, appId: a, apiKey: k, senderId: s.toString());
  }

  Map<String, String> toJson() => {
        'projectId': projectId,
        'appId': appId,
        'apiKey': apiKey,
        'senderId': senderId,
      };
}

/// `fn::push::info()`.
@immutable
class PushInfo {
  const PushInfo({
    this.webEnabled = false,
    this.publicKey,
    this.kid,
    this.providers = const {},
    this.android,
  });

  /// A host published a VAPID key (browsers can subscribe).
  final bool webEnabled;
  final String? publicKey;
  final String? kid;

  /// Every kind of device the project can reach.
  final Set<PushKind> providers;
  final FirebaseClientConfig? android;

  bool supports(PushKind kind) => providers.contains(kind);

  factory PushInfo.fromJson(Object? v) {
    final m = v is Map ? v : const {};
    final providers = <PushKind>{
      for (final p in (m['providers'] as List? ?? const []))
        if (_enumByName(PushKind.values, p) case final k?) k,
    };
    return PushInfo(
      webEnabled: m['enabled'] == true,
      publicKey: m['publicKey'] as String?,
      kid: m['kid'] as String?,
      providers: providers,
      android: FirebaseClientConfig.fromJson(m['android']),
    );
  }
}

DateTime? _date(Object? v) => v is String ? DateTime.tryParse(v) : null;

/// One of the user's devices (`fn::push::list()`), without its keys.
@immutable
class PushDevice {
  const PushDevice({
    required this.id,
    required this.endpoint,
    this.kind = PushKind.web,
    this.platform,
    this.appId,
    this.environment,
    this.label,
    this.userAgent,
    this.rules,
    this.meta,
    this.createdAt,
    this.updatedAt,
    this.lastOkAt,
    this.lastError,
    this.failures = 0,
    this.disabledAt,
    this.disabledReason,
    this.current = true,
    this.thisDevice = false,
  });

  final String id;
  final String endpoint;
  final PushKind kind;
  final PushPlatform? platform;
  final String? appId;
  final ApnsEnvironment? environment;
  final String? label;
  final String? userAgent;

  /// Rule names this device wants; `null` = every rule.
  final List<String>? rules;
  final Map<String, Object?>? meta;
  final DateTime? createdAt;
  final DateTime? updatedAt;
  final DateTime? lastOkAt;
  final String? lastError;
  final int failures;
  final DateTime? disabledAt;
  final String? disabledReason;

  /// `false`: a web row made under an older VAPID key.
  final bool current;

  /// The device this client registered.
  final bool thisDevice;

  bool get disabled => disabledAt != null;

  factory PushDevice.fromJson(Map<dynamic, dynamic> m, {String? thisEndpoint}) {
    final endpoint = (m['endpoint'] ?? '').toString();
    return PushDevice(
      id: (m['id'] ?? '').toString(),
      endpoint: endpoint,
      kind: _enumByName(PushKind.values, m['kind']) ?? PushKind.web,
      platform: _enumByName(PushPlatform.values, m['platform']),
      appId: m['app_id'] as String?,
      environment: _enumByName(ApnsEnvironment.values, m['environment']),
      label: m['label'] as String?,
      userAgent: m['user_agent'] as String?,
      rules: (m['rules'] as List?)?.map((e) => e.toString()).toList(),
      meta: (m['meta'] as Map?)?.cast<String, Object?>(),
      createdAt: _date(m['created_at']),
      updatedAt: _date(m['updated_at']),
      lastOkAt: _date(m['last_ok_at']),
      lastError: m['last_error'] as String?,
      failures: (m['failures'] as num?)?.toInt() ?? 0,
      disabledAt: _date(m['disabled_at']),
      disabledReason: m['disabled_reason'] as String?,
      current: m['current'] != false,
      thisDevice: thisEndpoint != null && endpoint == thisEndpoint,
    );
  }
}

/// A push to yourself (`fn::push::notify`): a reminder, a test.
@immutable
class PushMessageInput {
  const PushMessageInput({
    this.notification,
    this.native,
    this.data,
    this.topic,
    this.urgency,
    this.ttl,
    this.sendAt,
  });

  final Map<String, Object?>? notification;

  /// `{ notification?, apns?, android? }`, as a rule's `native:` block.
  final Map<String, Object?>? native;
  final Map<String, Object?>? data;
  final String? topic;

  /// `very-low`, `low`, `normal` or `high`.
  final String? urgency;
  final Duration? ttl;

  /// In the future: scheduled.
  final DateTime? sendAt;

  Map<String, Object?> toJson() => {
        if (notification != null) 'notification': notification,
        if (native != null) 'native': native,
        if (data != null) 'data': data,
        if (topic != null) 'topic': topic,
        if (urgency != null) 'urgency': urgency,
        if (ttl != null) 'ttl': ttl!.inSeconds,
        if (sendAt != null) 'sendAt': sendAt!.toUtc().toIso8601String(),
      };
}

@immutable
class PushMessage {
  const PushMessage(
      {required this.id, required this.status, this.sendAt, this.createdAt});
  final String id;
  final String status;
  final DateTime? sendAt;
  final DateTime? createdAt;

  factory PushMessage.fromJson(Object? v) {
    final m = v is Map ? v : const {};
    return PushMessage(
      id: (m['id'] ?? '').toString(),
      status: (m['status'] ?? '').toString(),
      sendAt: _date(m['send_at']),
      createdAt: _date(m['created_at']),
    );
  }
}

/// What a push carries for the app (the `sp00ky` key of an APNs payload,
/// the `sp00ky` data field of an FCM message). The OS shows the title and
/// body; `notification` keeps what the app still needs (`url`, `tag`, ...).
/// No `notification`: a silent push (a nudge to refresh).
@immutable
class PushPayload {
  const PushPayload({
    required this.v,
    required this.kind,
    this.rule,
    this.message,
    this.table,
    this.id,
    this.op,
    this.topic,
    this.notification,
    this.data,
    this.ts = 0,
  });

  final int v;

  /// `rule` or `message`.
  final String kind;
  final String? rule;
  final String? message;
  final String? table;
  final String? id;
  final String? op;
  final String? topic;
  final Map<String, Object?>? notification;
  final Object? data;
  final int ts;

  bool get isNudge => notification == null;
  String? get url => notification?['url'] as String?;

  /// From a map (iOS `userInfo['sp00ky']`) or a JSON string (FCM data).
  /// `null` for anything that is not a v1 sp00ky payload.
  static PushPayload? tryParse(Object? value) {
    Object? v = value;
    if (v is String) {
      try {
        v = jsonDecode(v);
      } catch (_) {
        return null;
      }
    }
    if (v is! Map || v['v'] != 1) return null;
    final kind = v['kind'];
    if (kind != 'rule' && kind != 'message') return null;
    String? s(String k) => v is Map ? v[k] as String? : null;
    return PushPayload(
      v: 1,
      kind: kind as String,
      rule: s('rule'),
      message: s('message'),
      table: s('table'),
      id: s('id'),
      op: s('op'),
      topic: s('topic'),
      notification: (v['notification'] as Map?)?.cast<String, Object?>(),
      data: v['data'],
      ts: (v['ts'] as num?)?.toInt() ?? 0,
    );
  }
}

enum PushErrorCode {
  signedOut,
  impersonating,

  /// The project cannot reach this kind of device (no `push.apns` /
  /// `push.fcm`).
  disabled,
  notSubscribed,

  /// The OS refused notifications (raised by platform code such as
  /// `spooky_push`, never by this module).
  permissionDenied,

  /// The OS gave no device token (no Play services, no network, simulator
  /// without APNs).
  noRegistration,
  server,
}

class PushError implements Exception {
  const PushError(this.code, this.message, [this.cause]);
  final PushErrorCode code;
  final String message;
  final Object? cause;
  @override
  String toString() => 'PushError(${code.name}): $message';
}

/// What [PushModule.sync] found or did. It never throws.
enum PushSyncStatus {
  /// Registered and current.
  ok,

  /// The server had no usable row for this token; it has one now.
  registered,

  /// The token changed (rotation, sandbox vs production); the old row went.
  resubscribed,
  signedOut,
  impersonating,
  permissionDefault,
  permissionDenied,

  /// The user never enabled push on this device, or turned it off.
  notSubscribed,

  /// The project cannot reach this kind of device.
  disabled,

  /// No token (yet).
  noRegistration,
  error,
}

/// The per-account record of this device's choice, kept in the client's
/// persistence under [pushEndpointKey].
@immutable
class PushRecord {
  const PushRecord(
      {required this.userId, required this.enabled, this.endpoint, this.at});
  final String userId;

  /// `false`: the user turned push off here; nothing re-enables it but them.
  final bool enabled;
  final String? endpoint;
  final DateTime? at;

  Map<String, Object?> toJson() => {
        'userId': userId,
        'enabled': enabled,
        if (endpoint != null) 'endpoint': endpoint,
        'at': (at ?? DateTime.now()).toUtc().toIso8601String(),
      };

  static PushRecord? fromJson(Object? v) {
    if (v is String) {
      try {
        v = jsonDecode(v);
      } catch (_) {
        return null;
      }
    }
    if (v is! Map || v['userId'] is! String) return null;
    return PushRecord(
      userId: v['userId'] as String,
      enabled: v['enabled'] == true,
      endpoint: v['endpoint'] as String?,
      at: _date(v['at']),
    );
  }
}

const pushEndpointKey = 'sp00ky_push_endpoint';

// ── The sync decision ────────────────────────────────────────────────────

/// Everything [decidePushSync] looks at. `info` and `devices` are `null`
/// until fetched; the first call tells whether they are needed.
@immutable
class PushSyncInput {
  const PushSyncInput({
    required this.userId,
    required this.impersonating,
    required this.permission,
    required this.token,
    required this.record,
    this.autoRegister = false,
    this.info,
    this.devices,
  });

  final String? userId;
  final bool impersonating;
  final PushPermission permission;
  final PushToken? token;

  /// The stored record, only if it belongs to [userId].
  final PushRecord? record;
  final bool autoRegister;
  final PushInfo? info;
  final List<PushDevice>? devices;
}

sealed class PushSyncDecision {
  const PushSyncDecision(this.status);
  final PushSyncStatus status;
}

/// Nothing to do but report.
final class PushSyncDone extends PushSyncDecision {
  const PushSyncDone(super.status);
}

/// Ask the server (`info`, `list`) and decide again.
final class PushSyncNeedsServer extends PushSyncDecision {
  const PushSyncNeedsServer() : super(PushSyncStatus.ok);
}

/// Permission gone: remove these rows, keep the user's choice so the device
/// comes back when the permission does.
final class PushSyncDropRows extends PushSyncDecision {
  const PushSyncDropRows(super.status, this.endpoints);
  final List<String> endpoints;
}

/// (Re-)register [token], carrying label, rules and meta over, then remove
/// [remove] (the previous endpoint) if set.
final class PushSyncRegister extends PushSyncDecision {
  const PushSyncRegister(super.status, this.token,
      {this.label, this.rules, this.meta, this.remove});
  final PushToken token;
  final String? label;
  final List<String>? rules;
  final Map<String, Object?>? meta;
  final String? remove;
}

/// What `sync` does, as a function of state only.
PushSyncDecision decidePushSync(PushSyncInput i) {
  if (i.userId == null) return const PushSyncDone(PushSyncStatus.signedOut);
  if (i.impersonating) return const PushSyncDone(PushSyncStatus.impersonating);
  final record = i.record;
  final optedIn = record?.enabled == true;
  final optedOut = record?.enabled == false;
  if (!i.permission.allowsDelivery) {
    final status = i.permission == PushPermission.denied
        ? PushSyncStatus.permissionDenied
        : PushSyncStatus.permissionDefault;
    final endpoints = {
      if (record?.endpoint != null) record!.endpoint!,
      if (i.token != null) i.token!.endpoint,
    }.toList();
    return optedIn && endpoints.isNotEmpty
        ? PushSyncDropRows(status, endpoints)
        : PushSyncDone(status);
  }
  final token = i.token;
  if (token == null) return const PushSyncDone(PushSyncStatus.noRegistration);
  if (optedOut || (!optedIn && !i.autoRegister)) {
    return const PushSyncDone(PushSyncStatus.notSubscribed);
  }
  final info = i.info, devices = i.devices;
  if (info == null || devices == null) return const PushSyncNeedsServer();
  if (!info.supports(token.kind))
    return const PushSyncDone(PushSyncStatus.disabled);

  PushDevice? find(String? e) {
    if (e == null) return null;
    for (final d in devices) {
      if (d.endpoint == e) return d;
    }
    return null;
  }

  final row = find(token.endpoint);
  final previousEndpoint =
      record?.endpoint != null && record!.endpoint != token.endpoint
          ? record.endpoint
          : null;
  final expectedEnv = token.kind == PushKind.apns
      ? (token.environment ?? ApnsEnvironment.production)
      : null;
  final healthy = row != null &&
      !row.disabled &&
      row.current &&
      row.appId == token.appId &&
      row.environment == expectedEnv;
  if (healthy && previousEndpoint == null) {
    return const PushSyncDone(PushSyncStatus.ok);
  }
  final carry = row ?? find(previousEndpoint);
  return PushSyncRegister(
    previousEndpoint != null
        ? PushSyncStatus.resubscribed
        : PushSyncStatus.registered,
    token,
    label: carry?.label,
    rules: carry?.rules,
    meta: carry?.meta,
    remove: previousEndpoint,
  );
}

// ── The module ───────────────────────────────────────────────────────────

typedef RemoteQuery = Future<List<dynamic>> Function(String sql,
    [Map<String, dynamic>? vars]);

class PushModule {
  PushModule({
    required RemoteQuery remote,
    required Sp00kyAuth auth,
    required PersistenceClient storage,
    required SpookyLogger logger,
  })  : _remote = remote,
        _auth = auth,
        _storage = storage,
        _logger = logger.child('PushModule');

  final RemoteQuery _remote;
  final Sp00kyAuth _auth;
  final PersistenceClient _storage;
  final SpookyLogger _logger;

  /// Remove this device's row when the user signs out (the choice itself is
  /// kept, so it comes back on their next sign-in).
  bool unsubscribeOnSignOut = true;
  static const signOutTimeout = Duration(milliseconds: 1500);

  PushInfo? _info;
  PushToken? _lastToken;
  void Function()? _offSignOut;
  Future<void> _chain = Future.value();

  /// Registers the sign-out hook. Idempotent.
  void attach() {
    _offSignOut ??= _auth.onBeforeSignOut(_onSignOut);
  }

  void dispose() {
    _offSignOut?.call();
    _offSignOut = null;
  }

  /// One at a time: an auto-sync must never race an explicit enable.
  Future<T> _serial<T>(Future<T> Function() op) {
    final result = _chain.then((_) => op());
    _chain = result.then((_) {}, onError: (_) {});
    return result;
  }

  String? get _userId => _auth.currentUser?['id']?.toString();

  Future<List<dynamic>> _call(String sql, [Map<String, dynamic>? vars]) async {
    try {
      return await _remote(sql, vars);
    } catch (e) {
      throw PushError(PushErrorCode.server, '$e', e);
    }
  }

  Future<Object?> _one(String sql, [Map<String, dynamic>? vars]) async {
    final r = await _call(sql, vars);
    return r.isEmpty ? null : r.last;
  }

  Future<PushRecord?> _record(String userId) async {
    final r = PushRecord.fromJson(await _storage.get<Object>(pushEndpointKey));
    return r?.userId == userId ? r : null;
  }

  Future<void> _saveRecord(PushRecord r) =>
      _storage.set(pushEndpointKey, jsonEncode(r.toJson()));

  Future<String?> _thisEndpoint(String? userId) async {
    if (_lastToken != null) return _lastToken!.endpoint;
    if (userId == null) return null;
    return (await _record(userId))?.endpoint;
  }

  void _requireUser() {
    if (_userId == null) {
      throw const PushError(PushErrorCode.signedOut, 'sign in first');
    }
    if (_auth.isImpersonating) {
      throw const PushError(
          PushErrorCode.impersonating, 'not available while impersonating');
    }
  }

  /// `fn::push::info()`, cached; a failed read is not.
  Future<PushInfo> info({bool refresh = false}) async {
    final cached = _info;
    if (cached != null && !refresh) return cached;
    return _info = PushInfo.fromJson(await _one('RETURN fn::push::info();'));
  }

  /// This device has a stored, enabled registration for the signed-in user.
  /// Local only.
  Future<bool> isRegistered() async {
    final userId = _userId;
    if (userId == null) return false;
    final r = await _record(userId);
    return r != null && r.enabled && r.endpoint != null;
  }

  /// Register this device for the signed-in user. Remembers the choice.
  Future<PushDevice> register(PushToken token,
          {String? label,
          List<String>? rules,
          Map<String, Object?>? meta,
          String? userAgent}) =>
      _serial(() => _register(token,
          label: label, rules: rules, meta: meta, userAgent: userAgent));

  Future<PushDevice> _register(PushToken token,
      {String? label,
      List<String>? rules,
      Map<String, Object?>? meta,
      String? userAgent,
      String? remove}) async {
    _requireUser();
    final userId = _userId!;
    final info = await this.info(refresh: true);
    if (!info.supports(token.kind)) {
      throw PushError(PushErrorCode.disabled,
          'the project cannot reach ${token.kind.name} devices');
    }
    final previous = remove ?? (await _record(userId))?.endpoint;
    final row = await _one(r'RETURN fn::push::register($device, $opts);', {
      'device': token.toJson(),
      'opts': {
        if (label != null) 'label': label,
        if (rules != null && rules.isNotEmpty) 'rules': rules,
        if (meta != null) 'meta': meta,
        if (userAgent != null) 'userAgent': userAgent,
      },
    });
    _lastToken = token;
    // The new row first: a failure here leaves the old one working.
    if (previous != null && previous != token.endpoint) {
      try {
        await _call(r'RETURN fn::push::unsubscribe($e);', {'e': previous});
      } catch (e) {
        _logger.debug('Removing the previous push endpoint failed: $e');
      }
    }
    await _saveRecord(
        PushRecord(userId: userId, enabled: true, endpoint: token.endpoint));
    return PushDevice.fromJson(row is Map ? row : const {},
        thisEndpoint: token.endpoint);
  }

  /// Turn push off on this device (`all`: on every device of the user).
  /// Remembered, so no automatic sync turns it back on.
  Future<int> unsubscribe({bool all = false}) => _serial(() async {
        final userId = _userId;
        if (userId == null) {
          throw const PushError(PushErrorCode.signedOut, 'sign in first');
        }
        var removed = 0;
        final endpoint = await _thisEndpoint(userId);
        // An impersonating session cannot touch the rows; the choice is
        // still recorded.
        if (!_auth.isImpersonating && (all || endpoint != null)) {
          // Dart null travels as NULL, which `option<string>` refuses: spell
          // "every device" as NONE.
          final n = all
              ? await _one('RETURN fn::push::unsubscribe(NONE);')
              : await _one(
                  r'RETURN fn::push::unsubscribe($e);', {'e': endpoint});
          removed = (n as num?)?.toInt() ?? 0;
        }
        await _saveRecord(PushRecord(userId: userId, enabled: false));
        return removed;
      });

  /// Bring the server in line with this device: register after a token
  /// rotation, an APNs environment switch, a disabled row, or a permission
  /// that came back; remove the row when the permission went. Never throws.
  Future<PushSyncStatus> sync(PushToken? token,
          {PushPermission permission = PushPermission.granted,
          bool autoRegister = false,
          String? userAgent}) =>
      _serial(() async {
        try {
          final userId = _userId;
          if (token != null) _lastToken = token;
          var input = PushSyncInput(
            userId: userId,
            impersonating: _auth.isImpersonating,
            permission: permission,
            token: token,
            record: userId == null ? null : await _record(userId),
            autoRegister: autoRegister,
          );
          var decision = decidePushSync(input);
          if (decision is PushSyncNeedsServer) {
            final info = await this.info(refresh: true);
            final list = await _one('RETURN fn::push::list();');
            input = PushSyncInput(
              userId: input.userId,
              impersonating: input.impersonating,
              permission: input.permission,
              token: input.token,
              record: input.record,
              autoRegister: input.autoRegister,
              info: info,
              devices: [
                for (final row in (list as List? ?? const []))
                  if (row is Map) PushDevice.fromJson(row),
              ],
            );
            decision = decidePushSync(input);
          }
          switch (decision) {
            case PushSyncDone():
            case PushSyncNeedsServer():
              if (decision.status == PushSyncStatus.ok &&
                  userId != null &&
                  input.record?.endpoint != token?.endpoint) {
                await _saveRecord(PushRecord(
                    userId: userId, enabled: true, endpoint: token?.endpoint));
              }
            case PushSyncDropRows(:final endpoints):
              for (final e in endpoints) {
                await _call(r'RETURN fn::push::unsubscribe($e);', {'e': e});
              }
            case PushSyncRegister(
                :final token,
                :final label,
                :final rules,
                :final meta,
                :final remove
              ):
              await _register(token,
                  label: label,
                  rules: rules,
                  meta: meta,
                  userAgent: userAgent,
                  remove: remove);
          }
          return decision.status;
        } catch (e) {
          _logger.debug('push sync failed: $e');
          return PushSyncStatus.error;
        }
      });

  /// Change this device's label / rules (`[]` = every rule again) / meta.
  Future<PushDevice?> update(
      {String? label, List<String>? rules, Map<String, Object?>? meta}) async {
    _requireUser();
    final endpoint = await _thisEndpoint(_userId);
    if (endpoint == null) {
      throw const PushError(
          PushErrorCode.notSubscribed, 'this device is not registered');
    }
    final rows = await _one(r'RETURN fn::push::update($e, $o);', {
      'e': endpoint,
      'o': {
        if (label != null) 'label': label,
        if (rules != null) 'rules': rules,
        if (meta != null) 'meta': meta,
      },
    });
    final first = rows is List && rows.isNotEmpty ? rows.first : null;
    return first is Map
        ? PushDevice.fromJson(first, thisEndpoint: endpoint)
        : null;
  }

  /// Every device of the signed-in user (web and native).
  Future<List<PushDevice>> devices() async {
    final userId = _userId;
    if (userId == null) return const [];
    final endpoint = await _thisEndpoint(userId);
    final rows = await _one('RETURN fn::push::list();');
    return [
      for (final row in (rows as List? ?? const []))
        if (row is Map) PushDevice.fromJson(row, thisEndpoint: endpoint),
    ];
  }

  /// A push to yourself, now or at [PushMessageInput.sendAt].
  Future<PushMessage> notify(PushMessageInput message) async {
    _requireUser();
    return PushMessage.fromJson(
        await _one(r'RETURN fn::push::notify($m);', {'m': message.toJson()}));
  }

  /// Cancel one of your scheduled pushes.
  Future<bool> cancel(String id) async {
    _requireUser();
    return await _one(
            r'RETURN fn::push::cancel($id);', {'id': RecordId.parse(id)}) ==
        true;
  }

  /// A visible test push to every device of the user.
  Future<PushMessage> test({String? title, String? body}) async {
    _requireUser();
    return PushMessage.fromJson(await _one(r'RETURN fn::push::test($o);', {
      'o': {if (title != null) 'title': title, if (body != null) 'body': body},
    }));
  }

  Future<void> _onSignOut(SignOutContext ctx) async {
    if (!unsubscribeOnSignOut || ctx.impersonating || ctx.userId == null)
      return;
    try {
      final endpoint = await _thisEndpoint(ctx.userId);
      if (endpoint == null) return;
      await _remote(r'RETURN fn::push::unsubscribe($e);', {'e': endpoint})
          .timeout(signOutTimeout);
    } catch (e) {
      // Best effort: the next owner of this device (newest owner wins) or the
      // push service (token gone) cleans up otherwise.
      _logger.debug('push unsubscribe on sign-out failed: $e');
    }
  }
}
