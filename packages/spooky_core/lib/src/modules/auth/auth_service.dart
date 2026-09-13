import 'dart:convert';
import 'sp00ky_auth.dart';
import '../../utils/error_classification.dart';

import '../../events/event_system.dart';
import '../../services/database/remote_database_service.dart';
import '../../services/logger/logger.dart';
import '../../types.dart';
import '../../utils/parser.dart' show ColumnSchema;

/// Auth event type name (TS `AuthEventTypes`).
abstract final class AuthEventTypes {
  static const authStateChanged = 'AUTH_STATE_CHANGED';
}

EventSystem createAuthEventSystem() =>
    EventSystem([AuthEventTypes.authStateChanged]);

/// Auth state management (TS `AuthService`). The TS conditional access-param
/// types collapse to runtime `Map` validation against `schema['access']`.
class AuthService implements Sp00kyAuth {
  AuthService(
      this._schema, this._remote, this._persistence, SpookyLogger logger)
      : _logger = logger.child('AuthService');

  final Map<String, dynamic> _schema;
  final RemoteDatabaseService _remote;
  final PersistenceClient _persistence;
  final SpookyLogger _logger;
  final EventSystem _events = createAuthEventSystem();

  static const _tokenKey = 'sp00ky_auth_token';

  /// Run before the session is dropped, so a write the user already made can
  /// still reach the server under their own identity.
  ///
  /// Sign-out flips the bucket, and a bucket switch abandons the outgoing
  /// outbox in the old store: it is only picked up again the next time that
  /// user signs in, long after the screen that made the write is gone. Pushing
  /// after the token is cleared is not an option either - the statements would
  /// run unauthenticated and come back as rejections, which roll the writes
  /// back. So the flush has to happen here, first, and it is best-effort: a
  /// failure must never block signing out.
  Future<void> Function()? onBeforeSignOut;
  Future<void> Function(String?)? onSessionChanged;
  int _generation = 0;
  Future<void>? _checking;
  bool needsVerification = false;
  AuthVerificationError? _verificationError;
  AuthVerificationError? get verificationError => _verificationError;

  void dispose() {
    _generation++;
    onSessionChanged = null;
    onBeforeSignOut = null;
  }

  Future<void> publishSession() async {
    final generation = _generation;
    await onSessionChanged?.call(currentUser?['id']?.toString());
    if (generation == _generation) _notifyListeners();
  }

  String? _token;
  String? get token => _token;
  Map<String, dynamic>? _currentUser;
  Map<String, dynamic>? get currentUser => _currentUser;
  bool _isAuthenticated = false;
  bool get isAuthenticated => _isAuthenticated;
  bool _isLoading = true;
  bool get isLoading => _isLoading;

  /// The record-access method the session was opened with (TS `auth.access`),
  /// e.g. "account". Needed for SSP permission injection: a table permission
  /// written against `$access` cannot resolve locally without it.
  ///
  /// Set on signIn/signUp, and recovered from the token's `AC` claim on
  /// [check] so it survives a restart (where no signIn call happens).
  String? _access;
  String? get access => _access;

  EventSystem get eventSystem => _events;

  Future<void> init() => check();

  Map<String, dynamic>? getAccessDefinition(String name) {
    final access = _schema['access'];
    if (access is Map) return (access[name] as Map?)?.cast<String, dynamic>();
    return null;
  }

  /// Subscribe to auth state. Fires immediately with the current user id.
  void Function() subscribe(void Function(String? userId) cb) {
    cb(currentUser?['id']?.toString());
    final id = _events.subscribe(
        AuthEventTypes.authStateChanged, (e) => cb(e.payload as String?));
    return () => _events.unsubscribe(id);
  }

  void _notifyListeners() {
    _events.emit(
        AuthEventTypes.authStateChanged, currentUser?['id']?.toString());
  }

  /// Read the `AC` (access) claim out of a SurrealDB JWT without verifying it.
  /// Verification is the server's job; this only recovers which access method
  /// the existing session used so [access] survives a restart.
  static String? _accessFromToken(String? jwt) => _claimsFromToken(jwt).access;

  /// Restore a session from the cached token WITHOUT a round trip, returning
  /// the user id it names.
  ///
  /// The server still enforces the token on every request; this only lets the
  /// client act on what it already holds so a warm boot paints as the right
  /// user before the network answers. `check()` replaces the user row wholesale
  /// once the server does answer.
  Future<String?> restoreSessionFromToken({bool notify = true}) async {
    _isLoading = false;
    final tok = await _persistence.get<String>(_tokenKey);
    if (tok == null) return null;
    final claims = _claimsFromToken(tok);
    final userId = claims.userId;
    if (userId == null) return null;

    _token = tok;
    needsVerification = true;
    // Hand it to the transport too, so a socket rebuilt from scratch later (the
    // supervisor's revive loop) comes back authenticated. Without this the
    // client kept reporting this user while its session was anonymous, and
    // every view registered afterwards was stamped with an empty identity.
    _remote.setAuthToken(tok);
    // Only the id: the full row is not in the token. It lands from the local
    // cache when the app's own `user` query paints.
    _currentUser = Map.unmodifiable({'id': userId});
    _isAuthenticated = true;
    _access = claims.access;
    _isLoading = false;
    if (notify) await publishSession();
    _logger.debug('Session restored optimistically from the cached token');
    return userId;
  }

  /// The `AC` (access method) and `ID` (the auth record id) claims of a SurrealDB
  /// record-access JWT, read WITHOUT verifying it. Nulls on malformed input.
  static ({String? access, String? userId}) _claimsFromToken(String? jwt) {
    if (jwt == null) return (access: null, userId: null);
    final parts = jwt.split('.');
    if (parts.length < 2) return (access: null, userId: null);
    try {
      var payload = parts[1].replaceAll('-', '+').replaceAll('_', '/');
      payload = payload.padRight((payload.length + 3) ~/ 4 * 4, '=');
      final decoded = jsonDecode(utf8.decode(base64.decode(payload)));
      if (decoded is! Map) return (access: null, userId: null);
      final ac = decoded['AC'] ?? decoded['ac'];
      final id = decoded['ID'] ?? decoded['id'];
      return (
        access: ac is String ? ac : null,
        userId: id is String ? id : null,
      );
    } catch (_) {
      // A malformed token is not worth failing auth over.
      return (access: null, userId: null);
    }
  }

  /// Validate an existing or supplied token and hydrate the user.
  Future<void> check([String? accessToken]) {
    if (accessToken != null) return _check(accessToken);
    return _checking ??= _check(null).whenComplete(() => _checking = null);
  }

  Future<void> _check(String? accessToken) async {
    final generation = _generation;
    final previousToken = token;
    _isLoading = true;
    try {
      final tok = accessToken ?? await _persistence.get<String>(_tokenKey);
      if (generation != _generation) return;
      if (tok == null) {
        needsVerification = false;
        return;
      }
      await _remote.authenticate(tok);
      if (generation != _generation) return;
      final user = await _fetchAuthUser();
      if (generation != _generation) return;
      if (user != null && user['id'] != null) {
        await _setSession(tok, user);
        if (generation == _generation) {
          needsVerification = false;
          _verificationError = null;
        }
      } else {
        throw StateError('Invalid authentication: account not found');
      }
    } catch (error, stack) {
      if (generation != _generation) return;
      _verificationError = AuthVerificationError(
          classifySyncError(error), error.toString(), stack.toString());
      final message = error.toString().toLowerCase();
      final rejected = message.contains('invalid authentication') ||
          message.contains('invalid token') ||
          message.contains('token expired') ||
          message.contains('authentication failed') ||
          message.contains('problem with authentication');
      if (rejected) {
        await _signOut(flush: false);
      } else {
        needsVerification = previousToken != null;
        _logger.warn('Auth verification deferred: $error');
      }
      // A newly supplied credential must never appear to succeed offline.
      if (accessToken != null ||
          previousToken == null ||
          classifySyncError(error) != 'network') rethrow;
    } finally {
      if (generation == _generation) {
        _isLoading = false;
        _notifyListeners();
      }
    }
  }

  Future<Map<String, dynamic>?> _fetchAuthUser() async {
    final result = await _remote.queryAuthUser();
    final first = result.isNotEmpty ? result.first : null;
    if (first is List)
      return first.isNotEmpty
          ? (first.first as Map?)?.cast<String, dynamic>()
          : null;
    if (first is Map) return first.cast<String, dynamic>();
    return null;
  }

  Future<void> signOut() => _signOut();

  Future<void> _signOut({bool flush = true}) async {
    final generation = ++_generation;
    _isLoading = false;
    try {
      if (flush) await onBeforeSignOut?.call();
    } catch (err) {
      _logger.debug('Outbox flush before signOut failed: $err');
    }
    if (generation != _generation) return;
    final transition = _remote.beginSessionTransition();
    try {
      needsVerification = false;
      _verificationError = null;
      _token = null;
      _currentUser = null;
      _isAuthenticated = false;
      _access = null;
      _remote.setAuthToken(null);
      await _persistence.remove(_tokenKey);
      await publishSession();
      try {
        await _remote.invalidate().timeout(const Duration(seconds: 2));
      } catch (err) {
        // Local sign-out already cleared the token/session above; a failed remote
        // invalidate (e.g. server unreachable) must not block signing out.
        _logger.debug('Remote token invalidate failed during signOut: $err');
      }
    } finally {
      _remote.endSessionTransition(transition);
    }
  }

  Future<void> _setSession(String token, Map<String, dynamic> user) async {
    _remote.setAuthToken(token);
    _token = token;
    _currentUser = Map.unmodifiable(user);
    _isAuthenticated = true;
    // The token is authoritative for the access method, and is the only source
    // on a restored session (no signIn call ran). Keep any explicitly-set value
    // as the fallback for tokens without an AC claim.
    _access = _accessFromToken(token) ?? access;
    await _persistence.set(_tokenKey, token);
    // _notifyListeners is LAST: subscribers may register $auth-gated queries
    // synchronously, and they need token/currentUser/access already in place.
    _isLoading = false;
    await publishSession();
  }

  Future<void> signUp(String accessName, Map<String, dynamic> params) async {
    _validateAccessParams(accessName, 'signup', params);
    final generation = ++_generation;
    final transition = _remote.beginSessionTransition();
    try {
      final result =
          await _remote.signup({'access': accessName, 'variables': params});
      if (generation != _generation) {
        await _remote.forceClose();
        return;
      }
      final credential = _extractAccessToken(result);
      if (credential == null)
        throw StateError('Authentication returned no token');
      await check(credential);
    } catch (_) {
      // A failed verification may follow a successful socket sign-in. Restore
      // the transport to the still-current saved session before app work runs.
      await _remote.forceClose();
      rethrow;
    } finally {
      _remote.endSessionTransition(transition);
    }
  }

  Future<void> signIn(String accessName, Map<String, dynamic> params) async {
    _validateAccessParams(accessName, 'signIn', params);
    final generation = ++_generation;
    final transition = _remote.beginSessionTransition();
    try {
      final result =
          await _remote.signin({'access': accessName, 'variables': params});
      if (generation != _generation) {
        await _remote.forceClose();
        return;
      }
      final credential = _extractAccessToken(result);
      if (credential == null)
        throw StateError('Authentication returned no token');
      await check(credential);
    } catch (_) {
      // A failed verification may follow a successful socket sign-in. Restore
      // the transport to the still-current saved session before app work runs.
      await _remote.forceClose();
      rethrow;
    } finally {
      _remote.endSessionTransition(transition);
    }
  }

  void _validateAccessParams(
      String accessName, String method, Map<String, dynamic> params) {
    final def = getAccessDefinition(accessName);
    if (def == null) {
      throw StateError("Access definition '$accessName' not found");
    }
    final methodDef = (def[method] as Map?)?.cast<String, dynamic>();
    final declared =
        (methodDef?['params'] as Map?)?.cast<String, dynamic>() ?? {};
    final missing = <String>[];
    declared.forEach((name, schema) {
      final optional = schema is ColumnSchema
          ? schema.optional
          : (schema is Map && schema['optional'] == true);
      if (!optional && !params.containsKey(name)) missing.add(name);
    });
    if (missing.isNotEmpty) {
      throw StateError(
          "Missing required $method params for '$accessName': ${missing.join(', ')}");
    }
  }

  String? _extractAccessToken(dynamic result) {
    if (result is Map && result['access'] != null)
      return result['access'].toString();
    if (result is String) return result;
    return null;
  }
}
