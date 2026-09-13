import 'dart:convert';

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
class AuthService {
  AuthService(
      this._schema, this._remote, this._persistence, SpookyLogger logger)
      : _logger = logger.child('AuthService');

  final Map<String, dynamic> _schema;
  final RemoteDatabaseService _remote;
  final PersistenceClient _persistence;
  final SpookyLogger _logger;
  final EventSystem _events = createAuthEventSystem();

  static const _tokenKey = 'sp00ky_auth_token';

  String? token;
  Map<String, dynamic>? currentUser;
  bool isAuthenticated = false;
  bool isLoading = true;

  /// The record-access method the session was opened with (TS `auth.access`),
  /// e.g. "account". Needed for SSP permission injection: a table permission
  /// written against `$access` cannot resolve locally without it.
  ///
  /// Set on signIn/signUp, and recovered from the token's `AC` claim on
  /// [check] so it survives a restart (where no signIn call happens).
  String? access;

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
  Future<String?> restoreSessionFromToken() async {
    final tok = await _persistence.get<String>(_tokenKey);
    if (tok == null) return null;
    final claims = _claimsFromToken(tok);
    final userId = claims.userId;
    if (userId == null) return null;

    token = tok;
    // Hand it to the transport too, so a socket rebuilt from scratch later (the
    // supervisor's revive loop) comes back authenticated. Without this the
    // client kept reporting this user while its session was anonymous, and
    // every view registered afterwards was stamped with an empty identity.
    _remote.setAuthToken(tok);
    // Only the id: the full row is not in the token. It lands from the local
    // cache when the app's own `user` query paints.
    currentUser = {'id': userId};
    isAuthenticated = true;
    access = claims.access;
    _notifyListeners();
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
  Future<void> check([String? accessToken]) async {
    isLoading = true;
    try {
      final tok = accessToken ?? await _persistence.get<String>(_tokenKey);
      if (tok == null) {
        isLoading = false;
        isAuthenticated = false;
        _notifyListeners();
        return;
      }

      await _remote.getClient().authenticate(tok);
      final user = await _fetchAuthUser();
      if (user != null && user['id'] != null) {
        await _setSession(tok, user);
      } else {
        await signOut();
      }
    } catch (error) {
      _logger.error('Auth check failed', error);
      await signOut();
    } finally {
      isLoading = false;
    }
  }

  Future<Map<String, dynamic>?> _fetchAuthUser() async {
    final result = await _remote.query('SELECT * FROM ONLY \$auth.id');
    final first = result.isNotEmpty ? result.first : null;
    if (first is List)
      return first.isNotEmpty
          ? (first.first as Map?)?.cast<String, dynamic>()
          : null;
    if (first is Map) return first.cast<String, dynamic>();
    return null;
  }

  Future<void> signOut() async {
    token = null;
    currentUser = null;
    isAuthenticated = false;
    access = null;
    _remote.setAuthToken(null);
    await _persistence.remove(_tokenKey);
    try {
      await _remote.getClient().invalidate();
    } catch (err) {
      // Local sign-out already cleared the token/session above; a failed remote
      // invalidate (e.g. server unreachable) must not block signing out.
      _logger.debug('Remote token invalidate failed during signOut: $err');
    }
    _notifyListeners();
  }

  Future<void> _setSession(String token, Map<String, dynamic> user) async {
    _remote.setAuthToken(token);
    this.token = token;
    currentUser = user;
    isAuthenticated = true;
    // The token is authoritative for the access method, and is the only source
    // on a restored session (no signIn call ran). Keep any explicitly-set value
    // as the fallback for tokens without an AC claim.
    access = _accessFromToken(token) ?? access;
    await _persistence.set(_tokenKey, token);
    // _notifyListeners is LAST: subscribers may register $auth-gated queries
    // synchronously, and they need token/currentUser/access already in place.
    _notifyListeners();
  }

  Future<void> signUp(String accessName, Map<String, dynamic> params) async {
    _validateAccessParams(accessName, 'signup', params);
    access = accessName;
    final result =
        await _remote.signup({'access': accessName, 'variables': params});
    await check(_extractAccessToken(result));
  }

  Future<void> signIn(String accessName, Map<String, dynamic> params) async {
    _validateAccessParams(accessName, 'signIn', params);
    access = accessName;
    final result =
        await _remote.signin({'access': accessName, 'variables': params});
    await check(_extractAccessToken(result));
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
