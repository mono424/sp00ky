import 'dart:convert';

import '../../modules/ref_tables.dart' show anonUserId, bucketIdForUser;
import '../../types.dart';
import '../database/local_database_service.dart';
import '../logger/logger.dart';
import 'sqlite_persistence.dart';

/// Session routing must outlive account-store switches. One atomic metadata row
/// is authoritative even when signed out, so legacy credentials cannot return.
class SessionPersistence extends PersistenceClient {
  SessionPersistence._(this._metadata, this._account);
  final LocalDatabaseService _metadata;
  final SqlitePersistenceClient _account;
  static const _key = 'session_v1';
  static const _legacyMarker = 'sp00ky_session_migrated';
  static const tokenKey = 'sp00ky_auth_token';
  static const hintKey = 'sp00ky_boot_bucket';

  static String bucketPath(String base, String bucket) {
    final dot = base.lastIndexOf('.');
    return dot <= 0
        ? '$base.$bucket'
        : '${base.substring(0, dot)}.$bucket${base.substring(dot)}';
  }

  static SessionPersistence open(DatabaseConfig config, SpookyLogger logger,
      LocalDatabaseService Function() account) {
    final base = config.localDbPath ?? 'spooky.db';
    final metadata = LocalDatabaseService.open(logger,
        store: config.store, path: bucketPath(base, 'session'));
    metadata.provision();
    final result =
        SessionPersistence._(metadata, SqlitePersistenceClient(account));
    if (metadata.kvGet(_key) == null) {
      String? token;
      if (config.store != StoreType.memory) {
        // Only consult the legacy locator once. Never scan other accounts.
        final anon = LocalDatabaseService.open(logger,
            store: config.store, path: bucketPath(base, anonUserId));
        anon.provision();
        String? hint;
        try {
          if (anon.kvGet(_legacyMarker) != 'true') {
            hint = _decodeString(anon.kvGet(hintKey));
            token = _decodeString(anon.kvGet(tokenKey));
          }
        } finally {
          anon.close();
        }
        if (hint != null &&
            RegExp(r'^[A-Za-z0-9_-]+$').hasMatch(hint) &&
            hint != anonUserId) {
          final old = LocalDatabaseService.open(logger,
              store: config.store, path: bucketPath(base, hint));
          old.provision();
          try {
            token = _decodeString(old.kvGet(tokenKey)) ?? token;
          } finally {
            old.close();
          }
        }
      }
      final user = userIdFromToken(token);
      result._write(user == null ? null : token, bucketIdForUser(user));
    }
    if (config.store != StoreType.memory) {
      // Commit the authoritative row first. The legacy marker additionally
      // prevents credential resurrection if the metadata file is later lost.
      final anon = LocalDatabaseService.open(logger,
          store: config.store, path: bucketPath(base, anonUserId));
      try {
        anon.provision();
        if (anon.kvGet(_legacyMarker) != 'true')
          anon.kvSet(_legacyMarker, 'true');
      } finally {
        anon.close();
      }
    }
    return result;
  }

  static String? _decodeString(String? raw) {
    try {
      final value = raw == null ? null : jsonDecode(raw);
      return value is String ? value : null;
    } catch (_) {
      return null;
    }
  }

  static String? userIdFromToken(String? token) {
    try {
      final part = token!.split('.')[1];
      final claims =
          jsonDecode(utf8.decode(base64Url.decode(base64Url.normalize(part))));
      final id = claims['ID'] ?? claims['id'];
      return id is String && id.contains(':') ? id : null;
    } catch (_) {
      return null;
    }
  }

  Map<String, dynamic> _read() {
    try {
      final row = jsonDecode(_metadata.kvGet(_key)!) as Map<String, dynamic>;
      final token = row['token'];
      if (token == null) return {'token': null, 'bucket': anonUserId};
      if (token is! String || userIdFromToken(token) == null) {
        throw const FormatException('Invalid saved session');
      }
      // The token claim, never a mutable hint, selects the account cache.
      return {
        'token': token,
        'bucket': bucketIdForUser(userIdFromToken(token))
      };
    } catch (_) {
      // A corrupt metadata row is signed out, never a reason to revive legacy auth.
      return {'token': null, 'bucket': anonUserId};
    }
  }

  void _write(String? token, String bucket) =>
      _metadata.kvSet(_key, jsonEncode({'token': token, 'bucket': bucket}));

  @override
  Future<T?> get<T>(String key) async {
    if (key == tokenKey) return _read()['token'] as T?;
    if (key == hintKey) return (_read()['bucket'] ?? anonUserId) as T?;
    return _account.get<T>(key);
  }

  @override
  Future<void> set(String key, dynamic value) async {
    if (key == tokenKey) {
      final token = value as String;
      final user = userIdFromToken(token);
      _write(token,
          user == null ? _read()['bucket'] as String : bucketIdForUser(user));
    } else if (key == hintKey) {
      _write(_read()['token'] as String?, value as String);
    } else {
      await _account.set(key, value);
    }
  }

  @override
  Future<void> remove(String key) async {
    if (key == tokenKey || key == hintKey) {
      _write(null, anonUserId);
    } else {
      await _account.remove(key);
    }
  }

  void close() => _metadata.close();
}
