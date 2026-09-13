import 'dart:convert';

import '../../types.dart';
import '../database/local_database_service.dart';

/// [PersistenceClient] backed by the local sqlite `_00_kv` table, so values
/// (circuit state, the auth token, the boot bucket hint) survive restarts.
///
/// Values are JSON-encoded, so arbitrary JSON-safe types round-trip; string
/// values come back as plain strings.
///
/// Resolves the store on every call rather than holding one: a bucket switch
/// replaces it, and a captured handle would keep writing into a closed
/// database.
class SqlitePersistenceClient implements PersistenceClient {
  SqlitePersistenceClient(this._resolve);

  final LocalDatabaseService Function() _resolve;

  LocalDatabaseService get _db => _resolve();

  @override
  Future<void> set(String key, dynamic value) async {
    _db.kvSet(key, jsonEncode(value));
  }

  @override
  Future<T?> get<T>(String key) async {
    final raw = _db.kvGet(key);
    if (raw == null) return null;
    return jsonDecode(raw) as T?;
  }

  @override
  Future<void> remove(String key) async {
    _db.kvRemove(key);
  }
}
