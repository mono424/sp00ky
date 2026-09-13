import 'dart:convert';

import 'package:sqlite3/sqlite3.dart';

import '../../surreal/value.dart';
import '../../types.dart';
import '../../utils/record_id_utils.dart';
import '../logger/logger.dart';

/// Encode a record document to JSON for sqlite storage. `parseParams` coerces
/// schema-typed fields to Dart objects (record links -> [RecordId], datetimes
/// -> [DateTime], durations -> [SurrealDuration]) which aren't natively
/// JSON-encodable; serialize them back to their canonical string forms so the
/// stored doc round-trips to the same string values the typed models expect.
String _encodeDoc(Map<String, dynamic> doc) =>
    jsonEncode(doc, toEncodable: _jsonEncodable);

Object? _jsonEncodable(Object? value) {
  if (value is RecordId) return encodeRecordId(value);
  if (value is DateTime) return value.toUtc().toIso8601String();
  if (value is SurrealDuration) return value.toString();
  throw ArgumentError('Cannot JSON-encode ${value.runtimeType}');
}

/// sqlite-backed local store, replacing the JS `@surrealdb/wasm` local engine.
///
/// Domain records are stored document-style as JSON in a single `records`
/// table partitioned by logical table name, so arbitrary records round-trip.
/// Dedicated tables hold query state, stream-processor state, the pending
/// mutation outbox, and the schema hash. The materialization read path resolves
/// result sets from the DBSP `localArray` via [getById] rather than executing
/// SurrealQL (sqlite cannot run SurrealQL).
class LocalDatabaseService {
  LocalDatabaseService(this._db, SpookyLogger logger)
      : _logger = logger.child('LocalDatabaseService');

  final Database _db;
  final SpookyLogger _logger;

  /// Open a database. [path] of `:memory:` matches the JS `memory` store.
  factory LocalDatabaseService.open(SpookyLogger logger,
      {StoreType store = StoreType.memory, String? path}) {
    final dbPath =
        store == StoreType.memory ? ':memory:' : (path ?? 'spooky.db');
    final db = sqlite3.open(dbPath);
    return LocalDatabaseService(db, logger);
  }

  /// Create the system tables. Idempotent.
  void provision() {
    _db.execute('''
      CREATE TABLE IF NOT EXISTS records (
        tbl TEXT NOT NULL,
        id  TEXT NOT NULL,
        doc TEXT NOT NULL,
        rv  INTEGER NOT NULL DEFAULT 1,
        PRIMARY KEY (tbl, id)
      );
      CREATE INDEX IF NOT EXISTS idx_records_tbl ON records(tbl);
      CREATE TABLE IF NOT EXISTS _00_query (
        id  TEXT PRIMARY KEY,
        doc TEXT NOT NULL
      );
      CREATE TABLE IF NOT EXISTS _00_stream_processor_state (
        id    TEXT PRIMARY KEY,
        state TEXT NOT NULL
      );
      CREATE TABLE IF NOT EXISTS _00_pending_mutations (
        id  TEXT PRIMARY KEY,
        doc TEXT NOT NULL
      );
      CREATE TABLE IF NOT EXISTS _00_schema (
        hash       TEXT PRIMARY KEY,
        created_at TEXT NOT NULL
      );
      CREATE TABLE IF NOT EXISTS _00_kv (
        id    TEXT PRIMARY KEY,
        value TEXT NOT NULL
      );
    ''');
    _logger.debug('Provisioned local sqlite store');
  }

  // ---- record CRUD (logical-table aware) ----------------------------------

  /// `SELECT * FROM ONLY $id` -> the full record, or null.
  Map<String, dynamic>? getById(String id) {
    final tbl = extractTablePart(id);
    final rs = _db
        .select('SELECT doc FROM records WHERE tbl = ? AND id = ?', [tbl, id]);
    if (rs.isEmpty) return null;
    return jsonDecode(rs.first['doc'] as String) as Map<String, dynamic>;
  }

  /// `SELECT * FROM <table>` -> all records for the logical table.
  List<Map<String, dynamic>> getAll(String table) {
    final rs = _db.select('SELECT doc FROM records WHERE tbl = ?', [table]);
    return rs
        .map((row) => jsonDecode(row['doc'] as String) as Map<String, dynamic>)
        .toList();
  }

  /// `CREATE ONLY $id CONTENT $doc`.
  void create(String id, Map<String, dynamic> doc) {
    final withId = {...doc, 'id': id};
    _writeDoc(id, withId);
  }

  /// `UPSERT ONLY $id REPLACE $doc` (rollback restore).
  void replace(String id, Map<String, dynamic> doc) {
    final withId = {...doc, 'id': id};
    _writeDoc(id, withId);
  }

  /// `UPSERT ONLY $id MERGE $patch`: deep-merge into the existing doc (creating
  /// it if absent), preserving keys the patch omits (e.g. `_00_crdt`).
  void upsertMerge(String id, Map<String, dynamic> patch) {
    final existing = getById(id) ?? <String, dynamic>{'id': id};
    final merged = _deepMerge(existing, patch);
    merged['id'] = id;
    _writeDoc(id, merged);
  }

  /// `UPDATE ONLY $id MERGE $patch`: like [upsertMerge] but a no-op if absent.
  void updateMerge(String id, Map<String, dynamic> patch) {
    final existing = getById(id);
    if (existing == null) return;
    final merged = _deepMerge(existing, patch);
    merged['id'] = id;
    _writeDoc(id, merged);
  }

  /// `UPDATE $id SET <fields>`: shallow field assignment.
  void updateSet(String id, Map<String, dynamic> fields) {
    final existing = getById(id);
    if (existing == null) return;
    final updated = {...existing, ...fields, 'id': id};
    _writeDoc(id, updated);
  }

  /// `_00_rv += 1`.
  void incrementRv(String id) {
    final existing = getById(id);
    if (existing == null) return;
    final rv = ((existing['_00_rv'] as num?) ?? 0).toInt() + 1;
    existing['_00_rv'] = rv;
    _writeDoc(id, existing);
  }

  /// `DELETE $id`.
  void delete(String id) {
    final tbl = extractTablePart(id);
    _db.execute('DELETE FROM records WHERE tbl = ? AND id = ?', [tbl, id]);
  }

  void _writeDoc(String id, Map<String, dynamic> doc) {
    final tbl = extractTablePart(id);
    final rv = ((doc['_00_rv'] as num?) ?? 1).toInt();
    _db.execute(
      'INSERT INTO records (tbl, id, doc, rv) VALUES (?, ?, ?, ?) '
      'ON CONFLICT(tbl, id) DO UPDATE SET doc = excluded.doc, rv = excluded.rv',
      [tbl, id, _encodeDoc(doc), rv],
    );
  }

  // ---- generic document access ----------------------------------------------
  //
  // The saga core addresses the store as a document store by `(table, id)`.
  // Domain rows and the internal tables that have no dedicated sqlite table
  // (`_00_view`, `_00_window`, `_00_failed_mutations`) live in `records`;
  // `_00_query` and `_00_pending_mutations` keep the dedicated tables they
  // already had, so an existing local database keeps working.

  int _epoch = 0;

  /// Bumped whenever the store this service points at is replaced. A write
  /// fenced with a stale epoch is dropped rather than landing in the wrong
  /// bucket.
  int get epoch => _epoch;

  void bumpEpoch() => _epoch++;

  bool _isQueryRegistry(String table) => table == '_00_query';
  bool _isOutbox(String table) => table == '_00_pending_mutations';

  Map<String, dynamic>? _withId(Map<String, dynamic>? doc, String id) {
    if (doc == null) return null;
    return doc['id'] == null ? {...doc, 'id': id} : doc;
  }

  Map<String, dynamic>? getDoc(String table, String id) {
    if (_isQueryRegistry(table)) return _withId(getQueryConfig(id), id);
    if (_isOutbox(table)) {
      final rs =
          _db.select('SELECT doc FROM _00_pending_mutations WHERE id = ?', [id]);
      if (rs.isEmpty) return null;
      return _withId(
          jsonDecode(rs.first['doc'] as String) as Map<String, dynamic>, id);
    }
    return getById(id);
  }

  /// Resolve many rows by id, in the order asked. Ids the store does not hold
  /// are dropped: this is the materialization read.
  List<Map<String, dynamic>> getDocs(String table, List<String> ids) {
    final out = <Map<String, dynamic>>[];
    for (final id in ids) {
      final doc = getDoc(table, id);
      if (doc != null) out.add(doc);
    }
    return out;
  }

  List<Map<String, dynamic>> getAllDocs(String table) {
    if (_isQueryRegistry(table)) return getAllQueryConfigs();
    if (_isOutbox(table)) {
      final rs = _db.select('SELECT id, doc FROM _00_pending_mutations');
      return [
        for (final row in rs)
          _withId(jsonDecode(row['doc'] as String) as Map<String, dynamic>,
              row['id'] as String)!
      ];
    }
    return getAll(table);
  }

  void putDoc(String table, String id, Map<String, dynamic> data,
      {bool merge = false}) {
    if (_isQueryRegistry(table)) {
      if (merge) {
        patchQueryConfig(id, data);
      } else {
        putQueryConfig(id, data);
      }
      return;
    }
    if (_isOutbox(table)) {
      putMutation(id, data);
      return;
    }
    if (merge) {
      upsertMerge(id, data);
    } else {
      replace(id, data);
    }
  }

  void deleteDoc(String table, String id) {
    if (_isQueryRegistry(table)) return deleteQueryConfig(id);
    if (_isOutbox(table)) return deleteMutation(id);
    return delete(id);
  }

  void bumpRv(String table, String id) => incrementRv(id);

  // ---- _00_query registry --------------------------------------------------

  Map<String, dynamic>? getQueryConfig(String id) {
    final rs = _db.select('SELECT doc FROM _00_query WHERE id = ?', [id]);
    if (rs.isEmpty) return null;
    return jsonDecode(rs.first['doc'] as String) as Map<String, dynamic>;
  }

  List<Map<String, dynamic>> getAllQueryConfigs() {
    final rs = _db.select('SELECT doc FROM _00_query');
    return rs
        .map((row) => jsonDecode(row['doc'] as String) as Map<String, dynamic>)
        .toList();
  }

  void putQueryConfig(String id, Map<String, dynamic> doc) {
    _db.execute(
      'INSERT INTO _00_query (id, doc) VALUES (?, ?) '
      'ON CONFLICT(id) DO UPDATE SET doc = excluded.doc',
      [id, _encodeDoc(doc)],
    );
  }

  void patchQueryConfig(String id, Map<String, dynamic> fields) {
    final existing = getQueryConfig(id);
    if (existing == null) return;
    putQueryConfig(id, {...existing, ...fields});
  }

  void deleteQueryConfig(String id) {
    _db.execute('DELETE FROM _00_query WHERE id = ?', [id]);
  }

  // ---- pending mutations ----------------------------------------------------

  void putMutation(String id, Map<String, dynamic> doc) {
    _db.execute(
      'INSERT INTO _00_pending_mutations (id, doc) VALUES (?, ?) '
      'ON CONFLICT(id) DO UPDATE SET doc = excluded.doc',
      [id, _encodeDoc(doc)],
    );
  }

  void deleteMutation(String id) {
    _db.execute('DELETE FROM _00_pending_mutations WHERE id = ?', [id]);
  }

  /// All pending mutations ordered by their stored `created_at` (oldest first).
  List<Map<String, dynamic>> getAllMutations() {
    final rs = _db.select('SELECT doc FROM _00_pending_mutations');
    final docs = rs
        .map((row) => jsonDecode(row['doc'] as String) as Map<String, dynamic>)
        .toList();
    docs.sort((a, b) => (a['created_at'] as String? ?? '')
        .compareTo(b['created_at'] as String? ?? ''));
    return docs;
  }

  // ---- stream processor state ----------------------------------------------

  String? getStreamState() {
    final rs = _db.select(
        "SELECT state FROM _00_stream_processor_state WHERE id = '_00_stream_processor_state'");
    if (rs.isEmpty) return null;
    return rs.first['state'] as String;
  }

  void setStreamState(String state) {
    _db.execute(
      'INSERT INTO _00_stream_processor_state (id, state) VALUES '
      "('_00_stream_processor_state', ?) "
      'ON CONFLICT(id) DO UPDATE SET state = excluded.state',
      [state],
    );
  }

  // ---- generic key-value (backs SqlitePersistenceClient) --------------------

  String? kvGet(String key) {
    final rs = _db.select('SELECT value FROM _00_kv WHERE id = ?', [key]);
    if (rs.isEmpty) return null;
    return rs.first['value'] as String;
  }

  void kvSet(String key, String value) {
    _db.execute(
      'INSERT INTO _00_kv (id, value) VALUES (?, ?) '
      'ON CONFLICT(id) DO UPDATE SET value = excluded.value',
      [key, value],
    );
  }

  void kvRemove(String key) {
    _db.execute('DELETE FROM _00_kv WHERE id = ?', [key]);
  }

  // ---- schema hash ----------------------------------------------------------

  String? latestSchemaHash() {
    final rs = _db
        .select('SELECT hash FROM _00_schema ORDER BY created_at DESC LIMIT 1');
    if (rs.isEmpty) return null;
    return rs.first['hash'] as String;
  }

  void recordSchemaHash(String hash, String createdAtIso) {
    _db.execute(
      'INSERT INTO _00_schema (hash, created_at) VALUES (?, ?) '
      'ON CONFLICT(hash) DO UPDATE SET created_at = excluded.created_at',
      [hash, createdAtIso],
    );
  }

  // ---- migration ------------------------------------------------------------

  /// Wipe cached domain data, query registrations, the outbox, and the
  /// persisted circuit state on a schema change. Preserves the schema-hash
  /// table and the auth token (the `sp00ky_auth_token` kv key).
  void resetLocalData() {
    _db.execute('DELETE FROM records');
    _db.execute('DELETE FROM _00_query');
    _db.execute('DELETE FROM _00_pending_mutations');
    _db.execute('DELETE FROM _00_stream_processor_state');
    kvRemove('_00_stream_processor_state');
  }

  // ---- transactions ---------------------------------------------------------

  T tx<T>(T Function() body) {
    _db.execute('BEGIN');
    try {
      final result = body();
      _db.execute('COMMIT');
      return result;
    } catch (_) {
      _db.execute('ROLLBACK');
      rethrow;
    }
  }

  void close() => _db.dispose();

  Map<String, dynamic> _deepMerge(
      Map<String, dynamic> base, Map<String, dynamic> patch) {
    final out = Map<String, dynamic>.from(base);
    patch.forEach((key, value) {
      final existing = out[key];
      if (existing is Map<String, dynamic> && value is Map<String, dynamic>) {
        out[key] = _deepMerge(existing, value);
      } else {
        out[key] = value;
      }
    });
    return out;
  }
}
