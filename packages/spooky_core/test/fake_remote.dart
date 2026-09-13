import 'dart:async';

import 'package:spooky_core/spooky_core.dart';
import 'package:spooky_core/src/kernel/effects.dart' show StatementResult;
import 'package:spooky_core/src/surreal/remote_client.dart'
    show StatementAwareRemote;

/// In-memory fake of the SurrealDB client, speaking the protocol the saga core
/// actually issues: one multi-statement register request, batched edge reads,
/// body fetches by id, heartbeats, and LIVE over `_00_list_ref`.
///
/// Answers per statement (it implements [StatementAwareRemote]), so a test can
/// reproduce a partial failure the way the server produces one.
class FakeRemote implements RemoteSurrealClient, StatementAwareRemote {
  bool connected = false;
  String? usedNamespace;

  /// Every SQL string the client sent, in order.
  final List<String> queries = [];

  /// Bodies the server holds, by record id.
  final Map<String, Map<String, dynamic>> records = {};

  /// The server's view membership: `_00_query` id -> `(recordId, version)`.
  final Map<String, List<(String, int)>> membership = {};

  /// Membership for any view no entry in [membership] names, so a test can seed
  /// rows before it knows the hash the registration will mint.
  List<(String, int)>? defaultMembership;

  /// What the `_00_query` row reports. Absent means "no such view", which the
  /// client reads as a lost view rather than an empty one; [defaultMembership]
  /// implies a `ready` row of its own size.
  final Map<String, ({int rowCount, String state})> views = {};

  /// Make mutation pushes fail as a network error, so the write stays queued.
  bool blockMutations = false;

  /// Make every request fail.
  bool offline = false;

  /// Ids the last body fetch asked for.
  List<String> lastFetchedIds = const [];

  final _connected = StreamController<void>.broadcast();
  final _disconnected = StreamController<void>.broadcast();
  final _live = StreamController<LiveMessage>.broadcast();

  @override
  Stream<void> get onConnected => _connected.stream;
  @override
  Stream<void> get onDisconnected => _disconnected.stream;

  /// Publish a `_00_list_ref` notification, as the server's LIVE feed would.
  void pushEdge(String action, String queryId, String recordId,
      {int version = 1, Map<String, dynamic>? body}) {
    _live.add(LiveMessage(action, {
      'in': queryId,
      'out': body ?? recordId,
      'version': version,
    }));
  }

  void drop() => _disconnected.add(null);
  void restore() => _connected.add(null);

  /// Seed a view's membership and the row it reports.
  void publish(String viewId, List<(String, int)> ids) {
    membership[viewId] = ids;
    views[viewId] = (rowCount: ids.length, state: 'ready');
  }

  @override
  Future<void> connect(String endpoint) async {
    connected = true;
    _connected.add(null);
  }

  @override
  Future<void> use({required String namespace, required String database}) async {
    usedNamespace = namespace;
  }

  @override
  Future<dynamic> authenticate(String token) async => null;
  @override
  Future<dynamic> signin(Map<String, dynamic> params) async => {'access': 'tok'};
  @override
  Future<dynamic> signup(Map<String, dynamic> params) async => {'access': 'tok'};
  @override
  Future<void> invalidate() async {}

  @override
  Future<List<dynamic>> query(String sql, [Map<String, dynamic>? vars]) async {
    final out = await queryStatements(sql, vars);
    return [
      for (final s in out)
        if (s.isOk) s.result else throw StateError('Query error: ${s.error}')
    ];
  }

  @override
  Future<List<StatementResult>> queryStatements(String sql,
      [Map<String, dynamic>? vars]) async {
    queries.add(sql);
    if (offline) throw StateError('connection refused');
    return [
      for (final statement in sql.split(';\n'))
        _answer(statement.trim(), vars ?? const {})
    ];
  }

  List<(String, int)> _idsOf(String id) =>
      membership[id] ?? defaultMembership ?? const [];

  ({int rowCount, String state})? _viewOf(String id) {
    final explicit = views[id];
    if (explicit != null) return explicit;
    final fallback = defaultMembership;
    return fallback == null
        ? null
        : (rowCount: fallback.length, state: 'ready');
  }

  StatementResult _answer(String sql, Map<String, dynamic> vars) {
    if (sql == 'RETURN true') return const StatementResult.ok(true);
    if (sql.contains(r'$auth.id') && sql.startsWith('RETURN')) {
      return const StatementResult.ok('user:u1');
    }
    if (sql.startsWith('SELECT * FROM ONLY \$auth.id')) {
      return StatementResult.ok(records['user:u1']);
    }
    if (sql.contains('fn::query::register')) {
      return const StatementResult.ok(null);
    }
    if (sql.contains('fn::query::heartbeat')) {
      // A non-empty answer means the view row is still there.
      return const StatementResult.ok([1]);
    }
    if (sql.contains('fn::query::unsubscribe')) {
      return const StatementResult.ok(null);
    }
    if (sql.startsWith('SELECT out, version FROM')) {
      final id = vars['in'].toString();
      final subquery = sql.contains('parent IS NOT NONE');
      if (subquery) return const StatementResult.ok(<dynamic>[]);
      return StatementResult.ok([
        for (final (rid, v) in _idsOf(id)) {'out': rid, 'version': v}
      ]);
    }
    if (sql.startsWith('SELECT in, out, version, parent FROM')) {
      final ins = (vars['ins'] as List? ?? const []).map((e) => e.toString());
      return StatementResult.ok([
        for (final id in ins)
          for (final (rid, v) in _idsOf(id))
            {'in': id, 'out': rid, 'version': v, 'parent': null}
      ]);
    }
    if (sql.contains('rowCount: rowCount')) {
      if (sql.contains('FROM ONLY')) {
        final view = _viewOf(vars['in'].toString());
        return StatementResult.ok(view == null
            ? null
            : {'rowCount': view.rowCount, 'state': view.state});
      }
      final ins = (vars['ins'] as List? ?? const []).map((e) => e.toString());
      return StatementResult.ok([
        for (final id in ins)
          if (_viewOf(id) != null)
            {
              'id': id,
              'rowCount': _viewOf(id)!.rowCount,
              'state': _viewOf(id)!.state,
            }
      ]);
    }
    if (sql == 'SELECT * FROM \$ids') {
      final ids = (vars['ids'] as List? ?? const []).map((e) => e.toString());
      lastFetchedIds = ids.toList();
      return StatementResult.ok([
        for (final id in ids)
          if (records[id] != null) records[id]!
      ]);
    }
    if (sql.startsWith('CREATE ONLY') ||
        sql.startsWith('UPDATE') ||
        sql.startsWith('DELETE')) {
      if (blockMutations) {
        // Network-classified, so the write stays queued rather than rolling back.
        return const StatementResult.err('connection refused');
      }
      return const StatementResult.ok(null);
    }
    return const StatementResult.ok(null);
  }

  @override
  Future<(String, Stream<LiveMessage>)> live(String sql,
      [Map<String, dynamic>? vars]) async {
    queries.add(sql);
    if (offline) throw StateError('connection refused');
    return ('live-1', _live.stream);
  }

  @override
  Future<void> kill(String liveId) async {}

  @override
  Future<void> close() async {
    await _connected.close();
    await _disconnected.close();
    await _live.close();
  }
}

/// Let the engine's microtasks and short timers run.
Future<void> settle([int ms = 80]) =>
    Future<void>.delayed(Duration(milliseconds: ms));
