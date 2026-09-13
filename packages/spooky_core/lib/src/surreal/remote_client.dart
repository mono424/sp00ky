import 'dart:async';

import 'package:meta/meta.dart';
import 'package:web_socket_channel/web_socket_channel.dart';

import '../kernel/effects.dart' show StatementResult;
import 'cbor_codec.dart';

/// A LIVE-query notification (TS SurrealDB `live` message).
class LiveMessage {
  const LiveMessage(this.action, this.value);

  /// `'CREATE' | 'UPDATE' | 'DELETE' | 'KILLED'`.
  final String action;
  final Map<String, dynamic> value;
}

/// Implemented by a client that can answer a multi-statement request WITHOUT
/// collapsing it: the saga core reads four statements from one register request
/// and tolerates an error on the last three.
///
/// Optional on purpose. [RemoteSurrealClient] keeps its throwing [query] as the
/// one method every implementation (including the test fakes) must provide;
/// `statementResults` in the adapter layer falls back to it.
abstract interface class StatementAwareRemote {
  /// Per-statement outcomes, in order. Never throws for a failed statement.
  Future<List<StatementResult>> queryStatements(String sql,
      [Map<String, dynamic>? vars]);
}

/// The subset of the SurrealDB client the core needs. Decouples the core from
/// the transport so a community package could be dropped in later.
abstract class RemoteSurrealClient {
  Future<void> connect(String endpoint);
  Future<void> use({required String namespace, required String database});
  Future<dynamic> authenticate(String token);
  Future<dynamic> signin(Map<String, dynamic> params);
  Future<dynamic> signup(Map<String, dynamic> params);
  Future<void> invalidate();

  /// Run a SURQL query; returns the per-statement results array.
  Future<List<dynamic>> query(String sql, [Map<String, dynamic>? vars]);

  /// Start a LIVE query; returns the live id and a stream of notifications.
  Future<(String liveId, Stream<LiveMessage>)> live(String sql,
      [Map<String, dynamic>? vars]);

  Future<void> kill(String liveId);

  /// Connection lifecycle: emits on connect / disconnect.
  Stream<void> get onConnected;
  Stream<void> get onDisconnected;

  Future<void> close();
}

/// Minimal SurrealDB WebSocket RPC client over the **CBOR** subprotocol.
///
/// Implements only what the core uses (`use`/`signin`/`signup`/`authenticate`/
/// `invalidate`/`query`/`live`/`kill` + lifecycle). CBOR (not JSON) is required
/// so bind vars carry real record ids / datetimes — SurrealDB won't match a
/// `record<…>` field against a plain string, which silently broke every
/// record-filtered query. See [cbor_codec] for the encode/normalize rules.
class WebSocketSurrealClient
    implements RemoteSurrealClient, StatementAwareRemote {
  WebSocketSurrealClient();

  WebSocketChannel? _channel;
  int _nextId = 0;
  final Map<String, Completer<dynamic>> _pending = {};
  final Map<String, StreamController<LiveMessage>> _liveControllers = {};
  final StreamController<void> _connected = StreamController.broadcast();
  final StreamController<void> _disconnected = StreamController.broadcast();

  @override
  Stream<void> get onConnected => _connected.stream;
  @override
  Stream<void> get onDisconnected => _disconnected.stream;

  /// Opens the WebSocket for [uri]. Overridable in tests to inject a fake
  /// channel (see `@visibleForTesting`).
  @visibleForTesting
  WebSocketChannel createChannel(Uri uri) =>
      // Negotiate SurrealDB's CBOR subprotocol so record ids / datetimes travel
      // as typed values (frames are then binary CBOR, not JSON text).
      WebSocketChannel.connect(uri, protocols: const ['cbor']);

  /// The RPC endpoint a raw [endpoint] normalizes to (exposed for tests).
  @visibleForTesting
  String rpcEndpoint(String endpoint) => _rpcEndpoint(endpoint);

  @override
  Future<void> connect(String endpoint) async {
    if (_channel != null) await forceClose();
    final uri = Uri.parse(_rpcEndpoint(endpoint));
    final channel = createChannel(uri);
    _channel = channel;
    await channel.ready;
    channel.stream.listen(
      _onMessage,
      onDone: () => _onSocketEnd(null),
      onError: (Object error) => _onSocketEnd(error),
      cancelOnError: false,
    );
    _connected.add(null);
  }

  /// The socket ended. Every in-flight RPC is now unanswerable: completing it
  /// with an error is what lets a caller retry instead of hanging forever, and
  /// what makes the supervisor's probe fail fast on a dead socket.
  void _onSocketEnd(Object? error) {
    final pending = [..._pending.values];
    _pending.clear();
    final reason = StateError(
        'WebSocket closed before the response arrived${error == null ? '' : ': $error'}');
    for (final completer in pending) {
      if (!completer.isCompleted) completer.completeError(reason);
    }
    // The server-side LIVE queries die with the socket; the sagas re-subscribe
    // on the next `connected`.
    for (final controller in _liveControllers.values) {
      unawaited(controller.close());
    }
    _liveControllers.clear();
    if (!_disconnected.isClosed) _disconnected.add(null);
  }

  /// True while a socket is open. The supervisor reads this rather than
  /// tracking the transport itself.
  bool get isConnected => _channel != null;

  /// Drop the socket without disposing the client, so the supervisor can force
  /// the close a half-open connection never delivers and then re-open.
  Future<void> forceClose() async {
    final channel = _channel;
    _channel = null;
    if (channel == null) return;
    try {
      await channel.sink.close();
    } catch (_) {
      // A socket that is already gone is exactly what we wanted.
    }
    _onSocketEnd(null);
  }

  String _rpcEndpoint(String endpoint) {
    var e = endpoint;
    if (e.startsWith('http://')) e = 'ws://${e.substring(7)}';
    if (e.startsWith('https://')) e = 'wss://${e.substring(8)}';
    if (!e.endsWith('/rpc')) e = '${e.replaceAll(RegExp(r'/$'), '')}/rpc';
    return e;
  }

  Future<dynamic> _rpc(String method, List<dynamic> params) {
    final channel = _channel;
    if (channel == null) {
      throw StateError('WebSocket not connected');
    }
    final id = (_nextId++).toString();
    final completer = Completer<dynamic>();
    _pending[id] = completer;
    channel.sink
        .add(surrealCborEncode({'id': id, 'method': method, 'params': params}));
    return completer.future;
  }

  void _onMessage(dynamic raw) {
    // CBOR frames arrive as binary (List<int>/Uint8List); normalize to the same
    // JSON-shaped maps the rest of the client expects.
    if (raw is! List<int>) return;
    final decoded = surrealCborDecode(raw);
    if (decoded is! Map) return;
    final map = decoded.cast<String, dynamic>();

    // LIVE notifications carry no matching request id.
    final result = map['result'];
    if (map['id'] == null && result is Map && result['id'] != null) {
      _dispatchLive(result.cast<String, dynamic>());
      return;
    }

    final id = map['id']?.toString();
    if (id == null) return;
    final completer = _pending.remove(id);
    if (completer == null) return;
    if (map['error'] != null) {
      final err = map['error'];
      completer.completeError(StateError(err is Map
          ? (err['message']?.toString() ?? err.toString())
          : err.toString()));
    } else {
      completer.complete(map['result']);
    }
  }

  void _dispatchLive(Map<String, dynamic> notification) {
    final liveId = notification['id'].toString();
    final action = (notification['action'] as String?)?.toUpperCase() ?? '';
    final value =
        (notification['result'] as Map?)?.cast<String, dynamic>() ?? {};
    final controller = _liveControllers[liveId];
    controller?.add(LiveMessage(action, value));
  }

  String? _ns;
  String? _db;

  @override
  Future<void> use({required String namespace, required String database}) {
    _ns = namespace;
    _db = database;
    return _rpc('use', [namespace, database]);
  }

  @override
  Future<dynamic> authenticate(String token) => _rpc('authenticate', [token]);

  @override
  Future<dynamic> signin(Map<String, dynamic> params) =>
      _rpc('signin', [_authParams(params)]);

  @override
  Future<dynamic> signup(Map<String, dynamic> params) =>
      _rpc('signup', [_authParams(params)]);

  /// Normalize auth params to the SurrealDB RPC wire shape.
  ///
  /// `AuthService` (faithful to the JS SDK) passes `{access, variables}`; the
  /// raw RPC instead wants `{ns, db, ac, ...variables}` with the access
  /// variables flattened. Root logins (`{user, pass}`) pass through unchanged.
  Map<String, dynamic> _authParams(Map<String, dynamic> params) {
    if (!params.containsKey('access') && !params.containsKey('variables')) {
      return params; // e.g. root {user, pass}
    }
    final vars =
        (params['variables'] as Map?)?.cast<String, dynamic>() ?? const {};
    return {
      if (_ns != null) 'ns': _ns,
      if (_db != null) 'db': _db,
      if (params['access'] != null) 'ac': params['access'],
      ...vars,
    };
  }

  @override
  Future<void> invalidate() => _rpc('invalidate', []);

  @override
  Future<List<dynamic>> query(String sql, [Map<String, dynamic>? vars]) async {
    final result = await _rpc('query', [sql, vars ?? const <String, dynamic>{}]);
    // SurrealDB returns [{ status, result }, ...]; surface the result list to
    // match the JS `.query()` which returns the per-statement results.
    //
    // A statement with status 'ERR' throws (matching the JS SDK). This is
    // load-bearing for sync: a `LIVE SELECT` on a not-yet-created
    // `_00_list_ref_user_<id>` table fails per-statement in v3, and
    // Sp00kySync's retry backoff relies on the throw to re-attempt.
    if (result is List) {
      return result.map((stmt) {
        if (stmt is Map && stmt.containsKey('result')) {
          if (stmt['status'] != null && stmt['status'] != 'OK') {
            throw StateError('Query error: ${stmt['result']}');
          }
          return stmt['result'];
        }
        return stmt;
      }).toList();
    }
    return [result];
  }

  @override
  Future<List<StatementResult>> queryStatements(String sql,
      [Map<String, dynamic>? vars]) async {
    final result = await _rpc('query', [sql, vars ?? const <String, dynamic>{}]);
    if (result is! List) return [StatementResult.ok(result)];
    return [
      for (final stmt in result)
        if (stmt is Map && stmt.containsKey('result'))
          if (stmt['status'] != null && stmt['status'] != 'OK')
            StatementResult.err(stmt['result'].toString())
          else
            StatementResult.ok(stmt['result'])
        else
          StatementResult.ok(stmt)
    ];
  }

  @override
  Future<(String, Stream<LiveMessage>)> live(String sql,
      [Map<String, dynamic>? vars]) async {
    final results = await query(sql, vars);
    final liveId = results.isNotEmpty ? results.first.toString() : '';
    final controller = StreamController<LiveMessage>.broadcast();
    _liveControllers[liveId] = controller;
    return (liveId, controller.stream);
  }

  @override
  Future<void> kill(String liveId) async {
    await _rpc('kill', [liveId]);
    await _liveControllers.remove(liveId)?.close();
  }

  @override
  Future<void> close() async {
    final channel = _channel;
    _channel = null;
    await channel?.sink.close();
    for (final c in _liveControllers.values) {
      await c.close();
    }
    _liveControllers.clear();
    for (final completer in _pending.values) {
      if (!completer.isCompleted) {
        completer.completeError(StateError('Client closed'));
      }
    }
    _pending.clear();
    await _connected.close();
    await _disconnected.close();
  }
}
