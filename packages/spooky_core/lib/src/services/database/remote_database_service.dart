import 'dart:async';

import '../../kernel/effects.dart' show StatementResult;
import '../../surreal/remote_client.dart';
import '../../types.dart';
import '../../utils/surql.dart';
import '../logger/logger.dart';

/// Remote SurrealDB access over a [RemoteSurrealClient], with the serialized
/// query queue from the JS `AbstractDatabaseService` (prevents overlapping
/// transactions).
class RemoteDatabaseService {
  RemoteDatabaseService(this._config, this._client, SpookyLogger logger)
      : _logger = logger.child('RemoteDatabaseService');

  final DatabaseConfig _config;
  final RemoteSurrealClient _client;
  final SpookyLogger _logger;

  Future<void> _queryQueue = Future<void>.value();

  /// The token the session last authenticated with, re-applied by [connect] so
  /// a socket rebuilt from scratch comes back authenticated.
  ///
  /// Without this a reconnect left the client reporting a signed-in user while
  /// its session was anonymous, and every view registered afterwards was
  /// stamped with an empty identity.
  String? _authToken;

  RemoteSurrealClient getClient() => _client;
  DatabaseConfig getConfig() => _config;

  void setAuthToken(String? token) => _authToken = token;

  /// True while a socket is open, for the supervisor.
  bool get isConnected {
    final client = _client;
    return client is WebSocketSurrealClient ? client.isConnected : true;
  }

  /// Drop the socket without disposing the client, so the supervisor can force
  /// the close a half-open connection never delivers.
  Future<void> forceClose() async {
    final client = _client;
    if (client is WebSocketSurrealClient) await client.forceClose();
  }

  Future<void> connect() async {
    final endpoint = _config.endpoint;
    if (endpoint == null) {
      _logger.warn('No endpoint configured for remote database');
      return;
    }
    await _client.connect(endpoint);
    await _client.use(namespace: _config.namespace, database: _config.database);
    final token = _authToken ?? _config.token;
    if (token != null) {
      await _client.authenticate(token);
    }
    _logger.info('Connected to remote database');
  }

  /// Serialized query: chains onto [_queryQueue] so calls never overlap.
  Future<List<dynamic>> query(String sql, [Map<String, dynamic>? vars]) {
    final completer = Completer<List<dynamic>>();
    _queryQueue = _queryQueue.then((_) async {
      try {
        completer.complete(await _client.query(sql, vars));
      } catch (err) {
        completer.completeError(err);
      }
    }).catchError((Object err) {
      // The query's own error already reached the caller via `completeError`
      // above; swallow here only so one failed link can't wedge the serialized
      // queue for every subsequent query.
      _logger.debug('Serialized query link failed (already surfaced): $err');
    });
    return completer.future;
  }

  /// Per-statement outcomes, serialized on the same queue as [query] so the
  /// supervisor's probe cannot miss a wedged one.
  ///
  /// A client that cannot answer per statement (a test fake) falls back to
  /// [query]: its arity is preserved and a throw becomes one ERR, which is the
  /// all-or-nothing shape such a client actually has.
  Future<List<StatementResult>> queryStatements(String sql,
      [Map<String, dynamic>? vars]) {
    final RemoteSurrealClient client = _client;
    if (client is! StatementAwareRemote) {
      return query(sql, vars).then(
        (rows) => [for (final row in rows) StatementResult.ok(row)],
        onError: (Object e) => [StatementResult.err(e.toString())],
      );
    }
    final completer = Completer<List<StatementResult>>();
    _queryQueue = _queryQueue.then((_) async {
      try {
        completer.complete(
            await (client as StatementAwareRemote).queryStatements(sql, vars));
      } catch (err) {
        completer.completeError(err);
      }
    }).catchError((Object err) {
      _logger.debug('Serialized query link failed (already surfaced): $err');
    });
    return completer.future;
  }

  Future<T> execute<T>(SealedQuery<T> query,
      [Map<String, dynamic>? vars]) async {
    final raw = await this.query(query.sql, vars);
    return query.extract(raw);
  }

  Future<dynamic> signin(Map<String, dynamic> params) => _client.signin(params);
  Future<dynamic> signup(Map<String, dynamic> params) => _client.signup(params);
  Future<dynamic> authenticate(String token) => _client.authenticate(token);
  Future<void> invalidate() => _client.invalidate();

  Future<void> close() => _client.close();
}
