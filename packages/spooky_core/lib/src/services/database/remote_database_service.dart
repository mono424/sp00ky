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
  int _authEpoch = 0;
  int _transition = 0;
  Completer<void>? _identityGate;

  int beginSessionTransition() {
    _authEpoch++;
    if (_identityGate == null || _identityGate!.isCompleted) {
      _identityGate = Completer<void>();
    }
    return ++_transition;
  }

  void endSessionTransition(int transition) {
    if (transition == _transition && !(_identityGate?.isCompleted ?? true)) {
      _identityGate!.complete();
    }
  }

  Future<void> _awaitIdentity() async {
    await _identityGate?.future;
  }

  // Verification reads bypass the app query queue: that queue may be waiting
  // for this very transition to finish. They cannot write account cache data.
  Future<List<dynamic>> queryAuthUser() async {
    await _awaitConnect();
    return _client.query(r'SELECT * FROM ONLY $auth.id');
  }

  void Function()? onHandshake;

  /// Completes when the current connect attempt has finished, successfully or
  /// not. Every RPC waits on it.
  ///
  /// The engine's boot is local-first: it returns before the network half has
  /// run, so `use(ns, db)` and `authenticate(token)` have NOT happened yet when
  /// the app makes its first call. A signup issued in that window reaches the
  /// server with no namespace and fails as "There was a problem with signing
  /// up". Re-armed on every connect, so an RPC issued during a reconnect waits
  /// for the new socket's handshake rather than riding the dead one.
  Completer<void>? _connectGate;

  /// How long an RPC waits for a connect before going ahead anyway. Going ahead
  /// fails it as a network error, which the outbox and the registrations retry;
  /// waiting forever would wedge them instead.
  Duration connectGateTimeout = const Duration(seconds: 20);

  /// Hold RPCs until the first connect attempt finishes. Called before the
  /// engine boots, so a call made before [connect] still waits for it.
  void armConnectGate() => _connectGate ??= Completer<void>();

  Future<void> _awaitConnect() async {
    final gate = _connectGate;
    if (gate == null || gate.isCompleted) return;
    await gate.future.timeout(connectGateTimeout, onTimeout: () {
      _logger.warn('Proceeding without a connected socket: '
          'no handshake within ${connectGateTimeout.inSeconds}s');
    });
  }

  void _openConnectGate() {
    final gate = _connectGate;
    if (gate != null && !gate.isCompleted) gate.complete();
  }

  RemoteSurrealClient getClient() => _client;

  /// Gated LIVE subscribe. A LIVE issued before the handshake is rejected with
  /// "Specify a namespace to use" and then never retried, so the down-path runs
  /// on the poll alone until the next reconnect.
  Future<(String, Stream<LiveMessage>)> live(String sql,
      [Map<String, dynamic>? vars]) async {
    final epoch = _authEpoch;
    await _awaitIdentity();
    await _awaitConnect();
    _checkEpoch(epoch);
    final result = await _client.live(sql, vars);
    _checkEpoch(epoch);
    return result;
  }

  Future<void> kill(String liveId) async {
    await _awaitConnect();
    return _client.kill(liveId);
  }

  DatabaseConfig getConfig() => _config;

  void setAuthToken(String? token) {
    if (token != _authToken) _authEpoch++;
    _authToken = token;
  }

  void _checkEpoch(int epoch) {
    if (epoch != _authEpoch)
      throw StateError('Account changed during remote operation');
  }

  Future<T> _authRpc<T>(Future<T> Function() rpc) async {
    final epoch = _authEpoch;
    await _awaitConnect();
    _checkEpoch(epoch);
    try {
      final result = await rpc();
      _checkEpoch(epoch);
      return result;
    } finally {
      // A late auth RPC can change the server socket's identity even when its
      // result is discarded locally. Reconnect with the current saved token.
      if (epoch != _authEpoch) await forceClose();
    }
  }

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
      _openConnectGate();
      return;
    }
    final gate = _connectGate;
    if (gate == null || gate.isCompleted) _connectGate = Completer<void>();
    try {
      await _client.connect(endpoint);
      await _client.use(
          namespace: _config.namespace, database: _config.database);
      final epoch = _authEpoch;
      final token = _authToken ?? _config.token;
      if (token != null) {
        await _client.authenticate(token);
      }
      if (epoch != _authEpoch) {
        await forceClose();
        _checkEpoch(epoch);
      }
      _logger.info('Connected to remote database');
    } finally {
      // Success or failure: the attempt is over, so held RPCs stop waiting.
      _openConnectGate();
      scheduleMicrotask(() => onHandshake?.call());
    }
  }

  /// Serialized query: chains onto [_queryQueue] so calls never overlap.
  Future<List<dynamic>> query(String sql, [Map<String, dynamic>? vars]) {
    final epoch = _authEpoch;
    final completer = Completer<List<dynamic>>();
    _queryQueue = _queryQueue.then((_) async {
      try {
        await _awaitIdentity();
        await _awaitConnect();
        _checkEpoch(epoch);
        final result = await _client.query(sql, vars);
        _checkEpoch(epoch);
        completer.complete(result);
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
    final epoch = _authEpoch;
    final completer = Completer<List<StatementResult>>();
    _queryQueue = _queryQueue.then((_) async {
      try {
        await _awaitIdentity();
        await _awaitConnect();
        _checkEpoch(epoch);
        final result =
            await (client as StatementAwareRemote).queryStatements(sql, vars);
        _checkEpoch(epoch);
        completer.complete(result);
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

  // Auth RPCs wait for the handshake too: `signin`/`signup` carry the
  // namespace and database the `use` call established, and reach the server
  // without them if they run first.
  Future<dynamic> signin(Map<String, dynamic> params) =>
      _authRpc(() => _client.signin(params));

  Future<dynamic> signup(Map<String, dynamic> params) =>
      _authRpc(() => _client.signup(params));

  Future<dynamic> authenticate(String token) =>
      _authRpc(() => _client.authenticate(token));

  Future<void> invalidate() => _authRpc(_client.invalidate);

  Future<void> close() {
    endSessionTransition(_transition);
    _openConnectGate(); // never leave an RPC waiting on a client that is gone
    return _client.close();
  }
}
