import 'dart:async';
import 'dart:isolate';
import '../sp00ky_client.dart';
import '../in_process_client.dart';
import '../modules/auth/sp00ky_auth.dart';
import '../modules/query_builder.dart';
import '../mutation/rows.dart';
import '../state/client_state.dart';
import '../state/selectors.dart' as selectors;
import '../types.dart';
import '../utils/error_classification.dart';
import 'worker_protocol.dart';

class WorkerSp00kyClient extends Sp00kyClient {
  WorkerSp00kyClient(this.config) : super.internal() {
    if (config.persistenceClient != null &&
        config.persistenceClient != 'memory') {
      throw ArgumentError('Custom persistence requires InProcessSp00kyClient');
    }
    _auth = _RemoteAuth(this);
  }
  @override
  final Sp00kyConfig config;
  late final _RemoteAuth _auth;
  @override
  Sp00kyAuth get auth {
    if (config.database.endpoint == null)
      throw StateError('Auth requires a remote endpoint');
    return _auth;
  }

  Isolate? _worker;
  ReceivePort? _inbox;
  SendPort? _port;
  Future<void>? _initializing;
  Future<void>? _closing;
  final _ready = Completer<void>();
  bool _closed = false;
  int _epoch = 0;
  int _nextId = 0;
  final _pending = <int, Completer<dynamic>>{};
  final _streams = <int, StreamController<List<Map<String, dynamic>>>>{};
  final _queries = <String, WorkerQueryState>{};
  final _listeners = <String, Set<void Function(dynamic)>>{};
  @override
  bool isLocalReady = false;
  @override
  int pendingMutationCount = 0;
  @override
  int fetchingQueryCount = 0;
  @override
  int failedMutationCount = 0;
  @override
  Set<String> unsyncedRecordIds = const {};
  @override
  SyncHealth syncHealth = const SyncHealth(
      status: SyncHealthStatus.healthy,
      consecutiveFailures: 0,
      everConnected: false);

  @override
  Future<void> init() {
    if (_closed) return Future.error(StateError('Client is closed'));
    return _initializing ??= _start();
  }

  Future<void> _start() async {
    final inbox = _inbox = ReceivePort();
    inbox.listen(_receive);
    try {
      _worker = await spawnWorker(inbox.sendPort);
      await _ready.future;
      isLocalReady = true;
    } catch (e, s) {
      _terminate(e, s);
      rethrow;
    }
  }

  // Internal seam for deterministic worker-failure tests. The normal public
  // factory never accepts executable callbacks or custom worker factories.
  Future<Isolate> spawnWorker(SendPort output) =>
      Isolate.spawn(_workerMain, (output, config),
          debugName: 'sp00ky core', onExit: output, onError: output);

  void _receive(dynamic event) {
    if (event is SendPort) {
      _port = event;
      if (!_ready.isCompleted) _ready.complete();
    } else if (event is WorkerResult) {
      final pending = _pending.remove(event.id);
      if (event.error case final error?) {
        pending?.completeError(error, StackTrace.fromString(error.stack));
      } else {
        pending?.complete(event.value);
      }
    } else if (event is WorkerFailure) {
      _terminate(event, StackTrace.fromString(event.stack));
    } else if (event is WorkerState) {
      if (event.epoch != _epoch) {
        _epoch = event.epoch;
        _queries.clear();
        for (final stream in _streams.values) {
          stream.add(const []);
        }
      }
      pendingMutationCount = event.pending;
      fetchingQueryCount = event.fetching;
      failedMutationCount = event.failed;
      unsyncedRecordIds = Set.unmodifiable(event.unsynced);
      syncHealth = event.health;
      if (event.auth case final state?) _auth.apply(state);
      _emit('pending', pendingMutationCount);
      _emit('fetching', fetchingQueryCount);
      _emit('failed', failedMutationCount);
      _emit('unsynced', unsyncedRecordIds);
      _emit('health', syncHealth);
    } else if (event is WorkerQueryState) {
      if (event.epoch != _epoch) return;
      final previous = _queries[event.hash];
      _queries[event.hash] = event;
      if (previous?.status != event.status)
        _emit('status:${event.hash}', event.status);
      if (previous?.authoritative != event.authoritative)
        _emit('authority:${event.hash}', event.authoritative);
    } else if (event is WorkerRows) {
      if (event.epoch != _epoch) return;
      if (event.error case final error?) {
        _streams[event.id]?.addError(error, StackTrace.fromString(error.stack));
      } else {
        _streams[event.id]?.add(event.rows);
      }
    } else {
      _terminate(StateError('Sync worker stopped: $event'), StackTrace.current);
    }
  }

  void _terminate(Object error, StackTrace stack) {
    _closed = true;
    isLocalReady = false;
    if (_initializing != null && !_ready.isCompleted)
      _ready.completeError(error, stack);
    for (final pending in _pending.values) {
      pending.completeError(error, stack);
    }
    _pending.clear();
    for (final stream in _streams.values.toList()) {
      if (_closing == null) stream.addError(error, stack);
      unawaited(stream.close());
    }
    _streams.clear();
    _listeners.clear();
    _auth.clearListeners();
    _worker?.kill(priority: Isolate.immediate);
    _inbox?.close();
  }

  Future<T> _call<T>(WorkerCommand command) async {
    await init();
    if (_closed) throw StateError('Client is closed');
    final id = ++_nextId;
    final pending = Completer<dynamic>();
    _pending[id] = pending;
    try {
      _port!.send(WorkerRequest(id, command));
    } catch (e, s) {
      _pending.remove(id);
      pending.completeError(e, s);
    }
    return await pending.future as T;
  }

  void _send(WorkerCommand command) {
    if (!_closed) _port?.send(WorkerRequest(0, command));
  }

  void _emit(String key, dynamic value) {
    for (final callback in _listeners[key]?.toList() ?? const []) {
      callback(value);
    }
  }

  void Function() _listen<T>(
      String key, void Function(T) callback, T value, bool immediate) {
    void cb(dynamic value) => callback(value as T);
    (_listeners[key] ??= {}).add(cb);
    if (immediate) callback(value);
    return () => _listeners[key]?.remove(cb);
  }

  @override
  Future<String> queryRaw(String sql, Map<String, dynamic> params,
          {QueryTimeToLive ttl = defaultTtl,
          List<RelationPlan> relations = const []}) =>
      _call(QueryCommand(sql, params, ttl, relations));
  @override
  Future<List<dynamic>> queryRemote(String sql, [Map<String, dynamic>? vars]) =>
      _call(RemoteCommand(sql, vars));
  @override
  Future<void> preload(String sql, Map<String, dynamic> params,
          {QueryTimeToLive ttl = defaultTtl}) =>
      _call(PreloadCommand(sql, params, ttl));
  @override
  Stream<List<Map<String, dynamic>>> subscribeStream(String hash,
      {bool immediate = true}) {
    late StreamController<List<Map<String, dynamic>>> controller;
    int? subscription;
    controller = StreamController.broadcast(onListen: () {
      final id = subscription = ++_nextId;
      _streams[id] = controller;
      init().then((_) {
        if (_streams.containsKey(id))
          _send(SubscribeCommand(id, hash, immediate));
      }, onError: (Object e, StackTrace s) {
        if (!controller.isClosed) {
          controller.addError(e, s);
          unawaited(controller.close());
        }
      });
    }, onCancel: () {
      final id = subscription;
      _streams.remove(id);
      if (id != null) _send(UnsubscribeCommand(id));
    });
    return controller.stream;
  }

  @override
  void Function() subscribeQueryStatus(String hash, QueryStatusCallback cb,
          {bool immediate = false}) =>
      _listen('status:$hash', cb, _queries[hash]?.status ?? QueryStatus.idle,
          immediate);
  @override
  void Function() subscribeQueryAuthority(
          String hash, QueryAuthorityCallback cb,
          {bool immediate = false}) =>
      _listen('authority:$hash', cb, isQueryAuthoritative(hash), immediate);
  @override
  bool isQueryAuthoritative(String hash) =>
      _queries[hash]?.authoritative ?? false;
  @override
  bool isQuerySettled(String hash) => _queries[hash]?.settled ?? false;
  @override
  QueryTimings? queryTimings(String hash) => _queries[hash]?.timings;
  @override
  void reportFrontendTiming(String hash, double ms) =>
      _send(TimingCommand(hash, ms));
  @override
  void deregisterQuery(String hash) => _send(DeregisterCommand(hash));
  @override
  Future<Map<String, dynamic>> create(String id, Map<String, dynamic> data) =>
      _call(CreateCommand(id, data));
  @override
  Future<Map<String, dynamic>> update(
          String table, String id, Map<String, dynamic> data,
          {UpdateOptions? options}) =>
      _call(UpdateCommand(table, id, data, options));
  @override
  Future<void> delete(String table, String id) =>
      _call(DeleteCommand(table, id));
  @override
  Future<void> run(String backend, String path, Map<String, dynamic> payload,
          {RunOptions? options}) =>
      _call(RunCommand(backend, path, payload, options));
  @override
  Future<List<FailedMutationRow>> listFailedMutations() =>
      _call(FailedCommand());
  @override
  Future<bool> retryFailedMutation(String id) => _call(RetryCommand(id));
  @override
  Future<bool> discardFailedMutation(String id) => _call(DiscardCommand(id));
  @override
  Future<dynamic> authenticate(String token) =>
      _call(AuthenticateCommand(token));
  @override
  Future<void> deauthenticate() => _call(DeauthenticateCommand());
  @override
  Future<ClientState> inspectState() => _call(InspectCommand());
  @override
  Future<void> checkpoint() => _call(CheckpointCommand());
  @override
  Future<void> detach() => _call(DetachCommand());
  @override
  void wake() => _send(WakeCommand());
  @override
  void Function() subscribeToPendingMutations(void Function(int) cb) =>
      _listen('pending', cb, pendingMutationCount, true);
  @override
  void Function() subscribeToFetchActivity(void Function(int) cb) =>
      _listen('fetching', cb, fetchingQueryCount, true);
  @override
  void Function() subscribeToFailedMutations(void Function(int) cb) =>
      _listen('failed', cb, failedMutationCount, true);
  @override
  void Function() subscribeToUnsyncedRecords(void Function(Set<String>) cb) =>
      _listen('unsynced', cb, unsyncedRecordIds, true);
  @override
  void Function() subscribeToSyncHealth(void Function(SyncHealth) cb) =>
      _listen('health', cb, syncHealth, true);
  @override
  Future<void> close() => _closing ??= _close();
  Future<void> _close() async {
    if (_closed) return;
    closeModules();
    try {
      if (_initializing != null)
        await _call<void>(CloseCommand()).timeout(const Duration(seconds: 5));
    } finally {
      _terminate(StateError('Client is closed'), StackTrace.current);
    }
  }
}

class _RemoteAuth implements Sp00kyAuth {
  _RemoteAuth(this.client);
  final WorkerSp00kyClient client;
  final _listeners = <void Function(String?)>{};
  @override
  String? token;
  @override
  AuthVerificationError? verificationError;
  @override
  Map<String, dynamic>? currentUser;
  @override
  String? access;
  @override
  bool isAuthenticated = false;
  @override
  bool isLoading = true;
  void apply(AuthSnapshot state) {
    final previous = (
      token,
      currentUser,
      access,
      isAuthenticated,
      isLoading,
      verificationError
    );
    verificationError = state.verificationError;
    token = state.token;
    currentUser = state.user == null ? null : Map.unmodifiable(state.user!);
    access = state.access;
    isAuthenticated = state.authenticated;
    isLoading = state.loading;
    if (previous.$1 != token ||
        previous.$2.toString() != currentUser.toString() ||
        previous.$3 != access ||
        previous.$4 != isAuthenticated ||
        previous.$5 != isLoading ||
        previous.$6?.message != verificationError?.message) {
      for (final cb in _listeners.toList()) {
        cb(currentUser?['id']?.toString());
      }
    }
  }

  void clearListeners() => _listeners.clear();
  @override
  void Function() subscribe(void Function(String?) cb) {
    _listeners.add(cb);
    cb(currentUser?['id']?.toString());
    return () => _listeners.remove(cb);
  }

  @override
  Future<void> signIn(String name, Map<String, dynamic> params) =>
      client._call(SignInCommand(name, params));
  @override
  Future<void> signUp(String name, Map<String, dynamic> params) =>
      client._call(SignUpCommand(name, params));
  @override
  Future<void> signOut() => client._call(SignOutCommand());
}

WorkerFailure _failure(Object e, StackTrace s) => WorkerFailure(
    classifySyncError(e) == 'network' ? 'network' : e.runtimeType.toString(),
    '$e',
    '$s');

Future<void> _workerMain((SendPort, Sp00kyConfig) input) async {
  final (output, config) = input;
  final client = InProcessSp00kyClient(config);
  final commands = ReceivePort();
  final subscriptions = <int, StreamSubscription<List<Map<String, dynamic>>>>{};
  final queryObservers = <String, List<void Function()>>{};
  final observers = <void Function()>[];
  final queuedRows = <int, WorkerRows>{};
  bool scheduled = false;
  bool stopped = false;
  int getEpoch() => client.storeEpoch;
  void state() {
    if (stopped || client.sessionTransitioning) return;
    output.send(WorkerState(
        getEpoch(),
        config.database.endpoint == null ? null : AuthSnapshot(client.auth),
        client.pendingMutationCount,
        client.fetchingQueryCount,
        client.failedMutationCount,
        client.unsyncedRecordIds,
        client.syncHealth));
  }

  void queryState(String hash) {
    if (stopped || client.sessionTransitioning) return;
    output.send(WorkerQueryState(
        getEpoch(),
        hash,
        client.isQueryAuthoritative(hash),
        client.isQuerySettled(hash),
        selectors.queryStatus(client.state, hash) ?? QueryStatus.idle,
        client.queryTimings(hash)));
  }

  void watchQuery(String hash) {
    queryObservers.putIfAbsent(
        hash,
        () => [
              client.subscribeQueryStatus(hash, (_) => queryState(hash)),
              client.subscribeQueryAuthority(hash, (_) => queryState(hash)),
            ]);
    queryState(hash);
  }

  void flushRows() {
    scheduled = false;
    if (client.sessionTransitioning) return;
    final rows = queuedRows.values.toList();
    queuedRows.clear();
    if (stopped) return;
    state();
    for (final event in rows) {
      if (subscriptions.containsKey(event.id) && event.epoch == getEpoch())
        output.send(event);
    }
  }

  try {
    // The restored session goes out the moment the cached token is decoded,
    // before the circuit primes: the host paints the signed-in identity while
    // the rest of the local boot is still running. Delivered ahead of the
    // command port, which the host only receives once init() has finished.
    client.onSessionRestored = state;
    await client.init();
    if (config.database.endpoint != null)
      observers.add(client.auth.subscribe((_) {
        state();
        for (final hash in queryObservers.keys) {
          queryState(hash);
        }
        flushRows();
      }));
    observers.addAll([
      client.subscribeToPendingMutations((_) => state()),
      client.subscribeToFetchActivity((_) => state()),
      client.subscribeToFailedMutations((_) => state()),
      client.subscribeToUnsyncedRecords((_) => state()),
      client.subscribeToSyncHealth((_) => state()),
    ]);
    state();
    output.send(commands.sendPort);
  } catch (e, s) {
    output.send(_failure(e, s));
    await client.close();
    commands.close();
    return;
  }
  commands.listen((dynamic message) async {
    final request = message as WorkerRequest;
    final epoch = getEpoch();
    try {
      dynamic result;
      switch (request.command) {
        case QueryCommand command:
          result = await client.queryRaw(command.sql, command.params,
              ttl: command.ttl, relations: command.relations);
          watchQuery(result);
        case RemoteCommand command:
          result = await client.queryRemote(command.sql, command.vars);
        case PreloadCommand command:
          await client.preload(command.sql, command.params, ttl: command.ttl);
        case CreateCommand command:
          result = await client.create(command.recordId, command.data);
        case UpdateCommand command:
          result = await client.update(
              command.table, command.recordId, command.data,
              options: command.options);
        case DeleteCommand command:
          await client.delete(command.table, command.recordId);
        case RunCommand command:
          await client.run(command.backend, command.path, command.payload,
              options: command.options);
        case SignInCommand command:
          await client.auth.signIn(command.access, command.params);
        case SignUpCommand command:
          await client.auth.signUp(command.access, command.params);
        case SignOutCommand():
          await client.auth.signOut();
        case AuthenticateCommand command:
          result = await client.authenticate(command.token);
        case DeauthenticateCommand():
          await client.deauthenticate();
        case InspectCommand():
          result = await client.inspectState();
        case CheckpointCommand():
          await client.checkpoint();
        case DetachCommand():
          await client.detach();
        case FailedCommand():
          result = await client.listFailedMutations();
        case RetryCommand command:
          result = await client.retryFailedMutation(command.mutationId);
        case DiscardCommand command:
          result = await client.discardFailedMutation(command.mutationId);
        case WakeCommand():
          client.wake();
        case TimingCommand command:
          client.reportFrontendTiming(command.hash, command.milliseconds);
          queryState(command.hash);
        case DeregisterCommand command:
          client.deregisterQuery(command.hash);
          if (!client.state.queries.containsKey(command.hash)) {
            for (final off in queryObservers.remove(command.hash) ?? []) {
              off();
            }
          }
        case SubscribeCommand command:
          final id = command.subscriptionId;
          final hash = command.hash;
          watchQuery(hash);
          subscriptions[id] = client
              .subscribeStream(hash, immediate: command.immediate)
              .listen((rows) {
            queryState(hash);
            queuedRows[id] = WorkerRows(getEpoch(), id, rows);
            if (!scheduled) {
              scheduled = true;
              scheduleMicrotask(flushRows);
            }
          }, onError: (Object e, StackTrace s) {
            output.send(WorkerRows(getEpoch(), id, const [], _failure(e, s)));
          });
        case UnsubscribeCommand command:
          queuedRows.remove(command.subscriptionId);
          await subscriptions.remove(command.subscriptionId)?.cancel();
        case CloseCommand():
          stopped = true;
          for (final off in observers) {
            off();
          }
          for (final list in queryObservers.values) {
            for (final off in list) {
              off();
            }
          }
          for (final sub in subscriptions.values) {
            await sub.cancel();
          }
          await client.close();
          commands.close();
      }
      if (!stopped) state();
      if (request.id != 0) {
        if (epoch != getEpoch() &&
            ![
              WorkerOp.signIn,
              WorkerOp.signUp,
              WorkerOp.signOut,
              WorkerOp.close
            ].contains(request.op)) {
          throw StateError('Account changed during operation');
        }
        output.send(WorkerResult(request.id, result));
      }
    } catch (e, s) {
      state();
      if (request.id != 0)
        output.send(WorkerResult(request.id, null, _failure(e, s)));
    }
  });
}
