import 'dart:async';

import 'package:spooky_core/src/surreal/cbor_codec.dart';
import 'package:spooky_core/src/surreal/remote_client.dart';
import 'package:spooky_core/src/surreal/value.dart';
import 'package:test/test.dart';
import 'package:web_socket_channel/web_socket_channel.dart';

/// A controllable fake [WebSocketChannel]: records frames the client sends and
/// lets the test push frames back (RPC responses + LIVE notifications).
class FakeChannel implements WebSocketChannel {
  final _incoming = StreamController<dynamic>.broadcast();
  final List<Map<String, dynamic>> sent = [];
  late final _FakeSink _sink = _FakeSink(sent, this);

  void push(Map<String, dynamic> frame) =>
      _incoming.add(surrealCborEncode(frame));

  /// The socket ended, as the transport reports it.
  void end() => unawaited(_incoming.close());

  /// What closing the sink waits for, as a half-open socket's close does.
  Future<void>? closeResult;

  /// Auto-complete the most recent RPC with [result].
  void respondLast(dynamic result) {
    final id = sent.last['id'];
    push({'id': id, 'result': result});
  }

  @override
  Future<void> get ready => Future.value();
  @override
  Stream<dynamic> get stream => _incoming.stream;
  @override
  WebSocketSink get sink => _sink;
  @override
  dynamic noSuchMethod(Invocation i) => super.noSuchMethod(i);
}

class _FakeSink implements WebSocketSink {
  _FakeSink(this.sent, this.owner);
  final List<Map<String, dynamic>> sent;
  final FakeChannel owner;
  @override
  void add(dynamic data) =>
      sent.add(surrealCborDecode(data as List<int>) as Map<String, dynamic>);
  @override
  Future<void> close([int? closeCode, String? closeReason]) async =>
      owner.closeResult;
  @override
  dynamic noSuchMethod(Invocation i) => super.noSuchMethod(i);
}

/// WS client handing out a new [FakeChannel] per connect.
class SequenceClient extends WebSocketSurrealClient {
  SequenceClient(this.channels);
  final List<FakeChannel> channels;
  var _next = 0;
  @override
  WebSocketChannel createChannel(Uri uri) => channels[_next++];
}

/// WS client wired to a [FakeChannel].
class TestClient extends WebSocketSurrealClient {
  TestClient(this.fake);
  final FakeChannel fake;
  @override
  WebSocketChannel createChannel(Uri uri) => fake;
}

void main() {
  group('rpcEndpoint normalization', () {
    final c = WebSocketSurrealClient();
    test('adds /rpc and converts http(s) -> ws(s)', () {
      expect(c.rpcEndpoint('ws://h:8000'), 'ws://h:8000/rpc');
      expect(c.rpcEndpoint('http://h:8000'), 'ws://h:8000/rpc');
      expect(c.rpcEndpoint('https://h:8000/'), 'wss://h:8000/rpc');
      expect(c.rpcEndpoint('ws://h:8000/rpc'), 'ws://h:8000/rpc');
    });
  });

  group('WebSocketSurrealClient (fake channel)', () {
    late FakeChannel fake;
    late TestClient client;

    setUp(() async {
      fake = FakeChannel();
      client = TestClient(fake);
      await client.connect('ws://x:8000');
    });
    tearDown(() => client.close());

    test('connect emits onConnected', () async {
      final fake2 = FakeChannel();
      final c2 = TestClient(fake2);
      final connected = Completer<void>();
      c2.onConnected.listen((_) => connected.complete());
      await c2.connect('ws://x:8000');
      await connected.future.timeout(const Duration(seconds: 1));
      await c2.close();
    });

    test('signin with {access, variables} flattens to {ns, db, ac, ...vars}',
        () async {
      final useF = client.use(namespace: 'myns', database: 'mydb');
      fake.respondLast({'namespace': 'myns', 'database': 'mydb'});
      await useF;
      final f = client.signin({
        'access': 'account',
        'variables': {'email': 'a@b.c', 'password': 'pw'},
      });
      // The use() + signin() each produced a frame; inspect the signin one.
      final frame = fake.sent.last;
      expect(frame['method'], 'signin');
      final params = (frame['params'] as List).first as Map;
      expect(params, {
        'ns': 'myns',
        'db': 'mydb',
        'ac': 'account',
        'email': 'a@b.c',
        'password': 'pw',
      });
      fake.respondLast('tok');
      expect(await f, 'tok');
    });

    test('root signin {user, pass} passes through unchanged', () async {
      final f = client.signin({'user': 'root', 'pass': 'root'});
      final params = (fake.sent.last['params'] as List).first as Map;
      expect(params, {'user': 'root', 'pass': 'root'});
      fake.respondLast('roottok');
      await f;
    });

    test('query unwraps per-statement results', () async {
      final f = client.query('SELECT * FROM thing');
      fake.respondLast([
        {
          'status': 'OK',
          'result': [
            {'id': 'thing:a'}
          ]
        }
      ]);
      final out = await f;
      expect(out.first, [
        {'id': 'thing:a'}
      ]);
    });

    test('query throws on a per-statement ERR', () async {
      final f = client.query('THROW "boom"');
      fake.respondLast([
        {'status': 'ERR', 'result': 'boom'}
      ]);
      await expectLater(f, throwsA(isA<StateError>()));
    });

    test('query encodes RecordId / DateTime bind vars', () async {
      final f = client.query(r'SELECT * FROM $id', {
        'id': RecordId('thing', 'x'),
        'when': DateTime.utc(2026, 1, 2, 3, 4, 5),
      });
      final params = (fake.sent.last['params'] as List);
      final vars = params[1] as Map;
      expect(vars['id'], 'thing:x');
      expect(vars['when'], '2026-01-02T03:04:05.000Z');
      fake.respondLast([
        {'status': 'OK', 'result': []}
      ]);
      await f;
    });

    test('live registers and routes notifications by id; kill stops them',
        () async {
      final liveFuture = client.live('LIVE SELECT * FROM thing');
      fake.respondLast([
        {'status': 'OK', 'type': 'live', 'result': 'live-uuid-1'}
      ]);
      final (liveId, stream) = await liveFuture;
      expect(liveId, 'live-uuid-1');

      final got = <LiveMessage>[];
      final sub = stream.listen(got.add);

      // Notification frame (no matching request id).
      fake.push({
        'result': {
          'id': 'live-uuid-1',
          'action': 'CREATE',
          'result': {'id': 'thing:a', 'name': 'bob'},
        }
      });
      await Future<void>.delayed(Duration.zero);
      expect(got, hasLength(1));
      expect(got.first.action, 'CREATE');
      expect(got.first.value['name'], 'bob');

      // A notification for a different live id is ignored.
      fake.push({
        'result': {'id': 'other', 'action': 'CREATE', 'result': {}}
      });
      await Future<void>.delayed(Duration.zero);
      expect(got, hasLength(1));

      final killFuture = client.kill('live-uuid-1');
      fake.respondLast(null);
      await killFuture;
      await sub.cancel();
    });

    test('a socket that ends reads as closed and fails what was in flight',
        () async {
      expect(client.isConnected, isTrue);
      final disconnected = expectLater(client.onDisconnected, emits(null));
      final pending = client.query('RETURN true');
      final failed = expectLater(pending, throwsA(isA<StateError>()));
      fake.end();
      await disconnected;
      await failed;
      expect(client.isConnected, isFalse,
          reason: 'the revive loop reads this and would do nothing');
      await expectLater(client.query('RETURN true'), throwsA(isA<StateError>()));
    });

    test('forceClose fails what was in flight before the close finishes',
        () async {
      final closing = Completer<void>();
      fake.closeResult = closing.future;
      final pending = client.query('RETURN true');
      final failed = expectLater(pending, throwsA(isA<StateError>()));
      final closed = client.forceClose();
      await failed.timeout(const Duration(seconds: 1));
      closing.complete();
      await closed;
    });

    test('the end of a replaced socket does not touch the current one',
        () async {
      final first = FakeChannel();
      final second = FakeChannel();
      final c = SequenceClient([first, second]);
      await c.connect('ws://x:8000');
      await c.connect('ws://x:8000');
      var disconnects = 0;
      c.onDisconnected.listen((_) => disconnects++);
      final pending = c.query('RETURN true');
      first.end();
      await Future<void>.delayed(Duration.zero);
      expect(c.isConnected, isTrue);
      expect(disconnects, 0);
      second.respondLast([
        {'status': 'OK', 'result': 1}
      ]);
      expect(await pending, [1]);
      await c.close();
    });

    test('an error frame rejects the pending RPC', () async {
      final f = client.query('bad');
      fake.push({
        'id': fake.sent.last['id'],
        'error': {'message': 'parse error'}
      });
      await expectLater(f, throwsA(isA<StateError>()));
    });
  });
}
