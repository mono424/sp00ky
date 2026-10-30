import 'dart:async';

import 'package:spooky_core/advanced.dart';
import 'package:spooky_core/spooky_core.dart';
import 'package:spooky_core/src/services/persistence/memory_persistence.dart';
import 'package:test/test.dart';

import '../channel_net.dart';
import '../fake_remote.dart';
import '../sync_integration_test.dart' show base64Url;

const _surql = '''
DEFINE TABLE msg SCHEMAFULL PERMISSIONS FULL;
DEFINE FIELD convo ON msg TYPE string;
DEFINE FIELD text ON msg TYPE string;
DEFINE FIELD mood ON msg TYPE option<string>;
''';

final _schema = <String, dynamic>{
  'msg': {
    'columns': {
      'convo': const ColumnSchema(type: 'string'),
      'text': const ColumnSchema(type: 'string'),
      'mood':
          const ColumnSchema(type: 'string', optional: true, ephemeral: true),
    },
    'channel': true,
  },
};

/// Polls [read] until [ok] holds; the runtime materializes on its own timers.
Future<T> eventually<T>(T Function() read, bool Function(T) ok,
    {Duration timeout = const Duration(seconds: 5)}) async {
  final until = DateTime.now().add(timeout);
  while (true) {
    final v = read();
    if (ok(v)) return v;
    if (DateTime.now().isAfter(until)) {
      fail('condition not met; last value: $v');
    }
    await Future<void>.delayed(const Duration(milliseconds: 10));
  }
}

class Device {
  Device._(this.client, this.remote);

  final InProcessSp00kyClient client;
  final FakeRemote remote;
  List<Map<String, dynamic>> rows = const [];
  StreamSubscription<List<Map<String, dynamic>>>? _sub;

  static Future<Device> open(String user, MemoryNet net) async {
    final remote = FakeRemote()
      ..authUser = user
      ..records[user] = {'id': user};
    final persistence = MemoryPersistenceClient();
    await persistence.set('sp00ky_auth_token',
        'h.${base64Url('{"ID":"$user","AC":"account"}')}.s');
    final client = InProcessSp00kyClient(
      Sp00kyConfig(
        database: const DatabaseConfig(
            endpoint: 'ws://localhost:8000',
            namespace: 'test',
            database: 'test'),
        schema: _schema,
        schemaSurql: _surql,
        persistenceClient: persistence,
        channels: ChannelsConfig(
          transport: net.factory,
          accept: {'msg': (op) => op.patch['text'] != 'spam'},
        ),
      ),
      remoteClient: remote,
    );
    await client.init();
    final d = Device._(client, remote);
    final stream = await client.query('msg').stream();
    d._sub = stream.listen((r) => d.rows = r);
    return d;
  }

  Map<String, dynamic>? row(String id) =>
      rows.where((r) => r['id'] == id).firstOrNull;

  Future<void> close() async {
    await _sub?.cancel();
    await client.close();
  }
}

void main() {
  group('peer rows and signals cross the worker boundary', () {
    for (final worker in [false, true]) {
      test(worker ? 'background default' : 'in-process', () async {
        final config = Sp00kyConfig(
          database: const DatabaseConfig(namespace: 'test', database: 'test'),
          schema: _schema,
          schemaSurql: _surql,
          channels: ChannelsConfig(accept: {'msg': (_) => true}),
        );
        final c = worker ? Sp00kyClient(config) : InProcessSp00kyClient(config);
        addTearDown(c.close);
        await c.init();
        final stream = await c.query('msg').stream();
        var rows = <Map<String, dynamic>>[];
        final sub = stream.listen((r) => rows = r);
        addTearDown(sub.cancel);

        expect(
            await c.peerCreate('msg:1', {'convo': 'c1', 'text': 'hi'}, 60000),
            isTrue);
        expect(await c.peerCreate('msg:1', {'convo': 'c1', 'text': 'x'}, 60000),
            isFalse,
            reason: 'one unconfirmed peer write per row');
        expect((await c.readRow('msg:1'))?['text'], 'hi');
        await eventually(() => rows, (r) => r.any((x) => x['id'] == 'msg:1'));
        expect(c.pendingMutationCount, 0);

        await c.create('msg:2', {'convo': 'c1', 'text': 'own'});
        expect(await c.peerDelete('msg:2', 60000), isTrue);
        await eventually(() => rows, (r) => r.every((x) => x['id'] != 'msg:2'));

        await expectLater(
            c.channelSignal.send(const OutgoingSignal(
                to: 'user:b',
                toPeer: null,
                fromPeer: 'p',
                kind: SignalKind.hello,
                payload: null)),
            throwsA(anything),
            reason: 'no server to signal through');
        c.channelSignal.listen(const PeerRef('user:a', 'p'), (_) {})();
      });
    }
  });

  late MemoryNet net;
  late Device a;
  late Device b;

  setUp(() async {
    net = MemoryNet();
    a = await Device.open('user:a', net);
    b = await Device.open('user:b', net);
  });
  tearDown(() async {
    await a.close();
    await b.close();
  });

  Future<(Sp00kyChannel, Sp00kyChannel)> connect() async {
    final ca = a.client
        .channel(a.client.query('msg').where({'convo': 'c1'}), ['user:b']);
    final cb = b.client.channel(b.client.query('msg').where({'convo': 'c1'}),
        [const RecordId('user', 'a')]);
    await eventually(
        () => ca.state.status, (s) => s == ChannelStatus.connected);
    await eventually(
        () => cb.state.status, (s) => s == ChannelStatus.connected);
    expect(ca.state.peers, ['user:b']);
    return (ca, cb);
  }

  test(
      'a create reaches the peer store and circuit, ephemeral fields only its overlay',
      () async {
    await connect();
    final out = await a.client
        .create('msg:1', {'convo': 'c1', 'text': 'hi', 'mood': 'happy'});
    expect(out['mood'], 'happy', reason: 'the caller sees what it wrote');
    expect(a.client.pendingMutationCount, 1);

    final seen = await eventually(() => b.row('msg:1'), (r) => r != null);
    expect(seen!['text'], 'hi');
    expect(seen['mood'], 'happy');
    expect(b.client.pendingMutationCount, 0,
        reason: "a peer's create never enters our outbox");
    final stored = await b.client.readRow('msg:1');
    expect(stored?['text'], 'hi');
    expect(stored?.containsKey('mood'), isFalse,
        reason: 'ephemeral values never reach a store');
    expect((await b.client.inspectState()).peerWrites, contains('msg:1'));
  });

  test(
      'an ephemeral update is peers-only; a persistent one shows before the server has it',
      () async {
    await connect();
    await a.client.create('msg:1', {'convo': 'c1', 'text': 'hi'});
    await eventually(() => b.row('msg:1'), (r) => r != null);
    final pending = a.client.pendingMutationCount;

    await a.client.update('msg', 'msg:1', {'mood': 'typing'});
    expect(a.client.pendingMutationCount, pending,
        reason: 'an ephemeral-only update writes nothing');
    await eventually(() => b.row('msg:1')?['mood'], (m) => m == 'typing');
    expect(a.row('msg:1')?['mood'], 'typing',
        reason: 'own ephemeral values show locally too');

    await a.client.update('msg', 'msg:1', {'text': 'edited'});
    await eventually(() => b.row('msg:1')?['text'], (t) => t == 'edited');
  });

  test('a delete hides the row on the peer', () async {
    await connect();
    await a.client.create('msg:1', {'convo': 'c1', 'text': 'hi'});
    await eventually(() => b.row('msg:1'), (r) => r != null);
    await a.client.delete('msg', 'msg:1');
    await eventually(() => b.row('msg:1'), (r) => r == null);
  });

  test('rows outside the filter and refused rows stay home', () async {
    final (ca, _) = await connect();
    await a.client.create('msg:2', {'convo': 'c2', 'text': 'elsewhere'});
    await a.client.create('msg:3', {'convo': 'c1', 'text': 'spam'});
    await a.client.create('msg:4', {'convo': 'c1', 'text': 'ok'});
    await eventually(() => b.row('msg:4'), (r) => r != null);
    expect(b.row('msg:2'), isNull);
    expect(b.row('msg:3'), isNull, reason: 'the accept rule refused it');

    expect(() => ca.update('note', 'note:1', {}), throwsArgumentError);
    await ca.create('msg:5', {'convo': 'c1', 'text': 'scoped'});
    await eventually(() => b.row('msg:5'), (r) => r != null);
    ca.close();
    expect(ca.isClosed, isTrue);
  });

  test('a channel needs a @channel table', () {
    expect(() => a.client.channel(a.client.query('nope'), ['user:b']),
        throwsArgumentError);
    expect(() => a.client.channel(42, ['user:b']), throwsArgumentError);
  });
}
