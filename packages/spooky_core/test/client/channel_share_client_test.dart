import 'dart:async';

import 'package:spooky_core/advanced.dart';
import 'package:spooky_core/spooky_core.dart';
import 'package:spooky_core/src/services/persistence/memory_persistence.dart';
import 'package:test/test.dart';

import '../fake_remote.dart';
import '../room_relay.dart';
import '../sync_integration_test.dart' show base64Url;
import 'channel_client_test.dart' show eventually;

// `channels.share` through the client: two devices of one user, each with a
// server of its own, so a row can only get from one to the other through the
// room relay.

const _surql = '''
DEFINE TABLE note SCHEMAFULL PERMISSIONS FULL;
DEFINE FIELD folder ON note TYPE string;
DEFINE FIELD title ON note TYPE string;
''';

/// No `-- @channel` mark: a shared table needs none.
final _schema = <String, dynamic>{
  'note': {
    'columns': {
      'folder': const ColumnSchema(type: 'string'),
      'title': const ColumnSchema(type: 'string'),
    },
  },
};

String _tokenOf(String user) =>
    'h.${base64Url('{"ID":"$user","AC":"account"}')}.s';

/// A relay that knows the session tokens of [users].
FakeRelay _relayFor(List<String> users) {
  final byToken = {for (final user in users) _tokenOf(user): user};
  return FakeRelay(userOf: (token) => byToken[token]);
}

ChannelsConfig _sharing(FakeRelay relay) => ChannelsConfig(
      relay: ChannelRelayConfig('ws://relay.test/room', connect: relay.connect),
      share: const ChannelShareConfig(tables: {'note': ChannelShareRule.all}),
    );

class Device {
  Device._(this.client);

  final InProcessSp00kyClient client;
  List<Map<String, dynamic>> rows = const [];
  StreamSubscription<List<Map<String, dynamic>>>? _sub;

  /// A signed-in device showing the notes of [folder].
  static Future<Device> open(String user, FakeRelay relay,
      {String folder = 'a', bool share = true}) async {
    final remote = FakeRemote()
      ..authUser = user
      ..records[user] = {'id': user};
    final persistence = MemoryPersistenceClient();
    await persistence.set('sp00ky_auth_token', _tokenOf(user));
    final client = InProcessSp00kyClient(
      Sp00kyConfig(
        database: const DatabaseConfig(
            endpoint: 'ws://localhost:8000',
            namespace: 'test',
            database: 'test'),
        schema: _schema,
        schemaSurql: _surql,
        persistenceClient: persistence,
        channels: share ? _sharing(relay) : null,
      ),
      remoteClient: remote,
    );
    await client.init();
    final d = Device._(client);
    final stream =
        await client.query('note').where({'folder': folder}).stream();
    d._sub = stream.listen((r) => d.rows = r);
    return d;
  }

  Map<String, dynamic>? row(String id) =>
      rows.where((r) => r['id'] == id).firstOrNull;

  Future<void> connected() => eventually<ChannelState>(
      () => client.shareState, (s) => s.status == ChannelStatus.connected);

  Future<void> close() async {
    await _sub?.cancel();
    await client.close();
  }
}

void main() {
  test(
      "a write shows on the user's other device before any server has it, "
      'and only where a query can show it', () async {
    final relay = _relayFor(['user:a']);
    final phone = await Device.open('user:a', relay);
    final laptop = await Device.open('user:a', relay);
    addTearDown(phone.close);
    addTearDown(laptop.close);
    await phone.connected();
    await laptop.connected();
    expect(phone.client.shareState.peers, ['user:a']);
    expect(relay.rooms['user:a'], hasLength(2));
    // Each has told the other what its query shows (folder a). Until that
    // word arrives a device counts as showing what it said when they met.
    await pumpEventQueue();

    await phone.client.create('note:1', {'folder': 'a', 'title': 'hi'});
    final seen = await eventually(() => laptop.row('note:1'), (r) => r != null);
    expect(seen!['title'], 'hi');
    expect(laptop.client.pendingMutationCount, 0,
        reason: 'the phone pushes its own write');
    expect((await laptop.client.inspectState()).peerWrites, contains('note:1'));

    await phone.client.update('note', 'note:1', {'title': 'edited'});
    await eventually(
        () => laptop.row('note:1')?['title'], (t) => t == 'edited');

    // A note in another folder is not sent to the laptop at all.
    final before = relay.forwarded;
    await phone.client.create('note:2', {'folder': 'b', 'title': 'elsewhere'});
    await pumpEventQueue();
    expect(relay.forwarded, before);
    expect(await laptop.client.readRow('note:2'), isNull);
    await phone.client.create('note:3', {'folder': 'a', 'title': 'here'});
    await eventually(() => laptop.row('note:3'), (r) => r != null);
    expect(relay.forwarded, before + 1);

    await phone.client.delete('note', 'note:1');
    await eventually(() => laptop.row('note:1'), (r) => r == null);
  });

  test(
      'a stream on raw SurrealQL asks for everything, the client\'s own '
      'modules ask for nothing', () async {
    final relay = _relayFor(['user:a']);
    final phone = await Device.open('user:a', relay);
    final laptop = await Device.open('user:a', relay);
    addTearDown(phone.close);
    addTearDown(laptop.close);
    await phone.connected();
    await laptop.connected();
    await pumpEventQueue();

    /// What the laptop (the second socket) last said it shows.
    Object? said() => [
          for (final frame in relay.sockets[1].sent)
            if (frame case {'d': {'m': {'m': {'t': 'interest'} && final m}}}) m
        ].last;
    const folderA = {
      't': 'interest',
      'v': {
        'note': [
          [
            {'field': 'folder', 'op': '=', 'value': 'a'}
          ]
        ]
      }
    };
    expect(said(), folderA);

    // Feature flags read a table of their own, straight from the engine.
    final off = laptop.client.feature('demo').subscribe((_) {});
    addTearDown(off);
    await Future<void>.delayed(const Duration(milliseconds: 100));
    expect(said(), folderA);

    // No builder described this one: what it shows is anybody's guess.
    final raw = await laptop.client.queryStream('SELECT * FROM note', {});
    final sub = raw.listen((_) {});
    await pumpEventQueue();
    expect(said(), {'t': 'interest', 'all': true});
    await sub.cancel();
    await pumpEventQueue();
    expect(said(), folderA);
  });

  test('shareState follows the other devices coming, going and signing out',
      () async {
    final relay = _relayFor(['user:a', 'user:b']);
    final phone = await Device.open('user:a', relay);
    addTearDown(phone.close);
    final states = <ChannelStatus>[];
    final sub = phone.client.shareStates.listen((s) => states.add(s.status));
    addTearDown(sub.cancel);
    await eventually(() => states, (s) => s.isNotEmpty);
    expect(states, [ChannelStatus.connecting], reason: 'alone in the room');

    // Somebody else's device is no company.
    final bob = await Device.open('user:b', relay);
    addTearDown(bob.close);
    await eventually(() => relay.rooms.length, (n) => n == 2);
    expect(phone.client.shareState.status, ChannelStatus.connecting);

    final laptop = await Device.open('user:a', relay);
    await eventually(() => states.last, (s) => s == ChannelStatus.connected);
    await laptop.connected();
    await laptop.client.auth.signOut();
    await eventually(() => states.last, (s) => s == ChannelStatus.connecting);
    expect(laptop.client.shareState.status, ChannelStatus.idle);
    expect(relay.rooms['user:a'], hasLength(1));
    await laptop.close();
    expect(states, [
      ChannelStatus.connecting,
      ChannelStatus.connected,
      ChannelStatus.connecting
    ]);

    var calls = 0;
    final off = phone.client.subscribeShareState((_) => calls++);
    expect(calls, 1, reason: 'called at once with the current state');
    off();
  });

  test('nothing is shared, and no socket opened, unless share is set',
      () async {
    final relay = _relayFor(['user:a']);
    final plain = await Device.open('user:a', relay, share: false);
    addTearDown(plain.close);
    await plain.client.create('note:1', {'folder': 'a', 'title': 'hi'});
    await pumpEventQueue();
    expect(plain.client.shareState, const ChannelState(ChannelStatus.idle));
    expect(await plain.client.shareStates.first,
        const ChannelState(ChannelStatus.idle));
    expect(relay.sockets, isEmpty);
  });

  group('signed out, a sharing client is idle and still works', () {
    for (final worker in [false, true]) {
      test(worker ? 'background default' : 'in-process', () async {
        final relay = _relayFor(const []);
        final config = Sp00kyConfig(
          database: const DatabaseConfig(namespace: 'test', database: 'test'),
          schema: _schema,
          schemaSurql: _surql,
          channels: _sharing(relay),
        );
        final c = worker ? Sp00kyClient(config) : InProcessSp00kyClient(config);
        addTearDown(c.close);
        await c.init();
        final stream = await c.query('note').where({'folder': 'a'}).stream();
        var rows = <Map<String, dynamic>>[];
        final sub = stream.listen((r) => rows = r);
        addTearDown(sub.cancel);
        await c.create('note:1', {'folder': 'a', 'title': 'hi'});
        await eventually(() => rows, (r) => r.any((x) => x['id'] == 'note:1'));
        await c.update('note', 'note:1', {'title': 'edited'});
        await eventually(() => rows, (r) => r.first['title'] == 'edited');
        await c.delete('note', 'note:1');
        await eventually(() => rows, (r) => r.isEmpty);
        expect(c.shareState, const ChannelState(ChannelStatus.idle));
        expect(relay.sockets, isEmpty, reason: 'nobody to join a room as');
      });
    }
  });
}
