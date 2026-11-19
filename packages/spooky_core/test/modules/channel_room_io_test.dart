import 'dart:async';
import 'dart:convert';
import 'dart:io';

import 'package:spooky_core/src/modules/channel/channel.dart';
import 'package:spooky_core/src/modules/channel/room.dart';
import 'package:test/test.dart';

import '../channel_share.dart';
import '../client/channel_client_test.dart' show eventually;

// The room link over real sockets, against a relay on a loopback port: what
// the in-memory relay of `channel_room_test.dart` cannot show (a connection
// that is refused, a server that goes away, the socket's own buffering).

/// Protocol v1 of `apps/relay/README.md` on `127.0.0.1`, without the limits.
/// `t-<name>` is the session token of `user:<name>`.
class LoopbackRelay {
  LoopbackRelay._(this.port);

  /// Start one on [port], or on a free port.
  static Future<LoopbackRelay> start([int port = 0]) async {
    final relay = LoopbackRelay._(port);
    await relay.listen();
    return relay;
  }

  /// A port nothing listens on (it was free a moment ago).
  static Future<int> freePort() async {
    final server = await ServerSocket.bind(InternetAddress.loopbackIPv4, 0);
    final port = server.port;
    await server.close();
    return port;
  }

  int port;
  HttpServer? _server;
  final _sockets = <WebSocket>{};
  final rooms = <String, Map<String, WebSocket>>{};

  /// Sockets accepted so far.
  int accepted = 0;

  /// Do not answer pings.
  bool mute = false;

  String get url => 'ws://127.0.0.1:$port/room';

  Future<void> listen() async {
    final server =
        _server = await HttpServer.bind(InternetAddress.loopbackIPv4, port);
    port = server.port;
    server.listen((request) async {
      if (request.uri.path != '/room' ||
          !WebSocketTransformer.isUpgradeRequest(request)) {
        request.response.statusCode = HttpStatus.notFound;
        await request.response.close();
        return;
      }
      _serve(await WebSocketTransformer.upgrade(request));
    });
  }

  /// The process ends: every socket closes with it.
  Future<void> stop() async {
    final server = _server;
    _server = null;
    // Nobody is told who left: the sockets all go at once.
    rooms.clear();
    for (final socket in _sockets.toList()) {
      unawaited(socket.close());
    }
    await server?.close(force: true);
  }

  void _serve(WebSocket socket) {
    accepted++;
    _sockets.add(socket);
    String? user;
    String? peer;
    void send(WebSocket to, Object? frame) {
      try {
        to.add(jsonEncode(frame));
      } on StateError {
        // It hung up while its last frame was still on the way in.
      }
    }

    socket.listen((data) {
      final frame = (jsonDecode(data as String) as Map).cast<String, dynamic>();
      if (user == null) {
        final token = frame['token'];
        final id = frame['peer'];
        if (frame['t'] != 'hello' ||
            token is! String ||
            !token.startsWith('t-') ||
            id is! String) {
          send(socket, {'t': 'error', 'code': 'unauthorized', 'message': '-'});
          unawaited(socket.close());
          return;
        }
        final name = user = 'user:${token.substring(2)}';
        peer = id;
        final room = rooms.putIfAbsent(name, () => {});
        send(socket, {
          't': 'welcome',
          'user': name,
          'peer': id,
          'peers': room.keys.toList()..sort()
        });
        for (final other in room.values) {
          send(other, {'t': 'join', 'peer': id});
        }
        room[id] = socket;
        return;
      }
      if (frame['t'] == 'ping') {
        if (!mute) send(socket, {'t': 'pong'});
        return;
      }
      if (frame['t'] != 'msg') return;
      final to = frame['to'];
      rooms[user]?.forEach((id, other) {
        if (id == peer || (to is List && !to.contains(id))) return;
        send(other, {'t': 'msg', 'from': peer, 'd': frame['d']});
      });
    }, onDone: () {
      _sockets.remove(socket);
      final room = rooms[user];
      if (room == null || !identical(room[peer], socket)) return;
      room.remove(peer);
      if (room.isEmpty) rooms.remove(user);
      for (final other in room.values) {
        send(other, {'t': 'leave', 'peer': peer});
      }
    }, onError: (Object _) {});
  }
}

Duration ms(int n) => Duration(milliseconds: n);

/// The real socket (no `connect` override), with waits short enough to test.
RoomLinkOptions options(String url, String token, {Duration? beat}) =>
    RoomLinkOptions(
      url: url,
      token: () => token,
      retryBase: ms(20),
      retryMax: ms(100),
      retryRefused: ms(150),
      beat: beat ?? const Duration(seconds: 20),
    );

ChannelTransportFactory viaLoopback(String url, String token) =>
    (hooks) => roomTransport(
        hooks,
        RoomTransportOptions(
            link: (linkHooks) => roomLink(linkHooks, options(url, token))));

/// One member of `user:<name>`'s room and what its link reported.
class Member {
  Member(String url, String name, this.peer, {Duration? beat}) {
    link = roomLink(
        RoomLinkHooks(
          self: () => PeerRef('user:$name', peer),
          onPeer: (p, up) => events.add('${up ? 'up' : 'down'} $p'),
          onMessage: (from, data) => messages.add([from, data]),
          onStatus: status.add,
        ),
        options(url, 't-$name', beat: beat));
  }

  final String peer;
  late final RoomLink link;
  final events = <String>[];
  final messages = <List<Object?>>[];
  final status = <PeerLinkStatus>[];
}

void main() {
  test('two devices meet on the relay and one shows the write of the other',
      () async {
    final relay = await LoopbackRelay.start();
    addTearDown(relay.stop);
    final phone =
        Device('user:ann', viaLoopback(relay.url, 't-ann'), held: [note('1')]);
    final laptop =
        Device('user:ann', viaLoopback(relay.url, 't-ann'), held: [note('1')]);
    addTearDown(phone.m.close);
    addTearDown(laptop.m.close);
    final view = laptop.show(inFolder('a'));
    await eventually(
        () => phone.m.ownState.status, (s) => s == ChannelStatus.connected);
    await eventually(
        () => laptop.m.ownState.status, (s) => s == ChannelStatus.connected);
    expect(relay.rooms['user:ann']!.keys,
        unorderedEquals([phone.m.peerId, laptop.m.peerId]));
    phone.m.local('note', 'note:1', {'title': 'over a socket'}, null);
    await eventually(() => view.last[0]['title'], (t) => t == 'over a socket');
    laptop.m.close();
    await eventually(
        () => phone.m.ownState.status, (s) => s == ChannelStatus.connecting);
    expect(relay.rooms['user:ann'], hasLength(1));
  });

  test('keeps asking a relay that is not there, and joins once it is',
      () async {
    final port = await LoopbackRelay.freePort();
    final a = Member('ws://127.0.0.1:$port/room', 'ann', 'a');
    addTearDown(a.link.close);
    // Refused, a little later each time: after the third it says so.
    await eventually(() => a.status.last, (s) => s == PeerLinkStatus.failed);
    expect(a.status.first, PeerLinkStatus.connecting);
    final relay = await LoopbackRelay.start(port);
    addTearDown(relay.stop);
    await eventually(() => a.status.last, (s) => s == PeerLinkStatus.connected);
    expect(relay.accepted, 1);
  });

  test('comes back after the relay restarts, with everybody down in between',
      () async {
    final relay = await LoopbackRelay.start();
    addTearDown(relay.stop);
    final a = Member(relay.url, 'ann', 'a');
    final b = Member(relay.url, 'ann', 'b');
    addTearDown(a.link.close);
    addTearDown(b.link.close);
    await eventually(() => a.events, (e) => e.contains('up b'));
    await eventually(() => b.events, (e) => e.contains('up a'));
    a.link.send(null, {'n': 1});
    await eventually(() => b.messages, (m) => m.isNotEmpty);
    expect(b.messages, [
      [
        'a',
        {'n': 1}
      ]
    ]);

    await relay.stop();
    await eventually(() => a.events, (e) => e.contains('down b'));
    await eventually(() => b.events, (e) => e.contains('down a'));
    expect(a.status.last, isNot(PeerLinkStatus.connected));
    await relay.listen();
    await eventually(
        () => a.events.where((e) => e == 'up b').length, (n) => n == 2);
    await eventually(
        () => b.events.where((e) => e == 'up a').length, (n) => n == 2);
    b.link.send(['a'], {'n': 2});
    await eventually(() => a.messages, (m) => m.isNotEmpty);
    expect(a.messages.last, [
      'b',
      {'n': 2}
    ]);
  });

  test('is told no for a token the relay does not know, and waits', () async {
    final relay = await LoopbackRelay.start();
    addTearDown(relay.stop);
    final status = <PeerLinkStatus>[];
    final link = roomLink(
        RoomLinkHooks(
          self: () => const PeerRef('user:ann', 'a'),
          onPeer: (p, up) {},
          onMessage: (from, data) {},
          onStatus: status.add,
        ),
        options(relay.url, 'expired'));
    addTearDown(link.close);
    await eventually(() => status, (s) => s.contains(PeerLinkStatus.failed));
    expect(status, [PeerLinkStatus.connecting, PeerLinkStatus.failed]);
    expect(relay.rooms, isEmpty);
    // It asks again after the wait, not in a loop.
    await eventually(() => relay.accepted, (n) => n >= 2);
    expect(relay.accepted, lessThan(4));
  });

  test('hangs up on a relay that stopped answering, and connects anew',
      () async {
    final relay = await LoopbackRelay.start()
      ..mute = true;
    addTearDown(relay.stop);
    final a = Member(relay.url, 'ann', 'a', beat: ms(40));
    addTearDown(a.link.close);
    await eventually(() => relay.accepted, (n) => n >= 2);
    expect(
        a.status.take(3),
        [
          PeerLinkStatus.connecting,
          PeerLinkStatus.connected,
          PeerLinkStatus.connecting
        ],
        reason: 'two beats without a pong');
    relay.mute = false;
    final settled = relay.accepted;
    await eventually(() => a.status.last, (s) => s == PeerLinkStatus.connected);
    await Future<void>.delayed(ms(200));
    expect(relay.accepted, lessThanOrEqualTo(settled + 1),
        reason: 'answered, it stays');
  });
}
