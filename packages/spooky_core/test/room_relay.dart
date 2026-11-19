import 'dart:async';
import 'dart:convert';

import 'package:web_socket_channel/web_socket_channel.dart';

/// A stand-in for `sp00ky-relay`, speaking protocol v1 as
/// `apps/relay/README.md` documents it, in memory. The Dart twin of
/// `FakeRelay` in `packages/core/src/modules/channel/rooms.fixture.ts`.
class FakeRelay {
  FakeRelay({String? Function(Object? token)? userOf})
      : userOf = userOf ?? _userOfTestToken;

  /// Who a session token belongs to, or null to refuse it. By default
  /// `t-<name>` is the token of `user:<name>`.
  final String? Function(Object? token) userOf;

  /// Every socket a client opened, in order.
  final sockets = <FakeSocket>[];

  /// The rooms: user, then peer id, then that member's socket.
  final rooms = <String, Map<String, FakeSocket>>{};

  /// Refuse every connection, as an unreachable relay does.
  bool down = false;

  /// Answer the next hello with this error code instead of a welcome.
  String? refuseWith;

  /// Do not answer pings (a dead connection the client has to notice).
  bool mute = false;

  /// How the welcome spells the user (the database may wrap the key).
  String Function(String user) spellUser = (user) => user;

  /// Frames forwarded between members: how much went through the relay.
  int forwarded = 0;

  /// What a client passes as its socket factory.
  WebSocketChannel connect(Uri uri) => FakeSocket(this, uri);

  static String? _userOfTestToken(Object? token) =>
      token is String && token.startsWith('t-')
          ? 'user:${token.substring(2)}'
          : null;

  void receive(FakeSocket socket, Map<String, dynamic> frame) {
    if (!socket.isOpen) return;
    final member = socket.member;
    if (member == null) {
      final user = userOf(frame['token']);
      final peer = frame['peer'];
      final code = refuseWith ??
          (frame['t'] != 'hello' || peer is! String
              ? 'bad_request'
              : user == null
                  ? 'unauthorized'
                  : null);
      if (code != null || user == null || peer is! String) {
        refuseWith = null;
        socket.push({'t': 'error', 'code': code});
        scheduleMicrotask(socket.drop);
        return;
      }
      final room = rooms.putIfAbsent(user, () => {});
      final replaced = room[peer];
      socket.member = (user: user, peer: peer);
      socket.push({
        't': 'welcome',
        'user': spellUser(user),
        'peer': peer,
        'peers': [
          for (final p in room.keys)
            if (p != peer) p
        ]..sort(),
      });
      room[peer] = socket;
      if (replaced != null) {
        // The seat changes hands: the others see neither a leave nor a join.
        replaced.push({'t': 'error', 'code': 'replaced'});
        scheduleMicrotask(replaced.drop);
        return;
      }
      for (final other in room.values) {
        if (!identical(other, socket)) other.push({'t': 'join', 'peer': peer});
      }
      return;
    }
    if (frame['t'] == 'ping') {
      if (!mute) socket.push({'t': 'pong'});
      return;
    }
    if (frame['t'] != 'msg') return;
    final rawTo = frame['to'];
    final to = rawTo is List ? rawTo.toSet() : null;
    for (final other in rooms[member.user]?.values.toList() ?? const []) {
      final peer = other.member?.peer;
      if (peer == member.peer || (to != null && !to.contains(peer))) continue;
      forwarded++;
      other.push({'t': 'msg', 'from': member.peer, 'd': frame['d']});
    }
  }

  void left(FakeSocket socket) {
    final member = socket.member;
    if (member == null) return;
    socket.member = null;
    final room = rooms[member.user];
    if (room == null || !identical(room[member.peer], socket)) return;
    room.remove(member.peer);
    if (room.isEmpty) rooms.remove(member.user);
    for (final other in room.values) {
      other.push({'t': 'leave', 'peer': member.peer});
    }
  }

  /// The relay tells a member something and hangs up (`reauth`, ...).
  void kick(String user, String peer, String code) {
    final socket = rooms[user]?[peer];
    if (socket == null) return;
    socket.push({'t': 'error', 'code': code});
    scheduleMicrotask(socket.drop);
  }
}

/// One client connection to the [FakeRelay], as the client sees a
/// [WebSocketChannel]: frames written before it is open wait for that, and it
/// ends by its stream closing.
class FakeSocket implements WebSocketChannel {
  FakeSocket(this.relay, this.uri) {
    relay.sockets.add(this);
    scheduleMicrotask(() {
      if (_state != _connecting) return;
      if (relay.down) {
        final error = WebSocketChannelException('connection refused');
        _ready.completeError(error);
        _incoming.addError(error);
        drop();
        return;
      }
      _state = _open;
      _ready.complete();
      final queued = _queued.toList();
      _queued.clear();
      queued.forEach(_deliver);
    });
  }

  static const _connecting = 0;
  static const _open = 1;
  static const _closed = 3;

  final FakeRelay relay;
  final Uri uri;
  int _state = _connecting;
  final _ready = Completer<void>();
  final _incoming = StreamController<dynamic>();
  final _queued = <Map<String, dynamic>>[];

  /// Every frame the client sent, parsed.
  final sent = <Map<String, dynamic>>[];

  /// Who this socket is in its room, once welcomed.
  ({String user, String peer})? member;

  bool get isOpen => _state == _open;
  bool get isClosed => _state == _closed;

  @override
  Future<void> get ready => _ready.future;

  @override
  Stream<dynamic> get stream => _incoming.stream;

  @override
  late final WebSocketSink sink = _FakeSink(this);

  void _send(String text) {
    if (_state == _closed) throw StateError('Cannot add event after closing');
    final frame = (jsonDecode(text) as Map).cast<String, dynamic>();
    sent.add(frame);
    if (_state == _connecting) {
      _queued.add(frame);
    } else {
      _deliver(frame);
    }
  }

  void _deliver(Map<String, dynamic> frame) =>
      scheduleMicrotask(() => relay.receive(this, frame));

  /// The relay's side: a frame for the client.
  void push(Object? frame) {
    final text = jsonEncode(frame);
    scheduleMicrotask(() {
      if (_state == _open) _incoming.add(text);
    });
  }

  /// The connection ends, from either side.
  void drop() {
    if (_state == _closed) return;
    _state = _closed;
    if (!_ready.isCompleted) _ready.complete();
    relay.left(this);
    scheduleMicrotask(_incoming.close);
  }

  /// The client's end dies and the relay does not notice (a phone changing
  /// networks): the seat stays taken until its peer id says hello again.
  void lose() {
    if (_state == _closed) return;
    _state = _closed;
    if (!_ready.isCompleted) _ready.complete();
    scheduleMicrotask(_incoming.close);
  }

  @override
  dynamic noSuchMethod(Invocation invocation) => super.noSuchMethod(invocation);
}

class _FakeSink implements WebSocketSink {
  _FakeSink(this._socket);
  final FakeSocket _socket;

  @override
  void add(dynamic data) => _socket._send(data as String);

  @override
  Future<void> close([int? closeCode, String? closeReason]) async =>
      _socket.drop();

  @override
  dynamic noSuchMethod(Invocation invocation) => super.noSuchMethod(invocation);
}

/// A member of a room that is not this client: it says hello and then sends
/// and records raw frames, the way a test wants another implementation (a
/// browser's hub) to have sent them.
class RawMember {
  RawMember(FakeRelay relay,
      {required String token, required this.peer, this.onFrame})
      : socket = FakeSocket(relay, Uri.parse('ws://relay.test/room')) {
    socket.stream.listen((data) {
      final frame = (jsonDecode(data as String) as Map).cast<String, dynamic>();
      frames.add(frame);
      onFrame?.call(this, frame);
    });
    socket.sink
        .add(jsonEncode({'t': 'hello', 'v': 1, 'token': token, 'peer': peer}));
  }

  /// Called with every frame the relay sends it, to answer as the other
  /// implementation would.
  final void Function(RawMember self, Map<String, dynamic> frame)? onFrame;

  final String peer;
  final FakeSocket socket;

  /// Every frame the relay sent it.
  final frames = <Map<String, dynamic>>[];

  /// The payloads (`d`) the other members sent it, each after its sender.
  List<List<Object?>> get received => [
        for (final f in frames)
          if (f['t'] == 'msg') [f['from'], f['d']]
      ];

  /// A `msg` frame carrying [d], to the members named or (null) to all.
  void send(Object? d, {List<String>? to}) => socket.sink
      .add(jsonEncode({'t': 'msg', if (to != null) 'to': to, 'd': d}));

  void leave() => socket.drop();
}
