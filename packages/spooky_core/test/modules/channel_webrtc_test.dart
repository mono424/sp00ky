import 'dart:async';
import 'dart:convert';

import 'package:spooky_core/src/modules/channel/channel.dart';
import 'package:spooky_core/src/modules/channel/webrtc.dart';
import 'package:spooky_core/src/surreal/remote_client.dart' show LiveMessage;
import 'package:spooky_core/src/surreal/value.dart';
import 'package:spooky_core/src/types.dart' show ConnectionState;
import 'package:test/test.dart';

class FakeChannel implements RtcDataChannel {
  bool _open = false;
  bool _closed = false;
  void Function()? _onOpen;
  void Function()? _onClose;
  void Function(String)? _onMessage;
  FakeChannel? other;

  @override
  set onOpen(void Function()? cb) => _onOpen = cb;
  @override
  set onClose(void Function()? cb) => _onClose = cb;
  @override
  set onMessage(void Function(String)? cb) => _onMessage = cb;
  @override
  bool get isOpen => _open;

  @override
  void send(String text) =>
      scheduleMicrotask(() => other?._onMessage?.call(text));

  @override
  Future<void> close() async {
    if (_closed) return;
    _closed = true;
    _open = false;
    _onClose?.call();
    await other?.close();
  }

  void open() {
    _open = true;
    _onOpen?.call();
  }
}

/// Offers by SDP, so the answering side can find who it is talking to.
final _offers = <String, FakePC>{};
final _all = <FakePC>{};
var _sdpSeq = 0;

class FakePC implements RtcPeerConnection {
  FakePC() {
    _all.add(this);
  }

  void Function(Map<String, dynamic>)? _onIce;
  void Function(RtcDataChannel)? _onDataChannel;
  FakeChannel? dc;
  FakePC? remote;
  RtcDescription? local;
  final ice = <Map<String, dynamic>>[];
  bool closed = false;

  @override
  set onIceCandidate(void Function(Map<String, dynamic>)? cb) => _onIce = cb;
  @override
  set onConnectionState(void Function(String)? cb) {}
  @override
  set onDataChannel(void Function(RtcDataChannel)? cb) => _onDataChannel = cb;

  @override
  Future<RtcDataChannel> createDataChannel(String label,
      {bool ordered = true}) async {
    expect(label, 'sp00ky');
    expect(ordered, isTrue);
    return dc = FakeChannel();
  }

  @override
  Future<RtcDescription> createOffer() async {
    final sdp = 'offer-${++_sdpSeq}';
    _offers[sdp] = this;
    return RtcDescription('offer', sdp);
  }

  @override
  Future<RtcDescription> createAnswer() async =>
      RtcDescription('answer', 'answer-${++_sdpSeq}');

  @override
  Future<void> setLocalDescription(RtcDescription d) async {
    local = d;
    // Two candidates in one burst: they go out as one batched signal.
    scheduleMicrotask(() {
      _onIce?.call({'candidate': 'c1', 'sdpMid': '0', 'sdpMLineIndex': 0});
      _onIce?.call({'candidate': 'c2', 'sdpMid': '0', 'sdpMLineIndex': 0});
    });
  }

  @override
  Future<void> setRemoteDescription(RtcDescription d) async {
    if (d.type == 'offer') remote = _offers[d.sdp];
    if (d.type == 'answer') {
      // The offerer learns the answer: wire both data channels and open them.
      final answerer = _all.where((p) => identical(p.remote, this)).lastOrNull;
      final mine = dc;
      if (answerer == null || mine == null) return;
      final theirs = FakeChannel();
      mine.other = theirs;
      theirs.other = mine;
      answerer._onDataChannel?.call(theirs);
      scheduleMicrotask(() {
        mine.open();
        theirs.open();
      });
    }
  }

  @override
  Future<RtcDescription?> localDescription() async => local;

  @override
  Future<void> addIceCandidate(Map<String, dynamic> candidate) async =>
      ice.add(candidate);

  @override
  Future<void> close() async {
    closed = true;
    await dc?.close();
  }
}

/// Signals between users, delivered like the LIVE would: to every device of
/// the recipient.
class MemorySignals {
  final listeners = <String, Set<void Function(Signal)>>{};
  final sent = <({String from, SignalKind kind, String? payload})>[];

  SignalPort port(String user) => _Port(this, user);
}

class _Port implements SignalPort {
  _Port(this.net, this.user);
  final MemorySignals net;
  final String user;

  @override
  Future<void> send(OutgoingSignal s) async {
    net.sent.add((from: user, kind: s.kind, payload: s.payload));
    final signal = Signal(
        from: user,
        fromPeer: s.fromPeer,
        toPeer: s.toPeer,
        kind: s.kind,
        payload: s.payload);
    scheduleMicrotask(() {
      for (final cb in [...?net.listeners[s.to]]) {
        cb(signal);
      }
    });
  }

  @override
  void Function() listen(PeerRef self, void Function(Signal) cb) {
    (net.listeners[self.user] ??= {}).add(cb);
    return () => net.listeners[self.user]?.remove(cb);
  }
}

class Hooks implements TransportHooks {
  Hooks(this.user, this.peer);
  final String user;
  final String peer;
  final events = <String>[];
  final messages = <Object?>[];
  final statuses = <String>[];

  @override
  PeerRef? self() => PeerRef(user, peer);
  @override
  void onMessage(PeerRef from, Object? message) => messages.add(message);
  @override
  void onPeer(PeerRef peer, bool up) =>
      events.add('${up ? 'up' : 'down'} ${peer.user}');
  @override
  void onStatus(String user, PeerLinkStatus status) =>
      statuses.add('$user ${status.name}');
}

({ChannelTransport t, Hooks hooks}) device(
    String user, String peer, MemorySignals signals,
    {bool rtc = true}) {
  final hooks = Hooks(user, peer);
  final t = webrtcTransport(
      hooks,
      WebRtcOptions(
        signal: signals.port(user),
        rtc: rtc ? (_) async => FakePC() : null,
      ));
  return (t: t, hooks: hooks);
}

Future<void> settle() async {
  for (var i = 0; i < 20; i++) {
    await Future<void>.delayed(Duration.zero);
  }
  // ICE batches flush after 20 ms.
  await Future<void>.delayed(const Duration(milliseconds: 30));
  for (var i = 0; i < 20; i++) {
    await Future<void>.delayed(Duration.zero);
  }
}

void main() {
  setUp(() {
    _offers.clear();
    _all.clear();
  });

  test('connects two devices that want each other, whichever starts first',
      () async {
    final signals = MemorySignals();
    final a = device('user:a', 'pa', signals);
    final b = device('user:b', 'pb', signals);
    a.t.want({'user:b'});
    await settle();
    expect(a.hooks.events, isEmpty);
    b.t.want({'user:a'});
    await settle();
    expect(a.hooks.events, ['up user:b']);
    expect(b.hooks.events, ['up user:a']);
    a.t.send(
        const PeerRef('user:b', 'pb'),
        const ChannelMessage(
            table: 'call',
            id: 'call:1',
            patch: {'view_pos': '1'},
            hlc: Hlc(1, 0, 'pa')));
    await settle();
    expect(b.hooks.messages, [
      {
        't': 'op',
        'table': 'call',
        'id': 'call:1',
        'patch': {'view_pos': '1'},
        'hlc': [1, 0, 'pa'],
      }
    ]);
    // The lower `user#peer` key offers; candidates trickle as one batch per
    // burst, tagged with the negotiation they belong to.
    final offers = signals.sent.where((s) => s.kind == SignalKind.offer);
    expect(offers.single.from, 'user:a');
    final ice = signals.sent.where((s) => s.kind == SignalKind.ice).toList();
    expect(ice, hasLength(2), reason: 'one batch per side');
    final batch = jsonDecode(ice.first.payload!) as Map;
    expect(batch['candidates'], hasLength(2));
    expect(batch['sid'], isA<String>());
    a.t.close();
    b.t.close();
  });

  test('the higher key answers a broadcast hello with a directed one',
      () async {
    final signals = MemorySignals();
    final b = device('user:b', 'pb', signals);
    final a = device('user:a', 'pa', signals);
    b.t.want({'user:a'});
    a.t.want({'user:b'});
    await settle();
    expect(a.hooks.events, ['up user:b']);
    expect(b.hooks.events, ['up user:a']);
    expect(signals.sent.where((s) => s.kind == SignalKind.offer).single.from,
        'user:a');
    a.t.close();
    b.t.close();
  });

  test('never connects to a user who does not want it back', () async {
    final signals = MemorySignals();
    final a = device('user:a', 'pa', signals);
    final b = device('user:b', 'pb', signals);
    a.t.want({'user:b'});
    b.t.want({'user:c'});
    await settle();
    expect(a.hooks.events, isEmpty);
    expect(b.hooks.events, isEmpty);
    expect(signals.sent.where((s) => s.kind == SignalKind.offer), isEmpty);
    a.t.close();
    b.t.close();
  });

  test('closes the link when the user is no longer wanted', () async {
    final signals = MemorySignals();
    final a = device('user:a', 'pa', signals);
    final b = device('user:b', 'pb', signals);
    a.t.want({'user:b'});
    b.t.want({'user:a'});
    await settle();
    a.t.want({});
    await settle();
    expect(a.hooks.events, ['up user:b', 'down user:b']);
    expect(b.hooks.events, ['up user:a', 'down user:a']);
    a.t.close();
    b.t.close();
  });

  test('is unsupported without a peer connection factory, and sends nothing',
      () async {
    final signals = MemorySignals();
    final a = device('user:a', 'pa', signals, rtc: false);
    expect(a.t.supported, isFalse);
    a.t.want({'user:b'});
    await settle();
    expect(signals.sent, isEmpty);
  });

  group('surrealSignal', () {
    test('writes a row per signal and reads its LIVE, re-armed on reconnect',
        () async {
      final queries = <(String, Map<String, dynamic>)>[];
      final lives = <StreamController<LiveMessage>>[];
      final killed = <String>[];
      void Function(ConnectionState)? connection;
      final port = surrealSignal(SignalRemote(
        query: (sql, vars) async {
          queries.add((sql, vars));
          return [];
        },
        live: (sql, vars) async {
          expect(
              sql, contains('FROM _00_channel_signal WHERE recipient = \$me'));
          expect(vars['me'], const RecordId('user', 'a'));
          final c = StreamController<LiveMessage>.broadcast();
          lives.add(c);
          return ('live-${lives.length}', c.stream);
        },
        kill: (id) async => killed.add(id),
        subscribeConnection: (cb) {
          connection = cb;
          cb(ConnectionState.connected);
          return () => connection = null;
        },
      ));

      await port.send(const OutgoingSignal(
          to: 'user:b',
          toPeer: null,
          fromPeer: 'pa',
          kind: SignalKind.hello,
          payload: null));
      expect(queries.single.$1,
          r'CREATE _00_channel_signal CONTENT $c RETURN NONE');
      expect(queries.single.$2['c'], {
        'recipient': const RecordId('user', 'b'),
        'sender_peer': 'pa',
        'kind': 'hello',
      });

      final got = <Signal>[];
      final stop = port.listen(const PeerRef('user:a', 'pa'), got.add);
      await pumpEventQueue();
      lives.single.add(const LiveMessage('CREATE', {
        'sender': RecordId('user', 'b'),
        'sender_peer': 'pb',
        'recipient_peer': 'pa',
        'kind': 'offer',
        'payload': '{}',
      }));
      lives.single.add(const LiveMessage('DELETE', {'sender': 'user:b'}));
      lives.single.add(const LiveMessage('CREATE', {'sender': 'user:b'}));
      await pumpEventQueue();
      expect(got, hasLength(1));
      expect(got.single.from, 'user:b');
      expect(got.single.kind, SignalKind.offer);
      expect(got.single.toPeer, 'pa');

      connection!(ConnectionState.reconnecting);
      connection!(ConnectionState.connected);
      await pumpEventQueue();
      expect(lives, hasLength(2), reason: 'a reconnect re-arms the LIVE');
      expect(killed, ['live-1']);
      stop();
      await pumpEventQueue();
      expect(killed, ['live-1', 'live-2']);
      expect(connection, isNull);
    });
  });
}
