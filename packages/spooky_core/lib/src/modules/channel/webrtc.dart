import 'dart:async';
import 'dart:convert';
import 'dart:math';

import '../../services/logger/logger.dart';
import '../../surreal/remote_client.dart' show LiveMessage;
import '../../surreal/value.dart';
import '../../types.dart' show ConnectionState;
import 'channel.dart';

/// The default channel transport: one WebRTC data channel per pair of devices,
/// set up through signaling rows in the database. Port of
/// `packages/core/src/modules/channel/webrtc.ts`, and the same protocol, so a
/// Dart device and a browser tab connect to each other.
///
/// Signaling (all rows of `_00_channel_signal`, read through one raw LIVE):
///
/// 1. A device that wants user U writes `hello` to U. Every device of U hears
///    it.
/// 2. Of each pair of devices, the one with the lower `user#peer` key offers.
///    A device that hears a hello from a higher key offers at once; one that
///    hears it from a lower key answers with a `hello` addressed back to that
///    device, which then offers. So both orders of arrival connect, and nobody
///    loops.
/// 3. `offer` / `answer` carry the SDPs, `ice` batches of candidates
///    (trickled, so nothing waits for gathering to finish).
///
/// A device only ever reacts to signals from users it wants itself, so a
/// connection needs both sides to list each other. The database stamps every
/// row's `sender`, and the offer carries the DTLS fingerprint of the
/// connection, so the user on the other end of a data channel is proven.

enum SignalKind { hello, offer, answer, ice }

class Signal {
  const Signal({
    required this.from,
    required this.fromPeer,
    required this.toPeer,
    required this.kind,
    required this.payload,
  });

  final String from;
  final String fromPeer;
  final String? toPeer;
  final SignalKind kind;
  final String? payload;
}

class OutgoingSignal {
  const OutgoingSignal({
    required this.to,
    required this.toPeer,
    required this.fromPeer,
    required this.kind,
    required this.payload,
  });

  final String to;
  final String? toPeer;
  final String fromPeer;
  final SignalKind kind;
  final String? payload;
}

/// Where signals go and come from. The database by default.
abstract interface class SignalPort {
  Future<void> send(OutgoingSignal signal);
  void Function() listen(PeerRef self, void Function(Signal signal) cb);
}

class WebRtcOptions {
  const WebRtcOptions({
    required this.signal,
    this.rtc,
    this.iceServers,
    this.logger,
    this.connectTimeout = const Duration(seconds: 15),
  });

  final SignalPort signal;

  /// Builds peer connections; without it the transport is unsupported.
  final RtcPeerConnectionFactory? rtc;
  final List<Map<String, dynamic>>? iceServers;
  final SpookyLogger? logger;

  /// Give up on one attempt after this long.
  final Duration connectTimeout;
}

const _defaultIce = [
  {'urls': 'stun:stun.cloudflare.com:3478'},
];
const _iceFlush = Duration(milliseconds: 20);
const _retryBaseMs = 2000;
const _retryMaxMs = 30000;

class _Link {
  _Link(this.ref, this.offerer, this.sid);

  final PeerRef ref;
  final bool offerer;

  /// One negotiation: answers and candidates of another are ignored.
  final String sid;
  late final Future<RtcPeerConnection> pc;
  RtcDataChannel? dc;
  bool open = false;

  /// An answer was taken (offerer) or the offer applied (answerer).
  bool described = false;
  bool remoteSet = false;
  final List<Map<String, dynamic>> pendingIce = [];
  final List<Map<String, dynamic>> outIce = [];
  Timer? iceTimer;
  Timer? connectTimer;

  /// Signals of this link go out in order (offer before its candidates).
  Future<void> queue = Future.value();
  bool closed = false;

  String get key => ref.key;
}

class _Retry {
  int attempts = 0;
  Timer? timer;
}

final _random = Random();
String _randomSid() => [
      for (var i = 0; i < 8; i++)
        '0123456789abcdefghijklmnopqrstuvwxyz'[_random.nextInt(36)],
    ].join();

ChannelTransport webrtcTransport(TransportHooks hooks, WebRtcOptions opts) =>
    _WebRtcTransport(hooks, opts);

class _WebRtcTransport implements ChannelTransport {
  _WebRtcTransport(this.hooks, this.opts);

  final TransportHooks hooks;
  final WebRtcOptions opts;
  final Map<String, _Link> _links = {};
  final Map<String, _Retry> _retries = {};
  Set<String> _wanted = {};
  void Function()? _stopListen;
  String? _listeningAs;
  bool _closed = false;

  SpookyLogger? get _log => opts.logger;

  @override
  bool get supported => opts.rtc != null;

  @override
  void want(Set<String> users) {
    if (_closed || opts.rtc == null) {
      _wanted = {...users};
      return;
    }
    final before = _wanted;
    _wanted = {...users};
    if (_wanted.isNotEmpty) _ensureListening();
    for (final link in _links.values.toList()) {
      if (!_wanted.contains(link.ref.user)) _dropLink(link, false);
    }
    for (final MapEntry(:key, value: r) in _retries.entries) {
      if (!_wanted.contains(key.substring(0, key.lastIndexOf('#')))) {
        r.timer?.cancel();
        r.timer = null;
      }
    }
    for (final user in _wanted) {
      if (before.contains(user)) continue;
      _signal(null, user, null, SignalKind.hello, null);
      hooks.onStatus(user, PeerLinkStatus.connecting);
    }
  }

  @override
  void send(PeerRef to, ChannelMessage message) {
    final link = _links[to.key];
    final dc = link?.dc;
    if (link == null || !link.open || dc == null || !dc.isOpen) return;
    try {
      dc.send(jsonEncode(message.toJson()));
    } catch (error) {
      _log?.debug('channel send failed: $error');
    }
  }

  @override
  void close() {
    _closed = true;
    for (final r in _retries.values) {
      r.timer?.cancel();
    }
    _retries.clear();
    for (final link in _links.values.toList()) {
      _dropLink(link, false);
    }
    _stopListen?.call();
    _stopListen = null;
  }

  String? _selfKey() => hooks.self()?.key;

  void _signal(_Link? link, String to, String? toPeer, SignalKind kind,
      String? payload) {
    final self = hooks.self();
    if (self == null) return;
    final out = OutgoingSignal(
        to: to,
        toPeer: toPeer,
        fromPeer: self.peer,
        kind: kind,
        payload: payload);
    Future<void> run() => opts.signal.send(out).catchError((Object error) {
          _log?.debug('channel signal (${kind.name}) failed: $error');
        });
    if (link != null) {
      link.queue = link.queue.then((_) => run());
    } else {
      unawaited(run());
    }
  }

  void _dropLink(_Link link, bool failed) {
    if (link.closed) return;
    link.closed = true;
    link.iceTimer?.cancel();
    link.connectTimer?.cancel();
    final dc = link.dc;
    if (dc != null) unawaited(dc.close().catchError((Object _) {}));
    unawaited(link.pc
        .then((pc) => pc.close())
        .catchError((Object _) {/* already gone */}));
    if (identical(_links[link.key], link)) _links.remove(link.key);
    if (link.open) {
      link.open = false;
      hooks.onPeer(link.ref, false);
    }
    if (failed && _wanted.contains(link.ref.user) && !_closed) {
      final anyOpen =
          _links.values.any((l) => l.ref.user == link.ref.user && l.open);
      if (!anyOpen) hooks.onStatus(link.ref.user, PeerLinkStatus.failed);
      _scheduleRetry(link.ref);
    }
  }

  void _scheduleRetry(PeerRef ref) {
    final key = ref.key;
    final r = _retries.putIfAbsent(key, _Retry.new);
    if (r.timer != null) return;
    final delay = min(_retryMaxMs, _retryBaseMs * pow(2, r.attempts).toInt());
    r.attempts++;
    r.timer = Timer(Duration(milliseconds: delay), () {
      r.timer = null;
      if (_closed || !_wanted.contains(ref.user) || _links.containsKey(key)) {
        return;
      }
      final mine = _selfKey();
      if (mine == null) return;
      if (mine.compareTo(key) < 0) {
        _startOffer(ref);
      } else {
        _signal(null, ref.user, ref.peer, SignalKind.hello, null);
      }
    });
  }

  void _attachChannel(_Link link, RtcDataChannel dc) {
    link.dc = dc;
    dc.onOpen = () {
      if (link.closed) return;
      link.open = true;
      link.connectTimer?.cancel();
      link.connectTimer = null;
      _retries.remove(link.key);
      hooks.onPeer(link.ref, true);
      hooks.onStatus(link.ref.user, PeerLinkStatus.connected);
    };
    dc.onClose = () => _dropLink(link, true);
    dc.onMessage = (text) {
      Object? message;
      try {
        message = jsonDecode(text);
      } on FormatException {
        return;
      }
      hooks.onMessage(link.ref, message);
    };
  }

  _Link? _newLink(PeerRef ref, bool offerer, String sid) {
    final rtc = opts.rtc;
    if (rtc == null) return null;
    final prev = _links[ref.key];
    if (prev != null) _dropLink(prev, false);
    final link = _Link(ref, offerer, sid);
    _links[link.key] = link;
    link.pc = rtc({'iceServers': opts.iceServers ?? _defaultIce});
    // Registered before anything else awaits the connection, so the handlers
    // are in place before a description is applied.
    unawaited(link.pc.then((pc) {
      if (link.closed) {
        unawaited(pc.close().catchError((Object _) {}));
        return;
      }
      pc.onIceCandidate = (candidate) {
        if (link.closed) return;
        link.outIce.add(candidate);
        link.iceTimer ??= Timer(_iceFlush, () {
          link.iceTimer = null;
          final candidates = List.of(link.outIce);
          link.outIce.clear();
          if (candidates.isEmpty || link.closed) return;
          _signal(link, ref.user, ref.peer, SignalKind.ice,
              jsonEncode({'sid': sid, 'candidates': candidates}));
        });
      };
      pc.onConnectionState = (state) {
        if (state == 'failed' || state == 'closed') _dropLink(link, true);
      };
      if (!offerer) pc.onDataChannel = (dc) => _attachChannel(link, dc);
    }, onError: (Object error) {
      _log?.debug('channel peer connection failed: $error');
      _dropLink(link, true);
    }));
    link.connectTimer = Timer(opts.connectTimeout, () {
      if (!link.open) _dropLink(link, true);
    });
    hooks.onStatus(ref.user, PeerLinkStatus.connecting);
    return link;
  }

  void _startOffer(PeerRef ref) {
    final link = _newLink(ref, true, _randomSid());
    if (link == null) return;
    unawaited(() async {
      try {
        final pc = await link.pc;
        if (link.closed) return;
        _attachChannel(link, await pc.createDataChannel('sp00ky'));
        final offer = await pc.createOffer();
        await pc.setLocalDescription(offer);
        final local = await pc.localDescription();
        if (link.closed) return;
        _signal(link, ref.user, ref.peer, SignalKind.offer,
            jsonEncode({'sid': link.sid, 'sdp': local?.sdp ?? offer.sdp}));
      } catch (error) {
        _log?.debug('channel offer failed: $error');
        _dropLink(link, true);
      }
    }());
  }

  Future<void> _addIce(
      _Link link, List<Map<String, dynamic>> candidates) async {
    final pc = await link.pc;
    for (final c in candidates) {
      try {
        await pc.addIceCandidate(c);
      } catch (_) {
        // a candidate of a torn-down negotiation, or one the platform rejects
      }
    }
  }

  void _onSignal(Signal s) {
    if (_closed) return;
    final self = hooks.self();
    if (self == null) return;
    final from = idKey(s.from);
    if (from == null || !_wanted.contains(from)) return;
    if (s.toPeer != null && s.toPeer != self.peer) return;
    final ref = PeerRef(from, s.fromPeer);
    final key = ref.key;
    final mine = self.key;
    if (key == mine) return;
    final payload = _parsePayload(s.payload);
    final link = _links[key];

    switch (s.kind) {
      case SignalKind.hello:
        if (link != null && !link.closed) return;
        if (mine.compareTo(key) < 0) {
          _startOffer(ref);
        } else if (s.toPeer == null) {
          _signal(null, from, ref.peer, SignalKind.hello, null);
        }
      case SignalKind.offer:
        final sdp = payload?['sdp'];
        final sid = payload?['sid'];
        if (mine.compareTo(key) < 0 || sdp is! String || sid is! String) return;
        final answerer = _newLink(ref, false, sid);
        if (answerer == null) return;
        answerer.described = true;
        unawaited(() async {
          try {
            final pc = await answerer.pc;
            await pc.setRemoteDescription(RtcDescription('offer', sdp));
            answerer.remoteSet = true;
            final pending = List.of(answerer.pendingIce);
            answerer.pendingIce.clear();
            await _addIce(answerer, pending);
            final answer = await pc.createAnswer();
            await pc.setLocalDescription(answer);
            final local = await pc.localDescription();
            if (answerer.closed) return;
            _signal(
                answerer,
                from,
                ref.peer,
                SignalKind.answer,
                jsonEncode(
                    {'sid': answerer.sid, 'sdp': local?.sdp ?? answer.sdp}));
          } catch (error) {
            _log?.debug('channel answer failed: $error');
            _dropLink(answerer, true);
          }
        }());
      case SignalKind.answer:
        final sdp = payload?['sdp'];
        if (link == null ||
            !link.offerer ||
            link.described ||
            payload?['sid'] != link.sid ||
            sdp is! String) {
          return;
        }
        link.described = true;
        unawaited(() async {
          try {
            final pc = await link.pc;
            await pc.setRemoteDescription(RtcDescription('answer', sdp));
            link.remoteSet = true;
            final pending = List.of(link.pendingIce);
            link.pendingIce.clear();
            await _addIce(link, pending);
          } catch (_) {
            _dropLink(link, true);
          }
        }());
      case SignalKind.ice:
        final candidates = payload?['candidates'];
        if (link == null ||
            payload?['sid'] != link.sid ||
            candidates is! List) {
          return;
        }
        final list = [
          for (final c in candidates)
            if (c is Map) {for (final e in c.entries) '${e.key}': e.value}
        ];
        if (link.remoteSet) {
          unawaited(_addIce(link, list));
        } else {
          link.pendingIce.addAll(list);
        }
    }
  }

  void _ensureListening() {
    final self = hooks.self();
    final as = self?.key;
    if (as == _listeningAs) return;
    _stopListen?.call();
    _stopListen = null;
    _listeningAs = as;
    if (self != null) _stopListen = opts.signal.listen(self, _onSignal);
  }
}

Map<String, dynamic>? _parsePayload(String? payload) {
  if (payload == null || payload.isEmpty) return null;
  try {
    final v = jsonDecode(payload);
    return v is Map ? {for (final e in v.entries) '${e.key}': e.value} : null;
  } on FormatException {
    return null;
  }
}

// ---- signaling over SurrealDB -------------------------------------------------

const signalTable = '_00_channel_signal';
const _listenRetry = Duration(seconds: 3);

/// The bits of the remote service signaling needs.
class SignalRemote {
  const SignalRemote({
    required this.query,
    required this.live,
    required this.kill,
    required this.subscribeConnection,
  });

  final Future<List<dynamic>> Function(String sql, Map<String, dynamic> vars)
      query;
  final Future<(String, Stream<LiveMessage>)> Function(
      String sql, Map<String, dynamic> vars) live;
  final Future<void> Function(String liveId) kill;

  /// Called at once with the current state, then on every change.
  final void Function() Function(void Function(ConnectionState) cb)
      subscribeConnection;
}

SignalKind? _kindOf(Object? name) {
  for (final k in SignalKind.values) {
    if (k.name == name) return k;
  }
  return null;
}

/// Signals as rows of `_00_channel_signal` (generated for schemas with a
/// `@channel` table): a CREATE to send, one raw `LIVE SELECT` on rows
/// addressed to this user to receive. Not the outbox and not a synced query:
/// each signal costs one round trip, which is what keeps connection setup
/// short. The LIVE is re-armed whenever the socket reconnects.
SignalPort surrealSignal(SignalRemote remote, {SpookyLogger? logger}) =>
    _SurrealSignal(remote, logger);

class _SurrealSignal implements SignalPort {
  _SurrealSignal(this.remote, this.logger);

  final SignalRemote remote;
  final SpookyLogger? logger;

  @override
  Future<void> send(OutgoingSignal s) async {
    await remote.query('CREATE $signalTable CONTENT \$c RETURN NONE', {
      'c': {
        'recipient': RecordId.parse(s.to),
        'sender_peer': s.fromPeer,
        'kind': s.kind.name,
        if (s.toPeer != null) 'recipient_peer': s.toPeer,
        if (s.payload != null) 'payload': s.payload,
      },
    });
  }

  @override
  void Function() listen(PeerRef self, void Function(Signal) cb) {
    var stopped = false;
    String? liveId;
    StreamSubscription<LiveMessage>? sub;
    Timer? retry;
    ConnectionState? state;

    void killLive(String id) =>
        unawaited(remote.kill(id).catchError((Object _) {}));

    Future<void> arm() async {
      if (stopped) return;
      retry?.cancel();
      retry = null;
      final old = liveId;
      liveId = null;
      await sub?.cancel();
      sub = null;
      if (old != null) killLive(old);
      try {
        final (id, stream) = await remote.live(
            'LIVE SELECT * FROM $signalTable WHERE recipient = \$me',
            {'me': RecordId.parse(self.user)});
        if (stopped) {
          killLive(id);
          return;
        }
        liveId = id;
        sub = stream.listen((message) {
          if (stopped || message.action != 'CREATE') return;
          final v = message.value;
          final from = idKey(v['sender']);
          final fromPeer = v['sender_peer'];
          final kind = _kindOf(v['kind']);
          if (from == null || fromPeer is! String || kind == null) return;
          final toPeer = v['recipient_peer'];
          final payload = v['payload'];
          cb(Signal(
            from: from,
            fromPeer: fromPeer,
            toPeer: toPeer is String ? toPeer : null,
            kind: kind,
            payload: payload is String ? payload : null,
          ));
        });
      } catch (error) {
        logger?.debug('channel signal listen failed; retrying: $error');
        if (!stopped) retry = Timer(_listenRetry, () => unawaited(arm()));
      }
    }

    final unsubscribe = remote.subscribeConnection((next) {
      final was = state;
      state = next;
      if (next == ConnectionState.connected &&
          was != null &&
          was != ConnectionState.connected) {
        unawaited(arm());
      }
    });
    unawaited(arm());
    return () {
      stopped = true;
      unsubscribe();
      retry?.cancel();
      unawaited(sub?.cancel());
      final id = liveId;
      if (id != null) killLive(id);
    };
  }
}
