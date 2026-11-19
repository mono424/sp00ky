import 'dart:async';
import 'dart:convert';
import 'dart:math';

import 'package:web_socket_channel/web_socket_channel.dart';

import '../../services/logger/logger.dart';
import 'channel.dart';

/// The link between the devices of one user (`channels.share`).
///
/// Port of `packages/core/src/modules/channel/room.ts` and `hub.ts`, on the
/// same wire protocol, so a Dart device and a browser of one user talk to each
/// other.
///
/// Every device keeps one socket to the room relay (`sp00ky-relay`, protocol in
/// `apps/relay/README.md`). The relay proves the user from the session token,
/// puts the socket into that user's room and forwards what the members send
/// each other without reading it. So a message is there after one hop to the
/// relay and one back, whatever network each device sits in, and nothing waits
/// for a connection to be negotiated.
///
/// A browser multiplexes its tabs over one such socket: its hub tells the
/// other members which tabs it speaks for, and every message names the tab it
/// comes from and the tabs it is for. A Dart client is one device with nothing
/// to multiplex, and it still speaks that envelope, as a hub with exactly one
/// tab (itself): without it a browser would not know who the message is from.
///
/// Two browsers alone in a room also open a WebRTC link to each other through
/// the relay. A Dart client does not: it ignores that signaling, and the
/// browser simply keeps talking through the relay.
///
/// Nothing here is kept or retried: a message lost on the way is one the
/// normal sync delivers a moment later.

/// What a room link tells its owner.
class RoomLinkHooks {
  const RoomLinkHooks({
    required this.self,
    required this.onPeer,
    required this.onMessage,
    required this.onStatus,
  });

  /// Who this device is, or null while signed out.
  final PeerRef? Function() self;

  /// A member of the room (by the id it joined with) came or went.
  final void Function(String peer, bool up) onPeer;
  final void Function(String from, Object? data) onMessage;
  final void Function(PeerLinkStatus status) onStatus;
}

abstract interface class RoomLink {
  /// Send to these members of the room, or (null) to all of them. Fire and
  /// forget.
  void send(List<String>? to, Object? data);
  void close();
}

typedef RoomLinkFactory = RoomLink Function(RoomLinkHooks hooks);

/// Starts a one-shot timer. `Timer.new` unless a test brings its own clock.
typedef RoomTimer = Timer Function(Duration delay, void Function() callback);

class RoomLinkOptions {
  const RoomLinkOptions({
    required this.url,
    required this.token,
    this.connect,
    this.logger,
    this.timer,
    this.random,
    this.beat = const Duration(seconds: 20),
    this.retryBase = const Duration(milliseconds: 500),
    this.retryMax = const Duration(seconds: 30),
    this.retryRefused = const Duration(seconds: 30),
  });

  /// The relay's WebSocket address.
  final String url;

  /// The session token the relay proves the user with; null while there is
  /// none.
  final String? Function() token;

  /// Opens the socket. `WebSocketChannel.connect` when omitted.
  final WebSocketChannel Function(Uri uri)? connect;
  final SpookyLogger? logger;

  /// Injectable for tests.
  final RoomTimer? timer;

  /// Spreads the reconnects of many devices apart.
  final Random? random;

  /// How often the client pings. Two beats without a pong in between and the
  /// socket counts as dead.
  final Duration beat;

  /// The first wait before a reconnect; doubled per failed attempt up to
  /// [retryMax].
  final Duration retryBase;
  final Duration retryMax;

  /// The wait after a refusal that a retry will not fix soon (not signed in at
  /// the relay, room full).
  final Duration retryRefused;
}

const _protocol = 1;

final _uuidPrefix = RegExp('^u(?=[\'"])');
final _idQuotes = RegExp('[⟨⟩`\'"]');

/// Two spellings of one record id. The database prints a key the way it would
/// have to be written (`user:⟨a-b⟩`, backticks, or `user:u'0190…'` for a
/// uuid), the client keeps it bare (`user:a-b`): the wrapping is not part of
/// who it is. Verified against SurrealDB 4.0.1.
String _bareId(String id) {
  final at = id.indexOf(':');
  return id.substring(0, at + 1) +
      id
          .substring(at + 1)
          .replaceFirst(_uuidPrefix, '')
          .replaceAll(_idQuotes, '');
}

/// Whether two record ids name the same user, whatever escaping the database
/// added to one of them.
bool sameUser(String a, String b) => _bareId(a) == _bareId(b);

/// Join the room of the signed-in user on the relay, and stay in it: the
/// socket is reopened whenever it drops.
RoomLink roomLink(RoomLinkHooks hooks, RoomLinkOptions opts) =>
    _RoomLink(hooks, opts).._start();

class _RoomLink implements RoomLink {
  _RoomLink(this.hooks, this.opts)
      : _timer = opts.timer ?? Timer.new,
        _random = opts.random ?? Random();

  final RoomLinkHooks hooks;
  final RoomLinkOptions opts;
  final RoomTimer _timer;
  final Random _random;

  /// The other members of the room.
  final Set<String> _peers = {};
  WebSocketChannel? _socket;
  StreamSubscription<dynamic>? _frames;
  bool _welcomed = false;
  bool _closed = false;
  int _attempts = 0;
  Timer? _retry;
  Timer? _beat;
  bool _awaitingPong = false;

  SpookyLogger? get _log => opts.logger;

  void _start() {
    hooks.onStatus(PeerLinkStatus.connecting);
    _connect();
  }

  @override
  void send(List<String>? to, Object? data) {
    if (_closed || !_welcomed) return;
    final targets = to == null
        ? _peers.toList()
        : [
            for (final p in to.toSet())
              if (_peers.contains(p)) p
          ];
    if (targets.isEmpty) return;
    // To all of them is one frame without a list.
    _relay(targets.length == _peers.length ? null : targets, {'m': data});
  }

  @override
  void close() {
    if (_closed) return;
    _closed = true;
    _retry?.cancel();
    _retry = null;
    _stopBeat();
    final ws = _socket;
    _welcomed = false;
    _peers.clear();
    if (ws != null) _hangUp(ws);
  }

  void _relay(List<String>? to, Object? d) {
    final ws = _socket;
    if (ws == null || !_welcomed) return;
    _write(ws, jsonEncode({'t': 'msg', if (to != null) 'to': to, 'd': d}));
  }

  void _write(WebSocketChannel ws, String text) {
    try {
      ws.sink.add(text);
    } catch (error) {
      // Closing: the end of the stream does the rest.
      _log?.debug('room send failed: $error');
    }
  }

  /// Let go of [ws]: nothing it still says is heard.
  void _hangUp(WebSocketChannel ws) {
    if (identical(_socket, ws)) {
      _socket = null;
      unawaited(_frames?.cancel());
      _frames = null;
    }
    unawaited(ws.sink.close().catchError((Object _) {/* already gone */}));
  }

  void _stopBeat() {
    _beat?.cancel();
    _beat = null;
    _awaitingPong = false;
  }

  void _startBeat(WebSocketChannel ws) {
    _beat = _timer(opts.beat, () {
      if (!identical(_socket, ws)) return;
      // Two beats without an answer in between: the socket is dead, however
      // long the app sat in the background meanwhile.
      if (_awaitingPong) {
        _hangUp(ws);
        _dropped();
        return;
      }
      _awaitingPong = true;
      _write(ws, '{"t":"ping"}');
      _startBeat(ws);
    });
  }

  void _scheduleRetry(Duration delay) {
    if (_closed || _retry != null) return;
    _retry = _timer(delay, () {
      _retry = null;
      _connect();
    });
  }

  /// The socket is gone, and every member with it.
  void _dropped([Duration? delay]) {
    _socket = null;
    _frames = null;
    _stopBeat();
    final was = _peers.toList();
    _peers.clear();
    _welcomed = false;
    for (final p in was) {
      hooks.onPeer(p, false);
    }
    if (_closed) return;
    _attempts++;
    hooks.onStatus(_attempts > 2 || delay != null
        ? PeerLinkStatus.failed
        : PeerLinkStatus.connecting);
    _scheduleRetry(delay ?? _backoff());
  }

  Duration _backoff() {
    final base = min(opts.retryMax.inMilliseconds,
        opts.retryBase.inMilliseconds * pow(2, min(_attempts - 1, 20)).toInt());
    return Duration(
        milliseconds: (base * (0.75 + _random.nextDouble() * 0.5)).round());
  }

  void _connect() {
    if (_closed || _socket != null) return;
    final self = hooks.self();
    if (self == null) return;
    final token = opts.token();
    if (token == null || token.isEmpty) {
      // Signed in, and the session has no token yet (it is being restored).
      _scheduleRetry(opts.retryBase);
      return;
    }
    final WebSocketChannel ws;
    try {
      ws = (opts.connect ?? WebSocketChannel.connect)(Uri.parse(opts.url));
    } catch (error) {
      _log?.warn('the room relay address cannot be opened: $error');
      hooks.onStatus(PeerLinkStatus.failed);
      return;
    }
    _socket = ws;
    // A relay that cannot be reached also ends the stream, which is where the
    // retry starts; this only keeps the error from going unhandled.
    unawaited(ws.ready.then((_) {}, onError: (Object _) {}));
    _frames = ws.stream.listen(
      (data) {
        if (identical(_socket, ws) && data is String) _onFrame(ws, data, self);
      },
      onError: (Object error) {
        _log?.debug('room socket error: $error');
      },
      onDone: () {
        if (identical(_socket, ws)) _dropped();
      },
    );
    // Queued until the socket is open.
    _write(
        ws,
        jsonEncode(
            {'t': 'hello', 'v': _protocol, 'token': token, 'peer': self.peer}));
  }

  void _onFrame(WebSocketChannel ws, String text, PeerRef self) {
    final Object? parsed;
    try {
      parsed = jsonDecode(text);
    } on FormatException {
      return;
    }
    if (parsed is! Map) return;
    switch (parsed['t']) {
      case 'welcome':
        if (_welcomed) return;
        final user = parsed['user'];
        if (user is! String || !sameUser(user, self.user)) {
          // The token belongs to somebody else than the client thinks is
          // signed in: not a room to speak in under this name.
          _log?.warn('the room relay answered for another user; not joining');
          _hangUp(ws);
          _dropped(opts.retryRefused);
          return;
        }
        _welcomed = true;
        _attempts = 0;
        _awaitingPong = false;
        _startBeat(ws);
        hooks.onStatus(PeerLinkStatus.connected);
        final peers = parsed['peers'];
        for (final p in peers is List ? peers : const <Object?>[]) {
          if (p is! String || p == self.peer || !_peers.add(p)) continue;
          hooks.onPeer(p, true);
        }
      case 'join':
        final peer = parsed['peer'];
        if (!_welcomed ||
            peer is! String ||
            peer == self.peer ||
            !_peers.add(peer)) {
          return;
        }
        hooks.onPeer(peer, true);
      case 'leave':
        final peer = parsed['peer'];
        if (peer is! String || !_peers.remove(peer)) return;
        hooks.onPeer(peer, false);
      case 'msg':
        final from = parsed['from'];
        final d = parsed['d'];
        if (from is! String || !_peers.contains(from) || d is! Map) return;
        // `s` is the signaling of the direct WebRTC link two browsers open.
        // This client has none, so the browser stays on the relay.
        if (d.containsKey('s')) return;
        if (d.containsKey('m')) hooks.onMessage(from, d['m']);
      case 'pong':
        _awaitingPong = false;
      case 'error':
        final raw = parsed['code'];
        final code = raw is String ? raw : 'unknown';
        _log?.debug('room relay refused: $code');
        _hangUp(ws);
        // `reauth` is the relay ending a long session: come back with the
        // token of now. Everything else is not fixed by asking again at once.
        if (code == 'reauth') {
          _attempts = 0;
          _dropped();
        } else {
          _dropped(opts.retryRefused);
        }
    }
  }
}

// ---- the room as a channel transport -------------------------------------------

class RoomTransportOptions {
  const RoomTransportOptions({
    required this.link,
    this.supported = true,
    this.unheardAfter = const Duration(seconds: 3),
  });

  /// Builds the device's link to the room. Called when the signed-in user is
  /// wanted, and again after a sign-in as somebody else.
  final RoomLinkFactory link;
  final bool supported;

  /// After a lost socket: how long a member may take to name its tabs before
  /// this device leaves and joins anew (see [roomTransport]).
  final Duration unheardAfter;
}

/// A browser names at most this many tabs, each by an id this long at most.
const _maxTabs = 64;
bool _isTabId(Object? v) => v is String && v.isNotEmpty && v.length <= 64;
List<String>? _tabList(Object? v) =>
    v is List && v.length <= _maxTabs && v.every(_isTabId)
        ? v.cast<String>()
        : null;

/// The user's own devices as channel peers (TS `hubTransport`, for a client
/// with one "tab").
///
/// To the [ChannelManager] a peer is one tab of a browser or one Dart client:
/// every member of the room says which tabs it speaks for, each of those is
/// reported up as a peer of the signed-in user, and down again when the list
/// shrinks or the member leaves. So a value a browser tab wrote still
/// disappears with that tab, not with its browser.
///
/// Members name their tabs to each other when one joins. The relay tells
/// nobody, though, when it hands a seat back to its peer id because the old
/// socket still looked alive (a phone that changed networks reconnects long
/// before the relay misses it): the device is welcomed and knows nobody, and
/// the others never hear that it forgot them. Two things mend that here, both
/// with the frames the protocol already has. A member that names the same
/// tabs again has done just that, so it is told this device's tab again and
/// treated as having left and joined. And a device that lost its socket and,
/// back in the room, is not told everybody's tabs in a moment leaves and
/// joins once more, this time with a leave for all to see. The browser client
/// does neither yet.
ChannelTransport roomTransport(
        TransportHooks hooks, RoomTransportOptions opts) =>
    _RoomTransport(hooks, opts);

class _RoomTransport implements ChannelTransport {
  _RoomTransport(this.hooks, this.opts);

  final TransportHooks hooks;
  final RoomTransportOptions opts;

  /// This device while it takes part, else null.
  PeerRef? _me;
  RoomLink? _link;

  /// The other members of the room and the tabs each speaks for.
  final Map<String, Set<String>> _members = {};

  /// The tabs this transport has reported as up.
  final Set<String> _up = {};
  PeerLinkStatus _status = PeerLinkStatus.connecting;
  PeerLinkStatus? _reported;

  /// Messages of this turn, each with the tabs it goes to: one frame per
  /// message, not one per tab.
  final Map<ChannelWire, List<String>> _pending = Map.identity();
  bool _flushing = false;

  /// The link was connected and lost its socket, so its next welcome may be
  /// on a seat the relay handed back without a word to the others.
  bool _lostSocket = false;
  Timer? _unheard;

  /// Members just told this device's tab again for naming theirs unchanged.
  /// One answer a second each: two clients doing this cannot keep each other
  /// busy.
  final Set<String> _answered = {};

  @override
  bool get supported => opts.supported;

  @override
  void want(Set<String> users) {
    final self = hooks.self();
    final wanted = self != null && users.contains(self.user);
    if (_me != null && (!wanted || self != _me)) _stop();
    if (wanted && _me == null) _begin(self);
  }

  @override
  void send(PeerRef to, ChannelWire message) {
    final me = _me;
    if (me == null || to.user != me.user) return;
    (_pending[message] ??= []).add(to.peer);
    if (_flushing) return;
    _flushing = true;
    scheduleMicrotask(_flush);
  }

  @override
  void close() => _stop();

  void _begin(PeerRef self) {
    _me = self;
    _status = PeerLinkStatus.connecting;
    _reported = null;
    _members.clear();
    _link = opts.link(RoomLinkHooks(
      self: () => _me,
      onPeer: (member, up) {
        if (up) {
          _members[member] = {};
          // A browser has to hear which tab this device is before it takes a
          // message from it.
          _nameTabs(member, self);
        } else if (_members.remove(member) == null) {
          return;
        }
        _apply();
      },
      onMessage: _onMessage,
      onStatus: (next) {
        final was = _status;
        _status = next;
        if (next != PeerLinkStatus.connected) {
          if (was == PeerLinkStatus.connected) _lostSocket = true;
        } else if (_lostSocket) {
          _lostSocket = false;
          _unheard?.cancel();
          _unheard = Timer(opts.unheardAfter, _rejoinIfUnheard);
        }
        _apply();
      },
    ));
    _apply();
  }

  void _nameTabs(String member, PeerRef self) => _link?.send([
        member
      ], {
        'h': 'tabs',
        'tabs': [self.peer]
      });

  /// Back in the room after a lost socket, and some member has not named its
  /// tabs: it never saw this device go. Leave and join, for all to see. Once
  /// per lost socket, so a member that never names any costs one extra hello.
  void _rejoinIfUnheard() {
    _unheard = null;
    final self = _me;
    if (self == null || _status != PeerLinkStatus.connected) return;
    if (!_members.values.any((tabs) => tabs.isEmpty)) return;
    _stop();
    _begin(self);
  }

  void _stop() {
    final self = _me;
    if (self == null) return;
    _link?.close();
    _link = null;
    _unheard?.cancel();
    _unheard = null;
    _lostSocket = false;
    _answered.clear();
    _members.clear();
    _pending.clear();
    final was = _up.toList();
    _up.clear();
    _me = null;
    for (final tab in was) {
      hooks.onPeer(PeerRef(self.user, tab), false);
    }
  }

  void _onMessage(String member, Object? data) {
    final self = _me;
    final tabs = _members[member];
    if (self == null || tabs == null || data is! Map) return;
    switch (data['h']) {
      case 'tabs':
        final list = _tabList(data['tabs']);
        if (list == null) return;
        final next = list.toSet();
        if (tabs.isNotEmpty &&
            tabs.length == next.length &&
            tabs.containsAll(next) &&
            _answered.add(member)) {
          // The same tabs again: the member took its seat anew and the relay
          // told nobody. It has forgotten this device, and what it showed and
          // wrote is as gone as after a leave. Start over with it.
          Timer(const Duration(seconds: 1), () => _answered.remove(member));
          _members[member] = {};
          _apply();
          _nameTabs(member, self);
        }
        _members[member] = next;
        _apply();
      case 'm':
        final from = data['from'];
        final rawTo = data['to'];
        final to = rawTo == null ? null : _tabList(rawTo);
        // A member speaks for its own tabs only, and says who a message is
        // for: every tab (an explicit null) or the ones it names.
        if (from is! String ||
            !tabs.contains(from) ||
            !data.containsKey('to') ||
            (rawTo != null && to == null)) {
          return;
        }
        if (to == null || to.contains(self.peer)) {
          hooks.onMessage(PeerRef(self.user, from), data['m']);
        }
    }
  }

  /// Tell the manager which tabs of the other members came and went, and how
  /// the link is doing.
  void _apply() {
    final self = _me;
    if (self == null) return;
    final now = <String>{
      for (final tabs in _members.values)
        for (final tab in tabs)
          if (tab != self.peer) tab
    };
    for (final tab in _up.toList()) {
      if (now.contains(tab) || !_up.remove(tab)) continue;
      hooks.onPeer(PeerRef(self.user, tab), false);
    }
    for (final tab in now) {
      if (!_up.add(tab)) continue;
      hooks.onPeer(PeerRef(self.user, tab), true);
    }
    if (_status != _reported) {
      _reported = _status;
      hooks.onStatus(self.user, _status);
    }
  }

  void _flush() {
    _flushing = false;
    final self = _me;
    final batch = Map.of(_pending);
    _pending.clear();
    if (self == null) return;
    for (final MapEntry(key: message, value: to) in batch.entries) {
      // One frame naming every tab, to the members that speak for one of them.
      final dest = [
        for (final MapEntry(key: member, value: tabs) in _members.entries)
          if (to.any(tabs.contains)) member
      ];
      if (dest.isEmpty) continue;
      _link?.send(dest.length == _members.length ? null : dest,
          {'h': 'm', 'from': self.peer, 'to': to, 'm': message.toJson()});
    }
  }
}

/// One transport out of two: the user's own devices are reached through [own]
/// (the room), everybody else through [others] (the mesh between users).
/// Without [own], the user's own devices are simply never reached.
ChannelTransportFactory splitTransport(
        ChannelTransportFactory? own, ChannelTransportFactory others) =>
    (hooks) => _SplitTransport(hooks, others(hooks), own?.call(hooks));

class _SplitTransport implements ChannelTransport, PerUserSupport {
  _SplitTransport(this.hooks, this.mesh, this.room);

  final TransportHooks hooks;
  final ChannelTransport mesh;
  final ChannelTransport? room;

  @override
  bool get supported => mesh.supported || (room?.supported ?? false);

  @override
  bool supportsUser(String user) =>
      user == hooks.self()?.user ? (room?.supported ?? false) : mesh.supported;

  @override
  void want(Set<String> users) {
    final self = hooks.self()?.user;
    final rest = {...users};
    final mine = self != null && rest.remove(self);
    room?.want({if (mine) self});
    mesh.want(rest);
  }

  @override
  void send(PeerRef to, ChannelWire message) {
    final room = this.room;
    (room != null && to.user == hooks.self()?.user ? room : mesh)
        .send(to, message);
  }

  @override
  void close() {
    room?.close();
    mesh.close();
  }
}
