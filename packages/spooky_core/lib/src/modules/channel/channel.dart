import 'dart:async';
import 'dart:convert';
import 'dart:math';
import 'dart:typed_data';

import 'package:web_socket_channel/web_socket_channel.dart'
    show WebSocketChannel;

import '../../services/logger/logger.dart';
import '../../surreal/value.dart';
import '../../utils/parser.dart' show ColumnSchema;
import '../query_builder.dart' show QueryBuilder, QueryOp, RelationPlan;

/// Peer to peer channels (`-- @channel` tables, `client.channel(query, peers)`).
///
/// Port of `packages/core/src/modules/channel/index.ts`; the messages, the
/// signaling rows and the WebRTC negotiation are the same on the wire, so a
/// Dart client and a browser client talk to each other.
///
/// A channel is a filter (the `where` of one or more queries on `@channel`
/// tables) plus the users it connects to. Writes to rows that match the filter
/// reach the users' devices straight over the transport (WebRTC by default),
/// and their clients fold them in before the database has seen them. The filter
/// is evaluated here, in plain Dart, against the row; the query is never
/// registered. Optimistic writes, distributed:
///
/// - Updates: persistent fields still go through the normal outbox. A peer
///   shows the sent value until its own synced row catches up to it, or for
///   `confirmTimeoutMs` at most, so a value the server rejected (or that a peer
///   made up) falls back to the server's on its own.
/// - `-- @ephemeral` fields never reach the database. They live in memory, last
///   writer wins, and a peer's values vanish when it disconnects.
/// - Creates go into the peer's store and circuit like its own optimistic
///   create (minus the outbox); deletes hide the row. Both are undone unless the
///   server confirms them (see the peer sagas in `mutation/write_saga.dart`).
///
/// Nothing here enforces permissions beyond identity: the transport proves
/// which user a peer is (the database stamps the signaling rows), a message is
/// only taken from a user listed by an open channel whose filter the row
/// matches, and fields must exist and fit their column type. Everything else
/// is the app's call, through [ChannelsConfig.accept].
///
/// Update and ephemeral values are folded into subscription output only, never
/// into the local store.
///
/// The same machinery carries a user's own writes to their other devices
/// ([ChannelsConfig.share]): one internal channel whose only user is whoever
/// is signed in, over the room relay (`room.dart`) instead of WebRTC.

typedef Row = Map<String, dynamic>;

/// Hybrid logical clock: wall ms, counter, peer id. Orders every write.
class Hlc {
  const Hlc(this.ms, this.counter, this.peer);

  final int ms;
  final int counter;
  final String peer;

  List<Object> toJson() => [ms, counter, peer];

  @override
  bool operator ==(Object other) =>
      other is Hlc &&
      other.ms == ms &&
      other.counter == counter &&
      other.peer == peer;

  @override
  int get hashCode => Object.hash(ms, counter, peer);

  @override
  String toString() => '[$ms, $counter, $peer]';
}

int compareHlc(Hlc a, Hlc b) {
  if (a.ms != b.ms) return a.ms.compareTo(b.ms);
  if (a.counter != b.counter) return a.counter.compareTo(b.counter);
  return a.peer.compareTo(b.peer);
}

/// One device (one client instance) of one user.
class PeerRef {
  const PeerRef(this.user, this.peer);

  final String user;
  final String peer;

  /// `user#peer`: unique per device, and what orders the two ends of a link.
  String get key => '$user#$peer';

  @override
  bool operator ==(Object other) =>
      other is PeerRef && other.user == user && other.peer == peer;

  @override
  int get hashCode => Object.hash(user, peer);

  @override
  String toString() => key;
}

/// What a message does to its row.
enum ChannelOpKind { update, create, delete }

/// Everything a transport carries: a write to a row ([ChannelMessage]), or
/// what a device shows ([InterestMessage]).
sealed class ChannelWire {
  /// The JSON a link carries.
  Map<String, dynamic> toJson();
}

/// The message channels exchange: a write to one row.
class ChannelMessage implements ChannelWire {
  const ChannelMessage({
    this.kind = ChannelOpKind.update,
    required this.table,
    required this.id,
    required this.patch,
    required this.hlc,
  });

  final ChannelOpKind kind;
  final String table;
  final String id;

  /// Wire-encoded values (see [encodeValue]): the patch, the whole row for a
  /// create, `{}` for a delete.
  final Map<String, dynamic> patch;
  final Hlc hlc;

  /// The JSON a data channel carries. `kind` is left out for an update, as the
  /// browser client does.
  @override
  Map<String, dynamic> toJson() => {
        't': 'op',
        if (kind != ChannelOpKind.update) 'kind': kind.name,
        'table': table,
        'id': id,
        'patch': patch,
        'hlc': hlc.toJson(),
      };
}

/// How a channel is doing.
enum ChannelStatus { idle, connecting, connected, failed, unsupported }

class ChannelState {
  const ChannelState(this.status, [this.peers = const []]);

  final ChannelStatus status;

  /// Users with at least one device connected.
  final List<String> peers;

  @override
  bool operator ==(Object other) =>
      other is ChannelState &&
      other.status == status &&
      other.peers.join(',') == peers.join(',');

  @override
  int get hashCode => Object.hash(status, peers.join(','));

  @override
  String toString() => 'ChannelState(${status.name}, $peers)';
}

/// What an `accept` rule decides on.
class ChannelOp {
  const ChannelOp({
    required this.kind,
    required this.from,
    required this.table,
    required this.id,
    required this.row,
    required this.patch,
  });

  final ChannelOpKind kind;

  /// The sending user, as the database proved it.
  final String from;
  final String table;
  final String id;

  /// The row as this client currently shows it, overlay included; null for a
  /// create.
  final Row? row;

  /// Decoded field values: the patch, the new row for a create, `{}` for a
  /// delete.
  final Row patch;
}

typedef ChannelAcceptRule = bool Function(ChannelOp op);

// ---- transport ----------------------------------------------------------------

/// Per user: an attempt is running, a device is connected, or attempts failed.
enum PeerLinkStatus { connecting, connected, failed }

abstract interface class TransportHooks {
  /// This device, or null while signed out.
  PeerRef? self();
  void onMessage(PeerRef from, Object? message);
  void onPeer(PeerRef peer, bool up);
  void onStatus(String user, PeerLinkStatus status);
}

/// Moves messages between devices. WebRTC by default; an app that already has a
/// room (LiveKit, its own relay) can plug that in. [send] is fire and forget.
abstract interface class ChannelTransport {
  bool get supported;

  /// Users this device wants connections to: the union over open channels.
  void want(Set<String> users);
  void send(PeerRef to, ChannelWire message);
  void close();
}

/// A transport made of several, each reaching some users and not others. A
/// channel is then `unsupported` only when none of its users can be reached,
/// not whenever one half of the transport is missing.
abstract interface class PerUserSupport {
  bool supportsUser(String user);
}

typedef ChannelTransportFactory = ChannelTransport Function(
    TransportHooks hooks);

/// Which rows of a table are shared with the user's own devices: all of them,
/// or the ones a test passes.
class ChannelShareRule {
  const ChannelShareRule._(this.enabled, this.test);

  /// Only the rows [test] returns true for. It is asked on the device that
  /// writes, and again on the one that receives. A test that throws shares
  /// nothing.
  const ChannelShareRule.where(bool Function(Row row) test)
      : this._(true, test);

  /// Every row of the table.
  static const all = ChannelShareRule._(true, null);

  /// No row: the same as leaving the table out.
  static const none = ChannelShareRule._(false, null);

  final bool enabled;
  final bool Function(Row row)? test;
}

/// Sharing optimistic writes with the user's own other devices (see
/// [ChannelsConfig.share]). Needs [ChannelsConfig.relay].
class ChannelShareConfig {
  const ChannelShareConfig({required this.tables});

  /// Per table: [ChannelShareRule.all] shares every row,
  /// [ChannelShareRule.where] only the rows its test passes. A table left out
  /// (or set to [ChannelShareRule.none]) is never shared. The table needs no
  /// `-- @channel` mark: these writes only ever reach devices signed in as the
  /// same user.
  final Map<String, ChannelShareRule> tables;
}

/// Where the room relay (`sp00ky-relay`) is, and how to reach it.
class ChannelRelayConfig {
  const ChannelRelayConfig(this.url, {this.direct = true, this.connect});

  /// Its WebSocket address, e.g. `wss://relay.example.com/room`.
  final String url;

  /// Browsers only: two browsers alone in a room also open a direct WebRTC
  /// link and talk over that while it is up. A Dart client has no such link
  /// and keeps everything on the relay, whatever this says.
  final bool direct;

  /// Opens the socket to the relay; `WebSocketChannel.connect` when omitted.
  /// For tests, or an app that has to set headers or go through a proxy.
  final WebSocketChannel Function(Uri uri)? connect;
}

/// Peer to peer channels for `-- @channel` tables (TS `channels` config).
class ChannelsConfig {
  const ChannelsConfig({
    this.accept = const {},
    this.iceServers,
    this.rtc,
    this.transport,
    this.confirmTimeoutMs = 5000,
    this.relay,
    this.share,
  });

  /// Per table: decide whether to apply a peer's write. Runs after the built-in
  /// checks (sender listed by an open channel whose filter the row matches,
  /// fields exist and fit). Without a rule every such write is applied.
  final Map<String, ChannelAcceptRule> accept;

  /// STUN/TURN servers for the default WebRTC transport, in the
  /// `RTCConfiguration.iceServers` shape (`{'urls': 'stun:...'}`). A public STUN
  /// server when omitted; add TURN for networks that block direct connections.
  final List<Map<String, dynamic>>? iceServers;

  /// Builds WebRTC peer connections for the default transport. The core is
  /// pure Dart and has none of its own: in Flutter pass `flutterWebRtc` from
  /// `spooky_flutter`. Without it (and without [transport]) channels report
  /// [ChannelStatus.unsupported].
  final RtcPeerConnectionFactory? rtc;

  /// Replace the WebRTC transport.
  final ChannelTransportFactory? transport;

  /// How long a peer's persistent value shows without the server confirming
  /// it.
  final int confirmTimeoutMs;

  /// The room relay this user's devices meet on. One socket per client.
  final ChannelRelayConfig? relay;

  /// Send this user's optimistic writes straight to their other devices, so a
  /// change made on the phone shows on the laptop before the server has seen
  /// it. Off unless set, and per table. The devices tell each other which rows
  /// their open queries can show, and a write nobody shows is not sent. Needs
  /// [relay].
  final ChannelShareConfig? share;
}

/// A session description: `type` is `offer` or `answer`.
class RtcDescription {
  const RtcDescription(this.type, this.sdp);
  final String type;
  final String sdp;
}

/// The bits of an `RTCPeerConnection` the default transport uses. The core
/// stays free of any WebRTC plugin; `spooky_flutter` adapts `flutter_webrtc`.
abstract interface class RtcPeerConnection {
  /// A local ICE candidate, as `RTCIceCandidate.toJSON()` shapes it
  /// (`candidate`, `sdpMid`, `sdpMLineIndex`).
  set onIceCandidate(void Function(Map<String, dynamic> candidate)? cb);

  /// `new`, `connecting`, `connected`, `disconnected`, `failed` or `closed`.
  set onConnectionState(void Function(String state)? cb);

  /// A data channel the other side opened (the answering side).
  set onDataChannel(void Function(RtcDataChannel channel)? cb);

  Future<RtcDataChannel> createDataChannel(String label, {bool ordered = true});
  Future<RtcDescription> createOffer();
  Future<RtcDescription> createAnswer();
  Future<void> setLocalDescription(RtcDescription description);
  Future<void> setRemoteDescription(RtcDescription description);
  Future<RtcDescription?> localDescription();
  Future<void> addIceCandidate(Map<String, dynamic> candidate);
  Future<void> close();
}

abstract interface class RtcDataChannel {
  set onOpen(void Function()? cb);
  set onClose(void Function()? cb);

  /// Text frames only; binary frames are not part of the protocol.
  set onMessage(void Function(String text)? cb);
  bool get isOpen;
  void send(String text);
  Future<void> close();
}

/// Builds a peer connection from an `RTCConfiguration` map
/// (`{'iceServers': [...]}`).
typedef RtcPeerConnectionFactory = Future<RtcPeerConnection> Function(
    Map<String, dynamic> configuration);

// ---- pure helpers ---------------------------------------------------------------

/// `table:id` for a record id in any spelling, or null.
String? idKey(Object? v) {
  if (v is String) return v.contains(':') ? v : null;
  if (v is RecordId) return v.encode();
  if (v is Map && v.containsKey('table') && v.containsKey('id')) {
    final t = v['table'];
    final name = t is String ? t : (t is Map ? t['name'] : null);
    if (name is String) return '$name:${v['id']}';
  }
  return null;
}

/// An ISO string with millisecond precision, as JS `Date.toISOString()` writes
/// it: what a browser peer can parse back.
String _iso(DateTime d) =>
    DateTime.fromMillisecondsSinceEpoch(d.millisecondsSinceEpoch, isUtc: true)
        .toIso8601String();

/// JSON-safe form of a field value: record ids, dates and bytes are tagged.
Object? encodeValue(Object? v) {
  if (v == null) return null;
  if (v is RecordId) return {r'$rid': v.encode()};
  if (v is DateTime) return {r'$date': _iso(v)};
  if (v is Uint8List) return {r'$bytes': base64Encode(v)};
  if (v is List) return [for (final x in v) encodeValue(x)];
  if (v is Map) {
    return <String, dynamic>{
      for (final e in v.entries) '${e.key}': encodeValue(e.value)
    };
  }
  return v;
}

/// A wire value back in the shape this client's rows hold it: the local store
/// keeps documents as JSON, so record ids are `table:id` strings and dates ISO
/// strings. Bytes come back as a [Uint8List].
Object? decodeValue(Object? v) {
  if (v is List) return [for (final x in v) decodeValue(x)];
  if (v is! Map) return v;
  if (v.length == 1) {
    final rid = v[r'$rid'];
    if (rid is String) return rid;
    final date = v[r'$date'];
    if (date is String) {
      final parsed = DateTime.tryParse(date);
      return parsed == null ? date : _iso(parsed);
    }
    final bytes = v[r'$bytes'];
    if (bytes is String) {
      try {
        return base64Decode(bytes);
      } on FormatException {
        return bytes;
      }
    }
  }
  return <String, dynamic>{
    for (final e in v.entries) '${e.key}': decodeValue(e.value)
  };
}

String? _json(Object? v) {
  try {
    return jsonEncode(encodeValue(v));
  } catch (_) {
    return null;
  }
}

bool sameValue(Object? a, Object? b) {
  if (identical(a, b) || a == b) return true;
  final x = _json(a);
  return x != null && x == _json(b);
}

/// A well-formed `op` message, or null.
ChannelMessage? parseMessage(Object? raw) {
  if (raw is! Map) return null;
  final kindName = raw['kind'];
  final ChannelOpKind kind;
  switch (kindName) {
    case null:
    case 'update':
      kind = ChannelOpKind.update;
    case 'create':
      kind = ChannelOpKind.create;
    case 'delete':
      kind = ChannelOpKind.delete;
    default:
      return null;
  }
  final table = raw['table'];
  final id = raw['id'];
  final patch = raw['patch'];
  final hlc = raw['hlc'];
  if (raw['t'] != 'op' ||
      table is! String ||
      id is! String ||
      patch is! Map ||
      hlc is! List ||
      hlc.length != 3 ||
      hlc[0] is! num ||
      hlc[1] is! num ||
      hlc[2] is! String) {
    return null;
  }
  return ChannelMessage(
    kind: kind,
    table: table,
    id: id,
    patch: {for (final e in patch.entries) '${e.key}': e.value},
    hlc:
        Hlc((hlc[0] as num).toInt(), (hlc[1] as num).toInt(), hlc[2] as String),
  );
}

final _arrayType = RegExp(r'^array\s*(?:<\s*(.*?)\s*(?:,\s*\d+\s*)?>)?$');
final _optionType = RegExp(r'^option\s*<\s*(.*)\s*>$');

/// Whether a decoded value fits a column, loosely (the shapes the client itself
/// writes).
bool fitsColumn(ColumnSchema col, Object? v) {
  if (v == null) return col.optional;
  if (col.recordId) return v is String || v is RecordId;
  if (col.dateTime) return v is String || v is DateTime;
  return _fitsType((col.type ?? '').trim().toLowerCase(), v);
}

bool _fitsType(String type, Object? v) {
  final option = _optionType.firstMatch(type);
  if (option != null) return v == null || _fitsType(option.group(1)!, v);
  if (v == null) return false;
  final array = _arrayType.firstMatch(type);
  if (array != null) {
    if (v is! List) return false;
    final inner = array.group(1);
    return inner == null ||
        inner.isEmpty ||
        v.every((x) => _fitsType(inner, x));
  }
  if (type.startsWith('record')) return v is String || v is RecordId;
  switch (type) {
    case 'string':
    case 'uuid':
      return v is String;
    case 'int':
      return v is num && v.isFinite && v == v.roundToDouble();
    case 'number':
    case 'float':
    case 'decimal':
      return v is num && v.isFinite;
    case 'bool':
      return v is bool;
    case 'datetime':
      return v is String || v is DateTime;
    case 'bytes':
      return v is List<int>;
    case 'object':
      return v is Map;
    default:
      return true;
  }
}

/// Why a peer patch is refused against the table's columns, or null when it
/// fits.
String? checkPatch(Map<String, ColumnSchema> columns, Row patch,
    {bool allowEmpty = false}) {
  if (patch.isEmpty && !allowEmpty) return 'empty patch';
  for (final MapEntry(key: field, :value) in patch.entries) {
    if (field == 'id') return 'patches the id';
    final col = columns[field];
    if (col == null) return "unknown field '$field'";
    if (!fitsColumn(col, value)) return "'$field' does not fit its column type";
  }
  return null;
}

/// A patch split into the fields the database gets and the ones only peers get.
({Row persistent, Row? ephemeral}) splitPatch(
    Map<String, ColumnSchema> columns, Row data) {
  Row? ephemeral;
  final persistent = <String, dynamic>{};
  for (final MapEntry(:key, :value) in data.entries) {
    if (columns[key]?.ephemeral ?? false) {
      (ephemeral ??= {})[key] = value;
    } else {
      persistent[key] = value;
    }
  }
  return (persistent: persistent, ephemeral: ephemeral);
}

/// One overlaid field: the value, who wrote it and when.
class OverlayEntry {
  const OverlayEntry({
    required this.value,
    required this.hlc,
    required this.from,
    required this.ephemeral,
    required this.expiresAt,
  });

  final Object? value;
  final Hlc hlc;

  /// [PeerRef.key] of the author, or `self`.
  final String from;
  final bool ephemeral;

  /// When a persistent peer value stops showing; null for ephemeral and own
  /// values.
  final int? expiresAt;
}

typedef Overlay = Map<String, Map<String, OverlayEntry>>;

const _overlayDepth = 4;

/// Rows with the overlay folded in, nested related rows included. Untouched
/// rows (and the list itself, when nothing changed) keep their identity.
List<Row> applyOverlay(List<Row> rows, Overlay overlay) {
  if (overlay.isEmpty) return rows;
  final out = _foldList(rows, overlay, 0);
  return identical(out, rows) ? rows : out.cast<Row>();
}

List<Object?> _foldList(List<Object?> list, Overlay overlay, int depth) {
  List<Object?>? next;
  for (var i = 0; i < list.length; i++) {
    final x = list[i];
    final y = _foldValue(x, overlay, depth);
    if (!identical(x, y)) (next ??= List.of(list))[i] = y;
  }
  return next ?? list;
}

Object? _foldValue(Object? v, Overlay overlay, int depth) {
  if (depth > _overlayDepth) return v;
  if (v is List) return _foldList(v, overlay, depth + 1);
  if (v is! Map<String, dynamic>) return v;
  Row? next;
  for (final MapEntry(:key, value: x) in v.entries) {
    if (key == 'id') continue;
    final y = _foldValue(x, overlay, depth + 1);
    if (!identical(x, y)) (next ??= {...v})[key] = y;
  }
  final rid = v.containsKey('id') ? idKey(v['id']) : null;
  final fields = rid == null ? null : overlay[rid];
  if (fields != null) {
    for (final MapEntry(key: field, value: entry) in fields.entries) {
      if (!sameValue(v[field], entry.value)) {
        (next ??= {...v})[field] = entry.value;
      }
    }
  }
  return next ?? v;
}

/// Every record id in rows, nested related rows included.
Set<String> collectIds(List<Object?> rows, [Set<String>? into, int depth = 0]) {
  final out = into ?? <String>{};
  if (depth > _overlayDepth) return out;
  for (final v in rows) {
    if (v is List) {
      collectIds(v, out, depth + 1);
      continue;
    }
    if (v is! Map<String, dynamic>) continue;
    final rid = v.containsKey('id') ? idKey(v['id']) : null;
    if (rid != null) out.add(rid);
    for (final MapEntry(:key, :value) in v.entries) {
      if (key != 'id' && (value is Map || value is List)) {
        collectIds([value], out, depth + 1);
      }
    }
  }
  return out;
}

Row? _findRow(List<Object?> rows, String rid, int depth) {
  if (depth > _overlayDepth) return null;
  for (final v in rows) {
    if (v is List) {
      final hit = _findRow(v, rid, depth + 1);
      if (hit != null) return hit;
      continue;
    }
    if (v is! Map<String, dynamic>) continue;
    if (v.containsKey('id') && idKey(v['id']) == rid) return v;
    for (final MapEntry(:key, :value) in v.entries) {
      if (key == 'id' || (value is! Map && value is! List)) continue;
      final hit = _findRow([value], rid, depth + 1);
      if (hit != null) return hit;
    }
  }
  return null;
}

/// Normalized peer ids, without [self].
Set<String> peerUsers(Iterable<Object?> peers, String? self) => {
      for (final p in peers)
        if (idKey(p) case final key? when key != self) key,
    };

/// One node of a filter: a comparison ([ChannelCondition]), or a group of them
/// of which one has to pass ([ChannelOr]).
sealed class ChannelWhereNode {
  /// The wire form, values tagged as [encodeValue] does.
  Map<String, dynamic> toJson();
}

/// One comparison of a channel filter: `field op value`, or `value op field`
/// when [swap] is set.
class ChannelCondition implements ChannelWhereNode {
  const ChannelCondition(this.field, this.op, this.value, {this.swap = false});

  final String field;
  final String op;
  final Object? value;
  final bool swap;

  @override
  Map<String, dynamic> toJson() => {
        'field': field,
        'op': op,
        'value': encodeValue(value),
        if (swap) 'swap': true,
      };
}

/// A `(c1 OR c2 ...)` group. The Dart query builder has none, but a browser's
/// `_or` query sends one in what it says it shows.
class ChannelOr implements ChannelWhereNode {
  const ChannelOr(this.or);

  final List<ChannelCondition> or;

  @override
  Map<String, dynamic> toJson() => {
        'or': [for (final c in or) c.toJson()]
      };
}

/// What a channel carries: the rows of one `@channel` table its filter matches.
class ChannelScope {
  const ChannelScope(this.table, this.where, {this.test});

  final String table;

  /// AND-joined, like the query builder's `where`.
  final List<ChannelCondition> where;

  /// A further test on the row (a share rule given as a function).
  final bool Function(Row row)? test;
}

bool _isIn(Object? a, Object? b) => b is List && b.any((x) => _sameKey(a, x));
bool _contains(Object? a, Object? b) =>
    a is List && a.any((x) => _sameKey(x, b));

final Map<String, bool Function(Object?, Object?)> _ops = {
  '=': _sameKey,
  '==': _sameKey,
  '!=': (a, b) => !_sameKey(a, b),
  '>': (a, b) => _ordered(a, b, (c) => c > 0),
  '>=': (a, b) => _ordered(a, b, (c) => c >= 0),
  '<': (a, b) => _ordered(a, b, (c) => c < 0),
  '<=': (a, b) => _ordered(a, b, (c) => c <= 0),
  'IN': _isIn,
  'INSIDE': _isIn,
  '∈': _isIn,
  'NOT IN': (a, b) => !_isIn(a, b),
  'NOTINSIDE': (a, b) => !_isIn(a, b),
  '∉': (a, b) => !_isIn(a, b),
  'CONTAINS': _contains,
  '∋': _contains,
  'CONTAINSNOT': (a, b) => !_contains(a, b),
  '∌': (a, b) => !_contains(a, b),
};

/// A comparable form: record ids by `table:id`, dates by ms.
Object? _keyOf(Object? v) {
  if (v is RecordId) return v.encode();
  if (v is DateTime) return v.millisecondsSinceEpoch;
  return v;
}

/// A date compared against a string spelling of one (rows hold ISO strings).
int? _dateMs(Object? v) => v is DateTime
    ? v.millisecondsSinceEpoch
    : (v is String ? DateTime.tryParse(v)?.millisecondsSinceEpoch : null);

bool _sameKey(Object? a, Object? b) {
  if (a is DateTime || b is DateTime) {
    final x = _dateMs(a);
    return x != null && x == _dateMs(b);
  }
  final x = _keyOf(a);
  final y = _keyOf(b);
  if (x == y) return true;
  return (x is Map || x is List) && (y is Map || y is List) && sameValue(x, y);
}

/// An ordering comparison; values of different kinds never compare.
bool _ordered(Object? a, Object? b, bool Function(int) test) {
  final c = _order(a, b);
  return c != null && test(c);
}

int? _order(Object? a, Object? b) {
  if (a is DateTime || b is DateTime) {
    final x = _dateMs(a);
    final y = _dateMs(b);
    return x == null || y == null ? null : x.compareTo(y);
  }
  final x = _keyOf(a);
  final y = _keyOf(b);
  if (x is num && y is num) return x.compareTo(y);
  if (x is String && y is String) return x.compareTo(y);
  return null;
}

Object? _pathValue(Row row, String path) {
  Object? v = row;
  for (final part in path.split('.')) {
    if (v is! Map) return null;
    v = v[part];
  }
  return v;
}

bool _matchesCondition(ChannelCondition c, Row row) {
  final field = _pathValue(row, c.field);
  final op = _ops[c.op]!;
  return c.swap ? op(c.value, field) : op(field, c.value);
}

/// Whether [row] passes a filter (a conjunction of comparisons and OR
/// groups).
bool matchesWhere(List<ChannelWhereNode> where, Row row) =>
    where.every((node) => switch (node) {
          ChannelCondition c => _matchesCondition(c, row),
          ChannelOr(:final or) => or.any((c) => _matchesCondition(c, row)),
        });

/// The parts of a channel query that do nothing there (a channel only reads
/// the `where`). Not an error, worth a warning unless `.ignoreUnusedArgs()`.
List<String> unusedArgs(QueryBuilder q) => [
      if (q.isOne) '.one()',
      if (q.orderFields.isNotEmpty) 'orderBy',
      if (q.limitValue != null) 'limit',
      if (q.offsetValue != null) 'offset',
      if (q.selectFields.any((f) => f != '*')) 'select',
    ];

/// `{ table: { columns: {...}, channel: true } }` as the generated schema map
/// holds it.
Map<String, ColumnSchema> _columnsOf(
    Map<String, dynamic> schema, String table) {
  final entry = schema[table];
  final columns = entry is Map && entry['columns'] is Map
      ? entry['columns'] as Map
      : (entry is Map ? entry : const {});
  return {
    for (final e in columns.entries)
      if (e.value is ColumnSchema) e.key as String: e.value as ColumnSchema
  };
}

bool isChannelTable(Map<String, dynamic> schema, String table) {
  final entry = schema[table];
  return entry is Map && entry['channel'] == true;
}

/// A query as a channel scope, or an [ArgumentError] saying why it cannot be
/// one: the table must be `@channel`, and the filter must be decidable from the
/// row alone (its own fields, literal values, supported operators). Order,
/// limit and projection do not matter to a channel and are ignored.
ChannelScope compileScope(Map<String, dynamic> schema, QueryBuilder q) {
  final table = q.table;
  if (!isChannelTable(schema, table)) {
    throw ArgumentError(
        "Cannot open a channel on '$table': the table is not marked "
        "'-- @channel'. Add '-- @channel' above 'DEFINE TABLE $table' and "
        'regenerate the schema.');
  }
  if (q.relations.isNotEmpty) {
    throw ArgumentError("A channel query on '$table' cannot use .related(): "
        'its filter must be decidable from the row alone.');
  }
  final columns = _columnsOf(schema, table);
  final where = <ChannelCondition>[];
  for (final MapEntry(key: field, :value) in q.conditions.entries) {
    final condition = value is QueryOp
        ? ChannelCondition(field, value.op, value.value, swap: value.swap)
        : ChannelCondition(field, '=', value);
    if (!_ops.containsKey(condition.op)) {
      throw ArgumentError(
          "A channel query on '$table' cannot use the operator '${condition.op}'.");
    }
    final v = condition.value;
    if (v is String && v.startsWith(r'$')) {
      throw ArgumentError("A channel query on '$table' cannot compare '$field' "
          'with a parameter ($v); use a value.');
    }
    final root = field.split('.').first;
    if (field.contains('.') && (columns[root]?.recordId ?? false)) {
      throw ArgumentError("A channel query on '$table' cannot filter on "
          "'$field': it follows a record link, which the row alone does not "
          'hold.');
    }
    where.add(condition);
  }
  return ChannelScope(table, where);
}

// ---- interest: what a device's open queries can show ----------------------------

/// One query's claim on a table: the rows its filter passes, or (null) all of
/// them.
class QueryInterest {
  const QueryInterest(this.table, this.where);

  final String table;
  final List<ChannelWhereNode>? where;
}

/// What a device shows of the shared tables, as its open queries add up.
class Interest {
  const Interest(this.tables) : all = false;

  /// Everything: the device could not say more.
  const Interest.all()
      : tables = const {},
        all = true;

  final bool all;

  /// Per table: null for every row, else the filters of the queries on it. A
  /// table left out shows nothing.
  final Map<String, List<List<ChannelWhereNode>>?> tables;
}

/// A device telling its user's other devices what it shows, so they do not
/// send it writes it has no use for. Only ever exchanged between devices of
/// one user.
class InterestMessage implements ChannelWire {
  const InterestMessage(this.interest);

  final Interest interest;

  /// `{"t":"interest","all":true}`, or `{"t":"interest","v":{...}}` with per
  /// table `true` or a list of filters, their values wire-encoded.
  @override
  Map<String, dynamic> toJson() => interest.all
      ? {'t': 'interest', 'all': true}
      : {
          't': 'interest',
          'v': <String, dynamic>{
            for (final MapEntry(key: table, value: filters)
                in interest.tables.entries)
              table: filters == null
                  ? true
                  : [
                      for (final where in filters)
                        [for (final node in where) node.toJson()]
                    ],
          },
        };
}

/// Past this many filters on one table a device just asks for all of it: the
/// message stays small.
const _maxInterestFilters = 24;

/// A filter value typed by its column before it goes out, so a browser gets a
/// record link as a record id and a date as a date (the Dart rows, and the
/// queries on them, hold both as strings).
Object? _typedOperand(ColumnSchema? column, bool isId, Object? v) {
  if (v is List) return [for (final x in v) _typedOperand(column, isId, x)];
  if (v is! String) return v;
  if ((isId || (column?.recordId ?? false)) && v.contains(':')) {
    return RecordId.parse(v);
  }
  if (column?.dateTime ?? false) return DateTime.tryParse(v) ?? v;
  return v;
}

/// A builder's `where` as a test the row alone decides. Null when it cannot
/// be one (an operator this file does not evaluate, a path through a record
/// link, a reference to a variable only the server holds): then the query
/// counts as showing the whole table, which only costs a few messages too
/// many.
List<ChannelWhereNode>? _rowFilter(
    Map<String, ColumnSchema>? columns, Map<String, Object?> conditions) {
  final out = <ChannelWhereNode>[];
  for (final MapEntry(key: field, :value) in conditions.entries) {
    final op = value is QueryOp ? value.op : '=';
    final operand = value is QueryOp ? value.value : value;
    if (!_ops.containsKey(op)) return null;
    final nested = field.contains('.');
    final column = columns?[field.split('.').first];
    if (nested && (column?.recordId ?? false)) return null;
    // The builder passes a `$name` operand of a QueryOp through as a
    // reference, and its value is not known here.
    if (value is QueryOp && operand is String && operand.startsWith(r'$')) {
      return null;
    }
    final typed = _typedOperand(nested ? null : column, field == 'id', operand);
    // A value the wire cannot carry.
    if (_json(typed) == null) return null;
    out.add(ChannelCondition(field, op, typed,
        swap: value is QueryOp && value.swap));
  }
  return out;
}

/// What a registered query can show: the rows of its own table that pass its
/// filter, and every row of the tables it reads through `.related()` (which
/// children it shows depends on the parents, not on the child row alone).
/// Order, limit and offset are ignored: a row inside the filter and outside
/// the window is sent and then not shown.
List<QueryInterest> queryInterest(
    Map<String, ColumnSchema>? Function(String table) columnsOf,
    QueryBuilder q) {
  final out = [
    QueryInterest(q.table, _rowFilter(columnsOf(q.table), q.conditions))
  ];
  void walk(List<RelationPlan> relations) {
    for (final r in relations) {
      out.add(QueryInterest(r.table, null));
      walk(r.relations);
    }
  }

  walk(q.relations);
  return out;
}

/// The interests of every open subscription as one, for the tables [shared]
/// names. Everything once one of them is unknown (null). Each filter once and
/// in a fixed order, so the same set of queries always reads the same.
Interest mergeInterest(
    Iterable<List<QueryInterest>?> list, bool Function(String table) shared) {
  // Per table its filters by their wire form, or null for every row.
  final tables = <String, Map<String, List<ChannelWhereNode>>?>{};
  for (final queries in list) {
    if (queries == null) return const Interest.all();
    for (final q in queries) {
      if (!shared(q.table)) continue;
      final cur = tables[q.table];
      if (cur == null && tables.containsKey(q.table)) continue;
      final where = q.where;
      if (where == null || where.isEmpty) {
        tables[q.table] = null;
        continue;
      }
      final filters = cur ?? {};
      filters[jsonEncode([for (final node in where) node.toJson()])] = where;
      tables[q.table] = filters.length > _maxInterestFilters ? null : filters;
    }
  }
  return Interest({
    for (final table in tables.keys.toList()..sort())
      table: switch (tables[table]) {
        null => null,
        final filters => [
            for (final key in filters.keys.toList()..sort()) filters[key]!
          ],
      },
  });
}

/// The message that says [interest] (TS `encodeInterest`).
InterestMessage encodeInterest(Interest interest) => InterestMessage(interest);

/// A wire value of a filter. Unlike a row's field ([decodeValue]) a date stays
/// a [DateTime]: it is only ever compared, and by the instant, whatever
/// precision the row's own string spells.
Object? _decodeOperand(Object? v) {
  if (v is List) return [for (final x in v) _decodeOperand(x)];
  if (v is Map && v.length == 1) {
    final date = v[r'$date'];
    if (date is String) return DateTime.tryParse(date) ?? date;
  }
  return decodeValue(v);
}

ChannelCondition? _decodeComparison(Object? raw) {
  if (raw is! Map) return null;
  final field = raw['field'];
  final op = raw['op'];
  if (field is! String || op is! String || !_ops.containsKey(op)) return null;
  return ChannelCondition(field, op, _decodeOperand(raw['value']),
      swap: raw['swap'] == true);
}

/// The filters of one table, or null when one of them cannot be read.
List<List<ChannelWhereNode>>? _decodeFilters(List<Object?> filters) {
  if (filters.length > _maxInterestFilters) return null;
  final decoded = <List<ChannelWhereNode>>[];
  for (final where in filters) {
    if (where is! List) return null;
    final nodes = <ChannelWhereNode>[];
    for (final node in where) {
      final or = node is Map ? node['or'] : null;
      final group = or is List ? or : null;
      final list = [
        for (final c in group ?? [node]) _decodeComparison(c)
      ];
      if (list.isEmpty || list.contains(null)) return null;
      nodes.add(group == null
          ? list.first!
          : ChannelOr(list.cast<ChannelCondition>()));
    }
    decoded.add(nodes);
  }
  return decoded;
}

/// A peer's interest message, or null when it is not one. A filter this side
/// cannot read turns its table into "all of it": never into less than the
/// peer asked for.
Interest? decodeInterest(Object? raw) {
  if (raw is! Map || raw['t'] != 'interest') return null;
  if (raw['all'] == true) return const Interest.all();
  final v = raw['v'];
  if (v is! Map) return null;
  final out = <String, List<List<ChannelWhereNode>>?>{};
  for (final MapEntry(key: table, value: filters) in v.entries) {
    if (filters is List) {
      out['$table'] = _decodeFilters(filters);
    } else if (filters == true) {
      out['$table'] = null;
    }
  }
  return Interest(out);
}

/// Whether a device with this interest can show one of [rows] (a row before
/// and after a write) of [table].
bool showsRow(Interest interest, String table, List<Row> rows) {
  if (interest.all) return true;
  if (!interest.tables.containsKey(table)) return false;
  final filters = interest.tables[table];
  return filters == null ||
      filters.any((where) => rows.any((r) => matchesWhere(where, r)));
}

// ---- manager --------------------------------------------------------------------

class _Tap {
  _Tap(this.cb, this.interest);
  final void Function(List<Row> rows) cb;
  List<Row>? raw;

  /// Every record id in [raw], computed on first need.
  Set<String>? ids;

  /// What its query can show, or null when nobody said.
  final List<QueryInterest>? interest;
}

class _Channel {
  _Channel(this.scopes, this.listed, this.users, {this.own = false});

  /// The one channel to the user's own other devices
  /// ([ChannelsConfig.share]): its only user is whoever is signed in.
  final bool own;

  List<ChannelScope> scopes;

  /// Peers as given, kept so a sign-in can drop the new self from them.
  List<Object?> listed;
  Set<String> users;
  final Set<void Function(ChannelState)> listeners = {};
  ChannelState state = const ChannelState(ChannelStatus.idle);
  bool closed = false;

  Set<String> get tables => {for (final sc in scopes) sc.table};
}

/// An open channel. `client.channel(...)` wraps it with scoped writes.
class ChannelHandle {
  ChannelHandle._(this._manager, this._channel);

  final ChannelManager _manager;
  final _Channel _channel;

  /// The tables its queries read.
  List<String> get tables => _channel.tables.toList();

  ChannelState get state => _channel.state;

  bool get isClosed => _channel.closed;

  /// Called at once with the current state, then on every change.
  void Function() subscribe(void Function(ChannelState) cb) {
    cb(_channel.state);
    if (_channel.closed) return () {};
    _channel.listeners.add(cb);
    return () => _channel.listeners.remove(cb);
  }

  /// Replace who it connects to (your own id is skipped).
  void setPeers(Iterable<Object?> peers) {
    if (_channel.closed) return;
    _channel.listed = peers.toList();
    _channel.users = peerUsers(_channel.listed, _manager._user);
    _manager._syncWanted();
    _manager._notify(_channel);
  }

  /// Replace what it carries (the queries' filters changed).
  void setScopes(List<ChannelScope> scopes) {
    if (_channel.closed) return;
    if (scopes.isEmpty) {
      throw ArgumentError('A channel needs at least one query.');
    }
    _channel.scopes = scopes;
  }

  void close() {
    if (_channel.closed) return;
    _channel.closed = true;
    _manager._channels.remove(_channel);
    _channel.listeners.clear();
    _manager._syncWanted();
  }
}

/// How peers' creates and deletes reach the store (the runtime's peer sagas).
class ChannelRows {
  const ChannelRows({
    required this.create,
    required this.delete,
    required this.read,
  });

  /// Insert a peer's row, unconfirmed for `ttlMs`. False when refused.
  final Future<bool> Function(String table, String id, Row row, int ttlMs)
      create;

  /// Hide a row a peer deleted (or undo its unconfirmed create). False when
  /// there was nothing to do.
  final Future<bool> Function(String table, String id, int ttlMs) delete;

  /// The row as the local store holds it, or null. What a filter is checked
  /// against.
  final Future<Row?> Function(String id) read;
}

const _sweepEvery = Duration(seconds: 1);

String _randomPeerId(Random random) => [
      for (var i = 0; i < 8; i++)
        random.nextInt(256).toRadixString(16).padLeft(2, '0'),
    ].join();

class ChannelManager {
  ChannelManager(
    this._schema, {
    ChannelsConfig? config,
    this.rows,
    SpookyLogger? logger,
    ChannelTransportFactory? transport,
    int Function()? now,
    Random? random,
  })  : _config = config,
        _logger = logger,
        _transportFactory = transport,
        _now = now ?? (() => DateTime.now().millisecondsSinceEpoch),
        _confirmMs = max(0, config?.confirmTimeoutMs ?? 5000) {
    _peerId = _randomPeerId(random ?? Random.secure());
    _clock = Hlc(0, 0, _peerId);
    final share = config?.share?.tables ?? const <String, ChannelShareRule>{};
    for (final MapEntry(key: table, value: rule) in share.entries) {
      if (!rule.enabled) continue;
      if (!_isTable(_schema, table)) {
        logger?.warn("channels.share names '$table', which is not a table of "
            'the schema');
        continue;
      }
      _shared[table] = rule;
    }
    final own = _ownChannel = _shared.isEmpty
        ? null
        : _Channel([
            for (final MapEntry(key: table, value: rule) in _shared.entries)
              ChannelScope(table, const [],
                  test: switch (rule.test) {
                    null => null,
                    final test => _safeTest(test),
                  }),
          ], const [], {}, own: true);
    if (own != null) _channels.add(own);
  }

  final Map<String, dynamic> _schema;
  final ChannelsConfig? _config;
  final SpookyLogger? _logger;
  final ChannelTransportFactory? _transportFactory;
  final int Function() _now;
  final int _confirmMs;

  /// Without it, peers' creates and deletes are ignored.
  final ChannelRows? rows;

  final Overlay _overlay = {};
  final Set<_Tap> _taps = {};
  final Set<_Channel> _channels = {};
  final Map<String, PeerRef> _connected = {};
  final Map<String, PeerLinkStatus> _userStatus = {};

  /// Unused-argument warnings already given, so a rebuilt query does not
  /// repeat them.
  final Set<String> _warnedUnused = {};

  /// `channels.share`: the rule per shared table, and the channel that
  /// carries them to the user's own devices.
  final Map<String, ChannelShareRule> _shared = {};
  late final _Channel? _ownChannel;

  /// What each of the user's other devices said it shows, by [PeerRef.key]. A
  /// device that said nothing gets everything.
  final Map<String, Interest> _interest = {};

  /// The interest message last sent, so an unchanged one is not sent again.
  String? _announced;
  bool _announcing = false;
  late final String _peerId;
  late Hlc _clock;
  String? _user;
  ChannelTransport? _transport;
  Timer? _sweepTimer;
  bool _closed = false;
  final Map<String, Map<String, ColumnSchema>> _columnCache = {};

  /// This device's peer id (random per client instance).
  String get peerId => _peerId;

  /// True when any table is a channel table or shared; otherwise the client
  /// skips all of this.
  late final bool enabled =
      _shared.isNotEmpty || _schema.keys.any((t) => isChannelTable(_schema, t));

  /// Whether the user's own devices are part of this (`channels.share` names
  /// a table).
  bool get sharing => _ownChannel != null;

  /// How the link to the user's own devices is doing: `idle` when nothing is
  /// shared or nobody is signed in.
  ChannelState get ownState =>
      _ownChannel?.state ?? const ChannelState(ChannelStatus.idle);

  /// Called at once with [ownState], then on every change.
  void Function() subscribeOwn(void Function(ChannelState) cb) {
    final ch = _ownChannel;
    cb(ownState);
    if (ch == null || ch.closed) return () {};
    ch.listeners.add(cb);
    return () => ch.listeners.remove(cb);
  }

  /// What a registered query can show, as [tap] takes it.
  List<QueryInterest>? interestOf(QueryBuilder? query) =>
      query == null ? null : queryInterest(_tableColumns, query);

  Map<String, ColumnSchema>? _tableColumns(String table) =>
      _isTable(_schema, table)
          ? _columnCache[table] ??= _columnsOf(_schema, table)
          : null;

  /// The columns of a table that takes peer writes: a `@channel` one, or a
  /// shared one. A shared table needs no mark in the schema, its writes only
  /// reach devices of the same user.
  Map<String, ColumnSchema>? _channelColumns(String table) {
    if (!isChannelTable(_schema, table) && !_shared.containsKey(table)) {
      return null;
    }
    return _columnCache[table] ??= _columnsOf(_schema, table);
  }

  /// The signed-in user changed: peers, their values and the transport start
  /// over.
  void setUser(String? user) {
    if (user == _user) return;
    _user = user;
    _transport?.close();
    _transport = null;
    _connected.clear();
    _userStatus.clear();
    _interest.clear();
    _announced = null;
    final touched = _overlay.keys.toSet();
    _overlay.clear();
    for (final ch in _channels) {
      ch.users = ch.own ? {if (user != null) user} : peerUsers(ch.listed, user);
    }
    _syncWanted();
    _emitFor(touched);
    _notifyAll();
  }

  /// Compile a query into a channel scope (throws when it cannot be one). Parts
  /// the channel ignores draw one warning per distinct query shape, unless the
  /// query says so itself (`ignoreUnusedArgs()`).
  ChannelScope scope(QueryBuilder q) {
    final scope = compileScope(_schema, q);
    final unused = q.ignoresUnusedArgs ? const <String>[] : unusedArgs(q);
    if (unused.isNotEmpty) {
      final message = "channel query on '${q.table}' has ${unused.join(', ')}, "
          'which a channel ignores (it only uses the where filter). Remove '
          'them, or add .ignoreUnusedArgs() to the query to say it is on '
          'purpose.';
      if (_warnedUnused.add(message)) _logger?.warn(message);
    }
    return scope;
  }

  /// Open a channel carrying the rows [scopes] match to [peers].
  ChannelHandle open(List<ChannelScope> scopes, Iterable<Object?> peers) {
    if (scopes.isEmpty) {
      throw ArgumentError('A channel needs at least one query.');
    }
    final listed = peers.toList();
    final ch = _Channel(scopes, listed, peerUsers(listed, _user));
    _channels.add(ch);
    _syncWanted();
    ch.state = _stateOf(ch);
    return ChannelHandle._(this, ch);
  }

  /// Throws unless [handle] is an open channel of this manager with a query on
  /// [table].
  void assertCarries(ChannelHandle handle, String table) {
    final ch = handle._channel;
    if (!identical(handle._manager, this) || ch.closed) {
      throw StateError('This channel is closed.');
    }
    if (!ch.tables.contains(table)) {
      throw ArgumentError("This channel has no query on '$table' (it reads "
          '${ch.tables.join(', ')}).');
    }
  }

  /// Follow a subscription: `feed` it the raw rows, and [cb] gets them with the
  /// overlay folded in, again whenever the overlay changes for its rows.
  /// [interest] is what its query can show ([interestOf]); without one the
  /// user's other devices send this one everything.
  ({void Function(List<Row> rows) feed, void Function() stop}) tap(
      void Function(List<Row> rows) cb,
      [List<QueryInterest>? interest]) {
    final tap = _Tap(cb, interest);
    _taps.add(tap);
    _announceSoon();
    return (
      feed: (rows) {
        tap.raw = rows;
        tap.ids = null;
        _emit(tap);
      },
      stop: () {
        if (_taps.remove(tap)) _announceSoon();
      },
    );
  }

  /// Split an own write on [table] into database fields and peer-only fields.
  ({Row persistent, Row? ephemeral}) split(String table, Row data) {
    final columns = _channelColumns(table);
    if (columns == null) return (persistent: data, ephemeral: null);
    return splitPatch(columns, data);
  }

  /// An own update: ephemeral fields go into the overlay (they have no other
  /// home), persistent fields drop any peer value they supersede, and the whole
  /// patch goes to the peers (of [scope], or of every channel whose filter the
  /// row matches).
  void local(String table, String id, Row persistent, Row? ephemeral,
      [ChannelHandle? scope]) {
    final columns = _channelColumns(table);
    if (columns == null || _closed) return;
    final rid = _normalizeId(table, id);
    final hlc = _tick();
    final fields = _overlay[rid];
    var touched = false;
    if (fields != null) {
      for (final f in persistent.keys) {
        touched = fields.remove(f) != null || touched;
      }
    }
    if (ephemeral != null) {
      final map = fields ?? <String, OverlayEntry>{};
      for (final MapEntry(key: f, :value) in ephemeral.entries) {
        map[f] = OverlayEntry(
            value: value,
            hlc: hlc,
            from: 'self',
            ephemeral: true,
            expiresAt: null);
      }
      _overlay[rid] = map;
      touched = true;
    }
    if (fields != null && fields.isEmpty) _overlay.remove(rid);
    if (touched) _emitFor({rid});
    final patch = {...persistent, ...?ephemeral};
    if (patch.isEmpty || !_canSend(table)) return;
    // Sent if the row matches before or after the patch, so a row moving out
    // of a channel's filter still reaches its peers.
    unawaited(_rowFor(rid).then((row) {
      final after = {
        ...(row ?? {'id': rid}),
        ...patch
      };
      final rows = row == null ? [after] : [row, after];
      _send(_targets(table, rows, scope), table, rid, _typed(columns, patch),
          hlc, ChannelOpKind.update, rows);
    }));
  }

  /// An own create: its ephemeral fields go into the overlay (the store never
  /// holds them), and the whole row goes to the peers.
  void localCreate(String table, String id, Row row, [ChannelHandle? scope]) {
    final columns = _channelColumns(table);
    if (columns == null || _closed) return;
    final rid = _normalizeId(table, id);
    final full = _pickColumns(columns, row);
    final hlc = _tick();
    final ephemeral = [
      for (final f in full.keys)
        if (columns[f]?.ephemeral ?? false) f
    ];
    if (ephemeral.isNotEmpty) {
      final map = _overlay[rid] ?? <String, OverlayEntry>{};
      for (final f in ephemeral) {
        map[f] = OverlayEntry(
            value: full[f],
            hlc: hlc,
            from: 'self',
            ephemeral: true,
            expiresAt: null);
      }
      _overlay[rid] = map;
      _emitFor({rid});
    }
    final rows = [
      {...full, 'id': rid}
    ];
    _send(_targets(table, rows, scope), table, rid, _typed(columns, full), hlc,
        ChannelOpKind.create, rows);
  }

  /// An own delete: the peers hide the row at once. Completes once it is sent;
  /// await it before the local delete, which the filter check reads past.
  Future<void> localDelete(String table, String id,
      [ChannelHandle? scope]) async {
    if (_channelColumns(table) == null || _closed) return;
    final rid = _normalizeId(table, id);
    final hlc = _tick();
    if (_canSend(table)) {
      final row = await _rowFor(rid);
      if (row != null) {
        _send(_targets(table, [row], scope), table, rid, const {}, hlc,
            ChannelOpKind.delete, [row]);
      }
    }
    if (_overlay.remove(rid) != null) _emitFor({rid});
  }

  /// A transport delivered a message from [from].
  void receive(PeerRef from, Object? raw) {
    if (_closed) return;
    if (raw is Map && raw['t'] == 'interest') {
      return _receiveInterest(from, raw);
    }
    final msg = parseMessage(raw);
    if (msg == null) return _drop(from, 'malformed message');
    _observe(msg.hlc);
    final columns = _channelColumns(msg.table);
    if (columns == null) {
      return _drop(from, "'${msg.table}' is not a @channel table");
    }
    final rid = _normalizeId(msg.table, msg.id);
    if (rid.substring(0, rid.indexOf(':')) != msg.table) {
      return _drop(from, 'id is not a row of the table');
    }
    if (!_listedOn(msg.table, from.user)) {
      return _drop(from, "no channel on '${msg.table}' lists ${from.user}");
    }
    switch (msg.kind) {
      case ChannelOpKind.create:
        return _receiveCreate(from, msg, rid, columns);
      case ChannelOpKind.delete:
        unawaited(_receiveDelete(from, msg.table, rid));
        return;
      case ChannelOpKind.update:
        final patch = <String, dynamic>{
          for (final e in msg.patch.entries) e.key: decodeValue(e.value)
        };
        final problem = checkPatch(columns, patch);
        if (problem != null) return _drop(from, problem);
        unawaited(_receiveUpdate(from, msg, rid, patch, columns));
    }
  }

  /// A peer updated a row we hold, inside a channel's filter (before or after
  /// the patch).
  Future<void> _receiveUpdate(PeerRef from, ChannelMessage msg, String rid,
      Row patch, Map<String, ColumnSchema> columns) async {
    final row = await _rowFor(rid);
    if (_closed) return;
    if (row == null) {
      return _drop(from, '$rid is not held here, so no filter can be checked');
    }
    if (!_matchesFor(
        msg.table,
        [
          row,
          {...row, ...patch}
        ],
        from.user)) {
      return _drop(from, '$rid is outside every channel listing ${from.user}');
    }
    if (!_accepts(ChannelOp(
        kind: ChannelOpKind.update,
        from: from.user,
        table: msg.table,
        id: rid,
        row: row,
        patch: patch))) {
      return _drop(from, 'refused by the accept rule');
    }
    final map = _overlay[rid] ?? <String, OverlayEntry>{};
    var touched = false;
    for (final MapEntry(key: field, :value) in patch.entries) {
      final prev = map[field];
      if (prev != null && compareHlc(prev.hlc, msg.hlc) >= 0) continue;
      final ephemeral = columns[field]?.ephemeral ?? false;
      map[field] = OverlayEntry(
        value: value,
        hlc: msg.hlc,
        from: from.key,
        ephemeral: ephemeral,
        expiresAt: ephemeral ? null : _now() + _confirmMs,
      );
      touched = true;
    }
    if (map.isNotEmpty) _overlay[rid] = map;
    if (!touched) return;
    _ensureSweep();
    _emitFor({rid});
  }

  /// A peer created a row: into the store like an own optimistic create,
  /// unconfirmed.
  void _receiveCreate(PeerRef from, ChannelMessage msg, String rid,
      Map<String, ColumnSchema> columns) {
    final rows = this.rows;
    if (rows == null) return _drop(from, 'creates are not wired');
    final row = <String, dynamic>{
      for (final e in msg.patch.entries)
        if (e.key != 'id') e.key: decodeValue(e.value)
    };
    final problem = checkPatch(columns, row, allowEmpty: true);
    if (problem != null) return _drop(from, problem);
    if (!_matchesFor(
        msg.table,
        [
          {...row, 'id': rid}
        ],
        from.user)) {
      return _drop(
          from, 'the new $rid is outside every channel listing ${from.user}');
    }
    if (!_accepts(ChannelOp(
        kind: ChannelOpKind.create,
        from: from.user,
        table: msg.table,
        id: rid,
        row: null,
        patch: row))) {
      return _drop(from, 'refused by the accept rule');
    }
    final persistent = <String, dynamic>{};
    final ephemeral = <String, dynamic>{};
    for (final MapEntry(:key, :value) in row.entries) {
      ((columns[key]?.ephemeral ?? false) ? ephemeral : persistent)[key] =
          value;
    }
    unawaited(
        rows.create(msg.table, rid, persistent, _confirmMs).then((applied) {
      if (!applied || _closed || ephemeral.isEmpty) return;
      final map = _overlay[rid] ?? <String, OverlayEntry>{};
      for (final MapEntry(key: f, :value) in ephemeral.entries) {
        map[f] = OverlayEntry(
            value: value,
            hlc: msg.hlc,
            from: from.key,
            ephemeral: true,
            expiresAt: null);
      }
      _overlay[rid] = map;
      _emitFor({rid});
    }, onError: (Object error) {
      _logger?.debug('peer create failed: $error');
    }));
  }

  /// A peer deleted a row inside a channel's filter: hide it until the server
  /// confirms (or undo its own create).
  Future<void> _receiveDelete(PeerRef from, String table, String rid) async {
    final rows = this.rows;
    if (rows == null) return _drop(from, 'deletes are not wired');
    final row = await _rowFor(rid);
    if (_closed) return;
    if (row == null) return _drop(from, '$rid is not held here');
    if (!_matchesFor(table, [row], from.user)) {
      return _drop(from, '$rid is outside every channel listing ${from.user}');
    }
    if (!_accepts(ChannelOp(
        kind: ChannelOpKind.delete,
        from: from.user,
        table: table,
        id: rid,
        row: row,
        patch: const {}))) {
      return _drop(from, 'refused by the accept rule');
    }
    if (_overlay.remove(rid) != null) _emitFor({rid});
    try {
      await rows.delete(table, rid, _confirmMs);
    } catch (error) {
      _logger?.debug('peer delete failed: $error');
    }
  }

  /// One of the user's own devices said what it shows. Anybody else's word on
  /// that is ignored.
  void _receiveInterest(PeerRef from, Object? raw) {
    if (from.user != _user || _ownChannel == null) {
      return _drop(from, 'interest from another user');
    }
    final interest = decodeInterest(raw);
    if (interest == null) return _drop(from, 'malformed interest');
    _interest[from.key] = interest;
  }

  bool _accepts(ChannelOp op) {
    final rule = _config?.accept[op.table];
    if (rule == null) return true;
    try {
      return rule(op) == true;
    } catch (error) {
      _logger?.warn('channel accept rule threw: $error');
      return false;
    }
  }

  /// A peer device came up (send it our ephemeral state) or went away (drop
  /// its).
  void peerChanged(PeerRef peer, bool up) {
    final key = peer.key;
    if (up) {
      _connected[key] = peer;
      _userStatus[peer.user] = PeerLinkStatus.connected;
      if (_ownChannel != null && peer.user == _user) {
        _transport?.send(peer, encodeInterest(_ownInterest()));
      }
      _sendSnapshot(peer);
    } else {
      _connected.remove(key);
      _interest.remove(key);
      final touched = <String>{};
      for (final MapEntry(key: rid, value: fields) in _overlay.entries) {
        fields.removeWhere((_, e) {
          final gone = e.from == key && e.ephemeral;
          if (gone) touched.add(rid);
          return gone;
        });
      }
      _overlay.removeWhere((_, fields) => fields.isEmpty);
      _emitFor(touched);
    }
    _notifyAll();
  }

  void statusChanged(String user, PeerLinkStatus status) {
    if (status != PeerLinkStatus.connected &&
        _connected.values.any((p) => p.user == user)) {
      return;
    }
    _userStatus[user] = status;
    _notifyAll();
  }

  void close() {
    _closed = true;
    _transport?.close();
    _transport = null;
    _sweepTimer?.cancel();
    _sweepTimer = null;
    _taps.clear();
    for (final ch in _channels) {
      ch.closed = true;
      ch.listeners.clear();
    }
    _channels.clear();
    _overlay.clear();
  }

  // ---- internals ---------------------------------------------------------------

  void _syncWanted() {
    if (_closed) return;
    final wanted = <String>{
      for (final ch in _channels) ...ch.users,
    };
    var transport = _transport;
    if (transport == null) {
      final factory = _transportFactory;
      if (wanted.isEmpty || _user == null || factory == null) return;
      transport = _transport = factory(_Hooks(this));
    }
    transport.want(wanted);
  }

  /// Whether an own write on [table] could reach anyone: a transport is up,
  /// somebody is connected and an open channel carries the table. Skips the
  /// row read otherwise, which a shared table would pay on every write of a
  /// user with one device.
  bool _canSend(String table) =>
      _transport != null &&
      _connected.isNotEmpty &&
      _channels.any((ch) => ch.tables.contains(table));

  /// Some open channel lists [user] and has a query on [table] (the cheap check
  /// before any read).
  bool _listedOn(String table, String user) => _channels
      .any((ch) => ch.users.contains(user) && ch.tables.contains(table));

  /// Some open channel listing [user] has a query on [table] one of [rows]
  /// matches.
  bool _matchesFor(String table, List<Row> rows, String user) => _channels
      .any((ch) => ch.users.contains(user) && _carries(ch, table, rows));

  /// Who an own write goes to: the peers of every open channel (or only
  /// [scope]) whose filter one of [rows] matches. That is what keeps a message
  /// of one conversation away from the participants of another.
  Set<String> _targets(String table, List<Row> rows, ChannelHandle? scope) {
    final users = <String>{};
    final own = _ownChannel;
    // A write made through one channel still reaches the user's own devices.
    final pick =
        scope == null ? _channels : [scope._channel, if (own != null) own];
    for (final ch in pick) {
      if (ch.closed || !_carries(ch, table, rows)) continue;
      users.addAll(ch.users);
    }
    return users;
  }

  /// The row as shown (overlay included): from a subscription, else the store.
  Future<Row?> _rowFor(String rid) async {
    final shown = _rowOf(rid);
    if (shown != null) return shown;
    Row? stored;
    try {
      stored = await rows?.read(rid);
    } catch (_) {
      stored = null;
    }
    return stored == null ? null : applyOverlay([stored], _overlay).first;
  }

  void _send(Set<String> users, String table, String rid, Row patch, Hlc hlc,
      ChannelOpKind kind, List<Row> rows) {
    final transport = _transport;
    if (transport == null || users.isEmpty) return;
    final message = ChannelMessage(
      kind: kind,
      table: table,
      id: rid,
      patch: {for (final e in patch.entries) e.key: encodeValue(e.value)},
      hlc: hlc,
    );
    for (final MapEntry(:key, value: peer) in _connected.entries.toList()) {
      if (!users.contains(peer.user)) continue;
      // One of the user's own devices that said what it shows, and it is not
      // this row: it has no use for the write, and the server brings it
      // anyway.
      final interest = peer.user == _user ? _interest[key] : null;
      if (interest != null && !showsRow(interest, table, rows)) continue;
      transport.send(peer, message);
    }
  }

  /// What this device's subscriptions show of the shared tables.
  Interest _ownInterest() =>
      mergeInterest(_taps.map((t) => t.interest), _shared.containsKey);

  /// Tell the user's other devices what this one shows, once the
  /// subscriptions of this turn are all in.
  void _announceSoon() {
    if (_ownChannel == null || _announcing || _closed) return;
    _announcing = true;
    scheduleMicrotask(() {
      _announcing = false;
      final transport = _transport;
      final user = _user;
      if (_closed || transport == null || user == null) return;
      final message = encodeInterest(_ownInterest());
      final text = jsonEncode(message.toJson());
      if (text == _announced) return;
      _announced = text;
      for (final peer in _connected.values.toList()) {
        if (peer.user == user) transport.send(peer, message);
      }
    });
  }

  /// Own ephemeral values of rows inside a channel a newly connected peer is
  /// listed on.
  void _sendSnapshot(PeerRef peer) {
    for (final MapEntry(key: rid, value: fields) in _overlay.entries) {
      final own = [
        for (final e in fields.entries)
          if (e.value.from == 'self') e
      ];
      if (own.isEmpty) continue;
      final table = rid.substring(0, rid.indexOf(':'));
      if (!_listedOn(table, peer.user)) continue;
      final patch = <String, dynamic>{};
      var hlc = own.first.value.hlc;
      for (final MapEntry(key: f, value: e) in own) {
        patch[f] = encodeValue(e.value);
        if (compareHlc(e.hlc, hlc) > 0) hlc = e.hlc;
      }
      unawaited(_rowFor(rid).then((row) {
        if (row == null || !_matchesFor(table, [row], peer.user)) return;
        _transport?.send(peer,
            ChannelMessage(table: table, id: rid, patch: patch, hlc: hlc));
      }));
    }
  }

  Row? _rowOf(String rid) {
    for (final tap in _taps) {
      final raw = tap.raw;
      if (raw == null || !_tapIds(tap).contains(rid)) continue;
      final row = _findRow(raw, rid, 0);
      if (row != null) return applyOverlay([row], _overlay).first;
    }
    return null;
  }

  void _emit(_Tap tap) {
    final raw = tap.raw;
    if (raw == null) return;
    final rows = applyOverlay(raw, _overlay);
    _confirm(raw);
    tap.cb(rows);
  }

  void _emitFor(Set<String> rids) {
    if (rids.isEmpty) return;
    for (final tap in _taps.toList()) {
      if (tap.raw == null) continue;
      final ids = _tapIds(tap);
      if (rids.any(ids.contains)) _emit(tap);
    }
  }

  /// Persistent peer values the synced rows already hold are done showing.
  void _confirm(List<Row> raw) {
    if (_overlay.isEmpty) return;
    for (final rid in collectIds(raw)) {
      final fields = _overlay[rid];
      if (fields == null) continue;
      final row = _findRow(raw, rid, 0);
      if (row == null) continue;
      fields.removeWhere((f, e) =>
          !e.ephemeral &&
          e.from != 'self' &&
          row.containsKey(f) &&
          sameValue(row[f], e.value));
      if (fields.isEmpty) _overlay.remove(rid);
    }
  }

  void _ensureSweep() {
    if (_sweepTimer != null || _closed) return;
    _sweepTimer = Timer.periodic(_sweepEvery, (_) => _sweep());
  }

  void _sweep() {
    final now = _now();
    final touched = <String>{};
    var pending = false;
    for (final MapEntry(key: rid, value: fields) in _overlay.entries) {
      fields.removeWhere((_, e) {
        final at = e.expiresAt;
        if (at == null) return false;
        if (at <= now) {
          touched.add(rid);
          return true;
        }
        pending = true;
        return false;
      });
    }
    _overlay.removeWhere((_, fields) => fields.isEmpty);
    if (!pending) {
      _sweepTimer?.cancel();
      _sweepTimer = null;
    }
    _emitFor(touched);
  }

  Hlc _tick() {
    final now = _now();
    _clock = now > _clock.ms
        ? Hlc(now, 0, _peerId)
        : Hlc(_clock.ms, _clock.counter + 1, _peerId);
    return _clock;
  }

  void _observe(Hlc h) {
    if (h.ms > _clock.ms || (h.ms == _clock.ms && h.counter > _clock.counter)) {
      _clock = Hlc(h.ms, h.counter, _peerId);
    }
  }

  ChannelState _stateOf(_Channel ch) {
    final users = ch.users.toList();
    if (users.isEmpty) return const ChannelState(ChannelStatus.idle);
    final transport = _transport;
    if (transport != null && users.every((u) => !_reaches(transport, u))) {
      return const ChannelState(ChannelStatus.unsupported);
    }
    final up = [
      for (final u in users)
        if (_connected.values.any((p) => p.user == u)) u
    ];
    if (up.isNotEmpty) return ChannelState(ChannelStatus.connected, up);
    if (users.every((u) => _userStatus[u] == PeerLinkStatus.failed)) {
      return const ChannelState(ChannelStatus.failed);
    }
    return const ChannelState(ChannelStatus.connecting);
  }

  void _notify(_Channel ch) {
    final next = _stateOf(ch);
    if (next == ch.state) return;
    ch.state = next;
    for (final cb in ch.listeners.toList()) {
      cb(next);
    }
  }

  void _notifyAll() {
    for (final ch in _channels.toList()) {
      _notify(ch);
    }
  }

  void _drop(PeerRef from, String reason) =>
      _logger?.debug('channel message from ${from.user} dropped: $reason');
}

class _Hooks implements TransportHooks {
  _Hooks(this._m);
  final ChannelManager _m;

  @override
  PeerRef? self() {
    final user = _m._user;
    return user == null ? null : PeerRef(user, _m._peerId);
  }

  @override
  void onMessage(PeerRef from, Object? message) => _m.receive(from, message);

  @override
  void onPeer(PeerRef peer, bool up) => _m.peerChanged(peer, up);

  @override
  void onStatus(String user, PeerLinkStatus status) =>
      _m.statusChanged(user, status);
}

/// Whether a channel has a query on [table] that one of [rows] matches.
bool _carries(_Channel ch, String table, List<Row> rows) =>
    ch.scopes.any((sc) =>
        sc.table == table &&
        rows.any(
            (r) => matchesWhere(sc.where, r) && (sc.test?.call(r) ?? true)));

/// Whether [transport] can connect to [user] at all.
bool _reaches(ChannelTransport transport, String user) {
  final Object t = transport;
  return t is PerUserSupport ? t.supportsUser(user) : transport.supported;
}

/// A share rule that never throws: a rule that does shares nothing.
bool Function(Row row) _safeTest(bool Function(Row row) rule) => (row) {
      try {
        return rule(row);
      } catch (_) {
        return false;
      }
    };

/// Whether the schema map has [table] (its other keys describe access,
/// backends and relationships).
bool _isTable(Map<String, dynamic> schema, String table) =>
    schema[table] is Map &&
    table != 'access' &&
    table != 'backends' &&
    table != 'relationships';

/// The fields of [row] the table has as columns (no id, no nulls).
Row _pickColumns(Map<String, ColumnSchema> columns, Row row) => {
      for (final MapEntry(:key, :value) in row.entries)
        if (key != 'id' && columns.containsKey(key) && value != null)
          key: value,
    };

/// Values typed by their column before they go out, so a browser peer gets a
/// record link as a record id and a date as a date (the Dart rows hold both as
/// strings).
Row _typed(Map<String, ColumnSchema> columns, Row patch) => {
      for (final MapEntry(:key, :value) in patch.entries)
        key: switch ((columns[key], value)) {
          (ColumnSchema(recordId: true), String s) when s.contains(':') =>
            RecordId.parse(s),
          (ColumnSchema(dateTime: true), String s) => DateTime.tryParse(s) ?? s,
          _ => value,
        },
    };

Set<String> _tapIds(_Tap tap) =>
    tap.ids ??= tap.raw == null ? <String>{} : collectIds(tap.raw!);

/// `table:id` for an id given with or without its table.
String _normalizeId(String table, String id) =>
    id.contains(':') ? id : '$table:$id';
