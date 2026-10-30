import 'dart:async';

import 'package:flutter_webrtc/flutter_webrtc.dart' as rtc;
import 'package:spooky_core/spooky_core.dart';

/// WebRTC peer connections for sp00ky channels (`-- @channel` tables), over
/// `flutter_webrtc`. The core is pure Dart and builds none itself:
///
/// ```dart
/// Sp00kyConfig(
///   ...,
///   channels: ChannelsConfig(rtc: flutterWebRtc),
/// )
/// ```
///
/// Same data channel (`sp00ky`, ordered, text frames) and the same ICE
/// candidate shape a browser tab uses, so Flutter and web clients connect to
/// each other.
Future<RtcPeerConnection> flutterWebRtc(
        Map<String, dynamic> configuration) async =>
    _FlutterPeerConnection(await rtc.createPeerConnection(configuration));

class _FlutterPeerConnection implements RtcPeerConnection {
  _FlutterPeerConnection(this._pc);

  final rtc.RTCPeerConnection _pc;

  @override
  set onIceCandidate(void Function(Map<String, dynamic> candidate)? cb) {
    _pc.onIceCandidate = cb == null
        ? null
        : (c) {
            final candidate = c.candidate;
            // The end-of-candidates marker carries no candidate.
            if (candidate == null || candidate.isEmpty) return;
            cb({
              'candidate': candidate,
              'sdpMid': c.sdpMid,
              'sdpMLineIndex': c.sdpMLineIndex,
            });
          };
  }

  @override
  set onConnectionState(void Function(String state)? cb) {
    _pc.onConnectionState = cb == null ? null : (s) => cb(_stateName(s));
  }

  @override
  set onDataChannel(void Function(RtcDataChannel channel)? cb) {
    _pc.onDataChannel = cb == null ? null : (dc) => cb(_FlutterDataChannel(dc));
  }

  @override
  Future<RtcDataChannel> createDataChannel(String label,
          {bool ordered = true}) async =>
      _FlutterDataChannel(await _pc.createDataChannel(
          label, rtc.RTCDataChannelInit()..ordered = ordered));

  @override
  Future<RtcDescription> createOffer() async => _desc(await _pc.createOffer());

  @override
  Future<RtcDescription> createAnswer() async =>
      _desc(await _pc.createAnswer());

  @override
  Future<void> setLocalDescription(RtcDescription d) =>
      _pc.setLocalDescription(rtc.RTCSessionDescription(d.sdp, d.type));

  @override
  Future<void> setRemoteDescription(RtcDescription d) =>
      _pc.setRemoteDescription(rtc.RTCSessionDescription(d.sdp, d.type));

  @override
  Future<RtcDescription?> localDescription() async {
    final d = await _pc.getLocalDescription();
    final sdp = d?.sdp;
    final type = d?.type;
    return sdp == null || type == null ? null : RtcDescription(type, sdp);
  }

  @override
  Future<void> addIceCandidate(Map<String, dynamic> c) =>
      _pc.addCandidate(rtc.RTCIceCandidate(
        c['candidate'] as String?,
        c['sdpMid'] as String?,
        (c['sdpMLineIndex'] as num?)?.toInt(),
      ));

  @override
  Future<void> close() async {
    try {
      await _pc.close();
    } finally {
      await _pc.dispose();
    }
  }
}

RtcDescription _desc(rtc.RTCSessionDescription d) {
  final sdp = d.sdp;
  final type = d.type;
  if (sdp == null || type == null) {
    throw StateError('WebRTC returned an empty session description');
  }
  return RtcDescription(type, sdp);
}

String _stateName(rtc.RTCPeerConnectionState s) => switch (s) {
      rtc.RTCPeerConnectionState.RTCPeerConnectionStateNew => 'new',
      rtc.RTCPeerConnectionState.RTCPeerConnectionStateConnecting =>
        'connecting',
      rtc.RTCPeerConnectionState.RTCPeerConnectionStateConnected => 'connected',
      rtc.RTCPeerConnectionState.RTCPeerConnectionStateDisconnected =>
        'disconnected',
      rtc.RTCPeerConnectionState.RTCPeerConnectionStateFailed => 'failed',
      rtc.RTCPeerConnectionState.RTCPeerConnectionStateClosed => 'closed',
    };

/// The plugin reports state changes through one callback that may fire before
/// the transport has attached its own (the answering side gets its channel
/// already open), so open and close are remembered and delivered once.
class _FlutterDataChannel implements RtcDataChannel {
  _FlutterDataChannel(this._dc) : _state = _dc.state {
    _dc.onDataChannelState = (s) {
      _state = s;
      if (s == rtc.RTCDataChannelState.RTCDataChannelOpen) _fireOpen();
      if (s == rtc.RTCDataChannelState.RTCDataChannelClosed) _fireClose();
    };
    _dc.onMessage = (m) {
      if (!m.isBinary) _onMessage?.call(m.text);
    };
  }

  final rtc.RTCDataChannel _dc;
  rtc.RTCDataChannelState? _state;
  void Function()? _onOpen;
  void Function()? _onClose;
  void Function(String text)? _onMessage;
  bool _opened = false;
  bool _closed = false;

  @override
  set onOpen(void Function()? cb) {
    _onOpen = cb;
    if (cb != null && isOpen) scheduleMicrotask(_fireOpen);
  }

  @override
  set onClose(void Function()? cb) {
    _onClose = cb;
    if (cb != null && _state == rtc.RTCDataChannelState.RTCDataChannelClosed) {
      scheduleMicrotask(_fireClose);
    }
  }

  @override
  set onMessage(void Function(String text)? cb) => _onMessage = cb;

  @override
  bool get isOpen => _state == rtc.RTCDataChannelState.RTCDataChannelOpen;

  @override
  void send(String text) =>
      unawaited(_dc.send(rtc.RTCDataChannelMessage(text)).catchError((_) {}));

  @override
  Future<void> close() => _dc.close();

  void _fireOpen() {
    final cb = _onOpen;
    if (_opened || cb == null) return;
    _opened = true;
    cb();
  }

  void _fireClose() {
    final cb = _onClose;
    if (_closed || cb == null) return;
    _closed = true;
    cb();
  }
}
