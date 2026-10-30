import 'dart:convert';

import 'package:spooky_core/src/modules/channel/channel.dart';

/// Devices connect as soon as both want each other's user, like the real one.
class MemoryNet {
  final nodes = <String, _Node>{};

  /// Every message as it went over the wire.
  final wire = <String>[];

  void _reconcile() {
    for (final a in nodes.values.toList()) {
      for (final b in nodes.values.toList()) {
        if (identical(a, b)) continue;
        final kb = b.self.key;
        final mutual =
            a.wanted.contains(b.self.user) && b.wanted.contains(a.self.user);
        if (mutual && !a.links.contains(kb)) {
          a.links.add(kb);
          a.hooks.onPeer(b.self, true);
        } else if (!mutual && a.links.contains(kb)) {
          a.links.remove(kb);
          a.hooks.onPeer(b.self, false);
        }
      }
    }
  }

  ChannelTransport factory(TransportHooks hooks) {
    final node = _Node(this, hooks.self()!, hooks);
    nodes[node.self.key] = node;
    return node;
  }
}

class _Node implements ChannelTransport {
  _Node(this.net, this.self, this.hooks);
  final MemoryNet net;
  final PeerRef self;
  final TransportHooks hooks;
  Set<String> wanted = {};
  final Set<String> links = {};

  @override
  bool get supported => true;

  @override
  void want(Set<String> users) {
    wanted = {...users};
    net._reconcile();
  }

  @override
  void send(PeerRef to, ChannelMessage message) {
    if (!links.contains(to.key)) return;
    final json = jsonEncode(message.toJson());
    net.wire.add(json);
    net.nodes[to.key]?.hooks.onMessage(self, jsonDecode(json));
  }

  @override
  void close() {
    wanted = {};
    net._reconcile();
    net.nodes.remove(self.key);
  }
}
