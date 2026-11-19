@Tags(['integration'])
library;

import 'dart:convert';
import 'dart:io';

import 'package:spooky_core/src/modules/channel/channel.dart';
import 'package:spooky_core/src/modules/channel/room.dart';
import 'package:test/test.dart';

import '../channel_share.dart';
import '../client/channel_client_test.dart' show eventually;

/// The room link against a real `sp00ky-relay` (`apps/relay`), which asks a
/// real SurrealDB whose token it is. Opt-in: nothing runs unless
/// `SPKY_RELAY_IT_URL` names a running relay and `SPKY_RELAY_IT_TOKEN` holds a
/// session token of a user of its database. For example:
///
///   docker run -d --name relay-it-db -p 18941:8000 \
///     surrealdb/surrealdb:v4.0.1 start --user root --pass root memory
///   (define a record access, sign a user up, keep the token)
///   SPKY_DB_URL=ws://127.0.0.1:18941 SPKY_DB_NS=main SPKY_DB_NAME=main \
///     SPKY_RELAY_LISTEN_ADDR=127.0.0.1:18942 target/debug/sp00ky-relay
///   SPKY_RELAY_IT_URL=ws://127.0.0.1:18942/room SPKY_RELAY_IT_TOKEN=... \
///     dart test test/integration/relay_room_test.dart
///
/// `SPKY_RELAY_IT_USER` overrides the user id read from the token's `ID` claim.
void main() {
  final url = Platform.environment['SPKY_RELAY_IT_URL'];
  final token = Platform.environment['SPKY_RELAY_IT_TOKEN'];
  final skip = url == null || token == null
      ? 'set SPKY_RELAY_IT_URL and SPKY_RELAY_IT_TOKEN to run'
      : null;

  /// The record id a SurrealDB session token names.
  String userOf(String jwt) {
    final claims = jsonDecode(
        utf8.decode(base64Url.decode(base64Url.normalize(jwt.split('.')[1]))));
    return (claims as Map)['ID'] as String;
  }

  ChannelTransportFactory room(String withToken) => (hooks) => roomTransport(
      hooks,
      RoomTransportOptions(
          link: (linkHooks) => roomLink(
              linkHooks,
              RoomLinkOptions(
                  url: url!,
                  token: () => withToken,
                  retryRefused: const Duration(milliseconds: 500)))));

  test('two devices of one user meet in their room and share a write',
      () async {
    final user = Platform.environment['SPKY_RELAY_IT_USER'] ?? userOf(token!);
    final phone = Device(user, room(token!), held: [note('1')]);
    final laptop = Device(user, room(token), held: [note('1')]);
    addTearDown(phone.m.close);
    addTearDown(laptop.m.close);
    final view = laptop.show(inFolder('a'));
    final none = phone.show(inFolder('nothing'));
    await eventually(
        () => phone.m.ownState.status, (s) => s == ChannelStatus.connected,
        timeout: const Duration(seconds: 15));
    await eventually(
        () => laptop.m.ownState.status, (s) => s == ChannelStatus.connected,
        timeout: const Duration(seconds: 15));
    expect(phone.m.ownState.peers, [user]);
    // Give each one's word on what it shows the time to cross.
    await Future<void>.delayed(const Duration(milliseconds: 300));
    phone.m.local('note', 'note:1', {'title': 'through the relay'}, null);
    await eventually(
        () => view.last[0]['title'], (t) => t == 'through the relay');
    // The phone shows another folder: the laptop does not send it this one.
    laptop.m.local('note', 'note:1', {'title': 'not for the phone'}, null);
    await Future<void>.delayed(const Duration(milliseconds: 300));
    expect(none.last[0]['title'], 't');
    laptop.m.close();
    await eventually(
        () => phone.m.ownState.status, (s) => s == ChannelStatus.connecting);
  }, skip: skip);

  test('a token the database does not know is refused', () async {
    final status = <PeerLinkStatus>[];
    final link = roomLink(
        RoomLinkHooks(
          self: () => const PeerRef('user:nobody', 'p1'),
          onPeer: (p, up) {},
          onMessage: (from, data) {},
          onStatus: status.add,
        ),
        RoomLinkOptions(url: url!, token: () => 'not-a-token'));
    addTearDown(link.close);
    await eventually(() => status, (s) => s.contains(PeerLinkStatus.failed),
        timeout: const Duration(seconds: 15));
    expect(status, [PeerLinkStatus.connecting, PeerLinkStatus.failed]);
  }, skip: skip);
}
