import 'dart:async';
import 'dart:math';

import 'package:fake_async/fake_async.dart';
import 'package:logging/logging.dart';
import 'package:spooky_core/src/modules/channel/channel.dart';
import 'package:spooky_core/src/modules/channel/room.dart';
import 'package:spooky_core/src/services/logger/logger.dart';
import 'package:test/test.dart';

import '../channel_share.dart';
import '../room_relay.dart';

// The room of one user's devices: the link to the relay (port of
// `packages/core/src/modules/channel/room.test.ts`), and the envelope a
// browser's hub speaks on it (`hub.ts`), which a Dart client has to speak too.

/// One device of `user:<name>`: a link to the relay and what it heard on it.
class Member {
  Member(FakeRelay relay, String name, this.peer, {SpookyLogger? logger})
      : token = 't-$name' {
    link = roomLink(
        RoomLinkHooks(
          self: () => PeerRef('user:$name', peer),
          onPeer: (p, up) => events.add('${up ? 'up' : 'down'} $p'),
          onMessage: (from, data) => messages.add([from, data]),
          onStatus: status.add,
        ),
        RoomLinkOptions(
            url: 'ws://relay.test/room',
            token: () => token,
            connect: relay.connect,
            logger: logger));
  }

  final String peer;
  String? token;
  late final RoomLink link;
  final events = <String>[];
  final messages = <List<Object?>>[];
  final status = <PeerLinkStatus>[];
}

/// The room on [relay] as a channel transport, signed in with [token].
ChannelTransportFactory viaRelay(FakeRelay relay, String token) =>
    (hooks) => roomTransport(
        hooks,
        RoomTransportOptions(
            link: (linkHooks) => roomLink(
                linkHooks,
                RoomLinkOptions(
                    url: 'ws://relay.test/room',
                    token: () => token,
                    connect: relay.connect))));

const connecting = PeerLinkStatus.connecting;
const connected = PeerLinkStatus.connected;
const failed = PeerLinkStatus.failed;

Duration ms(int n) => Duration(milliseconds: n);

/// A transport that only writes down what it is asked.
class LogTransport implements ChannelTransport {
  LogTransport(this.log, this.name, {this.supported = true});
  final List<String> log;
  final String name;

  @override
  final bool supported;

  @override
  void want(Set<String> users) => log.add('$name want ${users.join(',')}');

  @override
  void send(PeerRef to, ChannelWire message) =>
      log.add('$name send ${to.user}');

  @override
  void close() => log.add('$name close');
}

class Hooks implements TransportHooks {
  Hooks(this.user);
  final String? user;

  @override
  PeerRef? self() => user == null ? null : PeerRef(user!, 'p');

  @override
  void onMessage(PeerRef from, Object? message) {}

  @override
  void onPeer(PeerRef peer, bool up) {}

  @override
  void onStatus(String user, PeerLinkStatus status) {}
}

void main() {
  group('roomLink', () {
    test('joins the room of its user and learns who is in it', () {
      fakeAsync((async) {
        final relay = FakeRelay();
        final a = Member(relay, 'ann', 'a');
        async.flushMicrotasks();
        expect(a.status, [connecting, connected]);
        expect(relay.sockets[0].sent[0],
            {'t': 'hello', 'v': 1, 'token': 't-ann', 'peer': 'a'});
        final b = Member(relay, 'ann', 'b');
        final c = Member(relay, 'ann', 'c');
        async.flushMicrotasks();
        expect(a.events, ['up b', 'up c']);
        expect(b.events, ['up a', 'up c']);
        expect(c.events, ['up a', 'up b']);
      });
    });

    test('sends to everybody or to the devices named, never to another user',
        () {
      fakeAsync((async) {
        final relay = FakeRelay();
        final a = Member(relay, 'ann', 'a');
        final b = Member(relay, 'ann', 'b');
        final c = Member(relay, 'ann', 'c');
        final other = Member(relay, 'bob', 'x');
        async.flushMicrotasks();
        a.link.send(null, {'n': 1});
        a.link.send(['c'], {'n': 2});
        a.link.send(['nobody', 'b'], {'n': 3});
        a.link.send(['nobody'], {'n': 4});
        async.flushMicrotasks();
        expect(b.messages, [
          [
            'a',
            {'n': 1}
          ],
          [
            'a',
            {'n': 3}
          ]
        ]);
        expect(c.messages, [
          [
            'a',
            {'n': 1}
          ],
          [
            'a',
            {'n': 2}
          ]
        ]);
        expect(a.messages, isEmpty);
        expect(other.messages, isEmpty);
        expect(other.events, isEmpty);
        // To all of them is one frame without a list.
        final sent = [
          for (final f in relay.sockets[0].sent)
            if (f['t'] == 'msg') f
        ];
        expect(sent, [
          {
            't': 'msg',
            'd': {
              'm': {'n': 1}
            }
          },
          {
            't': 'msg',
            'to': ['c'],
            'd': {
              'm': {'n': 2}
            }
          },
          {
            't': 'msg',
            'to': ['b'],
            'd': {
              'm': {'n': 3}
            }
          },
        ]);
      });
    });

    test('reports a device that left, and ignores what it sends after', () {
      fakeAsync((async) {
        final relay = FakeRelay();
        final a = Member(relay, 'ann', 'a');
        final b = Member(relay, 'ann', 'b');
        async.flushMicrotasks();
        b.link.close();
        async.flushMicrotasks();
        expect(a.events, ['up b', 'down b']);
        b.link.send(null, {'late': true});
        a.link.send(['b'], {'n': 1});
        async.flushMicrotasks();
        expect(a.messages, isEmpty);
        expect(relay.rooms['user:ann'], hasLength(1));
      });
    });

    test('comes back after the socket drops, with everybody down in between',
        () {
      fakeAsync((async) {
        final relay = FakeRelay();
        final a = Member(relay, 'ann', 'a');
        final b = Member(relay, 'ann', 'b');
        async.elapse(ms(1));
        expect(a.events, ['up b']);
        relay.down = true;
        relay.sockets[0].drop();
        async.elapse(ms(1));
        expect(a.events, ['up b', 'down b']);
        expect(a.status.last, connecting);
        a.link.send(null, {'lost': true});
        // Refused while the relay is down, a little later each time.
        async.elapse(ms(5000));
        expect(a.status.last, failed);
        final tries = relay.sockets.length;
        expect(tries, greaterThan(3));
        relay.down = false;
        async.elapse(ms(40000));
        expect(a.status.last, connected);
        expect(relay.sockets.length, lessThan(tries + 4),
            reason: 'the waits grow, up to 30 s');
        expect(a.events, ['up b', 'down b', 'up b']);
        expect(b.events, ['up a', 'down a', 'up a']);
        expect(b.messages, isEmpty);
        a.link.close();
        b.link.close();
      });
    });

    test('is refused without a valid token, and asks again later', () {
      fakeAsync((async) {
        final relay = FakeRelay();
        final a = Member(relay, 'ann', 'a');
        a.token = 'nonsense';
        // The first socket was opened with the good token already.
        relay.sockets[0].drop();
        async.elapse(ms(1000));
        expect(a.status.last, failed);
        expect(relay.rooms, isEmpty);
        final tries = relay.sockets.length;
        async.elapse(ms(20000));
        expect(relay.sockets.length, tries);
        a.token = 't-ann';
        async.elapse(ms(15000));
        expect(a.status.last, connected);
        a.link.close();
      });
    });

    test('waits for a token before it opens a socket', () {
      fakeAsync((async) {
        final relay = FakeRelay();
        final a = Member(relay, 'ann', 'a');
        relay.sockets[0].drop();
        a.token = null;
        async.elapse(ms(3000));
        final before = relay.sockets.length;
        async.elapse(ms(3000));
        expect(relay.sockets.length, before);
        a.token = 't-ann';
        async.elapse(ms(1000));
        expect(a.status.last, connected);
        a.link.close();
      });
    });

    test('signs in again at once when the relay ends a long session', () {
      fakeAsync((async) {
        final relay = FakeRelay();
        final a = Member(relay, 'ann', 'a');
        async.elapse(ms(1));
        relay.kick('user:ann', 'a', 'reauth');
        async.elapse(ms(1000));
        expect(a.status, [connecting, connected, connecting, connected]);
        expect(relay.sockets, hasLength(2));
        a.link.close();
      });
    });

    test('waits out any other refusal, a full room say', () {
      fakeAsync((async) {
        final relay = FakeRelay()..refuseWith = 'room_full';
        final a = Member(relay, 'ann', 'a');
        async.elapse(ms(20000));
        expect(a.status, [connecting, failed]);
        expect(relay.sockets, hasLength(1));
        async.elapse(ms(11000));
        expect(a.status.last, connected);
        a.link.close();
      });
    });

    test('notices a socket that stopped answering', () {
      fakeAsync((async) {
        final relay = FakeRelay();
        final a = Member(relay, 'ann', 'a');
        async.elapse(ms(1));
        async.elapse(ms(20000));
        expect(relay.sockets[0].sent.last, {'t': 'ping'});
        expect(relay.sockets[0].isOpen, isTrue);
        // Answered: an hour of it changes nothing.
        async.elapse(const Duration(hours: 1));
        expect(relay.sockets, hasLength(1));
        relay.mute = true;
        async.elapse(ms(40000));
        expect(relay.sockets[0].isClosed, isTrue);
        relay.mute = false;
        async.elapse(ms(5000));
        expect(a.status.last, connected);
        a.link.close();
      });
    });

    test('does not join when the relay answers for another user', () {
      fakeAsync((async) {
        final relay = FakeRelay();
        final logs = <LogRecord>[];
        final sub = Logger('room-test').onRecord.listen(logs.add);
        final events = <Object>[];
        final link = roomLink(
            RoomLinkHooks(
              self: () => const PeerRef('user:ann', 'a'),
              onPeer: (p, up) => events.add(p),
              onMessage: (from, data) => events.add('message'),
              onStatus: events.add,
            ),
            RoomLinkOptions(
                url: 'ws://relay.test/room',
                token: () => 't-bob',
                connect: relay.connect,
                logger: SpookyLogger.root('room-test')));
        final bob = Member(relay, 'bob', 'x');
        async.flushMicrotasks();
        expect([
          for (final r in logs)
            if (r.level == Level.WARNING) r.message
        ], hasLength(1));
        expect(events, [connecting, failed]);
        link.send(null, {'n': 1});
        async.flushMicrotasks();
        expect(bob.messages, isEmpty);
        link.close();
        sub.cancel();
      });
    });

    test('takes a welcome that spells the user the way the database prints it',
        () {
      fakeAsync((async) {
        final relay = FakeRelay(
            userOf: (_) => 'user:0190f2a4-7c1b-7d3e-9f00-795045881219')
          ..spellUser = (user) => "user:u'${user.substring(5)}'";
        final status = <PeerLinkStatus>[];
        final link = roomLink(
            RoomLinkHooks(
              self: () => const PeerRef(
                  'user:0190f2a4-7c1b-7d3e-9f00-795045881219', 'a'),
              onPeer: (p, up) {},
              onMessage: (from, data) {},
              onStatus: status.add,
            ),
            RoomLinkOptions(
                url: 'ws://relay.test/room',
                token: () => 'jwt',
                connect: relay.connect));
        async.flushMicrotasks();
        expect(status, [connecting, connected]);
        link.close();
      });
    });

    test('does nothing while nobody is signed in, or after it is closed', () {
      fakeAsync((async) {
        final relay = FakeRelay();
        final link = roomLink(
            RoomLinkHooks(
              self: () => null,
              onPeer: (p, up) {},
              onMessage: (from, data) {},
              onStatus: (_) {},
            ),
            RoomLinkOptions(
                url: 'ws://relay.test/room',
                token: () => 't-ann',
                connect: relay.connect));
        async.elapse(ms(60000));
        expect(relay.sockets, isEmpty);
        link.close();
        link.close();
        final a = Member(relay, 'ann', 'a');
        async.flushMicrotasks();
        a.link.close();
        relay.down = false;
        async.elapse(const Duration(minutes: 5));
        expect(relay.sockets, hasLength(1), reason: 'no reconnect once closed');
        expect(a.status, [connecting, connected]);
      });
    });

    test('says it failed when the address cannot be opened', () {
      fakeAsync((async) {
        final status = <PeerLinkStatus>[];
        final link = roomLink(
            RoomLinkHooks(
              self: () => const PeerRef('user:ann', 'a'),
              onPeer: (p, up) {},
              onMessage: (from, data) {},
              onStatus: status.add,
            ),
            RoomLinkOptions(
                url: 'ws://relay.test/room',
                token: () => 't-ann',
                connect: (_) => throw ArgumentError('no such scheme')));
        async.elapse(ms(60000));
        expect(status, [connecting, failed]);
        link.close();
      });
    });

    test("ignores the signaling of a browser's direct link, and odd frames",
        () {
      fakeAsync((async) {
        final relay = FakeRelay();
        final a = Member(relay, 'ann', 'a');
        final browser = RawMember(relay, token: 't-ann', peer: 'b');
        async.flushMicrotasks();
        final sent = relay.sockets[0].sent.length;
        // What a browser alone in the room with this device sends first.
        browser.send({
          's': {'kind': 'hello', 'payload': null, 'toPeer': null}
        });
        browser.send('text');
        browser.send(null);
        browser.send({'other': 1});
        async.flushMicrotasks();
        expect(a.messages, isEmpty);
        expect(relay.sockets[0].sent, hasLength(sent),
            reason: 'no answer: the browser stays on the relay');
        browser.send({
          'm': {'n': 1}
        });
        // The relay's own frames, malformed: nothing to act on.
        final socket = relay.sockets[0];
        socket.push('not an object');
        socket.push({'t': 'join'});
        socket.push({'t': 'leave', 'peer': 'nobody'});
        socket.push({
          't': 'msg',
          'from': 'stranger',
          'd': {'m': 1}
        });
        socket.push({'t': 'welcome', 'user': 'user:eve', 'peers': []});
        async.flushMicrotasks();
        expect(a.messages, [
          [
            'b',
            {'n': 1}
          ]
        ]);
        expect(a.events, ['up b']);
        expect(a.status, [connecting, connected]);
        a.link.close();
      });
    });

    test('waits half a second, then twice as long each time up to thirty',
        () async {
      // A clock of the test's own: every wait is written down, and ends when
      // the test says so.
      final waits = <Duration>[];
      void Function()? fire;
      Timer manual(Duration delay, void Function() callback) {
        waits.add(delay);
        fire = callback;
        return _ManualTimer();
      }

      Future<List<int>> backoff(Random random) async {
        waits.clear();
        final relay = FakeRelay()..down = true;
        final link = roomLink(
            RoomLinkHooks(
              self: () => const PeerRef('user:ann', 'a'),
              onPeer: (p, up) {},
              onMessage: (from, data) {},
              onStatus: (_) {},
            ),
            RoomLinkOptions(
                url: 'ws://relay.test/room',
                token: () => 't-ann',
                connect: relay.connect,
                timer: manual,
                random: random));
        for (var attempt = 0; attempt < 9; attempt++) {
          await pumpEventQueue();
          expect(waits, hasLength(attempt + 1));
          fire!();
        }
        link.close();
        expect(relay.sockets, hasLength(10));
        return [for (final wait in waits) wait.inMilliseconds];
      }

      // In the middle of the spread the waits are the plain doubling.
      expect(await backoff(_Fixed(0.5)),
          [500, 1000, 2000, 4000, 8000, 16000, 30000, 30000, 30000]);
      // And each is a quarter off at most, so devices do not return as one.
      expect(await backoff(_Fixed(0)),
          [375, 750, 1500, 3000, 6000, 12000, 22500, 22500, 22500]);
      final spread = await backoff(Random(7));
      const plain = [500, 1000, 2000, 4000, 8000, 16000, 30000, 30000, 30000];
      for (var i = 0; i < plain.length; i++) {
        expect(spread[i], inInclusiveRange(plain[i] * 0.75, plain[i] * 1.25));
      }
      expect(spread, isNot(plain));
    });

    test('compares record ids whatever escaping the database added', () {
      expect(sameUser('user:⟨a-b⟩', 'user:a-b'), isTrue);
      expect(sameUser('user:`a-b`', 'user:⟨a-b⟩'), isTrue);
      expect(
          sameUser("user:u'0190f2a4-7c1b-7d3e-9f00-795045881219'",
              'user:0190f2a4-7c1b-7d3e-9f00-795045881219'),
          isTrue);
      expect(sameUser('user:a', 'user:b'), isFalse);
      expect(sameUser('user:ua', 'user:a'), isFalse);
      expect(sameUser('admin:a', 'user:a'), isFalse);
    });
  });

  group('roomTransport: the room as channel peers', () {
    test(
        'shows a write of one Dart device on the other, and sends only what the other shows',
        () {
      fakeAsync((async) {
        final relay = FakeRelay();
        List<Row> held() => [
              note('1'),
              note('2', {'folder': 'b'})
            ];
        final phone =
            Device('user:ann', viaRelay(relay, 't-ann'), held: held());
        final laptop =
            Device('user:ann', viaRelay(relay, 't-ann'), held: held());
        final tablet =
            Device('user:ann', viaRelay(relay, 't-ann'), held: held());
        final bob = Device('user:bob', viaRelay(relay, 't-bob'), held: held());
        final view = laptop.show(inFolder('a'));
        final theirs = bob.show();
        async.flushMicrotasks();
        expect(relay.rooms['user:ann'], hasLength(3));
        // Each device joined under its channel peer id.
        expect(relay.rooms['user:ann']!.keys,
            containsAll([phone.m.peerId, laptop.m.peerId, tablet.m.peerId]));
        for (final d in [phone, laptop, tablet]) {
          expect(d.m.ownState,
              const ChannelState(ChannelStatus.connected, ['user:ann']));
        }
        expect(bob.m.ownState.status, ChannelStatus.connecting,
            reason: 'alone in his room');

        final before = relay.forwarded;
        phone.m.local('note', 'note:1', {'title': 'from the phone'}, null);
        phone.m.local('note', 'note:2', {'title': 'elsewhere'}, null);
        async.flushMicrotasks();
        expect(view.last[0]['title'], 'from the phone');
        expect(view.last[1]['title'], 't');
        // The tablet shows nothing and bob is somebody else: one frame, to the
        // laptop.
        expect(relay.forwarded - before, 1);
        expect(theirs.last[0]['title'], 't');

        // Signed out, the device leaves the room; signed in, it is back.
        laptop.m.setUser(null);
        async.flushMicrotasks();
        expect(relay.rooms['user:ann'], hasLength(2));
        expect(laptop.m.ownState.status, ChannelStatus.idle);
        laptop.m.setUser('user:ann');
        async.flushMicrotasks();
        expect(relay.rooms['user:ann'], hasLength(3));
        phone.m.local('note', 'note:1', {'title': 'again'}, null);
        async.flushMicrotasks();
        expect(view.last[0]['title'], 'again');

        // The relay goes away: nobody is connected, and nothing is lost but
        // speed. It comes back: so do they.
        relay.down = true;
        for (final socket in relay.sockets.toList()) {
          socket.drop();
        }
        async.elapse(ms(100));
        expect(phone.m.ownState.status, ChannelStatus.connecting);
        async.elapse(ms(10000));
        expect(phone.m.ownState.status, ChannelStatus.failed);
        relay.down = false;
        async.elapse(ms(60000));
        expect(phone.m.ownState,
            const ChannelState(ChannelStatus.connected, ['user:ann']));
        phone.m.local('note', 'note:1', {'title': 'after'}, null);
        async.flushMicrotasks();
        expect(view.last[0]['title'], 'after');
        for (final d in [phone, laptop, tablet, bob]) {
          d.m.close();
        }
        async.flushMicrotasks();
        expect(relay.rooms, isEmpty);
      });
    });

    test('speaks the envelope of a browser hub with two tabs', () {
      fakeAsync((async) {
        final relay = FakeRelay();
        // A browser of the same user. Its hub tab `b1` holds the socket and
        // speaks for the tabs `b1` and `b2`; the frames it sends below are the
        // ones `hubTransport` over `roomLink` writes in packages/core.
        final browser = RawMember(relay, token: 't-ann', peer: 'b1');
        async.flushMicrotasks();
        final phone = Device('user:ann', viaRelay(relay, 't-ann'),
            held: [
              note('1'),
              note('2', {'folder': 'b'})
            ],
            now: () => 1000);
        final view = phone.show(inFolder('a'));
        async.flushMicrotasks();
        final me = phone.m.peerId;
        expect(RegExp(r'^[A-Za-z0-9_-]{1,64}$').hasMatch(me), isTrue);
        expect(relay.sockets[1].sent,
            hasLength(2)); // hello, and the tabs it speaks for
        expect(relay.sockets[1].sent[0],
            {'t': 'hello', 'v': 1, 'token': 't-ann', 'peer': me});
        // On meeting the browser it says which tabs it speaks for: itself.
        expect(relay.sockets[1].sent[1], {
          't': 'msg',
          'd': {
            'm': {
              'h': 'tabs',
              'tabs': [me]
            }
          }
        });
        expect(browser.received, [
          [
            me,
            {
              'm': {
                'h': 'tabs',
                'tabs': [me]
              }
            }
          ]
        ]);
        expect(phone.m.ownState.status, ChannelStatus.connecting,
            reason: 'a member is not a peer before it names its tabs');

        // The browser's hub does the same, on seeing this device join.
        browser.send({
          'm': {
            'h': 'tabs',
            'tabs': ['b1', 'b2']
          }
        });
        async.flushMicrotasks();
        expect(phone.m.ownState,
            const ChannelState(ChannelStatus.connected, ['user:ann']));
        // Each tab is a peer of its own, and is told what this device shows.
        final interest = {
          't': 'interest',
          'v': {
            'note': [
              [eq('folder', 'a')]
            ]
          }
        };
        expect(browser.received.sublist(1), [
          for (final tab in ['b1', 'b2'])
            [
              me,
              {
                'm': {
                  'h': 'm',
                  'from': me,
                  'to': [tab],
                  'm': interest
                }
              }
            ]
        ]);

        // Tab b1 shows folder a, tab b2 no note at all.
        browser.send({
          'm': {
            'h': 'm',
            'from': 'b1',
            'to': [me],
            'm': interest
          }
        });
        browser.send({
          'm': {
            'h': 'm',
            'from': 'b2',
            'to': [me],
            'm': {'t': 'interest', 'v': {}}
          }
        });
        async.flushMicrotasks();
        var seen = browser.received.length;
        phone.m.local('note', 'note:1', {'title': 'from the phone'}, null);
        phone.m.local('note', 'note:2', {'title': 'in b'}, null);
        async.flushMicrotasks();
        // One frame, naming the one tab that shows the row.
        expect(browser.received.sublist(seen), [
          [
            me,
            {
              'm': {
                'h': 'm',
                'from': me,
                'to': ['b1'],
                'm': {
                  't': 'op',
                  'table': 'note',
                  'id': 'note:1',
                  'patch': {'title': 'from the phone'},
                  'hlc': [1000, 0, me]
                }
              }
            }
          ]
        ]);

        // A write of tab b2 shows here at once.
        Map<String, dynamic> op(String title, int at) => {
              't': 'op',
              'table': 'note',
              'id': 'note:1',
              'patch': {'title': title},
              'hlc': [at, 0, 'b2']
            };
        browser.send({
          'm': {
            'h': 'm',
            'from': 'b2',
            'to': [me],
            'm': op('from b2', 2000)
          }
        });
        async.flushMicrotasks();
        expect(view.last[0]['title'], 'from b2');

        // What is not a message of one of its tabs to this device is dropped.
        final shown = view.seen.length;
        for (final forged in [
          // a tab the browser does not speak for
          {'h': 'm', 'from': 'zz', 'to': null, 'm': op('forged', 3000)},
          // `to` is not a list
          {'h': 'm', 'from': 'b1', 'to': me, 'm': op('forged', 3000)},
          // for another tab
          {
            'h': 'm',
            'from': 'b1',
            'to': ['elsewhere'],
            'm': op('forged', 3000)
          },
          // for nobody
          {'h': 'm', 'from': 'b1', 'm': op('forged', 3000)},
          {'h': 'tabs', 'tabs': 'nope'},
          {'h': 'what'},
          null,
        ]) {
          browser.send({'m': forged});
        }
        async.flushMicrotasks();
        expect(view.seen, hasLength(shown));
        expect(phone.m.ownState.status, ChannelStatus.connected);
        // An explicit null is "every tab".
        browser.send({
          'm': {'h': 'm', 'from': 'b1', 'to': null, 'm': op('to all', 4000)}
        });
        async.flushMicrotasks();
        expect(view.last[0]['title'], 'to all');

        // Both tabs show the row now: one frame names them both.
        browser.send({
          'm': {
            'h': 'm',
            'from': 'b2',
            'to': [me],
            'm': {'t': 'interest', 'all': true}
          }
        });
        async.flushMicrotasks();
        seen = browser.received.length;
        phone.m.local('note', 'note:1', {'title': 'for both'}, null);
        async.flushMicrotasks();
        expect(browser.received.sublist(seen), hasLength(1));
        expect(((browser.received.last[1] as Map)['m'] as Map)['to'],
            ['b1', 'b2']);

        // Tab b2 closes: its hub names the tabs that are left.
        browser.send({
          'm': {
            'h': 'tabs',
            'tabs': ['b1']
          }
        });
        async.flushMicrotasks();
        seen = browser.received.length;
        phone.m.local('note', 'note:1', {'title': 'for one'}, null);
        async.flushMicrotasks();
        expect(((browser.received.last[1] as Map)['m'] as Map)['to'], ['b1']);
        expect(browser.received.sublist(seen), hasLength(1));

        // The browser goes: nobody is left.
        browser.leave();
        async.flushMicrotasks();
        expect(phone.m.ownState.status, ChannelStatus.connecting);
        phone.m.local('note', 'note:1', {'title': 'alone'}, null);
        async.flushMicrotasks();
        phone.m.close();
      });
    });

    // The relay hands a seat back to its peer id without telling the others
    // when the old socket still looked alive, which is what a phone changing
    // networks produces: the device is welcomed, and nobody says who they are.

    test('a Dart device that took its seat back is told again who is there',
        () {
      fakeAsync((async) {
        final relay = FakeRelay();
        final phone =
            Device('user:ann', viaRelay(relay, 't-ann'), held: [note('1')]);
        final laptop =
            Device('user:ann', viaRelay(relay, 't-ann'), held: [note('1')]);
        final onPhone = phone.show(inFolder('a'));
        final onLaptop = laptop.show(inFolder('a'));
        async.flushMicrotasks();
        const up = ChannelState(ChannelStatus.connected, ['user:ann']);
        expect(phone.m.ownState, up);

        relay.sockets[0].lose();
        async.elapse(ms(1000));
        expect(relay.sockets, hasLength(3), reason: 'the phone is back');
        expect(relay.rooms['user:ann'], hasLength(2));
        // The laptop saw neither a leave nor a join, only the phone naming
        // its tab again, and answered with its own.
        expect(phone.m.ownState, up);
        expect(laptop.m.ownState, up);
        laptop.m.local('note', 'note:1', {'title': 'to the phone'}, null);
        async.flushMicrotasks();
        expect(onPhone.last[0]['title'], 'to the phone');
        phone.m.local('note', 'note:1', {'title': 'to the laptop'}, null);
        async.flushMicrotasks();
        expect(onLaptop.last[0]['title'], 'to the laptop');
        // Everybody answered: no reason to join anew.
        async.elapse(ms(30000));
        expect(relay.sockets, hasLength(3));
        // And again, as often as the network changes.
        relay.sockets[2].lose();
        async.elapse(ms(2000));
        expect(phone.m.ownState, up);
        phone.m.close();
        laptop.m.close();
      });
    });

    test('tells a browser that took its seat back who this device is', () {
      fakeAsync((async) {
        final relay = FakeRelay();
        final browser = RawMember(relay, token: 't-ann', peer: 'b1');
        async.flushMicrotasks();
        final phone =
            Device('user:ann', viaRelay(relay, 't-ann'), held: [note('1')]);
        phone.show(inFolder('a'));
        async.flushMicrotasks();
        final me = phone.m.peerId;
        const tabs = {
          'm': {
            'h': 'tabs',
            'tabs': ['b1', 'b2']
          }
        };
        browser.send(tabs);
        async.flushMicrotasks();
        expect(phone.m.ownState.status, ChannelStatus.connected);

        // The hub is welcomed on its old seat and names its tabs to everybody
        // it finds there. This device never saw it go.
        var seen = browser.received.length;
        browser.send(tabs);
        async.flushMicrotasks();
        final interest = {
          't': 'interest',
          'v': {
            'note': [
              [eq('folder', 'a')]
            ]
          }
        };
        expect(browser.received.sublist(seen), [
          [
            me,
            {
              'm': {
                'h': 'tabs',
                'tabs': [me]
              }
            }
          ],
          for (final tab in ['b1', 'b2'])
            [
              me,
              {
                'm': {
                  'h': 'm',
                  'from': me,
                  'to': [tab],
                  'm': interest
                }
              }
            ],
        ]);
        expect(phone.m.ownState.status, ChannelStatus.connected);

        // Not more than once a second, whatever the other side does.
        seen = browser.received.length;
        browser.send(tabs);
        browser.send(tabs);
        async.flushMicrotasks();
        expect(browser.received, hasLength(seen));
        async.elapse(ms(1000));
        browser.send(tabs);
        async.flushMicrotasks();
        expect(browser.received, hasLength(seen + 3));
        // A changed list is news, not a return: nothing to answer.
        seen = browser.received.length;
        browser.send({
          'm': {
            'h': 'tabs',
            'tabs': ['b1']
          }
        });
        async.elapse(ms(2000));
        expect(browser.received, hasLength(seen));
        phone.m.close();
      });
    });

    test(
        'joins anew, for all to see, when a browser stays silent after it took its seat back',
        () {
      fakeAsync((async) {
        final relay = FakeRelay();
        // A browser hub as packages/core has it: it names its tabs to a
        // member when that member joins, and at no other time.
        var announced = 0;
        final browser = RawMember(relay, token: 't-ann', peer: 'b1',
            onFrame: (self, frame) {
          if (frame['t'] != 'join') return;
          announced++;
          self.send({
            'm': {
              'h': 'tabs',
              'tabs': ['b1', 'b2']
            }
          });
        });
        async.flushMicrotasks();
        final phone =
            Device('user:ann', viaRelay(relay, 't-ann'), held: [note('1')]);
        async.flushMicrotasks();
        const up = ChannelState(ChannelStatus.connected, ['user:ann']);
        expect(phone.m.ownState, up);
        final me = phone.m.peerId;

        relay.sockets[1].lose();
        async.elapse(ms(1000));
        expect(relay.sockets, hasLength(3));
        expect(announced, 1, reason: 'the browser saw nobody join');
        expect(phone.m.ownState.status, ChannelStatus.connecting);
        // A moment of that, and the phone leaves and joins again.
        async.elapse(ms(4000));
        expect([
          for (final f in browser.frames)
            if (f['t'] == 'leave' || f['t'] == 'join') '${f['t']} ${f['peer']}'
        ], [
          'join $me',
          'leave $me',
          'join $me'
        ]);
        expect(phone.m.ownState, up);
        expect(relay.sockets, hasLength(4));
        async.elapse(const Duration(minutes: 10));
        expect(relay.sockets, hasLength(4));
        phone.m.close();
      });
    });

    test(
        'a member that never names its tabs costs one such join per lost socket',
        () {
      fakeAsync((async) {
        final relay = FakeRelay();
        RawMember(relay, token: 't-ann', peer: 'mute');
        async.flushMicrotasks();
        final phone = Device('user:ann', viaRelay(relay, 't-ann'));
        async.elapse(const Duration(minutes: 1));
        expect(relay.sockets, hasLength(2),
            reason: 'a first hello is seen by all: nothing to repair');
        relay.sockets[1].lose();
        async.elapse(const Duration(minutes: 10));
        expect(relay.sockets, hasLength(4));
        expect(phone.m.ownState.status, ChannelStatus.connecting);
        phone.m.close();
      });
    });

    test('leaves when the user is no longer wanted, and returns when wanted',
        () {
      fakeAsync((async) {
        final relay = FakeRelay();
        final events = <String>[];
        final transport =
            viaRelay(relay, 't-ann')(_Recording('user:ann', 'p1', events));
        expect(transport.supported, isTrue);
        final other = Member(relay, 'ann', 'p2');
        transport.want({'user:bob'});
        async.flushMicrotasks();
        expect(relay.sockets, hasLength(1), reason: 'only its own user');
        transport.want({'user:ann', 'user:bob'});
        async.flushMicrotasks();
        expect(events, ['status connecting', 'status connected']);
        expect(other.events, ['up p1']);
        // The other member has no tabs yet; once it names one, that is a peer.
        other.link.send([
          'p1'
        ], {
          'h': 'tabs',
          'tabs': ['p2']
        });
        async.flushMicrotasks();
        expect(events.last, 'up user:ann#p2');
        transport.send(const PeerRef('user:bob', 'p2'),
            const InterestMessage(Interest.all()));
        transport.send(const PeerRef('user:ann', 'ghost'),
            const InterestMessage(Interest.all()));
        async.flushMicrotasks();
        expect(other.messages, hasLength(1), reason: 'only its tabs so far');
        transport.want({});
        async.flushMicrotasks();
        expect(events.last, 'down user:ann#p2');
        expect(other.events, ['up p1', 'down p1']);
        transport.want({'user:ann'});
        async.flushMicrotasks();
        expect(other.events, ['up p1', 'down p1', 'up p1']);
        transport.close();
        transport.close();
        async.flushMicrotasks();
        expect(relay.rooms['user:ann'], hasLength(1));
        other.link.close();
      });
    });
  });

  group('splitTransport', () {
    const all = InterestMessage(Interest.all());

    test(
        'reaches the own user through the room and everybody else through the mesh',
        () {
      final log = <String>[];
      final t = splitTransport((_) => LogTransport(log, 'room'),
          (_) => LogTransport(log, 'mesh'))(Hooks('user:a'));
      t.want({'user:a', 'user:b'});
      t.send(const PeerRef('user:a', 'x'), all);
      t.send(const PeerRef('user:b', 'y'), all);
      t.want({'user:b'});
      t.close();
      expect(log, [
        'room want user:a',
        'mesh want user:b',
        'room send user:a',
        'mesh send user:b',
        'room want ',
        'mesh want user:b',
        'room close',
        'mesh close'
      ]);
      expect(t.supported, isTrue);
    });

    test('is the mesh alone without a room', () {
      final log = <String>[];
      final t = splitTransport(null,
          (_) => LogTransport(log, 'mesh', supported: false))(Hooks('user:a'));
      t.want({'user:a', 'user:b'});
      t.send(const PeerRef('user:a', 'x'), all);
      t.close();
      expect(log, ['mesh want user:b', 'mesh send user:a', 'mesh close']);
      expect(t.supported, isFalse);
      splitTransport(null, (_) => LogTransport(log, 'mesh'))(Hooks(null))
          .want({'user:a'});
      expect(log.last, 'mesh want user:a');
    });

    test('a channel is unsupported only when its own half is missing', () {
      ChannelManager manager({required bool room, required bool mesh}) {
        final log = <String>[];
        final m = ChannelManager(schema,
            config: share,
            transport: splitTransport(
                room ? (_) => LogTransport(log, 'room') : null,
                (_) => LogTransport(log, 'mesh', supported: mesh)));
        m.setUser('user:a');
        return m;
      }

      ChannelStatus toBob(ChannelManager m) =>
          m.open([const ChannelScope('call', [])], ['user:b']).state.status;

      // A Dart client without a WebRTC factory, with a relay: its own devices
      // connect, a channel to somebody else cannot.
      final relayOnly = manager(room: true, mesh: false);
      expect(relayOnly.ownState.status, ChannelStatus.connecting);
      expect(toBob(relayOnly), ChannelStatus.unsupported);
      // WebRTC and no relay: the other way around.
      final meshOnly = manager(room: false, mesh: true);
      expect(meshOnly.ownState.status, ChannelStatus.unsupported);
      expect(toBob(meshOnly), ChannelStatus.connecting);
      final neither = manager(room: false, mesh: false);
      expect(neither.ownState.status, ChannelStatus.unsupported);
      expect(toBob(neither), ChannelStatus.unsupported);
    });
  });
}

/// A timer that never fires by itself.
class _ManualTimer implements Timer {
  @override
  bool isActive = true;

  @override
  int get tick => 0;

  @override
  void cancel() => isActive = false;
}

/// A source of "random" numbers that always says the same.
class _Fixed implements Random {
  _Fixed(this.value);
  final double value;

  @override
  double nextDouble() => value;

  @override
  dynamic noSuchMethod(Invocation invocation) => super.noSuchMethod(invocation);
}

/// Transport hooks that write down what the transport reports.
class _Recording implements TransportHooks {
  _Recording(this.user, this.peer, this.events);
  final String user;
  final String peer;
  final List<String> events;

  @override
  PeerRef? self() => PeerRef(user, peer);

  @override
  void onMessage(PeerRef from, Object? message) =>
      events.add('message ${from.key}');

  @override
  void onPeer(PeerRef peer, bool up) =>
      events.add('${up ? 'up' : 'down'} ${peer.key}');

  @override
  void onStatus(String user, PeerLinkStatus status) =>
      events.add('status ${status.name}');
}
