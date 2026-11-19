import 'dart:convert';

import 'package:logging/logging.dart';
import 'package:spooky_core/src/modules/channel/channel.dart';
import 'package:spooky_core/src/modules/query_builder.dart';
import 'package:spooky_core/src/services/logger/logger.dart';
import 'package:spooky_core/src/surreal/value.dart';
import 'package:test/test.dart';

import '../channel_net.dart';
import '../channel_share.dart';

// Port of `packages/core/src/modules/channel/share.test.ts`: sharing a user's
// optimistic writes with their own devices, and the interest that keeps a
// device from being sent what it does not show.

/// A query's claims as plain data: each table with its filter in wire form
/// (null for every row).
List<List<Object?>> claims(List<QueryInterest> list) => [
      for (final i in list)
        [i.table, i.where?.map((node) => node.toJson()).toList()]
    ];

QueryInterest noteIn(String folder) =>
    QueryInterest('note', [ChannelCondition('folder', '=', folder)]);

/// An interest after a trip over the wire.
Map<String, dynamic> wire(Interest interest) =>
    (jsonDecode(jsonEncode(encodeInterest(interest).toJson())) as Map)
        .cast<String, dynamic>();

void main() {
  group('interest', () {
    test('reads what a query can show from the builder', () {
      expect(claims(queryInterest(columnsOf, inFolder('a'))), [
        [
          'note',
          [eq('folder', 'a')]
        ]
      ]);
      expect(claims(queryInterest(columnsOf, q('note'))), [
        ['note', <Object?>[]]
      ]);
      // Operators and swapped operands keep their shape.
      expect(
          claims(queryInterest(
              columnsOf,
              q('note').where({
                'folder': const QueryOp('∈', ['b'], swap: true),
                'title': const QueryOp('!=', '')
              }))),
          [
            [
              'note',
              [
                {
                  'field': 'folder',
                  'op': '∈',
                  'value': ['b'],
                  'swap': true
                },
                {'field': 'title', 'op': '!=', 'value': ''},
              ]
            ]
          ]);
      // What the row alone cannot decide is the whole table.
      for (final where in <Map<String, Object?>>[
        {'title': const QueryOp('@@', 'x')},
        {'author.name': 'x'},
        {'folder': const QueryOp('=', r'$missing')},
        {'folder': Object()},
      ]) {
        expect(claims(queryInterest(columnsOf, q('note').where(where))), [
          ['note', null]
        ]);
      }
      // Related tables: every row, however deep.
      final related = queryInterest(columnsOf,
          inFolder('a').related('tags', (tags) => tags.related('secret')));
      expect([
        for (final i in related) [i.table, i.where == null]
      ], [
        ['note', false],
        ['tag', true],
        ['secret', true],
      ]);
    });

    test(
        'types record links and dates by their column, as a browser reads them',
        () {
      final filter = claims(queryInterest(
          columnsOf,
          q('note').where({
            'id': 'note:1',
            'author': const QueryOp('IN', ['user:a', 'user:b']),
            'at': '2026-01-01T00:00:00Z',
            'folder': 'user:a',
          }))).single[1];
      expect(filter, [
        eq('id', {r'$rid': 'note:1'}),
        {
          'field': 'author',
          'op': 'IN',
          'value': [
            {r'$rid': 'user:a'},
            {r'$rid': 'user:b'}
          ]
        },
        eq('at', {r'$date': '2026-01-01T00:00:00.000Z'}),
        // A string column keeps its string, whatever it looks like.
        eq('folder', 'user:a'),
      ]);
    });

    test('merges the open queries into one claim per shared table', () {
      bool shared(String t) => t == 'note' || t == 'tag';
      expect(wire(mergeInterest([], shared)), {'t': 'interest', 'v': {}});
      expect(
          wire(mergeInterest([
            [const QueryInterest('secret', null)]
          ], shared))['v'],
          {});
      final two = wire(mergeInterest([
        [const QueryInterest('tag', null), noteIn('a')],
        [noteIn('b')],
      ], shared))['v'] as Map;
      expect(two, {
        'note': [
          [eq('folder', 'a')],
          [eq('folder', 'b')]
        ],
        'tag': true
      });
      expect(two.keys, ['note', 'tag'], reason: 'tables in a fixed order');
      // The same filter twice is one, and the order of the queries does not
      // show.
      expect(
          wire(mergeInterest([
            [noteIn('b')],
            [noteIn('a')],
            [noteIn('b')]
          ], shared))['v'],
          {
            'note': [
              [eq('folder', 'a')],
              [eq('folder', 'b')]
            ]
          });
      // An unfiltered query, or one nobody described, asks for everything.
      expect(
          wire(mergeInterest([
            [noteIn('a')],
            [const QueryInterest('note', [])],
            [noteIn('c')]
          ], shared))['v'],
          {'note': true});
      expect(
          wire(mergeInterest([
            [noteIn('a')],
            null
          ], shared)),
          {'t': 'interest', 'all': true});
      expect(
          wire(mergeInterest([
            for (var i = 0; i < 30; i++) [noteIn('f$i')]
          ], shared))['v'],
          {'note': true});
    });

    test('survives the wire, record ids and dates included', () {
      final interest = Interest({
        'note': [
          [
            const ChannelCondition('author', '=', RecordId('user', 'a')),
            ChannelOr([
              const ChannelCondition('folder', '=', 'a'),
              ChannelCondition('at', '>',
                  DateTime.fromMillisecondsSinceEpoch(5, isUtc: true),
                  swap: true),
            ]),
          ]
        ],
        'tag': null,
      });
      final text = jsonEncode(encodeInterest(interest).toJson());
      // `JSON.stringify(encodeInterest(...))` of the same interest in
      // packages/core, byte for byte.
      expect(
          text,
          r'{"t":"interest","v":{"note":[[{"field":"author","op":"=","value":{"$rid":"user:a"}},'
          r'{"or":[{"field":"folder","op":"=","value":"a"},'
          r'{"field":"at","op":">","value":{"$date":"1970-01-01T00:00:00.005Z"},"swap":true}]}]],'
          r'"tag":true}}');
      final back = decodeInterest(jsonDecode(text))!;
      // Rows hold record links and dates as strings.
      expect(
          showsRow(back, 'note', [
            {'author': 'user:a', 'folder': 'a'}
          ]),
          isTrue);
      expect(
          showsRow(back, 'note', [
            {'author': 'user:b', 'folder': 'a'}
          ]),
          isFalse);
      expect(
          showsRow(back, 'note', [
            {'author': 'user:a', 'folder': 'z', 'at': '1970-01-01T00:00:00Z'}
          ]),
          isTrue,
          reason: 'the other arm of the group: 5 ms is after the row');
      expect(
          showsRow(back, 'note', [
            {'author': 'user:a', 'folder': 'z', 'at': '1970-01-01T00:00:01Z'}
          ]),
          isFalse);
      expect(showsRow(back, 'tag', [{}]), isTrue);
      expect(showsRow(back, 'secret', [{}]), isFalse);
      expect(
          decodeInterest(jsonDecode(
                  jsonEncode(encodeInterest(const Interest.all()).toJson())))
              ?.all,
          isTrue);
      expect(showsRow(const Interest.all(), 'anything', []), isTrue);
      // Before or after a write: either is enough.
      expect(
          showsRow(back, 'note', [
            {'author': 'user:b'},
            {'author': 'user:a', 'folder': 'a'}
          ]),
          isTrue);
    });

    test(
        'reads a filter it cannot evaluate as the whole table, and refuses what is not an interest',
        () {
      for (final raw in [
        null,
        'x',
        {'t': 'op'},
        {'t': 'interest'},
        {'t': 'interest', 'v': []},
        {'t': 'interest', 'v': null},
      ]) {
        expect(decodeInterest(raw), isNull);
      }
      final odd = decodeInterest({
        't': 'interest',
        'v': {
          'a': [
            [
              {'field': 'x', 'op': 'constructor', 'value': 1}
            ]
          ],
          'b': [
            [
              {'field': 1, 'op': '=', 'value': 1}
            ]
          ],
          'c': ['nope'],
          'd': [
            [
              {'or': []}
            ]
          ],
          'e': [
            for (var i = 0; i < 40; i++) [eq('x', 1)]
          ],
          'f': [
            [null]
          ],
          'g': 'yes',
          'h': [
            [eq('x', 1)]
          ],
        },
      })!;
      expect(wire(odd)['v'], {
        'a': true,
        'b': true,
        'c': true,
        'd': true,
        'e': true,
        'f': true,
        'h': [
          [eq('x', 1)]
        ]
      });
    });
  });

  group('ChannelManager: sharing with the own devices', () {
    test(
        'sends an update of a shared table to the other device, which shows it at once',
        () async {
      final net = MemoryNet();
      final phone = Device('user:a', net.factory, held: [note('1')]);
      final laptop = Device('user:a', net.factory, held: [note('1')]);
      final view = laptop.show(inFolder('a'));
      await flush();
      expect(phone.m.sharing, isTrue);
      expect(phone.m.enabled, isTrue);
      expect(phone.m.ownState,
          const ChannelState(ChannelStatus.connected, ['user:a']));
      phone.m.local('note', 'note:1', {'title': 'new'}, null);
      await flush();
      expect(view.last[0]['title'], 'new');
      // Never in the store: the synced row confirms it, or it expires.
      expect(laptop.store['note:1']?['title'], 't');
    });

    test('carries creates and deletes too', () async {
      final net = MemoryNet();
      final phone = Device('user:a', net.factory, held: [note('1')]);
      final laptop = Device('user:a', net.factory, held: [note('1')]);
      laptop.show();
      await flush();
      phone.m.localCreate('note', 'note:2', {'folder': 'a', 'title': 'two'});
      await phone.m.localDelete('note', 'note:1');
      await flush();
      expect(laptop.calls, ['create note:2', 'delete note:1']);
      expect(laptop.store['note:2'], containsPair('title', 'two'));
      expect(laptop.store['note:2'], containsPair('folder', 'a'));
    });

    test('shares nothing of a table that was not named, or was turned off',
        () async {
      final net = MemoryNet();
      const config = ChannelsConfig(
          share: ChannelShareConfig(tables: {
        'note': ChannelShareRule.all,
        'secret': ChannelShareRule.none,
      }));
      final phone = Device('user:a', net.factory, config: config, held: [
        {'id': 'secret:1', 'text': 'x'},
        {'id': 'tag:1', 'note': 'note:1', 'label': 'l'}
      ]);
      final laptop = Device('user:a', net.factory, config: config, held: [
        {'id': 'secret:1', 'text': 'x'}
      ]);
      final view = laptop.show();
      await flush();
      phone.m.local('secret', 'secret:1', {'text': 'y'}, null);
      phone.m.local('tag', 'tag:1', {'label': 'm'}, null);
      await flush();
      expect(net.ops, isEmpty);
      // And a device takes none either, whoever sends it.
      laptop.m.receive(PeerRef('user:a', phone.m.peerId), {
        't': 'op',
        'table': 'secret',
        'id': 'secret:1',
        'patch': {'text': 'z'},
        'hlc': [1, 0, 'x']
      });
      await flush();
      expect(laptop.calls, isEmpty);
      expect(view.last[0]['text'], 'x');
    });

    test('warns about a table the schema does not have and shares the rest',
        () {
      final logs = <String>[];
      final sub = Logger('share-test').onRecord.listen((r) {
        logs.add(r.message);
      });
      addTearDown(sub.cancel);
      final m = ChannelManager(schema,
          logger: SpookyLogger.root('share-test'),
          config: const ChannelsConfig(
              share: ChannelShareConfig(tables: {
            'nope': ChannelShareRule.all,
            'relationships': ChannelShareRule.all,
            'note': ChannelShareRule.all,
          })));
      expect(logs, hasLength(2));
      expect(logs.first, contains("'nope'"));
      expect(m.sharing, isTrue);
      expect(
          ChannelManager(schema,
              config: const ChannelsConfig(
                  share: ChannelShareConfig(
                      tables: {'note': ChannelShareRule.none}))).sharing,
          isFalse);
      expect(ChannelManager(schema).ownState,
          const ChannelState(ChannelStatus.idle));
    });

    test(
        'applies a rule given as a function on the device that writes and on the one that receives',
        () async {
      final net = MemoryNet();
      final config = ChannelsConfig(
          share: ChannelShareConfig(tables: {
        'note': ChannelShareRule.where((row) => row['draft'] != true),
      }));
      List<Row> held() => [
            note('1', {'draft': true}),
            note('2'),
            note('5', {'draft': true})
          ];
      final phone = Device('user:a', net.factory, config: config, held: held());
      final laptop =
          Device('user:a', net.factory, config: config, held: held());
      final view = laptop.show();
      await flush();
      phone.m.local('note', 'note:1', {'title': 'still a draft'}, null);
      phone.m.local('note', 'note:2', {'title': 'public'}, null);
      await flush();
      expect([for (final s in net.ops) s.message['id']], ['note:2']);
      expect([for (final r in view.last) r['title']], ['t', 'public', 't']);
      // Publishing a draft: the row after the write passes, so it goes out.
      phone.m.local('note', 'note:1', {'draft': false}, null);
      await flush();
      expect([for (final s in net.ops) s.message['id']], ['note:2', 'note:1']);
      // A device whose own rule says no drops what another build of the app
      // sent.
      laptop.m.receive(const PeerRef('user:a', 'other'), {
        't': 'op',
        'table': 'note',
        'id': 'note:5',
        'patch': {'title': 'forced'},
        'hlc': [9000000000000000, 0, 'x']
      });
      await flush();
      expect(view.last[2]['title'], 't');
      // A rule that throws shares nothing.
      final bad = Device('user:a', net.factory,
          config: ChannelsConfig(
              share: ChannelShareConfig(tables: {
            'note': ChannelShareRule.where((_) => throw StateError('boom')),
          })),
          held: [note('3')]);
      bad.m.local('note', 'note:3', {'title': 'x'}, null);
      await flush();
      expect(net.ops, hasLength(2));
    });

    test('never shares with another user, and takes nothing shared from one',
        () async {
      final net = MemoryNet();
      final ann = Device('user:a', net.factory, held: [note('1')]);
      final bob = Device('user:b', net.factory, held: [note('1')]);
      final view = bob.show();
      await flush();
      ann.m.local('note', 'note:1', {'title': 'mine'}, null);
      await flush();
      expect(net.sent, isEmpty);
      bob.m.receive(const PeerRef('user:a', 'x'), {
        't': 'op',
        'table': 'note',
        'id': 'note:1',
        'patch': {'title': 'mine'},
        'hlc': [1, 0, 'x']
      });
      bob.m.receive(
          const PeerRef('user:a', 'x'), {'t': 'interest', 'all': true});
      await flush();
      expect(view.last[0]['title'], 't');
    });

    test('follows the signed-in user', () async {
      final net = MemoryNet();
      final phone = Device('user:a', net.factory, held: [note('1')]);
      final laptop = Device('user:a', net.factory, held: [note('1')]);
      final view = laptop.show();
      await flush();
      laptop.m.setUser('user:b');
      phone.m.local('note', 'note:1', {'title': 'for a'}, null);
      await flush();
      expect(net.ops, isEmpty);
      expect(laptop.m.ownState.status, ChannelStatus.connecting);
      laptop.m.setUser(null);
      expect(laptop.m.ownState.status, ChannelStatus.idle);
      laptop.m.setUser('user:a');
      await flush();
      phone.m.local('note', 'note:1', {'title': 'for a'}, null);
      await flush();
      expect(view.last[0]['title'], 'for a');
    });

    test('reports how the own devices are doing to a subscriber', () async {
      final net = MemoryNet();
      final phone = Device('user:a', net.factory);
      final states = <ChannelStatus>[];
      final stop = phone.m.subscribeOwn((s) => states.add(s.status));
      final laptop = Device('user:a', net.factory);
      await flush();
      laptop.m.close();
      await flush();
      expect(states, [
        ChannelStatus.connecting,
        ChannelStatus.connected,
        ChannelStatus.connecting
      ]);
      stop();
      ChannelManager(schema).subscribeOwn((s) => states.add(s.status))();
      expect(states.last, ChannelStatus.idle);
    });

    test(
        'also reaches the own devices when a write goes through a channel to another user',
        () async {
      final net = MemoryNet();
      const callScope = ChannelScope('call', []);
      List<Row> held() => [
            {'id': 'call:1', 'view_id': ''}
          ];
      final phone = Device('user:a', net.factory, held: held());
      final laptop = Device('user:a', net.factory, held: held());
      final bob = Device('user:b', net.factory, config: null, held: held());
      final toBob = phone.m.open([callScope], ['user:b']);
      bob.m.open([callScope], ['user:a']);
      final mine = laptop.show();
      final theirs = bob.show();
      await flush();
      phone.m.local('call', 'call:1', {'view_id': 'v'}, null, toBob);
      await flush();
      expect(mine.last[0]['view_id'], 'v');
      expect(theirs.last[0]['view_id'], 'v');
    });
  });

  group('ChannelManager: sending only what the other device shows', () {
    List<Object?> sentIds(MemoryNet net) =>
        [for (final s in net.ops) s.message['id']];

    test(
        'skips a device that shows nothing of the table, and one whose filter the row is outside of',
        () async {
      final net = MemoryNet();
      final phone = Device('user:a', net.factory, held: [
        note('1'),
        note('2', {'folder': 'b'}),
        {'id': 'tag:1', 'note': 'note:1', 'label': 'l'}
      ]);
      final laptop = Device('user:a', net.factory, held: [
        note('1'),
        note('2', {'folder': 'b'})
      ]);
      await flush();
      // Nothing is mounted on the laptop: nothing is sent to it.
      phone.m.local('note', 'note:1', {'title': 'x'}, null);
      await flush();
      expect(net.ops, isEmpty);

      final view = laptop.show(inFolder('a'));
      await flush();
      phone.m.local('note', 'note:1', {'title': 'in a'}, null);
      phone.m.local('note', 'note:2', {'title': 'in b'}, null);
      phone.m.local('tag', 'tag:1', {'label': 'm'}, null);
      await flush();
      expect(sentIds(net), ['note:1']);
      expect(view.last[0]['title'], 'in a');

      // A row moving into the folder it shows, or out of it, is sent.
      phone.m.local('note', 'note:2', {'folder': 'a'}, null);
      phone.m.local('note', 'note:1', {'folder': 'z'}, null);
      await flush();
      expect(sentIds(net), ['note:1', 'note:2', 'note:1']);
      // So is a create inside it; one outside is not.
      phone.m.localCreate('note', 'note:3', {'folder': 'a', 'title': 'n'});
      phone.m.localCreate('note', 'note:4', {'folder': 'q', 'title': 'n'});
      await flush();
      expect(sentIds(net), ['note:1', 'note:2', 'note:1', 'note:3']);
      // And a delete of a row inside it, not of one outside.
      await phone.m.localDelete('note', 'note:2');
      await phone.m.localDelete('note', 'note:1');
      await flush();
      expect(sentIds(net), ['note:1', 'note:2', 'note:1', 'note:3', 'note:1']);

      // The query goes away: so does the claim.
      view.stop();
      await flush();
      phone.m.localCreate('note', 'note:5', {'folder': 'a', 'title': 'n'});
      await flush();
      expect(net.ops, hasLength(5));
    });

    test(
        'sends everything of a table read through .related(), and everything to a subscription nobody described',
        () async {
      final net = MemoryNet();
      List<Row> held() => [
            {'id': 'tag:1', 'note': 'note:1', 'label': 'l'},
            note('9', {'folder': 'zzz'})
          ];
      final phone = Device('user:a', net.factory, held: held());
      final laptop = Device('user:a', net.factory, held: held());
      final withTags = laptop.show(inFolder('a').related('tags'));
      await flush();
      phone.m.local('tag', 'tag:1', {'label': 'm'}, null);
      phone.m.local('note', 'note:9', {'title': 'far'}, null);
      await flush();
      expect(sentIds(net), ['tag:1']);
      withTags.stop();
      laptop.show();
      await flush();
      phone.m.local('note', 'note:9', {'title': 'far'}, null);
      await flush();
      expect(sentIds(net), ['tag:1', 'note:9']);
    });

    test('announces once per change, and tells a device that joins later',
        () async {
      final net = MemoryNet();
      final phone = Device('user:a', net.factory, held: [note('1')]);
      final laptop = Device('user:a', net.factory, held: [note('1')]);
      await flush();
      List<Map<String, dynamic>> interests() => [
            for (final s in net.sent)
              if (s.message['t'] == 'interest') s.message
          ];
      // Each told the other on meeting.
      expect(interests(), hasLength(2));
      final a = laptop.show(inFolder('a'));
      final b = laptop.show(inFolder('a'));
      laptop.show(inFolder('b'));
      await flush();
      expect(interests(), hasLength(3));
      expect(interests().last['v'], {
        'note': [
          [eq('folder', 'a')],
          [eq('folder', 'b')]
        ]
      });
      // One of two queries on the folder goes: the claim is the same, and not
      // said again.
      a.stop();
      a.stop();
      await flush();
      expect(interests(), hasLength(3));
      b.stop();
      await flush();
      expect(interests(), hasLength(4));
      expect(interests().last['v'], {
        'note': [
          [eq('folder', 'b')]
        ]
      });
      laptop.show(inFolder('a'));
      await flush();
      expect(interests(), hasLength(5));
      final tablet = Device('user:a', net.factory, held: [note('1')]);
      final view = tablet.show(inFolder('a'));
      await flush();
      phone.m.local('note', 'note:1', {'title': 'all three'}, null);
      await flush();
      expect(view.last[0]['title'], 'all three');
      expect({for (final s in net.ops) s.to}, hasLength(2));
      expect(net.ops, hasLength(2));
    });

    test('forgets what a device showed once it is gone', () async {
      final net = MemoryNet();
      final phone = Device('user:a', net.factory, held: [note('1')]);
      final laptop = Device('user:a', net.factory, held: [note('1')]);
      laptop.show(inFolder('nothing'));
      await flush();
      phone.m.local('note', 'note:1', {'title': 'x'}, null);
      await flush();
      expect(net.ops, isEmpty);
      laptop.m.close();
      final again = Device('user:a', net.factory, held: [note('1')]);
      again.show();
      await flush();
      phone.m.local('note', 'note:1', {'title': 'y'}, null);
      await flush();
      expect(net.ops, hasLength(1));
      // Garbage in place of an interest changes nothing.
      phone.m.receive(const PeerRef('user:a', 'x'), {'t': 'interest', 'v': 7});
      phone.m.local('note', 'note:1', {'title': 'z'}, null);
      await flush();
      expect(net.ops, hasLength(2));
    });

    test('reads what a browser tab says it shows against rows held as strings',
        () async {
      final net = MemoryNet();
      final phone = Device('user:a', net.factory, held: [
        note('1', {'author': 'user:a', 'at': '2026-03-01T00:00:00Z'}),
        note('2', {'author': 'user:b', 'at': '2026-03-01T00:00:00Z'}),
        note('3', {'author': 'user:a', 'at': '2025-01-01T00:00:00Z'}),
      ]);
      // A device the phone is connected to, speaking for itself as a browser
      // tab does: record ids and dates tagged.
      final laptop = Device('user:a', net.factory);
      await flush();
      phone.m.receive(
          PeerRef('user:a', laptop.m.peerId),
          jsonDecode(r'{"t":"interest","v":{"note":[['
              r'{"field":"author","op":"=","value":{"$rid":"user:a"}},'
              r'{"field":"at","op":">=","value":{"$date":"2026-01-01T00:00:00.000Z"}}]]}}'));
      for (final id in ['1', '2', '3']) {
        phone.m.local('note', 'note:$id', {'title': 'x'}, null);
      }
      await flush();
      expect(sentIds(net), ['note:1']);
    });
  });
}
