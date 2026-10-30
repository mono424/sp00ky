import 'dart:convert';
import 'dart:typed_data';

import 'package:fake_async/fake_async.dart';
import 'package:spooky_core/src/modules/channel/channel.dart';
import 'package:spooky_core/src/modules/query_builder.dart';
import 'package:logging/logging.dart';
import 'package:spooky_core/src/services/logger/logger.dart';
import 'package:spooky_core/src/surreal/value.dart';
import 'package:spooky_core/src/utils/parser.dart' show ColumnSchema;
import 'package:test/test.dart';

import '../channel_net.dart';

const schema = <String, dynamic>{
  'call': {
    'columns': <String, ColumnSchema>{
      'caller': ColumnSchema(type: 'record', recordId: true),
      'callee': ColumnSchema(type: 'record', recordId: true),
      'view_id': ColumnSchema(type: 'string', optional: true),
      'view_pos': ColumnSchema(type: 'string', optional: true, ephemeral: true),
      'count': ColumnSchema(type: 'float', optional: true),
    },
    'channel': true,
  },
  'msg': {
    'columns': <String, ColumnSchema>{
      'convo': ColumnSchema(type: 'string'),
      'text': ColumnSchema(type: 'string'),
      'mood': ColumnSchema(type: 'string', optional: true, ephemeral: true),
    },
    'channel': true,
  },
  'note': {
    'columns': <String, ColumnSchema>{
      'title': ColumnSchema(type: 'string'),
    },
  },
};

Map<String, ColumnSchema> cols(String table) =>
    (schema[table] as Map)['columns'] as Map<String, ColumnSchema>;

OverlayEntry entry(Object? value, {bool ephemeral = true}) => OverlayEntry(
    value: value,
    hlc: const Hlc(1, 0, 'p'),
    from: 'x',
    ephemeral: ephemeral,
    expiresAt: null);

QueryBuilder q(String table) => QueryBuilder(table, schema: schema);

/// The runtime's peer sagas, as a map: refuses an id it already holds.
class FakeRows {
  final store = <String, Row>{};
  final calls = <String>[];

  late final rows = ChannelRows(
    create: (_, id, row, ttl) async {
      calls.add('create $id $ttl');
      if (store.containsKey(id)) return false;
      store[id] = {'id': id, ...row};
      return true;
    },
    delete: (_, id, ttl) async {
      calls.add('delete $id $ttl');
      return store.remove(id) != null;
    },
    read: (id) async => store[id],
  );
}

final allCalls = q('call');
QueryBuilder convo(String c) => q('msg').where({'convo': c});

/// One device: a manager, an open channel and one subscription to watch.
class Device {
  Device(
    String user,
    MemoryNet net, {
    ChannelsConfig? config,
    int Function()? now,
    List<QueryBuilder>? scopes,
    List<Object?> peers = const ['user:a', 'user:b'],
  }) {
    m = ChannelManager(schema,
        transport: net.factory, rows: fr.rows, config: config, now: now);
    m.setUser(user);
    ch = m.open([
      for (final s in scopes ?? [allCalls, convo('c1')]) m.scope(s)
    ], peers);
    tap = m.tap(seen.add);
  }

  final fr = FakeRows();
  late final ChannelManager m;
  late final ChannelHandle ch;
  late final ({void Function(List<Row>) feed, void Function() stop}) tap;
  final seen = <List<Row>>[];

  Row? get last => seen.isEmpty || seen.last.isEmpty ? null : seen.last.first;
}

Row callRow([Row extra = const {}]) => {
      'id': 'call:1',
      'caller': 'user:a',
      'callee': 'user:b',
      'view_id': '',
      'view_pos': null,
      ...extra,
    };

Row msg([Row extra = const {}]) =>
    {'id': 'msg:1', 'convo': 'c1', 'text': 'hi', ...extra};

List<int> hlcNow() => [DateTime.now().millisecondsSinceEpoch, 0];

Map<String, dynamic> op(Map<String, dynamic> m) => {
      't': 'op',
      'hlc': [...hlcNow(), 'x'],
      ...m
    };

Future<void> flush() => pumpEventQueue();

void main() {
  group('pure helpers', () {
    test('leaves rows untouched when the overlay is empty or misses them', () {
      final rows = [
        <String, dynamic>{'id': 'call:1', 'view_pos': null}
      ];
      expect(identical(applyOverlay(rows, {}), rows), isTrue);
      expect(
          identical(
              applyOverlay(rows, {
                'call:2': {'view_pos': entry('3')}
              }),
              rows),
          isTrue);
    });

    test('folds overlay values into matching rows, nested ones included', () {
      final child = <String, dynamic>{'id': 'call:c', 'view_pos': null};
      final rows = [
        <String, dynamic>{
          'id': const RecordId('note', 'n'),
          'title': 't',
          'calls': [child]
        },
        <String, dynamic>{'id': 'call:1', 'view_pos': null},
      ];
      final out = applyOverlay(rows, {
        'call:c': {'view_pos': entry('7')},
        'call:1': {'view_pos': entry('9')},
      });
      expect(((out[0]['calls'] as List)[0] as Map)['view_pos'], '7');
      expect(out[1]['view_pos'], '9');
      expect(child['view_pos'], isNull, reason: 'the input is never mutated');
    });

    test('round-trips record ids, dates and bytes into the row shape', () {
      final value = {
        'at': DateTime.utc(2026, 10, 26, 10),
        'who': const RecordId('user', 'a'),
        'blob': Uint8List.fromList([1, 2, 3]),
        'n': [1, 'x'],
      };
      final wire = jsonEncode(encodeValue(value));
      expect(wire, contains(r'"$date":"2026-10-26T10:00:00.000Z"'));
      expect(wire, contains(r'"$rid":"user:a"'));
      final back = decodeValue(jsonDecode(wire)) as Map;
      expect(back['at'], '2026-10-26T10:00:00.000Z');
      expect(back['who'], 'user:a');
      expect(back['blob'], [1, 2, 3]);
      expect(back['n'], [1, 'x']);
    });

    test('reads what a browser peer encodes', () {
      // `JSON.stringify(encodeValue(...))` from packages/core.
      final back = decodeValue(jsonDecode(
              r'{"at":{"$date":"2026-10-26T10:00:00.000Z"},"who":{"$rid":"user:a"},"blob":{"$bytes":"AQID"}}'))
          as Map;
      expect(back, {
        'at': '2026-10-26T10:00:00.000Z',
        'who': 'user:a',
        'blob': [1, 2, 3],
      });
    });

    test('checks patches against the columns', () {
      final c = cols('call');
      expect(checkPatch(c, {'view_pos': '3'}), isNull);
      expect(checkPatch(c, {'view_pos': null}), isNull);
      expect(checkPatch(c, {'count': 2}), isNull);
      expect(checkPatch(c, {'caller': 'user:x'}), isNull);
      expect(checkPatch(c, {'nope': 1}), contains('unknown field'));
      expect(checkPatch(c, {'id': 'call:2'}), contains('id'));
      expect(checkPatch(c, {'count': 'x'}), contains('type'));
      expect(checkPatch(c, {'caller': null}), contains('type'));
      expect(checkPatch(c, {}), contains('empty'));
      expect(checkPatch(c, {}, allowEmpty: true), isNull);
    });

    test('fits array, int and option types', () {
      expect(
          fitsColumn(const ColumnSchema(type: 'array<string>'), ['a']), isTrue);
      expect(
          fitsColumn(const ColumnSchema(type: 'array<string>'), [1]), isFalse);
      expect(fitsColumn(const ColumnSchema(type: 'array<int, 3>'), [1, 2]),
          isTrue);
      expect(fitsColumn(const ColumnSchema(type: 'int'), 2.0), isTrue);
      expect(fitsColumn(const ColumnSchema(type: 'int'), 2.5), isFalse);
      expect(
          fitsColumn(const ColumnSchema(type: 'array<option<string>>'), [null]),
          isTrue);
      expect(fitsColumn(const ColumnSchema(type: 'bool'), 'true'), isFalse);
    });

    test('splits ephemeral fields off a write', () {
      final c = cols('call');
      final s1 = splitPatch(c, {'view_id': 'a', 'view_pos': '1'});
      expect(s1.persistent, {'view_id': 'a'});
      expect(s1.ephemeral, {'view_pos': '1'});
      final s2 = splitPatch(c, {'view_id': 'a'});
      expect(s2.ephemeral, isNull);
    });

    test('normalizes peers and skips self', () {
      expect(
          peerUsers([const RecordId('user', 'a'), 'user:b', null, 'not-an-id'],
              'user:a'),
          {'user:b'});
    });

    test('parses the message a browser peer sends', () {
      final m = parseMessage(jsonDecode(
          '{"t":"op","table":"call","id":"call:1","patch":{"view_pos":"1"},"hlc":[1730000000000,2,"ab"]}'))!;
      expect(m.kind, ChannelOpKind.update);
      expect(m.hlc, const Hlc(1730000000000, 2, 'ab'));
      expect(parseMessage({'t': 'op', 'kind': 'nuke'}), isNull);
      expect(
          parseMessage({
            't': 'op',
            'table': 'x',
            'id': 'x:1',
            'patch': [],
            'hlc': [1, 0, 'a']
          }),
          isNull);
      expect(
          ChannelMessage(
                  table: 'call',
                  id: 'call:1',
                  patch: const {},
                  hlc: const Hlc(1, 0, 'p'))
              .toJson()
              .containsKey('kind'),
          isFalse,
          reason: 'an update leaves `kind` out, as the browser does');
    });
  });

  group('channel filters', () {
    ChannelCondition eq(String field, Object? value) =>
        ChannelCondition(field, '=', value);

    test('matches equality across record id spellings, dates and nested paths',
        () {
      expect(
          matchesWhere([eq('caller', const RecordId('user', 'a'))],
              {'caller': 'user:a'}),
          isTrue);
      expect(
          matchesWhere([eq('caller', const RecordId('user', 'a'))],
              {'caller': const RecordId('user', 'b')}),
          isFalse);
      expect(
          matchesWhere([eq('at', DateTime.utc(2026, 1, 1))],
              {'at': '2026-01-01T00:00:00.000Z'}),
          isTrue,
          reason: 'rows hold dates as ISO strings');
      expect(
          matchesWhere([
            eq('meta.kind', 'x')
          ], {
            'meta': {'kind': 'x'}
          }),
          isTrue);
      expect(matchesWhere([eq('meta.kind', 'x')], {'meta': null}), isFalse);
      expect(
          matchesWhere([
            eq('tags', ['a'])
          ], {
            'tags': ['a']
          }),
          isTrue);
      expect(matchesWhere([eq('missing', null)], {}), isTrue);
      expect(matchesWhere([], {}), isTrue);
    });

    test('supports comparisons, membership and swapped operands', () {
      final row = {
        'n': 5,
        's': 'b',
        'tags': ['x', 'y'],
        'who': 'user:a'
      };
      expect(
          matchesWhere([
            const ChannelCondition('n', '>', 4),
            const ChannelCondition('n', '<=', 5)
          ], row),
          isTrue);
      expect(matchesWhere([const ChannelCondition('n', '<', 5)], row), isFalse);
      expect(
          matchesWhere([const ChannelCondition('n', '>=', 'a')], row), isFalse);
      expect(matchesWhere([const ChannelCondition('n', '!=', 4)], row), isTrue);
      expect(
          matchesWhere([
            const ChannelCondition('s', 'IN', ['a', 'b'])
          ], row),
          isTrue);
      expect(
          matchesWhere([
            const ChannelCondition('s', 'NOT IN', ['a', 'b'])
          ], row),
          isFalse);
      expect(
          matchesWhere([const ChannelCondition('tags', 'CONTAINS', 'y')], row),
          isTrue);
      expect(
          matchesWhere(
              [const ChannelCondition('tags', 'CONTAINSNOT', 'y')], row),
          isFalse);
      expect(
          matchesWhere([
            const ChannelCondition('who', '∈', [RecordId('user', 'a')])
          ], row),
          isTrue);
      expect(
          matchesWhere([
            const ChannelCondition('s', '∈', ['b'], swap: true)
          ], {
            's': ['b']
          }),
          isFalse);
    });

    test('names the parts of a query a channel ignores', () {
      expect(unusedArgs(q('msg')), isEmpty);
      expect(
          unusedArgs(q('msg')
            ..orderBy('text')
            ..limit(5)
            ..offset(2)
            ..select(['text'])),
          ['orderBy', 'limit', 'offset', 'select']);
      expect(unusedArgs(q('msg')..one()), ['.one()']);
    });

    test('warns once per query shape about ignored parts, never when told', () {
      final logs = <String>[];
      final sub = Logger('channel-test').onRecord.listen((r) {
        logs.add(r.message);
      });
      addTearDown(sub.cancel);
      final m =
          ChannelManager(schema, logger: SpookyLogger.root('channel-test'));
      QueryBuilder plan() => q('msg')
        ..limit(50)
        ..orderBy('text');
      m.scope(plan());
      m.scope(plan());
      expect(logs, hasLength(1));
      expect(logs.single,
          matches(RegExp(r'orderBy, limit.*\.ignoreUnusedArgs\(\)')));
      m.scope(q('msg')..one());
      expect(logs, hasLength(2));
      m.scope(q('msg')
        ..offset(3)
        ..ignoreUnusedArgs());
      m.scope(q('msg'));
      expect(logs, hasLength(2));
    });

    test(
        'compiles a query into a scope, refusing what the row alone cannot decide',
        () {
      final scope = compileScope(schema, q('msg').where({'convo': 'c1'}));
      expect(scope.table, 'msg');
      expect(scope.where.single.op, '=');
      expect(scope.where.single.value, 'c1');
      expect(compileScope(schema, q('call')).where, isEmpty);
      expect(
          compileScope(
                  schema, q('msg').where({'text': const QueryOp('!=', '')}))
              .where
              .single
              .op,
          '!=');
      expect(
          () => compileScope(schema, q('note')),
          throwsA(isA<ArgumentError>()
              .having((e) => e.message, 'message', contains('@channel'))));
      expect(
          () => compileScope(
              schema, q('msg').where({'text': const QueryOp('@@', 'x')})),
          throwsA(isA<ArgumentError>()
              .having((e) => e.message, 'message', contains('operator'))));
      expect(
          () => compileScope(schema,
              q('msg').where({'convo': const QueryOp('=', r'$auth.id')})),
          throwsA(isA<ArgumentError>()
              .having((e) => e.message, 'message', contains('parameter'))));
      expect(
          () => compileScope(schema, q('call').where({'caller.name': 'x'})),
          throwsA(isA<ArgumentError>()
              .having((e) => e.message, 'message', contains('record link'))));
    });

    test('the query builder refuses an ephemeral field in where and orderBy',
        () {
      expect(() => q('call').where({'view_pos': '1'}), throwsArgumentError);
      expect(() => q('call').orderBy('view_pos'), throwsArgumentError);
    });
  });

  group('ChannelManager: opening', () {
    test('connects the two parties and reports it', () {
      final net = MemoryNet();
      final a = Device('user:a', net);
      final states = <ChannelStatus>[];
      a.ch.subscribe((s) => states.add(s.status));
      expect(a.ch.state.status, ChannelStatus.connecting);
      final b = Device('user:b', net);
      expect(
          a.ch.state, const ChannelState(ChannelStatus.connected, ['user:b']));
      expect(b.ch.state.peers, ['user:a']);
      expect(states, [ChannelStatus.connecting, ChannelStatus.connected]);
      expect(a.ch.tables, ['call', 'msg']);
    });

    test('needs at least one query', () {
      final m = ChannelManager(schema);
      expect(() => m.open([], []), throwsArgumentError);
      final ch = m.open([m.scope(allCalls)], []);
      expect(() => ch.setScopes([]), throwsArgumentError);
      ch.setScopes([m.scope(convo('c2'))]);
      expect(ch.tables, ['msg']);
    });

    test('is idle with nobody but yourself, and skips you once you sign in',
        () {
      final net = MemoryNet();
      final m = ChannelManager(schema, transport: net.factory);
      final ch = m.open([m.scope(allCalls)], ['user:a', 'user:b']);
      expect(ch.state.status, ChannelStatus.connecting);
      m.setUser('user:a');
      expect(net.nodes, hasLength(1));
      ch.setPeers([const RecordId('user', 'a')]);
      expect(ch.state.status, ChannelStatus.idle);
    });

    test('is unsupported without a peer connection factory', () {
      final m = ChannelManager(schema, transport: (_) => _Unsupported());
      m.setUser('user:a');
      final ch = m.open([m.scope(allCalls)], ['user:b']);
      expect(ch.state.status, ChannelStatus.unsupported);
    });

    test('setPeers and close change who it wants', () {
      final net = MemoryNet();
      final a = Device('user:a', net);
      final b = Device('user:b', net);
      a.ch.setPeers(['user:c']);
      expect(b.ch.state.status, ChannelStatus.connecting);
      a.ch.setPeers(['user:b']);
      expect(b.ch.state.status, ChannelStatus.connected);
      a.ch.close();
      a.ch.close();
      a.ch.setPeers(['user:b']);
      a.ch.setScopes([]);
      expect(b.ch.state.status, ChannelStatus.connecting);
      expect(() => a.m.assertCarries(a.ch, 'call'), throwsStateError);
    });

    test('a sign-out drops the transport and the peer values', () async {
      final net = MemoryNet();
      final a = Device('user:a', net);
      final b = Device('user:b', net);
      b.tap.feed([callRow()]);
      a.m.local('call', 'call:1', {}, {'view_pos': '4'});
      await flush();
      expect(b.last?['view_pos'], '4');
      b.m.setUser(null);
      expect(b.last?['view_pos'], isNull);
      expect(a.ch.state.status, ChannelStatus.connecting);
    });
  });

  group('ChannelManager: updates', () {
    test(
        'sends ephemeral values to the peer and shows them locally, never as a write',
        () async {
      final net = MemoryNet();
      final a = Device('user:a', net);
      final b = Device('user:b', net);
      a.tap.feed([callRow()]);
      b.tap.feed([callRow()]);
      final split = a.m.split('call', {'view_pos': '12'});
      expect(split.persistent, isEmpty);
      a.m.local('call', 'call:1', split.persistent, split.ephemeral);
      expect(a.last?['view_pos'], '12');
      await flush();
      expect(b.last?['view_pos'], '12');
    });

    test('shows a persistent peer value until the synced row catches up',
        () async {
      final net = MemoryNet();
      final a = Device('user:a', net);
      final b = Device('user:b', net);
      b.tap.feed([callRow()]);
      a.m.local('call', 'call:1', {'view_id': 'game:x'}, null);
      await flush();
      expect(b.last?['view_id'], 'game:x');
      b.tap.feed([
        callRow({'view_id': 'game:x'})
      ]);
      b.tap.feed([
        callRow({'view_id': 'game:y'})
      ]);
      expect(b.last?['view_id'], 'game:y');
    });

    test('drops an unconfirmed persistent peer value after the timeout', () {
      fakeAsync((async) {
        var now = 1000;
        final net = MemoryNet();
        final a = Device('user:a', net, now: () => now);
        final b = Device('user:b', net,
            now: () => now,
            config: const ChannelsConfig(confirmTimeoutMs: 5000));
        b.tap.feed([callRow()]);
        a.m.local('call', 'call:1', {'view_id': 'forged'}, null);
        async.flushMicrotasks();
        expect(b.last?['view_id'], 'forged');
        now += 6000;
        async.elapse(const Duration(seconds: 1));
        expect(b.last?['view_id'], '');
        b.m.close();
        a.m.close();
      });
    });

    test(
        'ignores an unlisted user, a non-channel table, a row outside the filter and a row it does not hold',
        () async {
      final net = MemoryNet();
      final b = Device('user:b', net);
      b.tap.feed([
        callRow(),
        msg({'id': 'msg:2', 'convo': 'c2'})
      ]);
      const from = PeerRef('user:a', 'x');
      b.m.receive(
          const PeerRef('user:c', 'x'),
          op({
            'table': 'call',
            'id': 'call:1',
            'patch': {'view_pos': '1'}
          }));
      b.m.receive(
          from,
          op({
            'table': 'note',
            'id': 'note:1',
            'patch': {'title': 'x'}
          }));
      b.m.receive(
          from,
          op({
            'table': 'msg',
            'id': 'msg:2',
            'patch': {'text': 'x'}
          }));
      b.m.receive(
          from,
          op({
            'table': 'msg',
            'id': 'msg:9',
            'patch': {'text': 'x'}
          }));
      b.m.receive(
          from,
          op({
            'table': 'call',
            'id': 'note:1',
            'patch': {'view_pos': '1'}
          }));
      b.m.receive(
          from,
          op({
            'table': 'call',
            'id': 'call:1',
            'patch': {'view_pos': 1}
          }));
      await flush();
      expect(b.seen, hasLength(1));
    });

    test('takes a row moving out of the filter (it matched before the patch)',
        () async {
      final net = MemoryNet();
      final b = Device('user:b', net);
      b.fr.store['msg:1'] = msg();
      b.tap.feed([msg()]);
      b.m.receive(
          const PeerRef('user:a', 'x'),
          op({
            'table': 'msg',
            'id': 'msg:1',
            'patch': {'convo': 'c2'}
          }));
      await flush();
      expect(b.last?['convo'], 'c2');
    });

    test('applies the app accept rule', () async {
      final net = MemoryNet();
      final a = Device('user:a', net);
      final b = Device('user:b', net,
          config: ChannelsConfig(accept: {
            'call': (op) => op.from == op.row?['callee'],
          }));
      b.tap.feed([callRow()]);
      a.m.local('call', 'call:1', {}, {'view_pos': '5'});
      await flush();
      expect(b.last?['view_pos'], isNull);
    });

    test('a throwing accept rule refuses', () async {
      final net = MemoryNet();
      final a = Device('user:a', net);
      final b = Device('user:b', net,
          config:
              ChannelsConfig(accept: {'call': (_) => throw StateError('x')}));
      b.tap.feed([callRow()]);
      a.m.local('call', 'call:1', {}, {'view_pos': '5'});
      await flush();
      expect(b.last?['view_pos'], isNull);
    });

    test('keeps the newest write when two arrive out of order', () async {
      final net = MemoryNet();
      final b = Device('user:b', net);
      b.tap.feed([callRow()]);
      const from = PeerRef('user:a', 'x');
      b.m.receive(from, {
        't': 'op',
        'table': 'call',
        'id': 'call:1',
        'patch': {'view_pos': 'new'},
        'hlc': [2000, 0, 'x']
      });
      await flush();
      b.m.receive(from, {
        't': 'op',
        'table': 'call',
        'id': 'call:1',
        'patch': {'view_pos': 'old'},
        'hlc': [1000, 0, 'x']
      });
      await flush();
      expect(b.last?['view_pos'], 'new');
    });

    test(
        'sends its ephemeral state to a peer that connects later, and forgets a peer that leaves',
        () async {
      final net = MemoryNet();
      final a = Device('user:a', net);
      a.fr.store['call:1'] = callRow();
      a.m.local('call', 'call:1', {}, {'view_pos': '3'});
      a.m.local('msg', 'msg:7', {}, {'mood': 'x'});
      final b = Device('user:b', net);
      b.tap.feed([callRow()]);
      await flush();
      expect(b.last?['view_pos'], '3');
      a.m.close();
      expect(b.last?['view_pos'], isNull);
    });

    test('passes writes on non-channel tables through untouched', () {
      final m = ChannelManager(schema);
      final s = m.split('note', {'title': 'x'});
      expect(s.persistent, {'title': 'x'});
      expect(s.ephemeral, isNull);
      m.local('note', 'note:1', {'title': 'x'}, null);
      m.localCreate('note', 'note:1', {});
    });

    test('an own persistent write supersedes a peer value for the same field',
        () async {
      final net = MemoryNet();
      final a = Device('user:a', net);
      final b = Device('user:b', net);
      b.tap.feed([callRow()]);
      a.m.local('call', 'call:1', {'view_id': 'game:a'}, null);
      await flush();
      expect(b.last?['view_id'], 'game:a');
      b.m.local('call', 'call:1', {'view_id': 'game:b'}, null);
      b.tap.feed([
        callRow({'view_id': 'game:b'})
      ]);
      expect(b.last?['view_id'], 'game:b');
    });

    test('a record link goes out as a record id and lands as a string',
        () async {
      final net = MemoryNet();
      final a = Device('user:a', net);
      final b = Device('user:b', net);
      b.tap.feed([callRow()]);
      a.m.local('call', 'call:1', {'caller': 'user:z'}, null);
      await flush();
      expect(net.wire.single, contains(r'"caller":{"$rid":"user:z"}'),
          reason: 'a browser peer reads a RecordId, not a string');
      expect(b.last?['caller'], 'user:z',
          reason: 'Dart rows hold record links as strings');
    });
  });

  group('ChannelManager: routing by filter', () {
    ({Device a, Device b, Device c, ChannelHandle toC}) threeWay() {
      final net = MemoryNet();
      final a = Device('user:a', net, scopes: [convo('c1')], peers: ['user:b']);
      final toC = a.m.open([a.m.scope(convo('c2'))], ['user:c']);
      final b = Device('user:b', net, scopes: [convo('c1')], peers: ['user:a']);
      final c = Device('user:c', net, scopes: [convo('c2')], peers: ['user:a']);
      return (a: a, b: b, c: c, toC: toC);
    }

    test(
        "sends a message only to the participants of its conversation's channel",
        () async {
      final t = threeWay();
      t.a.m.localCreate('msg', 'msg:1',
          {'convo': 'c1', 'text': 'hi', '_00_rv': 1, 'junk': 2});
      await flush();
      expect(t.b.fr.calls, ['create msg:1 5000']);
      expect(
          t.b.fr.store['msg:1'], {'id': 'msg:1', 'convo': 'c1', 'text': 'hi'});
      expect(t.c.fr.calls, isEmpty);
    });

    test(
        'routes updates and deletes by the stored row; a row changing conversation reaches both',
        () async {
      final t = threeWay();
      t.a.fr.store['msg:1'] = msg();
      for (final d in [t.b, t.c]) {
        d.fr.store['msg:1'] = msg();
        d.tap.feed([msg()]);
      }
      t.a.m.local('msg', 'msg:1', {'text': 'edited'}, null);
      await flush();
      expect(t.b.last?['text'], 'edited');
      expect(t.c.last?['text'], 'hi');
      await t.a.m.localDelete('msg', 'msg:1');
      await flush();
      expect(t.b.fr.calls, ['delete msg:1 5000']);
      expect(t.c.fr.calls, isEmpty);

      final moved = threeWay();
      moved.a.fr.store['msg:1'] = msg();
      for (final d in [moved.b, moved.c]) {
        d.fr.store['msg:1'] = msg();
        d.tap.feed([msg()]);
      }
      moved.a.m.local('msg', 'msg:1', {'convo': 'c2'}, null);
      await flush();
      expect(moved.b.last?['convo'], 'c2');
      expect(moved.c.last?['convo'], 'c2');
    });

    test(
        'a scoped write goes to that channel only, and only for rows its filter matches',
        () async {
      final t = threeWay();
      t.a.m.localCreate('msg', 'msg:1', {'convo': 'c1', 'text': 'hi'}, t.toC);
      t.a.m.localCreate('msg', 'msg:2', {'convo': 'c2', 'text': 'hi'}, t.toC);
      await flush();
      expect(t.b.fr.calls, isEmpty);
      expect(t.c.fr.calls, ['create msg:2 5000']);
      expect(
          () => t.a.m.assertCarries(t.toC, 'call'),
          throwsA(isA<ArgumentError>().having(
              (e) => e.message, 'message', contains("no query on 'call'"))));
      t.a.m.assertCarries(t.toC, 'msg');
    });

    test('an update of a row it does not hold is matched on the patch alone',
        () async {
      final t = threeWay();
      t.b.fr.store['msg:5'] = msg({'id': 'msg:5'});
      t.b.tap.feed([
        msg({'id': 'msg:5'})
      ]);
      t.a.m.local('msg', 'msg:5', {'convo': 'c1', 'text': 'x'}, null);
      await flush();
      expect(t.b.last?['text'], 'x');
      await t.a.m.localDelete('msg', 'msg:404');
    });
  });

  group('ChannelManager: creates and deletes', () {
    test(
        'carries ephemeral fields of a create to the peer overlay, never into its store',
        () async {
      final net = MemoryNet();
      final a = Device('user:a', net);
      final b = Device('user:b', net);
      a.tap.feed([msg()]);
      a.m.localCreate(
          'msg', 'msg:1', {'convo': 'c1', 'text': 'hi', 'mood': 'happy'});
      expect(a.last?['mood'], 'happy');
      await flush();
      expect(b.fr.store['msg:1'], {'id': 'msg:1', 'convo': 'c1', 'text': 'hi'});
      b.tap.feed([msg()]);
      expect(b.last?['mood'], 'happy');
    });

    test(
        'refuses a create from an unlisted user, outside the filter, against the accept rule, of the wrong shape or kind',
        () async {
      final net = MemoryNet();
      final b = Device('user:b', net,
          config: ChannelsConfig(accept: {
            'msg': (op) =>
                op.kind != ChannelOpKind.create || op.patch['text'] != 'spam'
          }));
      void send(String user, Map<String, dynamic> m) =>
          b.m.receive(PeerRef(user, 'x'), op(m));
      send('user:c', {
        'kind': 'create',
        'table': 'msg',
        'id': 'msg:1',
        'patch': {'convo': 'c1', 'text': 'hi'}
      });
      send('user:a', {
        'kind': 'create',
        'table': 'msg',
        'id': 'msg:1',
        'patch': {'convo': 'c9', 'text': 'hi'}
      });
      send('user:a', {
        'kind': 'create',
        'table': 'msg',
        'id': 'msg:1',
        'patch': {'convo': 'c1', 'text': 'spam'}
      });
      send('user:a', {
        'kind': 'create',
        'table': 'msg',
        'id': 'msg:1',
        'patch': {'convo': 1}
      });
      send('user:a',
          {'kind': 'nuke', 'table': 'msg', 'id': 'msg:1', 'patch': {}});
      await flush();
      expect(b.fr.calls, isEmpty);

      final noRows = ChannelManager(schema);
      noRows.setUser('user:b');
      noRows.open([noRows.scope(convo('c1'))], ['user:a']);
      noRows.receive(
          const PeerRef('user:a', 'x'),
          op({
            'kind': 'create',
            'table': 'msg',
            'id': 'msg:1',
            'patch': {'convo': 'c1'}
          }));
      noRows.receive(const PeerRef('user:a', 'x'),
          op({'kind': 'delete', 'table': 'msg', 'id': 'msg:1', 'patch': {}}));
      await flush();
    });

    test('sends an own delete; the peer hides it', () async {
      final net = MemoryNet();
      final a = Device('user:a', net);
      final b = Device('user:b', net);
      a.fr.store['msg:1'] = msg();
      b.fr.store['msg:1'] = msg();
      await a.m.localDelete('msg', 'msg:1');
      await flush();
      expect(b.fr.calls, ['delete msg:1 5000']);
    });

    test(
        'ignores a delete from an unlisted user, outside the filter, of a row it does not hold, or against the accept rule',
        () async {
      final net = MemoryNet();
      final b = Device('user:b', net,
          config: ChannelsConfig(accept: {'call': (_) => false}));
      b.fr.store['msg:2'] = msg({'id': 'msg:2', 'convo': 'c2'});
      b.fr.store['call:1'] = callRow();
      void del(String user, String table, String id) => b.m.receive(
          PeerRef(user, 'x'),
          op({'kind': 'delete', 'table': table, 'id': id, 'patch': {}}));
      del('user:c', 'msg', 'msg:1');
      del('user:a', 'msg', 'msg:2');
      del('user:a', 'msg', 'msg:404');
      del('user:a', 'call', 'call:1');
      await flush();
      expect(b.fr.calls, isEmpty);
    });
  });

  test('a write with no transport up never reads the row', () async {
    var reads = 0;
    final m = ChannelManager(schema,
        rows: ChannelRows(
          create: (_, __, ___, ____) async => true,
          delete: (_, __, ___) async => true,
          read: (_) async {
            reads++;
            return null;
          },
        ));
    m.local('msg', 'msg:1', {'text': 'x'}, null);
    await m.localDelete('msg', 'msg:1');
    await flush();
    expect(reads, 0);
  });
}

class _Unsupported implements ChannelTransport {
  @override
  bool get supported => false;
  @override
  void want(Set<String> users) {}
  @override
  void send(PeerRef to, ChannelMessage message) {}
  @override
  void close() {}
}
