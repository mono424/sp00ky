import 'package:spooky_core/src/modules/channel/channel.dart';
import 'package:spooky_core/src/modules/query_builder.dart';
import 'package:spooky_core/src/utils/parser.dart' show ColumnSchema;
import 'package:test/test.dart';

/// What the sharing tests have in common (`channels.share`): a schema with
/// shared tables, and a device that holds rows and mounts queries. The Dart
/// twin of the helpers in `packages/core/src/modules/channel/share.test.ts`.

const schema = <String, dynamic>{
  'note': {
    'columns': <String, ColumnSchema>{
      'folder': ColumnSchema(type: 'string'),
      'title': ColumnSchema(type: 'string'),
      'draft': ColumnSchema(type: 'bool', optional: true),
      'author': ColumnSchema(type: 'record', recordId: true, optional: true),
      'at': ColumnSchema(type: 'datetime', dateTime: true, optional: true),
    },
  },
  'tag': {
    'columns': <String, ColumnSchema>{
      'note': ColumnSchema(type: 'record', recordId: true),
      'label': ColumnSchema(type: 'string'),
    },
  },
  'secret': {
    'columns': <String, ColumnSchema>{
      'text': ColumnSchema(type: 'string'),
    },
  },
  'call': {
    'columns': <String, ColumnSchema>{
      'view_id': ColumnSchema(type: 'string', optional: true),
    },
    'channel': true,
  },
  'relationships': [
    {'from': 'note', 'field': 'tags', 'to': 'tag', 'cardinality': 'many'},
    {'from': 'tag', 'field': 'secret', 'to': 'secret', 'cardinality': 'one'},
  ],
};

Map<String, ColumnSchema>? columnsOf(String table) => switch (schema[table]) {
      {'columns': final Map<String, ColumnSchema> columns} => columns,
      _ => null,
    };

QueryBuilder q(String table) => QueryBuilder(table, schema: schema);
QueryBuilder inFolder(String folder) => q('note').where({'folder': folder});

/// One comparison as it travels: what `{ field, op: '=', value }` is in TS.
Map<String, dynamic> eq(String field, Object? value) =>
    {'field': field, 'op': '=', 'value': value};

const share = ChannelsConfig(
    share: ChannelShareConfig(tables: {
  'note': ChannelShareRule.all,
  'tag': ChannelShareRule.all,
  'call': ChannelShareRule.all,
}));

/// What a mounted query got, and how to unmount it.
class View {
  View(this.seen, this.stop);
  final List<List<Row>> seen;
  final void Function() stop;
  List<Row> get last => seen.last;
}

/// One device: a manager signed in as a user, its local rows, and a way to
/// subscribe.
class Device {
  Device(String user, ChannelTransportFactory transport,
      {ChannelsConfig? config = share,
      List<Row> held = const [],
      int Function()? now}) {
    for (final row in held) {
      store['${row['id']}'] = row;
    }
    m = ChannelManager(
      schema,
      transport: transport,
      config: config,
      now: now,
      rows: ChannelRows(
        create: (_, id, row, ttl) async {
          calls.add('create $id');
          if (store.containsKey(id)) return false;
          store[id] = {'id': id, ...row};
          return true;
        },
        delete: (_, id, ttl) async {
          calls.add('delete $id');
          return store.remove(id) != null;
        },
        read: (id) async => store[id],
      ),
    );
    m.setUser(user);
  }

  final store = <String, Row>{};
  final calls = <String>[];
  late final ChannelManager m;

  /// Subscribe like a mounted query: its rows are what the store holds of its
  /// table. Without a query, a subscription nobody described.
  View show([QueryBuilder? query]) {
    final seen = <List<Row>>[];
    final tap = m.tap(seen.add, m.interestOf(query));
    tap.feed([
      for (final row in store.values)
        if (query == null || '${row['id']}'.startsWith('${query.table}:')) row
    ]);
    return View(seen, tap.stop);
  }
}

Row note(String id, [Row extra = const {}]) =>
    {'id': 'note:$id', 'folder': 'a', 'title': 't', ...extra};

Future<void> flush() => pumpEventQueue();
