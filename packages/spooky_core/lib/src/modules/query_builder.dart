import 'dart:convert';

import '../services/logger/logger.dart';
import '../utils/parser.dart' show ColumnSchema;
import '../utils/duration_utils.dart';
import 'relationships.dart';

/// A comparison operator condition for [QueryBuilder.where], mirroring the JS
/// `{ _op, _val, _swap }` shape (e.g. `QueryOp('>', 18)` -> `field > $field`).
class QueryOp {
  const QueryOp(this.op, this.value, {this.swap = false});
  final String op;
  final Object? value;
  final bool swap;
}

/// Registers a built query and returns its hash. [relations] carries the
/// `.related()` plan so the client can resolve joined rows from the local cache.
typedef QueryRegistrar = Future<String> Function(
  String surql,
  Map<String, dynamic> vars,
  QueryTimeToLive ttl,
  List<RelationPlan> relations,
);

/// Shapes one `.related()` branch: how to fetch and attach a relation's rows
/// (TS `RelationPlan`). Nested `.related()` calls populate [relations].
class RelationPlan {
  RelationPlan({
    required this.alias,
    required this.table,
    required this.cardinality,
    required this.foreignKeyField,
    this.select = const ['*'],
    this.where = const {},
    this.orderBy = const [],
    this.limit,
    this.relations = const [],
  });

  /// Result key the joined value is attached under (the `.related()` field).
  final String alias;

  /// Table the joined rows come from.
  final String table;

  /// `one` attaches a single row (or null); `many` attaches a list.
  final String cardinality;

  /// Correlation field: for `many`, the child field pointing at the parent id;
  /// for `one`, the parent field holding the child id.
  final String foreignKeyField;

  final List<String> select;
  final Map<String, Object?> where;
  final List<(String field, String dir)> orderBy;
  final int? limit;
  final List<RelationPlan> relations;

  bool get isOne => cardinality == 'one';
}

/// Subscribes to a registered query hash as a Stream.
typedef QuerySubscriber = Stream<List<Map<String, dynamic>>> Function(
    String hash);

/// Fluent SURQL query builder. Produces the same SURQL shape as the JS
/// `buildQueryFromOptions` for select/where/orderBy/limit/offset/one, including
/// `.related()` subquery projections.
class QueryBuilder {
  QueryBuilder(
    this._table, {
    QueryRegistrar? registrar,
    QuerySubscriber? subscriber,
    Map<String, dynamic> schema = const {},
    SpookyLogger? logger,
  })  : _registrar = registrar,
        _subscriber = subscriber,
        _relationships = relationshipsOf(schema),
        _schema = schema,
        _logger = logger;

  final String _table;
  final QueryRegistrar? _registrar;
  final QuerySubscriber? _subscriber;
  final Map<String, dynamic> _schema;
  final List<SchemaRelationship> _relationships;
  final SpookyLogger? _logger;

  List<String> _select = const ['*'];
  final Map<String, Object?> _where = {};
  final List<(String field, String dir)> _orderBy = [];
  final List<RelationPlan> _relations = [];
  int? _limit;
  int? _offset;
  bool _isOne = false;
  bool _ignoreUnusedArgs = false;

  /// The table the query reads.
  String get table => _table;

  /// The `where` conditions as given (AND-joined): a plain value means `=`, a
  /// [QueryOp] its own operator.
  Map<String, Object?> get conditions => Map.unmodifiable(_where);

  List<String> get selectFields => List.unmodifiable(_select);
  List<(String field, String dir)> get orderFields =>
      List.unmodifiable(_orderBy);
  int? get limitValue => _limit;
  int? get offsetValue => _offset;
  bool get isOne => _isOne;

  /// Whether [ignoreUnusedArgs] was called.
  bool get ignoresUnusedArgs => _ignoreUnusedArgs;

  QueryBuilder select(List<String> fields) {
    _select = fields.isEmpty ? const ['*'] : fields;
    return this;
  }

  /// Add equality / operator conditions (AND-joined). A value of [QueryOp]
  /// emits `field <op> $field`; any other value emits `field = $field`.
  QueryBuilder where(Map<String, Object?> conditions) {
    for (final field in conditions.keys) {
      _assertNotEphemeral(field, 'where');
    }
    _where.addAll(conditions);
    return this;
  }

  QueryBuilder orderBy(String field, [String direction = 'ASC']) {
    _assertNotEphemeral(field, 'orderBy');
    _orderBy.add((field, direction));
    return this;
  }

  /// An `-- @ephemeral` field only travels between channel peers; the database
  /// never holds it, so a query cannot filter or order on it.
  void _assertNotEphemeral(String field, String clause) {
    final entry = _schema[_table];
    final columns = entry is Map ? entry['columns'] : null;
    if (columns is! Map) return;
    final column = columns[field] ?? columns[field.split('.').first];
    if (column is ColumnSchema && column.ephemeral) {
      throw ArgumentError("Cannot use '$field' in $clause(): it is marked "
          "'-- @ephemeral' on $_table, so it only travels between peers and "
          'the database never holds it. Read the field from query results '
          'instead.');
    }
  }

  QueryBuilder limit(int count) {
    _limit = count;
    return this;
  }

  QueryBuilder offset(int count) {
    _offset = count;
    return this;
  }

  QueryBuilder one() {
    _isOne = true;
    return this;
  }

  /// Say that parts of this query are ignored where it is used, on purpose. A
  /// channel (`client.channel(query, peers)`) only reads the `where`, so
  /// `orderBy`, `limit`, `offset`, `select` and `.one()` do nothing there and
  /// draw a warning; this silences it while showing the reader the query
  /// carries arguments that do not apply (handy when one query object serves
  /// both a list and a channel). Does not change the query's hash.
  QueryBuilder ignoreUnusedArgs() {
    _ignoreUnusedArgs = true;
    return this;
  }

  /// Include a related table as a subquery projection (TS `related`).
  ///
  /// [field] is a relationship declared in the schema: a forward `record<x>`
  /// field (attached as a single row under the same name) or the derived reverse
  /// name (attached as a list). [modifier] shapes the joined rows — its
  /// `select` / `where` / `orderBy` / `limit` apply per parent; nested
  /// `.related()` inside it joins another level.
  ///
  /// An unknown relationship is SKIPPED with a warning rather than throwing: a
  /// table owned by a devOnly backend is absent from the generated schema on
  /// some deployments, and failing hard would take the whole query (and its
  /// other `.related()` siblings) down. Mirrors the server's "unpermitted
  /// subquery -> empty" degradation.
  QueryBuilder related(String field, [void Function(QueryBuilder)? modifier]) {
    if (_relations.any((r) => r.alias == field)) return this;

    final rel = findRelationship(_relationships, _table, field);
    if (rel == null) {
      _logger?.warn(
          ".related('$field') skipped — no such relationship on '$_table' in the client schema");
      return this;
    }

    final sub = QueryBuilder(rel.to, schema: _schema, logger: _logger);
    modifier?.call(sub);

    _relations.add(RelationPlan(
      alias: field,
      table: rel.to,
      cardinality: rel.cardinality,
      foreignKeyField: rel.foreignKeyField,
      select: sub._select,
      where: sub._where,
      orderBy: sub._orderBy,
      // A one-to-one join takes at most one row (TS adds an implicit LIMIT 1).
      limit: sub._limit ?? (rel.isMany ? null : 1),
      relations: sub._relations,
    ));
    return this;
  }

  /// The accumulated `.related()` plan, for the client to resolve joined rows
  /// from the local cache.
  List<RelationPlan> get relations => List.unmodifiable(_relations);

  /// Build the SURQL string and its bind vars.
  (String, Map<String, dynamic>) build() {
    final effectiveLimit = _isOne ? 1 : _limit;
    final fetchClauses = _relations.isEmpty
        ? ''
        : ', ${_relations.map(_buildSubquery).join(', ')}';
    final selectClause = _select.join(', ');
    var query = 'SELECT $selectClause$fetchClauses FROM $_table';

    final vars = <String, dynamic>{};
    if (_where.isNotEmpty) {
      final conditions = <String>[];
      _where.forEach((key, value) {
        if (value is QueryOp) {
          String rightSide;
          if (value.value is String &&
              (value.value as String).startsWith(r'$')) {
            rightSide = value.value as String;
          } else {
            vars[key] = value.value;
            rightSide = '\$$key';
          }
          conditions.add(value.swap
              ? '$rightSide ${value.op} $key'
              : '$key ${value.op} $rightSide');
        } else {
          vars[key] = value;
          conditions.add('$key = \$$key');
        }
      });
      query += ' WHERE ${conditions.join(' AND ')}';
    }

    if (_orderBy.isNotEmpty) {
      final clauses = _orderBy.map((o) => '${o.$1} ${o.$2}').join(', ');
      query += ' ORDER BY $clauses';
    }
    if (effectiveLimit != null) query += ' LIMIT $effectiveLimit';
    if (_offset != null) query += ' START $_offset';

    query += ';';
    return (query, vars);
  }

  /// Emit one `.related()` branch as a correlated subquery projection
  /// (TS `buildSubquery`).
  ///
  /// A `one` relation correlates on the parent's foreign key
  /// (`WHERE id=$parent.<fk>`) and takes the first row (`[0]`); a `many`
  /// relation correlates on the child's back-reference
  /// (`WHERE <fk>=$parent.id`). Sub-`where` values are INLINED as literals, not
  /// bound: a subquery runs per parent row, so it cannot carry its own params.
  String _buildSubquery(RelationPlan rel) {
    var select = rel.select.isEmpty ? '*' : rel.select.join(', ');
    if (rel.relations.isNotEmpty) {
      select += ', ${rel.relations.map(_buildSubquery).join(', ')}';
    }

    final where = rel.where.isEmpty
        ? ''
        : ' AND ${rel.where.entries.map((e) => '${e.key} = ${_literal(e.value)}').join(' AND ')}';
    final orderBy = rel.orderBy.isEmpty
        ? ''
        : ' ORDER BY ${rel.orderBy.map((o) => '${o.$1} ${o.$2}').join(', ')}';
    final limit = rel.limit == null ? '' : ' LIMIT ${rel.limit}';
    final correlation = rel.isOne
        ? 'WHERE id=\$parent.${rel.foreignKeyField}'
        : 'WHERE ${rel.foreignKeyField}=\$parent.id';

    final subquery =
        '(SELECT $select FROM ${rel.table} $correlation$where$orderBy$limit)';
    return '$subquery${rel.isOne ? '[0]' : ''} AS ${rel.alias}';
  }

  /// Render a sub-`where` value as a SURQL literal. A `table:id` string is a
  /// record id and must NOT be quoted, or the comparison never matches.
  String _literal(Object? value) {
    if (value is String && RegExp(r'^[A-Za-z_][A-Za-z0-9_]*:\S+$').hasMatch(value)) {
      return value;
    }
    return jsonEncode(value);
  }

  /// Register the query and return its hash (TS `.run()`).
  Future<String> run({QueryTimeToLive ttl = defaultTtl}) {
    final registrar = _registrar;
    if (registrar == null) {
      throw StateError('QueryBuilder has no registrar (build-only)');
    }
    final (sql, vars) = build();
    return registrar(sql, vars, ttl, relations);
  }

  /// Register the query and return a result Stream (consume with StreamBuilder).
  Future<Stream<List<Map<String, dynamic>>>> stream(
      {QueryTimeToLive ttl = defaultTtl}) async {
    final subscriber = _subscriber;
    if (subscriber == null) {
      throw StateError('QueryBuilder has no subscriber (build-only)');
    }
    final hash = await run(ttl: ttl);
    return subscriber(hash);
  }
}
