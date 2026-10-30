import '../modules/query_builder.dart';
import '../utils/duration_utils.dart';
import 'fields.dart';

/// A typed view over the dynamic [QueryBuilder]: filters with [Condition]s and
/// orders by [TypedField]s, and yields typed models via [_fromJson].
///
/// Conditions are AND-joined, one per field (matching the dynamic builder's
/// `Map`); multiple conditions on the same field collapse to the last one.
class TypedQuery<M> {
  TypedQuery(this._builder, this._fromJson);

  final QueryBuilder _builder;
  final M Function(Map<String, dynamic> json) _fromJson;

  /// The dynamic query underneath, e.g. for `client.channel(...)`.
  QueryBuilder get builder => _builder;

  /// See [QueryBuilder.ignoreUnusedArgs].
  TypedQuery<M> ignoreUnusedArgs() {
    _builder.ignoreUnusedArgs();
    return this;
  }

  TypedQuery<M> where(List<Condition> conditions) {
    for (final c in conditions) {
      _builder
          .where({c.field: c.op == null ? c.value : QueryOp(c.op!, c.value)});
    }
    return this;
  }

  TypedQuery<M> orderBy(TypedField field, {bool desc = false}) {
    _builder.orderBy(field.name, desc ? 'DESC' : 'ASC');
    return this;
  }

  TypedQuery<M> limit(int count) {
    _builder.limit(count);
    return this;
  }

  TypedQuery<M> offset(int count) {
    _builder.offset(count);
    return this;
  }

  /// Register the query and return its hash.
  Future<String> run({QueryTimeToLive ttl = defaultTtl}) =>
      _builder.run(ttl: ttl);

  /// Register and watch as a typed stream of result lists.
  Stream<List<M>> watch({QueryTimeToLive ttl = defaultTtl}) {
    return Stream.fromFuture(_builder.stream(ttl: ttl))
        .asyncExpand((inner) => inner)
        .map((rows) => rows.map(_fromJson).toList());
  }

  /// Register and watch a single (first) result, or null when empty.
  Stream<M?> watchOne({QueryTimeToLive ttl = defaultTtl}) {
    _builder.one();
    return watch(ttl: ttl).map((list) => list.isEmpty ? null : list.first);
  }
}
