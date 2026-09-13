import '../types.dart';

/// The narrow seam a module needs from the client: register a shared query and
/// subscribe to its rows.
///
/// Keeps [FeatureFlagModule] and [AppReleaseModule] off the engine's internals -
/// they only ever want one live query each.
abstract interface class QueryHost {
  Future<String> registerQuery(
      String table, String surql, Map<String, dynamic> params, QueryTimeToLive ttl);

  void Function() subscribe(String hash, QueryUpdateCallback cb,
      {bool immediate});
}
