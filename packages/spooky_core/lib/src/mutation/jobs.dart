import 'dart:convert';

import '../types.dart';

/// Build the outbox job record for a backend route and resolve its table.
/// Every job is a single execution; recurring work is declared server-side.
({String tableName, Map<String, dynamic> record}) buildJobRecord(
  Map<String, dynamic> schema,
  String backend,
  String path,
  Map<String, dynamic> data, {
  RunOptions? options,
}) {
  final backends = schema['backends'];
  final backendDef = (backends is Map ? backends[backend] : null) as Map?;
  if (backendDef == null) throw ArgumentError('Backend $backend not found');
  final route = (backendDef['routes'] as Map?)?[path] as Map?;
  if (route == null) throw ArgumentError('Route $backend.$path not found');
  final tableName = backendDef['outboxTable'] as String?;
  if (tableName == null) {
    throw ArgumentError('Outbox table for backend $backend not found');
  }

  final args = (route['args'] as Map?) ?? const {};
  final payload = <String, dynamic>{};
  args.forEach((argName, argDef) {
    final optional = argDef is Map && argDef['optional'] == true;
    if (!data.containsKey(argName) && !optional) {
      throw ArgumentError('Missing required argument $argName');
    }
    payload[argName as String] = data[argName];
  });

  final record = <String, dynamic>{
    'path': path,
    'payload': jsonEncode(payload),
    // Explicit: the schema's server-side DEFAULT does not apply to an
    // optimistic local create, and in-flight indicators key on `pending`.
    'status': 'pending',
    'max_retries': options?.maxRetries ?? 3,
    'retry_strategy': options?.retryStrategy ?? 'linear',
  };
  if (options?.timeout != null) record['timeout'] = options!.timeout;
  if (options?.delay != null) record['delay'] = options!.delay;
  if (options?.assignedTo != null) record['assigned_to'] = options!.assignedTo;
  return (tableName: tableName, record: record);
}
