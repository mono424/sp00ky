import 'dart:async';

import 'package:spooky_core/src/kernel/effects.dart' show StatementResult;
import 'package:spooky_core/src/services/database/remote_database_service.dart';
import 'package:spooky_core/src/services/logger/logger.dart';
import 'package:spooky_core/src/types.dart';
import 'package:test/test.dart';

import '../fake_remote.dart';

/// Holds every registration until [release], the way a server building an index
/// for each of them does.
class _SlowRegistrations extends FakeRemote {
  final _gate = Completer<void>();
  void release() => _gate.complete();

  Future<void> _maybeHold(String sql) async {
    if (sql.contains('fn::query::register')) await _gate.future;
  }

  @override
  Future<List<dynamic>> query(String sql, [Map<String, dynamic>? vars]) async {
    await _maybeHold(sql);
    return super.query(sql, vars);
  }

  @override
  Future<List<StatementResult>> queryStatements(String sql,
      [Map<String, dynamic>? vars]) async {
    await _maybeHold(sql);
    return super.queryStatements(sql, vars);
  }
}

Future<bool> finishes(Future<void> f) async {
  var done = false;
  unawaited(f.whenComplete(() => done = true));
  await Future<void>.delayed(const Duration(milliseconds: 20));
  return done;
}

void main() {
  late _SlowRegistrations client;
  late RemoteDatabaseService remote;

  setUp(() {
    client = _SlowRegistrations()..defaultMembership = const [];
    remote = RemoteDatabaseService(
      const DatabaseConfig(namespace: 'm', database: 'm'),
      client,
      SpookyLogger.root('test'),
    );
    // Every read slot held by a registration, more of them queued behind.
    for (var i = 0; i < 12; i++) {
      unawaited(remote.queryStatements('fn::query::register(\$config)'));
    }
  });

  tearDown(() => client.release());

  test('a read in the shared pool waits behind held registrations', () async {
    expect(await finishes(remote.queryStatements('RETURN true')), isFalse);
  });

  test('a body fetch does not', () async {
    expect(await finishes(remote.bodyStatements('RETURN true')), isTrue);
  });

  test('a read a screen is waiting on does not', () async {
    expect(await finishes(remote.queryInteractive('RETURN true')), isTrue);
  });

  test('the socket probe does not', () async {
    expect(await finishes(remote.probeSocket()), isTrue);
  });
}
