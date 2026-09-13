import 'dart:convert';
import 'dart:io';
import 'package:spooky_core/spooky_core.dart';
import 'package:spooky_core/src/services/database/local_database_service.dart';
import 'package:spooky_core/src/services/logger/logger.dart';
import 'package:spooky_core/src/services/persistence/session_persistence.dart';
import 'package:test/test.dart';

String jwt(String id) => 'h.${base64Url.encode(utf8.encode(jsonEncode({
          'ID': id,
          'AC': 'account'
        })))}.s';
void main() {
  late Directory dir;
  late DatabaseConfig config;
  final logger = SpookyLogger.root('test');
  LocalDatabaseService open(String bucket) {
    final db = LocalDatabaseService.open(logger,
        store: StoreType.indexeddb,
        path: SessionPersistence.bucketPath(config.localDbPath!, bucket));
    db.provision();
    return db;
  }

  setUp(() {
    dir = Directory.systemTemp.createTempSync('spooky-session');
    config = DatabaseConfig(
        namespace: 'n',
        database: 'd',
        store: StoreType.indexeddb,
        localDbPath: '${dir.path}/cache.db');
  });
  tearDown(() => dir.deleteSync(recursive: true));
  test('migrates the locator once and preserves legacy account documents',
      () async {
    final anon = open('anon');
    anon.kvSet(SessionPersistence.hintKey, jsonEncode('a'));
    final old = open('a');
    old.kvSet(SessionPersistence.tokenKey, jsonEncode(jwt('user:a')));
    old.putDoc('thread', 'thread:a', {'title': 'untouched'});
    old.close();
    var persistence = SessionPersistence.open(config, logger, () => anon);
    expect(await persistence.get<String>(SessionPersistence.hintKey), 'a');
    expect(await persistence.get<String>(SessionPersistence.tokenKey),
        jwt('user:a'));
    await persistence.remove(SessionPersistence.tokenKey);
    persistence.close();
    persistence = SessionPersistence.open(config, logger, () => anon);
    expect(await persistence.get<String>(SessionPersistence.tokenKey), isNull);
    expect(await persistence.get<String>(SessionPersistence.hintKey), 'anon');
    persistence.close();
    final stillThere = open('a');
    expect(stillThere.getById('thread:a')?['title'], 'untouched');
    stillThere.close();
    anon.close();
  });
  test('token claims choose the account even when the old locator disagrees',
      () async {
    final anon = open('anon');
    anon.kvSet(SessionPersistence.hintKey, jsonEncode('old'));
    final old = open('old');
    old.kvSet(SessionPersistence.tokenKey, jsonEncode(jwt('user:actual')));
    old.close();
    final persistence = SessionPersistence.open(config, logger, () => anon);
    expect(await persistence.get<String>(SessionPersistence.hintKey), 'actual');
    await persistence.set(SessionPersistence.tokenKey, jwt('user:next'));
    expect(await persistence.get<String>(SessionPersistence.hintKey), 'next');
    persistence.close();
    anon.close();
  });
  test('malformed token or unsafe locator cannot restore another account',
      () async {
    final anon = open('anon');
    anon.kvSet(SessionPersistence.hintKey, jsonEncode('../../outside'));
    anon.kvSet(SessionPersistence.tokenKey, jsonEncode('bad-token'));
    final persistence = SessionPersistence.open(config, logger, () => anon);
    expect(await persistence.get<String>(SessionPersistence.tokenKey), isNull);
    expect(await persistence.get<String>(SessionPersistence.hintKey), 'anon');
    persistence.close();
    anon.close();
  });
  test('lost metadata after sign-out cannot resurrect legacy credentials',
      () async {
    final anon = open('anon');
    anon.kvSet(SessionPersistence.hintKey, jsonEncode('alice'));
    final alice = open('alice');
    alice.kvSet(SessionPersistence.tokenKey, jsonEncode(jwt('user:alice')));
    alice.close();
    var persistence = SessionPersistence.open(config, logger, () => anon);
    await persistence.remove(SessionPersistence.tokenKey);
    persistence.close();
    File(SessionPersistence.bucketPath(config.localDbPath!, 'session'))
        .deleteSync();
    persistence = SessionPersistence.open(config, logger, () => anon);
    expect(await persistence.get<String>(SessionPersistence.tokenKey), isNull);
    expect(await persistence.get<String>(SessionPersistence.hintKey), 'anon');
    persistence.close();
    anon.close();
  });
}
