import 'package:spooky_core/advanced.dart';
import 'package:spooky_core/spooky_core.dart';
import 'package:spooky_core/src/kernel/events.dart' show PollTick, SyncOutcome;
import 'package:spooky_core/src/services/persistence/memory_persistence.dart';
import 'package:test/test.dart';

import 'fake_remote.dart';

/// Sync health as an app observes it through the client: a run of failed rounds
/// degrades, a clean one recovers, and a subscriber hears every transition once.
///
/// The fold itself is pinned in `sync/policy_test.dart` and the sagas that drive
/// it in `sync/sync_sagas_test.dart`; this is the seam between them and the app.
void main() {
  const schemaSurql =
      'DEFINE TABLE thread SCHEMAFULL PERMISSIONS FOR select WHERE true;';
  final schema = {
    'thread': {
      'columns': {'title': const ColumnSchema(type: 'string')},
    },
  };

  late FakeRemote remote;
  late InProcessSp00kyClient client;

  Future<InProcessSp00kyClient> open({int degradeAfter = 3}) async {
    final c = InProcessSp00kyClient(
      Sp00kyConfig(
        database: const DatabaseConfig(
            endpoint: 'ws://x', namespace: 't', database: 't'),
        schema: schema,
        schemaSurql: schemaSurql,
        persistenceClient: MemoryPersistenceClient(),
        syncHealth:
            SyncHealthConfig(degradeAfterConsecutiveFailures: degradeAfter),
        // Keep the poll off the critical path of these assertions.
        refSyncIntervalMs: 3600000,
      ),
      remoteClient: remote,
    );
    await c.init();
    await settle();
    return c;
  }

  setUp(() async {
    remote = FakeRemote();
    client = await open();
  });
  tearDown(() => client.close());

  Future<void> round(bool ok) =>
      client.dispatch(SyncOutcome(ok, ok ? null : StateError('socket closed')));

  test('degrades only after a run of failures, and recovers on one success',
      () async {
    await round(false);
    await round(false);
    expect(client.syncHealth.isDegraded, isFalse,
        reason: 'a single failure is absorbed by the retry');

    await round(false);
    expect(client.syncHealth.isDegraded, isTrue);
    expect(client.syncHealth.consecutiveFailures, 3);
    expect(client.syncHealth.kind, 'network');

    await round(true);
    expect(client.syncHealth.isDegraded, isFalse);
    expect(client.syncHealth.consecutiveFailures, 0);
  });

  test('everConnected latches on the first success and never resets', () async {
    // Boot already probed the server through the poll, so this client has
    // connected; the cold-start case is covered by the local-only client in
    // `client_lifecycle_test.dart`.
    await round(true);
    expect(client.syncHealth.everConnected, isTrue);
    for (var i = 0; i < 4; i++) {
      await round(false);
    }
    expect(client.syncHealth.isDegraded, isTrue);
    expect(client.syncHealth.everConnected, isTrue);
  });

  test('degradeAfterConsecutiveFailures 0 disables reporting', () async {
    await client.close();
    client = await open(degradeAfter: 0);
    for (var i = 0; i < 5; i++) {
      await round(false);
    }
    expect(client.syncHealth.isDegraded, isFalse);
    expect(client.syncHealth.consecutiveFailures, 0);
  });

  test('a subscriber fires immediately, then once per transition', () async {
    final seen = <SyncHealthStatus>[];
    final off = client.subscribeToSyncHealth((h) => seen.add(h.status));
    expect(seen, [SyncHealthStatus.healthy]);

    for (var i = 0; i < 3; i++) {
      await round(false);
    }
    expect(seen.last, SyncHealthStatus.degraded);
    final afterDegrade = seen.length;

    // More failures move `consecutiveFailures`, which IS a health change.
    await round(true);
    expect(seen.last, SyncHealthStatus.healthy);
    expect(seen.length, greaterThan(afterDegrade));

    off();
    await round(false);
    expect(seen.last, SyncHealthStatus.healthy,
        reason: 'an unsubscribed callback stays quiet');
  });

  test('an idle client still reports, because the poll probes', () async {
    // No queries at all: the poll falls back to a bare probe, which is the only
    // health signal a quiet client produces.
    remote.offline = true;
    await client.dispatch(const PollTick());
    await settle();
    expect(client.syncHealth.consecutiveFailures, 1);

    remote.offline = false;
    await client.dispatch(const PollTick());
    await settle();
    expect(client.syncHealth.consecutiveFailures, 0);
    expect(client.syncHealth.everConnected, isTrue);
  });
}
