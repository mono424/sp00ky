import 'package:spooky_core/src/kernel/constants.dart';
import 'package:spooky_core/src/surreal/value.dart';
import 'package:spooky_core/src/sync/policy.dart';
import 'package:spooky_core/src/types.dart';
import 'package:test/test.dart';

void main() {
  group('list_ref poll batching', () {
    test('packs due queries oldest-first under the row budget', () {
      final chunks = planListRefPollChunks(
        const [
          ListRefPollCandidate(hash: 'a', rows: 600, lastPolledAt: 300),
          ListRefPollCandidate(hash: 'b', rows: 600, lastPolledAt: 100),
          ListRefPollCandidate(hash: 'c', rows: 0, lastPolledAt: 0),
          ListRefPollCandidate(hash: 'd', rows: 900, lastPolledAt: 200),
        ],
        now: 1000,
        rowBudget: 1400,
      );
      // c (never polled) first, then b, d, a by age; d does not fit next to b.
      expect(chunks, [
        ['c', 'b'],
        ['d'],
        ['a'],
      ]);
    });

    test('never splits below one query and always refreshes a lone huge view',
        () {
      expect(
        planListRefPollChunks(
          const [
            ListRefPollCandidate(hash: 'big', rows: 10000, lastPolledAt: 0)
          ],
          now: 100000,
          rowBudget: 100,
        ),
        [
          ['big']
        ],
      );
    });

    test('defers large views until their minimum age has passed', () {
      const large =
          ListRefPollCandidate(hash: 'large', rows: 5000, lastPolledAt: 990000);
      const small =
          ListRefPollCandidate(hash: 'small', rows: 10, lastPolledAt: 990000);
      expect(
        planListRefPollChunks([large, small],
            now: 1000000, largeViewRows: 1000, largeViewMinAgeMs: 15000),
        [
          ['small']
        ],
      );
      // Same age: hash order breaks the tie, and the large view fills its own
      // chunk.
      expect(
        planListRefPollChunks([large, small],
            now: 1006000, largeViewRows: 1000, largeViewMinAgeMs: 15000),
        [
          ['large'],
          ['small'],
        ],
      );
    });

    test('returns no chunks when nothing is due', () {
      expect(planListRefPollChunks(const [], now: 1), isEmpty);
    });
  });

  group('applyRecordVersionDiff', () {
    test('applies adds, updates and removals, sorted by record id', () {
      final out = applyRecordVersionDiff(
        [('t:b', 1), ('t:a', 1), ('t:gone', 1)],
        RecordVersionDiff(
          added: [(id: RecordId('t', 'c'), version: 1)],
          updated: [(id: RecordId('t', 'a'), version: 4)],
          removed: [RecordId('t', 'gone')],
        ),
      );
      expect(out, [('t:a', 4), ('t:b', 1), ('t:c', 1)]);
    });

    test('an empty diff returns the same set, sorted', () {
      expect(
        applyRecordVersionDiff(
          [('t:b', 1), ('t:a', 1)],
          RecordVersionDiff(added: [], updated: [], removed: []),
        ),
        [('t:a', 1), ('t:b', 1)],
      );
    });
  });

  group('nextHealth', () {
    HealthInput input({
      SyncHealthStatus status = SyncHealthStatus.healthy,
      int consecutiveFailures = 0,
      bool hasSyncedOnce = false,
      bool everConnected = false,
    }) =>
        HealthInput(
          health: SyncHealth(
            status: status,
            consecutiveFailures: consecutiveFailures,
            everConnected: everConnected,
          ),
          consecutiveFailures: consecutiveFailures,
          hasSyncedOnce: hasSyncedOnce,
        );

    test('degradeAfter <= 0 disables reporting', () {
      final out = nextHealth(input(), false, 'boom', 0);
      expect(out.health.status, SyncHealthStatus.healthy);
      expect(out.consecutiveFailures, 0);
      expect(out.degradedNow, isFalse);
    });

    test('a run of failures degrades, the next success recovers', () {
      var out = nextHealth(input(), false, Exception('socket closed'), 3);
      expect(out.consecutiveFailures, 1);
      expect(out.health.status, SyncHealthStatus.healthy);
      expect(out.health.kind, 'network');
      expect(out.degradedNow, isFalse);

      out = nextHealth(
          input(consecutiveFailures: 1), false, Exception('socket closed'), 3);
      expect(out.degradedNow, isFalse);

      out = nextHealth(
          input(consecutiveFailures: 2), false, Exception('socket closed'), 3);
      expect(out.consecutiveFailures, 3);
      expect(out.health.status, SyncHealthStatus.degraded);
      expect(out.degradedNow, isTrue);

      final recovered = nextHealth(
          input(status: SyncHealthStatus.degraded, consecutiveFailures: 3),
          true,
          null,
          3);
      expect(recovered.health.status, SyncHealthStatus.healthy);
      expect(recovered.consecutiveFailures, 0);
      expect(recovered.recoveredNow, isTrue);
      expect(recovered.hasSyncedOnce, isTrue);
      expect(recovered.health.everConnected, isTrue);
      expect(recovered.health.kind, isNull);
    });

    test('a clean success latches everConnected without churning health', () {
      final out = nextHealth(input(), true, null, 3);
      expect(out.hasSyncedOnce, isTrue);
      expect(out.health.everConnected, isTrue);
      expect(out.recoveredNow, isFalse);
    });

    test('the transport state survives a health fold', () {
      final out = nextHealth(
        HealthInput(
          health: const SyncHealth(
            status: SyncHealthStatus.healthy,
            consecutiveFailures: 0,
            everConnected: true,
            connection: ConnectionState.connected,
          ),
          consecutiveFailures: 2,
          hasSyncedOnce: true,
        ),
        false,
        'boom',
        3,
      );
      expect(out.health.connection, ConnectionState.connected);
      expect(out.health.status, SyncHealthStatus.degraded);
    });
  });

  group('selfHealDelayMs', () {
    test('doubles per attempt and caps', () {
      expect(selfHealDelayMs(0), selfHealBaseMs);
      expect(selfHealDelayMs(1), selfHealBaseMs * 2);
      expect(selfHealDelayMs(3), selfHealBaseMs * 8);
      expect(selfHealDelayMs(50), selfHealMaxMs);
    });
  });
}
