import 'package:spooky_core/src/state/lifecycle.dart';
import 'package:spooky_core/src/types.dart' show QueryStatus;
import 'package:test/test.dart';

QueryLifecycle at(
  QueryPhase phase, {
  RemotePhase remote = RemotePhase.unregistered,
  int fetchDepth = 0,
  bool notified = false,
}) =>
    QueryLifecycle(
      phase: phase,
      remote: remote,
      fetchDepth: fetchDepth,
      notified: notified,
    );

void main() {
  group('transition table (phase axis)', () {
    test('seed', () {
      expect(transition(at(QueryPhase.live), const SeedEvent(true)).phase,
          QueryPhase.cached);
      expect(transition(at(QueryPhase.live), const SeedEvent(false)).phase,
          QueryPhase.cold);
    });

    test('membership-applied moves every phase to live', () {
      for (final p in [QueryPhase.cold, QueryPhase.cached, QueryPhase.live]) {
        expect(transition(at(p), const MembershipAppliedEvent(true)).phase,
            QueryPhase.live);
        expect(transition(at(p), const MembershipAppliedEvent(false)).phase,
            QueryPhase.live);
      }
      expect(
          transition(at(QueryPhase.viewLost), const MembershipAppliedEvent(true))
              .phase,
          QueryPhase.live);
    });

    test('view-lost needs a present row to recover', () {
      expect(
        () => transition(
            at(QueryPhase.viewLost), const MembershipAppliedEvent(false)),
        throwsA(isA<LifecycleError>()),
      );
    });

    test('row-missing: cold stays cold, everything else goes view-lost', () {
      expect(transition(at(QueryPhase.cold), const RowMissingEvent()).phase,
          QueryPhase.cold);
      expect(transition(at(QueryPhase.cached), const RowMissingEvent()).phase,
          QueryPhase.viewLost);
      expect(transition(at(QueryPhase.live), const RowMissingEvent()).phase,
          QueryPhase.viewLost);
      expect(transition(at(QueryPhase.viewLost), const RowMissingEvent()).phase,
          QueryPhase.viewLost);
    });

    test('bucket-switch reseeds and clears fetch depth', () {
      final l = transition(
        at(QueryPhase.live, fetchDepth: 3, notified: true),
        const BucketSwitchEvent(true),
      );
      expect(l.phase, QueryPhase.cached);
      expect(l.remote, RemotePhase.unregistered);
      expect(l.fetchDepth, 0);
      expect(l.notified, isFalse);
    });
  });

  group('remote / fetch / notified axes', () {
    test('remote transitions', () {
      var l = at(QueryPhase.cached);
      l = transition(l, const RemoteRegisteringEvent());
      expect(l.remote, RemotePhase.registering);
      l = transition(l, const RemoteRegisteredEvent());
      expect(l.remote, RemotePhase.registered);
      l = transition(l, const NotifiedEvent());
      expect(l.notified, isTrue);
      l = transition(l, const RemoteDroppedEvent());
      expect(l.remote, RemotePhase.unregistered);
      expect(l.notified, isFalse);
      l = transition(l, const RemoteFailedEvent());
      expect(l.remote, RemotePhase.failed);
    });

    test('fetch depth is a refcount that never goes negative', () {
      var l = at(QueryPhase.live);
      l = transition(l, const FetchBeginEvent());
      l = transition(l, const FetchBeginEvent());
      expect(deriveStatus(l), QueryStatus.fetching);
      l = transition(l, const FetchEndEvent());
      expect(deriveStatus(l), QueryStatus.fetching);
      l = transition(l, const FetchEndEvent());
      expect(deriveStatus(l), QueryStatus.idle);
      expect(identical(transition(l, const FetchEndEvent()), l), isTrue);
    });

    test('notified is idempotent', () {
      final l = transition(at(QueryPhase.live), const NotifiedEvent());
      expect(identical(transition(l, const NotifiedEvent()), l), isTrue);
    });
  });

  group('derived predicates', () {
    test('authority and server membership follow the phase', () {
      expect(isAuthoritative(at(QueryPhase.cold)), isFalse);
      expect(isAuthoritative(at(QueryPhase.cached)), isTrue);
      expect(hasServerMembership(at(QueryPhase.cached)), isFalse);
      expect(hasServerMembership(at(QueryPhase.live)), isTrue);
      expect(hasServerMembership(at(QueryPhase.viewLost)), isTrue);
      expect(seedLifecycle(true).phase, QueryPhase.cached);
    });
  });

  group('invariants over random event sequences', () {
    const events = <LifecycleEvent>[
      MembershipAppliedEvent(true),
      RowMissingEvent(),
      RemoteRegisteringEvent(),
      RemoteRegisteredEvent(),
      RemoteDroppedEvent(),
      FetchBeginEvent(),
      FetchEndEvent(),
      NotifiedEvent(),
      BucketSwitchEvent(false),
      SeedEvent(true),
    ];

    test('fetchDepth >= 0, view-lost never from cold', () {
      // Deterministic LCG so the sequence is reproducible.
      var seed = 42;
      double rand() {
        seed = (seed * 1664525 + 1013904223) % 4294967296;
        return seed / 4294967296;
      }

      for (var run = 0; run < 200; run++) {
        var l = seedLifecycle(rand() < 0.5);
        for (var i = 0; i < 25; i++) {
          final ev = events[(rand() * events.length).floor()];
          final before = l;
          try {
            l = transition(l, ev);
          } on LifecycleError {
            continue;
          }
          expect(l.fetchDepth, greaterThanOrEqualTo(0));
          if (ev is RowMissingEvent && before.phase == QueryPhase.cold) {
            expect(l.phase, QueryPhase.cold);
          }
          if (l.phase == QueryPhase.viewLost) {
            expect(before.phase, isNot(QueryPhase.cold));
          }
        }
      }
    });
  });
}
