import 'package:spooky_core/src/kernel/saga.dart';
import 'package:test/test.dart';

void main() {
  group('lanes', () {
    test('serial: first acquires, second waits, release hands over', () {
      const lane = Lane.serial('outbox');
      final a = acquire(emptyLanes(), lane);
      expect(a.decision, LaneDecision.start);
      final b = acquire(a.state, lane);
      expect(b.decision, LaneDecision.wait);
      final c = acquire(b.state, lane);
      expect(c.decision, LaneDecision.wait);
      expect(c.state.waiting['outbox'], 2);
      final r1 = release(c.state, 'outbox');
      expect(r1.startNext, isTrue);
      expect(r1.state.waiting['outbox'], 1);
      expect(r1.state.running.contains('outbox'), isTrue);
      final r2 = release(r1.state, 'outbox');
      expect(r2.startNext, isTrue);
      expect(r2.state.waiting.containsKey('outbox'), isFalse);
      final r3 = release(r2.state, 'outbox');
      expect(r3.startNext, isFalse);
      expect(r3.state.running.contains('outbox'), isFalse);
    });

    test('dedupe: a request during a run joins it and does not queue', () {
      const lane = Lane.dedupe('mat:h');
      final a = acquire(emptyLanes(), lane);
      final b = acquire(a.state, lane);
      expect(b.decision, LaneDecision.join);
      expect(identical(b.state, a.state), isTrue);
      final r = release(b.state, 'mat:h');
      expect(r.startNext, isFalse);
      expect(r.state.running, isEmpty);
    });

    test('keys are independent', () {
      final a = acquire(emptyLanes(), const Lane.serial('x'));
      final b = acquire(a.state, const Lane.serial('y'));
      expect(b.decision, LaneDecision.start);
    });
  });
}
