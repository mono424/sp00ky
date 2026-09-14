import 'package:spooky_core/src/query/render_set.dart';
import 'package:spooky_core/src/state/lifecycle.dart' show QueryPhase;
import 'package:spooky_core/src/state/selectors.dart' show Overlay;
import 'package:test/test.dart';

Overlay ov([List<String> writes = const [], List<String> deletes = const []]) =>
    Overlay(writes: writes.toSet(), deletes: deletes.toSet());

void main() {
  group('resolveMembership', () {
    test('cold renders the local window, anything else renders membership', () {
      const remoteArray = [('t:1', 1)];
      const localArray = [('t:2', 1)];
      expect(
        resolveMembership(const RenderInput(
            phase: QueryPhase.cold,
            remoteArray: remoteArray,
            localArray: localArray)),
        localArray,
      );
      expect(
        resolveMembership(const RenderInput(
            phase: QueryPhase.cached,
            remoteArray: remoteArray,
            localArray: localArray)),
        remoteArray,
      );
      expect(
        resolveMembership(const RenderInput(
            phase: QueryPhase.viewLost,
            remoteArray: remoteArray,
            localArray: localArray)),
        remoteArray,
      );
    });
  });

  group('buildRenderIds', () {
    const opts = RenderOptions(hasExplicitOrder: false, isWindow: false);

    test('membership minus deletes, sorted', () {
      expect(
        buildRenderIds([('t:b', 1), ('t:a', 1), ('t:c', 1)], const [],
            ov(const [], ['t:c']), opts),
        ['t:a', 't:b'],
      );
    });

    test(
        'adds pending writes the local view admits, never twice, never deleted',
        () {
      expect(
        buildRenderIds(
          [('t:1', 1)],
          [('t:1', 1), ('t:2', 1), ('t:3', 1), ('t:4', 1)],
          ov(['t:1', 't:2', 't:3', 't:9'], ['t:3']),
          opts,
        ),
        ['t:1', 't:2'],
      );
    });

    test('keeps server order for windows and explicit orderBy; dedupes', () {
      const m = [('t:b', 1), ('t:a', 1), ('t:b', 1)];
      expect(
        buildRenderIds(m, const [], ov(),
            const RenderOptions(hasExplicitOrder: true, isWindow: false)),
        ['t:b', 't:a'],
      );
      expect(
        buildRenderIds(m, const [], ov(),
            const RenderOptions(hasExplicitOrder: false, isWindow: true)),
        ['t:b', 't:a'],
      );
    });
  });
}
