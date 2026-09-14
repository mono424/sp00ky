import 'package:spooky_core/src/modules/ref_tables.dart';
import 'package:test/test.dart';

void main() {
  group('listRefTableFor', () {
    test('regular user → per-user table in dedicated mode', () {
      expect(listRefTableFor(RefMode.dedicated, 'user:abc'),
          '_00_list_ref_user_abc');
      expect(
          listRefTableFor(RefMode.dedicated, 'abc'), '_00_list_ref_user_abc');
    });

    test('single mode → the global table', () {
      expect(listRefTableFor(RefMode.single, 'user:abc'), '_00_list_ref');
    });

    test('null / unsanitizable user → the global table', () {
      expect(listRefTableFor(RefMode.dedicated, null), '_00_list_ref');
      expect(
          listRefTableFor(RefMode.dedicated, 'user:bad id!'), '_00_list_ref');
    });

    group('anonymous sentinel', () {
      test('resolves to the shared _00_list_ref_anon in both modes', () {
        expect(listRefTableFor(RefMode.dedicated, anonUserId),
            '_00_list_ref_anon');
        expect(
            listRefTableFor(RefMode.single, anonUserId), '_00_list_ref_anon');
      });

      test('a real user record "user:anon" is NOT the sentinel', () {
        // The sentinel carries no `user:` prefix, so it can never collide with
        // a real user id — `user:anon` must route to its own per-user table.
        expect(listRefTableFor(RefMode.dedicated, 'user:anon'),
            '_00_list_ref_user_anon');
      });
    });
  });

  group('bucketIdForUser', () {
    test('anon and a sanitizable id map to themselves', () {
      expect(bucketIdForUser(null), anonUserId);
      expect(bucketIdForUser(anonUserId), anonUserId);
      expect(bucketIdForUser('user:abc'), 'abc');
    });

    test('an unsanitizable id still gets a deterministic per-user bucket', () {
      // Falling back to `anon` here would put an authenticated user in the
      // shared bucket and recreate the cross-user leak.
      final bucket = bucketIdForUser('user:weird id');
      expect(bucket, isNot(anonUserId));
      expect(bucket, bucketIdForUser('user:weird id'));
      expect(bucket, isNot(bucketIdForUser('user:other id')));
    });

    test('cyrb53 matches the JavaScript client digit for digit', () {
      // Golden values produced by the TS `cyrb53` in packages/query-builder.
      expect(cyrb53('user:abc'), 515669491689055);
      expect(cyrb53('hello'), 4625896200565286);
      expect(cyrb53(''), 3338908027751811);
      expect(cyrb53('user:weird id'), 3600728299660593);
    });
  });
}
