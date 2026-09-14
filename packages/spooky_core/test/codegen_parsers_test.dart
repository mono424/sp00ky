import 'package:spooky_core/codegen.dart';
import 'package:test/test.dart';

void main() {
  group('parseAccesses', () {
    test('extracts signup/signin vars from DEFINE ACCESS (built-ins excluded)',
        () {
      const surql = '''
        DEFINE ACCESS account ON DATABASE TYPE RECORD
          SIGNUP {
            RETURN CREATE ONLY user SET
              username = \$username,
              password = crypto::argon2::generate(\$password),
              share_pubkey = \$share_pubkey;
          }
          SIGNIN ( SELECT * FROM user WHERE username = \$username
                   AND crypto::argon2::compare(password, \$password) )
          DURATION FOR TOKEN 15m, FOR SESSION 30d;
        DEFINE TABLE user SCHEMAFULL;
      ''';
      final accesses = parseAccesses(surql);
      expect(accesses, hasLength(1));
      final a = accesses.single;
      expect(a.name, 'account');
      expect(a.signupParams, ['username', 'password', 'share_pubkey']);
      expect(a.signinParams, ['username', 'password']);
    });

    test('excludes @nosync tables (and their fields) from the client schema',
        () {
      const surql = '''
        DEFINE TABLE user SCHEMAFULL;
        DEFINE FIELD email ON user TYPE string;

        -- @nosync
        DEFINE TABLE signup_code SCHEMAFULL PERMISSIONS NONE;
        DEFINE FIELD code ON signup_code TYPE string;
        DEFINE FIELD used_at ON signup_code TYPE option<datetime>;

        DEFINE TABLE game SCHEMAFULL;
      ''';
      final tables = parseSchema(surql).map((t) => t.name).toList();
      expect(tables, ['user', 'game']);
      expect(tables, isNot(contains('signup_code')));
    });

    test('a field-level @nosync drops only that field, never the next table',
        () {
      // Regression: `pending` was cleared only by a DEFINE TABLE, so a
      // field-level `-- @nosync` leaked forward and marked the FOLLOWING table
      // as server-only — silently deleting `game` from the generated Dart types
      // with no warning.
      const surql = '''
        DEFINE TABLE user SCHEMAFULL;
        DEFINE FIELD email ON user TYPE string;
        -- @nosync
        DEFINE FIELD secret_token ON user TYPE string;
        DEFINE TABLE game SCHEMAFULL;
        DEFINE FIELD title ON game TYPE string;
      ''';
      final tables = parseSchema(surql);
      expect(tables.map((t) => t.name), ['user', 'game']);
      final user = tables.firstWhere((t) => t.name == 'user');
      expect(user.fields.map((f) => f.name), ['email']);
      final game = tables.firstWhere((t) => t.name == 'game');
      expect(game.fields.map((f) => f.name), ['title']);
    });

    test('a blank line orphans an annotation instead of leaking it forward',
        () {
      const surql = '''
        -- @nosync

        DEFINE TABLE game SCHEMAFULL;
      ''';
      expect(parseSchema(surql).map((t) => t.name), ['game']);
    });

    test('@opaque keeps the field and flags it', () {
      const surql = '''
        DEFINE TABLE user SCHEMAFULL;
        DEFINE FIELD email ON user TYPE string;
        -- @opaque
        DEFINE FIELD avatar_blob ON user TYPE option<bytes>;
      ''';
      final user = parseSchema(surql).single;
      expect(user.fields.map((f) => f.name), ['email', 'avatar_blob']);
      expect(user.fields.firstWhere((f) => f.name == 'email').opaque, isFalse);
      expect(user.fields.firstWhere((f) => f.name == 'avatar_blob').opaque,
          isTrue);
    });

    test('non-annotation prose between marker and field still attaches', () {
      const surql = '''
        DEFINE TABLE user SCHEMAFULL;
        -- @nosync
        -- the API token, never leaves the server
        DEFINE FIELD secret_token ON user TYPE string;
        DEFINE FIELD email ON user TYPE string;
      ''';
      final user = parseSchema(surql).single;
      expect(user.fields.map((f) => f.name), ['email']);
    });

    test('stripServerOnlyFields removes the field and its annotation block',
        () {
      const surql = '''DEFINE TABLE user SCHEMAFULL;
DEFINE FIELD email ON user TYPE string;
-- @nosync
-- the API token
DEFINE FIELD secret_token ON user TYPE string;
DEFINE TABLE game SCHEMAFULL;''';
      final out = stripServerOnlyFields(surql);
      expect(out, isNot(contains('secret_token')));
      // The orphan must go too, or re-parsing the embedded schema would mark
      // `game` as nosync and drop it.
      expect(out, isNot(contains('@nosync')));
      expect(out, contains('DEFINE TABLE game'));
      expect(parseSchema(out).map((t) => t.name), ['user', 'game']);
    });

    test('excludes a table carrying the materialized sp00ky:nosync COMMENT',
        () {
      const surql = '''
        DEFINE TABLE user SCHEMAFULL;
        DEFINE TABLE signup_code TYPE NORMAL SCHEMAFULL COMMENT 'sp00ky:nosync' PERMISSIONS NONE;
        DEFINE FIELD code ON signup_code TYPE string;
      ''';
      final tables = parseSchema(surql).map((t) => t.name).toList();
      expect(tables, ['user']);
    });

    test('excludes _00_ system/meta tables (and their fields) from types', () {
      // Feature-flag meta tables sync and are read at runtime but must never
      // appear in generated types (mirrors the CLI's `_00_` strip). The runtime
      // circuit permission for `_00_user_feature` is seeded as a built-in
      // instead of from the parsed schema.
      const surql = '''
        DEFINE TABLE thread SCHEMAFULL PERMISSIONS FOR select WHERE true;
        DEFINE FIELD title ON thread TYPE string;

        DEFINE TABLE _00_feature_flag SCHEMAFULL PERMISSIONS NONE;
        DEFINE FIELD key ON _00_feature_flag TYPE string;

        DEFINE TABLE _00_user_feature SCHEMAFULL PERMISSIONS FOR select WHERE user = \$auth.id;
        DEFINE FIELD variant ON _00_user_feature TYPE string;
      ''';
      final tables = parseSchema(surql).map((t) => t.name).toList();
      expect(tables, ['thread']);
      expect(tables, isNot(contains('_00_user_feature')));
      expect(tables, isNot(contains('_00_feature_flag')));
    });

    test(
        'a DEFINE FIELD after a comment containing a semicolon is still parsed',
        () {
      // The parser must strip comments before splitting on `;`. A `;` inside a
      // `-- ...` comment used to fragment the following statement, leaving the
      // `^`-anchored DEFINE FIELD regex unmatched — silently dropping the field
      // (this is exactly how `broadcast.owner`/`restream_providers` vanished).
      const surql = '''
        DEFINE TABLE broadcast SCHEMAFULL
          PERMISSIONS
            FOR select WHERE true
            FOR create, update, delete WHERE \$access = "account" AND owner = \$auth.id
        ;
        -- owner of this broadcast (the publisher; = relay stream id)
        DEFINE FIELD owner ON TABLE broadcast TYPE record<user>;
        DEFINE FIELD enabled ON TABLE broadcast TYPE bool DEFAULT false;
        -- the create stays valid; mirrors stream_presence.moves.
        DEFINE FIELD restream_providers ON TABLE broadcast TYPE array<string> DEFAULT [];
      ''';
      final table =
          parseSchema(surql).singleWhere((t) => t.name == 'broadcast');
      final names = table.fields.map((f) => f.name).toList();
      expect(names, containsAll(['owner', 'enabled', 'restream_providers']));
      final owner = table.fields.firstWhere((f) => f.name == 'owner');
      expect(owner.isRecord, isTrue);
      expect(owner.recordTable, 'user');
    });

    test('parseProject returns tables + accesses together', () {
      const surql = '''
        DEFINE ACCESS account ON DATABASE TYPE RECORD
          SIGNUP ( CREATE user SET email = \$email, password = \$password )
          SIGNIN ( SELECT * FROM user WHERE email = \$email );
        DEFINE TABLE user SCHEMAFULL;
        DEFINE FIELD email ON user TYPE string;
      ''';
      final parsed = parseProject(surql);
      expect(parsed.tables.map((t) => t.name), contains('user'));
      expect(parsed.accesses.single.signupParams, ['email', 'password']);
      expect(parsed.accesses.single.signinParams, ['email']);
    });
  });

  group('parseOpenApi', () {
    const spec = '''
openapi: 3.1.0
paths:
  /spookify:
    post:
      requestBody:
        content:
          application/json:
            schema:
              type: object
              properties:
                id:
                  type: string
                count:
                  type: integer
              required:
                - id
  /noargs:
    post:
      responses:
        '200': { description: ok }
''';

    test('parses routes + typed args with required/optional', () {
      final backend = parseOpenApi('api', spec, outboxTable: 'job');
      expect(backend.name, 'api');
      expect(backend.outboxTable, 'job');
      expect(backend.routes.map((r) => r.path), ['/noargs', '/spookify']);

      final spookify = backend.routes.firstWhere((r) => r.path == '/spookify');
      final id = spookify.args.firstWhere((a) => a.name == 'id');
      final count = spookify.args.firstWhere((a) => a.name == 'count');
      expect(id.type, 'String');
      expect(id.optional, isFalse);
      expect(count.type, 'int');
      expect(count.optional, isTrue);

      final noargs = backend.routes.firstWhere((r) => r.path == '/noargs');
      expect(noargs.args, isEmpty);
    });
  });
}
