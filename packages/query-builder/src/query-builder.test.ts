import { describe, it, expect, expectTypeOf } from 'vitest';
import { QueryBuilder, buildQueryFromOptions } from './query-builder';
import { RecordId } from 'surrealdb';
import type { ChannelTableNames, TableModel } from './table-schema';

// Schema for testing the new array-based API
const testSchema = {
  tables: [
    {
      name: 'user' as const,
      columns: {
        id: { type: 'string' as const, optional: false },
        username: { type: 'string' as const, optional: false },
        email: { type: 'string' as const, optional: false },
        created_at: { type: 'number' as const, optional: false },
      },
      primaryKey: ['id'] as const,
    },
    {
      name: 'thread' as const,
      columns: {
        id: { type: 'string' as const, optional: false },
        title: { type: 'string' as const, optional: false },
        content: { type: 'string' as const, optional: false },
        author: { type: 'string' as const, optional: false },
        comments: { type: 'string' as const, optional: true },
        created_at: { type: 'number' as const, optional: false },
      },
      primaryKey: ['id'] as const,
    },
    {
      name: 'comment' as const,
      columns: {
        id: { type: 'string' as const, optional: false },
        content: { type: 'string' as const, optional: false },
        author: { type: 'string' as const, optional: false },
        thread: { type: 'string' as const, optional: false },
        created_at: { type: 'number' as const, optional: false },
      },
      primaryKey: ['id'] as const,
    },
  ],
  relationships: [
    {
      from: 'thread' as const,
      field: 'author' as const,
      to: 'user' as const,
      cardinality: 'one' as const,
    },
    {
      from: 'thread' as const,
      field: 'comments' as const,
      to: 'comment' as const,
      cardinality: 'many' as const,
    },
    {
      from: 'comment' as const,
      field: 'author' as const,
      to: 'user' as const,
      cardinality: 'one' as const,
    },
    {
      from: 'comment' as const,
      field: 'thread_ref' as const,
      to: 'thread' as const,
      cardinality: 'one' as const,
    },
  ],
  backends: {},
} as const;

describe('QueryBuilder', () => {
  it('should build basic SELECT query', () => {
    const builder = new QueryBuilder(testSchema, 'user', (q) => q.selectQuery);

    const result = builder.build().run();

    expect(result.query).toBe('SELECT * FROM user;');
    expect(result.vars).toBeUndefined();
  });

  it('should build query with where conditions', () => {
    const builder = new QueryBuilder(testSchema, 'user', (q) => q.selectQuery);

    builder.where({ username: 'john', email: 'john@example.com' });
    const result = builder.build().run();

    expect(result.query).toBe('SELECT * FROM user WHERE username = $username AND email = $email;');
    expect(result.vars).toEqual({
      username: 'john',
      email: 'john@example.com',
    });
  });

  it('should build a comparison operator condition via { _op, _val }', () => {
    const builder = new QueryBuilder(testSchema, 'user', (q) => q.selectQuery);
    builder.where({ created_at: { _op: '<=', _val: 5 } });
    const result = builder.build().run();

    expect(result.query).toBe('SELECT * FROM user WHERE created_at <= $created_at;');
    expect(result.vars).toEqual({ created_at: 5 });
  });

  it('should build an OR group via _or with position-indexed params', () => {
    const builder = new QueryBuilder(testSchema, 'user', (q) => q.selectQuery);
    builder.where({ _or: [{ username: 'x' }, { email: 'x' }] });
    const result = builder.build().run();

    expect(result.query).toBe(
      'SELECT * FROM user WHERE (username = $username__or0 OR email = $email__or1);'
    );
    expect(result.vars).toEqual({ username__or0: 'x', email__or1: 'x' });
  });

  it('should not collide an _or branch with a top-level condition on the same field', () => {
    // Mirrors the game filter where a color filter (white = me) coexists with an
    // opponent OR on white/black: the OR branch must use its own param name.
    const builder = new QueryBuilder(testSchema, 'user', (q) => q.selectQuery);
    builder.where({ username: 'me', _or: [{ username: 'opp' }, { email: 'opp' }] });
    const result = builder.build().run();

    expect(result.query).toBe(
      'SELECT * FROM user WHERE username = $username AND ' +
        '(username = $username__or0 OR email = $email__or1);'
    );
    expect(result.vars).toEqual({ username: 'me', username__or0: 'opp', email__or1: 'opp' });
  });

  it('should combine equality + comparison + OR group + order/limit/offset', () => {
    // The shape the filtered game list produces: scope equality, a date floor as
    // an integer sort_index comparison, an opponent OR group, paginated.
    const builder = new QueryBuilder(testSchema, 'user', (q) => q.selectQuery);
    builder
      .where({ email: 'e', created_at: { _op: '<=', _val: 5 }, _or: [{ username: 'p' }, { email: 'p' }] })
      .orderBy('created_at', 'asc')
      .limit(50)
      .offset(0);
    const result = builder.build().run();

    expect(result.query).toBe(
      'SELECT * FROM user WHERE email = $email AND created_at <= $created_at AND ' +
        '(username = $username__or0 OR email = $email__or1) ORDER BY created_at asc LIMIT 50 START 0;'
    );
    expect(result.vars).toEqual({
      email: 'e',
      created_at: 5,
      username__or0: 'p',
      email__or1: 'p',
    });
  });

  it('should produce a stable hash for the same logical filtered query', () => {
    const make = () =>
      new QueryBuilder(testSchema, 'user', (q) => q.selectQuery)
        .where({ email: 'e', _or: [{ username: 'p' }, { email: 'p' }] })
        .orderBy('created_at', 'asc')
        .limit(50)
        .offset(0)
        .build()
        .run();
    expect(make().hash).toBe(make().hash);

    const different = new QueryBuilder(testSchema, 'user', (q) => q.selectQuery)
      .where({ email: 'e', _or: [{ username: 'q' }, { email: 'q' }] })
      .orderBy('created_at', 'asc')
      .limit(50)
      .offset(0)
      .build()
      .run();
    expect(different.hash).not.toBe(make().hash);
  });

  it('should build query with select fields', () => {
    const builder = new QueryBuilder(testSchema, 'user', (q) => q.selectQuery);
    builder.select('username', 'email');
    const result = builder.build().run();

    expect(result.query).toBe('SELECT username, email FROM user;');
  });

  it('should throw error when calling select twice', () => {
    const builder = new QueryBuilder(testSchema, 'user', (q) => q.selectQuery);
    builder.select('username');
    expect(() => builder.select('email')).toThrow('Select can only be called once per query');
  });

  it('should build query with ordering, limit, and offset', () => {
    const builder = new QueryBuilder(testSchema, 'user', (q) => q.selectQuery);
    builder.orderBy('created_at', 'desc').limit(10).offset(5);
    const result = builder.build().run();

    expect(result.query).toBe('SELECT * FROM user ORDER BY created_at desc LIMIT 10 START 5;');
  });

  it('should support method chaining', () => {
    const builder = new QueryBuilder(testSchema, 'user', (q) => q.selectQuery);
    const result = builder
      .where({ username: 'john' })
      .select('username', 'email')
      .orderBy('created_at', 'desc')
      .limit(10)
      .build()
      .run();

    expect(result.query).toBe(
      'SELECT username, email FROM user WHERE username = $username ORDER BY created_at desc LIMIT 10;'
    );
    expect(result.vars).toEqual({ username: 'john' });
  });

  it('should build LIVE SELECT query (ignores ORDER BY, LIMIT, START)', () => {
    const builder = new QueryBuilder(testSchema, 'user', (q) => q.selectQuery);
    builder.where({ username: 'john' }).orderBy('created_at', 'desc').limit(10).offset(5);
    const result = builder.build().selectLive();

    expect(result.query).toBe('LIVE SELECT * FROM user WHERE username = $username;');
  });
});

describe('Relationship Queries', () => {
  // Using testSchema from top-level scope

  it('should build query with one-to-one relationship', () => {
    const builder = new QueryBuilder(testSchema, 'thread', (q) => q.selectQuery);
    builder.related('author');
    const result = builder.build().run();

    expect(result.query).toBe(
      'SELECT *, (SELECT * FROM user WHERE id=$parent.author LIMIT 1)[0] AS author FROM thread;'
    );
  });

  it('should build query with one-to-many relationship', () => {
    const builder = new QueryBuilder(testSchema, 'thread', (q) => q.selectQuery);
    builder.related('comments');
    const result = builder.build().run();

    expect(result.query).toBe(
      'SELECT *, (SELECT * FROM comment WHERE thread=$parent.id) AS comments FROM thread;'
    );
  });

  it('should build query with relationship modifiers', () => {
    const builder = new QueryBuilder(testSchema, 'thread', (q) => q.selectQuery);
    builder.related('comments', (q) => q.where({ author: 'user:123' }).limit(5));
    const result = builder.build().run();

    expect(result.query).toBe(
      'SELECT *, (SELECT * FROM comment WHERE thread=$parent.id AND author = user:⟨123⟩ LIMIT 5) AS comments FROM thread;'
    );
  });

  it('should build query with nested relationships', () => {
    const builder = new QueryBuilder(testSchema, 'thread', (q) => q.selectQuery);
    builder.related('comments', (q) => q.related('author'));
    const result = builder.build().run();

    expect(result.query).toBe(
      'SELECT *, (SELECT *, (SELECT * FROM user WHERE id=$parent.author LIMIT 1)[0] AS author FROM comment WHERE thread=$parent.id) AS comments FROM thread;'
    );
  });

  // An unknown relationship (e.g. a table owned by a devOnly backend that a
  // free/Cloudflare deployment never provisions, so codegen drops it from the
  // client schema) must be SKIPPED, not throw — otherwise it takes the whole
  // query (and its other `.related()` siblings) down. This is what left the
  // ThreadDetail page stuck on "Loading..." when `jobs` disappeared.
  it('skips an unknown relationship instead of throwing', () => {
    const builder = new QueryBuilder(testSchema, 'thread', (q) => q.selectQuery);
    expect(() => {
      builder.related('author' as any);
      builder.related('does_not_exist' as any); // must NOT throw
    }).not.toThrow();
    const result = builder.build().run();
    // The valid relation is still projected; the unknown one is simply absent.
    expect(result.query).toContain('AS author');
    expect(result.query).not.toContain('does_not_exist');
  });
});

describe('buildQueryFromOptions', () => {
  it('should build query from options', () => {
    const result = buildQueryFromOptions<TableModel<(typeof testSchema)['tables'][0]>, boolean>(
      'SELECT',
      'user',
      {
        where: { username: 'john' },
        select: ['username', 'email'],
        orderBy: { username: 'desc' },
        limit: 10,
      },
      testSchema
    );

    expect(result.query).toBe(
      'SELECT username, email FROM user WHERE username = $username ORDER BY username desc LIMIT 10;'
    );
    expect(result.vars).toEqual({ username: 'john' });
  });

  it('should build LIVE SELECT query from options', () => {
    const result = buildQueryFromOptions<TableModel<(typeof testSchema)['tables'][0]>, boolean>(
      'LIVE SELECT',
      'user',
      {
        where: { username: 'john' },
      },
      testSchema
    );

    expect(result.query).toBe('LIVE SELECT * FROM user WHERE username = $username;');
  });

  // Regression guard for the thread-detail "crossed results → 404" bug: the
  // engine-neutral plan's top-level WHERE must reference the SAME var the surql
  // binds (`$username`), so materialization (`select(plan, params)`) filters by
  // the query's own `params` (its identity) instead of a baked literal that
  // could belong to another query's plan. So the top-level plan node must carry
  // `paramRef` equal to the surql var name.
  it('top-level plan WHERE uses paramRef matching the surql var (slaved to params)', () => {
    const result = buildQueryFromOptions<TableModel<(typeof testSchema)['tables'][0]>, boolean>(
      'SELECT',
      'user',
      { where: { username: 'john' } },
      testSchema
    );
    // surql binds $username …
    expect(result.query).toBe('SELECT * FROM user WHERE username = $username;');
    expect(result.vars).toEqual({ username: 'john' });
    // … and the plan references that same var, not just a baked literal.
    expect(result.plan?.where).toEqual([
      { field: 'username', op: '=', value: 'john', paramRef: 'username' },
    ]);
  });
});

describe('RecordId Parsing', () => {
  it('should parse string IDs to RecordId', () => {
    const builder = new QueryBuilder(testSchema, 'thread', (q) => q.selectQuery);
    builder.where({ author: 'user:123', id: 'abc123' });
    const result = builder.build().run();

    expect(result.vars).toBeDefined();
    expect(result.vars!.author).toBeInstanceOf(RecordId);
    expect((result.vars!.author as RecordId).toString()).toBe('user:⟨123⟩');
    expect(result.vars!.id).toBeInstanceOf(RecordId);
    expect((result.vars!.id as RecordId).toString()).toBe('thread:abc123');
  });

  it('should not parse non-ID strings', () => {
    const builder = new QueryBuilder(testSchema, 'user', (q) => q.selectQuery);
    builder.where({ username: 'john_doe' });
    const result = builder.build().run();

    expect(result.vars?.username).toBe('john_doe');
    expect(result.vars?.username).not.toBeInstanceOf(RecordId);
  });
});

describe('Edge Cases', () => {
  it('should handle empty where object', () => {
    const builder = new QueryBuilder(testSchema, 'user', (q) => q.selectQuery);
    builder.where({});
    const result = builder.build().run();

    expect(result.query).toBe('SELECT * FROM user;');
  });

  it('should handle special characters in strings', () => {
    const builder = new QueryBuilder(testSchema, 'user', (q) => q.selectQuery);
    builder.where({ username: 'john"doe' });
    const result = builder.build().run();

    expect(result.vars?.username).toBe('john"doe');
  });

  it('should return options via getOptions()', () => {
    const builder = new QueryBuilder(testSchema, 'user', (q) => q.selectQuery);
    builder.where({ username: 'john' }).limit(10);
    const options = builder.getOptions();

    expect(options.where).toEqual({ username: 'john' });
    expect(options.limit).toBe(10);
  });
});

describe('Type Tests', () => {
  it('should enforce correct table names', () => {
    // Valid table names should work
    // oxlint-disable-next-line no-new
    new QueryBuilder(testSchema, 'user');
    // oxlint-disable-next-line no-new
    new QueryBuilder(testSchema, 'thread');
    // oxlint-disable-next-line no-new
    new QueryBuilder(testSchema, 'comment');

    // @ts-expect-error - invalid table name should not compile
    // oxlint-disable-next-line no-new
    new QueryBuilder(testSchema, 'invalid_table');
  });

  it('should enforce correct field names in where clause', () => {
    const builder = new QueryBuilder(testSchema, 'user', (q) => q.selectQuery);

    // Valid fields should work
    builder.where({ username: 'john' });
    builder.where({ email: 'john@example.com' });
    builder.where({ id: 'user:123' });

    // @ts-expect-error - invalid field should not compile
    builder.where({ invalid_field: 'value' });
  });

  it('should enforce correct field names in select', () => {
    const builder = new QueryBuilder(testSchema, 'comment', (q) => q.selectQuery);
    const builder2 = new QueryBuilder(testSchema, 'user', (q) => q.selectQuery);

    // Valid fields should work
    builder.select('content', 'thread');
    builder2.select('id', 'created_at');

    const builder3 = new QueryBuilder(testSchema, 'user');
    // @ts-expect-error - invalid field should not compile
    builder3.select('invalid_field');
  });

  it('should enforce correct field names in orderBy', () => {
    const builder = new QueryBuilder(testSchema, 'user', (q) => q.selectQuery);

    // Valid fields should work
    builder.orderBy('username', 'asc');
    builder.orderBy('created_at', 'desc');

    // @ts-expect-error - invalid field should not compile
    builder.orderBy('invalid_field', 'asc');
  });

  it('should enforce correct relationship field names', () => {
    const builder = new QueryBuilder(testSchema, 'thread', (q) => q.selectQuery);

    // Valid relationship fields should work (cardinality now required)
    builder.related('author');
    builder.related('comments');
  });

  it('should enforce relationship metadata types', () => {
    const builder = new QueryBuilder(testSchema, 'thread', (q) => q.selectQuery);

    // The related method should accept a modifier function with correct types (cardinality as 2nd param)
    builder.related('comments', (q) => {
      // Should be able to call methods on the related query builder
      q.where({ content: 'test' });
      q.limit(5);
      return q;
    });

    builder.related('author', 'one', (q) => {
      // Should be able to call methods on the related query builder
      q.where({ username: 'john' });
      return q;
    });
  });

  it('should enforce correct types in where clause values', () => {
    const builder = new QueryBuilder(testSchema, 'user', (q) => q.selectQuery);

    // String fields should accept strings
    builder.where({ username: 'john' });
    builder.where({ email: 'test@example.com' });

    // Number fields should accept numbers
    builder.where({ created_at: 123456 });

    // ID fields should accept strings (will be parsed to RecordId)
    builder.where({ id: 'user:123' });
  });

  it('should return correctly typed query result', () => {
    const builder = new QueryBuilder(testSchema, 'user', (q) => q.selectQuery);
    const result = builder.build().run();

    // Query should be a string
    expectTypeOf(result.query).toBeString();

    // Vars should be a record or undefined
    expectTypeOf(result.vars).toEqualTypeOf<Record<string, unknown> | undefined>();
  });

  it('should enforce correct select return type', () => {
    const builder = new QueryBuilder(testSchema, 'user', (q) => q.selectQuery);

    // Select should return the builder for chaining
    const result = builder.select('username', 'email');
    expectTypeOf(result).toMatchTypeOf<typeof builder>();
  });

  it('should enforce correct method chaining types', () => {
    const builder = new QueryBuilder(testSchema, 'user', (q) => q.selectQuery);

    // All methods should return the builder for chaining
    const result = builder
      .where({ username: 'john' })
      .select('username', 'email')
      .orderBy('created_at', 'desc')
      .limit(10)
      .offset(5);

    expectTypeOf(result).toMatchTypeOf<typeof builder>();
  });
});

describe('Schema Metadata Integration', () => {
  it('should accept testSchema in constructor', () => {
    const builder = new QueryBuilder(testSchema, 'thread', (q) => q.selectQuery);

    expect(builder).toBeDefined();
  });

  it('should build query with metadata-driven relationships', () => {
    const builder = new QueryBuilder(testSchema, 'thread', (q) => q.selectQuery);

    builder.related('author', 'one');
    const result = builder.build().run();

    expect(result.query).toBe(
      'SELECT *, (SELECT * FROM user WHERE id=$parent.author LIMIT 1)[0] AS author FROM thread;'
    );
  });

  it('should handle one-to-many relationships with metadata', () => {
    const builder = new QueryBuilder(testSchema, 'thread', (q) => q.selectQuery);

    builder.related('comments');
    const result = builder.build().run();

    expect(result.query).toBe(
      'SELECT *, (SELECT * FROM comment WHERE thread=$parent.id) AS comments FROM thread;'
    );
  });
});

describe('Update and Delete Queries', () => {
  it('should build UPDATE query with patches', () => {
    const builder = new QueryBuilder(testSchema, 'user', (q) => q.selectQuery);
    builder.where({ username: 'john' });
    const patches = [{ op: 'replace', path: '/email', value: 'new@example.com' }];
    const result = builder.build().buildUpdateQuery(patches);

    expect(result.query).toBe(
      `UPDATE user WHERE username = $username PATCH ${JSON.stringify(patches)};`
    );
    expect(result.vars).toEqual({ username: 'john' });
  });

  it('should build DELETE query', () => {
    const builder = new QueryBuilder(testSchema, 'user', (q) => q.selectQuery);
    builder.where({ username: 'john' });
    const result = builder.build().buildDeleteQuery();

    expect(result.query).toBe('DELETE FROM user WHERE username = $username;');
    expect(result.vars).toEqual({ username: 'john' });
  });
});

describe('Subquery Filtering', () => {
  it('should inject parent filter into subqueries', () => {
    const builder = new QueryBuilder(testSchema, 'thread', (q) => q.selectQuery);
    builder.related('comments');
    const query = builder.build();

    const innerQuery = query.innerQuery;
    const subqueries = innerQuery.subqueries;

    expect(subqueries).toHaveLength(1);
    const commentSubquery = subqueries[0];

    // Check that the subquery has the parent filter injected
    // thread table has 'comments' field pointing to 'comment' table (one-to-many)
    // So it should have WHERE $parentIds ∋ thread_ref (found via reverse relationship)
    // And 'thread_ref' should NOT be in vars because it's a direct variable reference
    expect(commentSubquery.selectQuery.query).toContain('$parentIds ∋ thread_ref');
    expect(commentSubquery.selectQuery.vars).not.toEqual(
      expect.objectContaining({
        thread_ref: '$parentIds',
      })
    );
  });
});

// An `-- @opaque` column is synced to the client but never stored server-side,
// so nothing on the server can evaluate a predicate against it. The failure mode
// without a guard is silent and asymmetric: the LOCAL cache does hold the value,
// so the clause filters correctly on screen while the server-side membership set
// it is reconciled against was computed without it — rows appear and vanish
// instead of erroring. Fail at the call site instead.
const opaqueSchema = {
  tables: [
    {
      name: 'document' as const,
      columns: {
        id: { type: 'string' as const, optional: false },
        title: { type: 'string' as const, optional: false },
        thumbnail: {
          type: 'Uint8Array' as const,
          optional: true,
          bytes: true,
          opaque: true,
        },
        meta: { type: 'json' as const, optional: true, opaque: true },
      },
      primaryKey: ['id'] as const,
    },
  ],
  relationships: [],
  backends: {},
} as const;

describe('@opaque column guards', () => {
  const qb = () => new QueryBuilder(opaqueSchema, 'document');

  it('rejects an opaque column in where()', () => {
    expect(() => qb().where({ thumbnail: null } as never)).toThrow(/thumbnail/);
    expect(() => qb().where({ thumbnail: null } as never)).toThrow(/@opaque/);
  });

  it('rejects an opaque column used with a comparison operator object', () => {
    expect(() =>
      qb().where({ thumbnail: { _op: '!=', _val: null } } as never)
    ).toThrow(/@opaque/);
  });

  it('rejects an opaque column inside an _or branch', () => {
    expect(() =>
      qb().where({
        _or: [{ title: 'a' }, { thumbnail: null }],
      } as never)
    ).toThrow(/thumbnail/);
  });

  it('rejects a nested path rooted at an opaque column', () => {
    expect(() => qb().where({ 'meta.secret': 'x' } as never)).toThrow(/@opaque/);
  });

  it('rejects an opaque column in orderBy()', () => {
    expect(() => qb().orderBy('thumbnail' as never)).toThrow(/@opaque/);
  });

  it('allows a normal column in where() and orderBy()', () => {
    expect(() => qb().where({ title: 'a' }).orderBy('title')).not.toThrow();
  });

  it('allows selecting an opaque column', () => {
    // Projection is the whole point of @opaque: the value IS delivered, the
    // client just cannot ask the server to filter on it.
    expect(() => qb().select('id', 'thumbnail' as never)).not.toThrow();
  });

  it('does not reject an opaque column name on a different table', () => {
    // The flag is per (table, column); names are not globally unique.
    const multi = {
      tables: [
        ...opaqueSchema.tables,
        {
          name: 'other' as const,
          columns: {
            id: { type: 'string' as const, optional: false },
            thumbnail: { type: 'string' as const, optional: false },
          },
          primaryKey: ['id'] as const,
        },
      ],
      relationships: [],
      backends: {},
    } as const;
    expect(() =>
      new QueryBuilder(multi, 'other').where({ thumbnail: 'x' } as never)
    ).not.toThrow();
  });
});

// `-- @channel` tables accept `.channel(peers)`; `-- @ephemeral` columns only
// travel between peers, so the server can never filter on them.
const channelSchema = {
  tables: [
    {
      name: 'call' as const,
      columns: {
        id: { type: 'string' as const, optional: false, recordId: true },
        caller: { type: 'string' as const, optional: false, recordId: true },
        callee: { type: 'string' as const, optional: false, recordId: true },
        view_pos: { type: 'string' as const, optional: true, ephemeral: true },
      },
      channel: true as const,
      primaryKey: ['id'] as const,
    },
    {
      name: 'note' as const,
      columns: {
        id: { type: 'string' as const, optional: false, recordId: true },
        title: { type: 'string' as const, optional: false },
      },
      primaryKey: ['id'] as const,
    },
  ],
  relationships: [],
  backends: {},
} as const;

describe('channel schema', () => {
  it('rejects an ephemeral column in where() and orderBy()', () => {
    expect(() => new QueryBuilder(channelSchema, 'call').where({ view_pos: '1' } as never)).toThrow(/@ephemeral/);
    expect(() => new QueryBuilder(channelSchema, 'call').orderBy('view_pos' as never)).toThrow(/@ephemeral/);
  });

  it('ignoreUnusedArgs marks the query without changing it', () => {
    const plain = new QueryBuilder(channelSchema, 'call').limit(5).build();
    const marked = new QueryBuilder(channelSchema, 'call').limit(5).ignoreUnusedArgs().build();
    expect(plain.ignoresUnusedArgs).toBe(false);
    expect(marked.ignoresUnusedArgs).toBe(true);
    expect(marked.hash).toBe(plain.hash);
    expect(marked.innerQuery.selectQuery.query).toBe(plain.innerQuery.selectQuery.query);
  });

  it('names the channel tables', () => {
    expectTypeOf<ChannelTableNames<typeof channelSchema>>().toEqualTypeOf<'call'>();
  });
});
