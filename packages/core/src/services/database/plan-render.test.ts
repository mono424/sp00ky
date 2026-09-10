import { describe, it, expect } from 'vitest';
import type { QueryPlan } from '@spooky-sync/query-builder';
import { renderBaseSelectSurql, renderRelationFetchSurql } from './plan-render';
import { buildWindowMaterializationPlan } from '../../query/window-query';

describe('renderBaseSelectSurql', () => {
  it('renders projection, where, order, limit, offset', () => {
    const plan: QueryPlan = {
      table: 'post',
      select: ['title', 'body'],
      where: [{ field: 'published', op: '=', value: true }],
      orderBy: [['created', 'desc']],
      limit: 10,
      offset: 20,
    };
    const { sql, vars } = renderBaseSelectSurql(plan);
    expect(sql).toBe('SELECT title, body FROM post WHERE published = $__p0 ORDER BY created desc LIMIT 10 START 20;');
    expect(vars).toEqual({ __p0: true });
  });

  it('renders OR groups and comparison ops', () => {
    const plan: QueryPlan = {
      table: 'game',
      where: [
        { or: [{ field: 'white', op: '=', value: 'u:1' }, { field: 'black', op: '=', value: 'u:1' }] },
        { field: 'moves', op: '>=', value: 5 },
      ],
    };
    const { sql, vars } = renderBaseSelectSurql(plan);
    expect(sql).toBe('SELECT * FROM game WHERE (white = $__p0 OR black = $__p1) AND moves >= $__p2;');
    expect(vars).toEqual({ __p0: 'u:1', __p1: 'u:1', __p2: 5 });
  });

  it('honors paramRef and swap without binding a new var', () => {
    const plan: QueryPlan = {
      table: 't',
      where: [{ field: 'tags', op: 'CONTAINS', value: undefined, paramRef: 'tag', swap: true }],
    };
    const { sql, vars } = renderBaseSelectSurql(plan, { tag: 'x' });
    expect(sql).toBe('SELECT * FROM t WHERE $tag CONTAINS tags;');
    expect(vars).toEqual({ tag: 'x' });
  });
});

// Regression guard for the "authors + comments don't load" class: any filter on
// a RecordId column (`id`) must coerce its string value with
// `type::record(<string> …)`. On SurrealDB `id = "thread:x"` (string) never
// matches a RecordId, so a base select filtered by id resolves EMPTY and its
// whole `.related()` subtree (author, comments) loads nothing. The string-based
// MemStore in relation-resolver.test.ts can't catch this (it compares keys as
// strings), so these assert the rendered SurrealQL directly — both the parent
// (base select) and child (relation fetch) sides.
describe('record-id coercion (authors/comments loading regression)', () => {
  it('coerces a base-select `id = <value>` filter to a record id', () => {
    const plan: QueryPlan = { table: 'thread', where: [{ field: 'id', op: '=', value: 'thread:abc' }] };
    const { sql, vars } = renderBaseSelectSurql(plan);
    expect(sql).toBe('SELECT * FROM thread WHERE id = type::record(<string> $__p0);');
    expect(vars).toEqual({ __p0: 'thread:abc' });
  });

  it('coerces a base-select `id = $paramRef` filter (the ThreadDetail path)', () => {
    const plan: QueryPlan = { table: 'thread', where: [{ field: 'id', op: '=', value: undefined, paramRef: 'id' }] };
    const { sql } = renderBaseSelectSurql(plan, { id: 'thread:abc' });
    expect(sql).toBe('SELECT * FROM thread WHERE id = type::record(<string> $id);');
  });

  it('does NOT coerce non-id fields (plain string/bool columns stay literal)', () => {
    const plan: QueryPlan = {
      table: 'thread',
      where: [{ field: 'title', op: '=', value: 'hi' }, { field: 'published', op: '=', value: true }],
    };
    const { sql } = renderBaseSelectSurql(plan);
    expect(sql).toBe('SELECT * FROM thread WHERE title = $__p0 AND published = $__p1;');
  });

  // Regression guard for the thread-detail "crossed results → 404" bug: a
  // slave-mode node carries BOTH a baked `value` and a `paramRef` (= the field
  // name). Materialization MUST bind `params[paramRef]` (the query's identity),
  // NOT the baked value — otherwise a stale/other plan whose baked id ≠ the
  // query's params would surface a different record's row. So rendering a plan
  // baked to id-A with params for id-B must filter by B.
  it('slave-mode node binds params over the baked value (records follow identity)', () => {
    const planBakedToA: QueryPlan = {
      table: 'thread',
      where: [{ field: 'id', op: '=', value: 'thread:A', paramRef: 'id' }],
    };
    const { sql, vars } = renderBaseSelectSurql(planBakedToA, { id: 'thread:B' });
    expect(sql).toBe('SELECT * FROM thread WHERE id = type::record(<string> $id);');
    expect(vars).toEqual({ id: 'thread:B' }); // B wins — the baked A is ignored
  });

  it('slave-mode node falls back to the baked value when the param is absent', () => {
    const plan: QueryPlan = {
      table: 'thread',
      where: [{ field: 'id', op: '=', value: 'thread:A', paramRef: 'id' }],
    };
    const { sql, vars } = renderBaseSelectSurql(plan, {}); // no params.id
    expect(sql).toBe('SELECT * FROM thread WHERE id = type::record(<string> $__p0);');
    expect(vars).toEqual({ __p0: 'thread:A' });
  });

  it('coerces `id` inside an OR group too', () => {
    const plan: QueryPlan = {
      table: 'thread',
      where: [{ or: [{ field: 'id', op: '=', value: 'thread:a' }, { field: 'id', op: '=', value: 'thread:b' }] }],
    };
    const { sql } = renderBaseSelectSurql(plan);
    expect(sql).toBe(
      'SELECT * FROM thread WHERE (id = type::record(<string> $__p0) OR id = type::record(<string> $__p1));'
    );
  });

  it('relation fetch coerces its matchField keys (the .118 fix — kept locked)', () => {
    const { sql } = renderRelationFetchSurql({ table: 'user', matchField: 'id', keys: ['user:1'] });
    expect(sql).toContain('id IN $__keys.map(|$__k| type::record(<string> $__k))');
  });
});

describe('renderRelationFetchSurql', () => {
  it('builds a WHERE ... IN $__keys batch fetch, omitting LIMIT', () => {
    const { sql, vars } = renderRelationFetchSurql({
      table: 'comment',
      matchField: 'post',
      keys: ['post:1', 'post:2'],
      where: [{ field: 'hidden', op: '=', value: false }],
      orderBy: [['rank', 'asc']],
    });
    expect(sql).toBe('SELECT * FROM comment WHERE post IN $__keys.map(|$__k| type::record(<string> $__k)) AND hidden = $__p0 ORDER BY rank asc;');
    expect(vars).toEqual({ __keys: ['post:1', 'post:2'], __p0: false });
  });
});

describe('buildWindowMaterializationPlan', () => {
  it('restricts to the id-set for offset queries, dropping where/limit/offset', () => {
    const plan: QueryPlan = {
      table: 'post',
      where: [{ field: 'x', op: '=', value: 1 }],
      orderBy: [['created', 'desc']],
      limit: 10,
      offset: 20,
      relations: [{ alias: 'a', table: 'u', cardinality: 'one', foreignKeyField: 'a' }],
    };
    const win = buildWindowMaterializationPlan(plan, ['post:5', 'post:6']);
    expect(win).toEqual({
      table: 'post',
      orderBy: [['created', 'desc']],
      relations: [{ alias: 'a', table: 'u', cardinality: 'one', foreignKeyField: 'a' }],
      ids: ['post:5', 'post:6'],
      where: undefined,
      limit: undefined,
      offset: undefined,
    });
  });

  it('returns null for non-offset queries (keep normal path)', () => {
    expect(buildWindowMaterializationPlan({ table: 'post', limit: 10 }, ['post:1'])).toBeNull();
    expect(buildWindowMaterializationPlan({ table: 'post', offset: 0 }, ['post:1'])).toBeNull();
  });
});
