import { describe, it, expect } from 'vitest';
import type { QueryPlan } from '@spooky-sync/query-builder';
import { buildIdSetPlan, buildIdSetSurql, buildWindowMaterializationPlan } from './window-query';
import { executeSelect, type SelectDb } from '../services/database/sqlite-select';
import type { Row } from '../services/database/cache-engine';

describe('buildWindowMaterializationPlan', () => {
  it('returns null without an offset and an id-set plan with one', () => {
    const plan: QueryPlan = { table: 't', where: [{ field: 'a', op: '=', value: 1 } as any], limit: 5 };
    expect(buildWindowMaterializationPlan(plan, ['t:1'])).toBeNull();
    expect(buildWindowMaterializationPlan({ ...plan, offset: 0 }, ['t:1'])).toBeNull();
    const win = buildWindowMaterializationPlan({ ...plan, offset: 5 }, ['t:1']);
    expect(win).toMatchObject({ ids: ['t:1'], where: undefined, limit: undefined, offset: undefined });
  });
});

// `buildIdSetPlan` is what makes membership-authoritative rendering possible on
// any local engine: it turns a query's plan into "these exact ids, ordered,
// relations intact". Dropping `where` is the point — re-applying the predicate
// against stale local bodies is what let removed rows keep rendering.

const BASE: QueryPlan = {
  table: 'thread',
  where: [{ field: 'done', op: '=', value: false }],
  limit: 50,
  offset: 0,
  orderBy: [['title', 'asc']],
  relations: [
    { alias: 'comments', table: 'comment', cardinality: 'many', foreignKeyField: 'thread' },
  ],
} as any;

describe('buildIdSetPlan', () => {
  it('binds the id-set and drops where/limit/offset, keeping orderBy + relations', () => {
    const out = buildIdSetPlan(BASE, ['thread:A']);
    expect(out.ids).toEqual(['thread:A']);
    expect(out.where).toBeUndefined();
    expect(out.limit).toBeUndefined();
    expect(out.offset).toBeUndefined();
    expect(out.orderBy).toEqual([['title', 'asc']]);
    expect(out.relations).toEqual(BASE.relations);
    expect(out.table).toBe('thread');
  });

  it('applies to non-offset plans, unlike the windowed-only helper', () => {
    // The windowed helper deliberately bails so the old caller kept its scan;
    // membership rendering needs the same rewrite for every query.
    expect(buildWindowMaterializationPlan(BASE, ['thread:A'])).toBeNull();
    expect(buildIdSetPlan(BASE, ['thread:A']).ids).toEqual(['thread:A']);
  });

  it('does not mutate the input plan', () => {
    buildIdSetPlan(BASE, ['thread:A']);
    expect(BASE.where).toEqual([{ field: 'done', op: '=', value: false }]);
    expect(BASE.limit).toBe(50);
  });
});

describe('buildIdSetSurql', () => {
  it('rewrites a non-offset query to the id-set source, keeping ORDER BY', () => {
    const out = buildIdSetSurql('SELECT * FROM thread WHERE done = false ORDER BY title ASC LIMIT 50;');
    expect(out?.query).toBe('SELECT * FROM $__win ORDER BY title ASC');
  });

  it('preserves the projection', () => {
    const out = buildIdSetSurql('SELECT id, title FROM thread WHERE done = false;');
    expect(out?.query).toBe('SELECT id, title FROM $__win');
  });

  it('returns null when there is no top-level FROM to replace', () => {
    expect(buildIdSetSurql('RETURN true;')).toBeNull();
  });
});

// ---- Engine parity -------------------------------------------------------
//
// Both local engines short-circuit to `selectByIds` when `plan.ids` is set. This
// pins that the id-set plan really does filter AND still resolves `.related()`
// on the SQLite engine (the `where`-dropping path the SurrealDB engine shares).

const FIXTURE: Record<string, Row[]> = {
  thread: [
    { id: 'thread:A', title: 'a thread', done: false },
    { id: 'thread:B', title: 'b thread', done: false },
  ],
  comment: [
    { id: 'comment:1', thread: 'thread:A', body: 'on A' },
    { id: 'comment:2', thread: 'thread:B', body: 'on B' },
  ],
};

function makeSelectDb(): SelectDb {
  const respond = (sql: string, bind: unknown[] = []): { data: string }[] => {
    const table = /FROM "([^"]+)"/.exec(sql)?.[1];
    let rows = table ? (FIXTURE[table] ?? []) : [];
    const rel = /WHERE json_extract\(data, '\$\.([A-Za-z0-9_.]+)'\) IN/.exec(sql);
    if (rel) rows = rows.filter((r) => bind.includes(r[rel[1]]));
    else if (/WHERE id IN/.test(sql)) rows = rows.filter((r) => bind.includes(r.id));
    return rows.map((r) => ({ data: JSON.stringify(r) }));
  };
  return {
    exec: (sql, bind) => respond(sql, bind ?? []),
    run: () => {},
    knownTables: new Set<string>(),
  };
}

describe('id-set plan on the SQLite engine', () => {
  it('returns only the id-set even though both bodies match the predicate', async () => {
    const { rows } = await executeSelect(
      buildIdSetPlan(BASE, ['thread:A']),
      {},
      makeSelectDb()
    );
    expect(rows.map((r) => r.id)).toEqual(['thread:A']);
  });

  it('still resolves .related() children for the id-set rows', async () => {
    const { rows, relationFetches } = await executeSelect(
      buildIdSetPlan(BASE, ['thread:A']),
      {},
      makeSelectDb()
    );
    expect(relationFetches).toBe(1);
    expect((rows[0] as any).comments.map((c: Row) => c.id)).toEqual(['comment:1']);
  });

  it('renders nothing for an empty id-set (known-and-empty membership)', async () => {
    const { rows } = await executeSelect(buildIdSetPlan(BASE, []), {}, makeSelectDb());
    expect(rows).toEqual([]);
  });
});
