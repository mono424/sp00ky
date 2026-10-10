import { describe, it, expect } from 'vitest';
import { RecordId } from 'surrealdb';
import type { QueryPlan } from '@spooky-sync/query-builder';
import { executeSelect, type SelectDb } from './sqlite-select';
import { SqliteCacheEngine } from './sqlite-cache-engine';
import { stubTransport } from './sqlite-transport.fixture';
import { stableKey } from './relation-resolver';
import type { Row } from './cache-engine';

function makeLogger(): any {
  const noop = () => {};
  const l: any = { debug: noop, info: noop, warn: noop, error: noop, trace: noop };
  l.child = () => l;
  return l;
}

/**
 * A tiny scripted "SQL responder" for the EXACT statement shapes the SQLite
 * renderers emit (base select by id, id-window IN, relation json_extract IN).
 * Not a SQL engine — just enough to make legacy-vs-worker parity observable.
 */
function makeResponder(tables: Record<string, Row[]>) {
  return (sql: string, bind: unknown[] = []): { data: string }[] => {
    const table = /FROM "([^"]+)"/.exec(sql)?.[1];
    let rows = table ? (tables[table] ?? []) : [];
    const rel = /WHERE json_extract\(data, '\$\.([A-Za-z0-9_.]+)'\) IN/.exec(sql);
    if (rel) {
      rows = rows.filter((r) => bind.includes(r[rel[1]]));
    } else if (/WHERE id IN/.test(sql)) {
      rows = rows.filter((r) => bind.includes(r.id));
    } else if (/WHERE id = \?/.test(sql)) {
      rows = rows.filter((r) => r.id === bind[0]);
    }
    return rows.map((r) => ({ data: JSON.stringify(r) }));
  };
}

const FIXTURE: Record<string, Row[]> = {
  thread: [
    { id: 'thread:A', title: 'thread A', author: 'user:1' },
    { id: 'thread:B', title: 'thread B', author: 'user:2' },
  ],
  comment: [
    { id: 'comment:1', thread: 'thread:B', body: 'first' },
    { id: 'comment:2', thread: 'thread:B', body: 'second' },
    { id: 'comment:3', thread: 'thread:A', body: 'other thread' },
  ],
  game: [
    { id: 'game:476gsdlvro7cme8nwied', white: 'player_name:a' },
    { id: 'game:stgyqp7fm3sc4tmyc8fm', white: 'player_name:b' },
  ],
};

/** The `.one()` detail-view plan shape: id baked as a literal AND slaved to
 *  the `id` param (aa4af79b), plus a `many` relation. */
const DETAIL_PLAN: QueryPlan = {
  table: 'thread',
  where: [{ field: 'id', op: '=', value: 'thread:A', paramRef: 'id' }],
  relations: [
    { alias: 'comments', table: 'comment', cardinality: 'many', foreignKeyField: 'thread' },
  ],
};

function makeSelectDb(
  tables: Record<string, Row[]>,
  calls: { sql: string; bind?: unknown[] }[]
): SelectDb {
  const respond = makeResponder(tables);
  return {
    exec: (sql, bind) => {
      calls.push({ sql, bind });
      return respond(sql, bind ?? []);
    },
    run: (sql, bind) => {
      calls.push({ sql, bind });
    },
    knownTables: new Set<string>(),
  };
}

/** An engine whose worker answers exec/run against the fixture (legacy path)
 *  and optionally the one-hop `select` op, recording every message. */
function makeEngine(opts: {
  workerSelect: boolean;
  answerSelect?: boolean;
  calls?: { type: string; sql?: string; bind?: unknown[]; payload?: any }[];
}) {
  const respond = makeResponder(FIXTURE);
  const calls = opts.calls ?? [];
  const engine = new SqliteCacheEngine(
    { namespace: 'n', database: 'd' } as any,
    makeLogger(),
    { useOpfs: false, workerSelect: opts.workerSelect }
  );
  stubTransport(engine, async (type, payload) => {
    calls.push({ type, sql: payload?.sql, bind: payload?.bind, payload });
    if (type === 'open') return { persisted: false };
    if (type === 'exec') return { rows: respond(payload.sql, payload.bind ?? []) };
    if (type === 'run' || type === 'batch' || type === 'close') return {};
    if (type === 'select') {
      if (!opts.answerSelect) throw new Error(`sqlite worker: unknown message ${type}`);
      const dbCalls: { sql: string; bind?: unknown[] }[] = [];
      return executeSelect(payload.plan, payload.params ?? {}, makeSelectDb(FIXTURE, dbCalls));
    }
    throw new Error(`sqlite worker: unknown message ${type}`);
  });
  return { engine, calls };
}

describe('executeSelect (worker-side plan execution)', () => {
  it('slaves the base filter to params over the baked literal (aa4af79b guard)', async () => {
    const calls: { sql: string; bind?: unknown[] }[] = [];
    const { rows } = await executeSelect(DETAIL_PLAN, { id: 'thread:B' }, makeSelectDb(FIXTURE, calls));
    // Filtered by the PARAM (thread:B), not the baked literal (thread:A).
    expect(rows).toHaveLength(1);
    expect(rows[0].id).toBe('thread:B');
    expect(rows[0].comments).toEqual([
      { id: 'comment:1', thread: 'thread:B', body: 'first' },
      { id: 'comment:2', thread: 'thread:B', body: 'second' },
    ]);
  });

  it('falls back to the baked literal when the param key is absent', async () => {
    const calls: { sql: string; bind?: unknown[] }[] = [];
    const { rows } = await executeSelect(DETAIL_PLAN, {}, makeSelectDb(FIXTURE, calls));
    expect(rows).toHaveLength(1);
    expect(rows[0].id).toBe('thread:A');
  });

  it('counts relation fetches and honors the ids window ordering', async () => {
    const calls: { sql: string; bind?: unknown[] }[] = [];
    const plan: QueryPlan = {
      table: 'comment',
      ids: ['comment:2', 'comment:1'],
    };
    const { rows, relationFetches } = await executeSelect(plan, {}, makeSelectDb(FIXTURE, calls));
    expect(relationFetches).toBe(0);
    // ids order preserved (no ORDER BY).
    expect(rows.map((r) => r.id)).toEqual(['comment:2', 'comment:1']);
  });
});

describe('worker-select parity with the legacy multi-hop path', () => {
  it('produces identical rows AND an identical SQL statement sequence', async () => {
    // Legacy: engine drives one worker round-trip per statement.
    const legacy = makeEngine({ workerSelect: false });
    await legacy.engine.connect('anon');
    const legacyRows = await legacy.engine.select(DETAIL_PLAN, { id: 'thread:B' });
    const legacySql = legacy.calls
      .filter((c) => c.type === 'exec' || c.type === 'run')
      .map((c) => ({ sql: c.sql, bind: c.bind ?? [] }));

    // Worker path: same plan executed in one hop via executeSelect.
    const workerCalls: { sql: string; bind?: unknown[] }[] = [];
    const { rows: workerRows } = await executeSelect(
      DETAIL_PLAN,
      { id: 'thread:B' },
      makeSelectDb(FIXTURE, workerCalls)
    );

    expect(workerRows).toEqual(legacyRows);
    expect(workerCalls.map((c) => ({ sql: c.sql, bind: c.bind ?? [] }))).toEqual(legacySql);
  });
});

describe('SqliteCacheEngine.select via worker (one hop)', () => {
  it('sends exactly one select message, zero exec/run', async () => {
    const { engine, calls } = makeEngine({ workerSelect: true, answerSelect: true });
    await engine.connect('anon');
    const rows = await engine.select(DETAIL_PLAN, { id: 'thread:B' });
    expect(rows).toHaveLength(1);
    expect(rows[0].id).toBe('thread:B');
    const afterOpen = calls.filter((c) => c.type !== 'open');
    expect(afterOpen.map((c) => c.type)).toEqual(['select']);
  });

  it('normalizes RecordId param VALUES to strings but never drops param KEYS', async () => {
    const { engine, calls } = makeEngine({ workerSelect: true, answerSelect: true });
    await engine.connect('anon');
    await engine.select(DETAIL_PLAN, { id: new RecordId('thread', 'B'), untouched: 7 });
    const sel = calls.find((c) => c.type === 'select')!;
    // VALUE normalized (a class instance would lose its prototype in the real
    // structured clone); KEY present — comparisonSql's paramRef resolution
    // checks hasOwnProperty, and a dropped key silently falls back to the
    // baked literal (the aa4af79b crossed-results class).
    expect(sel.payload.params).toEqual({ id: 'thread:B', untouched: 7 });
    expect(Object.prototype.hasOwnProperty.call(sel.payload.params, 'id')).toBe(true);
  });

  it('normalizes a window plan\'s RecordId ids to the same strings the legacy path binds', async () => {
    const rid = new RecordId('comment', '2');
    const { engine, calls } = makeEngine({ workerSelect: true, answerSelect: true });
    await engine.connect('anon');
    const rows = await engine.select({ table: 'comment', ids: [rid, 'comment:1'] }, {});
    const sel = calls.find((c) => c.type === 'select')!;
    // The raw spelling the rows are stored under, never the SDK's escaped
    // `comment:⟨2⟩`: that one matched no stored row.
    expect(sel.payload.plan.ids).toEqual(['comment:2', 'comment:1']);
    expect(rows.map((r) => r.id)).toEqual(['comment:2', 'comment:1']);
  });

  it('finds a window row whose id starts with a digit (SDK 2.1.0 escapes it)', async () => {
    // `materializeEffect` parses every window id into a RecordId, and SDK
    // 2.1.0's `toString()` spells this one `game:⟨476g…⟩`. Keyed that way the
    // row went missing, the window came back short and read as the list end.
    const { engine } = makeEngine({ workerSelect: true, answerSelect: true });
    await engine.connect('anon');
    const ids = [new RecordId('game', '476gsdlvro7cme8nwied'), new RecordId('game', 'stgyqp7fm3sc4tmyc8fm')];
    const rows = await engine.select({ table: 'game', ids }, {});
    expect(rows.map((r) => r.id)).toEqual(['game:476gsdlvro7cme8nwied', 'game:stgyqp7fm3sc4tmyc8fm']);
  });

  it('normalizes RecordId values baked inside the plan\'s where tree', async () => {
    const { engine, calls } = makeEngine({ workerSelect: true, answerSelect: true });
    await engine.connect('anon');
    const plan: QueryPlan = {
      table: 'thread',
      where: [{ field: 'author', op: '=', value: new RecordId('user', 'u1') }],
      relations: [
        {
          alias: 'comments',
          table: 'comment',
          cardinality: 'many',
          foreignKeyField: 'thread',
          where: [{ field: 'author', op: '=', value: new RecordId('user', 'u1') }],
        },
      ],
    };
    await engine.select(plan, {});
    const sel = calls.find((c) => c.type === 'select')!;
    // Both the top-level where and the relation sub-where cross the boundary
    // as strings — a RecordId instance would structured-clone to `{}`.
    expect(sel.payload.plan.where[0].value).toBe('user:u1');
    expect(sel.payload.plan.relations[0].where[0].value).toBe('user:u1');
  });

  it('folds the worker-reported relationFetches into __sqliteStats', async () => {
    const { engine } = makeEngine({ workerSelect: true, answerSelect: true });
    await engine.connect('anon');
    const before = (globalThis as any).__sqliteStats?.relationFetches ?? 0;
    await engine.select(DETAIL_PLAN, { id: 'thread:B' });
    // DETAIL_PLAN has one relation level → executeSelect reports 1 fetch.
    expect((globalThis as any).__sqliteStats.relationFetches).toBe(before + 1);
  });

  it('degrades to the legacy path when the worker lacks the select op', async () => {
    const { engine, calls } = makeEngine({ workerSelect: true, answerSelect: false });
    await engine.connect('anon');
    const rows = await engine.select(DETAIL_PLAN, { id: 'thread:B' });
    expect(rows).toHaveLength(1);
    expect(rows[0].id).toBe('thread:B');
    // First attempt was 'select'; the rejection flipped the flag and the
    // legacy path completed with exec/run hops.
    expect(calls.some((c) => c.type === 'select')).toBe(true);
    expect(calls.some((c) => c.type === 'exec')).toBe(true);
    // The flag stays off: a second select never retries the worker op.
    const before = calls.filter((c) => c.type === 'select').length;
    await engine.select(DETAIL_PLAN, { id: 'thread:A' });
    expect(calls.filter((c) => c.type === 'select').length).toBe(before);
  });
});
