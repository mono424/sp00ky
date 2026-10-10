import { describe, it, expect } from 'vitest';
import { RecordId } from 'surrealdb';
import type { RelationPlan, WhereNode } from '@spooky-sync/query-builder';
import { looksLikeRecordId, resolveRelations, sortRows, stableKey } from './relation-resolver';
import {
  RelationCycleError,
  type RelationFetch,
  type Row,
  type RowFetcher,
} from './cache-engine';

/**
 * Deterministic in-memory store used as the reference for decomposition. It
 * implements the SAME fetch primitive the real engines expose
 * (`WHERE matchField IN keys AND <where>`, ORDER BY, projection) so a passing
 * resolver here is correct independent of SurrealDB/SQLite specifics. Rows are
 * deep-cloned on read (as a real engine returns fresh objects), so nested
 * attachment never mutates the store.
 */
class MemStore implements RowFetcher {
  constructor(private tables: Record<string, Row[]>) {}

  async fetchRelation(req: RelationFetch): Promise<Row[]> {
    const keySet = new Set(req.keys.map(stableKey));
    let rows = (this.tables[req.table] ?? []).filter((r) =>
      keySet.has(stableKey(r[req.matchField]))
    );
    if (req.where) rows = rows.filter((r) => matchesWhere(r, req.where!));
    rows = rows.map((r) => structuredClone(r));
    if (req.orderBy) rows = sortRows(rows, req.orderBy);
    if (req.select) {
      rows = rows.map((r) => {
        const out: Row = {};
        for (const f of ['id', ...req.select!]) if (f in r) out[f] = r[f];
        return out;
      });
    }
    return rows;
  }
}

function matchesWhere(row: Row, where: WhereNode[]): boolean {
  const cmp = (c: { field: string; op: string; value: unknown }): boolean => {
    const v = row[c.field];
    switch (c.op) {
      case '=':
        return stableKey(v) === stableKey(c.value);
      case '!=':
        return stableKey(v) !== stableKey(c.value);
      case '>':
        return (v as number) > (c.value as number);
      case '>=':
        return (v as number) >= (c.value as number);
      case '<':
        return (v as number) < (c.value as number);
      case '<=':
        return (v as number) <= (c.value as number);
      default:
        return false;
    }
  };
  return where.every((node) => {
    if ('or' in node) return node.or.some(cmp);
    return cmp(node);
  });
}

// Convenience builder for a RelationPlan.
function rel(p: Partial<RelationPlan> & Pick<RelationPlan, 'alias' | 'table' | 'cardinality' | 'foreignKeyField'>): RelationPlan {
  return p as RelationPlan;
}

describe('resolveRelations — decomposition', () => {
  it('1. flat, no relations: leaves parents untouched', async () => {
    const store = new MemStore({});
    const parents: Row[] = [{ id: 'post:1', title: 'a' }];
    await resolveRelations(parents, undefined, store);
    expect(parents).toEqual([{ id: 'post:1', title: 'a' }]);
  });

  it('2a. one-to-one match: attaches the single related row', async () => {
    const store = new MemStore({ user: [{ id: 'user:1', name: 'ana' }] });
    const parents: Row[] = [{ id: 'post:1', author: 'user:1' }];
    await resolveRelations(
      parents,
      [rel({ alias: 'author', table: 'user', cardinality: 'one', foreignKeyField: 'author', limit: 1 })],
      store
    );
    expect(parents[0].author).toEqual({ id: 'user:1', name: 'ana' });
  });

  it('2b. one-to-one no match: attaches null', async () => {
    const store = new MemStore({ user: [] });
    const parents: Row[] = [{ id: 'post:1', author: 'user:404' }];
    await resolveRelations(
      parents,
      [rel({ alias: 'author', table: 'user', cardinality: 'one', foreignKeyField: 'author', limit: 1 })],
      store
    );
    expect(parents[0].author).toBeNull();
  });

  it('3. one-to-many: empty vs N children grouped per parent', async () => {
    const store = new MemStore({
      comment: [
        { id: 'comment:1', post: 'post:1', body: 'x' },
        { id: 'comment:2', post: 'post:1', body: 'y' },
        { id: 'comment:3', post: 'post:2', body: 'z' },
      ],
    });
    const parents: Row[] = [{ id: 'post:1' }, { id: 'post:2' }, { id: 'post:3' }];
    await resolveRelations(
      parents,
      [rel({ alias: 'comments', table: 'comment', cardinality: 'many', foreignKeyField: 'post' })],
      store
    );
    expect((parents[0].comments as Row[]).map((c) => c.id)).toEqual(['comment:1', 'comment:2']);
    expect((parents[1].comments as Row[]).map((c) => c.id)).toEqual(['comment:3']);
    expect(parents[2].comments).toEqual([]);
  });

  it('4. per-parent LIMIT + ORDER applied within each group, not globally', async () => {
    const store = new MemStore({
      comment: [
        { id: 'comment:1', post: 'post:1', rank: 3 },
        { id: 'comment:2', post: 'post:1', rank: 1 },
        { id: 'comment:3', post: 'post:1', rank: 2 },
        { id: 'comment:4', post: 'post:2', rank: 5 },
        { id: 'comment:5', post: 'post:2', rank: 4 },
      ],
    });
    const parents: Row[] = [{ id: 'post:1' }, { id: 'post:2' }];
    await resolveRelations(
      parents,
      [
        rel({
          alias: 'comments',
          table: 'comment',
          cardinality: 'many',
          foreignKeyField: 'post',
          orderBy: [['rank', 'asc']],
          limit: 2,
        }),
      ],
      store
    );
    // post:1 keeps its OWN top-2 by rank (1,2) — not a global top-2 (which would
    // have starved post:2).
    expect((parents[0].comments as Row[]).map((c) => c.rank)).toEqual([1, 2]);
    expect((parents[1].comments as Row[]).map((c) => c.rank)).toEqual([4, 5]);
  });

  it('5. sub-where filters the relation batch', async () => {
    const store = new MemStore({
      comment: [
        { id: 'comment:1', post: 'post:1', hidden: false },
        { id: 'comment:2', post: 'post:1', hidden: true },
      ],
    });
    const parents: Row[] = [{ id: 'post:1' }];
    await resolveRelations(
      parents,
      [
        rel({
          alias: 'comments',
          table: 'comment',
          cardinality: 'many',
          foreignKeyField: 'post',
          where: [{ field: 'hidden', op: '=', value: false }],
        }),
      ],
      store
    );
    expect((parents[0].comments as Row[]).map((c) => c.id)).toEqual(['comment:1']);
  });

  it('6. 2-level nested with cross-level $parent dependency (many -> one)', async () => {
    const store = new MemStore({
      comment: [
        { id: 'comment:1', post: 'post:1', author: 'user:1' },
        { id: 'comment:2', post: 'post:1', author: 'user:2' },
      ],
      user: [
        { id: 'user:1', name: 'ana' },
        { id: 'user:2', name: 'bob' },
      ],
    });
    const parents: Row[] = [{ id: 'post:1' }];
    await resolveRelations(
      parents,
      [
        rel({
          alias: 'comments',
          table: 'comment',
          cardinality: 'many',
          foreignKeyField: 'post',
          relations: [
            rel({ alias: 'author', table: 'user', cardinality: 'one', foreignKeyField: 'author', limit: 1 }),
          ],
        }),
      ],
      store
    );
    const comments = parents[0].comments as Row[];
    expect(comments.map((c) => (c.author as Row).name)).toEqual(['ana', 'bob']);
  });

  it('7. 3-level deep, mixed cardinality, O(depth) batch count', async () => {
    let batches = 0;
    const base = new MemStore({
      comment: [{ id: 'comment:1', post: 'post:1', author: 'user:1' }],
      user: [{ id: 'user:1', org: 'org:1' }],
      org: [{ id: 'org:1', name: 'acme' }],
    });
    const counting: RowFetcher = {
      fetchRelation: (req) => {
        batches++;
        return base.fetchRelation(req);
      },
    };
    const parents: Row[] = [{ id: 'post:1' }];
    await resolveRelations(
      parents,
      [
        rel({
          alias: 'comments',
          table: 'comment',
          cardinality: 'many',
          foreignKeyField: 'post',
          relations: [
            rel({
              alias: 'author',
              table: 'user',
              cardinality: 'one',
              foreignKeyField: 'author',
              limit: 1,
              relations: [
                rel({ alias: 'org', table: 'org', cardinality: 'one', foreignKeyField: 'org', limit: 1 }),
              ],
            }),
          ],
        }),
      ],
      counting
    );
    const org = ((parents[0].comments as Row[])[0].author as Row).org as Row;
    expect(org.name).toBe('acme');
    // One batch per level (3 levels), NOT per row.
    expect(batches).toBe(3);
  });

  it('8. null/absent foreign keys mid-tree yield empty, no spurious fetch', async () => {
    let fetchedKeys: unknown[] = [];
    const base = new MemStore({ user: [{ id: 'user:1', name: 'ana' }] });
    const spy: RowFetcher = {
      fetchRelation: (req) => {
        fetchedKeys = req.keys;
        return base.fetchRelation(req);
      },
    };
    const parents: Row[] = [
      { id: 'post:1', author: 'user:1' },
      { id: 'post:2', author: null },
      { id: 'post:3' }, // absent
    ];
    await resolveRelations(
      parents,
      [rel({ alias: 'author', table: 'user', cardinality: 'one', foreignKeyField: 'author', limit: 1 })],
      spy
    );
    expect(fetchedKeys).toEqual(['user:1']); // null/absent excluded
    expect(parents[0].author).toEqual({ id: 'user:1', name: 'ana' });
    expect(parents[1].author).toBeNull();
    expect(parents[2].author).toBeNull();
  });

  it('9. nesting past MAX_RELATION_DEPTH throws RelationCycleError', async () => {
    // Build a plan nested deeper than the guard.
    let leaf: RelationPlan = rel({ alias: 'r', table: 't', cardinality: 'one', foreignKeyField: 'r' });
    for (let i = 0; i < 20; i++) {
      leaf = rel({ alias: 'r', table: 't', cardinality: 'one', foreignKeyField: 'r', relations: [leaf] });
    }
    const store = new MemStore({ t: [{ id: 't:1', r: 't:1' }] });
    await expect(resolveRelations([{ id: 't:1', r: 't:1' }], [leaf], store)).rejects.toBeInstanceOf(
      RelationCycleError
    );
  });

  it('10. RecordId-shaped keys group identically to their string form', async () => {
    // Parent FK is a RecordId-like object; child id is the string form.
    const rid = { tb: 'user', id: '1', toString: () => 'user:1' };
    const store = new MemStore({ user: [{ id: 'user:1', name: 'ana' }] });
    const parents: Row[] = [{ id: 'post:1', author: rid }];
    await resolveRelations(
      parents,
      [rel({ alias: 'author', table: 'user', cardinality: 'one', foreignKeyField: 'author', limit: 1 })],
      store
    );
    expect((parents[0].author as Row).name).toBe('ana');
    // alias appended LAST (key order parity with `SELECT *, <sub> AS alias`).
    expect(Object.keys(parents[0])).toEqual(['id', 'author']);
  });
});

describe('stableKey', () => {
  it('collapses RecordId object and string form', () => {
    expect(stableKey({ tb: 'user', id: '1' })).toBe('user:1');
    expect(stableKey('user:1')).toBe('user:1');
  });

  it('spells an SDK RecordId raw, never as escaped SurrealQL', () => {
    // SDK 2.1.0 `toString()`s these as `game:⟨476g…⟩`, `t:⟨123⟩`, `t:⟨a-b⟩`.
    expect(stableKey(new RecordId('game', '476gsdlvro7cme8nwied'))).toBe('game:476gsdlvro7cme8nwied');
    expect(stableKey(new RecordId('t', '123'))).toBe('t:123');
    expect(stableKey(new RecordId('t', 'a-b'))).toBe('t:a-b');
    expect(stableKey(new RecordId('t', 7))).toBe('t:7');
    expect(looksLikeRecordId(new RecordId('game', '476gsdlvro7cme8nwied'))).toBe(true);
  });

  it('leaves a complex key to toString and a plain row with table/id columns alone', () => {
    const complex = new RecordId('t', [1, 'x']);
    expect(stableKey(complex)).toBe(complex.toString());
    const row = { table: { name: 'x' }, id: 'y' };
    expect(stableKey(row)).toBe(JSON.stringify(row));
  });
});

/**
 * Independent, obviously-correct reference resolver: for each parent, fetch its
 * relations directly (no batching, no dedup) and recurse. The batched
 * `resolveRelations` must produce byte-identical output for random trees.
 */
async function naiveResolve(parents: Row[], relations: RelationPlan[] | undefined, store: MemStore): Promise<void> {
  if (!relations) return;
  for (const parent of parents) {
    for (const r of relations) {
      const isOne = r.cardinality === 'one';
      const key = isOne ? parent[r.foreignKeyField] : parent['id'];
      let bucket: Row[] = [];
      if (key != null) {
        bucket = await store.fetchRelation({
          table: r.table,
          matchField: isOne ? 'id' : r.foreignKeyField,
          keys: [key],
          where: r.where,
          orderBy: r.orderBy,
          select: r.select,
        });
        await naiveResolve(bucket, r.relations, store);
      }
      if (r.orderBy) bucket = sortRows(bucket, r.orderBy);
      if (r.limit !== undefined) bucket = bucket.slice(0, r.limit);
      parent[r.alias] = isOne ? bucket[0] ?? null : bucket;
    }
  }
}

// Tiny seeded PRNG so failures reproduce (Math.random is nondeterministic).
function mulberry32(seed: number): () => number {
  return () => {
    seed |= 0;
    seed = (seed + 0x6d2b79f5) | 0;
    let t = Math.imul(seed ^ (seed >>> 15), 1 | seed);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

describe('resolveRelations — property: batched == naive over random trees', () => {
  const TABLES = ['a', 'b', 'c', 'd'];

  it('matches the naive reference for 200 random datasets/plans', async () => {
    for (let seed = 0; seed < 200; seed++) {
      const rng = mulberry32(seed + 1);
      const pick = <T,>(arr: T[]): T => arr[Math.floor(rng() * arr.length)];

      // Random dataset: each table gets a few rows with random FK fields.
      const tables: Record<string, Row[]> = {};
      for (const t of TABLES) {
        const n = Math.floor(rng() * 5);
        tables[t] = [];
        for (let i = 0; i < n; i++) {
          tables[t].push({
            id: `${t}:${i}`,
            rank: Math.floor(rng() * 5),
            // FK columns pointing at every table (some valid, some dangling).
            ...Object.fromEntries(
              TABLES.map((ft) => [`${ft}_fk`, rng() < 0.7 ? `${ft}:${Math.floor(rng() * 5)}` : null])
            ),
          });
        }
      }

      // Random relation tree, depth <= 4.
      const buildPlan = (depth: number): RelationPlan[] => {
        if (depth > 4 || rng() < 0.35) return [];
        const count = Math.floor(rng() * 2) + 1;
        const out: RelationPlan[] = [];
        for (let i = 0; i < count; i++) {
          const table = pick(TABLES);
          const cardinality = rng() < 0.5 ? 'one' : 'many';
          out.push(
            rel({
              alias: `rel_${depth}_${i}`,
              table,
              cardinality,
              foreignKeyField: cardinality === 'one' ? `${table}_fk` : `${pick(TABLES)}_fk`,
              orderBy: rng() < 0.5 ? [['rank', pick(['asc', 'desc'] as const)]] : undefined,
              limit: rng() < 0.5 ? Math.floor(rng() * 3) + 1 : undefined,
              relations: buildPlan(depth + 1),
            })
          );
        }
        return out;
      };
      const plan = buildPlan(0);

      const rootTable = pick(TABLES);
      const roots = (tables[rootTable] ?? []).map((r) => structuredClone(r));
      if (roots.length === 0) continue;

      const batched = structuredClone(roots);
      const naive = structuredClone(roots);
      await resolveRelations(batched, plan, new MemStore(structuredClone(tables)));
      await naiveResolve(naive, plan, new MemStore(structuredClone(tables)));

      expect(batched, `seed ${seed}`).toEqual(naive);
    }
  });
});

describe('to-one relations over a `string | record` column', () => {
  const rel = (alias: string, fk: string): RelationPlan => ({
    table: 'player_name',
    alias,
    cardinality: 'one',
    foreignKeyField: fk,
  });

  it('keeps a legacy plain string when the alias is the foreign-key column', async () => {
    const store = new MemStore({
      player_name: [{ id: 'player_name:PN_1', name: 'Magnus' }],
    });
    const rows: Row[] = [
      { id: 'game:1', white: 'player_name:PN_1' },
      { id: 'game:2', white: 'Hikaru' }, // pre-normalization row: not a link
      { id: 'game:3', white: 'player_name:PN_gone' }, // dangling link
    ];
    await resolveRelations(rows, [rel('white', 'white')], store);
    expect(rows[0].white).toEqual({ id: 'player_name:PN_1', name: 'Magnus' });
    expect(rows[1].white).toBe('Hikaru');
    // A dangling LINK is null, like SurrealQL's `(SELECT …)[0] AS white`.
    expect(rows[2].white).toBeNull();
  });

  it('never sends a non-record value as a correlation key', async () => {
    const seen: unknown[][] = [];
    const store: RowFetcher = {
      fetchRelation: async (req) => {
        seen.push(req.keys);
        return [];
      },
    };
    const rows: Row[] = [{ id: 'game:1', white: 'Hikaru' }, { id: 'game:2', white: 'player_name:PN_2' }];
    await resolveRelations(rows, [rel('white', 'white')], store);
    expect(seen).toEqual([['player_name:PN_2']]);
  });

  it('nulls a distinct alias when nothing resolves', async () => {
    const store = new MemStore({ player_name: [] });
    const rows: Row[] = [{ id: 'game:1', white: 'player_name:PN_gone' }];
    await resolveRelations(rows, [rel('whiteRow', 'white')], store);
    expect(rows[0].whiteRow).toBeNull();
    expect(rows[0].white).toBe('player_name:PN_gone');
  });

  it('looksLikeRecordId accepts table:id strings and RecordId shapes only', () => {
    expect(looksLikeRecordId('player_name:PN_1')).toBe(true);
    expect(looksLikeRecordId({ tb: 'player_name', id: 'PN_1' })).toBe(true);
    expect(looksLikeRecordId('Magnus')).toBe(false);
    expect(looksLikeRecordId(42)).toBe(false);
    expect(looksLikeRecordId(null)).toBe(false);
  });
});
