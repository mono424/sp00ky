import type { QueryPlan } from '@spooky-sync/query-builder';
import { LocalDatabaseService } from './local';
import { resolveRelations } from './relation-resolver';
import {
  renderBaseSelectSurql,
  renderRelationFetchSurql,
} from './plan-render';
import type {
  EngineTx,
  Id,
  LocalCacheEngine,
  OrderBy,
  RelationFetch,
  Row,
} from './cache-engine';
import { stableKey } from './relation-resolver';
import { surql } from '../../utils/surql';
import { RecordId } from 'surrealdb';

/**
 * Default local cache backend: the in-browser SurrealDB-WASM store. Implemented
 * as a subclass of {@link LocalDatabaseService} so it remains a 100% drop-in for
 * every existing `this.local.query(...)` / `execute` / `switchStore` / `epoch`
 * call site (zero behavior change), while adding the engine-neutral verb surface
 * (`select`/`selectByIds`/`getById`/CRUD) used by the pluggable path.
 *
 * Relations are resolved with the SAME shared {@link resolveRelations} as the
 * SQLite backend, so the two engines decompose `.related()` identically.
 */
export class SurrealCacheEngine extends LocalDatabaseService implements LocalCacheEngine {
  /** SurrealDB needs its SurrealQL schema provisioned locally. */
  readonly usesSurqlSchema = true;

  readonly engineKind = 'surrealdb' as const;

  /** {@link LocalCacheEngine} alias for {@link LocalDatabaseService.switchStore}. */
  switchBucket(bucketId: string): Promise<void> {
    return this.switchStore(bucketId);
  }

  async fetchRelation(req: RelationFetch): Promise<Row[]> {
    const { sql, vars } = renderRelationFetchSurql(req);
    const [rows] = await this.query<[Row[]]>(sql, vars);
    return rows ?? [];
  }

  async select(plan: QueryPlan, params: Record<string, unknown> = {}): Promise<Row[]> {
    // Window materialization: base rows are exactly `plan.ids`, ordered.
    if (plan.ids) {
      const result = await this.selectByIds(plan.table, plan.ids, {
        select: plan.select,
        orderBy: plan.orderBy,
      });
      await resolveRelations(result, plan.relations, this);
      return result;
    }
    const { sql, vars } = renderBaseSelectSurql(plan, params);
    const [rows] = await this.query<[Row[]]>(sql, vars);
    const result = rows ?? [];
    await resolveRelations(result, plan.relations, this);
    return result;
  }

  async selectByIds(
    table: string,
    ids: Id[],
    opts?: { select?: string[]; orderBy?: OrderBy }
  ): Promise<Row[]> {
    if (ids.length === 0) return [];
    const projection =
      opts?.select && opts.select.length > 0 ? ['id', ...opts.select].join(', ') : '*';
    // As documents: over bare record ids the wasm engine throws on a field
    // list and ignores ORDER BY (see `idSetSource`).
    let sql = `SELECT ${projection} FROM (SELECT * FROM $__ids)`;
    if (opts?.orderBy && opts.orderBy.length > 0) {
      sql += ` ORDER BY ${opts.orderBy.map(([f, d]) => `${f} ${d}`).join(', ')}`;
    }
    const [rows] = await this.query<[Row[]]>(`${sql};`, { __ids: ids });
    const result = rows ?? [];
    // Preserve caller's id order when no explicit ORDER BY (SurrealDB does not
    // guarantee `FROM $ids` order).
    if (!opts?.orderBy || opts.orderBy.length === 0) {
      const pos = new Map(ids.map((id, i) => [stableKey(id), i]));
      result.sort(
        (a, b) => (pos.get(stableKey(a.id)) ?? 0) - (pos.get(stableKey(b.id)) ?? 0)
      );
    }
    return result;
  }

  /**
   * Coerce the contract's `Id` (a RecordId, a stable `table:id` string, or a
   * bare id + the verb's `table` param) to a real RecordId. SurrealDB binds
   * `$__id` verbatim: a plain string makes `FROM ONLY $__id` "select" the
   * string itself (a truthy non-row) and `UPSERT $__id` an InternalError, so
   * string ids silently broke every id-verb on this engine.
   */
  private toRecordId(table: string, id: Id): unknown {
    if (typeof id !== 'string') return id;
    const raw = id.startsWith(`${table}:`) ? id.slice(table.length + 1) : id;
    return new RecordId(table, raw);
  }

  /**
   * `(id, _00_rv)` of every row per table. Runs on the main thread against the
   * embedded engine (no worker here), which is why the circuit prime caps how
   * much it is willing to read through this path.
   */
  async scanVersions(tables: string[]): Promise<Record<string, [string, number][]>> {
    const out: Record<string, [string, number][]> = {};
    for (const table of tables) {
      try {
        const [rows] = await this.query<[{ id: Id; _00_rv?: unknown }[]]>(
          `SELECT id, _00_rv FROM ${table};`
        );
        out[table] = (rows ?? []).map((r) => [stableKey(r.id), Number(r._00_rv) || 0]);
      } catch {
        out[table] = [];
      }
    }
    return out;
  }

  async getById(table: string, id: Id): Promise<Row | null> {
    const [row] = await this.query<[Row | null]>('SELECT * FROM ONLY $__id;', {
      __id: this.toRecordId(table, id),
    });
    // `FROM ONLY <non-record>` echoes the value back; only a real row counts.
    return row && typeof row === 'object' ? row : null;
  }

  async upsert(table: string, id: Id, data: Row, mode: 'replace' | 'merge'): Promise<void> {
    const sql = mode === 'merge' ? surql.upsertMerge('__id', '__data') : surql.upsert('__id', '__data');
    await this.query(surql.seal(sql), { __id: this.toRecordId(table, id), __data: data });
  }

  async patch(table: string, id: Id, patches: unknown[]): Promise<void> {
    await this.query(surql.seal('UPDATE ONLY $__id PATCH $__patches'), {
      __id: this.toRecordId(table, id),
      __patches: patches,
    });
  }

  async delete(table: string, id: Id): Promise<void> {
    await this.query(surql.seal(surql.delete('__id')), { __id: this.toRecordId(table, id) });
  }

  /**
   * Serialized (not strictly atomic) transaction: verbs run in order on the
   * same serialized query queue. The SurrealDB-WASM store already funnels every
   * `query()` through one queue, so these never interleave with other work;
   * true multi-statement atomicity is not required by the current call sites
   * (single-record CRDT / mutation writes).
   */
  async transaction<T>(fn: (tx: EngineTx) => Promise<T>): Promise<T> {
    const tx: EngineTx = {
      upsert: (t, id, data, mode) => this.upsert(t, id, data, mode),
      patch: (t, id, patches) => this.patch(t, id, patches),
      delete: (t, id) => this.delete(t, id),
    };
    return fn(tx);
  }
}
