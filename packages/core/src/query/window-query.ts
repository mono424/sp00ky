/**
 * Rewrite a windowed (`LIMIT n START m`, m>0) SELECT so its rows are
 * materialized from an explicit record-id set — the window the SSP already
 * computed — instead of re-running the original query (with its `START m`)
 * against the shared local DB.
 *
 * Why: `DataModule.processStreamUpdate` materializes a query's rows by
 * re-querying the local in-browser SurrealDB with the original surql. For an
 * offset query that re-applies `START m` against the *shared* local store —
 * which, with sparse windowing, may hold only this window's rows — so it skips
 * them all and returns nothing (the "page 2 returns 0 rows" bug). The SSP's
 * materialized view (`StreamUpdate.localArray`) is exactly this window's row
 * ids, so we select those ids directly and re-apply the original `ORDER BY` for
 * stable display order.
 *
 * Returns `null` for non-offset queries (`START` absent or 0) — the caller
 * keeps the normal re-query path, so only the broken case changes behavior.
 *
 * Preserves the `SELECT <projection>` clause and the top-level `ORDER BY`;
 * drops the original `FROM`/`WHERE`/`LIMIT`/`START`. Subqueries inside the
 * projection are preserved verbatim (the scanner is paren/quote aware), with
 * one caveat: SurrealDB v3 drops `*` in the `SELECT *, <subquery> FROM $param`
 * shape — such windowed+subquery queries are rare and were already returning 0,
 * so this is no regression.
 */
import type { QueryPlan } from '@spooky-sync/query-builder';

/**
 * Plan-level window materialization: restrict a windowed query's base rows to
 * exactly the SSP-computed id-set, dropping `where`/`limit`/`offset` but keeping
 * `orderBy`, `select` and `relations`. The engine-neutral counterpart of
 * {@link buildWindowMaterialization} (which does the same by string surgery for
 * the raw-SurrealQL path). Returns `null` for non-offset queries so the caller
 * keeps the normal re-query path.
 */
export function buildWindowMaterializationPlan(
  plan: QueryPlan,
  ids: unknown[]
): QueryPlan | null {
  if (plan.offset === undefined || plan.offset <= 0) return null;
  return buildIdSetPlan(plan, ids);
}

/**
 * Raw-SurrealQL counterpart of {@link buildIdSetPlan}, for queries that arrived
 * without a plan. Same rewrite as {@link buildWindowMaterialization} without the
 * offset guard: keep the projection and top-level `ORDER BY`, replace the source
 * with the bound id-set.
 */
export function buildIdSetSurql(
  surql: string,
  idsParam = '__win'
): { query: string } | null {
  const kw = scanTopLevelClauses(surql);
  if (kw.fromIndex === null) return null;
  return { query: renderIdSetSelect(surql, kw, idsParam) };
}

/** Shared body of the two id-set surql rewrites: keep the projection and the
 *  top-level `ORDER BY`, replace the source with the bound id-set. */
function renderIdSetSelect(surql: string, kw: TopLevelClauses, idsParam: string): string {
  const selectClause = surql.slice(0, kw.fromIndex!).trimEnd(); // "SELECT <projection>"

  let orderBy = '';
  const orderByIndex = kw.orderByIndex;
  if (orderByIndex !== null) {
    const ends = [kw.limitIndex, kw.startIndex, kw.semicolonIndex, surql.length].filter(
      (n): n is number => n !== null && n > orderByIndex
    );
    orderBy = ' ' + surql.slice(orderByIndex, Math.min(...ends)).trim();
  }

  return `${selectClause} FROM $${idsParam}${orderBy}`;
}

/**
 * Restrict any plan's base rows to an explicit id-set, regardless of offset.
 *
 * Same rewrite as {@link buildWindowMaterializationPlan} without the offset
 * guard, for rendering a query from its authoritative membership list rather
 * than by re-running its predicate.
 *
 * `where` is dropped deliberately: the id-set already IS the answer to the
 * predicate as the server evaluated it, and re-applying the `WHERE` locally is
 * exactly the bug — a row that left the window keeps its (stale, never
 * re-fetched) local body, so it keeps matching and keeps rendering.
 * `limit`/`offset` go for the same reason: both are already baked into which ids
 * are in the set, so the set itself is the bound. (Both engines ignore them on
 * the `ids` path regardless — only `select` and `orderBy` are honored there.)
 * `orderBy` is kept so display order stays deterministic.
 */
export function buildIdSetPlan(plan: QueryPlan, ids: unknown[]): QueryPlan {
  return {
    ...plan,
    ids,
    where: undefined,
    limit: undefined,
    offset: undefined,
  };
}

export function buildWindowMaterialization(
  surql: string,
  idsParam = '__win'
): { query: string } | null {
  const kw = scanTopLevelClauses(surql);
  // `START` is only recorded once `FROM` was seen, so a positive start value
  // implies a FROM index.
  if (kw.startValue === null || kw.startValue <= 0) return null;
  return { query: renderIdSetSelect(surql, kw, idsParam) };
}

interface TopLevelClauses {
  fromIndex: number | null;
  orderByIndex: number | null;
  limitIndex: number | null;
  startIndex: number | null;
  startValue: number | null;
  semicolonIndex: number | null;
}

// Single pass over the query, tracking paren depth and single-quoted strings so
// that clauses inside subqueries (which live in parens) are ignored — only the
// outermost (depth-0) clause keywords are recorded.
function scanTopLevelClauses(sql: string): TopLevelClauses {
  const out: TopLevelClauses = {
    fromIndex: null,
    orderByIndex: null,
    limitIndex: null,
    startIndex: null,
    startValue: null,
    semicolonIndex: null,
  };
  let depth = 0;
  let inStr = false;

  for (let i = 0; i < sql.length; i++) {
    const ch = sql[i];
    if (inStr) {
      if (ch === '\\') i++; // skip escaped char
      else if (ch === "'") inStr = false;
      continue;
    }
    if (ch === "'") { inStr = true; continue; }
    if (ch === '(') { depth++; continue; }
    if (ch === ')') { depth--; continue; }
    if (depth !== 0) continue;
    if (ch === ';' && out.semicolonIndex === null) { out.semicolonIndex = i; continue; }

    // Only test keywords at a word boundary.
    if (!isWordBoundary(sql, i)) continue;
    if (out.fromIndex === null && matchKeyword(sql, i, 'FROM')) { out.fromIndex = i; continue; }
    // FROM must come before the trailing clauses; only record them once seen.
    if (out.fromIndex === null) continue;
    if (out.orderByIndex === null && matchKeyword(sql, i, 'ORDER BY')) { out.orderByIndex = i; continue; }
    if (out.limitIndex === null && matchKeyword(sql, i, 'LIMIT')) { out.limitIndex = i; continue; }
    if (out.startIndex === null && matchKeyword(sql, i, 'START')) {
      out.startIndex = i;
      out.startValue = readNumberAfter(sql, i + 'START'.length);
      continue;
    }
  }
  return out;
}

function isWordBoundary(sql: string, i: number): boolean {
  if (i === 0) return true;
  return !/[A-Za-z0-9_]/.test(sql[i - 1]);
}

// Case-insensitive keyword match where internal whitespace (e.g. "ORDER BY")
// matches any run of whitespace, and the keyword ends on a non-word char.
function matchKeyword(sql: string, i: number, keyword: string): boolean {
  const parts = keyword.split(' ');
  let pos = i;
  for (let p = 0; p < parts.length; p++) {
    const word = parts[p];
    if (sql.slice(pos, pos + word.length).toUpperCase() !== word) return false;
    pos += word.length;
    if (p < parts.length - 1) {
      const wsStart = pos;
      while (pos < sql.length && /\s/.test(sql[pos])) pos++;
      if (pos === wsStart) return false; // required whitespace
    }
  }
  // Must end on a non-word char (or end of string).
  return pos >= sql.length || !/[A-Za-z0-9_]/.test(sql[pos]);
}

function readNumberAfter(sql: string, from: number): number | null {
  let pos = from;
  while (pos < sql.length && /\s/.test(sql[pos])) pos++;
  const m = /^\d+/.exec(sql.slice(pos));
  return m ? parseInt(m[0], 10) : null;
}
