import type { WhereNode } from '@spooky-sync/query-builder';
import type { OrderBy, Row } from './cache-engine';
import { stableKey } from './relation-resolver';

/**
 * Translate the BOUNDED set of SurrealQL statements the client actually emits
 * into engine-neutral operations the SQLite backend executes. This is NOT a
 * general SurrealQL parser — it recognizes exactly the shapes produced by the
 * `surql` helper and the handful of literal queries in the codebase. Anything
 * unrecognized throws (with the offending SQL) so gaps surface loudly during a
 * trial rather than silently returning wrong data.
 *
 * The arbitrary user SELECT (`config.surql`, with nested `.related()`
 * subqueries) is intentionally NOT handled here — it flows through
 * `engine.select(plan)` instead, so the translator never faces an unbounded
 * query shape.
 */

export type SqlOp =
  | { kind: 'getById'; id: unknown; select?: string[]; value?: string }
  | {
      kind: 'selectByIds';
      ids: unknown[];
      select?: string[];
      orderBy?: OrderBy;
      value?: string;
      limit?: number;
      start?: number;
    }
  | {
      kind: 'selectTable';
      table: string;
      where?: WhereNode[];
      orderBy?: OrderBy;
      select?: string[];
      value?: string;
      limit?: number;
      start?: number;
    }
  /** `SELECT count() FROM t [WHERE …] GROUP ALL` — one `{ count }` row. */
  | { kind: 'count'; table: string; where?: WhereNode[] }
  /** `INFO FOR DB` — the table list, in SurrealDB's `{ tables: {…} }` shape. */
  | { kind: 'infoForDb' }
  | { kind: 'upsert'; id: unknown; data: Row; mode: 'replace' | 'merge' }
  | { kind: 'updateSet'; id: unknown; sets: SetClause[]; returnNone: boolean }
  | { kind: 'delete'; id: unknown }
  | { kind: 'deleteAll'; table: string }
  | { kind: 'let'; var: string; inner: SqlOp }
  | { kind: 'return'; entries: { key: string; var: string }[] }
  | { kind: 'noop' };

export interface SetClause {
  path: string;
  op: '=' | '+=' | '-=';
  value: unknown;
}

export interface TranslatedQuery {
  /** True for a `BEGIN TRANSACTION … COMMIT` block — the engine prepends a
   *  `null` begin-result so `surql.seal`'s `idx+1` extraction still lines up. */
  transaction: boolean;
  ops: SqlOp[];
}

const rid = (v: unknown): unknown => v;

/** Table name from a record id value (`table:id` string or RecordId). */
export function tableOf(id: unknown): string {
  return stableKey(id).split(':')[0];
}

export function translateSurql(sql: string, vars: Record<string, unknown>): TranslatedQuery {
  const trimmed = sql.trim().replace(/;\s*$/, '');

  if (/^BEGIN\s+TRANSACTION/i.test(trimmed)) {
    const inner = trimmed
      .replace(/^BEGIN\s+TRANSACTION\s*;?/i, '')
      .replace(/;?\s*COMMIT\s+TRANSACTION\s*$/i, '');
    const ops = splitStatements(inner).map((s) => translateStatement(s, vars));
    return { transaction: true, ops };
  }

  // A plain multi-statement string (e.g. `DEFINE DATABASE x; USE DB x`) — split
  // and translate each. Single statement is just the one-element case.
  const parts = splitStatements(trimmed);
  return { transaction: false, ops: parts.map((s) => translateStatement(s, vars)) };
}

function splitStatements(block: string): string[] {
  // Statements in the client's tx blocks never contain a bare `;` inside a
  // string/paren, so a simple split is sufficient for this bounded vocabulary.
  return block
    .split(';')
    .map((s) => s.trim())
    .filter(Boolean);
}

function translateStatement(stmt: string, vars: Record<string, unknown>): SqlOp {
  const s = stmt.trim();

  // LET $var = ( <inner statement> ) — bind the inner result into query scope.
  let mLet = /^LET\s+\$(\w+)\s*=\s*\(([\s\S]+)\)$/i.exec(s);
  if (mLet) {
    return { kind: 'let', var: mLet[1], inner: translateStatement(mLet[2].trim(), vars) };
  }

  // RETURN { key: $var, ... } — build an object from scope/vars.
  let mRet = /^RETURN\s+\{([\s\S]+)\}$/i.exec(s);
  if (mRet) {
    const entries = splitTopLevel(mRet[1], ',').map((pair) => {
      const c = pair.indexOf(':');
      const key = pair.slice(0, c).trim();
      const v = pair.slice(c + 1).trim();
      return { key, var: v.startsWith('$') ? v.slice(1) : v };
    });
    return { kind: 'return', entries };
  }

  // `INFO FOR DB` is the ONE INFO statement with a real answer here: the
  // DevTools Database explorer enumerates tables with it. Answered from
  // `sqlite_master` (see the engine) instead of being swallowed by the DDL
  // noop below, which left the explorer with an empty table list.
  if (/^INFO\s+FOR\s+DB(\s+STRUCTURE)?$/i.test(s)) return { kind: 'infoForDb' };

  // ---- schema / session DDL: no-ops on a schemaless engine --------------
  // SQLite creates tables lazily and has no namespaces/DB DDL, so DEFINE /
  // REMOVE / USE / INFO / RETURN statements are safely ignored.
  if (/^(DEFINE|REMOVE|USE|INFO|RETURN|CANCEL|COMMIT|BEGIN)\b/i.test(s)) {
    return { kind: 'noop' };
  }

  // ---- writes -----------------------------------------------------------
  let m =
    /^CREATE\s+ONLY\s+\$(\w+)\s+CONTENT\s+\$(\w+)$/i.exec(s) ||
    /^UPSERT\s+ONLY\s+\$(\w+)\s+REPLACE\s+\$(\w+)$/i.exec(s);
  if (m) {
    return { kind: 'upsert', id: rid(vars[m[1]]), data: asRow(vars[m[2]]), mode: 'replace' };
  }

  // The `ONLY` variants come from `surql`; the bare `UPDATE <id> MERGE $x` form
  // (with a LITERAL record id) is what the DevTools row editor emits.
  m =
    idRe(String.raw`^UPSERT\s+ONLY\s+%ID%\s+MERGE\s+\$(\w+)$`).exec(s) ||
    idRe(String.raw`^UPDATE\s+(?:ONLY\s+)?%ID%\s+MERGE\s+\$(\w+)$`).exec(s);
  if (m) {
    return { kind: 'upsert', id: idOperand(m[1], vars), data: asRow(vars[m[2]]), mode: 'merge' };
  }

  // CREATE ONLY $id SET a = ..., b = ...  (createSet / createMutation)
  m = /^CREATE\s+ONLY\s+\$(\w+)\s+SET\s+(.+)$/i.exec(s);
  if (m) {
    const data: Row = {};
    for (const { path, value } of parseSetClauses(m[2], vars)) setPath(data, path, value);
    return { kind: 'upsert', id: rid(vars[m[1]]), data, mode: 'replace' };
  }

  // UPDATE $id SET a.b = $x, c = $y [RETURN NONE]
  m = idRe(String.raw`^UPDATE\s+%ID%\s+SET\s+(.+?)(\s+RETURN\s+NONE)?$`).exec(s);
  if (m) {
    return {
      kind: 'updateSet',
      id: idOperand(m[1], vars),
      sets: parseSetClauses(m[2], vars),
      returnNone: !!m[3],
    };
  }

  m = /^DELETE\s+([A-Za-z_]\w*)$/i.exec(s); // DELETE <table> (no `:` — a table)
  if (m) return { kind: 'deleteAll', table: m[1] };

  // DELETE $id, and `DELETE <table>:<id>` from the DevTools row actions.
  m = idRe(String.raw`^DELETE\s+%ID%$`).exec(s);
  if (m) return { kind: 'delete', id: idOperand(m[1], vars) };

  // ---- reads ------------------------------------------------------------
  // The paging tail (`LIMIT n [START m]`) is peeled off first so it can't be
  // swallowed by the lazily-matched WHERE/ORDER BY groups below. Everything
  // after this point sees a window-free statement.
  const { head, limit, start } = peelWindow(s, vars);

  // SELECT count() FROM <table> [WHERE ...] GROUP ALL — the row-count query the
  // DevTools Database explorer pages with. GROUP ALL is required: without it
  // SurrealDB counts per row, which is a different result this can't fake.
  m = /^SELECT\s+count\(\)\s+FROM\s+([A-Za-z_]\w*)(?:\s+WHERE\s+(.+?))?\s+GROUP\s+ALL$/i.exec(head);
  if (m) {
    return { kind: 'count', table: m[1], where: m[2] ? parseWhere(m[2], vars) : undefined };
  }

  // SELECT [VALUE] <proj> FROM ONLY <id>
  m = idRe(String.raw`^SELECT\s+(VALUE\s+)?(.+?)\s+FROM\s+ONLY\s+%ID%$`).exec(head);
  if (m) {
    const value = m[1] ? m[2].trim() : undefined;
    return { kind: 'getById', id: idOperand(m[3], vars), select: projFields(m[2], !!m[1]), value };
  }

  // SELECT [VALUE] <proj> FROM $arrayParam, or FROM (SELECT * FROM $arrayParam)
  // (the id-set read's shape for a field list, see `idSetSource`).
  m = /^SELECT\s+(VALUE\s+)?(.+?)\s+FROM\s+(?:\$(\w+)|\(\s*SELECT\s+\*\s+FROM\s+\$(\w+)\s*\))$/i.exec(head);
  if (m) {
    const value = m[1] ? m[2].trim() : undefined;
    return {
      kind: 'selectByIds',
      ids: (vars[m[3] ?? m[4]] as unknown[]) ?? [],
      select: projFields(m[2], !!m[1]),
      value,
      limit,
      start,
    };
  }

  // SELECT <proj> FROM <table> [WHERE ...] [ORDER BY ...]
  m =
    /^SELECT\s+(VALUE\s+)?(.+?)\s+FROM\s+([A-Za-z_]\w*)(?:\s+WHERE\s+(.+?))?(?:\s+ORDER\s+BY\s+(.+?))?$/i.exec(
      head
    );
  if (m) {
    return {
      kind: 'selectTable',
      table: m[3],
      where: m[4] ? parseWhere(m[4], vars) : undefined,
      orderBy: m[5] ? parseOrderBy(m[5]) : undefined,
      select: projFields(m[2], !!m[1]),
      value: m[1] ? m[2].trim() : undefined,
      limit,
      start,
    };
  }

  throw new Error(`SqliteCacheEngine: unsupported SurrealQL for translation: ${stmt}`);
}

// ==================== clause parsers ====================

/**
 * Split a SELECT's trailing `LIMIT n [START m]` off its head.
 *
 * Peeled rather than folded into the SELECT regexes because those match WHERE
 * and ORDER BY lazily: with the window left in place the WHERE group happily
 * expands over `… LIMIT 20`, which is how `SELECT * FROM t LIMIT 20 START 0`
 * ended up unmatched entirely.
 *
 * A tail sitting inside a string literal (`WHERE note = 'LIMIT 5'`) is left
 * alone — an odd quote count in the head means the match is inside a string.
 */
function peelWindow(
  stmt: string,
  vars: Record<string, unknown>
): { head: string; limit?: number; start?: number } {
  let head = stmt;
  let limit: number | undefined;
  let start: number | undefined;

  const peel = (re: RegExp): string | undefined => {
    const m = re.exec(head);
    if (!m) return undefined;
    const before = head.slice(0, m.index);
    if (countQuotes(before) % 2 !== 0) return undefined;
    head = before;
    return m[1];
  };

  // START is the outer token, so it comes off first.
  const startTok = peel(/\s+START(?:\s+AT)?\s+(\$?\w+)\s*$/i);
  const limitTok = peel(/\s+LIMIT(?:\s+BY)?\s+(\$?\w+)\s*$/i);
  if (startTok !== undefined) start = numOperand(startTok, vars);
  if (limitTok !== undefined) limit = numOperand(limitTok, vars);
  return { head, limit, start };
}

function countQuotes(s: string): number {
  let n = 0;
  for (let i = 0; i < s.length; i++) {
    if (s[i] === "'" && s[i - 1] !== '\\') n++;
  }
  return n;
}

/** A `LIMIT`/`START` operand: a literal integer or a `$var` holding one. */
function numOperand(token: string, vars: Record<string, unknown>): number | undefined {
  const raw = token.startsWith('$') ? vars[token.slice(1)] : token;
  const n = Number(raw);
  return Number.isFinite(n) ? n : undefined;
}

/**
 * A single-record operand: `$var` or a LITERAL `table:id`.
 *
 * A bare identifier deliberately does NOT match: in SurrealQL that is a table,
 * and `UPDATE game MERGE $x` rewrites every row of `game` — a shape this
 * vocabulary does not implement. Not matching means such a statement reaches
 * the "unsupported SurrealQL" throw instead of quietly writing one row called
 * `game`.
 */
const ID_TOKEN = String.raw`(\$\w+|[A-Za-z_]\w*:\S+)`;

/** Build a statement regex, `%ID%` expanding to {@link ID_TOKEN}. */
function idRe(pattern: string): RegExp {
  return new RegExp(pattern.replace('%ID%', ID_TOKEN), 'i');
}

/**
 * Resolve an {@link ID_TOKEN} match. Literals matter because the DevTools row
 * editor inlines the id (`UPDATE game:abc MERGE $updates`) rather than binding
 * it; `stableKey` treats that string and a `RecordId` identically downstream.
 */
function idOperand(token: string, vars: Record<string, unknown>): unknown {
  const t = token.trim();
  return t.startsWith('$') ? rid(vars[t.slice(1)]) : t;
}

function projFields(proj: string, isValue: boolean): string[] | undefined {
  const p = proj.trim();
  if (isValue) return undefined; // VALUE returns a scalar, no projection object
  if (p === '*') return undefined;
  return p.split(',').map((f) => f.trim());
}

function asRow(v: unknown): Row {
  return (v && typeof v === 'object' ? v : {}) as Row;
}

/** Parse `a = $x, b.c = 'lit', _00_rv += 1` into path/op/value triples. */
function parseSetClauses(clause: string, vars: Record<string, unknown>): SetClause[] {
  return splitTopLevel(clause, ',').map((part) => {
    const m = /^(.+?)\s*(\+=|-=|=)\s*([\s\S]+)$/.exec(part.trim());
    if (!m) throw new Error(`SqliteCacheEngine: cannot parse SET clause: ${part}`);
    return { path: m[1].trim(), op: m[2] as SetClause['op'], value: literalOrVar(m[3], vars) };
  });
}

function literalOrVar(token: string, vars: Record<string, unknown>): unknown {
  const t = token.trim();
  if (t.startsWith('$')) return vars[t.slice(1)];
  if (/^'.*'$/.test(t) || /^".*"$/.test(t)) return t.slice(1, -1);
  if (/^-?\d+(\.\d+)?$/.test(t)) return Number(t);
  if (t === 'true') return true;
  if (t === 'false') return false;
  if (t === 'NONE' || t === 'NULL' || t === 'null') return null;
  return t;
}

function parseWhere(clause: string, vars: Record<string, unknown>): WhereNode[] {
  // Only simple AND-of-equality/comparison is emitted by the client's raw
  // queries; OR/nested go through plan-based select().
  return splitTopLevel(clause, 'AND').map((cond) => {
    const m = /^(\S+)\s*(=|!=|>=|<=|>|<)\s*(.+)$/.exec(cond.trim());
    if (!m) throw new Error(`SqliteCacheEngine: cannot parse WHERE condition: ${cond}`);
    return { field: m[1], op: m[2] as WhereComparisonOp, value: literalOrVar(m[3], vars) };
  });
}

type WhereComparisonOp = '=' | '!=' | '>=' | '<=' | '>' | '<';

function parseOrderBy(clause: string): OrderBy {
  return clause.split(',').map((c) => {
    const [f, dir] = c.trim().split(/\s+/);
    return [f, (dir ?? 'asc').toLowerCase() === 'desc' ? 'desc' : 'asc'];
  });
}

/** Split on a top-level separator, ignoring parens/quotes (bounded inputs). */
function splitTopLevel(input: string, sep: string): string[] {
  const out: string[] = [];
  let depth = 0;
  let inStr: string | null = null;
  let cur = '';
  const isWord = /[A-Za-z]/.test(sep);
  for (let i = 0; i < input.length; i++) {
    const ch = input[i];
    if (inStr) {
      cur += ch;
      if (ch === inStr) inStr = null;
      continue;
    }
    if (ch === "'" || ch === '"') {
      inStr = ch;
      cur += ch;
      continue;
    }
    if (ch === '(') depth++;
    if (ch === ')') depth--;
    const matches = isWord
      ? depth === 0 && input.slice(i, i + sep.length).toUpperCase() === sep && /\s/.test(input[i - 1] ?? ' ')
      : depth === 0 && ch === sep;
    if (matches) {
      out.push(cur.trim());
      cur = '';
      i += sep.length - 1;
      continue;
    }
    cur += ch;
  }
  if (cur.trim()) out.push(cur.trim());
  return out;
}

/** Read a possibly-dotted path (`a.b.c`) from an object. */
export function getPath(obj: Row, path: string): unknown {
  let cur: unknown = obj;
  for (const k of path.split('.')) {
    if (cur == null || typeof cur !== 'object') return undefined;
    cur = (cur as Row)[k];
  }
  return cur;
}

/** Set a possibly-dotted path (`a.b.c`) on an object. */
export function setPath(obj: Row, path: string, value: unknown): void {
  const parts = path.split('.');
  let cur: Row = obj;
  for (let i = 0; i < parts.length - 1; i++) {
    const k = parts[i];
    if (typeof cur[k] !== 'object' || cur[k] === null) cur[k] = {};
    cur = cur[k] as Row;
  }
  cur[parts[parts.length - 1]] = value;
}
