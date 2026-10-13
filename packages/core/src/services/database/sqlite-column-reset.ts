/**
 * Cached bodies outlive a schema change on the SQLite engine, and that loses
 * columns for good.
 *
 * A body is landed through `cleanRecord`, which keeps only the columns the
 * running app's schema declares, and is stored at the server's `_00_rv`. The
 * engine is schemaless (no migrator, nothing is recreated when the schema
 * changes), so a tab on an older build that fetched a row after the server
 * added and backfilled a column stored it WITHOUT that column at the new
 * version. The next build knows the column, but its local version equals the
 * server's, so the fetch plan never asks for the row again and the column
 * stays missing until something else bumps the row. (WhitePawn, 2026-10-11:
 * every puzzle read as unconverted because `layout` never arrived.)
 *
 * So the store remembers each table's column set (`_00_schema`, row
 * `columns`). On boot, before the prime reads the versions, a table whose set
 * changed, or every table when no set was recorded yet, gets its cached
 * `_00_rv` reset to 0. The bodies stay and keep rendering; any view that names
 * one of those rows sees it as stale and fetches it again, whole. Rows with an
 * unsettled local write keep their version: their echo is a newer body anyway.
 */

/** `_00_schema` row holding the recorded column sets. */
export const COLUMNS_ROW_ID = 'columns';

export interface ColumnResetPlan {
  /** Tables with cached rows whose versions are reset. */
  changed: string[];
  /** One transaction: the resets, then the new column sets. Empty when nothing changed. */
  statements: { sql: string; bind?: unknown[] }[];
}

/**
 * The column set of every table, as `cleanRecord` sees it: the sorted declared
 * column names. Plain names rather than a hash, so a stored set says what it is.
 */
export function columnFingerprints(
  tables: readonly { name: string; columns: Record<string, unknown> }[]
): Record<string, string> {
  const out: Record<string, string> = {};
  for (const t of tables) out[t.name] = Object.keys(t.columns).sort().join(',');
  return out;
}

const sameSets = (a: Record<string, string> | null, b: Record<string, string>): boolean => {
  if (!a) return false;
  const ka = Object.keys(a);
  return ka.length === Object.keys(b).length && ka.every((k) => a[k] === b[k]);
};

/**
 * What to run for the column sets `current`, given the `stored` ones (null when
 * none were recorded), the tables that exist in the store, and the encoded ids
 * (`table:id`) with an unsettled local write.
 */
export function planColumnReset(
  stored: Record<string, string> | null,
  current: Record<string, string>,
  existingTables: ReadonlySet<string>,
  keep: Iterable<string>
): ColumnResetPlan {
  if (sameSets(stored, current)) return { changed: [], statements: [] };

  const keepByTable = new Map<string, string[]>();
  for (const id of keep) {
    const table = id.slice(0, id.indexOf(':'));
    if (!keepByTable.has(table)) keepByTable.set(table, []);
    keepByTable.get(table)!.push(id);
  }

  const changed: string[] = [];
  const statements: ColumnResetPlan['statements'] = [];
  for (const [table, set] of Object.entries(current)) {
    if (stored && stored[table] === set) continue;
    if (!existingTables.has(table)) continue;
    changed.push(table);
    const quoted = `"${table.replace(/"/g, '""')}"`;
    const reset = `UPDATE ${quoted} SET data = json_set(data, '$._00_rv', 0) WHERE json_extract(data, '$._00_rv') != 0`;
    const kept = keepByTable.get(table);
    statements.push(
      kept
        ? { sql: `${reset} AND id NOT IN (SELECT value FROM json_each(?))`, bind: [JSON.stringify(kept)] }
        : { sql: reset }
    );
  }
  statements.push({
    sql: `INSERT OR REPLACE INTO "_00_schema"(id, data) VALUES (?, ?)`,
    bind: [COLUMNS_ROW_ID, JSON.stringify(current)],
  });
  return { changed, statements };
}
