import { UPSTREAM, upstreamHeaders } from './fixtures/target';

// Tables to clean — application data only.
// NOT `user` (registerUser handles "already exists").
// NOT `_00_*` (internal sync tables).
// The CRDT edit-log and presence tables go with their threads (the fold only
// drops orphans on its sweep).
const TABLES_TO_CLEAN = ['commented_on', 'comment', 'thread__crdt_log', 'thread__crdt_presence', 'thread', 'job'];

// Rows go in small batches, each retried: one `DELETE thread` over a whole run's
// threads is a long transaction (every delete fires the sync events) and kept
// losing write conflicts to the SSP's own background writes.
const BATCH = 50;
const MAX_RETRIES = 8;
const RETRY_DELAY_MS = 300;

async function execSql(sql: string): Promise<unknown[]> {
  const response = await fetch(UPSTREAM.url, {
    method: 'POST',
    headers: upstreamHeaders(),
    body: sql,
  });

  if (!response.ok) {
    const body = await response.text();
    throw new Error(`DB query failed: ${response.status} ${response.statusText}\n${body}`);
  }

  const results = await response.json();
  for (const result of results) {
    if (result.status === 'ERR') {
      throw new Error(result.result);
    }
  }
  return results.map((r: { result: unknown }) => r.result);
}

/** Delete up to `BATCH` rows of `table`; how many there were before. */
async function deleteBatch(table: string): Promise<number> {
  const [ids] = (await execSql(`SELECT VALUE id FROM ${table} LIMIT ${BATCH};`)) as unknown[][];
  if (!ids?.length) return 0;
  await execSql(`DELETE (SELECT VALUE id FROM ${table} LIMIT ${BATCH});`);
  return ids.length;
}

export async function cleanDatabase(): Promise<void> {
  for (const table of TABLES_TO_CLEAN) {
    let lastError: Error | undefined;

    for (;;) {
      let removed = 0;
      for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
        try {
          removed = await deleteBatch(table);
          lastError = undefined;
          break;
        } catch (err) {
          lastError = err as Error;
          // A table the project does not have (the outbox of a dev-only
          // backend on a cloud deployment) has nothing to clean.
          if (/does not exist/.test(lastError.message)) {
            lastError = undefined;
            break;
          }
          if (attempt < MAX_RETRIES) {
            await new Promise((r) => setTimeout(r, RETRY_DELAY_MS * attempt));
          }
        }
      }
      if (lastError || removed === 0) break;
    }

    if (lastError) {
      throw new Error(`Failed to clean table "${table}" after ${MAX_RETRIES} attempts: ${lastError.message}`);
    }
  }

  console.log(
    `[e2e:setup] Cleaned ${TABLES_TO_CLEAN.length} tables: ${TABLES_TO_CLEAN.join(', ')}`
  );
}
