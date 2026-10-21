import { test, expect, uniqueUser, registerUser } from '../fixtures/test-fixtures';
import { UPSTREAM, upstreamHeaders } from '../fixtures/target';

// Regression guard for #40: when a user record is DELETEd upstream,
// the schema's `_00_user_delete` event fires the SSP ingest with
// op=DELETE, and `ingest_handler` should drop that user's dedicated
// `_00_list_ref_user_<id>` table. Without the cleanup, dedicated
// tables accumulate forever, and the `INFO FOR DB` output (plus
// every cluster snapshot) keeps growing.

const ROOT_HEADERS = upstreamHeaders();

async function rootQuery<T = unknown>(body: string): Promise<T> {
  const resp = await fetch(UPSTREAM.url, {
    method: 'POST',
    headers: ROOT_HEADERS,
    body,
  });
  return (await resp.json()) as T;
}

async function listRefTablesForUserSuffix(suffix: string): Promise<boolean> {
  const result = await rootQuery<Array<{ result?: { tables?: Record<string, unknown> } }>>(
    'INFO FOR DB',
  );
  const tables = result?.[0]?.result?.tables ?? {};
  return Object.prototype.hasOwnProperty.call(
    tables,
    `_00_list_ref_user_${suffix}`,
  );
}

async function userIdByUsername(username: string): Promise<string> {
  const result = await rootQuery<
    Array<{ result?: Array<{ id?: { id?: string } | string }> }>
  >(`SELECT id FROM user WHERE username = '${username}'`);
  const row = result?.[0]?.result?.[0];
  if (!row?.id) throw new Error(`user ${username} not found upstream`);
  // SurrealDB returns id either as {tb,id} or "user:abc" string depending on
  // the HTTP /sql codec mode. Normalize to the raw id segment.
  if (typeof row.id === 'string') {
    return row.id.startsWith('user:') ? row.id.slice('user:'.length) : row.id;
  }
  return String(row.id.id ?? '');
}

test('deleting a user drops only that user\'s _00_list_ref_user_<id> table', async ({
  browser,
}) => {
  const alice = uniqueUser('gc_a');
  const bob = uniqueUser('gc_b');

  const ctxA = await browser.newContext();
  const ctxB = await browser.newContext();
  const pageA = await ctxA.newPage();
  const pageB = await ctxB.newPage();

  try {
    await registerUser(pageA, alice);
    await registerUser(pageB, bob);

    const aliceId = await userIdByUsername(alice.username);
    const bobId = await userIdByUsername(bob.username);

    // Pre-emptive ensure_user_tables (#41) should have already
    // created both tables by the time auth finished, so assert both
    // exist before we proceed.
    await expect.poll(() => listRefTablesForUserSuffix(aliceId), { timeout: 5_000 }).toBe(true);
    await expect.poll(() => listRefTablesForUserSuffix(bobId), { timeout: 5_000 }).toBe(true);

    // Delete alice as root. (The UI's "delete account" flow doesn't
    // exist; root-auth is the deterministic path.)
    await rootQuery(`DELETE user:${aliceId}`);

    // The SSP receives `op=DELETE` via the `_00_user_delete` schema
    // event's `http::post` and runs `drop_user_tables` async; poll
    // upstream until alice's table is gone, and confirm bob's is
    // untouched.
    await expect
      .poll(() => listRefTablesForUserSuffix(aliceId), { timeout: 10_000 })
      .toBe(false);
    expect(await listRefTablesForUserSuffix(bobId)).toBe(true);
  } finally {
    await ctxA.close();
    await ctxB.close();
  }
});
