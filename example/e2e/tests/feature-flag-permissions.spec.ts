import { test, expect } from '@playwright/test';

/**
 * Permission negative tests for the built-in feature flag tables.
 *
 * The two system tables added by `apps/cli/src/meta_tables_remote.surql`
 * are the foundation of the feature flag system, so their permission
 * guarantees are load-bearing:
 *
 *   _00_feature_flag   - readable/updatable only by users listed in
 *                        _00_admin; invisible to everyone else, so
 *                        targeting rules never reach an ordinary client.
 *   _00_user_feature   - select scoped to WHERE user = $auth.id;
 *                        create/update/delete open only to admins, so a
 *                        client cannot self-enable a flag.
 *   _00_admin          - the roster itself. Root-written only (`spky
 *                        admin`), self-scoped on select, so nobody can
 *                        promote themselves or enumerate the operators.
 *
 * The admin path exists for the DevTools Access tab. It is deliberately
 * exercised here too: the interesting property isn't "an admin can", it's
 * that everything stays shut for everyone else.
 *
 * These tests run against the upstream SurrealDB directly (the same
 * pattern as `waitForThreadInUpstream` in `test-fixtures.ts`).
 */

const SURREAL_URL = 'http://localhost:8666';
const NS = 'main';
const DB = 'example';
const ROOT_AUTH = 'Basic ' + Buffer.from('root:root').toString('base64');

async function rootSql(query: string): Promise<unknown> {
  const resp = await fetch(`${SURREAL_URL}/sql`, {
    method: 'POST',
    headers: {
      Accept: 'application/json',
      'surreal-ns': NS,
      'surreal-db': DB,
      Authorization: ROOT_AUTH,
    },
    body: query,
  });
  expect(resp.ok, `root SQL failed: ${resp.status}`).toBeTruthy();
  return resp.json();
}

async function signupUser(
  username: string,
  password: string
): Promise<string> {
  const resp = await fetch(`${SURREAL_URL}/signup`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', Accept: 'application/json' },
    body: JSON.stringify({
      ns: NS,
      db: DB,
      ac: 'account',
      username,
      password,
    }),
  });
  expect(resp.ok, `signup failed: ${resp.status}`).toBeTruthy();
  const body = (await resp.json()) as { token?: string };
  expect(body.token, 'signup did not return a token').toBeTruthy();
  return body.token as string;
}

async function recordSql(token: string, query: string): Promise<{
  status: number;
  body: Array<{ status: string; result?: unknown }>;
}> {
  const resp = await fetch(`${SURREAL_URL}/sql`, {
    method: 'POST',
    headers: {
      Accept: 'application/json',
      'surreal-ns': NS,
      'surreal-db': DB,
      Authorization: `Bearer ${token}`,
    },
    body: query,
  });
  const status = resp.status;
  const body = (await resp.json()) as Array<{
    status: string;
    result?: unknown;
  }>;
  return { status, body };
}

function uniq(prefix: string) {
  return `${prefix}_${Date.now().toString(36)}${Math.random()
    .toString(36)
    .slice(2, 6)}`;
}

test.describe.serial('Feature flag permissions', () => {
  let aliceToken = '';
  let bobToken = '';
  let aliceId = '';
  let bobId = '';
  const flagKey = uniq('e2e_flag');

  test.beforeAll(async () => {
    const aliceName = uniq('alice');
    const bobName = uniq('bob');
    aliceToken = await signupUser(aliceName, 'pw_alice_1234');
    bobToken = await signupUser(bobName, 'pw_bob_1234');

    const aliceLookup = (await rootSql(
      `SELECT VALUE id FROM user WHERE username = '${aliceName}' LIMIT 1;`
    )) as Array<{ result?: string[] }>;
    aliceId = aliceLookup[0]?.result?.[0] ?? '';
    const bobLookup = (await rootSql(
      `SELECT VALUE id FROM user WHERE username = '${bobName}' LIMIT 1;`
    )) as Array<{ result?: string[] }>;
    bobId = bobLookup[0]?.result?.[0] ?? '';
    expect(aliceId, 'failed to resolve alice id').toBeTruthy();
    expect(bobId, 'failed to resolve bob id').toBeTruthy();
  });

  test.afterAll(async () => {
    if (flagKey) {
      await rootSql(`DELETE _00_feature_flag WHERE key = '${flagKey}';`);
      await rootSql(`DELETE _00_user_feature WHERE key = '${flagKey}';`);
    }
    // Never leave a test user on the operator roster.
    if (aliceId) await rootSql(`DELETE _00_admin WHERE user = ${aliceId};`);
    if (bobId) await rootSql(`DELETE _00_admin WHERE user = ${bobId};`);
  });

  test('record-token client cannot CREATE _00_user_feature', async () => {
    const { body } = await recordSql(
      aliceToken,
      `CREATE _00_user_feature SET user = ${aliceId}, key = '${flagKey}', variant = 'on';`
    );
    const statuses = body.map((r) => r.status);
    expect(
      statuses.every((s) => s === 'ERR') ||
        body.every((r) => Array.isArray(r.result) && r.result.length === 0),
      `expected denial, got: ${JSON.stringify(body)}`
    ).toBeTruthy();

    const after = (await rootSql(
      `SELECT count() AS n FROM _00_user_feature WHERE key = '${flagKey}' GROUP ALL;`
    )) as Array<{ result?: Array<{ n: number }> }>;
    const n = after[0]?.result?.[0]?.n ?? 0;
    expect(n, 'self-enable mutation must not have persisted any row').toBe(0);
  });

  test('record-token client cannot UPDATE _00_feature_flag', async () => {
    await rootSql(
      `CREATE _00_feature_flag SET key = '${flagKey}', variants = ['off','on'], default_variant = 'off', rules = [], enabled = true;`
    );

    const { body } = await recordSql(
      aliceToken,
      `UPDATE _00_feature_flag SET enabled = false WHERE key = '${flagKey}';`
    );
    const statuses = body.map((r) => r.status);
    expect(
      statuses.every((s) => s === 'ERR') ||
        body.every((r) => Array.isArray(r.result) && r.result.length === 0),
      `expected denial, got: ${JSON.stringify(body)}`
    ).toBeTruthy();

    const after = (await rootSql(
      `SELECT VALUE enabled FROM _00_feature_flag WHERE key = '${flagKey}';`
    )) as Array<{ result?: boolean[] }>;
    expect(after[0]?.result?.[0], 'flag.enabled must not have changed').toBe(true);
  });

  test('record-token client cannot SELECT _00_feature_flag', async () => {
    const { body } = await recordSql(
      aliceToken,
      `SELECT * FROM _00_feature_flag WHERE key = '${flagKey}';`
    );
    const rows = (body[0]?.result ?? []) as unknown[];
    expect(
      rows.length,
      'targeting rules must not be visible to record-token clients'
    ).toBe(0);
  });

  test('record-token client only sees its own _00_user_feature rows', async () => {
    await rootSql(
      `UPSERT _00_user_feature SET user = ${aliceId}, key = '${flagKey}', variant = 'on' WHERE user = ${aliceId} AND key = '${flagKey}';`
    );
    await rootSql(
      `UPSERT _00_user_feature SET user = ${bobId}, key = '${flagKey}', variant = 'off' WHERE user = ${bobId} AND key = '${flagKey}';`
    );

    const { body: aliceBody } = await recordSql(
      aliceToken,
      `SELECT user, variant FROM _00_user_feature WHERE key = '${flagKey}';`
    );
    const aliceRows = (aliceBody[0]?.result ?? []) as Array<{
      user?: string;
      variant?: string;
    }>;
    expect(aliceRows.length, 'alice sees exactly one row').toBe(1);
    expect(aliceRows[0]?.variant).toBe('on');

    const { body: bobBody } = await recordSql(
      bobToken,
      `SELECT user, variant FROM _00_user_feature WHERE key = '${flagKey}';`
    );
    const bobRows = (bobBody[0]?.result ?? []) as Array<{
      user?: string;
      variant?: string;
    }>;
    expect(bobRows.length, 'bob sees exactly one row').toBe(1);
    expect(bobRows[0]?.variant).toBe('off');
  });

  // ---- _00_admin -----------------------------------------------------

  test('record-token client cannot see or join the admin roster', async () => {
    await rootSql(`UPSERT _00_admin SET user = ${aliceId} WHERE user = ${aliceId};`);

    // Bob is not an admin: the roster must be invisible to him, otherwise he
    // learns who the operators are.
    const { body: seen } = await recordSql(bobToken, `SELECT * FROM _00_admin;`);
    expect(
      ((seen[0]?.result ?? []) as unknown[]).length,
      'the admin roster must not be enumerable by a non-admin'
    ).toBe(0);

    // Self-promotion is the whole threat model for this table.
    await recordSql(bobToken, `CREATE _00_admin SET user = $auth.id;`);
    const after = (await rootSql(
      `SELECT count() AS n FROM _00_admin WHERE user = ${bobId} GROUP ALL;`
    )) as Array<{ result?: Array<{ n: number }> }>;
    expect(after[0]?.result?.[0]?.n ?? 0, 'bob promoted himself to admin').toBe(0);

    // Alice IS an admin, and still only ever sees her own row.
    const { body: own } = await recordSql(aliceToken, `SELECT user FROM _00_admin;`);
    const ownRows = (own[0]?.result ?? []) as Array<{ user?: string }>;
    expect(ownRows.length, 'an admin sees exactly their own roster row').toBe(1);
  });

  test('an admin can read and flip flags; a non-admin still cannot', async () => {
    await rootSql(`DELETE _00_feature_flag WHERE key = '${flagKey}';`);
    await rootSql(
      `CREATE _00_feature_flag SET key = '${flagKey}', variants = ['off','on'], default_variant = 'off', rules = [], enabled = true;`
    );
    await rootSql(`UPSERT _00_admin SET user = ${aliceId} WHERE user = ${aliceId};`);

    // Read: visible to the admin, still invisible to bob.
    const { body: aliceSees } = await recordSql(
      aliceToken,
      `SELECT key FROM _00_feature_flag WHERE key = '${flagKey}';`
    );
    expect(
      ((aliceSees[0]?.result ?? []) as unknown[]).length,
      'an admin must be able to read flag definitions'
    ).toBe(1);

    const { body: bobSees } = await recordSql(
      bobToken,
      `SELECT key FROM _00_feature_flag WHERE key = '${flagKey}';`
    );
    expect(
      ((bobSees[0]?.result ?? []) as unknown[]).length,
      'a non-admin must still see nothing'
    ).toBe(0);

    // Write: the admin's change must reach BOB's assignment, not just her own.
    // That round trip is the entire point of the DevTools Access tab.
    await recordSql(
      aliceToken,
      `RETURN fn::feature::allow('${flagKey}', 'on', ${bobId});`
    );
    const bobVariant = (await rootSql(
      `SELECT VALUE variant FROM _00_user_feature WHERE key = '${flagKey}' AND user = ${bobId};`
    )) as Array<{ result?: string[] }>;
    expect(
      bobVariant[0]?.result?.[0],
      "an admin's allowlist change must materialize onto the other user"
    ).toBe('on');

    // And bob cannot call the same function himself.
    const { body: denied } = await recordSql(
      bobToken,
      `RETURN fn::feature::allow('${flagKey}', 'on', $auth.id);`
    );
    expect(
      denied.some((r) => r.status === 'ERR'),
      `a non-admin must be denied fn::feature::allow, got: ${JSON.stringify(denied)}`
    ).toBeTruthy();
  });
});
