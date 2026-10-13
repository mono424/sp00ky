import {
  test,
  expect,
  registerUser,
  uniqueUser,
  waitForAppReady,
} from '../fixtures/test-fixtures';

/**
 * SDK-driven real-time propagation test for feature flags.
 *
 * The permission spec (feature-flag-permissions.spec.ts) proves the access
 * rules over raw HTTP. This spec proves the *reactive* path through the real
 * browser SDK: a client that has already subscribed to a flag must see a
 * variant change pushed by the server (root) without a reload.
 *
 * This exercises the gap fixed by the `_00_user_feature` ingest-notify events
 * (apps/cli/src/sp00ky.rs): assignments are written by the root scheduler/CLI
 * session, so without those events the SSP never learns of the change and the
 * subscriber stays stuck at its initial value.
 *
 * The `FeatureFlagDemo` component (mounted on /profile) subscribes to the
 * `demo` flag via `useFeatureFlag('demo')` and renders the variant/state in
 * `data-testid` elements.
 */

const SURREAL_URL = 'http://localhost:8666';
const NS = 'main';
const DB = 'example';
const ROOT_AUTH = 'Basic ' + Buffer.from('root:root').toString('base64');
const FLAG_KEY = 'demo'; // matches FeatureFlagDemo's useFeatureFlag('demo')

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

async function getUserId(username: string): Promise<string> {
  const result = (await rootSql(
    `SELECT VALUE id FROM user WHERE username = '${username}' LIMIT 1;`
  )) as Array<{ result?: string[] }>;
  const id = result[0]?.result?.[0] ?? '';
  expect(id, `failed to resolve user id for ${username}`).toBeTruthy();
  return id;
}

// Upsert this user's assignment as root (the scheduler/CLI write path) and
// fire the `_00_user_feature_mutation` ingest event.
async function setVariant(userId: string, variant: string): Promise<void> {
  await rootSql(
    `UPSERT _00_user_feature SET user = ${userId}, key = '${FLAG_KEY}', variant = '${variant}' ` +
      `WHERE user = ${userId} AND key = '${FLAG_KEY}';`
  );
}

test.describe('Feature flag real-time propagation (SDK)', () => {
  test.afterAll(async () => {
    await rootSql(`DELETE _00_user_feature WHERE key = '${FLAG_KEY}';`);
    await rootSql(`DELETE _00_feature_flag WHERE key = '${FLAG_KEY}';`);
  });

  test('subscribed client sees variant flip on/off without reload', async ({
    page,
  }) => {
    const user = uniqueUser('ffrt');

    // Sign up and resolve the record id (root write path targets it).
    await registerUser(page, user);
    const userId = await getUserId(user.username);

    // Realistic setup: a flag definition exists (clients can't read it).
    await rootSql(
      `CREATE _00_feature_flag SET key = '${FLAG_KEY}', variants = ['off','on'], ` +
        `default_variant = 'off', rules = [], enabled = true;`
    ).catch(() => {
      /* already exists from a prior run; the unique user keeps assignments isolated */
    });

    // Land on /profile where FeatureFlagDemo mounts and subscribes. From this
    // point the client holds a live subscription on its own _00_user_feature.
    await page.goto('/profile');
    await waitForAppReady(page);
    const variant = page.locator('[data-testid="feature-flag-demo-variant"]');
    const state = page.locator('[data-testid="feature-flag-demo-state"]');
    await expect(page.locator('[data-testid="feature-flag-demo"]')).toBeVisible({
      timeout: 15_000,
    });

    // Initial state: no assignment yet -> unset / default experience.
    await expect(variant).toHaveText('(unset)', { timeout: 10_000 });
    await expect(state).toHaveText('Default experience.', { timeout: 10_000 });

    // Server enables the flag AFTER the client subscribed — the reactive case.
    await setVariant(userId, 'on');
    await expect(variant).toHaveText('on', { timeout: 15_000 });
    await expect(state).toHaveText('New experience enabled for this user.', {
      timeout: 15_000,
    });

    // Server disables it — should flip back live.
    await setVariant(userId, 'off');
    await expect(variant).toHaveText('off', { timeout: 15_000 });
    await expect(state).toHaveText('Default experience.', { timeout: 15_000 });
  });
});
