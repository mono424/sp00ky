import { test, expect, uniqueUser, registerUser } from '../fixtures/test-fixtures';
import { SSP_URL } from '../fixtures/target';

// Smoke test for `refMode: single` (the legacy shared `_00_list_ref`).
// Requires the stack to be running with `SPKY_SSP_REF_MODE=single`
// (set `refMode: single` in `example/sp00ky.yml` and restart
// `pnpm dev:reset`). Auto-skips otherwise so it doesn't break the
// normal `refMode: dedicated` CI run.
//
// **Known finding (2026-05-22):** when this suite was first invoked
// against a single-mode stack the auth-loading screen never resolved
// (`Sp00kyProvider` stayed at "Loading..."), so the same-session
// create case below isn't currently exercised. The hang is
// independent of the deferred-ingest / pre-emptive-table work — both
// are no-ops in single mode — and probably indicates a pre-existing
// regression in the single-mode auth bootstrap. Leaving the test in
// place so a future fix can flip it green by clearing the auth hang
// and confirming both assertions.
//
// Two purposes:
//
// 1. **Same-session smoke** — a single client can create a thread
//    and see it in its own home list. Single mode's writes go to the
//    global `_00_list_ref`, which the schema's `auth_id = $auth.id`
//    permission rule reads correctly within one WS session. If this
//    breaks, the routing-table change in `apps/ssp/src/tables.rs`
//    regressed single-mode behavior.
//
// 2. **Cross-session sync gap documentation** — a second client of
//    the same user (separate browser context) does NOT pick up the
//    new thread within the realtime window, because single mode
//    triggers the SurrealDB v3 LIVE-permission filter on `_00_list_ref`
//    that silently drops cross-session notifications even when the
//    permission expression would pass. This gap is the whole reason
//    `refMode: dedicated` exists; this test pins the negative behavior
//    so a "single mode also works cross-session" claim has to come
//    with a passing assertion.

async function detectRefMode(): Promise<'single' | 'dedicated' | 'unknown'> {
  // The SSP exposes its current ref mode via the public `/info`
  // endpoint (no auth). Probing it directly keeps the test from
  // having to read sp00ky.yml.
  try {
    const resp = await fetch(`${SSP_URL}/info`);
    if (!resp.ok) return 'unknown';
    const body = (await resp.json()) as Array<{ ref_mode?: string }>;
    const mode = body?.[0]?.ref_mode;
    if (mode === 'single') return 'single';
    if (mode === 'dedicated') return 'dedicated';
    return 'unknown';
  } catch {
    return 'unknown';
  }
}

test.describe('Single-mode smoke', () => {
  test.beforeAll(async () => {
    const mode = await detectRefMode();
    test.skip(
      mode !== 'single',
      `Stack is running in refMode=${mode}; set SPKY_SSP_REF_MODE=single and restart to exercise this suite.`,
    );
  });

  test('same-session create + home-list visibility', async ({ page }) => {
    const user = uniqueUser('single_a');
    await registerUser(page, user);

    await page.getByRole('button', { name: 'New thread' }).click();
    const title = `Single-mode thread ${Date.now()}`;
    await page.locator('#create-title').fill(title);
    await page.getByRole('button', { name: 'Publish' }).click();
    await page
      .locator('#create-title')
      .waitFor({ state: 'hidden', timeout: 15_000 });

    await page.goto('/');
    await expect(
      page.getByText(title, { exact: true }),
    ).toBeVisible({ timeout: 15_000 });
  });

  test('cross-session sync DOES NOT propagate (documents the gap)', async ({
    browser,
  }) => {
    const user = uniqueUser('single_b');
    const ctxA = await browser.newContext();
    const ctxB = await browser.newContext();
    const pageA = await ctxA.newPage();
    const pageB = await ctxB.newPage();

    try {
      await registerUser(pageA, user);
      await registerUser(pageB, user);

      await pageA.getByRole('button', { name: 'New thread' }).click();
      const title = `Single-mode cross ${Date.now()}`;
      await pageA.locator('#create-title').fill(title);
      await pageA.getByRole('button', { name: 'Publish' }).click();
      await pageA
        .locator('#create-title')
        .waitFor({ state: 'hidden', timeout: 15_000 });

      // We expect bob (same user, different session) NOT to see the
      // thread within a short realtime window. If single mode ever
      // starts delivering cross-session LIVE notifications on
      // permission-gated tables, this assertion flips and we should
      // re-evaluate whether `refMode: dedicated` is still needed.
      await pageB.goto('/');
      await expect(
        pageB.getByText(title, { exact: true }),
      ).not.toBeVisible({ timeout: 5_000 });
    } finally {
      await ctxA.close();
      await ctxB.close();
    }
  });
});
