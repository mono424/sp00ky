import { test, expect } from '@playwright/test';
import type { Page } from '@playwright/test';
import {
  navigateToThread,
  registerUser,
  testUser,
  waitForAppReady,
  typeIntoEditor,
  waitForThreadInUpstream,
  sortHomeNewest,
} from '../fixtures/test-fixtures';

// Regression guard for "a removed list item comes back".
//
// Two things used to conspire here. Rendering was predicate-driven: a query's
// rows came from re-scanning the local store with its WHERE, so any cached body
// that still matched showed up regardless of whether the server still listed it
// in the query's window. And membership itself was not durable: it lived on the
// `_00_query` row, whose id is salted with `session::id()` — new on every page
// load, `''` offline — and which is wiped on a bucket switch. Bodies were
// durable, membership was not.
//
// Net effect: delete a row in one session, and another session that had already
// cached the body would show it again after a reload, permanently if offline.
// Rendering is now driven by the authoritative membership list, mirrored into
// the durable `_00_window` table.
//
// Same-user two-session (not cross-user): the cross-user fan-out is a known SSP
// gap and every test in `z-multi-user-sync.spec.ts` is `test.fail()` for it.

const seedThread = async (page: Page, title: string, body: string) => {
  await page.getByRole('button', { name: 'New thread' }).click();
  await page.locator('#create-title').fill(title);
  await typeIntoEditor(page, '.ProseMirror[contenteditable="true"]', body);
  await page.getByRole('button', { name: 'Publish' }).click();
  await page.locator('#create-title').waitFor({ state: 'hidden', timeout: 15_000 });
  await waitForThreadInUpstream(page, title);
};

/**
 * Skip the rest of the test unless a service worker serves the app: an offline
 * reload needs one, and `vite dev` registers none (`devOptions.enabled: false`).
 */
const skipOfflineReloadWithoutServiceWorker = async (page: Page) => {
  const controlled = await page.evaluate(() => !!navigator.serviceWorker?.controller);
  test.skip(!controlled, 'an offline reload needs the PWA service worker, which the dev server does not register');
};

/** Delete the open thread via More options → Delete post (window.confirm). */
const deleteOpenThread = async (page: Page) => {
  page.once('dialog', (d) => void d.accept());
  await page.getByRole('button', { name: 'More options' }).click();
  await page.getByRole('button', { name: 'Delete post', exact: true }).click();
};

test.describe('Removed items stay removed', () => {
  test('a deleted thread does not come back after an offline reload', async ({ browser }) => {
    test.setTimeout(180_000);

    const title = `Offline Removal ${Date.now()}`;
    const ctxA = await browser.newContext();
    const ctxB = await browser.newContext();
    const pageA = await ctxA.newPage();
    const pageB = await ctxB.newPage();

    try {
      await registerUser(pageA, testUser);
      await registerUser(pageB, testUser);

      // B publishes; A must have the row (and therefore its body) cached — that
      // cached body is what used to resurrect the row.
      await seedThread(pageB, title, 'will be deleted');

      await pageA.goto('/');
      await waitForAppReady(pageA);
      await sortHomeNewest(pageA);
      await expect(pageA.getByText(title, { exact: true })).toBeVisible({ timeout: 20_000 });

      // B deletes it. A drops it in realtime (the LIVE `_00_list_ref` DELETE).
      await navigateToThread(pageB, title);
      await deleteOpenThread(pageB);
      await expect(pageA.getByText(title, { exact: true })).not.toBeVisible({
        timeout: 20_000,
      });

      // The actual regression: go offline, reload. With no network there is
      // nothing to re-derive membership from, so only the durable list can keep
      // the row out.
      await skipOfflineReloadWithoutServiceWorker(pageA);
      await ctxA.setOffline(true);
      await pageA.reload();
      await waitForAppReady(pageA);

      await expect(pageA.getByText(title, { exact: true })).not.toBeVisible({
        timeout: 20_000,
      });
      // And it must still be gone once the network is back, i.e. the durable
      // list was right rather than merely empty-because-offline.
      await ctxA.setOffline(false);
      await expect(pageA.getByText(title, { exact: true })).not.toBeVisible({
        timeout: 20_000,
      });
    } finally {
      await ctxA.close();
      await ctxB.close();
    }
  });

  test('an offline-created thread still renders after a reload', async ({ browser }) => {
    // The other half of the render formula. Membership comes from the server, so
    // a not-yet-acked local write is only visible because pending outbox ids are
    // unioned in. If that union broke, filtering by membership would make your
    // own offline writes vanish — a worse bug than the one being fixed.
    test.setTimeout(180_000);

    const title = `Offline Create ${Date.now()}`;
    const ctx = await browser.newContext();
    const page = await ctx.newPage();

    try {
      await registerUser(page, testUser);
      await waitForAppReady(page);

      await ctx.setOffline(true);
      await page.getByRole('button', { name: 'New thread' }).click();
      await page.locator('#create-title').fill(title);
      await typeIntoEditor(page, '.ProseMirror[contenteditable="true"]', 'written offline');
      await page.getByRole('button', { name: 'Publish' }).click();
      await page.locator('#create-title').waitFor({ state: 'hidden', timeout: 15_000 });

      // Publishing opens the new thread, rendered from the local store alone.
      await page.waitForURL(/\/thread\//, { timeout: 10_000 });
      const titleInput = page.locator('#thread-title');
      await expect(titleInput).toHaveValue(title, { timeout: 20_000 });

      await skipOfflineReloadWithoutServiceWorker(page);
      await page.reload();
      await waitForAppReady(page);
      await expect(titleInput).toHaveValue(title, { timeout: 20_000 });
    } finally {
      await ctx.close();
    }
  });
});
