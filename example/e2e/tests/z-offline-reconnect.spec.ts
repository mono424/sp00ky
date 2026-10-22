import type { Page } from '@playwright/test';
import {
  test,
  expect,
  registerUser,
  testUser,
  waitForAppReady,
  typeIntoEditor,
  waitForThreadInUpstream,
  sortHomeNewest,
} from '../fixtures/test-fixtures';

// Regression guard for the "stops updating after a while" class of bug.
//
// The SurrealDB SDK's default reconnect budget is 5 attempts on an exponential
// backoff, roughly 62 seconds of downtime — after which it gives up for the life
// of the page and nothing reconnects. Separately, a recovered reconnect emits
// `reconnecting` -> `connected` and never `disconnected`, so a handler watching
// only `disconnected` misses it and leaves the (session-scoped, now dead)
// server-side LIVE subscription unreplaced.
//
// Either one produces the same user-visible symptom: the app looks fine, local
// edits work, and remote changes silently stop arriving.
//
// This test stays offline well past that 62s budget on purpose. That's what makes
// it a guard rather than a smoke test.

const OFFLINE_MS = 75_000;

const readSyncState = (page: Page) =>
  page.evaluate(() => {
    const w = window as unknown as {
      __sp00ky__?: { syncHealth?: { status: string; connection: string } };
    };
    return w.__sp00ky__?.syncHealth ?? null;
  });

test.describe('Offline reconnect', () => {
  test('resumes realtime updates after an outage longer than the SDK retry budget', async ({
    browser,
  }) => {
    // Offline wait + app boot on two contexts + sync convergence.
    test.setTimeout(OFFLINE_MS + 150_000);

    const title = `Offline Reconnect ${Date.now()}`;
    const ctxA = await browser.newContext();
    const ctxB = await browser.newContext();
    const pageA = await ctxA.newPage();
    const pageB = await ctxB.newPage();

    try {
      await registerUser(pageA, testUser);
      await registerUser(pageB, testUser);
      await waitForAppReady(pageA);
      await waitForAppReady(pageB);
      await sortHomeNewest(pageA);

      // Baseline: A is connected and syncing before we break anything.
      await expect
        .poll(() => readSyncState(pageA).then((s) => s?.connection), { timeout: 15_000 })
        .toBe('connected');

      // Cut A off for longer than the SDK would ever keep retrying.
      await ctxA.setOffline(true);
      await pageA.waitForTimeout(OFFLINE_MS);

      // Meanwhile B publishes a thread A has never seen.
      await pageB.getByRole('button', { name: 'New thread' }).click();
      await pageB.locator('#create-title').fill(title);
      await typeIntoEditor(
        pageB,
        '.ProseMirror[contenteditable="true"]',
        'written while the other client was offline'
      );
      await pageB.getByRole('button', { name: 'Publish' }).click();
      await pageB.locator('#create-title').waitFor({ state: 'hidden', timeout: 15_000 });
      await waitForThreadInUpstream(pageB, title);

      // Restore the network. Nothing else: no reload, no user action. The
      // supervisor must notice and rebuild the connection on its own.
      await ctxA.setOffline(false);

      await expect
        .poll(() => readSyncState(pageA).then((s) => s?.connection), { timeout: 60_000 })
        .toBe('connected');

      // The real assertion: realtime data flows again. This is what fails when
      // the reconnect happens but the LIVE subscription is never re-issued.
      await expect(pageA.getByText(title, { exact: true })).toBeVisible({
        timeout: 60_000,
      });

      await expect
        .poll(() => readSyncState(pageA).then((s) => s?.status), { timeout: 30_000 })
        .toBe('healthy');
    } finally {
      await ctxA.close();
      await ctxB.close();
    }
  });
});
