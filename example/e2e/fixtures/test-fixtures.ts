import type { Browser, BrowserContext, Page } from '@playwright/test';
import { test as base, expect } from '@playwright/test';
import { UPSTREAM, upstreamHeaders } from './target';

export interface TestUser {
  username: string;
  password: string;
}

// Fixed test user — registerUser handles "already exists" by falling back
// to login. Stable name avoids issues with Playwright re-evaluating
// modules per test file.
export const testUser: TestUser = {
  username: 'e2e_testuser',
  password: 'e2e_testpass',
};

/**
 * Build a unique TestUser per call. Cross-user tests need TWO distinct
 * accounts, and re-running the suite must produce fresh usernames so
 * the fixture's register-or-login fallback doesn't quietly log us into
 * the previous run's account (which would alias alice and bob onto
 * the same row and defeat the test). Stale accounts pile up in the
 * upstream `user` table; cleanup is out of scope here.
 */
export function uniqueUser(prefix: string): TestUser {
  const tag =
    Date.now().toString(36) + Math.random().toString(36).slice(2, 6);
  return { username: `${prefix}_${tag}`, password: 'e2e_testpass' };
}

/**
 * Wait for the SolidJS app to finish initializing its DB connection.
 * Boot screen shows a "Connecting..." spinner; once the layout mounts the
 * <header> appears. This is the universal "app is interactive" gate.
 */
export async function waitForAppReady(page: Page) {
  await page.waitForSelector('header', { timeout: 30_000 });
}

/**
 * Wait for the authenticated layout — the "New thread" button is the
 * cheapest unambiguous signal that auth + thread list rendered.
 */
async function waitForAuthenticated(page: Page) {
  await expect(page.getByRole('button', { name: 'New thread' }))
    .toBeVisible({ timeout: 15_000 });
}

/**
 * Register a new user via the UI. If the username already exists (signup
 * fails), close the dialog and fall back to login with the same creds.
 */
export async function registerUser(page: Page, user: TestUser) {
  await page.goto('/');
  await waitForAppReady(page);

  // Two "Sign up" entry points exist (header + landing "Create account");
  // grab the header button by its exact label.
  await page.getByRole('button', { name: 'Sign up', exact: true }).click();

  const form = page.locator('form').filter({ has: page.locator('#username') });
  await form.locator('#username').fill(user.username);
  await form.locator('#password').fill(user.password);
  await form.getByRole('button', { name: 'Create account', exact: true }).click();

  // Race "auth succeeded" vs "username taken" (or any other error
  // surfaced inside the dialog). The success signal is the dialog
  // closing AND the thread list mounting.
  const result = await Promise.race([
    waitForAuthenticated(page).then(() => 'ok' as const),
    page
      .locator('div.bg-red-500\\/10') // error banner inside the dialog
      .waitFor({ timeout: 15_000 })
      .then(() => 'error' as const),
  ]);

  if (result === 'error') {
    await page.locator('button[aria-label="Close"]').click();
    await loginUser(page, user);
  }
}

/**
 * Login an existing user via the UI. Multiple "Sign in" buttons can be
 * visible when logged out (header + landing + dialog submit), so we
 * scope dialog interactions to the form once it's mounted.
 */
export async function loginUser(page: Page, user: TestUser) {
  await page.goto('/');
  await waitForAppReady(page);

  // Header "Sign in" — first() pins the header button (mounts before
  // landing copy renders). Landing has its own "Sign in" but tapping
  // either opens the same dialog.
  await page.getByRole('button', { name: 'Sign in', exact: true }).first().click();

  // Scope all dialog work to the form so no "Sign in" elsewhere on the
  // page can accidentally match.
  const form = page.locator('form').filter({ has: page.locator('#username') });
  await form.locator('#username').fill(user.username);
  await form.locator('#password').fill(user.password);
  await form.getByRole('button', { name: 'Sign in', exact: true }).click();

  await waitForAuthenticated(page);
}

/**
 * Sign out via the header button. After click the landing screen
 * ("Welcome to sp00ky threads") should be visible again.
 */
export async function signOutUser(page: Page) {
  await page.getByRole('button', { name: 'Sign out' }).click();
  await expect(page.getByText('Welcome to sp00ky threads'))
    .toBeVisible({ timeout: 10_000 });
}

/**
 * Navigate to a thread by clicking its card in the list. Finds the card
 * via `[data-thread-index]` filtered by the title text so we hit the
 * element that owns the `onMouseDown` handler (clicking the heading
 * directly sometimes doesn't reach the parent's mousedown via
 * Playwright). Title-based filtering also keeps the helper correct when
 * the DB has multiple threads — the alphabetical sort can put any of
 * them at index 0.
 */
export async function navigateToThread(page: Page, title: string) {
  await page.goto('/');
  await waitForAppReady(page);
  await sortHomeNewest(page);
  await expect(page.getByText(title, { exact: true })).toBeVisible({
    timeout: 15_000,
  });
  const card = page
    .locator('[data-thread-index]')
    .filter({ has: page.getByText(title, { exact: true }) });
  await card.first().click();
  await page.waitForURL(/\/thread\//, { timeout: 10_000 });
}

/**
 * Show the home list newest first. Its default A-Z order shows ten threads,
 * so a thread made during the run can sort past the ones earlier specs left.
 */
export async function sortHomeNewest(page: Page) {
  await page.getByRole('button', { name: 'Sort' }).click();
  await page.getByRole('button', { name: 'Newest' }).click();
}

/**
 * Poll the upstream SurrealDB SQL endpoint until a thread with `title`
 * appears, or throw after `attempts * 500ms`. Used as a deterministic
 * gate after `Publish` so subsequent UI assertions (home list, sidebar,
 * second-client view) don't race the create-flow's debounced sync-up.
 * `title` is a `@crdt text` field: a create carries it at once, but a rename
 * reaches the column only with the SSP fold (after 5 s idle by default), so
 * the default allows for one.
 */
export async function waitForThreadInUpstream(
  page: Page,
  title: string,
  attempts = 60
) {
  for (let i = 0; i < attempts; i++) {
    const resp = await fetch(UPSTREAM.url, {
      method: 'POST',
      headers: upstreamHeaders(),
      body: 'SELECT id, title FROM thread',
    });
    const body = (await resp.json()) as Array<{
      result?: Array<{ title?: string }>;
    }>;
    const rows = body[0]?.result ?? [];
    if (rows.some((r) => r.title === title)) return;
    await page.waitForTimeout(500);
  }
  throw new Error(
    `Thread "${title}" never appeared in upstream after ${attempts * 500}ms.`
  );
}

/** Run root SurrealQL against the local upstream and return statement 0's rows. */
async function upstreamRows<T>(sql: string): Promise<T[]> {
  const resp = await fetch(UPSTREAM.url, {
    method: 'POST',
    headers: upstreamHeaders(),
    body: sql,
  });
  const body = (await resp.json()) as Array<{ result?: T[] }>;
  return body[0]?.result ?? [];
}

/**
 * Poll until the `@crdt` `content` edits of the thread titled `title` have
 * reached upstream: an edit-log row, or a document the SSP already folded.
 * The edits travel as their own rows after the thread itself, so a test that
 * reloads (or opens the thread on another client) right after publishing
 * waits for this, not just for the thread.
 */
export async function waitForThreadContentInUpstream(page: Page, title: string, attempts = 30) {
  const sql = `SELECT VALUE id FROM thread WHERE title = ${JSON.stringify(title)}
    AND (count((SELECT id FROM thread__crdt_log WHERE doc = $parent.id)) > 0
      OR count((SELECT id FROM _00_crdt_doc WHERE doc = $parent.id)) > 0)`;
  for (let i = 0; i < attempts; i++) {
    if ((await upstreamRows<string>(sql)).length > 0) return;
    await page.waitForTimeout(500);
  }
  throw new Error(`Content of thread "${title}" never reached upstream after ${attempts * 500}ms.`);
}

/**
 * Poll until the server-written plain-text mirror of the thread's `content`
 * contains every one of `parts`. The mirror is only written by the SSP fold,
 * which runs once the edit log has been idle for `SPKY_CRDT_FOLD_IDLE_SECS`
 * (5 s by default).
 */
export async function waitForThreadMirror(page: Page, title: string, parts: string[], timeoutMs = 120_000) {
  const sql = `SELECT VALUE content FROM thread WHERE title = ${JSON.stringify(title)}`;
  const deadline = Date.now() + timeoutMs;
  let last = '';
  while (Date.now() < deadline) {
    last = (await upstreamRows<string>(sql))[0] ?? '';
    if (parts.every((p) => last.includes(p))) return last;
    await page.waitForTimeout(1000);
  }
  throw new Error(`Mirror of "${title}" never held ${JSON.stringify(parts)}; last: ${JSON.stringify(last)}`);
}

/**
 * Type into a Tiptap/ProseMirror editor identified by `selector`. The
 * editor is `[contenteditable]`, so `.fill()` doesn't work — we click to
 * focus then `.type()` keystrokes so LoroSyncPlugin captures each one.
 */
export async function typeIntoEditor(page: Page, selector: string, text: string) {
  const editor = page.locator(selector);
  await editor.click();
  await editor.press('ControlOrMeta+a');
  await editor.press('Delete');
  await page.keyboard.type(text);
}

/**
 * Close a context's pages before the context itself. `context.close()` tears
 * the pages down without `pagehide`, so the app never sent its leave beacon
 * and every view a test opened stayed registered for its full 10-minute TTL.
 * Mid-suite the SSP held 600+ views for a handful of open pages, and each
 * edit to a published thread was written into all of them: publication fell
 * behind and the later specs failed on it (canary.348/.349). `page.close()`
 * does fire `pagehide`.
 */
async function closePagesFirst(context: BrowserContext) {
  await Promise.all(context.pages().map((page) => page.close().catch(() => {})));
}

const releasing = new WeakSet<Browser>();

export const test = base.extend<{ testUser: TestUser }, { releasingBrowser: Browser }>({
  // The worker's browser, patched once so contexts a test makes itself
  // (`browser.newContext()`) close their pages first too.
  releasingBrowser: [
    async ({ browser }, use) => {
      if (!releasing.has(browser)) {
        releasing.add(browser);
        const newContext = browser.newContext.bind(browser);
        browser.newContext = async (options) => {
          const context = await newContext(options);
          const close = context.close.bind(context);
          context.close = async (closeOptions) => {
            await closePagesFirst(context);
            return close(closeOptions);
          };
          return context;
        };
      }
      await use(browser);
    },
    { scope: 'worker', auto: true },
  ],
  // The built-in `page`'s context.
  context: async ({ context }, use) => {
    await use(context);
    await closePagesFirst(context);
  },
  // eslint-disable-next-line no-empty-pattern
  testUser: async ({}, use) => {
    await use(testUser);
  },
});

export { expect };
