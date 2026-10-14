/**
 * Helpers for the app suite (tests/app): every action goes through the UI the
 * way a person would, on as many devices (browser contexts) as a test needs,
 * and `withoutReload` asserts the single-page app never reloads to get there.
 */
import { expect, test as base, type Locator, type Page } from '@playwright/test';
import { uniqueUser, waitForAppReady, typeIntoEditor, type TestUser } from './test-fixtures';

export { expect, uniqueUser, type TestUser };

/** Root SurrealQL against the local upstream; the rows of statement 0. */
export async function upstream<T = unknown>(sql: string): Promise<T[]> {
  const resp = await fetch('http://localhost:8666/sql', {
    method: 'POST',
    headers: {
      Accept: 'application/json',
      'surreal-ns': 'main',
      'surreal-db': 'example',
      Authorization: 'Basic ' + Buffer.from('root:root').toString('base64'),
    },
    body: sql,
  });
  const body = (await resp.json()) as Array<{ result?: T[] }>;
  return body[0]?.result ?? [];
}

/** Run `action`, then assert the page is the same document: no reload, no
 *  full navigation, only in-app routing. */
export async function withoutReload<T>(page: Page, action: () => Promise<T>): Promise<T> {
  const token = await page.evaluate(() => {
    const t = Math.random().toString(36).slice(2);
    (window as unknown as { __e2eDocument: string }).__e2eDocument = t;
    return t;
  });
  const out = await action();
  const after = await page.evaluate(() => (window as unknown as { __e2eDocument?: string }).__e2eDocument);
  expect(after, 'the page reloaded').toBe(token);
  return out;
}

// ---- auth -------------------------------------------------------------------

const authForm = (page: Page) => page.locator('form').filter({ has: page.locator('#username') });
const errorBanner = (page: Page) => page.locator('div.bg-red-500\\/10');

/** Signed in: the header shows the account's controls and the landing page
 *  is gone (the route itself is kept, so this is not always the home list). */
export async function expectSignedIn(page: Page) {
  await expect(page.getByRole('button', { name: 'Sign out' })).toBeVisible({ timeout: 20_000 });
  await expect(page.locator('header a[href="/profile"]')).toBeVisible();
  await expect(page.getByText('Welcome to sp00ky threads')).toHaveCount(0);
}

export async function expectSignedOut(page: Page) {
  await expect(page.getByText('Welcome to sp00ky threads')).toBeVisible({ timeout: 20_000 });
  await expect(page.getByRole('button', { name: 'Sign out' })).toHaveCount(0);
}

async function submitAuth(page: Page, mode: 'signup' | 'signin', user: TestUser) {
  await page.getByRole('button', { name: mode === 'signup' ? 'Sign up' : 'Sign in', exact: true }).first().click();
  const form = authForm(page);
  await form.locator('#username').fill(user.username);
  await form.locator('#password').fill(user.password);
  await form.getByRole('button', { name: mode === 'signup' ? 'Create account' : 'Sign in', exact: true }).click();
}

/** Sign up from the landing page, in place. */
export async function signUp(page: Page, user: TestUser) {
  await withoutReload(page, async () => {
    await submitAuth(page, 'signup', user);
    await expectSignedIn(page);
  });
}

/** Sign in from the landing page, in place. */
export async function signIn(page: Page, user: TestUser) {
  await withoutReload(page, async () => {
    await submitAuth(page, 'signin', user);
    await expectSignedIn(page);
  });
}

/** Try to authenticate and expect the dialog to show `message`, in place. */
export async function expectAuthError(page: Page, mode: 'signup' | 'signin', user: TestUser, message: RegExp) {
  await withoutReload(page, async () => {
    await submitAuth(page, mode, user);
    await expect(errorBanner(page)).toContainText(message, { timeout: 15_000 });
    await expect(page.getByRole('button', { name: 'Sign out' })).toHaveCount(0);
  });
  await page.keyboard.press('Escape');
}

export async function signOut(page: Page) {
  await withoutReload(page, async () => {
    await page.getByRole('button', { name: 'Sign out' }).click();
    await expectSignedOut(page);
  });
}

// ---- devices ----------------------------------------------------------------

/** A test with `device()`: a fresh browser profile on the app's landing page,
 *  closed after the test; and `track(key)`: threads deleted after the test,
 *  so the home list (ten threads) stays readable for later tests. */
export const test = base.extend<{
  device: () => Promise<Page>;
  track: (threadKey: string) => void;
}>({
  device: async ({ browser }, use) => {
    const contexts: Awaited<ReturnType<typeof browser.newContext>>[] = [];
    await use(async () => {
      const ctx = await browser.newContext();
      contexts.push(ctx);
      const page = await ctx.newPage();
      await page.goto('/');
      await waitForAppReady(page);
      return page;
    });
    for (const ctx of contexts) await ctx.close();
  },
  // eslint-disable-next-line no-empty-pattern
  track: async ({}, use) => {
    const keys: string[] = [];
    await use((key) => keys.push(key));
    for (const key of keys) await upstream(`DELETE type::record('thread', '${key}')`);
  },
});

/** A new device with `user` signed up (or signed in, for a second device). */
export async function deviceFor(device: () => Promise<Page>, user: TestUser, mode: 'signup' | 'signin') {
  const page = await device();
  await (mode === 'signup' ? signUp(page, user) : signIn(page, user));
  return page;
}

// ---- navigation ---------------------------------------------------------------

/** Home through the logo, the list newest first (ten rows; earlier tests'
 *  threads must not push this test's off it). */
export async function goHome(page: Page) {
  await page.getByRole('link', { name: 'sp00ky threads home' }).click();
  await expect(page.getByRole('button', { name: 'New thread' })).toBeVisible();
  await page.getByRole('button', { name: 'Sort' }).click();
  await page.getByRole('button', { name: 'Newest' }).click();
}

export const card = (page: Page, title: string) => page.locator('[data-thread-index]').filter({ hasText: title });

/** Open a thread from the home list. */
export async function openFromHome(page: Page, title: string) {
  await goHome(page);
  await card(page, title).click();
  await expect(titleInput(page)).toHaveValue(title, { timeout: 20_000 });
}

/** Open a thread by its URL (a link someone sent): a full page load. */
export async function openByUrl(page: Page, key: string) {
  await page.goto(`/thread/${key}`);
  await waitForAppReady(page);
}

export const titleInput = (page: Page) => page.locator('#thread-title');
export const editor = (page: Page) => page.locator('.ProseMirror').first();
export const notFound = (page: Page) => page.getByText('Thread not found');

// ---- threads ------------------------------------------------------------------

/** Create a thread through the dialog; it opens it. Returns the record key. */
export async function createThread(page: Page, title: string, body: string): Promise<string> {
  await page.getByRole('button', { name: 'New thread' }).click();
  await page.locator('#create-title').fill(title);
  await typeIntoEditor(page, '.ProseMirror[contenteditable="true"]', body);
  await page.getByRole('button', { name: 'Publish' }).click();
  await page.waitForURL(/\/thread\//, { timeout: 15_000 });
  await expect(titleInput(page)).toHaveValue(title, { timeout: 15_000 });
  return new URL(page.url()).pathname.split('/thread/')[1];
}

async function threadMenu(page: Page, item: string) {
  await page.getByRole('button', { name: 'More options' }).click();
  await page.getByRole('button', { name: item, exact: true }).click();
}

/** Publish or unpublish the open thread from its menu. */
export async function setPublished(page: Page, published: boolean) {
  await threadMenu(page, published ? 'Publish' : 'Unpublish');
  // The lock badge marks a private thread.
  await expect(page.getByLabel('Private')).toHaveCount(published ? 0 : 1, { timeout: 15_000 });
}

/** Delete the open thread (confirming the prompt); the app goes home. */
export async function deleteOpenThread(page: Page) {
  page.once('dialog', (d) => void d.accept());
  await threadMenu(page, 'Delete post');
  await expect(page.getByRole('button', { name: 'New thread' })).toBeVisible({ timeout: 15_000 });
}

/** Replace the open thread's title by typing, like a person. */
export async function retitle(page: Page, title: string) {
  const input = titleInput(page);
  await expect(input).not.toHaveAttribute('readonly', { timeout: 20_000 });
  await input.fill(title);
}

/** Wait until the server's copy of a thread satisfies `check`. */
export async function untilUpstream(key: string, check: (row: { title?: string; content?: string; published?: boolean } | undefined) => boolean, timeout = 60_000) {
  await expect
    .poll(async () => check((await upstream<{ title?: string; content?: string; published?: boolean }>(`SELECT title, content, published FROM type::record('thread', '${key}')`))[0]), { timeout })
    .toBe(true);
}

// ---- comments -----------------------------------------------------------------

export const comment = (page: Page, text: string) => page.locator('.group').filter({ hasText: text });

export async function postComment(page: Page, text: string) {
  const box = page.locator('#comment-textarea');
  await box.click();
  await box.fill(text);
  await page.getByRole('button', { name: 'Reply' }).click();
  await expect(comment(page, text)).toBeVisible({ timeout: 15_000 });
}

export async function deleteComment(page: Page, text: string) {
  const row = comment(page, text).first();
  await row.hover();
  await row.getByRole('button', { name: 'Delete' }).click();
}

// ---- profile -------------------------------------------------------------------

export async function openProfile(page: Page) {
  await page.locator('header a[href="/profile"]').click();
  await expect(page.getByRole('heading', { name: 'Edit profile' })).toBeVisible({ timeout: 15_000 });
}

export async function changeUsername(page: Page, username: string) {
  const input = page.getByPlaceholder('Enter username');
  await input.fill(username);
  await page.getByRole('button', { name: 'Save changes' }).click();
  await expect(page.getByText('Username updated.')).toBeVisible({ timeout: 15_000 });
}

export async function uploadAvatar(page: Page, file: 'avatar-red.png' | 'avatar-blue.png') {
  await page.locator('input[type="file"]').setInputFiles(`fixtures/${file}`);
}

/** The first avatar image inside `scope`, once it has loaded (crossfaded in). */
export function avatar(scope: Locator) {
  return scope.locator('img[decoding="async"]').first();
}

/** The header avatar's image URL, once loaded: what every other place must show. */
export async function headerAvatarSrc(page: Page): Promise<string> {
  const img = avatar(page.locator('header'));
  await expect(img).toHaveCSS('opacity', '1', { timeout: 20_000 });
  return (await img.getAttribute('src')) ?? '';
}

/** An avatar shows the user's picture (loaded, not the initial-letter plate). */
export async function expectPicture(scope: Locator) {
  await expect(avatar(scope)).toHaveAttribute('src', /^blob:/, { timeout: 30_000 });
  await expect(avatar(scope)).toHaveCSS('opacity', '1', { timeout: 30_000 });
}

/** A unique, readable title for one test. */
export const uniqueTitle = (what: string) => `${what} ${Date.now().toString(36)}${Math.random().toString(36).slice(2, 5)}`;
