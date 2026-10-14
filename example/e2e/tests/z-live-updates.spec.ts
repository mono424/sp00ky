import { test, expect, type Page, type Browser } from '@playwright/test';
import {
  registerUser,
  loginUser,
  uniqueUser,
  typeIntoEditor,
  waitForThreadInUpstream,
  waitForThreadContentInUpstream,
  sortHomeNewest,
  type TestUser,
} from '../fixtures/test-fixtures';

// Live propagation between two open clients, without reloads: every change
// one client makes must reach the other's open screen. Each test runs the
// same-account case (two devices) or the cross-account case (an author and a
// reader of a published thread).

test.describe.configure({ timeout: 120_000 });

// Threads made here are deleted after each test: the home list shows ten, and
// leftovers would push later specs' threads off it.
const created: string[] = [];
test.afterEach(async () => {
  for (const id of created.splice(0)) await upstream(`DELETE thread:${id}`);
});

async function upstream<T>(sql: string): Promise<T[]> {
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

async function createThread(page: Page, title: string, body: string, publish: boolean): Promise<string> {
  await page.getByRole('button', { name: 'New thread' }).click();
  await page.locator('#create-title').fill(title);
  await typeIntoEditor(page, '.ProseMirror[contenteditable="true"]', body);
  await page.getByRole('button', { name: 'Publish' }).click();
  await page.locator('#create-title').waitFor({ state: 'hidden', timeout: 15_000 });
  await waitForThreadInUpstream(page, title);
  await waitForThreadContentInUpstream(page, title);
  const [id] = await upstream<string>(`SELECT VALUE <string>id FROM thread WHERE title = ${JSON.stringify(title)}`);
  if (publish) await upstream(`UPDATE ${id} SET published = true`);
  created.push(id.split(':')[1]);
  return id.split(':')[1];
}

async function openThread(page: Page, id: string) {
  await page.goto(`/thread/${id}`);
  await expect(page.locator('.ProseMirror').first()).toBeVisible({ timeout: 20_000 });
}

async function postComment(page: Page, text: string) {
  const box = page.locator('#comment-textarea');
  await box.click();
  await box.fill(text);
  await page.getByRole('button', { name: 'Reply' }).click();
}

async function deleteComment(page: Page, text: string) {
  const row = page.locator('.group').filter({ hasText: text }).first();
  await row.hover();
  await row.getByRole('button', { name: 'Delete' }).click();
}

async function pair(browser: Browser, a: TestUser, b: TestUser) {
  const ca = await browser.newContext();
  const cb = await browser.newContext();
  const pa = await ca.newPage();
  const pb = await cb.newPage();
  await registerUser(pa, a);
  if (a.username === b.username) await loginUser(pb, b);
  else await registerUser(pb, b);
  return { pa, pb, close: async () => { await ca.close(); await cb.close(); } };
}

for (const crossUser of [false, true]) {
  const label = crossUser ? 'another account' : 'the same account on another device';

  test(`comments added and deleted show up live for ${label}`, async ({ browser }) => {
    const a = uniqueUser('live_a');
    const b = crossUser ? uniqueUser('live_b') : a;
    const { pa, pb, close } = await pair(browser, a, b);
    try {
      const id = await createThread(pa, `Live comments ${crossUser ? 'x' : 's'} ${Date.now()}`, 'body', true);
      await openThread(pa, id);
      await openThread(pb, id);

      const text = `comment ${Date.now()}`;
      await postComment(pa, text);
      await expect(pb.getByText(text)).toBeVisible({ timeout: 20_000 });

      await deleteComment(pa, text);
      await expect(pa.getByText(text)).toBeHidden({ timeout: 10_000 });
      await expect(pb.getByText(text)).toBeHidden({ timeout: 20_000 });
    } finally {
      await close();
    }
  });
}

test('title edits merge live between two devices and reach both lists', async ({ browser }) => {
  const a = uniqueUser('live_t');
  const { pa, pb, close } = await pair(browser, a, a);
  try {
    const base = `Base ${Date.now()}`;
    const id = await createThread(pa, base, 'body', false);
    await openThread(pa, id);
    await openThread(pb, id);
    const titleA = pa.locator('#thread-title');
    const titleB = pb.locator('#thread-title');
    await expect(titleA).toHaveValue(base, { timeout: 20_000 });
    await expect(titleB).toHaveValue(base, { timeout: 20_000 });

    // A appends, B prepends, at the same time: both edits survive on both.
    await titleA.click();
    await pa.keyboard.press('End');
    await titleB.click();
    await pb.keyboard.press('Home');
    await Promise.all([pa.keyboard.type(' from A', { delay: 40 }), pb.keyboard.type('B: ', { delay: 40 })]);
    const merged = `B: ${base} from A`;
    await expect(titleA).toHaveValue(merged, { timeout: 20_000 });
    await expect(titleB).toHaveValue(merged, { timeout: 20_000 });

    // The server folds the title and every list shows it.
    await expect
      .poll(async () => (await upstream<string>(`SELECT VALUE title FROM thread:${id}`))[0], { timeout: 45_000 })
      .toBe(merged);
    await pb.goto('/');
    await expect(pb.getByText(merged, { exact: true })).toBeVisible({ timeout: 20_000 });
  } finally {
    await close();
  }
});

test('a reader sees the author retitle and edit a published thread live', async ({ browser }) => {
  const a = uniqueUser('live_ra');
  const b = uniqueUser('live_rb');
  const { pa, pb, close } = await pair(browser, a, b);
  try {
    const base = `Reader base ${Date.now()}`;
    const id = await createThread(pa, base, 'first words', true);
    await openThread(pa, id);
    await openThread(pb, id);
    const titleA = pa.locator('#thread-title');
    const titleB = pb.locator('#thread-title');
    await expect(titleB).toHaveValue(base, { timeout: 20_000 });

    await titleA.click();
    await pa.keyboard.press('End');
    await pa.keyboard.type(' (edited)', { delay: 30 });
    await expect(titleB).toHaveValue(`${base} (edited)`, { timeout: 20_000 });

    const editorA = pa.locator('.ProseMirror[contenteditable="true"]').first();
    await editorA.click();
    await pa.keyboard.press('End');
    await pa.keyboard.type(' and more', { delay: 30 });
    await expect(pb.locator('.ProseMirror').first()).toContainText('first words and more', { timeout: 20_000 });
  } finally {
    await close();
  }
});

test('threads created, published, unpublished and deleted show up live in the other lists', async ({ browser }) => {
  const a = uniqueUser('live_la');
  const b = uniqueUser('live_lb');
  const { pa, pb, close } = await pair(browser, a, b);
  const a2ctx = await browser.newContext();
  const pa2 = await a2ctx.newPage();
  try {
    await loginUser(pa2, a);
    await pb.goto('/');
    await pa2.goto('/');
    const title = `Lifecycle ${Date.now()}`;
    const id = await createThread(pa, title, 'body', false);

    // Same account, other device: there at once. Other account: only once published.
    await expect(pa2.getByText(title, { exact: true })).toBeVisible({ timeout: 20_000 });
    await expect(pb.getByText(title, { exact: true })).toBeHidden();
    await upstream(`UPDATE thread:${id} SET published = true`);
    await expect(pb.getByText(title, { exact: true })).toBeVisible({ timeout: 20_000 });
    await upstream(`UPDATE thread:${id} SET published = false`);
    await expect(pb.getByText(title, { exact: true })).toBeHidden({ timeout: 20_000 });
    await upstream(`UPDATE thread:${id} SET published = true`);
    await expect(pb.getByText(title, { exact: true })).toBeVisible({ timeout: 20_000 });

    // The author deletes it from the thread page: gone from both lists.
    await openThread(pa, id);
    pa.once('dialog', (d) => d.accept());
    await pa.getByRole('button', { name: 'More options' }).click();
    await pa.getByRole('button', { name: 'Delete post' }).click();
    await expect(pa2.getByText(title, { exact: true })).toBeHidden({ timeout: 20_000 });
    await expect(pb.getByText(title, { exact: true })).toBeHidden({ timeout: 20_000 });
  } finally {
    await a2ctx.close();
    await close();
  }
});

test('an invite link makes the recipient a collaborator whose title edits reach the author live', async ({ browser }) => {
  const a = uniqueUser('live_ia');
  const b = uniqueUser('live_ib');
  const { pa, pb, close } = await pair(browser, a, b);
  try {
    const base = `Invite ${Date.now()}`;
    const id = await createThread(pa, base, 'shared body', true);
    await openThread(pa, id);
    await pa.getByRole('button', { name: 'More options' }).click();
    await pa.getByRole('button', { name: 'Share', exact: true }).click();
    await pa.getByRole('button', { name: 'New link' }).click();
    await expect
      .poll(async () => (await upstream<string>(`SELECT VALUE jwt FROM share_link WHERE thread = thread:${id}`))[0] ?? '', { timeout: 20_000 })
      .not.toBe('');
    const [jwt] = await upstream<string>(`SELECT VALUE jwt FROM share_link WHERE thread = thread:${id}`);
    await pa.getByRole('button', { name: 'Close' }).first().click();

    await pb.goto(`/invite/${jwt}`);
    await pb.waitForURL(new RegExp(`/thread/${id}`), { timeout: 20_000 });
    const titleB = pb.locator('#thread-title');
    await expect(titleB).toHaveValue(base, { timeout: 20_000 });
    await expect(titleB).not.toHaveAttribute('readonly', { timeout: 20_000 });
    await titleB.click();
    await pb.keyboard.press('End');
    await pb.keyboard.type(' +bob', { delay: 30 });
    await expect(pa.locator('#thread-title')).toHaveValue(`${base} +bob`, { timeout: 20_000 });

    // A forged link is refused with a message, not a network error.
    await pb.goto(`/invite/${jwt.slice(0, -4)}AAAA`);
    await expect(pb.getByText('This invite link is invalid or has expired.')).toBeVisible({ timeout: 20_000 });
  } finally {
    await close();
  }
});

// A related row (here a thread's author) changing must re-render every list
// that embeds it. It used to stay stale until something else re-rendered the
// list, and the old picture's file is deleted on upload, so the card went blank.
test('a new profile picture shows in the home list and the thread without a reload', async ({ browser }) => {
  const a = uniqueUser('live_av');
  const ctx = await browser.newContext();
  const page = await ctx.newPage();
  try {
    await registerUser(page, a);
    const title = `Avatar ${Date.now()}`;
    const id = await createThread(page, title, 'body', false);
    // The header's avatar is the reference: every other place must show the same image.
    const loaded = (scope: ReturnType<Page['locator']>) => scope.locator('img[decoding="async"]').first();
    const sameAsHeader = async (scope: ReturnType<Page['locator']>) => {
      const header = page.locator('header');
      await expect(loaded(header)).toHaveCSS('opacity', '1', { timeout: 15_000 });
      const src = await loaded(header).getAttribute('src');
      await expect(loaded(scope)).toHaveAttribute('src', src ?? 'missing', { timeout: 15_000 });
      await expect(loaded(scope)).toHaveCSS('opacity', '1', { timeout: 15_000 });
    };
    await page.goto('/profile');
    for (const file of ['avatar-red.png', 'avatar-blue.png']) {
      await page.locator('input[type="file"]').setInputFiles(`fixtures/${file}`);
      await expect(loaded(page.locator('header'))).toHaveCSS('opacity', '1', { timeout: 15_000 });
      // In-app navigation only: home, then the thread.
      await page.getByRole('link', { name: 'sp00ky threads home' }).click();
      await sortHomeNewest(page);
      await sameAsHeader(page.locator('[data-thread-index]').filter({ hasText: title }));
      await page.locator('[data-thread-index]').filter({ hasText: title }).click();
      await sameAsHeader(page.locator('article'));
      await page.locator('header a[href="/profile"]').click();
    }
    void id;
  } finally {
    await ctx.close();
  }
});
