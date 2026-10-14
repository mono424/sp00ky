import {
  test,
  expect,
  uniqueUser,
  uniqueTitle,
  signUp,
  signIn,
  signOut,
  expectSignedIn,
  expectSignedOut,
  expectAuthError,
  createThread,
  card,
  goHome,
  withoutReload,
} from '../../fixtures/app';

// Auth happens in place: the dialog swaps the landing page for the app (and
// back) without reloading, and the local store switches to the account's own.

test.describe('auth', () => {
  test('signs up without a reload and lands in the app', async ({ device }) => {
    const page = await device();
    await expectSignedOut(page);
    await signUp(page, uniqueUser('su'));
    // A fresh account can start writing at once.
    await expect(page.getByText('No threads yet').or(page.locator('[data-thread-index]').first())).toBeVisible();
  });

  test('signs out and back in without a reload, and gets its private threads back', async ({ device, track }) => {
    const user = uniqueUser('inout');
    const page = await device();
    await signUp(page, user);
    const title = uniqueTitle('Private');
    track(await createThread(page, title, 'only mine'));

    await signOut(page);
    await expect(page.getByText(title)).toHaveCount(0);

    await signIn(page, user);
    await withoutReload(page, async () => {
      await goHome(page);
      await expect(card(page, title)).toBeVisible({ timeout: 20_000 });
    });
  });

  test("switching accounts in one tab never shows the previous account's private threads", async ({ device, track }) => {
    const alice = uniqueUser('alice');
    const bob = uniqueUser('bob');
    const page = await device();
    await signUp(page, alice);
    const title = uniqueTitle('Alice only');
    track(await createThread(page, title, 'secret'));
    await goHome(page);
    await expect(card(page, title)).toBeVisible();

    await signOut(page);
    await signUp(page, bob);
    await goHome(page);
    // Bob's list and the sidebar never show alice's private thread.
    await expect(page.getByRole('button', { name: 'New thread' })).toBeVisible();
    await page.waitForTimeout(2000);
    await expect(page.getByText(title)).toHaveCount(0);

    await signOut(page);
    await signIn(page, alice);
    await goHome(page);
    await expect(card(page, title)).toBeVisible({ timeout: 20_000 });
  });

  test('a wrong password and a taken username are reported in the dialog', async ({ device }) => {
    const user = uniqueUser('err');
    const page = await device();
    await signUp(page, user);
    await signOut(page);

    await expectAuthError(page, 'signin', { ...user, password: 'not-the-password' }, /.+/);
    await expectAuthError(page, 'signup', user, /.+/);
    // The right credentials still work afterwards, in the same document.
    await signIn(page, user);
  });

  test('a reload and a new tab keep the session', async ({ device }) => {
    const page = await device();
    await signUp(page, uniqueUser('keep'));
    await page.reload();
    await expectSignedIn(page);
    const tab = await page.context().newPage();
    await tab.goto('/');
    await expectSignedIn(tab);
  });
});
