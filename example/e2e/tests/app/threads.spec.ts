import {
  test,
  expect,
  uniqueUser,
  uniqueTitle,
  deviceFor,
  createThread,
  setPublished,
  deleteOpenThread,
  retitle,
  goHome,
  card,
  openFromHome,
  openByUrl,
  titleInput,
  editor,
  notFound,
  untilUpstream,
  withoutReload,
} from '../../fixtures/app';

// Threads across users and devices. Observers never reload: what they see
// arrives live.

test.describe.configure({ timeout: 120_000 });

test.describe('threads', () => {
  test('a private thread is seen on every device of its author and by no one else', async ({ device, track }) => {
    const alice = uniqueUser('alice');
    const a1 = await deviceFor(device, alice, 'signup');
    const a2 = await deviceFor(device, alice, 'signin');
    const bob = await deviceFor(device, uniqueUser('bob'), 'signup');
    await goHome(a2);
    await goHome(bob);

    const title = uniqueTitle('Private');
    const key = await createThread(a1, title, 'for my eyes');
    track(key);

    await withoutReload(a2, () => expect(card(a2, title)).toBeVisible({ timeout: 20_000 }));
    await withoutReload(a2, () => openFromHome(a2, title));
    await expect(editor(a2)).toContainText('for my eyes', { timeout: 20_000 });

    await bob.waitForTimeout(3000);
    await expect(card(bob, title)).toHaveCount(0);
    await openByUrl(bob, key);
    await expect(notFound(bob)).toBeVisible({ timeout: 30_000 });
  });

  test('publishing and unpublishing reach other users live, an open thread included', async ({ device, track }) => {
    const alice = await deviceFor(device, uniqueUser('alice'), 'signup');
    const bob = await deviceFor(device, uniqueUser('bob'), 'signup');
    await goHome(bob);

    const title = uniqueTitle('Going public');
    track(await createThread(alice, title, 'hello world'));
    await setPublished(alice, true);

    await withoutReload(bob, async () => {
      await expect(card(bob, title)).toBeVisible({ timeout: 30_000 });
      await card(bob, title).click();
      await expect(titleInput(bob)).toHaveValue(title, { timeout: 20_000 });
      await expect(editor(bob)).toContainText('hello world', { timeout: 20_000 });
      // A reader cannot edit.
      await expect(titleInput(bob)).toHaveAttribute('readonly', '');
    });

    await setPublished(alice, false);
    await withoutReload(bob, async () => {
      await expect(notFound(bob)).toBeVisible({ timeout: 30_000 });
      await goHome(bob);
      await expect(card(bob, title)).toHaveCount(0, { timeout: 30_000 });
    });
  });

  test("title and body edits reach other users' open thread and every list", async ({ device, track }) => {
    const aliceUser = uniqueUser('alice');
    const alice = await deviceFor(device, aliceUser, 'signup');
    const aliceLaptop = await deviceFor(device, aliceUser, 'signin');
    const bob = await deviceFor(device, uniqueUser('bob'), 'signup');

    const title = uniqueTitle('Draft title');
    const key = await createThread(alice, title, 'first words');
    track(key);
    await setPublished(alice, true);
    await withoutReload(bob, async () => {
      await goHome(bob);
      await card(bob, title).click();
      await expect(titleInput(bob)).toHaveValue(title, { timeout: 20_000 });
    });
    await goHome(aliceLaptop);

    const renamed = `${title} (final)`;
    await retitle(alice, renamed);
    await editor(alice).click();
    await alice.keyboard.press('End');
    await alice.keyboard.type(' and more');

    await withoutReload(bob, async () => {
      await expect(titleInput(bob)).toHaveValue(renamed, { timeout: 20_000 });
      await expect(editor(bob)).toContainText('first words and more', { timeout: 20_000 });
    });
    // Lists show the server's copy, written when it folds the edits.
    await untilUpstream(key, (row) => row?.title === renamed && !!row.content?.includes('and more'));
    await withoutReload(aliceLaptop, () => expect(card(aliceLaptop, renamed)).toBeVisible({ timeout: 30_000 }));
    await expect(card(aliceLaptop, renamed)).toContainText('first words and more', { timeout: 30_000 });
    await withoutReload(bob, async () => {
      await goHome(bob);
      await expect(card(bob, renamed)).toBeVisible({ timeout: 30_000 });
    });
  });

  test('deleting a thread removes it from every device and every open page', async ({ device, track }) => {
    const aliceUser = uniqueUser('alice');
    const alice = await deviceFor(device, aliceUser, 'signup');
    const aliceLaptop = await deviceFor(device, aliceUser, 'signin');
    const bob = await deviceFor(device, uniqueUser('bob'), 'signup');

    const title = uniqueTitle('Short lived');
    track(await createThread(alice, title, 'soon gone'));
    await setPublished(alice, true);
    await goHome(aliceLaptop);
    await expect(card(aliceLaptop, title)).toBeVisible({ timeout: 20_000 });
    await withoutReload(bob, () => openFromHome(bob, title));

    await deleteOpenThread(alice);
    await expect(card(alice, title)).toHaveCount(0, { timeout: 20_000 });
    await withoutReload(aliceLaptop, () => expect(card(aliceLaptop, title)).toHaveCount(0, { timeout: 30_000 }));
    await withoutReload(bob, async () => {
      await expect(notFound(bob)).toBeVisible({ timeout: 30_000 });
      await goHome(bob);
      await expect(card(bob, title)).toHaveCount(0);
    });
  });
});
