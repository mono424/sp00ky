import {
  test,
  expect,
  uniqueUser,
  uniqueTitle,
  deviceFor,
  createThread,
  setPublished,
  openFromHome,
  comment,
  postComment,
  deleteComment,
  withoutReload,
} from '../../fixtures/app';

test.describe.configure({ timeout: 120_000 });

const replies = (page: import('@playwright/test').Page) => page.getByRole('heading', { name: /Replies/ });

test.describe('comments', () => {
  test('comments come and go live between users, and only their author can delete them', async ({ device, track }) => {
    const alice = await deviceFor(device, uniqueUser('alice'), 'signup');
    const bob = await deviceFor(device, uniqueUser('bob'), 'signup');
    const title = uniqueTitle('Discussion');
    track(await createThread(alice, title, 'what do you think?'));
    await setPublished(alice, true);
    await withoutReload(bob, () => openFromHome(bob, title));

    const fromBob = `bob says ${Date.now()}`;
    await postComment(bob, fromBob);
    await withoutReload(alice, async () => {
      await expect(comment(alice, fromBob)).toBeVisible({ timeout: 20_000 });
      await expect(replies(alice)).toContainText('1');
    });
    // Alice may not delete bob's comment: no button for it.
    await comment(alice, fromBob).first().hover();
    await expect(comment(alice, fromBob).getByRole('button', { name: 'Delete' })).toHaveCount(0);

    const fromAlice = `alice answers ${Date.now()}`;
    await postComment(alice, fromAlice);
    await withoutReload(bob, () => expect(comment(bob, fromAlice)).toBeVisible({ timeout: 20_000 }));

    await deleteComment(bob, fromBob);
    await expect(comment(bob, fromBob)).toHaveCount(0, { timeout: 15_000 });
    await withoutReload(alice, async () => {
      await expect(comment(alice, fromBob)).toHaveCount(0, { timeout: 20_000 });
      await expect(comment(alice, fromAlice)).toBeVisible();
    });
  });

  test('"Mine" shows only my own comments', async ({ device, track }) => {
    const alice = await deviceFor(device, uniqueUser('alice'), 'signup');
    const bob = await deviceFor(device, uniqueUser('bob'), 'signup');
    const title = uniqueTitle('Filter');
    track(await createThread(alice, title, 'comment away'));
    await setPublished(alice, true);
    await openFromHome(bob, title);
    const mine = `bob ${Date.now()}`;
    const theirs = `alice ${Date.now()}`;
    await postComment(bob, mine);
    await postComment(alice, theirs);
    await expect(comment(bob, theirs)).toBeVisible({ timeout: 20_000 });

    await withoutReload(bob, async () => {
      await bob.getByRole('button', { name: 'Mine' }).click();
      await expect(comment(bob, mine)).toBeVisible({ timeout: 15_000 });
      await expect(comment(bob, theirs)).toHaveCount(0, { timeout: 15_000 });
      await bob.getByRole('button', { name: 'All' }).click();
      await expect(comment(bob, theirs)).toBeVisible({ timeout: 15_000 });
    });
  });
});
