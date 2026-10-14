import {
  test,
  expect,
  uniqueUser,
  uniqueTitle,
  deviceFor,
  createThread,
  setPublished,
  openFromHome,
  retitle,
  titleInput,
  comment,
  postComment,
  withoutReload,
} from '../../fixtures/app';

test.describe.configure({ timeout: 180_000 });

test.describe('offline', () => {
  test('edits made offline show at once locally and reach other users after reconnecting', async ({ device, track }) => {
    const alice = await deviceFor(device, uniqueUser('alice'), 'signup');
    const bob = await deviceFor(device, uniqueUser('bob'), 'signup');
    const title = uniqueTitle('Offline edits');
    track(await createThread(alice, title, 'online body'));
    await setPublished(alice, true);
    await openFromHome(bob, title);

    await alice.context().setOffline(true);
    const note = `written offline ${Date.now()}`;
    await withoutReload(alice, async () => {
      await postComment(alice, note);
      await retitle(alice, `${title} (offline)`);
      await expect(titleInput(alice)).toHaveValue(`${title} (offline)`);
    });
    await bob.waitForTimeout(3000);
    await expect(comment(bob, note)).toHaveCount(0);

    await alice.context().setOffline(false);
    await withoutReload(bob, async () => {
      await expect(comment(bob, note)).toBeVisible({ timeout: 90_000 });
      await expect(titleInput(bob)).toHaveValue(`${title} (offline)`, { timeout: 90_000 });
    });
  });
});
