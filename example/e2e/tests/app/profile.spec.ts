import {
  test,
  expect,
  uniqueUser,
  uniqueTitle,
  deviceFor,
  createThread,
  setPublished,
  openFromHome,
  goHome,
  card,
  comment,
  postComment,
  openProfile,
  changeUsername,
  uploadAvatar,
  avatar,
  headerAvatarSrc,
  expectPicture,
  withoutReload,
  upstream,
} from '../../fixtures/app';

test.describe.configure({ timeout: 150_000 });

test.describe('profile', () => {
  test('a new username shows live wherever other users see the author', async ({ device, track }) => {
    const alice = await deviceFor(device, uniqueUser('alice'), 'signup');
    const bob = await deviceFor(device, uniqueUser('bob'), 'signup');
    const title = uniqueTitle('Signed');
    track(await createThread(alice, title, 'by me'));
    await setPublished(alice, true);
    const note = `note ${Date.now()}`;
    await postComment(alice, note);
    await withoutReload(bob, () => openFromHome(bob, title));

    const renamed = uniqueUser('alice_renamed').username;
    await withoutReload(alice, async () => {
      await openProfile(alice);
      await changeUsername(alice, renamed);
    });
    await withoutReload(bob, async () => {
      await expect(bob.locator('article')).toContainText(renamed, { timeout: 20_000 });
      await expect(comment(bob, note)).toContainText(renamed, { timeout: 20_000 });
      await goHome(bob);
      await expect(card(bob, title)).toContainText(renamed, { timeout: 20_000 });
    });
  });

  // A user who arrives after the upload has nothing cached: the list and the
  // thread must fetch the picture from the server. On the hosted demo this
  // broke unnoticed: its database kept files in memory and lost them on a
  // restart, while every other test watched uploads live from devices that
  // already held the bytes.
  test('a picture reaches users who arrive later, its bytes kept by the SSP rather than SurrealDB', async ({ device, track }) => {
    const alice = await deviceFor(device, uniqueUser('alice'), 'signup');
    const title = uniqueTitle('Later');
    track(await createThread(alice, title, 'pictured'));
    await setPublished(alice, true);
    await openProfile(alice);
    await uploadAvatar(alice, 'avatar-red.png');
    await headerAvatarSrc(alice);

    const dave = await deviceFor(device, uniqueUser('dave'), 'signup');
    await goHome(dave);
    await expect(card(dave, title)).toBeVisible({ timeout: 20_000 });
    await expectPicture(card(dave, title));
    await openFromHome(dave, title);
    await expectPicture(dave.locator('article'));

    // `BACKEND "r2"`: SurrealDB defines the bucket but holds none of its files.
    expect(await upstream('RETURN file::list("profile_pictures");')).toEqual([]);
  });

  test('a new profile picture shows live on every device and for other users', async ({ device, track }) => {
    const aliceUser = uniqueUser('alice');
    const alice = await deviceFor(device, aliceUser, 'signup');
    const laptop = await deviceFor(device, aliceUser, 'signin');
    const bob = await deviceFor(device, uniqueUser('bob'), 'signup');
    const title = uniqueTitle('Portrait');
    track(await createThread(alice, title, 'look at me'));
    await setPublished(alice, true);
    const note = `selfie ${Date.now()}`;
    await postComment(alice, note);
    await openFromHome(laptop, title);
    await openFromHome(bob, title);
    // Lists embed the author as a related row. A list that is off screen when
    // the author changes and shown again afterwards used to keep the old one.
    const carol = await deviceFor(device, uniqueUser('carol'), 'signup');
    await goHome(carol);
    await expect(card(carol, title)).toBeVisible({ timeout: 20_000 });
    let carolBefore = '';
    let ownBefore = '';
    await goHome(alice);
    await expect(card(alice, title)).toBeVisible({ timeout: 20_000 });
    await openProfile(alice);

    // What each place showed before the upload: every one must change.
    const places = [
      { name: 'laptop thread', scope: laptop.locator('article') },
      { name: 'laptop comment', scope: comment(laptop, note) },
      { name: 'laptop header', scope: laptop.locator('header') },
      { name: 'bob thread', scope: bob.locator('article') },
      { name: 'bob comment', scope: comment(bob, note) },
    ];
    const srcOf = async (scope: (typeof places)[number]['scope']) =>
      (await avatar(scope).count()) ? ((await avatar(scope).getAttribute('src')) ?? '') : '';
    let headerBefore = '';
    for (const file of ['avatar-red.png', 'avatar-blue.png'] as const) {
      const before = await Promise.all(places.map((p) => srcOf(p.scope)));
      await card(carol, title).click();
      await expect(carol.locator('article')).toBeVisible({ timeout: 20_000 });
      await withoutReload(alice, async () => {
        await uploadAvatar(alice, file);
        await expect.poll(() => headerAvatarSrc(alice), { timeout: 30_000 }).not.toBe(headerBefore);
      });
      headerBefore = await headerAvatarSrc(alice);
      for (const observer of [laptop, bob, carol]) await observer.evaluate(() => ((window as any).__e2eDocument = 'observer'));
      for (const [i, place] of places.entries()) {
        await expect.poll(() => srcOf(place.scope), { message: `${place.name} after ${file}`, timeout: 30_000 }).not.toBe(before[i]);
        await expectPicture(place.scope);
      }
      // The author's own list, shown again after the upload, in place.
      await withoutReload(alice, async () => {
        await goHome(alice);
        await expect.poll(() => srcOf(card(alice, title)), { message: `own home card after ${file}`, timeout: 30_000 }).not.toBe(ownBefore);
        await expect.poll(() => srcOf(card(alice, title)), { timeout: 30_000 }).toBe(await headerAvatarSrc(alice));
        ownBefore = await srcOf(card(alice, title));
        await openProfile(alice);
      });
      await goHome(carol);
      await expect.poll(() => srcOf(card(carol, title)), { message: `carol's home card after ${file}`, timeout: 30_000 }).not.toBe(carolBefore);
      await expectPicture(card(carol, title));
      carolBefore = await srcOf(card(carol, title));
      for (const observer of [laptop, bob, carol]) {
        expect(await observer.evaluate(() => (window as any).__e2eDocument), 'an observer reloaded').toBe('observer');
      }
    }
  });
});
