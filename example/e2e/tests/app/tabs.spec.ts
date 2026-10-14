import {
  test,
  expect,
  uniqueUser,
  uniqueTitle,
  deviceFor,
  createThread,
  setPublished,
  goHome,
  card,
  openFromHome,
  comment,
  postComment,
  withoutReload,
} from '../../fixtures/app';
import type { Page } from '@playwright/test';

// Tabs of one browser share one local store (SQLite on OPFS): one leader tab
// owns it and the sync loop, the others work through it.

test.describe.configure({ timeout: 120_000 });

const role = (page: Page) =>
  page.evaluate(() => {
    const w = window as unknown as { __sp00ky__?: { getSp00ky?: () => any } & Record<string, any> };
    const sp = w.__sp00ky__?.getSp00ky?.() ?? w.__sp00ky__;
    return (sp?.runtime?.state?.tabRole as string | undefined) ?? 'unknown';
  });

async function secondTab(first: Page): Promise<Page> {
  const tab = await first.context().newPage();
  await tab.goto('/');
  await expect(tab.getByRole('button', { name: 'Sign out' })).toBeVisible({ timeout: 30_000 });
  return tab;
}

test.describe('tabs', () => {
  test("tabs of one browser share a store and see each other's writes live", async ({ device, track }) => {
    const first = await deviceFor(device, uniqueUser('tabs'), 'signup');
    const second = await secondTab(first);
    await expect.poll(async () => [await role(first), await role(second)].sort().join(','), { timeout: 20_000 }).toBe('follower,leader');
    await goHome(first);

    const title = uniqueTitle('From a tab');
    track(await createThread(second, title, 'written in the other tab'));
    await withoutReload(first, () => expect(card(first, title)).toBeVisible({ timeout: 20_000 }));
    await withoutReload(first, () => openFromHome(first, title));

    const note = `tab note ${Date.now()}`;
    await postComment(first, note);
    await withoutReload(second, () => expect(comment(second, note)).toBeVisible({ timeout: 20_000 }));
  });

  test('closing the leader tab hands the store to another tab, which keeps syncing', async ({ device, track }) => {
    const user = uniqueUser('handoff');
    const first = await deviceFor(device, user, 'signup');
    const second = await secondTab(first);
    await expect.poll(async () => [await role(first), await role(second)].sort().join(','), { timeout: 20_000 }).toBe('follower,leader');
    const [leader, survivor] = (await role(first)) === 'leader' ? [first, second] : [second, first];

    await leader.close();
    await expect.poll(() => role(survivor), { timeout: 30_000 }).toBe('leader');

    // Still live: another user's new thread arrives, and this tab can write.
    const bob = await deviceFor(device, uniqueUser('bob'), 'signup');
    await goHome(survivor);
    const fromBob = uniqueTitle('From bob');
    track(await createThread(bob, fromBob, 'hello'));
    await setPublished(bob, true);
    await withoutReload(survivor, () => expect(card(survivor, fromBob)).toBeVisible({ timeout: 30_000 }));
    const mine = uniqueTitle('After handoff');
    track(await createThread(survivor, mine, 'still works'));
    const laptop = await deviceFor(device, user, 'signin');
    await goHome(laptop);
    await expect(card(laptop, mine)).toBeVisible({ timeout: 30_000 });
  });
});
