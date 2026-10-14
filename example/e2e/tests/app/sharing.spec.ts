import {
  test,
  expect,
  uniqueUser,
  uniqueTitle,
  deviceFor,
  createThread,
  setPublished,
  retitle,
  titleInput,
  editor,
  upstream,
  withoutReload,
} from '../../fixtures/app';
import type { Page } from '@playwright/test';

test.describe.configure({ timeout: 150_000 });

/** Make an invite link from the open thread's Share dialog; returns its JWT. */
async function inviteLink(page: Page, key: string): Promise<string> {
  await page.getByRole('button', { name: 'More options' }).click();
  await page.getByRole('button', { name: 'Share', exact: true }).click();
  await page.getByRole('button', { name: 'New link' }).click();
  let jwt = '';
  await expect
    .poll(async () => (jwt = (await upstream<string>(`SELECT VALUE jwt FROM share_link WHERE thread = type::record('thread', '${key}')`))[0] ?? ''), { timeout: 20_000 })
    .not.toBe('');
  // The dialog lists it too, ready to copy.
  await expect(page.getByRole('button', { name: 'Copy' })).toBeVisible();
  await page.getByRole('button', { name: 'Close' }).first().click();
  return jwt;
}

test.describe('sharing', () => {
  test('an invite makes the recipient a collaborator, until the author removes them', async ({ device, track }) => {
    const alice = await deviceFor(device, uniqueUser('alice'), 'signup');
    const bob = await deviceFor(device, uniqueUser('bob'), 'signup');
    const title = uniqueTitle('Together');
    const key = await createThread(alice, title, 'shared body');
    track(key);
    await setPublished(alice, true);
    const jwt = await inviteLink(alice, key);

    // Opening the link (a full load, like following it from a message).
    await bob.goto(`/invite/${jwt}`);
    await bob.waitForURL(new RegExp(`/thread/${key}`), { timeout: 20_000 });
    await expect(titleInput(bob)).toHaveValue(title, { timeout: 20_000 });

    // The collaborator edits; the author sees it live, and sees who joined.
    await retitle(bob, `${title} +bob`);
    await editor(bob).click();
    await bob.keyboard.press('End');
    await bob.keyboard.type(' (bob was here)');
    await withoutReload(alice, async () => {
      await expect(titleInput(alice)).toHaveValue(`${title} +bob`, { timeout: 20_000 });
      await expect(editor(alice)).toContainText('(bob was here)', { timeout: 20_000 });
    });
    await alice.reload();
    const remove = alice.getByRole('button', { name: 'Remove collaborator' });
    await expect(remove).toHaveCount(1, { timeout: 20_000 });

    // Removed: the author's list of collaborators empties, and the
    // collaborator is a reader again.
    await remove.click({ force: true });
    await expect(remove).toHaveCount(0, { timeout: 15_000 });
    await expect
      .poll(async () => (await upstream(`SELECT id FROM collaborates_on WHERE out = type::record('thread', '${key}')`)).length, { timeout: 20_000 })
      .toBe(0);
    await bob.reload();
    await expect(titleInput(bob)).toHaveValue(`${title} +bob`, { timeout: 20_000 });
    await expect(titleInput(bob)).toHaveAttribute('readonly', '');
  });

  test('a made-up invite link is refused with a message', async ({ device }) => {
    const bob = await deviceFor(device, uniqueUser('bob'), 'signup');
    const forged = ['eyJhbGciOiJFZERTQSJ9', Buffer.from(JSON.stringify({ sub: 'thread:nope', jti: 'x' })).toString('base64url'), 'sig'].join('.');
    await bob.goto(`/invite/${forged}`);
    await expect(bob.getByText('This invite link is invalid or has expired.')).toBeVisible({ timeout: 20_000 });
    await bob.goto('/invite/not-a-jwt');
    await expect(bob.getByText('This invite link is malformed.')).toBeVisible({ timeout: 20_000 });
  });
});
