// @ts-ignore: the package has no Node types; Playwright runs this under Node.
import { spawn } from 'node:child_process';
import {
  test,
  expect,
  uniqueUser,
  uniqueTitle,
  deviceFor,
  createThread,
  setPublished,
  goHome,
  openFromHome,
  card,
  comment,
  postComment,
  expectSignedIn,
  upstream,
} from '../../fixtures/app';
import { UPSTREAM_ORIGIN } from '../../fixtures/target';

// `spky db upgrade` against a deployed project, with people using it: signed-in
// browsers on two accounts keep writing and watching while the database moves.
// Nobody may be signed out, no page may reload, every write that was made must
// arrive exactly once, and live updates must keep flowing.
//
// Only runs when told how to upgrade, because it changes the project for good:
//   E2E_UPGRADE_CMD      shell command that upgrades and returns once it is done
//   E2E_UPGRADE_VERSION  what GET /version must say afterwards
// plus the E2E_BASE_URL family of fixtures/target.ts. A command can chain steps,
// e.g. `spky db upgrade --confirm x && sleep 30 && spky db upgrade cancel && ...`.

declare const process: { env: Record<string, string | undefined> };

const CMD = process.env.E2E_UPGRADE_CMD;
const VERSION = process.env.E2E_UPGRADE_VERSION;

test.skip(!CMD, 'E2E_UPGRADE_CMD is not set');
test.describe.configure({ timeout: 40 * 60_000 });

/** Marks a page's document, so a reload can be told afterwards. */
async function markDocument(page: { evaluate: (fn: () => void) => Promise<void> }) {
  await page.evaluate(() => {
    (window as unknown as { __e2eUpgrade: boolean }).__e2eUpgrade = true;
  });
}

async function documentKept(page: { evaluate: (fn: () => boolean) => Promise<boolean> }) {
  return page.evaluate(() => (window as unknown as { __e2eUpgrade?: boolean }).__e2eUpgrade === true);
}

/** Runs the upgrade; the promise ends with its exit code. */
function runUpgrade(cmd: string): Promise<number> {
  return new Promise((resolve) => {
    const child = spawn('sh', ['-c', cmd], { stdio: 'inherit' });
    child.on('exit', (code: number | null) => resolve(code ?? 1));
  });
}

test('people keep working through a database upgrade', async ({ device, track }) => {
  const alice = uniqueUser('alice');
  const a1 = await deviceFor(device, alice, 'signup');
  const a2 = await deviceFor(device, alice, 'signin');
  const bobAccount = uniqueUser('bob');
  const bob = await deviceFor(device, bobAccount, 'signup');
  await goHome(a2);
  await goHome(bob);

  // A thread both accounts look at. Publishing brings it to Bob live.
  const title = uniqueTitle('Upgrade');
  const key = await createThread(a1, title, 'written before the upgrade');
  track(key);
  await setPublished(a1, true);
  await expect(card(bob, title)).toBeVisible({ timeout: 30_000 });
  await openFromHome(bob, title);
  await openFromHome(a2, title);

  for (const page of [a1, a2, bob]) await markDocument(page);
  for (const [name, page] of [['a1', a1], ['a2', a2], ['bob', bob]] as const) {
    page.on('console', (m) => {
      if (m.type() === 'error' || m.type() === 'warning') console.log(`[${name}] ${m.type()}: ${m.text().slice(0, 300)}`);
    });
    page.on('response', async (r) => {
      if (r.status() >= 500) console.log(`[${name}] HTTP ${r.status()} ${r.request().method()} ${r.url()} ${(await r.text().catch(() => '')).slice(0, 300)}`);
    });
    page.on('pageerror', (e) => console.log(`[${name}] pageerror: ${String(e).slice(0, 300)}`));
  }

  const posted: string[] = [];
  const post = async () => {
    const text = `c${posted.length} ${Date.now().toString(36)}`;
    await postComment(a1, text);
    posted.push(text);
    return text;
  };

  // Live before anything happens.
  const first = await post();
  await expect(comment(bob, first)).toBeVisible({ timeout: 30_000 });

  // The default list is the ten newest; Alice's own view has no limit, so every
  // write she makes can be seen landing. Its first row says it has loaded.
  await a1.getByRole('button', { name: 'Mine', exact: true }).click();
  await expect(comment(a1, first)).toBeVisible({ timeout: 30_000 });

  const startedAt = Date.now();
  let exit: number | null = null;
  const upgrade = runUpgrade(CMD as string).then((code) => {
    exit = code;
    return code;
  });

  // Alice keeps writing from before the cutover to after it, the way a person
  // would: a write the database cannot take yet stays queued in her browser. At
  // this pace, not faster: every comment on a thread re-touches all of its comments
  // in the SSP, so one a second on one thread backs a small database up after about
  // ninety of them, with or without an upgrade.
  let longest = 0;
  while (exit === null) {
    const before = Date.now();
    await post();
    longest = Math.max(longest, Date.now() - before);
    await a1.waitForTimeout(4000);
  }
  const code = await upgrade;
  expect(code, 'the upgrade command failed').toBe(0);
  console.log(`[upgrade] ${posted.length} comments over ${Math.round((Date.now() - startedAt) / 1000)} s; slowest post ${longest} ms`);

  if (VERSION) {
    const version = await (await fetch(`${UPSTREAM_ORIGIN}/version`)).text();
    expect(version.trim()).toBe(VERSION);
  }

  // After it, writes still go through and show up live for the other account.
  for (let i = 0; i < 3; i++) {
    const text = await post();
    try {
      await expect(comment(bob, text)).toBeVisible({ timeout: 60_000 });
    } catch (e) {
      // Where did it stop: the row, the view's edge, or the screen?
      const row = await upstream<{ id: string }>(`SELECT id, created_at FROM comment WHERE content = '${text}';`);
      const bobUser = await upstream<{ id: string }>(`SELECT id FROM user WHERE username = '${bobAccount.username}';`);
      const uid = String(bobUser[0]?.id ?? '').split(':')[1];
      const edges = await upstream(`SELECT count() FROM _00_list_ref_user_${uid} GROUP ALL;`);
      const edge = await upstream(`SELECT id, in, version FROM _00_list_ref_user_${uid} WHERE out = ${row[0]?.id};`);
      const newest = await upstream(`SELECT out, version FROM _00_list_ref_user_${uid} ORDER BY updatedAt DESC LIMIT 3;`);
      console.log('[diag] row', JSON.stringify(row), 'edges', JSON.stringify(edges));
      console.log('[diag] edge for this comment', JSON.stringify(edge));
      console.log('[diag] newest edges', JSON.stringify(newest));
      console.log('[diag] bob sees', (await bob.locator('body').innerText()).slice(0, 260).replace(/\n/g, ' | '));
      // Does a reload bring it? Then the data is fine and only the live path broke.
      await bob.reload();
      await bob.waitForTimeout(8000);
      console.log('[diag] after a reload bob sees it:', await comment(bob, text).count());
      throw e;
    }
  }

  // The newest reached the other screens, which show ten, without anyone
  // reloading. The server check below covers all of them.
  const settle = Date.now();
  for (const text of posted.slice(-10)) {
    await expect(comment(bob, text), `"${text}" did not reach the other account`).toBeVisible({ timeout: 120_000 });
    await expect(comment(a2, text), `"${text}" did not reach the other device`).toBeVisible({ timeout: 120_000 });
  }
  console.log(`[upgrade] the newest comments on every screen ${Math.round((Date.now() - settle) / 1000)} s after the upgrade`);

  // And exactly once on the server.
  const stored = await upstream<{ content: string }>(`SELECT content FROM comment WHERE thread = type::record('thread', '${key}');`);
  const counts = new Map<string, number>();
  for (const row of stored) counts.set(row.content, (counts.get(row.content) ?? 0) + 1);
  for (const text of posted) {
    expect(counts.get(text) ?? 0, `"${text}" on the server`).toBe(1);
  }

  for (const page of [a1, a2, bob]) {
    await expectSignedIn(page);
    expect(await documentKept(page), 'a page reloaded').toBe(true);
  }

  // A fresh sign-in works on the database as it is now.
  const a3 = await deviceFor(device, alice, 'signin');
  await expectSignedIn(a3);
  await goHome(a3);
  await expect(card(a3, title)).toBeVisible({ timeout: 30_000 });
});
