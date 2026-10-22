import { test, expect } from '@playwright/test';
import {
  registerUser,
  testUser,
  waitForAppReady,
  typeIntoEditor,
  waitForThreadInUpstream,
  waitForThreadContentInUpstream,
  waitForThreadMirror,
} from '../fixtures/test-fixtures';

// Two-client sync tests via the SSP. Both contexts log in as the same
// user so they have author-level read+write on each other's threads
// (the schema's `thread.select` rule allows
// `author.id = $auth.id`, so unpublished threads are visible to the
// same account from another session).
//
// Each test owns its own thread and creates a fresh pair of contexts.
// That keeps the report honest: a single contract failing won't cascade
// into "the others were skipped because describe.serial."

const navigateToThread = async (page: any, title: string) => {
  await page.goto('/');
  await waitForAppReady(page);
  await expect(page.getByText(title, { exact: true })).toBeVisible({
    timeout: 15_000,
  });
  const card = page
    .locator('[data-thread-index]')
    .filter({ has: page.getByText(title, { exact: true }) });
  await card.first().click();
  await page.waitForURL(/\/thread\//, { timeout: 10_000 });
};

const seedThread = async (
  page: any,
  title: string,
  initialContent: string
) => {
  await page.getByRole('button', { name: 'New thread' }).click();
  await page.locator('#create-title').fill(title);
  await typeIntoEditor(
    page,
    '.ProseMirror[contenteditable="true"]',
    initialContent
  );
  await page.getByRole('button', { name: 'Publish' }).click();
  await page
    .locator('#create-title')
    .waitFor({ state: 'hidden', timeout: 15_000 });
  await waitForThreadInUpstream(page, title);
  await waitForThreadContentInUpstream(page, title);
};

test.describe('Multi-client real-time sync', () => {
  // Two sign-ins plus a seeded thread whose CRDT content must reach the
  // server before the second client opens it.
  test.describe.configure({ timeout: 90_000 });

  test('thread created on A appears on B without reload', async ({ browser }) => {
    const title = 'Multi A2B Create Thread';
    const ctxA = await browser.newContext();
    const ctxB = await browser.newContext();
    const pageA = await ctxA.newPage();
    const pageB = await ctxB.newPage();

    try {
      await registerUser(pageA, testUser);
      await registerUser(pageB, testUser);

      // B sits on the home list, ready to react.
      await pageB.goto('/');
      await waitForAppReady(pageB);

      await seedThread(pageA, title, 'first content');

      // B's home list should pick up the new row via the SSP push,
      // gated by the user-scoped `_00_list_ref.auth_id = $auth.id`
      // permission rule. 15s gives the SSP's HTTP register + DBSP
      // step + RELATE round-trip enough margin on a loaded CI box.
      await expect(
        pageB.getByText(title, { exact: true })
      ).toBeVisible({ timeout: 15_000 });
    } finally {
      await ctxA.close();
      await ctxB.close();
    }
  });

  test('title edits propagate from A to B in real-time', async ({ browser }) => {
    const baseTitle = 'Multi A2B Title Base';
    const renamedTitle = 'Multi A2B Title Renamed';
    const ctxA = await browser.newContext();
    const ctxB = await browser.newContext();
    const pageA = await ctxA.newPage();
    const pageB = await ctxB.newPage();

    try {
      await registerUser(pageA, testUser);
      await registerUser(pageB, testUser);

      await seedThread(pageA, baseTitle, 'base content');
      await navigateToThread(pageA, baseTitle);

      // B watches the home list. With cross-session sync working, B
      // should see the seeded base title via the SSP push without
      // a reload.
      await pageB.goto('/');
      await waitForAppReady(pageB);
      await expect(
        pageB.getByText(baseTitle, { exact: true })
      ).toBeVisible({ timeout: 15_000 });

      // A renames the thread.
      const titleInputA = pageA.locator('input[placeholder="Untitled"]');
      await expect(titleInputA).toBeVisible({ timeout: 10_000 });
      await titleInputA.fill(renamedTitle);
      await waitForThreadInUpstream(pageA, renamedTitle);

      // B's list should swap to the new title without reload.
      await expect(
        pageB.getByText(renamedTitle, { exact: true })
      ).toBeVisible({ timeout: 15_000 });
      await expect(
        pageB.getByText(baseTitle, { exact: true })
      ).not.toBeVisible();
    } finally {
      await ctxA.close();
      await ctxB.close();
    }
  });

  test('CRDT content syncs from A to B in real-time', async ({ browser }) => {
    const title = 'Multi A2B CRDT Thread';
    const initial = 'crdt seed';
    const appended = ' — appended by A';
    const ctxA = await browser.newContext();
    const ctxB = await browser.newContext();
    const pageA = await ctxA.newPage();
    const pageB = await ctxB.newPage();

    try {
      await registerUser(pageA, testUser);
      await registerUser(pageB, testUser);

      await seedThread(pageA, title, initial);

      await navigateToThread(pageA, title);
      await navigateToThread(pageB, title);

      // Both editors must be mounted before A starts typing — otherwise
      // B could miss early ops if its LoroSyncPlugin hasn't subscribed.
      const editorA = pageA
        .locator('.ProseMirror[contenteditable="true"]')
        .first();
      const editorB = pageB
        .locator('.ProseMirror[contenteditable="true"]')
        .first();
      await expect(editorA).toBeVisible({ timeout: 15_000 });
      await expect(editorB).toBeVisible({ timeout: 15_000 });
      await expect(editorA).toContainText(initial, { timeout: 15_000 });
      await expect(editorB).toContainText(initial, { timeout: 15_000 });

      // A appends; LoroSyncPlugin captures each keystroke as a CRDT op,
      // and A's own new ops travel as `thread__crdt_log` rows.
      await editorA.click();
      await pageA.keyboard.press('End');
      await pageA.keyboard.type(appended);

      // B's editor applies the op and renders the appended text.
      await expect(editorB).toContainText(appended, { timeout: 30_000 });
    } finally {
      await ctxA.close();
      await ctxB.close();
    }
  });

  test('concurrent typing on A and B converges and survives the server fold', async ({ browser }) => {
    const title = 'Multi A2B Concurrent Thread';
    const seed = 'shared seed';
    const fromA = ' typed on A';
    const fromB = 'typed on B ';
    const ctxA = await browser.newContext();
    const ctxB = await browser.newContext();
    const pageA = await ctxA.newPage();
    const pageB = await ctxB.newPage();

    try {
      await registerUser(pageA, testUser);
      await registerUser(pageB, testUser);

      await seedThread(pageA, title, seed);
      await navigateToThread(pageA, title);
      await navigateToThread(pageB, title);

      const editorA = pageA.locator('.ProseMirror[contenteditable="true"]').first();
      const editorB = pageB.locator('.ProseMirror[contenteditable="true"]').first();
      await expect(editorA).toContainText(seed, { timeout: 15_000 });
      await expect(editorB).toContainText(seed, { timeout: 15_000 });

      // A types at the end and B at the start, at the same time. The old
      // snapshot push kept whichever client wrote last, so one side's
      // words were lost server-side.
      await editorA.click();
      await pageA.keyboard.press('End');
      await editorB.click();
      await pageB.keyboard.press('Home');
      await Promise.all([pageA.keyboard.type(fromA, { delay: 30 }), pageB.keyboard.type(fromB, { delay: 30 })]);

      // The document's text, remote cursor widgets left out: a cursor's
      // username label renders inline, so with the other client's caret
      // inside a word the editor's text read "typed o⁠e2e_testuser⁠n A".
      const docText = (editor: typeof editorA) =>
        editor.evaluate((el) => {
          const copy = el.cloneNode(true) as HTMLElement;
          copy.querySelectorAll('.ProseMirror-loro-cursor').forEach((c) => c.remove());
          return (copy.textContent ?? '').replace(/\u2060/g, '');
        });
      for (const editor of [editorA, editorB]) {
        await expect.poll(() => docText(editor), { timeout: 30_000 }).toContain(fromA);
        await expect.poll(() => docText(editor), { timeout: 30_000 }).toContain(fromB);
      }
      // Same document on both sides.
      await expect.poll(() => docText(editorB), { timeout: 10_000 }).toBe(await docText(editorA));

      // The server folds both edit streams into one document and mirrors
      // its text onto the thread row.
      await waitForThreadMirror(pageA, title, [seed, fromA.trim(), fromB.trim()]);

      // A fresh client opens the folded document with both edits.
      const ctxC = await browser.newContext();
      try {
        const pageC = await ctxC.newPage();
        await registerUser(pageC, testUser);
        await navigateToThread(pageC, title);
        const editorC = pageC.locator('.ProseMirror[contenteditable="true"]').first();
        await expect(editorC).toContainText(fromA, { timeout: 15_000 });
        await expect(editorC).toContainText(fromB, { timeout: 15_000 });
      } finally {
        await ctxC.close();
      }
    } finally {
      await ctxA.close();
      await ctxB.close();
    }
  });

  test('remote cursor labels are visible across clients while A edits', async ({ browser }) => {
    const title = 'Multi A2B Cursor Thread';
    const ctxA = await browser.newContext();
    const ctxB = await browser.newContext();
    const pageA = await ctxA.newPage();
    const pageB = await ctxB.newPage();

    try {
      await registerUser(pageA, testUser);
      await registerUser(pageB, testUser);

      await seedThread(pageA, title, 'cursor seed');

      await navigateToThread(pageA, title);
      await navigateToThread(pageB, title);

      const editorA = pageA
        .locator('.ProseMirror[contenteditable="true"]')
        .first();
      const editorB = pageB
        .locator('.ProseMirror[contenteditable="true"]')
        .first();
      await expect(editorA).toBeVisible({ timeout: 15_000 });
      await expect(editorB).toBeVisible({ timeout: 15_000 });

      // A places its caret. The CollaborativeEditor's presence push is
      // debounced 100ms, so a tiny no-op edit ensures a cursor-move
      // event fires reliably.
      await editorA.click();
      await pageA.keyboard.press('End');
      await pageA.keyboard.type('.');
      await pageA.keyboard.press('Backspace');

      // The plugin renders remote peers as
      //   <span class="ProseMirror-loro-cursor" style="border-color:..">
      //     <div style="background-color:..">username</div>
      //   </span>
      // The local peer's own cursor is not rendered, so on B we see
      // exactly A's. (Same `username` for both, distinct LoroDoc
      // peerIds.)
      const remoteCursor = pageB.locator('.ProseMirror-loro-cursor');
      await expect(remoteCursor.first()).toBeVisible({ timeout: 30_000 });
      await expect(
        remoteCursor.locator('div').first()
      ).toContainText(testUser.username, { timeout: 10_000 });
    } finally {
      await ctxA.close();
      await ctxB.close();
    }
  });
});
