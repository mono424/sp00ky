import { UPSTREAM, upstreamHeaders } from '../fixtures/target';
import {
  test,
  expect,
  registerUser,
  loginUser,
  testUser,
  waitForAppReady,
  navigateToThread,
  typeIntoEditor,
  waitForThreadInUpstream,
} from '../fixtures/test-fixtures';

test.describe.serial('Thread CRUD operations', () => {
  const threadTitle = 'E2E Test Thread';
  const threadContent = 'Automated test content for e2e testing';
  const updatedTitle = 'Updated E2E Test Thread';
  const commentContent = 'E2E automated test comment';

  test('creates a new thread', async ({ page }) => {
    // Capture console for diagnosing create-flow failures (e.g.
    // db.create rejection, schema mismatch). Surface errors as test
    // annotations rather than swallowing them.
    page.on('console', (msg) => {
      const t = msg.type();
      if (t === 'error') {
        // eslint-disable-next-line no-console
        console.log(`[browser:${t}] ${msg.text()}`);
      }
    });
    page.on('pageerror', (err) => {
      // eslint-disable-next-line no-console
      console.log(`[browser:pageerror] ${err.message}`);
    });
    page.on('requestfailed', (req) => {
      // eslint-disable-next-line no-console
      console.log(`[browser:reqfail] ${req.method()} ${req.url()} — ${req.failure()?.errorText}`);
    });
    page.on('response', (resp) => {
      if (resp.status() >= 500) {
        // eslint-disable-next-line no-console
        console.log(`[browser:resp${resp.status()}] ${resp.request().method()} ${resp.url()}`);
      }
    });

    await registerUser(page, testUser);

    // Open the create dialog (route: /create-thread renders the dialog).
    await page.getByRole('button', { name: 'New thread' }).click();

    const titleInput = page.locator('#create-title');
    await expect(titleInput).toBeVisible({ timeout: 10_000 });

    await titleInput.fill(threadTitle);
    // `content` is `@crdt @cursor`, rendered via Tiptap/ProseMirror —
    // not a plain textarea. Click the editor to focus and type via the
    // keyboard so LoroSyncPlugin records each keystroke.
    await typeIntoEditor(
      page,
      '.ProseMirror[contenteditable="true"]',
      threadContent
    );

    await page.getByRole('button', { name: 'Publish' }).click();

    // After Publish, the dialog's `handleClose` fires `window.history.back()`
    // and the dialog also calls `navigate('/thread/:id)`. Both run, the
    // back is async, so the URL transiently visits /thread/:id and then
    // pops back to whatever was on top before — usually `/`. Instead of
    // chasing that race, wait for the dialog to close, then enter the
    // thread the same way a user would: click its card on the list.
    // If the dialog stayed open with an error banner, the test would
    // hang here waiting forever — surface that as a clear failure
    // instead of a 15s timeout.
    const errorBanner = page.locator('div.bg-red-500\\/10');
    const dialogClosedOrErrored = Promise.race([
      page.locator('#create-title').waitFor({ state: 'hidden', timeout: 15_000 }).then(() => 'closed' as const),
      errorBanner.waitFor({ state: 'visible', timeout: 15_000 }).then(() => 'error' as const),
    ]);
    const outcome = await dialogClosedOrErrored;
    if (outcome === 'error') {
      const text = (await errorBanner.textContent()) ?? '<no text>';
      throw new Error(`Create-thread dialog surfaced error: ${text}`);
    }

    // Persistence check at the source: poll upstream SurrealDB until the
    // thread shows up. The local list query has lag through DBSP/SSP +
    // sync-up, so a fast `goto('/')` can race the propagation; verifying
    // upstream first decouples the round-trip check from UI timing.
    let upstreamSeen = false;
    for (let i = 0; i < 30 && !upstreamSeen; i++) {
      const resp = await fetch(UPSTREAM.url, {
        method: 'POST',
        headers: upstreamHeaders(),
        body: 'SELECT id, title FROM thread',
      });
      const body = (await resp.json()) as Array<{ result?: Array<{ title?: string }> }>;
      const rows = body[0]?.result ?? [];
      if (rows.some((r) => r.title === threadTitle)) {
        upstreamSeen = true;
        break;
      }
      await page.waitForTimeout(500);
    }
    if (!upstreamSeen) {
      throw new Error(
        `Thread "${threadTitle}" never appeared in upstream after Publish. The dialog closed cleanly, so db.create resolved, but the row didn't sync up — likely a permission rule or schema mismatch on remote.`
      );
    }

    await page.goto('/');
    await waitForAppReady(page);
    await expect(page.getByText(threadTitle)).toBeVisible({ timeout: 15_000 });
    // The card's click handler is on `onMouseDown` (not click), bound on
    // the parent `<div data-thread-index>` not the heading. Clicking the
    // heading text doesn't always reach the parent's mousedown via
    // Playwright's auto-targeting, so target the card root explicitly.
    // Filter by title text instead of hardcoding index 0 — the home list
    // sort is alphabetical, so prior tests in the suite may have left
    // threads that occupy earlier positions.
    const card = page
      .locator('[data-thread-index]')
      .filter({ has: page.getByText(threadTitle, { exact: true }) });
    await card.first().click();
    await page.waitForURL(/\/thread\//, { timeout: 10_000 });

    // Local check: detail page mounts with the title in the editable input.
    const detailTitleInput = page.locator(
      'input[placeholder="Untitled"]'
    );
    await expect(detailTitleInput).toHaveValue(threadTitle, {
      timeout: 15_000,
    });

    // Content editor mounts (CollaborativeEditor renders ProseMirror).
    await expect(
      page.locator('.ProseMirror[contenteditable="true"]').first()
    ).toBeVisible({ timeout: 15_000 });

    // Persistence check: reload, both fields survive.
    await page.reload();
    await waitForAppReady(page);

    await expect(detailTitleInput).toHaveValue(threadTitle, {
      timeout: 15_000,
    });
    // The content editor's text matches what we typed (decoded via
    // CrdtField + LoroDoc inside ProseMirror).
    await expect(
      page.locator('.ProseMirror[contenteditable="true"]').first()
    ).toContainText(threadContent, { timeout: 15_000 });
  });

  test('updates a thread title', async ({ page }) => {
    await loginUser(page, testUser);
    await navigateToThread(page, threadTitle);

    const titleInput = page.locator('input[placeholder="Untitled"]');
    await expect(titleInput).toBeVisible({ timeout: 15_000 });

    await titleInput.fill(updatedTitle);
    // Local check: input shows the new value immediately.
    await expect(titleInput).toHaveValue(updatedTitle);

    // The title is a `@crdt text` field: the edit goes out as a log row and
    // reaches the column with the SSP fold. Gate on upstream rather than
    // waiting blindly, so the reload below starts from the folded title.
    await waitForThreadInUpstream(page, updatedTitle);

    // Persistence check: reload, title survives.
    await page.reload();
    await waitForAppReady(page);

    await expect(titleInput).toHaveValue(updatedTitle, { timeout: 15_000 });

    // List view also reflects the update.
    await page.goto('/');
    await waitForAppReady(page);
    await expect(page.getByText(updatedTitle)).toBeVisible({
      timeout: 15_000,
    });
    await expect(
      page.getByText(threadTitle, { exact: true })
    ).not.toBeVisible();
  });

  test('creates a comment on the thread', async ({ page }) => {
    await loginUser(page, testUser);
    await navigateToThread(page, updatedTitle);

    const commentTextarea = page.locator('#comment-textarea');
    await expect(commentTextarea).toBeVisible({ timeout: 15_000 });

    await commentTextarea.fill(commentContent);
    await page.getByRole('button', { name: 'Reply' }).click();

    // Submit clears the textarea.
    await expect(commentTextarea).toHaveValue('', { timeout: 10_000 });

    // Persistence at the source: poll upstream SurrealDB directly for
    // the comment row. The create flow is a local `db.create('comment',…)`
    // followed by sync-up; verifying upstream first decouples the
    // round-trip check from the thread detail's `.related('comments')`
    // sub-query, which has a separate reactivity story.
    let upstreamSeen = false;
    for (let i = 0; i < 30 && !upstreamSeen; i++) {
      const resp = await fetch(UPSTREAM.url, {
        method: 'POST',
        headers: upstreamHeaders(),
        body: 'SELECT id, content FROM comment',
      });
      const body = (await resp.json()) as Array<{ result?: Array<{ content?: string }> }>;
      const rows = body[0]?.result ?? [];
      if (rows.some((r) => r.content === commentContent)) {
        upstreamSeen = true;
        break;
      }
      await page.waitForTimeout(500);
    }
    if (!upstreamSeen) {
      throw new Error(
        `Comment "${commentContent}" never appeared in upstream after Reply. db.create likely didn't sync up — check permissions or schema on the comment table.`
      );
    }

    // UI surface check: the thread's `.related('comments', …)` sub-query
    // should pick the new row up after sync-down. Reload a few times if
    // the first render misses it (the sub-query depends on the local
    // comment row landing, which can lag by a tick after upstream confirms).
    let uiSeen = false;
    for (let i = 0; i < 5 && !uiSeen; i++) {
      await page.reload();
      await waitForAppReady(page);
      try {
        await expect(page.getByText(commentContent)).toBeVisible({ timeout: 5_000 });
        uiSeen = true;
      } catch {
        await page.waitForTimeout(1000);
      }
    }
    if (!uiSeen) {
      throw new Error(
        `Comment "${commentContent}" is in upstream but the thread detail's .related('comments') sub-query never surfaced it after 5 reloads. Local sync-down or reactivity gap on comment table.`
      );
    }
  });

  test('shows the thread in the list and navigates back to detail', async ({ page }) => {
    await loginUser(page, testUser);
    await page.goto('/');
    await waitForAppReady(page);

    // List render: title decoded from the row's plain string column.
    await expect(page.getByText(updatedTitle)).toBeVisible({
      timeout: 15_000,
    });

    await page.getByText(updatedTitle).first().click();
    await page.waitForURL(/\/thread\//, { timeout: 10_000 });

    await expect(
      page.locator('input[placeholder="Untitled"]')
    ).toHaveValue(updatedTitle, { timeout: 15_000 });

    // Persistence check: reload still on the same thread.
    await page.reload();
    await waitForAppReady(page);

    await expect(
      page.locator('input[placeholder="Untitled"]')
    ).toHaveValue(updatedTitle, { timeout: 15_000 });
  });
});
