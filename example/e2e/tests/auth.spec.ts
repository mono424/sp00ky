import {
  test,
  expect,
  registerUser,
  loginUser,
  signOutUser,
  testUser,
  waitForAppReady,
} from '../fixtures/test-fixtures';

test.describe.serial('Authentication flows', () => {
  test('shows landing page when unauthenticated', async ({ page }) => {
    await page.goto('/');
    await waitForAppReady(page);

    await expect(page.getByText('Welcome to sp00ky threads')).toBeVisible();

    // Header has both Sign in + Sign up; landing has Sign in + Create
    // account. The header buttons are stable (the landing variants
    // duplicate them via `openAuth`).
    await expect(
      page.getByRole('button', { name: 'Sign in', exact: true }).first()
    ).toBeVisible();
    await expect(
      page.getByRole('button', { name: 'Sign up', exact: true })
    ).toBeVisible();
  });

  test('registers a new user', async ({ page }) => {
    await registerUser(page, testUser);

    // Local check: authenticated layout shows the "New thread" trigger.
    await expect(
      page.getByRole('button', { name: 'New thread' })
    ).toBeVisible();

    // The unauth landing shouldn't be visible anymore.
    await expect(page.getByText('Welcome to sp00ky threads')).not.toBeVisible();

    // Persistence check: reload should keep us logged in.
    await page.reload();
    await waitForAppReady(page);

    await expect(
      page.getByRole('button', { name: 'New thread' })
    ).toBeVisible({ timeout: 15_000 });
    // Sign out is the easiest "logged in" header marker.
    await expect(
      page.getByRole('button', { name: 'Sign out' })
    ).toBeVisible();
  });

  test('signs out and signs back in', async ({ page }) => {
    await loginUser(page, testUser);

    await signOutUser(page);
    await loginUser(page, testUser);

    // Local check: authenticated layout restored.
    await expect(
      page.getByRole('button', { name: 'New thread' })
    ).toBeVisible();

    // Persistence check: reload still authenticated.
    await page.reload();
    await waitForAppReady(page);

    await expect(
      page.getByRole('button', { name: 'Sign out' })
    ).toBeVisible({ timeout: 15_000 });
    await expect(
      page.getByRole('button', { name: 'New thread' })
    ).toBeVisible();
  });

  test('toggles between sign-in and sign-up modes inside the dialog', async ({ page }) => {
    await page.goto('/');
    await waitForAppReady(page);

    // Open in sign-up mode (header button).
    await page.getByRole('button', { name: 'Sign up', exact: true }).click();

    // Scope to the dialog body (matches the wrapper that holds both the
    // form *and* the toggle button below it). The toggle button is the
    // only one in the page with these long sentence labels — they're
    // unique by text on their own — but we keep the scope tight so a
    // future relabel of "Sign in"/"Create account" elsewhere doesn't
    // make this test ambiguous.
    const dialog = page
      .locator('div')
      .filter({ has: page.locator('#username') })
      .first();

    // Form submit + heading are scoped to the dialog so we don't pick
    // up the header/landing "Sign in" or "Create account" buttons.
    const form = dialog.locator('form');

    await expect(
      form.getByRole('button', { name: 'Create account', exact: true })
    ).toBeVisible();
    await expect(
      dialog.getByRole('button', { name: "Already have an account? Sign in" })
    ).toBeVisible();

    // Toggle to sign-in mode.
    await dialog
      .getByRole('button', { name: "Already have an account? Sign in" })
      .click();
    await expect(
      form.getByRole('button', { name: 'Sign in', exact: true })
    ).toBeVisible();
    await expect(
      dialog.getByRole('button', { name: "Don't have an account? Sign up" })
    ).toBeVisible();

    // Toggle back to sign-up mode.
    await dialog
      .getByRole('button', { name: "Don't have an account? Sign up" })
      .click();
    await expect(
      form.getByRole('button', { name: 'Create account', exact: true })
    ).toBeVisible();
  });
});
