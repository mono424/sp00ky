# sp00ky threads e2e

Playwright tests for the example app (`example/app-solid`) against a local
`spky dev` stack: SurrealDB, the SSP and the Vite dev server on
`http://localhost:3006`.

## Running

From `example/`, start the stack against a local database (the committed
`sp00ky.yml` points at the hosted demo; use a local `surrealdb:` block):

```bash
spky dev -y --clean-db
```

Then, from `example/e2e/`:

```bash
pnpm test        # everything (the setup project wipes the local database first)
pnpm test:app    # only the app suite below
```

Tests run one at a time (`workers: 1`): several of them drive two or three
browsers against one shared database.

## The app suite (`tests/app`)

What a person does, through the UI, on as many devices (browser contexts) as
it takes, with `withoutReload` asserting the page never reloads to show the
result. Helpers live in `fixtures/app.ts`.

| File | Covers |
|---|---|
| `auth.spec.ts` | sign-up, sign-in, sign-out in place; switching accounts in one tab without seeing the previous account's private threads; wrong password and taken username; the session across a reload and a new tab |
| `threads.spec.ts` | a private thread on every device of its author and on no one else's; publish and unpublish reaching another user live, an open thread included; title and body edits reaching an open thread and every list; delete removing a thread from every device and open page |
| `comments.spec.ts` | comments coming and going live between users, only the author can delete one; the "Mine" filter |
| `profile.spec.ts` | a new username and a new profile picture showing live on the author's other devices and for other users, in threads, comments and lists |
| `sharing.spec.ts` | an invite link making the recipient a collaborator who edits live, until the author removes them; forged and malformed links refused |
| `tabs.spec.ts` | tabs of one browser sharing one store and seeing each other's writes; closing the leader tab hands the store over and sync keeps working |
| `offline.spec.ts` | edits made offline showing at once and reaching other users after reconnecting |

Each test signs up its own users and deletes the threads it created
(`track`), so the home list (ten threads) stays readable for the next one.

## Other specs (`tests/*.spec.ts`)

Older regression tests for specific sync bugs: CRDT merging and folds, the
cross-session publication paths, offline reloads (skipped on the dev server,
which registers no service worker), feature flags and their permissions,
orphaned per-user tables, and single-mode SSP behaviour.
