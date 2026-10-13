# sp00ky cookbook

A short, scannable list of patterns an AI agent (or human) reaches for when writing code against a sp00ky-backed app. Each entry has a one-sentence "when to use," the canonical snippet, and one gotcha.

Render any recipe with:
```
spky recipe <recipe> --table <your-table>
```
or pass `--out path/to/file.tsx` to write the snippet directly.

## Recipes

### `live-list`
**When to use:** you want a reactive, sorted list of rows from a table that updates as records change.
**Render:** `spky recipe live-list --table thread`
**Gotcha:** end the query with `.build()` — `useQuery` will hang on a bare builder.

### `optimistic-mutation`
**When to use:** you're inserting a new record from a UI action (form submit, button click) and want the local cache to update immediately while the mutation drains to the remote.
**Render:** `spky recipe optimistic-mutation --table thread`
**Gotcha:** `db.create` takes a *full* record-ID string (`'thread:abc'`), not `(table, payload)`. Use `Uuid` to mint IDs.

### `crdt-text-field`
**When to use:** a `TYPE string` field is annotated `-- @crdt text` in your schema and several people should edit it at once.
**Render:** `spky recipe crdt-text-field --table note --field body`
**Gotcha:** edit through `useCrdtField(table, () => id, field)` and its `getDoc()`; never put the field in a `db.create` / `db.update` payload (the server owns it). The column itself is the server-written plain text, fine to read with `useQuery` for previews, but it trails live edits until the server folds the edit log. For `@crdt prosemirror` fields bind a ProseMirror editor with `loro-prosemirror` instead of a textarea.

## Related

- See `AGENTS.md` (in your project root or `node_modules/@spooky-sync/*/AGENTS.md`) for the broader mental model and gotchas.
- After editing the schema: `spky generate` then `spky doctor` to confirm everything is in sync.
