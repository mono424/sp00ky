# LIVE feeds misfire when permission rules dereference related records

**Observed on:** SurrealDB `v3.0.5`. Upstream issues `#3602` and `#4026`.

## Symptom

A LIVE query on a table whose `PERMISSIONS FOR select` rule walks through a related record's field — e.g. `out.published = true OR out.author.id = $auth.id` — fails to deliver notifications correctly. The notification path either:

- never fires, or
- fires with stale data (the related record's old field values), or
- fires from the wrong session.

The exact failure mode depends on whether the dereferenced row was updated, deleted, or replaced.

## Reproduction

```surql
DEFINE TABLE comment SCHEMALESS
  PERMISSIONS FOR select
    WHERE thread.published = true
       OR ($access = 'account' AND thread.author.id = $auth.id);
DEFINE FIELD thread ON comment TYPE record<thread>;

-- Authenticated record-user session opens:
LIVE SELECT * FROM comment;

-- Another session creates a thread (published=false), then a comment under it,
-- then flips the thread to published=true.
-- The LIVE on the first session is expected to start delivering comments
-- once the parent thread is published. In practice it doesn't reliably.
```

The dereference-through-`thread.published` is the trigger.

## Workaround

CRDT v2 no longer relies on this: a field's edits are rows of a generated `<table>__crdt_log` table that reach clients through the normal view pipeline (SSP views and `_00_list_ref`) like any synced table, with permissions rebased onto the parent through the row's `doc` link. The old `_00_crdt` / `_00_cursor` meta rows, the parent `_00_rv` bump and `fn::can_update_record` are gone (see `docs/crdt-v2.md`).
