-- Migration: crdt_title
-- Created: 2026-10-13

-- `thread.title` is now a `@crdt text` field: edits from several devices
-- merge. The column holds its plain text, written by the creating client and
-- then by the server's fold. Its edit-log tables come with the internal
-- schema.

-- Modified
DEFINE FIELD OVERWRITE title ON thread TYPE string DEFAULT '' VALUE (IF $auth.id != NONE AND $before != NONE { $before } ELSE { $value OR '' }) PERMISSIONS FULL;
