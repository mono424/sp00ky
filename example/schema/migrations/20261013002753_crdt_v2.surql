-- Migration: crdt_v2
-- Created: 2026-10-13T00:28:01.106101+00:00

-- `thread.content` is now a `@crdt prosemirror` field: a plain string the
-- server writes from the folded document. Its edit log, presence and
-- canonical-document tables come with the internal schema.
--
-- Threads written before this migration hold an old-format Loro snapshot
-- object in `content`, which does not convert. Reset them as root with
-- `UPDATE thread SET content = ''` if the database has any.

-- Modified
DEFINE FIELD OVERWRITE content ON thread TYPE string DEFAULT '' VALUE (IF $auth.id != NONE { $before OR '' } ELSE { $value }) PERMISSIONS FULL;
