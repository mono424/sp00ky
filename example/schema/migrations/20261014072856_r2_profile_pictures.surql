-- Migration: r2_profile_pictures
-- Created: 2026-10-14

-- `profile_pictures` is now `BACKEND "r2"`: the SSP keeps its files (R2 on
-- Cloudflare, a directory under `spky dev`), because the demo's database is
-- Surreal Cloud, whose memory buckets lose every file on a restart. SurrealDB
-- keeps the bucket as an empty memory bucket (file pointers need it), and its
-- permission clause moves into `fn::_00_bucket::profile_pictures`, which the
-- SSP calls as the user before every file operation. The old clause's size
-- check is gone: the client enforces `sp00ky:maxSize`.

-- Modified
DEFINE BUCKET OVERWRITE profile_pictures BACKEND 'memory' PERMISSIONS WHERE $action NOTINSIDE ['put'] OR string::ends_with(file::key($file), '.jpg') OR string::ends_with(file::key($file), '.jpeg') OR string::ends_with(file::key($file), '.png') OR string::ends_with(file::key($file), '.gif') COMMENT 'sp00ky:maxSize=5242880';

-- Added
DEFINE FUNCTION OVERWRITE fn::_00_bucket::profile_pictures($action: string, $file: any, $target: any) { RETURN !!($action NOTINSIDE ['put'] OR string::ends_with(file::key($file), '.jpg') OR string::ends_with(file::key($file), '.jpeg') OR string::ends_with(file::key($file), '.png') OR string::ends_with(file::key($file), '.gif')) } PERMISSIONS FULL;
