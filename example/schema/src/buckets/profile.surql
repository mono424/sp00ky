-- Uploads are limited to 5.0 MB by the CLIENT (the `sp00ky:maxSize` comment
-- below is read by codegen into the bucket's client config). The engine cannot
-- check an upload's size in a PERMISSIONS clause (it evaluates the clause
-- before the file exists), so do not add a `file::head($file).size` predicate:
-- it reads the size of the file already at that key, which lets any first
-- upload through and blocks legitimate overwrites.
DEFINE BUCKET IF NOT EXISTS profile_pictures BACKEND "memory"
  PERMISSIONS WHERE
    $action NOT IN ['put']
    OR (
      string::ends_with(file::key($file), '.jpg')
      OR string::ends_with(file::key($file), '.jpeg')
      OR string::ends_with(file::key($file), '.png')
      OR string::ends_with(file::key($file), '.gif')
    )
  COMMENT 'sp00ky:maxSize=5242880';
