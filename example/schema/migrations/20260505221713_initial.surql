-- Migration: initial
-- Created: 2026-05-05T22:17:14.302133+00:00
--
-- SECURITY (edited 2026-10-20): the original generated `DEFINE ACCESS account`
-- carried `WITH JWT ALGORITHM HS512 KEY '[REDACTED]' WITH ISSUER KEY
-- '[REDACTED]'` — SurrealDB's redaction placeholder, not a real key. Applying
-- that set the signing key to the literal string "[REDACTED]", so anyone could
-- forge a token for any user. The clause was removed so SurrealDB mints a
-- random key on apply. If you applied the old version to a real database, see
-- docs/security/redacted-jwt-signing-key.md to rotate.

-- Added
DEFINE ACCESS OVERWRITE account ON DATABASE TYPE RECORD SIGNUP { RETURN (CREATE ONLY user SET username = $username, password = crypto::argon2::generate($password), share_pubkey = $share_pubkey, share_privkey = $share_privkey) } SIGNIN (SELECT * FROM user WHERE username = $username AND crypto::argon2::compare(password, $password)) DURATION FOR TOKEN 15m, FOR SESSION 4w2d;
DEFINE BUCKET OVERWRITE profile_pictures BACKEND 'memory' PERMISSIONS WHERE $action NOTINSIDE ['put'] OR file::head($file).size <= 5242880 AND (string::ends_with(file::key($file), '.jpg') OR string::ends_with(file::key($file), '.jpeg') OR string::ends_with(file::key($file), '.png') OR string::ends_with(file::key($file), '.gif'));
DEFINE TABLE OVERWRITE _00_list_ref TYPE ANY SCHEMALESS PERMISSIONS FOR select WHERE clientId = <string> session::id(), FOR create, update, delete NONE;
DEFINE TABLE OVERWRITE _00_module_state TYPE ANY SCHEMALESS PERMISSIONS FULL;
DEFINE TABLE OVERWRITE _00_query TYPE ANY SCHEMALESS PERMISSIONS FOR select, create, update WHERE clientId = <string> session::id(), FOR delete NONE;
DEFINE TABLE OVERWRITE _00_version TYPE ANY SCHEMALESS PERMISSIONS FOR select WHERE record_id.id != NONE, FOR create, update, delete NONE;
DEFINE TABLE OVERWRITE collaborates_on TYPE RELATION IN user OUT thread SCHEMAFULL PERMISSIONS FOR select WHERE true, FOR create, update WHERE false, FOR delete WHERE $access = 'account' AND (in.id = $auth.id OR out.author.id = $auth.id);
DEFINE TABLE OVERWRITE comment TYPE NORMAL SCHEMAFULL PERMISSIONS FOR select WHERE thread.published = true OR $access = 'account' AND thread.author.id = $auth.id, FOR create, update, delete WHERE $access = 'account' AND author.id = $auth.id;
DEFINE TABLE OVERWRITE commented_on TYPE RELATION IN comment OUT thread SCHEMAFULL PERMISSIONS FOR select WHERE out.published = true OR $access = 'account' AND out.author.id = $auth.id, FOR create, update, delete NONE;
DEFINE TABLE OVERWRITE job TYPE NORMAL SCHEMAFULL PERMISSIONS FOR select, create, update, delete WHERE $access = 'account' AND assigned_to.author.id = $auth.id;
DEFINE TABLE OVERWRITE share_link TYPE NORMAL SCHEMAFULL PERMISSIONS FOR select WHERE issuer.id = $auth.id, FOR create WHERE $access = 'account' AND issuer = $auth.id AND thread.author.id = $auth.id, FOR update, delete WHERE false;
DEFINE TABLE OVERWRITE thread TYPE NORMAL SCHEMAFULL PERMISSIONS FOR select WHERE published = true OR $access = 'account' AND author.id = $auth.id, FOR create, delete WHERE $access = 'account' AND author.id = $auth.id, FOR update WHERE $access = 'account' AND (author.id = $auth.id OR $auth.id INSIDE (SELECT VALUE in FROM collaborates_on WHERE out = $parent.id));
DEFINE TABLE OVERWRITE user TYPE NORMAL SCHEMAFULL PERMISSIONS FOR select, create WHERE true, FOR update, delete WHERE $access = 'account' AND id = $auth.id;
DEFINE FIELD OVERWRITE clientId ON _00_list_ref TYPE string PERMISSIONS FULL;
DEFINE FIELD OVERWRITE in ON _00_list_ref TYPE record PERMISSIONS FULL;
DEFINE FIELD OVERWRITE out ON _00_list_ref TYPE record PERMISSIONS FULL;
DEFINE FIELD OVERWRITE parent ON _00_list_ref TYPE none | record<_00_list_ref> PERMISSIONS FULL;
DEFINE FIELD OVERWRITE parent_rel ON _00_list_ref TYPE none | string PERMISSIONS FULL;
DEFINE FIELD OVERWRITE updatedAt ON _00_list_ref TYPE datetime READONLY VALUE time::now() PERMISSIONS FULL;
DEFINE FIELD OVERWRITE version ON _00_list_ref TYPE int PERMISSIONS FULL;
DEFINE FIELD OVERWRITE clientId ON _00_query TYPE none | string PERMISSIONS FULL;
DEFINE FIELD OVERWRITE createdAt ON _00_query TYPE datetime DEFAULT time::now() READONLY PERMISSIONS FULL;
DEFINE FIELD OVERWRITE errorCount ON _00_query TYPE int DEFAULT 0 PERMISSIONS FULL;
DEFINE FIELD OVERWRITE lastActiveAt ON _00_query TYPE datetime DEFAULT time::now() PERMISSIONS FULL;
DEFINE FIELD OVERWRITE lastIngestLatency ON _00_query TYPE none | float PERMISSIONS FULL;
DEFINE FIELD OVERWRITE materializationP55 ON _00_query TYPE none | float PERMISSIONS FULL;
DEFINE FIELD OVERWRITE materializationP90 ON _00_query TYPE none | float PERMISSIONS FULL;
DEFINE FIELD OVERWRITE materializationP99 ON _00_query TYPE none | float PERMISSIONS FULL;
DEFINE FIELD OVERWRITE registrationTime ON _00_query TYPE none | float PERMISSIONS FULL;
DEFINE FIELD OVERWRITE rowCount ON _00_query TYPE int DEFAULT 0 PERMISSIONS FULL;
DEFINE FIELD OVERWRITE surql ON _00_query TYPE none | string PERMISSIONS FULL;
DEFINE FIELD OVERWRITE ttl ON _00_query TYPE duration PERMISSIONS FULL;
DEFINE FIELD OVERWRITE updateCount ON _00_query TYPE int DEFAULT 0 PERMISSIONS FULL;
DEFINE FIELD OVERWRITE record_id ON _00_version TYPE record PERMISSIONS FULL;
DEFINE FIELD OVERWRITE version ON _00_version TYPE int PERMISSIONS FULL;
DEFINE FIELD OVERWRITE in ON collaborates_on TYPE record<user> PERMISSIONS FULL;
DEFINE FIELD OVERWRITE out ON collaborates_on TYPE record<thread> PERMISSIONS FULL;
DEFINE FIELD OVERWRITE author ON comment TYPE record<user> PERMISSIONS FULL;
DEFINE FIELD OVERWRITE content ON comment TYPE string ASSERT $value != NONE AND string::len($value) > 0 PERMISSIONS FULL;
DEFINE FIELD OVERWRITE created_at ON comment TYPE datetime VALUE time::now() PERMISSIONS FULL;
DEFINE FIELD OVERWRITE thread ON comment TYPE record<thread> PERMISSIONS FULL;
DEFINE FIELD OVERWRITE in ON commented_on TYPE record<comment> PERMISSIONS FULL;
DEFINE FIELD OVERWRITE out ON commented_on TYPE record<thread> PERMISSIONS FULL;
DEFINE FIELD OVERWRITE assigned_to ON job TYPE record<thread> PERMISSIONS FOR select, create WHERE true, FOR update WHERE false;
DEFINE FIELD OVERWRITE created_at ON job TYPE datetime VALUE time::now() PERMISSIONS FOR select, create WHERE true, FOR update WHERE false;
DEFINE FIELD OVERWRITE errors ON job TYPE array<object> DEFAULT ALWAYS [] PERMISSIONS FOR select, update WHERE false, FOR create WHERE true;
DEFINE FIELD OVERWRITE max_retries ON job TYPE int DEFAULT ALWAYS 3 PERMISSIONS FULL;
DEFINE FIELD OVERWRITE path ON job TYPE string PERMISSIONS FOR select, create WHERE true, FOR update WHERE false;
DEFINE FIELD OVERWRITE payload ON job TYPE any PERMISSIONS FOR select, create WHERE true, FOR update WHERE false;
DEFINE FIELD OVERWRITE retries ON job TYPE int DEFAULT ALWAYS 0 PERMISSIONS FOR select, create WHERE true, FOR update WHERE false;
DEFINE FIELD OVERWRITE retry_strategy ON job TYPE string DEFAULT ALWAYS 'linear' ASSERT $value INSIDE ['linear', 'exponential'] PERMISSIONS FOR select, create WHERE true, FOR update WHERE false;
DEFINE FIELD OVERWRITE status ON job TYPE string DEFAULT ALWAYS 'pending' ASSERT $value INSIDE ['pending', 'processing', 'success', 'failed'] PERMISSIONS FOR select, create WHERE true, FOR update WHERE false;
DEFINE FIELD OVERWRITE updated_at ON job TYPE datetime DEFAULT ALWAYS time::now() PERMISSIONS FOR select, create WHERE true, FOR update WHERE false;
DEFINE FIELD OVERWRITE created_at ON share_link TYPE datetime VALUE time::now() PERMISSIONS FULL;
DEFINE FIELD OVERWRITE exp ON share_link TYPE datetime PERMISSIONS FULL;
DEFINE FIELD OVERWRITE issuer ON share_link TYPE record<user> VALUE $auth.id PERMISSIONS FULL;
DEFINE FIELD OVERWRITE jti ON share_link TYPE string PERMISSIONS FULL;
DEFINE FIELD OVERWRITE jwt ON share_link TYPE string PERMISSIONS FULL;
DEFINE FIELD OVERWRITE thread ON share_link TYPE record<thread> PERMISSIONS FULL;
DEFINE FIELD OVERWRITE active ON thread TYPE bool VALUE $value OR false PERMISSIONS FULL;
DEFINE FIELD OVERWRITE author ON thread TYPE record<user> PERMISSIONS FULL;
DEFINE FIELD OVERWRITE content ON thread TYPE none | object FLEXIBLE PERMISSIONS FULL;
DEFINE FIELD OVERWRITE content_suggestion ON thread TYPE none | string PERMISSIONS FULL;
DEFINE FIELD OVERWRITE created_at ON thread TYPE datetime VALUE time::now() PERMISSIONS FULL;
DEFINE FIELD OVERWRITE published ON thread TYPE bool VALUE $value OR false PERMISSIONS FULL;
DEFINE FIELD OVERWRITE title ON thread TYPE string PERMISSIONS FULL;
DEFINE FIELD OVERWRITE title_suggestion ON thread TYPE none | string PERMISSIONS FULL;
DEFINE FIELD OVERWRITE created_at ON user TYPE datetime VALUE time::now() PERMISSIONS FOR select WHERE false, FOR create WHERE true, FOR update WHERE $access = 'account' AND id = $auth.id;
DEFINE FIELD OVERWRITE password ON user TYPE string ASSERT $value != NONE AND string::len($value) > 0 PERMISSIONS FOR select WHERE false, FOR create WHERE true, FOR update WHERE $access = 'account' AND id = $auth.id;
DEFINE FIELD OVERWRITE profile_picture ON user TYPE none | string PERMISSIONS FOR select WHERE true, FOR create, update WHERE $access = 'account' AND id = $auth.id;
DEFINE FIELD OVERWRITE share_privkey ON user TYPE none | string PERMISSIONS FOR select, update WHERE $access = 'account' AND id = $auth.id, FOR create WHERE true;
DEFINE FIELD OVERWRITE share_pubkey ON user TYPE none | string PERMISSIONS FOR select, create WHERE true, FOR update WHERE $access = 'account' AND id = $auth.id;
DEFINE FIELD OVERWRITE username ON user TYPE string ASSERT $value != NONE AND string::len($value) > 3 PERMISSIONS FOR select, create WHERE true, FOR update WHERE $access = 'account' AND id = $auth.id;
DEFINE INDEX OVERWRITE idx_record_id ON _00_version FIELDS record_id UNIQUE;
DEFINE INDEX OVERWRITE unique_collab ON collaborates_on FIELDS in, out UNIQUE;
DEFINE INDEX OVERWRITE unique_share_jti ON share_link FIELDS jti UNIQUE;
DEFINE INDEX OVERWRITE unique_username ON user FIELDS username UNIQUE;
DEFINE EVENT OVERWRITE _00_dbsp_cleanup ON _00_query WHEN $event = 'DELETE' THEN { LET $payload = { id: <string> $before.id }; LET $result = http::post($sp00ky_endpoint + '/view/unregister', $payload, { Authorization: 'Bearer ' + $sp00ky_secret }); };
DEFINE EVENT OVERWRITE comment_created ON comment WHEN $event = 'CREATE' THEN (RELATE ($after.id) -> commented_on -> ($after.thread));
