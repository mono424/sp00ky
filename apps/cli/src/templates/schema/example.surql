-- ##################################################################
-- SCOPES & AUTHENTICATION
-- ##################################################################

DEFINE ACCESS account ON DATABASE TYPE RECORD
  SIGNUP ( CREATE user SET username = $username, password = crypto::argon2::generate($password) )
  SIGNIN ( SELECT * FROM user WHERE username = $username AND crypto::argon2::compare(password, $password) )
  DURATION FOR TOKEN 15m, FOR SESSION 30d
;

-- ##################################################################
-- USER TABLE
-- ##################################################################

DEFINE TABLE user SCHEMAFULL
PERMISSIONS
  FOR update, delete WHERE $access = "account" AND id = $auth.id
  FOR create, select FULL;

DEFINE FIELD username ON TABLE user TYPE string
ASSERT $value != NONE AND string::len($value) > 3
PERMISSIONS
    FOR select FULL
    FOR create WHERE true
    FOR update WHERE $access = "account" AND id = $auth.id;

DEFINE INDEX unique_username ON TABLE user FIELDS username UNIQUE;

DEFINE FIELD password ON TABLE user TYPE string
ASSERT $value != NONE AND string::len($value) > 0
PERMISSIONS
    FOR select WHERE false
    FOR create WHERE true
    FOR update WHERE $access = "account" AND id = $auth.id;

DEFINE FIELD created_at ON TABLE user TYPE datetime
VALUE time::now()
PERMISSIONS
    FOR select WHERE false
    FOR create WHERE true
    FOR update WHERE $access = "account" AND id = $auth.id;

DEFINE FIELD profile_picture ON TABLE user TYPE option<string>
PERMISSIONS
    FOR select FULL
    FOR create, update WHERE $access = "account" AND id = $auth.id;

-- ##################################################################
-- THREAD TABLE
-- ##################################################################

DEFINE TABLE thread SCHEMAFULL
  PERMISSIONS
    FOR select FULL
    FOR update, delete, create WHERE $access = "account" AND author.id = $auth.id
;

DEFINE FIELD title ON TABLE thread TYPE string
    ASSERT $value != NONE AND string::len($value) > 0 AND string::len($value) <= 200;

DEFINE FIELD content ON TABLE thread TYPE string
    ASSERT $value != NONE AND string::len($value) > 0;

DEFINE FIELD title_suggestion ON TABLE thread TYPE option<string>;

DEFINE FIELD content_suggestion ON TABLE thread TYPE option<string>;

DEFINE FIELD author ON TABLE thread TYPE record<user>; -- @parent

DEFINE FIELD created_at ON TABLE thread TYPE datetime
    VALUE time::now();

DEFINE FIELD active ON TABLE thread TYPE bool VALUE $value OR false;

-- ##################################################################
-- COMMENT TABLE
-- ##################################################################

DEFINE TABLE comment SCHEMAFULL
  PERMISSIONS
    FOR select FULL
    FOR update, delete, create WHERE $access = "account" AND author.id = $auth.id
;

DEFINE FIELD thread ON TABLE comment TYPE record<thread>; -- @parent

DEFINE FIELD content ON TABLE comment TYPE string
    ASSERT $value != NONE AND string::len($value) > 0;

DEFINE FIELD author ON TABLE comment TYPE record<user>;

DEFINE FIELD created_at ON TABLE comment TYPE datetime
    VALUE time::now();

-- ##################################################################
-- RELATION TABLES
-- ##################################################################

DEFINE TABLE commented_on SCHEMAFULL TYPE RELATION
  FROM comment TO thread
  PERMISSIONS FOR select FULL;

DEFINE EVENT comment_created ON TABLE comment WHEN $event = "CREATE" THEN
  RELATE ($after.id)->commented_on->($after.thread)
;
