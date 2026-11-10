-- A schema that exercises every codegen path: relations both ways, access
-- params, optional and required fields, records, datetimes, arrays, objects,
-- keyword and nested field names, and the channel, ephemeral, crdt, parent and
-- nosync annotations.

DEFINE ACCESS account ON DATABASE TYPE RECORD
  SIGNUP (
    CREATE user SET
      email = $email,
      password = crypto::argon2::generate($password),
      display_name = $display_name
  )
  SIGNIN ( SELECT * FROM user WHERE email = $email AND crypto::argon2::compare(password, $password) )
  DURATION FOR TOKEN 15m, FOR SESSION 30d;

DEFINE TABLE user SCHEMAFULL PERMISSIONS FOR select WHERE true;
DEFINE FIELD email ON user TYPE string ASSERT string::is::email($value);
DEFINE FIELD display_name ON user TYPE option<string>;
DEFINE FIELD created_at ON user TYPE datetime DEFAULT time::now();
-- @nosync
-- the password hash never leaves the server
DEFINE FIELD password ON user TYPE string;

DEFINE TABLE thread SCHEMAFULL PERMISSIONS FOR select WHERE true;
DEFINE FIELD title ON thread TYPE string;
DEFINE FIELD author ON thread TYPE record<user>;
DEFINE FIELD published ON thread TYPE bool DEFAULT false;
DEFINE FIELD score ON thread TYPE int DEFAULT 0;
DEFINE FIELD rating ON thread TYPE option<float>;
DEFINE FIELD tags ON thread TYPE array<string> DEFAULT [];
DEFINE FIELD watchers ON thread TYPE option<array<record<user>>>;
DEFINE FIELD edited_at ON thread TYPE option<datetime>;
DEFINE FIELD meta ON thread TYPE option<object> FLEXIBLE;
DEFINE FIELD meta.color ON thread TYPE option<string>;
DEFINE FIELD type ON thread TYPE string DEFAULT "post";
-- @crdt text
DEFINE FIELD body ON thread TYPE option<string>;

DEFINE TABLE comment SCHEMAFULL PERMISSIONS FOR select WHERE true;
DEFINE FIELD body ON comment TYPE string;
DEFINE FIELD thread ON comment TYPE record<thread>; -- @parent
DEFINE FIELD author ON comment TYPE record<user>;
DEFINE FIELD created_at ON comment TYPE datetime
  DEFAULT time::now()
  READONLY;

-- @channel
DEFINE TABLE call SCHEMAFULL PERMISSIONS FULL;
DEFINE FIELD room ON call TYPE string;
-- @ephemeral
DEFINE FIELD cursor_pos ON call TYPE option<object>;

-- @nosync
DEFINE TABLE signup_code SCHEMAFULL PERMISSIONS NONE;
DEFINE FIELD code ON signup_code TYPE string;
