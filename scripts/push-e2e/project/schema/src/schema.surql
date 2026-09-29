DEFINE ACCESS account ON DATABASE TYPE RECORD
  SIGNUP ( CREATE user SET username = $username, password = crypto::argon2::generate($password) )
  SIGNIN ( SELECT * FROM user WHERE username = $username AND crypto::argon2::compare(password, $password) )
  DURATION FOR TOKEN 1d, FOR SESSION 30d
;

DEFINE TABLE user SCHEMAFULL
PERMISSIONS
  FOR update, delete WHERE $access = "account" AND id = $auth.id
  FOR create, select WHERE true;
DEFINE FIELD username ON TABLE user TYPE string;
DEFINE INDEX unique_username ON TABLE user FIELDS username UNIQUE;
DEFINE FIELD password ON TABLE user TYPE string PERMISSIONS FOR select WHERE false;

DEFINE TABLE message SCHEMAFULL
PERMISSIONS
  FOR select WHERE $auth.id = sender OR $auth.id = recipient
  FOR create WHERE $auth.id = sender
  FOR update, delete WHERE false;
DEFINE FIELD sender ON TABLE message TYPE record<user>;
DEFINE FIELD recipient ON TABLE message TYPE record<user>;
DEFINE FIELD text ON TABLE message TYPE string;

DEFINE TABLE alert SCHEMAFULL
PERMISSIONS
  FOR select, update WHERE owner = $auth.id
  FOR create WHERE $auth.id != NONE
  FOR delete WHERE false;
DEFINE FIELD owner ON TABLE alert TYPE record<user>;
DEFINE FIELD title ON TABLE alert TYPE string;
DEFINE FIELD seen ON TABLE alert TYPE bool DEFAULT false;
