
DEFINE TABLE alert SCHEMAFULL
PERMISSIONS
  FOR select, update WHERE owner = $auth.id
  FOR create WHERE $auth.id != NONE
  FOR delete WHERE false;
DEFINE FIELD owner ON TABLE alert TYPE record<user>;
DEFINE FIELD title ON TABLE alert TYPE string;
DEFINE FIELD seen ON TABLE alert TYPE bool DEFAULT false;
