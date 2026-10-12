-- ##################################################################
-- SCOPES & AUTHENTICATION
-- ##################################################################

DEFINE ACCESS account ON DATABASE TYPE RECORD
  SIGNUP {
    -- When the browser fails to generate a keypair (e.g. WebCrypto Ed25519
    -- not yet available) we still let signup succeed; the user can rotate
    -- a key in via UPDATE later, they just can't issue share links until
    -- they do.
    RETURN CREATE ONLY user SET
      username      = $username,
      password      = crypto::argon2::generate($password),
      share_pubkey  = $share_pubkey,
      share_privkey = $share_privkey;
  }
  SIGNIN ( SELECT * FROM user WHERE username = $username AND crypto::argon2::compare(password, $password) )
  DURATION FOR TOKEN 15m, FOR SESSION 30d
;

-- ##################################################################
-- USER TABLE
-- ##################################################################

DEFINE TABLE user SCHEMAFULL
PERMISSIONS
  FOR update, delete WHERE $access = "account" AND id = $auth.id
  FOR create, select WHERE true;

DEFINE FIELD username ON TABLE user TYPE string
ASSERT $value != NONE AND string::len($value) > 3
PERMISSIONS
    FOR select WHERE true
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
    FOR select WHERE true
    FOR create, update WHERE $access = "account" AND id = $auth.id;

-- Ed25519 public key (PEM SPKI). Read by anyone — used to verify share-link
-- JWTs the user has signed.
DEFINE FIELD share_pubkey ON TABLE user TYPE option<string>
PERMISSIONS
    FOR select WHERE true
    FOR create WHERE true
    FOR update WHERE $access = "account" AND id = $auth.id;

-- Matching Ed25519 private key (PEM PKCS8). Readable only by the owning
-- user, like `password` but author-scoped instead of always-denied.
DEFINE FIELD share_privkey ON TABLE user TYPE option<string>
PERMISSIONS
    FOR select WHERE $access = "account" AND id = $auth.id
    FOR create WHERE true
    FOR update WHERE $access = "account" AND id = $auth.id;

-- ##################################################################
-- THREAD TABLE
-- ##################################################################

-- Permission shape constraint: the SSP's predicate parser doesn't accept
-- subqueries (packages/ssp/src/converter.rs:498-504) and its filter
-- vocabulary has no IN/contains operator (operator/predicate.rs). So we
-- can't gate SELECT on `$auth.id IN (SELECT … FROM collaborates_on)` —
-- the SSP would fail closed and the materialized view would never reach
-- a collaborator. Keep SELECT to author-only for now; collaborator
-- visibility is recovered through the `collaborates_on`-driven UPDATE
-- rule and an explicit client-side query for invited threads.
DEFINE TABLE thread SCHEMAFULL
  PERMISSIONS
    FOR select WHERE published = true OR ($access = "account" AND author.id = $auth.id)
    FOR create WHERE $access = "account" AND author.id = $auth.id
    FOR update WHERE $access = "account"
      AND (author.id = $auth.id
           OR $auth.id IN (SELECT VALUE in FROM collaborates_on WHERE out = $parent.id))
    FOR delete WHERE $access = "account" AND author.id = $auth.id
;

DEFINE FIELD title ON TABLE thread TYPE string;

-- @crdt prosemirror
-- @cursor
-- The thread body is a collaborative ProseMirror document. This field holds
-- its plain text, written by the server whenever it folds the edit log, so it
-- can be listed, searched and filtered. Edit it with `useCrdtField`; a client
-- never writes it directly.
DEFINE FIELD content ON TABLE thread TYPE string;

DEFINE FIELD title_suggestion ON TABLE thread TYPE option<string>;

DEFINE FIELD content_suggestion ON TABLE thread TYPE option<string>;

DEFINE FIELD author ON TABLE thread TYPE record<user>; -- @parent

DEFINE FIELD created_at ON TABLE thread TYPE datetime
    VALUE time::now();

DEFINE FIELD active ON TABLE thread TYPE bool VALUE $value OR false;

DEFINE FIELD published ON TABLE thread TYPE bool VALUE $value OR false;

-- ##################################################################
-- COMMENT TABLE
-- ##################################################################

DEFINE TABLE comment SCHEMAFULL
  PERMISSIONS
    FOR select WHERE thread.published = true OR ($access = "account" AND thread.author.id = $auth.id)
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
  PERMISSIONS
    FOR select WHERE out.published = true OR ($access = "account" AND out.author.id = $auth.id);

DEFINE EVENT comment_created ON TABLE comment WHEN $event = "CREATE" THEN
  RELATE ($after.id)->commented_on->($after.thread)
;

-- ##################################################################
-- COLLABORATORS
-- ##################################################################

-- Per-issuer history of share links. Append-only by design — there's no
-- way to revoke a JWT once it's out, so the dialog only shows the issuer
-- the links they've made and warns them. Acceptance happens server-side
-- (example/api /share/accept), not via this table; rows here are purely
-- informational.
DEFINE TABLE share_link SCHEMAFULL
  PERMISSIONS
    FOR select WHERE issuer.id = $auth.id
    FOR create WHERE $access = "account" AND issuer = $auth.id AND thread.author.id = $auth.id
    FOR update, delete WHERE false;

DEFINE FIELD issuer     ON TABLE share_link TYPE record<user> VALUE $auth.id;
DEFINE FIELD thread     ON TABLE share_link TYPE record<thread>;
DEFINE FIELD jwt        ON TABLE share_link TYPE string;
DEFINE FIELD jti        ON TABLE share_link TYPE string;
DEFINE FIELD exp        ON TABLE share_link TYPE datetime;
DEFINE FIELD created_at ON TABLE share_link TYPE datetime VALUE time::now();
DEFINE INDEX unique_share_jti ON TABLE share_link FIELDS jti UNIQUE;

-- Only root may CREATE a `collaborates_on` edge — the JWT-verifying API
-- endpoint does it on the recipient's behalf after checking the share
-- link's signature against the issuer's `share_pubkey`. Author can still
-- kick collaborators off via FOR delete.
DEFINE TABLE collaborates_on SCHEMAFULL TYPE RELATION FROM user TO thread
  PERMISSIONS
    FOR select WHERE true
    FOR create WHERE false
    FOR delete WHERE $access = "account"
      AND (in.id = $auth.id OR out.author.id = $auth.id)
    FOR update WHERE false;

DEFINE INDEX unique_collab ON TABLE collaborates_on FIELDS in, out UNIQUE;
