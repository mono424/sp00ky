-- Migration: share_accept
-- Created: 2026-10-13

-- Invite links are accepted in the database: the recipient creates a
-- `share_accept` row and its event joins them to the thread when the JWT is
-- an unexpired link its author stored in `share_link`. Replaces the api
-- backend's /share/accept, so a deployment without a backend accepts invites.

-- Added
DEFINE TABLE OVERWRITE share_accept SCHEMAFULL
  PERMISSIONS
    FOR select WHERE user = $auth.id
    FOR create WHERE $access = "account"
    FOR update, delete WHERE false;
DEFINE FIELD OVERWRITE user ON TABLE share_accept TYPE record<user> VALUE $auth.id READONLY;
DEFINE FIELD OVERWRITE jwt ON TABLE share_accept TYPE string READONLY;
DEFINE FIELD OVERWRITE created_at ON TABLE share_accept TYPE datetime VALUE time::now() READONLY;
DEFINE EVENT OVERWRITE accept_invite ON TABLE share_accept WHEN $event = "CREATE" THEN {
    LET $link = (SELECT thread, exp FROM ONLY share_link WHERE jwt = $after.jwt LIMIT 1);
    IF $link = NONE OR $link.exp < time::now() {
        THROW "This invite link is invalid or has expired.";
    };
    IF array::len((SELECT VALUE id FROM collaborates_on WHERE in = $after.user AND out = $link.thread)) = 0 {
        RELATE ($after.user)->collaborates_on->($link.thread);
    };
};
