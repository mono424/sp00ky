-- ==================================================
-- ADMIN IMPERSONATION (sp00ky.yml `impersonation.enabled: true`)
-- ==================================================
-- Rendered by `schema_builder::build_impersonation_schema` only when the
-- feature is on; otherwise the REMOVE block there runs instead, which also
-- invalidates every token already issued.
--
-- Flow: an admin calls fn::_00_impersonate::start in their own session. It
-- records a `_00_impersonation` row and asks the backend (SSP or scheduler,
-- with the shared bearer) to sign a token for the `_00_impersonate` access
-- method. The browser then authenticates with that token, so `$auth.id` is
-- the target user and every permission rule applies exactly as it would for
-- them. The AUTHENTICATE block below re-validates the session row on every
-- authenticate, which is what makes stop, expiry and roster removal effective.
--
-- The admin predicate below is a copy of the one on `_00_feature_flag` in
-- meta_tables_remote.surql. Keep them identical.

DEFINE TABLE OVERWRITE _00_impersonation SCHEMAFULL
    PERMISSIONS
        FOR select, update WHERE
            ($access != '{{ACCESS_NAME}}' AND $auth.id != NONE AND array::len((SELECT VALUE id FROM _00_admin WHERE user = $auth.id LIMIT 1)) > 0)
            OR ($access = '{{ACCESS_NAME}}' AND <string>id = <string>$token.spky_imp)
        FOR create WHERE
            $access != '{{ACCESS_NAME}}' AND $auth.id != NONE AND array::len((SELECT VALUE id FROM _00_admin WHERE user = $auth.id LIMIT 1)) > 0
        FOR delete NONE;

-- Every field an admin could abuse is computed or READONLY, so writing a row
-- directly (instead of through start) gains nothing: the admin is always the
-- caller, the window always starts now, and a session can only be ended.
-- A row alone is not a credential either: tokens come only from the backend.
DEFINE FIELD OVERWRITE admin ON _00_impersonation TYPE record VALUE $before OR $auth.id READONLY;
DEFINE FIELD OVERWRITE target ON _00_impersonation TYPE record
    ASSERT <string>$value != <string>$auth.id READONLY;
DEFINE FIELD OVERWRITE access ON _00_impersonation TYPE string VALUE $before OR <string>($access OR '') READONLY;
DEFINE FIELD OVERWRITE reason ON _00_impersonation TYPE string
    ASSERT string::len(string::trim($value)) >= 3 READONLY;
DEFINE FIELD OVERWRITE started_at ON _00_impersonation TYPE datetime VALUE $before OR time::now() READONLY;
DEFINE FIELD OVERWRITE expires_at ON _00_impersonation TYPE datetime VALUE $before OR time::now() + {{MAX_DURATION}} READONLY;
DEFINE FIELD OVERWRITE ended_at ON _00_impersonation TYPE option<datetime>
    VALUE IF $before != NONE { $before } ELSE IF $value != NONE { time::now() } ELSE { NONE };
DEFINE FIELD OVERWRITE ended_by ON _00_impersonation TYPE option<string>
    VALUE IF $before != NONE { $before }
        ELSE IF $value = NONE { NONE }
        ELSE IF $access = '{{ACCESS_NAME}}' { 'impersonated-session' }
        ELSE IF $auth.id != NONE { <string>$auth.id }
        ELSE { 'operator' };
DEFINE INDEX OVERWRITE _00_impersonation_admin ON _00_impersonation FIELDS admin;
DEFINE INDEX OVERWRITE _00_impersonation_target ON _00_impersonation FIELDS target;

-- One row per write made while impersonating, filled by the `_00_imp_audit`
-- event on each user table (events run elevated, so nobody writes it directly).
DEFINE TABLE OVERWRITE _00_impersonation_write SCHEMAFULL
    PERMISSIONS
        FOR select WHERE
            $access != '{{ACCESS_NAME}}' AND $auth.id != NONE AND array::len((SELECT VALUE id FROM _00_admin WHERE user = $auth.id LIMIT 1)) > 0
        FOR create, update, delete NONE;
DEFINE FIELD OVERWRITE session ON _00_impersonation_write TYPE record<_00_impersonation>;
DEFINE FIELD OVERWRITE admin ON _00_impersonation_write TYPE string;
DEFINE FIELD OVERWRITE tb ON _00_impersonation_write TYPE string;
DEFINE FIELD OVERWRITE record ON _00_impersonation_write TYPE option<record>;
DEFINE FIELD OVERWRITE action ON _00_impersonation_write TYPE string;
DEFINE FIELD OVERWRITE at ON _00_impersonation_write TYPE datetime;
DEFINE INDEX OVERWRITE _00_impersonation_write_session ON _00_impersonation_write FIELDS session;

DEFINE ACCESS OVERWRITE {{ACCESS_NAME}} ON DATABASE TYPE RECORD
    WITH JWT ALGORITHM HS256 KEY '{{IMPERSONATION_KEY}}'
    AUTHENTICATE {
        LET $s = (SELECT * FROM ONLY type::record($token.spky_imp));
        IF $s = NONE OR $s.ended_at != NONE OR $s.expires_at < time::now()
            OR <string>$s.target != <string>$auth.id {
            THROW 'Impersonation session is no longer valid.';
        };
        -- Elevated here, so these see the whole roster and every user.
        IF array::len((SELECT VALUE id FROM _00_admin WHERE user = $s.admin LIMIT 1)) = 0
            OR array::len((SELECT VALUE id FROM _00_admin WHERE user = $s.target LIMIT 1)) > 0
            OR !record::exists($s.target) {
            THROW 'Impersonation session is no longer valid.';
        };
        RETURN $auth;
    }
    DURATION FOR TOKEN {{TOKEN_TTL}}, FOR SESSION {{TOKEN_TTL}};

-- Start impersonating `$target` (a record id string). Returns the token the
-- client authenticates with, plus the session it is bound to.
DEFINE FUNCTION OVERWRITE fn::_00_impersonate::start($target: string, $reason: string) {
    IF $access = '{{ACCESS_NAME}}' {
        THROW 'Cannot start impersonation from an impersonated session.';
    };
    LET $who = type::record($target);
    IF record::tb($who) != '{{USER_TABLE}}' {
        THROW 'Only {{USER_TABLE}} records can be impersonated.';
    };
    LET $s = (CREATE ONLY _00_impersonation SET target = $who, reason = $reason);
    LET $minted = http::post('{{ENDPOINT}}/impersonate/mint', {
        session: <string>$s.id,
        target: <string>$s.target,
        admin: <string>$s.admin,
        access: $s.access,
        ns: session::ns(),
        db: session::db(),
        ttl_secs: {{TOKEN_TTL_SECS}},
        session_remaining_secs: duration::secs($s.expires_at - time::now())
    }, { "Authorization": "Bearer {{SECRET}}" });
    RETURN {
        token: $minted.token,
        exp: $minted.exp,
        session: <string>$s.id,
        target: <string>$s.target,
        admin: <string>$s.admin,
        access: $s.access,
        reason: $s.reason,
        expires_at: $s.expires_at
    };
} PERMISSIONS WHERE $access != '{{ACCESS_NAME}}' AND $auth.id != NONE AND array::len((SELECT VALUE id FROM _00_admin WHERE user = $auth.id LIMIT 1)) > 0;

-- Re-sign the caller's own token before it expires. Only the impersonated
-- session itself can renew, and never past the session's `expires_at`.
DEFINE FUNCTION OVERWRITE fn::_00_impersonate::renew() {
    IF $access != '{{ACCESS_NAME}}' {
        THROW 'Not an impersonated session.';
    };
    LET $s = (SELECT * FROM ONLY type::record($token.spky_imp));
    IF $s = NONE OR $s.ended_at != NONE OR $s.expires_at < time::now() {
        THROW 'Impersonation session has ended.';
    };
    LET $minted = http::post('{{ENDPOINT}}/impersonate/mint', {
        session: <string>$s.id,
        target: <string>$s.target,
        admin: <string>$s.admin,
        access: $s.access,
        ns: session::ns(),
        db: session::db(),
        ttl_secs: {{TOKEN_TTL_SECS}},
        session_remaining_secs: duration::secs($s.expires_at - time::now())
    }, { "Authorization": "Bearer {{SECRET}}" });
    RETURN { token: $minted.token, exp: $minted.exp };
} PERMISSIONS FULL;

-- End a session. The impersonated session may end itself; an admin may end
-- any session (the table's update rule decides which rows are visible).
DEFINE FUNCTION OVERWRITE fn::_00_impersonate::stop($session: string) {
    LET $rec = type::record($session);
    IF record::tb($rec) != '_00_impersonation' {
        THROW 'Not an impersonation session.';
    };
    IF $access = '{{ACCESS_NAME}}' AND <string>$rec != <string>$token.spky_imp {
        THROW 'An impersonated session can only end itself.';
    };
    UPDATE $rec SET ended_at = time::now(), ended_by = 'set';
    RETURN NONE;
} PERMISSIONS FULL;

-- The caller's live impersonation, or NONE. The client reads it at boot and
-- polls it: an open WebSocket keeps its session until the token's own expiry,
-- so this is how a stop or revocation elsewhere reaches it within a minute.
DEFINE FUNCTION OVERWRITE fn::_00_impersonate::current() {
    IF $access != '{{ACCESS_NAME}}' {
        RETURN NONE;
    };
    LET $s = (SELECT * FROM ONLY type::record($token.spky_imp));
    IF $s = NONE OR $s.ended_at != NONE OR $s.expires_at < time::now() {
        RETURN NONE;
    };
    RETURN {
        session: <string>$s.id,
        target: <string>$s.target,
        admin: <string>$s.admin,
        access: $s.access,
        reason: $s.reason,
        expires_at: $s.expires_at
    };
} PERMISSIONS FULL;

-- User search for the DevTools picker. An admin session cannot read other
-- users, so the backend runs it with root access; the table and fields are
-- fixed here from sp00ky.yml, only the search text comes from the caller.
DEFINE FUNCTION OVERWRITE fn::_00_impersonate::list_users($search: string) {
    RETURN http::post('{{ENDPOINT}}/impersonate/users', {
        table: '{{USER_TABLE}}',
        fields: {{SEARCH_FIELDS}},
        search: $search,
        limit: 25
    }, { "Authorization": "Bearer {{SECRET}}" });
} PERMISSIONS WHERE $access != '{{ACCESS_NAME}}' AND $auth.id != NONE AND array::len((SELECT VALUE id FROM _00_admin WHERE user = $auth.id LIMIT 1)) > 0;

-- Open sessions, for the DevTools and the admin dashboard.
DEFINE FUNCTION OVERWRITE fn::_00_impersonate::active() {
    RETURN (SELECT <string>id AS session, <string>target AS target, <string>admin AS admin,
        reason, started_at, expires_at FROM _00_impersonation
        WHERE ended_at = NONE AND expires_at > time::now() ORDER BY started_at DESC LIMIT 50);
} PERMISSIONS WHERE $access != '{{ACCESS_NAME}}' AND $auth.id != NONE AND array::len((SELECT VALUE id FROM _00_admin WHERE user = $auth.id LIMIT 1)) > 0;
