-- ==================================================
-- PUSH (platform): Web Push, APNs, FCM
--
-- Everything the push engine (`packages/push-core`, hosted by the scheduler in
-- cluster mode and by the standalone SSP in singlenode) reads and writes, plus
-- the record-user API (`fn::push::*`). Its own file so push-core's tests run
-- against exactly the DDL that ships, same arrangement as schedule_tables.surql.
-- Already all-OVERWRITE: apply_internal_schema appends it verbatim.
--
-- Params the engine host publishes at boot (never defined here, so a deploy
-- never wipes them): `$sp00ky_vapid_public_key` (base64url, uncompressed P-256
-- point = the browser's `applicationServerKey`) and `$sp00ky_vapid_kid` (hex id
-- of that key). Both PERMISSIONS FULL. NONE until a host with push enabled has
-- started once.
--
-- Param the CLI writes at migrate (from `push.apns` / `push.fcm`, never a
-- secret): `$sp00ky_push_native` = { apns: bool, fcm: bool, bundleIds?,
-- android?: { projectId, appId, apiKey, senderId } }. NONE = no native push.
-- ==================================================

-- --------------------------------------------------
-- _00_push_config: the `push:` block of sp00ky.yml, normalized by the CLI and
-- written at every migrate/deploy as one row (`_00_push_config:default`). The
-- engine re-reads it every 30 s and on boot; `hash` short-circuits the parse.
-- Root only: rules can hold SurrealQL (`to.query`, `with`).
-- --------------------------------------------------
DEFINE TABLE OVERWRITE _00_push_config SCHEMAFULL PERMISSIONS NONE;
DEFINE FIELD OVERWRITE spec_json ON TABLE _00_push_config TYPE string;
DEFINE FIELD OVERWRITE hash ON TABLE _00_push_config TYPE string;
DEFINE FIELD OVERWRITE updated_at ON TABLE _00_push_config TYPE datetime DEFAULT time::now();

-- --------------------------------------------------
-- _00_push_credential: native provider secrets, resolved by the CLI from
-- `push.apns.key` / `push.fcm.serviceAccount` at migrate/deploy. Rows `apns`
-- (`{ teamId, keyId, key }` as JSON) and `fcm` (the service account JSON);
-- `hash` lets the engine notice a rotation without comparing secrets. Root
-- only, never synced, never ingested.
-- --------------------------------------------------
DEFINE TABLE OVERWRITE _00_push_credential SCHEMAFULL PERMISSIONS NONE;
DEFINE FIELD OVERWRITE secret ON TABLE _00_push_credential TYPE string;
DEFINE FIELD OVERWRITE hash ON TABLE _00_push_credential TYPE string;
DEFINE FIELD OVERWRITE updated_at ON TABLE _00_push_credential TYPE datetime DEFAULT time::now();

-- --------------------------------------------------
-- _00_push_subscription: one row per (user, device).
--
-- `kind` says how to reach it: `web` (a browser push endpoint, written by
-- fn::push::subscribe), `apns` or `fcm` (a native device token, written by
-- fn::push::register, `endpoint` = '<kind>:<token>' so everything keyed by
-- endpoint works the same for every kind).
--
-- Id = sha256(auth_id + "|" + endpoint), so the same browser signed in as two
-- users is two rows; the engine delivers an endpoint only to its most recently
-- updated owner and deletes the stale row, so a shared machine never shows the
-- previous user's pushes. Written through fn::push::subscribe / update /
-- unsubscribe; the engine (root) maintains the delivery fields and deletes rows
-- the push service reports gone (404/410).
--
-- Never readable while impersonating: an operator must not attach their own
-- browser to someone else's pushes.
-- --------------------------------------------------
DEFINE TABLE OVERWRITE _00_push_subscription SCHEMAFULL
    PERMISSIONS
      FOR select, create, update, delete
        WHERE $auth.id != NONE AND $access != "_00_impersonate" AND auth_id = <string> $auth.id;

DEFINE FIELD OVERWRITE auth_id ON TABLE _00_push_subscription TYPE string;
-- NONE = web: rows from before native push have no kind, and a required
-- field would make every later UPDATE of them fail the type check.
DEFINE FIELD OVERWRITE kind ON TABLE _00_push_subscription TYPE option<string>
    ASSERT $value = NONE OR $value IN ['web', 'apns', 'fcm'];
DEFINE FIELD OVERWRITE endpoint ON TABLE _00_push_subscription TYPE string;
-- Web only: the browser's encryption keys.
DEFINE FIELD OVERWRITE p256dh ON TABLE _00_push_subscription TYPE option<string>;
DEFINE FIELD OVERWRITE auth ON TABLE _00_push_subscription TYPE option<string>;
-- Native only: the device token, the app (bundle id / package name = APNs
-- topic), the OS, and for APNs which of Apple's hosts the token belongs to.
DEFINE FIELD OVERWRITE token ON TABLE _00_push_subscription TYPE option<string>;
DEFINE FIELD OVERWRITE app_id ON TABLE _00_push_subscription TYPE option<string>;
DEFINE FIELD OVERWRITE platform ON TABLE _00_push_subscription TYPE option<string>
    ASSERT $value = NONE OR $value IN ['web', 'ios', 'android', 'macos'];
DEFINE FIELD OVERWRITE environment ON TABLE _00_push_subscription TYPE option<string>
    ASSERT $value = NONE OR $value IN ['sandbox', 'production'];
-- VAPID key id a web subscription was created under. A web row whose kid is
-- not the current one was made for a key the push service will reject (403):
-- the engine skips it and the client re-subscribes on its next boot. Native
-- rows leave it empty.
DEFINE FIELD OVERWRITE kid ON TABLE _00_push_subscription TYPE string DEFAULT '';
DEFINE FIELD OVERWRITE client_id ON TABLE _00_push_subscription TYPE option<string>;
DEFINE FIELD OVERWRITE user_agent ON TABLE _00_push_subscription TYPE option<string>;
DEFINE FIELD OVERWRITE label ON TABLE _00_push_subscription TYPE option<string>;
-- Rule names this device wants. NONE = every rule. Direct messages
-- (`_00_push_message`) always pass.
DEFINE FIELD OVERWRITE rules ON TABLE _00_push_subscription TYPE option<array<string>>;
-- Free-form app data about the device (platform, app version, ...).
DEFINE FIELD OVERWRITE meta ON TABLE _00_push_subscription TYPE option<object> FLEXIBLE;
DEFINE FIELD OVERWRITE created_at ON TABLE _00_push_subscription TYPE datetime DEFAULT time::now();
DEFINE FIELD OVERWRITE updated_at ON TABLE _00_push_subscription TYPE datetime DEFAULT time::now();
DEFINE FIELD OVERWRITE last_ok_at ON TABLE _00_push_subscription TYPE option<datetime>;
DEFINE FIELD OVERWRITE last_error ON TABLE _00_push_subscription TYPE option<string>;
DEFINE FIELD OVERWRITE failures ON TABLE _00_push_subscription TYPE int DEFAULT 0;
DEFINE FIELD OVERWRITE disabled_at ON TABLE _00_push_subscription TYPE option<datetime>;
DEFINE FIELD OVERWRITE disabled_reason ON TABLE _00_push_subscription TYPE option<string>;

DEFINE INDEX OVERWRITE idx_push_sub_auth ON TABLE _00_push_subscription FIELDS auth_id;
DEFINE INDEX OVERWRITE idx_push_sub_endpoint ON TABLE _00_push_subscription FIELDS endpoint;

-- --------------------------------------------------
-- _00_push_message: direct pushes, no rule involved.
--
-- Root (a backend, a DEFINE EVENT, an operator) may CREATE one for any users;
-- a record user may create one for themselves only (reminders, "test push").
-- `send_at` in the future parks it until the engine's sweep sends it. The engine
-- is the only writer of the delivery fields; the creator may cancel a pending
-- message. Sent/failed/cancelled rows are pruned after 7 days.
-- --------------------------------------------------
DEFINE TABLE OVERWRITE _00_push_message SCHEMAFULL
    PERMISSIONS
      FOR select, delete WHERE $auth.id != NONE AND created_by = <string> $auth.id
      FOR create WHERE $auth.id != NONE AND $access != "_00_impersonate"
        AND created_by = <string> $auth.id AND to = [<string> $auth.id]
      FOR update WHERE $auth.id != NONE AND created_by = <string> $auth.id AND status IN ['pending', 'cancelled'];

-- Recipients: user record ids as strings ("user:abc"), same form as $auth.id.
DEFINE FIELD OVERWRITE to ON TABLE _00_push_message TYPE array<string>;
-- Shown as is by the service worker bridge (title, body, icon, badge, image,
-- tag, url, actions, requireInteraction, renotify, silent, data, ...).
-- Absent = a content-free nudge.
DEFINE FIELD OVERWRITE notification ON TABLE _00_push_message TYPE option<object> FLEXIBLE;
-- What iOS / Android show, over `notification`: { notification?, apns?,
-- android? } (same keys as a rule's `native:` block, taken as written).
DEFINE FIELD OVERWRITE native ON TABLE _00_push_message TYPE option<object> FLEXIBLE;
DEFINE FIELD OVERWRITE data ON TABLE _00_push_message TYPE option<object> FLEXIBLE;
DEFINE FIELD OVERWRITE topic ON TABLE _00_push_message TYPE option<string>;
DEFINE FIELD OVERWRITE urgency ON TABLE _00_push_message TYPE option<string>
    ASSERT $value = NONE OR $value IN ['very-low', 'low', 'normal', 'high'];
-- Push-service TTL in seconds.
DEFINE FIELD OVERWRITE ttl ON TABLE _00_push_message TYPE option<int>;
DEFINE FIELD OVERWRITE send_at ON TABLE _00_push_message TYPE option<datetime>;
DEFINE FIELD OVERWRITE status ON TABLE _00_push_message TYPE string DEFAULT 'pending'
    ASSERT $value IN ['pending', 'sending', 'sent', 'failed', 'cancelled'];
-- When the engine claimed it (status -> sending). A row stuck in `sending` for
-- minutes means the host died mid-send; the sweep puts it back to `pending`.
DEFINE FIELD OVERWRITE claimed_at ON TABLE _00_push_message TYPE option<datetime>;
DEFINE FIELD OVERWRITE sent_at ON TABLE _00_push_message TYPE option<datetime>;
-- Subscriptions the push service accepted the message for.
DEFINE FIELD OVERWRITE delivered ON TABLE _00_push_message TYPE option<int>;
DEFINE FIELD OVERWRITE error ON TABLE _00_push_message TYPE option<string>;
DEFINE FIELD OVERWRITE created_by ON TABLE _00_push_message TYPE option<string>;
DEFINE FIELD OVERWRITE created_at ON TABLE _00_push_message TYPE datetime DEFAULT time::now();

DEFINE INDEX OVERWRITE idx_push_msg_due ON TABLE _00_push_message FIELDS status, send_at;

-- --------------------------------------------------
-- Record-user API. Every function refuses anonymous callers and
-- impersonation sessions (see _00_push_subscription).
-- --------------------------------------------------

-- The key a browser subscribes with, and what native apps need. `enabled`
-- (web push) is false until a host with push on has published its key;
-- `providers` lists every kind of device that can be reached; `android` is
-- the Firebase client config an Android app initialises FCM with.
DEFINE FUNCTION OVERWRITE fn::push::info() {
    LET $native = $sp00ky_push_native;
    RETURN {
        enabled: $sp00ky_vapid_public_key != NONE,
        publicKey: $sp00ky_vapid_public_key,
        kid: $sp00ky_vapid_kid,
        providers: array::concat(
            IF $sp00ky_vapid_public_key != NONE { ['web'] } ELSE { [] },
            IF $native.apns = true { ['apns'] } ELSE { [] },
            IF $native.fcm = true { ['fcm'] } ELSE { [] }
        ),
        android: IF $native.fcm = true { $native.android } ELSE { NONE }
    };
} PERMISSIONS FULL;

-- $sub is `PushSubscription.toJSON()`: { endpoint, keys: { p256dh, auth } }.
-- $opts: { label?, rules?, userAgent?, meta? }. Re-subscribing the same
-- endpoint refreshes the keys, the kid and clears any disabled state.
DEFINE FUNCTION OVERWRITE fn::push::subscribe($sub: object, $opts: option<object>) {
    IF $auth.id = NONE { THROW "push: sign in before subscribing" };
    IF $access = "_00_impersonate" { THROW "push: not available while impersonating" };
    IF $sub.endpoint = NONE OR $sub.keys.p256dh = NONE OR $sub.keys.auth = NONE {
        THROW "push: expected PushSubscription.toJSON() with endpoint and keys"
    };
    LET $uid = <string> $auth.id;
    LET $endpoint = <string> $sub.endpoint;
    -- Browser push services are always https. The engine refuses anything
    -- else too (it must never be steered at an internal address).
    IF !string::starts_with($endpoint, 'https://') { THROW "push: the endpoint must be an https URL" };
    LET $id = type::record('_00_push_subscription', crypto::sha256($uid + '|' + $endpoint));
    RETURN UPSERT ONLY $id SET
        auth_id = $uid,
        kind = 'web',
        platform = 'web',
        endpoint = $endpoint,
        p256dh = <string> $sub.keys.p256dh,
        auth = <string> $sub.keys.auth,
        kid = $sp00ky_vapid_kid ?? '',
        client_id = <string> session::id(),
        user_agent = $opts.userAgent,
        label = $opts.label,
        rules = $opts.rules,
        meta = $opts.meta,
        updated_at = time::now(),
        failures = 0,
        last_error = NONE,
        disabled_at = NONE,
        disabled_reason = NONE
    RETURN id, endpoint, kid, label, rules, meta, created_at, updated_at;
} PERMISSIONS FULL;

-- A native device. $device: { kind: 'apns' | 'fcm', token, appId, platform?
-- ('ios' | 'android' | 'macos'), environment? ('sandbox' | 'production', APNs
-- only, default production) }. $opts as for subscribe. Re-registering the
-- same token refreshes the row and clears any disabled state.
DEFINE FUNCTION OVERWRITE fn::push::register($device: object, $opts: option<object>) {
    IF $auth.id = NONE { THROW "push: sign in before registering" };
    IF $access = "_00_impersonate" { THROW "push: not available while impersonating" };
    LET $kind = $device.kind;
    IF !($kind IN ['apns', 'fcm']) { THROW "push: kind must be apns or fcm" };
    LET $native = $sp00ky_push_native;
    IF $kind = 'apns' AND $native.apns != true { THROW "push: push.apns is not configured" };
    IF $kind = 'fcm' AND $native.fcm != true { THROW "push: push.fcm is not configured" };
    -- (`$token` is a protected variable.)
    LET $device_token = <string> ($device.token ?? '');
    LET $app = <string> ($device.appId ?? '');
    -- Character-class checks spelled without string::is::* (renamed in
    -- SurrealDB 3): split into characters, keep the ones not allowed.
    -- `string::split(s, '')` yields empty strings at both ends.
    LET $outside = |$s: string, $allowed: string| array::len(array::filter(
        string::split($s, ''), |$c| $c != '' AND !($c IN string::split($allowed, ''))
    ));
    LET $alnum = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789';
    IF $kind = 'apns' AND (string::len($device_token) < 64 OR string::len($device_token) > 200
        OR $outside($device_token, '0123456789abcdefABCDEF') > 0) {
        THROW "push: an APNs token is the hex device token"
    };
    IF $kind = 'fcm' AND (string::len($device_token) < 20 OR string::len($device_token) > 4096
        OR $outside($device_token, $alnum + '_:-') > 0) {
        THROW "push: not an FCM registration token"
    };
    IF string::len($app) = 0 OR string::len($app) > 255 OR $outside($app, $alnum + '._-') > 0 {
        THROW "push: appId must be the bundle id / package name"
    };
    IF $kind = 'apns' AND array::len($native.bundleIds ?? []) > 0 AND !($app IN $native.bundleIds) {
        THROW "push: appId is not in push.apns.bundleIds"
    };
    LET $platform = $device.platform ?? (IF $kind = 'apns' { 'ios' } ELSE { 'android' });
    IF !($platform IN ['ios', 'android', 'macos']) { THROW "push: platform must be ios, android or macos" };
    LET $environment = IF $kind = 'apns' { $device.environment ?? 'production' } ELSE { NONE };
    IF $environment != NONE AND !($environment IN ['sandbox', 'production']) {
        THROW "push: environment must be sandbox or production"
    };
    LET $uid = <string> $auth.id;
    LET $device_token = IF $kind = 'apns' { string::lowercase($device_token) } ELSE { $device_token };
    LET $endpoint = $kind + ':' + $device_token;
    LET $id = type::record('_00_push_subscription', crypto::sha256($uid + '|' + $endpoint));
    RETURN UPSERT ONLY $id SET
        auth_id = $uid,
        kind = $kind,
        endpoint = $endpoint,
        token = $device_token,
        app_id = $app,
        platform = $platform,
        environment = $environment,
        p256dh = NONE,
        auth = NONE,
        kid = '',
        client_id = <string> session::id(),
        user_agent = $opts.userAgent,
        label = $opts.label,
        rules = $opts.rules,
        meta = $opts.meta,
        updated_at = time::now(),
        failures = 0,
        last_error = NONE,
        disabled_at = NONE,
        disabled_reason = NONE
    RETURN id, endpoint, kind, platform, app_id, environment, kid, label, rules, meta, created_at, updated_at;
} PERMISSIONS FULL;

-- Change label / rules / meta of one of the caller's subscriptions.
DEFINE FUNCTION OVERWRITE fn::push::update($endpoint: string, $opts: object) {
    IF $auth.id = NONE { THROW "push: sign in first" };
    LET $uid = <string> $auth.id;
    -- `rules: []` resets the device to every rule.
    RETURN UPDATE _00_push_subscription SET
        label = $opts.label ?? label,
        rules = IF $opts.rules = NONE { rules } ELSE IF $opts.rules = [] { NONE } ELSE { $opts.rules },
        meta = $opts.meta ?? meta,
        updated_at = time::now()
    WHERE auth_id = $uid AND endpoint = $endpoint
    RETURN id, endpoint, kid, label, rules, meta, created_at, updated_at;
} PERMISSIONS FULL;

-- Remove one of the caller's subscriptions, or all of them when $endpoint is
-- NONE. Returns how many rows went.
DEFINE FUNCTION OVERWRITE fn::push::unsubscribe($endpoint: option<string>) {
    IF $auth.id = NONE { RETURN 0 };
    LET $uid = <string> $auth.id;
    LET $gone = DELETE _00_push_subscription
        WHERE auth_id = $uid AND ($endpoint = NONE OR endpoint = $endpoint)
        RETURN BEFORE;
    RETURN array::len($gone);
} PERMISSIONS FULL;

-- The caller's devices, without the encryption keys.
DEFINE FUNCTION OVERWRITE fn::push::list() {
    IF $auth.id = NONE { RETURN [] };
    LET $uid = <string> $auth.id;
    RETURN SELECT id, endpoint, (kind ?? 'web') AS kind, platform, app_id, environment,
        kid, label, user_agent, rules, meta, created_at,
        updated_at, last_ok_at, last_error, failures, disabled_at, disabled_reason,
        ((kind ?? 'web') != 'web' OR kid = ($sp00ky_vapid_kid ?? '')) AS current
        FROM _00_push_subscription WHERE auth_id = $uid ORDER BY updated_at DESC;
} PERMISSIONS FULL;

-- Push to yourself: { notification?, native?, data?, topic?, urgency?, ttl?,
-- sendAt? }.
-- sendAt (datetime or ISO string) in the future schedules it.
DEFINE FUNCTION OVERWRITE fn::push::notify($msg: object) {
    IF $auth.id = NONE { THROW "push: sign in first" };
    IF $access = "_00_impersonate" { THROW "push: not available while impersonating" };
    LET $uid = <string> $auth.id;
    LET $at = $msg.sendAt ?? $msg.send_at;
    RETURN CREATE ONLY _00_push_message CONTENT {
        to: [$uid],
        notification: $msg.notification,
        native: $msg.native,
        data: $msg.data,
        topic: $msg.topic,
        urgency: $msg.urgency,
        ttl: $msg.ttl,
        send_at: IF $at = NONE { NONE } ELSE { <datetime> $at },
        created_by: $uid,
        status: 'pending'
    } RETURN id, status, send_at, created_at;
} PERMISSIONS FULL;

-- Cancel one of the caller's scheduled messages.
DEFINE FUNCTION OVERWRITE fn::push::cancel($id: record<_00_push_message>) {
    IF $auth.id = NONE { RETURN false };
    LET $done = UPDATE $id SET status = 'cancelled'
        WHERE status = 'pending' AND created_by = <string> $auth.id RETURN AFTER;
    RETURN array::len($done) > 0;
} PERMISSIONS FULL;

-- A visible notification to every device of the caller.
DEFINE FUNCTION OVERWRITE fn::push::test($opts: option<object>) {
    RETURN fn::push::notify({
        notification: {
            title: $opts.title ?? 'Push notifications are on',
            body: $opts.body ?? 'This device will receive notifications.',
            tag: 'sp00ky-push-test'
        },
        data: { test: true },
        topic: 'sp00ky-push-test',
        ttl: 60
    });
} PERMISSIONS FULL;
