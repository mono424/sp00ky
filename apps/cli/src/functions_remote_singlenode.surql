-- ==================================================
-- SINGLENODE / CLUSTER (HTTP) IMPLEMENTATION
-- ==================================================

DEFINE FUNCTION OVERWRITE fn::query::register($config: object) {
    -- clientId comes from session::id() (server-trusted); whatever the
    -- caller passed in $config.clientId is ignored. This is what the
    -- _00_query / _00_list_ref permission rules match against at read
    -- time, so the value must be derived inside the user's session.
    --
    -- $auth.id is injected server-side into params under `auth.id` so the
    -- SSP's permission rewrite (packages/ssp/src/policy.rs) can resolve
    -- `$auth.id` references in the registered table's SELECT predicate
    -- per-user. Without this, predicates that reference $auth fail closed
    -- (Predicate::False) at packages/ssp/src/policy.rs:184-194.
    LET $params_in = $config.params OR {};
    -- Inject both `$auth.id` and `$access` so the SSP's permission
    -- evaluator can substitute either when applying the table's SELECT
    -- predicate. Predicates like `published = true OR $access = "account"
    -- AND author.id = $auth.id` evaluate to "only published rows" if
    -- $access is missing — invisible-by-default for record-access users.
    -- An impersonated session authenticates through `_00_impersonate`; its
    -- token carries the target's own access method so the SSP resolves
    -- `$access` in permission predicates the way it would for that user.
    LET $params_with_auth = object::extend($params_in, {
        auth: { id: <string>($auth.id OR '') },
        access: <string>($token.spky_as_access OR $access OR '')
    });
    -- User-scoped identity for the registration. The SSP propagates
    -- this onto each `_00_list_ref` row so the table's permission
    -- rule (`auth_id = $auth.id`) can deliver LIVE updates to every
    -- session of the same user, not just the one whose session::id()
    -- happened to be live at registration time.
    --
    -- Compute the auth-id string BEFORE building $module_config. An
    -- inline `<string>($auth.id OR '')` at the object-literal site
    -- silently drops the field when $auth.id is NONE — SurrealDB
    -- collapses NONE-typed entries out of object literals. Binding to
    -- a LET first forces the value into a concrete `string`, which
    -- survives object construction and JSON serialization in
    -- `http::post`.
    -- NOTE: any extra fields added to $module_config here are silently
    -- dropped by SurrealDB's `http::post` (only the original recognized
    -- set survives the call boundary). Auth identity for `_00_query.
    -- auth_id` propagation must travel through `params.auth.id`, which
    -- IS preserved, and the SSP extracts it server-side in
    -- `prepare_registration_dbsp`.
    LET $module_config = {
        id: <string>$config.id,
        surql: <string>$config.surql,
        params: <string>$params_with_auth,
        clientId: <string>session::id(),
        ttl: <string>($config.ttl OR '1h'),
        lastActiveAt: <string>time::now(),
        format: <string>($config.format OR 'flat')
    };

    -- Call SSP via HTTP
    http::post('{{ENDPOINT}}/view/register', $module_config, { "Authorization": "Bearer {{SECRET}}" });
};

-- Stack/version discovery as a callable function. Returns the full entity
-- list the backend `/info` endpoint exposes (ssp + scheduler + backend, each
-- with version, surrealdb_version, status, uptime, ip, views, …) as a parsed
-- structured value — so WS callers (the DevTools) get it natively without any
-- HTTP/CORS round-trip. `{{ENDPOINT}}` is the ssp (singlenode) or scheduler
-- (cluster) base URL. PERMISSIONS FULL so record-access clients may invoke it.
DEFINE FUNCTION OVERWRITE fn::spooky::info() {
    RETURN http::get('{{ENDPOINT}}/info', {
        "Authorization": "Bearer {{SECRET}}"
    });
} PERMISSIONS FULL;

-- Operator job controls. `{{ENDPOINT}}` is the ssp (singlenode) or scheduler
-- (cluster) base URL — the entry point that owns the `/job/kill` + `/job/retry`
-- HTTP routes. Unlike `fn::spooky::info`, these intentionally OMIT
-- `PERMISSIONS FULL`: killing/retrying jobs is an operator action, and the CLI
-- connects as root, so leaving them system-access-only keeps record-access end
-- users from stopping each other's jobs.

-- Stop a job: cancels the in-flight request if it is currently processing, or
-- drops it from the queue if still pending. Idempotent on terminal jobs.
DEFINE FUNCTION OVERWRITE fn::job::kill($id: string) {
    RETURN http::post('{{ENDPOINT}}/job/kill', { id: $id }, {
        "Authorization": "Bearer {{SECRET}}"
    });
};

-- Re-run a terminal (failed/success) job: resets retries/errors and re-enqueues
-- it (a plain UPDATE would not re-trigger the CREATE-gated ingest path).
DEFINE FUNCTION OVERWRITE fn::job::retry($id: string) {
    RETURN http::post('{{ENDPOINT}}/job/retry', { id: $id }, {
        "Authorization": "Bearer {{SECRET}}"
    });
};

-- Keep the HTTP `/spooky` custom API for external consumers (e.g. the CLI's
-- `spooky cloud status`). It proxies the SSP's `/info/text` route, which
-- returns the info array already serialized to a JSON string (text/plain).
DEFINE API OVERWRITE "/spooky"
    FOR get
    THEN {
        -- A DEFINE API response `body` must be None/bytes/string on the
        -- deployed SurrealDB (3.0.x). `/info/text` hands us pre-serialized
        -- JSON text, so we pass it through verbatim and skip DB-side JSON
        -- encoding (encoding::json::encode and object-body auto-serialization
        -- are both 3.1.0+). `fn::spooky::info()` still calls `/info`
        -- (structured) for WS/DevTools consumers.
        LET $info = http::get('{{ENDPOINT}}/info/text', {
            "Authorization": "Bearer {{SECRET}}"
        });
        RETURN {
            status: 200,
            body: <string>$info,
            headers: {
                'Content-Type': 'application/json'
            }
        };
    };
