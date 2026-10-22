//! End-to-end test for the SurrealQL feature-flag evaluator.
//!
//! `fn::feature::materialize` in `meta_tables_remote.surql` is a **fourth**
//! implementation of the evaluator, alongside `flag.rs::evaluate_one`,
//! `apps/scheduler/src/feature_flags.rs::evaluate_one`, and the bucketing in
//! `fn::feature::hash`. It exists so an admin can flip a flag from the DevTools
//! panel without shelling out to the CLI.
//!
//! Four implementations only stay in agreement if drift fails a test, so this
//! asserts the *same* golden hash vectors and the *same* `evaluate_one`
//! behaviours that `flag.rs`'s unit tests pin in Rust. If a variant resolves
//! differently depending on who flipped the flag, that is the bug this catches.
//!
//! It also covers the two gates the DevTools tab depends on:
//!   - a non-admin record token can neither read definitions nor call the
//!     mutation functions, and cannot forge a `_00_user_feature` row;
//!   - an admin can, but still cannot promote themselves.
//!
//! Requires SurrealDB **3.x**: the evaluator uses closures, `array::fold` and
//! `array::find_index`, and v3 removed the hex int cast the old
//! `fn::feature::hash` one-liner relied on.
//!
//! Marked `#[ignore]` so the default `cargo test` stays fast and CI-safe:
//!
//! ```sh
//! cargo test -p sp00ky-cli --test feature_materialize_e2e -- --ignored --test-threads=1
//! ```
//!
//! (One container name and port for every test, so they cannot run in
//! parallel.)
//!
//! Skips (rather than fails) when Docker is unavailable, mirroring
//! `flag_sql_e2e.rs`.

use std::process::{Command, Stdio};
use std::thread::sleep;
use std::time::{Duration, Instant};

const IMAGE: &str = "surrealdb/surrealdb:v3.1.0-beta.3";
const NAME: &str = "spky-feature-materialize-e2e";
const PORT: &str = "18078";
const AUTH: &str = "Basic cm9vdDpyb290"; // base64("root:root")
const NS: &str = "e2e";
const DB: &str = "e2e";

/// The internal schema the CLI applies. Sliced at runtime so the test always
/// exercises the shipped definitions rather than a copy that can rot.
const META_TABLES: &str = include_str!("../src/meta_tables_remote.surql");

fn docker_available() -> bool {
    Command::new("docker")
        .arg("version")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

fn remove_container() {
    let _ = Command::new("docker")
        .args(["rm", "-f", NAME])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status();
}

struct Container;
impl Drop for Container {
    fn drop(&mut self) {
        remove_container();
    }
}

/// POST to `/sql` as root. `None` on a transport error (server not up yet).
fn try_sql(query: &str) -> Option<String> {
    post(query, AUTH)
}

fn post(query: &str, auth: &str) -> Option<String> {
    let url = format!("http://127.0.0.1:{PORT}/sql");
    match ureq::post(&url)
        .set("Authorization", auth)
        .set("surreal-ns", NS)
        .set("surreal-db", DB)
        .set("Accept", "application/json")
        .send_string(query)
    {
        Ok(r) => Some(r.into_string().unwrap_or_default()),
        Err(ureq::Error::Status(_, r)) => Some(r.into_string().unwrap_or_default()),
        Err(_) => None,
    }
}

fn sql(query: &str) -> String {
    try_sql(query).expect("SurrealDB query failed (transport error)")
}

/// Run a query as a record-access user, using the JWT from `signup`/`signin`.
fn sql_as(token: &str, query: &str) -> String {
    post(query, &format!("Bearer {token}")).expect("SurrealDB query failed (transport error)")
}

/// Sign a user up through the `account` record access and return their JWT.
fn signup(username: &str) -> String {
    let url = format!("http://127.0.0.1:{PORT}/signup");
    let body = format!(
        r#"{{"NS":"{NS}","DB":"{DB}","AC":"account","username":"{username}","password":"pw12345"}}"#
    );
    let resp = ureq::post(&url)
        .set("Accept", "application/json")
        .send_string(&body)
        .expect("signup failed")
        .into_string()
        .unwrap_or_default();
    let token = resp
        .split("\"token\":\"")
        .nth(1)
        .and_then(|s| s.split('"').next())
        .unwrap_or_else(|| panic!("no token in signup response: {resp}"));
    token.to_string()
}

/// The `_00_admin` → `fn::feature::*` slice of the shipped internal schema.
///
/// Applying the whole file would drag in the ingest events and the DBSP
/// registration hooks, which need an SSP. This takes the feature-flag block
/// verbatim from the real file, so a change to the shipped SurrealQL is what
/// this test runs.
fn feature_schema() -> String {
    let start = META_TABLES
        .find("-- SPOOKY ADMIN")
        .expect("`-- SPOOKY ADMIN` header missing from meta_tables_remote.surql");
    let start = META_TABLES[..start]
        .rfind("-- ==")
        .expect("no comment banner before the SPOOKY ADMIN block");
    let end = META_TABLES
        .find("-- SPOOKY APP RELEASE")
        .expect("`-- SPOOKY APP RELEASE` header missing from meta_tables_remote.surql");
    let end = META_TABLES[..end]
        .rfind("-- ==")
        .expect("no comment banner before the SPOOKY APP RELEASE block");
    format!(
        "DEFINE TABLE OVERWRITE _00_module_state SCHEMALESS PERMISSIONS FULL;\n{}",
        &META_TABLES[start..end]
    )
}

/// True when every statement in the response reported `status: OK`.
fn all_ok(body: &str) -> bool {
    !body.contains("\"status\":\"ERR\"") && !body.contains("\"code\":400")
}

fn start_surrealdb() -> Container {
    remove_container();
    let started = Command::new("docker")
        .args([
            "run",
            "-d",
            "--name",
            NAME,
            "-p",
            &format!("{PORT}:8000"),
            IMAGE,
            "start",
            "--user",
            "root",
            "--pass",
            "root",
            "--allow-all",
            "memory",
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .expect("failed to invoke docker run");
    assert!(started.success(), "docker run failed for {IMAGE}");
    let guard = Container;

    let deadline = Instant::now() + Duration::from_secs(90);
    while try_sql("RETURN 1;").is_none() {
        assert!(
            Instant::now() < deadline,
            "SurrealDB did not become ready within 90s"
        );
        sleep(Duration::from_millis(500));
    }
    guard
}

/// Seed an app schema (a `user` table + the `account` access) plus the
/// feature-flag half of the internal schema.
fn seed_schema() {
    // v3 does not auto-create the namespace/database from the request headers.
    let init = format!(
        "DEFINE NAMESPACE IF NOT EXISTS {NS}; USE NS {NS}; \
         DEFINE DATABASE IF NOT EXISTS {DB}; USE NS {NS} DB {DB};"
    );
    let body = sql(&init);
    assert!(all_ok(&body), "failed to create the namespace/database: {body}");

    let app = r#"
        DEFINE TABLE user SCHEMAFULL
        PERMISSIONS
          FOR update, delete WHERE $access = "account" AND id = $auth.id
          FOR create, select WHERE true;
        DEFINE FIELD username ON TABLE user TYPE string;
        DEFINE FIELD password ON TABLE user TYPE string;
        DEFINE INDEX idx_username ON TABLE user COLUMNS username UNIQUE;
        DEFINE ACCESS account ON DATABASE TYPE RECORD
          SIGNUP ( CREATE user SET username = $username, password = crypto::argon2::generate($password) )
          SIGNIN ( SELECT * FROM user WHERE username = $username AND crypto::argon2::compare(password, $password) )
          DURATION FOR TOKEN 15m, FOR SESSION 30d;
    "#;
    let body = sql(app);
    assert!(all_ok(&body), "failed to apply the app schema: {body}");

    let body = sql(&feature_schema());
    assert!(
        all_ok(&body),
        "failed to apply the feature-flag internal schema: {body}"
    );

    // Stand-in for the ingest event `sp00ky.rs` puts on `_00_user_feature`:
    // the same WHEN, but it records the write instead of posting it to the
    // SSP. Each `_write` row is one SSP round trip in production.
    let body = sql(
        "DEFINE TABLE _write SCHEMALESS PERMISSIONS FULL;
         DEFINE EVENT count_writes ON TABLE _00_user_feature
         WHEN $before != $after AND $event != \"DELETE\"
         THEN { CREATE _write SET user = <string>$after.user, key = $after.key, variant = $after.variant };",
    );
    assert!(all_ok(&body), "failed to define the write counter: {body}");
}

/// Writes that would have posted to the SSP for `key`, per user.
fn posted(key: &str) -> String {
    sql(&format!(
        "SELECT VALUE user FROM _write WHERE key = '{key}' ORDER BY user;"
    ))
}

/// A response body without its per-statement timing, so two reads compare.
fn result_of(body: &str) -> String {
    body.split("\"time\"").next().unwrap_or_default().to_string()
}

/// Number of writes that would have posted to the SSP for `key`.
fn posted_count(key: &str) -> usize {
    let body = sql(&format!(
        "RETURN count(SELECT VALUE id FROM _write WHERE key = '{key}');"
    ));
    body.split("\"result\":")
        .nth(1)
        .and_then(|s| s.split(|c: char| !c.is_ascii_digit()).find(|t| !t.is_empty()))
        .and_then(|t| t.parse().ok())
        .unwrap_or_else(|| panic!("no count in {body}"))
}

#[test]
#[ignore = "requires Docker; run with: cargo test -p sp00ky-cli --test feature_materialize_e2e -- --ignored"]
fn surrealql_evaluator_matches_the_rust_golden_vectors() {
    if !docker_available() {
        eprintln!("skipping feature_materialize_e2e: Docker is not available");
        return;
    }
    let _guard = start_surrealdb();
    seed_schema();

    // ---- fn::feature::hash --------------------------------------------
    // The exact values pinned by `flag.rs::rollout_hash_matches_golden_vectors`
    // and by the scheduler's copy. v3 removed the `<int>'0x…'` cast the
    // original one-liner used, so this also guards the hand-rolled hex fold.
    let body = sql(
        "RETURN [
            fn::feature::hash('checkout-v2', 'user:alice'),
            fn::feature::hash('checkout-v2', 'user:bob'),
            fn::feature::hash('flag-x', 'user:abc'),
            fn::feature::hash('beta', 'user:42'),
            fn::feature::hash('rollout', 'user:zzz')
        ];",
    );
    assert!(
        body.contains("[8,62,0,90,17]") || body.contains("[8, 62, 0, 90, 17]"),
        "fn::feature::hash drifted from the Rust golden vectors: {body}"
    );

    sql("CREATE user:abc SET username = 'abc', password = 'x';
         CREATE user:xyz SET username = 'xyz', password = 'x';
         CREATE user:alice SET username = 'alice', password = 'x';");

    // ---- disabled flag ignores its rules ------------------------------
    sql("CREATE _00_feature_flag SET key = 'd1', enabled = false, default_variant = 'off',
           variants = ['off','on'],
           rules = [{ kind: 'allowlist', variant: 'on', users: ['user:abc'], priority: 10 }];
         RETURN fn::feature::materialize('d1');");
    assert!(
        sql("SELECT VALUE variant FROM _00_user_feature WHERE key = 'd1' AND user = user:abc;")
            .contains("\"off\""),
        "a disabled flag must return default_variant regardless of rules"
    );

    // ---- rollout 0 never matches, 100 always matches -------------------
    sql("CREATE _00_feature_flag SET key = 'r0', enabled = true, default_variant = 'off',
           variants = ['off','on'],
           rules = [{ kind: 'rollout', variant: 'on', percent: 0, priority: 50 }];
         CREATE _00_feature_flag SET key = 'r100', enabled = true, default_variant = 'off',
           variants = ['off','on'],
           rules = [{ kind: 'rollout', variant: 'on', percent: 100, priority: 50 }];
         RETURN fn::feature::materialize('r0');
         RETURN fn::feature::materialize('r100');");
    let zero = sql("SELECT VALUE variant FROM _00_user_feature WHERE key = 'r0';");
    assert!(!zero.contains("\"on\""), "0% rollout matched someone: {zero}");
    let hundred = sql("SELECT VALUE variant FROM _00_user_feature WHERE key = 'r100';");
    assert!(
        !hundred.contains("\"off\""),
        "100% rollout missed someone: {hundred}"
    );

    // ---- allowlist beats a lower-priority rollout ----------------------
    sql("CREATE _00_feature_flag SET key = 'ab', enabled = true, default_variant = 'off',
           variants = ['off','on'],
           rules = [
             { kind: 'rollout', variant: 'on', percent: 0, priority: 50 },
             { kind: 'allowlist', variant: 'on', users: ['user:abc'], priority: 10 }
           ];
         RETURN fn::feature::materialize('ab');");
    assert!(
        sql("SELECT VALUE variant FROM _00_user_feature WHERE key = 'ab' AND user = user:abc;")
            .contains("\"on\""),
        "allowlisted user should win over the 0% rollout"
    );
    assert!(
        sql("SELECT VALUE variant FROM _00_user_feature WHERE key = 'ab' AND user = user:xyz;")
            .contains("\"off\""),
        "non-allowlisted user should fall through to the default"
    );

    // ---- payload resolves for the chosen variant -----------------------
    sql("CREATE _00_feature_flag SET key = 'pl', enabled = true, default_variant = 'off',
           variants = ['off','treatment'],
           rules = [{ kind: 'allowlist', variant: 'treatment', users: ['user:abc'], priority: 10 }],
           payloads = { treatment: { copy: 'Hello' } };
         RETURN fn::feature::materialize('pl');");
    let payload =
        sql("SELECT variant, payload FROM _00_user_feature WHERE key = 'pl' AND user = user:abc;");
    assert!(
        payload.contains("treatment") && payload.contains("Hello"),
        "payload did not resolve for the chosen variant: {payload}"
    );

    // ---- rollout bucketing agrees with the hash ------------------------
    // alice hashes to 8 for 'checkout-v2', so a 10% rollout must include her.
    sql("CREATE _00_feature_flag SET key = 'checkout-v2', enabled = true, default_variant = 'off',
           variants = ['off','on'],
           rules = [{ kind: 'rollout', variant: 'on', percent: 10, priority: 50 }];
         RETURN fn::feature::materialize('checkout-v2');");
    assert!(
        sql("SELECT VALUE variant FROM _00_user_feature WHERE key = 'checkout-v2' AND user = user:alice;")
            .contains("\"on\""),
        "rollout bucketing disagrees with fn::feature::hash (alice hashes to 8, under 10%)"
    );

    // ---- equal priorities keep declaration order -----------------------
    // Rust sorts with the stable `sort_by_key`, so the first rule wins.
    sql("CREATE _00_feature_flag SET key = 'tie', enabled = true, default_variant = 'off',
           variants = ['off','first','second'],
           rules = [
             { kind: 'allowlist', variant: 'first', users: ['user:abc'] },
             { kind: 'allowlist', variant: 'second', users: ['user:abc'] }
           ];
         RETURN fn::feature::materialize('tie');");
    assert!(
        sql("SELECT VALUE variant FROM _00_user_feature WHERE key = 'tie' AND user = user:abc;")
            .contains("\"first\""),
        "equal priorities must keep declaration order (stable sort)"
    );

    // ---- allow() / disallow() round trip -------------------------------
    sql("RETURN fn::feature::allow('ab', 'on', user:xyz);");
    assert!(
        sql("SELECT VALUE variant FROM _00_user_feature WHERE key = 'ab' AND user = user:xyz;")
            .contains("\"on\""),
        "fn::feature::allow should assign and re-materialize"
    );
    sql("RETURN fn::feature::disallow('ab', user:xyz);");
    assert!(
        sql("SELECT VALUE variant FROM _00_user_feature WHERE key = 'ab' AND user = user:xyz;")
            .contains("\"off\""),
        "fn::feature::disallow should revoke and re-materialize"
    );

    // ---- unknown flag drops orphaned assignments -----------------------
    let gone = sql("RETURN fn::feature::materialize('does-not-exist');");
    assert!(
        gone.contains("false"),
        "materializing an unknown flag should report found: false, got {gone}"
    );

    // ---- the user-count guard fires ------------------------------------
    // `SELECT VALUE id FROM user` runs under the CALLER's permissions, so an
    // app that hides users would silently materialize almost nobody. The
    // scheduler stamps the true count for exactly this check.
    sql("UPSERT _00_module_state:feature_user_count SET count = 99;");
    let throttled = sql("RETURN fn::feature::materialize('ab');");
    assert!(
        throttled.contains("can only see"),
        "the user-count guard did not fire: {throttled}"
    );
    sql("DELETE _00_module_state:feature_user_count;");
}

#[test]
#[ignore = "requires Docker; run with: cargo test -p sp00ky-cli --test feature_materialize_e2e -- --ignored"]
fn only_admins_can_read_or_change_flags() {
    if !docker_available() {
        eprintln!("skipping feature_materialize_e2e: Docker is not available");
        return;
    }
    let _guard = start_surrealdb();
    seed_schema();

    let admin_token = signup("adm");
    let user_token = signup("reg");

    sql("CREATE _00_feature_flag SET key = 'beta', enabled = true, default_variant = 'off',
           variants = ['off','on'], rules = [];
         LET $a = (SELECT VALUE id FROM ONLY user WHERE username = 'adm' LIMIT 1);
         UPSERT _00_admin SET user = $a WHERE user = $a;
         RETURN fn::feature::materialize('beta');");

    // ---- non-admin ------------------------------------------------------
    let flags = sql_as(&user_token, "SELECT * FROM _00_feature_flag;");
    assert!(
        !flags.contains("\"beta\""),
        "a non-admin must not see flag definitions (targeting rules leak otherwise): {flags}"
    );
    let roster = sql_as(&user_token, "SELECT * FROM _00_admin;");
    assert!(
        !roster.contains("_00_admin:"),
        "a non-admin must not see the admin roster: {roster}"
    );
    assert!(
        sql_as(&user_token, "RETURN fn::feature::materialize('beta');").contains("permission"),
        "a non-admin must not be able to call fn::feature::materialize"
    );
    assert!(
        sql_as(&user_token, "RETURN fn::feature::allow('beta','on', $auth.id);")
            .contains("permission"),
        "a non-admin must not be able to call fn::feature::allow"
    );

    // `assign` takes the flag as an argument, so an open one would let anyone
    // write any assignment for themselves from a forged definition.
    assert!(
        sql_as(
            &user_token,
            "RETURN fn::feature::assign({ key: 'beta', default_variant: 'HACKED' }, [], $auth.id);"
        )
        .contains("permission"),
        "a non-admin must not be able to call fn::feature::assign"
    );
    assert!(
        sql_as(&user_token, "RETURN fn::feature::reassign('beta', $auth.id);").contains("permission"),
        "a non-admin must not be able to call fn::feature::reassign"
    );

    // A denied write is a silent no-op in SurrealDB, so assert the DATA, not
    // the response: this is the check that stops self-enabling a flag.
    sql_as(
        &user_token,
        "UPSERT _00_user_feature SET user = $auth.id, key = 'beta', variant = 'HACKED' \
         WHERE key = 'beta' AND user = $auth.id;",
    );
    sql_as(&user_token, "CREATE _00_admin SET user = $auth.id;");
    let after = sql("SELECT VALUE variant FROM _00_user_feature WHERE key = 'beta';");
    assert!(
        !after.contains("HACKED"),
        "a non-admin forged a _00_user_feature row: {after}"
    );
    let roster_count = sql("SELECT count() FROM _00_admin GROUP ALL;");
    assert!(
        roster_count.contains("\"count\":1"),
        "a non-admin promoted themselves into _00_admin: {roster_count}"
    );

    // Their OWN assignment stays readable — that is what `client.feature()` uses.
    assert!(
        sql_as(&user_token, "SELECT key FROM _00_user_feature;").contains("beta"),
        "a user must still be able to read their own assignment"
    );

    // ---- admin ----------------------------------------------------------
    assert!(
        sql_as(&admin_token, "SELECT key FROM _00_feature_flag;").contains("beta"),
        "an admin must be able to read flag definitions"
    );
    let allow = sql_as(
        &admin_token,
        "LET $r = (SELECT VALUE id FROM ONLY user WHERE username = 'reg' LIMIT 1);
         RETURN fn::feature::allow('beta', 'on', $r);",
    );
    assert!(all_ok(&allow), "an admin must be able to allowlist a user: {allow}");
    assert!(
        sql("SELECT VALUE variant FROM _00_user_feature WHERE key = 'beta' AND user.username = 'reg';")
            .contains("\"on\""),
        "the admin's change did not reach the other user's assignment"
    );

    // Even an admin cannot grow the roster — that stays root-only (`spky admin`).
    sql_as(&admin_token, "CREATE _00_admin SET user = $auth.id;");
    let roster_count = sql("SELECT count() FROM _00_admin GROUP ALL;");
    assert!(
        roster_count.contains("\"count\":1"),
        "an admin was able to write _00_admin: {roster_count}"
    );
}

#[test]
#[ignore = "requires Docker; run with: cargo test -p sp00ky-cli --test feature_materialize_e2e -- --ignored"]
fn only_changed_assignments_are_written() {
    if !docker_available() {
        eprintln!("skipping feature_materialize_e2e: Docker is not available");
        return;
    }
    let _guard = start_surrealdb();
    seed_schema();

    sql("CREATE user:abc SET username = 'abc', password = 'x';
         CREATE user:xyz SET username = 'xyz', password = 'x';
         CREATE user:alice SET username = 'alice', password = 'x';
         CREATE _00_feature_flag SET key = 'w', enabled = true, default_variant = 'off',
           variants = ['off','on'],
           rules = [{ kind: 'allowlist', variant: 'on', users: ['user:abc'], priority: 10 }],
           payloads = { on: { copy: 'Hello' } };");

    // ---- first materialize creates one row per user --------------------
    sql("RETURN fn::feature::materialize('w');");
    assert_eq!(posted_count("w"), 3, "one create per user: {}", posted("w"));
    let stamps = result_of(&sql("SELECT user, evaluated_at FROM _00_user_feature WHERE key = 'w' ORDER BY user;"));

    // ---- a re-materialize with nothing changed writes nothing ----------
    sql("RETURN fn::feature::materialize('w');");
    assert_eq!(
        posted_count("w"),
        3,
        "an unchanged assignment must not post to the SSP: {}",
        posted("w")
    );
    assert_eq!(
        result_of(&sql("SELECT user, evaluated_at FROM _00_user_feature WHERE key = 'w' ORDER BY user;")),
        stamps,
        "evaluated_at must stay put while the assignment does"
    );

    // ---- allow / disallow write the one user they touch ----------------
    let allowed = sql("RETURN fn::feature::allow('w', 'on', user:xyz);");
    assert!(allowed.contains("\"users\":1"), "allow re-evaluates one user: {allowed}");
    assert_eq!(posted_count("w"), 4, "allow must write only user:xyz: {}", posted("w"));
    assert!(
        sql("SELECT variant, payload FROM _00_user_feature WHERE key = 'w' AND user = user:xyz;")
            .contains("Hello"),
        "allow must assign the variant and its payload"
    );
    sql("RETURN fn::feature::disallow('w', user:xyz);");
    assert_eq!(posted_count("w"), 5, "disallow must write only user:xyz: {}", posted("w"));
    assert!(
        sql("SELECT VALUE variant FROM _00_user_feature WHERE key = 'w' AND user = user:xyz;")
            .contains("\"off\""),
        "disallow must fall back to the default"
    );
    let abc_stamp = sql("SELECT VALUE evaluated_at FROM _00_user_feature WHERE key = 'w' AND user = user:abc;");

    // ---- a definition change posts only for the users it moves ---------
    // A 100% rollout under the allowlist moves everyone not already 'on'.
    sql("UPDATE _00_feature_flag SET rules += { kind: 'rollout', variant: 'on', percent: 100, priority: 50 } WHERE key = 'w';
         RETURN fn::feature::materialize('w');");
    assert_eq!(
        posted_count("w"),
        7,
        "only xyz and alice moved to 'on'; abc was already there: {}",
        posted("w")
    );
    assert_eq!(
        result_of(&sql("SELECT VALUE evaluated_at FROM _00_user_feature WHERE key = 'w' AND user = user:abc;")),
        result_of(&abc_stamp),
        "abc's assignment did not change, so its stamp must not either"
    );

    // ---- allow on a never-materialized flag writes one row -------------
    // The e2e spec's shape: a flag created by hand, so nobody has a row yet.
    // Filling the others in is the scheduler sweep's job, not this edit's.
    sql("CREATE _00_feature_flag SET key = 'fresh', enabled = true, default_variant = 'off',
           variants = ['off','on'], rules = [];
         RETURN fn::feature::allow('fresh', 'on', user:alice);");
    assert_eq!(posted_count("fresh"), 1, "allow on a fresh flag: {}", posted("fresh"));
    assert!(
        sql("SELECT VALUE variant FROM _00_user_feature WHERE key = 'fresh' AND user = user:alice;")
            .contains("\"on\""),
        "the allowlisted user must be on"
    );

    // ---- only user records get a row ------------------------------------
    sql("RETURN fn::feature::allow('fresh', 'on', post:1);");
    assert_eq!(posted_count("fresh"), 1, "a non-user record must not get a row");

    // ---- the same holds for an admin calling from the browser ----------
    // The SET reads the stored row under the admin's permissions, which
    // cannot SELECT other users' assignments. The no-op must still hold.
    let admin_token = signup("adm");
    sql("LET $a = (SELECT VALUE id FROM ONLY user WHERE username = 'adm' LIMIT 1);
         UPSERT _00_admin SET user = $a WHERE user = $a;
         RETURN fn::feature::materialize('w');");
    let before = posted_count("w");
    let again = sql_as(&admin_token, "RETURN fn::feature::materialize('w');");
    assert!(all_ok(&again), "an admin must be able to re-materialize: {again}");
    assert_eq!(
        posted_count("w"),
        before,
        "an admin's no-op re-materialize must not post: {}",
        posted("w")
    );
    let allowed = sql_as(&admin_token, "RETURN fn::feature::disallow('w', user:abc);");
    assert!(all_ok(&allowed), "an admin must be able to disallow: {allowed}");
    assert_eq!(
        posted_count("w"),
        before,
        "abc stays 'on' through the 100% rollout, so nothing moves: {}",
        posted("w")
    );
}
