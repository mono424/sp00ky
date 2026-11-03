//! The data plane against a real embedded SurrealDB: every statement here is one
//! the dashboard sends, so a shape the engine stops accepting fails in this file.

use std::sync::Arc;

use serde_json::{json, Value};
use surrealdb::engine::local::{Db as MemEngine, Mem};
use surrealdb::opt::capabilities::{Capabilities, ExperimentalFeature};
use surrealdb::opt::Config;
use surrealdb::Surreal;

use super::*;

struct MemDb(Arc<Surreal<MemEngine>>);

#[async_trait::async_trait]
impl ScheduleDb for MemDb {
    async fn query(&self, surql: &str, binds: &[(&str, Value)]) -> Result<Vec<Value>, ScheduleDbError> {
        let mut q = self.0.query(surql);
        for (name, value) in binds {
            q = q.bind(((*name).to_string(), value.clone()));
        }
        let mut response = q.await.map_err(|e| ScheduleDbError::Transport(e.to_string()))?;
        let n = response.num_statements();
        let mut out = Vec::with_capacity(n);
        for i in 0..n {
            let val: surrealdb::types::Value = response.take(i).map_err(|e| ScheduleDbError::Query(e.to_string()))?;
            out.push(val.into_json_value());
        }
        Ok(out)
    }
}

const SCHEMA: &str = r#"
DEFINE ACCESS account ON DATABASE TYPE RECORD
  SIGNUP { RETURN CREATE ONLY user SET username = $username, password = crypto::argon2::generate($password); }
  SIGNIN ( SELECT * FROM user WHERE username = $username AND crypto::argon2::compare(pw_hash, $password) )
  DURATION FOR TOKEN 15m, FOR SESSION 30d;
DEFINE TABLE user SCHEMAFULL;
DEFINE FIELD username ON TABLE user TYPE string ASSERT string::len($value) > 3;
DEFINE FIELD pw_hash ON TABLE user TYPE string;
DEFINE FIELD api_token ON TABLE user TYPE option<string>;
DEFINE FIELD tokens_used ON TABLE user TYPE option<int>;
DEFINE FIELD created_at ON TABLE user TYPE datetime DEFAULT time::now() READONLY;
DEFINE FIELD age ON TABLE user TYPE option<int>;
DEFINE FIELD last_seen ON TABLE user TYPE option<datetime>;
DEFINE FIELD friend ON TABLE user TYPE option<record<user>>;
DEFINE FIELD friends ON TABLE user TYPE option<array<record<user>>>;
DEFINE FIELD bio ON TABLE user TYPE option<string>;
DEFINE FIELD meta ON TABLE user TYPE option<object> FLEXIBLE;
DEFINE INDEX unique_username ON TABLE user FIELDS username UNIQUE;
DEFINE TABLE note SCHEMALESS;
DEFINE TABLE likes TYPE RELATION FROM user TO user SCHEMAFULL;
DEFINE TABLE _00_admin SCHEMAFULL;
DEFINE FIELD user ON _00_admin TYPE record;
DEFINE FIELD note ON _00_admin TYPE option<string>;
DEFINE FIELD added_at ON _00_admin TYPE datetime DEFAULT time::now() READONLY;
DEFINE TABLE _00_user_feature SCHEMAFULL;
DEFINE FIELD user ON _00_user_feature TYPE record;
DEFINE FIELD key ON _00_user_feature TYPE string;
DEFINE FIELD variant ON _00_user_feature TYPE string;
DEFINE TABLE _00_query SCHEMALESS;
DEFINE BUCKET pics BACKEND "memory" COMMENT 'sp00ky:maxSize=5242880';
DEFINE BUCKET avatars BACKEND "memory";
DEFINE FUNCTION fn::_00_bucket::avatars($action: string, $file: string, $target: option<string>) { RETURN true };
CREATE user:alice SET username = 'alice', pw_hash = crypto::argon2::generate('correct horse'), api_token = 'tok-1', tokens_used = 7, age = 30, bio = 'hi';
CREATE user:bobby SET username = 'bobby', pw_hash = 'x', age = 20;
CREATE user:carol SET username = 'carol', pw_hash = 'x', meta = { secret: 'nope', public: 'yes' };
CREATE _00_admin SET user = user:alice;
CREATE _00_user_feature SET user = user:alice, key = 'beta', variant = 'on';
CREATE _00_query SET auth_id = 'user:alice', clientId = 'c1', lastActiveAt = time::now(), ttl = 1h;
CREATE _00_query SET auth_id = 'user:alice', clientId = 'c1', lastActiveAt = time::now(), ttl = 1h;
CREATE _00_query SET auth_id = 'user:alice', clientId = 'c2', lastActiveAt = time::now(), ttl = 1h;
CREATE _00_query SET auth_id = 'user:bobby', clientId = 'c3', lastActiveAt = time::now() - 3h, ttl = 1h;
CREATE _00_query SET auth_id = 'anon', clientId = 'c4', lastActiveAt = time::now(), ttl = 1h;
DEFINE TABLE _00_push_credential SCHEMAFULL;
DEFINE FIELD secret ON _00_push_credential TYPE string;
CREATE _00_push_credential SET secret = 'apns-key';
DEFINE TABLE _00_push_subscription SCHEMALESS;
"#;

async fn setup() -> MemDb {
    let capabilities = Capabilities::all().with_experimental_feature_allowed(ExperimentalFeature::Files);
    let db = Surreal::new::<Mem>(Config::new().capabilities(capabilities)).await.unwrap();
    db.use_ns("t").use_db("t").await.unwrap();
    db.query(SCHEMA).await.unwrap().check().unwrap();
    MemDb(Arc::new(db))
}

async fn call(db: &MemDb, method: &str, path: &str, query: &[(&str, &str)], body: Value) -> AdminResult<Value> {
    call_as(db, "breakglass", method, path, query, body).await
}

async fn call_as(db: &MemDb, actor: &str, method: &str, path: &str, query: &[(&str, &str)], body: Value) -> AdminResult<Value> {
    let segments: Vec<&str> = path.split('/').collect();
    let pairs: Vec<(String, String)> = query.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect();
    let bytes = if body.is_null() { Vec::new() } else { body.to_string().into_bytes() };
    dispatch(db, Request { method, path: &segments, query: &pairs, body: &bytes, actor, dirs: None }).await
}

// --- pure ---------------------------------------------------------------------

#[test]
fn cast_kinds_read_the_database_spellings() {
    assert_eq!(cast_kind("datetime").as_deref(), Some("datetime"));
    assert_eq!(cast_kind("none | datetime").as_deref(), Some("datetime"));
    assert_eq!(cast_kind("option<int>").as_deref(), Some("int"));
    assert_eq!(cast_kind("none | array<record<user>>").as_deref(), Some("array<record<user>>"));
    assert_eq!(cast_kind("array<int | string>").as_deref(), Some("array<int | string>"));
    assert_eq!(cast_kind("any"), None);
    assert_eq!(cast_kind("none | string | int"), None);
    assert_eq!(cast_kind("'a' | 'b'"), None);
    assert!(is_optional_kind("none | string"));
    assert!(!is_optional_kind("array<none | string>"));
}

#[test]
fn signin_clauses_name_the_table_and_fields() {
    let s = parse_signin("account", "SELECT * FROM user WHERE username = $username AND crypto::argon2::compare(password, $password)").unwrap();
    assert_eq!(s.table, "user");
    assert_eq!(s.identifier.as_deref(), Some("username"));
    assert_eq!(s.password.as_deref(), Some("password"));
    assert_eq!(s.algorithm.as_deref(), Some("argon2"));

    // SurrealDB 4 keeps the parentheses; the order of the tests is the app's.
    let s = parse_signin("a", "(SELECT * FROM account WHERE crypto::bcrypt::compare(pass, $p) AND email = $email)").unwrap();
    assert_eq!(s.table, "account");
    assert_eq!(s.identifier.as_deref(), Some("email"));
    assert_eq!(s.password.as_deref(), Some("pass"));
    assert_eq!(s.algorithm.as_deref(), Some("bcrypt"));

    // A block that reads the row into a variable, and an ONLY.
    let s = parse_signin("a", "{ LET $u = (SELECT * FROM ONLY member WHERE email = $email); IF crypto::scrypt::compare($u.pw_h, $pass) { RETURN $u; }; }").unwrap();
    assert_eq!(s.table, "member");
    assert_eq!(s.identifier.as_deref(), Some("email"));
    assert_eq!(s.password.as_deref(), Some("pw_h"));
    assert_eq!(s.algorithm.as_deref(), Some("scrypt"));
    assert!(parse_signin("a", "SELECT * FROM type::table('user') WHERE a = $a").is_none(), "a function is not a table");

    // Nothing usable about the password is not the same as nothing at all.
    let s = parse_signin("a", "SELECT * FROM user WHERE string::lowercase(email) = $email").unwrap();
    assert_eq!(s.table, "user");
    assert_eq!(s.identifier, None);
    assert_eq!(s.password, None);
    assert!(parse_signin("a", "RETURN fn::custom($x)").is_none());
}

#[test]
fn a_bare_select_gets_a_limit_and_a_timeout() {
    let g = |sql: &str| guarded_select(sql, 1001).map(|(s, _)| s.split_whitespace().collect::<Vec<_>>().join(" "));
    assert_eq!(g("SELECT * FROM user").as_deref(), Some("SELECT * FROM user LIMIT 1001 TIMEOUT 30s"));
    assert_eq!(g("select * from user;").as_deref(), Some("select * from user LIMIT 1001 TIMEOUT 30s ;"));
    assert_eq!(g("SELECT * FROM user LIMIT 5").as_deref(), Some("SELECT * FROM user LIMIT 5 TIMEOUT 30s"));
    assert_eq!(g("SELECT * FROM user LIMIT 5 TIMEOUT 1s"), None);
    assert_eq!(g("SELECT * FROM user TIMEOUT 1s").as_deref(), Some("SELECT * FROM user LIMIT 1001 TIMEOUT 1s"));
    assert_eq!(g("SELECT * FROM user START 5 FETCH friend").as_deref(), Some("SELECT * FROM user LIMIT 1001 START 5 FETCH friend TIMEOUT 30s"));
    assert_eq!(g("SELECT * FROM user EXPLAIN").as_deref(), Some("SELECT * FROM user LIMIT 1001 TIMEOUT 30s EXPLAIN"));
    // A word in a string, a subquery, or before FROM is not a clause.
    assert_eq!(g("SELECT limit, start FROM user").as_deref(), Some("SELECT limit, start FROM user LIMIT 1001 TIMEOUT 30s"));
    assert_eq!(g("SELECT * FROM user WHERE a = 'LIMIT 1'").as_deref(), Some("SELECT * FROM user WHERE a = 'LIMIT 1' LIMIT 1001 TIMEOUT 30s"));
    assert_eq!(
        g("SELECT *, (SELECT * FROM x LIMIT 1) AS y FROM user").as_deref(),
        Some("SELECT *, (SELECT * FROM x LIMIT 1) AS y FROM user LIMIT 1001 TIMEOUT 30s")
    );
    // A trailing comment stays behind the clauses.
    assert_eq!(g("SELECT * FROM user -- all").as_deref(), Some("SELECT * FROM user LIMIT 1001 TIMEOUT 30s -- all"));
    // A field named after a clause is a field: this still needs its limit.
    assert_eq!(g("SELECT * FROM user WHERE limit = 5").as_deref(), Some("SELECT * FROM user WHERE limit = 5 LIMIT 1001 TIMEOUT 30s"));
    assert_eq!(g("SELECT * FROM user WHERE start > 5").as_deref(), Some("SELECT * FROM user WHERE start > 5 LIMIT 1001 TIMEOUT 30s"));
    assert_eq!(g("SELECT * FROM user LIMIT $n").as_deref(), Some("SELECT * FROM user LIMIT $n TIMEOUT 30s"));
    // Everything else is the operator's own.
    assert_eq!(g("UPDATE user SET a = 1"), None);
    assert_eq!(g("SELECT * FROM a; SELECT * FROM b"), None);
    assert_eq!(g("RETURN 1"), None);
}

#[test]
fn keys_cannot_leave_the_bucket() {
    assert_eq!(checked_key(Some("/a/b.txt")).unwrap(), "a/b.txt");
    for bad in ["", "/", "a/", "../x", "a/../x", "a//b", "a/./b", "a\0b"] {
        assert!(checked_key(Some(bad)).is_err(), "{bad:?} should be refused");
    }
    assert_eq!(checked_prefix(Some("")).unwrap(), "");
    assert_eq!(checked_prefix(Some("/a/b/")).unwrap(), "a/b/");
    assert!(checked_prefix(Some("a/../b")).is_err());
}

#[test]
fn credential_names_catch_the_field_not_the_word_next_to_it() {
    for name in [
        "password", "apiToken", "share_privkey", "accessKey", "api-key", "x-api-key", "private-key", "jwt", "pass", "pw_hash",
        "salt", "AUTHORIZATION", "user_jwt",
    ] {
        assert!(is_credential_name(name), "{name}");
    }
    for name in ["username", "author", "compass", "passage", "description", "bypass", "keyboard", "assets"] {
        assert!(!is_credential_name(name), "{name}");
    }
}

#[test]
fn the_kill_switch_reads_like_a_person_wrote_it() {
    for off in ["0", "false", "off", "OFF", "False", " no "] {
        assert!(is_off(Some(off)), "{off}");
    }
    assert!(!is_off(None));
    assert!(!is_off(Some("on")));
}

#[test]
fn ids_must_name_the_route_table() {
    assert_eq!(checked_id("user", Some("user:abc")).unwrap(), "user:abc");
    assert_eq!(checked_id("user", Some("user:`a-b`")).unwrap(), "user:`a-b`");
    for bad in ["_00_admin:x", "user", "user:", "users:x", "x"] {
        assert!(checked_id("user", Some(bad)).is_err(), "{bad} should be refused");
    }
    assert!(checked_id("user", None).is_err());
}

// --- tables -------------------------------------------------------------------

#[tokio::test]
async fn tables_list_without_leaking_access_keys() {
    let db = setup().await;
    let out = call(&db, "GET", "tables", &[], Value::Null).await.unwrap();
    let names: Vec<&str> = out["tables"].as_array().unwrap().iter().map(|t| t["name"].as_str().unwrap()).collect();
    assert!(names.contains(&"user") && names.contains(&"_00_admin"));
    let likes = out["tables"].as_array().unwrap().iter().find(|t| t["name"] == "likes").unwrap();
    assert_eq!(likes["kind"], "relation");
    assert_eq!(likes["in"], json!(["user"]));
    assert!(!out.to_string().contains("REDACTED") && !out.to_string().contains("argon2"));
}

#[tokio::test]
async fn table_detail_says_what_each_field_takes() {
    let db = setup().await;
    let out = call(&db, "GET", "tables/user", &[], Value::Null).await.unwrap();
    let field = |name: &str| out["fields"].as_array().unwrap().iter().find(|f| f["name"] == name).unwrap().clone();
    assert_eq!(field("username")["required"], true);
    assert_eq!(field("age")["optional"], true);
    assert_eq!(field("created_at")["readonly"], true);
    assert_eq!(field("created_at")["required"], false);
    // By name, and by the SIGNIN clause for a name that does not say so.
    assert_eq!(field("api_token")["credential"], true);
    assert_eq!(field("pw_hash")["credential"], true);
    assert_eq!(field("age")["credential"], false);
    assert_eq!(out["indexes"][0]["name"], "unique_username");
    assert_eq!(out["read_only"], false);
    assert_eq!(call(&db, "GET", "tables/nope", &[], Value::Null).await.unwrap_err().status, 404);
    assert_eq!(call(&db, "GET", "tables/a b", &[], Value::Null).await.unwrap_err().status, 400);
}

#[tokio::test]
async fn rows_page_and_hide_credentials() {
    let db = setup().await;
    let out = call(&db, "GET", "tables/user/rows", &[("limit", "2"), ("sort", "username"), ("dir", "desc")], Value::Null).await.unwrap();
    assert_eq!(out["has_more"], true);
    let rows = out["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0]["id"], "user:carol");
    // Nested too, and only strings: a count called tokens_used is a count.
    assert_eq!(rows[0]["pw_hash"], json!({ "$redacted": true }));
    assert_eq!(rows[0]["meta"]["secret"], json!({ "$redacted": true }));
    assert_eq!(rows[0]["meta"]["public"], "yes");

    let out = call(&db, "GET", "tables/user/rows", &[("limit", "2"), ("start", "2"), ("sort", "username")], Value::Null).await.unwrap();
    assert_eq!(out["has_more"], false);
    assert_eq!(out["rows"][0]["id"], "user:carol");

    let out = call(&db, "GET", "tables/user/row", &[("id", "user:alice")], Value::Null).await.unwrap();
    assert_eq!(out["row"]["api_token"], json!({ "$redacted": true }));
    assert_eq!(out["row"]["tokens_used"], 7);
    assert_eq!(call(&db, "GET", "tables/user/row", &[("id", "user:nope")], Value::Null).await.unwrap_err().status, 404);
    assert_eq!(call(&db, "GET", "tables/user/row", &[("id", "_00_admin:x")], Value::Null).await.unwrap_err().status, 400);
}

#[tokio::test]
async fn a_credential_cannot_be_read_by_asking_about_it() {
    let db = setup().await;
    let filter = |field: &str, op: &str, value: &str| json!([{ "field": field, "op": op, "value": value }]).to_string();
    // By name, by the SIGNIN clause, through a record link, in both routes that filter.
    for (field, op) in [("api_token", "gte"), ("pw_hash", "starts_with"), ("friend.pw_hash", "gte"), ("friend.username", "eq")] {
        let f = filter(field, op, "a");
        assert_eq!(call(&db, "GET", "tables/user/rows", &[("filter", f.as_str())], Value::Null).await.unwrap_err().status, 400, "{field}");
        assert_eq!(call(&db, "GET", "tables/user/count", &[("filter", f.as_str())], Value::Null).await.unwrap_err().status, 400, "{field}");
    }
    for sort in ["api_token", "pw_hash", "friend.pw_hash"] {
        assert_eq!(call(&db, "GET", "tables/user/rows", &[("sort", sort)], Value::Null).await.unwrap_err().status, 400, "{sort}");
        assert_eq!(call(&db, "GET", "users", &[("sort", sort)], Value::Null).await.unwrap_err().status, 400, "{sort}");
    }
    // An object's text cannot be searched for what is masked inside it.
    let f = filter("meta", "contains", "nope");
    let out = call(&db, "GET", "tables/user/rows", &[("filter", f.as_str())], Value::Null).await.unwrap();
    assert_eq!(out["rows"], json!([]));
    // Single values are still searchable, whatever their type.
    for (field, text) in [("id", "ALI"), ("age", "3"), ("username", "ALI")] {
        let f = filter(field, "contains", text);
        let out = call(&db, "GET", "tables/user/rows", &[("filter", f.as_str())], Value::Null).await.unwrap();
        assert_eq!(out["rows"].as_array().unwrap().len(), 1, "{field}");
    }
    let many: Vec<Value> = (0..17).map(|_| json!({ "field": "age", "op": "is_none" })).collect();
    let f = Value::Array(many).to_string();
    assert_eq!(call(&db, "GET", "tables/user/rows", &[("filter", f.as_str())], Value::Null).await.unwrap_err().status, 400);
}

#[tokio::test]
async fn whatever_a_credential_is_shaped_like_it_stays_hidden() {
    let db = setup().await;
    db.query(
        "DEFINE TABLE vault SCHEMALESS; CREATE vault:a SET secret = { stripe: 'sk_live' }, credentials = ['one'], \
         apiKey = 123456, tokens_used = 7, settings = { 'x-api-key': 'abc', theme: 'dark' };",
        &[],
    )
    .await
    .unwrap();
    let out = call(&db, "GET", "tables/vault/row", &[("id", "vault:a")], Value::Null).await.unwrap();
    let row = &out["row"];
    let mask = json!({ "$redacted": true });
    assert_eq!(row["secret"], mask);
    assert_eq!(row["credentials"], mask);
    assert_eq!(row["apiKey"], mask);
    assert_eq!(row["settings"]["x-api-key"], mask);
    assert_eq!(row["settings"]["theme"], "dark");
    assert_eq!(row["tokens_used"], 7, "a count is not a credential");
    assert!(!out.to_string().contains("sk_live"));
}

#[tokio::test]
async fn saving_a_form_never_writes_a_mask_over_what_it_hides() {
    let db = setup().await;
    let masked = json!({ "set": { "meta": { "secret": { "$redacted": true }, "public": "edited" } } });
    let refused = call(&db, "PUT", "tables/user/row", &[("id", "user:carol")], masked).await.unwrap_err();
    assert_eq!(refused.status, 400, "{refused:?}");
    let cut = json!({ "set": { "bio": { "$trimmed": true, "bytes": 9, "preview": "x" } } });
    assert_eq!(call(&db, "PUT", "tables/user/row", &[("id", "user:carol")], cut).await.unwrap_err().status, 400);
    let kept = db.query("RETURN user:carol.meta.secret;", &[]).await.unwrap();
    assert_eq!(kept[0], "nope", "the value behind the mask is untouched");
}

#[tokio::test]
async fn push_keys_and_device_tokens_are_not_browsed() {
    let db = setup().await;
    let tables = call(&db, "GET", "tables", &[], Value::Null).await.unwrap();
    assert!(!tables.to_string().contains("_00_push"));
    for (path, query) in [
        ("tables/_00_push_credential", vec![]),
        ("tables/_00_push_credential/rows", vec![]),
        ("tables/_00_push_credential/count", vec![]),
        ("tables/_00_push_credential/row", vec![("id", "_00_push_credential:x")]),
        ("tables/_00_push_subscription/rows", vec![]),
    ] {
        assert_eq!(call(&db, "GET", path, &query, Value::Null).await.unwrap_err().status, 403, "{path}");
    }
}

#[tokio::test]
async fn a_definition_does_not_carry_literals_out() {
    let db = setup().await;
    db.query(
        "DEFINE FIELD webhook_secret ON TABLE user TYPE option<string> DEFAULT 'sk_literal'; \
         DEFINE EVENT notify ON user WHEN $event = 'CREATE' THEN { LET $h = 'Bearer sk_event'; };",
        &[],
    )
    .await
    .unwrap();
    let out = call(&db, "GET", "tables/user", &[], Value::Null).await.unwrap();
    let text = out.to_string();
    assert!(!text.contains("sk_literal"), "{text}");
    assert!(!text.contains("sk_event"), "{text}");
    assert_eq!(out["events"][0]["name"], "notify");
    let buckets = call(&db, "GET", "buckets", &[], Value::Null).await.unwrap();
    assert!(buckets["buckets"][0].get("permissions").is_none());
}

#[tokio::test]
async fn long_values_are_cut_in_a_list_and_kept_on_the_row() {
    let db = setup().await;
    let long = "x".repeat(LIST_CELL_BYTES + 10);
    call(&db, "PUT", "tables/user/row", &[("id", "user:bobby")], json!({ "set": { "bio": long } })).await.unwrap();
    let out = call(&db, "GET", "tables/user/rows", &[("sort", "username")], Value::Null).await.unwrap();
    let bio = &out["rows"][1]["bio"];
    assert_eq!(bio["$trimmed"], true);
    assert_eq!(bio["bytes"], LIST_CELL_BYTES + 10);
    assert_eq!(bio["preview"].as_str().unwrap().len(), PREVIEW_CHARS);
    let out = call(&db, "GET", "tables/user/row", &[("id", "user:bobby")], Value::Null).await.unwrap();
    assert_eq!(out["row"]["bio"].as_str().unwrap().len(), LIST_CELL_BYTES + 10);
}

#[tokio::test]
async fn filters_are_typed_by_the_schema() {
    let db = setup().await;
    let run_filter = |filter: Value| {
        let db = &db;
        async move {
            let text = filter.to_string();
            let out = call(db, "GET", "tables/user/rows", &[("filter", text.as_str()), ("sort", "username")], Value::Null).await?;
            AdminResult::Ok(out["rows"].as_array().unwrap().iter().map(|r| r["username"].as_str().unwrap().to_string()).collect::<Vec<_>>())
        }
    };
    assert_eq!(run_filter(json!([{ "field": "age", "op": "gte", "value": "30" }])).await.unwrap(), ["alice"]);
    assert_eq!(run_filter(json!([{ "field": "username", "op": "contains", "value": "OBB" }])).await.unwrap(), ["bobby"]);
    assert_eq!(run_filter(json!([{ "field": "username", "op": "starts_with", "value": "ca" }])).await.unwrap(), ["carol"]);
    assert_eq!(run_filter(json!([{ "field": "age", "op": "is_none" }])).await.unwrap(), ["carol"]);
    assert_eq!(run_filter(json!([{ "field": "age", "op": "not_none" }, { "field": "age", "op": "lt", "value": "25" }])).await.unwrap(), ["bobby"]);
    assert_eq!(run_filter(json!([{ "field": "id", "op": "eq", "value": "user:carol" }])).await.unwrap(), ["carol"]);
    assert_eq!(run_filter(json!([{ "field": "created_at", "op": "gt", "value": "2000-01-01T00:00:00Z" }])).await.unwrap().len(), 3);
    // A value is a value, however it reads.
    assert!(run_filter(json!([{ "field": "username", "op": "eq", "value": "x' OR true --" }])).await.unwrap().is_empty());
    assert_eq!(run_filter(json!([{ "field": "age; DELETE user", "op": "eq", "value": "1" }])).await.unwrap_err().status, 400);
    assert_eq!(run_filter(json!([{ "field": "age", "op": "DROP", "value": "1" }])).await.unwrap_err().status, 400);
    // A value the column cannot be is the database's own sentence.
    assert_eq!(run_filter(json!([{ "field": "age", "op": "eq", "value": "old" }])).await.unwrap_err().status, 400);

    let filter = json!([{ "field": "age", "op": "not_none" }]).to_string();
    let out = call(&db, "GET", "tables/user/count", &[("filter", filter.as_str())], Value::Null).await.unwrap();
    assert_eq!(out["count"], 2);
    let out = call(&db, "GET", "tables/user/count", &[], Value::Null).await.unwrap();
    assert_eq!(out["count"], 3);
}

#[tokio::test]
async fn rows_write_with_the_casts_their_fields_need() {
    let db = setup().await;
    let out = call(
        &db,
        "PUT",
        "tables/user/row",
        &[("id", "user:alice")],
        json!({ "set": {
            "age": 31,
            "last_seen": "2026-01-02T03:04:05Z",
            "friend": "user:bobby",
            "friends": ["user:bobby", "user:carol"],
            "meta": { "a": 1 },
            "bio": null
        } }),
    )
    .await
    .unwrap();
    assert_eq!(out["row"]["age"], 31);
    assert_eq!(out["row"]["last_seen"], "2026-01-02T03:04:05Z");
    assert_eq!(out["row"]["friend"], "user:bobby");
    assert_eq!(out["row"]["friends"], json!(["user:bobby", "user:carol"]));
    assert!(out["row"].get("bio").is_none_or(Value::is_null));
    assert_eq!(out["row"]["pw_hash"], json!({ "$redacted": true }));

    // The ones it stores as a record and a datetime, not as strings.
    let check = db.query("SELECT VALUE [type::is_record(friend), type::is_datetime(last_seen)] FROM ONLY user:alice;", &[]).await.unwrap();
    assert_eq!(check[0], json!([true, true]));

    let out = call(&db, "PUT", "tables/user/row", &[("id", "user:alice")], json!({ "unset": ["age"] })).await.unwrap();
    assert!(out["row"].get("age").is_none_or(Value::is_null));

    let refused = |set: Value| call(&db, "PUT", "tables/user/row", &[("id", "user:alice")], json!({ "set": set }));
    assert_eq!(refused(json!({ "pw_hash": "x" })).await.unwrap_err().status, 400);
    assert_eq!(refused(json!({ "api_token": "x" })).await.unwrap_err().status, 400);
    assert_eq!(refused(json!({ "id": "user:x" })).await.unwrap_err().status, 400);
    assert_eq!(refused(json!({ "age = 1, username": "x" })).await.unwrap_err().status, 400);
    assert_eq!(refused(json!({ "username": "abc" })).await.unwrap_err().status, 400); // ASSERT
    assert_eq!(refused(json!({ "age": "old" })).await.unwrap_err().status, 400); // cast
    assert_eq!(call(&db, "PUT", "tables/user/row", &[("id", "user:nope")], json!({ "set": { "age": 1 } })).await.unwrap_err().status, 404);
}

#[tokio::test]
async fn rows_create_and_delete() {
    let db = setup().await;
    let out = call(&db, "POST", "tables/note/rows", &[], json!({ "id": "first", "set": { "text": "hello", "n": 2 } })).await.unwrap();
    assert_eq!(out["row"]["id"], "note:first");
    assert_eq!(out["row"]["n"], 2);
    let out = call(&db, "POST", "tables/note/rows", &[], json!({ "set": { "text": "second" } })).await.unwrap();
    let second = out["row"]["id"].as_str().unwrap().to_string();
    assert!(second.starts_with("note:"));
    let out = call(&db, "POST", "tables/note/rows", &[], json!({})).await.unwrap();
    let third = out["row"]["id"].as_str().unwrap().to_string();

    let out = call(&db, "POST", "tables/likes/rows", &[], json!({ "set": { "in": "user:alice", "out": "user:bobby" } })).await.unwrap();
    assert_eq!(out["row"]["in"], "user:alice");
    assert_eq!(call(&db, "POST", "tables/likes/rows", &[], json!({ "set": { "in": "user:alice" } })).await.unwrap_err().status, 400);

    let out = call(&db, "DELETE", "tables/note/row", &[("id", "note:first")], Value::Null).await.unwrap();
    assert_eq!(out["deleted"], 1);
    let out = call(&db, "POST", "tables/note/rows/delete", &[], json!({ "ids": [second, third, "note:gone"] })).await.unwrap();
    assert_eq!(out["deleted"], 2);
    assert_eq!(call(&db, "POST", "tables/note/rows/delete", &[], json!({ "ids": ["user:alice"] })).await.unwrap_err().status, 400);
    let left = call(&db, "GET", "tables/note/count", &[], Value::Null).await.unwrap();
    assert_eq!(left["count"], 0);
}

#[tokio::test]
async fn internal_tables_read_but_do_not_write() {
    let db = setup().await;
    let out = call(&db, "GET", "tables/_00_admin/rows", &[], Value::Null).await.unwrap();
    let id = out["rows"][0]["id"].as_str().unwrap().to_string();
    let detail = call(&db, "GET", "tables/_00_admin", &[], Value::Null).await.unwrap();
    assert_eq!(detail["read_only"], true);
    for (method, path, query, body) in [
        ("PUT", "tables/_00_admin/row", vec![("id", id.as_str())], json!({ "set": { "note": "x" } })),
        ("DELETE", "tables/_00_admin/row", vec![("id", id.as_str())], Value::Null),
        ("POST", "tables/_00_admin/rows", vec![], json!({ "set": {} })),
        ("POST", "tables/_00_admin/rows/delete", vec![], json!({ "ids": [id] })),
    ] {
        assert_eq!(call(&db, method, path, &query, body).await.unwrap_err().status, 403, "{method} {path}");
    }
}

#[tokio::test]
async fn the_editor_runs_statements_and_bounds_a_bare_select() {
    let db = setup().await;
    let out = call(&db, "POST", "query", &[], json!({ "sql": "SELECT VALUE username FROM user ORDER BY username" })).await.unwrap();
    assert_eq!(out["results"][0]["rows"], json!(["alice", "bobby", "carol"]));
    assert_eq!(out["limit_added"], MAX_QUERY_ROWS);

    let out = call(&db, "POST", "query", &[], json!({ "sql": "SELECT VALUE username FROM user ORDER BY username LIMIT 1" })).await.unwrap();
    assert_eq!(out["results"][0]["rows"], json!(["alice"]));
    assert_eq!(out["limit_added"], Value::Null);

    let out = call(&db, "POST", "query", &[], json!({ "sql": "LET $x = 2; RETURN $x + 1; CREATE note:n SET a = 1;" })).await.unwrap();
    let results = out["results"].as_array().unwrap();
    assert_eq!(results.len(), 3);
    assert_eq!(results[1]["value"], 3);
    assert_eq!(results[2]["rows"][0]["id"], "note:n");

    // A table named after a clause reads as one, and the original still runs.
    db.query("DEFINE TABLE start SCHEMALESS; CREATE start:one;", &[]).await.unwrap();
    let out = call(&db, "POST", "query", &[], json!({ "sql": "SELECT * FROM start" })).await.unwrap();
    assert_eq!(out["results"][0]["rows"][0]["id"], "start:one");

    // More rows than the page shows says so.
    db.query("FOR $i IN 0..1100 { CREATE note SET n = $i; };", &[]).await.unwrap();
    let out = call(&db, "POST", "query", &[], json!({ "sql": "RETURN (SELECT * FROM note)" })).await.unwrap();
    assert_eq!(out["results"][0]["truncated"], true);
    assert_eq!(out["results"][0]["rows"].as_array().unwrap().len(), MAX_QUERY_ROWS);

    assert_eq!(call(&db, "POST", "query", &[], json!({ "sql": "  " })).await.unwrap_err().status, 400);
    let bad = call(&db, "POST", "query", &[], json!({ "sql": "SELECT FROM WHERE" })).await.unwrap_err();
    assert_eq!(bad.status, 400);
}

// --- users --------------------------------------------------------------------

#[tokio::test]
async fn users_come_from_the_signin_clause() {
    let db = setup().await;
    let out = call(&db, "GET", "users", &[], Value::Null).await.unwrap();
    let source = &out["source"];
    assert_eq!(source["table"], "user");
    assert_eq!(source["access"], "account");
    assert_eq!(source["identifier"], "username");
    assert_eq!(source["created_field"], "created_at");
    assert_eq!(source["can_set_password"], true);
    let users = out["users"].as_array().unwrap();
    assert_eq!(users.len(), 3);
    assert!(!out.to_string().contains("argon2"), "a hash must not reach the page");

    let alice = users.iter().find(|u| u["row"]["id"] == "user:alice").unwrap();
    assert_eq!(alice["is_admin"], true);
    assert_eq!(alice["views"], 3);
    assert_eq!(alice["sessions"], 2);
    assert_eq!(alice["row"]["pw_hash"], json!({ "$redacted": true }));
    let bobby = users.iter().find(|u| u["row"]["id"] == "user:bobby").unwrap();
    assert_eq!(bobby["is_admin"], false);
    assert_eq!(bobby["views"], 0, "an expired view is not presence");

    let ids = |out: &Value| out["users"].as_array().unwrap().iter().map(|u| u["row"]["id"].as_str().unwrap().to_string()).collect::<Vec<_>>();
    let out = call(&db, "GET", "users", &[("q", "BOB")], Value::Null).await.unwrap();
    assert_eq!(ids(&out), ["user:bobby"]);
    let out = call(&db, "GET", "users", &[("filter", "admins")], Value::Null).await.unwrap();
    assert_eq!(ids(&out), ["user:alice"]);
    let out = call(&db, "GET", "users", &[("filter", "online")], Value::Null).await.unwrap();
    assert_eq!(ids(&out), ["user:alice"]);
    let out = call(&db, "GET", "users", &[("filter", "admins"), ("q", "bob")], Value::Null).await.unwrap();
    assert!(ids(&out).is_empty());

    let out = call(&db, "GET", "users/stats", &[], Value::Null).await.unwrap();
    assert_eq!(out, json!({ "total": 3, "admins": 1, "online": 1 }));
}

#[tokio::test]
async fn user_detail_carries_roster_sessions_and_flags() {
    let db = setup().await;
    let out = call(&db, "GET", "users/user", &[("id", "user:alice")], Value::Null).await.unwrap();
    assert_eq!(out["row"]["username"], "alice");
    assert_eq!(out["is_admin"], true);
    assert_eq!(out["flags"], json!([{ "key": "beta", "variant": "on" }]));
    let mut sessions = out["sessions"].as_array().unwrap().clone();
    sessions.sort_by_key(|s| s["client_id"].as_str().unwrap().to_string());
    assert_eq!(sessions, [json!({ "client_id": "c1", "views": 2 }), json!({ "client_id": "c2", "views": 1 })]);
    let out = call(&db, "GET", "users/user", &[("id", "user:carol")], Value::Null).await.unwrap();
    assert_eq!(out["is_admin"], false);
    assert_eq!(call(&db, "GET", "users/user", &[("id", "user:nope")], Value::Null).await.unwrap_err().status, 404);
}

#[tokio::test]
async fn passwords_are_set_the_way_signin_checks_them() {
    let db = setup().await;
    let check = |who: &'static str, password: &'static str| {
        let db = &db;
        async move {
            let out = db.query(
                "SELECT VALUE crypto::argon2::compare(pw_hash, $p) FROM user WHERE username = $u;",
                &[("u", json!(who)), ("p", json!(password))],
            ).await.unwrap();
            out[0][0] == json!(true)
        }
    };
    assert!(check("alice", "correct horse").await);
    call(&db, "PUT", "users/user/password", &[("id", "user:alice")], json!({ "password": "battery staple" })).await.unwrap();
    assert!(check("alice", "battery staple").await);
    assert!(!check("alice", "correct horse").await);
    assert_eq!(call(&db, "PUT", "users/user/password", &[("id", "user:alice")], json!({ "password": "short" })).await.unwrap_err().status, 400);
    assert_eq!(call(&db, "PUT", "users/user/password", &[("id", "user:nope")], json!({ "password": "long enough" })).await.unwrap_err().status, 404);

    let out = call(&db, "POST", "users", &[], json!({ "identifier": "newuser", "password": "long enough", "fields": { "age": 9 } })).await.unwrap();
    assert_eq!(out["row"]["username"], "newuser");
    assert_eq!(out["row"]["age"], 9);
    assert_eq!(out["row"]["pw_hash"], json!({ "$redacted": true }));
    assert!(check("newuser", "long enough").await);
    assert_eq!(call(&db, "POST", "users", &[], json!({ "identifier": "newuser", "password": "long enough" })).await.unwrap_err().status, 400, "unique index");
    assert_eq!(call(&db, "POST", "users", &[], json!({ "identifier": "abc", "password": "long enough" })).await.unwrap_err().status, 400, "ASSERT");
    assert_eq!(call(&db, "POST", "users", &[], json!({ "password": "long enough" })).await.unwrap_err().status, 400);
    assert_eq!(call(&db, "POST", "users", &[], json!({ "identifier": "other", "password": "long enough", "fields": { "api_token": "x" } })).await.unwrap_err().status, 400);
}

#[tokio::test]
async fn the_roster_moves_and_nobody_removes_themselves() {
    let db = setup().await;
    let is_admin = |id: &'static str| {
        let db = &db;
        async move { call(db, "GET", "users/user", &[("id", id)], Value::Null).await.unwrap()["is_admin"] == json!(true) }
    };
    assert!(!is_admin("user:carol").await);
    call(&db, "PUT", "users/user/admin", &[("id", "user:carol")], json!({ "admin": true, "note": "oncall" })).await.unwrap();
    assert!(is_admin("user:carol").await);
    let detail = call(&db, "GET", "users/user", &[("id", "user:carol")], Value::Null).await.unwrap();
    assert_eq!(detail["admin"]["note"], "oncall");

    // Twice is still one row (the index is unique on a real schema).
    call(&db, "PUT", "users/user/admin", &[("id", "user:carol")], json!({ "admin": true })).await.unwrap();
    let out = db.query("SELECT VALUE id FROM _00_admin WHERE user = user:carol;", &[]).await.unwrap();
    assert_eq!(out[0].as_array().unwrap().len(), 1);

    call(&db, "PUT", "users/user/admin", &[("id", "user:carol")], json!({ "admin": false })).await.unwrap();
    assert!(!is_admin("user:carol").await);
    assert_eq!(call(&db, "PUT", "users/user/admin", &[("id", "user:nope")], json!({ "admin": true })).await.unwrap_err().status, 404);

    // However the id is spelled, it is the same person.
    for spelled in ["user:alice", "user:`alice`", "user:⟨alice⟩"] {
        let refused = call_as(&db, "user:alice", "PUT", "users/user/admin", &[("id", spelled)], json!({ "admin": false })).await.unwrap_err();
        assert_eq!(refused.status, 400, "{spelled}");
        assert!(is_admin("user:alice").await, "{spelled}");
        let refused = call_as(&db, "user:alice", "DELETE", "users/user", &[("id", spelled)], Value::Null).await.unwrap_err();
        assert_eq!(refused.status, 400, "{spelled}");
    }

    // A note stays when nobody says otherwise.
    call(&db, "PUT", "users/user/admin", &[("id", "user:carol")], json!({ "admin": true, "note": "oncall" })).await.unwrap();
    call(&db, "PUT", "users/user/admin", &[("id", "user:carol")], json!({ "admin": true })).await.unwrap();
    let detail = call(&db, "GET", "users/user", &[("id", "user:carol")], Value::Null).await.unwrap();
    assert_eq!(detail["admin"]["note"], "oncall");
}

#[tokio::test]
async fn users_are_sorted_when_asked_or_when_an_index_makes_it_free() {
    let db = setup().await;
    // The fixture's created_at has no index: id order until someone asks.
    let out = call(&db, "GET", "users", &[], Value::Null).await.unwrap();
    assert_eq!(out["source"]["created_indexed"], false);
    assert_eq!(out["users"][0]["row"]["id"], "user:alice");

    let out = call(&db, "GET", "users", &[("sort", "username"), ("dir", "desc")], Value::Null).await.unwrap();
    assert_eq!(out["users"][0]["row"]["id"], "user:carol");

    db.query("DEFINE INDEX user_created ON TABLE user FIELDS created_at;", &[]).await.unwrap();
    let out = call(&db, "GET", "users", &[], Value::Null).await.unwrap();
    assert_eq!(out["source"]["created_indexed"], true);
    assert_eq!(out["users"][0]["row"]["id"], "user:carol", "newest first");
}

#[tokio::test]
async fn an_access_on_a_missing_table_falls_through_to_one_that_exists() {
    let db = setup().await;
    db.query(
        "DEFINE ACCESS aaa_gone ON DATABASE TYPE RECORD SIGNIN (SELECT * FROM nowhere WHERE a = $a) DURATION FOR TOKEN 15m;",
        &[],
    )
    .await
    .unwrap();
    let out = call(&db, "GET", "users", &[], Value::Null).await.unwrap();
    assert_eq!(out["source"]["table"], "user");
    assert_eq!(out["source"]["access"], "account");
}

#[tokio::test]
async fn keys_keep_every_character_they_were_given() {
    let db = setup().await;
    put(&db, "a.txt ", "spaced").await;
    put(&db, "a.txt", "plain").await;
    let out = call(&db, "GET", "buckets/pics/file", &[("key", "a.txt "), ("meta", "1")], Value::Null).await.unwrap();
    assert_eq!(out["size"], 6);
}

#[tokio::test]
async fn deleting_a_user_takes_their_internal_rows() {
    let db = setup().await;
    call(&db, "DELETE", "users/user", &[("id", "user:alice")], Value::Null).await.unwrap();
    assert_eq!(call(&db, "GET", "users/user", &[("id", "user:alice")], Value::Null).await.unwrap_err().status, 404);
    let out = db.query("SELECT VALUE id FROM _00_admin; SELECT VALUE id FROM _00_user_feature;", &[]).await.unwrap();
    assert_eq!(out[0], json!([]));
    assert_eq!(out[1], json!([]));
    assert_eq!(call(&db, "DELETE", "users/user", &[("id", "user:alice")], Value::Null).await.unwrap_err().status, 404);
}

#[tokio::test]
async fn a_project_without_users_says_so() {
    let capabilities = Capabilities::all().with_experimental_feature_allowed(ExperimentalFeature::Files);
    let inner = Surreal::new::<Mem>(Config::new().capabilities(capabilities)).await.unwrap();
    inner.use_ns("t").use_db("t").await.unwrap();
    let db = MemDb(Arc::new(inner));
    let out = call(&db, "GET", "users", &[], Value::Null).await.unwrap();
    assert_eq!(out["source"], Value::Null);
    assert_eq!(call(&db, "GET", "users/stats", &[], Value::Null).await.unwrap()["total"], Value::Null);
    assert_eq!(call(&db, "GET", "tables", &[], Value::Null).await.unwrap()["tables"], json!([]));
    assert_eq!(call(&db, "GET", "buckets", &[], Value::Null).await.unwrap()["buckets"], json!([]));
}

// --- buckets ------------------------------------------------------------------

async fn put(db: &MemDb, key: &str, text: &str) {
    let data = base64(text.as_bytes());
    call(db, "PUT", "buckets/pics/file", &[("key", key)], json!({ "data": data })).await.unwrap();
}

fn base64(bytes: &[u8]) -> String {
    const ALPHABET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::new();
    for chunk in bytes.chunks(3) {
        let n = (chunk[0] as u32) << 16 | (*chunk.get(1).unwrap_or(&0) as u32) << 8 | *chunk.get(2).unwrap_or(&0) as u32;
        for i in 0..=chunk.len() {
            out.push(ALPHABET[(n >> (18 - 6 * i) & 63) as usize] as char);
        }
    }
    while out.len() % 4 != 0 {
        out.push('=');
    }
    out
}

#[tokio::test]
async fn buckets_describe_their_backend() {
    let db = setup().await;
    let out = call(&db, "GET", "buckets", &[], Value::Null).await.unwrap();
    let bucket = |name: &str| out["buckets"].as_array().unwrap().iter().find(|b| b["name"] == name).unwrap().clone();
    assert_eq!(bucket("pics")["backend"], "memory");
    assert_eq!(bucket("pics")["max_bytes"], 5242880);
    assert_eq!(bucket("pics")["browsable"], true);
    assert_eq!(bucket("pics")["lists_folders"], true);
    assert_eq!(bucket("avatars")["backend"], "r2");
    assert_eq!(bucket("avatars")["browsable"], false);
    assert_eq!(call(&db, "PUT", "buckets/avatars/file", &[("key", "x")], json!({ "data": "" })).await.unwrap_err().status, 501);
    assert_eq!(call(&db, "PUT", "buckets/nope/file", &[("key", "x")], json!({ "data": "" })).await.unwrap_err().status, 404);
}

#[tokio::test]
async fn a_folder_lists_files_and_subfolders_one_level_deep() {
    let db = setup().await;
    // The emoji sorts past where a folder is stepped over to, so it is met again.
    for key in ["top.txt", "a/one.txt", "a/two.txt", "a/b/deep.txt", "a/b/c/deeper.txt", "a/b/\u{1F600}.txt", "ab/z.txt", "zz/last.txt"] {
        put(&db, key, "x").await;
    }
    let folders = |v: &Value| v["folders"].as_array().unwrap().iter().map(|s| s.as_str().unwrap().to_string()).collect::<Vec<_>>();
    let files = |v: &Value| v["files"].as_array().unwrap().iter().map(|f| f["name"].as_str().unwrap().to_string()).collect::<Vec<_>>();

    let root = call(&db, "GET", "buckets/pics/files", &[], Value::Null).await.unwrap();
    assert_eq!(folders(&root), ["a", "ab", "zz"]);
    assert_eq!(files(&root), ["top.txt"]);
    assert_eq!(root["next"], Value::Null);

    let a = call(&db, "GET", "buckets/pics/files", &[("prefix", "a/")], Value::Null).await.unwrap();
    assert_eq!(folders(&a), ["b"]);
    assert_eq!(files(&a), ["one.txt", "two.txt"]);
    assert_eq!(a["files"][0]["key"], "a/one.txt");
    assert_eq!(a["files"][0]["size"], 1);
    assert!(a["files"][0]["updated"].as_str().unwrap().contains('T'));

    let empty = call(&db, "GET", "buckets/pics/files", &[("prefix", "nope")], Value::Null).await.unwrap();
    assert!(folders(&empty).is_empty() && files(&empty).is_empty());
    assert_eq!(call(&db, "GET", "buckets/pics/files", &[("prefix", "../etc")], Value::Null).await.unwrap_err().status, 400);
}

#[tokio::test]
async fn a_folder_pages_through_everything_exactly_once() {
    let db = setup().await;
    let mut expected = Vec::new();
    for i in 0..7 {
        put(&db, &format!("f{i}.txt"), "x").await;
        expected.push(format!("f{i}.txt"));
    }
    for i in 0..4 {
        put(&db, &format!("dir{i}/inner.txt"), "x").await;
        put(&db, &format!("dir{i}/more.txt"), "x").await;
        expected.push(format!("dir{i}/"));
    }
    let mut seen = Vec::new();
    let mut start: Option<String> = None;
    for _ in 0..20 {
        let mut query = vec![("limit", "3")];
        if let Some(s) = &start {
            query.push(("start", s.as_str()));
        }
        let page = call(&db, "GET", "buckets/pics/files", &query, Value::Null).await.unwrap();
        seen.extend(page["folders"].as_array().unwrap().iter().map(|f| format!("{}/", f.as_str().unwrap())));
        seen.extend(page["files"].as_array().unwrap().iter().map(|f| f["name"].as_str().unwrap().to_string()));
        start = page["next"].as_str().map(str::to_string);
        if start.is_none() {
            break;
        }
    }
    seen.sort();
    expected.sort();
    assert_eq!(seen, expected);
}

#[tokio::test]
async fn files_round_trip_rename_and_delete() {
    let db = setup().await;
    put(&db, "docs/hello.txt", "hello world").await;

    let out = call(&db, "GET", "buckets/pics/file", &[("key", "docs/hello.txt")], Value::Null).await.unwrap();
    assert_eq!(out["size"], 11);
    assert_eq!(out["data"].as_str().unwrap().trim_end_matches('='), base64(b"hello world").trim_end_matches('='));
    let out = call(&db, "GET", "buckets/pics/file", &[("key", "docs/hello.txt"), ("meta", "1")], Value::Null).await.unwrap();
    assert_eq!(out["size"], 11);
    assert!(out.get("data").is_none());
    assert_eq!(call(&db, "GET", "buckets/pics/file", &[("key", "nope")], Value::Null).await.unwrap_err().status, 404);

    // Bytes that are not text survive the trip.
    let binary: Vec<u8> = (0u8..=255).collect();
    call(&db, "PUT", "buckets/pics/file", &[("key", "bin")], json!({ "data": base64(&binary) })).await.unwrap();
    let out = call(&db, "GET", "buckets/pics/file", &[("key", "bin")], Value::Null).await.unwrap();
    assert_eq!(out["size"], 256);
    assert_eq!(out["data"].as_str().unwrap().trim_end_matches('='), base64(&binary).trim_end_matches('='));

    // A key in use takes `overwrite`.
    let data = base64(b"other");
    let conflict = call(&db, "PUT", "buckets/pics/file", &[("key", "docs/hello.txt")], json!({ "data": data })).await.unwrap_err();
    assert_eq!(conflict.status, 409);
    call(&db, "PUT", "buckets/pics/file", &[("key", "docs/hello.txt"), ("overwrite", "1")], json!({ "data": data })).await.unwrap();
    assert_eq!(call(&db, "GET", "buckets/pics/file", &[("key", "docs/hello.txt"), ("meta", "1")], Value::Null).await.unwrap()["size"], 5);

    call(&db, "POST", "buckets/pics/rename", &[], json!({ "key": "docs/hello.txt", "target": "docs/renamed.txt" })).await.unwrap();
    assert_eq!(call(&db, "GET", "buckets/pics/file", &[("key", "docs/hello.txt")], Value::Null).await.unwrap_err().status, 404);
    put(&db, "docs/taken.txt", "x").await;
    let conflict = call(&db, "POST", "buckets/pics/rename", &[], json!({ "key": "docs/renamed.txt", "target": "docs/taken.txt" })).await.unwrap_err();
    assert_eq!(conflict.status, 409);

    assert_eq!(call(&db, "GET", "buckets/pics/file", &[("key", "docs/taken.txt"), ("meta", "1")], Value::Null).await.unwrap()["size"], 1);
    let missing = call(&db, "POST", "buckets/pics/rename", &[], json!({ "key": "docs/ghost.txt", "target": "docs/ghost2.txt" })).await;
    assert!(missing.is_err(), "renaming nothing must not read as success: {missing:?}");

    call(&db, "DELETE", "buckets/pics/file", &[("key", "docs/taken.txt")], Value::Null).await.unwrap();
    call(&db, "POST", "buckets/pics/delete", &[], json!({ "keys": ["docs/renamed.txt", "bin"] })).await.unwrap();
    let root = call(&db, "GET", "buckets/pics/files", &[], Value::Null).await.unwrap();
    assert_eq!(root["files"], json!([]));
    assert_eq!(root["folders"], json!([]));
}

/// A bucket volume, as a host that mounts it would read it.
struct FakeDisk;

#[async_trait::async_trait]
impl BucketDirs for FakeDisk {
    fn covers(&self, location: &str) -> bool {
        location.starts_with("/buckets/")
    }

    async fn list(&self, location: &str, prefix: &str, start: Option<&str>, limit: usize) -> Result<DirPage, String> {
        assert_eq!(location, "/buckets/disk");
        if prefix == "boom/" {
            return Err("permission denied".to_string());
        }
        assert_eq!(limit, 100);
        assert!(start.is_none_or(|s| !s.contains('/')), "a disk listing pages by name: {start:?}");
        Ok(DirPage {
            folders: vec![(format!("{prefix}sub").replace('/', "_"), 3)],
            files: vec![DirFile { name: "a.png".into(), size: 7, updated: Some("2026-01-01T00:00:00Z".into()) }],
            next: start.is_none().then(|| "a.png".to_string()),
        })
    }
}

async fn with_disk(db: &MemDb, path: &str, query: &[(&str, &str)]) -> AdminResult<Value> {
    let segments: Vec<&str> = path.split('/').collect();
    let pairs: Vec<(String, String)> = query.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect();
    dispatch(db, Request { method: "GET", path: &segments, query: &pairs, body: &[], actor: "x", dirs: Some(&FakeDisk) }).await
}

#[tokio::test]
async fn a_host_with_the_volume_lists_subfolders_on_disk_buckets() {
    let db = setup().await;
    db.query("DEFINE BUCKET disk BACKEND \"memory\";", &[]).await.unwrap();
    // A memory bucket stands in: the listing only reads its backend's location.
    let info = call(&db, "GET", "buckets", &[], Value::Null).await.unwrap();
    assert_eq!(info["buckets"].as_array().unwrap().iter().find(|b| b["name"] == "pics").unwrap()["lists_folders"], true);

    // Pure: a file: backend lists folders only where a host covers it.
    let entry = json!({ "name": "disk", "backend": "file:/buckets/disk" });
    let empty = json!({});
    assert_eq!(describe_bucket(&entry, &empty, None)["lists_folders"], false);
    assert_eq!(describe_bucket(&entry, &empty, Some(&FakeDisk))["lists_folders"], true);
    let elsewhere = json!({ "name": "disk", "backend": "file:/elsewhere/disk" });
    assert_eq!(describe_bucket(&elsewhere, &empty, Some(&FakeDisk))["lists_folders"], false);

    // The page has the database listing's shape, keys included, plus the counts.
    let page = list_dir(&FakeDisk, "/buckets/disk", "a/b/", None, 100).await.unwrap();
    assert_eq!(page["prefix"], "a/b/");
    assert_eq!(page["folders"], json!(["a_b_sub"]));
    assert_eq!(page["counts"], json!({ "a_b_sub": 3 }));
    assert_eq!(page["files"][0]["key"], "a/b/a.png");
    assert_eq!(page["files"][0]["name"], "a.png");
    assert_eq!(page["files"][0]["size"], 7);
    assert_eq!(page["next"], "a/b/a.png");
    // The cursor is a key, as in the database's listing; FakeDisk is handed the name.
    let last = list_dir(&FakeDisk, "/buckets/disk", "a/b/", Some("a/b/a.png"), 100).await.unwrap();
    assert_eq!(last["next"], Value::Null);
    let failed = list_dir(&FakeDisk, "/buckets/disk", "boom/", None, 100).await.unwrap_err();
    assert_eq!(failed.status, 502);
    assert!(failed.message.contains("permission denied"));

    // Without a file: backend, the disk is not consulted: FakeDisk would panic.
    let root = with_disk(&db, "buckets/pics/files", &[]).await.unwrap();
    assert_eq!(root["folders"], json!([]));
    // A traversal is refused before any host sees it.
    assert_eq!(with_disk(&db, "buckets/pics/files", &[("prefix", "../x")]).await.unwrap_err().status, 400);
}

#[tokio::test]
async fn unknown_routes_are_404() {
    let db = setup().await;
    assert_eq!(call(&db, "GET", "nope", &[], Value::Null).await.unwrap_err().status, 404);
    assert_eq!(call(&db, "DELETE", "tables/user", &[], Value::Null).await.unwrap_err().status, 404);
}
