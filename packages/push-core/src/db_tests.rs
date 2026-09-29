//! Close-to-e2e tests: a real embedded SurrealDB with the REAL push DDL
//! (`include_str!`'d from `apps/cli/src/push_tables.surql`, so a DDL change
//! that breaks the engine's SQL or the record-user API fails here rather than
//! on a deploy), record users signing up through a real `DEFINE ACCESS`, and
//! a recording `PushHttp` whose bodies are decrypted with the fake browser's
//! private key.

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine as _;
use p256::elliptic_curve::sec1::ToEncodedPoint;
use p256::SecretKey;
use rand_core::{OsRng, RngCore};
use serde_json::{json, Value};
use surrealdb::engine::local::{Db as MemEngine, Mem};
use surrealdb::opt::auth::Record;
use surrealdb::Surreal;

use crate::config::{PayloadKind, PushConfig, PushPayload};
use crate::engine::{EngineOptions, ObservedChange, Origin, PushEngine, PushHttp};
use crate::util::{b64url, hex, now_ms, sha256};
use crate::{Op, ScheduleDb, ScheduleDbError, VapidKeys};

// --- adapters ---------------------------------------------------------------

struct MemDb(Surreal<MemEngine>);

#[async_trait::async_trait]
impl ScheduleDb for MemDb {
    async fn query(
        &self,
        surql: &str,
        binds: &[(&str, Value)],
    ) -> Result<Vec<Value>, ScheduleDbError> {
        let mut q = self.0.query(surql);
        for (name, value) in binds {
            q = q.bind(((*name).to_string(), value.clone()));
        }
        let mut response = q
            .await
            .map_err(|e| ScheduleDbError::Transport(e.to_string()))?;
        let n = response.num_statements();
        let mut out = Vec::with_capacity(n);
        for i in 0..n {
            let val: surrealdb::types::Value = response
                .take(i)
                .map_err(|e| ScheduleDbError::Query(e.to_string()))?;
            out.push(val.into_json_value());
        }
        Ok(out)
    }
}

const PUSH_TABLES: &str = include_str!("../../../apps/cli/src/push_tables.surql");

/// A minimal app: record users with a signup/signin access method, plus the
/// access name the platform uses for impersonation so its refusal is tested
/// against the real `$access` check.
const APP_DDL: &str = "\
DEFINE TABLE OVERWRITE user SCHEMAFULL PERMISSIONS FOR select WHERE id = $auth.id;
DEFINE FIELD OVERWRITE username ON user TYPE string;
DEFINE FIELD OVERWRITE pass ON user TYPE string;
DEFINE ACCESS OVERWRITE account ON DATABASE TYPE RECORD
    SIGNUP (CREATE user SET username = $username, pass = $pass)
    SIGNIN (SELECT * FROM user WHERE username = $username AND pass = $pass)
    DURATION FOR SESSION 1h;
DEFINE ACCESS OVERWRITE _00_impersonate ON DATABASE TYPE RECORD
    SIGNIN (SELECT * FROM user WHERE username = $username)
    DURATION FOR SESSION 1h;
DEFINE TABLE OVERWRITE message SCHEMALESS;
DEFINE TABLE OVERWRITE task SCHEMALESS;
DEFINE TABLE OVERWRITE member SCHEMALESS;";

async fn fresh_db() -> Surreal<MemEngine> {
    let db = Surreal::new::<Mem>(()).await.expect("start mem db");
    db.use_ns("test").use_db("test").await.expect("use ns/db");
    let mut r = db.query(PUSH_TABLES).await.expect("apply push DDL");
    let errors = r.take_errors();
    assert!(errors.is_empty(), "push_tables.surql rejected: {errors:?}");
    let mut r = db.query(APP_DDL).await.expect("apply app DDL");
    let errors = r.take_errors();
    assert!(errors.is_empty(), "app DDL rejected: {errors:?}");
    db
}

/// Run as `session`, failing the test on any statement error.
async fn run(session: &Surreal<MemEngine>, sql: &str, binds: Vec<(&str, Value)>) -> Vec<Value> {
    try_run(session, sql, binds)
        .await
        .unwrap_or_else(|e| panic!("`{sql}` failed: {e}"))
}

async fn try_run(
    session: &Surreal<MemEngine>,
    sql: &str,
    binds: Vec<(&str, Value)>,
) -> Result<Vec<Value>, String> {
    MemDb(session.clone())
        .query(sql, &binds)
        .await
        .map_err(|e| e.to_string())
}

async fn one(session: &Surreal<MemEngine>, sql: &str, binds: Vec<(&str, Value)>) -> Value {
    run(session, sql, binds).await.pop().unwrap_or(Value::Null)
}

async fn sign_up(root: &Surreal<MemEngine>, username: &str) -> (Surreal<MemEngine>, String) {
    let session = root.clone();
    session
        .signup(Record {
            namespace: "test".into(),
            database: "test".into(),
            access: "account".into(),
            params: json!({ "username": username, "pass": "pw" }),
        })
        .await
        .expect("signup");
    let id = one(&session, "RETURN <string> $auth.id", vec![]).await;
    (session, id.as_str().expect("auth id").to_string())
}

// --- fake browsers ----------------------------------------------------------

struct Device {
    endpoint: String,
    private: SecretKey,
    p256dh: String,
    auth: [u8; 16],
}

impl Device {
    fn new(endpoint: &str) -> Device {
        let private = SecretKey::random(&mut OsRng);
        let p256dh = b64url(private.public_key().to_encoded_point(false).as_bytes());
        let mut auth = [0u8; 16];
        OsRng.fill_bytes(&mut auth);
        Device {
            endpoint: endpoint.to_string(),
            private,
            p256dh,
            auth,
        }
    }

    fn subscription_json(&self) -> Value {
        json!({ "endpoint": self.endpoint, "keys": { "p256dh": self.p256dh, "auth": b64url(&self.auth) } })
    }

    fn open(&self, body: &[u8]) -> PushPayload {
        let plain = crate::ece::decrypt(body, &self.private, &self.auth)
            .expect("body decrypts with the device key");
        serde_json::from_slice(&plain).expect("payload is a PushPayload")
    }
}

// --- recording push service -------------------------------------------------

#[derive(Debug, Clone)]
struct Req {
    url: String,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
}

impl Req {
    fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
    }
}

type Answer = Result<(u16, String), String>;

#[derive(Default)]
struct MockHttp {
    requests: Mutex<Vec<Req>>,
    scripted: Mutex<HashMap<String, VecDeque<Answer>>>,
}

impl MockHttp {
    fn script(&self, url: &str, answers: Vec<Answer>) {
        self.scripted
            .lock()
            .unwrap()
            .insert(url.to_string(), answers.into());
    }
    fn take(&self) -> Vec<Req> {
        std::mem::take(&mut *self.requests.lock().unwrap())
    }
}

#[async_trait::async_trait]
impl PushHttp for MockHttp {
    async fn post(
        &self,
        url: &str,
        headers: Vec<(String, String)>,
        body: Vec<u8>,
    ) -> Result<(u16, String), String> {
        self.requests.lock().unwrap().push(Req {
            url: url.to_string(),
            headers,
            body,
        });
        let scripted = self
            .scripted
            .lock()
            .unwrap()
            .get_mut(url)
            .and_then(VecDeque::pop_front);
        scripted.unwrap_or(Ok((201, String::new())))
    }
}

// --- engine harness ---------------------------------------------------------

const SECRET: &str = "db-tests-secret";

struct Harness {
    root: Surreal<MemEngine>,
    http: Arc<MockHttp>,
    clock: Arc<AtomicU64>,
    engine: PushEngine,
    keys: VapidKeys,
}

fn config_json(yaml: &str) -> String {
    let cfg: PushConfig = serde_yaml::from_str(yaml).expect("config yaml");
    assert!(cfg.is_valid(), "{:?}", cfg.validate());
    serde_json::to_string(&cfg).unwrap()
}

async fn write_config(root: &Surreal<MemEngine>, yaml: &str) {
    let spec = config_json(yaml);
    let hash = hex(&sha256(spec.as_bytes())[..8]);
    run(
        root,
        "UPSERT _00_push_config:default SET spec_json = $spec, hash = $hash",
        vec![("spec", json!(spec)), ("hash", json!(hash))],
    )
    .await;
}

async fn harness(yaml: &str) -> Harness {
    let root = fresh_db().await;
    write_config(&root, yaml).await;
    let http = Arc::new(MockHttp::default());
    let clock = Arc::new(AtomicU64::new(now_ms()));
    let c = Arc::clone(&clock);
    let keys = VapidKeys::from_secret(SECRET).unwrap();
    let opts = EngineOptions {
        clock: Some(Arc::new(move || c.load(Ordering::SeqCst))),
        ..EngineOptions::default()
    };
    let engine = PushEngine::new(
        Arc::new(MemDb(root.clone())),
        Arc::clone(&http) as Arc<dyn PushHttp>,
        Some(keys.clone()),
        opts,
    );
    engine.tick().await;
    let h = Harness {
        root,
        http,
        clock,
        engine,
        keys,
    };
    run(&h.root, "CREATE user:ada SET username = 'ada', pass = 'x'; CREATE user:bob SET username = 'bob', pass = 'x';", vec![]).await;
    h
}

impl Harness {
    fn advance(&self, ms: u64) {
        self.clock.fetch_add(ms, Ordering::SeqCst);
    }

    /// A subscription row as `fn::push::subscribe` would leave it, written as
    /// root so tests control `updated_at`, `kid` and `rules`.
    async fn device(&self, user: &str, endpoint: &str, extra: &str) -> Device {
        let d = Device::new(endpoint);
        let key = hex(&sha256(format!("{user}|{endpoint}").as_bytes()));
        let sql = format!(
            "CREATE type::record('_00_push_subscription', $k) CONTENT {{ auth_id: $u, endpoint: $e, p256dh: $p, auth: $a, kid: $kid }}; \
             UPDATE type::record('_00_push_subscription', $k) SET {} RETURN NONE;",
            if extra.is_empty() { "failures = 0" } else { extra }
        );
        run(
            &self.root,
            &sql,
            vec![
                ("k", json!(key)),
                ("u", json!(user)),
                ("e", json!(endpoint)),
                ("p", json!(d.p256dh)),
                ("a", json!(b64url(&d.auth))),
                ("kid", json!(self.keys.kid())),
            ],
        )
        .await;
        d
    }

    /// Write a row, read it back flattened (what the ingest path carries),
    /// and observe it.
    async fn write_and_observe(&self, sql: &str, op: Op) -> Value {
        let rows = run(&self.root, sql, vec![]).await;
        let record = match rows.into_iter().last() {
            Some(Value::Array(mut items)) if !items.is_empty() => items.remove(0),
            Some(v @ Value::Object(_)) => v,
            other => panic!("`{sql}` returned {other:?}"),
        };
        self.observe(&record, op, Origin::Live).await;
        record
    }

    async fn observe(&self, record: &Value, op: Op, origin: Origin) {
        let id = record["id"].as_str().expect("row id").to_string();
        let table = id.split(':').next().unwrap().to_string();
        assert!(
            self.engine.wants(&table, op)
                || origin == Origin::Repair
                || table == "_00_push_message"
        );
        self.engine
            .observe(ObservedChange {
                table,
                op,
                id,
                record: record.clone(),
                origin,
                seq: 0,
            })
            .await;
    }

    async fn sub(&self, user: &str, endpoint: &str) -> Value {
        let key = hex(&sha256(format!("{user}|{endpoint}").as_bytes()));
        one(
            &self.root,
            "SELECT * FROM ONLY type::record('_00_push_subscription', $k)",
            vec![("k", json!(key))],
        )
        .await
    }

    async fn message(&self, key: &str) -> Value {
        one(
            &self.root,
            "SELECT * FROM ONLY type::record('_00_push_message', $k)",
            vec![("k", json!(key))],
        )
        .await
    }
}

fn jwt_claims(authorization: &str) -> Value {
    let t = authorization
        .strip_prefix("vapid t=")
        .expect("vapid scheme");
    let jwt = t.split(", k=").next().unwrap();
    let claims = jwt.split('.').nth(1).unwrap();
    serde_json::from_slice(&URL_SAFE_NO_PAD.decode(claims).unwrap()).unwrap()
}

// --- record-user API --------------------------------------------------------

#[tokio::test]
async fn record_users_see_and_change_only_their_own_rows() {
    let root = fresh_db().await;
    // What the engine host does at boot.
    run(&root, "DEFINE PARAM OVERWRITE $sp00ky_vapid_public_key VALUE 'BPUB' PERMISSIONS FULL; DEFINE PARAM OVERWRITE $sp00ky_vapid_kid VALUE 'k1' PERMISSIONS FULL;", vec![]).await;
    let (alice, alice_id) = sign_up(&root, "alice").await;
    let (bob, bob_id) = sign_up(&root, "bob").await;
    assert_ne!(alice_id, bob_id);

    let info = one(&alice, "RETURN fn::push::info()", vec![]).await;
    assert_eq!(
        info,
        json!({ "enabled": true, "publicKey": "BPUB", "kid": "k1", "providers": ["web"], "android": null })
    );

    let a1 = Device::new("https://push.example/a1");
    let b1 = Device::new("https://push.example/b1");
    let sub = one(
        &alice,
        "RETURN fn::push::subscribe($s, { label: 'Laptop', userAgent: 'UA' })",
        vec![("s", a1.subscription_json())],
    )
    .await;
    assert_eq!(sub["endpoint"], json!(a1.endpoint));
    assert_eq!(sub["kid"], json!("k1"));
    assert_eq!(sub["label"], json!("Laptop"));
    one(
        &bob,
        "RETURN fn::push::subscribe($s, NONE)",
        vec![("s", b1.subscription_json())],
    )
    .await;
    // Re-subscribing the same endpoint is the same row.
    one(
        &alice,
        "RETURN fn::push::subscribe($s, { label: 'Laptop 2' })",
        vec![("s", a1.subscription_json())],
    )
    .await;

    let alice_list = one(&alice, "RETURN fn::push::list()", vec![]).await;
    let alice_list = alice_list.as_array().unwrap();
    assert_eq!(alice_list.len(), 1);
    assert_eq!(alice_list[0]["endpoint"], json!(a1.endpoint));
    assert_eq!(alice_list[0]["current"], json!(true));
    assert_eq!(alice_list[0]["label"], json!("Laptop 2"));
    assert!(alice_list[0].get("p256dh").is_none(), "keys are not listed");
    let bob_list = one(&bob, "RETURN fn::push::list()", vec![]).await;
    assert_eq!(bob_list.as_array().unwrap().len(), 1);
    assert_eq!(bob_list[0]["endpoint"], json!(b1.endpoint));

    // Direct table access is scoped too.
    let seen = one(
        &alice,
        "SELECT VALUE endpoint FROM _00_push_subscription",
        vec![],
    )
    .await;
    assert_eq!(seen, json!([a1.endpoint]));
    let hijack = try_run(
        &alice,
        "UPDATE _00_push_subscription SET auth_id = $me RETURN AFTER",
        vec![("me", json!(alice_id))],
    )
    .await;
    assert!(
        hijack
            .map(|r| r[0].as_array().map(Vec::len) == Some(1))
            .unwrap_or(true),
        "only her own row can be touched"
    );
    let bob_row = one(
        &root,
        "SELECT VALUE auth_id FROM _00_push_subscription WHERE endpoint = $e",
        vec![("e", json!(b1.endpoint))],
    )
    .await;
    assert_eq!(bob_row, json!([bob_id]));

    // update: own rows only; `rules: []` resets to every rule.
    let upd = one(
        &alice,
        "RETURN fn::push::update($e, { rules: ['new-message'] })",
        vec![("e", json!(a1.endpoint))],
    )
    .await;
    assert_eq!(upd[0]["rules"], json!(["new-message"]));
    let upd = one(
        &alice,
        "RETURN fn::push::update($e, { rules: [] })",
        vec![("e", json!(a1.endpoint))],
    )
    .await;
    assert_eq!(
        upd[0].get("rules").cloned().unwrap_or(Value::Null),
        Value::Null
    );
    let foreign = one(
        &alice,
        "RETURN fn::push::update($e, { label: 'mine now' })",
        vec![("e", json!(b1.endpoint))],
    )
    .await;
    assert_eq!(foreign, json!([]));

    // notify / test create pending self-pushes.
    let msg = one(
        &alice,
        "RETURN fn::push::notify({ notification: { title: 'Remind me' }, topic: 'r1' })",
        vec![],
    )
    .await;
    assert_eq!(msg["status"], json!("pending"));
    let msg_id = msg["id"].as_str().unwrap().to_string();
    let row = one(
        &root,
        "SELECT * FROM ONLY type::record($id)",
        vec![("id", json!(msg_id))],
    )
    .await;
    assert_eq!(row["to"], json!([alice_id]));
    assert_eq!(row["created_by"], json!(alice_id));
    let later = one(
        &alice,
        "RETURN fn::push::notify({ data: { x: 1 }, sendAt: '2099-01-01T00:00:00Z' })",
        vec![],
    )
    .await;
    assert!(later["send_at"].as_str().unwrap().starts_with("2099-01-01"));
    let test_msg = one(&alice, "RETURN fn::push::test(NONE)", vec![]).await;
    let test_row = one(
        &root,
        "SELECT * FROM ONLY type::record($id)",
        vec![("id", test_msg["id"].clone())],
    )
    .await;
    assert_eq!(
        test_row["notification"]["title"],
        json!("Push notifications are on")
    );
    assert_eq!(test_row["ttl"], json!(60));

    // Messages: create for yourself only, see your own only.
    let forged = try_run(
        &alice,
        "CREATE _00_push_message CONTENT { to: [$other], created_by: $me }",
        vec![("other", json!(bob_id)), ("me", json!(alice_id))],
    )
    .await;
    assert!(
        forged.map(|r| r[0] == json!([])).unwrap_or(true),
        "a record user cannot push to someone else"
    );
    let to_bob = one(
        &root,
        "SELECT VALUE id FROM _00_push_message WHERE to CONTAINS $b",
        vec![("b", json!(bob_id))],
    )
    .await;
    assert_eq!(to_bob, json!([]));
    let own = run(
        &alice,
        "CREATE _00_push_message CONTENT { to: [$me], created_by: $me }",
        vec![("me", json!(alice_id))],
    )
    .await;
    assert_eq!(own[0].as_array().map(Vec::len), Some(1));
    let bob_sees = one(&bob, "SELECT VALUE id FROM _00_push_message", vec![]).await;
    assert_eq!(bob_sees, json!([]));
    let alice_sees = one(&alice, "SELECT VALUE id FROM _00_push_message", vec![]).await;
    assert_eq!(alice_sees.as_array().unwrap().len(), 4);

    // cancel: own pending messages only.
    let not_bobs = one(
        &bob,
        "RETURN fn::push::cancel(<record> $id)",
        vec![("id", json!(msg_id))],
    )
    .await;
    assert_eq!(not_bobs, json!(false));
    let cancelled = one(
        &alice,
        "RETURN fn::push::cancel(<record> $id)",
        vec![("id", json!(msg_id))],
    )
    .await;
    assert_eq!(cancelled, json!(true));
    let again = one(
        &alice,
        "RETURN fn::push::cancel(<record> $id)",
        vec![("id", json!(msg_id))],
    )
    .await;
    assert_eq!(again, json!(false));

    // unsubscribe: never someone else's row; NONE = all of mine.
    let n = one(
        &alice,
        "RETURN fn::push::unsubscribe($e)",
        vec![("e", json!(b1.endpoint))],
    )
    .await;
    assert_eq!(n, json!(0));
    let n = one(&alice, "RETURN fn::push::unsubscribe(NONE)", vec![]).await;
    assert_eq!(n, json!(1));
    let left = one(
        &root,
        "SELECT VALUE endpoint FROM _00_push_subscription",
        vec![],
    )
    .await;
    assert_eq!(left, json!([b1.endpoint]));
}

#[tokio::test]
async fn anonymous_and_impersonating_sessions_are_refused() {
    let root = fresh_db().await;
    sign_up(&root, "carol").await;
    let dev = Device::new("https://push.example/c1");

    let (dave, _) = sign_up(&root, "dave").await;
    let insecure = Device::new("http://push.example/insecure");
    let refused = try_run(
        &dave,
        "RETURN fn::push::subscribe($s, NONE)",
        vec![("s", insecure.subscription_json())],
    )
    .await;
    assert!(
        refused.unwrap_err().contains("https"),
        "plain http endpoints are refused"
    );

    // Root has no $auth.id.
    let anon = try_run(
        &root,
        "RETURN fn::push::subscribe($s, NONE)",
        vec![("s", dev.subscription_json())],
    )
    .await;
    assert!(
        anon.unwrap_err().contains("sign in"),
        "no subscription without a user"
    );
    assert_eq!(
        one(&root, "RETURN fn::push::list()", vec![]).await,
        json!([])
    );
    assert_eq!(
        one(&root, "RETURN fn::push::unsubscribe(NONE)", vec![]).await,
        json!(0)
    );

    let imp = root.clone();
    imp.signin(Record {
        namespace: "test".into(),
        database: "test".into(),
        access: "_00_impersonate".into(),
        params: json!({ "username": "carol" }),
    })
    .await
    .expect("impersonation signin");
    let refused = try_run(
        &imp,
        "RETURN fn::push::subscribe($s, NONE)",
        vec![("s", dev.subscription_json())],
    )
    .await;
    assert!(refused.unwrap_err().contains("impersonating"));
    let refused = try_run(&imp, "RETURN fn::push::notify({})", vec![]).await;
    assert!(refused.unwrap_err().contains("impersonating"));
    let direct = try_run(&imp, "CREATE _00_push_subscription CONTENT { auth_id: <string> $auth.id, endpoint: 'x', p256dh: 'x', auth: 'x' }", vec![]).await;
    assert!(
        direct.map(|r| r[0] == json!([])).unwrap_or(true),
        "no direct write while impersonating"
    );
    assert_eq!(
        one(&root, "SELECT VALUE id FROM _00_push_subscription", vec![]).await,
        json!([])
    );
}

// --- engine, end to end -----------------------------------------------------

const DM_RULE: &str = r#"
subject: mailto:ops@example.com
rules:
  new-message:
    table: message
    on: [create]
    when: { kind: text }
    to: recipient
    except: sender
    topic: "dm:{{conversation | key}}"
    urgency: high
    ttl: 45s
    with:
      sender: SELECT username FROM ONLY $row.sender
      broken: THROW 'nope'
    notification:
      title: "{{sender.username}}"
      body: "{{text | truncate(20)}}"
      url: "/m/{{conversation | key}}"
      badge: "{{broken | default('/b.png')}}"
    data: [conversation]
"#;

#[tokio::test]
async fn a_matching_row_is_pushed_encrypted_and_signed() {
    let h = harness(DM_RULE).await;
    assert!(h.engine.status().vapid_published);
    let published = one(&h.root, "RETURN fn::push::info()", vec![]).await;
    assert_eq!(published["publicKey"], json!(h.keys.public_key_b64url()));
    assert_eq!(published["kid"], json!(h.keys.kid()));

    let bob = h
        .device("user:bob", "https://fcm.example.com/send/bob1", "")
        .await;
    let _ada = h
        .device("user:ada", "https://fcm.example.com/send/ada1", "")
        .await;
    // Made under another key: skipped, not deleted.
    let stale = h
        .device(
            "user:bob",
            "https://fcm.example.com/send/bob-old",
            "kid = 'another'",
        )
        .await;
    // Only wants another rule.
    let _picky = h
        .device(
            "user:bob",
            "https://fcm.example.com/send/bob-picky",
            "rules = ['other']",
        )
        .await;

    h.write_and_observe(
        "CREATE message:m1 SET kind = 'text', sender = user:ada, recipient = user:bob, \
         conversation = conversation:c1, text = 'Are we still on for chess tonight?'",
        Op::Create,
    )
    .await;

    let reqs = h.http.take();
    assert_eq!(reqs.len(), 1, "one device of the one recipient: {reqs:?}");
    let req = &reqs[0];
    assert_eq!(req.url, bob.endpoint);
    assert_eq!(req.header("TTL"), Some("45"));
    assert_eq!(req.header("Urgency"), Some("high"));
    assert_eq!(req.header("Content-Encoding"), Some("aes128gcm"));
    assert_eq!(req.header("Content-Type"), Some("application/octet-stream"));
    assert_eq!(
        req.header("Topic"),
        Some(crate::rules::topic_header("dm:c1").as_str())
    );
    let authorization = req.header("Authorization").unwrap();
    assert!(authorization.ends_with(&format!(", k={}", h.keys.public_key_b64url())));
    let claims = jwt_claims(authorization);
    assert_eq!(claims["aud"], json!("https://fcm.example.com"));
    assert_eq!(claims["sub"], json!("mailto:ops@example.com"));

    let payload = bob.open(&req.body);
    assert_eq!(payload.kind, PayloadKind::Rule);
    assert_eq!(payload.rule.as_deref(), Some("new-message"));
    assert_eq!(payload.table.as_deref(), Some("message"));
    assert_eq!(payload.id.as_deref(), Some("message:m1"));
    assert_eq!(payload.op, Some(Op::Create));
    assert_eq!(payload.topic.as_deref(), Some("dm:c1"));
    assert_eq!(
        payload.notification.unwrap(),
        json!({
            "title": "ada",
            "body": "Are we still on f...",
            "url": "/m/c1",
            "badge": "/b.png",
            "tag": "dm:c1",
        })
    );
    assert_eq!(
        payload.data.unwrap(),
        json!({ "conversation": "conversation:c1" })
    );

    let row = h.sub("user:bob", &bob.endpoint).await;
    assert!(
        row["last_ok_at"].is_string(),
        "a delivery stamps last_ok_at"
    );
    assert!(
        h.sub("user:bob", &stale.endpoint).await.is_object(),
        "other-kid rows are kept"
    );

    // A row the rule does not match pushes nothing.
    h.write_and_observe(
        "CREATE message:m2 SET kind = 'image', sender = user:ada, recipient = user:bob, conversation = conversation:c1",
        Op::Create,
    )
    .await;
    // Re-ingesting the same row is deduped.
    let m1 = one(&h.root, "SELECT * FROM ONLY message:m1", vec![]).await;
    h.observe(&m1, Op::Create, Origin::Replay).await;
    assert!(h.http.take().is_empty());
    let st = h.engine.status();
    assert_eq!(st.totals.sent, 1);
    assert_eq!(st.totals.matched, 1);
    assert_eq!(st.totals.deduped, 1);
    assert_eq!(st.last_minute.sent, 1);
}

#[tokio::test]
async fn drift_repair_never_pushes() {
    let h = harness(DM_RULE).await;
    h.device("user:bob", "https://push.example/bob", "").await;
    let rows = run(&h.root, "CREATE message:r1 SET kind = 'text', sender = user:ada, recipient = user:bob, conversation = conversation:c1, text = 'x'", vec![]).await;
    let record = rows[0][0].clone();
    h.observe(&record, Op::Create, Origin::Repair).await;
    assert!(h.http.take().is_empty());
    assert_eq!(h.engine.status().totals.skipped_repair, 1);
    // And it was not remembered as pushed: a live observation still fires.
    h.observe(&record, Op::Create, Origin::Live).await;
    assert_eq!(h.http.take().len(), 1);
}

#[tokio::test]
async fn throttle_sends_one_now_and_one_trailing_with_the_latest_row() {
    let h = harness(
        r#"
rules:
  dm:
    table: message
    to: recipient
    topic: "dm:{{conversation | key}}"
    throttle: 30s
    notification: { title: "{{text}}" }
"#,
    )
    .await;
    let bob = h.device("user:bob", "https://push.example/bob", "").await;
    for (i, text) in ["one", "two", "three"].iter().enumerate() {
        h.write_and_observe(
            &format!("CREATE message:t{i} SET recipient = user:bob, conversation = conversation:c1, text = '{text}'"),
            Op::Create,
        )
        .await;
        h.advance(1_000);
    }
    let first = h.http.take();
    assert_eq!(first.len(), 1);
    assert_eq!(
        bob.open(&first[0].body).notification.unwrap()["title"],
        json!("one")
    );
    assert_eq!(h.engine.status().totals.throttled, 2);
    assert_eq!(h.engine.status().queues.trailing, 1);

    h.advance(10_000);
    h.engine.tick().await;
    assert!(h.http.take().is_empty(), "still inside the gap");

    h.advance(20_000);
    h.engine.tick().await;
    let trailing = h.http.take();
    assert_eq!(trailing.len(), 1, "exactly one trailing push");
    let p = bob.open(&trailing[0].body);
    assert_eq!(
        p.id.as_deref(),
        Some("message:t2"),
        "it carries the latest row"
    );
    assert_eq!(p.notification.unwrap()["title"], json!("three"));
    assert_eq!(h.engine.status().queues.trailing, 0);

    // Another conversation is another topic: not throttled by the first.
    h.write_and_observe(
        "CREATE message:t9 SET recipient = user:bob, conversation = conversation:c2, text = 'x'",
        Op::Create,
    )
    .await;
    assert_eq!(h.http.take().len(), 1);
}

/// Hosts spawn one observe per row, so rows can reach the throttle out of
/// ingest order. The trailing push must still end on the newest row, and a
/// row older than what was already sent must never follow it.
#[tokio::test]
async fn out_of_order_observes_still_end_on_the_newest_row() {
    let h = harness(
        r#"
rules:
  dm:
    table: message
    to: recipient
    topic: "dm:{{conversation | key}}"
    throttle: 30s
    notification: { title: "{{text}}" }
"#,
    )
    .await;
    let bob = h.device("user:bob", "https://push.example/bob", "").await;
    let mut rows = Vec::new();
    for (i, text) in ["one", "two", "three"].iter().enumerate() {
        let r = run(
            &h.root,
            &format!("CREATE message:o{i} SET recipient = user:bob, conversation = conversation:c1, text = '{text}'"),
            vec![],
        )
        .await;
        let row = match r.into_iter().last() {
            Some(Value::Array(mut items)) => items.remove(0),
            Some(v) => v,
            None => panic!("no row"),
        };
        rows.push(row);
    }
    // Ingest order is one, two, three; the observes land two, one, three.
    let seqs: Vec<u64> = (0..3).map(|_| h.engine.next_seq()).collect();
    for idx in [1usize, 0, 2] {
        h.engine
            .observe(ObservedChange {
                table: "message".into(),
                op: Op::Create,
                id: rows[idx]["id"].as_str().unwrap().to_string(),
                record: rows[idx].clone(),
                origin: Origin::Live,
                seq: seqs[idx],
            })
            .await;
    }
    let first = h.http.take();
    assert_eq!(first.len(), 1);
    assert_eq!(bob.open(&first[0].body).notification.unwrap()["title"], json!("two"));
    h.advance(31_000);
    h.engine.tick().await;
    let trailing = h.http.take();
    assert_eq!(trailing.len(), 1, "one trailing push, and not the stale `one`");
    assert_eq!(bob.open(&trailing[0].body).notification.unwrap()["title"], json!("three"));
}

#[tokio::test]
async fn once_fires_once_per_state() {
    let h = harness(
        r#"
rules:
  ready:
    table: task
    on: [create, update]
    when: { state: ready }
    once: [state]
    to: owner
"#,
    )
    .await;
    let dev = h.device("user:bob", "https://push.example/bob", "").await;
    h.write_and_observe(
        "CREATE task:k1 SET owner = user:bob, state = 'draft', n = 0",
        Op::Create,
    )
    .await;
    h.write_and_observe("UPDATE task:k1 SET state = 'ready', n = 1", Op::Update)
        .await;
    h.write_and_observe("UPDATE task:k1 SET n = 2", Op::Update)
        .await;
    h.write_and_observe("UPDATE task:k1 SET n = 3", Op::Update)
        .await;
    let reqs = h.http.take();
    assert_eq!(reqs.len(), 1);
    let p = dev.open(&reqs[0].body);
    assert!(
        p.notification.is_none(),
        "no notification template: a nudge"
    );
    assert_eq!(
        p.topic.as_deref(),
        Some("task:k1"),
        "topic defaults to the record id"
    );
    // A different record reaching the state pushes again.
    h.write_and_observe(
        "CREATE task:k2 SET owner = user:bob, state = 'ready'",
        Op::Create,
    )
    .await;
    assert_eq!(h.http.take().len(), 1);
}

#[tokio::test]
async fn to_query_fans_out_with_row_links_as_records() {
    let h = harness(
        r#"
rules:
  club-post:
    table: message
    when: { kind: post }
    to: { query: "SELECT VALUE user FROM member WHERE club = $row.club" }
    except: sender
    notification: { title: "New post in {{club | key}}" }
"#,
    )
    .await;
    run(&h.root, "CREATE member SET club = club:chess, user = user:ada; CREATE member SET club = club:chess, user = user:bob; CREATE member SET club = club:go, user = user:cy;", vec![]).await;
    let bob = h.device("user:bob", "https://push.example/bob", "").await;
    h.device("user:ada", "https://push.example/ada", "").await;
    h.device("user:cy", "https://push.example/cy", "").await;
    h.write_and_observe(
        "CREATE message:p1 SET kind = 'post', club = club:chess, sender = user:ada",
        Op::Create,
    )
    .await;
    let reqs = h.http.take();
    assert_eq!(reqs.len(), 1, "{reqs:?}");
    assert_eq!(reqs[0].url, bob.endpoint);
    assert_eq!(
        bob.open(&reqs[0].body).notification.unwrap()["title"],
        json!("New post in chess")
    );
}

/// FCM answers 410 for a subscription made seconds ago (Chrome, observed in
/// the browser e2e). Deleting it would break "enable, then test push".
#[tokio::test]
async fn a_410_for_a_fresh_subscription_is_retried_not_deleted() {
    let h = harness(DM_RULE).await;
    let fresh = h.device("user:bob", "https://push.example/fresh", "").await;
    h.http.script(&fresh.endpoint, vec![Ok((410, "unsubscribed or expired".into()))]);
    h.write_and_observe(
        "CREATE message:f1 SET kind = 'text', sender = user:ada, recipient = user:bob, conversation = conversation:c1, text = 'x'",
        Op::Create,
    )
    .await;
    assert_eq!(h.http.take().len(), 1);
    let row = h.sub("user:bob", &fresh.endpoint).await;
    assert!(row.is_object(), "the fresh subscription is kept");
    assert_eq!(h.engine.status().queues.retry, 1, "and the push is retried");
    // The retry succeeds (the mock answers 201 once its script is spent).
    h.advance(2_500);
    h.engine.tick().await;
    let reqs = h.http.take();
    assert_eq!(reqs.len(), 1);
    // Decrypts with the subscription's keys, like the first attempt would have.
    let _ = fresh.open(&reqs[0].body);
    assert_eq!(h.engine.status().queues.retry, 0);
}

#[tokio::test]
async fn gone_subscriptions_are_deleted_and_rejected_ones_disabled() {
    let h = harness(DM_RULE).await;
    let gone = h.device("user:bob", "https://push.example/gone", "").await;
    let denied = h
        .device("user:bob", "https://push.example/denied", "")
        .await;
    let fine = h
        .device(
            "user:bob",
            "https://push.example/fine",
            "failures = 2, last_error = 'old'",
        )
        .await;
    // Registered by a record user to make the engine probe an internal host.
    let internal = h
        .device("user:bob", "https://metadata.internal/computeMetadata", "")
        .await;
    // Past the fresh-subscription grace, where a 410 means gone.
    h.advance(3 * 60_000);
    h.http
        .script(&gone.endpoint, vec![Ok((410, "expired".into()))]);
    h.http
        .script(&denied.endpoint, vec![Ok((403, "invalid JWT".into()))]);
    h.write_and_observe(
        "CREATE message:g1 SET kind = 'text', sender = user:ada, recipient = user:bob, conversation = conversation:c1, text = 'x'",
        Op::Create,
    )
    .await;
    let reqs = h.http.take();
    assert_eq!(reqs.len(), 3);
    assert!(
        reqs.iter().all(|r| r.url != internal.endpoint),
        "never POSTed to an internal host"
    );
    let i = h.sub("user:bob", &internal.endpoint).await;
    assert!(i["disabled_at"].is_string());
    assert!(i["disabled_reason"]
        .as_str()
        .unwrap()
        .starts_with("bad_endpoint:"));
    assert_eq!(
        h.sub("user:bob", &gone.endpoint).await,
        Value::Null,
        "410 deletes the row"
    );
    let d = h.sub("user:bob", &denied.endpoint).await;
    assert!(d["disabled_at"].is_string());
    assert_eq!(d["disabled_reason"], json!("http_403: invalid JWT"));
    let f = h.sub("user:bob", &fine.endpoint).await;
    assert_eq!(f["failures"], json!(0), "a success resets failures");
    assert_eq!(f["last_error"], Value::Null);

    // The disabled row is no longer used.
    h.advance(10_000);
    h.write_and_observe(
        "CREATE message:g2 SET kind = 'text', sender = user:ada, recipient = user:bob, conversation = conversation:c1, text = 'y'",
        Op::Create,
    )
    .await;
    let reqs = h.http.take();
    assert_eq!(
        reqs.iter().map(|r| r.url.as_str()).collect::<Vec<_>>(),
        vec![fine.endpoint.as_str()]
    );
    let st = h.engine.status();
    assert_eq!(st.totals.failed, 3);
    assert_eq!(st.totals.sent, 2);
}

#[tokio::test]
async fn transport_failures_are_retried_with_backoff() {
    let h = harness(DM_RULE).await;
    let dev = h.device("user:bob", "https://push.example/flaky", "").await;
    h.http.script(
        &dev.endpoint,
        vec![Ok((503, "busy".into())), Err("connection reset".into())],
    );
    h.write_and_observe(
        "CREATE message:f1 SET kind = 'text', sender = user:ada, recipient = user:bob, conversation = conversation:c1, text = 'x'",
        Op::Create,
    )
    .await;
    assert_eq!(h.http.take().len(), 1);
    let row = h.sub("user:bob", &dev.endpoint).await;
    assert_eq!(row["failures"], json!(1));
    assert_eq!(row["last_error"], json!("http_503: busy"));
    assert_eq!(h.engine.status().queues.retry, 1);

    h.advance(1_000);
    h.engine.tick().await;
    assert!(h.http.take().is_empty(), "first retry waits 2 s");
    h.advance(1_500);
    h.engine.tick().await;
    assert_eq!(h.http.take().len(), 1, "second attempt (transport error)");
    h.advance(10_500);
    h.engine.tick().await;
    let third = h.http.take();
    assert_eq!(third.len(), 1, "third attempt succeeds");
    assert_eq!(dev.open(&third[0].body).id.as_deref(), Some("message:f1"));
    let row = h.sub("user:bob", &dev.endpoint).await;
    assert_eq!(row["failures"], json!(0));
    let st = h.engine.status();
    assert_eq!(
        (st.totals.retried, st.totals.sent, st.queues.retry),
        (2, 1, 0)
    );
}

#[tokio::test]
async fn a_shared_endpoint_only_reaches_its_newest_owner() {
    let h = harness(DM_RULE).await;
    // Same browser, first signed in as ada, now as bob.
    let endpoint = "https://push.example/shared";
    h.device("user:ada", endpoint, "updated_at = time::now() - 1h")
        .await;
    let bob = h.device("user:bob", endpoint, "").await;
    h.write_and_observe(
        "CREATE message:s1 SET kind = 'text', sender = user:bob, recipient = user:ada, conversation = conversation:c1, text = 'for ada'",
        Op::Create,
    )
    .await;
    assert!(h.http.take().is_empty(), "the browser now belongs to bob");
    assert_eq!(
        h.sub("user:ada", endpoint).await,
        Value::Null,
        "the stale owner row is deleted"
    );
    h.write_and_observe(
        "CREATE message:s2 SET kind = 'text', sender = user:ada, recipient = user:bob, conversation = conversation:c1, text = 'for bob'",
        Op::Create,
    )
    .await;
    let reqs = h.http.take();
    assert_eq!(reqs.len(), 1);
    assert_eq!(bob.open(&reqs[0].body).id.as_deref(), Some("message:s2"));
}

#[tokio::test]
async fn direct_messages_immediate_and_scheduled() {
    let h = harness("defaults: { notification: { icon: /icon.png } }").await;
    let bob = h
        .device(
            "user:bob",
            "https://push.example/bob",
            "rules = ['some-rule']",
        )
        .await;

    // Immediate.
    h.write_and_observe(
        "CREATE _00_push_message:d1 CONTENT { to: ['user:bob'], notification: { title: 'Hi' }, topic: 'hello', urgency: 'low', ttl: 120 }",
        Op::Create,
    )
    .await;
    let reqs = h.http.take();
    assert_eq!(
        reqs.len(),
        1,
        "direct messages ignore the device rule filter"
    );
    assert_eq!(reqs[0].header("TTL"), Some("120"));
    assert_eq!(reqs[0].header("Urgency"), Some("low"));
    let p = bob.open(&reqs[0].body);
    assert_eq!(p.kind, PayloadKind::Message);
    assert_eq!(p.message.as_deref(), Some("_00_push_message:d1"));
    assert_eq!(
        p.notification.unwrap(),
        json!({ "title": "Hi", "icon": "/icon.png", "tag": "hello" })
    );
    let row = h.message("d1").await;
    assert_eq!(row["status"], json!("sent"));
    assert_eq!(row["delivered"], json!(1));
    assert!(row["sent_at"].is_string());
    assert_eq!(
        row.get("error").cloned().unwrap_or(Value::Null),
        Value::Null
    );

    // Scheduled in the future: parked.
    h.write_and_observe(
        "CREATE _00_push_message:d2 CONTENT { to: ['user:bob'], data: { n: 2 }, send_at: time::now() + 1h }",
        Op::Create,
    )
    .await;
    assert!(h.http.take().is_empty());
    assert_eq!(h.message("d2").await["status"], json!("pending"));

    // Scheduled in the past: sent right away.
    h.write_and_observe(
        "CREATE _00_push_message:d3 CONTENT { to: ['user:bob'], data: { n: 3 }, send_at: time::now() - 1m }",
        Op::Create,
    )
    .await;
    let reqs = h.http.take();
    assert_eq!(reqs.len(), 1);
    assert_eq!(bob.open(&reqs[0].body).data.unwrap(), json!({ "n": 3 }));
    assert_eq!(h.message("d3").await["status"], json!("sent"));

    // The parked one comes due; the sweep sends it.
    run(
        &h.root,
        "UPDATE _00_push_message:d2 SET send_at = time::now() - 1s",
        vec![],
    )
    .await;
    h.advance(5_000);
    h.engine.tick().await;
    let reqs = h.http.take();
    assert_eq!(reqs.len(), 1);
    assert_eq!(bob.open(&reqs[0].body).data.unwrap(), json!({ "n": 2 }));
    assert_eq!(h.message("d2").await["status"], json!("sent"));

    // Nobody to deliver to: failed, with a reason.
    h.write_and_observe(
        "CREATE _00_push_message:d4 CONTENT { to: ['user:nobody'] }",
        Op::Create,
    )
    .await;
    let row = h.message("d4").await;
    assert_eq!(row["status"], json!("failed"));
    assert_eq!(row["error"], json!("no subscriptions"));
    assert_eq!(row["delivered"], json!(0));

    // A cancelled message is never claimed.
    h.write_and_observe(
        "CREATE _00_push_message:d5 CONTENT { to: ['user:bob'], status: 'cancelled' }",
        Op::Create,
    )
    .await;
    h.advance(5_000);
    h.engine.tick().await;
    assert!(h.http.take().is_empty());
    assert_eq!(h.message("d5").await["status"], json!("cancelled"));
}

/// Under the http transport the ingest event fires inside the creating
/// transaction: the engine sees the CREATE before the row is visible. It must
/// retry the claim (with the host's sleep) instead of waiting for the sweep.
#[tokio::test]
async fn a_message_observed_before_its_commit_is_sent_once_visible() {
    let h = harness("").await;
    let bob = h.device("user:bob", "https://push.example/bob", "").await;
    let created = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let root = h.root.clone();
    let flag = Arc::clone(&created);
    let sleep: crate::engine::Sleep = Arc::new(move |_ms| {
        let root = root.clone();
        let flag = Arc::clone(&flag);
        Box::pin(async move {
            if !flag.swap(true, Ordering::SeqCst) {
                run(&root, "CREATE _00_push_message:late CONTENT { to: ['user:bob'], notification: { title: 'Late' } }", vec![]).await;
            }
        })
    });
    let engine = PushEngine::new(
        Arc::new(MemDb(h.root.clone())),
        Arc::clone(&h.http) as Arc<dyn PushHttp>,
        Some(h.keys.clone()),
        EngineOptions { sleep: Some(sleep), ..EngineOptions::default() },
    );
    engine.tick().await;
    engine
        .observe(ObservedChange {
            table: "_00_push_message".into(),
            op: Op::Create,
            id: "_00_push_message:late".into(),
            record: json!({ "id": "_00_push_message:late", "to": ["user:bob"], "status": "pending" }),
            origin: Origin::Live,
            seq: 0,
        })
        .await;
    assert!(created.load(Ordering::SeqCst), "the engine waited for the row");
    let reqs = h.http.take();
    assert_eq!(reqs.len(), 1);
    assert_eq!(bob.open(&reqs[0].body).notification.unwrap()["title"], json!("Late"));
    assert_eq!(h.message("late").await["status"], json!("sent"));
}

#[tokio::test]
async fn a_message_waiting_on_a_retry_gets_its_status_when_the_retry_settles() {
    let h = harness("").await;
    let dev = h.device("user:bob", "https://push.example/bob", "").await;
    h.http.script(&dev.endpoint, vec![Ok((500, String::new()))]);
    h.write_and_observe(
        "CREATE _00_push_message:w1 CONTENT { to: ['user:bob'] }",
        Op::Create,
    )
    .await;
    assert_eq!(h.message("w1").await["status"], json!("sending"));
    assert_eq!(h.engine.status().queues.messages_in_flight, 1);
    h.advance(2_000);
    h.engine.tick().await;
    assert_eq!(h.http.take().len(), 2);
    let row = h.message("w1").await;
    assert_eq!(row["status"], json!("sent"));
    assert_eq!(row["delivered"], json!(1));
    assert_eq!(h.engine.status().queues.messages_in_flight, 0);
}

#[tokio::test]
async fn the_sweep_recovers_missed_and_stuck_messages() {
    let h = harness("").await;
    h.device("user:bob", "https://push.example/bob", "").await;
    // Ingest never saw these (host restarting, a failed http event).
    run(
        &h.root,
        "CREATE _00_push_message:old CONTENT { to: ['user:bob'], created_at: time::now() - 1m }",
        vec![],
    )
    .await;
    run(
        &h.root,
        "CREATE _00_push_message:young CONTENT { to: ['user:bob'] }",
        vec![],
    )
    .await;
    // Its host died mid-send.
    run(&h.root, "CREATE _00_push_message:stuck CONTENT { to: ['user:bob'], status: 'sending', claimed_at: time::now() - 10m, created_at: time::now() - 10m }", vec![]).await;
    // Another host is sending this one right now.
    run(&h.root, "CREATE _00_push_message:busy CONTENT { to: ['user:bob'], status: 'sending', claimed_at: time::now() }", vec![]).await;

    h.advance(5_000);
    h.engine.tick().await;
    assert_eq!(h.http.take().len(), 2);
    assert_eq!(h.message("old").await["status"], json!("sent"));
    assert_eq!(h.message("stuck").await["status"], json!("sent"));
    assert_eq!(
        h.message("young").await["status"],
        json!("pending"),
        "ingest gets 30 s before the sweep steps in"
    );
    assert_eq!(h.message("busy").await["status"], json!("sending"));
}

#[tokio::test]
async fn the_per_user_limit_drops_the_61st_push() {
    let h = harness("rules: { any: { table: message, to: recipient } }").await;
    h.device("user:bob", "https://push.example/bob", "").await;
    for i in 0..61 {
        h.write_and_observe(
            &format!("CREATE message:l{i} SET recipient = user:bob"),
            Op::Create,
        )
        .await;
    }
    assert_eq!(h.http.take().len(), 60);
    let st = h.engine.status();
    assert_eq!(st.totals.dropped, 1);
    assert_eq!(st.totals.sent, 60);
    // A second later one token has refilled.
    h.advance(1_000);
    h.write_and_observe("CREATE message:l61 SET recipient = user:bob", Op::Create)
        .await;
    assert_eq!(h.http.take().len(), 1);
}

#[tokio::test]
async fn enabled_false_stops_every_send() {
    let h = harness("enabled: false\nrules: { any: { table: message, to: recipient } }").await;
    h.device("user:bob", "https://push.example/bob", "").await;
    assert!(!h.engine.wants("message", Op::Create));
    assert!(h.engine.wants("_00_push_message", Op::Create));
    let st = h.engine.status();
    assert!(!st.enabled);
    assert!(st.reason.unwrap().contains("enabled: false"));
    assert!(st.vapid_published, "keys are still published");
    let row = run(&h.root, "CREATE message:x SET recipient = user:bob", vec![]).await[0][0].clone();
    h.engine
        .observe(ObservedChange {
            table: "message".into(),
            op: Op::Create,
            id: "message:x".into(),
            record: row,
            origin: Origin::Live,
            seq: 0,
        })
        .await;
    h.write_and_observe(
        "CREATE _00_push_message:e1 CONTENT { to: ['user:bob'] }",
        Op::Create,
    )
    .await;
    h.advance(60_000);
    h.engine.tick().await;
    assert!(h.http.take().is_empty());
    assert_eq!(h.message("e1").await["status"], json!("pending"));
}

#[tokio::test]
async fn config_reload_picks_up_a_changed_rule() {
    let h = harness("rules: { r: { table: message, to: recipient, notification: { title: A } } }")
        .await;
    let bob = h.device("user:bob", "https://push.example/bob", "").await;
    let hash_a = h.engine.status().config_hash.unwrap();
    assert!(!h.engine.wants("task", Op::Create));

    write_config(&h.root, "rules: { r: { table: message, to: recipient, notification: { title: B } }, t: { table: task, to: owner } }").await;
    h.advance(10_000);
    h.engine.tick().await;
    assert_eq!(
        h.engine.status().config_hash.unwrap(),
        hash_a,
        "not re-read before 30 s"
    );
    h.advance(21_000);
    h.engine.tick().await;
    assert_ne!(h.engine.status().config_hash.unwrap(), hash_a);
    assert!(h.engine.wants("task", Op::Create));
    assert_eq!(
        h.engine.status().rule_names,
        vec!["r".to_string(), "t".to_string()]
    );

    h.write_and_observe("CREATE message:c1 SET recipient = user:bob", Op::Create)
        .await;
    let reqs = h.http.take();
    assert_eq!(
        bob.open(&reqs[0].body).notification.unwrap()["title"],
        json!("B")
    );

    // A row that does not parse keeps the running config.
    run(
        &h.root,
        "UPDATE _00_push_config:default SET spec_json = '{ not json', hash = 'broken'",
        vec![],
    )
    .await;
    assert!(h.engine.reload_config().await.is_err());
    assert!(h.engine.wants("task", Op::Create));
    assert!(h
        .engine
        .status()
        .last_error
        .unwrap()
        .contains("does not parse"));

    // No row at all: defaults (no rules, direct messages still on).
    run(&h.root, "DELETE _00_push_config:default", vec![]).await;
    assert!(h.engine.reload_config().await.unwrap());
    assert!(!h.engine.wants("message", Op::Create));
    assert!(h.engine.status().enabled);
}

// --- native devices (APNs, FCM) ---------------------------------------------

const NATIVE_PARAM: &str = "DEFINE PARAM OVERWRITE $sp00ky_push_native VALUE { apns: true, fcm: true, bundleIds: ['im.app'], android: { projectId: 'sp00ky-test', appId: '1:42:android:ab', apiKey: 'AIzaX', senderId: '42' } } PERMISSIONS FULL;";

const APNS_TOKEN: &str = "a1b2c3d4e5f60718293a4b5c6d7e8f90a1b2c3d4e5f60718293a4b5c6d7e8f90";
const FCM_TOKEN: &str = "fcm-token_0123456789:abcdefghij";

#[tokio::test]
async fn native_devices_register_through_fn_push_register() {
    let root = fresh_db().await;
    let (alice, alice_id) = sign_up(&root, "alice").await;
    let device = json!({ "kind": "apns", "token": APNS_TOKEN.to_uppercase(), "appId": "im.app", "environment": "sandbox" });

    // Nothing configured: refused, and info says so.
    let refused = try_run(&alice, "RETURN fn::push::register($d, NONE)", vec![("d", device.clone())]).await;
    assert!(refused.unwrap_err().contains("push.apns is not configured"));
    assert_eq!(one(&alice, "RETURN fn::push::info().providers", vec![]).await, json!([]));

    run(&root, NATIVE_PARAM, vec![]).await;
    let info = one(&alice, "RETURN fn::push::info()", vec![]).await;
    assert_eq!(info["enabled"], json!(false), "web push is still off");
    assert_eq!(info["providers"], json!(["apns", "fcm"]));
    assert_eq!(info["android"]["senderId"], json!("42"));

    let row = one(&alice, "RETURN fn::push::register($d, { label: 'iPhone', rules: ['dm'] })", vec![("d", device)]).await;
    assert_eq!(row["endpoint"], json!(format!("apns:{APNS_TOKEN}")), "token lowercased");
    assert_eq!(row["kind"], json!("apns"));
    assert_eq!(row["platform"], json!("ios"));
    assert_eq!(row["environment"], json!("sandbox"));
    assert_eq!(row["label"], json!("iPhone"));
    let fcm = one(
        &alice,
        "RETURN fn::push::register({ kind: 'fcm', token: $t, appId: 'im.app.android' }, NONE)",
        vec![("t", json!(FCM_TOKEN))],
    )
    .await;
    assert_eq!(fcm["platform"], json!("android"));
    assert_eq!(fcm["environment"], Value::Null);

    let list = one(&alice, "RETURN fn::push::list()", vec![]).await;
    let list = list.as_array().unwrap();
    assert_eq!(list.len(), 2);
    assert!(list.iter().all(|d| d["current"] == json!(true)), "native rows are always current");
    assert!(list.iter().all(|d| d.get("token").is_none()));

    // The same token again is the same row; update / unsubscribe work by endpoint.
    one(&alice, "RETURN fn::push::register({ kind: 'apns', token: $t, appId: 'im.app' }, NONE)", vec![("t", json!(APNS_TOKEN))]).await;
    let stored = one(&root, "SELECT VALUE environment FROM _00_push_subscription WHERE kind = 'apns'", vec![]).await;
    assert_eq!(stored, json!(["production"]), "re-registering takes the new environment");
    one(&alice, "RETURN fn::push::update($e, { label: 'Phone' })", vec![("e", json!(format!("fcm:{FCM_TOKEN}")))]).await;
    assert_eq!(
        one(&root, "SELECT VALUE label FROM _00_push_subscription WHERE kind = 'fcm'", vec![]).await,
        json!(["Phone"])
    );
    assert_eq!(one(&alice, "RETURN fn::push::unsubscribe($e)", vec![("e", json!(format!("fcm:{FCM_TOKEN}")))]).await, json!(1));

    for (bad, why) in [
        (json!({ "kind": "web", "token": APNS_TOKEN, "appId": "im.app" }), "kind"),
        (json!({ "kind": "apns", "token": "not-hex", "appId": "im.app" }), "hex"),
        (json!({ "kind": "apns", "token": "ab", "appId": "im.app" }), "hex"),
        (json!({ "kind": "apns", "token": APNS_TOKEN, "appId": "com.other" }), "bundleIds"),
        (json!({ "kind": "apns", "token": APNS_TOKEN, "appId": "a/b" }), "appId"),
        (json!({ "kind": "apns", "token": APNS_TOKEN, "appId": "im.app", "environment": "dev" }), "environment"),
        (json!({ "kind": "fcm", "token": "short", "appId": "im.app" }), "FCM"),
        (json!({ "kind": "fcm", "token": format!("{FCM_TOKEN}/../x"), "appId": "im.app" }), "FCM"),
        (json!({ "kind": "fcm", "token": FCM_TOKEN, "appId": "im.app", "platform": "tv" }), "platform"),
    ] {
        let err = try_run(&alice, "RETURN fn::push::register($d, NONE)", vec![("d", bad.clone())])
            .await
            .expect_err(&format!("{bad} must be refused"));
        assert!(err.contains(why), "{bad}: {err}");
    }

    let anon = try_run(&root, "RETURN fn::push::register({ kind: 'fcm', token: $t, appId: 'x' }, NONE)", vec![("t", json!(FCM_TOKEN))]).await;
    assert!(anon.unwrap_err().contains("sign in"));
    let imp = root.clone();
    imp.signin(Record {
        namespace: "test".into(),
        database: "test".into(),
        access: "_00_impersonate".into(),
        params: json!({ "username": "alice" }),
    })
    .await
    .expect("impersonation signin");
    let refused = try_run(&imp, "RETURN fn::push::register({ kind: 'fcm', token: $t, appId: 'x' }, NONE)", vec![("t", json!(FCM_TOKEN))]).await;
    assert!(refused.unwrap_err().contains("impersonating"));
    assert_eq!(
        one(&root, "SELECT VALUE auth_id FROM _00_push_subscription", vec![]).await,
        json!([alice_id])
    );
}

const NATIVE_RULES: &str = r#"
subject: mailto:ops@example.com
apns: { teamId: TEAM123456, keyId: KEY1234567, key: "<stored>", bundleIds: [im.app] }
fcm: { serviceAccount: "<stored>" }
defaults:
  native: { android: { channelId: general } }
rules:
  dm:
    table: message
    when: { kind: text }
    to: recipient
    topic: "dm:{{conversation | key}}"
    notification: { title: "Web {{text}}", url: "/m/{{conversation | key}}" }
    native:
      notification: { title: "Phone {{text}}" }
      apns: { badge: "{{unread}}" }
  ios-only:
    table: message
    when: { kind: ping }
    to: recipient
    platforms: [ios]
"#;

const TOKEN_ANSWER: &str = r#"{"access_token":"ya29.test","expires_in":3600,"token_type":"Bearer"}"#;

impl Harness {
    async fn native_harness() -> Harness {
        let h = harness(NATIVE_RULES).await;
        let apns = json!({ "teamId": "TEAM123456", "keyId": "KEY1234567", "key": crate::native::tests::TEST_P8 }).to_string();
        run(
            &h.root,
            "UPSERT _00_push_credential:apns SET secret = $a, hash = 'a1'; UPSERT _00_push_credential:fcm SET secret = $f, hash = 'f1';",
            vec![("a", json!(apns)), ("f", json!(crate::native::tests::service_account()))],
        )
        .await;
        assert_eq!(h.engine.reload_config().await, Ok(true));
        let st = h.engine.status();
        assert!(st.providers.apns.ready && st.providers.fcm.ready, "{:?}", st.providers);
        h
    }

    /// A native row as `fn::push::register` would leave it.
    async fn native(&self, user: &str, kind: &str, token: &str, extra: &str) -> String {
        let endpoint = format!("{kind}:{token}");
        let key = hex(&sha256(format!("{user}|{endpoint}").as_bytes()));
        let sql = format!(
            "CREATE type::record('_00_push_subscription', $k) CONTENT {{ auth_id: $u, kind: $kind, endpoint: $e, token: $t, app_id: 'im.app', platform: IF $kind = 'apns' {{ 'ios' }} ELSE {{ 'android' }}, environment: IF $kind = 'apns' {{ 'production' }} ELSE {{ NONE }} }}; \
             UPDATE type::record('_00_push_subscription', $k) SET {} RETURN NONE;",
            if extra.is_empty() { "failures = 0" } else { extra }
        );
        run(
            &self.root,
            &sql,
            vec![("k", json!(key)), ("u", json!(user)), ("kind", json!(kind)), ("e", json!(endpoint)), ("t", json!(token))],
        )
        .await;
        endpoint
    }
}

fn apns_url(sandbox: bool) -> String {
    let host = if sandbox { crate::native::APNS_SANDBOX_HOST } else { crate::native::APNS_HOST };
    format!("{host}/3/device/{APNS_TOKEN}")
}

const FCM_SEND: &str = "https://fcm.googleapis.com/v1/projects/sp00ky-test/messages:send";

#[tokio::test]
async fn one_rule_reaches_web_apns_and_fcm_devices() {
    let h = Harness::native_harness().await;
    let web = h.device("user:bob", "https://push.example/bob", "").await;
    h.native("user:bob", "apns", APNS_TOKEN, "").await;
    h.native("user:bob", "fcm", FCM_TOKEN, "").await;
    h.http.script(crate::native::GOOGLE_TOKEN_URL, vec![Ok((200, TOKEN_ANSWER.into()))]);

    h.write_and_observe(
        "CREATE message:n1 SET kind = 'text', recipient = user:bob, conversation = conversation:c1, text = 'hi', unread = 4",
        Op::Create,
    )
    .await;
    let reqs = h.http.take();
    let urls: Vec<&str> = reqs.iter().map(|r| r.url.as_str()).collect();
    assert_eq!(reqs.len(), 4, "{urls:?}");

    let w = reqs.iter().find(|r| r.url == web.endpoint).unwrap();
    assert_eq!(web.open(&w.body).notification.unwrap()["title"], json!("Web hi"));

    let a = reqs.iter().find(|r| r.url == apns_url(false)).unwrap();
    assert_eq!(a.header("apns-topic"), Some("im.app"));
    assert!(a.header("authorization").unwrap().starts_with("bearer "));
    assert_eq!(a.header("apns-collapse-id"), Some("dm:c1"));
    let body: Value = serde_json::from_slice(&a.body).unwrap();
    assert_eq!(body["aps"]["alert"]["title"], json!("Phone hi"));
    assert_eq!(body["aps"]["badge"], json!(4));
    assert_eq!(body["sp00ky"]["notification"]["url"], json!("/m/c1"));
    assert_eq!(body["sp00ky"]["id"], json!("message:n1"));

    let t = reqs.iter().find(|r| r.url == crate::native::GOOGLE_TOKEN_URL).unwrap();
    assert!(String::from_utf8_lossy(&t.body).contains("assertion="));
    let f = reqs.iter().find(|r| r.url == FCM_SEND).unwrap();
    assert_eq!(f.header("Authorization"), Some("Bearer ya29.test"));
    let m: Value = serde_json::from_slice(&f.body).unwrap();
    assert_eq!(m["message"]["token"], json!(FCM_TOKEN));
    assert_eq!(m["message"]["notification"]["title"], json!("Phone hi"));
    assert_eq!(m["message"]["android"]["notification"]["channel_id"], json!("general"));

    // The access token is reused; the ios-only nudge skips web and Android.
    h.write_and_observe("CREATE message:n2 SET kind = 'ping', recipient = user:bob", Op::Create).await;
    let reqs = h.http.take();
    assert_eq!(reqs.len(), 1, "{:?}", reqs.iter().map(|r| &r.url).collect::<Vec<_>>());
    assert_eq!(reqs[0].url, apns_url(false));
    assert_eq!(reqs[0].header("apns-push-type"), Some("background"));
    let body: Value = serde_json::from_slice(&reqs[0].body).unwrap();
    assert_eq!(body["aps"], json!({ "content-available": 1 }));
    assert!(h.sub("user:bob", &format!("fcm:{FCM_TOKEN}")).await["last_ok_at"].is_string());
}

#[tokio::test]
async fn provider_answers_keep_the_device_table_right() {
    let h = Harness::native_harness().await;
    let apns = h.native("user:bob", "apns", APNS_TOKEN, "").await;
    let fcm = h.native("user:bob", "fcm", FCM_TOKEN, "").await;
    h.http.script(crate::native::GOOGLE_TOKEN_URL, vec![Ok((200, TOKEN_ANSWER.into()))]);

    // A development build's token sent to production: moved to sandbox.
    h.http.script(&apns_url(false), vec![Ok((400, r#"{"reason":"BadDeviceToken"}"#.into()))]);
    h.http.script(&apns_url(true), vec![Ok((200, String::new()))]);
    // Our credentials refused: retried later, the device untouched.
    h.http.script(FCM_SEND, vec![Ok((401, r#"{"error":{"status":"UNAUTHENTICATED","message":"bad token"}}"#.into()))]);
    h.write_and_observe("CREATE message:p1 SET kind = 'text', recipient = user:bob, conversation = conversation:c, text = 'a'", Op::Create).await;
    assert_eq!(h.sub("user:bob", &apns).await["environment"], json!("sandbox"));
    let f = h.sub("user:bob", &fcm).await;
    assert_eq!(f["failures"], json!(0), "a provider refusal is not the device's fault");
    assert!(f["disabled_at"].is_null());
    assert!(h.engine.status().last_error.unwrap().contains("fcm_401"));
    h.http.take();

    // The retry asks for a new access token first.
    h.http.script(crate::native::GOOGLE_TOKEN_URL, vec![Ok((200, TOKEN_ANSWER.into()))]);
    h.advance(2_500);
    h.engine.tick().await;
    let urls: Vec<String> = h.http.take().into_iter().map(|r| r.url).collect();
    assert_eq!(urls, vec![crate::native::GOOGLE_TOKEN_URL.to_string(), FCM_SEND.to_string()]);

    // Dead tokens are deleted.
    h.http.script(&apns_url(true), vec![Ok((410, r#"{"reason":"Unregistered"}"#.into()))]);
    h.http.script(
        FCM_SEND,
        vec![Ok((404, r#"{"error":{"status":"NOT_FOUND","details":[{"errorCode":"UNREGISTERED"}]}}"#.into()))],
    );
    h.write_and_observe("CREATE message:p2 SET kind = 'text', recipient = user:bob, conversation = conversation:c, text = 'b'", Op::Create).await;
    assert!(h.sub("user:bob", &apns).await.is_null());
    assert!(h.sub("user:bob", &fcm).await.is_null());

    // A row a record user pointed at another app is disabled, never sent.
    let other = h.native("user:bob", "apns", &"cd".repeat(32), "app_id = 'com.other'").await;
    h.http.take();
    h.write_and_observe("CREATE message:p3 SET kind = 'text', recipient = user:bob, conversation = conversation:c, text = 'c'", Op::Create).await;
    assert!(h.http.take().is_empty());
    let o = h.sub("user:bob", &other).await;
    assert!(o["disabled_reason"].as_str().unwrap().starts_with("bad_app_id:"));
}

#[tokio::test]
async fn credentials_hot_reload_and_a_bad_one_only_stops_its_provider() {
    let h = Harness::native_harness().await;
    h.native("user:bob", "apns", APNS_TOKEN, "").await;
    h.native("user:bob", "fcm", FCM_TOKEN, "").await;
    run(&h.root, "UPDATE _00_push_credential:fcm SET secret = '{}', hash = 'f2'", vec![]).await;
    assert_eq!(h.engine.reload_config().await, Ok(true));
    let st = h.engine.status();
    assert!(st.providers.apns.ready);
    assert!(!st.providers.fcm.ready && st.providers.fcm.error.is_some());
    assert!(st.enabled);

    h.write_and_observe("CREATE message:c1 SET kind = 'text', recipient = user:bob, conversation = conversation:c, text = 'a'", Op::Create).await;
    let reqs = h.http.take();
    assert_eq!(reqs.len(), 1);
    assert_eq!(reqs[0].url, apns_url(false));
    assert_eq!(h.engine.status().totals.no_provider, 1);
    assert_eq!(h.engine.reload_config().await, Ok(false), "unchanged rows are not reloaded");

    run(&h.root, "DELETE _00_push_credential", vec![]).await;
    assert_eq!(h.engine.reload_config().await, Ok(true));
    assert!(!h.engine.status().providers.apns.configured);
}

#[tokio::test]
async fn native_only_engine_and_direct_messages_with_a_native_block() {
    // No VAPID key at all: native push still works.
    let root = fresh_db().await;
    write_config(&root, NATIVE_RULES).await;
    let apns = json!({ "teamId": "TEAM123456", "keyId": "KEY1234567", "key": crate::native::tests::TEST_P8 }).to_string();
    run(&root, "UPSERT _00_push_credential:apns SET secret = $a, hash = 'a1'", vec![("a", json!(apns))]).await;
    run(&root, "CREATE user:bob SET username = 'bob', pass = 'x'", vec![]).await;
    let http = Arc::new(MockHttp::default());
    let engine = PushEngine::new(
        Arc::new(MemDb(root.clone())),
        Arc::clone(&http) as Arc<dyn PushHttp>,
        None,
        EngineOptions::default(),
    );
    engine.tick().await;
    let st = engine.status();
    assert!(st.enabled, "{:?}", st.reason);
    assert!(!st.providers.web);
    let key = hex(&sha256(format!("user:bob|apns:{APNS_TOKEN}").as_bytes()));
    run(
        &root,
        "CREATE type::record('_00_push_subscription', $k) CONTENT { auth_id: 'user:bob', kind: 'apns', endpoint: $e, token: $t, app_id: 'im.app' }",
        vec![("k", json!(key)), ("e", json!(format!("apns:{APNS_TOKEN}"))), ("t", json!(APNS_TOKEN))],
    )
    .await;
    let row = one(
        &root,
        "CREATE ONLY _00_push_message:m1 CONTENT { to: ['user:bob'], native: { notification: { title: 'Reminder', body: '{{raw}}' }, apns: { sound: 'bell.caf' } }, topic: 'r1' }",
        vec![],
    )
    .await;
    engine
        .observe(ObservedChange {
            table: "_00_push_message".into(),
            op: Op::Create,
            id: "_00_push_message:m1".into(),
            record: row,
            origin: Origin::Live,
            seq: 0,
        })
        .await;
    let reqs = http.take();
    assert_eq!(reqs.len(), 1);
    let body: Value = serde_json::from_slice(&reqs[0].body).unwrap();
    assert_eq!(body["aps"]["alert"], json!({ "title": "Reminder", "body": "{{raw}}" }));
    assert_eq!(body["aps"]["sound"], json!("bell.caf"));
    assert_eq!(body["sp00ky"]["kind"], json!("message"));
    let m = one(&root, "SELECT status, delivered FROM ONLY _00_push_message:m1", vec![]).await;
    assert_eq!(m, json!({ "status": "sent", "delivered": 1 }));

    // SPKY_PUSH=off stops native too.
    let off = PushEngine::new(
        Arc::new(MemDb(root.clone())),
        Arc::clone(&http) as Arc<dyn PushHttp>,
        None,
        EngineOptions { off: true, ..EngineOptions::default() },
    );
    off.tick().await;
    assert!(!off.status().enabled);
    assert!(!off.wants("message", Op::Create));
}

#[test]
fn engine_futures_are_send() {
    fn is_send<T: Send>(_: &T) {}
    fn check(engine: &PushEngine, change: ObservedChange) {
        is_send(&engine.observe(change));
        is_send(&engine.tick());
        is_send(&engine.reload_config());
    }
    let _ = check;
}
