//! Per-device key/value state: the boot bucket hint, the auth token, anything a
//! module wants to survive a restart (Dart `PersistenceClient` and its three
//! implementations).

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

use async_trait::async_trait;
use base64::Engine;

use crate::error::{Error, Result};
use crate::modules::ref_tables::{bucket_id_for_user, ANON_USER_ID};
use crate::services::local::{bucket_path, LocalDatabaseService};
use crate::types::StoreType;
use crate::value::Value;

/// Where the client keeps per-device key/value state. A stored `null` reads
/// back as `None`, as Dart's `get` answers `null`.
#[async_trait(?Send)]
pub trait PersistenceClient {
    async fn set(&self, key: &str, value: Value) -> Result<()>;
    async fn get(&self, key: &str) -> Result<Option<Value>>;
    async fn remove(&self, key: &str) -> Result<()>;
}

/// The key the auth token is stored under.
pub const TOKEN_KEY: &str = "sp00ky_auth_token";
/// The key the boot bucket hint is stored under.
pub const HINT_KEY: &str = "sp00ky_boot_bucket";

fn some_value(v: Value) -> Option<Value> {
    (!v.is_nullish()).then_some(v)
}

// ---- memory ----------------------------------------------------------------------

/// Process memory only: nothing survives a restart.
#[derive(Default)]
pub struct MemoryPersistence {
    store: RefCell<HashMap<String, Value>>,
}

impl MemoryPersistence {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait(?Send)]
impl PersistenceClient for MemoryPersistence {
    async fn set(&self, key: &str, value: Value) -> Result<()> {
        self.store.borrow_mut().insert(key.to_string(), value);
        Ok(())
    }

    async fn get(&self, key: &str) -> Result<Option<Value>> {
        Ok(self.store.borrow().get(key).cloned().and_then(some_value))
    }

    async fn remove(&self, key: &str) -> Result<()> {
        self.store.borrow_mut().remove(key);
        Ok(())
    }
}

// ---- sqlite --------------------------------------------------------------------

/// Resolves the store a persistence call lands in.
pub type StoreResolver = Rc<dyn Fn() -> Result<Rc<LocalDatabaseService>>>;

/// Backed by the local sqlite `_00_kv` table, so values (the auth token, the
/// boot bucket hint) survive restarts.
///
/// Values are JSON-encoded, so arbitrary JSON-safe values round-trip; string
/// values come back as plain strings.
///
/// Resolves the store on every call rather than holding one: a bucket switch
/// replaces it, and a captured handle would keep writing into a closed
/// database.
#[derive(Clone)]
pub struct SqlitePersistence {
    resolve: StoreResolver,
}

impl SqlitePersistence {
    pub fn new(resolve: StoreResolver) -> Self {
        SqlitePersistence { resolve }
    }

    /// Persistence over one fixed store.
    pub fn over(db: Rc<LocalDatabaseService>) -> Self {
        Self::new(Rc::new(move || Ok(db.clone())))
    }

    fn db(&self) -> Result<Rc<LocalDatabaseService>> {
        (self.resolve)()
    }
}

#[async_trait(?Send)]
impl PersistenceClient for SqlitePersistence {
    async fn set(&self, key: &str, value: Value) -> Result<()> {
        self.db()?.kv_set(key, &value.to_json_string())
    }

    async fn get(&self, key: &str) -> Result<Option<Value>> {
        let Some(raw) = self.db()?.kv_get(key)? else {
            return Ok(None);
        };
        let json: serde_json::Value = serde_json::from_str(&raw)?;
        Ok(some_value(Value::from(json)))
    }

    async fn remove(&self, key: &str) -> Result<()> {
        self.db()?.kv_remove(key)
    }
}

// ---- session -------------------------------------------------------------------

/// The user id (`table:key`) a JWT's `ID` (or `id`) claim names, or `None`
/// when the token is malformed or names no record.
pub fn user_id_from_token(token: Option<&str>) -> Option<String> {
    let part = token?.split('.').nth(1)?;
    // Either base64 alphabet, padded or not (Dart `base64Url.normalize`).
    let normalized: String = part
        .chars()
        .filter(|c| *c != '=')
        .map(|c| match c {
            '+' => '-',
            '/' => '_',
            c => c,
        })
        .collect();
    let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(normalized)
        .ok()?;
    let claims: serde_json::Value = serde_json::from_slice(&bytes).ok()?;
    let id = claims
        .get("ID")
        .filter(|v| !v.is_null())
        .or_else(|| claims.get("id"))?;
    id.as_str().filter(|s| s.contains(':')).map(str::to_string)
}

/// A kv value written by `jsonEncode`, read back as a string.
fn decode_string(raw: Option<String>) -> Option<String> {
    let value: serde_json::Value = serde_json::from_str(&raw?).ok()?;
    value.as_str().map(str::to_string)
}

fn is_safe_hint(hint: &str) -> bool {
    !hint.is_empty()
        && hint
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
}

/// Session routing must outlive account-store switches. One atomic metadata row
/// is authoritative even when signed out, so legacy credentials cannot return.
///
/// The token and the boot hint live in a `session` database next to the bucket
/// stores; every other key goes to the open account store.
pub struct SessionPersistence {
    metadata: LocalDatabaseService,
    account: SqlitePersistence,
}

struct SessionRow {
    token: Option<String>,
    bucket: String,
}

impl SessionPersistence {
    const KEY: &'static str = "session_v1";
    const LEGACY_MARKER: &'static str = "sp00ky_session_migrated";

    /// Open the session store for `store` (in memory for a memory store),
    /// migrating the legacy locator once, and route other keys to `account`.
    pub fn open(store: &StoreType, account: StoreResolver) -> Result<Self> {
        let base = match store {
            StoreType::Memory => None,
            StoreType::File(base) => Some(base.clone()),
        };
        let open_bucket = |bucket: &str| -> Result<LocalDatabaseService> {
            let db = LocalDatabaseService::open(
                store,
                base.as_ref().map(|b| bucket_path(b, bucket)).as_deref(),
            )?;
            db.provision()?;
            Ok(db)
        };

        let metadata = open_bucket("session")?;
        let result = SessionPersistence {
            metadata,
            account: SqlitePersistence::new(account),
        };
        if result.metadata.kv_get(Self::KEY)?.is_none() {
            let mut token: Option<String> = None;
            if base.is_some() {
                // Only consult the legacy locator once. Never scan other accounts.
                let anon = open_bucket(ANON_USER_ID)?;
                let mut hint = None;
                if anon.kv_get(Self::LEGACY_MARKER)?.as_deref() != Some("true") {
                    hint = decode_string(anon.kv_get(HINT_KEY)?);
                    token = decode_string(anon.kv_get(TOKEN_KEY)?);
                }
                anon.close();
                if let Some(hint) = hint.filter(|h| is_safe_hint(h) && h != ANON_USER_ID) {
                    let old = open_bucket(&hint)?;
                    token = decode_string(old.kv_get(TOKEN_KEY)?).or(token);
                    old.close();
                }
            }
            let user = user_id_from_token(token.as_deref());
            let bucket = bucket_id_for_user(user.as_deref());
            result.write(user.and(token).as_deref(), &bucket)?;
        }
        if base.is_some() {
            // Commit the authoritative row first. The legacy marker additionally
            // prevents credential resurrection if the metadata file is later lost.
            let anon = open_bucket(ANON_USER_ID)?;
            let marked = match anon.kv_get(Self::LEGACY_MARKER) {
                Ok(Some(v)) if v == "true" => Ok(()),
                Ok(_) => anon.kv_set(Self::LEGACY_MARKER, "true"),
                Err(e) => Err(e),
            };
            anon.close();
            marked?;
        }
        Ok(result)
    }

    fn read(&self) -> SessionRow {
        let signed_out = || SessionRow {
            token: None,
            bucket: ANON_USER_ID.to_string(),
        };
        let Ok(Some(raw)) = self.metadata.kv_get(Self::KEY) else {
            return signed_out();
        };
        let Ok(row) = serde_json::from_str::<serde_json::Value>(&raw) else {
            return signed_out();
        };
        match row.get("token") {
            None | Some(serde_json::Value::Null) => signed_out(),
            Some(serde_json::Value::String(token)) => match user_id_from_token(Some(token)) {
                // The token claim, never a mutable hint, selects the account cache.
                Some(user) => SessionRow {
                    token: Some(token.clone()),
                    bucket: bucket_id_for_user(Some(&user)),
                },
                // A corrupt metadata row is signed out, never a reason to
                // revive legacy auth.
                None => signed_out(),
            },
            Some(_) => signed_out(),
        }
    }

    fn write(&self, token: Option<&str>, bucket: &str) -> Result<()> {
        let row = serde_json::json!({ "token": token, "bucket": bucket });
        self.metadata.kv_set(Self::KEY, &row.to_string())
    }

    /// Close the session store.
    pub fn close(&self) {
        self.metadata.close();
    }
}

#[async_trait(?Send)]
impl PersistenceClient for SessionPersistence {
    async fn get(&self, key: &str) -> Result<Option<Value>> {
        match key {
            TOKEN_KEY => Ok(self.read().token.map(Value::String)),
            HINT_KEY => Ok(Some(Value::String(self.read().bucket))),
            _ => self.account.get(key).await,
        }
    }

    async fn set(&self, key: &str, value: Value) -> Result<()> {
        match key {
            TOKEN_KEY => {
                let token = value.as_str().ok_or_else(|| {
                    Error::InvalidArgument("the auth token must be a string".into())
                })?;
                let bucket = match user_id_from_token(Some(token)) {
                    Some(user) => bucket_id_for_user(Some(&user)),
                    None => self.read().bucket,
                };
                self.write(Some(token), &bucket)
            }
            HINT_KEY => {
                let bucket = value.as_str().ok_or_else(|| {
                    Error::InvalidArgument("the boot hint must be a string".into())
                })?;
                self.write(self.read().token.as_deref(), bucket)
            }
            _ => self.account.set(key, value).await,
        }
    }

    async fn remove(&self, key: &str) -> Result<()> {
        match key {
            TOKEN_KEY | HINT_KEY => self.write(None, ANON_USER_ID),
            _ => self.account.remove(key).await,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::run_local;
    use serde_json::json;
    use std::path::PathBuf;

    fn mem_db() -> Rc<LocalDatabaseService> {
        let db = LocalDatabaseService::open_memory().unwrap();
        db.provision().unwrap();
        Rc::new(db)
    }

    // ---- persistence_test.dart ----

    #[tokio::test]
    async fn sqlite_set_get_remove_round_trip() {
        run_local(async {
            let p = SqlitePersistence::over(mem_db());
            assert_eq!(p.get("k").await.unwrap(), None);
            p.set("k", Value::from("value")).await.unwrap();
            assert_eq!(p.get("k").await.unwrap(), Some(Value::from("value")));
            p.remove("k").await.unwrap();
            assert_eq!(p.get("k").await.unwrap(), None);
        })
        .await;
    }

    #[tokio::test]
    async fn sqlite_preserves_non_string_values() {
        run_local(async {
            let db = mem_db();
            let p = SqlitePersistence::over(db.clone());
            p.set("n", Value::from(42)).await.unwrap();
            p.set("m", Value::from(json!({"a": 1}))).await.unwrap();
            p.set("z", Value::Null).await.unwrap();
            assert_eq!(p.get("n").await.unwrap(), Some(Value::Int(42)));
            assert_eq!(
                p.get("m").await.unwrap(),
                Some(Value::from(json!({"a": 1})))
            );
            assert_eq!(p.get("z").await.unwrap(), None);
            // Stored as JSON, as the Dart client writes it.
            assert_eq!(db.kv_get("m").unwrap().as_deref(), Some(r#"{"a":1}"#));
            assert_eq!(db.kv_get("z").unwrap().as_deref(), Some("null"));
        })
        .await;
    }

    #[tokio::test]
    async fn a_value_written_then_reopened_is_still_present() {
        run_local(async {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("test.db");
            let store = StoreType::File(path.clone());
            let db1 = Rc::new(LocalDatabaseService::open(&store, None).unwrap());
            db1.provision().unwrap();
            SqlitePersistence::over(db1.clone())
                .set(TOKEN_KEY, Value::from("tok-123"))
                .await
                .unwrap();
            db1.close();

            let db2 = Rc::new(LocalDatabaseService::open(&store, Some(&path)).unwrap());
            db2.provision().unwrap();
            let restored = SqlitePersistence::over(db2.clone())
                .get(TOKEN_KEY)
                .await
                .unwrap();
            db2.close();
            assert_eq!(restored, Some(Value::from("tok-123")));
        })
        .await;
    }

    #[tokio::test]
    async fn sqlite_resolves_the_store_on_every_call() {
        run_local(async {
            let current = Rc::new(RefCell::new(mem_db()));
            let c = current.clone();
            let p = SqlitePersistence::new(Rc::new(move || Ok(c.borrow().clone())));
            p.set("k", Value::from("a")).await.unwrap();
            *current.borrow_mut() = mem_db();
            assert_eq!(
                p.get("k").await.unwrap(),
                None,
                "the replaced store is not consulted"
            );
        })
        .await;
    }

    #[tokio::test]
    async fn memory_round_trip() {
        run_local(async {
            let p = MemoryPersistence::new();
            assert_eq!(p.get("k").await.unwrap(), None);
            p.set("k", Value::from(json!({"a": [1]}))).await.unwrap();
            assert_eq!(
                p.get("k").await.unwrap(),
                Some(Value::from(json!({"a": [1]})))
            );
            p.set("k", Value::Null).await.unwrap();
            assert_eq!(p.get("k").await.unwrap(), None);
            p.set("k", Value::from(1)).await.unwrap();
            p.remove("k").await.unwrap();
            assert_eq!(p.get("k").await.unwrap(), None);
        })
        .await;
    }

    // ---- services/session_persistence_test.dart ----

    fn jwt(id: &str) -> String {
        let claims = json!({"ID": id, "AC": "account"}).to_string();
        format!(
            "h.{}.s",
            base64::engine::general_purpose::URL_SAFE.encode(claims)
        )
    }

    struct Env {
        _dir: tempfile::TempDir,
        base: PathBuf,
    }

    impl Env {
        fn new() -> Self {
            let dir = tempfile::tempdir().unwrap();
            let base = dir.path().join("cache.db");
            Env { _dir: dir, base }
        }

        fn store(&self) -> StoreType {
            StoreType::File(self.base.clone())
        }

        fn open(&self, bucket: &str) -> Rc<LocalDatabaseService> {
            let db = LocalDatabaseService::open_file(&bucket_path(&self.base, bucket)).unwrap();
            db.provision().unwrap();
            Rc::new(db)
        }

        fn session(&self, account: &Rc<LocalDatabaseService>) -> SessionPersistence {
            let account = account.clone();
            SessionPersistence::open(&self.store(), Rc::new(move || Ok(account.clone()))).unwrap()
        }

        fn session_path(&self) -> PathBuf {
            bucket_path(&self.base, "session")
        }
    }

    fn enc(s: &str) -> String {
        serde_json::to_string(s).unwrap()
    }

    async fn get_str(p: &SessionPersistence, key: &str) -> Option<String> {
        p.get(key)
            .await
            .unwrap()
            .map(|v| v.as_str().unwrap().to_string())
    }

    #[tokio::test]
    async fn migrates_the_locator_once_and_preserves_legacy_account_documents() {
        run_local(async {
            let env = Env::new();
            let anon = env.open("anon");
            anon.kv_set(HINT_KEY, &enc("a")).unwrap();
            let old = env.open("a");
            old.kv_set(TOKEN_KEY, &enc(&jwt("user:a"))).unwrap();
            old.put_doc(
                "thread",
                "thread:a",
                crate::testing::row(json!({"title": "untouched"})),
                false,
                &[],
            )
            .unwrap();
            old.close();

            let p = env.session(&anon);
            assert_eq!(get_str(&p, HINT_KEY).await.as_deref(), Some("a"));
            assert_eq!(get_str(&p, TOKEN_KEY).await, Some(jwt("user:a")));
            p.remove(TOKEN_KEY).await.unwrap();
            p.close();

            let p = env.session(&anon);
            assert_eq!(get_str(&p, TOKEN_KEY).await, None);
            assert_eq!(get_str(&p, HINT_KEY).await.as_deref(), Some("anon"));
            p.close();

            let still_there = env.open("a");
            assert_eq!(
                still_there.get_by_id("thread:a").unwrap().unwrap()["title"],
                Value::from("untouched")
            );
            still_there.close();
            anon.close();
        })
        .await;
    }

    #[tokio::test]
    async fn token_claims_choose_the_account_even_when_the_old_locator_disagrees() {
        run_local(async {
            let env = Env::new();
            let anon = env.open("anon");
            anon.kv_set(HINT_KEY, &enc("old")).unwrap();
            let old = env.open("old");
            old.kv_set(TOKEN_KEY, &enc(&jwt("user:actual"))).unwrap();
            old.close();
            let p = env.session(&anon);
            assert_eq!(get_str(&p, HINT_KEY).await.as_deref(), Some("actual"));
            p.set(TOKEN_KEY, Value::from(jwt("user:next")))
                .await
                .unwrap();
            assert_eq!(get_str(&p, HINT_KEY).await.as_deref(), Some("next"));
            p.close();
            anon.close();
        })
        .await;
    }

    #[tokio::test]
    async fn malformed_token_or_unsafe_locator_cannot_restore_another_account() {
        run_local(async {
            let env = Env::new();
            let anon = env.open("anon");
            anon.kv_set(HINT_KEY, &enc("../../outside")).unwrap();
            anon.kv_set(TOKEN_KEY, &enc("bad-token")).unwrap();
            let p = env.session(&anon);
            assert_eq!(get_str(&p, TOKEN_KEY).await, None);
            assert_eq!(get_str(&p, HINT_KEY).await.as_deref(), Some("anon"));
            p.close();
            anon.close();
        })
        .await;
    }

    #[tokio::test]
    async fn lost_metadata_after_sign_out_cannot_resurrect_legacy_credentials() {
        run_local(async {
            let env = Env::new();
            let anon = env.open("anon");
            anon.kv_set(HINT_KEY, &enc("alice")).unwrap();
            let alice = env.open("alice");
            alice.kv_set(TOKEN_KEY, &enc(&jwt("user:alice"))).unwrap();
            alice.close();
            let p = env.session(&anon);
            p.remove(TOKEN_KEY).await.unwrap();
            p.close();
            std::fs::remove_file(env.session_path()).unwrap();

            let p = env.session(&anon);
            assert_eq!(get_str(&p, TOKEN_KEY).await, None);
            assert_eq!(get_str(&p, HINT_KEY).await.as_deref(), Some("anon"));
            p.close();
            anon.close();
        })
        .await;
    }

    #[tokio::test]
    async fn other_keys_go_to_the_account_store_and_memory_stores_work() {
        run_local(async {
            let account = mem_db();
            let a = account.clone();
            let p = SessionPersistence::open(&StoreType::Memory, Rc::new(move || Ok(a.clone())))
                .unwrap();
            assert_eq!(get_str(&p, TOKEN_KEY).await, None);
            assert_eq!(get_str(&p, HINT_KEY).await.as_deref(), Some("anon"));
            p.set("feature", Value::from(json!({"on": true})))
                .await
                .unwrap();
            assert_eq!(
                account.kv_get("feature").unwrap().as_deref(),
                Some(r#"{"on":true}"#)
            );
            assert_eq!(
                p.get("feature").await.unwrap(),
                Some(Value::from(json!({"on": true})))
            );
            p.remove("feature").await.unwrap();
            assert_eq!(p.get("feature").await.unwrap(), None);

            // A token that names no user keeps the current bucket.
            p.set(HINT_KEY, Value::from("b1")).await.unwrap();
            p.set(TOKEN_KEY, Value::from("not-a-jwt")).await.unwrap();
            assert_eq!(
                get_str(&p, TOKEN_KEY).await,
                None,
                "an unreadable token reads as signed out"
            );
            p.set(TOKEN_KEY, Value::from(jwt("user:carol")))
                .await
                .unwrap();
            assert_eq!(get_str(&p, HINT_KEY).await.as_deref(), Some("carol"));
            assert!(p.set(TOKEN_KEY, Value::from(1)).await.is_err());
            p.remove(HINT_KEY).await.unwrap();
            assert_eq!(get_str(&p, TOKEN_KEY).await, None);
        })
        .await;
    }

    #[test]
    fn user_id_from_token_reads_the_id_claim() {
        assert_eq!(
            user_id_from_token(Some(&jwt("user:a"))).as_deref(),
            Some("user:a")
        );
        let padded = format!(
            "h.{}.s",
            base64::engine::general_purpose::STANDARD.encode(r#"{"id":"user:b"}"#)
        );
        assert_eq!(user_id_from_token(Some(&padded)).as_deref(), Some("user:b"));
        assert_eq!(user_id_from_token(Some("h.e30.s")), None, "no claim");
        assert_eq!(user_id_from_token(Some(&jwt("nocolon"))), None);
        assert_eq!(user_id_from_token(Some("garbage")), None);
        assert_eq!(user_id_from_token(None), None);
    }
}
