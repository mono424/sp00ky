//! Everything the `Services` port names, bound to the real services (Dart
//! `ClientServices`).

use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, HashSet};
use std::rc::Rc;
use std::sync::LazyLock;

use async_trait::async_trait;
use regex::Regex;

use crate::error::Result;
use crate::kernel::ports::Services;
use crate::services::local::LocalStoreHolder;
use crate::services::persistence::{PersistenceClient, HINT_KEY};
use crate::services::remote::RemoteDatabaseService;
use crate::services::ssp::StreamProcessorService;
use crate::services::supervisor::ConnectionSupervisor;
use crate::services::migrator;
use crate::value::{Object, Value};

/// What the services need from the auth module.
#[async_trait(?Send)]
pub trait AuthSession {
    /// Restore the session from the stored token without notifying
    /// subscribers; the signed-in user id, if any.
    async fn restore_session(&self) -> Result<Option<String>>;
    /// Verify the session against the server (`check`).
    async fn init(&self) -> Result<()>;
    fn current_user(&self) -> Option<Object>;
    fn access(&self) -> Option<String>;
    fn token(&self) -> Option<String>;
}

/// The real services behind [`Services`].
pub struct ClientServices {
    pub holder: Rc<LocalStoreHolder>,
    pub ssp: Rc<StreamProcessorService>,
    pub persistence: Rc<dyn PersistenceClient>,
    pub schema_surql: String,
    /// Tables the prime walks.
    pub schema_tables: Vec<String>,
    /// Each synced table's column set (`column_fingerprints`).
    pub column_sets: BTreeMap<String, String>,
    pub remote: RefCell<Option<RemoteDatabaseService>>,
    pub auth: RefCell<Option<Rc<dyn AuthSession>>>,
    pub supervisor: RefCell<Option<ConnectionSupervisor>>,
    /// What the host last passed to `set_offline`.
    pub offline: Rc<Cell<bool>>,
    /// Where the primed `(id, rv)` pairs go (the runtime's `VersionsPrimed`).
    pub on_versions: RefCell<Option<Rc<dyn Fn(Vec<(String, i64)>)>>>,
    /// Modules whose live query must start once boot has finished.
    pub late_modules: RefCell<Vec<Rc<dyn Fn()>>>,
}

static VIEW_ID: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"^_00_query:[0-9a-f]{64}$").unwrap());

impl ClientServices {
    pub fn new(
        holder: Rc<LocalStoreHolder>,
        ssp: Rc<StreamProcessorService>,
        persistence: Rc<dyn PersistenceClient>,
        schema_surql: String,
        schema_tables: Vec<String>,
        column_sets: BTreeMap<String, String>,
    ) -> Self {
        ClientServices {
            holder,
            ssp,
            persistence,
            schema_surql,
            schema_tables,
            column_sets,
            remote: RefCell::new(None),
            auth: RefCell::new(None),
            supervisor: RefCell::new(None),
            offline: Rc::new(Cell::new(false)),
            on_versions: RefCell::new(None),
            late_modules: RefCell::new(Vec::new()),
        }
    }

    fn auth(&self) -> Option<Rc<dyn AuthSession>> {
        self.auth.borrow().clone()
    }

    fn remote(&self) -> Option<RemoteDatabaseService> {
        self.remote.borrow().clone()
    }
}

#[async_trait(?Send)]
impl Services for ClientServices {
    async fn hint_read(&self) -> Result<Option<String>> {
        Ok(self
            .persistence
            .get(HINT_KEY)
            .await?
            .and_then(|v| v.as_str().map(str::to_string)))
    }

    async fn hint_write(&self, bucket_id: &str) -> Result<()> {
        self.persistence.set(HINT_KEY, Value::String(bucket_id.to_string())).await
    }

    async fn local_connect(&self, bucket_id: &str) -> Result<()> {
        self.holder.connect(bucket_id)
    }

    async fn local_switch_store(&self, bucket_id: &str) -> Result<()> {
        if let Some(snapshot) = self.ssp.save_store_snapshot().filter(|s| !s.is_empty()) {
            if let Ok(db) = self.holder.db() {
                db.put_snapshot(&snapshot)?;
            }
        }
        self.holder.connect(bucket_id)
    }

    fn local_current_bucket_id(&self) -> String {
        self.holder.bucket_id()
    }

    async fn migrator_provision(&self) -> Result<()> {
        migrator::provision(&*self.holder.db()?, &self.schema_surql)
    }

    async fn ssp_init(&self) -> Result<()> {
        self.ssp.init();
        Ok(())
    }

    fn ssp_set_permissions(&self) -> Result<()> {
        self.ssp.seed_permissions_from_schema(&self.schema_surql);
        Ok(())
    }

    fn ssp_set_session_auth(&self, auth_id: Option<&str>, access: Option<&str>) {
        self.ssp.set_session_auth(auth_id, access);
    }

    async fn ssp_prime(&self, pending_ids: Vec<String>) -> Result<()> {
        let db = self.holder.db()?;
        let pending: HashSet<String> = pending_ids.into_iter().collect();
        let on_versions = self.on_versions.borrow().clone();
        let forward = |_table: &str, entries: Vec<(String, i64)>| {
            if let Some(cb) = &on_versions {
                cb(entries);
            }
        };
        self.ssp
            .prime_from_store(&db, &self.schema_tables, &self.column_sets, &pending, Some(&forward))?;
        Ok(())
    }

    async fn ssp_reset(&self) -> Result<()> {
        self.ssp.reset_circuit();
        Ok(())
    }

    fn ssp_set_persistence(&self, _enabled: bool) {}

    async fn auth_restore_session(&self) -> Result<Option<String>> {
        match self.auth() {
            Some(a) => a.restore_session().await,
            None => Ok(None),
        }
    }

    async fn auth_init(&self) -> Result<()> {
        match self.auth() {
            Some(a) => a.init().await,
            None => Ok(()),
        }
    }

    fn auth_session_auth_id(&self) -> Option<String> {
        self.auth()?
            .current_user()?
            .get("id")
            .filter(|v| !v.is_nullish())
            .map(Value::to_id_string)
    }

    fn auth_access(&self) -> Option<String> {
        self.auth()?.access()
    }

    fn auth_token(&self) -> Option<String> {
        self.auth()?.token()
    }

    fn auth_current_user(&self) -> Option<Object> {
        self.auth()?.current_user()
    }

    async fn remote_connect(&self) -> Result<()> {
        match self.remote() {
            Some(r) => r.connect().await,
            None => Ok(()),
        }
    }

    fn net_offline(&self) -> bool {
        self.offline.get()
    }

    fn remote_release_views(&self, ids: Vec<String>) {
        let Some(remote) = self.remote() else { return };
        let valid: Vec<String> = ids.into_iter().filter(|id| VIEW_ID.is_match(id)).collect();
        if valid.is_empty() {
            return;
        }
        // Best effort and unawaited: the process is going away. The ids are
        // checked against a strict pattern, so interpolating them is safe.
        let sql = format!(
            "FOR $id IN [{}] {{ LET $_released = fn::query::unsubscribe($id); }};",
            valid.join(", ")
        );
        let fut = remote.query(&sql, None);
        tokio::task::spawn_local(async move {
            let _ = fut.await;
        });
    }

    fn supervisor_start(&self) {
        if let Some(s) = self.supervisor.borrow().as_ref() {
            s.start();
        }
    }

    fn crdt_set_session_id(&self, _session_id: &str) {}

    fn crdt_close_all(&self, _flush: bool) {}

    async fn persistence_set(&self, key: &str, value: Option<Value>) -> Result<()> {
        match value {
            Some(v) if !v.is_nullish() => self.persistence.set(key, v).await,
            _ => self.persistence.remove(key).await,
        }
    }

    fn features_init(&self) {
        let modules = self.late_modules.borrow().clone();
        for start in modules {
            start();
        }
    }

    fn releases_init(&self) {}
}
