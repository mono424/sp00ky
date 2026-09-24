//! The scheduler's backend list, kept across scheduler restarts.
//!
//! The control plane hands the scheduler its backends with `PUT /backends`
//! once per deploy, in the deploy's apps phase. Nothing pushes the list again
//! when only the scheduler restarts: an admin restart or reclone, a crash, a
//! supervisor relaunch, an infra-only redeploy. The list used to live in
//! memory alone, so every such restart left the scheduler with no backends to
//! health-check (an empty dashboard, no backend incidents) until the next
//! deploy happened to push it again.
//!
//! Every pushed list is now also written upstream to
//! `_00_scheduler_state:backends` and read back at boot when the environment
//! (`SPKY_BACKENDS`) carries no list of its own. The table is root-only
//! (`PERMISSIONS NONE`) and, like every `_00_` table, stays out of sync and out
//! of the replica, so a reclone neither wipes nor drift-checks it. Env values
//! are stored masked, exactly as the dashboard shows them: the list is only
//! ever displayed, and secrets have no business in the application database.
//!
//! A push always wins: the stored copy is applied only when no push has
//! arrived since boot, under the same lock a push takes.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};

use tokio::sync::Mutex;
use tracing::{info, warn};

use crate::backend_health::{BackendHealthCache, BackendHealthConfig, SharedBackendConfigs};
use maintenance::db::ReconnectingDb;

const DEFINE_TABLE: &str =
    "DEFINE TABLE IF NOT EXISTS _00_scheduler_state SCHEMALESS PERMISSIONS NONE;";
const UPSERT: &str =
    "UPSERT _00_scheduler_state:backends SET backends = $backends, updated_at = time::now();";
const SELECT: &str = "SELECT backends FROM _00_scheduler_state:backends;";

/// How long a boot-time restore keeps trying to read the stored list when the
/// database is not answering yet, and the pause between attempts.
const RESTORE_ATTEMPTS: u32 = 10;
const RESTORE_BACKOFF: std::time::Duration = std::time::Duration::from_secs(3);

pub struct BackendRegistry {
    configs: SharedBackendConfigs,
    cache: BackendHealthCache,
    /// The environment supplied a list at boot: that list is authoritative and
    /// the stored copy is never read.
    from_env: bool,
    /// A `PUT /backends` arrived since boot.
    pushed: AtomicBool,
    /// Serialises a push against the boot-time restore.
    apply: Mutex<()>,
    db: OnceLock<Arc<ReconnectingDb>>,
}

impl BackendRegistry {
    pub fn new(
        configs: SharedBackendConfigs,
        cache: BackendHealthCache,
        from_env: bool,
    ) -> Arc<Self> {
        Arc::new(Self {
            configs,
            cache,
            from_env,
            pushed: AtomicBool::new(false),
            apply: Mutex::new(()),
            db: OnceLock::new(),
        })
    }

    /// `PUT /backends`: apply the list and store it for the next boot.
    pub async fn replace(&self, backends: Vec<BackendHealthConfig>) {
        let _guard = self.apply.lock().await;
        self.pushed.store(true, Ordering::SeqCst);
        crate::backend_health::update_backends(&self.configs, &self.cache, backends.clone()).await;
        match self.db.get() {
            Some(db) => persist(db, &backends).await,
            // Pushed before the upstream handle exists: `attach_db` stores it.
            None => info!(
                "Backend list pushed before the database connection; storing it once connected"
            ),
        }
    }

    /// Hand over the upstream handle once the scheduler has one. Stores a list
    /// that was pushed before this point, or restores the stored list when
    /// nothing was pushed and the environment had none. Runs in the background.
    pub fn attach_db(self: &Arc<Self>, db: Arc<ReconnectingDb>) {
        if self.db.set(db).is_err() {
            return;
        }
        let this = Arc::clone(self);
        tokio::spawn(async move { this.after_connect().await });
    }

    async fn after_connect(&self) {
        let Some(db) = self.db.get() else { return };
        if self.pushed.load(Ordering::SeqCst) {
            let _guard = self.apply.lock().await;
            let current = self.configs.read().await.clone();
            persist(db, &current).await;
            return;
        }
        if self.from_env {
            return;
        }
        for attempt in 1..=RESTORE_ATTEMPTS {
            match load(db).await {
                Ok(stored) => {
                    self.restore(stored).await;
                    return;
                }
                Err(e) => {
                    db.note_error(&format!("{e:#}"));
                    if attempt == RESTORE_ATTEMPTS {
                        warn!(error = %e, "Could not read the stored backend list; backends stay empty until the next deploy pushes them");
                        return;
                    }
                    tokio::time::sleep(RESTORE_BACKOFF).await;
                }
            }
            if self.pushed.load(Ordering::SeqCst) {
                return;
            }
        }
    }

    async fn restore(&self, stored: Option<Vec<BackendHealthConfig>>) {
        let _guard = self.apply.lock().await;
        if self.pushed.load(Ordering::SeqCst) {
            return; // the control plane's word arrived first
        }
        match stored {
            Some(list) if !list.is_empty() => {
                info!(
                    count = list.len(),
                    "Restored the backend list stored by the last deploy"
                );
                crate::backend_health::update_backends(&self.configs, &self.cache, list).await;
            }
            _ => info!("No stored backend list; waiting for the control plane to push one"),
        }
    }
}

/// What gets stored: the list as pushed, env values masked.
fn stored_form(backends: &[BackendHealthConfig]) -> Vec<BackendHealthConfig> {
    backends
        .iter()
        .map(|b| BackendHealthConfig {
            env: b.env.as_ref().map(|env| mask_env(env)),
            ..b.clone()
        })
        .collect()
}

fn mask_env(env: &[String]) -> Vec<String> {
    crate::metrics::mask_sensitive_env(crate::metrics::vec_env_to_map(env))
        .into_iter()
        .map(|(k, v)| match v {
            serde_json::Value::String(s) => format!("{k}={s}"),
            other => format!("{k}={other}"),
        })
        .collect()
}

async fn persist(db: &ReconnectingDb, backends: &[BackendHealthConfig]) {
    let stored = stored_form(backends);
    let value = match serde_json::to_value(&stored) {
        Ok(v) => v,
        Err(e) => {
            warn!(error = %e, "Could not encode the backend list for storage");
            return;
        }
    };
    let handle = db.handle();
    let result = async {
        handle.query(DEFINE_TABLE).await?.check()?;
        handle
            .query(UPSERT)
            .bind(("backends", value))
            .await?
            .check()?;
        Ok::<(), surrealdb::Error>(())
    }
    .await;
    match result {
        Ok(()) => info!(
            count = stored.len(),
            "Stored the backend list for the next scheduler start"
        ),
        Err(e) => {
            db.note_error(&e.to_string());
            warn!(error = %e, "Could not store the backend list; a scheduler restart before the next deploy will start without it");
        }
    }
}

async fn load(db: &ReconnectingDb) -> anyhow::Result<Option<Vec<BackendHealthConfig>>> {
    let handle = db.handle();
    // Defined first, so a scheduler that boots before any deploy stored a
    // list reads an empty table instead of erroring on a missing one.
    handle.query(DEFINE_TABLE).await?.check()?;
    let mut response = handle.query(SELECT).await?.check()?;
    let rows: Vec<serde_json::Value> = response.take(0)?;
    let Some(list) = rows
        .into_iter()
        .next()
        .and_then(|row| row.get("backends").cloned())
    else {
        return Ok(None);
    };
    if list.is_null() {
        return Ok(None);
    }
    Ok(Some(serde_json::from_value(list)?))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn backend(name: &str, env: Option<Vec<&str>>) -> BackendHealthConfig {
        BackendHealthConfig {
            name: name.into(),
            url: format!("http://{name}:3000"),
            healthcheck: "/health".into(),
            port: Some(3000),
            env: env.map(|e| e.into_iter().map(String::from).collect()),
        }
    }

    #[test]
    fn stored_form_masks_secrets_and_keeps_the_rest() {
        let stored = stored_form(&[backend(
            "relay",
            Some(vec![
                "CEC_ANALYSIS_TOKEN=abc",
                "PORT=3670",
                "RELAY_PUBLIC_URL=wss://x",
            ]),
        )]);
        let env = stored[0].env.clone().unwrap();
        assert!(
            env.contains(&"CEC_ANALYSIS_TOKEN=****".to_string()),
            "{env:?}"
        );
        assert!(env.contains(&"PORT=3670".to_string()), "{env:?}");
        assert!(
            env.contains(&"RELAY_PUBLIC_URL=wss://x".to_string()),
            "{env:?}"
        );
        assert_eq!(stored[0].name, "relay");
        assert_eq!(stored[0].port, Some(3000));
    }

    #[test]
    fn stored_form_leaves_a_backend_without_env_alone() {
        let stored = stored_form(&[backend("web", None)]);
        assert!(stored[0].env.is_none());
    }

    async fn registry(from_env: bool) -> Arc<BackendRegistry> {
        let configs = crate::backend_health::create_shared_configs(&[]);
        let cache = crate::backend_health::create_health_cache(&[]);
        BackendRegistry::new(configs, cache, from_env)
    }

    #[tokio::test]
    async fn restore_applies_the_stored_list_when_nothing_was_pushed() {
        let reg = registry(false).await;
        reg.restore(Some(vec![
            backend("gamesync", None),
            backend("relay", None),
        ]))
        .await;
        let names: Vec<String> = reg
            .configs
            .read()
            .await
            .iter()
            .map(|b| b.name.clone())
            .collect();
        assert_eq!(names, vec!["gamesync", "relay"]);
        assert_eq!(reg.cache.read().await.len(), 2);
    }

    #[tokio::test]
    async fn a_push_wins_over_the_stored_list() {
        let reg = registry(false).await;
        reg.replace(vec![backend("fresh", None)]).await;
        reg.restore(Some(vec![backend("stale", None)])).await;
        let names: Vec<String> = reg
            .configs
            .read()
            .await
            .iter()
            .map(|b| b.name.clone())
            .collect();
        assert_eq!(names, vec!["fresh"]);
    }

    #[tokio::test]
    async fn an_empty_stored_list_changes_nothing() {
        let reg = registry(false).await;
        reg.restore(Some(vec![])).await;
        reg.restore(None).await;
        assert!(reg.configs.read().await.is_empty());
    }
}
