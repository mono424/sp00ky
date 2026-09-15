//! Node-side state for the query allowlist (`ssp::allowlist`): the compiled
//! entries, when they were loaded, and what the gate decided so far.
//!
//! Loaded from `_00_query_allowlist` (root-only rows the CLI writes at
//! deploy / release / dev start) at every Ready transition and whenever the
//! table's ingest notification arrives. A refusal also triggers one throttled
//! reload, so an SSP that missed the notification during a deploy still picks
//! up the new release on the first miss instead of refusing the whole rollout.

use serde::Serialize;
use serde_json::{json, Value};
use ssp::allowlist::{self, Compiled, Decision, Entry, Mode, Shape, Source};
use ssp::converter::LinkMap;
use std::collections::{BTreeSet, HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::{Mutex, RwLock};
use tracing::{info, warn};

use crate::ports::Db;

/// Reload at most this often in response to refusals.
const REFRESH_THROTTLE_MS: u64 = 5_000;
/// Refused shapes kept for `/info`.
const LAST_REFUSED_CAP: usize = 20;

#[derive(Clone, Debug, Serialize)]
pub struct RefusedSample {
    pub at_epoch_ms: u64,
    pub table: String,
    pub surql: String,
    pub reason: String,
}

#[derive(Default, Serialize)]
pub struct Counters {
    pub checked: AtomicU64,
    pub allowed_static: AtomicU64,
    pub allowed_any: AtomicU64,
    pub allowed_builtin: AtomicU64,
    pub warned: AtomicU64,
    pub refused: AtomicU64,
}

pub struct QueryAllowlist {
    pub mode: Mode,
    compiled: RwLock<Compiled>,
    last_loaded_ms: AtomicU64,
    reload_lock: Mutex<()>,
    pub counters: Counters,
    last_refused: Mutex<VecDeque<RefusedSample>>,
}

impl QueryAllowlist {
    pub fn new(mode: Mode) -> Self {
        Self {
            mode,
            compiled: RwLock::new(Compiled::default()),
            last_loaded_ms: AtomicU64::new(0),
            reload_lock: Mutex::new(()),
            counters: Counters::default(),
            last_refused: Mutex::new(VecDeque::new()),
        }
    }

    pub fn enabled(&self) -> bool {
        self.mode != Mode::Off
    }

    /// Read every `_00_query_allowlist` row and recompile. A missing table
    /// (older internal schema) is an empty allowlist, not an error, exactly
    /// like `_00_query` at bootstrap.
    pub async fn reload(&self, db: &dyn Db, links: &LinkMap) {
        let _guard = self.reload_lock.lock().await;
        let rows = match db
            .query(
                "SELECT app, version, entries, released_at FROM _00_query_allowlist",
                &[],
            )
            .await
        {
            Ok(mut out) => out
                .pop()
                .and_then(|v| v.as_array().cloned())
                .unwrap_or_default(),
            // SurrealDB answers a SELECT on a never-defined table with an
            // error, not an empty set: that is the empty allowlist.
            Err(e) if crate::tables::is_missing_table_error(&e.to_string()) => Vec::new(),
            Err(e) => {
                warn!(target: "ssp::policy", error = %e, "query allowlist: read failed; keeping the previous allowlist");
                return;
            }
        };
        let mut entries = Vec::new();
        let mut sources = Vec::new();
        for row in rows {
            let app = row
                .get("app")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string();
            let version = row
                .get("version")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string();
            let released_at = row
                .get("released_at")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string();
            let row_entries: Vec<Entry> = row
                .get("entries")
                .cloned()
                .and_then(|v| serde_json::from_value(v).ok())
                .unwrap_or_default();
            sources.push(Source {
                app: app.clone(),
                version: version.clone(),
                released_at,
                entries: row_entries.len(),
            });
            entries.extend(row_entries.into_iter().map(|mut e| {
                e.app = app.clone();
                e.version = version.clone();
                e
            }));
        }
        let compiled = allowlist::compile(entries, sources, links);
        for (name, err) in &compiled.skipped {
            warn!(target: "ssp::policy", entry = %name, error = %err, "query allowlist: entry does not parse; skipped");
        }
        info!(
            target: "ssp::policy",
            mode = %self.mode,
            entries = compiled.len(),
            sources = compiled.sources.len(),
            "query allowlist loaded"
        );
        *self.compiled.write().await = compiled;
        self.last_loaded_ms
            .store(crate::now_epoch_ms(), Ordering::Relaxed);
    }

    /// Decide for `shape`; on a miss, reload once (throttled) and re-decide.
    /// Records counters and the refused sample in both `warn` and `enforce`.
    pub async fn decide_with_refresh(
        &self,
        shape: &Shape,
        columns: &HashMap<String, BTreeSet<String>>,
        surql: &str,
        db: &dyn Db,
        links: &LinkMap,
    ) -> Decision {
        self.counters.checked.fetch_add(1, Ordering::Relaxed);
        let mut decision = allowlist::decide(&*self.compiled.read().await, shape, columns);
        if matches!(decision, Decision::Refused { .. }) {
            let age =
                crate::now_epoch_ms().saturating_sub(self.last_loaded_ms.load(Ordering::Relaxed));
            if age > REFRESH_THROTTLE_MS {
                self.reload(db, links).await;
                decision = allowlist::decide(&*self.compiled.read().await, shape, columns);
            }
        }
        match &decision {
            Decision::Allowed(allowlist::Allowed::Static) => {
                self.counters.allowed_static.fetch_add(1, Ordering::Relaxed);
            }
            Decision::Allowed(allowlist::Allowed::Any) => {
                self.counters.allowed_any.fetch_add(1, Ordering::Relaxed);
            }
            Decision::Allowed(allowlist::Allowed::Builtin) => {
                self.counters
                    .allowed_builtin
                    .fetch_add(1, Ordering::Relaxed);
            }
            Decision::Refused { reason } => {
                let counter = if self.mode == Mode::Enforce {
                    &self.counters.refused
                } else {
                    &self.counters.warned
                };
                counter.fetch_add(1, Ordering::Relaxed);
                let mut samples = self.last_refused.lock().await;
                if samples.len() >= LAST_REFUSED_CAP {
                    samples.pop_front();
                }
                samples.push_back(RefusedSample {
                    at_epoch_ms: crate::now_epoch_ms(),
                    table: shape.root_table().unwrap_or_default(),
                    surql: surql.to_string(),
                    reason: reason.clone(),
                });
            }
        }
        decision
    }

    /// The `/info` block.
    pub async fn info(&self) -> Value {
        let compiled = self.compiled.read().await;
        let last_refused: Vec<RefusedSample> =
            self.last_refused.lock().await.iter().cloned().collect();
        json!({
            "mode": self.mode.to_string(),
            "loaded_at_epoch_ms": self.last_loaded_ms.load(Ordering::Relaxed),
            "entries": compiled.len(),
            "sources": compiled.sources,
            "skipped": compiled.skipped,
            "counters": self.counters,
            "last_refused": last_refused,
        })
    }
}
