//! Who is connected, and what they are watching.
//!
//! # Where the numbers come from
//!
//! `_00_query` is already the complete registry of live queries, and it is
//! richer than it looks. One row is one *client session x query*: the query id
//! is hashed with a per-browser-session salt (`DataModule.calculateHash`), so
//! two tabs of the same person register two rows. That accident of the id
//! scheme is what makes presence derivable at all — distinct `auth_id` counts
//! people, distinct `clientId` counts live tabs, and the row count is views.
//!
//! Nothing else in the stack knows who is online. The SSP is HTTP-only and the
//! client's WebSocket goes to SurrealDB, not to us, so there is no connection
//! table to consult and no socket to count.
//!
//! # Liveness
//!
//! A row counts while `lastActiveAt + ttl > time::now()` — the same rule
//! `fn::query::heartbeat` and the SSP's TTL sweep use, so this plane and the
//! database can never disagree about what is alive. The filter is not optional:
//! eager teardown (`fn::query::unsubscribe`) is off by default, so without it
//! every tab ever opened counts forever.
//!
//! The honest caveat, which the dashboard repeats to the operator: a client
//! refreshes `lastActiveAt` on a `0.9 * ttl` timer (~9 minutes at the default
//! `10m`), so a closed tab decays out of these numbers rather than vanishing
//! from them. That cadence is the resolution floor, and no amount of sampling
//! here can beat it.
//!
//! # One sampler
//!
//! A single background task reads the database on a timer and every reader —
//! `GET /presence`, and the totals folded into `/overview` — serves from its
//! snapshot. Ten open dashboards cost the database exactly what one does, which
//! is the same bargain `workflows.rs` strikes for its run poller.

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::Json;
use serde::Deserialize;
use serde_json::{json, Value};
use tracing::{debug, warn};

use maintenance::db::ReconnectingDb;

use super::{api_error, db_unavailable, esc, rows, AdminConfig, AdminState, ApiError};

/// How many samples the chart keeps. At the default 15s tick that is half an
/// hour of history, which outlives the ~9 minute decay tail it has to explain.
const SAMPLE_WINDOW: usize = 120;

/// Cap on `?limit=`, so one request cannot ask the database for everything.
const MAX_LIMIT: usize = 500;
const DEFAULT_LIMIT: usize = 100;

/// How many users the snapshot ranks. Enough to see who is heavy, short enough
/// that the overview payload stays a payload.
const TOP_USERS: usize = 20;

/// Cap on the sibling lookup — "how many others run this exact query" is a
/// count, not a list, past a certain size.
const MAX_SIBLINGS: usize = 200;

/// The `_00_query` key of a view id, whichever way it was spelled.
///
/// Registrations reach the scheduler as `_00_query:<hash>` from a live client
/// and as a bare `<hash>` from an SSP re-registering at boot, and both spellings
/// end up in [`crate::query::QueryTracker`]. Mirrors
/// `packages/ssp/src/lib.rs::canonical_query_id`; a key is a hex hash and holds
/// no `:` of its own, so this is idempotent on either.
fn canonical_query_id(id: &str) -> &str {
    id.rsplit(':').next().unwrap_or(id)
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/* ------------------------------------------------------------------ */
/* The sampler                                                         */
/* ------------------------------------------------------------------ */

#[derive(Debug, Clone, Copy, serde::Serialize)]
pub struct PresenceSample {
    /// Epoch ms.
    pub ts: u64,
    pub users: u64,
    pub sessions: u64,
    pub views: u64,
}

#[derive(Debug, Clone, Default, serde::Serialize)]
pub struct Totals {
    /// Distinct authenticated `auth_id`s with at least one live view.
    pub users: u64,
    /// Live sessions that never authenticated. Counted apart from `users`
    /// because one signed-out visitor is not one *user*, and folding them
    /// together would report a logged-out crowd as a user base.
    pub anon_sessions: u64,
    /// Distinct `clientId`s — live tabs, authenticated or not.
    pub sessions: u64,
    pub views: u64,
    /// Views more than one session is subscribed to.
    pub shared_views: u64,
    /// Views whose materialization p99 has reached the slow threshold.
    pub slow_views: u64,
    pub errored_views: u64,
    /// Views holding at least `large_view_rows` rows. An unwindowed live view
    /// this size republishes every row as a `_00_list_ref` edge on each cold
    /// registration; on whitepawn a 3861-row registry did that on every boot
    /// and SurrealDB 3.0.5 stalled under it (2026-09-08). Surfaced so the
    /// tenant sees the query to window before the database tells them.
    pub large_views: u64,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct TopUser {
    pub auth_id: String,
    pub views: u64,
    pub sessions: u64,
}

#[derive(Debug, Clone)]
struct Snapshot {
    taken_at_ms: u64,
    totals: Totals,
    top_users: Vec<TopUser>,
    by_ssp: Vec<Value>,
    /// The row cap was hit, so every figure here is a floor rather than a count.
    truncated: bool,
    /// What went wrong on the last tick, if anything. Reported rather than
    /// swallowed: a chart that silently flatlines on a broken query is worse
    /// than no chart.
    error: Option<String>,
}

/// One row of the sampler's projection.
struct LiveRow {
    id: String,
    auth_id: String,
    client_id: String,
    subscribers: i64,
    p99: Option<f64>,
    errors: i64,
    row_count: i64,
}

fn str_field(v: &Value, key: &str) -> String {
    v.get(key).and_then(|x| x.as_str()).unwrap_or("").to_string()
}

fn int_field(v: &Value, key: &str) -> i64 {
    v.get(key).and_then(|x| x.as_i64()).unwrap_or(0)
}

fn f64_field(v: &Value, key: &str) -> Option<f64> {
    v.get(key).and_then(|x| x.as_f64())
}

/// `auth_id` is `''` for an SSP-only registration and the literal `'anon'` for a
/// signed-out client (`ssp_protocol::ANON_AUTH_ID`). Neither is a person.
fn is_real_user(auth_id: &str) -> bool {
    !auth_id.is_empty() && auth_id != "anon"
}

pub struct PresenceTracker {
    snapshot: RwLock<Option<Snapshot>>,
    samples: Mutex<VecDeque<PresenceSample>>,
    interval: Duration,
    slow_ms: f64,
    /// Row count from which a view counts as large. See `Totals::large_views`.
    large_view_rows: u64,
    max_rows: usize,
}

impl PresenceTracker {
    pub fn new(config: &AdminConfig) -> Arc<Self> {
        Arc::new(Self {
            snapshot: RwLock::new(None),
            samples: Mutex::new(VecDeque::with_capacity(SAMPLE_WINDOW)),
            interval: config.presence_interval,
            slow_ms: config.presence_slow_ms,
            large_view_rows: config.presence_large_view_rows,
            max_rows: config.presence_max_rows,
        })
    }

    pub fn slow_ms(&self) -> f64 {
        self.slow_ms
    }

    pub fn large_view_rows(&self) -> u64 {
        self.large_view_rows
    }

    pub fn max_rows(&self) -> usize {
        self.max_rows
    }

    /// Start the one sampler. Ticks harmlessly while the scheduler is still
    /// cloning and has published no database handle yet, which is the same
    /// thing every other admin reader does.
    pub fn spawn(self: &Arc<Self>, state: AdminState) {
        let tracker = Arc::clone(self);
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(tracker.interval);
            // A slow tick must not queue up a burst once the database recovers.
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            debug!(
                interval_secs = tracker.interval.as_secs(),
                "Presence sampler started"
            );
            loop {
                ticker.tick().await;
                tracker.sample(&state).await;
            }
        });
    }

    async fn sample(&self, state: &AdminState) {
        let Some(db) = state.db() else { return };

        // One row per live view, projecting only what the rollup folds. `surql`
        // and `params` are deliberately absent: they are the large fields, and
        // nothing here reads them.
        //
        // `max_rows + 1` so a full page is distinguishable from an exactly-full
        // one, and the snapshot can say it is a floor.
        let surql = format!(
            "SELECT type::string(id) AS id, auth_id, clientId AS client_id, \
             array::len(subscribers ?? []) AS subscribers, \
             materializationP99 AS p99, errorCount AS errors, \
             rowCount AS row_count \
             FROM _00_query WHERE lastActiveAt + ttl > time::now() LIMIT {};",
            self.max_rows.saturating_add(1)
        );

        let handle = db.handle();
        // Bounded, because this await IS the loop. The sampler is a single task
        // on an interval, so a query that never returns does not "miss a
        // sample" — it parks the sampler for the life of the process. That is
        // what a wedged SurrealDB produces, and the symptom is silent: `error`
        // stays null (nothing failed, nothing returned) while `taken_at_ms`
        // freezes, so the dashboard keeps rendering minutes-old numbers as if
        // they were current. Observed 2026-09-04, stuck 18 minutes behind.
        //
        // One interval is the natural bound: a sample worth more than the gap
        // to the next one has already lost its reason to exist.
        let outcome: Result<Vec<Value>, String> =
            match tokio::time::timeout(self.interval, handle.query(&surql)).await {
                Err(_) => Err(format!(
                    "presence sample timed out after {}s",
                    self.interval.as_secs()
                )),
                Ok(res) => res
                    .and_then(|mut r| r.take(0))
                    .map_err(|e| format!("{e:#}")),
            };

        let fetched: Vec<Value> = match outcome {
            Ok(v) => v,
            Err(e) => {
                db.note_error(&e);
                warn!(error = %e, "Presence sample failed");
                // Keep the previous totals rather than publishing zeros: a
                // transient database error is not everybody logging out. The
                // error rides along so the dashboard can say the numbers are
                // stale.
                if let Ok(mut guard) = self.snapshot.write() {
                    if let Some(snap) = guard.as_mut() {
                        snap.error = Some(e);
                    } else {
                        *guard = Some(Snapshot {
                            taken_at_ms: now_ms(),
                            totals: Totals::default(),
                            top_users: Vec::new(),
                            by_ssp: Vec::new(),
                            truncated: false,
                            error: Some(e),
                        });
                    }
                }
                return;
            }
        };

        let truncated = fetched.len() > self.max_rows;
        let rows: Vec<LiveRow> = fetched
            .into_iter()
            .take(self.max_rows)
            .map(|v| LiveRow {
                id: str_field(&v, "id"),
                auth_id: str_field(&v, "auth_id"),
                client_id: str_field(&v, "client_id"),
                subscribers: int_field(&v, "subscribers"),
                p99: f64_field(&v, "p99"),
                errors: int_field(&v, "errors"),
                row_count: int_field(&v, "row_count"),
            })
            .collect();

        let assignments = state.metrics.query_tracker.all().await;
        let snapshot = self.fold(rows, truncated, &assignments);

        let sample = PresenceSample {
            ts: snapshot.taken_at_ms,
            users: snapshot.totals.users,
            sessions: snapshot.totals.sessions,
            views: snapshot.totals.views,
        };

        if let Ok(mut guard) = self.samples.lock() {
            if guard.len() == SAMPLE_WINDOW {
                guard.pop_front();
            }
            guard.push_back(sample);
        }
        if let Ok(mut guard) = self.snapshot.write() {
            *guard = Some(snapshot);
        }
    }

    /// Turn live rows into the rollup. Split out from [`Self::sample`] so it can
    /// be tested without a database.
    fn fold(
        &self,
        rows: Vec<LiveRow>,
        truncated: bool,
        assignments: &HashMap<String, String>,
    ) -> Snapshot {
        // query key -> ssp, both spellings collapsed to the key.
        let by_key: HashMap<&str, &str> = assignments
            .iter()
            .map(|(q, s)| (canonical_query_id(q), s.as_str()))
            .collect();

        let mut users: HashSet<&str> = HashSet::new();
        let mut sessions: HashSet<&str> = HashSet::new();
        let mut anon_sessions: HashSet<&str> = HashSet::new();
        let mut per_user: HashMap<&str, (u64, HashSet<&str>)> = HashMap::new();
        // BTreeMap so the SSP list comes out in a stable order rather than
        // reshuffling on every poll.
        let mut per_ssp: BTreeMap<&str, u64> = BTreeMap::new();

        let mut shared = 0u64;
        let mut slow = 0u64;
        let mut errored = 0u64;
        let mut large = 0u64;

        for row in &rows {
            let authed = is_real_user(&row.auth_id);
            if authed {
                users.insert(&row.auth_id);
            }
            if !row.client_id.is_empty() {
                sessions.insert(&row.client_id);
                if !authed {
                    anon_sessions.insert(&row.client_id);
                }
            }
            if authed {
                let entry = per_user.entry(&row.auth_id).or_insert((0, HashSet::new()));
                entry.0 += 1;
                if !row.client_id.is_empty() {
                    entry.1.insert(&row.client_id);
                }
            }
            if let Some(ssp) = by_key.get(canonical_query_id(&row.id)) {
                *per_ssp.entry(ssp).or_insert(0) += 1;
            }
            if row.subscribers > 1 {
                shared += 1;
            }
            if row.p99.is_some_and(|v| v >= self.slow_ms) {
                slow += 1;
            }
            if row.errors > 0 {
                errored += 1;
            }
            if row.row_count >= 0 && row.row_count as u64 >= self.large_view_rows {
                large += 1;
            }
        }

        let mut top_users: Vec<TopUser> = per_user
            .into_iter()
            .map(|(auth_id, (views, sess))| TopUser {
                auth_id: auth_id.to_string(),
                views,
                sessions: sess.len() as u64,
            })
            .collect();
        // Heaviest first, then by name so equal counts do not jitter between
        // polls.
        top_users.sort_by(|a, b| b.views.cmp(&a.views).then_with(|| a.auth_id.cmp(&b.auth_id)));
        top_users.truncate(TOP_USERS);

        Snapshot {
            taken_at_ms: now_ms(),
            totals: Totals {
                users: users.len() as u64,
                anon_sessions: anon_sessions.len() as u64,
                sessions: sessions.len() as u64,
                views: rows.len() as u64,
                shared_views: shared,
                slow_views: slow,
                errored_views: errored,
                large_views: large,
            },
            top_users,
            by_ssp: per_ssp
                .into_iter()
                .map(|(ssp_id, views)| json!({ "ssp_id": ssp_id, "views": views }))
                .collect(),
            truncated,
            error: None,
        }
    }

    fn samples(&self) -> Vec<PresenceSample> {
        self.samples
            .lock()
            .map(|g| g.iter().copied().collect())
            .unwrap_or_default()
    }

    /// The compact block folded into `GET /overview`.
    ///
    /// Free: it is memory the sampler already filled, so the sidebar count and
    /// the overview tile cost the poll the dashboard already makes and add
    /// nothing to the database.
    pub fn overview_block(&self) -> Value {
        let snap = self.snapshot.read().ok().and_then(|g| g.clone());
        match snap {
            Some(s) => json!({
                "totals": s.totals,
                "samples": self.samples(),
                "sample_interval_secs": self.interval.as_secs(),
                "taken_at_ms": s.taken_at_ms,
                "truncated": s.truncated,
                "error": s.error,
                "ready": true,
            }),
            // The sampler has not run yet (a fresh process, or the database
            // handle has not landed). Said explicitly rather than sent as
            // zeros, which the UI would draw as "nobody is here".
            None => json!({
                "totals": Totals::default(),
                "samples": [],
                "sample_interval_secs": self.interval.as_secs(),
                "taken_at_ms": Value::Null,
                "truncated": false,
                "error": Value::Null,
                "ready": false,
            }),
        }
    }

    fn presence_json(&self) -> Value {
        let mut out = self.overview_block();
        let snap = self.snapshot.read().ok().and_then(|g| g.clone());
        out["slow_ms"] = json!(self.slow_ms);
        out["large_view_rows"] = json!(self.large_view_rows);
        out["top_users"] = json!(snap.as_ref().map(|s| s.top_users.clone()).unwrap_or_default());
        out["by_ssp"] = json!(snap.as_ref().map(|s| s.by_ssp.clone()).unwrap_or_default());
        out
    }
}

/* ------------------------------------------------------------------ */
/* GET /admin/api/presence                                             */
/* ------------------------------------------------------------------ */

/// Served entirely from the sampler's snapshot — no database work per request.
pub async fn presence(State(state): State<AdminState>) -> Json<Value> {
    Json(state.presence.presence_json())
}

/* ------------------------------------------------------------------ */
/* GET /admin/api/views                                                */
/* ------------------------------------------------------------------ */

/// Every field the list projects. `params` is left out on purpose — it is
/// unbounded and only the detail view needs it.
const VIEW_FIELDS: &str = "type::string(id) AS id, auth_id, clientId AS client_id, surql, \
     array::len(subscribers ?? []) AS subscriber_count, \
     rowCount AS row_count, updateCount AS update_count, errorCount AS error_count, \
     registrationTime AS registration_ms, \
     materializationP55 AS p55, materializationP90 AS p90, materializationP99 AS p99, \
     lastIngestLatency AS last_ingest_ms, \
     type::string(createdAt) AS created_at, \
     type::string(lastActiveAt) AS last_active_at, \
     type::string(lastActiveAt + ttl) AS expires_at";

#[derive(Debug, Deserialize, Default)]
pub struct ViewsQuery {
    limit: Option<usize>,
    /// `auth_id` to filter by.
    user: Option<String>,
    /// SSP id. Applied after the fetch: the assignment lives in the scheduler's
    /// memory, not in the database.
    ssp: Option<String>,
    sort: Option<String>,
    slow_ms: Option<f64>,
    /// Substring of the registered SurrealQL.
    q: Option<String>,
    shared: Option<bool>,
    /// Only views holding at least the large-view row threshold.
    large: Option<bool>,
    include_expired: Option<bool>,
}

/// Map a sort name to an `ORDER BY` clause.
///
/// Every field named here is in [`VIEW_FIELDS`], which SurrealDB v3 requires —
/// ordering by something the projection does not carry is rejected outright.
fn order_by(sort: Option<&str>) -> Result<&'static str, ApiError> {
    Ok(match sort.unwrap_or("slowest") {
        "slowest" => "p99 DESC",
        "newest" => "created_at DESC",
        "rows" => "row_count DESC",
        "updates" => "update_count DESC",
        "errors" => "error_count DESC",
        "active" => "last_active_at DESC",
        other => {
            return Err(api_error(
                StatusCode::BAD_REQUEST,
                format!(
                    "Unknown sort '{}'. Use slowest, newest, rows, updates, errors or active.",
                    other
                ),
            ))
        }
    })
}

fn where_clause(params: &ViewsQuery, large_view_rows: u64) -> String {
    let mut conds: Vec<String> = Vec::new();
    if !params.include_expired.unwrap_or(false) {
        conds.push("lastActiveAt + ttl > time::now()".to_string());
    }
    if let Some(user) = params.user.as_deref().filter(|s| !s.is_empty()) {
        conds.push(format!("auth_id = '{}'", esc(user)));
    }
    if let Some(q) = params.q.as_deref().filter(|s| !s.is_empty()) {
        conds.push(format!("string::contains(surql ?? '', '{}')", esc(q)));
    }
    if params.shared.unwrap_or(false) {
        conds.push("array::len(subscribers ?? []) > 1".to_string());
    }
    if let Some(slow) = params.slow_ms.filter(|v| v.is_finite() && *v > 0.0) {
        conds.push(format!("materializationP99 >= {}", slow));
    }
    if params.large.unwrap_or(false) {
        conds.push(format!("rowCount >= {}", large_view_rows));
    }
    if conds.is_empty() {
        // `WHERE true` rather than branching the format strings below on
        // whether there is a clause at all.
        "true".to_string()
    } else {
        conds.join(" AND ")
    }
}

pub async fn list_views(
    State(state): State<AdminState>,
    Query(params): Query<ViewsQuery>,
) -> Result<Json<Value>, ApiError> {
    // Validate before reaching for the database: a bad sort is a bad request
    // whether or not the scheduler has finished cloning, and answering 503 to
    // it would send the caller looking in the wrong place.
    let order = order_by(params.sort.as_deref())?;
    let filter = where_clause(&params, state.presence.large_view_rows());

    let Some(db) = state.db() else {
        return Err(db_unavailable());
    };

    let limit = params.limit.unwrap_or(DEFAULT_LIMIT).clamp(1, MAX_LIMIT);

    // An SSP filter cannot be pushed into SurrealQL — the assignment is in the
    // scheduler's memory — so when one is asked for, take a wider page and cut
    // it down here. Without this the limit would apply BEFORE the filter and a
    // busy SSP could return an empty page while plainly serving views.
    let fetch = if params.ssp.is_some() {
        state.presence.max_rows()
    } else {
        limit
    };

    let listing = rows(
        &db,
        &format!(
            "SELECT {VIEW_FIELDS} FROM _00_query WHERE {filter} ORDER BY {order} LIMIT {fetch};"
        ),
    )
    .await?;

    let total = rows(
        &db,
        &format!("SELECT count() AS total FROM _00_query WHERE {filter} GROUP ALL;"),
    )
    .await?
    .first()
    .map(|v| int_field(v, "total"))
    .unwrap_or(0);

    let assignments = state.metrics.query_tracker.all().await;
    let by_key: HashMap<&str, &str> = assignments
        .iter()
        .map(|(q, s)| (canonical_query_id(q), s.as_str()))
        .collect();

    let now = now_ms();
    let wanted_ssp = params.ssp.as_deref().filter(|s| !s.is_empty());

    // `ttl_secs` is derived from the two stamps rather than selected with
    // `duration::secs`: `expires_at` IS `lastActiveAt + ttl`, so the answer is
    // already in the projection, and one fewer SurrealQL builtin is one fewer
    // way for every listing on this page to fail at once.

    let mut views: Vec<Value> = Vec::with_capacity(listing.len());
    for mut row in listing {
        let id = str_field(&row, "id");
        let key = canonical_query_id(&id).to_string();
        let ssp = by_key.get(key.as_str()).copied();
        if let Some(want) = wanted_ssp {
            if ssp != Some(want) {
                continue;
            }
        }
        let expires_at_ms = row
            .get("expires_at")
            .and_then(|v| v.as_str())
            .and_then(parse_iso_ms);
        row["ttl_secs"] = json!(ttl_secs(&row, expires_at_ms));
        row["key"] = json!(key);
        row["ssp_id"] = json!(ssp);
        row["shared"] = json!(int_field(&row, "subscriber_count") > 1);
        row["expires_at_ms"] = json!(expires_at_ms);
        row["expired"] = json!(expires_at_ms.is_some_and(|t| t <= now));
        views.push(row);
        if views.len() >= limit {
            break;
        }
    }

    Ok(Json(json!({
        "views": views,
        "returned": views.len(),
        // `total` counts the SurrealQL filter only; an SSP filter narrows the
        // page, not this. Said plainly rather than left to be inferred.
        "total": total,
        "ssp_filtered": wanted_ssp.is_some(),
        "limit": limit,
        "sort": params.sort.clone().unwrap_or_else(|| "slowest".to_string()),
        "slow_ms": params.slow_ms.unwrap_or_else(|| state.presence.slow_ms()),
        "large_view_rows": state.presence.large_view_rows(),
        "server_time_ms": now,
    })))
}

/// A row's TTL in whole seconds, from the gap between `lastActiveAt` and the
/// `lastActiveAt + ttl` the projection already computed.
fn ttl_secs(row: &Value, expires_at_ms: Option<u64>) -> u64 {
    let active = row
        .get("last_active_at")
        .and_then(|v| v.as_str())
        .and_then(parse_iso_ms);
    match (expires_at_ms, active) {
        (Some(exp), Some(act)) => exp.saturating_sub(act) / 1000,
        _ => 0,
    }
}

/// RFC3339 (what `type::string` on a datetime produces) to epoch ms.
fn parse_iso_ms(iso: &str) -> Option<u64> {
    chrono::DateTime::parse_from_rfc3339(iso)
        .ok()
        .map(|t| t.timestamp_millis().max(0) as u64)
}

/* ------------------------------------------------------------------ */
/* GET /admin/api/views/:key                                           */
/* ------------------------------------------------------------------ */

pub async fn view_detail(
    State(state): State<AdminState>,
    Path(key): Path<String>,
) -> Result<Json<Value>, ApiError> {
    let Some(db) = state.db() else {
        return Err(db_unavailable());
    };

    // Accept either spelling in the URL; the record is addressed by key.
    let key = canonical_query_id(&key).to_string();
    if key.is_empty() {
        return Err(api_error(StatusCode::BAD_REQUEST, "Empty view key"));
    }

    let record = format!("type::record('_00_query', '{}')", esc(&key));
    let found = rows(
        &db,
        &format!(
            "SELECT {VIEW_FIELDS}, params, \
             array::map(subscribers ?? [], |$s| <string>$s.id) AS subscriber_ids, \
             array::map(subscribers ?? [], |$s| type::string($s.seenAt)) AS subscriber_seen \
             FROM ONLY {record};"
        ),
    )
    .await?;

    // `FROM ONLY` yields one row, or none when the record is gone.
    let Some(mut view) = found.into_iter().next().filter(|v| !v.is_null()) else {
        return Err(api_error(
            StatusCode::NOT_FOUND,
            format!(
                "No registered view '{}'. A view is reclaimed once `lastActiveAt + ttl` passes.",
                key
            ),
        ));
    };

    let now = now_ms();
    let expires_at_ms = view
        .get("expires_at")
        .and_then(|v| v.as_str())
        .and_then(parse_iso_ms);
    let ttl_secs = ttl_secs(&view, expires_at_ms);
    view["ttl_secs"] = json!(ttl_secs);

    // Subscribers arrive as two parallel string arrays rather than one array of
    // objects: a closure yielding a scalar is the shape the rest of the schema
    // already uses on this field, and it keeps datetimes out of the JSON
    // conversion entirely.
    let ids: Vec<String> = view
        .get("subscriber_ids")
        .and_then(|v| v.as_array())
        .map(|a| {
            a.iter()
                .map(|x| x.as_str().unwrap_or("").to_string())
                .collect()
        })
        .unwrap_or_default();
    let seen: Vec<Option<u64>> = view
        .get("subscriber_seen")
        .and_then(|v| v.as_array())
        .map(|a| a.iter().map(|x| x.as_str().and_then(parse_iso_ms)).collect())
        .unwrap_or_default();

    let subscribers: Vec<Value> = ids
        .iter()
        .enumerate()
        .map(|(i, id)| {
            let seen_at = seen.get(i).copied().flatten();
            let age_secs = seen_at.map(|t| now.saturating_sub(t) / 1000);
            json!({
                "id": id,
                "seen_at_ms": seen_at,
                "age_secs": age_secs,
                // The same prune rule `fn::query::heartbeat` applies: an entry
                // older than the row's own TTL is no longer watching.
                "stale": age_secs.is_some_and(|a| ttl_secs > 0 && a > ttl_secs),
            })
        })
        .collect();

    if let Some(obj) = view.as_object_mut() {
        obj.remove("subscriber_ids");
        obj.remove("subscriber_seen");
    }

    let id = str_field(&view, "id");
    let assignments = state.metrics.query_tracker.all().await;
    let ssp_id = assignments
        .iter()
        .find(|(q, _)| canonical_query_id(q) == key)
        .map(|(_, s)| s.clone());

    view["key"] = json!(key);
    view["ssp_id"] = json!(ssp_id);
    view["shared"] = json!(subscribers.len() > 1);
    view["subscribers"] = json!(subscribers);
    view["expires_at_ms"] = json!(expires_at_ms);
    view["expired"] = json!(expires_at_ms.is_some_and(|t| t <= now));

    let ssp = match ssp_id.as_deref() {
        Some(ssp) => ssp_memory(&state, ssp, &key).await,
        None => None,
    };

    let siblings = siblings_of(&db, &view, &id).await?;

    Ok(Json(json!({
        "view": view,
        // Best effort: an unreachable SSP must not take the page down with it.
        // The registry rows above are the authority on what exists; this block
        // only adds what the circuit knows.
        "ssp": ssp,
        "siblings": siblings,
        "slow_ms": state.presence.slow_ms(),
        "large_view_rows": state.presence.large_view_rows(),
        "server_time_ms": now,
    })))
}

/// This view's footprint inside its SSP, plus the circuit-wide merge counters.
///
/// `graphs` against `subscribers` is the merge win measured rather than
/// inferred: equal counts with `merging` true means nothing actually merged.
async fn ssp_memory(state: &AdminState, ssp_id: &str, key: &str) -> Option<Value> {
    let url = {
        let pool = state.metrics.ssp_pool.read().await;
        pool.get(ssp_id)?.url.clone()
    };

    let response = match state.transport.get_from_ssp(&url, "/debug/memory").await {
        Ok(r) => r,
        Err(e) => {
            debug!(ssp = %ssp_id, error = %e, "Could not read SSP memory for a view");
            return None;
        }
    };
    let body: Value = response.json().await.ok()?;

    let mine = body
        .get("views")
        .and_then(|v| v.as_array())
        .and_then(|views| {
            views.iter().find(|v| {
                v.get("query_id")
                    .and_then(|q| q.as_str())
                    .is_some_and(|q| canonical_query_id(q) == key)
            })
        })
        .cloned();

    Some(json!({
        "ssp_id": ssp_id,
        "view": mine,
        "merging": body.get("merging"),
        "graphs": body.get("graphs"),
        "subscribers": body.get("subscribers"),
        "total_bytes": body.get("total_bytes"),
    }))
}

/// Other live registrations of the *same* query.
///
/// This is the observable answer to "is this shared?". The SSP's own dedup key
/// (`merge_key`) is never persisted, and graph-level merging is off by default
/// (`SPKY_SSP_MERGE_VIEWS`), so the honest thing to report is what the registry
/// can prove: identical SurrealQL and identical params, registered by other
/// sessions. Remember that the query id is salted per browser session, so two
/// tabs of one person are two rows here by design — that is the signal, not a
/// fault.
async fn siblings_of(
    db: &Arc<ReconnectingDb>,
    view: &Value,
    self_id: &str,
) -> Result<Value, ApiError> {
    let Some(surql) = view.get("surql").and_then(|v| v.as_str()) else {
        return Ok(json!({ "sessions": 0, "users": 0, "rows": [] }));
    };
    let params = view.get("params").cloned().unwrap_or(Value::Null);

    let found = rows(
        db,
        &format!(
            "SELECT type::string(id) AS id, auth_id, clientId AS client_id, \
             rowCount AS row_count, params, \
             type::string(lastActiveAt) AS last_active_at \
             FROM _00_query \
             WHERE surql = '{}' AND lastActiveAt + ttl > time::now() LIMIT {};",
            esc(surql),
            MAX_SIBLINGS
        ),
    )
    .await?;

    let mut users: HashSet<String> = HashSet::new();
    let mut sessions: HashSet<String> = HashSet::new();
    let mut out: Vec<Value> = Vec::new();

    for mut row in found {
        // Same SurrealQL is not the same query: two users' rows differ in the
        // params the permission rewrite bound into them.
        if row.get("params").cloned().unwrap_or(Value::Null) != params {
            continue;
        }
        let id = str_field(&row, "id");
        let auth_id = str_field(&row, "auth_id");
        let client_id = str_field(&row, "client_id");
        if is_real_user(&auth_id) {
            users.insert(auth_id.clone());
        }
        if !client_id.is_empty() {
            sessions.insert(client_id);
        }
        if id == self_id {
            continue;
        }
        row["key"] = json!(canonical_query_id(&id));
        // Echoing every sibling's params back would repeat this view's own,
        // which they match by construction.
        if let Some(obj) = row.as_object_mut() {
            obj.remove("params");
        }
        out.push(row);
    }

    Ok(json!({
        // Counts include this view, so "1 session" reads as "only you".
        "sessions": sessions.len(),
        "users": users.len(),
        "truncated": out.len() + 1 >= MAX_SIBLINGS,
        "rows": out,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tracker(slow_ms: f64) -> Arc<PresenceTracker> {
        Arc::new(PresenceTracker {
            snapshot: RwLock::new(None),
            samples: Mutex::new(VecDeque::new()),
            interval: Duration::from_secs(15),
            slow_ms,
            large_view_rows: 1000,
            max_rows: 100,
        })
    }

    fn row(id: &str, auth: &str, client: &str, subs: i64, p99: Option<f64>, errs: i64) -> LiveRow {
        LiveRow {
            id: id.to_string(),
            auth_id: auth.to_string(),
            client_id: client.to_string(),
            subscribers: subs,
            p99,
            errors: errs,
            row_count: 0,
        }
    }

    #[test]
    fn large_views_are_counted_off_the_row_threshold() {
        let t = tracker(250.0);
        let mut big = row("_00_query:a", "user:x", "s1", 1, None, 0);
        big.row_count = 3861;
        let mut edge = row("_00_query:b", "user:x", "s1", 1, None, 0);
        edge.row_count = 1000;
        let small = row("_00_query:c", "user:y", "s2", 1, None, 0);
        let snap = t.fold(vec![big, edge, small], false, &HashMap::new());
        assert_eq!(snap.totals.large_views, 2, "the threshold is inclusive");
        assert_eq!(snap.totals.views, 3);
    }

    #[test]
    fn the_large_filter_lowers_to_a_row_count_predicate() {
        let mut params = ViewsQuery::default();
        params.large = Some(true);
        let clause = where_clause(&params, 1000);
        assert!(clause.contains("rowCount >= 1000"), "{clause}");
        assert!(!where_clause(&ViewsQuery::default(), 1000).contains("rowCount"));
    }

    #[test]
    fn a_user_with_two_tabs_is_one_user_and_two_sessions() {
        // The query id is salted per browser session, so one person with two
        // tabs registers two rows. Collapsing those into one user is the whole
        // point of the rollup.
        let t = tracker(250.0);
        let snap = t.fold(
            vec![
                row("_00_query:a", "user:x", "sess-1", 1, None, 0),
                row("_00_query:b", "user:x", "sess-2", 1, None, 0),
            ],
            false,
            &HashMap::new(),
        );
        assert_eq!(snap.totals.users, 1);
        assert_eq!(snap.totals.sessions, 2);
        assert_eq!(snap.totals.views, 2);
    }

    #[test]
    fn anonymous_sessions_are_not_counted_as_users() {
        let t = tracker(250.0);
        let snap = t.fold(
            vec![
                row("_00_query:a", "anon", "sess-1", 1, None, 0),
                // An SSP-only registration records no auth at all.
                row("_00_query:b", "", "sess-2", 1, None, 0),
                row("_00_query:c", "user:x", "sess-3", 1, None, 0),
            ],
            false,
            &HashMap::new(),
        );
        assert_eq!(snap.totals.users, 1, "only the authenticated row is a user");
        assert_eq!(snap.totals.anon_sessions, 2);
        assert_eq!(snap.totals.sessions, 3);
    }

    #[test]
    fn slow_shared_and_errored_are_counted_off_the_thresholds() {
        let t = tracker(250.0);
        let snap = t.fold(
            vec![
                row("_00_query:a", "user:x", "s1", 3, Some(900.0), 0),
                row("_00_query:b", "user:x", "s1", 1, Some(249.9), 2),
                row("_00_query:c", "user:y", "s2", 1, None, 0),
            ],
            false,
            &HashMap::new(),
        );
        assert_eq!(snap.totals.shared_views, 1);
        assert_eq!(snap.totals.slow_views, 1, "249.9 is under the 250 threshold");
        assert_eq!(snap.totals.errored_views, 1);
    }

    #[test]
    fn ssp_attribution_survives_both_query_id_spellings() {
        // The tracker holds `_00_query:<hash>` from a live client and a bare
        // `<hash>` from an SSP re-registering at boot. Both must land on the
        // same SSP row, or the per-SSP counts halve for no reason.
        let t = tracker(250.0);
        let assignments: HashMap<String, String> = [
            ("_00_query:aaa".to_string(), "ssp-0".to_string()),
            ("bbb".to_string(), "ssp-0".to_string()),
        ]
        .into_iter()
        .collect();

        let snap = t.fold(
            vec![
                row("_00_query:aaa", "user:x", "s1", 1, None, 0),
                row("_00_query:bbb", "user:y", "s2", 1, None, 0),
            ],
            false,
            &assignments,
        );
        assert_eq!(snap.by_ssp, vec![json!({ "ssp_id": "ssp-0", "views": 2 })]);
    }

    #[test]
    fn top_users_rank_by_views_and_break_ties_stably() {
        let t = tracker(250.0);
        let snap = t.fold(
            vec![
                row("_00_query:1", "user:b", "s1", 1, None, 0),
                row("_00_query:2", "user:a", "s2", 1, None, 0),
                row("_00_query:3", "user:a", "s3", 1, None, 0),
                row("_00_query:4", "user:c", "s4", 1, None, 0),
            ],
            false,
            &HashMap::new(),
        );
        let names: Vec<&str> = snap.top_users.iter().map(|u| u.auth_id.as_str()).collect();
        assert_eq!(names, vec!["user:a", "user:b", "user:c"]);
        assert_eq!(snap.top_users[0].views, 2);
        assert_eq!(snap.top_users[0].sessions, 2);
    }

    #[test]
    fn the_liveness_filter_is_in_every_default_listing() {
        // Eager unsubscribe is off by default, so a listing without this reports
        // every tab ever opened.
        let clause = where_clause(&ViewsQuery::default(), 1000);
        assert!(clause.contains("lastActiveAt + ttl > time::now()"), "{clause}");

        let with_expired = ViewsQuery {
            include_expired: Some(true),
            ..Default::default()
        };
        assert!(!where_clause(&with_expired, 1000).contains("lastActiveAt + ttl"));
    }

    #[test]
    fn filters_are_escaped_into_the_where_clause() {
        let params = ViewsQuery {
            user: Some("user:o'brien".to_string()),
            q: Some("it's".to_string()),
            ..Default::default()
        };
        let clause = where_clause(&params, 1000);
        assert!(clause.contains("auth_id = 'user:o\\'brien'"), "{clause}");
        assert!(clause.contains("string::contains(surql ?? '', 'it\\'s')"), "{clause}");
    }

    #[test]
    fn every_sort_names_a_field_the_projection_carries() {
        // SurrealDB v3 rejects ORDER BY on a field that is not in the SELECT,
        // so a sort whose column is missing from VIEW_FIELDS is a 400 waiting
        // to happen rather than a typo.
        for sort in ["slowest", "newest", "rows", "updates", "errors", "active"] {
            let clause = order_by(Some(sort)).expect("known sort");
            let field = clause.split_whitespace().next().unwrap();
            assert!(
                VIEW_FIELDS.contains(&format!("AS {field}")),
                "sort '{sort}' orders by '{field}', which VIEW_FIELDS does not project"
            );
        }
        assert!(order_by(Some("sideways")).is_err());
    }

    #[test]
    fn ttl_is_derived_from_the_two_stamps_the_projection_carries() {
        // `expires_at` IS `lastActiveAt + ttl`, so the gap between them is the
        // TTL — no `duration::secs` needed, and one fewer builtin that every
        // listing on the page would depend on.
        let row = json!({
            "last_active_at": "2026-09-03T12:00:00Z",
            "expires_at": "2026-09-03T12:10:00Z",
        });
        let expires = parse_iso_ms("2026-09-03T12:10:00Z");
        assert_eq!(ttl_secs(&row, expires), 600);

        // A row missing either stamp reports 0 rather than a wild number, which
        // is what the "stale subscriber" rule keys off.
        assert_eq!(ttl_secs(&json!({}), expires), 0);
        assert_eq!(ttl_secs(&row, None), 0);
    }

    #[test]
    fn canonical_query_id_is_idempotent_on_both_spellings() {
        assert_eq!(canonical_query_id("_00_query:abc"), "abc");
        assert_eq!(canonical_query_id("abc"), "abc");
    }
}
