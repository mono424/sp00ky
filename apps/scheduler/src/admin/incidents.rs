//! Durable, bounded incident history independent of the upstream database.
//! Producers emit only typed, sanitized events; a background task owns disk I/O.
use super::{api_error, AdminState, ApiError};
use axum::{
    extract::{Path, Query, State},
    Json,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::{
    collections::VecDeque,
    path::{Path as FsPath, PathBuf},
    sync::{Arc, Mutex},
    time::Duration,
};

const MAX_INCIDENTS: usize = 2000;
const MAX_EVENTS: usize = 64;
const MAX_FILE_BYTES: u64 = 8 * 1024 * 1024;
const RETENTION_MS: u64 = 30 * 24 * 60 * 60 * 1000;
const TARGET: &str = "scheduler::incident";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Event {
    pub at: u64,
    pub component: String,
    pub kind: String,
    pub state: String,
    pub summary: String,
    pub operation_id: Option<String>,
    pub version: String,
}

/// Summaries must be operator-safe descriptions, never SQL, bindings or tokens.
pub fn emit(component: &str, kind: &str, state: &str, summary: &str, operation_id: Option<&str>) {
    let event = Event {
        at: super::ops::now_ms(),
        component: component.chars().take(128).collect(),
        kind: kind.into(),
        state: state.into(),
        summary: summary.chars().take(384).collect(),
        operation_id: operation_id.map(str::to_owned),
        version: env!("CARGO_PKG_VERSION").into(),
    };
    if let Ok(payload) = serde_json::to_string(&event) {
        tracing::info!(target: "scheduler::incident", incident = %payload, "Incident transition");
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Incident {
    pub id: String,
    pub component: String,
    pub kind: String,
    pub severity: String,
    pub state: String,
    pub started_at: u64,
    pub ended_at: Option<u64>,
    pub max_buffered_events: usize,
    #[serde(default)]
    pub max_publication_operations: u64,
    #[serde(default)]
    pub max_publication_bytes: u64,
    #[serde(default)]
    pub max_publication_age_ms: u64,
    #[serde(default)]
    pub publication: Option<ssp_protocol::PublicationMetrics>,
    pub event_count: u64,
    pub events: VecDeque<Event>,
}

#[derive(Default)]
struct History {
    rows: VecDeque<Incident>,
    revision: u64,
    storage_error: Option<String>,
}

pub struct Incidents {
    history: Mutex<History>,
    path: PathBuf,
    persistence: tokio::sync::Mutex<()>,
    /// When this recorder first saw each backend failing, in its current streak.
    /// In memory on purpose: it is the debounce for `backend_down`, and a streak
    /// must restart with the process - see [`Self::backend_transitions`].
    failing_since: Mutex<std::collections::HashMap<String, u64>>,
}

impl Incidents {
    pub fn open(path: PathBuf) -> Arc<Self> {
        let loaded = (|| -> anyhow::Result<VecDeque<Incident>> {
            if !path.exists() {
                return Ok(VecDeque::new());
            }
            anyhow::ensure!(
                std::fs::metadata(&path)?.len() <= MAX_FILE_BYTES,
                "incident file exceeds size limit"
            );
            Ok(serde_json::from_slice(&std::fs::read(&path)?)?)
        })();
        let mut history = History::default();
        match loaded {
            Ok(rows) => history.rows = rows,
            Err(e) => history.storage_error = Some(format!("Could not load incident history: {e}")),
        }
        let now = super::ops::now_ms();
        for row in &mut history.rows {
            if row.state == "open" {
                row.state = "interrupted".into();
                row.ended_at = Some(now);
                append(
                    row,
                    Event {
                        at: now,
                        component: row.component.clone(),
                        kind: "scheduler_restart".into(),
                        state: "interrupted".into(),
                        summary: "Scheduler restarted before recovery was observed".into(),
                        operation_id: None,
                        version: env!("CARGO_PKG_VERSION").into(),
                    },
                );
            }
        }
        history.revision = 1;
        trim(&mut history.rows, now);
        Arc::new(Self {
            history: Mutex::new(history),
            path,
            persistence: tokio::sync::Mutex::new(()),
            failing_since: Mutex::new(Default::default()),
        })
    }

    fn observe(&self, event: Event) {
        let mut h = self.history.lock().unwrap();
        // Restart intent arrives both through tracing and the synchronous
        // pre-exit flush. Deduplicate that delivery even if the log feed runs
        // after completion, when it must not reopen the operation.
        if event.operation_id.as_ref().is_some_and(|id| h.rows.iter().any(|row| {
            row.component == event.component && row.events.iter().any(|prior| {
                prior.operation_id.as_ref() == Some(id)
                    && prior.kind == event.kind && prior.state == event.state
                    && prior.summary == event.summary
            })
        })) {
            return;
        }
        // Correlate automatic SSP failures into one episode. Operator actions
        // have their own identity and cannot accidentally close that episode.
        let existing = h.rows.iter().position(|r| {
            event.state != "recorded"
                && r.state == "open"
                && r.component == event.component
                && r.events.front().and_then(|e| e.operation_id.as_deref())
                    == event.operation_id.as_deref()
        });
        if let Some(i) = existing {
            let row = &mut h.rows[i];
            if event.state != "open" {
                row.state = event.state.clone();
                row.ended_at = Some(event.at);
            }
            append(row, event.clone());
        } else if event.state == "open" || event.state == "recorded" {
            let terminal = event.state == "recorded";
            h.rows.push_front(Incident {
                id: uuid::Uuid::new_v4().simple().to_string(),
                component: event.component.clone(),
                kind: event.kind.clone(),
                severity: if event.operation_id.is_some() || terminal {
                    "info"
                } else {
                    "warning"
                }
                .into(),
                state: event.state.clone(),
                started_at: event.at,
                ended_at: terminal.then_some(event.at),
                max_buffered_events: 0,
                max_publication_operations: 0,
                max_publication_bytes: 0,
                max_publication_age_ms: 0,
                publication: None,
                event_count: 1,
                events: VecDeque::from([event.clone()]),
            });
        } else {
            return;
        }
        h.revision += 1;
        trim(&mut h.rows, event.at);
    }

    /// The overview's incident block: counts plus the last day's episodes, so
    /// the dashboard can draw a 24 h strip without a second request.
    pub fn summary(&self) -> Value {
        let h = self.history.lock().unwrap();
        let now = super::ops::now_ms();
        let day = now.saturating_sub(24 * 60 * 60 * 1000);
        let recent: Vec<Value> = h
            .rows
            .iter()
            .filter(|r| r.state == "open" || r.ended_at.unwrap_or(r.started_at) >= day)
            .take(60)
            .map(|r| json!({
                "id": r.id, "component": r.component, "kind": r.kind, "severity": r.severity,
                "state": r.state, "started_at": r.started_at, "ended_at": r.ended_at,
            }))
            .collect();
        let latest = h.rows.front().map(|r| json!({
            "id": r.id, "component": r.component, "kind": r.kind, "severity": r.severity,
            "state": r.state, "started_at": r.started_at, "ended_at": r.ended_at,
        }));
        json!({
            "open": h.rows.iter().filter(|r| r.state == "open").count(),
            "total": h.rows.len(),
            "last_24h": h.rows.iter().filter(|r| r.started_at >= day).count(),
            "recent": recent,
            "latest": latest,
            "retention_days": 30,
            "storage_error": h.storage_error,
            "server_time_ms": now,
        })
    }

    fn observe_line(&self, line: &maintenance::log_ring::LogLine) {
        if line.target == TARGET {
            if let Some(payload) = line.fields.strip_prefix("incident=") {
                if let Ok(event) = serde_json::from_str::<Event>(payload) {
                    self.observe(event);
                }
            }
        }
    }

    fn expire(&self, now: u64) {
        let mut h = self.history.lock().unwrap();
        let before = h.rows.len();
        h.rows
            .retain(|r| now.saturating_sub(r.ended_at.unwrap_or(r.started_at)) <= RETENTION_MS);
        if h.rows.len() != before {
            h.revision += 1;
        }
    }

    async fn persist(self: &Arc<Self>) {
        let _save = self.persistence.lock().await;
        let rows = {
            let h = self.history.lock().unwrap();
            if h.storage_error.is_some() {
                return;
            }
            h.rows.clone()
        };
        let path = self.path.clone();
        let result = tokio::task::spawn_blocking(move || save(&path, &rows)).await;
        if !matches!(result, Ok(Ok(()))) {
            self.history.lock().unwrap().storage_error = Some(
                "Incident persistence failed; history is memory-only until scheduler restart"
                    .into(),
            );
            tracing::error!("Incident history persistence failed");
        }
    }

    /// A process-ending action must reach disk before its delayed exit fires.
    pub async fn flush_operation(self: &Arc<Self>, op: &super::ops::Operation) {
        self.observe(Event {
            at: op.started_at,
            component: op.target.clone().unwrap_or_else(|| "scheduler".into()),
            kind: "operator_action".into(),
            state: "open".into(),
            summary: format!("Operator action {:?} requested", op.kind),
            operation_id: Some(op.id.clone()),
            version: env!("CARGO_PKG_VERSION").into(),
        });
        self.persist().await;
    }

    /// How long a backend must be failing before it becomes an incident.
    /// `backend_health` probes every 15s with a 3s timeout and no hysteresis
    /// of its own, so a single slow probe flips the cached status; four
    /// missed probes is a real outage.
    const BACKEND_DOWN_AFTER_MS: u64 = 60_000;

    /// Backend outages are the one cluster fault with no incident of its own.
    /// `maintenance::backend_health` assigns the probe result straight into
    /// its cache with no previous-status comparison and no failure counter,
    /// so there is no transition for it to report - and it lives in a crate
    /// beneath this one, so it could not call [`emit`] without a dependency
    /// cycle. The result was an eight-hour outage whose only trace was a WARN
    /// every 15s under a target the recorder does not read.
    ///
    /// Derive it here instead, from the entity list this tick already builds
    /// for `sample_entities`.
    ///
    /// The debounce is how long THIS recorder has watched the backend fail, not
    /// anything the backend reports about itself. The first version measured
    /// from `last_healthy`, falling back to the recorder's start for a backend
    /// that had never answered - and opened nine incidents, one per backend, on
    /// every scheduler restart. The control plane pushes the backend list about
    /// a hundred seconds after the scheduler boots, each entry `unknown` with no
    /// `last_healthy`, so every one of them had "been down" since boot the
    /// instant it appeared, and recovered at its first probe 13 seconds later.
    /// Two things follow:
    ///
    /// - `unknown` means not probed yet. It is not evidence of an outage.
    /// - a streak starts when it is first OBSERVED. A backend that really has
    ///   been down for hours still opens its incident, a minute after this
    ///   process starts watching it; `last_healthy` only says how long for.
    ///
    /// Returns the transitions to emit. The caller emits them after dropping
    /// the history guard, so every incident still enters through the one
    /// path - [`emit`] to the log ring, [`Self::observe_line`] back out.
    fn backend_transitions(
        &self,
        entities: &[Value],
        now: u64,
        after_ms: u64,
    ) -> Vec<(String, bool, String)> {
        let h = self.history.lock().unwrap();
        let mut failing = self.failing_since.lock().unwrap();
        let mut out = Vec::new();
        let mut seen = std::collections::HashSet::new();

        for entity in entities.iter().filter(|v| v["entity"] == "backend") {
            let Some(id) = entity["id"].as_str() else {
                continue;
            };
            seen.insert(id.to_string());
            let open = h
                .rows
                .iter()
                .any(|r| r.state == "open" && r.kind == "backend_down" && r.component == id);
            let status = entity["status"].as_str().unwrap_or("unknown");

            // A pool backend is never probed: between jobs there is no machine
            // to answer, and that is not an outage. Its pool has its own
            // incident (`pool_breaker_open`) for the case where it cannot get
            // one, so nothing is reported here; an incident opened before the
            // backend moved onto a pool is closed rather than left open forever.
            if let Some(pool) = entity["pool"]["pool"].as_str() {
                failing.remove(id);
                if open {
                    out.push((
                        id.to_string(),
                        false,
                        format!("Backend {id} runs on pool '{pool}'; its health is the pool's"),
                    ));
                }
                continue;
            }

            if !matches!(status, "unhealthy" | "unreachable") {
                failing.remove(id);
                if status == "healthy" && open {
                    out.push((
                        id.to_string(),
                        false,
                        format!("Backend {id} is answering its health check again"),
                    ));
                }
                continue;
            }

            let since = *failing.entry(id.to_string()).or_insert(now);
            if open || now.saturating_sub(since) < after_ms {
                continue;
            }
            // How long it has REALLY been down, when the backend can say; how
            // long it has been watched failing otherwise.
            let down_for = entity["last_healthy"]
                .as_str()
                .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
                .map(|t| now.saturating_sub(t.timestamp_millis().max(0) as u64))
                .unwrap_or_else(|| now.saturating_sub(since));
            out.push((
                id.to_string(),
                true,
                format!(
                    "Backend {id} is {status}; no successful health check for {}",
                    approx_duration(down_for)
                ),
            ));
        }
        // A backend removed from the deployment takes its streak with it.
        failing.retain(|id, _| seen.contains(id));
        out
    }

    fn sample_entities(&self, entities: &[Value]) {
        let mut h = self.history.lock().unwrap();
        let mut changed = false;
        for row in h.rows.iter_mut().filter(|r| r.state == "open") {
            if let Some(entity) = entities
                .iter()
                .find(|v| v["id"].as_str() == Some(&row.component))
            {
                let buffered = entity["buffered_events"].as_u64().unwrap_or(0) as usize;
                if buffered > row.max_buffered_events {
                    row.max_buffered_events = buffered;
                    changed = true;
                }
                if let Ok(publication) = serde_json::from_value::<ssp_protocol::PublicationMetrics>(
                    entity["publication"].clone(),
                ) {
                    // Keep the last nonempty sample so a recovery heartbeat
                    // cannot erase the views responsible for the backlog.
                    if publication.pending_batches > 0 || row.publication.is_none() {
                        row.max_publication_operations = row
                            .max_publication_operations
                            .max(publication.pending_operations);
                        row.max_publication_bytes =
                            row.max_publication_bytes.max(publication.pending_bytes);
                        row.max_publication_age_ms =
                            row.max_publication_age_ms.max(publication.oldest_age_ms);
                        if row.publication.as_ref() != Some(&publication) {
                            row.publication = Some(publication);
                            changed = true;
                        }
                    }
                }
            }
        }
        if changed {
            h.revision += 1;
        }
    }

    pub fn spawn(self: &Arc<Self>, state: AdminState) {
        // Subscribe before the first await; no polling gap on short incidents.
        let mut rx = state.logs.subscribe();
        let this = self.clone();
        let started_at = super::ops::now_ms();
        tokio::spawn(async move {
            this.observe(Event {
                at: started_at,
                component: "scheduler".into(),
                kind: "scheduler_start".into(),
                state: "recorded".into(),
                summary: "Scheduler incident recorder started".into(),
                operation_id: None,
                version: env!("CARGO_PKG_VERSION").into(),
            });
            let mut tick = tokio::time::interval(Duration::from_secs(1));
            let mut saved = 0;
            loop {
                tokio::select! {
                    line = rx.recv() => match line {
                        Ok(line) => this.observe_line(&line),
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                            this.observe(Event { at: super::ops::now_ms(), component: "scheduler".into(), kind: "history_gap".into(), state: "recorded".into(), summary: "Log feed overflowed; some incident transitions may be missing".into(), operation_id: None, version: env!("CARGO_PKG_VERSION").into() });
                        }
                        Err(_) => break,
                    },
                    _ = tick.tick() => {
                        let now = super::ops::now_ms();
                        this.expire(now);
                        let entities = crate::metrics::build_entities(&state.metrics).await;
                        this.sample_entities(&entities);
                        // Emitted outside `backend_transitions`, which holds
                        // the history guard while it reads the open rows.
                        for (backend, down, summary) in
                            this.backend_transitions(&entities, now, Self::BACKEND_DOWN_AFTER_MS)
                        {
                            emit(
                                &backend,
                                "backend_down",
                                if down { "open" } else { "recovered" },
                                &summary,
                                None,
                            );
                        }
                        let revision = this.history.lock().unwrap().revision;
                        if revision != saved { this.persist().await; saved = revision; }
                    }
                }
            }
        });
    }
}

/// Coarse, operator-readable age for an incident summary ("8h12m"). Only ever
/// rendered into prose, never parsed back.
fn approx_duration(ms: u64) -> String {
    let secs = ms / 1000;
    let (h, m) = (secs / 3600, (secs % 3600) / 60);
    if h > 0 {
        format!("{h}h{m}m")
    } else if m > 0 {
        format!("{m}m")
    } else {
        format!("{secs}s")
    }
}

fn append(row: &mut Incident, event: Event) {
    row.event_count += 1;
    if row.events.len() == MAX_EVENTS {
        row.events.remove(1);
    }
    row.events.push_back(event);
}
fn trim(rows: &mut VecDeque<Incident>, now: u64) {
    rows.retain(|r| now.saturating_sub(r.ended_at.unwrap_or(r.started_at)) <= RETENTION_MS);
    rows.truncate(MAX_INCIDENTS);
    // Keep the durable representation below its documented cap, even with
    // unusually long component names. In-memory and disk retention agree.
    while serde_json::to_vec(rows)
        .map(|v| v.len() as u64 > MAX_FILE_BYTES)
        .unwrap_or(false)
    {
        rows.pop_back();
    }
}
fn save(path: &FsPath, rows: &VecDeque<Incident>) -> anyhow::Result<()> {
    use std::io::Write;
    let parent = path.parent().unwrap_or_else(|| FsPath::new("."));
    std::fs::create_dir_all(parent)?;
    let mut tmp = tempfile::NamedTempFile::new_in(parent)?;
    tmp.write_all(&serde_json::to_vec(rows)?)?;
    tmp.as_file().sync_all()?;
    tmp.persist(path)?;
    std::fs::File::open(parent)?.sync_all()?;
    Ok(())
}

#[derive(Default, Deserialize)]
pub struct Filters {
    pub component: Option<String>,
    pub state: Option<String>,
    pub severity: Option<String>,
    pub since: Option<u64>,
    pub before: Option<u64>,
    pub offset: Option<usize>,
    pub limit: Option<usize>,
}

pub async fn list(State(state): State<AdminState>, Query(q): Query<Filters>) -> Json<Value> {
    let h = state.incidents.history.lock().unwrap();
    let rows: Vec<_> = h
        .rows
        .iter()
        .filter(|r| {
            q.component.as_ref().map_or(true, |c| c == &r.component)
                && q.state.as_ref().map_or(true, |s| s == &r.state)
                && q.severity.as_ref().map_or(true, |s| s == &r.severity)
                && q.since.map_or(true, |t| r.started_at >= t)
                && q.before.map_or(true, |t| r.started_at <= t)
        })
        .collect();
    let offset = q.offset.unwrap_or(0);
    let limit = q.limit.unwrap_or(50).clamp(1, 200);
    Json(
        json!({"incidents": rows.iter().skip(offset).take(limit).collect::<Vec<_>>(), "total": rows.len(), "offset": offset, "limit": limit, "storage_error": h.storage_error, "server_time_ms": super::ops::now_ms()}),
    )
}
pub async fn detail(
    State(state): State<AdminState>,
    Path(id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    let h = state.incidents.history.lock().unwrap();
    h.rows
        .iter()
        .find(|r| r.id == id)
        .map(|r| Json(json!({"incident": r})))
        .ok_or_else(|| api_error(axum::http::StatusCode::NOT_FOUND, "Incident not found"))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn duplicate_operator_delivery_does_not_append_or_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let history = Incidents::open(dir.path().join("incidents.json"));
        let mut request = event("open");
        request.kind = "operator_action".into();
        request.operation_id = Some("restart-1".into());
        history.observe(request.clone());
        request.at += 1;
        history.observe(request.clone());
        let mut completed = request.clone();
        completed.state = "recovered".into();
        completed.summary = "Operator action completed".into();
        history.observe(completed.clone());
        history.observe(completed);
        history.observe(request);
        let h = history.history.lock().unwrap();
        assert_eq!(h.rows.len(), 1);
        assert_eq!(h.rows[0].event_count, 2);
        assert_eq!(h.rows[0].state, "recovered");
    }

    #[tokio::test]
    async fn publication_samples_preserve_independent_peaks_and_survive_restart() {
        use ssp_protocol::{PublicationMetrics, PublicationViewMetrics};
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("incidents.json");
        let history = Incidents::open(path.clone());
        history.observe(event("open"));
        let first = PublicationMetrics {
            pending_batches: 2,
            pending_operations: 100,
            pending_bytes: 200,
            oldest_age_ms: 300,
            ..Default::default()
        };
        history.sample_entities(&[json!({"id": "ssp-0", "buffered_events": 50, "publication": first})]);
        let last_nonempty = PublicationMetrics {
            pending_batches: 1,
            pending_operations: 10,
            pending_bytes: 900,
            oldest_age_ms: 1200,
            worst_views: vec![PublicationViewMetrics {
                query_id: "query:slow".into(),
                pending_operations: 10,
                pending_bytes: 900,
                oldest_age_ms: 1200,
            }],
            ..Default::default()
        };
        history.sample_entities(&[json!({"id": "ssp-0", "buffered_events": 5, "publication": last_nonempty})]);
        history.sample_entities(&[json!({"id": "ssp-0", "buffered_events": 0, "publication": PublicationMetrics::default()})]);
        history.observe(event("recovered"));
        history.persist().await;
        let restored = Incidents::open(path);
        let h = restored.history.lock().unwrap();
        let row = &h.rows[0];
        assert_eq!(row.state, "recovered");
        assert_eq!(row.max_buffered_events, 50);
        assert_eq!(row.max_publication_operations, 100);
        assert_eq!(row.max_publication_bytes, 900);
        assert_eq!(row.max_publication_age_ms, 1200);
        assert_eq!(row.publication.as_ref(), Some(&last_nonempty));
    }

    #[test]
    fn emitted_transitions_roundtrip_through_log_ring_into_history() {
        use tracing_subscriber::prelude::*;
        let ring = maintenance::log_ring::LogRing::new(10);
        let mut rx = ring.subscribe();
        let subscriber =
            tracing_subscriber::registry().with(maintenance::log_ring::LogRingLayer::new(ring));
        let dir = tempfile::tempdir().unwrap();
        let history = Incidents::open(dir.path().join("incidents.json"));
        let summary = "Delivery failed: \"quoted\" text\\path\nnext line";
        tracing::subscriber::with_default(subscriber, || {
            emit("ssp-0", "lagging", "open", summary, None);
            emit("ssp-0", "ready", "recovered", "Replay completed", None);
        });
        for _ in 0..2 {
            history.observe_line(&rx.try_recv().expect("incident reached live log feed"));
        }
        let h = history.history.lock().unwrap();
        assert_eq!(h.rows.len(), 1);
        assert_eq!(h.rows[0].state, "recovered");
        assert_eq!(h.rows[0].event_count, 2);
        assert_eq!(h.rows[0].events[0].summary, summary);
        assert_eq!(h.rows[0].events[1].kind, "ready");
    }

    #[tokio::test]
    async fn retention_bounds_count_and_expires_without_new_transitions() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("incidents.json");
        let history = Incidents::open(path.clone());
        let mut initial = event("recorded");
        initial.at = RETENTION_MS + 1;
        history.observe(initial);
        {
            let mut h = history.history.lock().unwrap();
            let template = h.rows[0].clone();
            h.rows = (0..MAX_INCIDENTS + 1)
                .map(|i| {
                    let mut row = template.clone();
                    row.id = i.to_string();
                    row
                })
                .collect();
            trim(&mut h.rows, RETENTION_MS + 1);
            assert_eq!(h.rows.len(), MAX_INCIDENTS);
            assert_eq!(h.rows.back().unwrap().id, (MAX_INCIDENTS - 1).to_string());
        }
        history.expire(2 * RETENTION_MS + 1);
        assert_eq!(history.summary()["total"], MAX_INCIDENTS);
        let before = history.history.lock().unwrap().revision;
        history.expire(2 * RETENTION_MS + 2);
        assert_eq!(history.summary()["total"], 0);
        assert_eq!(history.history.lock().unwrap().revision, before + 1);
        history.persist().await;
        let stored: Vec<Incident> = serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
        assert!(
            stored.is_empty(),
            "periodic expiry must also remove durable rows"
        );
    }

    fn event(state: &str) -> Event {
        Event {
            at: super::super::ops::now_ms(),
            component: "ssp-0".into(),
            kind: "lagging".into(),
            state: state.into(),
            summary: "Delivery failed".into(),
            operation_id: None,
            version: "test".into(),
        }
    }
    fn backend(id: &str, status: &str, last_healthy: Option<&str>) -> Value {
        json!({
            "entity": "backend",
            "id": id,
            "status": status,
            "last_healthy": last_healthy,
        })
    }

    fn rfc3339(ms: u64) -> String {
        chrono::DateTime::from_timestamp_millis(ms as i64)
            .unwrap()
            .to_rfc3339()
    }

    const AFTER: u64 = Incidents::BACKEND_DOWN_AFTER_MS;

    fn recorder() -> (tempfile::TempDir, Arc<Incidents>) {
        let dir = tempfile::tempdir().unwrap();
        let history = Incidents::open(dir.path().join("incidents.json"));
        (dir, history)
    }

    fn open_backend_down(history: &Incidents, id: &str, at: u64) {
        history.observe(Event {
            at,
            component: id.into(),
            kind: "backend_down".into(),
            state: "open".into(),
            summary: "down".into(),
            operation_id: None,
            version: "test".into(),
        });
    }

    /// The regression: a scheduler restart. The control plane pushes the backend
    /// list ~100s after boot, every entry `unknown` with no `last_healthy`, and
    /// the first probe lands up to 15s later. The first version of this timed a
    /// never-healthy backend from the RECORDER's start, so all nine backends had
    /// "been down" for 100s the instant they appeared: nine incidents opened and
    /// closed 13 seconds apart, on every restart.
    #[test]
    fn a_restart_does_not_report_every_backend_as_down() {
        let (_dir, history) = recorder();
        let boot = 1_000_000_000;
        let pushed = boot + 107_000;
        let nine: Vec<Value> = (0..9).map(|i| backend(&format!("b{i}"), "unknown", None)).collect();
        assert!(
            history.backend_transitions(&nine, pushed, AFTER).is_empty(),
            "`unknown` is not-probed-yet, not an outage"
        );
        // The first probe fails for one still-starting container. One failed
        // probe is not an outage either, however long ago the process booted.
        let mut starting = nine.clone();
        starting[0] = backend("b0", "unreachable", None);
        assert!(history.backend_transitions(&starting, pushed + 15_000, AFTER).is_empty());
        // It comes up on the next probe: never reported at all.
        let up: Vec<Value> = (0..9)
            .map(|i| backend(&format!("b{i}"), "healthy", Some(&rfc3339(pushed + 30_000))))
            .collect();
        assert!(history.backend_transitions(&up, pushed + 30_000, AFTER).is_empty());
    }

    fn pool_backend(id: &str, status: &str) -> Value {
        json!({
            "entity": "backend",
            "id": id,
            "status": status,
            "last_healthy": null,
            "pool": { "pool": "render", "ready": 0, "starting": 0, "queued": 0 },
        })
    }

    /// A pool backend at zero machines is `idle` and, while the breaker is
    /// open, `unhealthy`: neither opens `backend_down`, however long it lasts.
    /// The pool's own breaker incident is the one that carries the reason.
    #[test]
    fn a_pool_backend_never_opens_a_backend_incident() {
        let (_dir, history) = recorder();
        let now = 1_000_000_000;
        for status in ["idle", "starting", "unhealthy"] {
            let entities = vec![pool_backend("renderer", status)];
            assert!(history.backend_transitions(&entities, now, AFTER).is_empty());
            assert!(
                history.backend_transitions(&entities, now + AFTER * 10, AFTER).is_empty(),
                "`{status}` on a pool backend is not an outage"
            );
        }
    }

    /// The upgrade path: an incident opened while the backend was still probed
    /// (unreachable at zero machines) is closed the first time the backend is
    /// seen pool-backed, whatever the pool's state.
    #[test]
    fn an_open_incident_recovers_once_the_backend_is_pool_backed() {
        let (_dir, history) = recorder();
        let now = 1_000_000_000;
        open_backend_down(&history, "renderer", now);
        let out = history.backend_transitions(&[pool_backend("renderer", "idle")], now + 1, AFTER);
        assert_eq!(out.len(), 1);
        assert!(!out[0].1, "recovered, not opened");
        assert!(out[0].2.contains("pool 'render'"));
    }

    #[test]
    fn a_single_slow_probe_does_not_open_a_backend_incident() {
        let (_dir, history) = recorder();
        let now = 1_000_000_000;
        let down = [backend("relay", "unreachable", Some(&rfc3339(now - 15_000)))];
        assert!(history.backend_transitions(&down, now, AFTER).is_empty());
        // It recovers before the threshold, and the streak is forgotten: a later
        // blip starts counting from zero, not from this one.
        let up = [backend("relay", "healthy", Some(&rfc3339(now + 15_000)))];
        assert!(history.backend_transitions(&up, now + 15_000, AFTER).is_empty());
        assert!(history.backend_transitions(&down, now + 120_000, AFTER).is_empty());
    }

    #[test]
    fn a_sustained_outage_opens_once_and_recovers_once() {
        let (_dir, history) = recorder();
        let now = 1_000_000_000;
        // Down for eight hours already when this recorder starts watching.
        let down = [backend("relay", "unreachable", Some(&rfc3339(now - 8 * 3_600_000)))];

        assert!(history.backend_transitions(&down, now, AFTER).is_empty(), "watched for 0s");
        let opened = history.backend_transitions(&down, now + AFTER, AFTER);
        assert_eq!(opened.len(), 1);
        assert!(opened[0].1, "opens as down");
        assert!(
            opened[0].2.contains("8h1m"),
            "the summary says how long it has REALLY been down: {}",
            opened[0].2
        );
        open_backend_down(&history, "relay", now + AFTER);

        // While it stays open the tick must not re-report it.
        assert!(history.backend_transitions(&down, now + AFTER + 1000, AFTER).is_empty());

        let healthy = [backend("relay", "healthy", Some(&rfc3339(now + AFTER + 2000)))];
        let recovered = history.backend_transitions(&healthy, now + AFTER + 2000, AFTER);
        assert_eq!(recovered.len(), 1);
        assert!(!recovered[0].1, "recovers");
    }

    #[test]
    fn a_backend_that_never_answers_is_still_reported() {
        let (_dir, history) = recorder();
        let t = 1_000_000_000;
        let never = [backend("relay", "unreachable", None)];
        assert!(history.backend_transitions(&never, t, AFTER).is_empty());
        assert!(history.backend_transitions(&never, t + 30_000, AFTER).is_empty());
        let out = history.backend_transitions(&never, t + AFTER, AFTER);
        assert_eq!(out.len(), 1, "a minute of watching it fail is enough: {out:?}");
    }

    #[test]
    fn a_backend_removed_from_the_deployment_takes_its_streak_with_it() {
        let (_dir, history) = recorder();
        let t = 1_000_000_000;
        let down = [backend("old", "unreachable", None)];
        assert!(history.backend_transitions(&down, t, AFTER).is_empty());
        assert!(history.backend_transitions(&[], t + 30_000, AFTER).is_empty());
        // Re-added later: the clock starts again, it does not resume.
        assert!(history.backend_transitions(&down, t + AFTER + 5_000, AFTER).is_empty());
    }

    #[test]
    fn episodes_coalesce_recover_and_survive_restart() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("incidents.json");
        let s = Incidents::open(path.clone());
        s.observe(event("open"));
        s.observe(event("open"));
        s.observe(event("recovered"));
        {
            let h = s.history.lock().unwrap();
            assert_eq!(h.rows.len(), 1);
            assert_eq!(h.rows[0].event_count, 3);
            assert_eq!(h.rows[0].state, "recovered");
            save(&path, &h.rows).unwrap();
        }
        let s = Incidents::open(path.clone());
        assert_eq!(s.history.lock().unwrap().rows[0].state, "recovered");
        s.observe(event("open"));
        save(&path, &s.history.lock().unwrap().rows).unwrap();
        let s = Incidents::open(path);
        assert_eq!(s.history.lock().unwrap().rows[0].state, "interrupted");
    }
    #[test]
    fn bounded_events_keep_start_and_latest_and_corrupt_history_is_not_overwritten() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("incidents.json");
        let s = Incidents::open(path.clone());
        for _ in 0..100 {
            s.observe(event("open"));
        }
        let h = s.history.lock().unwrap();
        assert_eq!(h.rows[0].event_count, 100);
        assert_eq!(h.rows[0].events.len(), MAX_EVENTS);
        drop(h);
        std::fs::write(&path, "broken").unwrap();
        let s = Incidents::open(path);
        assert!(s.summary()["storage_error"].is_string());
    }
}
