use axum::http::{Request, StatusCode};
use axum::Router;
use http_body_util::BodyExt;
use serde_json::{json, Value};
use std::collections::{BTreeSet, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tempfile::TempDir;
use tokio::sync::RwLock;
use tower::ServiceExt;

use scheduler::config::{DbConfig, LoadBalanceStrategy, SchedulerConfig};
use scheduler::ingest::{self, IngestState};
use scheduler::job_scheduler::{self, JobState, JobTracker};
use scheduler::messages::BufferedEvent;
use scheduler::metrics::{self, MetricsState};
use scheduler::proxy::{self, ProxyState};
use scheduler::query::{self, QueryState, QueryTracker};
use scheduler::replica::Replica;
use scheduler::router::SspPool;
use scheduler::ssp_management::{self, SspManagementState};
use scheduler::transport::{HttpTransport, SspInfo};
use scheduler::wal::EventWal;
use scheduler::SchedulerStatus;

// ---------------------------------------------------------------------------
// Test Harness
// ---------------------------------------------------------------------------

struct TestHarness {
    replica: Arc<RwLock<Replica>>,
    ssp_pool: Arc<RwLock<SspPool>>,
    status: Arc<RwLock<SchedulerStatus>>,
    event_buffer: Arc<RwLock<VecDeque<BufferedEvent>>>,
    seq_counter: Arc<AtomicU64>,
    wal: Arc<RwLock<EventWal>>,
    drain_lock: Arc<tokio::sync::Mutex<()>>,
    reclone_lock: Arc<tokio::sync::Mutex<()>>,
    transport: Arc<HttpTransport>,
    query_tracker: Arc<QueryTracker>,
    job_tracker: Arc<JobTracker>,
    config: Arc<SchedulerConfig>,
    snapshot_seq_cell: Arc<AtomicU64>,
    /// One per harness: `/ingest` hands delivery to this queue instead of
    /// doing it inline, so a test that asserts on SSP state after a post must
    /// `h.fanout.idle().await` first.
    fanout: Arc<ingest::Fanout>,
    _replica_dir: TempDir,
    _wal_dir: TempDir,
}

impl TestHarness {
    async fn new() -> Self {
        Self::with_options(SchedulerStatus::Ready, 10_000).await
    }

    async fn with_status(status: SchedulerStatus) -> Self {
        Self::with_options(status, 10_000).await
    }

    #[allow(dead_code)]
    async fn with_max_buffer(max_buffer: usize) -> Self {
        Self::with_options(SchedulerStatus::Ready, max_buffer).await
    }

    async fn with_options(status: SchedulerStatus, max_buffer: usize) -> Self {
        let replica_dir = TempDir::new().expect("Failed to create temp dir for replica");
        let wal_dir = TempDir::new().expect("Failed to create temp dir for WAL");

        let replica_path = replica_dir.path().join("replica_db");
        let wal_path = wal_dir.path().join("event_wal.log");

        let replica = Replica::new(replica_path.clone())
            .await
            .expect("Failed to create replica");

        let wal = EventWal::new(wal_path.clone()).expect("Failed to create WAL");

        let config = SchedulerConfig {
            db: DbConfig {
                url: "http://localhost:8000".to_string(),
                namespace: "sp00ky".to_string(),
                database: "sp00ky".to_string(),
                username: "root".to_string(),
                password: "root".to_string(),
            },
            load_balance: LoadBalanceStrategy::LeastQueries,
            heartbeat_interval_ms: 1000,
            heartbeat_timeout_ms: 5000,
            bootstrap_chunk_size: 100,
            job_tables: vec![],
            replica_db_path: replica_path,
            ingest_host: None,
            ingest_port: 0,
            snapshot_update_interval_secs: 300,
            max_buffer_per_ssp: max_buffer,
            bootstrap_timeout_secs: 5,
            ssp_poll_interval_ms: 100,
            wal_path,
            scheduler_id: "test-scheduler".to_string(),
            ..SchedulerConfig::default()
        };

        let snapshot_seq_cell = replica.snapshot_seq_cell();
        Self {
            snapshot_seq_cell,
            replica: Arc::new(RwLock::new(replica)),
            ssp_pool: Arc::new(RwLock::new(SspPool::new(
                LoadBalanceStrategy::LeastQueries,
                max_buffer,
            ))),
            status: Arc::new(RwLock::new(status)),
            event_buffer: Arc::new(RwLock::new(VecDeque::new())),
            seq_counter: Arc::new(AtomicU64::new(0)),
            wal: Arc::new(RwLock::new(wal)),
            drain_lock: Arc::new(tokio::sync::Mutex::new(())),
            reclone_lock: Arc::new(tokio::sync::Mutex::new(())),
            transport: Arc::new(HttpTransport::new()),
            query_tracker: Arc::new(QueryTracker::new()),
            job_tracker: Arc::new(JobTracker::new()),
            config: Arc::new(config),
            _replica_dir: replica_dir,
            _wal_dir: wal_dir,
            fanout: ingest::Fanout::start(),
        }
    }

    fn ingest_router(&self) -> Router {
        let state = IngestState {
            replica: Arc::clone(&self.replica),
            transport: Arc::clone(&self.transport),
            ssp_pool: Arc::clone(&self.ssp_pool),
            status: Arc::clone(&self.status),
            event_buffer: Arc::clone(&self.event_buffer),
            seq_counter: Arc::clone(&self.seq_counter),
            wal: Arc::clone(&self.wal),
            drain_lock: Arc::clone(&self.drain_lock),
            db_config: Arc::new(self.config.db.clone()),
            job_tables: Arc::new(vec![]),
            observer_permits: Arc::new(tokio::sync::Semaphore::new(8)),
            snapshot_seq: Arc::clone(&self.snapshot_seq_cell),
            fanout: Arc::clone(&self.fanout),
        };
        ingest::create_ingest_router(state)
    }

    fn ssp_router(&self) -> Router {
        let state = SspManagementState {
            ssp_pool: Arc::clone(&self.ssp_pool),
            replica: Arc::clone(&self.replica),
            transport: Arc::clone(&self.transport),
            config: Arc::clone(&self.config),
            status: Arc::clone(&self.status),
            event_buffer: Arc::clone(&self.event_buffer),
            seq_counter: Arc::clone(&self.seq_counter),
            reclone_lock: Arc::clone(&self.reclone_lock),
            wal: Arc::clone(&self.wal),
            drain_lock: Arc::clone(&self.drain_lock),
        };
        ssp_management::create_ssp_router(state)
    }

    fn proxy_router(&self) -> Router {
        let state = ProxyState {
            replica: Arc::clone(&self.replica),
            status: Arc::clone(&self.status),
        };
        proxy::create_proxy_router(state)
    }

    fn query_router(&self) -> Router {
        let state = QueryState {
            ssp_pool: Arc::clone(&self.ssp_pool),
            transport: Arc::clone(&self.transport),
            query_tracker: Arc::clone(&self.query_tracker),
        };
        query::create_query_router(state)
    }

    fn job_router(&self) -> Router {
        let state = JobState {
            ssp_pool: Arc::clone(&self.ssp_pool),
            transport: Arc::clone(&self.transport),
            job_tracker: Arc::clone(&self.job_tracker),
        };
        job_scheduler::create_job_router(state)
    }

    fn metrics_router(&self) -> Router {
        metrics::create_metrics_router(self.metrics_state())
    }

    /// Everything `admin::build` needs, over this harness's shared state.
    ///
    /// The db slot is never populated: these tests cover the plane's
    /// behaviour when the scheduler has no database handle yet, which is
    /// also its behaviour during the initial replica clone.
    fn admin_deps(
        &self,
        cloud_api_url: Option<&str>,
        auth_secret: Option<&str>,
    ) -> scheduler::admin::AdminDeps {
        let host: Arc<dyn maintenance::MaintenanceHost> =
            Arc::new(scheduler::maintenance_host::SchedulerHost {
                ingest: IngestState {
                    replica: Arc::clone(&self.replica),
                    transport: Arc::clone(&self.transport),
                    ssp_pool: Arc::clone(&self.ssp_pool),
                    status: Arc::clone(&self.status),
                    event_buffer: Arc::clone(&self.event_buffer),
                    seq_counter: Arc::clone(&self.seq_counter),
                    wal: Arc::clone(&self.wal),
                    drain_lock: Arc::clone(&self.drain_lock),
                    db_config: Arc::new(self.config.db.clone()),
                    job_tables: Arc::new(vec![]),
                    observer_permits: Arc::new(tokio::sync::Semaphore::new(8)),
                    snapshot_seq: Arc::clone(&self.snapshot_seq_cell),
            fanout: Arc::clone(&self.fanout),
                },
            });
        let (backup_tx, _backup_rx) = maintenance::backup::create_backup_channel();
        let (restore_tx, _restore_rx) = maintenance::restore::create_restore_channel();
        let backup = Arc::new(maintenance::BackupState {
            host,
            config: Arc::new(maintenance::BackupConfig::from_env()),
            registry: Arc::new(maintenance::backup::BackupRegistry::new()),
            tx: backup_tx,
            restore_registry: Arc::new(maintenance::restore::RestoreRegistry::new()),
            restore_tx,
            backup_restore_lock: Arc::new(tokio::sync::Mutex::new(())),
        });
        scheduler::admin::AdminDeps {
            metrics: self.metrics_state(),
            transport: Arc::clone(&self.transport),
            logs: maintenance::log_ring::LogRing::new(128),
            db_config: self.config.db.clone(),
            db_slot: scheduler::admin::new_db_slot(),
            backup,
            resync: ssp_management::ResyncArgs {
                ssp_pool: Arc::clone(&self.ssp_pool),
                replica: Arc::clone(&self.replica),
                config: Arc::clone(&self.config),
                status: Arc::clone(&self.status),
                seq_counter: Arc::clone(&self.seq_counter),
                reclone_lock: Arc::clone(&self.reclone_lock),
            },
            cloud: cloud_api_url.and_then(|url| {
                scheduler::admin::cloud::CloudLink::new(
                    url.to_string(),
                    "test-project".to_string(),
                    "cluster-secret".to_string(),
                )
            }),
            auth_secret: auth_secret.map(str::to_string),
            supervised: false,
        }
    }

    fn metrics_state(&self) -> MetricsState {
        MetricsState {
            ssp_pool: Arc::clone(&self.ssp_pool),
            query_tracker: Arc::clone(&self.query_tracker),
            job_tracker: Arc::clone(&self.job_tracker),
            start_time: std::time::Instant::now(),
            scheduler_id: "test-scheduler".to_string(),
            status: Arc::clone(&self.status),
            backend_health: scheduler::backend_health::create_health_cache(&[]),
            shared_backend_configs: scheduler::backend_health::create_shared_configs(&[]),
            ingest: IngestState {
                replica: Arc::clone(&self.replica),
                transport: Arc::clone(&self.transport),
                ssp_pool: Arc::clone(&self.ssp_pool),
                status: Arc::clone(&self.status),
                event_buffer: Arc::clone(&self.event_buffer),
                seq_counter: Arc::clone(&self.seq_counter),
                wal: Arc::clone(&self.wal),
                drain_lock: Arc::clone(&self.drain_lock),
                db_config: Arc::new(self.config.db.clone()),
                job_tables: Arc::new(vec![]),
                observer_permits: Arc::new(tokio::sync::Semaphore::new(8)),
                snapshot_seq: Arc::clone(&self.snapshot_seq_cell),
            fanout: Arc::clone(&self.fanout),
            },
            replica: Arc::clone(&self.replica),
            surrealdb_version: Arc::new(RwLock::new("unknown".to_string())),
            heartbeat: scheduler::heartbeat::HeartbeatStats::new(),
            heartbeat_config: scheduler::heartbeat::Config {
                interval_secs: 30,
                timeout_secs: 25,
                fail_threshold: 3,
                ping_url: None,
                webhook_url: None,
            },
            drift: Arc::new(RwLock::new(scheduler::drift::DriftState::default())),
            drift_config: scheduler::drift::DriftConfig::default(),
        }
    }

    fn full_app(&self) -> Router {
        Router::new()
            .merge(self.ingest_router())
            .merge(self.ssp_router())
            .merge(self.proxy_router())
            .merge(self.query_router())
            .merge(self.job_router())
            .merge(self.metrics_router())
    }

    async fn add_ready_ssp(&self, id: &str, url: &str) {
        let ssp_info = SspInfo {
            id: id.to_string(),
            url: url.to_string(),
            version: "test".to_string(),
            connected_at: std::time::Instant::now(),
            last_heartbeat: std::time::Instant::now(),
            query_count: 0,
            views: 0,
            cpu_usage: None,
            memory_usage: None,
            env: None,
            bootstrap: None,
        };
        let mut pool = self.ssp_pool.write().await;
        pool.upsert(ssp_info);
        pool.mark_bootstrapping(id);
        let _ = pool.mark_ready(id);
    }

    async fn add_bootstrapping_ssp(&self, id: &str, url: &str) {
        let ssp_info = SspInfo {
            id: id.to_string(),
            url: url.to_string(),
            version: "test".to_string(),
            connected_at: std::time::Instant::now(),
            last_heartbeat: std::time::Instant::now(),
            query_count: 0,
            views: 0,
            cpu_usage: None,
            memory_usage: None,
            env: None,
            bootstrap: None,
        };
        let mut pool = self.ssp_pool.write().await;
        pool.upsert(ssp_info);
        pool.mark_bootstrapping(id);
    }

    async fn set_status(&self, status: SchedulerStatus) {
        *self.status.write().await = status;
    }
}

// ---------------------------------------------------------------------------
// Request Helpers
// ---------------------------------------------------------------------------

async fn post_json(app: Router, path: &str, body: &Value) -> (StatusCode, Value) {
    let request = Request::builder()
        .method("POST")
        .uri(path)
        .header("content-type", "application/json")
        .body(axum::body::Body::from(serde_json::to_vec(body).unwrap()))
        .unwrap();

    let response = app.oneshot(request).await.unwrap();
    let status = response.status();
    let body_bytes = response.into_body().collect().await.unwrap().to_bytes();
    let body_value = serde_json::from_slice(&body_bytes).unwrap_or(Value::Null);
    (status, body_value)
}

async fn get_json(app: Router, path: &str) -> (StatusCode, Value) {
    let request = Request::builder()
        .method("GET")
        .uri(path)
        .body(axum::body::Body::empty())
        .unwrap();

    let response = app.oneshot(request).await.unwrap();
    let status = response.status();
    let body_bytes = response.into_body().collect().await.unwrap().to_bytes();
    let body_value = serde_json::from_slice(&body_bytes).unwrap_or(Value::Null);
    (status, body_value)
}

// ---------------------------------------------------------------------------
// Mock SSP Server
// ---------------------------------------------------------------------------

struct MockSsp {
    addr: String,
    received: Arc<tokio::sync::Mutex<Vec<Value>>>,
}

impl MockSsp {
    async fn start() -> Self {
        Self::start_with_health("ready").await
    }

    /// An SSP whose `/ingest` refuses the first `fail_first` deliveries (503)
    /// and accepts everything after. Stands in for a node that was blocked
    /// behind its circuit lock for one scheduler POST timeout.
    async fn start_flaky_ingest(fail_first: usize) -> Self {
        let received = Arc::new(tokio::sync::Mutex::new(Vec::new()));
        let received_clone = Arc::clone(&received);
        let failures_left = Arc::new(std::sync::atomic::AtomicUsize::new(fail_first));

        let app = Router::new()
            .route(
                "/ingest",
                axum::routing::post({
                    let received = Arc::clone(&received_clone);
                    move |axum::Json(body): axum::Json<Value>| {
                        let received = Arc::clone(&received);
                        let failures_left = Arc::clone(&failures_left);
                        async move {
                            let left = failures_left.load(Ordering::SeqCst);
                            if left > 0 {
                                failures_left.store(left - 1, Ordering::SeqCst);
                                return StatusCode::SERVICE_UNAVAILABLE;
                            }
                            received.lock().await.push(body);
                            StatusCode::OK
                        }
                    }
                }),
            )
            .route(
                "/health",
                axum::routing::get(|| async { axum::Json(json!({"status": "ready"})) }),
            );

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("Failed to bind mock SSP");
        let addr = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;

        MockSsp { addr, received }
    }

    /// Like `start`, but `/health` reports the given status (e.g. `"failed"`
    /// to exercise the scheduler's bootstrap-failure path).
    async fn start_with_health(health_status: &'static str) -> Self {
        let received = Arc::new(tokio::sync::Mutex::new(Vec::new()));
        let received_clone = Arc::clone(&received);

        let app = {
            let received = Arc::clone(&received_clone);
            Router::new()
                .route(
                    "/ingest",
                    axum::routing::post({
                        let received = Arc::clone(&received);
                        move |axum::Json(body): axum::Json<Value>| {
                            let received = Arc::clone(&received);
                            async move {
                                received.lock().await.push(body);
                                StatusCode::OK
                            }
                        }
                    }),
                )
                .route(
                    "/view/register",
                    axum::routing::post({
                        let received = Arc::clone(&received);
                        move |axum::Json(body): axum::Json<Value>| {
                            let received = Arc::clone(&received);
                            async move {
                                received.lock().await.push(body);
                                StatusCode::OK
                            }
                        }
                    }),
                )
                .route(
                    "/view/unregister",
                    axum::routing::post({
                        let received = Arc::clone(&received);
                        move |axum::Json(body): axum::Json<Value>| {
                            let received = Arc::clone(&received);
                            async move {
                                received.lock().await.push(body);
                                StatusCode::OK
                            }
                        }
                    }),
                )
                .route(
                    "/job/dispatch",
                    axum::routing::post({
                        let received = Arc::clone(&received);
                        move |axum::Json(body): axum::Json<Value>| {
                            let received = Arc::clone(&received);
                            async move {
                                received.lock().await.push(body);
                                StatusCode::OK
                            }
                        }
                    }),
                )
                .route(
                    "/health",
                    axum::routing::get(move || async move {
                        axum::Json(json!({"status": health_status}))
                    }),
                )
        };

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("Failed to bind mock SSP");
        let addr = format!("http://{}", listener.local_addr().unwrap());

        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        // Brief yield to let the server start
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;

        MockSsp {
            addr,
            received: received_clone,
        }
    }

    async fn received_count(&self) -> usize {
        self.received.lock().await.len()
    }

    async fn received_bodies(&self) -> Vec<Value> {
        self.received.lock().await.clone()
    }
}

// ---------------------------------------------------------------------------
// Helper: make an ingest payload
// ---------------------------------------------------------------------------

fn ingest_payload(table: &str, op: &str, id: &str) -> Value {
    json!({
        "table": table,
        "op": op,
        "id": id,
        "record": {"name": "test"}
    })
}

// ===========================================================================
// Module 1: Ingest Tests
// ===========================================================================

mod ingest_tests {
    use super::*;

    /// A live delivery the SSP did not acknowledge is not lost: the SSP is
    /// parked in `Lagging`, later events queue behind the missed one, and a
    /// redelivery task drains the queue in order and brings it back to Ready.
    /// Before this, a `/ingest` POST that timed out was only logged, and the
    /// row it carried was missing from every view on that SSP until a cold
    /// re-registration.
    #[tokio::test]
    async fn a_failed_live_delivery_is_redelivered_in_order() {
        let h = TestHarness::new().await;
        let ssp = MockSsp::start_flaky_ingest(1).await;
        h.add_ready_ssp("ssp-flaky", &ssp.addr).await;
        let app = h.ingest_router();

        // The first delivery is refused: the scheduler still answers the
        // upstream 200 (the event is in the WAL) and parks the SSP.
        let (status, _) =
            post_json(app.clone(), "/ingest", &ingest_payload("game", "CREATE", "game:1")).await;
        assert_eq!(status, StatusCode::OK);
        h.fanout.idle().await;
        {
            let pool = h.ssp_pool.read().await;
            assert!(pool.is_lagging("ssp-flaky"), "missed delivery parks the SSP");
            assert!(!pool.is_ready("ssp-flaky"));
        }

        // A second event while lagging must queue behind the first, never
        // overtake it on the live path.
        let (status, _) =
            post_json(app.clone(), "/ingest", &ingest_payload("game", "CREATE", "game:2")).await;
        assert_eq!(status, StatusCode::OK);
        h.fanout.idle().await;

        // The redelivery task retries with backoff (500ms first) and drains
        // the queue once the SSP answers again.
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        loop {
            let caught_up = h.ssp_pool.read().await.is_ready("ssp-flaky");
            if caught_up && ssp.received_count().await == 2 {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "SSP never caught up: ready={caught_up} received={}",
                ssp.received_count().await
            );
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        let ids: Vec<String> = ssp
            .received_bodies()
            .await
            .iter()
            .map(|b| b["id"].as_str().unwrap_or("").to_string())
            .collect();
        assert_eq!(ids, ["game:1", "game:2"], "redelivered in original order");

        // Back on the live path: the next event goes straight through.
        let (status, _) =
            post_json(app, "/ingest", &ingest_payload("game", "CREATE", "game:3")).await;
        assert_eq!(status, StatusCode::OK);
        h.fanout.idle().await;
        assert_eq!(ssp.received_count().await, 3);
        assert!(h.ssp_pool.read().await.is_ready("ssp-flaky"));
    }

    /// SurrealDB can fire a vertex table's DELETE event with a cascaded EDGE
    /// as `$before` (seen on `spky release`: `_00_app_release` events carrying
    /// `_00_list_ref_anon:<edge>` ids). Such an event is not a row of the
    /// table: it must be answered 200 (so upstream does not retry) and go
    /// nowhere — not to the buffer, the WAL, or any SSP.
    #[tokio::test]
    async fn ingest_ignores_an_event_whose_id_belongs_to_another_table() {
        let h = TestHarness::new().await;
        let ssp = MockSsp::start().await;
        h.add_ready_ssp("ssp-a", &ssp.addr).await;
        let app = h.ingest_router();

        let (status, _) = post_json(
            app.clone(),
            "/ingest",
            &ingest_payload("_00_app_release", "DELETE", "_00_list_ref_anon:b0va68i1z27z68tzgycm"),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        h.fanout.idle().await;
        assert_eq!(h.event_buffer.read().await.len(), 0, "not buffered");
        assert_eq!(ssp.received_count().await, 0, "not broadcast");

        // The ordinary qualified and bare forms still flow.
        let (status, _) =
            post_json(app.clone(), "/ingest", &ingest_payload("_00_app_release", "DELETE", "_00_app_release:web")).await;
        assert_eq!(status, StatusCode::OK);
        let (status, _) = post_json(app, "/ingest", &ingest_payload("_00_app_release", "CREATE", "web")).await;
        assert_eq!(status, StatusCode::OK);
        h.fanout.idle().await;
        assert_eq!(h.event_buffer.read().await.len(), 2);
        assert_eq!(ssp.received_count().await, 2);
    }

    #[test]
    fn foreign_table_prefix_only_flags_a_clean_other_table_name() {
        use scheduler::ingest::foreign_table_prefix;
        assert_eq!(foreign_table_prefix("_00_app_release", "_00_list_ref_anon:abc"), Some("_00_list_ref_anon".into()));
        assert_eq!(foreign_table_prefix("game", "game:abc"), None);
        assert_eq!(foreign_table_prefix("game", "abc"), None);
        // Escaped / composite ids are not second-guessed.
        assert_eq!(foreign_table_prefix("game", "⟨weird:id⟩"), None);
        assert_eq!(foreign_table_prefix("game", "{a:1}"), None);
    }

    #[tokio::test]
    async fn ingest_rejects_during_cloning() {
        // The initial clone: no replica content yet (snapshot_seq 0), so
        // there is nowhere to put the event. The upstream write aborts.
        let h = TestHarness::with_status(SchedulerStatus::Cloning).await;
        assert_eq!(h.snapshot_seq_cell.load(Ordering::SeqCst), 0);
        let app = h.ingest_router();

        let (status, _) = post_json(app, "/ingest", &ingest_payload("user", "CREATE", "u1")).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    }

    #[tokio::test]
    async fn ingest_is_accepted_while_booting_on_a_persisted_snapshot() {
        // A restart on a persisted snapshot spends its `Cloning` phase on
        // checks, not on a clone: the replica and the WAL are there, so the
        // event is buffered like during normal operation. Refusing it would
        // abort the user's write for the whole boot.
        let h = TestHarness::with_status(SchedulerStatus::Cloning).await;
        h.snapshot_seq_cell.store(468_893, Ordering::SeqCst);
        let app = h.ingest_router();

        let (status, _) = post_json(app, "/ingest", &ingest_payload("user", "CREATE", "u1")).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(h.event_buffer.read().await.len(), 1, "buffered for the first drain");
        assert_eq!(h.wal.read().await.recover().unwrap().len(), 1, "and durable in the WAL");

        // Restoring is a real gap (the replica is being replaced): still refused.
        *h.status.write().await = SchedulerStatus::Restoring;
        let (status, _) =
            post_json(h.ingest_router(), "/ingest", &ingest_payload("user", "CREATE", "u2")).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    }

    #[tokio::test]
    async fn ingest_succeeds_when_ready() {
        let h = TestHarness::new().await;
        let app = h.ingest_router();

        let (status, _) = post_json(app, "/ingest", &ingest_payload("user", "CREATE", "u1")).await;
        assert_eq!(status, StatusCode::OK);
    }

    #[tokio::test]
    async fn ingest_succeeds_when_snapshot_frozen() {
        let h = TestHarness::with_status(SchedulerStatus::SnapshotFrozen).await;
        let app = h.ingest_router();

        let (status, _) = post_json(app, "/ingest", &ingest_payload("user", "CREATE", "u1")).await;
        assert_eq!(status, StatusCode::OK);
    }

    #[tokio::test]
    async fn ingest_invalid_operation() {
        let h = TestHarness::new().await;
        let app = h.ingest_router();

        let (status, _) = post_json(app, "/ingest", &ingest_payload("user", "MERGE", "u1")).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn ingest_case_insensitive_op() {
        let h = TestHarness::new().await;

        for op in &["create", "Create", "CREATE", "update", "Update", "delete", "Delete"] {
            let app = h.ingest_router();
            let (status, _) =
                post_json(app, "/ingest", &ingest_payload("user", op, "u1")).await;
            assert_eq!(status, StatusCode::OK, "op '{}' should succeed", op);
        }
    }

    #[tokio::test]
    async fn ingest_assigns_monotonic_seq() {
        let h = TestHarness::new().await;

        for i in 0..5 {
            let app = h.ingest_router();
            let (status, _) = post_json(
                app,
                "/ingest",
                &ingest_payload("user", "CREATE", &format!("u{}", i)),
            )
            .await;
            assert_eq!(status, StatusCode::OK);
        }

        // seq_counter should be 5
        assert_eq!(h.seq_counter.load(Ordering::SeqCst), 5);

        // Buffer events should have seq 1..5
        let buffer = h.event_buffer.read().await;
        assert_eq!(buffer.len(), 5);
        for (i, event) in buffer.iter().enumerate() {
            assert_eq!(event.seq, (i + 1) as u64);
        }
    }

    #[tokio::test]
    async fn ingest_writes_to_wal() {
        let h = TestHarness::new().await;
        let app = h.ingest_router();

        let (status, _) = post_json(app, "/ingest", &ingest_payload("user", "CREATE", "u1")).await;
        assert_eq!(status, StatusCode::OK);

        // Verify WAL contains the event
        let wal = h.wal.read().await;
        let events = wal.recover().expect("WAL recovery failed");
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].seq, 1);
        assert_eq!(events[0].update.table, "user");
    }

    #[tokio::test]
    async fn ingest_buffers_for_bootstrapping_ssp() {
        let h = TestHarness::new().await;
        h.add_bootstrapping_ssp("ssp-1", "http://localhost:9999").await;

        let app = h.ingest_router();
        let (status, _) = post_json(app, "/ingest", &ingest_payload("user", "CREATE", "u1")).await;
        assert_eq!(status, StatusCode::OK);
        h.fanout.idle().await;

        // Check that the message was buffered for the bootstrapping SSP
        let pool = h.ssp_pool.read().await;
        assert!(pool.buffer_size("ssp-1") >= 1);
    }

    #[tokio::test]
    async fn ingest_broadcasts_to_ready_ssp() {
        let mock = MockSsp::start().await;
        let h = TestHarness::new().await;
        h.add_ready_ssp("ssp-1", &mock.addr).await;

        let app = h.ingest_router();
        let (status, _) = post_json(app, "/ingest", &ingest_payload("user", "CREATE", "u1")).await;
        assert_eq!(status, StatusCode::OK);

        // Give broadcast a moment to complete
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        // Mock should have received the ingest
        assert!(mock.received_count().await >= 1);
        let bodies = mock.received_bodies().await;
        assert_eq!(bodies[0]["table"], "user");
        assert_eq!(bodies[0]["op"], "CREATE");
    }

    #[tokio::test]
    async fn ingest_event_buffer_ordering() {
        let h = TestHarness::new().await;

        for i in 0..10 {
            let app = h.ingest_router();
            let (status, _) = post_json(
                app,
                "/ingest",
                &ingest_payload("user", "CREATE", &format!("u{}", i)),
            )
            .await;
            assert_eq!(status, StatusCode::OK);
        }

        let buffer = h.event_buffer.read().await;
        assert_eq!(buffer.len(), 10);

        // Verify strictly ascending
        for i in 1..buffer.len() {
            assert!(
                buffer[i].seq > buffer[i - 1].seq,
                "seq {} should be > seq {}",
                buffer[i].seq,
                buffer[i - 1].seq
            );
        }
    }
}

// ===========================================================================
// Module 2: SSP Management Tests
// ===========================================================================

mod ssp_management_tests {
    use super::*;

    fn register_payload(ssp_id: &str, url: &str) -> Value {
        json!({
            "ssp_id": ssp_id,
            "url": url,
            "version": "test"
        })
    }

    fn heartbeat_payload(ssp_id: &str) -> Value {
        json!({
            "ssp_id": ssp_id,
            "timestamp": 1000,
            "views": 5,
            "cpu_usage": 45.0,
            "memory_usage": 60.0,
            "version": "test"
        })
    }

    #[tokio::test]
    async fn register_returns_202_with_snapshot_seq() {
        let h = TestHarness::new().await;
        let app = h.ssp_router();

        let (status, body) = post_json(
            app,
            "/ssp/register",
            &register_payload("ssp-1", "http://localhost:9999"),
        )
        .await;

        assert_eq!(status, StatusCode::ACCEPTED);
        assert!(body.get("snapshot_seq").is_some());
    }

    #[tokio::test]
    async fn register_freezes_snapshot() {
        let h = TestHarness::new().await;
        let app = h.ssp_router();

        let (status, _) = post_json(
            app,
            "/ssp/register",
            &register_payload("ssp-1", "http://localhost:9999"),
        )
        .await;
        assert_eq!(status, StatusCode::ACCEPTED);

        let current = *h.status.read().await;
        assert_eq!(current, SchedulerStatus::SnapshotFrozen);
    }

    #[tokio::test]
    async fn register_marks_bootstrapping() {
        let h = TestHarness::new().await;
        let app = h.ssp_router();

        let (status, _) = post_json(
            app,
            "/ssp/register",
            &register_payload("ssp-1", "http://localhost:9999"),
        )
        .await;
        assert_eq!(status, StatusCode::ACCEPTED);

        let pool = h.ssp_pool.read().await;
        assert!(pool.get("ssp-1").is_some(), "SSP should exist in pool");
        assert!(!pool.is_ready("ssp-1"), "SSP should NOT be ready yet");
    }

    #[tokio::test]
    async fn register_records_bootstrap_seq() {
        let h = TestHarness::new().await;
        let app = h.ssp_router();

        let (status, body) = post_json(
            app,
            "/ssp/register",
            &register_payload("ssp-1", "http://localhost:9999"),
        )
        .await;
        assert_eq!(status, StatusCode::ACCEPTED);

        let expected_seq = body["snapshot_seq"].as_u64().unwrap();
        let pool = h.ssp_pool.read().await;
        assert_eq!(pool.get_bootstrap_seq("ssp-1"), Some(expected_seq));
    }

    #[tokio::test]
    async fn register_empty_ssp_id() {
        let h = TestHarness::new().await;

        // Empty ID
        let app = h.ssp_router();
        let (status, _) = post_json(
            app,
            "/ssp/register",
            &register_payload("", "http://localhost:9999"),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);

        // Whitespace-only ID
        let app = h.ssp_router();
        let (status, _) = post_json(
            app,
            "/ssp/register",
            &register_payload("   ", "http://localhost:9999"),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn register_invalid_url() {
        let h = TestHarness::new().await;

        for url in &["ftp://example.com", "ws://example.com", "example.com"] {
            let app = h.ssp_router();
            let (status, _) =
                post_json(app, "/ssp/register", &register_payload("ssp-1", url)).await;
            assert_eq!(
                status,
                StatusCode::BAD_REQUEST,
                "URL '{}' should be rejected",
                url
            );
        }
    }

    #[tokio::test]
    async fn register_during_cloning() {
        let h = TestHarness::with_status(SchedulerStatus::Cloning).await;
        let app = h.ssp_router();

        let (status, _) = post_json(
            app,
            "/ssp/register",
            &register_payload("ssp-1", "http://localhost:9999"),
        )
        .await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    }

    #[tokio::test]
    async fn heartbeat_unknown_ssp() {
        let h = TestHarness::new().await;
        let app = h.ssp_router();

        let (status, _) =
            post_json(app, "/ssp/heartbeat", &heartbeat_payload("nonexistent")).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn heartbeat_registered_ssp() {
        let h = TestHarness::new().await;
        h.add_ready_ssp("ssp-1", "http://localhost:9999").await;

        let app = h.ssp_router();
        let (status, _) = post_json(app, "/ssp/heartbeat", &heartbeat_payload("ssp-1")).await;
        assert_eq!(status, StatusCode::OK);
    }

    #[tokio::test]
    async fn heartbeat_updates_metrics() {
        let h = TestHarness::new().await;
        h.add_ready_ssp("ssp-1", "http://localhost:9999").await;

        let app = h.ssp_router();
        let (status, _) = post_json(app, "/ssp/heartbeat", &heartbeat_payload("ssp-1")).await;
        assert_eq!(status, StatusCode::OK);

        let pool = h.ssp_pool.read().await;
        let ssp = pool.get("ssp-1").unwrap();
        assert_eq!(ssp.views, 5);
        assert_eq!(ssp.cpu_usage, Some(45.0));
        assert_eq!(ssp.memory_usage, Some(60.0));
    }

    #[tokio::test]
    async fn multiple_registrations_keep_frozen() {
        let h = TestHarness::new().await;

        // Register first SSP
        let app = h.ssp_router();
        let (status, _) = post_json(
            app,
            "/ssp/register",
            &register_payload("ssp-1", "http://localhost:9991"),
        )
        .await;
        assert_eq!(status, StatusCode::ACCEPTED);

        // Register second SSP
        let app = h.ssp_router();
        let (status, _) = post_json(
            app,
            "/ssp/register",
            &register_payload("ssp-2", "http://localhost:9992"),
        )
        .await;
        assert_eq!(status, StatusCode::ACCEPTED);

        // Status should still be frozen
        let current = *h.status.read().await;
        assert_eq!(current, SchedulerStatus::SnapshotFrozen);

        // Both SSPs should be bootstrapping
        let pool = h.ssp_pool.read().await;
        assert!(pool.has_active_bootstrap());
    }
}

// ===========================================================================
// Module 3: Proxy Tests
// ===========================================================================

mod proxy_tests {
    use super::*;

    #[tokio::test]
    async fn proxy_signin_always_ok() {
        let h = TestHarness::new().await;
        let app = h.proxy_router();

        let (status, _) = post_json(app, "/proxy/signin", &json!({})).await;
        assert_eq!(status, StatusCode::OK);
    }

    #[tokio::test]
    async fn proxy_use_always_ok() {
        let h = TestHarness::new().await;
        let app = h.proxy_router();

        let (status, _) = post_json(app, "/proxy/use", &json!({})).await;
        assert_eq!(status, StatusCode::OK);
    }

    #[tokio::test]
    async fn proxy_query_empty_table() {
        let h = TestHarness::new().await;
        let app = h.proxy_router();

        let (status, body) = post_json(
            app,
            "/proxy/query",
            &json!({"query": "SELECT * FROM nonexistent"}),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert!(body.is_array());
        assert_eq!(body.as_array().unwrap().len(), 0);
    }

    #[tokio::test]
    async fn proxy_query_returns_results() {
        let h = TestHarness::new().await;

        // Use INFO FOR DB — always returns structured data from SurrealDB
        let app = h.proxy_router();
        let (status, body) = post_json(
            app,
            "/proxy/query",
            &json!({"query": "INFO FOR DB"}),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        // INFO FOR DB returns database metadata (response shape depends on
        // the SurrealDB version: array of results or a single object).
        assert!(
            body.is_array() || body.is_object(),
            "INFO FOR DB should return metadata, got: {body}"
        );
        assert!(!body.is_null(), "INFO FOR DB should return metadata");
    }

    #[tokio::test]
    async fn proxy_query_invalid_returns_error() {
        let h = TestHarness::new().await;

        let app = h.proxy_router();
        let (status, _) = post_json(
            app,
            "/proxy/query",
            &json!({"query": "THIS IS NOT VALID SURQL !!!"}),
        )
        .await;
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    }
}

// ===========================================================================
// Module 4: Query Tests
// ===========================================================================

mod query_tests {
    use super::*;

    #[tokio::test]
    async fn query_register_no_ssps() {
        let h = TestHarness::new().await;
        let app = h.query_router();

        let (status, _) = post_json(
            app,
            "/view/register",
            &json!({
                "id": "q1",
                "surql": "SELECT * FROM user",
                "clientId": "c1"
            }),
        )
        .await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    }

    #[tokio::test]
    async fn query_register_transport_failure() {
        let h = TestHarness::new().await;
        // SSP at unreachable URL
        h.add_ready_ssp("ssp-1", "http://127.0.0.1:1").await;

        let app = h.query_router();
        let (status, _) = post_json(
            app,
            "/view/register",
            &json!({
                "id": "q1",
                "surql": "SELECT * FROM user",
                "clientId": "c1"
            }),
        )
        .await;
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);

        // Tracker should be cleaned up
        assert!(h.query_tracker.get_assignment("q1").await.is_none());
    }

    #[tokio::test]
    async fn query_register_success() {
        let mock = MockSsp::start().await;
        let h = TestHarness::new().await;
        h.add_ready_ssp("ssp-1", &mock.addr).await;

        let app = h.query_router();
        let (status, body) = post_json(
            app,
            "/view/register",
            &json!({
                "id": "q1",
                "surql": "SELECT * FROM user",
                "clientId": "c1"
            }),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["query_id"], "q1");
        assert_eq!(body["ssp_id"], "ssp-1");

        // Mock should have received the forward
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert!(mock.received_count().await >= 1);
    }

    #[tokio::test]
    async fn query_reregister_is_sticky_and_count_stable() {
        let mock_a = MockSsp::start().await;
        let mock_b = MockSsp::start().await;
        let h = TestHarness::new().await;
        h.add_ready_ssp("ssp-0", &mock_a.addr).await;
        h.add_ready_ssp("ssp-1", &mock_b.addr).await;

        let payload = json!({
            "id": "q1",
            "surql": "SELECT * FROM user",
            "clientId": "c1"
        });
        let (status, body) = post_json(h.query_router(), "/view/register", &payload).await;
        assert_eq!(status, StatusCode::OK);
        let first_ssp = body["ssp_id"].as_str().unwrap().to_string();

        // Clients re-issue register on reconnect/keepalive. The assignment
        // must stay sticky on the owning SSP instead of round-robining onto
        // the other one (the old behavior ping-ponged ssp-0 ↔ ssp-1 every
        // few seconds under a client keepalive loop)...
        for _ in 0..3 {
            let (status, body) = post_json(h.query_router(), "/view/register", &payload).await;
            assert_eq!(status, StatusCode::OK);
            assert_eq!(body["ssp_id"].as_str().unwrap(), first_ssp);
        }

        // ...and query_count must not drift upward (the old path incremented
        // the newly selected SSP on every call without decrementing the
        // previous owner, permanently skewing least-queries balancing).
        let pool = h.ssp_pool.read().await;
        assert_eq!(pool.get(&first_ssp).unwrap().query_count, 1);
        let other = if first_ssp == "ssp-0" { "ssp-1" } else { "ssp-0" };
        assert_eq!(pool.get(other).unwrap().query_count, 0);
    }

    #[tokio::test]
    async fn query_unregister_not_found() {
        let h = TestHarness::new().await;
        let app = h.query_router();

        // Unregister is idempotent: an untracked query id (e.g. a stale
        // `_00_dbsp_cleanup` row after a scheduler restart) returns OK.
        let (status, _) = post_json(
            app,
            "/view/unregister",
            &json!({"id": "nonexistent"}),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
    }
}

// ===========================================================================
// Module 5: Metrics Tests
// ===========================================================================

mod metrics_tests {
    use super::*;

    #[tokio::test]
    async fn health_no_ssps() {
        let h = TestHarness::new().await;
        let app = h.metrics_router();

        let (status, _) = get_json(app, "/health").await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    }

    #[tokio::test]
    async fn health_with_ready_ssp() {
        let h = TestHarness::new().await;
        h.add_ready_ssp("ssp-1", "http://localhost:9999").await;

        let app = h.metrics_router();
        let (status, body) = get_json(app, "/health").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["status"], "healthy");
    }

    #[tokio::test]
    async fn health_bootstrapping_only() {
        let h = TestHarness::new().await;
        h.add_bootstrapping_ssp("ssp-1", "http://localhost:9999")
            .await;

        let app = h.metrics_router();
        let (status, _) = get_json(app, "/health").await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    }

    #[tokio::test]
    async fn metrics_returns_correct_counts() {
        let h = TestHarness::new().await;
        h.add_ready_ssp("ssp-1", "http://localhost:9991").await;
        h.add_ready_ssp("ssp-2", "http://localhost:9992").await;
        h.add_bootstrapping_ssp("ssp-3", "http://localhost:9993")
            .await;

        let app = h.metrics_router();
        let (status, body) = get_json(app, "/metrics").await;
        assert_eq!(status, StatusCode::OK);

        let scheduler = &body["scheduler"];
        assert_eq!(scheduler["total_ssps"], 3);
        assert_eq!(scheduler["ready_ssps"], 2);
    }
}

// ===========================================================================
// Module 6: Job Tests
// ===========================================================================

mod job_tests {
    use super::*;

    #[tokio::test]
    async fn job_dispatch_no_ssps() {
        let h = TestHarness::new().await;
        let app = h.job_router();

        let (status, _) = post_json(
            app,
            "/job/dispatch",
            &json!({
                "job_id": "j1",
                "table": "user",
                "payload": {"action": "compute"}
            }),
        )
        .await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    }

    #[tokio::test]
    async fn job_dispatch_transport_failure() {
        let h = TestHarness::new().await;
        h.add_ready_ssp("ssp-1", "http://127.0.0.1:1").await;

        let app = h.job_router();
        let (status, _) = post_json(
            app,
            "/job/dispatch",
            &json!({
                "job_id": "j1",
                "table": "user",
                "payload": {"action": "compute"}
            }),
        )
        .await;
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    }

    #[tokio::test]
    async fn job_result_unknown_job() {
        let h = TestHarness::new().await;
        let app = h.job_router();

        let (status, _) = post_json(
            app,
            "/job/result",
            &json!({
                "job_id": "nonexistent",
                "status": "completed",
                "result": null,
                "error": null
            }),
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn job_result_completes_job() {
        let h = TestHarness::new().await;

        // Pre-assign job in tracker
        h.job_tracker
            .assign("j1".to_string(), "ssp-1".to_string())
            .await;

        let app = h.job_router();
        let (status, _) = post_json(
            app,
            "/job/result",
            &json!({
                "job_id": "j1",
                "status": "completed",
                "result": {"data": 42},
                "error": null
            }),
        )
        .await;
        assert_eq!(status, StatusCode::OK);

        // Job should be removed from tracker
        assert!(h.job_tracker.get_assignment("j1").await.is_none());
    }
}

// ===========================================================================
// Module 7: Bootstrap Protocol Tests (cross-cutting)
// ===========================================================================

mod bootstrap_protocol_tests {
    use super::*;

    #[tokio::test]
    async fn ingest_then_register_flow() {
        let h = TestHarness::new().await;

        // Ingest 3 events
        for i in 0..3 {
            let app = h.ingest_router();
            let (status, _) = post_json(
                app,
                "/ingest",
                &ingest_payload("user", "CREATE", &format!("u{}", i)),
            )
            .await;
            assert_eq!(status, StatusCode::OK);
        }
        assert_eq!(h.seq_counter.load(Ordering::SeqCst), 3);

        // Register SSP (this triggers SnapshotFrozen)
        let app = h.ssp_router();
        let (status, _) = post_json(
            app,
            "/ssp/register",
            &json!({"ssp_id": "ssp-1", "url": "http://localhost:9999", "version": "test"}),
        )
        .await;
        assert_eq!(status, StatusCode::ACCEPTED);

        // Ingest 1 more event (should buffer for bootstrapping SSP)
        let app = h.ingest_router();
        let (status, _) =
            post_json(app, "/ingest", &ingest_payload("user", "CREATE", "u3")).await;
        assert_eq!(status, StatusCode::OK);

        // seq_counter should be 4
        assert_eq!(h.seq_counter.load(Ordering::SeqCst), 4);
        h.fanout.idle().await;

        // The new event should be buffered for the bootstrapping SSP
        let pool = h.ssp_pool.read().await;
        assert!(pool.buffer_size("ssp-1") >= 1);
    }

    #[tokio::test]
    async fn status_state_machine() {
        let h = TestHarness::with_status(SchedulerStatus::Cloning).await;

        // Cloning → Ready
        h.set_status(SchedulerStatus::Ready).await;
        assert_eq!(*h.status.read().await, SchedulerStatus::Ready);

        // Ready → SnapshotFrozen (via SSP registration)
        let app = h.ssp_router();
        let (status, _) = post_json(
            app,
            "/ssp/register",
            &json!({"ssp_id": "ssp-1", "url": "http://localhost:9999", "version": "test"}),
        )
        .await;
        assert_eq!(status, StatusCode::ACCEPTED);
        assert_eq!(*h.status.read().await, SchedulerStatus::SnapshotFrozen);
    }

    #[tokio::test]
    async fn concurrent_ingest_seq_uniqueness() {
        let h = TestHarness::new().await;

        // Launch 20 concurrent ingests
        let mut handles = Vec::new();
        for i in 0..20 {
            let ingest_state = IngestState {
                replica: Arc::clone(&h.replica),
                transport: Arc::clone(&h.transport),
                ssp_pool: Arc::clone(&h.ssp_pool),
                status: Arc::clone(&h.status),
                event_buffer: Arc::clone(&h.event_buffer),
                seq_counter: Arc::clone(&h.seq_counter),
                wal: Arc::clone(&h.wal),
                drain_lock: Arc::clone(&h.drain_lock),
                db_config: Arc::new(h.config.db.clone()),
                job_tables: Arc::new(vec![]),
                observer_permits: Arc::new(tokio::sync::Semaphore::new(8)),
                snapshot_seq: Arc::clone(&h.snapshot_seq_cell),
                fanout: Arc::clone(&h.fanout),
            };
            let app = ingest::create_ingest_router(ingest_state);

            handles.push(tokio::spawn(async move {
                let payload = ingest_payload("user", "CREATE", &format!("u{}", i));
                let (status, _) = post_json(app, "/ingest", &payload).await;
                assert_eq!(status, StatusCode::OK);
            }));
        }

        for handle in handles {
            handle.await.unwrap();
        }

        // All 20 seqs should be unique
        assert_eq!(h.seq_counter.load(Ordering::SeqCst), 20);

        let buffer = h.event_buffer.read().await;
        assert_eq!(buffer.len(), 20);

        let mut seqs: Vec<u64> = buffer.iter().map(|e| e.seq).collect();
        seqs.sort();
        seqs.dedup();
        assert_eq!(seqs.len(), 20, "All 20 sequence numbers should be unique");
    }

    #[tokio::test]
    async fn full_app_all_routes_reachable() {
        let h = TestHarness::new().await;
        h.add_ready_ssp("ssp-1", "http://localhost:9999").await;

        // Pre-populate tracker state so unregister/result endpoints
        // return handler errors (not route-level 404)
        h.query_tracker
            .assign("q-pre".to_string(), "ssp-1".to_string())
            .await;
        h.job_tracker
            .assign("j-pre".to_string(), "ssp-1".to_string())
            .await;

        // Verify no endpoint returns 404 (other status codes are expected)
        let endpoints: Vec<(&str, &str, Value)> = vec![
            ("POST", "/ingest", ingest_payload("user", "CREATE", "u1")),
            (
                "POST",
                "/ssp/register",
                json!({"ssp_id": "s1", "url": "http://localhost:1234", "version": "test"}),
            ),
            (
                "POST",
                "/ssp/heartbeat",
                json!({"ssp_id": "ssp-1", "timestamp": 0, "active_queries": 0}),
            ),
            ("POST", "/proxy/signin", json!({})),
            ("POST", "/proxy/use", json!({})),
            (
                "POST",
                "/proxy/query",
                json!({"query": "SELECT * FROM user"}),
            ),
            (
                "POST",
                "/view/register",
                json!({"id": "q1", "surql": "SELECT * FROM user", "clientId": "c1"}),
            ),
            (
                "POST",
                "/view/unregister",
                json!({"id": "q-pre"}),
            ),
            (
                "POST",
                "/job/dispatch",
                json!({"job_id": "j1", "table": "user", "payload": {}}),
            ),
            (
                "POST",
                "/job/result",
                json!({"job_id": "j-pre", "status": "completed"}),
            ),
        ];

        for (method, path, body) in endpoints {
            // Rebuild app for each request (oneshot consumes router)
            let app = h.full_app();
            let request = Request::builder()
                .method(method)
                .uri(path)
                .header("content-type", "application/json")
                .body(axum::body::Body::from(serde_json::to_vec(&body).unwrap()))
                .unwrap();

            let response = app.oneshot(request).await.unwrap();
            assert_ne!(
                response.status(),
                StatusCode::NOT_FOUND,
                "{} {} should not return 404",
                method,
                path
            );
        }

        // GET endpoints
        for path in &["/health", "/metrics"] {
            let app = h.full_app();
            let request = Request::builder()
                .method("GET")
                .uri(*path)
                .body(axum::body::Body::empty())
                .unwrap();

            let response = app.oneshot(request).await.unwrap();
            assert_ne!(
                response.status(),
                StatusCode::NOT_FOUND,
                "GET {} should not return 404",
                path
            );
        }
    }

    #[tokio::test]
    async fn updater_tick_recovers_latched_frozen_and_drains() {
        // The live-incident latch: SnapshotFrozen with no active bootstrap
        // (failed bootstrap whose error handler never ran). The tick must
        // self-recover to Ready and drain the pinned backlog.
        let h = TestHarness::with_status(SchedulerStatus::SnapshotFrozen).await;

        for i in 0..3 {
            let app = h.ingest_router();
            let (status, _) = post_json(
                app,
                "/ingest",
                &ingest_payload("user", "CREATE", &format!("u{}", i)),
            )
            .await;
            assert_eq!(status, StatusCode::OK);
        }
        assert_eq!(h.event_buffer.read().await.len(), 3);

        scheduler::snapshot_updater_tick(
            &h.status,
            &h.event_buffer,
            &h.replica,
            &h.ssp_pool,
            &h.wal,
            &h.drain_lock,
            std::time::Duration::from_secs(300),
            None,
        )
        .await;

        assert_eq!(*h.status.read().await, SchedulerStatus::Ready);
        assert!(h.event_buffer.read().await.is_empty(), "backlog drained");
        let rep = h.replica.read().await;
        assert_eq!(rep.snapshot_seq(), 3, "snapshot advanced");
        drop(rep);
        // WAL truncated up to the drained seq — a restart must not refill.
        assert!(h.wal.read().await.recover().unwrap().is_empty());
    }

    #[tokio::test]
    async fn updater_tick_recovers_latched_updating() {
        // A prior tick that died mid-drain pins SnapshotUpdating.
        let h = TestHarness::with_status(SchedulerStatus::SnapshotUpdating).await;
        scheduler::snapshot_updater_tick(
            &h.status,
            &h.event_buffer,
            &h.replica,
            &h.ssp_pool,
            &h.wal,
            &h.drain_lock,
            std::time::Duration::from_secs(300),
            None,
        )
        .await;
        assert_eq!(*h.status.read().await, SchedulerStatus::Ready);
    }

    // An idle tick must not advertise a drain it is not doing.
    //
    // `/health` derives `stalled` straight from this status, so flipping it
    // unconditionally every `snapshot_update_interval_secs` reported the whole
    // cluster "degraded" once per interval, forever, for a tick with no work.
    #[tokio::test]
    async fn updater_tick_stays_ready_with_an_empty_buffer() {
        let h = TestHarness::with_status(SchedulerStatus::Ready).await;
        assert!(h.event_buffer.read().await.is_empty(), "precondition: nothing buffered");

        let status = Arc::clone(&h.status);
        let seen = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let seen_writer = Arc::clone(&seen);
        // Sample the status while the tick runs: the assertion after it returns
        // would pass even if the tick had flipped to Updating and back.
        let watcher = tokio::spawn(async move {
            for _ in 0..200 {
                if *status.read().await == SchedulerStatus::SnapshotUpdating {
                    seen_writer.store(true, std::sync::atomic::Ordering::SeqCst);
                    return;
                }
                tokio::task::yield_now().await;
            }
        });

        scheduler::snapshot_updater_tick(
            &h.status,
            &h.event_buffer,
            &h.replica,
            &h.ssp_pool,
            &h.wal,
            &h.drain_lock,
            std::time::Duration::from_secs(300),
            None,
        )
        .await;
        watcher.abort();

        assert!(
            !seen.load(std::sync::atomic::Ordering::SeqCst),
            "an empty-buffer tick must never enter SnapshotUpdating"
        );
        assert_eq!(*h.status.read().await, SchedulerStatus::Ready);
    }

    #[tokio::test]
    async fn updater_tick_noop_with_fresh_active_bootstrap() {
        let h = TestHarness::with_status(SchedulerStatus::SnapshotFrozen).await;
        h.add_bootstrapping_ssp("ssp-1", "http://localhost:9999").await;

        let app = h.ingest_router();
        let (status, _) =
            post_json(app, "/ingest", &ingest_payload("user", "CREATE", "u1")).await;
        assert_eq!(status, StatusCode::OK);

        scheduler::snapshot_updater_tick(
            &h.status,
            &h.event_buffer,
            &h.replica,
            &h.ssp_pool,
            &h.wal,
            &h.drain_lock,
            std::time::Duration::from_secs(300),
            None,
        )
        .await;

        // Bootstrap in flight: no recovery, no drain.
        assert_eq!(*h.status.read().await, SchedulerStatus::SnapshotFrozen);
        assert_eq!(h.event_buffer.read().await.len(), 1);
    }

    #[tokio::test]
    async fn updater_tick_evicts_stale_bootstrap_then_recovers() {
        let h = TestHarness::with_status(SchedulerStatus::SnapshotFrozen).await;
        h.add_bootstrapping_ssp("ssp-parked", "http://localhost:9999").await;
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;

        scheduler::snapshot_updater_tick(
            &h.status,
            &h.event_buffer,
            &h.replica,
            &h.ssp_pool,
            &h.wal,
            &h.drain_lock,
            std::time::Duration::ZERO, // everything parked is instantly stale
            None,
        )
        .await;

        let pool = h.ssp_pool.read().await;
        assert!(pool.get("ssp-parked").is_none(), "parked SSP evicted");
        assert!(!pool.has_active_bootstrap());
        drop(pool);
        assert_eq!(*h.status.read().await, SchedulerStatus::Ready);
    }

    #[tokio::test]
    async fn register_drains_pending_events_and_hands_fresh_hashes() {
        let h = TestHarness::new().await;

        for i in 0..3 {
            let app = h.ingest_router();
            let (status, _) = post_json(
                app,
                "/ingest",
                &ingest_payload("user", "CREATE", &format!("u{}", i)),
            )
            .await;
            assert_eq!(status, StatusCode::OK);
        }
        assert_eq!(h.event_buffer.read().await.len(), 3);

        let app = h.ssp_router();
        let (status, body) = post_json(
            app,
            "/ssp/register",
            &json!({"ssp_id": "ssp-1", "url": "http://localhost:9999", "version": "test"}),
        )
        .await;
        assert_eq!(status, StatusCode::ACCEPTED);

        // The backlog was applied before hashes were captured...
        assert!(h.event_buffer.read().await.is_empty());
        let rep = h.replica.read().await;
        assert_eq!(rep.snapshot_seq(), 3);
        assert_eq!(body["snapshot_seq"].as_u64(), Some(3));

        // ...so the handed-out hash matches the replica content the SSP will
        // bootstrap from — the invariant whose violation crash-loops SSPs.
        let fresh = rep.compute_table_hashes().await.unwrap();
        assert_eq!(
            body["table_hashes"]["user"].as_str(),
            fresh.get("user").map(|s| s.as_str()),
            "registration hash must match replica content"
        );
    }

    /// The drain used to rehash every touched table from content; on whitepawn
    /// that paged the 408k-row `analysis` table out of RocksDB every five
    /// minutes and pinned the scheduler's whole CPU quota for minutes. Hashes
    /// are now folded per event in `Replica::apply` (XOR set-hash), so a drain
    /// rehashes nothing unless an event could not be folded. The fold must
    /// agree with a from-content rehash for every operation.
    #[tokio::test]
    async fn incremental_table_hash_agrees_with_a_full_rehash() {
        use scheduler::replica::RecordOp;
        let h = TestHarness::new().await;
        let mut rep = h.replica.write().await;

        // A table first seen through an event has no accumulator: it is
        // flagged for a from-content hash, which the drain computes once.
        rep.apply("user", RecordOp::Create, "user:u1", Some(json!({"name": "a", "_00_rv": 1})))
            .await
            .unwrap();
        assert!(rep.dirty_tables().contains("user"));
        rep.set_snapshot_state(1, Some(&std::collections::BTreeSet::new())).await.unwrap();
        assert!(rep.dirty_tables().is_empty());
        let after_create = rep.snapshot_hashes()["user"].clone();
        assert!(after_create.starts_with("x3:"), "got {after_create}");

        // From here every event is folded, never rehashed: a create, an
        // update carrying a reserved key and a null optional (both stripped
        // by the digest), a delete, another create.
        rep.apply("user", RecordOp::Create, "user:u2", Some(json!({"name": "b", "age": 2, "_00_rv": 1})))
            .await
            .unwrap();
        rep.apply("user", RecordOp::Update, "user:u1", Some(json!({"name": "a2", "nick": null, "_00_rv": 2})))
            .await
            .unwrap();
        rep.apply("user", RecordOp::Delete, "user:u2", None).await.unwrap();
        rep.apply("user", RecordOp::Create, "user:u3", Some(json!({"name": "c", "_00_rv": 1})))
            .await
            .unwrap();
        assert!(rep.dirty_tables().is_empty(), "no event needed a from-content rehash");

        let touched: std::collections::BTreeSet<String> = ["user".to_string()].into_iter().collect();
        rep.set_snapshot_state(2, Some(&touched)).await.unwrap();
        let folded = rep.snapshot_hashes()["user"].clone();
        let full = rep.compute_table_hashes().await.unwrap()["user"].clone();
        assert_eq!(folded, full, "folded hash must equal a from-content rehash");
        assert_ne!(folded, after_create);

        // Deleting everything folds back to the empty-table hash.
        rep.apply("user", RecordOp::Delete, "user:u1", None).await.unwrap();
        rep.apply("user", RecordOp::Delete, "user:u3", None).await.unwrap();
        assert_eq!(
            rep.snapshot_hashes()["user"],
            ssp_protocol::snapshot_hash::xor_empty_table_hash()
        );
    }

    #[tokio::test]
    async fn register_skips_drain_while_sibling_bootstraps() {
        let h = TestHarness::new().await;
        h.add_bootstrapping_ssp("ssp-0", "http://localhost:9990").await;

        for i in 0..3 {
            let app = h.ingest_router();
            let (status, _) = post_json(
                app,
                "/ingest",
                &ingest_payload("user", "CREATE", &format!("u{}", i)),
            )
            .await;
            assert_eq!(status, StatusCode::OK);
        }

        let app = h.ssp_router();
        let (status, _) = post_json(
            app,
            "/ssp/register",
            &json!({"ssp_id": "ssp-1", "url": "http://localhost:9991", "version": "test"}),
        )
        .await;
        assert_eq!(status, StatusCode::ACCEPTED);

        // Draining now would invalidate the hashes ssp-0 already holds.
        assert_eq!(h.event_buffer.read().await.len(), 3, "backlog must survive");
    }

    #[tokio::test]
    async fn register_bumps_registration_generation() {
        let h = TestHarness::new().await;
        for _ in 0..2 {
            let app = h.ssp_router();
            let (status, _) = post_json(
                app,
                "/ssp/register",
                &json!({"ssp_id": "ssp-1", "url": "http://localhost:9999", "version": "test"}),
            )
            .await;
            assert_eq!(status, StatusCode::ACCEPTED);
        }
        assert_eq!(h.ssp_pool.read().await.registration_gen("ssp-1"), 2);
    }

    #[tokio::test]
    async fn bootstrap_failure_unfreezes_snapshot() {
        let h = TestHarness::new().await;
        let mock = MockSsp::start_with_health("failed").await;

        let app = h.ssp_router();
        let (status, _) = post_json(
            app,
            "/ssp/register",
            &json!({"ssp_id": "ssp-fail", "url": mock.addr, "version": "test"}),
        )
        .await;
        assert_eq!(status, StatusCode::ACCEPTED);
        assert_eq!(*h.status.read().await, SchedulerStatus::SnapshotFrozen);

        // Poll task (100ms interval) sees "failed", bails, and the error
        // handler must remove the SSP AND unfreeze — the old code left the
        // status latched forever here.
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(3);
        loop {
            let unfrozen = *h.status.read().await == SchedulerStatus::Ready;
            let removed = h.ssp_pool.read().await.get("ssp-fail").is_none();
            if unfrozen && removed {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "bootstrap failure did not unfreeze the snapshot (status={:?})",
                *h.status.read().await
            );
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
    }

    #[tokio::test]
    async fn admin_resync_rehash_repairs_hash_drift() {
        let h = TestHarness::new().await;

        // Baseline content + persisted hashes, then drift the content
        // WITHOUT rehashing — exactly the stale-metadata corruption.
        {
            let mut rep = h.replica.write().await;
            rep.apply(
                "user",
                scheduler::replica::RecordOp::Create,
                "user:u1",
                Some(json!({"name": "a"})),
            )
            .await
            .unwrap();
            rep.set_snapshot_state(1, None).await.unwrap();
            // Past `apply`: an event folds its own delta into the hash now, so
            // stale metadata has to be produced the way it happens in the
            // wild — content changing underneath the cached hash.
            rep.query("UPDATE user:u1 SET name = 'b'").await.unwrap();
        }
        {
            let rep = h.replica.read().await;
            assert_ne!(
                rep.snapshot_hashes(),
                &rep.compute_table_hashes().await.unwrap(),
                "test setup: hashes must be stale"
            );
        }

        h.add_ready_ssp("ssp-1", "http://localhost:9999").await;
        let app = h.ssp_router();
        let (status, body) = post_json(app, "/admin/resync", &json!({"mode": "rehash"})).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["mode"], "rehash");
        assert_eq!(body["marked_for_resync"], 1);

        let rep = h.replica.read().await;
        assert_eq!(
            rep.snapshot_hashes(),
            &rep.compute_table_hashes().await.unwrap(),
            "hashes repaired from content"
        );
    }

    #[tokio::test]
    async fn admin_resync_reclone_conflicts_while_running() {
        let h = TestHarness::new().await;
        let _guard = h.reclone_lock.lock().await; // simulate in-flight re-clone

        let app = h.ssp_router();
        let (status, _) = post_json(app, "/admin/resync", &json!({"mode": "reclone"})).await;
        assert_eq!(status, StatusCode::CONFLICT);
    }

    #[tokio::test]
    async fn admin_resync_rejects_bad_mode_and_cloning() {
        let h = TestHarness::new().await;
        let app = h.ssp_router();
        let (status, _) = post_json(app, "/admin/resync", &json!({"mode": "nuke"})).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);

        let h = TestHarness::with_status(SchedulerStatus::Cloning).await;
        let app = h.ssp_router();
        let (status, _) = post_json(app, "/admin/resync", &json!({"mode": "rehash"})).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    }

    #[tokio::test]
    async fn health_reports_latched_stall_as_degraded() {
        let h = TestHarness::with_status(SchedulerStatus::SnapshotFrozen).await;
        h.add_ready_ssp("ssp-1", "http://localhost:9999").await;

        let (status, body) = get_json(h.metrics_router(), "/health").await;
        // Degraded, NOT 503: an orchestrator restart provably doesn't fix a
        // latched freeze (the WAL refills), and the updater self-heals.
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["status"], "degraded");
        assert_eq!(body["scheduler"]["status"], "frozen");
        assert_eq!(body["scheduler"]["stalled"], true);

        // A justified freeze (bootstrap in flight) is not a stall.
        h.add_bootstrapping_ssp("ssp-2", "http://localhost:9998").await;
        let (status, body) = get_json(h.metrics_router(), "/health").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["scheduler"]["stalled"], false);
    }

    #[tokio::test]
    async fn pre_backup_skips_drain_during_bootstrap() {
        use maintenance::MaintenanceHost;

        let h = TestHarness::with_status(SchedulerStatus::SnapshotFrozen).await;
        h.add_bootstrapping_ssp("ssp-1", "http://localhost:9999").await;

        let app = h.ingest_router();
        let (status, _) =
            post_json(app, "/ingest", &ingest_payload("user", "CREATE", "u1")).await;
        assert_eq!(status, StatusCode::OK);

        let host = scheduler::maintenance_host::SchedulerHost {
            ingest: IngestState {
                replica: Arc::clone(&h.replica),
                transport: Arc::clone(&h.transport),
                ssp_pool: Arc::clone(&h.ssp_pool),
                status: Arc::clone(&h.status),
                event_buffer: Arc::clone(&h.event_buffer),
                seq_counter: Arc::clone(&h.seq_counter),
                wal: Arc::clone(&h.wal),
                drain_lock: Arc::clone(&h.drain_lock),
                db_config: Arc::new(h.config.db.clone()),
                job_tables: Arc::new(vec![]),
                observer_permits: Arc::new(tokio::sync::Semaphore::new(8)),
                snapshot_seq: Arc::clone(&h.snapshot_seq_cell),
                fanout: Arc::clone(&h.fanout),
            },
        };
        let seq = host.pre_backup().await.unwrap();
        assert_eq!(seq, Some(0), "returns current seq without draining");
        assert_eq!(
            h.event_buffer.read().await.len(),
            1,
            "must not mutate the replica mid-bootstrap"
        );
    }

    /// The crash the boot path has to survive: the previous process applied
    /// a batch to the replica (rows on disk, hash folded in memory) and died
    /// before `set_snapshot_state` persisted the new seq + hashes. On
    /// restart the WAL still holds that batch past the persisted seq, and
    /// re-folding an already-applied UPDATE ("before out, after in" against a
    /// row that already carries the change) would leave the persisted hash
    /// wrong for good. The boot marks the backlog's tables dirty instead, and
    /// the first drain rehashes them from content. No boot-time full rehash.
    #[tokio::test]
    async fn wal_backlog_tables_are_rehashed_from_content_at_the_first_drain() {
        let replica_dir = TempDir::new().unwrap();
        let wal_dir = TempDir::new().unwrap();
        let config = SchedulerConfig {
            replica_db_path: replica_dir.path().join("replica_db"),
            wal_path: wal_dir.path().join("event_wal.log"),
            scheduler_id: "test-scheduler".to_string(),
            ..SchedulerConfig::default()
        };
        let update = BufferedEvent {
            seq: 2,
            update: scheduler::messages::RecordUpdate {
                table: "user".to_string(),
                operation: scheduler::messages::RecordOp::Update,
                record_id: "user:u1".to_string(),
                data: Some(json!({"name": "b"})),
                version: 2,
            },
            received_at: 0,
        };

        let sched = scheduler::Scheduler::new(config, Arc::new(HttpTransport::new()))
            .await
            .unwrap();

        // "Process 1": commit seq 1, then apply seq 2 and crash before
        // committing it.
        {
            let mut rep = sched.replica.write().await;
            rep.apply(
                "user",
                scheduler::replica::RecordOp::Create,
                "user:u1",
                Some(json!({"name": "a"})),
            )
            .await
            .unwrap();
            rep.set_snapshot_state(1, None).await.unwrap();
            sched.wal.write().await.append(&update).unwrap();
            // What a drain does: marker, then rows, then (not reached) commit.
            rep.begin_apply().await.unwrap();
            rep.apply(
                "user",
                scheduler::replica::RecordOp::Update,
                "user:u1",
                Some(json!({"name": "b"})),
            )
            .await
            .unwrap();
            // No commit_snapshot_state: the persisted hash still describes
            // name=a and the marker is still set.
        }

        // "Process 2" on the same RocksDB handle (RocksDB releases its LOCK
        // lazily, so a real reopen in-process races): reload the persisted
        // metadata the way a fresh `Replica::new` reads it, then run the
        // boot recovery.
        {
            let mut rep = sched.replica.write().await;
            assert_eq!(rep.reload_snapshot_seq().await.unwrap(), 1);
            assert!(rep.interrupted_apply(), "the marker survived the crash");
            assert!(rep.dirty_tables().is_empty(), "fresh process: nothing flagged yet");
            assert_ne!(
                rep.snapshot_hashes()["user"],
                rep.compute_table_hashes().await.unwrap()["user"],
                "the persisted hash is stale (describes name=a)"
            );

            let wal = sched.wal.read().await;
            let (backlog, initial_seq) =
                scheduler::recover_wal_backlog(&mut rep, &wal).unwrap();
            assert_eq!(backlog.len(), 1, "seq 2 recovered as backlog");
            assert_eq!(initial_seq, 2);
            assert!(
                rep.dirty_tables().contains("user"),
                "backlog table flagged for a content rehash"
            );
            *sched.event_buffer.write().await = backlog;
        }

        let applied = scheduler::drain_and_apply(&sched.event_buffer, &sched.replica, &sched.wal)
            .await
            .unwrap();
        assert_eq!(applied, 1);

        let rep = sched.replica.read().await;
        assert!(rep.dirty_tables().is_empty(), "the drain cleared the flag");
        assert!(!rep.interrupted_apply(), "the commit cleared the marker");
        assert_eq!(rep.snapshot_seq(), 2);
        assert_eq!(
            rep.snapshot_hashes(),
            &rep.compute_table_hashes().await.unwrap(),
            "hashes agree with content after the first drain"
        );
    }

    /// The normal restart: the previous process stopped between two drains,
    /// so the WAL backlog was buffered but never applied. Nothing is dirty
    /// and the first drain folds the backlog like any batch — no rehash.
    #[tokio::test]
    async fn a_never_applied_wal_backlog_folds_at_the_first_drain() {
        let replica_dir = TempDir::new().unwrap();
        let wal_dir = TempDir::new().unwrap();
        let config = SchedulerConfig {
            replica_db_path: replica_dir.path().join("replica_db"),
            wal_path: wal_dir.path().join("event_wal.log"),
            scheduler_id: "test-scheduler".to_string(),
            ..SchedulerConfig::default()
        };
        let sched = scheduler::Scheduler::new(config, Arc::new(HttpTransport::new()))
            .await
            .unwrap();
        {
            let mut rep = sched.replica.write().await;
            rep.apply(
                "user",
                scheduler::replica::RecordOp::Create,
                "user:u1",
                Some(json!({"name": "a"})),
            )
            .await
            .unwrap();
            rep.set_snapshot_state(1, None).await.unwrap();
            sched
                .wal
                .write()
                .await
                .append(&BufferedEvent {
                    seq: 2,
                    update: scheduler::messages::RecordUpdate {
                        table: "user".to_string(),
                        operation: scheduler::messages::RecordOp::Update,
                        record_id: "user:u1".to_string(),
                        data: Some(json!({"name": "b"})),
                        version: 2,
                    },
                    received_at: 0,
                })
                .unwrap();
            // Stopped here: seq 2 is in the WAL, not in the rows.
        }

        {
            let mut rep = sched.replica.write().await;
            assert_eq!(rep.reload_snapshot_seq().await.unwrap(), 1);
            assert!(!rep.interrupted_apply());
            let wal = sched.wal.read().await;
            let (backlog, initial_seq) = scheduler::recover_wal_backlog(&mut rep, &wal).unwrap();
            assert_eq!((backlog.len(), initial_seq), (1, 2));
            assert!(rep.dirty_tables().is_empty(), "no crash inside a drain: nothing to rehash");
            *sched.event_buffer.write().await = backlog;
        }

        let applied = scheduler::drain_and_apply(&sched.event_buffer, &sched.replica, &sched.wal)
            .await
            .unwrap();
        assert_eq!(applied, 1);
        let rep = sched.replica.read().await;
        assert_eq!(rep.snapshot_seq(), 2);
        assert!(rep.dirty_tables().is_empty());
        assert_eq!(
            rep.snapshot_hashes(),
            &rep.compute_table_hashes().await.unwrap(),
            "the folded hash agrees with content"
        );
    }

    /// A drain with nothing to apply must still rehash dirty tables: the
    /// persisted hash is wrong until then, and a registration would hand it
    /// out to the SSP.
    #[tokio::test]
    async fn an_empty_drain_still_rehashes_dirty_tables() {
        let replica_dir = TempDir::new().unwrap();
        let wal_dir = TempDir::new().unwrap();
        let config = SchedulerConfig {
            replica_db_path: replica_dir.path().join("replica_db"),
            wal_path: wal_dir.path().join("event_wal.log"),
            scheduler_id: "test-scheduler".to_string(),
            ..SchedulerConfig::default()
        };
        let sched = scheduler::Scheduler::new(config, Arc::new(HttpTransport::new()))
            .await
            .unwrap();
        {
            let mut rep = sched.replica.write().await;
            rep.apply(
                "user",
                scheduler::replica::RecordOp::Create,
                "user:u1",
                Some(json!({"name": "a"})),
            )
            .await
            .unwrap();
            rep.set_snapshot_state(3, None).await.unwrap();
            rep.query("UPDATE user:u1 SET name = 'b'").await.unwrap();
            rep.mark_tables_dirty(["user".to_string()]);
            assert_ne!(
                rep.snapshot_hashes()["user"],
                rep.compute_table_hashes().await.unwrap()["user"]
            );
        }

        let applied = scheduler::drain_and_apply(&sched.event_buffer, &sched.replica, &sched.wal)
            .await
            .unwrap();
        assert_eq!(applied, 0, "no events were applied");
        let rep = sched.replica.read().await;
        assert_eq!(rep.snapshot_seq(), 3, "the seq does not move");
        assert!(rep.dirty_tables().is_empty());
        assert_eq!(
            rep.snapshot_hashes(),
            &rep.compute_table_hashes().await.unwrap(),
            "the dirty table was rehashed from content"
        );
    }

    /// `startup_integrity_check` is no longer on the boot path (it paged
    /// every table out of RocksDB on every start); it stays available as an
    /// explicit full check and must still repair when called.
    #[tokio::test]
    async fn startup_integrity_check_repairs_stale_hashes_when_invoked() {
        let replica_dir = TempDir::new().unwrap();
        let wal_dir = TempDir::new().unwrap();
        let config = SchedulerConfig {
            replica_db_path: replica_dir.path().join("replica_db"),
            wal_path: wal_dir.path().join("event_wal.log"),
            scheduler_id: "test-scheduler".to_string(),
            ..SchedulerConfig::default()
        };
        let sched = scheduler::Scheduler::new(config, Arc::new(HttpTransport::new()))
            .await
            .unwrap();

        {
            let mut rep = sched.replica.write().await;
            rep.apply(
                "user",
                scheduler::replica::RecordOp::Create,
                "user:u1",
                Some(json!({"name": "a"})),
            )
            .await
            .unwrap();
            rep.set_snapshot_state(1, None).await.unwrap();
            // Content changes underneath the replica (manual edit, bad backup):
            // nothing folds, the persisted hash is now wrong.
            rep.query("UPDATE user:u1 SET name = 'b'").await.unwrap();
            assert_ne!(
                rep.snapshot_hashes()["user"],
                rep.compute_table_hashes().await.unwrap()["user"]
            );
        }

        sched.startup_integrity_check().await.unwrap();

        let rep = sched.replica.read().await;
        assert_eq!(
            rep.snapshot_hashes(),
            &rep.compute_table_hashes().await.unwrap(),
            "the check must repair, not just log"
        );
    }

    #[tokio::test]
    async fn snapshot_update_skips_during_bootstrap() {
        let h = TestHarness::new().await;

        // Register an SSP to trigger bootstrap
        let app = h.ssp_router();
        let (status, _) = post_json(
            app,
            "/ssp/register",
            &json!({"ssp_id": "ssp-1", "url": "http://localhost:9999", "version": "test"}),
        )
        .await;
        assert_eq!(status, StatusCode::ACCEPTED);

        // Verify has_active_bootstrap() is true
        let pool = h.ssp_pool.read().await;
        assert!(
            pool.has_active_bootstrap(),
            "Should have active bootstrap after registration"
        );

        // The snapshot updater would skip because of this flag
        // (we don't actually run the updater, just verify the condition)
        drop(pool);

        let current_status = *h.status.read().await;
        assert_eq!(
            current_status,
            SchedulerStatus::SnapshotFrozen,
            "Status should be frozen during bootstrap"
        );
    }

    /// Regression for the 2026-08-08 wedge: while a writer holds the replica
    /// write lock for a long time (drain/rehash/reclone), every probe endpoint
    /// and /ingest must still answer promptly. Before the fix, /health parked
    /// on `replica.read()` inside `pending_events_snapshot` (holding the
    /// backend_health read guard across the await), and one queued writer
    /// then convoyed every reader — total HTTP silence.
    #[tokio::test]
    async fn probes_answer_while_replica_write_lock_held() {
        let h = TestHarness::new().await;
        h.add_ready_ssp("ssp-1", "http://localhost:1").await;

        // Hold the replica write lock like a long drain would.
        let replica = Arc::clone(&h.replica);
        let (locked_tx, locked_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = tokio::sync::oneshot::channel::<()>();
        let holder = tokio::spawn(async move {
            let _guard = replica.write().await;
            let _ = locked_tx.send(());
            let _ = release_rx.await;
        });
        locked_rx.await.expect("lock holder started");

        let budget = std::time::Duration::from_secs(1);
        for path in ["/health", "/health/live", "/health/ready", "/metrics", "/info/text"] {
            let app = h.metrics_router();
            let res = tokio::time::timeout(budget, get_json(app, path)).await;
            assert!(
                res.is_ok(),
                "{path} must answer within {budget:?} while the replica write lock is held"
            );
        }

        // /ingest must also stay live (it never needs the replica lock).
        let app = h.ingest_router();
        let payload = ingest_payload("user", "CREATE", "wedge1");
        let res = tokio::time::timeout(budget, post_json(app, "/ingest", &payload)).await;
        let (status, _) = res.expect("/ingest must answer while replica write lock is held");
        assert_eq!(status, StatusCode::OK);

        let _ = release_tx.send(());
        let _ = holder.await;
    }

    /// The chunked drain (write guard released between chunks, hashing under
    /// a read guard) must land on exactly the same state as the old
    /// single-guard drain: every event applied, seq advanced, and the cached
    /// hashes equal to a fresh full recompute of the replica content.
    #[tokio::test]
    async fn chunked_drain_matches_full_recompute() {
        let h = TestHarness::new().await;

        // >1 chunk (chunk size 256) spread over three tables.
        for i in 0..300 {
            let table = match i % 3 {
                0 => "user",
                1 => "game",
                _ => "comment",
            };
            let app = h.ingest_router();
            let (status, _) = post_json(
                app,
                "/ingest",
                &ingest_payload(table, "CREATE", &format!("rec{i}")),
            )
            .await;
            assert_eq!(status, StatusCode::OK);
        }
        assert_eq!(h.event_buffer.read().await.len(), 300);

        let applied = {
            let _drain = h.drain_lock.lock().await;
            scheduler::drain_and_apply(&h.event_buffer, &h.replica, &h.wal).await.unwrap()
        };
        assert_eq!(applied, 300);

        let rep = h.replica.read().await;
        assert_eq!(rep.snapshot_seq(), 300);
        assert_eq!(
            rep.snapshot_hashes(),
            &rep.compute_table_hashes().await.unwrap(),
            "chunked drain must leave cached hashes equal to content"
        );
    }
}


// ── Replica drift detection ───────────────────────────────────────────────
//
// The replica is fed by ingest events only; rows written upstream while nothing
// was listening never reach it, and every SSP that bootstraps from it inherits
// the gap while every hash-based check passes (an empty table hashes the same
// on both sides). The drift check compares row counts against upstream and
// re-clones. These tests drive it with a stubbed upstream and a recording
// recloner; the decision table itself is unit-tested in `drift.rs`.
mod drift_tests {
    use super::*;
    use scheduler::drift::{self, Action, DriftConfig, DriftHook, DriftState, Recloner, UpstreamCounts};
    use std::collections::BTreeMap;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct StubUpstream(BTreeMap<String, Option<u64>>);

    #[async_trait::async_trait]
    impl UpstreamCounts for StubUpstream {
        async fn upstream_counts(&self) -> anyhow::Result<BTreeMap<String, Option<u64>>> {
            Ok(self.0.clone())
        }
    }

    struct RecordingRecloner {
        calls: AtomicUsize,
        ssp_pool: Arc<RwLock<SspPool>>,
    }

    #[async_trait::async_trait]
    impl Recloner for RecordingRecloner {
        async fn reclone_and_resync(&self) -> anyhow::Result<bool> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.ssp_pool.write().await.mark_all_for_resync();
            Ok(true)
        }
    }

    fn hook(h: &TestHarness, upstream: &[(&str, u64)], cfg: DriftConfig) -> (Arc<DriftHook>, Arc<RecordingRecloner>) {
        let recloner = Arc::new(RecordingRecloner {
            calls: AtomicUsize::new(0),
            ssp_pool: Arc::clone(&h.ssp_pool),
        });
        let hook = Arc::new(DriftHook {
            cfg,
            upstream: Arc::new(StubUpstream(
                upstream.iter().map(|(t, n)| (t.to_string(), Some(*n))).collect(),
            )),
            state: Arc::new(RwLock::new(DriftState::default())),
            reclone: recloner.clone(),
        });
        (hook, recloner)
    }

    #[tokio::test]
    async fn an_empty_replica_table_upstream_has_rows_for_reclones_and_flags_ssps() {
        let h = TestHarness::new().await;
        h.add_ready_ssp("ssp-a", "http://localhost:9999").await;
        // Upstream has contacts; the replica has never seen the table.
        let (hook, recloner) = hook(&h, &[("contact", 5386)], DriftConfig::default());

        let action = drift::run_check(&hook, &h.replica, &BTreeSet::new()).await;
        assert!(matches!(action, Action::Reclone { ref tables } if tables == &["contact".to_string()]), "{action:?}");
        assert_eq!(recloner.calls.load(Ordering::SeqCst), 1);
        assert!(h.ssp_pool.write().await.take_resync_flag("ssp-a"), "SSP flagged for re-bootstrap");

        let st = hook.state.read().await;
        assert_eq!(st.auto_reclones, 1);
        let json = drift::state_json(&st, &hook.cfg);
        assert_eq!(json["mismatched"], serde_json::json!(["contact"]));
        assert_eq!(json["tables"]["contact"]["upstream"], 5386);
        assert_eq!(json["tables"]["contact"]["replica"], 0);
    }

    #[tokio::test]
    async fn reporting_only_never_reclones_but_shows_up_in_health_snapshot() {
        let h = TestHarness::new().await;
        let cfg = DriftConfig { auto_reclone: false, ..DriftConfig::default() };
        let (hook, recloner) = hook(&h, &[("contact", 5)], cfg);

        let action = drift::run_check(&hook, &h.replica, &BTreeSet::new()).await;
        assert!(matches!(action, Action::Report { .. }), "{action:?}");
        assert_eq!(recloner.calls.load(Ordering::SeqCst), 0);

        // The metrics router renders the same state under `/health/snapshot`.
        let state = MetricsState {
            ssp_pool: Arc::clone(&h.ssp_pool),
            query_tracker: Arc::clone(&h.query_tracker),
            job_tracker: Arc::clone(&h.job_tracker),
            start_time: std::time::Instant::now(),
            scheduler_id: "test-scheduler".to_string(),
            status: Arc::clone(&h.status),
            backend_health: scheduler::backend_health::create_health_cache(&[]),
            shared_backend_configs: scheduler::backend_health::create_shared_configs(&[]),
            ingest: IngestState {
                replica: Arc::clone(&h.replica),
                transport: Arc::clone(&h.transport),
                ssp_pool: Arc::clone(&h.ssp_pool),
                status: Arc::clone(&h.status),
                event_buffer: Arc::clone(&h.event_buffer),
                seq_counter: Arc::clone(&h.seq_counter),
                wal: Arc::clone(&h.wal),
                drain_lock: Arc::clone(&h.drain_lock),
                db_config: Arc::new(h.config.db.clone()),
                job_tables: Arc::new(vec![]),
                observer_permits: Arc::new(tokio::sync::Semaphore::new(8)),
                snapshot_seq: Arc::clone(&h.snapshot_seq_cell),
                fanout: Arc::clone(&h.fanout),
            },
            replica: Arc::clone(&h.replica),
            surrealdb_version: Arc::new(RwLock::new("unknown".to_string())),
            heartbeat: scheduler::heartbeat::HeartbeatStats::new(),
            heartbeat_config: scheduler::heartbeat::Config {
                interval_secs: 30,
                timeout_secs: 25,
                fail_threshold: 3,
                ping_url: None,
                webhook_url: None,
            },
            drift: Arc::clone(&hook.state),
            drift_config: hook.cfg.clone(),
        };
        let app = metrics::create_metrics_router(state);
        let (status, body) = get_json(app, "/health/snapshot").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["drift"]["auto_reclone_enabled"], false);
        assert_eq!(body["drift"]["mismatched"], serde_json::json!(["contact"]));
    }

    #[tokio::test]
    async fn matching_counts_are_clean_and_touch_nothing() {
        let h = TestHarness::new().await;
        // Nothing upstream, nothing in the replica: agreement.
        let (hook, recloner) = hook(&h, &[("contact", 0)], DriftConfig::default());
        assert_eq!(drift::run_check(&hook, &h.replica, &BTreeSet::new()).await, Action::Clean);
        assert_eq!(recloner.calls.load(Ordering::SeqCst), 0);
        let st = hook.state.read().await;
        assert!(drift::state_json(&st, &hook.cfg)["mismatched"].as_array().unwrap().is_empty());
    }
}

// ===========================================================================
// Admin plane (`/admin/api/*`)
// ===========================================================================

mod admin_plane {
    use super::*;
    use axum::extract::ConnectInfo;
    use scheduler::admin::{self, AdminConfig};
    use std::net::SocketAddr;

    /// The admin router over the shared harness.
    ///
    /// `dir` deliberately points at a path that does not exist in most tests:
    /// a scheduler built from a checkout has no dashboard bundle, and that
    /// must be a working API with a placeholder page rather than a failure.
    fn admin_app(h: &TestHarness, password: Option<&str>) -> Router {
        let config = AdminConfig {
            enabled: true,
            host: "127.0.0.1".to_string(),
            port: 9668,
            dir: std::path::PathBuf::from("/nonexistent/dashboard"),
            password: password.map(str::to_string),
            access: None,
            session_ttl: std::time::Duration::from_secs(3600),
            // Long enough that the sampler never fires inside a test: these
            // cover the plane with no database handle, and a tick would only
            // add a race for nothing.
            presence_interval: std::time::Duration::from_secs(3600),
            presence_slow_ms: 250.0,
            presence_large_view_rows: 1000,
            presence_max_rows: 20_000,
        };
        let (_state, router) = admin::build(config, h.admin_deps(None, None));
        router
    }

    /// The admin router with a Sp00ky Cloud link pointed at `api_url` and a
    /// cluster secret, for the tests that need either.
    fn admin_app_with(
        h: &TestHarness,
        password: Option<&str>,
        cloud_api_url: Option<&str>,
        auth_secret: Option<&str>,
    ) -> Router {
        let config = AdminConfig {
            enabled: true,
            host: "127.0.0.1".to_string(),
            port: 9668,
            dir: std::path::PathBuf::from("/nonexistent/dashboard"),
            password: password.map(str::to_string),
            access: None,
            session_ttl: std::time::Duration::from_secs(3600),
            // Long enough that the sampler never fires inside a test: these
            // cover the plane with no database handle, and a tick would only
            // add a race for nothing.
            presence_interval: std::time::Duration::from_secs(3600),
            presence_slow_ms: 250.0,
            presence_large_view_rows: 1000,
            presence_max_rows: 20_000,
        };
        let (_state, router) = admin::build(config, h.admin_deps(cloud_api_url, auth_secret));
        router
    }

    /// `oneshot` carries no connection info, but the login handler rate-limits
    /// per peer address, so tests have to supply one.
    fn with_peer(mut req: Request<axum::body::Body>, ip: &str) -> Request<axum::body::Body> {
        let addr: SocketAddr = format!("{ip}:40000").parse().unwrap();
        req.extensions_mut().insert(ConnectInfo(addr));
        req
    }

    fn get(path: &str) -> Request<axum::body::Body> {
        Request::builder()
            .uri(path)
            .body(axum::body::Body::empty())
            .unwrap()
    }

    fn get_auth(path: &str, token: &str) -> Request<axum::body::Body> {
        Request::builder()
            .uri(path)
            .header("Authorization", format!("Bearer {token}"))
            .body(axum::body::Body::empty())
            .unwrap()
    }

    fn login_req(body: Value, ip: &str) -> Request<axum::body::Body> {
        with_peer(
            Request::builder()
                .method("POST")
                .uri("/admin/api/session")
                .header("Content-Type", "application/json")
                .body(axum::body::Body::from(body.to_string()))
                .unwrap(),
            ip,
        )
    }

    async fn body_json(res: axum::response::Response) -> Value {
        let bytes = res.into_body().collect().await.unwrap().to_bytes();
        serde_json::from_slice(&bytes).unwrap_or(Value::Null)
    }

    /// Sign in with the break-glass password and return the bearer token.
    ///
    /// Takes the router rather than the harness: each `admin_app` call builds
    /// its own `SessionStore`, so a token is only valid against the router that
    /// minted it. Tests must therefore build one router and clone it.
    async fn breakglass_token(app: &Router, password: &str) -> String {
        let res = app
            .clone()
            .oneshot(login_req(json!({ "password": password }), "10.0.0.1"))
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        body_json(res).await["token"].as_str().unwrap().to_string()
    }

    #[tokio::test]
    async fn config_is_reachable_without_a_token() {
        let h = TestHarness::new().await;
        let res = admin_app(&h, None)
            .oneshot(get("/admin/api/config"))
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);

        let body = body_json(res).await;
        assert_eq!(body["scheduler_id"], "test-scheduler");
        assert!(body["version"].is_string());
        // This is what tells the frontend whether to offer the break-glass
        // toggle at all.
        assert_eq!(body["breakglass_available"], false);
    }

    #[tokio::test]
    async fn config_reports_breakglass_when_a_password_is_set() {
        let h = TestHarness::new().await;
        let res = admin_app(&h, Some("pw"))
            .oneshot(get("/admin/api/config"))
            .await
            .unwrap();
        assert_eq!(body_json(res).await["breakglass_available"], true);
    }

    #[tokio::test]
    async fn every_other_endpoint_requires_a_token() {
        let h = TestHarness::new().await;
        for path in [
            "/admin/api/me",
            "/admin/api/overview",
            "/admin/api/backends",
            "/admin/api/backends/api",
            "/admin/api/logs",
            "/admin/api/workflows/runs",
            "/admin/api/workflows/runs/x",
            "/admin/api/workflows/stream",
            "/admin/api/schedules",
        ] {
            let res = admin_app(&h, None).oneshot(get(path)).await.unwrap();
            assert_eq!(
                res.status(),
                StatusCode::UNAUTHORIZED,
                "{path} must be behind auth"
            );
        }
    }

    #[tokio::test]
    async fn a_malformed_or_unknown_token_is_rejected() {
        let h = TestHarness::new().await;
        for token in ["", "nonsense", "Bearer nested"] {
            let res = admin_app(&h, None)
                .oneshot(get_auth("/admin/api/overview", token))
                .await
                .unwrap();
            assert_eq!(res.status(), StatusCode::UNAUTHORIZED, "token {token:?}");
        }
    }

    #[tokio::test]
    async fn breakglass_login_succeeds_and_the_token_works() {
        let h = TestHarness::new().await;
        let app = admin_app(&h, Some("hunter2"));
        let token = breakglass_token(&app, "hunter2").await;

        let res = app
            .oneshot(get_auth("/admin/api/me", &token))
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);

        let me = body_json(res).await;
        assert_eq!(me["mode"], "breakglass");
        assert_eq!(me["subject"], "breakglass");
    }

    #[tokio::test]
    async fn breakglass_rejects_a_wrong_password() {
        let h = TestHarness::new().await;
        let res = admin_app(&h, Some("hunter2"))
            .oneshot(login_req(json!({ "password": "wrong" }), "10.0.0.2"))
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn password_only_login_is_refused_when_no_password_is_configured() {
        let h = TestHarness::new().await;
        // Not merely "wrong password" — with SPKY_ADMIN_PASSWORD unset the
        // whole path must be closed, never fall back to some default.
        let res = admin_app(&h, None)
            .oneshot(login_req(json!({ "password": "" }), "10.0.0.3"))
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn an_empty_password_never_matches_an_unset_one() {
        let h = TestHarness::new().await;
        let res = admin_app(&h, None)
            .oneshot(login_req(json!({ "username": "alice", "password": "" }), "10.0.0.4"))
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn a_roster_login_without_a_database_fails_closed() {
        let h = TestHarness::new().await;
        // No database handle, so no access method can be discovered. This must
        // deny, not fall through to the break-glass branch.
        let res = admin_app(&h, Some("hunter2"))
            .oneshot(login_req(
                json!({ "username": "alice", "password": "hunter2" }),
                "10.0.0.5",
            ))
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn failures_do_not_disclose_why() {
        let h = TestHarness::new().await;

        let wrong_password = admin_app(&h, Some("hunter2"))
            .oneshot(login_req(json!({ "password": "nope" }), "10.0.0.6"))
            .await
            .unwrap();
        let unknown_user = admin_app(&h, Some("hunter2"))
            .oneshot(login_req(
                json!({ "username": "ghost", "password": "nope" }),
                "10.0.0.7",
            ))
            .await
            .unwrap();

        assert_eq!(wrong_password.status(), unknown_user.status());
        assert_eq!(
            body_json(wrong_password).await,
            body_json(unknown_user).await,
            "a login failure must not reveal whether the account exists"
        );
    }

    #[tokio::test]
    async fn login_is_rate_limited_per_address() {
        let h = TestHarness::new().await;
        // One router across the attempts, so they share a rate limiter.
        let app = admin_app(&h, Some("hunter2"));

        for i in 0..10 {
            let res = app
                .clone()
                .oneshot(login_req(json!({ "password": "wrong" }), "10.9.9.9"))
                .await
                .unwrap();
            assert_eq!(res.status(), StatusCode::UNAUTHORIZED, "attempt {i}");
        }

        let blocked = app
            .clone()
            .oneshot(login_req(json!({ "password": "wrong" }), "10.9.9.9"))
            .await
            .unwrap();
        assert_eq!(blocked.status(), StatusCode::TOO_MANY_REQUESTS);

        // A different address is unaffected — the limiter must not become a
        // denial of service against every other operator.
        let other = app
            .oneshot(login_req(json!({ "password": "hunter2" }), "10.9.9.10"))
            .await
            .unwrap();
        assert_eq!(other.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn logout_revokes_the_token() {
        let h = TestHarness::new().await;
        let app = admin_app(&h, Some("hunter2"));

        let res = app
            .clone()
            .oneshot(login_req(json!({ "password": "hunter2" }), "10.0.0.8"))
            .await
            .unwrap();
        let token = body_json(res).await["token"].as_str().unwrap().to_string();

        let logout = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/admin/api/logout")
                    .header("Authorization", format!("Bearer {token}"))
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(logout.status(), StatusCode::NO_CONTENT);

        let after = app
            .oneshot(get_auth("/admin/api/overview", &token))
            .await
            .unwrap();
        assert_eq!(after.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn overview_carries_the_whole_cluster() {
        let h = TestHarness::new().await;
        h.add_ready_ssp("ssp-1", "http://10.0.0.1:8667").await;
        let app = admin_app(&h, Some("pw"));
        let token = breakglass_token(&app, "pw").await;

        let res = app
            .oneshot(get_auth("/admin/api/overview", &token))
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);

        let body = body_json(res).await;
        assert_eq!(body["scheduler"]["entity"], "scheduler");
        assert_eq!(body["ssps"].as_array().unwrap().len(), 1);
        assert_eq!(body["totals"]["ssps"], 1);
        assert_eq!(body["totals"]["ssps_ready"], 1);
        // The dashboard draws its deadline bar against this.
        assert_eq!(body["bootstrap_timeout_secs"], h.config.bootstrap_timeout_secs);
        // Running operations ride along on the poll.
        assert!(body["operations"].is_array());
        assert_eq!(body["cloud_linked"], false);
        assert!(body["server_time_ms"].as_u64().unwrap() > 0);
    }

    #[tokio::test]
    async fn overview_and_info_agree_about_ssp_state() {
        let h = TestHarness::new().await;
        h.add_ready_ssp("ssp-1", "http://10.0.0.1:8667").await;
        let app = admin_app(&h, Some("pw"));
        let token = breakglass_token(&app, "pw").await;

        let admin = body_json(
            app.oneshot(get_auth("/admin/api/overview", &token))
                .await
                .unwrap(),
        )
        .await;
        let info = body_json(h.metrics_router().oneshot(get("/info")).await.unwrap()).await;

        let info_ssp = info
            .as_array()
            .unwrap()
            .iter()
            .find(|e| e["entity"] == "ssp")
            .unwrap();

        // Both are built from `metrics::build_entities`, and that must stay
        // true: an operator comparing the two must never see them disagree.
        assert_eq!(admin["ssps"][0]["id"], info_ssp["id"]);
        assert_eq!(admin["ssps"][0]["status"], info_ssp["status"]);
    }

    #[tokio::test]
    async fn ssp_entities_carry_the_progress_fields() {
        let h = TestHarness::new().await;
        h.add_ready_ssp("ssp-1", "http://10.0.0.1:8667").await;

        let info = body_json(h.metrics_router().oneshot(get("/info")).await.unwrap()).await;
        let ssp = info
            .as_array()
            .unwrap()
            .iter()
            .find(|e| e["entity"] == "ssp")
            .unwrap();

        assert!(ssp.get("state_seconds").is_some());
        assert_eq!(ssp["buffered_events"], 0);
        // A ready SSP has no bootstrap in flight.
        assert!(ssp["bootstrap"].is_null());
    }

    #[tokio::test]
    async fn backends_list_is_empty_but_well_formed() {
        let h = TestHarness::new().await;
        let app = admin_app(&h, Some("pw"));
        let token = breakglass_token(&app, "pw").await;

        let body = body_json(
            app.oneshot(get_auth("/admin/api/backends", &token))
                .await
                .unwrap(),
        )
        .await;
        assert_eq!(body["backends"].as_array().unwrap().len(), 0);
        assert_eq!(body["check_interval_secs"], 15);
    }

    #[tokio::test]
    async fn an_unknown_backend_is_a_404() {
        let h = TestHarness::new().await;
        let app = admin_app(&h, Some("pw"));
        let token = breakglass_token(&app, "pw").await;

        let res = app
            .oneshot(get_auth("/admin/api/backends/ghost", &token))
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn workflow_endpoints_are_unavailable_until_the_database_handle_lands() {
        let h = TestHarness::new().await;
        let app = admin_app(&h, Some("pw"));
        let token = breakglass_token(&app, "pw").await;

        // The admin plane comes up with the HTTP servers, before
        // `Scheduler::start()` has built the shared handle. That window must
        // be a clean 503, matching how every other endpoint behaves while the
        // scheduler is still cloning.
        for path in [
            "/admin/api/workflows/runs",
            "/admin/api/workflows/runs/abc",
            "/admin/api/workflows/stream",
            "/admin/api/schedules",
        ] {
            let res = app
                .clone()
                .oneshot(get_auth(path, &token))
                .await
                .unwrap();
            assert_eq!(res.status(), StatusCode::SERVICE_UNAVAILABLE, "{path}");
        }
    }

    #[tokio::test]
    async fn an_unknown_log_source_is_rejected() {
        let h = TestHarness::new().await;
        let app = admin_app(&h, Some("pw"));
        let token = breakglass_token(&app, "pw").await;

        let res = app
            .oneshot(get_auth("/admin/api/logs?source=backend:api", &token))
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn logs_for_an_unknown_ssp_are_a_404() {
        let h = TestHarness::new().await;
        let app = admin_app(&h, Some("pw"));
        let token = breakglass_token(&app, "pw").await;

        let res = app
            .oneshot(get_auth("/admin/api/logs?source=ssp:ghost", &token))
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn the_scheduler_log_stream_replays_history() {
        let h = TestHarness::new().await;
        let app = admin_app(&h, Some("pw"));
        let token = breakglass_token(&app, "pw").await;

        let res = app
            .oneshot(get_auth(
                "/admin/api/logs?source=scheduler&tail=false",
                &token,
            ))
            .await
            .unwrap();

        assert_eq!(res.status(), StatusCode::OK);
        assert_eq!(
            res.headers()
                .get("content-type")
                .and_then(|v| v.to_str().ok()),
            Some("text/event-stream")
        );
        // `tail=false` closes on its own, so the body is finite and safe to
        // collect here — a tailing stream would hang this test forever.
        let bytes = res.into_body().collect().await.unwrap().to_bytes();
        assert!(String::from_utf8_lossy(&bytes).len() < 10_000);
    }

    #[tokio::test]
    async fn a_missing_dashboard_bundle_serves_a_placeholder_not_a_broken_api() {
        let h = TestHarness::new().await;

        let page = admin_app(&h, None).oneshot(get("/admin")).await.unwrap();
        assert_eq!(page.status(), StatusCode::NOT_FOUND);
        let body = page.into_body().collect().await.unwrap().to_bytes();
        let html = String::from_utf8_lossy(&body);
        assert!(html.contains("Dashboard not bundled"), "{html}");

        // The API on the same port keeps working, which is the whole point of
        // serving the bundle from disk rather than embedding it.
        let api = admin_app(&h, None)
            .oneshot(get("/admin/api/config"))
            .await
            .unwrap();
        assert_eq!(api.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn the_admin_plane_does_not_serve_the_ingest_routes() {
        let h = TestHarness::new().await;
        // The separation is the security boundary: publishing the admin port
        // must not publish `/proxy/query`, which runs arbitrary SurrealQL.
        for path in ["/proxy/query", "/ingest", "/metrics", "/health", "/info"] {
            let res = admin_app(&h, None).oneshot(get(path)).await.unwrap();
            assert_eq!(
                res.status(),
                StatusCode::NOT_FOUND,
                "{path} must not exist on the admin port"
            );
        }
    }

    #[tokio::test]
    async fn the_ingest_plane_does_not_serve_the_admin_api() {
        let h = TestHarness::new().await;
        for path in ["/admin/api/config", "/admin/api/overview"] {
            let res = h.full_app().oneshot(get(path)).await.unwrap();
            assert_eq!(
                res.status(),
                StatusCode::NOT_FOUND,
                "{path} must not exist on the ingest port"
            );
        }
    }

    #[tokio::test]
    async fn bootstrap_progress_is_recorded_and_cleared_on_ready() {
        let h = TestHarness::new().await;

        // A registered but still-bootstrapping SSP.
        {
            let mut pool = h.ssp_pool.write().await;
            pool.upsert(SspInfo {
                id: "ssp-boot".to_string(),
                url: "http://10.0.0.9:8667".to_string(),
                version: "test".to_string(),
                connected_at: std::time::Instant::now(),
                last_heartbeat: std::time::Instant::now(),
                query_count: 0,
                views: 0,
                cpu_usage: None,
                memory_usage: None,
                env: None,
                bootstrap: None,
            });
            pool.mark_bootstrapping("ssp-boot");
        }

        let res = h
            .ssp_router()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/ssp/bootstrap-progress")
                    .header("Content-Type", "application/json")
                    .body(axum::body::Body::from(
                        json!({
                            "ssp_id": "ssp-boot",
                            "tables_done": 3,
                            "tables_total": 10,
                            "rows_loaded": 4200,
                            "current_table": "message"
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);

        let info = body_json(h.metrics_router().oneshot(get("/info")).await.unwrap()).await;
        let ssp = info
            .as_array()
            .unwrap()
            .iter()
            .find(|e| e["id"] == "ssp-boot")
            .unwrap();
        assert_eq!(ssp["status"], "bootstrapping");
        assert_eq!(ssp["bootstrap"]["tables_done"], 3);
        assert_eq!(ssp["bootstrap"]["tables_total"], 10);
        assert_eq!(ssp["bootstrap"]["rows_loaded"], 4200);
        assert_eq!(ssp["bootstrap"]["current_table"], "message");

        // Going ready must drop the bar rather than leave a stale one behind.
        h.ssp_pool.write().await.mark_ready("ssp-boot");
        let info = body_json(h.metrics_router().oneshot(get("/info")).await.unwrap()).await;
        let ssp = info
            .as_array()
            .unwrap()
            .iter()
            .find(|e| e["id"] == "ssp-boot")
            .unwrap();
        assert_eq!(ssp["status"], "ready");
        assert!(ssp["bootstrap"].is_null());
    }

    #[tokio::test]
    async fn progress_for_an_unknown_ssp_is_accepted_and_ignored() {
        let h = TestHarness::new().await;
        // Advisory: a report can outlive the SSP it describes, and that must
        // never be an error the bootstrapping SSP has to handle.
        let res = h
            .ssp_router()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/ssp/bootstrap-progress")
                    .header("Content-Type", "application/json")
                    .body(axum::body::Body::from(
                        json!({ "ssp_id": "ghost", "tables_done": 1, "tables_total": 2 })
                            .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
    }

    // -----------------------------------------------------------------------
    // Actions
    // -----------------------------------------------------------------------

    fn post_auth(path: &str, token: &str, body: Value) -> Request<axum::body::Body> {
        Request::builder()
            .method("POST")
            .uri(path)
            .header("Authorization", format!("Bearer {token}"))
            .header("Content-Type", "application/json")
            .body(axum::body::Body::from(body.to_string()))
            .unwrap()
    }

    fn post_auth_empty(path: &str, token: &str) -> Request<axum::body::Body> {
        Request::builder()
            .method("POST")
            .uri(path)
            .header("Authorization", format!("Bearer {token}"))
            .body(axum::body::Body::empty())
            .unwrap()
    }

    fn admin_heartbeat(ssp_id: &str) -> Value {
        json!({
            "ssp_id": ssp_id,
            "timestamp": 1000,
            "views": 5,
            "cpu_usage": 45.0,
            "memory_usage": 60.0,
            "version": "test"
        })
    }

    #[tokio::test]
    async fn every_action_requires_a_token() {
        let h = TestHarness::new().await;
        for path in [
            "/admin/api/ssps/x/restart",
            "/admin/api/ssps/restart-all",
            "/admin/api/scheduler/restart",
            "/admin/api/cloud/restart",
            "/admin/api/backups",
            "/admin/api/backups/x/restore",
            "/admin/api/workflows/runs/x/cancel",
            "/admin/api/workflows/runs/x/rerun",
            "/admin/api/workflows/runs/x/retry",
            "/admin/api/schedules/x/pause",
            "/admin/api/schedules/x/resume",
            "/admin/api/schedules/x/trigger",
            "/admin/api/jobs/x/kill",
            "/admin/api/jobs/x/retry",
        ] {
            let res = admin_app(&h, None)
                .oneshot(post_auth_empty(path, "nope"))
                .await
                .unwrap();
            assert_eq!(res.status(), StatusCode::UNAUTHORIZED, "{path} must be behind auth");
        }
        for path in ["/admin/api/operations", "/admin/api/operations/stream", "/admin/api/cloud/deployment"] {
            let res = admin_app(&h, None).oneshot(get(path)).await.unwrap();
            assert_eq!(res.status(), StatusCode::UNAUTHORIZED, "{path} must be behind auth");
        }
    }

    #[tokio::test]
    async fn the_ingest_plane_does_not_serve_the_actions() {
        let h = TestHarness::new().await;
        for path in ["/admin/api/ssps/restart-all", "/admin/api/scheduler/restart", "/admin/api/backups"] {
            let res = h
                .full_app()
                .oneshot(post_auth_empty(path, "x"))
                .await
                .unwrap();
            assert_eq!(res.status(), StatusCode::NOT_FOUND, "{path} must not exist on the ingest port");
        }
    }

    #[tokio::test]
    async fn config_reports_link_supervision_and_session_persistence() {
        let h = TestHarness::new().await;
        let res = admin_app(&h, None).oneshot(get("/admin/api/config")).await.unwrap();
        let body = body_json(res).await;
        assert_eq!(body["cloud_linked"], false);
        assert_eq!(body["supervised"], false);
        assert_eq!(body["sessions_persistent"], false);

        let res = admin_app_with(&h, None, Some("http://127.0.0.1:1"), Some("secret"))
            .oneshot(get("/admin/api/config"))
            .await
            .unwrap();
        let body = body_json(res).await;
        assert_eq!(body["cloud_linked"], true);
        assert_eq!(body["sessions_persistent"], true);
    }

    #[tokio::test]
    async fn a_signed_session_survives_a_rebuilt_plane() {
        let h = TestHarness::new().await;
        let first = admin_app_with(&h, Some("pw"), None, Some("cluster-secret"));
        let token = breakglass_token(&first, "pw").await;
        assert!(token.contains('.'), "signed tokens are payload.signature");

        // A "restart": a brand new router and session store, same secret.
        let second = admin_app_with(&h, Some("pw"), None, Some("cluster-secret"));
        let res = second.oneshot(get_auth("/admin/api/me", &token)).await.unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        assert_eq!(body_json(res).await["mode"], "breakglass");

        // And not with a different secret.
        let other = admin_app_with(&h, Some("pw"), None, Some("other"));
        let res = other.oneshot(get_auth("/admin/api/me", &token)).await.unwrap();
        assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn restarting_an_unknown_ssp_is_a_404() {
        let h = TestHarness::new().await;
        let app = admin_app(&h, Some("pw"));
        let token = breakglass_token(&app, "pw").await;
        let res = app
            .oneshot(post_auth("/admin/api/ssps/ghost/restart", &token, json!({ "mode": "restart" })))
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::NOT_FOUND);
        assert!(body_json(res).await["error"].as_str().unwrap().contains("ghost"));
    }

    #[tokio::test]
    async fn an_unknown_restart_mode_is_a_400() {
        let h = TestHarness::new().await;
        h.add_ready_ssp("ssp-1", "http://10.0.0.1:8667").await;
        let app = admin_app(&h, Some("pw"));
        let token = breakglass_token(&app, "pw").await;
        let res = app
            .clone()
            .oneshot(post_auth("/admin/api/ssps/ssp-1/restart", &token, json!({ "mode": "explode" })))
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::BAD_REQUEST);
        let res = app
            .oneshot(post_auth("/admin/api/scheduler/restart", &token, json!({ "mode": "explode" })))
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn restarting_an_ssp_flags_it_and_the_next_heartbeat_carries_the_directive() {
        let h = TestHarness::new().await;
        h.add_ready_ssp("ssp-1", "http://10.0.0.1:8667").await;
        let app = admin_app(&h, Some("pw"));
        let token = breakglass_token(&app, "pw").await;

        let res = app
            .clone()
            .oneshot(post_auth("/admin/api/ssps/ssp-1/restart", &token, json!({ "mode": "restart" })))
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::ACCEPTED);
        let body = body_json(res).await;
        assert_eq!(body["operation"]["kind"], "ssp_restart");
        assert_eq!(body["operation"]["target"], "ssp-1");
        assert_eq!(body["operation"]["status"], "running");
        let op_id = body["operation"]["id"].as_str().unwrap().to_string();

        // The flag is consumed by the SSP's next heartbeat as a 409 whose body
        // is the directive, not prose.
        let (status, hb) = post_json(h.ssp_router(), "/ssp/heartbeat", &admin_heartbeat("ssp-1")).await;
        assert_eq!(status, StatusCode::CONFLICT);
        assert_eq!(hb["clean"], false, "{hb}");
        assert!(hb["reason"].as_str().unwrap().contains("re-bootstrap"));

        // Consumed: a second heartbeat is plain.
        let (status, _) = post_json(h.ssp_router(), "/ssp/heartbeat", &admin_heartbeat("ssp-1")).await;
        assert_eq!(status, StatusCode::OK);

        // The operation is listed and still running (the SSP has not come back).
        let res = app.oneshot(get_auth("/admin/api/operations", &token)).await.unwrap();
        let ops = body_json(res).await;
        assert_eq!(ops["operations"][0]["id"], op_id);
        assert_eq!(ops["operations"][0]["status"], "running");
    }

    #[tokio::test]
    async fn a_clean_restart_asks_the_ssp_to_drop_its_snapshot() {
        let h = TestHarness::new().await;
        h.add_ready_ssp("ssp-1", "http://10.0.0.1:8667").await;
        let app = admin_app(&h, Some("pw"));
        let token = breakglass_token(&app, "pw").await;

        let res = app
            .oneshot(post_auth("/admin/api/ssps/ssp-1/restart", &token, json!({ "mode": "clean" })))
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::ACCEPTED);
        assert_eq!(body_json(res).await["operation"]["kind"], "ssp_clean");

        let (status, hb) = post_json(h.ssp_router(), "/ssp/heartbeat", &admin_heartbeat("ssp-1")).await;
        assert_eq!(status, StatusCode::CONFLICT);
        assert_eq!(hb["clean"], true, "{hb}");
        // And the SSP side parses exactly this.
        let directive = ssp_protocol::ResyncDirective::parse(&hb.to_string());
        assert!(directive.clean);
    }

    #[tokio::test]
    async fn a_clean_flag_is_not_downgraded_by_a_later_plain_resync() {
        let h = TestHarness::new().await;
        h.add_ready_ssp("ssp-1", "http://10.0.0.1:8667").await;
        {
            let mut pool = h.ssp_pool.write().await;
            pool.mark_for_resync_with("ssp-1", scheduler::router::ResyncKind::Clean);
            pool.mark_for_resync("ssp-1");
            assert_eq!(pool.pending_resync("ssp-1"), Some(scheduler::router::ResyncKind::Clean));
        }
        let (status, hb) = post_json(h.ssp_router(), "/ssp/heartbeat", &admin_heartbeat("ssp-1")).await;
        assert_eq!(status, StatusCode::CONFLICT);
        assert_eq!(hb["clean"], true);
    }

    #[tokio::test]
    async fn restart_all_at_once_flags_every_ssp() {
        let h = TestHarness::new().await;
        h.add_ready_ssp("ssp-1", "http://10.0.0.1:8667").await;
        h.add_ready_ssp("ssp-2", "http://10.0.0.2:8667").await;
        let app = admin_app(&h, Some("pw"));
        let token = breakglass_token(&app, "pw").await;

        let res = app
            .oneshot(post_auth("/admin/api/ssps/restart-all", &token, json!({ "mode": "restart", "rolling": false })))
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::ACCEPTED);
        let body = body_json(res).await;
        assert_eq!(body["operation"]["kind"], "rolling_restart");
        assert_eq!(body["operation"]["detail"]["total"], 2);

        let pool = h.ssp_pool.read().await;
        assert!(pool.pending_resync("ssp-1").is_some());
        assert!(pool.pending_resync("ssp-2").is_some());
    }

    #[tokio::test]
    async fn a_rolling_restart_flags_one_ssp_at_a_time_and_refuses_a_second() {
        let h = TestHarness::new().await;
        h.add_ready_ssp("ssp-1", "http://10.0.0.1:8667").await;
        h.add_ready_ssp("ssp-2", "http://10.0.0.2:8667").await;
        let app = admin_app(&h, Some("pw"));
        let token = breakglass_token(&app, "pw").await;

        let res = app
            .clone()
            .oneshot(post_auth("/admin/api/ssps/restart-all", &token, json!({ "rolling": true })))
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::ACCEPTED);
        // Give the roll task a moment to flag its first target.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        {
            let pool = h.ssp_pool.read().await;
            let flagged = ["ssp-1", "ssp-2"]
                .iter()
                .filter(|id| pool.pending_resync(id).is_some())
                .count();
            assert_eq!(flagged, 1, "a roll takes down one SSP at a time");
            assert!(pool.pending_resync("ssp-1").is_some(), "in id order");
        }

        let res = app
            .oneshot(post_auth("/admin/api/ssps/restart-all", &token, json!({ "rolling": true })))
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::CONFLICT);
    }

    #[tokio::test]
    async fn restart_all_with_no_ssps_is_a_409() {
        let h = TestHarness::new().await;
        let app = admin_app(&h, Some("pw"));
        let token = breakglass_token(&app, "pw").await;
        let res = app
            .oneshot(post_auth_empty("/admin/api/ssps/restart-all", &token))
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::CONFLICT);
    }

    #[tokio::test]
    async fn a_resync_is_refused_while_cloning_or_restoring() {
        for status in [SchedulerStatus::Cloning, SchedulerStatus::Restoring] {
            let h = TestHarness::with_status(status).await;
            let app = admin_app(&h, Some("pw"));
            let token = breakglass_token(&app, "pw").await;
            let res = app
                .oneshot(post_auth("/admin/api/scheduler/restart", &token, json!({ "mode": "rehash" })))
                .await
                .unwrap();
            assert_eq!(res.status(), StatusCode::SERVICE_UNAVAILABLE, "{status:?}");
        }
    }

    #[tokio::test]
    async fn a_rehash_runs_as_an_operation_and_flags_every_ssp() {
        let h = TestHarness::new().await;
        h.add_ready_ssp("ssp-1", "http://10.0.0.1:8667").await;
        let app = admin_app(&h, Some("pw"));
        let token = breakglass_token(&app, "pw").await;
        let res = app
            .clone()
            .oneshot(post_auth("/admin/api/scheduler/restart", &token, json!({ "mode": "rehash" })))
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::ACCEPTED);
        let op_id = body_json(res).await["operation"]["id"].as_str().unwrap().to_string();

        // The rehash is quick on an empty replica; wait for the op to settle.
        let mut settled = None;
        for _ in 0..50 {
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            let res = app.clone().oneshot(get_auth("/admin/api/operations", &token)).await.unwrap();
            let ops = body_json(res).await;
            let op = ops["operations"].as_array().unwrap().iter().find(|o| o["id"] == op_id).cloned().unwrap();
            if op["status"] != "running" {
                settled = Some(op);
                break;
            }
        }
        let op = settled.expect("rehash op should settle");
        assert_eq!(op["status"], "done", "{op}");
        assert_eq!(op["detail"]["mode"], "rehash");
        assert!(h.ssp_pool.read().await.pending_resync("ssp-1").is_some());
    }

    #[tokio::test]
    async fn cloud_actions_are_refused_when_unlinked() {
        let h = TestHarness::new().await;
        let app = admin_app(&h, Some("pw"));
        let token = breakglass_token(&app, "pw").await;
        for (method, path) in [
            ("POST", "/admin/api/cloud/restart"),
            ("GET", "/admin/api/cloud/deployment"),
            ("DELETE", "/admin/api/backups/x"),
            ("PUT", "/admin/api/backups/config"),
        ] {
            let req = Request::builder()
                .method(method)
                .uri(path)
                .header("Authorization", format!("Bearer {token}"))
                .header("Content-Type", "application/json")
                .body(axum::body::Body::from("{}"))
                .unwrap();
            let res = app.clone().oneshot(req).await.unwrap();
            assert_eq!(res.status(), StatusCode::CONFLICT, "{method} {path}");
            assert_eq!(body_json(res).await["error"], scheduler::admin::cloud::NOT_LINKED);
        }
    }

    #[tokio::test]
    async fn a_linked_scheduler_relays_the_control_planes_answer() {
        // A stand-in control plane that refuses with its own words.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let fake = Router::new().route(
            "/v1/internal/projects/test-project/restart",
            axum::routing::post(|req: Request<axum::body::Body>| async move {
                let auth = req.headers().get("authorization").and_then(|v| v.to_str().ok()).unwrap_or("").to_string();
                if auth != "Bearer cluster-secret" {
                    return (StatusCode::UNAUTHORIZED, axum::Json(json!({ "error": "invalid project credentials" })));
                }
                (StatusCode::CONFLICT, axum::Json(json!({ "error": "a restart is already in progress for this project", "code": "conflict" })))
            }),
        );
        tokio::spawn(async move { axum::serve(listener, fake).await.unwrap() });

        let h = TestHarness::new().await;
        let app = admin_app_with(&h, Some("pw"), Some(&format!("http://{addr}")), None);
        let token = breakglass_token(&app, "pw").await;
        let res = app
            .oneshot(post_auth("/admin/api/cloud/restart", &token, json!({ "roles": ["ssp"], "upgrade": true })))
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::CONFLICT);
        assert_eq!(body_json(res).await["error"], "a restart is already in progress for this project");
    }

    #[tokio::test]
    async fn backups_list_works_unlinked_and_says_what_is_missing() {
        let h = TestHarness::new().await;
        let app = admin_app(&h, Some("pw"));
        let token = breakglass_token(&app, "pw").await;
        let res = app.clone().oneshot(get_auth("/admin/api/backups", &token)).await.unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        let body = body_json(res).await;
        assert_eq!(body["linked"], false);
        assert_eq!(body["project_slug"], "default");
        assert!(body["catalog"].is_array());
        assert!(body["config"].is_null(), "schedules live in the control plane");
        assert_eq!(body["local"]["queue_len"], 0);

        // Creating without storage configured is refused with the env names.
        if !maintenance::BackupConfig::env_configured() {
            let res = app
                .oneshot(post_auth("/admin/api/backups", &token, json!({ "name": "x" })))
                .await
                .unwrap();
            assert_eq!(res.status(), StatusCode::SERVICE_UNAVAILABLE);
            assert!(body_json(res).await["error"].as_str().unwrap().contains("S3_ENDPOINT"));
        }
    }

    #[tokio::test]
    async fn a_restore_status_for_an_unknown_backup_is_a_404() {
        let h = TestHarness::new().await;
        let app = admin_app(&h, Some("pw"));
        let token = breakglass_token(&app, "pw").await;
        let res = app.oneshot(get_auth("/admin/api/backups/nope/restore", &token)).await.unwrap();
        assert_eq!(res.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn database_backed_actions_answer_503_without_a_handle() {
        let h = TestHarness::new().await;
        let app = admin_app(&h, Some("pw"));
        let token = breakglass_token(&app, "pw").await;
        for path in [
            "/admin/api/workflows/runs/_00_workflow_run:x/cancel",
            "/admin/api/workflows/runs/_00_workflow_run:x/rerun",
            "/admin/api/workflows/runs/_00_workflow_run:x/retry",
            "/admin/api/schedules/x/pause",
            "/admin/api/schedules/x/resume",
            "/admin/api/schedules/x/trigger",
        ] {
            let res = app.clone().oneshot(post_auth_empty(path, &token)).await.unwrap();
            assert_eq!(res.status(), StatusCode::SERVICE_UNAVAILABLE, "{path}");
        }
    }

    #[tokio::test]
    async fn a_run_action_on_a_non_run_id_is_a_400() {
        let h = TestHarness::new().await;
        let app = admin_app(&h, Some("pw"));
        let token = breakglass_token(&app, "pw").await;
        let res = app
            .oneshot(post_auth_empty("/admin/api/workflows/runs/_00_schedule:x/cancel", &token))
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn job_actions_without_a_ready_ssp_are_a_503_in_admin_words() {
        let h = TestHarness::new().await;
        let app = admin_app(&h, Some("pw"));
        let token = breakglass_token(&app, "pw").await;
        for path in ["/admin/api/jobs/job:1/kill", "/admin/api/jobs/job:1/retry"] {
            let res = app.clone().oneshot(post_auth_empty(path, &token)).await.unwrap();
            assert_eq!(res.status(), StatusCode::SERVICE_UNAVAILABLE, "{path}");
            let body = body_json(res).await;
            assert!(body["error"].as_str().unwrap().contains("no ready SSP"), "{body}");
        }
    }

    #[tokio::test]
    async fn the_operations_stream_opens_with_a_snapshot() {
        let h = TestHarness::new().await;
        let app = admin_app(&h, Some("pw"));
        let token = breakglass_token(&app, "pw").await;
        let res = app.oneshot(get_auth("/admin/api/operations/stream", &token)).await.unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        assert!(res
            .headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
            .starts_with("text/event-stream"));
    }

    // -----------------------------------------------------------------------
    // Presence and registered views
    // -----------------------------------------------------------------------

    /// The harness never fills the db slot, which is exactly the state a
    /// scheduler is in while it clones its replica. The sampler must not have
    /// published anything, and the plane must say so rather than reporting an
    /// empty stack as an idle one.
    #[tokio::test]
    async fn presence_is_not_ready_before_the_sampler_has_a_database() {
        let h = TestHarness::new().await;
        let app = admin_app(&h, Some("pw"));
        let token = breakglass_token(&app, "pw").await;

        let res = app
            .clone()
            .oneshot(get_auth("/admin/api/presence", &token))
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        let body = body_json(res).await;

        // `ready: false` is the load-bearing bit. Zero users with `ready: true`
        // means nobody is connected; zero users with `ready: false` means we do
        // not know yet, and the dashboard draws those differently.
        assert_eq!(body["ready"], false, "{body}");
        assert_eq!(body["totals"]["users"], 0);
        assert_eq!(body["totals"]["views"], 0);
        assert_eq!(body["samples"], json!([]));
        assert!(body["sample_interval_secs"].as_u64().unwrap() > 0);
        assert!(body["slow_ms"].as_f64().unwrap() > 0.0);
        assert_eq!(body["top_users"], json!([]));
        assert_eq!(body["by_ssp"], json!([]));
    }

    /// The sidebar count and the overview tile ride this poll rather than a
    /// request of their own, so the block has to be here.
    #[tokio::test]
    async fn the_overview_carries_the_presence_block() {
        let h = TestHarness::new().await;
        let app = admin_app(&h, Some("pw"));
        let token = breakglass_token(&app, "pw").await;

        let res = app
            .oneshot(get_auth("/admin/api/overview", &token))
            .await
            .unwrap();
        let body = body_json(res).await;
        assert!(body["presence"].is_object(), "{body}");
        assert_eq!(body["presence"]["ready"], false);
        assert!(body["presence"]["totals"]["sessions"].is_number());
    }

    /// Unlike `/presence`, the listings read the database live, so with no
    /// handle they must answer the same 503 every other database-backed admin
    /// endpoint does — not an empty list, which reads as "no one is here".
    #[tokio::test]
    async fn view_listings_answer_503_without_a_database_handle() {
        let h = TestHarness::new().await;
        let app = admin_app(&h, Some("pw"));
        let token = breakglass_token(&app, "pw").await;

        for path in ["/admin/api/views", "/admin/api/views/deadbeef"] {
            let res = app.clone().oneshot(get_auth(path, &token)).await.unwrap();
            assert_eq!(res.status(), StatusCode::SERVICE_UNAVAILABLE, "{path}");
            let body = body_json(res).await;
            assert!(
                body["error"].as_str().unwrap().contains("database handle"),
                "{path}: {body}"
            );
        }
    }

    /// An unknown sort is rejected before it reaches SurrealDB. It has to be:
    /// v3 refuses an ORDER BY on a field the projection does not carry, so a
    /// pass-through would surface as an opaque 502 instead of a bad request.
    #[tokio::test]
    async fn an_unknown_sort_is_a_bad_request_not_a_database_error() {
        let h = TestHarness::new().await;
        let app = admin_app(&h, Some("pw"));
        let token = breakglass_token(&app, "pw").await;

        let res = app
            .oneshot(get_auth("/admin/api/views?sort=sideways", &token))
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::BAD_REQUEST);
        let body = body_json(res).await;
        assert!(body["error"].as_str().unwrap().contains("slowest"), "{body}");
    }

    /// All three are reads, so a read-scope agent token must see and be able to
    /// call every one of them. This is the whole reason they are GETs.
    #[tokio::test]
    async fn a_read_token_can_see_and_call_the_presence_tools() {
        let h = TestHarness::new().await;
        let app = admin_app_with(&h, Some("pw"), None, Some("cluster-secret"));
        let person = breakglass_token(&app, "pw").await;
        let read = mint(&app, &person, "read").await;

        let (_, list) = mcp_call(&app, &read, rpc(1, "tools/list", json!({}))).await;
        let tools = list["result"]["tools"].as_array().unwrap();
        let names: Vec<&str> = tools.iter().map(|t| t["name"].as_str().unwrap()).collect();
        for expected in ["presence", "views_list", "view_get"] {
            assert!(names.contains(&expected), "{expected} missing from {names:?}");
        }

        let views_list = tools.iter().find(|t| t["name"] == "views_list").unwrap();
        assert_eq!(views_list["annotations"]["readOnlyHint"], true);
        assert_eq!(
            views_list["inputSchema"]["properties"]["sort"]["enum"][0],
            "slowest"
        );
        let view_get = tools.iter().find(|t| t["name"] == "view_get").unwrap();
        assert_eq!(view_get["inputSchema"]["required"], json!(["key"]));

        // Dispatch really goes through the router, so the tool answers with
        // what the endpoint answers.
        let (_, called) =
            mcp_call(&app, &read, rpc(2, "tools/call", json!({ "name": "presence", "arguments": {} })))
                .await;
        assert_eq!(called["result"]["isError"], false, "{called}");
        let text = called["result"]["content"][0]["text"].as_str().unwrap();
        let parsed: Value = serde_json::from_str(text).unwrap();
        assert_eq!(parsed["ready"], false, "{parsed}");

        // And a listing with no database is the endpoint's own 503, reported as
        // a tool error rather than swallowed.
        let (_, listing) = mcp_call(
            &app,
            &read,
            rpc(3, "tools/call", json!({ "name": "views_list", "arguments": { "sort": "slowest" } })),
        )
        .await;
        assert_eq!(listing["result"]["isError"], true, "{listing}");
        assert!(listing["result"]["content"][0]["text"]
            .as_str()
            .unwrap()
            .contains("HTTP 503"));
    }

    // -----------------------------------------------------------------------
    // Tokens, scope and MCP
    // -----------------------------------------------------------------------

    fn rpc(id: u64, method: &str, params: Value) -> Value {
        json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params })
    }

    async fn mcp_call(app: &Router, token: &str, message: Value) -> (StatusCode, Value) {
        let res = app
            .clone()
            .oneshot(post_auth("/admin/api/mcp", token, message))
            .await
            .unwrap();
        let status = res.status();
        (status, body_json(res).await)
    }

    async fn mint(app: &Router, token: &str, scope: &str) -> String {
        let res = app
            .clone()
            .oneshot(post_auth(
                "/admin/api/tokens",
                token,
                json!({ "label": "agent", "scope": scope, "ttl_days": 30 }),
            ))
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::CREATED);
        let body = body_json(res).await;
        assert_eq!(body["scope"], scope);
        assert_eq!(body["endpoint"], "/admin/api/mcp");
        body["token"].as_str().unwrap().to_string()
    }

    #[tokio::test]
    async fn mcp_requires_a_token_and_rejects_get() {
        let h = TestHarness::new().await;
        let app = admin_app(&h, Some("pw"));
        let res = app
            .clone()
            .oneshot(post_auth("/admin/api/mcp", "nope", rpc(1, "ping", json!({}))))
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
        let token = breakglass_token(&app, "pw").await;
        let res = app.oneshot(get_auth("/admin/api/mcp", &token)).await.unwrap();
        assert_eq!(res.status(), StatusCode::METHOD_NOT_ALLOWED);
    }

    #[tokio::test]
    async fn mcp_speaks_the_handshake() {
        let h = TestHarness::new().await;
        let app = admin_app(&h, Some("pw"));
        let token = breakglass_token(&app, "pw").await;

        let (status, init) = mcp_call(
            &app,
            &token,
            rpc(1, "initialize", json!({ "protocolVersion": "2025-03-26", "capabilities": {}, "clientInfo": { "name": "test", "version": "0" } })),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(init["id"], 1);
        assert_eq!(init["result"]["protocolVersion"], "2025-03-26");
        assert_eq!(init["result"]["serverInfo"]["name"], "spky-admin");
        assert!(init["result"]["capabilities"]["tools"].is_object());

        // The initialized notification has no id and is acknowledged empty.
        let res = app
            .clone()
            .oneshot(post_auth("/admin/api/mcp", &token, json!({ "jsonrpc": "2.0", "method": "notifications/initialized" })))
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::ACCEPTED);

        let (_, pong) = mcp_call(&app, &token, rpc(2, "ping", json!({}))).await;
        assert_eq!(pong["result"], json!({}));

        let (_, unknown) = mcp_call(&app, &token, rpc(3, "nope/nothing", json!({}))).await;
        assert_eq!(unknown["error"]["code"], -32601);

        let (_, batch) = mcp_call(&app, &token, json!([rpc(4, "ping", json!({}))])).await;
        assert_eq!(batch["error"]["code"], -32600);

        let res = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/admin/api/mcp")
                    .header("Authorization", format!("Bearer {token}"))
                    .header("Content-Type", "application/json")
                    .body(axum::body::Body::from("{not json"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(body_json(res).await["error"]["code"], -32700);
    }

    #[tokio::test]
    async fn tools_list_follows_scope_and_calls_dispatch_through_the_router() {
        let h = TestHarness::new().await;
        h.add_ready_ssp("ssp-1", "http://10.0.0.1:8667").await;
        let app = admin_app(&h, Some("pw"));
        let token = breakglass_token(&app, "pw").await;

        let (_, full) = mcp_call(&app, &token, rpc(1, "tools/list", json!({}))).await;
        let names: Vec<&str> = full["result"]["tools"]
            .as_array()
            .unwrap()
            .iter()
            .map(|t| t["name"].as_str().unwrap())
            .collect();
        assert!(names.contains(&"overview") && names.contains(&"scheduler_restart"), "{names:?}");
        let restart = full["result"]["tools"]
            .as_array()
            .unwrap()
            .iter()
            .find(|t| t["name"] == "scheduler_restart")
            .unwrap();
        assert_eq!(restart["annotations"]["destructiveHint"], true);
        assert_eq!(restart["inputSchema"]["properties"]["mode"]["enum"][1], "reclone");

        // A tool call is the same endpoint the dashboard uses.
        let (_, ov) = mcp_call(&app, &token, rpc(2, "tools/call", json!({ "name": "overview", "arguments": {} }))).await;
        assert_eq!(ov["result"]["isError"], false, "{ov}");
        let text = ov["result"]["content"][0]["text"].as_str().unwrap();
        let parsed: Value = serde_json::from_str(text).unwrap();
        assert_eq!(parsed["totals"]["ssps"], 1);

        // A failing call is a tool error carrying the endpoint's sentence.
        let (_, missing) = mcp_call(&app, &token, rpc(3, "tools/call", json!({ "name": "ssp_restart", "arguments": { "id": "ghost" } }))).await;
        assert_eq!(missing["result"]["isError"], true, "{missing}");
        assert!(missing["result"]["content"][0]["text"].as_str().unwrap().contains("HTTP 404"));

        // A real restart flags the SSP, exactly as the button does.
        let (_, ok) = mcp_call(&app, &token, rpc(4, "tools/call", json!({ "name": "ssp_restart", "arguments": { "id": "ssp-1", "mode": "clean" } }))).await;
        assert_eq!(ok["result"]["isError"], false, "{ok}");
        assert_eq!(h.ssp_pool.read().await.pending_resync("ssp-1"), Some(scheduler::router::ResyncKind::Clean));

        // Bounded logs come back as an array, not a stream.
        let (_, logs) = mcp_call(&app, &token, rpc(5, "tools/call", json!({ "name": "logs_recent", "arguments": { "backfill": 5 } }))).await;
        assert_eq!(logs["result"]["isError"], false, "{logs}");
        let text = logs["result"]["content"][0]["text"].as_str().unwrap();
        assert!(serde_json::from_str::<Value>(text).unwrap().is_array(), "{text}");

        let (_, unknown) = mcp_call(&app, &token, rpc(6, "tools/call", json!({ "name": "nope", "arguments": {} }))).await;
        assert_eq!(unknown["error"]["code"], -32602);
    }

    #[tokio::test]
    async fn a_read_token_sees_and_does_only_reads() {
        let h = TestHarness::new().await;
        h.add_ready_ssp("ssp-1", "http://10.0.0.1:8667").await;
        let app = admin_app_with(&h, Some("pw"), None, Some("cluster-secret"));
        let person = breakglass_token(&app, "pw").await;
        let read = mint(&app, &person, "read").await;
        assert!(read.contains('.'), "signed");

        let res = app.clone().oneshot(get_auth("/admin/api/me", &read)).await.unwrap();
        let me = body_json(res).await;
        assert_eq!(me["mode"], "mcp");
        assert_eq!(me["scope"], "read");
        assert_eq!(me["label"], "agent");

        let (_, list) = mcp_call(&app, &read, rpc(1, "tools/list", json!({}))).await;
        let names: Vec<&str> = list["result"]["tools"].as_array().unwrap().iter().map(|t| t["name"].as_str().unwrap()).collect();
        assert!(names.contains(&"overview") && !names.contains(&"scheduler_restart"), "{names:?}");

        // Calling a write tool anyway is a tool error, and the raw endpoint is a 403.
        let (_, refused) = mcp_call(&app, &read, rpc(2, "tools/call", json!({ "name": "ssp_restart", "arguments": { "id": "ssp-1" } }))).await;
        assert_eq!(refused["result"]["isError"], true);
        assert!(refused["result"]["content"][0]["text"].as_str().unwrap().contains("read-only"));
        let res = app
            .clone()
            .oneshot(post_auth("/admin/api/ssps/ssp-1/restart", &read, json!({ "mode": "restart" })))
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::FORBIDDEN);
        assert!(h.ssp_pool.read().await.pending_resync("ssp-1").is_none());

        // Reads through the plain API still work for it.
        let res = app.clone().oneshot(get_auth("/admin/api/overview", &read)).await.unwrap();
        assert_eq!(res.status(), StatusCode::OK);

        // And it cannot mint.
        let res = app
            .clone()
            .oneshot(post_auth("/admin/api/tokens", &read, json!({ "label": "x", "scope": "full" })))
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::FORBIDDEN);

        // A full MCP token cannot mint either.
        let full = mint(&app, &person, "full").await;
        let res = app
            .clone()
            .oneshot(post_auth("/admin/api/tokens", &full, json!({ "label": "x", "scope": "full" })))
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn tokens_can_be_revoked_and_validated() {
        let h = TestHarness::new().await;
        let app = admin_app(&h, Some("pw"));
        let person = breakglass_token(&app, "pw").await;

        for bad in [json!({ "label": "", "scope": "read" }), json!({ "label": "x", "scope": "root" }), json!({ "label": "x", "ttl_days": 9999 })] {
            let res = app.clone().oneshot(post_auth("/admin/api/tokens", &person, bad.clone())).await.unwrap();
            assert_eq!(res.status(), StatusCode::BAD_REQUEST, "{bad}");
        }

        let token = mint(&app, &person, "full").await;
        let res = app.clone().oneshot(get_auth("/admin/api/me", &token)).await.unwrap();
        assert_eq!(res.status(), StatusCode::OK);

        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri("/admin/api/tokens")
                    .header("Authorization", format!("Bearer {person}"))
                    .header("Content-Type", "application/json")
                    .body(axum::body::Body::from(json!({ "token": token }).to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::NO_CONTENT);
        let res = app.oneshot(get_auth("/admin/api/me", &token)).await.unwrap();
        assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn an_upstream_auth_failure_is_a_gateway_error_not_a_logout() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        // Every internal route refuses the credential, as a control plane
        // that does not know this scheduler's secret would.
        let fake = Router::new().fallback(|| async {
            (StatusCode::UNAUTHORIZED, axum::Json(json!({ "error": "invalid project credentials", "code": "unauthorized" })))
        });
        tokio::spawn(async move { axum::serve(listener, fake).await.unwrap() });

        let h = TestHarness::new().await;
        let app = admin_app_with(&h, Some("pw"), Some(&format!("http://{addr}")), None);
        let token = breakglass_token(&app, "pw").await;
        // The catalog is soft: the list still answers, with the sentence.
        let res = app.clone().oneshot(get_auth("/admin/api/backups", &token)).await.unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        let body = body_json(res).await;
        assert!(body["catalog_error"].as_str().unwrap().contains("rejected this scheduler's credentials"), "{body}");
        // A hard cloud call is 502 with the same sentence, never 401.
        let res = app
            .oneshot(post_auth("/admin/api/cloud/restart", &token, json!({ "roles": ["ssp"] })))
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::BAD_GATEWAY);
        let body = body_json(res).await;
        assert_eq!(body["code"], "cloud_auth");
    }
}
