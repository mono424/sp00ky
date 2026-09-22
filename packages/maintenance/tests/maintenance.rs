use anyhow::Result;
use maintenance::{
    backend_health, create_backup_channel, create_restore_channel, run_backup_worker,
    run_restore_worker, BackendStatus, BackupConfig, BackupRegistry, BackupStatus, DbConfig,
    MaintenanceHost, RestoreOutcome, RestoreProgress, RestoreRegistry, RestoreStatus,
};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::sync::Mutex;

// ---------------------------------------------------------------------------
// Registry FSMs
// ---------------------------------------------------------------------------

#[tokio::test]
async fn backup_registry_lifecycle() {
    let reg = BackupRegistry::new();
    reg.enqueue("b1".into(), "proj".into()).await;
    assert!(reg.contains("b1").await);
    assert_eq!(reg.queue_len().await, 1);

    reg.mark_running("b1").await;
    assert_eq!(reg.queue_len().await, 0);
    assert!(reg.current_running().await.is_some());

    reg.mark_completed("b1", 42, Some(7), "proj/b1.surql.gz".into())
        .await;
    let s = reg.get("b1").await.unwrap();
    assert!(matches!(s.status, BackupStatus::Completed));
    assert_eq!(s.size_bytes, Some(42));
    assert_eq!(s.snapshot_seq, Some(7));

    // snapshot_seq is host-optional (None for a standalone SSP)
    reg.enqueue("b2".into(), "proj".into()).await;
    reg.mark_completed("b2", 1, None, "proj/b2.surql.gz".into())
        .await;
    assert_eq!(reg.get("b2").await.unwrap().snapshot_seq, None);
}

#[tokio::test]
async fn restore_registry_lifecycle() {
    let reg = RestoreRegistry::new();
    reg.enqueue("r1".into(), "b1".into(), "proj".into(), "proj/b1.surql.gz".into())
        .await;
    reg.mark_running("r1").await;

    reg.mark_completed(
        "r1",
        RestoreOutcome {
            snapshot_seq: None,
            pending_cleared: 0,
            main_db_restored: true,
            host_state_restored: true,
            ssps_evicted: None,
        },
    )
    .await;
    let s = reg.get("r1").await.unwrap();
    assert!(matches!(s.status, RestoreStatus::Completed));
    assert!(s.main_db_restored);
    assert!(s.replica_restored); // serialized name for host_state_restored
    assert_eq!(s.ssps_evicted, None);

    reg.enqueue("r2".into(), "b1".into(), "proj".into(), "p".into())
        .await;
    reg.mark_failed(
        "r2",
        "boom".into(),
        RestoreProgress {
            gate_entered: true,
            main_db_restored: true,
            host_state_restored: false,
        },
    )
    .await;
    let s = reg.get("r2").await.unwrap();
    assert!(matches!(s.status, RestoreStatus::Failed));
    assert!(s.main_db_restored);
    assert!(!s.replica_restored);
    assert_eq!(s.error.as_deref(), Some("boom"));
}

// ---------------------------------------------------------------------------
// Env parsing (single test — env vars are process-global)
// ---------------------------------------------------------------------------

#[test]
fn env_parsing_and_fallbacks() {
    // SPKY_SCHEDULER_BACKENDS accepted as legacy fallback…
    unsafe {
        std::env::remove_var("SPKY_BACKENDS");
        std::env::set_var(
            "SPKY_SCHEDULER_BACKENDS",
            r#"[{"name":"api","url":"http://api:3000","healthcheck":"/health"}]"#,
        );
    }
    let backends = backend_health::backends_from_env();
    assert_eq!(backends.len(), 1);
    assert_eq!(backends[0].name, "api");

    // …but SPKY_BACKENDS wins when both are set.
    unsafe {
        std::env::set_var(
            "SPKY_BACKENDS",
            r#"[{"name":"a","url":"http://a:1","healthcheck":"/h"},{"name":"b","url":"http://b:1","healthcheck":"/h"}]"#,
        );
    }
    let backends = backend_health::backends_from_env();
    assert_eq!(backends.len(), 2);

    unsafe {
        std::env::remove_var("SPKY_BACKENDS");
        std::env::remove_var("SPKY_SCHEDULER_BACKENDS");
    }
    assert!(backend_health::backends_from_env().is_empty());

    // BackupConfig env round-trip.
    unsafe {
        std::env::set_var("S3_ENDPOINT", "http://minio:9000");
        std::env::set_var("S3_BUCKET", "test-bucket");
    }
    let cfg = BackupConfig::from_env();
    assert_eq!(cfg.s3_endpoint, "http://minio:9000");
    assert_eq!(cfg.s3_bucket, "test-bucket");
    assert!(BackupConfig::env_configured());
    unsafe {
        std::env::remove_var("S3_ENDPOINT");
        std::env::remove_var("S3_BUCKET");
    }
}

// ---------------------------------------------------------------------------
// Worker + host classification
// ---------------------------------------------------------------------------

struct MockHost {
    begin_called: AtomicUsize,
    finish_called: AtomicUsize,
    finish_saw_gate: AtomicBool,
}

impl MockHost {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            begin_called: AtomicUsize::new(0),
            finish_called: AtomicUsize::new(0),
            finish_saw_gate: AtomicBool::new(false),
        })
    }
}

#[async_trait::async_trait]
impl MaintenanceHost for MockHost {
    async fn pre_backup(&self) -> Result<Option<u64>> {
        Ok(None)
    }
    async fn begin_restore(&self) -> Result<()> {
        self.begin_called.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
    async fn post_restore(&self, _dump: &std::path::Path) -> Result<RestoreOutcome> {
        unreachable!("post_restore must not run when the S3 download fails")
    }
    async fn finish_restore(&self, _r: &Result<RestoreOutcome>, progress: RestoreProgress) {
        self.finish_called.fetch_add(1, Ordering::SeqCst);
        self.finish_saw_gate.store(progress.gate_entered, Ordering::SeqCst);
    }
}

fn unreachable_s3() -> Arc<BackupConfig> {
    Arc::new(BackupConfig {
        s3_endpoint: "http://127.0.0.1:1".into(), // nothing listens here
        s3_access_key: "x".into(),
        s3_secret_key: "x".into(),
        s3_bucket: "none".into(),
        s3_region: "us-east-1".into(),
    })
}

fn unreachable_db() -> Arc<DbConfig> {
    Arc::new(DbConfig {
        url: "http://127.0.0.1:1".into(),
        namespace: "t".into(),
        database: "t".into(),
        username: "root".into(),
        password: "root".into(),
    })
}

/// A restore whose S3 download fails must fail BEFORE the host gate:
/// `begin_restore`/`finish_restore` never run and the job is marked failed
/// with no progress flags set.
#[tokio::test]
async fn restore_failure_before_gate_skips_host_hooks() {
    let host = MockHost::new();
    let registry = Arc::new(RestoreRegistry::new());
    let (tx, rx) = create_restore_channel();
    let lock = Arc::new(Mutex::new(()));

    let worker = tokio::spawn(run_restore_worker(
        rx,
        host.clone() as Arc<dyn MaintenanceHost>,
        unreachable_s3(),
        unreachable_db(),
        registry.clone(),
        lock,
    ));

    registry
        .enqueue("r1".into(), "b1".into(), "proj".into(), "proj/missing.gz".into())
        .await;
    tx.send(maintenance::RestoreJob {
        restore_id: "r1".into(),
        backup_id: "b1".into(),
        project_slug: "proj".into(),
        storage_path: "proj/missing.gz".into(),
    })
    .await
    .unwrap();
    drop(tx); // close channel so the worker exits after the job
    worker.await.unwrap();

    let s = registry.get("r1").await.unwrap();
    assert!(matches!(s.status, RestoreStatus::Failed));
    assert!(!s.main_db_restored);
    assert!(!s.replica_restored);
    assert_eq!(host.begin_called.load(Ordering::SeqCst), 0);
    assert_eq!(host.finish_called.load(Ordering::SeqCst), 0);
}

/// A backup whose DB export fails is marked failed; `pre_backup` ran first.
#[tokio::test]
async fn backup_failure_marks_registry() {
    struct PreBackupHost(AtomicUsize);
    #[async_trait::async_trait]
    impl MaintenanceHost for PreBackupHost {
        async fn pre_backup(&self) -> Result<Option<u64>> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(Some(3))
        }
        async fn begin_restore(&self) -> Result<()> {
            unreachable!()
        }
        async fn post_restore(&self, _d: &std::path::Path) -> Result<RestoreOutcome> {
            unreachable!()
        }
        async fn finish_restore(&self, _r: &Result<RestoreOutcome>, _p: RestoreProgress) {
            unreachable!()
        }
    }

    let host = Arc::new(PreBackupHost(AtomicUsize::new(0)));
    let registry = Arc::new(BackupRegistry::new());
    let (tx, rx) = create_backup_channel();
    let lock = Arc::new(Mutex::new(()));

    let worker = tokio::spawn(run_backup_worker(
        rx,
        host.clone() as Arc<dyn MaintenanceHost>,
        unreachable_s3(),
        unreachable_db(),
        registry.clone(),
        lock,
    ));

    registry.enqueue("b1".into(), "proj".into()).await;
    tx.send(maintenance::BackupJob {
        backup_id: "b1".into(),
        project_slug: "proj".into(),
    })
    .await
    .unwrap();
    drop(tx);
    worker.await.unwrap();

    let s = registry.get("b1").await.unwrap();
    assert!(matches!(s.status, BackupStatus::Failed));
    assert_eq!(host.0.load(Ordering::SeqCst), 1);
}

// ---------------------------------------------------------------------------
// Backend health monitor (wiremock)
// ---------------------------------------------------------------------------

/// A pool backend at zero machines is `idle`, not down: there is no container
/// to probe and nothing wrong. The whole mapping, since it is what decides
/// whether a scaled-to-zero backend reads as an outage.
#[test]
fn pool_backing_status_is_idle_at_zero_machines() {
    use backend_health::PoolBacking;
    let rest = PoolBacking {
        pool: "render".into(),
        ready: 0,
        starting: 0,
        busy_slots: 0,
        queued: 0,
        paused: false,
        breaker_open: false,
        error: None,
    };
    assert_eq!(rest.status(), BackendStatus::Idle);
    assert_eq!(PoolBacking { queued: 2, ..rest.clone() }.status(), BackendStatus::Starting);
    assert_eq!(PoolBacking { starting: 1, ..rest.clone() }.status(), BackendStatus::Starting);
    // Paused with work queued: nothing is coming, so nothing is starting.
    assert_eq!(
        PoolBacking { queued: 2, paused: true, ..rest.clone() }.status(),
        BackendStatus::Idle
    );
    assert_eq!(PoolBacking { ready: 1, ..rest.clone() }.status(), BackendStatus::Healthy);
    assert_eq!(
        PoolBacking { ready: 1, breaker_open: true, ..rest.clone() }.status(),
        BackendStatus::Unhealthy
    );
    assert_eq!(
        PoolBacking { error: Some("no provider".into()), ..rest }.status(),
        BackendStatus::Unhealthy
    );
    assert!(!BackendStatus::Idle.is_failing() && !BackendStatus::Starting.is_failing());
}

/// The regression: a pool backend is pushed to the scheduler with a URL nothing
/// listens on (its machines come and go). The prober must leave it alone once
/// the pool sweep has claimed it, and the sweep's word must survive the
/// prober's sweeps. Un-claiming hands it back to the prober.
#[tokio::test]
async fn pool_backed_backend_is_not_probed() {
    use backend_health::PoolBacking;
    let renderer = backend_health::BackendHealthConfig {
        name: "renderer".into(),
        url: "http://127.0.0.1:1".into(),
        healthcheck: "/health".into(),
        port: None,
        env: None,
    };
    let cache = backend_health::create_health_cache(&[renderer.clone()]);
    let configs = backend_health::create_shared_configs(&[renderer]);
    let client = backend_health::health_http_client();

    let idle = PoolBacking {
        pool: "render".into(),
        ready: 0,
        starting: 0,
        busy_slots: 0,
        queued: 0,
        paused: false,
        breaker_open: false,
        error: None,
    };
    backend_health::set_pool_backing(&cache, &[("renderer".into(), idle.clone())]).await;
    backend_health::check_backends_once(&configs, &cache, &client).await;
    {
        let e = &cache.read().await[0];
        assert_eq!(e.status, BackendStatus::Idle, "the prober did not touch it");
        assert!(e.history.is_empty(), "no probe ran");
        assert_eq!(e.pool.as_ref(), Some(&idle));
        assert!(e.last_checked.is_some());
    }

    // The pool list without it: back to the prober, which finds it unreachable.
    backend_health::set_pool_backing(&cache, &[]).await;
    assert_eq!(cache.read().await[0].status, BackendStatus::Unknown);
    backend_health::check_backends_once(&configs, &cache, &client).await;
    let e = &cache.read().await[0];
    assert_eq!(e.status, BackendStatus::Unreachable);
    assert!(e.pool.is_none());
}

#[tokio::test]
async fn health_monitor_tracks_backend_status_and_live_updates() {
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/health"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&server)
        .await;

    let good = backend_health::BackendHealthConfig {
        name: "good".into(),
        url: server.uri(),
        healthcheck: "/health".into(),
        port: None,
        env: None,
    };
    let dead = backend_health::BackendHealthConfig {
        name: "dead".into(),
        url: "http://127.0.0.1:1".into(),
        healthcheck: "/health".into(),
        port: None,
        env: None,
    };

    let cache = backend_health::create_health_cache(&[good.clone(), dead.clone()]);
    let configs = backend_health::create_shared_configs(&[good.clone(), dead.clone()]);
    backend_health::start_backend_health_monitor(configs.clone(), cache.clone(), 1);

    // Wait for the first sweep to classify both backends.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        let entries = cache.read().await;
        let good_ok = entries.iter().any(|e| e.name == "good" && e.status == BackendStatus::Healthy);
        let dead_ko = entries
            .iter()
            .any(|e| e.name == "dead" && e.status == BackendStatus::Unreachable);
        if good_ok && dead_ko {
            break;
        }
        drop(entries);
        assert!(std::time::Instant::now() < deadline, "monitor never classified backends");
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }

    // Live update via update_backends (the PUT /backends path): drop `dead`,
    // keep `good` — its Healthy status must survive the reconcile.
    backend_health::update_backends(&configs, &cache, vec![good]).await;
    let entries = cache.read().await;
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].name, "good");
    assert_eq!(entries[0].status, BackendStatus::Healthy);
}
