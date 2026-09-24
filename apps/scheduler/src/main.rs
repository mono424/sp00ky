use anyhow::Result;
use scheduler::config::SchedulerConfig;
use scheduler::transport::HttpTransport;
use std::sync::Arc;
use tracing::{debug, error, info};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::EnvFilter;

fn main() -> Result<()> {
    // Explicit worker count instead of `#[tokio::main]`'s
    // `available_parallelism()`: production deploys default to a 1-vCPU
    // cgroup, which yields a SINGLE worker thread — one blocking call inside
    // the embedded SurrealDB/RocksDB replica (e.g. a WriteBufferManager
    // write stall, observed 2026-08-08) then parks the whole runtime: no IO
    // polling, no timers, total HTTP silence while the process stays alive.
    let workers = std::env::var("SPKY_WORKER_THREADS")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .filter(|n| *n > 0)
        .unwrap_or(4);
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(workers)
        .enable_all()
        .build()?
        .block_on(run())
}

async fn run() -> Result<()> {
    // Initialize tracing. Without RUST_LOG, keep third-party crates at warn so
    // the scheduler's own lines aren't buried. `SPKY_LOG_FORMAT=compact` (set
    // by `spky dev`) drops the timestamp: the CLI re-prefixes each line anyway.
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("scheduler=info,warn"))
        .add_directive("scheduler::incident=info".parse().expect("static incident directive"));
    let compact = std::env::var("SPKY_LOG_FORMAT").as_deref() == Ok("compact");

    // A second sink beside stdout: a bounded in-memory ring the admin
    // dashboard tails over SSE. Composed as a layer so `docker logs` behaviour
    // — including the compact/no-timestamp form `spky dev` relies on — is
    // completely unchanged.
    let log_ring = maintenance::log_ring::LogRing::new(10_000);
    let ring_layer = maintenance::log_ring::LogRingLayer::new(Arc::clone(&log_ring));

    let registry = tracing_subscriber::registry().with(filter).with(ring_layer);
    if compact {
        registry
            .with(tracing_subscriber::fmt::layer().compact().without_time())
            .init();
    } else {
        registry.with(tracing_subscriber::fmt::layer()).init();
    }

    // Resolve the local IP once, off the runtime — `hostname -I` is a
    // blocking fork+exec and used to run on every `/info` request.
    scheduler::metrics::init_local_ip().await;

    info!(
        "scheduler v{} starting (built {})",
        env!("CARGO_PKG_VERSION"),
        env!("SPOOKY_BUILD_TIMESTAMP"),
    );
    debug!(
        "\n ____        _              _       _\n/ ___|  ___| |__   ___  __| |_   _| | ___ _ __\n\\___ \\ / __| '_ \\ / _ \\/ _` | | | | |/ _ \\ '__|\n ___) | (__| | | |  __/ (_| | |_| | |  __/ |\n|____/ \\___|_| |_|\\___\\|\\__,_|\\__,_|_|\\___|_|    v{}\n\nSp00ky Cluster Scheduler",
        env!("CARGO_PKG_VERSION"),
    );

    // Load configuration
    let config = SchedulerConfig::load()?;
    
    // Initialize transport (HTTP)
    let transport = Arc::new(HttpTransport::new());
    
    // Create scheduler
    let scheduler = scheduler::Scheduler::new(config.clone(), transport.clone()).await?;
    
    // Create shared trackers for state consistency
    let query_tracker = std::sync::Arc::new(scheduler::query::QueryTracker::new());
    let job_tracker = std::sync::Arc::new(scheduler::job_scheduler::JobTracker::new());
    
    // Create HTTP server with all routers
    let ingest_router = scheduler::ingest::create_ingest_router(scheduler.ingest_state());
    
    let query_state = scheduler::query::QueryState {
        ssp_pool: std::sync::Arc::clone(&scheduler.ssp_pool),
        transport: std::sync::Arc::clone(&transport),
        query_tracker: std::sync::Arc::clone(&query_tracker),
    };
    let query_router = scheduler::query::create_query_router(query_state.clone());
    // The changefeed tail tears down views through the same tracker.
    scheduler.attach_query_state(query_state.clone());
    
    let job_state = scheduler::job_scheduler::JobState {
        ssp_pool: std::sync::Arc::clone(&query_state.ssp_pool),
        transport: std::sync::Arc::clone(&transport),
        job_tracker: std::sync::Arc::clone(&job_tracker),
    };
    let job_router = scheduler::job_scheduler::create_job_router(job_state.clone());

    let ssp_mgmt_state = scheduler::ssp_management::SspManagementState {
        ssp_pool: std::sync::Arc::clone(&query_state.ssp_pool),
        replica: scheduler.replica.clone(),
        transport: std::sync::Arc::clone(&transport),
        config: std::sync::Arc::new(config.clone()),
        status: scheduler.status.clone(),
        event_buffer: scheduler.event_buffer.clone(),
        seq_counter: std::sync::Arc::clone(&scheduler.seq_counter),
        reclone_lock: scheduler.reclone_lock.clone(),
        wal: scheduler.wal.clone(),
        drain_lock: scheduler.drain_lock.clone(),
        changefeed: std::sync::Arc::clone(&scheduler.changefeed),
        schema: std::sync::Arc::clone(&scheduler.schema),
    };
    let ssp_router = scheduler::ssp_management::create_ssp_router(ssp_mgmt_state);

    let proxy_router = scheduler::proxy::create_proxy_router(scheduler.proxy_state());

    // Create backend health cache and shared configs for live updates
    let backend_health_cache = scheduler::backend_health::create_health_cache(&config.backends);
    let shared_backend_configs = scheduler::backend_health::create_shared_configs(&config.backends);
    // Keeps the pushed list across restarts; before the first metrics_state().
    scheduler.attach_backend_registry(scheduler::backend_registry::BackendRegistry::new(
        shared_backend_configs.clone(),
        backend_health_cache.clone(),
        !config.backends.is_empty(),
    ));
    scheduler::backend_health::start_backend_health_monitor(
        shared_backend_configs.clone(),
        backend_health_cache.clone(),
        config.health_check_interval_secs,
    );

    // The admin plane builds its own MetricsState over the SAME caches, so the
    // dashboard and `/info` can never disagree about backend health.
    let backend_health_cache_for_admin = backend_health_cache.clone();
    let shared_backend_configs_for_admin = shared_backend_configs.clone();
    // And the pool sweep writes pool backends' status into the same cache.
    let backend_health_cache_for_pools = backend_health_cache.clone();

    let metrics_router = scheduler::metrics::create_metrics_router(
        scheduler.metrics_state(
            std::sync::Arc::clone(&query_tracker),
            std::sync::Arc::clone(&job_tracker),
            backend_health_cache,
            shared_backend_configs,
        )
    );
    
    let backup_config = Arc::new(scheduler::backup::BackupConfig::from_env());
    let backup_registry = Arc::new(scheduler::backup::BackupRegistry::new());
    let (backup_tx, backup_rx) = scheduler::backup::create_backup_channel();
    let restore_registry = Arc::new(scheduler::restore::RestoreRegistry::new());
    let (restore_tx, restore_rx) = scheduler::restore::create_restore_channel();
    let backup_restore_lock = Arc::new(tokio::sync::Mutex::new(()));
    let maintenance_host: Arc<dyn maintenance::MaintenanceHost> = scheduler.maintenance_host();
    let backup_state = Arc::new(maintenance::BackupState {
        host: Arc::clone(&maintenance_host),
        config: Arc::clone(&backup_config),
        registry: Arc::clone(&backup_registry),
        tx: backup_tx.clone(),
        restore_registry: Arc::clone(&restore_registry),
        restore_tx: restore_tx.clone(),
        backup_restore_lock: Arc::clone(&backup_restore_lock),
    });
    let backup_router = scheduler::backup::create_backup_router((*backup_state).clone());

    // Global request deadline: a handler that never resolves must produce a
    // 408, never an indefinitely-hung connection (the wedge signature was
    // /health and /ingest hanging forever while TCP kept accepting).
    // Generous because /proxy serves whole bootstrap pages from RocksDB.
    let http_timeout_secs = std::env::var("SPKY_HTTP_TIMEOUT_SECS")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .filter(|n| *n > 0)
        .unwrap_or(120);
    let app = axum::Router::new()
        .merge(ingest_router)
        .merge(query_router)
        .merge(job_router)
        .merge(ssp_router)
        .merge(proxy_router)
        .merge(metrics_router)
        .merge(backup_router)
        .merge(scheduler::impersonation::create_impersonation_router(
            scheduler::impersonation::ImpersonationState::from_env(std::sync::Arc::clone(
                &scheduler.db_slot,
            )),
        ))
        .layer(tower_http::timeout::TimeoutLayer::with_status_code(
            axum::http::StatusCode::REQUEST_TIMEOUT,
            std::time::Duration::from_secs(http_timeout_secs),
        ));
    
    let ingest_addr = format!("{}:{}", 
        scheduler.config().ingest_host.as_deref().unwrap_or("0.0.0.0"),
        scheduler.config().ingest_port
    );

    // Admin plane, on its OWN listener. Everything merged into `app` above is
    // unauthenticated on the assumption that the ingest port is private; the
    // dashboard is the first surface meant for a browser, so it gets a port an
    // operator can publish without publishing `/proxy/query` alongside it.
    // Machine pools: built before the admin plane, which routes pool job kills
    // through it; started further down with the other background work.
    let pool_config = scheduler::pool_engine::PoolHostConfig::from_env();
    let pool_host = pool_config.enabled.then(|| {
        let host = scheduler::pool_engine::PoolHost::new(
            pool_config.clone(),
            std::sync::Arc::clone(&scheduler.db_slot),
        );
        // So the schedule engine's job kill reaches pool jobs too.
        host.install_global();
        host
    });

    let admin_config = scheduler::admin::AdminConfig::from_env();
    let admin_server = if admin_config.enabled {
        let admin_addr = admin_config.bind_addr();
        let cloud = scheduler::admin::cloud::CloudLink::from_env();
        match &cloud {
            Some(link) => info!(api = %link.api_url, project = %link.project, "Linked to Sp00ky Cloud"),
            None => info!("Not linked to Sp00ky Cloud (SPKY_CLOUD_API_URL / SPKY_CLOUD_PROJECT unset); cloud-only dashboard actions are off"),
        }
        let (_admin_state, admin_router) = scheduler::admin::build(
            admin_config,
            scheduler::admin::AdminDeps {
                metrics: scheduler.metrics_state(
                    std::sync::Arc::clone(&query_tracker),
                    std::sync::Arc::clone(&job_tracker),
                    backend_health_cache_for_admin,
                    shared_backend_configs_for_admin,
                ),
                transport: std::sync::Arc::clone(&transport),
                logs: std::sync::Arc::clone(&log_ring),
                db_config: scheduler.config().db.clone(),
                db_slot: std::sync::Arc::clone(&scheduler.db_slot),
                backup: Arc::clone(&backup_state),
                resync: scheduler::ssp_management::ResyncArgs {
                    ssp_pool: std::sync::Arc::clone(&scheduler.ssp_pool),
                    replica: std::sync::Arc::clone(&scheduler.replica),
                    config: Arc::new(scheduler.config().clone()),
                    status: std::sync::Arc::clone(&scheduler.status),
                    seq_counter: std::sync::Arc::clone(&scheduler.seq_counter),
                    reclone_lock: scheduler.reclone_lock.clone(),
                    changefeed: std::sync::Arc::clone(&scheduler.changefeed),
                },
                cloud,
                auth_secret: std::env::var("SPKY_AUTH_SECRET").ok().filter(|s| !s.is_empty()),
                // The control plane runs containers under `unless-stopped`;
                // anything else (a checkout, a bare host) is on its own.
                supervised: std::env::var("SPKY_ENV").map(|v| v == "cloud").unwrap_or(false),
                pools: pool_host.clone(),
            },
        );
        // No global TimeoutLayer here, unlike the ingest app: `/admin/api/logs`
        // and `/admin/api/workflows/stream` are SSE streams that are SUPPOSED
        // to stay open indefinitely, and a request deadline would sever them on
        // a timer. The admin plane has no long-blocking handlers to protect.
        Some((admin_addr, admin_router))
    } else {
        info!("Admin dashboard disabled (SPKY_ADMIN_ENABLED)");
        None
    };
    
    // Start background monitors
    scheduler::metrics::start_query_reassignment_monitor(
        std::sync::Arc::clone(&query_state.ssp_pool),
        std::sync::Arc::clone(&query_tracker),
        // Same budget the snapshot updater uses for hung bootstraps; the
        // heartbeat-staleness timeout must never apply to an SSP that has not
        // started heartbeating yet.
        std::time::Duration::from_secs(scheduler.config().bootstrap_timeout_secs + 60),
    ).await;
    
    scheduler::job_scheduler::start_job_recovery_sweep(
        std::sync::Arc::clone(&job_state.ssp_pool),
        std::sync::Arc::clone(&transport),
        std::sync::Arc::clone(&scheduler.db_slot),
    ).await;

    // Declarative schedules + workflows. The scheduler is the cluster's single
    // ticker (SSPs leave their engine unbuilt in cluster mode).
    scheduler::schedule_engine::start_schedule_sweep(
        std::sync::Arc::clone(&job_state.ssp_pool),
        std::sync::Arc::clone(&transport),
        std::sync::Arc::clone(&scheduler.db_slot),
    );

    // Machine pools: jobs that run on their own, autoscaled machines. Same shape
    // as the schedule engine above (the scheduler is the single ticker), plus a
    // dedicated, token-authenticated listener for the machines to dial into.
    let pool_server = match pool_host {
        Some(host) => {
            host.start_sweep(backend_health_cache_for_pools);
            Some((pool_config.bind_addr(), host.router()))
        }
        None => {
            info!("Machine pools disabled (SPKY_POOL_ENABLED)");
            None
        }
    };

    // Spawn the single-consumer backup worker
    {
        let host = Arc::clone(&maintenance_host);
        let config = Arc::clone(&backup_config);
        let db_config = Arc::new(scheduler.config().db.clone());
        let registry = Arc::clone(&backup_registry);
        let lock = Arc::clone(&backup_restore_lock);
        tokio::spawn(async move {
            scheduler::backup::run_backup_worker(
                backup_rx, host, config, db_config, registry, lock,
            )
            .await;
        });
    }

    // Spawn the single-consumer restore worker
    {
        let host = Arc::clone(&maintenance_host);
        let s3_config = Arc::clone(&backup_config);
        let db_config = Arc::new(scheduler.config().db.clone());
        let registry = Arc::clone(&restore_registry);
        let lock = Arc::clone(&backup_restore_lock);
        tokio::spawn(async move {
            scheduler::restore::run_restore_worker(
                restore_rx, host, s3_config, db_config, registry, lock,
            )
            .await;
        });
    }

    info!("Started background monitors for query reassignment, job failover, backups, and restores");
    
    // Spawn HTTP server
    let server_handle = tokio::spawn(async move {
        info!("Starting HTTP server on {}...", ingest_addr);
        let listener = tokio::net::TcpListener::bind(&ingest_addr)
            .await
            .expect("Failed to bind port");
        
        axum::serve(listener, app)
            .await
            .expect("HTTP server failed");
    });

    // Spawn the admin server. A failure to bind it is loud but NOT fatal: an
    // occupied admin port must not take down sync for every client just to
    // deny an operator a dashboard.
    let admin_handle = tokio::spawn(async move {
        let Some((addr, router)) = admin_server else {
            // Nothing to serve; park forever so the select! arm never fires.
            std::future::pending::<()>().await;
            return;
        };
        info!("Starting admin server on {}...", addr);
        match tokio::net::TcpListener::bind(&addr).await {
            Ok(listener) => {
                // `into_make_service_with_connect_info` so the login handler
                // can see the peer address it rate-limits on.
                if let Err(e) = axum::serve(
                    listener,
                    router.into_make_service_with_connect_info::<std::net::SocketAddr>(),
                )
                .await
                {
                    error!(error = %e, "Admin server failed");
                }
            }
            Err(e) => {
                error!(addr = %addr, error = %e, "Failed to bind the admin port; dashboard unavailable");
            }
        }
        std::future::pending::<()>().await;
    });
    
    // Spawn the pool listener. Like the admin port, failing to bind it is loud
    // but not fatal: sync must not go down because pool machines cannot dial in.
    // No request deadline on this listener: a poll is SUPPOSED to be held open.
    tokio::spawn(async move {
        let Some((addr, router)) = pool_server else { return };
        info!("Starting pool listener on {}...", addr);
        match tokio::net::TcpListener::bind(&addr).await {
            Ok(listener) => {
                if let Err(e) = axum::serve(listener, router).await {
                    error!(error = %e, "Pool listener failed");
                }
            }
            Err(e) => error!(addr = %addr, error = %e, "Failed to bind the pool port; pool machines cannot connect"),
        }
    });

    // Handle graceful shutdown
    let (shutdown_tx, mut shutdown_rx) = tokio::sync::oneshot::channel();
    
    tokio::spawn(async move {
        tokio::signal::ctrl_c()
            .await
            .expect("Failed to listen for ctrl-c");
        info!("Received shutdown signal");
        let _ = shutdown_tx.send(());
    });
    
    // Start scheduler.
    //
    // A failed start() is fatal and must LOOK fatal: the process exits
    // non-zero so the container restart policy retries it and the exit code
    // says why. The previous `eprintln!` let the task end quietly, main return
    // `Ok(())`, and the container come back with no signal that bootstrap had
    // failed at all.
    let scheduler_handle = tokio::spawn(async move {
        if let Err(e) = scheduler.start().await {
            error!(error = %e, "Scheduler failed to start — exiting for restart");
            std::process::exit(1);
        }
    });
    
    // Wait for shutdown or error
    tokio::select! {
        _ = &mut shutdown_rx => {
            info!("Shutting down...");
        }
        _ = server_handle => info!("HTTP server stopped"),
        _ = admin_handle => info!("Admin server stopped"),
        _ = scheduler_handle => info!("Scheduler stopped"),
    }

    Ok(())
}
