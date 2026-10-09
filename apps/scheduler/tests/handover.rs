//! Scheduler blue/green handover, in process: a serving scheduler hands its
//! replica, SSP pool and view assignments to a successor on the same data
//! directory, and the successor opens the replica the moment it is released.
//! The gate is process-global, so everything runs in one test.

use std::sync::Arc;

use scheduler::config::SchedulerConfig;
use scheduler::handover::{self, Mode, Refusal};
use scheduler::query::QueryTracker;
use scheduler::router::SspState;
use scheduler::transport::{HttpTransport, SspInfo};
use scheduler::{Scheduler, SchedulerStatus};

fn ssp(id: &str) -> SspInfo {
    SspInfo {
        id: id.to_string(),
        url: format!("http://{id}:8667"),
        version: "test".to_string(),
        connected_at: std::time::Instant::now(),
        last_heartbeat: std::time::Instant::now(),
        query_count: 1,
        views: 1,
        cpu_usage: None,
        memory_usage: None,
        env: None,
        bootstrap: None,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_scheduler_hands_its_replica_and_pool_to_its_successor() {
    let dir = tempfile::tempdir().unwrap();
    let config = SchedulerConfig {
        replica_db_path: dir.path().join("replica"),
        wal_path: dir.path().join("event_wal.log"),
        ..Default::default()
    };
    let transport = Arc::new(HttpTransport::new());

    let blue = Arc::new(Scheduler::new(config.clone(), Arc::clone(&transport)).await.unwrap());
    *blue.status.write().await = SchedulerStatus::Ready;
    {
        let mut pool = blue.ssp_pool.write().await;
        pool.upsert(ssp("ssp-0"));
        let _ = pool.mark_ready("ssp-0");
    }
    let blue_tracker = QueryTracker::new();
    blue_tracker.assign("q1".into(), "ssp-0".into()).await;
    handover::gate().set_mode(Mode::Serve);

    // While blue holds the replica nobody else can open it.
    assert!(
        Scheduler::new(config.clone(), Arc::clone(&transport)).await.is_err(),
        "the replica lock is held by the serving scheduler"
    );

    let backup_lock = tokio::sync::Mutex::new(());
    let state = blue.hand_over("green-host", Default::default(), &blue_tracker, &backup_lock).await.unwrap();
    assert_eq!(handover::gate().mode(), Mode::Forward("green-host".to_string()));
    assert_eq!(handover::gate().role(), "retired");
    assert!(handover::ingest_gate().try_read().is_err(), "blue takes no event after handing over");
    assert!(matches!(
        blue.hand_over("someone-else", Default::default(), &blue_tracker, &backup_lock).await,
        Err(Refusal::Gone(_))
    ));

    // The state survives the wire.
    let state: handover::HandoverState =
        serde_json::from_str(&serde_json::to_string(&state).unwrap()).unwrap();

    // Green opens the very replica blue released, without waiting for blue to
    // exit. The engine lets go of the lock a few ms after the close, which is
    // why green retries the open (main.rs `open_scheduler` does the same).
    let mut green = None;
    for _ in 0..100 {
        match Scheduler::new(config.clone(), Arc::clone(&transport)).await {
            Ok(s) => {
                green = Some(s);
                break;
            }
            Err(e) if handover::is_lock_error(&e) => tokio::time::sleep(std::time::Duration::from_millis(20)).await,
            Err(e) => panic!("unexpected open error: {e:#}"),
        }
    }
    let green = green.expect("the replica lock frees within 2 s of the handover");
    let green_tracker = QueryTracker::new();
    green.import_handover(state, &green_tracker).await;

    let pool = green.ssp_pool.read().await;
    assert_eq!(pool.get_state("ssp-0"), Some(&SspState::Ready));
    assert_eq!(pool.get("ssp-0").map(|s| s.url.as_str()), Some("http://ssp-0:8667"));
    drop(pool);
    assert_eq!(green_tracker.get_assignment("q1").await.as_deref(), Some("ssp-0"));
}
