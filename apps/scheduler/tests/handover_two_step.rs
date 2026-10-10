//! The two-step scheduler handover: the predecessor releases its replica and
//! keeps taking events while the successor opens it, then commits; the
//! successor takes in the WAL tail written in between. The gate is
//! process-global, so everything runs in one test.

use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use scheduler::config::SchedulerConfig;
use scheduler::handover::{self, Mode};
use scheduler::query::QueryTracker;
use scheduler::router::SspState;
use scheduler::transport::{HttpTransport, SspInfo};
use scheduler::{Scheduler, SchedulerStatus};

async fn open_with_retry(config: &SchedulerConfig, transport: &Arc<HttpTransport>) -> Scheduler {
    for _ in 0..200 {
        match Scheduler::new(config.clone(), Arc::clone(transport)).await {
            Ok(s) => return s,
            Err(e) if handover::is_lock_error(&e) => tokio::time::sleep(Duration::from_millis(20)).await,
            Err(e) => panic!("unexpected open error: {e:#}"),
        }
    }
    panic!("the replica lock never freed");
}

fn event(id: &str) -> ssp_protocol::IngestRequest {
    ssp_protocol::IngestRequest {
        table: "note".to_string(),
        op: "CREATE".to_string(),
        id: format!("note:{id}"),
        record: serde_json::json!({ "id": format!("note:{id}"), "body": id }),
        job_assignee: None,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn release_then_commit_carries_events_taken_in_between() {
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
        pool.upsert(SspInfo {
            ingest_batch_limit: 0,
            id: "ssp-0".into(),
            url: "http://127.0.0.1:9".into(),
            version: "test".into(),
            connected_at: std::time::Instant::now(),
            last_heartbeat: std::time::Instant::now(),
            query_count: 0,
            views: 0,
            cpu_usage: None,
            memory_usage: None,
            env: None,
            bootstrap: None,
        });
        let _ = pool.mark_ready("ssp-0");
    }
    let tracker = QueryTracker::new();
    tracker.assign("q1".into(), "ssp-0".into()).await;
    handover::gate().set_mode(Mode::Serve);
    // A snapshot to resume from, as a running scheduler always has.
    scheduler::ingest::ingest_event(&blue.ingest_state(), event("before"), 0).await.unwrap();

    // Step one: released, still serving.
    let lock = tokio::sync::Mutex::new(());
    blue.release_replica("green", &lock).await.unwrap();
    assert_eq!(handover::gate().mode(), Mode::Serve, "a released predecessor serves on");
    assert_eq!(handover::gate().status().phase, "released");

    // The successor opens the replica while the predecessor still takes events.
    let green = open_with_retry(&config, &transport).await;
    let seq_at_open = green.seq_counter.load(Ordering::SeqCst);
    let after = scheduler::ingest::ingest_event(&blue.ingest_state(), event("between"), 0).await.unwrap();
    assert!(after > seq_at_open, "the predecessor kept taking events after the successor opened");

    // Step two.
    let state = blue.commit_handover("green", Default::default(), &tracker).await.unwrap();
    assert_eq!(handover::gate().mode(), Mode::Forward("green".to_string()));
    assert_eq!(handover::gate().role(), "retired");
    assert!(handover::ingest_gate().try_read().is_err());

    // The tail written in between reaches the successor's buffer and counter.
    assert_eq!(green.catch_up_wal().await.unwrap(), 1);
    assert_eq!(green.seq_counter.load(Ordering::SeqCst), after);
    assert!(green.event_buffer.read().await.iter().any(|e| e.update.record_id == "note:between"));
    // Idempotent.
    assert_eq!(green.catch_up_wal().await.unwrap(), 0);

    let green_tracker = QueryTracker::new();
    green.import_handover(state, &green_tracker).await;
    assert!(matches!(green.ssp_pool.read().await.get_state("ssp-0"), Some(SspState::Ready | SspState::Lagging)));
    assert_eq!(green_tracker.get_assignment("q1").await.as_deref(), Some("ssp-0"));
}
