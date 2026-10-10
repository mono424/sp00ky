use super::*;

#[derive(Clone, Default)]
struct BatchPeer {
    attempts: Arc<tokio::sync::Mutex<Vec<(String, Value)>>>,
    accepted: Arc<tokio::sync::Mutex<Vec<Value>>>,
    started: Arc<tokio::sync::Notify>,
    release: Arc<tokio::sync::Notify>,
    fail_batch: Arc<std::sync::atomic::AtomicBool>,
}

impl BatchPeer {
    async fn start(fail_batch: bool) -> (Self, String) {
        let peer = Self::default();
        peer.fail_batch.store(fail_batch, Ordering::SeqCst);
        let app = Router::new()
            .route(
                "/ingest",
                axum::routing::post({
                    let peer = peer.clone();
                    move |axum::Json(body): axum::Json<Value>| {
                        let peer = peer.clone();
                        async move {
                            if body["id"] == "thread:0" {
                                peer.started.notify_one();
                                peer.release.notified().await;
                            }
                            peer.attempts
                                .lock()
                                .await
                                .push(("/ingest".into(), body.clone()));
                            peer.accepted.lock().await.push(body);
                            StatusCode::OK
                        }
                    }
                }),
            )
            .route(
                "/ingest/batch",
                axum::routing::post({
                    let peer = peer.clone();
                    move |axum::Json(body): axum::Json<Value>| {
                        let peer = peer.clone();
                        async move {
                            peer.attempts
                                .lock()
                                .await
                                .push(("/ingest/batch".into(), body.clone()));
                            if peer.fail_batch.swap(false, Ordering::SeqCst) {
                                return StatusCode::SERVICE_UNAVAILABLE;
                            }
                            peer.accepted
                                .lock()
                                .await
                                .extend(body["records"].as_array().unwrap().iter().cloned());
                            StatusCode::OK
                        }
                    }
                }),
            );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        (peer, url)
    }
}

fn change(table: &str, i: usize, blob: usize) -> ssp_protocol::IngestRequest {
    ssp_protocol::IngestRequest {
        table: table.into(),
        op: "CREATE".into(),
        id: format!("{table}:{i}"),
        record: json!({"n":i,"blob":"x".repeat(blob),"_00_rv":i+1}),
        job_assignee: None,
    }
}

async fn capability(h: &TestHarness, id: &str, limit: usize) {
    let mut pool = h.ssp_pool.write().await;
    let mut info = pool.get(id).unwrap().clone();
    info.ingest_batch_limit = limit;
    pool.upsert(info);
}

#[tokio::test]
async fn fanout_batches_queued_rows_with_wire_bounds_and_legacy_fallback() {
    let h = TestHarness::new().await;
    let (peer, url) = BatchPeer::start(false).await;
    let legacy = MockSsp::start().await;
    h.add_ready_ssp("modern", &url).await;
    h.add_ready_ssp("legacy", &legacy.addr).await;
    capability(&h, "modern", 128).await;
    let mut state = h.ingest_state();
    state.job_tables = Arc::new(vec!["job".into()]);
    let mut expected = Vec::new();
    let first = change("thread", 0, 0);
    expected.push(first.id.clone());
    ingest::ingest_event(&state, first, 0).await.unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(5), peer.started.notified())
        .await
        .unwrap();
    for i in 1..141 {
        let row = change("thread", i, 0);
        expected.push(row.id.clone());
        ingest::ingest_event(&state, row, 0).await.unwrap();
    }
    for table in ["user", "job", "_00_heartbeat"] {
        let row = change(table, 1, 0);
        expected.push(row.id.clone());
        ingest::ingest_event(&state, row, 0).await.unwrap();
    }
    for i in 141..145 {
        let row = change("thread", i, 400_000);
        expected.push(row.id.clone());
        ingest::ingest_event(&state, row, 0).await.unwrap();
    }
    peer.release.notify_one();
    tokio::time::timeout(std::time::Duration::from_secs(15), h.fanout.idle())
        .await
        .unwrap();
    let ids = |rows: &[Value]| {
        rows.iter()
            .map(|v| v["id"].as_str().unwrap().to_string())
            .collect::<Vec<_>>()
    };
    assert_eq!(ids(&peer.accepted.lock().await), expected);
    assert_eq!(ids(&legacy.received.lock().await), expected);
    let attempts = peer.attempts.lock().await;
    let batches: Vec<_> = attempts
        .iter()
        .filter(|(path, _)| path == "/ingest/batch")
        .collect();
    assert!(batches
        .iter()
        .any(|(_, b)| b["records"].as_array().unwrap().len() == 128));
    for (path, body) in attempts.iter() {
        if path == "/ingest/batch" {
            let rows = body["records"].as_array().unwrap();
            assert!(rows.len() <= 128);
            assert!(
                serde_json::to_vec(body).unwrap().len() <= ssp_protocol::MAX_INGEST_BATCH_BYTES
            );
            assert!(rows.iter().all(|row| row["table"] == "thread"));
        }
    }
    // Every row remains independently durable and replayable.
    assert_eq!(h.event_buffer.read().await.len(), expected.len());
}

#[tokio::test]
async fn failed_batch_replays_the_whole_undelivered_suffix_in_order() {
    let h = TestHarness::new().await;
    let (peer, url) = BatchPeer::start(true).await;
    h.add_ready_ssp("modern", &url).await;
    capability(&h, "modern", 3).await;
    let state = h.ingest_state();
    ingest::ingest_event(&state, change("thread", 0, 0), 0)
        .await
        .unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(5), peer.started.notified())
        .await
        .unwrap();
    for i in 1..13 {
        ingest::ingest_event(&state, change("thread", i, 0), 0)
            .await
            .unwrap();
    }
    peer.release.notify_one();
    h.fanout.idle().await;
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            if peer.accepted.lock().await.len() == 13 && h.ssp_pool.read().await.is_ready("modern")
            {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    let rows = peer.accepted.lock().await;
    assert_eq!(
        rows.iter()
            .map(|v| v["id"].as_str().unwrap().to_string())
            .collect::<Vec<_>>(),
        (0..13).map(|i| format!("thread:{i}")).collect::<Vec<_>>()
    );
    assert!(peer
        .attempts
        .lock()
        .await
        .iter()
        .any(|(p, b)| p == "/ingest/batch" && b["records"].as_array().unwrap().len() == 3));
    assert_eq!(h.ssp_pool.write().await.drain_buffer("modern").len(), 0);
}

#[tokio::test]
async fn final_assignment_bytes_split_an_otherwise_fitting_batch() {
    let h = TestHarness::new().await;
    let (peer, url) = BatchPeer::start(false).await;
    let id = "s".repeat(1024);
    h.add_ready_ssp(&id, &url).await;
    capability(&h, &id, 128).await;
    let state = h.ingest_state();
    ingest::ingest_event(&state, change("thread", 0, 0), 0)
        .await
        .unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(5), peer.started.notified())
        .await
        .unwrap();
    let rows = vec![change("thread", 1, 523_500), change("thread", 2, 523_500)];
    assert!(
        serde_json::to_vec(&ssp_protocol::IngestBatchRequest {
            records: rows.clone()
        })
        .unwrap()
        .len()
            < ssp_protocol::MAX_INGEST_BATCH_BYTES
    );
    for row in rows {
        ingest::ingest_event(&state, row, 0).await.unwrap();
    }
    peer.release.notify_one();
    h.fanout.idle().await;
    let attempts = peer.attempts.lock().await;
    assert_eq!(attempts.len(), 3);
    assert!(
        attempts.iter().all(|(path, _)| path == "/ingest"),
        "assignment must be included in byte sizing"
    );
    assert_eq!(peer.accepted.lock().await.len(), 3);
}
