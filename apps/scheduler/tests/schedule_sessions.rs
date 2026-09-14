//! Job completion must not consume an upstream HTTP session per event.
//! Run with Docker: cargo test -p scheduler --test schedule_sessions -- --ignored
use std::sync::Arc;
use maintenance::db::{connect_http, connect_http_raw, DbConfig, ReconnectingDb};
use scheduler::{admin::new_db_slot, config::LoadBalanceStrategy, router::SspPool,
    schedule_engine::observe_job_terminal, transport::HttpTransport};
use tokio::sync::{RwLock, Semaphore};

struct Container(String);
impl Drop for Container {
    fn drop(&mut self) {
        let _ = std::process::Command::new("docker")
            .args(["rm", "-f", &self.0]).output();
    }
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn terminal_observers_reuse_the_scheduler_session() {
    let container = Container(format!("spky-observer-sessions-{}", std::process::id()));
    let output = std::process::Command::new("docker").args([
        "run", "-d", "--name", &container.0, "-p", "127.0.0.1::8000",
        "-e", "SURREAL_HTTP_MAX_ATTACHED_SESSIONS=4",
        "surrealdb/surrealdb:v3.1.5", "start", "--user", "root", "--pass", "root",
    ]).output().unwrap();
    assert!(output.status.success(), "Docker fixture failed");
    let port = std::process::Command::new("docker")
        .args(["port", &container.0, "8000"]).output().unwrap();
    let address = String::from_utf8(port.stdout).unwrap().trim().to_string();
    let cfg = DbConfig { url: format!("http://{address}"), namespace: "test".into(),
        database: "test".into(), username: "root".into(), password: "root".into() };
    tokio::time::timeout(std::time::Duration::from_secs(30), async {
        loop {
            if reqwest::get(format!("{}/health", cfg.url)).await
                .is_ok_and(|r| r.status().is_success()) { break; }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
    }).await.expect("SurrealDB did not become healthy");
    let db = connect_http_raw(&cfg).await.unwrap();
    db.query("DEFINE NAMESPACE test; USE NS test; DEFINE DATABASE test;")
        .await.unwrap().check().unwrap();
    db.use_ns("test").use_db("test").await.unwrap();
    let shared = ReconnectingDb::new(db, cfg.clone());
    let slot = new_db_slot();
    *slot.write().await = Some(Arc::clone(&shared));
    let pool = Arc::new(RwLock::new(SspPool::new(LoadBalanceStrategy::LeastQueries, 100)));
    let transport = Arc::new(HttpTransport::new());
    let permits = Arc::new(Semaphore::new(1));
    for index in 0..32 {
        observe_job_terminal(Arc::clone(&pool), Arc::clone(&transport), Arc::clone(&slot),
            Arc::clone(&permits), format!("job:test{index}"), "success".into());
        // The observer acquires synchronously; this waits for its completion.
        let completed = tokio::time::timeout(std::time::Duration::from_secs(5),
            permits.acquire()).await.unwrap().unwrap();
        drop(completed);
    }
    shared.handle().query("RETURN 1").await.unwrap().check().unwrap();
    // Existing sessions still work when the cap is exhausted. A new connection
    // is the regression check that catches accumulated attached sessions.
    let probe = connect_http(&cfg).await.expect("observers exhausted the HTTP session limit");
    probe.query("RETURN 1").await.unwrap().check().unwrap();
}
