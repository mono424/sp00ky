//! A scheduler restart must come back with the backend list the last deploy
//! pushed: the control plane pushes it once per deploy and never again.
//! Run with Docker: cargo test -p scheduler --test backend_registry_restart -- --ignored
use std::sync::Arc;
use std::time::Duration;

use maintenance::db::{connect_http_raw, DbConfig, ReconnectingDb};
use scheduler::backend_health::{create_health_cache, create_shared_configs, BackendHealthConfig};
use scheduler::backend_registry::BackendRegistry;

struct Container(String);
impl Drop for Container {
    fn drop(&mut self) {
        let _ = std::process::Command::new("docker")
            .args(["rm", "-f", &self.0])
            .output();
    }
}

fn backend(name: &str, env: &[&str]) -> BackendHealthConfig {
    BackendHealthConfig {
        name: name.into(),
        url: format!("http://{name}:3000"),
        healthcheck: "/health".into(),
        port: Some(3000),
        env: Some(env.iter().map(|e| e.to_string()).collect()),
    }
}

async fn wait_for<F, Fut>(what: &str, mut cond: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    for _ in 0..100 {
        if cond().await {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("timed out waiting for {what}");
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn a_restart_restores_the_pushed_backends() {
    let container = Container(format!("spky-backend-registry-{}", std::process::id()));
    let output = std::process::Command::new("docker")
        .args([
            "run",
            "-d",
            "--name",
            &container.0,
            "-p",
            "127.0.0.1::8000",
            "surrealdb/surrealdb:v3.1.5",
            "start",
            "--user",
            "root",
            "--pass",
            "root",
        ])
        .output()
        .unwrap();
    assert!(output.status.success(), "Docker fixture failed");
    let port = std::process::Command::new("docker")
        .args(["port", &container.0, "8000"])
        .output()
        .unwrap();
    let address = String::from_utf8(port.stdout).unwrap().trim().to_string();
    let cfg = DbConfig {
        url: format!("http://{address}"),
        namespace: "test".into(),
        database: "test".into(),
        username: "root".into(),
        password: "root".into(),
    };
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            if reqwest::get(format!("{}/health", cfg.url))
                .await
                .is_ok_and(|r| r.status().is_success())
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("SurrealDB did not become healthy");
    let db = connect_http_raw(&cfg).await.unwrap();
    db.query("DEFINE NAMESPACE test; USE NS test; DEFINE DATABASE test;")
        .await
        .unwrap()
        .check()
        .unwrap();
    db.use_ns("test").use_db("test").await.unwrap();
    let shared = ReconnectingDb::new(db, cfg.clone());

    // First boot: nothing stored yet, nothing pushed, the list stays empty.
    let configs = create_shared_configs(&[]);
    let first = BackendRegistry::new(configs.clone(), create_health_cache(&[]), false);
    first.attach_db(Arc::clone(&shared));
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(configs.read().await.is_empty());

    // The deploy pushes its list.
    first
        .replace(vec![
            backend("relay", &["CEC_ANALYSIS_TOKEN=abc", "PORT=3670"]),
            backend("gamesync", &[]),
        ])
        .await;
    assert_eq!(configs.read().await.len(), 2);

    // Restart: a fresh registry over empty caches, as a relaunched process has.
    let restarted = create_shared_configs(&[]);
    let cache = create_health_cache(&[]);
    let second = BackendRegistry::new(restarted.clone(), cache.clone(), false);
    second.attach_db(Arc::clone(&shared));
    wait_for("the restored list", || {
        let restarted = restarted.clone();
        async move { restarted.read().await.len() == 2 }
    })
    .await;
    let list = restarted.read().await.clone();
    assert_eq!(list[0].name, "relay");
    assert_eq!(list[1].name, "gamesync");
    let env = list[0].env.clone().unwrap();
    assert!(
        env.contains(&"CEC_ANALYSIS_TOKEN=****".to_string()),
        "secrets stored masked: {env:?}"
    );
    assert!(env.contains(&"PORT=3670".to_string()), "{env:?}");
    assert_eq!(
        cache.read().await.len(),
        2,
        "health cache follows the restored list"
    );

    // Root-only and never synced.
    let mut info = shared
        .handle()
        .query("INFO FOR DB")
        .await
        .unwrap()
        .check()
        .unwrap();
    let info: Option<serde_json::Value> = info.take(0).unwrap();
    let def = info.unwrap()["tables"]["_00_scheduler_state"]
        .as_str()
        .unwrap_or_default()
        .to_string();
    assert!(def.contains("PERMISSIONS NONE"), "{def}");
    assert!(!def.contains("CHANGEFEED"), "{def}");

    // An environment list is authoritative: the stored copy is never applied.
    let from_env = create_shared_configs(&[backend("envonly", &[])]);
    let third = BackendRegistry::new(from_env.clone(), create_health_cache(&[]), true);
    third.attach_db(Arc::clone(&shared));
    tokio::time::sleep(Duration::from_millis(300)).await;
    let names: Vec<String> = from_env
        .read()
        .await
        .iter()
        .map(|b| b.name.clone())
        .collect();
    assert_eq!(names, vec!["envonly"]);
}
