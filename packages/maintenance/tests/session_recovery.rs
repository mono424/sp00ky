//! Regression test for the outage where a SurrealDB restart permanently broke
//! every long-lived `Surreal<Http>` handle in the SSP and scheduler.
//!
//! The HTTP engine registers a session in SurrealDB's in-memory session map and
//! tags every later request with its UUID. A restart empties that map, so the
//! old handle fails forever with `Session not found: <uuid>` — and re-running
//! `signin` on it cannot help, because the signin is itself routed through the
//! dead session. Production ran 84 minutes with no jobs, no view registration
//! and no realtime until the containers were restarted by hand.
//!
//! This test reproduces that exact sequence against a real SurrealDB and
//! asserts both halves: that a plain handle stays broken, and that
//! `ReconnectingDb` recovers on its own.
//!
//! Requires Docker. Run with:
//!   cargo test -p maintenance --test session_recovery -- --ignored --nocapture

use maintenance::db::{connect_http, connect_http_raw, DbConfig, ReconnectingDb};

const IMAGE: &str = "surrealdb/surrealdb:v3.0.5";
const CONTAINER: &str = "spky-session-recovery-test";
const PORT: u16 = 18099;

fn docker(args: &[&str]) -> std::process::Output {
    std::process::Command::new("docker")
        .args(args)
        .output()
        .expect("docker not runnable")
}

fn config() -> DbConfig {
    DbConfig {
        url: format!("http://127.0.0.1:{PORT}"),
        namespace: "test".to_string(),
        database: "test".to_string(),
        username: "root".to_string(),
        password: "root".to_string(),
    }
}

async fn wait_until_serving() {
    let url = format!("http://127.0.0.1:{PORT}/health");
    for _ in 0..120 {
        if matches!(reqwest::get(&url).await, Ok(r) if r.status().is_success()) {
            // /health flips green a beat before the RPC endpoint settles.
            tokio::time::sleep(std::time::Duration::from_millis(300)).await;
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
    }
    panic!("SurrealDB never became healthy on port {PORT}");
}

#[tokio::test]
#[ignore = "requires docker"]
async fn reconnecting_db_survives_a_surrealdb_restart() {
    let _ = docker(&["rm", "-f", CONTAINER]);
    let run = docker(&[
        "run", "-d", "--name", CONTAINER,
        "-p", &format!("{PORT}:8000"),
        IMAGE,
        "start", "--user", "root", "--pass", "root", "--allow-all",
    ]);
    assert!(
        run.status.success(),
        "docker run failed: {}",
        String::from_utf8_lossy(&run.stderr)
    );

    // Teardown runs on the success path; a panicking run leaves the container
    // behind, which the `docker rm -f` above cleans up on the next run.
    run_scenario().await;
    let _ = docker(&["rm", "-f", CONTAINER]);
}

async fn run_scenario() {
    wait_until_serving().await;
    let cfg = config();

    // A plain long-lived handle, exactly what the SSP and scheduler used to
    // hold, plus the self-healing wrapper that replaced it.
    let plain = connect_http(&cfg).await.expect("plain connect failed");
    let reconnecting = ReconnectingDb::connect(&cfg)
        .await
        .expect("reconnecting connect failed");

    plain.query("RETURN 1").await.expect("plain: healthy query failed");
    reconnecting
        .handle()
        .query("RETURN 1")
        .await
        .expect("reconnecting: healthy query failed");

    // The incident: SurrealDB restarts. Its session map is memory-only, so
    // every session attached by the handles above is gone.
    let restart = docker(&["restart", CONTAINER]);
    assert!(
        restart.status.success(),
        "docker restart failed: {}",
        String::from_utf8_lossy(&restart.stderr)
    );
    wait_until_serving().await;

    // Half 1 — the bug. The old handle is dead, and re-signing in on it does
    // NOT bring it back: the signin travels through the same dead session.
    let err = plain
        .query("RETURN 1")
        .await
        .expect_err("plain handle should be dead after a restart")
        .to_string();
    assert!(
        maintenance::db::is_dead_session_error(&err),
        "expected a dead-session error, got: {err}"
    );

    let resignin = plain
        .signin(surrealdb::opt::auth::Root {
            username: cfg.username.clone(),
            password: cfg.password.clone(),
        })
        .await;
    assert!(
        resignin.is_err(),
        "re-signin on a dead session unexpectedly succeeded — if the SDK gained \
         session recovery, ReconnectingDb can be simplified"
    );
    assert!(
        plain.query("RETURN 1").await.is_err(),
        "plain handle recovered on its own, which the outage proves it does not"
    );

    // Half 2 — the fix. One refresh pass notices the dead session and swaps in
    // a brand-new handle, so callers keep working without a process restart.
    assert!(
        reconnecting.refresh().await,
        "refresh should report the connection usable after reconnecting"
    );
    reconnecting
        .handle()
        .query("RETURN 1")
        .await
        .expect("reconnecting handle should work again after refresh");
}

/// A SurrealDB 3.1.5 container on a free port, removed on drop.
struct Server {
    name: String,
    url: String,
}

impl Server {
    async fn start(name: &str) -> Server {
        let _ = docker(&["rm", "-f", name]);
        let run = docker(&[
            "run", "-d", "--name", name, "-p", "127.0.0.1::8000",
            "surrealdb/surrealdb:v3.1.5", "start", "--user", "root", "--pass", "root",
        ]);
        assert!(run.status.success(), "docker run failed: {}", String::from_utf8_lossy(&run.stderr));
        let port = docker(&["port", name, "8000"]);
        let server = Server {
            name: name.to_string(),
            url: format!("http://{}", String::from_utf8_lossy(&port.stdout).trim()),
        };
        for _ in 0..120 {
            if matches!(reqwest::get(format!("{}/health", server.url)).await, Ok(r) if r.status().is_success()) {
                tokio::time::sleep(std::time::Duration::from_millis(300)).await;
                let root = connect_http_raw(&server.config("root")).await.expect("root connect");
                // A root user whose tokens live two seconds, so rotation (and
                // what happens without it) is observable in a test.
                root.query(
                    "DEFINE NAMESPACE test; USE NS test; DEFINE DATABASE test;
                     DEFINE USER rot ON ROOT PASSWORD 'rot' ROLES OWNER DURATION FOR TOKEN 2s, FOR SESSION NONE;",
                )
                .await
                .expect("setup")
                .check()
                .expect("setup statements");
                return server;
            }
            tokio::time::sleep(std::time::Duration::from_millis(250)).await;
        }
        panic!("SurrealDB never became healthy at {}", server.url);
    }

    fn config(&self, user: &str) -> DbConfig {
        DbConfig {
            url: self.url.clone(),
            namespace: "test".to_string(),
            database: "test".to_string(),
            username: user.to_string(),
            password: user.to_string(),
        }
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = docker(&["rm", "-f", &self.name]);
    }
}

/// The token is renewed by swapping in a new handle, never by signing in on
/// the one in use, and the swap happens before the old token runs out.
#[tokio::test]
#[ignore = "requires docker"]
async fn reconnecting_db_rotates_before_its_token_expires() {
    let server = Server::start("spky-session-rotation-test").await;
    let cfg = server.config("rot");
    let plain = connect_http(&cfg).await.expect("plain connect");
    let db = ReconnectingDb::connect(&cfg).await.expect("reconnecting connect");

    assert!(db.refresh().await);
    assert_eq!(db.rotations(), 0, "a fresh token must not rotate");

    for _ in 0..4 {
        tokio::time::sleep(std::time::Duration::from_millis(1100)).await;
        assert!(db.refresh().await);
    }
    assert!(db.rotations() >= 3, "expected a rotation per half-spent token, got {}", db.rotations());
    assert_eq!(db.reconnect_metrics().0, 1, "a rotation is not a reconnect");

    // Well past the first token's expiry: the rotated handle still works, the
    // handle that was never renewed does not.
    db.handle()
        .query("INFO FOR DB")
        .await
        .expect("rotated handle")
        .check()
        .expect("rotated handle answers");
    let stale = plain.query("INFO FOR DB").await.and_then(|r| r.check());
    assert!(stale.is_err(), "a 2s token still worked after 4s; the rotation test proves nothing");
}

/// The 2026-09-16 wedge: a session write on a shared handle, landing between
/// the two session reads of a concurrent `query()`, deadlocks the session on
/// SurrealDB 3.1.x. With a signin as the probe, this loop wedged 4 of 60 and 7
/// of 250 bursts on a CPU-throttled server. The refresh must never do that,
/// rotation included (the 2s token makes it rotate throughout).
#[tokio::test]
#[ignore = "requires docker"]
async fn refresh_under_load_never_wedges_the_shared_session() {
    let server = Server::start("spky-session-wedge-test").await;
    let throttle = docker(&["update", "--cpus", "0.1", &server.name]);
    assert!(throttle.status.success(), "docker update failed");
    let db = ReconnectingDb::connect(&server.config("rot")).await.expect("connect");

    let deadline = std::time::Duration::from_secs(8);
    let mut wedged = 0;
    for burst in 0..150 {
        let mut readers = Vec::new();
        for r in 0..16u64 {
            let handle = db.handle();
            readers.push(tokio::spawn(async move {
                tokio::time::sleep(std::time::Duration::from_micros(r * 300)).await;
                tokio::time::timeout(deadline, handle.query("INFO FOR DB")).await.is_ok()
            }));
        }
        tokio::time::sleep(std::time::Duration::from_micros(2000)).await;
        let refreshed = tokio::time::timeout(deadline, db.refresh()).await.is_ok();
        let mut answered = 0;
        for reader in readers {
            answered += usize::from(reader.await.unwrap());
        }
        if answered != 16 || !refreshed {
            wedged += 1;
            eprintln!("burst {burst}: refresh returned={refreshed}, {answered}/16 queries answered");
        }
    }
    assert_eq!(wedged, 0, "{wedged} of 150 bursts wedged");
    assert!(db.rotations() > 0, "the run never rotated, so rotation was not exercised");
    assert_eq!(db.reconnect_metrics().0, 1, "no probe should have needed a reconnect");
}
