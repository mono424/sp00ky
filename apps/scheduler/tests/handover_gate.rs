//! The handover gate in front of every scheduler listener: hold, serve,
//! relay. The gate is process-global, so everything runs in one test.

use std::sync::Arc;
use std::time::Duration;

use axum::routing::{get, post};
use axum::Router;
use scheduler::handover::{self, Listener, Mode, RouterSlot};

async fn serve(router: Router) -> u16 {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    port
}

async fn free_port() -> u16 {
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    l.local_addr().unwrap().port()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_gate_holds_serves_and_relays() {
    let client = reqwest::Client::new();
    let gate = handover::gate();

    // The peer a relay goes to.
    let peer_port = serve(
        Router::new()
            .route("/hello", get(|| async { "peer" }))
            .route("/echo", post(|body: String| async move { body })),
    )
    .await;

    // Our listener: relays to the peer's port, serves `local` from its slot.
    let slot: RouterSlot = Default::default();
    let port = serve(handover::gated(Arc::clone(&slot), Listener::Main, peer_port)).await;
    let url = |path: &str| format!("http://127.0.0.1:{port}{path}");

    // Status is answered at once, whatever the mode.
    let status: serde_json::Value = client.get(url("/handover/status")).send().await.unwrap().json().await.unwrap();
    assert_eq!(status["role"], "starting");
    assert_eq!(status["supported"], true);

    // Held until there is something to serve with.
    let held = tokio::spawn({
        let client = client.clone();
        let u = url("/hello");
        async move { client.get(u).send().await.unwrap().text().await.unwrap() }
    });
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(!held.is_finished(), "a request waits while the gate holds");
    let _ = slot.set(Router::new().route("/hello", get(|| async { "local" })));
    gate.set_mode(Mode::Serve);
    assert_eq!(held.await.unwrap(), "local");

    // Relayed, body and all.
    gate.set_mode(Mode::Forward("127.0.0.1".to_string()));
    let resp = client.get(url("/hello")).send().await.unwrap();
    assert_eq!(resp.headers()["x-sp00ky-relayed-to"], "127.0.0.1");
    assert_eq!(resp.text().await.unwrap(), "peer");
    let echoed = client.post(url("/echo")).body("payload-123").send().await.unwrap().text().await.unwrap();
    assert_eq!(echoed, "payload-123");

    // A request that was relayed here already is never relayed again (two
    // schedulers relaying to each other would bounce it forever): it waits.
    let bounced = tokio::spawn({
        let client = client.clone();
        let u = url("/hello");
        async move { client.get(u).header("x-sp00ky-relayed", "1").send().await.unwrap().text().await.unwrap() }
    });
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(!bounced.is_finished(), "a relayed request is held, not relayed again");
    gate.set_mode(Mode::Serve);
    assert_eq!(bounced.await.unwrap(), "local");
    gate.set_mode(Mode::Forward("127.0.0.1".to_string()));

    // A relay target that is not listening yet: the request waits and is
    // answered once the mode moves on, instead of failing.
    let dead_port = free_port().await;
    let slot2: RouterSlot = Default::default();
    let _ = slot2.set(Router::new().route("/hello", get(|| async { "local2" })));
    let port2 = serve(handover::gated(slot2, Listener::Main, dead_port)).await;
    let waiting = tokio::spawn({
        let client = client.clone();
        async move {
            client
                .get(format!("http://127.0.0.1:{port2}/hello"))
                .send()
                .await
                .unwrap()
                .text()
                .await
                .unwrap()
        }
    });
    tokio::time::sleep(Duration::from_millis(600)).await;
    assert!(!waiting.is_finished(), "an unreachable relay target is waited for");
    gate.set_mode(Mode::Serve);
    assert_eq!(waiting.await.unwrap(), "local2");

    // In-flight accounting drains back to zero.
    assert!(gate.wait_idle(Duration::from_secs(1)).await);
}
