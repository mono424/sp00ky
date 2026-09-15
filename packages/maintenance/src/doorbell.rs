//! The changefeed's wake-up signal: one `LIVE SELECT` over WebSocket on a
//! table every synced mutation writes (`_00_version`), so the tail polls
//! `SHOW CHANGES` a few milliseconds after a commit instead of on a timer.
//!
//! Push is the optimisation, not the truth. A missed notification costs one
//! fallback interval; the feed itself is what gets read. That is why this is a
//! deliberately small JSON-RPC client over `tokio-tungstenite` rather than the
//! SDK's WebSocket engine: it needs `signin`, `use`, one `query` and the
//! notification stream, and its reconnect behaviour must be boringly obvious.
//!
//! Verified on SurrealDB 3.1.5: the notification arrives ~0.4 ms after the
//! commit ack, never for a cancelled or failed transaction, and `SHOW CHANGES`
//! issued right after it already contains the change.

use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::Message;
use tracing::{debug, info, warn};

use crate::changefeed::TailerStats;
use crate::db::{normalize_url, DbConfig};

/// Deadline for each RPC handshake step (signin, use, live).
const RPC_TIMEOUT: Duration = Duration::from_secs(10);
/// Application-level keepalive: an RPC `ping` this often, and a session
/// without any frame for twice that is torn down and rebuilt.
const KEEPALIVE: Duration = Duration::from_secs(20);
const RECONNECT_MIN: Duration = Duration::from_secs(1);
const RECONNECT_MAX: Duration = Duration::from_secs(30);

/// Keep one live query open on `table` for the life of the process; every
/// notification rings `notify`. Connection state and counters land in
/// `stats` so `/health` can say whether the tail is push- or poll-driven.
pub fn spawn(
    config: DbConfig,
    table: String,
    notify: Arc<tokio::sync::Notify>,
    stats: Arc<TailerStats>,
) {
    tokio::spawn(async move {
        let mut backoff = RECONNECT_MIN;
        loop {
            let started = std::time::Instant::now();
            match session(&config, &table, &notify, &stats).await {
                Ok(()) => debug!("Changefeed doorbell session ended"),
                Err(e) => {
                    warn!(error = %e, "Changefeed doorbell session failed; reconnecting");
                    stats.set_error(Some(format!("doorbell: {e}")));
                }
            }
            stats.doorbell_connected.store(false, Ordering::Relaxed);
            stats.doorbell_reconnects.fetch_add(1, Ordering::Relaxed);
            // A session that lived a while earned a fresh backoff.
            if started.elapsed() > Duration::from_secs(60) {
                backoff = RECONNECT_MIN;
            }
            tokio::time::sleep(backoff).await;
            backoff = (backoff * 2).min(RECONNECT_MAX);
        }
    });
}

fn rpc_url(config: &DbConfig) -> String {
    let (addr, secure) = normalize_url(&config.url);
    let addr = addr.trim_end_matches('/');
    let scheme = if secure { "wss" } else { "ws" };
    format!("{scheme}://{addr}/rpc")
}

async fn session(
    config: &DbConfig,
    table: &str,
    notify: &tokio::sync::Notify,
    stats: &TailerStats,
) -> anyhow::Result<()> {
    let url = rpc_url(config);
    let mut request = url.as_str().into_client_request()?;
    request
        .headers_mut()
        .insert("Sec-WebSocket-Protocol", "json".parse()?);
    let (ws, _) = tokio::time::timeout(RPC_TIMEOUT, tokio_tungstenite::connect_async(request))
        .await
        .map_err(|_| anyhow::anyhow!("connect to {url} timed out"))??;
    let (mut tx, mut rx) = ws.split();

    let mut next_id: u64 = 0;
    let mut call = |method: &str, params: Value| -> (String, String) {
        next_id += 1;
        let id = next_id.to_string();
        let frame = json!({ "id": id, "method": method, "params": params }).to_string();
        (id, frame)
    };

    // The handshake: three requests whose replies must come back in order.
    let steps = [
        call(
            "signin",
            json!([{ "user": config.username, "pass": config.password }]),
        ),
        call("use", json!([config.namespace, config.database])),
        call("query", json!([format!("LIVE SELECT id FROM {table};")])),
    ];
    for (id, frame) in steps {
        tx.send(Message::text(frame)).await?;
        let reply = tokio::time::timeout(RPC_TIMEOUT, async {
            loop {
                match rx.next().await {
                    Some(Ok(Message::Text(text))) => {
                        let v: Value = serde_json::from_str(&text)?;
                        if v.get("id").and_then(|i| i.as_str()) == Some(id.as_str()) {
                            return anyhow::Ok(v);
                        }
                    }
                    Some(Ok(Message::Ping(p))) => tx.send(Message::Pong(p)).await?,
                    Some(Ok(Message::Close(_))) | None => {
                        anyhow::bail!("connection closed during handshake")
                    }
                    Some(Ok(_)) => {}
                    Some(Err(e)) => return Err(e.into()),
                }
            }
        })
        .await
        .map_err(|_| anyhow::anyhow!("rpc {id} timed out"))??;
        if let Some(err) = reply.get("error") {
            anyhow::bail!("rpc {id} failed: {err}");
        }
        // The live query's reply is `[{status: OK, result: <uuid>}]`.
        if let Some(status) = reply
            .get("result")
            .and_then(|r| r.as_array())
            .and_then(|a| a.first())
            .and_then(|s| s.get("status"))
            .and_then(|s| s.as_str())
        {
            if status != "OK" {
                anyhow::bail!("live query refused: {reply}");
            }
        }
    }

    stats.doorbell_connected.store(true, Ordering::Relaxed);
    info!(
        table,
        "Changefeed doorbell connected (LIVE SELECT over WebSocket)"
    );

    let mut keepalive = tokio::time::interval(KEEPALIVE);
    keepalive.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    keepalive.tick().await;
    let mut last_frame = std::time::Instant::now();
    loop {
        tokio::select! {
            frame = rx.next() => {
                last_frame = std::time::Instant::now();
                match frame {
                    Some(Ok(Message::Text(text))) => {
                        // Notifications carry no request id; everything else
                        // is a reply to our keepalive ping.
                        let v: Value = match serde_json::from_str(&text) {
                            Ok(v) => v,
                            Err(_) => continue,
                        };
                        if v.get("id").is_none()
                            && v.get("result").and_then(|r| r.get("action")).is_some()
                        {
                            notify.notify_one();
                        }
                    }
                    Some(Ok(Message::Ping(p))) => tx.send(Message::Pong(p)).await?,
                    Some(Ok(Message::Close(frame))) => {
                        anyhow::bail!("server closed the doorbell: {frame:?}")
                    }
                    Some(Ok(_)) => {}
                    Some(Err(e)) => return Err(e.into()),
                    None => anyhow::bail!("doorbell stream ended"),
                }
            }
            _ = keepalive.tick() => {
                if last_frame.elapsed() > KEEPALIVE * 2 {
                    anyhow::bail!("no frame for {}s; session presumed dead", (KEEPALIVE * 2).as_secs());
                }
                let (_, frame) = call("ping", json!([]));
                tx.send(Message::text(frame)).await?;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rpc_url_follows_the_configured_scheme() {
        let mut cfg = DbConfig {
            url: "http://surrealdb:8000".into(),
            namespace: "main".into(),
            database: "main".into(),
            username: "root".into(),
            password: "x".into(),
        };
        assert_eq!(rpc_url(&cfg), "ws://surrealdb:8000/rpc");
        cfg.url = "https://db.example.com".into();
        assert_eq!(rpc_url(&cfg), "wss://db.example.com/rpc");
        cfg.url = "ws://localhost:8000/".into();
        assert_eq!(rpc_url(&cfg), "ws://localhost:8000/rpc");
    }
}
