//! The `sp00ky-relay` binary: configuration from the environment, logging,
//! and the listener. Everything else is in the library.

use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use sp00ky_relay::auth::SurrealVerifier;
use sp00ky_relay::{Config, Relay};
use tracing_subscriber::EnvFilter;

const DEFAULT_LISTEN_ADDR: &str = "0.0.0.0:8668";

#[tokio::main]
async fn main() {
    if let Err(message) = run().await {
        eprintln!("sp00ky-relay: {message}");
        std::process::exit(1);
    }
}

async fn run() -> Result<(), String> {
    // Without RUST_LOG, the relay's own lines at info and everything else at
    // warn. `SPKY_LOG_FORMAT=compact` drops the timestamp, as in the sibling
    // services, for a supervisor that prefixes each line itself.
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("sp00ky_relay=info,warn"));
    if std::env::var("SPKY_LOG_FORMAT").as_deref() == Ok("compact") {
        tracing_subscriber::fmt()
            .with_env_filter(filter)
            .compact()
            .without_time()
            .init();
    } else {
        tracing_subscriber::fmt().with_env_filter(filter).init();
    }

    let listen: SocketAddr = number("SPKY_RELAY_LISTEN_ADDR", DEFAULT_LISTEN_ADDR.parse().ok())?
        .ok_or("SPKY_RELAY_LISTEN_ADDR is not set")?;
    let verifier = SurrealVerifier::new(
        &required("SPKY_DB_URL")?,
        required("SPKY_DB_NS")?,
        required("SPKY_DB_NAME")?,
    )
    .map_err(|error| format!("SPKY_DB_URL: {error}"))?;
    let config = config()?;

    let listener = tokio::net::TcpListener::bind(listen)
        .await
        .map_err(|error| format!("could not listen on {listen}: {error}"))?;
    tracing::info!(
        %listen,
        database = verifier.rpc_url(),
        max_room_peers = config.max_room_peers,
        max_frame_bytes = config.max_frame_bytes,
        origins = if config.allowed_origins.is_empty() { "any".to_owned() } else { config.allowed_origins.join(",") },
        "sp00ky-relay listening",
    );

    let relay = Relay::new(config, Arc::new(verifier));
    axum::serve(listener, relay.router())
        // Frames are small and latency is the whole point: never wait to
        // batch them.
        .tcp_nodelay(true)
        .with_graceful_shutdown(shutdown())
        .await
        .map_err(|error| format!("the server stopped: {error}"))?;

    // Rooms live in memory only, so there is nothing to flush: the open
    // sockets close with the process and the clients reconnect.
    tracing::info!("sp00ky-relay stopped");
    Ok(())
}

/// The limits, each from its variable or the default.
fn config() -> Result<Config, String> {
    let defaults = Config::default();
    let or = |value: Option<u64>, default: Duration| value.map_or(default, Duration::from_secs);
    Ok(Config {
        max_frame_bytes: number("SPKY_RELAY_MAX_FRAME_BYTES", None)?
            .unwrap_or(defaults.max_frame_bytes),
        max_room_peers: number("SPKY_RELAY_MAX_ROOM_PEERS", None)?
            .unwrap_or(defaults.max_room_peers),
        rate_burst: number("SPKY_RELAY_RATE_BURST", None)?.unwrap_or(defaults.rate_burst),
        rate_per_sec: number("SPKY_RELAY_RATE_PER_SEC", None)?.unwrap_or(defaults.rate_per_sec),
        queue_frames: number("SPKY_RELAY_QUEUE_FRAMES", None)?.unwrap_or(defaults.queue_frames),
        session_max: or(
            number("SPKY_RELAY_SESSION_SECS", None)?,
            defaults.session_max,
        ),
        allowed_origins: std::env::var("SPKY_RELAY_ALLOWED_ORIGINS")
            .unwrap_or_default()
            .split(',')
            .map(str::trim)
            .filter(|origin| !origin.is_empty())
            .map(str::to_owned)
            .collect(),
        ..defaults
    })
}

/// A variable that has to be set.
fn required(name: &str) -> Result<String, String> {
    match std::env::var(name) {
        Ok(value) if !value.trim().is_empty() => Ok(value.trim().to_owned()),
        _ => Err(format!("{name} is required but not set")),
    }
}

/// An optional variable that has to parse when it is set: a typo in a limit
/// must not silently run with the default.
fn number<T: FromStr>(name: &str, default: Option<T>) -> Result<Option<T>, String> {
    match std::env::var(name) {
        Ok(value) if !value.trim().is_empty() => match value.trim().parse() {
            Ok(parsed) => Ok(Some(parsed)),
            Err(_) => Err(format!("{name}=`{value}` is not a valid value")),
        },
        _ => Ok(default),
    }
}

/// Resolves on ctrl-c or SIGTERM.
async fn shutdown() {
    let ctrl_c = async {
        if tokio::signal::ctrl_c().await.is_err() {
            // No signal handler: never resolve, the other branch still can.
            std::future::pending::<()>().await;
        }
    };
    #[cfg(unix)]
    let terminate = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut signal) => {
                signal.recv().await;
            }
            Err(_) => std::future::pending::<()>().await,
        }
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {}
        _ = terminate => {}
    }
    tracing::info!("shutdown signal received");
}
