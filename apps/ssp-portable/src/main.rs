//! Thin entrypoint for the reference portable host. Everything real lives in
//! the library so the integration test drives the exact same code.

use std::sync::Arc;

use surrealdb::engine::local::Mem;
use surrealdb::Surreal;

use ssp_portable::PortableHost;

fn main() -> anyhow::Result<()> {
    // An embedded SurrealDB 4 recurses on the calling thread (planner,
    // lowering, drop) and running out of stack aborts the process; tokio's
    // default of 2 MiB is not enough for it.
    tokio::runtime::Builder::new_multi_thread()
        .thread_stack_size(8 * 1024 * 1024)
        .enable_all()
        .build()?
        .block_on(run())
}

async fn run() -> anyhow::Result<()> {
    tracing_subscriber_init();

    let secret = std::env::var("SPKY_SSP_SECRET").unwrap_or_else(|_| "portable-dev".to_string());
    let addr: std::net::SocketAddr = std::env::var("SPKY_SSP_ADDR")
        .unwrap_or_else(|_| "127.0.0.1:8667".to_string())
        .parse()?;
    let state_dir = std::env::var("SPKY_SSP_STATE_DIR").unwrap_or_else(|_| "./ssp-state".to_string());
    std::fs::create_dir_all(&state_dir)?;

    let db = Arc::new(Surreal::new::<Mem>(()).await?);
    db.use_ns("spooky").use_db("spooky").await?;

    let host = PortableHost::new(db, &state_dir, secret, Some(30)).await;
    ssp_portable::serve(host.node.clone(), addr).await
}

fn tracing_subscriber_init() {
    // Best-effort; ignore if a global subscriber is already set.
    let _ = tracing::subscriber::set_global_default(
        tracing::subscriber::NoSubscriber::default(),
    );
}
