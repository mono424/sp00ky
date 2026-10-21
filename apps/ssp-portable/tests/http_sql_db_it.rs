//! Integration proof for `HttpSqlDb`: run the portable core's real
//! `bootstrap::rebuild_from_db` against an EXTERNAL SurrealDB over HTTP-RPC —
//! no surrealdb SDK, only the `HttpClient` port. This is the exact path an edge
//! host (DO with `fetch`) uses to talk to SurrealDB Cloud.
//!
//! Requires a running SurrealDB. Start one with:
//!   docker run -d --name surreal-spike -p 18000:8000 \
//!     surrealdb/surrealdb:v3.1.5 start --user root --pass root --allow-net
//! Point elsewhere via SPKY_TEST_SURREAL_URL. The test SKIPS (passes) if the
//! server is unreachable, so CI without docker stays green.

use std::sync::Arc;
use std::time::Duration;

use base64::Engine;
use serde_json::json;
use tokio::sync::RwLock;

use ssp::circuit::Circuit;
use ssp_node::ports::{
    CancelWatch, Db, HttpClient, HttpError, OutboundRequest, OutboundResponse,
};
use ssp_node::{Method, HttpSqlDb};

/// Minimal `HttpClient` on reqwest — the VM's `ReqwestHttp`, inlined so the
/// test needn't depend on apps/ssp. A DO shell swaps this for `fetch`.
struct ReqwestHttp(reqwest::Client);

#[async_trait::async_trait]
impl HttpClient for ReqwestHttp {
    async fn send(
        &self,
        req: OutboundRequest,
        _cancel: Option<CancelWatch>,
    ) -> Result<OutboundResponse, HttpError> {
        let mut b = match req.method {
            Method::Post => self.0.post(&req.url),
            Method::Get => self.0.get(&req.url),
            Method::Put => self.0.put(&req.url),
            Method::Delete => self.0.delete(&req.url),
        }
        .timeout(req.timeout);
        for (k, v) in &req.headers {
            b = b.header(k, v);
        }
        if let Some(body) = &req.json_body {
            b = b.json(body);
        }
        match b.send().await {
            Ok(r) => Ok(OutboundResponse {
                status: r.status().as_u16(),
                body: r.text().await.unwrap_or_default(),
            }),
            Err(e) => Err(HttpError::Transport(e.to_string())),
        }
    }
}

fn base_url() -> String {
    std::env::var("SPKY_TEST_SURREAL_URL").unwrap_or_else(|_| "http://127.0.0.1:18000".to_string())
}

fn root_auth() -> String {
    format!("Basic {}", base64::engine::general_purpose::STANDARD.encode("root:root"))
}

/// `version()` reads `GET /version` (`surrealdb-3.3.4`): the old
/// `RETURN surrealdb::version()` is a parse error on every 3.x tested.
#[tokio::test]
async fn version_reads_the_servers_version_route() {
    let http: Arc<dyn HttpClient> = Arc::new(ReqwestHttp(reqwest::Client::new()));
    let probe = HttpSqlDb::new(http.clone(), base_url(), "test", "test", root_auth())
        .with_timeout(Duration::from_millis(800));
    if probe.query("RETURN 1;", &[]).await.is_err() {
        eprintln!("SKIP: no SurrealDB at {} (start docker to run this)", base_url());
        return;
    }
    let version = probe.version().await.expect("GET /version");
    assert!(
        version.split('.').count() == 3 && version.chars().next().is_some_and(|c| c.is_ascii_digit()),
        "bare semver expected, got {version:?}"
    );

    // A credential the server refuses must not break it: the route is public.
    let stale = HttpSqlDb::new(http, base_url(), "test", "test", "Bearer stale")
        .with_timeout(Duration::from_millis(800));
    assert_eq!(stale.version().await.expect("GET /version without auth"), version);
}

#[tokio::test]
async fn rebuild_from_external_surreal_over_http_rpc() {
    let http: Arc<dyn HttpClient> = Arc::new(ReqwestHttp(reqwest::Client::new()));
    let auth = root_auth();

    // Reachability probe → skip cleanly if no server.
    let probe = HttpSqlDb::new(http.clone(), base_url(), "test", "test", auth.clone())
        .with_timeout(Duration::from_millis(800));
    if probe.query("RETURN 1;", &[]).await.is_err() {
        eprintln!("SKIP: no SurrealDB at {} (start docker to run this)", base_url());
        return;
    }

    // --- setup: ns/db/schema/rows via HttpSqlDb variants ------------------
    // ns/db creation must NOT target a not-yet-existing db, so use a db-less
    // adapter (empty surreal-db header) for the DEFINEs, then the real one.
    let ns_admin = HttpSqlDb::new(http.clone(), base_url(), "test", "", auth.clone());
    ns_admin
        .query("DEFINE NAMESPACE IF NOT EXISTS test; DEFINE DATABASE IF NOT EXISTS sspnode_it;", &[])
        .await
        .expect("define ns/db");

    let db = HttpSqlDb::new(http.clone(), base_url(), "test", "sspnode_it", auth.clone());
    // Idempotent reset + versioned rows (as a change feed would have stamped).
    db.query(
        "REMOVE TABLE IF EXISTS thread; \
         DEFINE TABLE thread SCHEMALESS PERMISSIONS FOR select FULL; \
         CREATE thread:1 SET title = 'a', _00_rv = 1; \
         CREATE thread:2 SET title = 'b', _00_rv = 2;",
        &[],
    )
    .await
    .expect("seed schema + rows");

    // Also exercise server-side binds through the port (the reason we use /rpc,
    // not /sql): a parameterized create.
    db.query(
        "CREATE type::record('thread', $id) SET title = $t, _00_rv = 3;",
        &[("id", json!("three")), ("t", json!("c"))],
    )
    .await
    .expect("parameterized create");

    // --- the actual test: portable core rebuild over the HTTP Db ----------
    let processor = Arc::new(RwLock::new(Circuit::new()));
    ssp_node::bootstrap::rebuild_from_db(&db, &processor, 200)
        .await
        .expect("rebuild_from_db over HTTP-RPC");

    let circuit = processor.read().await;
    let hashes = circuit.compute_table_hashes();
    assert!(hashes.contains_key("thread"), "thread table loaded: {hashes:?}");
    // All three rows crossed the HTTP-RPC Db port into the circuit — including
    // the one created via server-side bind params.
    assert!(circuit.contains("thread", "thread:1"), "row 1 loaded");
    assert!(circuit.contains("thread", "thread:2"), "row 2 loaded");
    assert!(circuit.contains("thread", "thread:three"), "bound row loaded");
    // Permission text was read back over HTTP (INFO FOR DB) and registered, so
    // a view would not default-deny.
    assert_eq!(circuit.max_row_versions().get("thread"), Some(&3), "max _00_rv folded");
}
