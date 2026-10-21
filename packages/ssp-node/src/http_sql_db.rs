//! A [`Db`] adapter over SurrealDB's HTTP-RPC endpoint, built ON TOP of the
//! [`HttpClient`] port — so it inherits that port's transport. On a VM that's
//! reqwest; in a Durable Object / edge worker it's `fetch`. This is the seam
//! that lets an ephemeral host talk to an EXTERNAL SurrealDB (e.g. SurrealDB
//! Cloud) without linking the surrealdb SDK — which sidesteps the "does the SDK
//! run on wasm32" spike entirely (see [`crate::ports::Db`] doc).
//!
//! Wire protocol (verified against surrealdb 3.1.5): `POST {base}/rpc` with a
//! JSON-RPC body `{"id":1,"method":"query","params":[surql, vars]}`,
//! `Authorization` + `surreal-ns` + `surreal-db` headers, `Accept:
//! application/json` (so RecordIds/Datetimes come back as plain strings — the
//! same flattened-JSON convention the SDK adapter produces). The response is
//! `{"result":[{"status":"OK"|"ERR","result":<data>}, ...]}`, one entry per
//! statement; we return each statement's `result`, matching the `Db` contract.
//! [`Db::version`] reads `GET {base}/version` instead, which answers plain text
//! (`surrealdb-3.3.4`).

use std::sync::Arc;
use std::time::Duration;

use serde_json::{json, Map, Value};

use crate::api::Method;
use crate::ports::{Db, DbError, HttpClient, OutboundRequest};

/// Default per-request timeout. Bootstrap page queries can be large; a host can
/// override via [`HttpSqlDb::with_timeout`].
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(30);

pub struct HttpSqlDb {
    http: Arc<dyn HttpClient>,
    rpc_url: String,
    version_url: String,
    /// Full `Authorization` header value, e.g. `"Basic <b64>"` (local root) or
    /// `"Bearer <jwt>"` (SurrealDB Cloud token). The caller owns credential
    /// encoding + refresh; keeping it opaque keeps this adapter wasm-clean (no
    /// base64/crypto deps).
    authorization: String,
    ns: String,
    db: String,
    timeout: Duration,
}

impl HttpSqlDb {
    pub fn new(
        http: Arc<dyn HttpClient>,
        base_url: impl AsRef<str>,
        ns: impl Into<String>,
        db: impl Into<String>,
        authorization: impl Into<String>,
    ) -> Self {
        // The endpoint may be a websocket URL (the browser client connects over
        // wss://); HTTP-RPC needs http(s). Normalize the scheme so a host can be
        // handed the same endpoint the SPA uses.
        let base = base_url.as_ref().trim_end_matches('/');
        let base = if let Some(rest) = base.strip_prefix("wss://") {
            format!("https://{rest}")
        } else if let Some(rest) = base.strip_prefix("ws://") {
            format!("http://{rest}")
        } else {
            base.to_string()
        };
        Self {
            http,
            rpc_url: format!("{base}/rpc"),
            version_url: format!("{base}/version"),
            authorization: authorization.into(),
            ns: ns.into(),
            db: db.into(),
            timeout: DEFAULT_TIMEOUT,
        }
    }

    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    fn request(&self, surql: &str, binds: &[(&str, Value)]) -> OutboundRequest {
        let vars: Map<String, Value> = binds
            .iter()
            .map(|(k, v)| ((*k).to_string(), v.clone()))
            .collect();
        let envelope = json!({
            "id": 1,
            "method": "query",
            "params": [surql, Value::Object(vars)],
        });
        OutboundRequest {
            method: Method::Post,
            url: self.rpc_url.clone(),
            bearer: None,
            headers: vec![
                ("Accept".to_string(), "application/json".to_string()),
                ("Authorization".to_string(), self.authorization.clone()),
                ("surreal-ns".to_string(), self.ns.clone()),
                ("surreal-db".to_string(), self.db.clone()),
            ],
            json_body: Some(envelope),
            timeout: self.timeout,
        }
    }
}

/// Parse a SurrealDB HTTP-RPC response body into one flattened value per
/// statement, or the appropriate `DbError`. Pulled out (pure) so it's unit
/// tested without a live server.
fn parse_rpc_response(body: &str) -> Result<Vec<Value>, DbError> {
    let parsed: Value =
        serde_json::from_str(body).map_err(|e| DbError::Query(format!("bad JSON: {e}")))?;

    // Top-level JSON-RPC error (parse error, auth rejected, etc.).
    if let Some(err) = parsed.get("error") {
        let msg = err
            .get("message")
            .and_then(|m| m.as_str())
            .map(str::to_string)
            .unwrap_or_else(|| err.to_string());
        // SurrealDB tags auth failures in the message; surface as Auth so the
        // caller's re-signin path can trigger.
        if msg.contains("authenticat") || msg.contains("not allowed") && msg.contains("token") {
            return Err(DbError::Auth(msg));
        }
        return Err(DbError::Query(msg));
    }

    let statements = parsed
        .get("result")
        .and_then(|r| r.as_array())
        .ok_or_else(|| DbError::Query("response missing `result` array".to_string()))?;

    // Inside a transaction SurrealDB reports the statements BEFORE the one
    // that failed as "The query was not executed due to a failed transaction"
    // (they were rolled back) and puts the real error on the failing
    // statement itself, which comes later in the array. Returning the first
    // ERR therefore hid every real cause behind that generic line for as long
    // as this parser existed. Prefer the first SPECIFIC error; fall back to
    // the first ERR only when none of them says anything else.
    let mut first_err: Option<String> = None;
    let mut specific_err: Option<String> = None;
    let mut out = Vec::with_capacity(statements.len());
    for st in statements {
        let status = st.get("status").and_then(|s| s.as_str()).unwrap_or("OK");
        let result = st.get("result").cloned().unwrap_or(Value::Null);
        if status != "OK" {
            let msg = result
                .as_str()
                .map(str::to_string)
                .unwrap_or_else(|| result.to_string());
            if first_err.is_none() {
                first_err = Some(msg.clone());
            }
            if specific_err.is_none() && !is_generic_tx_error(&msg) {
                specific_err = Some(msg);
            }
            continue;
        }
        out.push(result);
    }
    if let Some(msg) = specific_err.or(first_err) {
        return Err(DbError::Query(msg));
    }
    Ok(out)
}

/// The per-statement message SurrealDB gives every statement a transaction
/// rolled back; it carries no information about what actually failed.
fn is_generic_tx_error(msg: &str) -> bool {
    msg.starts_with("The query was not executed due to a failed transaction")
        || msg.starts_with("The query was not executed due to a cancelled transaction")
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl Db for HttpSqlDb {
    async fn query(&self, surql: &str, binds: &[(&str, Value)]) -> Result<Vec<Value>, DbError> {
        let resp = self
            .http
            .send(self.request(surql, binds), None)
            .await
            .map_err(|e| DbError::Transport(e.to_string()))?;
        if resp.status == 401 || resp.status == 403 {
            return Err(DbError::Auth(format!(
                "HTTP {}: {}",
                resp.status, resp.body
            )));
        }
        if resp.status >= 400 {
            return Err(DbError::Transport(format!(
                "HTTP {}: {}",
                resp.status, resp.body
            )));
        }
        parse_rpc_response(&resp.body)
    }

    /// `RETURN surrealdb::version()` is a parse error on every 3.x tested
    /// (3.1.5, 3.3.4), so ask the server's own `/version` route instead. It
    /// needs no credentials, and a stale one turns it into a 401 ("There was a
    /// problem with authentication"), so none is sent.
    async fn version(&self) -> Result<String, DbError> {
        let req = OutboundRequest {
            method: Method::Get,
            url: self.version_url.clone(),
            bearer: None,
            headers: Vec::new(),
            json_body: None,
            timeout: self.timeout,
        };
        let resp = self
            .http
            .send(req, None)
            .await
            .map_err(|e| DbError::Transport(e.to_string()))?;
        if resp.status >= 400 {
            return Err(DbError::Transport(format!(
                "HTTP {}: {}",
                resp.status, resp.body
            )));
        }
        Ok(parse_version_body(&resp.body))
    }
}

/// `GET /version` answers `surrealdb-3.3.4`. Report the bare semver, as the SDK
/// adapter (`apps/ssp/src/adapters/db.rs`) does, so `/info` reads the same on
/// either adapter.
fn parse_version_body(body: &str) -> String {
    let body = body.trim();
    let version = body.strip_prefix("surrealdb-").unwrap_or(body);
    if version.is_empty() {
        "unknown".to_string()
    } else {
        version.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_multi_statement_ok() {
        // Shape verified against surrealdb 3.1.5 /rpc.
        let body = r#"{"id":1,"result":[
            {"result":[],"status":"OK","time":"1ms"},
            {"result":[{"id":"thing:a","n":5}],"status":"OK","time":"0.4ms"}
        ]}"#;
        let out = parse_rpc_response(body).unwrap();
        assert_eq!(out.len(), 2);
        assert_eq!(out[1][0]["id"], "thing:a");
        assert_eq!(out[1][0]["n"], 5);
    }

    #[test]
    fn per_statement_error_becomes_query_err() {
        let body = r#"{"id":1,"result":[
            {"result":"The database 'x' does not exist","status":"ERR","time":"1ms"}
        ]}"#;
        let err = parse_rpc_response(body).unwrap_err();
        assert!(matches!(err, DbError::Query(m) if m.contains("does not exist")));
    }

    #[test]
    fn transaction_error_reports_the_failing_statement_not_the_rolled_back_ones() {
        // Exactly what 3.0.5 returned for a 4-statement edge transaction whose
        // DELETE was the broken one: the earlier statements are marked as not
        // executed, the real message sits on the last entry.
        let body = r#"{"id":1,"result":[
            {"result":null,"status":"OK","time":"0ms"},
            {"result":"The query was not executed due to a failed transaction","status":"ERR","time":"0ms"},
            {"result":"The query was not executed due to a failed transaction","status":"ERR","time":"0ms"},
            {"result":"Cannot execute DELETE statement using value: NONE","status":"ERR","time":"1ms"}
        ]}"#;
        let err = parse_rpc_response(body).unwrap_err();
        assert!(
            matches!(err, DbError::Query(ref m) if m.contains("Cannot execute DELETE")),
            "{err:?}"
        );

        // Only generic entries: fall back to the first one rather than nothing.
        let body = r#"{"id":1,"result":[
            {"result":"The query was not executed due to a failed transaction","status":"ERR","time":"0ms"}
        ]}"#;
        let err = parse_rpc_response(body).unwrap_err();
        assert!(
            matches!(err, DbError::Query(ref m) if m.contains("failed transaction")),
            "{err:?}"
        );
    }

    #[test]
    fn normalizes_ws_scheme_to_http_for_rpc() {
        let http: Arc<dyn HttpClient> = Arc::new(NoopHttp);
        let wss = HttpSqlDb::new(http.clone(), "wss://x.surreal.cloud", "ns", "db", "Basic z");
        assert_eq!(wss.rpc_url, "https://x.surreal.cloud/rpc");
        let ws = HttpSqlDb::new(http.clone(), "ws://localhost:8000/", "ns", "db", "Basic z");
        assert_eq!(ws.rpc_url, "http://localhost:8000/rpc");
        let https = HttpSqlDb::new(http, "https://x.surreal.cloud", "ns", "db", "Basic z");
        assert_eq!(https.rpc_url, "https://x.surreal.cloud/rpc");
    }

    struct NoopHttp;
    #[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
    #[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
    impl HttpClient for NoopHttp {
        async fn send(
            &self,
            _req: OutboundRequest,
            _cancel: Option<crate::ports::CancelWatch>,
        ) -> Result<crate::ports::OutboundResponse, crate::ports::HttpError> {
            unreachable!("not called in url-normalization test")
        }
    }

    #[tokio::test]
    async fn version_reads_get_version_not_a_query() {
        let http = Arc::new(RecordingHttp {
            seen: std::sync::Mutex::new(Vec::new()),
            reply: (200, "surrealdb-3.3.4".to_string()),
        });
        let db = HttpSqlDb::new(http.clone(), "wss://x.surreal.cloud/", "ns", "db", "Bearer t");
        let v = db.version().await.unwrap();
        assert_eq!(v, "3.3.4");
        let seen = http.seen.lock().unwrap();
        assert_eq!(seen.len(), 1);
        let (method, url, body, auth) = &seen[0];
        assert_eq!(*method, Method::Get);
        assert_eq!(url, "https://x.surreal.cloud/version");
        assert!(!body, "GET /version carries no body");
        assert_eq!(*auth, None, "a stale token would turn /version into a 401");
    }

    #[tokio::test]
    async fn version_surfaces_http_errors() {
        let http = Arc::new(RecordingHttp {
            seen: std::sync::Mutex::new(Vec::new()),
            reply: (502, "bad gateway".to_string()),
        });
        let db = HttpSqlDb::new(http, "http://localhost:8000", "ns", "db", "Basic z");
        let err = db.version().await.unwrap_err();
        assert!(matches!(err, DbError::Transport(ref m) if m.contains("502")), "{err:?}");
    }

    #[test]
    fn parses_version_body() {
        assert_eq!(parse_version_body("surrealdb-3.3.4"), "3.3.4");
        assert_eq!(parse_version_body("surrealdb-3.1.5\n"), "3.1.5");
        assert_eq!(parse_version_body("3.0.0"), "3.0.0");
        assert_eq!(parse_version_body(""), "unknown");
    }

    type Seen = (Method, String, bool, Option<String>);

    struct RecordingHttp {
        seen: std::sync::Mutex<Vec<Seen>>,
        reply: (u16, String),
    }
    #[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
    #[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
    impl HttpClient for RecordingHttp {
        async fn send(
            &self,
            req: OutboundRequest,
            _cancel: Option<crate::ports::CancelWatch>,
        ) -> Result<crate::ports::OutboundResponse, crate::ports::HttpError> {
            let auth = req
                .headers
                .iter()
                .find(|(k, _)| k == "Authorization")
                .map(|(_, v)| v.clone());
            self.seen
                .lock()
                .unwrap()
                .push((req.method, req.url, req.json_body.is_some(), auth));
            Ok(crate::ports::OutboundResponse {
                status: self.reply.0,
                body: self.reply.1.clone(),
            })
        }
    }

    #[test]
    fn top_level_parse_error_becomes_query_err() {
        let body = r#"{"id":1,"error":{"code":-32000,"message":"Parse error: bad"}}"#;
        let err = parse_rpc_response(body).unwrap_err();
        assert!(matches!(err, DbError::Query(m) if m.contains("Parse error")));
    }
}
