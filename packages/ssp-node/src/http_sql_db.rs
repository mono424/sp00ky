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
    /// `"Bearer <jwt>"` (SurrealDB Cloud token).
    authorization: String,
    /// The user and password of a `Basic` credential. SurrealDB verifies a
    /// Basic password with Argon2 on every request (53 ms of DB CPU each on
    /// the staging host, about 100 ms of latency per query from a Durable
    /// Object), so the adapter signs in with them once and sends the token.
    basic: Option<(String, String)>,
    /// The token of that sign-in; `None` before it and after an auth error.
    session: std::sync::Mutex<Option<String>>,
    /// Sign-in was refused: stay on the Basic credential.
    signin_refused: std::sync::atomic::AtomicBool,
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
        let authorization = authorization.into();
        Self {
            http,
            rpc_url: format!("{base}/rpc"),
            version_url: format!("{base}/version"),
            basic: basic_credentials(&authorization),
            session: std::sync::Mutex::new(None),
            signin_refused: std::sync::atomic::AtomicBool::new(false),
            authorization,
            ns: ns.into(),
            db: db.into(),
            timeout: DEFAULT_TIMEOUT,
        }
    }

    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// The binds go in as `LET` statements ahead of the query, not as RPC
    /// variables. SurrealDB's JSON RPC parses a string variable that looks
    /// like a record id, datetime or uuid into one (`"job:x"` became a record,
    /// which a `job_id: option<string>` field refused), while a quoted string in
    /// SurrealQL stays a string, which is what the SDK the VM shell uses sends
    /// too. `serde_json` writes raw UTF-8 and only `\"`, `\\`, `\n`-style and
    /// `\u00XX` escapes, all of which SurrealQL reads the same.
    fn request(&self, surql: &str, binds: &[(&str, Value)], authorization: &str) -> OutboundRequest {
        let mut query = String::with_capacity(surql.len() + 64 * binds.len());
        for (name, value) in binds {
            query.push_str("LET $");
            query.push_str(name);
            query.push_str(" = ");
            query.push_str(&serde_json::to_string(value).unwrap_or_else(|_| "NONE".into()));
            query.push_str(";\n");
        }
        query.push_str(surql);
        let envelope = json!({
            "id": 1,
            "method": "query",
            "params": [query, Value::Object(Map::new())],
        });
        OutboundRequest {
            method: Method::Post,
            url: self.rpc_url.clone(),
            bearer: None,
            headers: vec![
                ("Accept".to_string(), "application/json".to_string()),
                ("Authorization".to_string(), authorization.to_string()),
                ("surreal-ns".to_string(), self.ns.clone()),
                ("surreal-db".to_string(), self.db.clone()),
            ],
            json_body: Some(envelope),
            timeout: self.timeout,
        }
    }

    /// The header to send: a session token when a Basic credential could
    /// sign in, the configured header otherwise.
    async fn authorization(&self) -> String {
        let Some((user, pass)) = &self.basic else {
            return self.authorization.clone();
        };
        if self.signin_refused.load(std::sync::atomic::Ordering::Relaxed) {
            return self.authorization.clone();
        }
        if let Some(token) = self.session.lock().unwrap().clone() {
            return format!("Bearer {token}");
        }
        match self.signin(user, pass).await {
            Some(token) => {
                *self.session.lock().unwrap() = Some(token.clone());
                format!("Bearer {token}")
            }
            None => self.authorization.clone(),
        }
    }

    /// Sign in over RPC as a root user, then as a namespace user, then as a
    /// database user (the levels a Basic credential can name). A refusal at
    /// every level keeps the Basic credential for good; a transport failure
    /// only for this request.
    async fn signin(&self, user: &str, pass: &str) -> Option<String> {
        let levels = [
            json!({ "user": user, "pass": pass }),
            json!({ "ns": self.ns, "user": user, "pass": pass }),
            json!({ "ns": self.ns, "db": self.db, "user": user, "pass": pass }),
        ];
        for params in levels {
            let req = OutboundRequest {
                method: Method::Post,
                url: self.rpc_url.clone(),
                bearer: None,
                headers: vec![("Accept".to_string(), "application/json".to_string())],
                json_body: Some(json!({ "id": 1, "method": "signin", "params": [params] })),
                timeout: self.timeout,
            };
            let resp = self.http.send(req, None).await.ok()?;
            let body: Value = serde_json::from_str(&resp.body).unwrap_or(Value::Null);
            if let Some(token) = body.get("result").and_then(Value::as_str) {
                return Some(token.to_string());
            }
        }
        tracing::warn!("signing in with the Basic credential was refused; sending it on every request");
        self.signin_refused.store(true, std::sync::atomic::Ordering::Relaxed);
        None
    }

    async fn send_query(&self, surql: &str, binds: &[(&str, Value)], authorization: &str) -> Result<Vec<Value>, DbError> {
        let resp = self
            .http
            .send(self.request(surql, binds, authorization), None)
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
        // Drop the results of the `LET`s `request` put ahead of the query.
        let mut results = parse_rpc_response(&resp.body)?;
        results.drain(..binds.len().min(results.len()));
        Ok(results)
    }
}

/// The user and password inside a `Basic <base64>` header.
fn basic_credentials(authorization: &str) -> Option<(String, String)> {
    use base64::Engine as _;
    let encoded = authorization.strip_prefix("Basic ")?.trim();
    let decoded = base64::engine::general_purpose::STANDARD.decode(encoded).ok()?;
    let text = String::from_utf8(decoded).ok()?;
    let (user, pass) = text.split_once(':')?;
    Some((user.to_string(), pass.to_string()))
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
        let authorization = self.authorization().await;
        match self.send_query(surql, binds, &authorization).await {
            // The token expired or the server restarted with a new key: sign
            // in again and retry once.
            Err(DbError::Auth(_)) if authorization.starts_with("Bearer ") && self.basic.is_some() => {
                self.session.lock().unwrap().take();
                let authorization = self.authorization().await;
                self.send_query(surql, binds, &authorization).await
            }
            other => other,
        }
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

    /// A SurrealDB stand-in: `signin` hands out `tok<n>` (or refuses), a
    /// query answers 401 for any token but the current one.
    struct SigninHttp {
        log: std::sync::Mutex<Vec<String>>,
        issued: std::sync::atomic::AtomicUsize,
        refuse: bool,
    }
    #[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
    #[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
    impl HttpClient for SigninHttp {
        async fn send(
            &self,
            req: OutboundRequest,
            _cancel: Option<crate::ports::CancelWatch>,
        ) -> Result<crate::ports::OutboundResponse, crate::ports::HttpError> {
            use std::sync::atomic::Ordering;
            let body = req.json_body.unwrap_or(Value::Null);
            let auth = req.headers.iter().find(|(k, _)| k == "Authorization").map(|(_, v)| v.clone());
            let reply = |status, body: String| Ok(crate::ports::OutboundResponse { status, body });
            if body["method"] == "signin" {
                self.log.lock().unwrap().push("signin".into());
                if self.refuse || body["params"][0]["pass"] != "pw" {
                    return reply(200, r#"{"id":1,"error":{"code":-32000,"message":"There was a problem with authentication"}}"#.into());
                }
                let n = self.issued.fetch_add(1, Ordering::SeqCst) + 1;
                return reply(200, format!(r#"{{"id":1,"result":"tok{n}"}}"#));
            }
            let auth = auth.unwrap_or_default();
            self.log.lock().unwrap().push(auth.clone());
            let current = format!("Bearer tok{}", self.issued.load(Ordering::SeqCst));
            if auth == current || auth.starts_with("Basic ") {
                return reply(200, r#"{"id":1,"result":[{"status":"OK","result":1}]}"#.into());
            }
            reply(401, "InvalidToken".into())
        }
    }

    fn signin_db(refuse: bool) -> (Arc<SigninHttp>, HttpSqlDb) {
        use base64::Engine as _;
        let http = Arc::new(SigninHttp {
            log: std::sync::Mutex::new(Vec::new()),
            issued: std::sync::atomic::AtomicUsize::new(0),
            refuse,
        });
        let basic = format!("Basic {}", base64::engine::general_purpose::STANDARD.encode("root:pw"));
        (http.clone(), HttpSqlDb::new(http, "https://db.example", "ns", "db", basic))
    }

    #[tokio::test]
    async fn a_basic_credential_signs_in_once_and_sends_the_token() {
        let (http, db) = signin_db(false);
        db.query("RETURN 1", &[]).await.unwrap();
        db.query("RETURN 1", &[]).await.unwrap();
        assert_eq!(*http.log.lock().unwrap(), vec!["signin", "Bearer tok1", "Bearer tok1"]);
    }

    #[tokio::test]
    async fn an_expired_token_signs_in_again_and_retries_once() {
        let (http, db) = signin_db(false);
        db.query("RETURN 1", &[]).await.unwrap();
        // The server forgets the token (expiry, restart with a new key).
        http.issued.store(5, std::sync::atomic::Ordering::SeqCst);
        db.query("RETURN 1", &[]).await.unwrap();
        assert_eq!(
            *http.log.lock().unwrap(),
            vec!["signin", "Bearer tok1", "Bearer tok1", "signin", "Bearer tok6"]
        );
    }

    #[tokio::test]
    async fn a_refused_signin_keeps_the_basic_credential() {
        let (http, db) = signin_db(true);
        db.query("RETURN 1", &[]).await.unwrap();
        db.query("RETURN 1", &[]).await.unwrap();
        let log = http.log.lock().unwrap();
        // Three levels tried once, then Basic on every request.
        assert_eq!(log.iter().filter(|l| *l == "signin").count(), 3);
        assert_eq!(log.iter().filter(|l| l.starts_with("Basic ")).count(), 2);
    }

    struct EchoHttp {
        query: std::sync::Mutex<String>,
    }
    #[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
    #[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
    impl HttpClient for EchoHttp {
        async fn send(
            &self,
            req: OutboundRequest,
            _cancel: Option<crate::ports::CancelWatch>,
        ) -> Result<crate::ports::OutboundResponse, crate::ports::HttpError> {
            let body = req.json_body.unwrap_or(Value::Null);
            *self.query.lock().unwrap() = body["params"][0].as_str().unwrap_or_default().to_string();
            assert_eq!(body["params"][1], json!({}), "no RPC variables");
            Ok(crate::ports::OutboundResponse {
                status: 200,
                body: r#"{"id":1,"result":[{"status":"OK","result":null},{"status":"OK","result":null},{"status":"OK","result":"the answer"}]}"#.into(),
            })
        }
    }

    #[tokio::test]
    async fn binds_travel_as_let_statements_whose_results_are_dropped() {
        let http = Arc::new(EchoHttp { query: std::sync::Mutex::new(String::new()) });
        let db = HttpSqlDb::new(http.clone(), "https://db.example", "ns", "db", "Bearer t");
        let out = db
            .query("RETURN $id", &[("id", json!("job:x")), ("c", json!({ "s": "a\"b", "n": 1 }))])
            .await
            .unwrap();
        assert_eq!(out, vec![json!("the answer")]);
        assert_eq!(
            *http.query.lock().unwrap(),
            "LET $id = \"job:x\";\nLET $c = {\"n\":1,\"s\":\"a\\\"b\"};\nRETURN $id"
        );
    }

    #[test]
    fn only_a_basic_header_has_credentials() {
        assert_eq!(basic_credentials("Basic cm9vdDpyb290"), Some(("root".into(), "root".into())));
        assert_eq!(basic_credentials("Bearer abc"), None);
        assert_eq!(basic_credentials("Basic !!"), None);
    }

    #[test]
    fn top_level_parse_error_becomes_query_err() {
        let body = r#"{"id":1,"error":{"code":-32000,"message":"Parse error: bad"}}"#;
        let err = parse_rpc_response(body).unwrap_err();
        assert!(matches!(err, DbError::Query(m) if m.contains("Parse error")));
    }
}
