//! Spike S8: a SurrealDB conformance suite for sp00ky.
//!
//! One probe per SurrealDB feature sp00ky depends on, run against any endpoint
//! over the transport the Cloudflare SSP uses: `HttpSqlDb`, JSON-RPC on
//! `/rpc` behind the `HttpClient` port. Statements are copied from what the
//! CLI generates (`apps/cli/src/sp00ky.rs`, `schema_builder.rs`, the
//! `*_remote*.surql` files are applied verbatim) or built by the same
//! production helpers (`bootstrap_page_query`, `range_target`, the
//! impersonation `mint`), so a pass means production would work there.
//!
//! A small HTTP sink in this process stands in for the SSP / scheduler, so
//! `http::post` from events and functions is observed on the wire. `live.ts`
//! covers LIVE SELECT over WebSocket and is run at the end when node is found.
//! RESULTS.md says how to run it and what it found.

use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, Context};
use base64::Engine;
use serde_json::{json, Value};
use sha2::Digest;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use ssp_node::{
    CancelWatch, Db, HttpClient, HttpError, HttpSqlDb, Method, OutboundRequest, OutboundResponse,
};

type R = anyhow::Result<Verdict>;

const PROBE_TIMEOUT: Duration = Duration::from_secs(45);
const META_TABLES: &str = include_str!("../../../../apps/cli/src/meta_tables_remote.surql");
const FUNCTIONS_COMMON: &str = include_str!("../../../../apps/cli/src/functions_remote.surql");
const FUNCTIONS_HTTP: &str =
    include_str!("../../../../apps/cli/src/functions_remote_singlenode.surql");
const IMPERSONATION: &str = include_str!("../../../../apps/cli/src/impersonation_remote.surql");

// ---------------------------------------------------------------------------
// Outcomes and reporting
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Outcome {
    Pass,
    Fail,
    /// The server lacks the feature (parse error, unknown function, capability off).
    Unsupported,
    /// A regression probe reproduced a documented SurrealDB bug sp00ky works around.
    KnownBug,
    /// A prerequisite probe did not pass.
    Skip,
}

impl Outcome {
    fn label(self) -> &'static str {
        match self {
            Outcome::Pass => "pass",
            Outcome::Fail => "fail",
            Outcome::Unsupported => "unsupported",
            Outcome::KnownBug => "known-bug",
            Outcome::Skip => "skip",
        }
    }

    fn parse(s: &str) -> Outcome {
        match s {
            "pass" => Outcome::Pass,
            "unsupported" => Outcome::Unsupported,
            "known-bug" => Outcome::KnownBug,
            "skip" => Outcome::Skip,
            _ => Outcome::Fail,
        }
    }
}

struct Verdict {
    outcome: Outcome,
    detail: String,
}

fn pass(detail: impl Into<String>) -> R {
    Ok(Verdict {
        outcome: Outcome::Pass,
        detail: detail.into(),
    })
}
fn fail(detail: impl Into<String>) -> R {
    Ok(Verdict {
        outcome: Outcome::Fail,
        detail: detail.into(),
    })
}
fn known_bug(detail: impl Into<String>) -> R {
    Ok(Verdict {
        outcome: Outcome::KnownBug,
        detail: detail.into(),
    })
}
fn verdict(ok: bool, detail: impl Into<String>) -> R {
    if ok {
        pass(detail)
    } else {
        fail(detail)
    }
}

/// What an error means for the feature under test. Pure, so the mapping is
/// visible in one place. A probe can force a class with a `skip:` or
/// `unsupported:` prefix.
fn classify_error(msg: &str) -> Outcome {
    let m = msg.to_ascii_lowercase();
    if m.starts_with("skip:") {
        return Outcome::Skip;
    }
    if m.starts_with("unsupported:") {
        return Outcome::Unsupported;
    }
    const UNSUPPORTED: &[&str] = &[
        "parse error",
        "unexpected token",
        "invalid function",
        "unknown function",
        "no such method",
        "method not found",
        "not enabled",
        "experimental",
        "capabilit",
        "not supported",
        "not implemented",
        "is not allowed to be executed",
    ];
    if UNSUPPORTED.iter().any(|s| m.contains(s)) {
        Outcome::Unsupported
    } else {
        Outcome::Fail
    }
}

struct Report {
    group: String,
    id: String,
    outcome: Outcome,
    detail: String,
    ms: u128,
}

struct Runner {
    only: Option<String>,
    reports: Vec<Report>,
}

impl Runner {
    fn wants(&self, group: &str, id: &str) -> bool {
        self.only
            .as_deref()
            .map_or(true, |o| id.starts_with(o) || group == o)
    }

    async fn run(&mut self, group: &str, id: &str, probe: impl Future<Output = R>) -> Outcome {
        if !self.wants(group, id) {
            return Outcome::Skip;
        }
        let started = Instant::now();
        let v = match tokio::time::timeout(PROBE_TIMEOUT, probe).await {
            Ok(Ok(v)) => v,
            Ok(Err(e)) => {
                let detail = format!("{e:#}");
                Verdict {
                    outcome: classify_error(&detail),
                    detail,
                }
            }
            Err(_) => Verdict {
                outcome: Outcome::Fail,
                detail: format!("probe did not finish within {PROBE_TIMEOUT:?}"),
            },
        };
        let ms = started.elapsed().as_millis();
        println!(
            "{:<11} {:<44} {:>6}ms  {}",
            v.outcome.label().to_uppercase(),
            id,
            ms,
            one_line(&v.detail, 160)
        );
        self.reports.push(Report {
            group: group.into(),
            id: id.into(),
            outcome: v.outcome,
            detail: v.detail,
            ms,
        });
        v.outcome
    }
}

fn one_line(s: &str, max: usize) -> String {
    let flat: String = s.split_whitespace().collect::<Vec<_>>().join(" ");
    if flat.chars().count() > max {
        format!("{}...", flat.chars().take(max).collect::<String>())
    } else {
        flat
    }
}

fn render_markdown(reports: &[Report], version: &str, endpoint: &str) -> String {
    let mut counts: BTreeMap<&str, usize> = BTreeMap::new();
    for r in reports {
        *counts.entry(r.outcome.label()).or_default() += 1;
    }
    let summary: Vec<String> = counts.iter().map(|(k, v)| format!("{v} {k}")).collect();
    let mut out = format!(
        "Endpoint `{endpoint}`, SurrealDB `{version}`: {} probes, {}.\n\n| Group | Probe | Result | ms | Detail |\n|---|---|---|---|---|\n",
        reports.len(),
        summary.join(", ")
    );
    for r in reports {
        out.push_str(&format!(
            "| {} | `{}` | **{}** | {} | {} |\n",
            r.group,
            r.id,
            r.outcome.label(),
            r.ms,
            one_line(&r.detail, 400).replace('|', "\\|")
        ));
    }
    out
}

// ---------------------------------------------------------------------------
// HTTP client port (reqwest), the same shape as the VM's `ReqwestHttp`
// ---------------------------------------------------------------------------

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
        if let Some(bearer) = &req.bearer {
            b = b.bearer_auth(bearer);
        }
        for (k, v) in &req.headers {
            b = b.header(k, v);
        }
        if let Some(body) = &req.json_body {
            b = b.json(body);
        }
        let resp = b
            .send()
            .await
            .map_err(|e| HttpError::Transport(e.to_string()))?;
        let status = resp.status().as_u16();
        let body = resp
            .text()
            .await
            .map_err(|e| HttpError::Transport(e.to_string()))?;
        Ok(OutboundResponse { status, body })
    }
}

// ---------------------------------------------------------------------------
// Sink: stands in for the SSP / scheduler HTTP endpoints
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
struct SinkReq {
    path: String,
    headers: BTreeMap<String, String>,
    body: String,
    at: Instant,
}

impl SinkReq {
    fn json(&self) -> Value {
        serde_json::from_str(&self.body).unwrap_or(Value::Null)
    }
}

#[derive(Clone)]
struct Sink {
    log: Arc<Mutex<Vec<SinkReq>>>,
    port: u16,
}

/// How the sink answers a path. Pure. `/status/<code>` and `/hold/<ms>`
/// segments anywhere in the path set the status and a delay;
/// `/impersonate/mint` signs a real impersonation token with sp00ky's own
/// minting code, like the SSP route of the same name.
fn sink_reply(path: &str, body: &str, secret: &str) -> (u16, u64, &'static str, String) {
    let segs: Vec<&str> = path.split('/').collect();
    let after = |name: &str| {
        segs.iter()
            .position(|s| *s == name)
            .and_then(|i| segs.get(i + 1))
            .and_then(|v| v.parse::<u64>().ok())
    };
    let status = after("status").unwrap_or(200) as u16;
    let hold = after("hold").unwrap_or(0);
    if path.ends_with("/impersonate/mint") {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        let minted = serde_json::from_str::<ssp_protocol::impersonation::MintRequest>(body)
            .map_err(|e| format!("bad mint body: {e}"))
            .and_then(|req| {
                ssp_protocol::impersonation::mint(secret, &req, now).map_err(|e| format!("{e:?}"))
            });
        return match minted {
            Ok(m) => (
                200,
                hold,
                "application/json",
                json!({ "token": m.token, "exp": m.exp }).to_string(),
            ),
            Err(e) => (
                422,
                hold,
                "application/json",
                json!({ "error": e }).to_string(),
            ),
        };
    }
    let info = r#"{"ok":true,"source":"conformance-sink"}"#.to_string();
    if path.ends_with("/info/text") {
        return (status, hold, "text/plain", info);
    }
    if path.ends_with("/info") {
        return (status, hold, "application/json", info);
    }
    (
        status,
        hold,
        "application/json",
        r#"{"ok":true}"#.to_string(),
    )
}

impl Sink {
    async fn start(bind: &str, secret: String) -> anyhow::Result<Sink> {
        let listener = TcpListener::bind(bind)
            .await
            .with_context(|| format!("sink bind {bind}"))?;
        let port = listener.local_addr()?.port();
        let log = Arc::new(Mutex::new(Vec::new()));
        let sink = Sink {
            log: log.clone(),
            port,
        };
        let secret = Arc::new(secret);
        tokio::spawn(async move {
            loop {
                let Ok((sock, _)) = listener.accept().await else {
                    continue;
                };
                let (log, secret) = (log.clone(), secret.clone());
                tokio::spawn(async move {
                    let _ = handle_sink_conn(sock, log, &secret).await;
                });
            }
        });
        Ok(sink)
    }

    fn requests(&self, prefix: &str) -> Vec<SinkReq> {
        self.log
            .lock()
            .unwrap()
            .iter()
            .filter(|r| r.path.starts_with(prefix))
            .cloned()
            .collect()
    }

    /// Requests under `prefix` once `n` have arrived or `within` passed.
    async fn wait(&self, prefix: &str, n: usize, within: Duration) -> Vec<SinkReq> {
        let deadline = Instant::now() + within;
        loop {
            let got = self.requests(prefix);
            if got.len() >= n || Instant::now() >= deadline {
                return got;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }
}

async fn handle_sink_conn(
    mut sock: TcpStream,
    log: Arc<Mutex<Vec<SinkReq>>>,
    secret: &str,
) -> anyhow::Result<()> {
    let mut buf = Vec::new();
    let mut tmp = [0u8; 16384];
    let head_end = loop {
        let n = sock.read(&mut tmp).await?;
        if n == 0 {
            bail!("eof before headers");
        }
        buf.extend_from_slice(&tmp[..n]);
        if let Some(i) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
            break i + 4;
        }
    };
    let head = String::from_utf8_lossy(&buf[..head_end]).to_string();
    let mut lines = head.split("\r\n");
    let path = lines
        .next()
        .unwrap_or("")
        .split(' ')
        .nth(1)
        .unwrap_or("/")
        .to_string();
    let headers: BTreeMap<String, String> = lines
        .filter_map(|l| l.split_once(':'))
        .map(|(k, v)| (k.trim().to_ascii_lowercase(), v.trim().to_string()))
        .collect();
    let chunked = headers
        .get("transfer-encoding")
        .is_some_and(|v| v.contains("chunked"));
    let len: usize = headers
        .get("content-length")
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);
    let mut body = buf[head_end..].to_vec();
    if chunked {
        while !body.windows(5).any(|w| w == b"0\r\n\r\n") {
            let n = sock.read(&mut tmp).await?;
            if n == 0 {
                break;
            }
            body.extend_from_slice(&tmp[..n]);
        }
        body = dechunk(&body);
    } else {
        while body.len() < len {
            let n = sock.read(&mut tmp).await?;
            if n == 0 {
                break;
            }
            body.extend_from_slice(&tmp[..n]);
        }
    }
    let body = String::from_utf8_lossy(&body).to_string();
    let (status, hold, ctype, reply) = sink_reply(&path, &body, secret);
    log.lock().unwrap().push(SinkReq {
        path,
        headers,
        body,
        at: Instant::now(),
    });
    if hold > 0 {
        tokio::time::sleep(Duration::from_millis(hold)).await;
    }
    let resp = format!(
        "HTTP/1.1 {status} X\r\nContent-Type: {ctype}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{reply}",
        reply.len()
    );
    sock.write_all(resp.as_bytes()).await?;
    sock.shutdown().await.ok();
    Ok(())
}

fn dechunk(raw: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    let mut rest = raw;
    while let Some(i) = rest.windows(2).position(|w| w == b"\r\n") {
        let size =
            usize::from_str_radix(String::from_utf8_lossy(&rest[..i]).trim(), 16).unwrap_or(0);
        if size == 0 || rest.len() < i + 2 + size {
            break;
        }
        out.extend_from_slice(&rest[i + 2..i + 2 + size]);
        rest = &rest[(i + 4 + size).min(rest.len())..];
    }
    out
}

// ---------------------------------------------------------------------------
// Context
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct User {
    name: String,
    pass: String,
    id: String,
    token: String,
}

struct Ctx {
    http: Arc<dyn HttpClient>,
    base: String,
    ns: String,
    db: String,
    root_auth: String,
    sink: Sink,
    sink_url: String,
    secret: String,
    run_id: String,
    users: Mutex<BTreeMap<&'static str, User>>,
    async_form: Mutex<Option<String>>,
    feed_cursor: Mutex<Option<u64>>,
}

impl Ctx {
    fn db_with(&self, authorization: String) -> HttpSqlDb {
        HttpSqlDb::new(
            self.http.clone(),
            &self.base,
            &self.ns,
            &self.db,
            authorization,
        )
    }
    fn root(&self) -> HttpSqlDb {
        self.db_with(self.root_auth.clone())
    }
    async fn q(&self, sql: &str) -> anyhow::Result<Vec<Value>> {
        Ok(self.root().query(sql, &[]).await?)
    }
    async fn qb(&self, sql: &str, binds: &[(&str, Value)]) -> anyhow::Result<Vec<Value>> {
        Ok(self.root().query(sql, binds).await?)
    }
    /// Value of the last statement.
    async fn q1(&self, sql: &str) -> anyhow::Result<Value> {
        Ok(self.q(sql).await?.pop().unwrap_or(Value::Null))
    }
    async fn q_as(&self, user: &User, sql: &str) -> anyhow::Result<Value> {
        let db = self.db_with(format!("Bearer {}", user.token));
        Ok(db.query(sql, &[]).await?.pop().unwrap_or(Value::Null))
    }
    fn user(&self, who: &'static str) -> anyhow::Result<User> {
        self.users.lock().unwrap().get(who).cloned().ok_or_else(|| {
            anyhow!("skip: needs signed-in record users (access.record_signup_signin)")
        })
    }
    fn endpoint(&self, path: &str) -> String {
        format!("{}/p/{path}", self.sink_url)
    }
    /// Point the generated events at a sink path (`$sp00ky_endpoint`, as `spky migrate` sets it).
    async fn set_endpoint(&self, path: &str) -> anyhow::Result<()> {
        self.q(&format!(
            "DEFINE PARAM OVERWRITE $sp00ky_endpoint VALUE '{}' PERMISSIONS NONE;",
            self.endpoint(path)
        ))
        .await?;
        Ok(())
    }
    async fn exists(&self, rid: &str) -> anyhow::Result<bool> {
        Ok(self
            .q1(&format!("SELECT VALUE id FROM {rid};"))
            .await?
            .as_array()
            .is_some_and(|a| !a.is_empty()))
    }
    fn rpc_headers(&self, auth: Option<&str>) -> Vec<(String, String)> {
        let mut h = vec![
            ("Accept".to_string(), "application/json".to_string()),
            ("surreal-ns".to_string(), self.ns.clone()),
            ("surreal-db".to_string(), self.db.clone()),
        ];
        if let Some(a) = auth {
            h.push(("Authorization".to_string(), a.to_string()));
        }
        h
    }
    /// A raw JSON-RPC call on the same port, for the methods `HttpSqlDb` does not wrap (signup/signin).
    async fn rpc(&self, method: &str, params: Value, auth: Option<&str>) -> anyhow::Result<Value> {
        let resp = self
            .http
            .send(
                OutboundRequest {
                    method: Method::Post,
                    url: format!("{}/rpc", self.base),
                    bearer: None,
                    headers: self.rpc_headers(auth),
                    json_body: Some(json!({ "id": 1, "method": method, "params": params })),
                    timeout: Duration::from_secs(30),
                },
                None,
            )
            .await?;
        let v: Value = serde_json::from_str(&resp.body)
            .with_context(|| format!("HTTP {}: {}", resp.status, resp.body))?;
        if let Some(err) = v.get("error") {
            bail!(
                "{}",
                err.get("message")
                    .and_then(Value::as_str)
                    .map(str::to_string)
                    .unwrap_or_else(|| err.to_string())
            );
        }
        Ok(v.get("result").cloned().unwrap_or(Value::Null))
    }
}

fn arr(v: &Value) -> Vec<Value> {
    v.as_array().cloned().unwrap_or_default()
}

fn s(v: &Value) -> String {
    v.as_str()
        .map(str::to_string)
        .unwrap_or_else(|| v.to_string())
}

fn sha256_hex(input: &str) -> String {
    sha2::Sha256::digest(input.as_bytes())
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

/// Header and claims of a JWT, unverified.
fn jwt_parts(token: &str) -> anyhow::Result<(Value, Value)> {
    let mut it = token.split('.');
    let dec = |p: Option<&str>| -> anyhow::Result<Value> {
        let raw = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(p.ok_or_else(|| anyhow!("not a JWT"))?)?;
        Ok(serde_json::from_slice(&raw)?)
    };
    Ok((dec(it.next())?, dec(it.next())?))
}

fn token_of(v: &Value) -> Option<String> {
    v.as_str().map(str::to_string).or_else(|| {
        v.get("token")
            .or_else(|| v.get("access"))
            .and_then(Value::as_str)
            .map(str::to_string)
    })
}

// ---------------------------------------------------------------------------
// Statement shapes copied from the CLI generator
// ---------------------------------------------------------------------------

/// The ingest call every generated event makes (`sp00ky.rs::ingest_post`).
fn ingest_post(timeout_secs: u64) -> String {
    format!(
        "    SELECT * FROM http::post($sp00ky_endpoint + '/ingest', $payload, {{ \"Authorization\": \"Bearer \" + $sp00ky_secret }}) TIMEOUT {timeout_secs}s;\n"
    )
}

/// `_00_<t>_mutation` + `_00_<t>_delete` exactly as `generate_sp00ky_events`
/// emits them (singlenode/cluster, http transport) for a table with a string,
/// a record link, a datetime and an int field.
fn sp00ky_events(t: &str) -> String {
    let fields_after = "        at: <string>($after.at OR \"\"),\n        n: $after.n,\n        owner: <string>($after.owner OR \"\"),\n        title: $after.title,\n";
    let fields_before = fields_after.replace("$after", "$before");
    format!(
        "DEFINE EVENT OVERWRITE _00_{t}_mutation ON TABLE {t}\n\
WHEN $before != $after AND $event != \"DELETE\"\n\
THEN {{\n\
    LET $sp00ky_ver_rec = IF $event = \"CREATE\" {{\n\
        (CREATE _00_version SET record_id = $after.id, version = 1 RETURN AFTER)\n\
    }} ELSE IF $event = \"UPDATE\" {{\n\
        IF $sp00ky_target_version != NONE AND $sp00ky_target_version.id == $after.id {{\n\
            LET $u = (UPDATE _00_version SET version = <int>$sp00ky_target_version.version WHERE record_id = $after.id RETURN AFTER);\n\
            LET $sp00ky_target_version = NONE;\n\
            $u\n\
        }} ELSE {{\n\
            (UPDATE _00_version SET version += 1 WHERE record_id = $after.id RETURN AFTER)\n\
        }}\n\
    }};\n\
    LET $sp00ky_ver = $sp00ky_ver_rec[0].version;\n\n\
    LET $plain_after = {{\n\
        id: <string>($after.id OR \"\"),\n\
{fields_after}\
        _00_rv: (SELECT VALUE version FROM ONLY _00_version WHERE record_id = $after.id)\n\
    }};\n\
    LET $payload = {{\n\
        table: '{t}',\n\
        op: $event,\n\
        id: <string>($after.id OR \"\"),\n\
        record: $plain_after,\n\
        hash: \"\"\n\
    }};\n\
{post}\
}};\n\n\
DEFINE EVENT OVERWRITE _00_{t}_delete ON TABLE {t}\n\
WHEN $event = \"DELETE\"\n\
THEN {{\n\
    DELETE _00_version WHERE record_id = $before.id;\n\n\
    LET $plain_before = {{\n\
        id: <string>($before.id OR \"\"),\n\
{fields_before}\
    }};\n\
    LET $payload = {{\n\
        table: '{t}',\n\
        op: \"DELETE\",\n\
        id: <string>($before.id OR \"\"),\n\
        record: $plain_before,\n\
        hash: \"\"\n\
    }};\n\
{post}\
}};\n",
        post = ingest_post(10)
    )
}

/// The internal schema an http-transport project gets from `spky migrate`:
/// params (schema_builder.rs), meta tables, common + singlenode functions.
fn internal_schema(c: &Ctx) -> String {
    let meta = META_TABLES.replace(
        "let $result = mod::dbsp::unregister_view(<string>$before.id);",
        "let $payload = { id: <string>$before.id };\n    let $result = http::post($sp00ky_endpoint + '/view/unregister', $payload, { \"Authorization\": \"Bearer \" + $sp00ky_secret });",
    );
    let functions = FUNCTIONS_HTTP
        .replace("{{ENDPOINT}}", &c.endpoint("fn"))
        .replace("{{SECRET}}", &c.secret);
    format!(
        "DEFINE PARAM OVERWRITE $sp00ky_endpoint VALUE '{}' PERMISSIONS NONE;\nDEFINE PARAM OVERWRITE $sp00ky_secret VALUE '{}' PERMISSIONS NONE;\n\n{meta}\n{FUNCTIONS_COMMON}\n{functions}",
        c.endpoint("ingest"),
        c.secret
    )
}

fn impersonation_schema(c: &Ctx) -> anyhow::Result<String> {
    let key = ssp_protocol::impersonation::derive_key(&c.secret)
        .ok_or_else(|| anyhow!("empty secret"))?;
    Ok(IMPERSONATION
        .replace("{{ACCESS_NAME}}", ssp_protocol::impersonation::ACCESS_NAME)
        .replace("{{IMPERSONATION_KEY}}", &key)
        .replace("{{MAX_DURATION}}", "1h")
        .replace("{{TOKEN_TTL_SECS}}", "900")
        .replace("{{TOKEN_TTL}}", "15m")
        .replace("{{USER_TABLE}}", "user")
        .replace("{{SEARCH_FIELDS}}", "['username']")
        .replace("{{ENDPOINT}}", &c.endpoint("imp"))
        .replace("{{SECRET}}", &c.secret))
}

// ---------------------------------------------------------------------------
// Probes: transport and schema
// ---------------------------------------------------------------------------

/// `GET /version`, the one version source that answers on 3.1.5 and 3.3.4.
async fn http_version(c: &Ctx) -> anyhow::Result<String> {
    let resp = c
        .http
        .send(
            OutboundRequest {
                method: Method::Get,
                url: format!("{}/version", c.base),
                bearer: None,
                headers: vec![],
                json_body: None,
                timeout: Duration::from_secs(10),
            },
            None,
        )
        .await?;
    if resp.status != 200 {
        bail!("GET /version -> {} {}", resp.status, resp.body);
    }
    Ok(resp.body.trim().to_string())
}

async fn transport_version(c: &Ctx) -> R {
    pass(format!("GET /version = {}", http_version(c).await?))
}

/// `HttpSqlDb::version()` runs `RETURN surrealdb::version()`, which no 3.x
/// server parses. Not a SurrealDB gap: a sp00ky adapter bug this suite found.
async fn transport_httpsqldb_version(c: &Ctx) -> R {
    match c.root().version().await {
        Ok(v) => pass(format!("HttpSqlDb::version() = {v}")),
        Err(e) => known_bug(format!(
            "sp00ky-side: HttpSqlDb::version() sends `RETURN surrealdb::version()`, which is not a SurrealQL function ({}); fix by reading GET /version",
            one_line(&e.to_string(), 100)
        )),
    }
}

async fn transport_multi_statement(c: &Ctx) -> R {
    let out = c
        .q("RETURN 1; RETURN 'two'; LET $x = 3; RETURN $x;")
        .await?;
    verdict(
        out.len() == 4 && out[0] == json!(1) && out[1] == json!("two") && out[3] == json!(3),
        format!("one result per statement: {out:?}"),
    )
}

async fn schema_internal_apply(c: &Ctx) -> R {
    let sql = internal_schema(c);
    let n = c.q(&sql).await?.len();
    pass(format!("meta_tables_remote.surql + functions_remote*.surql + DEFINE PARAMs applied verbatim ({n} statements)"))
}

async fn schema_user_tables(c: &Ctx) -> R {
    c.q(&format!(
        "DEFINE TABLE OVERWRITE cf_item SCHEMAFULL PERMISSIONS FULL;
         DEFINE FIELD OVERWRITE title ON TABLE cf_item TYPE option<string>;
         DEFINE FIELD OVERWRITE owner ON TABLE cf_item TYPE option<record<user>>;
         DEFINE FIELD OVERWRITE at ON TABLE cf_item TYPE option<datetime>;
         DEFINE FIELD OVERWRITE n ON TABLE cf_item TYPE option<int>;
         {}",
        sp00ky_events("cf_item")
    ))
    .await?;
    pass("SCHEMAFULL table + generated _00_cf_item_mutation / _00_cf_item_delete events defined")
}

// ---------------------------------------------------------------------------
// Probes: access
// ---------------------------------------------------------------------------

const ACCESS_SCHEMA: &str = "
DEFINE TABLE OVERWRITE user SCHEMAFULL PERMISSIONS FOR select, create WHERE true, FOR update, delete WHERE $access = 'account' AND id = $auth.id;
DEFINE FIELD OVERWRITE username ON TABLE user TYPE string;
DEFINE FIELD OVERWRITE password ON TABLE user TYPE string;
DEFINE INDEX OVERWRITE idx_user_username ON TABLE user COLUMNS username UNIQUE;
DEFINE ACCESS OVERWRITE account ON DATABASE TYPE RECORD
  SIGNUP { RETURN CREATE ONLY user SET username = $username, password = crypto::argon2::generate($password); }
  SIGNIN ( SELECT * FROM user WHERE username = $username AND crypto::argon2::compare(password, $password) )
  DURATION FOR TOKEN 15m, FOR SESSION 4w2d;
";

async fn access_record_signup_signin(c: &Ctx) -> R {
    c.q(ACCESS_SCHEMA).await?;
    let mut notes = Vec::new();
    for who in ["alice", "bob"] {
        let name = format!("{who}_{}", c.run_id);
        let pass = format!("pw-{who}-{}", c.run_id);
        let vars = |upper: bool| {
            if upper {
                json!([{ "NS": c.ns, "DB": c.db, "AC": "account", "username": name, "password": pass }])
            } else {
                json!([{ "namespace": c.ns, "database": c.db, "access": "account", "username": name, "password": pass }])
            }
        };
        let up = match c.rpc("signup", vars(true), None).await {
            Ok(v) => v,
            Err(e) => {
                notes.push(format!(
                    "signup NS/DB/AC form failed ({e}), trying lowercase"
                ));
                c.rpc("signup", vars(false), None).await?
            }
        };
        token_of(&up).ok_or_else(|| anyhow!("signup returned no token: {up}"))?;
        let si = match c.rpc("signin", vars(true), None).await {
            Ok(v) => v,
            Err(_) => c.rpc("signin", vars(false), None).await?,
        };
        let token = token_of(&si).ok_or_else(|| anyhow!("signin returned no token: {si}"))?;
        let (header, claims) = jwt_parts(&token)?;
        let id = claims
            .get("ID")
            .or_else(|| claims.get("id"))
            .map(s)
            .ok_or_else(|| anyhow!("no ID claim: {claims}"))?;
        if who == "alice" {
            notes.push(format!(
                "signin result shape {}; JWT alg {}; claims {:?}",
                if si.is_string() { "string" } else { "object" },
                header["alg"],
                claims
                    .as_object()
                    .map(|o| o.keys().cloned().collect::<Vec<_>>())
                    .unwrap_or_default()
            ));
            if header["alg"] != json!("HS512") {
                return fail(format!("record access token is not HS512: {header}"));
            }
        }
        c.users.lock().unwrap().insert(
            who,
            User {
                name,
                pass,
                id,
                token,
            },
        );
    }
    let info = c.q1("INFO FOR DB;").await?;
    let acc = s(&info["accesses"]["account"]);
    notes.push(format!("INFO FOR DB renders: {}", one_line(&acc, 120)));
    pass(notes.join("; "))
}

async fn access_session_vars(c: &Ctx) -> R {
    let alice = c.user("alice")?;
    let v = c
        .q_as(&alice, "RETURN { auth: <string>$auth.id, access: $access, sid: <string>session::id(), ns: session::ns(), db: session::db(), tok_ac: $token.AC, guard: fn::sp00ky::session_live() };")
        .await?;
    verdict(
        v["auth"] == json!(alice.id)
            && v["access"] == json!("account")
            && v["guard"] == json!(true),
        format!("over Bearer on /rpc: {v}"),
    )
}

async fn access_param_permissions_none(c: &Ctx) -> R {
    let bob = c.user("bob")?;
    let root = c.q1("RETURN $sp00ky_secret;").await?;
    let user = c.q_as(&bob, "RETURN $sp00ky_secret;").await;
    let leaked = matches!(&user, Ok(v) if *v == json!(c.secret));
    verdict(
        root == json!(c.secret) && !leaked,
        format!(
            "root reads it; record user gets {:?}",
            user.map(|v| v.to_string())
                .map_err(|e| one_line(&e.to_string(), 120))
        ),
    )
}

async fn access_row_permissions(c: &Ctx) -> R {
    let (alice, bob) = (c.user("alice")?, c.user("bob")?);
    c.q("DEFINE TABLE OVERWRITE cf_thread SCHEMAFULL PERMISSIONS FOR select WHERE published = true OR $access = 'account' AND author.id = $auth.id, FOR create, delete WHERE $access = 'account' AND author.id = $auth.id, FOR update WHERE $access = 'account' AND (author.id = $auth.id OR $auth.id INSIDE (SELECT VALUE in FROM cf_collab WHERE out = $parent.id));
         DEFINE FIELD OVERWRITE title ON TABLE cf_thread TYPE string;
         DEFINE FIELD OVERWRITE author ON TABLE cf_thread TYPE record<user>;
         DEFINE FIELD OVERWRITE published ON TABLE cf_thread TYPE bool DEFAULT false;
         DEFINE INDEX OVERWRITE idx_cf_thread_author ON TABLE cf_thread COLUMNS author;
         DEFINE TABLE OVERWRITE cf_collab TYPE RELATION IN user OUT cf_thread SCHEMAFULL PERMISSIONS FOR select WHERE true, FOR create, update WHERE false, FOR delete WHERE $access = 'account' AND (in.id = $auth.id OR out.author.id = $auth.id);")
        .await?;
    let count = |v: &Value| v.as_array().map_or(0, Vec::len);
    let mut steps = Vec::new();
    let mut check = |name: &str, ok: bool, got: String| {
        steps.push(format!(
            "{}{name}: {got}",
            if ok { "" } else { "MISMATCH " }
        ));
        ok
    };
    c.q_as(
        &alice,
        "CREATE cf_thread:t1 SET title = 'a', author = $auth.id;",
    )
    .await?;
    let mut ok = check(
        "bob cannot read alice's draft",
        count(&c.q_as(&bob, "SELECT * FROM cf_thread:t1;").await?) == 0,
        "0 rows".into(),
    );
    let a = c.q_as(&alice, "SELECT * FROM cf_thread:t1;").await?;
    ok &= check(
        "alice reads own draft",
        count(&a) == 1,
        format!("{} rows", count(&a)),
    );
    let forged = c
        .q_as(
            &bob,
            &format!(
                "CREATE cf_thread:t2 SET title = 'b', author = {};",
                alice.id
            ),
        )
        .await;
    ok &= check(
        "bob cannot create as alice",
        !c.exists("cf_thread:t2").await?,
        format!("{:?}", forged.map_err(|e| one_line(&e.to_string(), 80))),
    );
    let _ = c
        .q_as(&bob, "UPDATE cf_thread:t1 SET title = 'hacked';")
        .await;
    let title = c.q1("SELECT VALUE title FROM ONLY cf_thread:t1;").await?;
    ok &= check("bob cannot update", title == json!("a"), title.to_string());
    c.q_as(&alice, "UPDATE cf_thread:t1 SET published = true;")
        .await?;
    ok &= check(
        "bob reads once published",
        count(&c.q_as(&bob, "SELECT * FROM cf_thread:t1;").await?) == 1,
        "1 row".into(),
    );
    c.q(&format!("RELATE {}->cf_collab->cf_thread:t1;", bob.id))
        .await?;
    let _ = c
        .q_as(&bob, "UPDATE cf_thread:t1 SET title = 'collab';")
        .await;
    let title = c.q1("SELECT VALUE title FROM ONLY cf_thread:t1;").await?;
    ok &= check(
        "collaborator update via $parent.id subquery",
        title == json!("collab"),
        title.to_string(),
    );
    verdict(ok, steps.join("; "))
}

async fn access_impersonation_external_jwt(c: &Ctx) -> R {
    let (alice, bob) = (c.user("alice")?, c.user("bob")?);
    c.q(&impersonation_schema(c)?).await?;
    c.q(&format!(
        "CREATE _00_admin SET user = {}, note = 'conformance';",
        alice.id
    ))
    .await?;
    // The whole production path: start() runs as the admin, posts to the
    // mint route (here the sink, signing with sp00ky's mint()), and returns
    // the token; SurrealDB then verifies it against the HS256 key and runs
    // the AUTHENTICATE block on every request.
    let started = c
        .q_as(
            &alice,
            &format!(
                "RETURN fn::_00_impersonate::start('{}', 'conformance probe');",
                bob.id
            ),
        )
        .await?;
    let token = started["token"]
        .as_str()
        .ok_or_else(|| anyhow!("start() returned no token: {started}"))?
        .to_string();
    let session = s(&started["session"]);
    let imp = User {
        token,
        ..bob.clone()
    };
    let who = c
        .q_as(&imp, "RETURN { auth: <string>$auth.id, access: $access, imp: $token.spky_imp, as_access: $token.spky_as_access, current: fn::_00_impersonate::current() };")
        .await?;
    let ok = who["auth"] == json!(bob.id)
        && who["access"] == json!("_00_impersonate")
        && who["as_access"] == json!("account")
        && who["current"].is_object();
    c.q_as(
        &alice,
        &format!("RETURN fn::_00_impersonate::stop('{session}');"),
    )
    .await?;
    let after = c.q_as(&imp, "RETURN <string>$auth.id;").await;
    let refused = after.is_err();
    verdict(
        ok && refused,
        format!(
            "as impersonated: {who}; after stop(): {}",
            match after {
                Ok(v) => format!("STILL ACCEPTED ({v})"),
                Err(e) => format!("refused ({})", one_line(&e.to_string(), 100)),
            }
        ),
    )
}

// ---------------------------------------------------------------------------
// Probes: functions and DEFINE API
// ---------------------------------------------------------------------------

async fn fn_session_guard(c: &Ctx) -> R {
    let alice = c.user("alice")?;
    let live = c.q_as(&alice, "RETURN fn::sp00ky::session_live();").await?;
    let probe = c
        .q_as(&alice, "SELECT live FROM ONLY _00_session_guard:probe;")
        .await?;
    verdict(
        live == json!(true) && probe["live"] == json!(true),
        format!("session_live() = {live}; probe row = {probe}"),
    )
}

async fn fn_query_register(c: &Ctx) -> R {
    let alice = c.user("alice")?;
    let out = c
        .q_as(&alice, "RETURN fn::query::register({ id: '_00_query:cfq1', surql: 'SELECT * FROM cf_thread', params: { limit: 10 }, ttl: '30m', owner: 'page-secret' });")
        .await?;
    let got = c
        .sink
        .wait("/p/fn/view/register", 1, Duration::from_secs(5))
        .await;
    let Some(req) = got.first() else {
        return fail(format!(
            "no POST reached /view/register; function returned {out}"
        ));
    };
    let body = req.json();
    let keys: BTreeSet<String> = body
        .as_object()
        .map(|o| o.keys().cloned().collect())
        .unwrap_or_default();
    let want: BTreeSet<String> = [
        "id",
        "surql",
        "params",
        "clientId",
        "ttl",
        "lastActiveAt",
        "format",
        "ownerHash",
    ]
    .iter()
    .map(|k| k.to_string())
    .collect();
    // `<string>$params_with_auth` is SurrealQL object text, not JSON; the SSP
    // parses it as such, so only check that the injected values are in it.
    let params = s(&body["params"]);
    let missing: Vec<_> = want.difference(&keys).collect();
    let ok = missing.is_empty()
        && params.contains(&alice.id)
        && params.contains("account")
        && body["ownerHash"] == json!(sha256_hex("page-secret"))
        && req.headers.get("authorization") == Some(&format!("Bearer {}", c.secret));
    verdict(ok, format!("missing keys {missing:?}; params `{params}`; clientId {}; ownerHash = sha256(owner): {}", body["clientId"], body["ownerHash"] == json!(sha256_hex("page-secret"))))
}

async fn fn_query_heartbeat(c: &Ctx) -> R {
    let alice = c.user("alice")?;
    c.q(&format!("CREATE _00_query:cfq2 SET surql = 'SELECT * FROM cf_thread', ttl = 30m, auth_id = '{}', subscribers = [];", alice.id)).await?;
    c.q_as(&alice, "fn::query::heartbeat(_00_query:cfq2);")
        .await?;
    c.q_as(&alice, "fn::query::heartbeat(_00_query:cfq2);")
        .await?;
    let subs = c
        .q1("SELECT VALUE subscribers FROM ONLY _00_query:cfq2;")
        .await?;
    let n = subs.as_array().map_or(0, Vec::len);
    verdict(n >= 1, format!("2 heartbeats over HTTP -> {n} subscriber entries (an HTTP request is its own session::id()): {subs}"))
}

async fn fn_query_leave_unsubscribe(c: &Ctx) -> R {
    let alice = c.user("alice")?;
    let leave = c
        .q_as(
            &alice,
            "RETURN fn::query::leave(_00_query:cfq2, ['not-a-session']);",
        )
        .await?;
    let unsub = c
        .q_as(&alice, "RETURN fn::query::unsubscribe(_00_query:cfq2);")
        .await?;
    verdict(
        leave["leaving"].is_boolean()
            && leave["remaining"].is_number()
            && unsub["released"].is_boolean()
            && unsub["remaining"].is_number(),
        format!("leave -> {leave}; unsubscribe -> {unsub}"),
    )
}

async fn fn_spooky_info(c: &Ctx) -> R {
    let alice = c.user("alice")?;
    let v = c.q_as(&alice, "RETURN fn::spooky::info();").await?;
    verdict(
        v["source"] == json!("conformance-sink"),
        format!("http::get inside a PERMISSIONS FULL function returned {v}"),
    )
}

async fn fn_throw_text(c: &Ctx) -> R {
    let alice = c.user("alice")?;
    let msg = "There was a problem with authentication: this session has ended, sign in again";
    c.q(&format!(
        "DEFINE FUNCTION OVERWRITE fn::cf::refuse() {{ THROW \"{msg}\"; }} PERMISSIONS FULL;"
    ))
    .await?;
    match c.q_as(&alice, "RETURN fn::cf::refuse();").await {
        Ok(v) => fail(format!("THROW did not error: {v}")),
        Err(e) => verdict(
            e.to_string().contains(msg),
            format!("client sees: {}", one_line(&e.to_string(), 200)),
        ),
    }
}

async fn api_define_api(c: &Ctx) -> R {
    let mut tries = Vec::new();
    for path in [
        format!("/api/{}/{}/spooky", c.ns, c.db),
        "/api/spooky".to_string(),
    ] {
        let resp = c
            .http
            .send(
                OutboundRequest {
                    method: Method::Get,
                    url: format!("{}{path}", c.base),
                    bearer: None,
                    headers: c.rpc_headers(Some(&c.root_auth)),
                    json_body: None,
                    timeout: Duration::from_secs(15),
                },
                None,
            )
            .await?;
        if resp.status == 200 && resp.body.contains("conformance-sink") {
            return pass(format!("GET {path} -> 200 {}", one_line(&resp.body, 120)));
        }
        tries.push(format!(
            "GET {path} -> {} {}",
            resp.status,
            one_line(&resp.body, 120)
        ));
    }
    fail(tries.join("; "))
}

// ---------------------------------------------------------------------------
// Probes: builtins (one row each so a missing function names itself)
// ---------------------------------------------------------------------------

const BUILTINS: &[(&str, &str, &str)] = &[
    (
        "crypto_sha256",
        "crypto::sha256('abc')",
        r#""ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad""#,
    ),
    (
        "crypto_argon2",
        "crypto::argon2::compare(crypto::argon2::generate('pw'), 'pw')",
        "true",
    ),
    (
        "object_extend",
        "object::extend({ a: 1 }, { b: 2 })",
        r#"{"a":1,"b":2}"#,
    ),
    (
        "array_filter_closure",
        "array::filter([1, 2, 3], |$x| $x > 1)",
        "[2,3]",
    ),
    (
        "array_clump_map_closure",
        "array::clump([1, 2, 3, 4, 5], 2).map(|$c| $c[0])",
        "[1,3,5]",
    ),
    (
        "array_append_flatten_len",
        "array::len(array::flatten([array::append([1], 2), [3]]))",
        "3",
    ),
    ("duration_arith", "<duration>'1h' + 30m", r#""1h30m""#),
    (
        "duration_from_column",
        "<duration>(string::concat(<string>1500, 'ms'))",
        r#""1s500ms""#,
    ),
    ("duration_secs", "duration::secs(90s)", "90"),
    (
        "datetime_plus_duration",
        "<datetime>'2026-01-01T00:00:00Z' + 1h > <datetime>'2026-01-01T00:30:00Z'",
        "true",
    ),
    ("time_now_minus", "time::now() - 1m < time::now()", "true"),
    (
        "type_record_two_args",
        "<string>type::record('_00_query', 'abc')",
        r#""_00_query:abc""#,
    ),
    (
        "type_record_one_arg",
        "record::tb(type::record('user:x'))",
        r#""user""#,
    ),
    (
        "record_exists",
        "record::exists(type::record('_00_query', 'nobody_here'))",
        "false",
    ),
    ("record_cast_string", "<string>user:x", r#""user:x""#),
    (
        "contains_notinside",
        "[1, 2] CONTAINS 2 AND 'x' NOTINSIDE ['y']",
        "true",
    ),
    (
        "string_fns",
        "string::ends_with('a.jpg', '.jpg') AND string::len(string::trim('  abc ')) = 3",
        "true",
    ),
    (
        "null_coalesce",
        "$cf_missing ?? 'fallback'",
        r#""fallback""#,
    ),
    ("or_fallback", "(NONE OR 'x')", r#""x""#),
    (
        "if_else_expression",
        "IF 1 > 2 { 'a' } ELSE IF 1 > 0 { 'b' } ELSE { 'c' }",
        r#""b""#,
    ),
    (
        "session_fns",
        "session::ns() != NONE AND session::db() != NONE",
        "true",
    ),
];

async fn builtin(c: &Ctx, expr: &str, want: &str) -> R {
    let got = c.q1(&format!("RETURN {expr};")).await?;
    let want: Value = serde_json::from_str(want)?;
    verdict(got == want, format!("RETURN {expr} = {got}"))
}

// ---------------------------------------------------------------------------
// Probes: events
// ---------------------------------------------------------------------------

async fn event_in_txn_vars(c: &Ctx) -> R {
    c.q("DEFINE TABLE OVERWRITE cf_ev SCHEMALESS PERMISSIONS FULL;
         DEFINE TABLE OVERWRITE cf_ev_log SCHEMALESS PERMISSIONS FULL;
         DEFINE EVENT OVERWRITE cf_log ON TABLE cf_ev WHEN $before != $after THEN {
             CREATE cf_ev_log SET ev = $event, rid = <string>($after.id OR $before.id), before = $before.n, after = $after.n;
         };")
        .await?;
    c.q("CREATE cf_ev:a SET n = 1;").await?;
    c.q("UPDATE cf_ev:a SET n = 2;").await?;
    c.q("DELETE cf_ev:a;").await?;
    let rows = c
        .q1("SELECT ev, before, after FROM cf_ev_log WHERE rid = 'cf_ev:a';")
        .await?;
    let mut seen: Vec<String> = arr(&rows)
        .iter()
        .map(|r| format!("{}:{}->{}", s(&r["ev"]), r["before"], r["after"]))
        .collect();
    seen.sort();
    let want = vec!["CREATE:null->1", "DELETE:2->null", "UPDATE:1->2"];
    verdict(
        seen == want,
        format!("$event/$before/$after seen: {seen:?}"),
    )
}

async fn event_cancelled_txn(c: &Ctx) -> R {
    let _ = c
        .q("BEGIN TRANSACTION; CREATE cf_ev:c SET n = 9; CANCEL TRANSACTION;")
        .await;
    let rows = c
        .q1("SELECT * FROM cf_ev_log WHERE rid = 'cf_ev:c';")
        .await?;
    verdict(
        arr(&rows).is_empty() && !c.exists("cf_ev:c").await?,
        format!("event side effects of a cancelled txn: {rows}"),
    )
}

async fn event_throw_aborts_write(c: &Ctx) -> R {
    c.q("DEFINE TABLE OVERWRITE cf_guard SCHEMALESS PERMISSIONS FULL;
         DEFINE EVENT OVERWRITE cf_guard ON TABLE cf_guard WHEN $after.bad = true THEN { THROW 'conformance guard: write refused' };")
        .await?;
    let res = c.q("CREATE cf_guard:x SET bad = true;").await;
    let exists = c.exists("cf_guard:x").await?;
    match res {
        Ok(v) => fail(format!("write succeeded despite THROW: {v:?}")),
        Err(e) => verdict(
            !exists && e.to_string().contains("conformance guard"),
            format!(
                "error: {}; row exists afterwards: {exists}",
                one_line(&e.to_string(), 120)
            ),
        ),
    }
}

async fn event_sp00ky_ingest(c: &Ctx) -> R {
    c.set_endpoint("ingest").await?;
    c.q("CREATE cf_item:a SET title = 't', owner = user:x, at = d'2026-01-01T00:00:00Z', n = 1;")
        .await?;
    c.q("UPDATE cf_item:a SET title = 'u';").await?;
    c.q("DELETE cf_item:a;").await?;
    let got = c.sink.wait("/p/ingest/", 3, Duration::from_secs(5)).await;
    let bodies: Vec<Value> = got.iter().map(SinkReq::json).collect();
    let ops: Vec<String> = bodies.iter().map(|b| s(&b["op"])).collect();
    let rvs: Vec<Value> = bodies
        .iter()
        .map(|b| b["record"]["_00_rv"].clone())
        .collect();
    let auth_ok = got
        .iter()
        .all(|r| r.headers.get("authorization") == Some(&format!("Bearer {}", c.secret)));
    let first = bodies.first().cloned().unwrap_or(Value::Null);
    let leftover = c
        .q1("SELECT * FROM _00_version WHERE record_id = cf_item:a;")
        .await?;
    let ok = ops == ["CREATE", "UPDATE", "DELETE"]
        && rvs[0] == json!(1)
        && rvs[1] == json!(2)
        && first["record"]["owner"] == json!("user:x")
        && first["record"]["at"].is_string()
        && first["table"] == json!("cf_item")
        && auth_ok
        && arr(&leftover).is_empty();
    verdict(ok, format!("ops {ops:?}, _00_rv {rvs:?}, bearer ok {auth_ok}, first payload {first}, _00_version left {leftover}"))
}

async fn event_outer_let_visible(c: &Ctx) -> R {
    c.set_endpoint("tv").await?;
    c.q("CREATE cf_item:tv SET n = 1;").await?;
    c.q(
        "LET $sp00ky_target_version = { id: cf_item:tv, version: 7 }; UPDATE cf_item:tv SET n = 2;",
    )
    .await?;
    let v = c
        .q1("SELECT VALUE version FROM ONLY _00_version WHERE record_id = cf_item:tv LIMIT 1;")
        .await?;
    verdict(
        v == json!(7),
        format!(
            "_00_version after UPDATE with $sp00ky_target_version = 7 in the same request: {v}"
        ),
    )
}

async fn http_post_order(c: &Ctx) -> R {
    c.set_endpoint("order").await?;
    c.q("BEGIN TRANSACTION; CREATE cf_item:o1 SET n = 1; CREATE cf_item:o2 SET n = 2; CREATE cf_item:o3 SET n = 3; COMMIT TRANSACTION;").await?;
    let got = c.sink.wait("/p/order/", 3, Duration::from_secs(5)).await;
    let ids: Vec<String> = got.iter().map(|r| s(&r.json()["id"])).collect();
    verdict(
        ids == ["cf_item:o1", "cf_item:o2", "cf_item:o3"],
        format!("arrival order {ids:?}"),
    )
}

async fn http_post_in_txn(c: &Ctx) -> R {
    c.set_endpoint("vis/hold/1500").await?;
    let db = c.root();
    let write = tokio::spawn(async move { db.query("CREATE cf_item:v1 SET n = 1;", &[]).await });
    let got = c.sink.wait("/p/vis/", 1, Duration::from_secs(5)).await;
    if got.is_empty() {
        return fail("event never posted");
    }
    let during = c.exists("cf_item:v1").await?;
    write.await??;
    let after = c.exists("cf_item:v1").await?;
    verdict(!during && after, format!("row visible to another request while http::post is in flight: {during}; after the write returned: {after} (in-txn: the post runs before commit)"))
}

async fn http_non_2xx_aborts(c: &Ctx) -> R {
    c.set_endpoint("n2xx/status/500").await?;
    let res = c.q("CREATE cf_item:e1 SET n = 1;").await;
    let exists = c.exists("cf_item:e1").await?;
    let ver = c
        .q1("SELECT * FROM _00_version WHERE record_id = cf_item:e1;")
        .await?;
    match res {
        Ok(_) => fail(format!(
            "write committed although /ingest answered 500 (row exists {exists})"
        )),
        Err(e) => verdict(
            !exists && arr(&ver).is_empty(),
            format!(
                "write refused: {}; row exists {exists}",
                one_line(&e.to_string(), 140)
            ),
        ),
    }
}

async fn http_timeout_aborts(c: &Ctx) -> R {
    c.q(&format!(
        "DEFINE TABLE OVERWRITE cf_slow SCHEMALESS PERMISSIONS FULL;
         DEFINE EVENT OVERWRITE cf_slow ON TABLE cf_slow WHEN $event = 'CREATE' THEN {{
             LET $payload = {{ id: <string>$after.id }};
         {}}};",
        ingest_post(1)
    ))
    .await?;
    c.set_endpoint("slow/hold/4000").await?;
    let t = Instant::now();
    let res = c.q("CREATE cf_slow:s1 SET n = 1;").await;
    let ms = t.elapsed().as_millis();
    let exists = c.exists("cf_slow:s1").await?;
    match res {
        Ok(_) => fail(format!(
            "write committed after {ms}ms despite TIMEOUT 1s against a 4s sink"
        )),
        Err(e) => verdict(
            !exists && ms < 3500,
            format!("aborted after {ms}ms: {}", one_line(&e.to_string(), 400)),
        ),
    }
}

async fn http_refused_aborts(c: &Ctx) -> R {
    c.q("DEFINE PARAM OVERWRITE $sp00ky_endpoint VALUE 'http://127.0.0.1:9' PERMISSIONS NONE;")
        .await?;
    let res = c.q("CREATE cf_item:r1 SET n = 1;").await;
    let exists = c.exists("cf_item:r1").await?;
    match res {
        Ok(_) => fail("write committed although /ingest was unreachable"),
        Err(e) => verdict(
            !exists,
            format!("write refused: {}", one_line(&e.to_string(), 140)),
        ),
    }
}

async fn http_get_parsed(c: &Ctx) -> R {
    let v = c
        .q1(&format!(
            "RETURN http::get('{}', {{}});",
            c.endpoint("get/info")
        ))
        .await?;
    verdict(
        v["source"] == json!("conformance-sink"),
        format!("http::get body decoded as {v}"),
    )
}

async fn regress_http_post_field_stripping(c: &Ctx) -> R {
    c.q(&format!(
        "LET $body = {{ existing: 'visible', added: 'invisible' }}; RETURN http::post('{}', $body, {{}});
         LET $ext = object::extend({{ existing: 'visible' }}, {{ added: 'extended' }}); RETURN http::post('{}', $ext, {{}});",
        c.endpoint("strip/literal"),
        c.endpoint("strip/extend")
    ))
    .await?;
    let got = c.sink.wait("/p/strip/", 2, Duration::from_secs(5)).await;
    let bodies: Vec<Value> = got.iter().map(SinkReq::json).collect();
    let intact = bodies.len() == 2
        && bodies
            .iter()
            .all(|b| b.get("added").is_some() && b.get("existing").is_some());
    if intact {
        pass(format!("all keys arrive: {bodies:?} (docs/surrealdb-bugs/http-post-field-stripping.md not reproduced)"))
    } else {
        known_bug(format!("bodies on the wire: {bodies:?}"))
    }
}

// ASYNC events: syntax first, then behaviour.

const ASYNC_BODY: &str = "{ SELECT * FROM http::post($sp00ky_endpoint + '/async', { id: <string>$after.id }, {}) TIMEOUT 5s; }";

async fn event_async_syntax(c: &Ctx) -> R {
    c.q("DEFINE TABLE OVERWRITE cf_async SCHEMALESS PERMISSIONS FULL;")
        .await?;
    let forms = [
        "DEFINE EVENT OVERWRITE cf_async ON TABLE cf_async ASYNC WHEN $event = 'CREATE' THEN BODY;",
        "DEFINE EVENT OVERWRITE cf_async ON TABLE cf_async ASYNC RETRY 1 MAXDEPTH 3 WHEN $event = 'CREATE' THEN BODY;",
        "DEFINE EVENT OVERWRITE cf_async ON TABLE cf_async WHEN $event = 'CREATE' THEN BODY ASYNC;",
    ];
    let mut errors = Vec::new();
    for form in forms {
        let sql = form.replace("BODY", ASYNC_BODY);
        match c.q(&sql).await {
            Ok(_) => {
                let info = c.q1("INFO FOR TABLE cf_async;").await?;
                let rendered = s(&info["events"]["cf_async"]);
                *c.async_form.lock().unwrap() = Some(form.to_string());
                return verdict(
                    rendered.to_uppercase().contains("ASYNC"),
                    format!(
                        "accepted: `{}`; INFO renders `{}`",
                        one_line(form, 120),
                        one_line(&rendered, 200)
                    ),
                );
            }
            Err(e) => errors.push(one_line(&e.to_string(), 120)),
        }
    }
    bail!("unsupported: no ASYNC form parsed: {}", errors.join(" | "))
}

fn need_async(c: &Ctx) -> anyhow::Result<()> {
    if c.async_form.lock().unwrap().is_none() {
        bail!("skip: needs event.async_syntax");
    }
    Ok(())
}

async fn event_async_post_commit(c: &Ctx) -> R {
    need_async(c)?;
    c.set_endpoint("async1/hold/1500").await?;
    let t = Instant::now();
    c.q("CREATE cf_async:a1 SET n = 1;").await?;
    let write_ms = t.elapsed().as_millis();
    let got = c.sink.wait("/p/async1/", 1, Duration::from_secs(10)).await;
    let Some(first) = got.first() else {
        return fail(format!(
            "write returned in {write_ms}ms but the async event never posted within 10s"
        ));
    };
    let delay = first.at.duration_since(t).as_millis();
    let visible = c.exists("cf_async:a1").await?;
    verdict(
        visible && write_ms < 1500,
        format!("write returned in {write_ms}ms (sink holds 1500ms); post arrived {delay}ms after the write started; row visible during the post: {visible}"),
    )
}

async fn event_async_non_2xx(c: &Ctx) -> R {
    need_async(c)?;
    c.set_endpoint("async2/status/500").await?;
    let res = c.q("CREATE cf_async:a2 SET n = 1;").await;
    tokio::time::sleep(Duration::from_secs(4)).await;
    let attempts = c.sink.requests("/p/async2/").len();
    let exists = c.exists("cf_async:a2").await?;
    verdict(
        res.is_ok() && exists,
        format!("write result ok={}; row exists {exists}; posts received in 4s: {attempts} (retries visible as >1)", res.is_ok()),
    )
}

/// Would ASYNC events keep order if ingest moved post-commit? Strict on
/// commits that follow each other; order inside one transaction is reported,
/// not required (it varies run to run on 3.1.5 and 3.3.4, and every payload
/// carries `_00_rv`, which is what the SSP orders by).
async fn event_async_order(c: &Ctx) -> R {
    need_async(c)?;
    c.set_endpoint("async5").await?;
    for i in 0..8 {
        c.q(&format!("CREATE cf_async:o{i} SET n = {i};")).await?;
    }
    c.q("BEGIN TRANSACTION; CREATE cf_async:t1 SET n = 1; CREATE cf_async:t2 SET n = 2; CREATE cf_async:t3 SET n = 3; COMMIT TRANSACTION;").await?;
    let got = c.sink.wait("/p/async5/", 11, Duration::from_secs(10)).await;
    let ids: Vec<String> = got
        .iter()
        .map(|r| s(&r.json()["id"]).replace("cf_async:", ""))
        .collect();
    let commits: Vec<String> = (0..8).map(|i| format!("o{i}")).collect();
    let mut txn: Vec<String> = ids.iter().skip(8).cloned().collect();
    let txn_ordered = txn == ["t1", "t2", "t3"];
    txn.sort();
    verdict(
        ids.len() == 11 && ids[..8] == commits[..] && txn == ["t1", "t2", "t3"],
        format!(
            "8 sequential commits arrive in commit order; rows of one txn {} ({ids:?})",
            if txn_ordered {
                "arrived in statement order this run"
            } else {
                "arrived OUT of statement order"
            }
        ),
    )
}

async fn event_async_rollback(c: &Ctx) -> R {
    need_async(c)?;
    c.set_endpoint("async3").await?;
    let _ = c
        .q("BEGIN TRANSACTION; CREATE cf_async:a3 SET n = 1; CANCEL TRANSACTION;")
        .await;
    let _ = c
        .q("BEGIN TRANSACTION; CREATE cf_async:a4 SET n = 1; THROW 'abort'; COMMIT TRANSACTION;")
        .await;
    tokio::time::sleep(Duration::from_secs(3)).await;
    let got = c.sink.requests("/p/async3/");
    verdict(
        got.is_empty(),
        format!("posts for cancelled / thrown txns within 3s: {}", got.len()),
    )
}

// ---------------------------------------------------------------------------
// Probes: graph (the SSP's edge statements and the client's membership reads)
// ---------------------------------------------------------------------------

async fn graph_relate_edges(c: &Ctx) -> R {
    let alice = c.user("alice")?;
    c.q(&format!(
        "CREATE _00_query:cfq3 SET surql = 'SELECT * FROM cf_thread', ttl = 30m, auth_id = '{0}', clientId = 'conformance';
         CREATE cf_thread:g2 SET title = 'g2', author = {0};",
        alice.id
    ))
    .await?;
    let sql = "BEGIN TRANSACTION;
        LET $from0 = type::record('_00_query', $from0key);
        LET $cid0 = (SELECT VALUE clientId FROM $from0 LIMIT 1)[0];
        LET $aid0 = (SELECT VALUE auth_id FROM $from0 LIMIT 1)[0];
        DELETE $from0->_00_list_ref;
        RELATE $from0->_00_list_ref->cf_thread:t1 SET version = 1, clientId = $cid0, auth_id = $aid0;
        RELATE $from0->_00_list_ref->cf_thread:g2 SET version = 3, clientId = $cid0, auth_id = $aid0;
        COMMIT TRANSACTION;";
    c.qb(sql, &[("from0key", json!("cfq3"))]).await?;
    let edges = c.q1("SELECT in, out, version, clientId, auth_id FROM _00_list_ref WHERE in = _00_query:cfq3 ORDER BY version;").await?;
    let rows = arr(&edges);
    verdict(
        rows.len() == 2
            && rows[0]["auth_id"] == json!(alice.id)
            && rows[0]["clientId"] == json!("conformance"),
        format!("edge txn as the SSP writes it -> {edges}"),
    )
}

async fn graph_traverse_membership(c: &Ctx) -> R {
    let (alice, bob) = (c.user("alice")?, c.user("bob")?);
    let single = "LET $in = type::record('_00_query', 'cfq3'); SELECT out, version FROM $in->_00_list_ref WHERE parent IS NONE;";
    let batch = "LET $ins = [type::record('_00_query', 'cfq3')]; SELECT in, out, version, parent FROM $ins->_00_list_ref;";
    let a1 = c.q_as(&alice, single).await?;
    let a2 = c.q_as(&alice, batch).await?;
    let b1 = c.q_as(&bob, single).await?;
    let flat = arr(&a2)
        .iter()
        .all(|r| r.get("in").is_some() && r.get("out").is_some());
    verdict(
        arr(&a1).len() == 2 && arr(&a2).len() == 2 && flat && arr(&b1).is_empty(),
        format!(
            "alice $in->T: {} rows, $ins->T: {} flat rows; bob (edge perms): {} rows",
            arr(&a1).len(),
            arr(&a2).len(),
            arr(&b1).len()
        ),
    )
}

async fn graph_edge_delete_forms(c: &Ctx) -> R {
    c.q("LET $from0 = type::record('_00_query', 'cfq3'); DELETE (SELECT VALUE id FROM cf_thread:g2<-_00_list_ref WHERE in = $from0);").await?;
    let left = arr(&c
        .q1("SELECT VALUE id FROM _00_list_ref WHERE in = _00_query:cfq3;")
        .await?)
    .len();
    let regress = c.q("LET $from0 = type::record('_00_query', 'cfq3'); DELETE $from0->_00_list_ref WHERE out = cf_thread:t1;").await;
    c.q(
        "DELETE array::flatten((SELECT VALUE id FROM [cf_thread:t1, cf_thread:g2]<-_00_list_ref));",
    )
    .await?;
    let end = arr(&c
        .q1("SELECT VALUE id FROM _00_list_ref WHERE in = _00_query:cfq3;")
        .await?)
    .len();
    let note = match &regress {
        Ok(_) => "`DELETE $from->edge WHERE out = x` works (3.0.5 bug gone)".to_string(),
        Err(e) => format!("`DELETE $from->edge WHERE out = x` still errors ({}), production uses the subquery form", one_line(&e.to_string(), 80)),
    };
    verdict(
        left == 1 && end == 0,
        format!("subquery DELETE left {left} edge; flatten DELETE left {end}; {note}"),
    )
}

// ---------------------------------------------------------------------------
// Probes: introspection, read through sp00ky's own parsers
// ---------------------------------------------------------------------------

async fn info_for_db(c: &Ctx) -> R {
    let info = c.q1("INFO FOR DB;").await?;
    let keys: Vec<String> = info
        .as_object()
        .map(|o| o.keys().cloned().collect())
        .unwrap_or_default();
    let table_is_string = info["tables"]["_00_version"]
        .as_str()
        .is_some_and(|d| d.starts_with("DEFINE TABLE"));
    let probe = ssp_node::bootstrap::probe_schema(&c.root()).await?;
    let synced: Vec<String> = probe
        .as_ref()
        .map(|p| p.synced.keys().cloned().collect())
        .unwrap_or_default();
    verdict(
        table_is_string && probe.is_some() && synced.iter().any(|t| t == "cf_thread"),
        format!("top-level keys {keys:?}; SchemaProbe::parse synced tables {synced:?}"),
    )
}

async fn info_for_table(c: &Ctx) -> R {
    let info = c.q1("INFO FOR DB;").await?;
    let define = s(&info["tables"]["cf_thread"]);
    let meta = ssp_node::bootstrap::load_table_meta(&c.root(), "cf_thread", &define).await;
    let idx: Vec<String> = meta
        .indexes
        .iter()
        .map(|i| format!("{}({})", i.name, i.fields.join(",")))
        .collect();
    verdict(
        meta.permission.contains("published")
            && meta.link_targets.get("author").map(String::as_str) == Some("user")
            && meta.columns.contains("title")
            && idx.iter().any(|i| i.starts_with("idx_cf_thread_author")),
        format!(
            "load_table_meta: permission `{}`, links {:?}, columns {:?}, indexes {idx:?}",
            one_line(&meta.permission, 100),
            meta.link_targets,
            meta.columns
        ),
    )
}

async fn info_for_index(c: &Ctx) -> R {
    let v = c
        .q1("INFO FOR INDEX idx_record_id ON TABLE _00_version;")
        .await?;
    verdict(v.is_object(), format!("INFO FOR INDEX -> {v}"))
}

// ---------------------------------------------------------------------------
// Probes: scans
// ---------------------------------------------------------------------------

async fn scan_keyset_bootstrap(c: &Ctx) -> R {
    c.q("DEFINE TABLE OVERWRITE cf_scan SCHEMALESS PERMISSIONS FULL;
         CREATE cf_scan:a; CREATE cf_scan:b; CREATE cf_scan:⟨d-e⟩; CREATE cf_scan:10; CREATE cf_scan:zz; CREATE cf_scan:c;")
        .await?;
    let mut after: Option<String> = None;
    let mut ids = Vec::new();
    for _ in 0..10 {
        let sql = ssp_node::bootstrap::bootstrap_page_query(
            "cf_scan",
            2,
            after.as_deref(),
            &BTreeSet::new(),
        );
        let page = arr(&c.q1(&sql).await?);
        if page.is_empty() {
            break;
        }
        for row in &page {
            ids.push(s(&row["id"]));
        }
        after = Some(s(&page.last().unwrap()["id"]));
    }
    let unique: BTreeSet<&String> = ids.iter().collect();
    verdict(
        ids.len() == 6 && unique.len() == 6,
        format!("bootstrap_page_query pages of 2 -> {ids:?}"),
    )
}

async fn scan_record_range(c: &Ctx) -> R {
    use ssp_protocol::range_hash::{boundary_query_every, range_target, range_target_after};
    c.q("DEFINE TABLE OVERWRITE cf_rng SCHEMALESS PERMISSIONS FULL; FOR $k IN ['a', 'b', 'c', 'd', 'e', 'f'] { CREATE type::record('cf_rng', $k) SET n = 1; };").await?;
    let ids = |v: Value| arr(&v).iter().map(s).collect::<Vec<_>>();
    let both = ids(c
        .q1(&format!(
            "SELECT VALUE id FROM {};",
            range_target("cf_rng", Some("b"), Some("e"))
        ))
        .await?);
    let after = ids(c
        .q1(&format!(
            "SELECT VALUE id FROM {};",
            range_target_after("cf_rng", "b", Some("e"))
        ))
        .await?);
    let below = ids(c
        .q1(&format!(
            "SELECT VALUE id FROM {};",
            range_target("cf_rng", None, Some("c"))
        ))
        .await?);
    let bounds = ids(c.q1(&boundary_query_every("cf_rng", 2)).await?);
    verdict(
        both == ["cf_rng:b", "cf_rng:c", "cf_rng:d"] && after == ["cf_rng:c", "cf_rng:d"] && below == ["cf_rng:a", "cf_rng:b"] && bounds == ["cf_rng:a", "cf_rng:c", "cf_rng:e"],
        format!("t:⟨b⟩..⟨e⟩ {both:?}; t:⟨b⟩>..⟨e⟩ {after:?}; t:..⟨c⟩ {below:?}; boundary_query {bounds:?}"),
    )
}

async fn scan_order_by_projection(c: &Ctx) -> R {
    let ok = c
        .q1("SELECT id, n FROM cf_rng ORDER BY id DESC LIMIT 2;")
        .await?;
    let strict = c.q("SELECT VALUE <string>id FROM cf_rng ORDER BY n;").await;
    verdict(
        arr(&ok).len() == 2,
        format!(
            "projected ORDER BY works; unprojected `SELECT VALUE <string>id ... ORDER BY n`: {}",
            match strict {
                Ok(_) => "accepted".to_string(),
                Err(e) => format!("rejected ({})", one_line(&e.to_string(), 100)),
            }
        ),
    )
}

// ---------------------------------------------------------------------------
// Probes: indexes and transactions
// ---------------------------------------------------------------------------

async fn index_unique(c: &Ctx) -> R {
    c.q("CREATE _00_version SET record_id = cf_x:dup, version = 1;")
        .await?;
    let dup = c
        .q("CREATE _00_version SET record_id = cf_x:dup, version = 2;")
        .await;
    c.q("DELETE _00_version WHERE record_id = cf_x:dup;")
        .await?;
    match dup {
        Ok(_) => fail("duplicate record_id accepted by idx_record_id UNIQUE"),
        Err(e) => pass(format!(
            "duplicate refused: {}",
            one_line(&e.to_string(), 140)
        )),
    }
}

async fn index_overwrite_rebuild(c: &Ctx) -> R {
    c.q("DEFINE TABLE OVERWRITE cf_uidx SCHEMALESS PERMISSIONS FULL; FOR $i IN 0..500 { CREATE type::record('cf_uidx', $i) SET k = $i; };").await?;
    c.q("DEFINE INDEX OVERWRITE idx_cf_uidx_k ON TABLE cf_uidx COLUMNS k UNIQUE;")
        .await?;
    let found = c.q1("RETURN [0, 1, 57, 123, 250, 333, 404, 498, 499].map(|$k| array::len((SELECT VALUE id FROM cf_uidx WHERE k = $k)));").await?;
    let dup = c.q("CREATE cf_uidx SET k = 7;").await;
    let status = c
        .q1("INFO FOR INDEX idx_cf_uidx_k ON TABLE cf_uidx;")
        .await?;
    let all_found = arr(&found).iter().all(|n| *n == json!(1));
    verdict(all_found && dup.is_err(), format!("index lookups after OVERWRITE rebuild of 500 rows {found}; duplicate refused {}; status {status}", dup.is_err()))
}

async fn index_concurrently(c: &Ctx) -> R {
    c.q("DEFINE INDEX IF NOT EXISTS idx_cf_uidx_conc ON cf_uidx COLUMNS k CONCURRENTLY;")
        .await?;
    let status = c
        .q1("INFO FOR INDEX idx_cf_uidx_conc ON TABLE cf_uidx;")
        .await?;
    pass(format!(
        "trailing CONCURRENTLY accepted (outbox index shape); status {status}"
    ))
}

async fn regress_return_in_txn(c: &Ctx) -> R {
    let res = c.q("BEGIN TRANSACTION; CREATE cf_ret:a SET n = 1; RETURN 1; THROW 'probe'; COMMIT TRANSACTION;").await;
    let exists = c.exists("cf_ret:a").await?;
    let shape = match &res {
        Ok(v) => format!("OK {v:?}"),
        Err(e) => format!("ERR {}", one_line(&e.to_string(), 80)),
    };
    if exists {
        known_bug(format!("RETURN ended the transaction and committed the CREATE before THROW ({shape}); see memory feedback_surreal_return_commits_txn"))
    } else {
        pass(format!("write rolled back ({shape})"))
    }
}

async fn regress_select_star_subquery_param(c: &Ctx) -> R {
    c.q("CREATE cf_sp:hello SET title = 'hi'; CREATE cf_spc:c1 SET thread = cf_sp:hello, text = 'first';").await?;
    let v = c.q1("LET $id = cf_sp:hello; SELECT *, (SELECT * FROM cf_spc WHERE thread = $parent.id) AS comments FROM $id;").await?;
    let row = arr(&v).first().cloned().unwrap_or(Value::Null);
    if row["title"] == json!("hi") && row["comments"].is_array() {
        pass(format!("`*` kept with a subquery over a bound id: {row}"))
    } else {
        known_bug(format!(
            "docs/surrealdb-bugs/select-from-param-subquery-drops-star.md reproduces: {row}"
        ))
    }
}

// ---------------------------------------------------------------------------
// Probes: changefeed
// ---------------------------------------------------------------------------

async fn show_changes(c: &Ctx, since: u64) -> anyhow::Result<Vec<Value>> {
    Ok(arr(&c
        .q1(&format!(
            "SHOW CHANGES FOR DATABASE SINCE {since} LIMIT 1000;"
        ))
        .await?))
}

fn mentions(entries: &[Value], needle: &str) -> bool {
    entries.iter().any(|e| e.to_string().contains(needle))
}

async fn cf_define(c: &Ctx) -> R {
    c.q("DEFINE TABLE OVERWRITE cf_feed SCHEMALESS CHANGEFEED 1h INCLUDE ORIGINAL PERMISSIONS FULL;
         DEFINE TABLE OVERWRITE cf_feed2 SCHEMALESS PERMISSIONS FULL;
         ALTER TABLE IF EXISTS cf_feed2 CHANGEFEED 1h INCLUDE ORIGINAL;
         DEFINE EVENT OVERWRITE cf_feed_guard ON TABLE cf_feed WHEN $after.bad = true THEN { THROW 'conformance: refused' };")
        .await?;
    let info = c.q1("INFO FOR DB;").await?;
    let (a, b) = (
        s(&info["tables"]["cf_feed"]),
        s(&info["tables"]["cf_feed2"]),
    );
    verdict(
        a.contains("CHANGEFEED") && b.contains("CHANGEFEED"),
        format!(
            "DEFINE renders `{}`; ALTER renders `{}`",
            one_line(&a, 120),
            one_line(&b, 120)
        ),
    )
}

async fn cf_show_changes(c: &Ctx) -> R {
    let now_s = c
        .q1("RETURN time::unix(time::now());")
        .await?
        .as_u64()
        .ok_or_else(|| anyhow!("time::unix not a number"))?;
    let cursor = (now_s * 1000 - 2000) << 16;
    *c.feed_cursor.lock().unwrap() = Some(cursor);
    c.q("CREATE cf_feed:a SET x = 1;").await?;
    c.q("UPDATE cf_feed:a SET x = 2;").await?;
    c.q("DELETE cf_feed:a;").await?;
    c.q("CREATE cf_feed2:b SET y = 1;").await?;
    let entries = show_changes(c, cursor).await?;
    let ours: Vec<&Value> = entries
        .iter()
        .filter(|e| e.to_string().contains("cf_feed"))
        .collect();
    let stamps: Vec<u64> = ours
        .iter()
        .filter_map(|e| e["versionstamp"].as_u64())
        .collect();
    let ordered = stamps.windows(2).all(|w| w[0] < w[1]);
    let stamp_ms_ok = stamps
        .first()
        .is_some_and(|vs| (vs >> 16).abs_diff(now_s * 1000) < 120_000);
    let kinds: BTreeSet<String> = ours
        .iter()
        .flat_map(|e| arr(&e["changes"]))
        .flat_map(|ch| {
            ch.as_object()
                .map(|o| o.keys().cloned().collect::<Vec<_>>())
                .unwrap_or_default()
        })
        .collect();
    let dt = c
        .q(&format!(
            "SHOW CHANGES FOR TABLE cf_feed SINCE d'{}' LIMIT 10;",
            c.q1("RETURN <string>(time::now() - 1m);")
                .await?
                .as_str()
                .unwrap_or("")
        ))
        .await;
    verdict(
        ours.len() >= 4 && ordered && stamp_ms_ok && kinds.contains("delete") && (kinds.contains("update") || kinds.contains("current")),
        format!(
            "{} entries for our writes, versionstamps increasing {ordered}, vs>>16 ~ unix ms {stamp_ms_ok}, change kinds {kinds:?}; SINCE <datetime> form {}",
            ours.len(),
            if dt.is_ok() { "accepted" } else { "rejected" }
        ),
    )
}

fn cursor(c: &Ctx) -> anyhow::Result<u64> {
    c.feed_cursor
        .lock()
        .unwrap()
        .ok_or_else(|| anyhow!("skip: needs cf.show_changes"))
}

async fn cf_cancel_excluded(c: &Ctx) -> R {
    let since = cursor(c)?;
    let _ = c
        .q("BEGIN TRANSACTION; CREATE cf_feed:cancelled SET x = 1; CANCEL TRANSACTION;")
        .await;
    let entries = show_changes(c, since).await?;
    verdict(
        !mentions(&entries, "cf_feed:cancelled"),
        "BEGIN..CANCEL never reaches SHOW CHANGES",
    )
}

async fn cf_throw_excluded(c: &Ctx) -> R {
    let since = cursor(c)?;
    let _ = c.q("BEGIN TRANSACTION; CREATE cf_feed:thrown SET x = 1; THROW 'abort'; COMMIT TRANSACTION;").await;
    let ev = c.q("CREATE cf_feed:bad SET bad = true;").await;
    let entries = show_changes(c, since).await?;
    verdict(
        ev.is_err() && !mentions(&entries, "cf_feed:thrown") && !mentions(&entries, "cf_feed:bad"),
        format!(
            "THROW txn in feed: {}; event-THROW-refused write in feed: {}",
            mentions(&entries, "cf_feed:thrown"),
            mentions(&entries, "cf_feed:bad")
        ),
    )
}

async fn cf_since_inclusive(c: &Ctx) -> R {
    let since = cursor(c)?;
    let entries = show_changes(c, since).await?;
    let Some(vs) = entries
        .iter()
        .filter(|e| e.to_string().contains("cf_feed"))
        .nth(1)
        .and_then(|e| e["versionstamp"].as_u64())
    else {
        return fail("fewer than two feed entries to test with");
    };
    let again = show_changes(c, vs).await?;
    let first = again.first().and_then(|e| e["versionstamp"].as_u64());
    verdict(first == Some(vs), format!("SINCE {vs} starts at {first:?}"))
}

// ---------------------------------------------------------------------------
// Probes: buckets
// ---------------------------------------------------------------------------

async fn bucket_memory(c: &Ctx) -> R {
    c.q("DEFINE BUCKET IF NOT EXISTS cf_files BACKEND \"memory\" PERMISSIONS FULL;")
        .await?;
    c.q("f\"cf_files:/hello.txt\".put(\"hello\");").await?;
    let exists = c.q1("RETURN f\"cf_files:/hello.txt\".exists();").await?;
    let head = c.q1("RETURN file::head(f\"cf_files:/hello.txt\");").await?;
    let get = c.q1("RETURN <string>f\"cf_files:/hello.txt\".get();").await;
    verdict(
        exists == json!(true) && head["size"] == json!(5),
        format!(
            "exists {exists}; head {head}; get {:?}",
            get.map(|v| v.to_string())
                .map_err(|e| one_line(&e.to_string(), 80))
        ),
    )
}

async fn bucket_permissions(c: &Ctx) -> R {
    let alice = c.user("alice")?;
    c.q("DEFINE BUCKET IF NOT EXISTS cf_open BACKEND \"memory\" PERMISSIONS WHERE $auth != NONE AND $action NOTINSIDE ['delete'];
         DEFINE BUCKET IF NOT EXISTS cf_shut BACKEND \"memory\" PERMISSIONS WHERE false;")
        .await?;
    let put = c
        .q_as(
            &alice,
            "f\"cf_open:/a.txt\".put(\"x\"); RETURN f\"cf_open:/a.txt\".exists();",
        )
        .await;
    let del = c.q_as(&alice, "f\"cf_open:/a.txt\".delete();").await;
    let shut = c.q_as(&alice, "f\"cf_shut:/b.txt\".put(\"x\");").await;
    let fmt = |r: &anyhow::Result<Value>| match r {
        Ok(v) => format!("ok {v}"),
        Err(e) => format!("denied ({})", one_line(&e.to_string(), 60)),
    };
    verdict(
        matches!(&put, Ok(v) if *v == json!(true)) && del.is_err() && shut.is_err(),
        format!(
            "put on $auth bucket: {}; delete excluded by $action: {}; put on WHERE false: {}",
            fmt(&put),
            fmt(&del),
            fmt(&shut)
        ),
    )
}

// ---------------------------------------------------------------------------
// live.ts (LIVE SELECT over WebSocket)
// ---------------------------------------------------------------------------

async fn run_live_ts(c: &Ctx, args: &Args, runner: &mut Runner) {
    let script = concat!(env!("CARGO_MANIFEST_DIR"), "/live.ts");
    let (alice, bob) = match (c.user("alice"), c.user("bob")) {
        (Ok(a), Ok(b)) => (a, b),
        _ => {
            runner
                .run("live", "live.ws", async {
                    bail!("skip: needs signed-in record users")
                })
                .await;
            return;
        }
    };
    let ws = c
        .base
        .replacen("https://", "wss://", 1)
        .replacen("http://", "ws://", 1);
    let output = tokio::process::Command::new("node")
        .args(["--experimental-strip-types", "--no-warnings", script])
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .env("CF_WS_URL", format!("{ws}/rpc"))
        .env("CF_NS", &c.ns)
        .env("CF_DB", &c.db)
        .env("CF_ROOT_USER", &args.user)
        .env("CF_ROOT_PASS", &args.pass)
        .env("CF_ROOT_TOKEN", args.token.clone().unwrap_or_default())
        .env("CF_ALICE_USER", &alice.name)
        .env("CF_ALICE_PASS", &alice.pass)
        .env("CF_ALICE_ID", &alice.id)
        .env("CF_BOB_USER", &bob.name)
        .env("CF_BOB_PASS", &bob.pass)
        .output()
        .await;
    let output = match output {
        Ok(o) => o,
        Err(e) => {
            let msg = format!("skip: node not runnable ({e})");
            runner
                .run("live", "live.ws", async move { bail!("{msg}") })
                .await;
            return;
        }
    };
    let stdout = String::from_utf8_lossy(&output.stdout);
    let mut any = false;
    for line in stdout.lines().filter(|l| l.starts_with('{')) {
        let Ok(v) = serde_json::from_str::<Value>(line) else {
            continue;
        };
        any = true;
        let outcome = Outcome::parse(v["outcome"].as_str().unwrap_or("fail"));
        let detail = s(&v["detail"]);
        let id = s(&v["id"]);
        if !runner.wants("live", &id) {
            continue;
        }
        println!(
            "{:<11} {:<44} {:>6}ms  {}",
            outcome.label().to_uppercase(),
            id,
            v["ms"],
            one_line(&detail, 160)
        );
        runner.reports.push(Report {
            group: "live".into(),
            id,
            outcome,
            detail,
            ms: v["ms"].as_u64().unwrap_or(0) as u128,
        });
    }
    if !any {
        let err = one_line(&String::from_utf8_lossy(&output.stderr), 400);
        runner
            .run("live", "live.ws", async move {
                bail!("live.ts produced no results: {err}")
            })
            .await;
    }
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

struct Args {
    url: String,
    ns: String,
    db: Option<String>,
    user: String,
    pass: String,
    token: Option<String>,
    sink_bind: String,
    sink_host: String,
    sink_url: Option<String>,
    only: Option<String>,
    live: bool,
    keep: bool,
    md: Option<String>,
}

const USAGE: &str = "db-conformance [--url http://127.0.0.1:18333] [--ns conformance] [--db <name>]
  [--user root --pass root | --token <bearer>] [--sink-bind 0.0.0.0:0] [--sink-host host.docker.internal]
  [--sink-url <public url of the sink>] [--only <probe id prefix or group>] [--no-live] [--keep] [--md <file>]";

impl Args {
    fn parse(argv: Vec<String>) -> anyhow::Result<Args> {
        let mut a = Args {
            url: "http://127.0.0.1:18333".into(),
            ns: "conformance".into(),
            db: None,
            user: "root".into(),
            pass: "root".into(),
            token: None,
            sink_bind: "0.0.0.0:0".into(),
            sink_host: "host.docker.internal".into(),
            sink_url: None,
            only: None,
            live: true,
            keep: false,
            md: None,
        };
        let mut it = argv.into_iter();
        while let Some(flag) = it.next() {
            let mut val = || {
                it.next()
                    .ok_or_else(|| anyhow!("{flag} needs a value\n{USAGE}"))
            };
            match flag.as_str() {
                "--url" => a.url = val()?,
                "--ns" => a.ns = val()?,
                "--db" => a.db = Some(val()?),
                "--user" => a.user = val()?,
                "--pass" => a.pass = val()?,
                "--token" => a.token = Some(val()?),
                "--sink-bind" => a.sink_bind = val()?,
                "--sink-host" => a.sink_host = val()?,
                "--sink-url" => a.sink_url = Some(val()?),
                "--only" => a.only = Some(val()?),
                "--md" => a.md = Some(val()?),
                "--no-live" => a.live = false,
                "--keep" => a.keep = true,
                "-h" | "--help" => bail!("{USAGE}"),
                other => bail!("unknown flag {other}\n{USAGE}"),
            }
        }
        Ok(a)
    }
}

fn ident_ok(s: &str) -> bool {
    !s.is_empty() && s.chars().all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse(std::env::args().skip(1).collect())?;
    let unix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_secs();
    let run_id = format!("{unix}");
    let generated_db = args.db.is_none();
    let db = args.db.clone().unwrap_or_else(|| format!("run_{run_id}"));
    if !ident_ok(&args.ns) || !ident_ok(&db) {
        bail!("--ns and --db must be [A-Za-z0-9_]");
    }
    let secret = format!("conformance-secret-{run_id}");
    let root_auth = match &args.token {
        Some(t) => format!("Bearer {t}"),
        None => format!(
            "Basic {}",
            base64::engine::general_purpose::STANDARD
                .encode(format!("{}:{}", args.user, args.pass))
        ),
    };
    let sink = Sink::start(&args.sink_bind, secret.clone()).await?;
    let sink_url = args
        .sink_url
        .clone()
        .unwrap_or_else(|| format!("http://{}:{}", args.sink_host, sink.port));
    let base = args
        .url
        .trim_end_matches('/')
        .replacen("wss://", "https://", 1)
        .replacen("ws://", "http://", 1);
    let c = Ctx {
        http: Arc::new(ReqwestHttp(reqwest::Client::new())),
        base,
        ns: args.ns.clone(),
        db: db.clone(),
        root_auth,
        sink,
        sink_url,
        secret,
        run_id,
        users: Mutex::new(BTreeMap::new()),
        async_form: Mutex::new(None),
        feed_cursor: Mutex::new(None),
    };
    println!(
        "endpoint {} ns {} db {} sink {} (listening :{})\n",
        c.base, c.ns, c.db, c.sink_url, c.sink.port
    );
    c.q(&format!(
        "DEFINE NAMESPACE IF NOT EXISTS {0}; USE NS {0}; DEFINE DATABASE IF NOT EXISTS {1};",
        c.ns, c.db
    ))
    .await
    .context(
        "creating the namespace/database (credentials need that right, or pass an existing --db)",
    )?;
    let version = http_version(&c)
        .await
        .unwrap_or_else(|e| format!("unknown ({e})"));

    let mut r = Runner {
        only: args.only.clone(),
        reports: Vec::new(),
    };
    r.run("transport", "transport.version", transport_version(&c))
        .await;
    r.run(
        "transport",
        "transport.httpsqldb_version",
        transport_httpsqldb_version(&c),
    )
    .await;
    r.run(
        "transport",
        "transport.multi_statement",
        transport_multi_statement(&c),
    )
    .await;
    r.run("schema", "schema.internal_apply", schema_internal_apply(&c))
        .await;
    r.run("schema", "schema.generated_events", schema_user_tables(&c))
        .await;

    r.run(
        "access",
        "access.record_signup_signin",
        access_record_signup_signin(&c),
    )
    .await;
    r.run("access", "access.session_vars", access_session_vars(&c))
        .await;
    r.run(
        "access",
        "access.param_permissions_none",
        access_param_permissions_none(&c),
    )
    .await;
    r.run(
        "access",
        "access.row_permissions",
        access_row_permissions(&c),
    )
    .await;
    r.run(
        "access",
        "access.impersonation_external_jwt",
        access_impersonation_external_jwt(&c),
    )
    .await;

    r.run("functions", "fn.session_guard", fn_session_guard(&c))
        .await;
    r.run("functions", "fn.query_register", fn_query_register(&c))
        .await;
    r.run("functions", "fn.query_heartbeat", fn_query_heartbeat(&c))
        .await;
    r.run(
        "functions",
        "fn.query_leave_unsubscribe",
        fn_query_leave_unsubscribe(&c),
    )
    .await;
    r.run("functions", "fn.spooky_info_http_get", fn_spooky_info(&c))
        .await;
    r.run("functions", "fn.throw_text", fn_throw_text(&c)).await;
    r.run("functions", "api.define_api", api_define_api(&c))
        .await;
    for (name, expr, want) in BUILTINS {
        r.run(
            "builtins",
            &format!("builtin.{name}"),
            builtin(&c, expr, want),
        )
        .await;
    }

    r.run("events", "event.in_txn_vars", event_in_txn_vars(&c))
        .await;
    r.run(
        "events",
        "event.cancelled_txn_discards",
        event_cancelled_txn(&c),
    )
    .await;
    r.run(
        "events",
        "event.throw_aborts_write",
        event_throw_aborts_write(&c),
    )
    .await;
    r.run("events", "event.sp00ky_ingest", event_sp00ky_ingest(&c))
        .await;
    r.run(
        "events",
        "event.outer_let_visible",
        event_outer_let_visible(&c),
    )
    .await;
    r.run("http", "http.post_order", http_post_order(&c)).await;
    r.run("http", "http.post_runs_in_txn", http_post_in_txn(&c))
        .await;
    r.run("http", "http.non_2xx_aborts_write", http_non_2xx_aborts(&c))
        .await;
    r.run("http", "http.timeout_aborts_write", http_timeout_aborts(&c))
        .await;
    r.run(
        "http",
        "http.unreachable_aborts_write",
        http_refused_aborts(&c),
    )
    .await;
    r.run("http", "http.get_parsed", http_get_parsed(&c)).await;
    r.run(
        "http",
        "regress.http_post_field_stripping",
        regress_http_post_field_stripping(&c),
    )
    .await;
    r.run("events", "event.async_syntax", event_async_syntax(&c))
        .await;
    r.run(
        "events",
        "event.async_post_commit",
        event_async_post_commit(&c),
    )
    .await;
    r.run(
        "events",
        "event.async_non_2xx_no_abort",
        event_async_non_2xx(&c),
    )
    .await;
    r.run(
        "events",
        "event.async_rollback_never_fires",
        event_async_rollback(&c),
    )
    .await;
    r.run("events", "event.async_order", event_async_order(&c))
        .await;
    // Later probes post through $sp00ky_endpoint; restore it after the refused-host probe.
    let _ = c.set_endpoint("ingest").await;

    r.run("graph", "graph.relate_edges", graph_relate_edges(&c))
        .await;
    r.run(
        "graph",
        "graph.traverse_membership",
        graph_traverse_membership(&c),
    )
    .await;
    r.run(
        "graph",
        "graph.edge_delete_forms",
        graph_edge_delete_forms(&c),
    )
    .await;

    r.run("info", "info.for_db", info_for_db(&c)).await;
    r.run("info", "info.for_table", info_for_table(&c)).await;
    r.run("info", "info.for_index", info_for_index(&c)).await;

    r.run("scan", "scan.keyset_bootstrap", scan_keyset_bootstrap(&c))
        .await;
    r.run("scan", "scan.record_range", scan_record_range(&c))
        .await;
    r.run(
        "scan",
        "scan.order_by_projection",
        scan_order_by_projection(&c),
    )
    .await;

    r.run("index", "index.unique", index_unique(&c)).await;
    r.run(
        "index",
        "regress.unique_index_rebuild_complete",
        index_overwrite_rebuild(&c),
    )
    .await;
    r.run("index", "index.concurrently", index_concurrently(&c))
        .await;
    r.run(
        "txn",
        "regress.return_in_txn_commits",
        regress_return_in_txn(&c),
    )
    .await;
    r.run(
        "txn",
        "regress.select_star_subquery_param",
        regress_select_star_subquery_param(&c),
    )
    .await;

    r.run("changefeed", "cf.define", cf_define(&c)).await;
    r.run("changefeed", "cf.show_changes", cf_show_changes(&c))
        .await;
    r.run("changefeed", "cf.cancel_excluded", cf_cancel_excluded(&c))
        .await;
    r.run("changefeed", "cf.throw_excluded", cf_throw_excluded(&c))
        .await;
    r.run("changefeed", "cf.since_inclusive", cf_since_inclusive(&c))
        .await;

    r.run("bucket", "bucket.memory", bucket_memory(&c)).await;
    r.run("bucket", "bucket.permissions", bucket_permissions(&c))
        .await;

    if args.live {
        run_live_ts(&c, &args, &mut r).await;
    }

    if generated_db && !args.keep {
        if let Err(e) = c
            .q(&format!(
                "USE NS {}; REMOVE DATABASE IF EXISTS {};",
                c.ns, c.db
            ))
            .await
        {
            eprintln!("cleanup failed: {e}");
        }
    }

    let md = render_markdown(&r.reports, &version, &c.base);
    let failed = r
        .reports
        .iter()
        .filter(|x| x.outcome == Outcome::Fail)
        .count();
    println!("\n{}", md.lines().next().unwrap_or(""));
    if let Some(path) = &args.md {
        std::fs::write(path, &md).with_context(|| format!("writing {path}"))?;
        println!("markdown table written to {path}");
    }
    std::process::exit(if failed > 0 { 1 } else { 0 });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classify_maps_feature_gaps_to_unsupported() {
        assert_eq!(
            classify_error("query: Parse error: Unexpected token `ASYNC`"),
            Outcome::Unsupported
        );
        assert_eq!(
            classify_error("query: Function 'http::post' is not allowed to be executed"),
            Outcome::Unsupported
        );
        assert_eq!(
            classify_error("query: There was a problem with the database"),
            Outcome::Fail
        );
        assert_eq!(classify_error("skip: needs users"), Outcome::Skip);
    }

    #[test]
    fn sink_reply_reads_status_hold_and_info() {
        assert_eq!(sink_reply("/p/x/status/500/ingest", "", "s").0, 500);
        assert_eq!(sink_reply("/p/x/hold/1500/ingest", "", "s").1, 1500);
        assert_eq!(sink_reply("/p/fn/info/text", "", "s").2, "text/plain");
        assert_eq!(sink_reply("/p/fn/ingest", "", "s").0, 200);
    }

    #[test]
    fn generated_events_keep_the_production_post() {
        let ev = sp00ky_events("t");
        assert!(ev.contains("DEFINE EVENT OVERWRITE _00_t_mutation ON TABLE t"));
        assert!(ev.contains("SELECT * FROM http::post($sp00ky_endpoint + '/ingest', $payload, { \"Authorization\": \"Bearer \" + $sp00ky_secret }) TIMEOUT 10s;"));
        assert!(ev.contains(
            "_00_rv: (SELECT VALUE version FROM ONLY _00_version WHERE record_id = $after.id)"
        ));
    }

    #[test]
    fn dechunk_joins_chunks() {
        assert_eq!(dechunk(b"3\r\nabc\r\n2\r\nde\r\n0\r\n\r\n"), b"abcde");
    }
}
