//! Files of the buckets declared `BACKEND "r2"`, served by the SSP.
//!
//! A SurrealDB bucket keeps its bytes wherever its server can: Surreal Cloud
//! allows only `memory`, which loses every file on a restart while the rows
//! that point at them stay. An `r2` bucket keeps its bytes in an
//! [`ObjectStore`] next to the SSP instead (R2 on Cloudflare, a directory on a
//! VM and under `spky dev`), and the client reads and writes them here over
//! HTTP rather than with `f"bucket:/path"` statements.
//!
//! Permissions stay SurrealDB's. The CLI turns each `r2` bucket's
//! `PERMISSIONS` clause into `fn::_00_bucket::<name>($action, $file, $target)`,
//! and every request first calls it AS THE CALLER (their token over HTTP-RPC),
//! so `$auth`, `$action`, `$file` and `$target` mean what they mean in the
//! clause. Two differences from a SurrealDB bucket: the clause runs with the
//! caller's table permissions (SurrealDB evaluates a bucket clause with them
//! off), and anonymous callers are refused outright.
//!
//! Routes (keys percent-encoded per segment, CORS open: auth is the bearer):
//! - `GET    /bucket/<name>/<key>` the bytes
//! - `PUT    /bucket/<name>/<key>` store the body
//! - `DELETE /bucket/<name>/<key>`
//! - `POST   /bucket/<name>/<key>` with `{"op": "head" | "exists" | "copy" |
//!   "rename", "target"?: "<key>"}`, answered with JSON

use std::sync::Arc;

use bytes::Bytes;
use serde_json::{json, Value};

use crate::api::{ApiBody, ApiRequest, ApiResponse, Method};
use crate::http_sql_db::HttpSqlDb;
use crate::ports::{Db, DbError, HttpClient, ObjectMeta, ObjectStore};

/// Largest upload accepted. A Cloudflare Worker takes 100 MB request bodies;
/// the client enforces each bucket's own `maxSize` before it gets here.
pub const DEFAULT_MAX_UPLOAD_BYTES: usize = 64 * 1024 * 1024;

pub struct BucketService {
    store: Arc<dyn ObjectStore>,
    http: Arc<dyn HttpClient>,
    db_url: String,
    ns: String,
    db: String,
    max_upload_bytes: usize,
}

/// A bucket operation, named as SurrealDB names it in `$action`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Action {
    Get,
    Put,
    Delete,
    Head,
    Exists,
    Copy,
    Rename,
}

impl Action {
    fn name(self) -> &'static str {
        match self {
            Action::Get => "get",
            Action::Put => "put",
            Action::Delete => "delete",
            Action::Head => "head",
            Action::Exists => "exists",
            Action::Copy => "copy",
            Action::Rename => "rename",
        }
    }
}

impl BucketService {
    pub fn new(
        store: Arc<dyn ObjectStore>,
        http: Arc<dyn HttpClient>,
        db_url: impl Into<String>,
        ns: impl Into<String>,
        db: impl Into<String>,
    ) -> Self {
        Self {
            store,
            http,
            db_url: db_url.into(),
            ns: ns.into(),
            db: db.into(),
            max_upload_bytes: DEFAULT_MAX_UPLOAD_BYTES,
        }
    }

    pub fn with_max_upload_bytes(mut self, bytes: usize) -> Self {
        self.max_upload_bytes = bytes;
        self
    }

    /// Whether `path` is one of these routes: shells check this first and
    /// send everything else to the node.
    pub fn handles(path: &str) -> bool {
        path == "/bucket" || path.starts_with("/bucket/")
    }

    /// The answer to a CORS preflight (`OPTIONS`) on these routes. Shells
    /// answer it themselves: [`Method`] has no `OPTIONS`.
    pub fn preflight() -> ApiResponse {
        let mut response = ApiResponse::text(204, "text/plain", String::new());
        add_cors(&mut response);
        response
    }

    pub async fn handle(&self, req: ApiRequest) -> ApiResponse {
        let mut response = self.dispatch(req).await;
        add_cors(&mut response);
        response
    }

    async fn dispatch(&self, req: ApiRequest) -> ApiResponse {
        let Some((bucket, key)) = parse_path(&req.path) else {
            return error(404, "not a bucket file path");
        };
        let Some(token) = req.bearer.as_deref().filter(|t| !t.is_empty()) else {
            return error(401, "sign in to use this bucket");
        };
        match req.method {
            Method::Get => {
                if let Err(denied) = self.check(token, &bucket, Action::Get, &key, None).await {
                    return denied;
                }
                match self.store.get(&store_key(&bucket, &key)).await {
                    Ok(Some(object)) => bytes_response(&key, object.meta.content_type, object.body),
                    Ok(None) => error(404, "no such file"),
                    Err(e) => store_error(&e),
                }
            }
            Method::Put => {
                if req.body.len() > self.max_upload_bytes {
                    return error(413, "file too large");
                }
                if let Err(denied) = self.check(token, &bucket, Action::Put, &key, None).await {
                    return denied;
                }
                match self.store.put(&store_key(&bucket, &key), req.body, content_type_for(&key)).await {
                    Ok(()) => ApiResponse::json(200, json!({ "ok": true })),
                    Err(e) => store_error(&e),
                }
            }
            Method::Delete => {
                if let Err(denied) = self.check(token, &bucket, Action::Delete, &key, None).await {
                    return denied;
                }
                match self.store.delete(&store_key(&bucket, &key)).await {
                    Ok(()) => ApiResponse::json(200, json!({ "ok": true })),
                    Err(e) => store_error(&e),
                }
            }
            Method::Post => self.operation(token, &bucket, &key, &req.body).await,
        }
    }

    /// `POST`: the operations that answer with JSON.
    async fn operation(&self, token: &str, bucket: &str, key: &str, body: &Bytes) -> ApiResponse {
        let parsed: Value = match serde_json::from_slice(body) {
            Ok(v) => v,
            Err(_) => return error(400, "expected a JSON body"),
        };
        let target = match parsed.get("target").and_then(Value::as_str) {
            Some(raw) => match normalize_key(raw) {
                Some(key) => Some(key),
                None => return error(400, "invalid target key"),
            },
            None => None,
        };
        let action = match parsed.get("op").and_then(Value::as_str) {
            Some("head") => Action::Head,
            Some("exists") => Action::Exists,
            Some("copy") => Action::Copy,
            Some("rename") => Action::Rename,
            // SurrealDB refuses `list` to record users; so does this.
            Some("list") => return error(403, "listing a bucket is not allowed"),
            _ => return error(400, "unknown op"),
        };
        if matches!(action, Action::Copy | Action::Rename) && target.is_none() {
            return error(400, "copy and rename need a target");
        }
        if let Err(denied) = self.check(token, bucket, action, key, target.as_deref()).await {
            return denied;
        }
        let source = store_key(bucket, key);
        match action {
            Action::Head => match self.store.head(&source).await {
                Ok(meta) => ApiResponse::json(200, meta.map_or(Value::Null, |m| head_json(key, &m))),
                Err(e) => store_error(&e),
            },
            Action::Exists => match self.store.head(&source).await {
                Ok(meta) => ApiResponse::json(200, json!(meta.is_some())),
                Err(e) => store_error(&e),
            },
            Action::Copy | Action::Rename => {
                let target = store_key(bucket, target.as_deref().unwrap_or_default());
                let object = match self.store.get(&source).await {
                    Ok(Some(object)) => object,
                    Ok(None) => return error(404, "no such file"),
                    Err(e) => return store_error(&e),
                };
                let content_type = object.meta.content_type.unwrap_or_else(|| content_type_for(key).to_string());
                if let Err(e) = self.store.put(&target, object.body, &content_type).await {
                    return store_error(&e);
                }
                if action == Action::Rename {
                    if let Err(e) = self.store.delete(&source).await {
                        return store_error(&e);
                    }
                }
                ApiResponse::json(200, json!({ "ok": true }))
            }
            Action::Get | Action::Put | Action::Delete => unreachable!("not a POST op"),
        }
    }

    /// Ask SurrealDB, as the caller, whether the bucket's permission clause
    /// allows this. `Err` is the response to send instead.
    async fn check(
        &self,
        token: &str,
        bucket: &str,
        action: Action,
        key: &str,
        target: Option<&str>,
    ) -> Result<(), ApiResponse> {
        let db = HttpSqlDb::new(
            Arc::clone(&self.http),
            &self.db_url,
            self.ns.clone(),
            self.db.clone(),
            format!("Bearer {token}"),
        );
        let target_expr = if target.is_some() { "type::file($bucket, $target)" } else { "NONE" };
        let surql = format!(
            "RETURN fn::_00_bucket::{bucket}($action, type::file($bucket, $key), {target_expr});"
        );
        let mut binds = vec![
            ("bucket", json!(bucket)),
            ("action", json!(action.name())),
            ("key", json!(key)),
        ];
        if let Some(target) = target {
            binds.push(("target", json!(target)));
        }
        match db.query(&surql, &binds).await {
            Ok(rows) if rows.first() == Some(&Value::Bool(true)) => Ok(()),
            Ok(_) => Err(error(403, "not allowed")),
            Err(DbError::Auth(_)) => Err(error(401, "invalid or expired token")),
            Err(DbError::Query(msg)) if msg.contains("does not exist") => {
                Err(error(404, "no such bucket"))
            }
            Err(DbError::Query(msg)) if msg.contains("Token") || msg.contains("token") => {
                Err(error(401, "invalid or expired token"))
            }
            Err(DbError::Query(msg)) if msg.contains("Not enough permissions") || msg.contains("not allowed") => {
                Err(error(403, "not allowed"))
            }
            Err(e) => {
                tracing::warn!(error = %e, bucket, action = action.name(), "bucket permission check failed");
                Err(error(502, "permission check failed"))
            }
        }
    }
}

/// `/bucket/<name>/<key...>` into the bucket name and SurrealDB's form of the
/// key (`/`-prefixed). `None` for anything malformed.
fn parse_path(path: &str) -> Option<(String, String)> {
    let rest = path.strip_prefix("/bucket/")?;
    let (name, key) = rest.split_once('/')?;
    let name = percent_decode(name)?;
    let valid_name = name.chars().next().is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
        && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_');
    if !valid_name {
        return None;
    }
    let mut decoded = Vec::new();
    for segment in key.split('/') {
        decoded.push(percent_decode(segment)?);
    }
    Some((name, normalize_key(&decoded.join("/"))?))
}

/// A key as SurrealDB writes it (`/a/b.png`), refusing empty segments, dot
/// segments, backslashes and control characters: these keys become paths in
/// a directory store.
fn normalize_key(raw: &str) -> Option<String> {
    let trimmed = raw.strip_prefix('/').unwrap_or(raw);
    if trimmed.is_empty() || trimmed.len() > 1024 {
        return None;
    }
    for segment in trimmed.split('/') {
        if segment.is_empty() || segment == "." || segment == ".." {
            return None;
        }
        if segment.chars().any(|c| c == '\\' || c.is_control()) {
            return None;
        }
    }
    Some(format!("/{trimmed}"))
}

/// Where a file lives in the store: `<bucket>/<key without its slash>`.
fn store_key(bucket: &str, key: &str) -> String {
    format!("{bucket}/{}", key.trim_start_matches('/'))
}

fn percent_decode(segment: &str) -> Option<String> {
    let bytes = segment.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' {
            let hex = segment.get(i + 1..i + 3)?;
            out.push(u8::from_str_radix(hex, 16).ok()?);
            i += 3;
        } else {
            out.push(bytes[i]);
            i += 1;
        }
    }
    String::from_utf8(out).ok()
}

/// The content type a file is served with, from its extension.
pub fn content_type_for(key: &str) -> &'static str {
    let ext = key.rsplit_once('.').map(|(_, ext)| ext.to_ascii_lowercase()).unwrap_or_default();
    match ext.as_str() {
        "png" => "image/png",
        "jpg" | "jpeg" => "image/jpeg",
        "gif" => "image/gif",
        "webp" => "image/webp",
        "avif" => "image/avif",
        "svg" => "image/svg+xml",
        "ico" => "image/x-icon",
        "pdf" => "application/pdf",
        "json" => "application/json",
        "txt" | "blurhash" => "text/plain; charset=utf-8",
        "mp4" => "video/mp4",
        "webm" => "video/webm",
        "mp3" => "audio/mpeg",
        _ => "application/octet-stream",
    }
}

fn head_json(key: &str, meta: &ObjectMeta) -> Value {
    json!({
        "key": key,
        "size": meta.size,
        "updated": meta.updated_epoch_ms,
    })
}

fn bytes_response(key: &str, content_type: Option<String>, body: Bytes) -> ApiResponse {
    let content_type = content_type.unwrap_or_else(|| content_type_for(key).to_string());
    ApiResponse {
        status: 200,
        // The client keeps its own copy (OPFS); a stale HTTP cache entry would
        // outlive an overwrite at the same path.
        headers: vec![("Cache-Control", "private, no-store".to_string())],
        body: ApiBody::Bytes { content_type, body },
    }
}

fn error(status: u16, message: &str) -> ApiResponse {
    ApiResponse::json(status, json!({ "error": message }))
}

/// The object store failed: a 502 the caller sees, and a warning the
/// operator sees.
fn store_error(e: &dyn std::fmt::Display) -> ApiResponse {
    tracing::warn!(error = %e, "bucket object store request failed");
    error(502, &e.to_string())
}

fn add_cors(response: &mut ApiResponse) {
    response.headers.push(("Access-Control-Allow-Origin", "*".to_string()));
    response
        .headers
        .push(("Access-Control-Allow-Methods", "GET, PUT, POST, DELETE, OPTIONS".to_string()));
    response
        .headers
        .push(("Access-Control-Allow-Headers", "Authorization, Content-Type".to_string()));
    response.headers.push(("Access-Control-Max-Age", "86400".to_string()));
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ports::{CancelWatch, HttpError, MemoryObjectStore, OutboundRequest, OutboundResponse};
    use std::sync::Mutex;

    /// SurrealDB's HTTP-RPC, answering the permission call: allows when the
    /// bearer is `good` and the action is not `delete`.
    #[derive(Default)]
    struct FakeDb {
        calls: Mutex<Vec<(String, Value)>>,
    }

    #[async_trait::async_trait]
    impl HttpClient for FakeDb {
        async fn send(&self, req: OutboundRequest, _cancel: Option<CancelWatch>) -> Result<OutboundResponse, HttpError> {
            let auth = req
                .headers
                .iter()
                .find(|(k, _)| k == "Authorization")
                .map(|(_, v)| v.clone())
                .unwrap_or_default();
            let body = req.json_body.unwrap();
            let sent = body["params"][0].as_str().unwrap().to_string();
            // HttpSqlDb sends its binds as `LET $name = <json>;` lines ahead of
            // the query, and SurrealDB answers every statement.
            let mut vars = serde_json::Map::new();
            let mut lets = Vec::new();
            let mut surql = String::new();
            for line in sent.lines() {
                match line.strip_prefix("LET $").and_then(|l| l.split_once(" = ")) {
                    Some((name, value)) => {
                        vars.insert(name.into(), serde_json::from_str(value.trim_end_matches(';')).unwrap());
                        lets.push(json!({ "status": "OK", "result": null }));
                    }
                    None => surql.push_str(line),
                }
            }
            let vars = Value::Object(vars);
            self.calls.lock().unwrap().push((surql.clone(), vars.clone()));
            let answer = |statement: Value| {
                let mut results = lets.clone();
                results.push(statement);
                json!({ "id": 1, "result": results }).to_string()
            };
            if auth != "Bearer good" {
                return Ok(OutboundResponse { status: 401, body: "InvalidToken".into() });
            }
            if surql.contains("fn::_00_bucket::nope") {
                let msg = "The function 'fn::_00_bucket::nope' does not exist";
                return Ok(OutboundResponse { status: 200, body: answer(json!({ "status": "ERR", "result": msg })) });
            }
            let allowed = vars["action"] != "delete";
            Ok(OutboundResponse { status: 200, body: answer(json!({ "status": "OK", "result": allowed })) })
        }
    }

    fn service() -> (BucketService, Arc<MemoryObjectStore>, Arc<FakeDb>) {
        let store = Arc::new(MemoryObjectStore::default());
        let db = Arc::new(FakeDb::default());
        let service = BucketService::new(store.clone(), db.clone(), "wss://db.example/rpc", "ns", "db");
        (service, store, db)
    }

    fn req(method: Method, path: &str, bearer: Option<&str>, body: &[u8]) -> ApiRequest {
        ApiRequest {
            method,
            path: path.to_string(),
            bearer: bearer.map(str::to_string),
            body: Bytes::copy_from_slice(body),
        }
    }

    fn json_of(response: &ApiResponse) -> Value {
        match &response.body {
            ApiBody::Json(v) => v.clone(),
            _ => panic!("not JSON"),
        }
    }

    #[tokio::test]
    async fn put_then_get_round_trips_the_bytes_with_cors() {
        let (service, store, db) = service();
        let put = service.handle(req(Method::Put, "/bucket/pics/u1/a%20b.png", Some("good"), b"PNG")).await;
        assert_eq!(put.status, 200);
        assert_eq!(store.keys(), vec!["pics/u1/a b.png".to_string()]);
        let get = service.handle(req(Method::Get, "/bucket/pics/u1/a%20b.png", Some("good"), b"")).await;
        assert_eq!(get.status, 200);
        assert!(get.headers.iter().any(|(k, v)| *k == "Access-Control-Allow-Origin" && v == "*"));
        match get.body {
            ApiBody::Bytes { content_type, body } => {
                assert_eq!(content_type, "image/png");
                assert_eq!(&body[..], b"PNG");
            }
            _ => panic!("expected bytes"),
        }
        // The permission call binds SurrealDB's key form and the action name.
        let calls = db.calls.lock().unwrap();
        assert!(calls[0].0.contains("fn::_00_bucket::pics($action, type::file($bucket, $key), NONE)"));
        assert_eq!(calls[0].1["key"], "/u1/a b.png");
        assert_eq!(calls[0].1["action"], "put");
        assert_eq!(calls[1].1["action"], "get");
    }

    #[tokio::test]
    async fn refusals_map_to_status_codes() {
        let (service, store, _) = service();
        store.put("pics/a.png", Bytes::from_static(b"x"), "image/png").await.unwrap();
        // No token at all, a token SurrealDB rejects, a clause that says no.
        assert_eq!(service.handle(req(Method::Get, "/bucket/pics/a.png", None, b"")).await.status, 401);
        assert_eq!(service.handle(req(Method::Get, "/bucket/pics/a.png", Some("bad"), b"")).await.status, 401);
        assert_eq!(service.handle(req(Method::Delete, "/bucket/pics/a.png", Some("good"), b"")).await.status, 403);
        assert_eq!(store.keys(), vec!["pics/a.png".to_string()]);
        // A bucket without a permission function is not an r2 bucket.
        assert_eq!(service.handle(req(Method::Get, "/bucket/nope/a.png", Some("good"), b"")).await.status, 404);
        assert_eq!(service.handle(req(Method::Get, "/bucket/pics/missing.png", Some("good"), b"")).await.status, 404);
    }

    #[tokio::test]
    async fn malformed_paths_never_reach_the_store_or_the_database() {
        let (service, _, db) = service();
        for path in [
            "/bucket/pics",
            "/bucket/pics/",
            "/bucket/pics/../etc/passwd",
            "/bucket/pics/a//b.png",
            "/bucket/pi-cs/a.png",
            "/bucket/pics/a%2F..%2Fb.png",
            "/bucket/pics/a%5Cb.png",
            "/bucket/pics/a%zz.png",
            "/bucket/1pics/a.png",
        ] {
            let response = service.handle(req(Method::Get, path, Some("good"), b"")).await;
            assert!(response.status == 404 || response.status == 400, "{path}: {}", response.status);
        }
        assert!(db.calls.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn json_ops_head_exists_copy_rename_and_no_list() {
        let (service, store, db) = service();
        store.put("pics/a.png", Bytes::from_static(b"abcd"), "image/png").await.unwrap();
        let post = |op: &str, target: Option<&str>| {
            let mut body = json!({ "op": op });
            if let Some(t) = target {
                body["target"] = json!(t);
            }
            req(Method::Post, "/bucket/pics/a.png", Some("good"), body.to_string().as_bytes())
        };
        let head = service.handle(post("head", None)).await;
        assert_eq!(json_of(&head)["size"], 4);
        assert_eq!(json_of(&service.handle(post("exists", None)).await), json!(true));
        assert_eq!(service.handle(post("copy", Some("/b.png"))).await.status, 200);
        assert_eq!(service.handle(post("rename", Some("c.png"))).await.status, 200);
        assert_eq!(store.keys(), vec!["pics/b.png".to_string(), "pics/c.png".to_string()]);
        assert_eq!(json_of(&service.handle(post("exists", None)).await), json!(false));
        assert_eq!(service.handle(post("copy", None)).await.status, 400);
        assert_eq!(service.handle(post("copy", Some("../x"))).await.status, 400);
        assert_eq!(service.handle(post("list", None)).await.status, 403);
        // copy passes the target to the clause as `$target`.
        let calls = db.calls.lock().unwrap();
        let copy = calls.iter().find(|(_, vars)| vars["action"] == "copy").unwrap();
        assert!(copy.0.contains("type::file($bucket, $target)"));
        assert_eq!(copy.1["target"], "/b.png");
    }

    #[tokio::test]
    async fn uploads_over_the_cap_are_refused_before_the_permission_call() {
        let (service, store, db) = service();
        let service = service.with_max_upload_bytes(3);
        let response = service.handle(req(Method::Put, "/bucket/pics/a.png", Some("good"), b"abcd")).await;
        assert_eq!(response.status, 413);
        assert!(store.keys().is_empty());
        assert!(db.calls.lock().unwrap().is_empty());
    }

    #[test]
    fn preflight_is_an_empty_cors_answer() {
        let response = BucketService::preflight();
        assert_eq!(response.status, 204);
        assert!(response.headers.iter().any(|(k, v)| *k == "Access-Control-Allow-Headers" && v.contains("Authorization")));
    }
}
