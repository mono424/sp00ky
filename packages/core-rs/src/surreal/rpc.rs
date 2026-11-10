//! The SurrealDB RPC transport: the [`RemoteSurrealClient`] seam the services
//! drive, and [`WebSocketSurrealClient`], the CBOR-over-WebSocket client behind
//! it (feature `remote`).

use async_trait::async_trait;
use tokio::sync::{broadcast, mpsc};

use crate::error::{Error, Result};
use crate::kernel::ports::StatementResult;
use crate::value::{Object, Value};

/// A LIVE-query notification (TS SurrealDB `live` message).
#[derive(Clone, Debug, PartialEq)]
pub struct LiveMessage {
    /// `CREATE`, `UPDATE`, `DELETE` or `KILLED`.
    pub action: String,
    pub value: Object,
}

/// The notifications of one LIVE query. Ends when the query is killed or the
/// socket that carried it closes.
pub type LiveStream = mpsc::UnboundedReceiver<LiveMessage>;

/// A socket lifecycle event (Dart `onConnected` / `onDisconnected`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TransportEvent {
    Connected,
    Disconnected,
}

/// The subset of the SurrealDB client the core needs. Decouples the core from
/// the transport, so tests drive the services over a fake.
#[async_trait(?Send)]
pub trait RemoteSurrealClient {
    async fn connect(&self, endpoint: &str) -> Result<()>;
    /// The `use` RPC: select the namespace and database.
    async fn use_db(&self, namespace: &str, database: &str) -> Result<()>;
    async fn authenticate(&self, token: &str) -> Result<Value>;
    async fn signin(&self, params: Object) -> Result<Value>;
    async fn signup(&self, params: Object) -> Result<Value>;
    async fn invalidate(&self) -> Result<()>;

    /// Run a SurrealQL request; the per-statement results. A statement that
    /// failed fails the whole call (as the JS SDK's `.query()` does).
    async fn query(&self, sql: &str, vars: Option<Object>) -> Result<Vec<Value>>;

    /// Per-statement outcomes, in order. Never fails for one statement: the
    /// saga core reads four statements from one register request and
    /// tolerates an error on the last three.
    ///
    /// The default answers through [`RemoteSurrealClient::query`], for a client
    /// that cannot answer per statement: its arity is preserved and a failure
    /// becomes one `ERR`, which is the all-or-nothing shape such a client has.
    async fn query_statements(
        &self,
        sql: &str,
        vars: Option<Object>,
    ) -> Result<Vec<StatementResult>> {
        Ok(match self.query(sql, vars).await {
            Ok(rows) => rows.into_iter().map(StatementResult::ok).collect(),
            Err(e) => vec![StatementResult::err(e.to_string())],
        })
    }

    /// Start a LIVE query: its id and its notifications.
    async fn live(&self, sql: &str, vars: Option<Object>) -> Result<(String, LiveStream)>;
    async fn kill(&self, live_id: &str) -> Result<()>;

    /// Lifecycle events from now on. The default never yields one.
    fn events(&self) -> broadcast::Receiver<TransportEvent> {
        broadcast::channel(1).1
    }

    /// True while a socket is open.
    fn is_connected(&self) -> bool {
        true
    }

    /// Drop the socket without disposing the client, so the supervisor can
    /// force the close a half-open connection never delivers, then re-open.
    async fn force_close(&self) {}

    /// Dispose the client: fails what is in flight, ends every LIVE stream.
    async fn close(&self);
}

/// The RPC endpoint a raw endpoint normalizes to: `http(s)` becomes `ws(s)`
/// and the path ends in `/rpc`.
pub fn rpc_endpoint(endpoint: &str) -> String {
    let mut e = if let Some(rest) = endpoint.strip_prefix("http://") {
        format!("ws://{rest}")
    } else if let Some(rest) = endpoint.strip_prefix("https://") {
        format!("wss://{rest}")
    } else {
        endpoint.to_string()
    };
    if !e.ends_with("/rpc") {
        if e.ends_with('/') {
            e.pop();
        }
        e.push_str("/rpc");
    }
    e
}

fn is_ok_status(stmt: &Object) -> bool {
    match stmt.get("status") {
        None => true,
        Some(s) if s.is_nullish() => true,
        Some(s) => s.as_str() == Some("OK"),
    }
}

/// A `query` RPC result as per-statement results, failing on the first `ERR`.
///
/// SurrealDB answers `[{ status, result }, ...]`. A statement with status
/// `ERR` fails the call, matching the JS SDK. This is load-bearing for sync: a
/// `LIVE SELECT` on a not-yet-created `_00_list_ref_user_<id>` table fails per
/// statement in v3, and the retry backoff relies on the failure to re-attempt.
pub fn unwrap_query_result(result: Value) -> Result<Vec<Value>> {
    match result {
        Value::Array(stmts) => stmts
            .into_iter()
            .map(|stmt| match stmt {
                Value::Object(mut m) if m.contains_key("result") => {
                    if !is_ok_status(&m) {
                        let message = m.get("result").map(Value::to_id_string).unwrap_or_default();
                        return Err(Error::Statement(message));
                    }
                    Ok(m.remove("result").unwrap_or_default())
                }
                other => Ok(other),
            })
            .collect(),
        other => Ok(vec![other]),
    }
}

/// A `query` RPC result as per-statement outcomes. Never fails.
pub fn statement_results(result: Value) -> Vec<StatementResult> {
    match result {
        Value::Array(stmts) => stmts
            .into_iter()
            .map(|stmt| match stmt {
                Value::Object(mut m) if m.contains_key("result") => {
                    if is_ok_status(&m) {
                        StatementResult::ok(m.remove("result").unwrap_or_default())
                    } else {
                        StatementResult::err(
                            m.get("result").map(Value::to_id_string).unwrap_or_default(),
                        )
                    }
                }
                other => StatementResult::ok(other),
            })
            .collect(),
        other => vec![StatementResult::ok(other)],
    }
}

/// Normalize auth params to the SurrealDB RPC wire shape.
///
/// The auth service (faithful to the JS SDK) passes `{access, variables}`; the
/// raw RPC instead wants `{ns, db, ac, ...variables}` with the access variables
/// flattened. Root logins (`{user, pass}`) pass through unchanged.
pub fn auth_params(params: Object, namespace: Option<&str>, database: Option<&str>) -> Object {
    if !params.contains_key("access") && !params.contains_key("variables") {
        return params;
    }
    let mut out = Object::new();
    if let Some(ns) = namespace {
        out.insert("ns".into(), Value::from(ns));
    }
    if let Some(db) = database {
        out.insert("db".into(), Value::from(db));
    }
    if let Some(access) = params.get("access").filter(|a| !a.is_nullish()) {
        out.insert("ac".into(), access.clone());
    }
    if let Some(Value::Object(vars)) = params.get("variables") {
        out.extend(vars.iter().map(|(k, v)| (k.clone(), v.clone())));
    }
    out
}

#[cfg(feature = "remote")]
pub use ws::WebSocketSurrealClient;

#[cfg(feature = "remote")]
mod ws {
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::{Arc, Mutex, MutexGuard, OnceLock};
    use std::time::Duration;

    use async_trait::async_trait;
    use futures::{SinkExt, StreamExt};
    use tokio::net::TcpStream;
    use tokio::sync::{broadcast, mpsc, oneshot};
    use tokio_tungstenite::tungstenite::client::IntoClientRequest;
    use tokio_tungstenite::tungstenite::http::HeaderValue;
    use tokio_tungstenite::tungstenite::Message;
    use tokio_tungstenite::{Connector, MaybeTlsStream, WebSocketStream};

    use super::*;
    use crate::surreal::cbor;

    /// How long a close may take before the socket is simply dropped: closing
    /// one that never finished opening can hang, and a client being torn down
    /// must not hold the process open.
    const CLOSE_TIMEOUT: Duration = Duration::from_secs(2);

    type Socket = WebSocketStream<MaybeTlsStream<TcpStream>>;

    enum Outbound {
        Frame(Vec<u8>),
        Close,
    }

    struct Pending {
        reply: oneshot::Sender<Result<Value>>,
        /// For a LIVE request: registered under the live id the answer names,
        /// before the caller sees it, so no notification can slip past.
        live: Option<mpsc::UnboundedSender<LiveMessage>>,
    }

    #[derive(Default)]
    struct ConnIo {
        ended: bool,
        pending: HashMap<String, Pending>,
        live: HashMap<String, mpsc::UnboundedSender<LiveMessage>>,
    }

    /// One open socket. In-flight requests and LIVE streams belong to the socket
    /// that carried them, so a late end of an old socket never touches the next.
    struct Conn {
        generation: u64,
        out: mpsc::UnboundedSender<Outbound>,
        io: Mutex<ConnIo>,
        task: Mutex<Option<tokio::task::JoinHandle<()>>>,
    }

    impl Conn {
        fn io(&self) -> MutexGuard<'_, ConnIo> {
            self.io.lock().unwrap_or_else(|e| e.into_inner())
        }
    }

    #[derive(Default)]
    struct State {
        conn: Option<Arc<Conn>>,
        namespace: Option<String>,
        database: Option<String>,
        closed: bool,
    }

    struct Inner {
        state: Mutex<State>,
        events: broadcast::Sender<TransportEvent>,
        next_id: AtomicU64,
        generation: AtomicU64,
        connect_timeout_ms: AtomicU64,
    }

    impl Inner {
        fn state(&self) -> MutexGuard<'_, State> {
            self.state.lock().unwrap_or_else(|e| e.into_inner())
        }

        /// The socket ended. Every in-flight RPC is now unanswerable: failing it
        /// is what lets a caller retry instead of hanging forever, and what makes
        /// the supervisor's probe fail fast on a dead socket. The server-side
        /// LIVE queries die with the socket; the sagas re-subscribe on the next
        /// `connected`. Idempotent per socket.
        fn end(&self, conn: &Conn, reason: Error) {
            let (pending, live) = {
                let mut io = conn.io();
                if io.ended {
                    return;
                }
                io.ended = true;
                (
                    std::mem::take(&mut io.pending),
                    std::mem::take(&mut io.live),
                )
            };
            for p in pending.into_values() {
                let _ = p.reply.send(Err(reason.clone()));
            }
            drop(live);
            let closed = {
                let mut st = self.state();
                if st
                    .conn
                    .as_ref()
                    .is_some_and(|c| c.generation == conn.generation)
                {
                    st.conn = None;
                }
                st.closed
            };
            if !closed {
                let _ = self.events.send(TransportEvent::Disconnected);
            }
        }
    }

    fn socket_closed(detail: Option<String>) -> Error {
        Error::Network(match detail {
            Some(d) => format!("WebSocket closed before the response arrived: {d}"),
            None => "WebSocket closed before the response arrived".into(),
        })
    }

    /// The TLS setup, built once with an explicit `ring` provider: relying on
    /// the process-default provider panics when an embedding app links another
    /// rustls provider too.
    fn tls_config() -> Result<Arc<rustls::ClientConfig>> {
        static CONFIG: OnceLock<Arc<rustls::ClientConfig>> = OnceLock::new();
        if let Some(config) = CONFIG.get() {
            return Ok(config.clone());
        }
        let mut roots = rustls::RootCertStore::empty();
        roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
        let config = rustls::ClientConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .map_err(|e| Error::Network(format!("TLS setup: {e}")))?
        .with_root_certificates(roots)
        .with_no_client_auth();
        Ok(CONFIG.get_or_init(|| Arc::new(config)).clone())
    }

    /// Minimal SurrealDB WebSocket RPC client over the **CBOR** subprotocol.
    ///
    /// Implements only what the core uses (`use`/`signin`/`signup`/
    /// `authenticate`/`invalidate`/`query`/`live`/`kill` plus lifecycle). CBOR
    /// (not JSON) is required so bind vars carry real record ids and datetimes:
    /// SurrealDB won't match a `record<...>` field against a plain string,
    /// which silently broke every record-filtered query. See
    /// [`crate::surreal::cbor`] for the encode and normalize rules.
    ///
    /// Thread-safe and cheap to clone; the socket runs on a spawned task. It
    /// never re-dials by itself: the connection supervisor owns recovery.
    #[derive(Clone)]
    pub struct WebSocketSurrealClient {
        inner: Arc<Inner>,
    }

    impl Default for WebSocketSurrealClient {
        fn default() -> Self {
            Self::new()
        }
    }

    impl WebSocketSurrealClient {
        pub fn new() -> Self {
            WebSocketSurrealClient {
                inner: Arc::new(Inner {
                    state: Mutex::new(State::default()),
                    events: broadcast::channel(16).0,
                    next_id: AtomicU64::new(0),
                    generation: AtomicU64::new(0),
                    connect_timeout_ms: AtomicU64::new(10_000),
                }),
            }
        }

        /// Deadline for [`WebSocketSurrealClient::connect`]. A refused port fails
        /// fast, but a black-holed one never answers.
        pub fn set_connect_timeout(&self, timeout: Duration) {
            self.inner.connect_timeout_ms.store(
                timeout.as_millis().min(u64::MAX as u128) as u64,
                Ordering::Relaxed,
            );
        }

        pub fn connect_timeout(&self) -> Duration {
            Duration::from_millis(self.inner.connect_timeout_ms.load(Ordering::Relaxed))
        }

        /// Lifecycle events from now on.
        pub fn events(&self) -> broadcast::Receiver<TransportEvent> {
            self.inner.events.subscribe()
        }

        /// True while a socket is open.
        pub fn is_connected(&self) -> bool {
            self.inner.state().conn.is_some()
        }

        /// Open the socket for `endpoint` (normalized by [`rpc_endpoint`]),
        /// replacing any open one.
        pub async fn connect(&self, endpoint: &str) -> Result<()> {
            if self.inner.state().closed {
                return Err(Error::Closed);
            }
            if self.is_connected() {
                self.force_close().await;
            }
            let url = rpc_endpoint(endpoint);
            let mut request = url
                .as_str()
                .into_client_request()
                .map_err(|e| Error::Network(e.to_string()))?;
            // Negotiate SurrealDB's CBOR subprotocol so record ids and datetimes
            // travel as typed values (frames are then binary CBOR, not JSON).
            request
                .headers_mut()
                .insert("Sec-WebSocket-Protocol", HeaderValue::from_static("cbor"));
            let connector = if url.starts_with("wss://") {
                Connector::Rustls(tls_config()?)
            } else {
                Connector::Plain
            };
            let timeout_ms = self.inner.connect_timeout_ms.load(Ordering::Relaxed);
            let attempt = tokio_tungstenite::connect_async_tls_with_config(
                request,
                None,
                true,
                Some(connector),
            );
            let (socket, _) = tokio::time::timeout(Duration::from_millis(timeout_ms), attempt)
                .await
                .map_err(|_| Error::Timeout(timeout_ms))?
                .map_err(|e| Error::Network(e.to_string()))?;

            let (out, rx) = mpsc::unbounded_channel();
            let conn = Arc::new(Conn {
                generation: self.inner.generation.fetch_add(1, Ordering::Relaxed) + 1,
                out,
                io: Mutex::new(ConnIo::default()),
                task: Mutex::new(None),
            });
            let installed = {
                let mut st = self.inner.state();
                (!st.closed).then(|| st.conn.replace(conn.clone()))
            };
            let Some(replaced) = installed else {
                // Closed while the socket was opening.
                let mut socket = socket;
                let _ = tokio::time::timeout(CLOSE_TIMEOUT, socket.close(None)).await;
                return Err(Error::Closed);
            };
            let task = tokio::spawn(run_socket(self.inner.clone(), conn.clone(), socket, rx));
            *conn.task.lock().unwrap_or_else(|e| e.into_inner()) = Some(task);
            if let Some(old) = replaced {
                // Two connects raced; the later socket wins.
                shutdown(&self.inner, &old, socket_closed(None)).await;
            }
            let _ = self.inner.events.send(TransportEvent::Connected);
            Ok(())
        }

        /// Drop the socket without disposing the client: in-flight requests
        /// fail, LIVE streams end and `Disconnected` fires.
        pub async fn force_close(&self) {
            let conn = self.inner.state().conn.take();
            if let Some(conn) = conn {
                // A socket that is already gone, or one too wedged to close, is
                // exactly the state we wanted to reach.
                shutdown(&self.inner, &conn, socket_closed(None)).await;
            }
        }

        /// Dispose the client. In-flight requests fail with [`Error::Closed`];
        /// no `Disconnected` fires and no later `connect` succeeds.
        pub async fn close(&self) {
            let conn = {
                let mut st = self.inner.state();
                st.closed = true;
                st.conn.take()
            };
            if let Some(conn) = conn {
                shutdown(&self.inner, &conn, Error::Closed).await;
            }
        }

        fn current(&self) -> Result<Arc<Conn>> {
            self.inner
                .state()
                .conn
                .clone()
                .ok_or_else(|| Error::NoRemote("WebSocket not connected".into()))
        }

        async fn rpc_inner(
            &self,
            method: &str,
            params: Vec<Value>,
            live: Option<mpsc::UnboundedSender<LiveMessage>>,
        ) -> Result<Value> {
            let conn = self.current()?;
            let id = self
                .inner
                .next_id
                .fetch_add(1, Ordering::Relaxed)
                .to_string();
            let (reply, answer) = oneshot::channel();
            {
                let mut io = conn.io();
                if io.ended {
                    return Err(socket_closed(None));
                }
                io.pending.insert(id.clone(), Pending { reply, live });
            }
            let mut envelope = Object::new();
            envelope.insert("id".into(), Value::String(id.clone()));
            envelope.insert("method".into(), Value::from(method));
            envelope.insert("params".into(), Value::Array(params));
            if conn
                .out
                .send(Outbound::Frame(cbor::encode(&Value::Object(envelope))))
                .is_err()
            {
                conn.io().pending.remove(&id);
                return Err(socket_closed(None));
            }
            answer.await.unwrap_or_else(|_| Err(socket_closed(None)))
        }

        /// One raw RPC: its `result`, or the server's error.
        pub async fn rpc(&self, method: &str, params: Vec<Value>) -> Result<Value> {
            self.rpc_inner(method, params, None).await
        }

        pub async fn use_db(&self, namespace: &str, database: &str) -> Result<()> {
            {
                let mut st = self.inner.state();
                st.namespace = Some(namespace.to_string());
                st.database = Some(database.to_string());
            }
            self.rpc("use", vec![namespace.into(), database.into()])
                .await?;
            Ok(())
        }

        pub async fn authenticate(&self, token: &str) -> Result<Value> {
            self.rpc("authenticate", vec![token.into()]).await
        }

        fn wire_auth_params(&self, params: Object) -> Object {
            let st = self.inner.state();
            auth_params(params, st.namespace.as_deref(), st.database.as_deref())
        }

        pub async fn signin(&self, params: Object) -> Result<Value> {
            let params = self.wire_auth_params(params);
            self.rpc("signin", vec![Value::Object(params)]).await
        }

        pub async fn signup(&self, params: Object) -> Result<Value> {
            let params = self.wire_auth_params(params);
            self.rpc("signup", vec![Value::Object(params)]).await
        }

        pub async fn invalidate(&self) -> Result<()> {
            self.rpc("invalidate", vec![]).await?;
            Ok(())
        }

        fn query_params(sql: &str, vars: Option<Object>) -> Vec<Value> {
            vec![sql.into(), Value::Object(vars.unwrap_or_default())]
        }

        pub async fn query(&self, sql: &str, vars: Option<Object>) -> Result<Vec<Value>> {
            unwrap_query_result(self.rpc("query", Self::query_params(sql, vars)).await?)
        }

        pub async fn query_statements(
            &self,
            sql: &str,
            vars: Option<Object>,
        ) -> Result<Vec<StatementResult>> {
            Ok(statement_results(
                self.rpc("query", Self::query_params(sql, vars)).await?,
            ))
        }

        pub async fn live(&self, sql: &str, vars: Option<Object>) -> Result<(String, LiveStream)> {
            let (tx, rx) = mpsc::unbounded_channel();
            let result = self
                .rpc_inner("query", Self::query_params(sql, vars), Some(tx))
                .await?;
            let results = unwrap_query_result(result)?;
            Ok((live_id_of(&results), rx))
        }

        pub async fn kill(&self, live_id: &str) -> Result<()> {
            self.rpc("kill", vec![live_id.into()]).await?;
            if let Ok(conn) = self.current() {
                conn.io().live.remove(live_id);
            }
            Ok(())
        }
    }

    fn live_id_of(results: &[Value]) -> String {
        results.first().map(Value::to_id_string).unwrap_or_default()
    }

    /// Close `conn` (bounded), then end it with `reason`.
    async fn shutdown(inner: &Arc<Inner>, conn: &Arc<Conn>, reason: Error) {
        // Fail what is in flight first, so it sees `reason` rather than the
        // socket task's own end.
        inner.end(conn, reason);
        let _ = conn.out.send(Outbound::Close);
        let task = conn.task.lock().unwrap_or_else(|e| e.into_inner()).take();
        if let Some(mut task) = task {
            if tokio::time::timeout(CLOSE_TIMEOUT, &mut task)
                .await
                .is_err()
            {
                task.abort();
            }
        }
    }

    /// The socket's one task: writes queued frames, reads answers and LIVE
    /// notifications, and ends the connection when either side closes.
    async fn run_socket(
        inner: Arc<Inner>,
        conn: Arc<Conn>,
        mut socket: Socket,
        mut rx: mpsc::UnboundedReceiver<Outbound>,
    ) {
        let detail = loop {
            tokio::select! {
                out = rx.recv() => match out {
                    Some(Outbound::Frame(bytes)) => {
                        if let Err(e) = socket.send(Message::Binary(bytes.into())).await {
                            break Some(e.to_string());
                        }
                    }
                    Some(Outbound::Close) | None => {
                        let _ = tokio::time::timeout(CLOSE_TIMEOUT, socket.close(None)).await;
                        break None;
                    }
                },
                frame = socket.next() => match frame {
                    Some(Ok(Message::Binary(bytes))) => on_frame(&conn, &bytes),
                    Some(Ok(Message::Close(_))) | None => break None,
                    // Text frames are not CBOR; pings are answered by the library.
                    Some(Ok(_)) => {}
                    Some(Err(e)) => break Some(e.to_string()),
                },
            }
        };
        inner.end(&conn, socket_closed(detail));
    }

    fn on_frame(conn: &Conn, bytes: &[u8]) {
        let Ok(Value::Object(mut frame)) = cbor::decode(bytes) else {
            return;
        };
        let id = frame
            .get("id")
            .filter(|v| !v.is_nullish())
            .map(Value::to_id_string);

        // LIVE notifications carry no matching request id.
        let Some(id) = id else {
            if let Some(Value::Object(notification)) = frame.remove("result") {
                if notification.get("id").is_some_and(|v| !v.is_nullish()) {
                    dispatch_live(conn, notification);
                }
            }
            return;
        };

        let Some(pending) = conn.io().pending.remove(&id) else {
            return;
        };
        if let Some(error) = frame.get("error").filter(|e| !e.is_nullish()) {
            let (code, message) = match error {
                Value::Object(e) => (
                    e.get("code").and_then(Value::as_i64).unwrap_or(0),
                    e.get("message")
                        .map(Value::to_id_string)
                        .unwrap_or_else(|| error.to_id_string()),
                ),
                other => (0, other.to_id_string()),
            };
            let _ = pending.reply.send(Err(Error::Rpc { code, message }));
            return;
        }
        let result = frame.remove("result").unwrap_or_default();
        if let Some(sink) = pending.live {
            if let Ok(results) = unwrap_query_result(result.clone()) {
                conn.io().live.insert(live_id_of(&results), sink);
            }
        }
        let _ = pending.reply.send(Ok(result));
    }

    fn dispatch_live(conn: &Conn, mut notification: Object) {
        let live_id = notification
            .get("id")
            .map(Value::to_id_string)
            .unwrap_or_default();
        let action = notification
            .get("action")
            .and_then(Value::as_str)
            .map(str::to_uppercase)
            .unwrap_or_default();
        let value = match notification.remove("result") {
            Some(Value::Object(o)) => o,
            _ => Object::new(),
        };
        let mut io = conn.io();
        if let Some(sink) = io.live.get(&live_id) {
            if sink.send(LiveMessage { action, value }).is_err() {
                // Nobody listens any more.
                io.live.remove(&live_id);
            }
        }
    }

    #[async_trait(?Send)]
    impl RemoteSurrealClient for WebSocketSurrealClient {
        async fn connect(&self, endpoint: &str) -> Result<()> {
            WebSocketSurrealClient::connect(self, endpoint).await
        }
        async fn use_db(&self, namespace: &str, database: &str) -> Result<()> {
            WebSocketSurrealClient::use_db(self, namespace, database).await
        }
        async fn authenticate(&self, token: &str) -> Result<Value> {
            WebSocketSurrealClient::authenticate(self, token).await
        }
        async fn signin(&self, params: Object) -> Result<Value> {
            WebSocketSurrealClient::signin(self, params).await
        }
        async fn signup(&self, params: Object) -> Result<Value> {
            WebSocketSurrealClient::signup(self, params).await
        }
        async fn invalidate(&self) -> Result<()> {
            WebSocketSurrealClient::invalidate(self).await
        }
        async fn query(&self, sql: &str, vars: Option<Object>) -> Result<Vec<Value>> {
            WebSocketSurrealClient::query(self, sql, vars).await
        }
        async fn query_statements(
            &self,
            sql: &str,
            vars: Option<Object>,
        ) -> Result<Vec<StatementResult>> {
            WebSocketSurrealClient::query_statements(self, sql, vars).await
        }
        async fn live(&self, sql: &str, vars: Option<Object>) -> Result<(String, LiveStream)> {
            WebSocketSurrealClient::live(self, sql, vars).await
        }
        async fn kill(&self, live_id: &str) -> Result<()> {
            WebSocketSurrealClient::kill(self, live_id).await
        }
        fn events(&self) -> broadcast::Receiver<TransportEvent> {
            WebSocketSurrealClient::events(self)
        }
        fn is_connected(&self) -> bool {
            WebSocketSurrealClient::is_connected(self)
        }
        async fn force_close(&self) {
            WebSocketSurrealClient::force_close(self).await
        }
        async fn close(&self) {
            WebSocketSurrealClient::close(self).await
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rpc_endpoint_normalization() {
        assert_eq!(rpc_endpoint("ws://h:8000"), "ws://h:8000/rpc");
        assert_eq!(rpc_endpoint("http://h:8000"), "ws://h:8000/rpc");
        assert_eq!(rpc_endpoint("https://h:8000/"), "wss://h:8000/rpc");
        assert_eq!(rpc_endpoint("ws://h:8000/rpc"), "ws://h:8000/rpc");
    }

    #[test]
    fn auth_params_flatten_access_and_keep_root_logins() {
        let flat = auth_params(
            crate::value::object_from_json(serde_json::json!({
                "access": "account",
                "variables": {"email": "a@b.c", "password": "pw"},
            })),
            Some("myns"),
            Some("mydb"),
        );
        assert_eq!(
            Value::Object(flat).to_json(),
            serde_json::json!({"ns": "myns", "db": "mydb", "ac": "account", "email": "a@b.c", "password": "pw"})
        );
        let root =
            crate::value::object_from_json(serde_json::json!({"user": "root", "pass": "root"}));
        assert_eq!(auth_params(root.clone(), Some("n"), Some("d")), root);
    }

    #[test]
    fn statement_shapes() {
        let raw = Value::from(serde_json::json!([
            {"status": "OK", "result": [{"id": "thing:a"}]},
            {"status": "ERR", "result": "boom"},
        ]));
        let stmts = statement_results(raw.clone());
        assert_eq!(
            stmts[0],
            StatementResult::ok(Value::from(serde_json::json!([{"id": "thing:a"}])))
        );
        assert_eq!(stmts[1], StatementResult::err("boom"));
        assert!(matches!(unwrap_query_result(raw), Err(Error::Statement(m)) if m == "boom"));
        // A bare (non-list) result is one OK statement.
        assert_eq!(
            unwrap_query_result(Value::Int(3)).unwrap(),
            vec![Value::Int(3)]
        );
    }
}

#[cfg(all(test, feature = "remote"))]
mod ws_tests {
    //! The client against an in-process WebSocket server standing in for
    //! SurrealDB.

    use std::time::Duration;

    use futures::{SinkExt, StreamExt};
    use tokio::net::TcpListener;
    use tokio::sync::mpsc;
    use tokio_tungstenite::tungstenite::handshake::server::{Request, Response};
    use tokio_tungstenite::tungstenite::http::HeaderValue;
    use tokio_tungstenite::tungstenite::Message;

    use super::*;
    use crate::surreal::cbor;
    use crate::value::{object_from_json, RecordId};

    enum ToClient {
        Frame(Object),
        Close,
    }

    /// A fake SurrealDB: hands every frame the client sends to the test and
    /// sends back whatever the test pushes.
    struct FakeServer {
        endpoint: String,
        received: mpsc::UnboundedReceiver<Object>,
        push: mpsc::UnboundedSender<ToClient>,
    }

    impl FakeServer {
        // The handshake callback's error type is tungstenite's.
        #[allow(clippy::result_large_err)]
        async fn start() -> FakeServer {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let endpoint = format!("ws://{}", listener.local_addr().unwrap());
            let (received_tx, received) = mpsc::unbounded_channel();
            let (push, mut push_rx) = mpsc::unbounded_channel::<ToClient>();
            tokio::spawn(async move {
                let (stream, _) = listener.accept().await.unwrap();
                let mut ws = tokio_tungstenite::accept_hdr_async(
                    stream,
                    |req: &Request, mut resp: Response| {
                        assert_eq!(
                            req.headers()
                                .get("Sec-WebSocket-Protocol")
                                .map(|v| v.to_str().unwrap()),
                            Some("cbor")
                        );
                        resp.headers_mut()
                            .insert("Sec-WebSocket-Protocol", HeaderValue::from_static("cbor"));
                        Ok(resp)
                    },
                )
                .await
                .unwrap();
                loop {
                    tokio::select! {
                        out = push_rx.recv() => match out {
                            Some(ToClient::Frame(frame)) => {
                                let bytes = cbor::encode(&Value::Object(frame));
                                if ws.send(Message::Binary(bytes.into())).await.is_err() { break }
                            }
                            Some(ToClient::Close) | None => { let _ = ws.close(None).await; break }
                        },
                        frame = ws.next() => match frame {
                            Some(Ok(Message::Binary(b))) => {
                                let Ok(Value::Object(o)) = cbor::decode(&b) else { continue };
                                let _ = received_tx.send(o);
                            }
                            Some(Ok(_)) => {}
                            _ => break,
                        },
                    }
                }
            });
            FakeServer {
                endpoint,
                received,
                push,
            }
        }

        async fn next(&mut self) -> Object {
            tokio::time::timeout(Duration::from_secs(5), self.received.recv())
                .await
                .expect("the client sent nothing")
                .expect("server gone")
        }

        fn push(&self, frame: serde_json::Value) {
            self.push
                .send(ToClient::Frame(object_from_json(frame)))
                .unwrap();
        }

        fn respond(&self, request: &Object, result: serde_json::Value) {
            let mut frame = Object::new();
            frame.insert("id".into(), request["id"].clone());
            frame.insert("result".into(), Value::from(result));
            self.push.send(ToClient::Frame(frame)).unwrap();
        }
    }

    async fn connected() -> (FakeServer, WebSocketSurrealClient) {
        let server = FakeServer::start().await;
        let client = WebSocketSurrealClient::new();
        client.connect(&server.endpoint).await.unwrap();
        (server, client)
    }

    fn spawn<T: Send + 'static>(
        f: impl std::future::Future<Output = T> + Send + 'static,
    ) -> tokio::task::JoinHandle<T> {
        tokio::spawn(f)
    }

    #[tokio::test]
    async fn connect_emits_connected() {
        let server = FakeServer::start().await;
        let client = WebSocketSurrealClient::new();
        let mut events = client.events();
        client.connect(&server.endpoint).await.unwrap();
        assert!(client.is_connected());
        let event = tokio::time::timeout(Duration::from_secs(1), events.recv())
            .await
            .unwrap();
        assert_eq!(event.unwrap(), TransportEvent::Connected);
        client.close().await;
    }

    #[tokio::test]
    async fn signin_flattens_access_params() {
        let (mut server, client) = connected().await;
        let c = client.clone();
        let use_call = spawn(async move { c.use_db("myns", "mydb").await });
        let frame = server.next().await;
        assert_eq!(frame["method"], Value::from("use"));
        server.respond(
            &frame,
            serde_json::json!({"namespace": "myns", "database": "mydb"}),
        );
        use_call.await.unwrap().unwrap();

        let c = client.clone();
        let signin = spawn(async move {
            c.signin(object_from_json(serde_json::json!({
                "access": "account",
                "variables": {"email": "a@b.c", "password": "pw"},
            })))
            .await
        });
        let frame = server.next().await;
        assert_eq!(frame["method"], Value::from("signin"));
        let params = frame["params"].as_array().unwrap()[0].to_json();
        assert_eq!(
            params,
            serde_json::json!({"ns": "myns", "db": "mydb", "ac": "account", "email": "a@b.c", "password": "pw"})
        );
        server.respond(&frame, serde_json::json!("tok"));
        assert_eq!(signin.await.unwrap().unwrap(), Value::from("tok"));
        client.close().await;
    }

    #[tokio::test]
    async fn root_signin_passes_through() {
        let (mut server, client) = connected().await;
        let c = client.clone();
        let signin = spawn(async move {
            c.signin(object_from_json(
                serde_json::json!({"user": "root", "pass": "root"}),
            ))
            .await
        });
        let frame = server.next().await;
        assert_eq!(
            frame["params"].as_array().unwrap()[0].to_json(),
            serde_json::json!({"user": "root", "pass": "root"})
        );
        server.respond(&frame, serde_json::json!("roottok"));
        signin.await.unwrap().unwrap();
        client.close().await;
    }

    #[tokio::test]
    async fn query_unwraps_and_fails_on_err() {
        let (mut server, client) = connected().await;
        let c = client.clone();
        let q = spawn(async move { c.query("SELECT * FROM thing", None).await });
        let frame = server.next().await;
        server.respond(
            &frame,
            serde_json::json!([{"status": "OK", "result": [{"id": "thing:a"}]}]),
        );
        let out = q.await.unwrap().unwrap();
        assert_eq!(out[0].to_json(), serde_json::json!([{"id": "thing:a"}]));

        let c = client.clone();
        let q = spawn(async move { c.query("THROW \"boom\"", None).await });
        let frame = server.next().await;
        server.respond(
            &frame,
            serde_json::json!([{"status": "ERR", "result": "boom"}]),
        );
        assert!(matches!(q.await.unwrap(), Err(Error::Statement(m)) if m == "boom"));

        // The per-statement form keeps both outcomes.
        let c = client.clone();
        let q = spawn(async move { c.query_statements("RETURN 1; THROW \"boom\"", None).await });
        let frame = server.next().await;
        server.respond(
            &frame,
            serde_json::json!([{"status": "OK", "result": 1}, {"status": "ERR", "result": "boom"}]),
        );
        let stmts = q.await.unwrap().unwrap();
        assert_eq!(
            stmts,
            vec![StatementResult::ok(1i64), StatementResult::err("boom")]
        );
        client.close().await;
    }

    #[tokio::test]
    async fn query_encodes_typed_bind_vars() {
        let (mut server, client) = connected().await;
        let mut vars = Object::new();
        vars.insert("id".into(), RecordId::new("thing", "x").into());
        vars.insert(
            "when".into(),
            Value::Datetime(
                chrono::DateTime::parse_from_rfc3339("2026-01-02T03:04:05Z")
                    .unwrap()
                    .into(),
            ),
        );
        let c = client.clone();
        let q = spawn(async move { c.query("SELECT * FROM $id", Some(vars)).await });
        let frame = server.next().await;
        // The fake decodes like the client does, so typed values read back
        // normalized.
        let sent = &frame["params"].as_array().unwrap()[1];
        assert_eq!(sent.get("id"), Some(&Value::from("thing:x")));
        assert_eq!(
            sent.get("when"),
            Some(&Value::from("2026-01-02T03:04:05.000Z"))
        );
        server.respond(&frame, serde_json::json!([{"status": "OK", "result": []}]));
        q.await.unwrap().unwrap();
        client.close().await;
    }

    #[tokio::test]
    async fn live_routes_notifications_by_id_and_kill_stops_them() {
        let (mut server, client) = connected().await;
        let c = client.clone();
        let live = spawn(async move { c.live("LIVE SELECT * FROM thing", None).await });
        let frame = server.next().await;
        server.respond(
            &frame,
            serde_json::json!([{"status": "OK", "type": "live", "result": "live-uuid-1"}]),
        );
        // A notification right behind the answer is not lost.
        server.push(serde_json::json!({"result": {
            "id": "live-uuid-1", "action": "CREATE", "result": {"id": "thing:a", "name": "bob"},
        }}));
        let (live_id, mut stream) = live.await.unwrap().unwrap();
        assert_eq!(live_id, "live-uuid-1");
        let got = tokio::time::timeout(Duration::from_secs(1), stream.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(got.action, "CREATE");
        assert_eq!(got.value["name"], Value::from("bob"));

        // A notification for another live id is ignored.
        server
            .push(serde_json::json!({"result": {"id": "other", "action": "CREATE", "result": {}}}));
        // Lowercase actions are normalized.
        server.push(serde_json::json!({"result": {"id": "live-uuid-1", "action": "update", "result": {"id": "thing:a"}}}));
        let got = tokio::time::timeout(Duration::from_secs(1), stream.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(got.action, "UPDATE");

        let c = client.clone();
        let kill = spawn(async move { c.kill("live-uuid-1").await });
        let frame = server.next().await;
        assert_eq!(frame["method"], Value::from("kill"));
        server.respond(&frame, serde_json::Value::Null);
        kill.await.unwrap().unwrap();
        assert!(stream.recv().await.is_none(), "kill ends the stream");
        client.close().await;
    }

    #[tokio::test]
    async fn an_error_frame_rejects_the_pending_rpc() {
        let (mut server, client) = connected().await;
        let c = client.clone();
        let q = spawn(async move { c.query("bad", None).await });
        let frame = server.next().await;
        let mut reply = Object::new();
        reply.insert("id".into(), frame["id"].clone());
        reply.insert(
            "error".into(),
            Value::from(serde_json::json!({"code": -32000, "message": "parse error"})),
        );
        server.push.send(ToClient::Frame(reply)).unwrap();
        match q.await.unwrap() {
            Err(Error::Rpc { code, message }) => {
                assert_eq!(code, -32000);
                assert_eq!(message, "parse error");
            }
            other => panic!("{other:?}"),
        }
        client.close().await;
    }

    #[tokio::test]
    async fn server_close_fails_in_flight_and_ends_live_streams() {
        let (mut server, client) = connected().await;
        let mut events = client.events();
        let c = client.clone();
        let live = spawn(async move { c.live("LIVE SELECT * FROM thing", None).await });
        let frame = server.next().await;
        server.respond(
            &frame,
            serde_json::json!([{"status": "OK", "result": "l1"}]),
        );
        let (_, mut stream) = live.await.unwrap().unwrap();

        let c = client.clone();
        let q = spawn(async move { c.query("SELECT * FROM thing", None).await });
        server.next().await;
        server.push.send(ToClient::Close).unwrap();
        assert!(matches!(q.await.unwrap(), Err(e) if e.is_network()));
        assert!(stream.recv().await.is_none());
        let event = tokio::time::timeout(Duration::from_secs(1), events.recv())
            .await
            .unwrap();
        assert_eq!(event.unwrap(), TransportEvent::Disconnected);
        assert!(!client.is_connected());
        // Not connected: an RPC fails at once.
        assert!(matches!(
            client.query("RETURN 1", None).await,
            Err(Error::NoRemote(_))
        ));
    }

    #[tokio::test]
    async fn force_close_fails_in_flight_once_and_allows_reconnect() {
        let (mut server, client) = connected().await;
        let mut events = client.events();
        let c = client.clone();
        let q = spawn(async move { c.query("SELECT * FROM thing", None).await });
        server.next().await;
        client.force_close().await;
        assert!(!client.is_connected());
        assert!(matches!(q.await.unwrap(), Err(Error::Network(_))));
        let event = tokio::time::timeout(Duration::from_secs(1), events.recv())
            .await
            .unwrap();
        assert_eq!(event.unwrap(), TransportEvent::Disconnected);
        // Exactly one Disconnected per socket.
        assert!(
            tokio::time::timeout(Duration::from_millis(100), events.recv())
                .await
                .is_err(),
            "a second Disconnected fired"
        );

        let server = FakeServer::start().await;
        client.connect(&server.endpoint).await.unwrap();
        assert!(client.is_connected());
        client.close().await;
    }

    #[tokio::test]
    async fn close_fails_in_flight_with_closed_and_stays_closed() {
        let (mut server, client) = connected().await;
        let mut events = client.events();
        let c = client.clone();
        let q = spawn(async move { c.query("SELECT * FROM thing", None).await });
        server.next().await;
        client.close().await;
        assert!(matches!(q.await.unwrap(), Err(Error::Closed)));
        assert!(
            tokio::time::timeout(Duration::from_millis(100), events.recv())
                .await
                .is_err(),
            "close must not report a disconnect"
        );
        assert!(matches!(
            client.connect(&server.endpoint).await,
            Err(Error::Closed)
        ));
    }

    #[tokio::test]
    async fn connect_times_out_on_a_silent_endpoint() {
        // Accepts TCP but never answers the WebSocket handshake.
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("ws://{}", listener.local_addr().unwrap());
        let hold = tokio::spawn(async move {
            let (_stream, _) = listener.accept().await.unwrap();
            tokio::time::sleep(Duration::from_secs(30)).await;
        });
        let client = WebSocketSurrealClient::new();
        client.set_connect_timeout(Duration::from_millis(150));
        assert!(matches!(
            client.connect(&endpoint).await,
            Err(Error::Timeout(150))
        ));
        assert!(!client.is_connected());
        hold.abort();
    }
}
