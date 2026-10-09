//! Blue/green scheduler handover: a new scheduler process (green) takes over
//! from a running one (blue) on the same data volume without a moment in which
//! nobody answers.
//!
//! The control plane starts green next to blue, on the same `/data` volume and
//! under the same network alias, with `SPKY_HANDOVER_FROM` naming blue. From
//! then on a request may land on either process, so both run every listener
//! through one [`Gate`] that decides per request where it goes:
//!
//! 1. Green binds its ports at once and holds every request ([`Mode::Hold`]).
//! 2. Green asks blue to hand over (`POST /handover/prepare`). Blue holds its
//!    own traffic, lets in-flight requests finish, stops ingest at a clean
//!    boundary, stops its background writers, exports what lives only in its
//!    memory (the SSP pool and the view assignments), closes its replica and
//!    WAL, and from then on relays every request to green ([`Mode::Forward`]).
//! 3. Green opens the replica blue just released, imports the state, boots
//!    without the steps a cold start needs (no `_00_query` wipe, no startup
//!    drift pass: the replica was live a second ago) and serves the requests
//!    it held. SSPs keep their registration and never notice.
//! 4. The control plane stops blue. On SIGTERM a relaying blue stops
//!    accepting, finishes what it is relaying and exits.
//!
//! A blue that predates handover answers the prepare with a 404: green then
//! relays to blue while it waits for the replica lock, and boots the ordinary
//! way the moment the control plane has stopped blue. Requests in that window
//! are held, not refused.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use axum::body::Body;
use axum::extract::Request;
use axum::http::{HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Router;
use serde::{Deserialize, Serialize};
use tokio::sync::watch;
use tower::ServiceExt;
use tracing::{info, warn};

/// Longest a request waits in [`Mode::Hold`] before it is refused. A handover
/// holds traffic for a second or two; this only bounds a green that never
/// comes up.
const HOLD_MAX: Duration = Duration::from_secs(90);

/// Set on every relayed request. A process asked to relay a request that was
/// relayed to it already holds it instead: two schedulers relaying to each
/// other (a prepare that timed out on the successor's side while the
/// predecessor finished it) would otherwise bounce it between them forever.
/// One of them is about to serve, and the held request goes there.
const RELAYED_HEADER: &str = "x-sp00ky-relayed";

/// Largest request body the relay buffers. Bodies are buffered so a relay that
/// finds its target not listening yet can try again; scheduler requests are
/// small JSON (an ingest event, a view registration, an SSP heartbeat).
const RELAY_BODY_LIMIT: usize = 64 * 1024 * 1024;

/// Where requests arriving at this process go.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Mode {
    /// Wait for the mode to change: green before its replica is open, blue
    /// while it quiesces.
    Hold,
    /// Answer here, through the installed router.
    Serve,
    /// Relay to the scheduler at this host, on the port the request came in
    /// on (blue and green run the same configuration).
    Forward(String),
}

/// Which listener a request came in on. Only decides what counts as
/// in-flight work the quiesce must wait for.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Listener {
    /// The ingest/SSP/proxy port: everything that touches the replica.
    Main,
    /// The admin dashboard.
    Admin,
    /// Pool machines. Their polls are held open on purpose and never touch
    /// the replica, so they are not waited for.
    Pool,
}

/// The successor's port per listener, when it differs from ours (two
/// schedulers on one host, as in a local test). `None` relays to the port the
/// request came in on, which is the production case: same configuration.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RelayPorts {
    #[serde(default)]
    pub main: Option<u16>,
    #[serde(default)]
    pub admin: Option<u16>,
    #[serde(default)]
    pub pool: Option<u16>,
}

impl RelayPorts {
    fn for_listener(&self, listener: Listener) -> Option<u16> {
        match listener {
            Listener::Main => self.main,
            Listener::Admin => self.admin,
            Listener::Pool => self.pool,
        }
    }
}

/// What `/handover/status` reports. The control plane polls it on green to
/// learn when blue can go.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StatusReport {
    pub supported: bool,
    /// `starting` | `standby` | `active` | `retired` | `failed`
    pub role: String,
    pub phase: String,
    pub peer: Option<String>,
    pub error: Option<String>,
    pub version: String,
}

/// The process-wide gate. One per process: every listener and the handover
/// itself share it.
pub struct Gate {
    mode: watch::Sender<Mode>,
    /// Requests being answered here whose response head is not out yet, on
    /// the listeners the quiesce waits for.
    inflight: AtomicU64,
    /// Requests being relayed right now (blue after the handover).
    relaying: AtomicU64,
    status: Mutex<StatusReport>,
    relay_ports: Mutex<RelayPorts>,
    client: reqwest::Client,
}

static GATE: OnceLock<Gate> = OnceLock::new();

/// The process's gate. Starts in [`Mode::Hold`]: nothing is served before a
/// router is installed and the boot path says so.
pub fn gate() -> &'static Gate {
    GATE.get_or_init(|| Gate {
        mode: watch::channel(Mode::Hold).0,
        inflight: AtomicU64::new(0),
        relaying: AtomicU64::new(0),
        status: Mutex::new(StatusReport {
            supported: true,
            role: "starting".into(),
            phase: "holding".into(),
            peer: None,
            error: None,
            version: env!("CARGO_PKG_VERSION").to_string(),
        }),
        relay_ports: Mutex::new(RelayPorts::default()),
        client: reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(2))
            // No overall timeout: the relay carries SSE streams and long
            // polls, and every relayed request already has its own deadline
            // on the side that answers it.
            .pool_idle_timeout(Duration::from_secs(30))
            .build()
            .expect("static reqwest client config"),
    })
}

impl Gate {
    pub fn mode(&self) -> Mode {
        self.mode.borrow().clone()
    }

    pub fn set_mode(&self, mode: Mode) {
        info!(mode = ?mode, "Request gate");
        self.mode.send_replace(mode);
    }

    pub fn set_status(&self, role: &str, phase: &str) {
        let mut status = self.status.lock().unwrap_or_else(|e| e.into_inner());
        status.role = role.to_string();
        status.phase = phase.to_string();
    }

    /// Ports to relay to when the peer's differ from ours; set before
    /// switching to [`Mode::Forward`].
    pub fn set_relay_ports(&self, ports: RelayPorts) {
        *self.relay_ports.lock().unwrap_or_else(|e| e.into_inner()) = ports;
    }

    fn relay_port(&self, listener: Listener) -> Option<u16> {
        self.relay_ports.lock().unwrap_or_else(|e| e.into_inner()).for_listener(listener)
    }

    pub fn set_peer(&self, peer: Option<String>) {
        self.status.lock().unwrap_or_else(|e| e.into_inner()).peer = peer;
    }

    pub fn set_error(&self, error: Option<String>) {
        self.status.lock().unwrap_or_else(|e| e.into_inner()).error = error;
    }

    pub fn status(&self) -> StatusReport {
        self.status.lock().unwrap_or_else(|e| e.into_inner()).clone()
    }

    pub fn role(&self) -> String {
        self.status().role
    }

    /// Wait until no request on the waited-for listeners is mid-answer, or
    /// `max` passes. Returns whether it got there.
    pub async fn wait_idle(&self, max: Duration) -> bool {
        let deadline = Instant::now() + max;
        loop {
            if self.inflight.load(Ordering::SeqCst) == 0 {
                return true;
            }
            if Instant::now() >= deadline {
                return false;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    pub fn inflight(&self) -> u64 {
        self.inflight.load(Ordering::SeqCst)
    }

    /// Wait until nothing is being relayed, or `max` passes.
    pub async fn wait_relays(&self, max: Duration) -> bool {
        let deadline = Instant::now() + max;
        while self.relaying.load(Ordering::SeqCst) > 0 {
            if Instant::now() >= deadline {
                return false;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        true
    }

    /// Wait for the mode to change from `seen`, up to `max`. Returns the new
    /// mode, or `None` on timeout.
    async fn changed_from(&self, seen: &Mode, max: Duration) -> Option<Mode> {
        let mut rx = self.mode.subscribe();
        let wait = async {
            loop {
                {
                    let now = rx.borrow_and_update();
                    if *now != *seen {
                        return now.clone();
                    }
                }
                if rx.changed().await.is_err() {
                    // The sender lives in a static; this cannot happen.
                    std::future::pending::<()>().await;
                }
            }
        };
        tokio::time::timeout(max, wait).await.ok()
    }
}

/// Decrements its counter when the answer's head is out (or the request is
/// dropped).
struct Tracked(&'static AtomicU64);

impl Tracked {
    fn new(counter: &'static AtomicU64) -> Self {
        counter.fetch_add(1, Ordering::SeqCst);
        Self(counter)
    }
}

impl Drop for Tracked {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

/// The router installed later, once the process has state to serve from.
pub type RouterSlot = Arc<OnceLock<Router>>;

/// Wrap a listener: every request goes through the gate, which serves it from
/// `slot`, holds it, or relays it. `/handover/status` is always answered here,
/// whatever the mode, so the control plane can see a green that is still
/// holding.
pub fn gated(slot: RouterSlot, listener: Listener, port: u16) -> Router {
    Router::new().fallback(move |req: Request| {
        let slot = Arc::clone(&slot);
        async move { dispatch(slot, listener, port, req).await }
    })
}

async fn dispatch(slot: RouterSlot, listener: Listener, port: u16, req: Request) -> Response {
    let gate = gate();
    if listener == Listener::Main && req.uri().path() == "/handover/status" {
        return axum::Json(gate.status()).into_response();
    }
    let deadline = Instant::now() + HOLD_MAX;
    // A body buffered by a relay attempt, so a retry or a fall-back to local
    // serving can rebuild the request.
    let mut req = Some(req);
    let mut buffered: Option<(axum::http::request::Parts, axum::body::Bytes)> = None;
    loop {
        let mode = gate.mode();
        let remaining = deadline.saturating_duration_since(Instant::now());
        match &mode {
            Mode::Serve => {
                if let Some(router) = slot.get() {
                    let req = take_request(&mut req, &mut buffered);
                    // `/handover/*` itself must not wait on itself.
                    let _tracked = (listener != Listener::Pool
                        && !req.uri().path().starts_with("/handover/"))
                        .then(|| Tracked::new(&gate.inflight));
                    return match router.clone().oneshot(req).await {
                        Ok(resp) => resp,
                        Err(never) => match never {},
                    };
                }
                // Serving was declared before the router landed: treat as a
                // hold until it does.
                if remaining.is_zero() {
                    return held_too_long();
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            Mode::Hold => {
                if remaining.is_zero() {
                    return held_too_long();
                }
                let _ = gate.changed_from(&mode, remaining).await;
            }
            // A retired process relays even a relayed request: its successor
            // holds or serves, it never relays back.
            Mode::Forward(_) if was_relayed(&req, &buffered) && gate.role() != "retired" => {
                if remaining.is_zero() {
                    return held_too_long();
                }
                // Short, so a change of role (not only of mode) is seen.
                let _ = gate.changed_from(&mode, Duration::from_millis(250).min(remaining)).await;
            }
            Mode::Forward(host) => {
                if buffered.is_none() {
                    let (parts, body) = take_request(&mut req, &mut buffered).into_parts();
                    let bytes = match axum::body::to_bytes(body, RELAY_BODY_LIMIT).await {
                        Ok(b) => b,
                        Err(e) => {
                            return (StatusCode::PAYLOAD_TOO_LARGE, format!("relay: {e}")).into_response();
                        }
                    };
                    buffered = Some((parts, bytes));
                }
                let (parts, bytes) = buffered.as_ref().expect("buffered above");
                let target_port = gate.relay_port(listener).unwrap_or(port);
                match relay(gate, host, target_port, parts, bytes.clone()).await {
                    Ok(resp) => return resp,
                    Err(RelayError::Unreachable(e)) => {
                        // The target is not listening (green still binding,
                        // or a predecessor already gone). Wait for the mode to
                        // move on, briefly, then try again.
                        if remaining.is_zero() {
                            warn!(host = %host, error = %e, "Relay target unreachable; giving up on the request");
                            return (StatusCode::SERVICE_UNAVAILABLE, "scheduler handover in progress").into_response();
                        }
                        let _ = gate.changed_from(&mode, Duration::from_millis(250).min(remaining)).await;
                    }
                    Err(RelayError::Failed(e)) => {
                        return (StatusCode::BAD_GATEWAY, format!("relay: {e}")).into_response();
                    }
                }
            }
        }
    }
}

fn take_request(
    req: &mut Option<Request>,
    buffered: &mut Option<(axum::http::request::Parts, axum::body::Bytes)>,
) -> Request {
    if let Some(req) = req.take() {
        return req;
    }
    let (parts, bytes) = buffered.take().expect("request is either live or buffered");
    Request::from_parts(parts, Body::from(bytes))
}

fn was_relayed(req: &Option<Request>, buffered: &Option<(axum::http::request::Parts, axum::body::Bytes)>) -> bool {
    let headers = match (req, buffered) {
        (Some(req), _) => req.headers(),
        (None, Some((parts, _))) => &parts.headers,
        (None, None) => return false,
    };
    headers.contains_key(RELAYED_HEADER)
}

fn held_too_long() -> Response {
    (StatusCode::SERVICE_UNAVAILABLE, "scheduler handover did not complete in time").into_response()
}

enum RelayError {
    /// Nothing answered at the target: safe to retry, the request never left.
    Unreachable(String),
    /// The request may have reached the target; never retried.
    Failed(String),
}

/// Hop-by-hop headers (RFC 9110 7.6.1) plus `host`, which must name the
/// target, not this process.
fn hop_by_hop(name: &str) -> bool {
    matches!(
        name,
        "connection"
            | "keep-alive"
            | "proxy-authenticate"
            | "proxy-authorization"
            | "te"
            | "trailer"
            | "transfer-encoding"
            | "upgrade"
            | "host"
            | "content-length"
    )
}

async fn relay(
    gate: &'static Gate,
    host: &str,
    port: u16,
    parts: &axum::http::request::Parts,
    body: axum::body::Bytes,
) -> Result<Response, RelayError> {
    let path = parts.uri.path_and_query().map(|p| p.as_str()).unwrap_or("/");
    let url = format!("http://{host}:{port}{path}");
    let mut headers = reqwest::header::HeaderMap::new();
    for (name, value) in parts.headers.iter() {
        if !hop_by_hop(name.as_str()) {
            headers.append(name.clone(), value.clone());
        }
    }
    headers.insert(RELAYED_HEADER, HeaderValue::from_static("1"));
    let tracked = Tracked::new(&gate.relaying);
    let resp = gate
        .client
        .request(parts.method.clone(), &url)
        .headers(headers)
        .body(body)
        .send()
        .await
        .map_err(|e| {
            if e.is_connect() {
                RelayError::Unreachable(e.to_string())
            } else {
                RelayError::Failed(e.to_string())
            }
        })?;
    let status = resp.status();
    let mut out_headers = HeaderMap::new();
    for (name, value) in resp.headers().iter() {
        if !hop_by_hop(name.as_str()) {
            out_headers.append(name.clone(), value.clone());
        }
    }
    out_headers.insert("x-sp00ky-relayed-to", HeaderValue::from_str(host).unwrap_or(HeaderValue::from_static("peer")));
    // The relay counts as in progress until its body is fully passed on.
    let stream = futures::StreamExt::map(resp.bytes_stream(), move |chunk| {
        let _keep = &tracked;
        chunk
    });
    let mut response = Response::new(Body::from_stream(stream));
    *response.status_mut() = status;
    *response.headers_mut() = out_headers;
    Ok(response)
}

// ---------------------------------------------------------------------------
// Background writers
// ---------------------------------------------------------------------------

static INGEST_GATE: OnceLock<tokio::sync::RwLock<()>> = OnceLock::new();

/// Held for reading by every event taken in (`ingest::ingest_event_from`, from
/// seq assignment to the fan-out hand-off). The handover takes it for writing
/// and never gives it back: once it holds it, no event is mid-way and none
/// can start on this process again.
pub fn ingest_gate() -> &'static tokio::sync::RwLock<()> {
    INGEST_GATE.get_or_init(|| tokio::sync::RwLock::new(()))
}

static SINGLETONS: Mutex<Vec<(&'static str, tokio::task::AbortHandle)>> = Mutex::new(Vec::new());

/// Spawn a background loop that only the serving scheduler may run: anything
/// that writes upstream, writes the replica or acts on the SSP pool on a
/// timer. A blue that hands over aborts them all before it closes its replica
/// (see [`abort_singletons`]), so blue and green never run them at once.
pub fn spawn_singleton<F>(name: &'static str, fut: F) -> tokio::task::JoinHandle<F::Output>
where
    F: std::future::Future + Send + 'static,
    F::Output: Send + 'static,
{
    let handle = tokio::spawn(fut);
    let mut all = SINGLETONS.lock().unwrap_or_else(|e| e.into_inner());
    all.retain(|(_, h)| !h.is_finished());
    all.push((name, handle.abort_handle()));
    handle
}

/// Abort every [`spawn_singleton`] task except the named ones, which stay
/// registered for a later [`abort_singletons`]. Returns how many it stopped.
pub fn abort_singletons_except(keep: &[&str]) -> usize {
    let mut all = SINGLETONS.lock().unwrap_or_else(|e| e.into_inner());
    let mut running = 0;
    all.retain(|(name, handle)| {
        if keep.contains(name) {
            return !handle.is_finished();
        }
        if !handle.is_finished() {
            running += 1;
            tracing::debug!(task = *name, "Stopping background task for the handover");
            handle.abort();
        }
        false
    });
    running
}

/// Abort every [`spawn_singleton`] task. Returns how many were running.
pub fn abort_singletons() -> usize {
    let all = std::mem::take(&mut *SINGLETONS.lock().unwrap_or_else(|e| e.into_inner()));
    let mut running = 0;
    for (name, handle) in all {
        if !handle.is_finished() {
            running += 1;
            tracing::debug!(task = name, "Stopping background task for the handover");
            handle.abort();
        }
    }
    running
}

// ---------------------------------------------------------------------------
// Wire types
// ---------------------------------------------------------------------------

/// `POST /handover/prepare`, green to blue.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PrepareRequest {
    /// Host blue relays to afterwards (green's container name or address).
    pub successor: String,
    /// Green's version, for the logs.
    #[serde(default)]
    pub version: String,
    /// Green's ports, when they differ from blue's.
    #[serde(default)]
    pub ports: RelayPorts,
}

/// Everything a scheduler holds only in memory that its successor needs to
/// carry on without any SSP noticing.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct HandoverState {
    /// Bumped on any incompatible change. A successor that does not know the
    /// format boots without the state (SSPs then re-register in place).
    pub format: u32,
    pub from_version: String,
    pub ssps: Vec<crate::router::SspSnapshot>,
    /// `(query id, ssp id, registered at ms)`.
    pub queries: Vec<(String, String, u64)>,
}

pub const STATE_FORMAT: u32 = 1;

/// How long a predecessor that released its replica waits for the commit
/// before it gives up and exits (see `Scheduler::release_replica`).
pub const COMMIT_DEADLINE: Duration = Duration::from_secs(120);

/// Why blue declined a prepare.
#[derive(Debug)]
pub enum Refusal {
    /// Not now (an SSP bootstrapping, a backup running, a promotion): try
    /// again shortly.
    Busy(String),
    /// Never from this process (already handed over).
    Gone(String),
}

// ---------------------------------------------------------------------------
// Green side
// ---------------------------------------------------------------------------

/// How the predecessor answered.
pub enum TakeOver {
    /// It handed over: its replica is closed and it relays to us.
    Granted(HandoverState),
    /// It cannot hand over (an older version, no shared secret): relay to it
    /// until the control plane stops it, then boot the ordinary way.
    Unsupported,
    /// Nothing answers at the predecessor's address: it is gone already.
    Unreachable,
    /// The predecessor handed over (its replica is free or about to be) but
    /// its state never arrived: boot on our own, SSPs re-register in place.
    Released,
}

/// How long green keeps asking a busy predecessor before it gives up.
const BUSY_RETRY_MAX: Duration = Duration::from_secs(180);

/// Ask the predecessor at `from` (a base URL, `http://host:port`) to hand
/// over. Retries while it is busy.
pub async fn take_over(from: &str, successor: &str, ports: RelayPorts, auth_secret: Option<&str>) -> TakeOver {
    let gate = gate();
    gate.set_peer(Some(from.to_string()));
    let Some(secret) = auth_secret else {
        warn!("SPKY_AUTH_SECRET unset; cannot authenticate a handover, waiting for the predecessor to stop instead");
        return TakeOver::Unsupported;
    };
    let url = format!("{}/handover/prepare", from.trim_end_matches('/'));
    let body = PrepareRequest {
        successor: successor.to_string(),
        version: env!("CARGO_PKG_VERSION").to_string(),
        ports,
    };
    let started = Instant::now();
    loop {
        gate.set_status("starting", "holding");
        let attempt = gate
            .client
            .post(&url)
            .bearer_auth(secret)
            .json(&body)
            // Blue holds its traffic for the whole prepare; it is a few
            // seconds at most, this only bounds a wedged predecessor.
            .timeout(Duration::from_secs(60))
            .send()
            .await;
        match attempt {
            Ok(resp) if resp.status().is_success() => match resp.json::<HandoverState>().await {
                Ok(state) => {
                    info!(
                        from = %from,
                        from_version = %state.from_version,
                        ssps = state.ssps.len(),
                        queries = state.queries.len(),
                        waited_ms = started.elapsed().as_millis() as u64,
                        "Predecessor handed over"
                    );
                    return TakeOver::Granted(state);
                }
                Err(e) => {
                    // It handed over (its replica is closed) but the state did
                    // not arrive intact. Boot without it: SSPs re-register in
                    // place on their next heartbeat.
                    warn!(error = %e, "Predecessor handed over but its state did not parse; booting without it");
                    return TakeOver::Granted(HandoverState::default());
                }
            },
            Ok(resp) if resp.status() == StatusCode::CONFLICT => {
                let reason = resp.text().await.unwrap_or_default();
                if reason.contains("\"gone\"") {
                    warn!(reason = %reason, "Predecessor already handed over to someone else; booting on our own");
                    return TakeOver::Unreachable;
                }
                if started.elapsed() > BUSY_RETRY_MAX {
                    warn!(reason = %reason, "Predecessor stayed busy; waiting for it to stop instead");
                    return TakeOver::Unsupported;
                }
                info!(reason = %reason, "Predecessor busy; asking again shortly");
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
            Ok(resp) => {
                warn!(status = %resp.status(), "Predecessor cannot hand over; relaying to it until it stops");
                return TakeOver::Unsupported;
            }
            Err(e) if e.is_connect() => {
                info!(error = %e, "Predecessor unreachable; taking the replica directly");
                return TakeOver::Unreachable;
            }
            Err(e) => {
                // A timeout or a reset mid-prepare: the predecessor may or may
                // not have handed over. Ask it, rather than guess: relaying to
                // a predecessor that already relays to us would hold every
                // request until we serve, and holding while it still serves
                // would hold half of them for nothing.
                warn!(error = %e, "Prepare request failed; asking the predecessor where it stands");
                return settle_ambiguous(from).await;
            }
        }
    }
}

/// How the predecessor answered a two-step release.
pub enum Release {
    /// It released its replica and serves on until we commit.
    Released,
    /// It predates the two-step protocol (404): use the one-step prepare.
    OneStep,
    /// Anything [`take_over`] would have concluded instead.
    Other(TakeOver),
}

/// Step one of the two-step handover: ask the predecessor to release its
/// replica while it keeps serving. Retries while it is busy.
pub async fn release(from: &str, successor: &str, ports: RelayPorts, auth_secret: Option<&str>) -> Release {
    let gate = gate();
    gate.set_peer(Some(from.to_string()));
    let Some(secret) = auth_secret else {
        warn!("SPKY_AUTH_SECRET unset; cannot authenticate a handover, waiting for the predecessor to stop instead");
        return Release::Other(TakeOver::Unsupported);
    };
    let url = format!("{}/handover/release", from.trim_end_matches('/'));
    let body = PrepareRequest { successor: successor.to_string(), version: env!("CARGO_PKG_VERSION").to_string(), ports };
    let started = Instant::now();
    loop {
        let attempt = gate.client.post(&url).bearer_auth(secret).json(&body).timeout(Duration::from_secs(60)).send().await;
        match attempt {
            Ok(resp) if resp.status().is_success() => {
                info!(from = %from, waited_ms = started.elapsed().as_millis() as u64, "Predecessor released its replica");
                return Release::Released;
            }
            Ok(resp) if resp.status() == StatusCode::NOT_FOUND => return Release::OneStep,
            Ok(resp) if resp.status() == StatusCode::CONFLICT => {
                let reason = resp.text().await.unwrap_or_default();
                if reason.contains("\"gone\"") {
                    warn!(reason = %reason, "Predecessor already handed over to someone else; booting on our own");
                    return Release::Other(TakeOver::Unreachable);
                }
                if started.elapsed() > BUSY_RETRY_MAX {
                    warn!(reason = %reason, "Predecessor stayed busy; waiting for it to stop instead");
                    return Release::Other(TakeOver::Unsupported);
                }
                info!(reason = %reason, "Predecessor busy; asking again shortly");
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
            Ok(resp) => {
                warn!(status = %resp.status(), "Predecessor cannot hand over; relaying to it until it stops");
                return Release::Other(TakeOver::Unsupported);
            }
            Err(e) if e.is_connect() => {
                info!(error = %e, "Predecessor unreachable; taking the replica directly");
                return Release::Other(TakeOver::Unreachable);
            }
            Err(e) => {
                warn!(error = %e, "Release request failed; asking the predecessor where it stands");
                return Release::Other(settle_ambiguous(from).await);
            }
        }
    }
}

/// Step two: the predecessor holds, exports and starts relaying to us.
/// Retried for a while: a predecessor that released and never sees a commit
/// exits after [`COMMIT_DEADLINE`]. `None` when no state could be had; the
/// caller then boots without it (SSPs re-register in place).
pub async fn commit(from: &str, successor: &str, ports: RelayPorts, auth_secret: Option<&str>) -> Option<HandoverState> {
    let secret = auth_secret?;
    let url = format!("{}/handover/commit", from.trim_end_matches('/'));
    let body = PrepareRequest { successor: successor.to_string(), version: env!("CARGO_PKG_VERSION").to_string(), ports };
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let attempt = gate().client.post(&url).bearer_auth(secret).json(&body).timeout(Duration::from_secs(30)).send().await;
        match attempt {
            Ok(resp) if resp.status().is_success() => match resp.json::<HandoverState>().await {
                Ok(state) => {
                    info!(from = %from, ssps = state.ssps.len(), queries = state.queries.len(), "Predecessor committed the handover");
                    return Some(state);
                }
                Err(e) => {
                    warn!(error = %e, "Predecessor committed but its state did not parse; booting without it");
                    return Some(HandoverState::default());
                }
            },
            Ok(resp) if resp.status() == StatusCode::CONFLICT => {
                let reason = resp.text().await.unwrap_or_default();
                if reason.contains("\"gone\"") {
                    // Committed already (an answer we lost): it relays to us.
                    warn!(reason = %reason, "Predecessor handed over already; booting without its state");
                    return None;
                }
                warn!(reason = %reason, "Commit refused");
            }
            Ok(resp) => warn!(status = %resp.status(), "Commit failed"),
            Err(e) if e.is_connect() => {
                warn!(error = %e, "Predecessor gone before the commit; booting without its state");
                return None;
            }
            Err(e) => warn!(error = %e, "Commit request failed; asking again"),
        }
        if Instant::now() >= deadline {
            warn!("No commit after 60 s; booting without the predecessor's state");
            return None;
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

/// After a prepare whose answer was lost: read the predecessor's own status.
async fn settle_ambiguous(from: &str) -> TakeOver {
    let url = format!("{}/handover/status", from.trim_end_matches('/'));
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        match gate().client.get(&url).timeout(Duration::from_secs(5)).send().await {
            Ok(resp) => match resp.json::<StatusReport>().await {
                Ok(status) if status.role == "retired" => return TakeOver::Released,
                // Still mid-handover: it finishes on its own (the work is
                // detached from our request), so look again shortly.
                Ok(status) if status.phase == "handing_over" && Instant::now() < deadline => {
                    tokio::time::sleep(Duration::from_millis(250)).await;
                }
                _ => return TakeOver::Unsupported,
            },
            Err(e) if e.is_connect() => return TakeOver::Unreachable,
            Err(_) if Instant::now() < deadline => tokio::time::sleep(Duration::from_secs(1)).await,
            Err(_) => return TakeOver::Unsupported,
        }
    }
}

/// The address green advertises to blue: `SPKY_HANDOVER_ADVERTISE`, else this
/// container's hostname (Docker resolves it on the project network).
pub fn advertise_host() -> String {
    if let Ok(v) = std::env::var("SPKY_HANDOVER_ADVERTISE") {
        if !v.trim().is_empty() {
            return v.trim().to_string();
        }
    }
    std::env::var("HOSTNAME")
        .ok()
        .filter(|h| !h.is_empty())
        .or_else(|| std::fs::read_to_string("/etc/hostname").ok().map(|h| h.trim().to_string()))
        .filter(|h| !h.is_empty())
        .unwrap_or_else(|| "localhost".to_string())
}

/// Port part of a base URL, if it names one.
pub fn port_of(base: &str) -> Option<u16> {
    let rest = base.split("://").nth(1).unwrap_or(base);
    let authority = rest.split('/').next().unwrap_or(rest);
    authority.rsplit_once(':').and_then(|(_, port)| port.parse().ok())
}

/// Host part of a base URL (`http://host:port` -> `host`).
pub fn host_of(base: &str) -> String {
    let rest = base.split("://").nth(1).unwrap_or(base);
    let authority = rest.split('/').next().unwrap_or(rest);
    match authority.rsplit_once(':') {
        Some((host, port)) if port.chars().all(|c| c.is_ascii_digit()) => host.to_string(),
        _ => authority.to_string(),
    }
}

/// Whether an error from opening the replica is the RocksDB lock another
/// process (or a predecessor still closing) holds.
pub fn is_lock_error(e: &anyhow::Error) -> bool {
    let text = format!("{e:#}");
    text.contains("/LOCK") || text.contains("lock hold") || text.contains("No locks available")
        || text.contains("Resource temporarily unavailable")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn host_of_strips_scheme_and_port() {
        assert_eq!(port_of("http://spooky-x-scheduler-abc:9667"), Some(9667));
        assert_eq!(port_of("http://scheduler"), None);
        assert_eq!(host_of("http://spooky-x-scheduler-abc:9667"), "spooky-x-scheduler-abc");
        assert_eq!(host_of("http://10.0.0.5:9667/"), "10.0.0.5");
        assert_eq!(host_of("scheduler"), "scheduler");
    }

    #[test]
    fn lock_errors_are_recognised() {
        let e = anyhow::anyhow!("IO error: lock hold by current process: /data/replica/LOCK: No locks available");
        assert!(is_lock_error(&e));
        assert!(!is_lock_error(&anyhow::anyhow!("permission denied")));
    }

    #[test]
    fn hop_by_hop_headers_are_dropped() {
        assert!(hop_by_hop("connection"));
        assert!(hop_by_hop("host"));
        assert!(!hop_by_hop("authorization"));
        assert!(!hop_by_hop("content-type"));
    }
}

// ---------------------------------------------------------------------------
// Routes on the main port
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub struct RouteDeps {
    pub scheduler: Arc<crate::Scheduler>,
    pub query_tracker: Arc<crate::query::QueryTracker>,
    pub backup_restore_lock: Arc<tokio::sync::Mutex<()>>,
    pub auth_secret: Option<String>,
}

/// `POST /handover/prepare` (bearer `SPKY_AUTH_SECRET`) and
/// `GET /handover/ssps`. `/handover/status` is answered by the gate itself.
pub fn routes(deps: RouteDeps) -> Router {
    use axum::routing::{get, post};
    Router::new()
        .route("/handover/prepare", post(prepare))
        .route("/handover/release", post(release_route))
        .route("/handover/commit", post(commit_route))
        .route("/handover/ssps", get(ssps))
        .with_state(deps)
}

fn authorized(deps: &RouteDeps, headers: &HeaderMap) -> Option<Response> {
    let Some(secret) = deps.auth_secret.as_deref() else {
        return Some((StatusCode::FORBIDDEN, "handover needs SPKY_AUTH_SECRET").into_response());
    };
    let presented = headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "));
    (presented != Some(secret)).then(|| StatusCode::UNAUTHORIZED.into_response())
}

fn refusal(r: Refusal) -> Response {
    match r {
        Refusal::Busy(reason) => {
            info!(reason = %reason, "Handover step refused for now");
            (StatusCode::CONFLICT, axum::Json(serde_json::json!({ "code": "busy", "reason": reason }))).into_response()
        }
        Refusal::Gone(reason) => {
            (StatusCode::CONFLICT, axum::Json(serde_json::json!({ "code": "gone", "reason": reason }))).into_response()
        }
    }
}

/// `POST /handover/release`: step one of the two-step handover. Detached for
/// the same reason as `prepare`.
async fn release_route(
    axum::extract::State(deps): axum::extract::State<RouteDeps>,
    headers: HeaderMap,
    axum::Json(req): axum::Json<PrepareRequest>,
) -> Response {
    if let Some(denied) = authorized(&deps, &headers) {
        return denied;
    }
    info!(successor = %req.successor, successor_version = %req.version, "Handover release requested");
    let task = tokio::spawn(async move {
        deps.scheduler.release_replica(req.successor.trim(), &deps.backup_restore_lock).await
    });
    match task.await {
        Ok(Ok(())) => axum::Json(serde_json::json!({ "released": true })).into_response(),
        Ok(Err(r)) => refusal(r),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, format!("release task failed: {e}")).into_response(),
    }
}

/// `POST /handover/commit`: step two, answers the handover state.
async fn commit_route(
    axum::extract::State(deps): axum::extract::State<RouteDeps>,
    headers: HeaderMap,
    axum::Json(req): axum::Json<PrepareRequest>,
) -> Response {
    if let Some(denied) = authorized(&deps, &headers) {
        return denied;
    }
    let task = tokio::spawn(async move {
        deps.scheduler
            .commit_handover(req.successor.trim(), req.ports, &deps.query_tracker)
            .await
    });
    match task.await {
        Ok(Ok(state)) => axum::Json(state).into_response(),
        Ok(Err(r)) => refusal(r),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, format!("commit task failed: {e}")).into_response(),
    }
}

async fn prepare(
    axum::extract::State(deps): axum::extract::State<RouteDeps>,
    headers: HeaderMap,
    axum::Json(req): axum::Json<PrepareRequest>,
) -> Response {
    if let Some(denied) = authorized(&deps, &headers) {
        return denied;
    }
    if req.successor.trim().is_empty() {
        return (StatusCode::BAD_REQUEST, "successor is required").into_response();
    }
    info!(successor = %req.successor, successor_version = %req.version, "Handover requested");
    // Detached: past its point of no return a handover must finish, and the
    // request's own deadline (or the successor giving up on it) would
    // otherwise drop it half done.
    let handing = tokio::spawn(async move {
        deps.scheduler
            .hand_over(req.successor.trim(), req.ports, &deps.query_tracker, &deps.backup_restore_lock)
            .await
    });
    let outcome = match handing.await {
        Ok(outcome) => outcome,
        Err(e) => {
            return (StatusCode::INTERNAL_SERVER_ERROR, format!("handover task failed: {e}")).into_response();
        }
    };
    match outcome {
        Ok(state) => axum::Json(state).into_response(),
        Err(r) => refusal(r),
    }
}

/// The SSP pool as the control plane watches it during an SSP blue/green swap.
async fn ssps(axum::extract::State(deps): axum::extract::State<RouteDeps>) -> Response {
    let pool = deps.scheduler.ssp_pool.read().await;
    let ssps: Vec<serde_json::Value> = pool
        .all()
        .iter()
        .map(|s| {
            let state = pool
                .get_state(&s.id)
                .and_then(|st| serde_json::to_value(st).ok())
                .unwrap_or(serde_json::Value::Null);
            serde_json::json!({
                "id": s.id,
                "url": s.url,
                "state": state,
                "standby": pool.is_standby(&s.id),
                "replaces": pool.standby_predecessor(&s.id),
                "version": s.version,
            })
        })
        .collect();
    drop(pool);
    axum::Json(serde_json::json!({
        "ssps": ssps,
        "promotions": crate::ssp_handover::shared().reports(),
    }))
    .into_response()
}
