//! The agent against a scripted scheduler and a real HTTP backend, in-process.
//!
//! The "backend process" the agent supervises is `sleep`; the HTTP surface it
//! would expose is served by the test on the port the scheduler advertises, so a
//! test can see exactly what the backend saw: which jobs started, which finished,
//! and which had their request dropped under them.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::routing::{get, post};
use axum::{Json, Router};
use pool_protocol::{
    Command, HelloReply, Outcome, PollReply, PollRequest, ResultReply, ResultRequest,
};
use serde_json::{json, Value};
use spky_agent::{run, AgentConfig, Exit};

const TOKEN: &str = "tok";
const MACHINE: &str = "_00_machine:test";

#[derive(Default)]
struct Sched {
    /// One entry per poll, in order; empty afterwards.
    script: Mutex<VecDeque<Vec<Command>>>,
    polls: Mutex<Vec<PollRequest>>,
    results: Mutex<Vec<ResultRequest>>,
    ready: AtomicUsize,
    down: AtomicBool,
    gone: AtomicBool,
    hello: Mutex<Option<HelloReply>>,
}

fn authorized(headers: &HeaderMap) -> bool {
    headers.get("authorization").and_then(|v| v.to_str().ok()) == Some(&format!("Bearer {TOKEN}"))
}

async fn hello(
    State(s): State<Arc<Sched>>,
    headers: HeaderMap,
) -> Result<Json<HelloReply>, StatusCode> {
    if !authorized(&headers) {
        return Err(StatusCode::UNAUTHORIZED);
    }
    if s.gone.load(Ordering::SeqCst) {
        return Err(StatusCode::GONE);
    }
    Ok(Json(
        s.hello.lock().unwrap().clone().expect("hello configured"),
    ))
}

async fn ready(State(s): State<Arc<Sched>>) -> StatusCode {
    s.ready.fetch_add(1, Ordering::SeqCst);
    StatusCode::NO_CONTENT
}

async fn poll(
    State(s): State<Arc<Sched>>,
    Json(req): Json<PollRequest>,
) -> Result<Json<PollReply>, StatusCode> {
    if s.down.load(Ordering::SeqCst) {
        return Err(StatusCode::SERVICE_UNAVAILABLE);
    }
    s.polls.lock().unwrap().push(req);
    let commands = s.script.lock().unwrap().pop_front().unwrap_or_default();
    if commands.is_empty() {
        tokio::time::sleep(Duration::from_millis(50)).await; // a (short) held poll
    }
    Ok(Json(PollReply { commands }))
}

async fn result(State(s): State<Arc<Sched>>, Json(req): Json<ResultRequest>) -> Json<ResultReply> {
    s.results.lock().unwrap().push(req);
    Json(ResultReply { accepted: true })
}

#[derive(Default)]
struct BackendSeen {
    started: AtomicUsize,
    finished: AtomicUsize,
    dropped: AtomicUsize,
}

/// Counts a request whose handler was dropped before it could answer, which is
/// what the backend observes when the agent abandons a job.
struct DropGuard(Arc<BackendSeen>, bool);
impl Drop for DropGuard {
    fn drop(&mut self) {
        if !self.1 {
            self.0.dropped.fetch_add(1, Ordering::SeqCst);
        }
    }
}

async fn work(
    State(seen): State<Arc<BackendSeen>>,
    Json(payload): Json<Value>,
) -> (StatusCode, String) {
    seen.started.fetch_add(1, Ordering::SeqCst);
    let mut guard = DropGuard(seen.clone(), false);
    tokio::time::sleep(Duration::from_millis(
        payload["sleep_ms"].as_u64().unwrap_or(0),
    ))
    .await;
    guard.1 = true;
    seen.finished.fetch_add(1, Ordering::SeqCst);
    let status = StatusCode::from_u16(payload["status"].as_u64().unwrap_or(200) as u16).unwrap();
    (status, json!({ "echo": payload["n"] }).to_string())
}

async fn serve(router: Router) -> u16 {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    port
}

struct World {
    sched: Arc<Sched>,
    backend: Arc<BackendSeen>,
    agent: tokio::task::JoinHandle<Exit>,
}

async fn world(lease_secs: u64, stop_margin_secs: u64, recycle: bool) -> World {
    let backend = Arc::new(BackendSeen::default());
    let backend_port = serve(
        Router::new()
            .route("/health", get(|| async { "ok" }))
            .route("/work", post(work))
            .with_state(backend.clone()),
    )
    .await;

    let sched = Arc::new(Sched::default());
    *sched.hello.lock().unwrap() = Some(HelloReply {
        port: backend_port,
        healthcheck: Some("/health".into()),
        env: Default::default(),
        poll_secs: 1,
        lease_secs,
        stop_margin_secs,
        recycle_per_job: recycle,
        orphan_secs: 3600,
    });
    let sched_port = serve(
        Router::new()
            .route("/pool/v1/hello", post(hello))
            .route("/pool/v1/ready", post(ready))
            .route("/pool/v1/poll", post(poll))
            .route("/pool/v1/result", post(result))
            .with_state(sched.clone()),
    )
    .await;

    let agent = tokio::spawn(run(AgentConfig {
        pool_url: format!("http://127.0.0.1:{sched_port}"),
        machine: MACHINE.into(),
        token: TOKEN.into(),
        command: vec!["sleep".into(), "3600".into()],
    }));
    World {
        sched,
        backend,
        agent,
    }
}

fn assign(job: &str, payload: Value, deadline_secs: u64) -> Command {
    Command::Assign {
        job: job.into(),
        epoch: 1,
        path: "/work".into(),
        payload,
        deadline_secs,
    }
}

async fn eventually(what: &str, mut check: impl FnMut() -> bool) {
    for _ in 0..400 {
        if check() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("timed out waiting for: {what}");
}

impl World {
    fn push(&self, commands: Vec<Command>) {
        self.sched.script.lock().unwrap().push_back(commands);
    }
    fn results(&self) -> Vec<ResultRequest> {
        self.sched.results.lock().unwrap().clone()
    }
    async fn shutdown(self) -> Exit {
        self.push(vec![Command::Shutdown]);
        tokio::time::timeout(Duration::from_secs(20), self.agent)
            .await
            .expect("agent exits")
            .unwrap()
    }
}

#[tokio::test]
async fn runs_an_assigned_job_on_loopback_and_reports_the_backends_answer() {
    let w = world(90, 30, false).await;
    eventually("ready", || w.sched.ready.load(Ordering::SeqCst) == 1).await;

    // Long enough that at least one heartbeat happens while it runs.
    w.push(vec![assign(
        "job:a",
        json!({ "n": 7, "sleep_ms": 900 }),
        60,
    )]);
    eventually("a result", || !w.results().is_empty()).await;

    let got = w.results().remove(0);
    assert_eq!(
        (got.machine.as_str(), got.job.as_str(), got.epoch),
        (MACHINE, "job:a", 1)
    );
    assert_eq!(
        got.outcome,
        Outcome::Success {
            body: json!({ "echo": 7 }).to_string()
        }
    );

    // While it ran, the poll told the scheduler so (that is what renews the lease).
    let reported = w
        .sched
        .polls
        .lock()
        .unwrap()
        .iter()
        .any(|p| p.running.iter().any(|r| r.job == "job:a"));
    assert!(
        reported,
        "a running attempt must be listed in the heartbeat"
    );
    assert_eq!(w.shutdown().await, Exit::Dismissed);
}

#[tokio::test]
async fn the_same_assignment_twice_runs_once() {
    let w = world(90, 30, false).await;
    let job = || assign("job:a", json!({ "sleep_ms": 400 }), 60);
    w.push(vec![job()]);
    w.push(vec![job()]); // a re-derived reply, because the first one was "lost"
    eventually("a result", || !w.results().is_empty()).await;
    assert_eq!(w.backend.started.load(Ordering::SeqCst), 1);
    w.shutdown().await;
}

#[tokio::test]
async fn a_backend_error_is_reported_with_its_status_for_the_retry_budget_to_judge() {
    let w = world(90, 30, false).await;
    w.push(vec![assign(
        "job:a",
        json!({ "status": 503, "n": "x" }),
        60,
    )]);
    eventually("a result", || !w.results().is_empty()).await;
    match w.results().remove(0).outcome {
        Outcome::Failed { code, reason } => {
            assert_eq!(code, json!(503));
            assert!(reason.contains("echo"));
        }
        other => panic!("expected a failure, got {other:?}"),
    }
    w.shutdown().await;
}

#[tokio::test]
async fn an_attempt_past_its_deadline_is_stopped_and_reported_as_such() {
    let w = world(90, 30, false).await;
    w.push(vec![assign("job:slow", json!({ "sleep_ms": 60_000 }), 1)]);
    eventually("a result", || !w.results().is_empty()).await;
    assert_eq!(w.results().remove(0).outcome, Outcome::DeadlineExceeded);
    eventually("the backend sees the request dropped", || {
        w.backend.dropped.load(Ordering::SeqCst) == 1
    })
    .await;
    w.shutdown().await;
}

#[tokio::test]
async fn cancel_stops_the_attempt_and_says_so() {
    let w = world(90, 30, false).await;
    w.push(vec![assign("job:long", json!({ "sleep_ms": 60_000 }), 600)]);
    eventually("the job starts", || {
        w.backend.started.load(Ordering::SeqCst) == 1
    })
    .await;

    w.push(vec![Command::Cancel {
        job: "job:long".into(),
        epoch: 1,
    }]);
    eventually("a result", || !w.results().is_empty()).await;
    assert_eq!(w.results().remove(0).outcome, Outcome::Cancelled);
    eventually("the backend sees the request dropped", || {
        w.backend.dropped.load(Ordering::SeqCst) == 1
    })
    .await;
    assert_eq!(w.backend.finished.load(Ordering::SeqCst), 0);
    w.shutdown().await;
}

/// The rule that makes double runs impossible: with the scheduler unreachable the
/// agent gives its jobs up at `lease - margin`, i.e. before anyone else may be
/// given them.
#[tokio::test]
async fn jobs_are_given_up_before_their_lease_can_expire_when_the_scheduler_goes_silent() {
    let w = world(4, 2, false).await; // gives up after 2s of silence; lease is 4s
    w.push(vec![assign("job:long", json!({ "sleep_ms": 60_000 }), 600)]);
    eventually("the job starts", || {
        w.backend.started.load(Ordering::SeqCst) == 1
    })
    .await;

    let went_silent = std::time::Instant::now();
    w.sched.down.store(true, Ordering::SeqCst);
    eventually("the agent stops the job on its own", || {
        w.backend.dropped.load(Ordering::SeqCst) == 1
    })
    .await;
    let took = went_silent.elapsed();
    assert!(
        took >= Duration::from_millis(1900),
        "not before the margin: {took:?}"
    );
    assert!(
        took < Duration::from_secs(4),
        "and strictly before the lease runs out: {took:?}"
    );

    // Contact returns: it no longer claims the job, so nothing renews a lease it gave up.
    w.sched.polls.lock().unwrap().clear();
    w.sched.down.store(false, Ordering::SeqCst);
    eventually("polls resume", || !w.sched.polls.lock().unwrap().is_empty()).await;
    assert!(w
        .sched
        .polls
        .lock()
        .unwrap()
        .iter()
        .all(|p| p.running.is_empty()));
    assert!(
        w.results().is_empty(),
        "an abandoned attempt reports nothing; its lease just expires"
    );
    w.shutdown().await;
}

#[tokio::test]
async fn a_machine_the_scheduler_does_not_want_exits_at_hello() {
    let sched = Arc::new(Sched::default());
    sched.gone.store(true, Ordering::SeqCst);
    let port = serve(
        Router::new()
            .route("/pool/v1/hello", post(hello))
            .with_state(sched),
    )
    .await;
    let exit = run(AgentConfig {
        pool_url: format!("http://127.0.0.1:{port}"),
        machine: MACHINE.into(),
        token: TOKEN.into(),
        command: vec!["sleep".into(), "3600".into()],
    })
    .await;
    assert_eq!(exit, Exit::Dismissed);
}

#[tokio::test]
async fn a_wrong_token_is_a_dismissal_not_a_retry_loop() {
    let sched = Arc::new(Sched::default());
    let port = serve(
        Router::new()
            .route("/pool/v1/hello", post(hello))
            .with_state(sched),
    )
    .await;
    let exit = run(AgentConfig {
        pool_url: format!("http://127.0.0.1:{port}"),
        machine: MACHINE.into(),
        token: "stolen".into(),
        command: vec!["sleep".into(), "3600".into()],
    })
    .await;
    assert_eq!(exit, Exit::Dismissed);
}
