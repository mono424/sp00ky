//! `spky-agent`: the process that turns a backend image into a pool machine.
//!
//! It is the container's (or the VM unit's) entrypoint and wraps the backend's
//! own command:
//!
//! ```text
//! spky-agent -- /renderer --port 8080
//! ```
//!
//! 1. `hello` to the scheduler: learn the backend's port, health path, extra
//!    environment and the lease parameters.
//! 2. Start the backend as a child, wait until it is healthy, report `ready`.
//! 3. Poll. A poll is the heartbeat, carries what is running, and brings back
//!    commands. `Assign` becomes `POST http://127.0.0.1:{port}{path}` held open
//!    until the backend answers; the outcome goes back as a `result`.
//!
//! The backend keeps the contract it always had with the job runner (handle a
//! POST, answer when done). What changes is that the held-open request now
//! crosses loopback instead of a network, and the part that does cross the
//! network is a short poll that renews a lease.
//!
//! **The safety rule.** The agent keeps its own clock on scheduler contact. Once
//! it has gone `lease - stop_margin` without a successful poll it stops every
//! job by itself. That is always BEFORE the scheduler may hand those jobs to
//! another machine, so a network partition can never leave two machines running
//! the same attempt. After `orphan_secs` without contact it exits altogether.

use std::collections::HashMap;
use std::process::Stdio;
use std::time::{Duration, Instant};

use pool_protocol::{
    Command, HelloReply, HelloRequest, JobRef, Outcome, PollReply, PollRequest, ReadyRequest,
    ResultRequest, ROUTE_PREFIX,
};
use reqwest::StatusCode;
use serde_json::{json, Value};
use tokio::process::{Child, Command as Process};
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;

pub const AGENT_VERSION: &str = env!("CARGO_PKG_VERSION");

/// Most of a backend's error body worth keeping on the job row.
const REASON_MAX_BYTES: usize = 2048;
/// How long a backend gets between SIGTERM and SIGKILL.
const STOP_GRACE: Duration = Duration::from_secs(10);

#[derive(Debug, Clone)]
pub struct AgentConfig {
    /// Base URL of the scheduler's pool listener.
    pub pool_url: String,
    pub machine: String,
    pub token: String,
    /// The backend's command line.
    pub command: Vec<String>,
}

impl AgentConfig {
    pub fn from_env(command: Vec<String>) -> Result<Self, String> {
        let var = |k: &str| {
            std::env::var(k)
                .ok()
                .filter(|v| !v.is_empty())
                .ok_or(format!("{k} is not set"))
        };
        Ok(Self {
            pool_url: var("SPKY_POOL_URL")?.trim_end_matches('/').to_string(),
            machine: var("SPKY_MACHINE_ID")?,
            token: var("SPKY_MACHINE_TOKEN")?,
            command,
        })
    }
}

/// Why the agent stopped. Every variant is a clean exit: whoever supervises the
/// machine decides what happens to it next.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Exit {
    /// The scheduler said so (`Shutdown`, or the machine is unknown to it).
    Dismissed,
    /// No scheduler contact for `orphan_secs`: the dead-man switch.
    Orphaned,
    /// SIGTERM / SIGINT.
    Signalled,
}

enum Call<T> {
    Ok(T),
    /// 401 / 410: this machine is not wanted. Stop.
    Dismissed,
    /// Anything else: try again.
    Retry(String),
}

struct Scheduler {
    http: reqwest::Client,
    cfg: AgentConfig,
}

impl Scheduler {
    async fn post<B: serde::Serialize, T: serde::de::DeserializeOwned>(
        &self,
        path: &str,
        body: &B,
        timeout: Duration,
    ) -> Call<T> {
        let sent = self
            .http
            .post(format!("{}{ROUTE_PREFIX}{path}", self.cfg.pool_url))
            .bearer_auth(&self.cfg.token)
            .timeout(timeout)
            .json(body)
            .send()
            .await;
        let resp = match sent {
            Ok(resp) => resp,
            Err(e) => return Call::Retry(e.to_string()),
        };
        match resp.status() {
            StatusCode::UNAUTHORIZED | StatusCode::GONE => Call::Dismissed,
            s if s.is_success() => {
                let text = resp.text().await.unwrap_or_default();
                let text = if text.trim().is_empty() {
                    "null"
                } else {
                    text.as_str()
                };
                match serde_json::from_str(text) {
                    Ok(v) => Call::Ok(v),
                    Err(e) => Call::Retry(format!("unreadable reply: {e}")),
                }
            }
            s => Call::Retry(format!("scheduler answered {s}")),
        }
    }
}

/// The supervised backend process.
struct Backend {
    command: Vec<String>,
    env: Vec<(String, String)>,
    port: u16,
    healthcheck: Option<String>,
    child: Option<Child>,
    http: reqwest::Client,
    /// True while the backend answers its health probe. Jobs wait on this.
    healthy: watch::Sender<bool>,
}

impl Backend {
    fn spawn(&mut self) -> Result<(), String> {
        let (program, args) = self.command.split_first().ok_or("empty backend command")?;
        let child = Process::new(program)
            .args(args)
            .envs(self.env.iter().cloned())
            .stdin(Stdio::null())
            .kill_on_drop(true)
            .spawn()
            .map_err(|e| format!("could not start `{program}`: {e}"))?;
        self.child = Some(child);
        Ok(())
    }

    async fn probe(&self) -> bool {
        match &self.healthcheck {
            Some(path) => self
                .http
                .get(format!("http://127.0.0.1:{}{path}", self.port))
                .timeout(Duration::from_secs(2))
                .send()
                .await
                .map(|r| r.status().is_success())
                .unwrap_or(false),
            None => tokio::net::TcpStream::connect(("127.0.0.1", self.port))
                .await
                .is_ok(),
        }
    }

    /// SIGTERM, a grace period, then SIGKILL.
    async fn stop(&mut self) {
        let _ = self.healthy.send(false);
        let Some(mut child) = self.child.take() else {
            return;
        };
        if let Some(pid) = child.id() {
            // SAFETY: plain syscall on a pid we own; a stale pid just yields ESRCH.
            unsafe { libc::kill(pid as i32, libc::SIGTERM) };
        }
        if tokio::time::timeout(STOP_GRACE, child.wait())
            .await
            .is_err()
        {
            let _ = child.kill().await;
        }
    }

    /// (Re)start and wait until healthy. Restarts a backend that dies while
    /// booting; how long that may go on is the pool's boot timeout, enforced by
    /// the scheduler, not here.
    async fn start_until_healthy(&mut self) {
        loop {
            self.stop().await;
            if let Err(e) = self.spawn() {
                eprintln!("[spky-agent] {e}");
                tokio::time::sleep(Duration::from_secs(2)).await;
                continue;
            }
            loop {
                if self.probe().await {
                    let _ = self.healthy.send(true);
                    return;
                }
                if let Some(Ok(Some(status))) = self.child.as_mut().map(|c| c.try_wait()) {
                    eprintln!("[spky-agent] backend exited while starting ({status}), restarting");
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    break;
                }
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
        }
    }
}

/// Run one attempt against the backend on loopback.
async fn run_job(
    http: reqwest::Client,
    port: u16,
    path: String,
    payload: Value,
    deadline: Duration,
    mut healthy: watch::Receiver<bool>,
) -> Outcome {
    let attempt = async {
        // A job assigned while the backend is being recycled waits for it.
        while !*healthy.borrow() {
            if healthy.changed().await.is_err() {
                return Outcome::Failed {
                    code: json!(0),
                    reason: "the agent is shutting down".into(),
                };
            }
        }
        let sent = http
            .post(format!("http://127.0.0.1:{port}{path}"))
            .json(&payload)
            .send()
            .await;
        match sent {
            Err(e) => Outcome::Failed {
                code: json!(0),
                reason: e.to_string(),
            },
            Ok(resp) => {
                let status = resp.status();
                let mut body = resp.text().await.unwrap_or_default();
                if status.is_success() {
                    Outcome::Success { body }
                } else {
                    if body.len() > REASON_MAX_BYTES {
                        let mut cut = REASON_MAX_BYTES;
                        while !body.is_char_boundary(cut) {
                            cut -= 1;
                        }
                        body.truncate(cut);
                    }
                    Outcome::Failed {
                        code: json!(status.as_u16()),
                        reason: body,
                    }
                }
            }
        }
    };
    tokio::time::timeout(deadline, attempt)
        .await
        .unwrap_or(Outcome::DeadlineExceeded)
}

pub async fn run(cfg: AgentConfig) -> Exit {
    // Loopback and scheduler clients are separate: jobs must never go through a
    // proxy, and must have no client-side timeout besides their deadline.
    let local = reqwest::Client::builder()
        .no_proxy()
        .build()
        .expect("http client");
    let sched = Scheduler {
        http: reqwest::Client::new(),
        cfg: cfg.clone(),
    };
    let mut signals = Signals::new();

    // -- hello
    let hello_req = HelloRequest {
        machine: cfg.machine.clone(),
        agent_version: AGENT_VERSION.into(),
    };
    let started = Instant::now();
    let hello: HelloReply = loop {
        tokio::select! {
            _ = signals.recv() => return Exit::Signalled,
            call = sched.post("/hello", &hello_req, Duration::from_secs(15)) => match call {
                Call::Ok(hello) => break hello,
                Call::Dismissed => return Exit::Dismissed,
                Call::Retry(why) => {
                    eprintln!("[spky-agent] hello failed: {why}");
                    // Nothing to fall back on before the first hello: give up
                    // after a while rather than sit on a paid machine forever.
                    if started.elapsed() > Duration::from_secs(1800) {
                        return Exit::Orphaned;
                    }
                    tokio::time::sleep(Duration::from_secs(2)).await;
                }
            }
        }
    };
    let lease = Duration::from_secs(hello.lease_secs.max(1));
    let give_up_after = lease.saturating_sub(Duration::from_secs(hello.stop_margin_secs));
    let orphan_after = Duration::from_secs(hello.orphan_secs.max(hello.lease_secs));
    let poll_pause = Duration::from_millis(200);

    // -- backend
    let (healthy_tx, healthy_rx) = watch::channel(false);
    let mut backend = Backend {
        command: cfg.command.clone(),
        env: hello.env.clone().into_iter().collect(),
        port: hello.port,
        healthcheck: hello.healthcheck.clone(),
        child: None,
        http: local.clone(),
        healthy: healthy_tx,
    };
    tokio::select! {
        _ = signals.recv() => { backend.stop().await; return Exit::Signalled; }
        _ = backend.start_until_healthy() => {}
    }

    // -- ready
    let ready_req = ReadyRequest {
        machine: cfg.machine.clone(),
    };
    loop {
        match sched
            .post::<_, Value>("/ready", &ready_req, Duration::from_secs(15))
            .await
        {
            Call::Ok(_) => break,
            Call::Dismissed => {
                backend.stop().await;
                return Exit::Dismissed;
            }
            Call::Retry(why) => {
                eprintln!("[spky-agent] ready failed: {why}");
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
        }
    }
    eprintln!(
        "[spky-agent] {} ready, backend on :{}",
        cfg.machine, hello.port
    );

    // -- poll loop
    let (done_tx, mut done_rx) = mpsc::unbounded_channel::<(JobRef, Outcome)>();
    let mut running: HashMap<JobRef, JoinHandle<()>> = HashMap::new();
    let mut last_contact = Instant::now();

    let exit = loop {
        let poll_req = PollRequest {
            machine: cfg.machine.clone(),
            running: running.keys().cloned().collect(),
            stats: Some(json!({ "agent": AGENT_VERSION, "running": running.len() })),
        };
        // Held open by the scheduler for up to ~10s; the client allows a bit more.
        let poll = sched.post::<_, PollReply>("/poll", &poll_req, Duration::from_secs(30));

        tokio::select! {
            _ = signals.recv() => break Exit::Signalled,

            Some((attempt, outcome)) = done_rx.recv() => {
                if running.remove(&attempt).is_none() {
                    continue; // cancelled meanwhile: its outcome no longer counts
                }
                report(&sched, &cfg.machine, &attempt, outcome, give_up_after).await;
                if hello.recycle_per_job && running.is_empty() {
                    backend.start_until_healthy().await;
                }
            }

            call = poll => match call {
                Call::Dismissed => break Exit::Dismissed,
                Call::Retry(why) => {
                    let silent = last_contact.elapsed();
                    if silent > orphan_after {
                        eprintln!("[spky-agent] no scheduler for {silent:?}: giving the machine up");
                        break Exit::Orphaned;
                    }
                    if silent > give_up_after && !running.is_empty() {
                        // The lease is about to run out and nobody can renew it:
                        // stop now, so that whoever gets these jobs next is alone.
                        eprintln!("[spky-agent] no scheduler for {silent:?} ({why}): stopping {} job(s)", running.len());
                        for (_, task) in running.drain() {
                            task.abort();
                        }
                        backend.start_until_healthy().await;
                    }
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
                Call::Ok(reply) => {
                    last_contact = Instant::now();
                    let mut shutdown = false;
                    for command in reply.commands {
                        match command {
                            Command::Assign { job, epoch, path, payload, deadline_secs } => {
                                let attempt = JobRef { job, epoch };
                                if running.contains_key(&attempt) {
                                    continue;
                                }
                                let work = run_job(
                                    local.clone(), hello.port, path, payload,
                                    Duration::from_secs(deadline_secs.max(1)), healthy_rx.clone(),
                                );
                                let (tx, key) = (done_tx.clone(), attempt.clone());
                                running.insert(attempt, tokio::spawn(async move {
                                    let _ = tx.send((key, work.await));
                                }));
                            }
                            Command::Cancel { job, epoch } => {
                                let attempt = JobRef { job, epoch };
                                if let Some(task) = running.remove(&attempt) {
                                    task.abort();
                                    report(&sched, &cfg.machine, &attempt, Outcome::Cancelled, give_up_after).await;
                                    // Dropping the request is only a hint to the
                                    // backend. When this was its only job, restart
                                    // it so the work has really stopped.
                                    if running.is_empty() {
                                        backend.start_until_healthy().await;
                                    }
                                }
                            }
                            Command::Shutdown => shutdown = true,
                        }
                    }
                    if shutdown {
                        break Exit::Dismissed;
                    }
                    tokio::time::sleep(poll_pause).await;
                }
            },
        }
    };

    for (_, task) in running.drain() {
        task.abort();
    }
    backend.stop().await;
    exit
}

/// Deliver an outcome. Retried for as long as the attempt can still own its job;
/// past that the scheduler would refuse it anyway.
async fn report(
    sched: &Scheduler,
    machine: &str,
    attempt: &JobRef,
    outcome: Outcome,
    window: Duration,
) {
    let req = ResultRequest {
        machine: machine.to_string(),
        job: attempt.job.clone(),
        epoch: attempt.epoch,
        outcome,
    };
    let started = Instant::now();
    loop {
        match sched
            .post::<_, Value>("/result", &req, Duration::from_secs(15))
            .await
        {
            Call::Ok(_) | Call::Dismissed => return,
            Call::Retry(why) if started.elapsed() > window => {
                eprintln!(
                    "[spky-agent] could not report {} ({why}); its lease will expire",
                    attempt.job
                );
                return;
            }
            Call::Retry(_) => tokio::time::sleep(Duration::from_secs(1)).await,
        }
    }
}

/// SIGTERM + SIGINT as one stream.
struct Signals {
    #[cfg(unix)]
    term: tokio::signal::unix::Signal,
    #[cfg(unix)]
    int: tokio::signal::unix::Signal,
}

impl Signals {
    fn new() -> Self {
        #[cfg(unix)]
        {
            use tokio::signal::unix::{signal, SignalKind};
            Self {
                term: signal(SignalKind::terminate()).expect("SIGTERM handler"),
                int: signal(SignalKind::interrupt()).expect("SIGINT handler"),
            }
        }
        #[cfg(not(unix))]
        Self {}
    }

    async fn recv(&mut self) {
        #[cfg(unix)]
        tokio::select! {
            _ = self.term.recv() => {}
            _ = self.int.recv() => {}
        }
        #[cfg(not(unix))]
        std::future::pending::<()>().await
    }
}

/// `spky-agent --install <dest>`: copy this executable to `dest`. The docker
/// provider uses it to fill the agent volume, because the agent image may be
/// `scratch` and have no `cp`.
pub fn install(dest: &str) -> std::io::Result<()> {
    let me = std::env::current_exe()?;
    if let Some(dir) = std::path::Path::new(dest).parent() {
        std::fs::create_dir_all(dir)?;
    }
    std::fs::copy(&me, dest)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(dest, std::fs::Permissions::from_mode(0o755))?;
    }
    Ok(())
}
