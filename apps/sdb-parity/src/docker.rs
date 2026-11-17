//! The three databases under test, as Docker containers with WhitePawn's
//! production limits (3 vCPUs, 6 GiB, SurrealKV block cache a quarter of that),
//! and the volumes that hold their datasets.

use std::process::Stdio;
use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::Command;

pub const SURREAL_335: &str = "surrealdb/surrealdb:v3.3.5";
pub const SURREAL_401: &str = "surrealdb/surrealdb:v4.0.1";
const HELPER_IMAGE: &str = "alpine:3.22";

/// The production DB container: `--cpus 3 --memory 6144m` (WhitePawn's DB VM,
/// 3 vCPU / 8 GB, runs SurrealDB under a 6144 MB cap).
pub const CPUS: &str = "3";
pub const MEMORY_MB: u64 = 6144;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind {
    /// 3.3.5 on RocksDB, the version WhitePawn ran until 2026-11-16.
    V335,
    /// 4.0.1 on SurrealKV, loaded fresh (compatibility level 4).
    V401Fresh,
    /// 4.0.1 on a 3.3.5 store it converted at startup, indexes rebuilt the
    /// way the replay upgrade rebuilt them (compatibility level 3): what
    /// WhitePawn runs now.
    V401Converted,
}

#[derive(Clone, Debug)]
pub struct Target {
    pub kind: Kind,
    pub key: &'static str,
    pub label: &'static str,
    pub port: u16,
}

impl Target {
    pub fn all() -> Vec<Target> {
        vec![
            Self::of(Kind::V335),
            Self::of(Kind::V401Fresh),
            Self::of(Kind::V401Converted),
        ]
    }

    pub fn of(kind: Kind) -> Target {
        match kind {
            Kind::V335 => Target {
                kind,
                key: "335",
                label: "3.3.5",
                port: 18335,
            },
            Kind::V401Fresh => Target {
                kind,
                key: "401f",
                label: "4.0.1 fresh",
                port: 18401,
            },
            Kind::V401Converted => Target {
                kind,
                key: "401c",
                label: "4.0.1 converted",
                port: 18402,
            },
        }
    }

    pub fn parse(key: &str) -> Result<Target> {
        Ok(match key {
            "335" => Self::of(Kind::V335),
            "401f" => Self::of(Kind::V401Fresh),
            "401c" => Self::of(Kind::V401Converted),
            other => bail!("unknown target `{other}` (335, 401f, 401c)"),
        })
    }

    pub fn image(&self) -> &'static str {
        match self.kind {
            Kind::V335 => SURREAL_335,
            _ => SURREAL_401,
        }
    }

    pub fn container(&self) -> String {
        format!("sdb4p-{}", self.key)
    }

    /// The pristine dataset. Never written by a run.
    pub fn data_volume(&self) -> String {
        format!("sdb4-parity-{}-data", self.key)
    }

    /// The copy a run works on, reset from the pristine dataset first.
    pub fn run_volume(&self) -> String {
        format!("sdb4-parity-{}-run", self.key)
    }

    pub fn url(&self) -> String {
        format!("http://127.0.0.1:{}", self.port)
    }

    pub fn is_v4(&self) -> bool {
        self.kind != Kind::V335
    }
}

/// How a container runs: with the production cache sizing, or the way the
/// upgrade's scratch copy ran (128 MiB SurrealKV block cache, startup
/// operations allowed an hour), which the conversion and the index rebuild need
/// to stay inside 6 GiB.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Mode {
    Production,
    Scratch,
    /// Production cache sizing for converting a 3.x store. With the scratch
    /// copy's 128 MiB block cache, 4.0.1's startup doc-ID migration of this
    /// 17M-row store re-read blocks from the page cache at ~700 MB/s (4.8 TB
    /// in 95 minutes) without finishing. A container already converting the
    /// same volume is attached to, not recreated.
    Convert,
    /// Production sizing plus the slow query log (statements over 200 ms),
    /// which the report summarises per server.
    Run,
}

pub async fn docker(args: &[&str]) -> Result<String> {
    let out = Command::new("docker")
        .args(args)
        .stdin(Stdio::null())
        .output()
        .await
        .context("running docker")?;
    if !out.status.success() {
        bail!(
            "docker {} failed: {}",
            args.join(" "),
            String::from_utf8_lossy(&out.stderr).trim()
        );
    }
    Ok(String::from_utf8_lossy(&out.stdout).trim().to_string())
}

/// A container's log. The server logs to stderr, which `docker logs` replays
/// on its own stderr: both streams, in that order.
pub async fn logs(container: &str, tail: Option<usize>) -> String {
    let mut args = vec!["logs".to_string()];
    if let Some(n) = tail {
        args.push("--tail".into());
        args.push(n.to_string());
    }
    args.push(container.to_string());
    match Command::new("docker")
        .args(&args)
        .stdin(Stdio::null())
        .output()
        .await
    {
        Ok(out) => format!(
            "{}{}",
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        ),
        Err(_) => String::new(),
    }
}

/// Wait until the container's CPU stays under `pct` (100 = one core) for
/// three samples in a row, or `limit` passes: a statement keeps running after
/// its client timed out, so a saturated server is still busy after a load.
pub async fn settle(container: &str, pct: f64, limit: Duration) -> Duration {
    let started = Instant::now();
    let mut calm = 0;
    while started.elapsed() < limit {
        let cpu = docker(&["stats", "--no-stream", "--format", "{{.CPUPerc}}", container])
            .await
            .ok()
            .and_then(|s| s.trim().trim_end_matches('%').parse::<f64>().ok())
            .unwrap_or(0.0);
        calm = if cpu < pct { calm + 1 } else { 0 };
        if calm >= 3 {
            break;
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
    started.elapsed()
}

pub async fn available() -> bool {
    docker(&["info", "--format", "{{.ServerVersion}}"])
        .await
        .is_ok()
}

pub async fn volume_exists(name: &str) -> bool {
    docker(&["volume", "inspect", name]).await.is_ok()
}

pub async fn ensure_volume(name: &str) -> Result<()> {
    if !volume_exists(name).await {
        docker(&["volume", "create", name]).await?;
    }
    Ok(())
}

/// Replace `to` with a copy of `from`.
pub async fn copy_volume(from: &str, to: &str) -> Result<()> {
    ensure_volume(to).await?;
    let started = Instant::now();
    docker(&[
        "run",
        "--rm",
        "-v",
        &format!("{from}:/from:ro"),
        "-v",
        &format!("{to}:/to"),
        HELPER_IMAGE,
        "sh",
        "-c",
        "find /to -mindepth 1 -delete && cp -a /from/. /to/",
    ])
    .await?;
    eprintln!(
        "  copied volume {from} -> {to} in {:.1}s",
        started.elapsed().as_secs_f64()
    );
    Ok(())
}

pub async fn remove_container(name: &str) {
    let _ = docker(&["rm", "-f", name]).await;
}

/// Start `target`'s server on `volume` and wait for /health.
pub async fn start(target: &Target, volume: &str, mode: Mode) -> Result<()> {
    let name = target.container();
    let memory = format!("{MEMORY_MB}m");
    let port = format!("{}:8000", target.port);
    let mount = format!("{volume}:/data");
    let mut env: Vec<String> = vec!["SURREAL_LOG=info".into()];
    let store;
    if target.is_v4() {
        env.push("SURREAL_CAPS_ALLOW_EXPERIMENTAL=surrealism".into());
        env.push("SURREAL_EXPERIMENTAL_UPGRADE_FROM_3X=true".into());
        // 4.0.1's startup data migration of a converted 3.x store runs in one
        // transaction on one core: 6.5 min on WhitePawn's DB VM, over an hour
        // here while two other servers were loading
        env.push("SURREAL_STARTUP_OPERATION_TIMEOUT=6h".into());
        let mem = MEMORY_MB << 20;
        let cache = match mode {
            Mode::Production | Mode::Run | Mode::Convert => mem / 4,
            Mode::Scratch => 128 << 20,
        };
        env.push(format!("SURREAL_SURREALKV_BLOCK_CACHE_CAPACITY={cache}"));
        env.push(format!(
            "SURREAL_SURREALKV_MAX_MEMTABLE_SIZE={}",
            (mem / 16).max(32 << 20)
        ));
        store = "surrealkv:/data/surreal/db";
    } else {
        env.push("SURREAL_CAPS_ALLOW_EXPERIMENTAL=files,surrealism".into());
        store = "rocksdb:/data/surreal/db";
    }
    env.push("SURREAL_BUCKET_FOLDER_ALLOWLIST=/buckets".into());
    if mode == Mode::Run {
        env.push("SURREAL_SLOW_QUERY_LOG_THRESHOLD=200ms".into());
    }
    let mut args: Vec<String> = vec![
        "run".into(),
        "-d".into(),
        "--name".into(),
        name.clone(),
        "--cpus".into(),
        CPUS.into(),
        "--memory".into(),
        memory,
        "-p".into(),
        port,
        "-v".into(),
        mount,
        "--tmpfs".into(),
        "/buckets".into(),
        "--user".into(),
        "root".into(),
        "--label".into(),
        "sdb4-parity=1".into(),
    ];
    for e in &env {
        args.push("-e".into());
        args.push(e.clone());
    }
    args.push(target.image().into());
    for a in [
        "start",
        "--bind",
        "0.0.0.0:8000",
        "--user",
        "root",
        "--pass",
        "root",
        "--allow-all",
        store,
    ] {
        args.push(a.into());
    }
    // A conversion interrupted while 4.0.1 migrates the store at startup keeps
    // its container: recreating it would roll the migration back and start over.
    if matches!(mode, Mode::Scratch | Mode::Convert) && running_with(&name, volume, &env).await {
        eprintln!("  {name} is already running on {volume}, waiting for it");
    } else {
        remove_container(&name).await;
        let refs: Vec<&str> = args.iter().map(String::as_str).collect();
        docker(&refs).await?;
    }
    wait_healthy(target, Duration::from_secs(6 * 3600)).await
}

/// `name` is running on `volume` with every variable of `env` set.
async fn running_with(name: &str, volume: &str, env: &[String]) -> bool {
    let Ok(out) = docker(&[
        "inspect",
        "-f",
        "{{.State.Status}}|{{range .Mounts}}{{.Name}} {{end}}|{{join .Config.Env \"|\"}}",
        name,
    ])
    .await
    else {
        return false;
    };
    let mut parts = out.splitn(3, '|');
    let (Some(state), Some(mounts), Some(have)) = (parts.next(), parts.next(), parts.next()) else {
        return false;
    };
    let have: Vec<&str> = have.split('|').collect();
    state == "running"
        && mounts.split_whitespace().any(|m| m == volume)
        && env.iter().all(|e| have.contains(&e.as_str()))
}

pub async fn wait_healthy(target: &Target, limit: Duration) -> Result<()> {
    let client = reqwest::Client::new();
    let started = Instant::now();
    let mut last_log = Instant::now();
    loop {
        // 4.x answers /health while a startup migration still runs in the
        // background and refuses queries with 503 "still starting up" until it
        // is done: ready means a query is answered.
        if let Ok(r) = client
            .post(format!("{}/sql", target.url()))
            .basic_auth("root", Some("root"))
            .header("Accept", "application/json")
            .body("RETURN 1;")
            .timeout(Duration::from_secs(5))
            .send()
            .await
        {
            if r.status().is_success() {
                return Ok(());
            }
        }
        let state = docker(&["inspect", "-f", "{{.State.Status}}", &target.container()])
            .await
            .unwrap_or_default();
        if state != "running" {
            let logs = logs(&target.container(), Some(30)).await;
            bail!("{} stopped ({state}):\n{logs}", target.container());
        }
        if started.elapsed() > limit {
            bail!("{} not healthy after {:?}", target.container(), limit);
        }
        if last_log.elapsed() > Duration::from_secs(60) {
            eprintln!(
                "  waiting for {} ({:.0}s)",
                target.container(),
                started.elapsed().as_secs_f64()
            );
            last_log = Instant::now();
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

/// Stop a server gracefully (SurrealKV flushes on SIGTERM), then remove it.
pub async fn stop(target: &Target) {
    let _ = docker(&["stop", "-t", "300", &target.container()]).await;
    remove_container(&target.container()).await;
}

/// One `docker stats` sample: CPU percent (100 = one core) and memory in MiB.
#[derive(Clone, Copy, Debug, Default, serde::Serialize)]
pub struct StatSample {
    pub t_ms: u64,
    pub cpu: f64,
    pub mem_mib: f64,
}

/// Streams `docker stats` for a container until dropped.
pub struct StatsSampler {
    child: tokio::process::Child,
    samples: std::sync::Arc<std::sync::Mutex<Vec<StatSample>>>,
}

impl StatsSampler {
    pub fn start(container: &str) -> Result<StatsSampler> {
        let mut child = Command::new("docker")
            .args(["stats", "--format", "{{.CPUPerc}} {{.MemUsage}}", container])
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .kill_on_drop(true)
            .spawn()
            .context("docker stats")?;
        let stdout = child.stdout.take().unwrap();
        let samples = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let sink = samples.clone();
        let started = Instant::now();
        tokio::spawn(async move {
            let mut lines = BufReader::new(stdout).lines();
            while let Ok(Some(line)) = lines.next_line().await {
                let clean = strip_ansi(&line);
                let mut parts = clean.split_whitespace();
                let (Some(cpu), Some(mem)) = (parts.next(), parts.next()) else {
                    continue;
                };
                let Ok(cpu) = cpu.trim_end_matches('%').parse::<f64>() else {
                    continue;
                };
                let mem_mib = parse_mem(mem);
                sink.lock().unwrap().push(StatSample {
                    t_ms: started.elapsed().as_millis() as u64,
                    cpu,
                    mem_mib,
                });
            }
        });
        Ok(StatsSampler { child, samples })
    }

    pub fn samples(&self) -> Vec<StatSample> {
        self.samples.lock().unwrap().clone()
    }

    pub async fn stop(mut self) -> Vec<StatSample> {
        let _ = self.child.kill().await;
        self.samples()
    }
}

/// `docker stats` repaints the terminal: drop its escape sequences
/// (`ESC [ <digits and ;> <letter>`), keeping the text after them.
fn strip_ansi(line: &str) -> String {
    let mut out = String::with_capacity(line.len());
    let mut chars = line.chars();
    while let Some(c) = chars.next() {
        if c == '\u{1b}' {
            if chars.next() == Some('[') {
                for c in chars.by_ref() {
                    if c.is_ascii_alphabetic() {
                        break;
                    }
                }
            }
            continue;
        }
        out.push(c);
    }
    out
}

fn parse_mem(s: &str) -> f64 {
    let (num, unit) = s.split_at(s.find(|c: char| c.is_ascii_alphabetic()).unwrap_or(s.len()));
    let n: f64 = num.parse().unwrap_or(0.0);
    match unit {
        "GiB" | "GB" => n * 1024.0,
        "KiB" | "kB" | "KB" => n / 1024.0,
        "B" => n / (1024.0 * 1024.0),
        _ => n,
    }
}

#[cfg(test)]
mod tests {
    use super::strip_ansi;

    #[test]
    fn stats_lines_keep_their_numbers() {
        assert_eq!(
            strip_ansi("\u{1b}[2J\u{1b}[H123.45% 2.7GiB / 6GiB"),
            "123.45% 2.7GiB / 6GiB"
        );
        assert_eq!(
            strip_ansi("\u{1b}[K0.50% 300MiB / 6GiB"),
            "0.50% 300MiB / 6GiB"
        );
        assert_eq!(strip_ansi("12.3% 1GiB / 6GiB"), "12.3% 1GiB / 6GiB");
    }
}
