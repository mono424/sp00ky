//! `sdb-parity`: every query sp00ky sends to SurrealDB, timed on 3.3.5,
//! 4.0.1 fresh and 4.0.1 converted from 3.3.5, at WhitePawn's scale, in
//! Docker with WhitePawn's production limits. See README.md.

mod db;
mod docker;
mod gen;
mod prepare;
mod report;
mod stub;
mod views;
mod workloads;

use std::path::PathBuf;

use anyhow::{bail, Result};
use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(
    name = "sdb-parity",
    about = "SurrealDB 3.3.5 vs 4.0.1 parity suite for sp00ky's queries"
)]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Build the pristine datasets (Docker volumes sdb4-parity-<target>-data).
    /// Resumable: a table already loaded is skipped.
    Prepare {
        /// Comma-separated targets: 335, 401f, 401c (401c converts 335's).
        #[arg(long, default_value = "335,401f,401c")]
        targets: String,
        /// Row-count scale (1.0 = WhitePawn on 2026-11-16, 17M rows).
        #[arg(long, default_value_t = 1.0)]
        scale: f64,
        /// `analysis` rows (default: 11.45M x scale).
        #[arg(long)]
        analysis_rows: Option<u64>,
        /// Share of `_00_version` rows still keyed randomly (pre-migration
        /// ledger), as WhitePawn's was during the incident.
        #[arg(long, default_value_t = 1.0)]
        legacy_ledger: f64,
        /// Concurrent INSERT requests per server.
        #[arg(long, default_value_t = 6)]
        parallel: usize,
        /// The app whose schema is deployed (sp00ky.yml + migrations/).
        #[arg(long, env = "SDB_PARITY_APP")]
        app: Option<PathBuf>,
        /// The `spky` binary that applies it.
        #[arg(long, default_value = "target/release/spky")]
        spky: PathBuf,
    },
    /// Run the workloads and write the report.
    Run {
        #[arg(long, default_value = "335,401f,401c")]
        targets: String,
        /// Only workloads whose id contains one of these (comma-separated).
        #[arg(long)]
        only: Option<String>,
        /// Repetitions per workload (p50/p95 over these).
        #[arg(long, default_value_t = 20)]
        reps: usize,
        /// Seconds of the mixed concurrent load per server.
        #[arg(long, default_value_t = 180)]
        mixed_secs: u64,
        /// Work on the pristine volume instead of a fresh copy (faster,
        /// mutates the dataset).
        #[arg(long)]
        no_reset: bool,
        /// Report directory.
        #[arg(long, default_value = "docs/perf")]
        out: PathBuf,
        /// Label for this run's results file (results/<label>.json).
        #[arg(long, default_value = "latest")]
        label: String,
        /// Re-apply sp00ky's internal schema with this `spky` before the
        /// workloads (to measure the fixed functions, events and indexes).
        #[arg(long)]
        apply_schema: Option<PathBuf>,
        /// SurrealQL to apply before the workloads (e.g. the WhitePawn event
        /// rewrites in docs/perf/whitepawn-events-4x.surql).
        #[arg(long)]
        app_patch: Option<PathBuf>,
        /// The app `--apply-schema` deploys (sp00ky.yml + migrations/).
        #[arg(long, env = "SDB_PARITY_APP")]
        app: Option<PathBuf>,
        /// Also run the `_00_version` migration and the keyed-ledger pages
        /// (5 to 20 minutes per server).
        #[arg(long)]
        migrate_ledger: bool,
        /// Instead of the migration, rebuild the ledger keyed from the
        /// generator (for 3.3.5, where the migration takes hours here).
        #[arg(long, conflicts_with = "migrate_ledger")]
        rekey_ledger: bool,
        /// Leave out the workloads whose id contains one of these
        /// (comma-separated).
        #[arg(long)]
        skip: Option<String>,
        /// Run only these phases (comma-separated: before, after).
        #[arg(long)]
        phases: Option<String>,
    },
    /// Render the Markdown report from saved results.
    Report {
        #[arg(long, default_value = "docs/perf")]
        out: PathBuf,
        /// Results to compare: before[,after].
        #[arg(long, default_value = "latest")]
        labels: String,
    },
    /// Stop and remove the suite's containers (keeps the dataset volumes).
    Down,
    /// Rebuild a killed run's results from the per-workload lines it printed
    /// (`[<server>] <id>: p50 .. ms p95 .. ms`), up to `--until`, taking group,
    /// source and statement from another results file.
    #[command(hide = true)]
    ImportLog {
        #[arg(long, default_value = "docs/perf")]
        out: PathBuf,
        /// Results file to write (results/<label>.json).
        #[arg(long)]
        label: String,
        #[arg(long)]
        target: String,
        #[arg(long)]
        log: PathBuf,
        #[arg(long, default_value = "before")]
        phase: String,
        /// Stop at the first line containing this.
        #[arg(long, default_value = "applying the fixes")]
        until: String,
        /// Results file (label) whose workloads give group, source and SQL.
        #[arg(long)]
        like: String,
    },
    /// Replace a results file's slow log with the one in a saved container
    /// log (`docker logs <container> > file 2>&1`).
    #[command(hide = true)]
    ImportSlowLog {
        #[arg(long, default_value = "docs/perf")]
        out: PathBuf,
        #[arg(long)]
        label: String,
        /// 335, 401f or 401c.
        #[arg(long)]
        target: String,
        #[arg(long)]
        file: PathBuf,
    },
    /// Time a record user's game edits over WebSocket and over HTTP against a
    /// running server (diagnostics).
    #[command(hide = true)]
    ProbeUserWrite {
        #[arg(long)]
        url: String,
        #[arg(long, default_value_t = 400)]
        start: u64,
    },
}

fn targets(s: &str) -> Result<Vec<docker::Target>> {
    s.split(',')
        .filter(|s| !s.is_empty())
        .map(|k| docker::Target::parse(k.trim()))
        .collect()
}

fn main() -> Result<()> {
    // Embedded-free, but the SDK and the SSP builders recurse; give every
    // worker thread the stack the workspace's runtimes use.
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_stack_size(8 << 20)
        .build()?;
    rt.block_on(run())
}

async fn run() -> Result<()> {
    let cli = Cli::parse();
    if !docker::available().await {
        bail!("Docker is not available: the suite runs SurrealDB in containers");
    }
    match cli.cmd {
        Cmd::Prepare {
            targets: t,
            scale,
            analysis_rows,
            legacy_ledger,
            parallel,
            app,
            spky,
        } => {
            let opts = prepare::PrepareOptions {
                spky,
                app: app.unwrap_or_else(prepare::default_app_dir),
                plan: gen::Plan::new(scale, analysis_rows, legacy_ledger),
                parallel,
            };
            let ts = targets(&t)?;
            // 335 and 401f load independently and side by side; 401c needs 335.
            let (convert, load): (Vec<_>, Vec<_>) = ts
                .into_iter()
                .partition(|t| t.kind == docker::Kind::V401Converted);
            let mut handles = Vec::new();
            let opts = std::sync::Arc::new(opts);
            for t in load {
                let opts = opts.clone();
                handles.push(tokio::spawn(
                    async move { prepare::prepare(&t, &opts).await },
                ));
            }
            for h in handles {
                h.await??;
            }
            for t in convert {
                prepare::prepare(&t, &opts).await?;
            }
            Ok(())
        }
        Cmd::Run {
            targets: t,
            only,
            reps,
            mixed_secs,
            no_reset,
            out,
            label,
            apply_schema,
            app_patch,
            app,
            migrate_ledger,
            rekey_ledger,
            phases,
            skip,
        } => {
            let opts = workloads::RunOptions {
                only: only.map(|s| s.split(',').map(|s| s.trim().to_string()).collect()),
                reps,
                mixed_secs,
                reset: !no_reset,
                apply_schema,
                app_patch,
                app: app.unwrap_or_else(prepare::default_app_dir),
                migrate_ledger,
                rekey_ledger,
                phases: phases.map(|s| s.split(',').map(|s| s.trim().to_string()).collect()),
                skip: skip.map(|s| s.split(',').map(|s| s.trim().to_string()).collect()),
            };
            let results = workloads::run_all(&targets(&t)?, &opts).await?;
            let dir = out.join("results");
            std::fs::create_dir_all(&dir)?;
            let path = dir.join(format!("{label}.json"));
            std::fs::write(&path, serde_json::to_string_pretty(&results)?)?;
            eprintln!("results: {}", path.display());
            report::render(&out, &[label])
        }
        Cmd::Report { out, labels } => report::render(
            &out,
            &labels.split(',').map(String::from).collect::<Vec<_>>(),
        ),
        Cmd::ProbeUserWrite { url, start } => workloads::probe_user_write(&url, start).await,
        Cmd::ImportLog {
            out,
            label,
            target,
            log,
            phase,
            until,
            like,
        } => {
            let tpl = report::load(&out, &[like])?;
            let text = std::fs::read_to_string(&log)?;
            let t = docker::Target::parse(&target)?;
            let mut r = workloads::TargetResult {
                target: t.key.to_string(),
                label: t.label.to_string(),
                server: String::new(),
                ..Default::default()
            };
            let prefix = format!("[{}] ", t.label);
            for line in text.lines() {
                if line.contains(&until) {
                    break;
                }
                let Some(rest) = line.trim().strip_prefix(&prefix) else {
                    continue;
                };
                // `<id>: p50 X ms p95 Y ms[ (N errors: ...)]`, or a one-shot `<id>: X ms`
                let (id, p50, p95, rest) = if let Some((id, rest)) = rest.split_once(": p50 ") {
                    let Some((p50, rest)) = rest.split_once(" ms p95 ") else {
                        continue;
                    };
                    let Some((p95, rest)) = rest.split_once(" ms") else {
                        continue;
                    };
                    let (Ok(p50), Ok(p95)) = (p50.parse::<f64>(), p95.parse::<f64>()) else {
                        continue;
                    };
                    (id, p50, p95, rest)
                } else {
                    let Some((id, rest)) = rest.split_once(": ") else {
                        continue;
                    };
                    let Some(v) = rest.strip_suffix(" ms").and_then(|v| v.parse::<f64>().ok()) else {
                        continue;
                    };
                    if id.contains(' ') {
                        continue;
                    }
                    (id, v, v, "")
                };
                let errors = rest
                    .trim()
                    .strip_prefix('(')
                    .and_then(|e| e.split_whitespace().next())
                    .and_then(|n| n.parse::<usize>().ok())
                    .unwrap_or(0);
                // ids that end in a row count (`.1000`) can differ between servers
                let stem = |s: &str| {
                    s.rsplit_once('.')
                        .filter(|(_, n)| n.parse::<u64>().is_ok())
                        .map_or(s.to_string(), |(a, _)| a.to_string())
                };
                let all = || tpl.iter().flat_map(|x| x.workloads.iter());
                let like = all()
                    .find(|w| w.id == id)
                    .or_else(|| all().find(|w| stem(&w.id) == stem(id)));
                r.workloads.push(workloads::WorkloadResult {
                    phase: phase.clone(),
                    id: id.to_string(),
                    group: like
                        .map(|w| w.group.clone())
                        .unwrap_or_else(|| "other".into()),
                    source: like.map(|w| w.source.clone()).unwrap_or_default(),
                    sql: like.map(|w| w.sql.clone()).unwrap_or_default(),
                    p50,
                    p95,
                    ms: Vec::new(),
                    errors,
                    first_error: (errors > 0).then(|| rest.trim().to_string()),
                    explain: None,
                    note: Some(
                        "from the run's log (the run was stopped before it wrote its results)"
                            .into(),
                    ),
                });
            }
            eprintln!("{} workloads", r.workloads.len());
            let dir = out.join("results");
            std::fs::create_dir_all(&dir)?;
            std::fs::write(
                dir.join(format!("{label}.json")),
                serde_json::to_string_pretty(&vec![r])?,
            )?;
            Ok(())
        }
        Cmd::ImportSlowLog {
            out,
            label,
            target,
            file,
        } => {
            let path = out.join("results").join(format!("{label}.json"));
            let mut rs: Vec<workloads::TargetResult> =
                serde_json::from_str(&std::fs::read_to_string(&path)?)?;
            let log = String::from_utf8_lossy(&std::fs::read(&file)?).into_owned();
            let slow = workloads::slow_log_of(&log);
            let Some(r) = rs.iter_mut().find(|r| r.target == target) else {
                bail!("no {target} in {}", path.display())
            };
            eprintln!("{} slow statements", slow.len());
            r.slow_log = slow;
            std::fs::write(&path, serde_json::to_string_pretty(&rs)?)?;
            Ok(())
        }
        Cmd::Down => {
            for t in docker::Target::all() {
                docker::stop(&t).await;
            }
            Ok(())
        }
    }
}
