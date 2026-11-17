//! Building the pristine datasets: the schema `spky migrate apply` deploys for
//! WhitePawn, then the generated rows, on 3.3.5 and on a fresh 4.0.1; then the
//! 3.3.5 store converted by 4.0.1 with its indexes rebuilt, the way the replay
//! upgrade produced WhitePawn's current database.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use serde_json::{json, Value};

use crate::db::Http;
use crate::docker::{self, Kind, Mode, Target};
use crate::gen::{self, Gen, Plan};
use crate::views;

/// A throwaway Ed25519 key pair for the bench databases only. WhitePawn's
/// `account` access is EdDSA-keyed from its vault (`{{JWT_PUBLIC_KEY}}`); the
/// bench substitutes this pair so record users can sign in.
const BENCH_JWT_PRIVATE: &str = "-----BEGIN PRIVATE KEY-----\nMC4CAQAwBQYDK2VwBCIEINKM1cGP5B4frRxHdGW6InGTu/7MJUjRbcqgiIcmLw6o\n-----END PRIVATE KEY-----";
const BENCH_JWT_PUBLIC: &str = "-----BEGIN PUBLIC KEY-----\nMCowBQYDK2VwAyEAAahixcv2I7hPrcV8axrB3wuiv5dgmcUNbcZjG+4U2Yo=\n-----END PUBLIC KEY-----";

/// The scheduler endpoint the generated functions post to. Nothing listens
/// there unless a workload starts the stub (`stub::serve`).
pub const ENDPOINT_PORT: u16 = 19667;
pub const SECRET: &str = "sdb4paritysecret";

pub struct PrepareOptions {
    pub spky: PathBuf,
    pub app: PathBuf,
    pub plan: Plan,
    pub parallel: usize,
}

pub async fn read_state(http: &Http) -> Value {
    match http
        .sql("DEFINE TABLE IF NOT EXISTS _sdb4p_meta SCHEMALESS PERMISSIONS NONE; SELECT * FROM ONLY _sdb4p_meta:state;")
        .await
    {
        Ok(mut v) => v.pop().filter(|v| !v.is_null()).unwrap_or_else(|| json!({})),
        Err(_) => json!({}),
    }
}

async fn mark(http: &Http, field: &str, value: Value) -> Result<()> {
    http.sql(&format!(
        "UPSERT _sdb4p_meta:state MERGE {{ {field}: {} }};",
        serde_json::to_string(&value)?
    ))
    .await?;
    Ok(())
}

pub async fn plan_of(http: &Http) -> Result<Plan> {
    let state = read_state(http).await;
    serde_json::from_value(
        state
            .get("plan")
            .cloned()
            .context("dataset has no plan: run `prepare` first")?,
    )
    .context("dataset plan")
}

pub async fn prepare(target: &Target, opts: &PrepareOptions) -> Result<()> {
    match target.kind {
        Kind::V335 | Kind::V401Fresh => load(target, opts).await,
        Kind::V401Converted => convert(target).await,
    }
}

async fn load(target: &Target, opts: &PrepareOptions) -> Result<()> {
    let volume = target.data_volume();
    docker::ensure_volume(&volume).await?;
    eprintln!("[{}] starting on {volume}", target.label);
    docker::start(target, &volume, Mode::Production).await?;
    let http = Http::new(&target.url());
    let result = load_inner(target, &http, opts).await;
    eprintln!("[{}] stopping", target.label);
    docker::stop(target).await;
    result
}

async fn load_inner(target: &Target, http: &Http, opts: &PrepareOptions) -> Result<()> {
    let state = read_state(http).await;
    if state.get("complete").and_then(Value::as_bool) == Some(true) {
        eprintln!("[{}] dataset complete", target.label);
        return Ok(());
    }
    if state.get("schema").and_then(Value::as_bool) != Some(true) {
        migrate(target, opts).await?;
        fix_access(http).await?;
        mark(http, "schema", json!(true)).await?;
    }
    let plan: Plan = match state.get("plan") {
        Some(p) => serde_json::from_value(p.clone())?,
        None => {
            mark(http, "plan", serde_json::to_value(&opts.plan)?).await?;
            opts.plan.clone()
        }
    };
    eprintln!(
        "[{}] plan: scale {} analysis {} legacy ledger {} ({} rows)",
        target.label,
        plan.scale,
        plan.analysis_rows,
        plan.legacy_ledger,
        plan.total()
    );
    http.sql("DELETE _sdb4p_meta WHERE id != _sdb4p_meta:state;")
        .await?;
    let hash = http
        .last(&format!(
            "RETURN crypto::argon2::generate({});",
            gen::q(gen::BENCH_PASSWORD)
        ))
        .await?;
    let schema = gen::read_schema(http).await?;
    let mut g = Gen::new(plan, schema);
    g.password_hash = hash.as_str().unwrap_or_default().to_string();
    let g = Arc::new(g);
    let done: Vec<String> = state
        .get("tables")
        .and_then(Value::as_array)
        .map(|a| {
            a.iter()
                .filter_map(|v| v.as_str().map(String::from))
                .collect()
        })
        .unwrap_or_default();
    let started = Instant::now();
    for table in g.tables() {
        if done.contains(&table) {
            continue;
        }
        let r = gen::load_table(http, g.clone(), &table, opts.parallel).await?;
        eprintln!(
            "[{}] {table}: {} rows in {:.1}s ({:.0}/s)",
            target.label,
            r.rows,
            r.elapsed.as_secs_f64(),
            r.rows as f64 / r.elapsed.as_secs_f64().max(0.001)
        );
        http.sql(&format!(
            "UPSERT _sdb4p_meta:state SET tables = array::union(tables ?? [], [{}]);",
            gen::q(&table)
        ))
        .await?;
    }
    if state.get("views").and_then(Value::as_bool) != Some(true) {
        load_views(target, http, &g, opts.parallel).await?;
        mark(http, "views", json!(true)).await?;
    }
    mark(http, "complete", json!(true)).await?;
    mark(http, "server", json!(http.version().await?)).await?;
    eprintln!(
        "[{}] loaded in {:.0}s",
        target.label,
        started.elapsed().as_secs_f64()
    );
    Ok(())
}

/// `spky migrate apply`: WhitePawn's 197 migrations, then sp00ky's internal
/// schema (meta tables, events, functions, schedules, push config), exactly
/// as a deploy applies them.
async fn migrate(target: &Target, opts: &PrepareOptions) -> Result<()> {
    let config = opts.app.join("sp00ky.yml");
    let migrations = opts.app.join("migrations");
    for p in [&opts.spky, &config, &migrations] {
        if !p.exists() {
            bail!("{} not found", p.display());
        }
    }
    eprintln!(
        "[{}] spky migrate apply ({})",
        target.label,
        opts.app.display()
    );
    let started = Instant::now();
    let spky = std::fs::canonicalize(&opts.spky)?;
    let out = tokio::process::Command::new(&spky)
        .current_dir(&opts.app)
        .args([
            "migrate",
            "apply",
            "--url",
            &target.url(),
            "--namespace",
            crate::db::NS,
            "--database",
            crate::db::DB,
            "--username",
            "root",
            "--password",
            "root",
            "--config",
        ])
        .arg(&config)
        .arg("--migrations-dir")
        .arg(&migrations)
        .args([
            "--mode",
            "cluster",
            "--endpoint",
            &format!("http://host.docker.internal:{ENDPOINT_PORT}"),
            "--secret",
            SECRET,
            "-y",
        ])
        .env("SPKY_YES", "1")
        .env("NO_COLOR", "1")
        .stdin(std::process::Stdio::null())
        .output()
        .await
        .context("running spky")?;
    let log = format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    if !out.status.success() {
        bail!("spky migrate apply failed:\n{log}");
    }
    let tail: Vec<&str> = log.lines().rev().take(8).collect();
    for l in tail.into_iter().rev() {
        eprintln!("    {l}");
    }
    eprintln!(
        "[{}] schema applied in {:.0}s",
        target.label,
        started.elapsed().as_secs_f64()
    );
    Ok(())
}

/// Substitute the bench key pair for the vault placeholders of the
/// `account` access, keeping its SIGNIN / AUTHENTICATE clauses.
async fn fix_access(http: &Http) -> Result<()> {
    let info = http.last("INFO FOR DB").await?;
    let Some(def) = info.pointer("/accesses/account").and_then(Value::as_str) else {
        return Ok(());
    };
    if !def.contains("{{JWT_PUBLIC_KEY}}") {
        return Ok(());
    }
    let fixed = def
        .replacen(
            "DEFINE ACCESS account",
            "DEFINE ACCESS OVERWRITE account",
            1,
        )
        .replace(
            "KEY '{{JWT_PUBLIC_KEY}}'",
            &format!("KEY '{BENCH_JWT_PUBLIC}'"),
        )
        .replace(
            "ISSUER KEY '[REDACTED]'",
            &format!("ISSUER KEY '{BENCH_JWT_PRIVATE}'"),
        );
    http.sql(&format!("{fixed};"))
        .await
        .context("redefining the account access")?;
    Ok(())
}

async fn load_views(target: &Target, http: &Http, g: &Arc<Gen>, parallel: usize) -> Result<()> {
    let started = Instant::now();
    let views = views::plan_views(g);
    let users = views::view_users(g);
    // the SSP's own DDL for each user's table, and the anon table
    let mut ddl = String::new();
    for u in 0..users {
        let table = views::list_ref_table(u);
        ddl.push_str(&ssp_node::tables::user_list_ref_ddl(
            &table,
            &gen::key("user", u),
        ));
        ddl.push('\n');
    }
    http.sql(&ddl).await.context("list_ref DDL")?;
    let bench = crate::db::BenchDb {
        db: crate::db::root_http(&target.url()).await?,
    };
    ssp_node::tables::ensure_anon_table(&bench).await?;
    // _00_query rows
    for chunk in views.chunks(500) {
        let rows: Vec<String> = chunk.iter().map(views::query_row).collect();
        http.sql(&format!(
            "OPTION IMPORT; INSERT IGNORE INTO _00_query [{}] RETURN NONE;",
            rows.join(",")
        ))
        .await
        .context("_00_query rows")?;
    }
    // edges, batched per table
    let mut batches: Vec<(String, Vec<String>)> = Vec::new();
    for v in &views {
        let table = v.table();
        for chunk in views::edge_rows(v).chunks(2000) {
            batches.push((table.clone(), chunk.to_vec()));
        }
    }
    let total: usize = batches.iter().map(|b| b.1.len()).sum();
    let batches = Arc::new(tokio::sync::Mutex::new(batches));
    let mut tasks = Vec::new();
    for _ in 0..parallel {
        let http = http.clone();
        let batches = batches.clone();
        tasks.push(tokio::spawn(async move {
            loop {
                let Some((table, rows)) = batches.lock().await.pop() else {
                    return Ok::<(), anyhow::Error>(());
                };
                let sql = format!(
                    "OPTION IMPORT; INSERT RELATION IGNORE INTO `{table}` [{}] RETURN NONE;",
                    rows.join(",")
                );
                // edges of different views share their target records' graph
                // pointers: concurrent batches conflict and are retried
                let mut attempt = 0u64;
                loop {
                    attempt += 1;
                    match http.sql_t(&sql, Duration::from_secs(600)).await {
                        Ok(_) => break,
                        Err(e) if attempt < 30 && format!("{e:#}").contains("can be retried") => {
                            tokio::time::sleep(Duration::from_millis(100 * attempt.min(20))).await;
                        }
                        Err(e) => return Err(e.context(format!("edges of {table}"))),
                    }
                }
            }
        }));
    }
    for t in tasks {
        t.await??;
    }
    eprintln!(
        "[{}] {} views, {} edges over {} user tables in {:.0}s",
        target.label,
        views.len(),
        total,
        users,
        started.elapsed().as_secs_f64()
    );
    Ok(())
}

/// The converted copy: the 3.3.5 dataset opened by 4.0.1, which converts it
/// in place, then every index removed and defined again with CONCURRENTLY
/// (the replay upgrade's `rebuild` step, spooky-cloud internal/dbupgrade),
/// under the scratch cache sizing the upgrade used.
async fn convert(target: &Target) -> Result<()> {
    let source = Target::of(Kind::V335);
    let volume = target.data_volume();
    if !docker::volume_exists(&volume).await {
        if !docker::volume_exists(&source.data_volume()).await {
            bail!("prepare 335 first: {} is missing", source.data_volume());
        }
        docker::copy_volume(&source.data_volume(), &volume).await?;
    }
    eprintln!(
        "[{}] starting 4.0.1 on {volume} (converts a 3.x store)",
        target.label
    );
    let started = Instant::now();
    docker::start(target, &volume, Mode::Convert).await?;
    eprintln!(
        "[{}] open after {:.0}s",
        target.label,
        started.elapsed().as_secs_f64()
    );
    let http = Http::new(&target.url());
    let result = async {
        let state = read_state(&http).await;
        if state.get("converted").and_then(Value::as_bool) == Some(true) {
            eprintln!("[{}] already converted", target.label);
            return Ok(());
        }
        if state.get("complete").and_then(Value::as_bool) != Some(true) {
            bail!("the 3.3.5 dataset is incomplete");
        }
        rebuild_indexes(target, &http).await?;
        mark(&http, "converted", json!(true)).await?;
        mark(&http, "server", json!(http.version().await?)).await?;
        Ok(())
    }
    .await;
    docker::stop(target).await;
    result
}

async fn rebuild_indexes(target: &Target, http: &Http) -> Result<()> {
    let info = http.last("INFO FOR DB").await?;
    let tables: Vec<String> = info
        .get("tables")
        .and_then(Value::as_object)
        .map(|m| m.keys().cloned().collect())
        .unwrap_or_default();
    let started = Instant::now();
    let mut n = 0;
    for table in tables {
        let ti = http.last(&format!("INFO FOR TABLE `{table}`")).await?;
        let Some(indexes) = ti.get("indexes").and_then(Value::as_object) else {
            continue;
        };
        for (name, def) in indexes {
            let def = def
                .as_str()
                .unwrap_or("")
                .trim()
                .trim_end_matches(';')
                .to_string();
            let def = if def.to_uppercase().contains(" CONCURRENTLY") {
                def
            } else {
                format!("{def} CONCURRENTLY")
            };
            let t0 = Instant::now();
            http.sql(&format!(
                "BEGIN; REMOVE INDEX `{name}` ON `{table}`; {def}; COMMIT;"
            ))
            .await?;
            loop {
                let i = http
                    .last(&format!("INFO FOR INDEX `{name}` ON `{table}`"))
                    .await?;
                let status = i
                    .pointer("/building/status")
                    .and_then(Value::as_str)
                    .unwrap_or("ready");
                match status {
                    "ready" | "" => break,
                    "error" => bail!("index {name} on {table}: {i}"),
                    _ => tokio::time::sleep(Duration::from_millis(500)).await,
                }
            }
            n += 1;
            if t0.elapsed() > Duration::from_secs(5) {
                eprintln!(
                    "[{}] rebuilt {table}.{name} in {:.0}s",
                    target.label,
                    t0.elapsed().as_secs_f64()
                );
            }
        }
    }
    eprintln!(
        "[{}] rebuilt {n} indexes in {:.0}s",
        target.label,
        started.elapsed().as_secs_f64()
    );
    Ok(())
}

pub fn default_app_dir() -> PathBuf {
    let home = std::env::var("HOME").unwrap_or_default();
    Path::new(&home).join("dev/whitepawn-sdk21/packages/schema")
}
