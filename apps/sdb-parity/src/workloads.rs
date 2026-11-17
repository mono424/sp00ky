//! The workloads: every server-side path sp00ky exercises, timed on one server.
//!
//! Where sp00ky has a public function or constant for a query, the workload
//! calls it (the SSP's edge publication builders, its job runner, the schedule
//! engine's tick and SQL constants, the jobs sampler, the ledger lookup forms
//! and migration), so the suite measures whatever the code sends today. The
//! scheduler's and the SSP's private queries are copied verbatim, each with
//! its source location, and `queries_match_their_source` checks the copies
//! against the files.

use std::collections::BTreeMap;
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{anyhow, Context, Result};
use serde::Serialize;
use serde_json::{json, Value};
use surrealdb::engine::any::Any;
use surrealdb::Surreal;

use crate::db::{self, BenchDb, Http};
use crate::docker::{self, Mode, Target};
use crate::gen::{self, key, rid, Gen};
use crate::views::{self, View};

pub struct RunOptions {
    pub only: Option<Vec<String>>,
    pub reps: usize,
    pub mixed_secs: u64,
    pub reset: bool,
    /// Re-apply sp00ky's schema with this `spky` before the workloads (the
    /// fixed functions, events and indexes).
    pub apply_schema: Option<std::path::PathBuf>,
    /// SurrealQL applied before the workloads (the WhitePawn event rewrites).
    pub app_patch: Option<std::path::PathBuf>,
    /// The app whose `sp00ky.yml` / migrations `apply_schema` deploys.
    pub app: std::path::PathBuf,
    /// Run the ledger migration (moves the 3M legacy rows; minutes).
    pub migrate_ledger: bool,
    /// Instead of the migration, rebuild `_00_version` keyed from the
    /// generator (the same rows, as the migration leaves them): 3.3.5 deletes
    /// legacy rows at about 10 s per 1,000-row step on this dataset.
    pub rekey_ledger: bool,
    /// Run only these phases (`before`, `after`).
    pub phases: Option<Vec<String>>,
    /// Leave out the workloads whose id contains one of these.
    pub skip: Option<Vec<String>>,
}

#[derive(Clone, Debug, Serialize, serde::Deserialize, Default)]
pub struct WorkloadResult {
    /// `before`: the dataset as deployed (WhitePawn's schema and sp00ky's
    /// production internal schema, legacy ledger). `after`: this branch's
    /// internal schema, the WhitePawn patch and the ledger migration applied.
    #[serde(default)]
    pub phase: String,
    pub id: String,
    pub group: String,
    /// Where the query text comes from.
    pub source: String,
    /// The statement (or a representative one), for the report.
    pub sql: String,
    pub ms: Vec<f64>,
    pub p50: f64,
    pub p95: f64,
    pub errors: usize,
    pub first_error: Option<String>,
    /// `EXPLAIN` of the statement when it has a plannable top-level form.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub explain: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub note: Option<String>,
}

#[derive(Clone, Debug, Serialize, serde::Deserialize, Default)]
pub struct MixedResult {
    pub secs: u64,
    pub requests: u64,
    pub errors: u64,
    pub cpu_mean: f64,
    pub cpu_p95: f64,
    pub cpu_max: f64,
    pub mem_max_mib: f64,
    pub per_task: BTreeMap<String, (u64, f64, f64)>,
    pub cpu_series: Vec<(u64, f64)>,
    /// task -> (errors, first error)
    #[serde(default)]
    pub errors_by_task: BTreeMap<String, (u64, String)>,
    /// Seconds the server stayed above 30% CPU after the load stopped (work
    /// the clients gave up on keeps running), capped at 900.
    #[serde(default)]
    pub drain_secs: Option<f64>,
}

#[derive(Clone, Debug, Serialize, serde::Deserialize, Default)]
pub struct TargetResult {
    pub target: String,
    pub label: String,
    pub server: String,
    pub workloads: Vec<WorkloadResult>,
    /// (phase, result)
    pub mixed: Vec<(String, MixedResult)>,
    /// Statements the server logged as slow (>= 200 ms), counted.
    pub slow_log: Vec<(String, u64, f64)>,
}

/// What every workload can reach.
pub struct Ctx {
    pub target: Target,
    pub http: Http,
    pub root: Surreal<Any>,
    pub bench: BenchDb,
    pub gen: Arc<Gen>,
    pub views: Vec<View>,
    pub games_by_user: BTreeMap<u64, Vec<u64>>,
    pub seq: AtomicU64,
}

impl Ctx {
    pub fn next(&self) -> u64 {
        self.seq.fetch_add(1, Ordering::Relaxed)
    }
    /// A record that exists: row `i` (mod count) of `table`.
    pub fn rec(&self, table: &str, i: u64) -> String {
        rid(table, i % self.gen.count_for(table).max(1))
    }
    pub async fn q(&self, sql: &str, vars: Value) -> Result<Vec<Value>> {
        db::query(&self.root, sql, &vars).await
    }
    /// Sign the root connection in again: its token lasts an hour, and a run
    /// outlives it (after that every root request answers 401).
    pub async fn reauth(&self) {
        let signed = self
            .root
            .signin(surrealdb::opt::auth::Root {
                username: "root".into(),
                password: "root".into(),
            })
            .await;
        if let Err(e) = signed {
            eprintln!("  [{}] root signin failed: {e:#}", self.target.label);
        }
        let _ = self.root.use_ns(db::NS).use_db(db::DB).await;
    }
    /// Through the SSP's database port (binds plain JSON, as the SSP does).
    pub async fn ssp(&self, sql: &str, binds: &[(&str, Value)]) -> Result<Vec<Value>> {
        use ssp_node::ports::Db;
        self.bench
            .query(sql, binds)
            .await
            .map_err(|e| anyhow!("{e}"))
    }
}

struct Runner<'a> {
    ctx: &'a Ctx,
    opts: &'a RunOptions,
    out: Vec<WorkloadResult>,
}

fn pct(sorted: &[f64], p: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let i = ((sorted.len() as f64 - 1.0) * p).round() as usize;
    sorted[i.min(sorted.len() - 1)]
}

impl<'a> Runner<'a> {
    fn wanted(&self, id: &str) -> bool {
        if self.opts.skip.as_ref().is_some_and(|s| s.iter().any(|s| id.contains(s.as_str()))) {
            return false;
        }
        match &self.opts.only {
            None => true,
            Some(list) => list.iter().any(|s| id.contains(s.as_str())),
        }
    }

    /// Time `f` `reps` times (fewer when a call is slow: a scan of 11M rows
    /// is measured three times, not twenty).
    async fn time<F, Fut>(
        &mut self,
        id: &str,
        group: &str,
        source: &str,
        sql: &str,
        mut f: F,
    ) -> Option<&mut WorkloadResult>
    where
        F: FnMut(usize) -> Fut,
        Fut: Future<Output = Result<()>>,
    {
        if !self.wanted(id) {
            return None;
        }
        let mut ms = Vec::new();
        let mut errors = 0;
        let mut first_error = None;
        let mut reps = self.opts.reps;
        let mut i = 0;
        while i < reps {
            let started = Instant::now();
            let r = tokio::time::timeout(Duration::from_secs(180), f(i)).await;
            let el = started.elapsed().as_secs_f64() * 1000.0;
            match r {
                Ok(Ok(())) => ms.push(el),
                Ok(Err(e)) => {
                    errors += 1;
                    first_error.get_or_insert_with(|| db::truncate(&format!("{e:#}"), 400));
                    ms.push(el);
                }
                Err(_) => {
                    errors += 1;
                    first_error.get_or_insert_with(|| "timed out after 180 s".to_string());
                    ms.push(el);
                }
            }
            if i == 0 {
                if el > 20_000.0 {
                    reps = reps.min(2);
                } else if el > 3_000.0 {
                    reps = reps.min(3);
                } else if el > 800.0 {
                    reps = reps.min(6);
                }
            }
            i += 1;
        }
        let mut sorted = ms.clone();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());
        // a statement that timed out keeps running on the server: wait for it
        // so it does not slow the workloads that follow, and say how long
        let mut note = None;
        if first_error.as_deref().is_some_and(|e| e.starts_with("timed out")) {
            let busy = docker::settle(&self.ctx.target.container(), 30.0, Duration::from_secs(3600)).await;
            eprintln!("  [{}] {id}: server busy for {:.0}s after the timeout", self.ctx.target.label, busy.as_secs_f64());
            note = Some(format!("server busy for {:.0} s after the client gave up", busy.as_secs_f64()));
        }
        let r = WorkloadResult {
            phase: String::new(),
            id: id.to_string(),
            group: group.to_string(),
            source: source.to_string(),
            sql: db::truncate(sql, 1500),
            p50: pct(&sorted, 0.5),
            p95: pct(&sorted, 0.95),
            ms,
            errors,
            first_error,
            explain: None,
            note,
        };
        eprintln!(
            "  [{}] {id}: p50 {:.1} ms p95 {:.1} ms{}",
            self.ctx.target.label,
            r.p50,
            r.p95,
            if r.errors > 0 {
                format!(
                    " ({} errors: {})",
                    r.errors,
                    r.first_error.clone().unwrap_or_default()
                )
            } else {
                String::new()
            }
        );
        self.out.push(r);
        self.out.last_mut()
    }

    /// A workload that is one statement run as-is with fixed bound variables.
    async fn stmt(&mut self, id: &str, group: &str, source: &str, sql: &str, vars: Value) {
        let ctx = self.ctx;
        let s = sql.to_string();
        let v = vars.clone();
        if let Some(r) = self
            .time(id, group, source, sql, |_| {
                let s = s.clone();
                let v = v.clone();
                async move { ctx.q(&s, v).await.map(|_| ()) }
            })
            .await
        {
            let plain = sql.trim().trim_end_matches(';');
            let upper = plain.to_ascii_uppercase();
            if upper.starts_with("SELECT ") && !plain.contains(';') {
                if let Ok(mut e) = ctx.q(&format!("{plain} EXPLAIN"), vars).await {
                    r.explain = e.pop();
                }
            }
        }
    }
}

// ------------------------------------------------------------------ setup

/// Shift the operational tables' timestamps from the dataset's fixed "now"
/// to the real one, so TTLs, leases and retention windows see what a live
/// deployment sees. Import mode: no events, no changefeed.
async fn freshen(ctx: &Ctx) -> Result<()> {
    let delta_secs = (std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_secs() as i64)
        - gen::NOW_SECS;
    let delta = format!("{delta_secs}s");
    let mut sql = String::from("OPTION IMPORT;\n");
    let tables = [
        "_00_schedule_run",
        "_00_workflow_run",
        "_00_step_run",
        "_00_wf_instance",
        "_00_push_message",
        "_00_user_feature",
        "connection",
        "job",
        "gameanalysis_job",
        "statistics_job",
        "email_job",
        "linkpreview_job",
        "camera_job",
        "render_job",
    ];
    for t in tables {
        let Some(info) = ctx.gen.schema.get(t) else {
            continue;
        };
        let sets: Vec<String> = info
            .fields
            .iter()
            .filter(|f| {
                matches!(f.ty, gen::Ty::Datetime)
                    || matches!(&f.ty, gen::Ty::Option(i) if matches!(**i, gen::Ty::Datetime))
            })
            .map(|f| {
                format!(
                    "`{0}` = IF `{0}` != NONE {{ `{0}` + {delta} }} ELSE {{ NONE }}",
                    f.name
                )
            })
            .collect();
        if !sets.is_empty() {
            sql.push_str(&format!(
                "UPDATE `{t}` SET {} RETURN NONE;\n",
                sets.join(", ")
            ));
        }
    }
    // game-sync picks connections not synced for 15 minutes: spread the
    // last syncs over that window, as a running deployment has them
    sql.push_str("UPDATE connection SET last_synced_at = time::now() - <duration>(<string>rand::int(0, 900) + 's') WHERE last_synced_at != NONE RETURN NONE;\n");
    // registered views: alive, 3% expired so the TTL sweep has work
    sql.push_str(
        "UPDATE _00_query SET lastActiveAt = IF rand::float() < 0.03 { time::now() - 3h } ELSE { time::now() - 5m }, \
         subscribers = array::map(subscribers ?? [], |$s| { id: $s.id, seenAt: time::now() - 5m }) RETURN NONE;\n",
    );
    // a server that just started may still be compacting: retry conflicts
    let mut attempt = 0;
    loop {
        attempt += 1;
        match ctx.http.sql(&sql).await {
            Ok(_) => return Ok(()),
            Err(e) if attempt < 10 && format!("{e:#}").contains("can be retried") => {
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
            Err(e) => return Err(e.context("freshen")),
        }
    }
}

async fn apply_schema(ctx: &Ctx, spky: &std::path::Path, app: &std::path::Path) -> Result<()> {
    let spky = std::fs::canonicalize(spky)?;
    let out = tokio::process::Command::new(&spky)
        .current_dir(app)
        .args([
            "migrate",
            "apply",
            "--url",
            &ctx.target.url(),
            "--namespace",
            db::NS,
            "--database",
            db::DB,
            "--config",
            "sp00ky.yml",
            "--migrations-dir",
            "migrations",
            "--mode",
            "cluster",
            "--endpoint",
        ])
        .arg(format!(
            "http://host.docker.internal:{}",
            crate::prepare::ENDPOINT_PORT
        ))
        .args(["--secret", crate::prepare::SECRET, "-y"])
        .env("SPKY_YES", "1")
        .env("NO_COLOR", "1")
        // 4.0.1 takes a minute or more for the internal schema on this dataset
        .env("SPKY_DB_HTTP_TIMEOUT_SECS", "900")
        .output()
        .await?;
    let log = format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    if !out.status.success() {
        anyhow::bail!("spky migrate apply: {log}");
    }
    for l in log
        .lines()
        .filter(|l| l.contains("Internal schema") || l.contains("Remote functions"))
    {
        eprintln!("  [{}] {}", ctx.target.label, l.trim());
    }
    Ok(())
}

// ------------------------------------------------------------------ groups

/// Writes through the generated events and WhitePawn's own events.
async fn group_writes(r: &mut Runner<'_>) {
    let ctx = r.ctx;
    let g = "writes";
    // The production top offender (WhitePawn gameanalysis-api, 2026-11-16 slow log).
    let sql = "LET $before = (UPDATE type::record($id) SET insights_computed_at = time::now(), insights_attempts = NONE, insights_next_attempt_at = NONE RETURN BEFORE)[0];";
    r.time(
        "write.game.insights_done",
        g,
        "WhitePawn gameanalysis-api (prod slow log)",
        sql,
        |i| async move {
            let id = ctx.rec("game", 1000 + i as u64 * 37);
            // alternate so `insights_computed_at` flips NONE -> set each time
            ctx.q(
                "UPDATE type::record($id) SET insights_computed_at = NONE RETURN NONE;",
                json!({"id": id.clone()}),
            )
            .await?;
            ctx.q(sql, json!({ "id": id })).await.map(|_| ())
        },
    )
    .await;
    // sp00ky's batched client mutation (packages/core/src/mutation/rows.ts remoteBatch)
    let sql = "LET $ok0 = { UPDATE $id0 MERGE $data0; true }";
    r.time("write.game.client_merge", g, "packages/core/src/mutation/rows.ts:399 (remoteBatch)", sql, |i| async move {
        let id = ctx.rec("game", 5000 + i as u64 * 41);
        ctx.q(sql, json!({ "id0": format!("r\"{id}\""), "data0": { "result": if i % 2 == 0 { "1-0" } else { "0-1" } } })).await.map(|_| ())
    })
    .await;
    let sql = "LET $ok0 = { CREATE ONLY $id0 SET owner = $d0_owner, database = $d0_database, white = $d0_white, black = $d0_black, pgn = $d0_pgn, result = $d0_result, status = $d0_status, date = $d0_date, sort_index = $d0_sort_index; true }";
    r.time(
        "write.game.client_create",
        g,
        "packages/core/src/mutation/rows.ts:358 (remoteStatement create)",
        sql,
        |_| async move {
            let n = ctx.next();
            let owner = 3u64;
            ctx.q(sql, json!({
            "id0": format!("r\"game:bench_c{n}_{}\"", ctx.target.key),
            "d0_owner": format!("r\"{}\"", rid("user", owner)),
            "d0_database": format!("r\"{}\"", rid("game_database", owner)),
            "d0_white": format!("r\"{}\"", rid("player_name", owner)),
            "d0_black": format!("r\"{}\"", rid("player_name", owner + 5000)),
            "d0_pgn": "1. e4 e5 2. Nf3 Nc6 1-0", "d0_result": "1-0", "d0_status": "finished",
            "d0_date": "d\"2026-11-15T10:00:00Z\"", "d0_sort_index": -1_794_700_000i64,
        })).await.map(|_| ())
        },
    )
    .await;
    // delete runs WhitePawn's game_children_cleanup: DELETE analysis WHERE game = $before.id
    let sql = "LET $ok0 = { DELETE $id0; true }";
    r.time(
        "write.game.client_delete",
        g,
        "packages/core/src/mutation/rows.ts:371 (remoteStatement delete)",
        sql,
        |i| async move {
            let id = ctx.rec("game", 700_000 + i as u64 * 13);
            ctx.q(sql, json!({ "id0": format!("r\"{id}\"") }))
                .await
                .map(|_| ())
        },
    )
    .await;
    let sql = "LET $ok0 = { UPDATE $id0 MERGE $data0; true }";
    r.time(
        "write.analysis.client_merge",
        g,
        "packages/core/src/mutation/rows.ts:399 (remoteBatch)",
        sql,
        |i| async move {
            let id = ctx.rec("analysis", 9_000_000 + i as u64 * 7919);
            ctx.q(
                sql,
                json!({ "id0": format!("r\"{id}\""), "data0": { "depth": 20 + (i % 7) } }),
            )
            .await
            .map(|_| ())
        },
    )
    .await;
    let sql = "UPSERT type::record($id) SET game = $game, owner = $owner, fen = $fen, engine = 'sf18-lite', depth = 22, variants = '[]', created_at = time::now(), updated_at = time::now()";
    r.time("write.analysis.backend_upsert", g, "WhitePawn gameanalysis-api cache write (shape)", sql, |_| async move {
        let n = ctx.next();
        ctx.q(sql, json!({ "id": format!("analysis:bench_a{n}"), "game": format!("r\"{}\"", ctx.rec("game", 77)), "owner": format!("r\"{}\"", rid("user", 0)), "fen": format!("bench fen {n}") })).await.map(|_| ())
    })
    .await;
    let sql = "UPDATE type::record($id) SET variant = $v";
    r.time(
        "write.user_feature.update",
        g,
        "sp00ky generated events only (_00_user_feature)",
        sql,
        |i| async move {
            let id = ctx.rec("_00_user_feature", i as u64 * 3);
            ctx.q(
                sql,
                json!({ "id": format!("r\"{id}\""), "v": if i % 2 == 0 { "on" } else { "off" } }),
            )
            .await
            .map(|_| ())
        },
    )
    .await;
    let sql = "UPDATE type::record($id) SET name = $n";
    r.time("write.game_database.rename", g, "app write (game_database events)", sql, |i| async move {
        ctx.q(sql, json!({ "id": format!("r\"{}\"", ctx.rec("game_database", 7 + i as u64)), "n": format!("renamed {i}") })).await.map(|_| ())
    })
    .await;
}

/// The SSP: registration, edge publication, source probes, metrics, TTL,
/// bootstrap pages.
async fn group_ssp(r: &mut Runner<'_>) {
    let ctx = r.ctx;
    let g = "ssp";
    let user0 = 0u64;
    let views0: Vec<&View> = ctx.views.iter().filter(|v| v.user == Some(user0)).collect();
    let view = views0
        .iter()
        .find(|v| v.edges.len() == 50)
        .copied()
        .or(views0.first().copied());
    let Some(view) = view else { return };
    let table = view.table();

    // registration (node.rs:1612 warm, 1977 cold, 1373 stranded probe)
    let warm = "UPSERT type::record($id) SET ownerHash = IF auth_id = 'anon' AND ownerHash AND ownerHash != $ownerHash { THROW 'owner_mismatch' } ELSE { ownerHash OR $ownerHash OR NONE }, clientId = <string>$clientId, auth_id = auth_id ?? <string>$authId, surql = surql ?? <string>$surql, params = params ?? $params, rowCount = rowCount ?? <int>$rowCount, state = state ?? <string>$state, lastActiveAt = <datetime>$lastActiveAt, ttl = (IF ttl = NONE OR <duration>$ttl > ttl { <duration>$ttl } ELSE { ttl }), subscribers = array::append( array::filter(subscribers ?? [], |$s| <string>$s.id != $sid AND <datetime>$s.seenAt + ttl > time::now()), { id: $sid, seenAt: time::now() }) RETURN BEFORE";
    let vk = view.key.clone();
    r.time(
        "ssp.view_register_warm_upsert",
        g,
        "packages/ssp-node/src/node.rs:1612",
        warm,
        |_| {
            let vk = vk.clone();
            async move {
                let binds = reg_binds(&format!("_00_query:{vk}"), "user:x", "ready");
                ctx.ssp(
                    warm,
                    &binds
                        .iter()
                        .map(|(k, v)| (*k, v.clone()))
                        .collect::<Vec<_>>(),
                )
                .await
                .map(|_| ())
            }
        },
    )
    .await;
    let probe = format!("LET $from = type::record('_00_query', $qid); SELECT VALUE count() FROM {table} WHERE in = $from GROUP ALL; SELECT VALUE count() FROM _00_query WHERE id = $from GROUP ALL;");
    let p2 = probe.clone();
    r.time(
        "ssp.view_register_stranded_probe",
        g,
        "packages/ssp-node/src/node.rs:1373",
        &probe,
        |_| {
            let p = p2.clone();
            let vk = vk.clone();
            async move { ctx.ssp(&p, &[("qid", json!(vk))]).await.map(|_| ()) }
        },
    )
    .await;
    let cold = "UPSERT type::record($id) SET ownerHash = IF auth_id = 'anon' AND ownerHash AND ownerHash != $ownerHash { THROW 'owner_mismatch' } ELSE { ownerHash OR $ownerHash OR NONE }, clientId = <string>$clientId, auth_id = <string>$authId, surql = <string>$surql, params = $params, ttl = (IF ttl = NONE OR <duration>$ttl > ttl { <duration>$ttl } ELSE { ttl }), lastActiveAt = <datetime>$lastActiveAt, registrationTime = <float>$registrationTime, rowCount = <int>$rowCount, state = <string>$state, subscribers = array::append( array::filter(subscribers ?? [], |$s| <string>$s.id != $sid AND <datetime>$s.seenAt + (IF ttl = NONE OR <duration>$ttl > ttl { <duration>$ttl } ELSE { ttl }) > time::now()), { id: $sid, seenAt: time::now() }) WHERE <string>$authId != '' OR auth_id = NONE OR auth_id = ''";
    r.time(
        "ssp.view_register_cold_upsert",
        g,
        "packages/ssp-node/src/node.rs:1977",
        cold,
        |_| async move {
            let n = ctx.next();
            let mut binds = reg_binds(
                &format!("_00_query:benchcold{n}"),
                &rid("user", 0),
                "materializing",
            );
            binds.push(("registrationTime", json!(12.5)));
            ctx.ssp(
                cold,
                &binds
                    .iter()
                    .map(|(k, v)| (*k, v.clone()))
                    .collect::<Vec<_>>(),
            )
            .await
            .map(|_| ())
        },
    )
    .await;
    // per-user table DDL (tables.rs:174)
    r.time(
        "ssp.ensure_user_tables",
        g,
        "packages/ssp-node/src/tables.rs:174 user_list_ref_ddl",
        "DEFINE TABLE OVERWRITE _00_list_ref_user_<uid> ... (10 statements)",
        |i| async move {
            let ddl = ssp_node::tables::user_list_ref_ddl(
                &views::list_ref_table(i as u64 % 5),
                &key("user", i as u64 % 5),
            );
            ctx.ssp(&ddl, &[]).await.map(|_| ())
        },
    )
    .await;

    // edge publication (edges.rs build_edge_batch + plan_transactions)
    let hot_views: Vec<View> = ctx
        .views
        .iter()
        .filter(|v| v.user == Some(user0) && v.edges.len() <= 200 && !v.edges.is_empty())
        .take(60)
        .cloned()
        .collect();
    let hot_record = hot_views
        .first()
        .map(|v| v.edges[0].0.clone())
        .unwrap_or_default();
    let holders: Vec<View> = hot_views
        .iter()
        .filter(|v| v.edges.iter().any(|e| e.0 == hot_record))
        .cloned()
        .collect();
    let sample = edge_txs(
        &holders,
        |v| delta(v, vec![], vec![], vec![hot_record.clone()], false),
        0,
    )
    .first()
    .cloned()
    .unwrap_or_default();
    r.time(
        &format!(
            "ssp.edge_publish.bump_one_record_in_{}_views",
            holders.len()
        ),
        g,
        "packages/ssp-node/src/edges.rs:978 build_edge_batch",
        &sample.0,
        |i| {
            let holders = holders.clone();
            let hot = hot_record.clone();
            async move {
                for (sql, binds) in edge_txs(
                    &holders,
                    |v| delta(v, vec![], vec![], vec![hot.clone()], false),
                    i as i64 + 20,
                ) {
                    ctx.ssp(
                        &sql,
                        &binds
                            .iter()
                            .map(|(k, v)| (k.as_str(), json!(v)))
                            .collect::<Vec<_>>(),
                    )
                    .await?;
                }
                Ok(())
            }
        },
    )
    .await;
    // a full round: 2000 version bumps across many views and users
    let round_views: Vec<View> = ctx
        .views
        .iter()
        .filter(|v| v.user.is_some() && v.edges.len() >= 20 && v.edges.len() <= 200)
        .take(40)
        .cloned()
        .collect();
    let round = |ver: i64| {
        edge_txs(
            &round_views,
            |v| {
                delta(
                    v,
                    vec![],
                    vec![],
                    v.edges
                        .iter()
                        .filter(|e| e.1.is_none())
                        .take(50)
                        .map(|e| e.0.clone())
                        .collect(),
                    false,
                )
            },
            ver,
        )
    };
    let first = round(30).first().cloned().unwrap_or_default();
    r.time(
        "ssp.edge_publish.round_bumps",
        g,
        "packages/ssp-node/src/edges.rs:978 + plan_transactions (500-statement transactions)",
        &first.0,
        |i| {
            let txs = round(31 + i as i64);
            async move {
                for (sql, binds) in txs {
                    ctx.ssp(
                        &sql,
                        &binds
                            .iter()
                            .map(|(k, v)| (k.as_str(), json!(v)))
                            .collect::<Vec<_>>(),
                    )
                    .await?;
                }
                Ok(())
            }
        },
    )
    .await;
    // cold registration: a full publish of a new 50-row view and a 3861-row view
    for (label, size) in [("initial_50", 50usize), ("initial_3861", 3861)] {
        let base = ctx
            .views
            .iter()
            .filter(|v| v.user == Some(user0))
            .max_by_key(|v| v.edges.len())
            .cloned();
        let Some(base) = base else { continue };
        let id = format!("ssp.edge_publish.{label}");
        r.time(&id, g, "packages/ssp-node/src/edges.rs:978 (initial delta) + node.rs:1977", "DELETE $from0->tbl; RELATE $from0->tbl->rec SET version = .., clientId = $cid0, auth_id = $aid0 ... UPDATE $from0 SET state = 'ready'", |_| {
            let base = base.clone();
            async move {
                let n = ctx.next();
                let mut v = base.clone();
                v.key = format!("benchinit{n}{}", ctx.target.key);
                v.edges = base.edges.iter().filter(|e| e.1.is_none()).take(size).cloned().collect();
                let mut binds = reg_binds(&format!("_00_query:{}", v.key), &v.auth_id(), "materializing");
                binds.push(("registrationTime", json!(1.0)));
                ctx.ssp(cold, &binds.iter().map(|(k, v)| (*k, v.clone())).collect::<Vec<_>>()).await?;
                let adds = v.edges.iter().map(|e| e.0.clone()).collect();
                for (sql, b) in edge_txs(&[v.clone()], |v| delta(v, adds_clone(&adds), vec![], vec![], true), 3) {
                    ctx.ssp(&sql, &b.iter().map(|(k, v)| (k.as_str(), json!(v))).collect::<Vec<_>>()).await?;
                }
                Ok(())
            }
        })
        .await;
    }
    // drop edges pointing at deleted records (edges.rs:337)
    let drop_sql = format!(
        "DELETE array::flatten((SELECT VALUE id FROM [{}]<-{table}))",
        view.edges
            .iter()
            .take(3)
            .map(|e| e.0.clone())
            .collect::<Vec<_>>()
            .join(", ")
    );
    r.stmt(
        "ssp.edge_drop_to_records",
        g,
        "packages/ssp-node/src/edges.rs:337",
        &drop_sql
            .replace("DELETE ", "SELECT VALUE id FROM ")
            .replace(
                "SELECT VALUE id FROM array::flatten",
                "RETURN array::flatten",
            ),
        json!({}),
    )
    .await;

    // source wait probe (node.rs:2935)
    let probe = "LET $spky_rid = type::record($rid); RETURN type::record('_00_version', [$spky_rid]).version ?? (SELECT VALUE version FROM ONLY _00_version WHERE record_id = $spky_rid LIMIT 1);";
    r.time(
        "ssp.source_wait_probe",
        g,
        "packages/ssp-node/src/node.rs:2935",
        probe,
        |i| async move {
            ctx.ssp(probe, &[("rid", json!(ctx.rec("game", i as u64 * 101)))])
                .await
                .map(|_| ())
        },
    )
    .await;
    // view metrics flush, 50 views per request (node.rs:3014)
    let flush_one = "UPDATE type::record($id_N) SET rowCount = <int>$rows_N, updateCount = (IF metricsFlush = $flush { updateCount } ELSE { (updateCount ?? 0) + <int>$updates_N }), metricsFlush = $flush, lastIngestLatency = (IF $updates_N > 0 { <float>$latency_N } ELSE { lastIngestLatency }), materializationP55 = $p55_N ?? materializationP55, materializationP90 = $p90_N ?? materializationP90, materializationP99 = $p99_N ?? materializationP99, registrationStages = $registration_N ?? registrationStages, ingestStages = $ingest_N ?? ingestStages RETURN NONE;";
    let flush_views: Vec<String> = ctx
        .views
        .iter()
        .take(50)
        .map(|v| format!("_00_query:{}", v.key))
        .collect();
    r.time(
        "ssp.view_metrics_flush_50",
        g,
        "packages/ssp-node/src/node.rs:3014",
        flush_one,
        |i| {
            let fv = flush_views.clone();
            async move {
                let mut sql = String::new();
                let mut binds: Vec<(String, Value)> =
                    vec![("flush".into(), json!(format!("bench-{i}")))];
                for (n, id) in fv.iter().enumerate() {
                    sql.push_str(&flush_one.replace("_N", &format!("_{n}")));
                    binds.push((format!("id_{n}"), json!(id)));
                    binds.push((format!("rows_{n}"), json!(50)));
                    binds.push((format!("updates_{n}"), json!(1)));
                    binds.push((format!("latency_{n}"), json!(3.5)));
                    for k in ["p55", "p90", "p99", "registration", "ingest"] {
                        binds.push((format!("{k}_{n}"), Value::Null));
                    }
                }
                ctx.ssp(
                    &sql,
                    &binds
                        .iter()
                        .map(|(k, v)| (k.as_str(), v.clone()))
                        .collect::<Vec<_>>(),
                )
                .await
                .map(|_| ())
            }
        },
    )
    .await;
    // TTL sweep (node.rs:2581, 2608; tables.rs:277, 301)
    r.stmt("ssp.ttl_prune_subscribers", g, "packages/ssp-node/src/node.rs:2581",
        "UPDATE _00_query SET subscribers = array::filter(subscribers ?? [], |$s| <datetime>$s.seenAt + ttl > time::now()) WHERE array::len(array::filter(subscribers ?? [], |$s| <datetime>$s.seenAt + ttl <= time::now())) > 0", json!({})).await;
    r.stmt("ssp.ttl_select_expired", g, "packages/ssp-node/src/node.rs:2608",
        "SELECT VALUE (<string>id + '|' + <string>(auth_id OR '')) FROM _00_query WHERE lastActiveAt + ttl < time::now() OR reaping = true", json!({})).await;
    r.stmt(
        "ssp.ttl_user_count",
        g,
        "packages/ssp-node/src/tables.rs:277",
        "SELECT VALUE count() FROM _00_query WHERE auth_id = $a GROUP ALL",
        json!({ "a": rid("user", 0) }),
    )
    .await;
    r.stmt(
        "ssp.ttl_orphan_tables",
        g,
        "packages/ssp-node/src/tables.rs:301",
        "SELECT VALUE auth_id FROM _00_query",
        json!({}),
    )
    .await;
    let strip = format!("RETURN array::len(type::record('_00_query', $qid)->({table} LIMIT 1000))");
    let vk2 = view.key.clone();
    r.time(
        "ssp.ttl_edge_probe",
        g,
        "packages/ssp-node/src/node.rs:2710",
        &strip,
        |_| {
            let s = strip.clone();
            let vk = vk2.clone();
            async move { ctx.ssp(&s, &[("qid", json!(vk))]).await.map(|_| ()) }
        },
    )
    .await;
    // schema probe and table info (bootstrap.rs:100, 184)
    r.stmt(
        "ssp.schema_probe",
        g,
        "packages/ssp-node/src/bootstrap.rs:100",
        "INFO FOR DB",
        json!({}),
    )
    .await;
    let infos: String = [
        "game",
        "analysis",
        "user",
        "player_name",
        "notification",
        "contact",
    ]
    .iter()
    .map(|t| format!("INFO FOR TABLE {t};"))
    .collect();
    r.stmt(
        "ssp.table_info_batch",
        g,
        "packages/ssp-node/src/bootstrap.rs:184",
        &infos,
        json!({}),
    )
    .await;
}

fn adds_clone(v: &Vec<String>) -> Vec<String> {
    v.clone()
}

fn reg_binds(id: &str, auth: &str, state: &str) -> Vec<(&'static str, Value)> {
    vec![
        ("id", json!(id)),
        ("clientId", json!("bench-client")),
        ("sid", json!("bench-client")),
        ("ownerHash", json!("")),
        ("authId", json!(auth)),
        (
            "surql",
            json!("SELECT * FROM game WHERE database = $database ORDER BY sort_index ASC LIMIT 50"),
        ),
        (
            "params",
            json!({ "auth": { "id": auth }, "access": "account" }),
        ),
        ("rowCount", json!(50)),
        ("state", json!(state)),
        ("ttl", json!("1h")),
        ("lastActiveAt", json!(chrono_now())),
    ]
}

fn chrono_now() -> String {
    gen::chrono_like(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64,
    )
}

fn delta(
    v: &View,
    additions: Vec<String>,
    removals: Vec<String>,
    updates: Vec<String>,
    initial: bool,
) -> ssp::circuit::ViewDelta {
    ssp::circuit::ViewDelta {
        query_id: format!("_00_query:{}", v.key),
        row_count: v.edges.len(),
        additions,
        removals,
        updates,
        result_hash: String::new(),
        subquery_items: vec![],
        auth_id: if v.user.is_some() {
            v.auth_id()
        } else {
            "anon".into()
        },
        initial,
    }
}

struct FixedVersion(i64);
impl ssp_node::edges::RecordVersions for FixedVersion {
    fn version_of(&self, _key: &str) -> i64 {
        self.0
    }
}

/// The transactions the SSP sends for these views' deltas.
fn edge_txs(
    views: &[View],
    mk: impl Fn(&View) -> ssp::circuit::ViewDelta,
    version: i64,
) -> Vec<(String, Vec<(String, String)>)> {
    let deltas: Vec<ssp::circuit::ViewDelta> = views.iter().map(&mk).collect();
    let refs: Vec<&ssp::circuit::ViewDelta> = deltas.iter().collect();
    let batch = ssp_node::edges::build_edge_batch(
        &refs,
        ssp_protocol::RefMode::Dedicated,
        &FixedVersion(version),
    );
    ssp_node::edges::plan_transactions(&batch, ssp_node::edges::MAX_TX_STATEMENTS)
        .into_iter()
        .map(|t| (t.sql, t.bindings))
        .collect()
}

/// Scan pages with every `_00_rv` form, on the legacy ledger (beta.3 forms
/// included: what WhitePawn's SSP and scheduler still sent on 2026-11-16).
async fn group_pages(r: &mut Runner<'_>, phase: &str) {
    let ctx = r.ctx;
    let g = "pages";
    use ssp_protocol::{with_durable_row_versions_using, VersionLookup};
    // a window over game keys starting near the middle of the key space
    let lo = "m";
    // The fallback form each line's scheduler and SSP choose while legacy rows
    // remain (bound on 3.x, $parent.id on 4.x), at 200 rows, and the other
    // line's form at 20 (it scans the ledger per row).
    let (native, other) = if ctx.target.is_v4() {
        (
            VersionLookup::KeyedThenParentId,
            VersionLookup::KeyedThenBoundId,
        )
    } else {
        (
            VersionLookup::KeyedThenBoundId,
            VersionLookup::KeyedThenParentId,
        )
    };
    let label = |l: VersionLookup| {
        if l == VersionLookup::KeyedThenParentId {
            "keyed_then_parent_id"
        } else {
            "keyed_then_bound_id"
        }
    };
    for (name, size, lookup) in [
        ("keyed", 1000usize, VersionLookup::Keyed),
        (label(native), 200, native),
        (label(other), 20, other),
    ] {
        let page = format!("SELECT * FROM game:⟨{lo}⟩.. ORDER BY id LIMIT {size}");
        let sql = with_durable_row_versions_using(&page, lookup);
        r.stmt(
            &format!("sched.clone_page.{phase}.{name}.{size}"),
            g,
            "apps/scheduler/src/replica.rs:296 + ssp-protocol with_durable_row_versions_using",
            &sql,
            json!({}),
        )
        .await;
    }
    // beta.3 (the release WhitePawn ran): BoundId on 3.x, ParentId on 4.x, chosen at boot
    if phase == "legacy" {
        for (name, proj) in [
            ("beta3_parent_id", "SELECT *, (SELECT VALUE version FROM _00_version WHERE record_id = $parent.id LIMIT 1)[0] ?? _00_rv ?? 1 AS _00_rv"),
            ("beta3_bound_id", "SELECT *, { LET $spky_row_id = id; RETURN (SELECT VALUE version FROM _00_version WHERE record_id = $spky_row_id LIMIT 1)[0] ?? _00_rv ?? 1; } AS _00_rv"),
        ] {
            let sql = format!("{proj} FROM game:⟨{lo}⟩.. ORDER BY id LIMIT 10");
            r.stmt(&format!("sched.clone_page.legacy.{name}.10"), g, "sp00ky/v1.0.0-beta.3 packages/ssp-protocol/src/lib.rs:819", &sql, json!({})).await;
        }
    }
    r.stmt(
        &format!("sched.ledger_head.{phase}"),
        g,
        "packages/ssp-protocol/src/version_ledger.rs:47 HEAD_QUERY",
        ssp_protocol::version_ledger::HEAD_QUERY,
        json!({}),
    )
    .await;
    let ids: Vec<String> = (0..500).map(|i| ctx.rec("game", i * 1499)).collect();
    let refetch = with_durable_row_versions_using(
        "SELECT * FROM $ids.map(|$i| <record> $i)",
        if phase == "legacy" {
            native
        } else {
            VersionLookup::Keyed
        },
    );
    r.stmt(
        &format!("sched.drift_refetch_rows.{phase}.500"),
        g,
        "apps/scheduler/src/replica.rs:2192",
        &refetch,
        json!({ "ids": ids }),
    )
    .await;
}

/// The scheduler's periodic and per-event queries.
async fn group_scheduler(r: &mut Runner<'_>) {
    let ctx = r.ctx;
    let g = "scheduler";
    use schedule_core::sql;
    // the engine's own tick (every 5 s) and prune (every 60 s)
    let engine = Arc::new(schedule_core::engine::ScheduleEngine::new(
        Arc::new(ctx.bench.clone()) as Arc<dyn schedule_core::db::ScheduleDb>,
        Arc::new(schedule_core::kill::NoopJobKill),
        schedule_core::engine::EngineConfig::default(),
    ));
    let e = engine.clone();
    // the first pass plans every schedule and runs the prune; steady passes after
    if r.wanted("sched.schedule_tick") {
        let started = Instant::now();
        let first = tokio::time::timeout(Duration::from_secs(900), e.tick_pass()).await;
        let ms = started.elapsed().as_secs_f64() * 1000.0;
        r.out.push(WorkloadResult {
            id: "sched.schedule_tick.first".into(),
            group: g.into(),
            source: "packages/schedule-core/src/engine.rs:290 tick_pass (first pass: plans every schedule, fires the due ones, prunes)".into(),
            sql: "ScheduleEngine::tick_pass()".into(),
            ms: vec![ms],
            p50: ms,
            p95: ms,
            errors: (!matches!(first, Ok(Ok(_)))) as usize,
            first_error: match first { Ok(Ok(_)) => None, Ok(Err(e)) => Some(format!("{e:#}")), Err(_) => Some("timed out after 900 s".into()) },
            ..Default::default()
        });
        eprintln!(
            "  [{}] sched.schedule_tick.first: {:.1} ms",
            ctx.target.label, ms
        );
    }
    r.time(
        "sched.schedule_tick",
        g,
        "packages/schedule-core/src/engine.rs:290 tick_pass (first pass includes the prune)",
        "ScheduleEngine::tick_pass()",
        |_| {
            let e = e.clone();
            async move { e.tick_pass().await.map(|_| ()) }
        },
    )
    .await;
    let before_none = json!("2020-01-01T00:00:00Z");
    // the forms before this branch (sp00ky d93beff66), one `RETURN { }` block each
    const OLD_PRUNE_WORKFLOW_RUNS: &str = "RETURN { LET $doomed = (SELECT id, schedule_name, finished_at, time::floor(finished_at, 1h) AS bucket FROM _00_workflow_run WHERE status = $status AND finished_at != NONE AND finished_at < <datetime>$before ORDER BY finished_at ASC LIMIT $batch); LET $ids = $doomed.id; DELETE _00_step_run WHERE workflow_run INSIDE $ids; DELETE $ids; RETURN $doomed; }";
    const OLD_PRUNE_SCHEDULE_RUNS: &str = "RETURN { LET $doomed = (SELECT id, schedule_name, finished_at, time::floor(finished_at, 1h) AS bucket FROM _00_schedule_run WHERE status = $status AND finished_at != NONE AND finished_at < <datetime>$before AND schedule_name NOT IN $except ORDER BY finished_at ASC LIMIT $batch); DELETE $doomed.id; RETURN $doomed; }";
    let wf_delete = sql::delete_workflow_runs(0);
    for (id, q, extra) in [
        (
            "sched.prune.schedule_runs.before",
            OLD_PRUNE_SCHEDULE_RUNS,
            json!({ "except": [] }),
        ),
        (
            "sched.prune.schedule_runs",
            sql::PRUNE_SCHEDULE_RUNS,
            json!({ "except": [] }),
        ),
        (
            "sched.prune.workflow_runs.before",
            OLD_PRUNE_WORKFLOW_RUNS,
            json!({}),
        ),
        (
            "sched.prune.workflow_runs",
            sql::PRUNE_WORKFLOW_RUNS,
            json!({}),
        ),
        (
            "sched.prune.workflow_runs.delete_batch",
            wf_delete.as_str(),
            json!({}),
        ),
        (
            "sched.prune.durable_instances",
            sql::PRUNE_DURABLE_INSTANCES,
            json!({}),
        ),
    ] {
        let mut vars = json!({ "status": "success", "before": before_none, "batch": 500 });
        for (k, v) in extra.as_object().unwrap() {
            vars[k] = v.clone();
        }
        let sqlt = q.to_string();
        r.time(
            &format!("{id}.nothing_due"),
            g,
            "packages/schedule-core/src/sql.rs",
            q,
            |_| {
                let s = sqlt.clone();
                let v = vars.clone();
                async move {
                    ctx.ssp(
                        &s,
                        &v.as_object()
                            .unwrap()
                            .iter()
                            .map(|(k, v)| (k.as_str(), v.clone()))
                            .collect::<Vec<_>>(),
                    )
                    .await
                    .map(|_| ())
                }
            },
        )
        .await;
    }
    let prune_job = sql::prune_job_table("job");
    r.time(
        "sched.prune.job_table.nothing_due",
        g,
        "packages/schedule-core/src/sql.rs:708 prune_job_table",
        &prune_job,
        |_| {
            let s = prune_job.clone();
            async move {
                ctx.ssp(
                    &s,
                    &[
                        ("status", json!("success")),
                        ("before", json!("2020-01-01T00:00:00Z")),
                        ("batch", json!(500)),
                    ],
                )
                .await
                .map(|_| ())
            }
        },
    )
    .await;
    for (id, q) in [
        ("sched.schedule_select_due", sql::SELECT_DUE),
        ("sched.schedule_select_unplanned", sql::SELECT_UNPLANNED),
        ("sched.schedule_select_triggered", sql::SELECT_TRIGGERED),
        ("sched.heal_running_job_runs", sql::SELECT_RUNNING_JOB_RUNS),
        ("sched.reap_expired_job_runs", sql::SELECT_EXPIRED_JOB_RUNS),
        (
            "sched.reap_expired_wf_runs",
            sql::SELECT_EXPIRED_WORKFLOW_RUNS,
        ),
    ] {
        let qs = q.to_string();
        r.time(id, g, "packages/schedule-core/src/sql.rs", q, |_| {
            let s = qs.clone();
            async move {
                ctx.ssp(
                    &s,
                    &[
                        ("now", json!(chrono_now())),
                        ("limit", json!(100)),
                        ("batch", json!(100)),
                    ],
                )
                .await
                .map(|_| ())
            }
        })
        .await;
    }
    let find = sql::FIND_RUN_BY_JOB.to_string();
    r.time(
        "sched.observe_find_run_by_job",
        g,
        "packages/schedule-core/src/sql.rs:440",
        &find,
        |i| {
            let s = find.clone();
            async move {
                ctx.ssp(&s, &[("job", json!(ctx.rec("job", i as u64 * 7)))])
                    .await
                    .map(|_| ())
            }
        },
    )
    .await;
    let create = sql::CREATE_JOB.to_string();
    r.time("sched.job_enqueue", g, "packages/schedule-core/src/sql.rs:236 CREATE_JOB", &create, |_| {
        let s = create.clone();
        async move {
            let n = ctx.next();
            ctx.ssp(&s, &[("tb", json!("job")), ("key", json!(format!("sch_bench_{n}"))), ("content", json!({ "path": "/syncGames", "payload": { "connection": ctx.rec("connection", n) }, "status": "pending", "max_retries": 2, "retry_strategy": "linear" }))]).await.map(|_| ())
        }
    })
    .await;
    // the jobs sampler (every 15 s, not viewer-gated): `_00_schedule_run WHERE job_id INSIDE [...]`
    r.time(
        "sched.admin.jobs_sampler",
        g,
        "packages/schedule-core/src/admin.rs:593 snapshot",
        "schedule_core::admin::snapshot()",
        |_| async move {
            schedule_core::admin::snapshot(&ctx.bench)
                .await
                .map(|_| ())
                .map_err(|e| anyhow!("{e:?}"))
        },
    )
    .await;
    // job recovery (job_scheduler.rs:503, 536), every 30 s per job table
    r.stmt("sched.job_recovery_pending", g, "apps/scheduler/src/job_scheduler.rs:503",
        "SELECT type::string(id) AS id, assignee, ((created_at + <duration>(string::concat(<string>(delay ?? 0), 'ms'))) <= time::now() - 600s) AS long_overdue FROM job WHERE status = 'pending' AND updated_at < time::now() - 30s AND (created_at + <duration>(string::concat(<string>(delay ?? 0), 'ms'))) <= time::now()", json!({})).await;
    r.stmt("sched.job_recovery_processing", g, "apps/scheduler/src/job_scheduler.rs:536",
        &format!("SELECT type::string(id) AS id, assignee, lease_until, updated_at FROM job WHERE status = 'processing' AND ({} OR assignee != NONE)", sql::LEASE_EXPIRED), json!({})).await;
    // feature flag sweep (feature_flags.rs:56-120), every 30 s
    r.stmt(
        "sched.flag_sweep_select_flags",
        g,
        "apps/scheduler/src/feature_flags.rs:56",
        "SELECT key, enabled, default_variant, variants, rules, payloads FROM _00_feature_flag;",
        json!({}),
    )
    .await;
    r.stmt(
        "sched.flag_sweep_select_users",
        g,
        "apps/scheduler/src/feature_flags.rs:67",
        "SELECT VALUE <string>id FROM user;",
        json!({}),
    )
    .await;
    r.stmt(
        "sched.flag_sweep_select_assigned",
        g,
        "apps/scheduler/src/feature_flags.rs:113",
        "SELECT VALUE <string>user FROM _00_user_feature WHERE key = $key;",
        json!({ "key": gen::FLAG_KEYS[3] }),
    )
    .await;
    // presence (presence.rs:551), every 15 s
    r.stmt("sched.admin.presence_sample", g, "apps/scheduler/src/admin/presence.rs:551",
        "SELECT type::string(id) AS id, auth_id, clientId AS client_id, array::len(subscribers ?? []) AS subscribers, materializationP99 AS p99, errorCount AS errors, rowCount AS row_count, updateCount AS updates FROM _00_query WHERE lastActiveAt + ttl > time::now() LIMIT 20001;", json!({})).await;
    // heartbeat (heartbeat.rs:349): UPSERT the probe row, its event runs
    r.stmt(
        "sched.heartbeat_probe",
        g,
        "apps/scheduler/src/heartbeat.rs:349",
        "UPSERT _00_heartbeat:probe SET hb_seq = $s",
        json!({ "s": 1 }),
    )
    .await;
    // drift check counts (drift.rs:292), every 300 s per synced table
    for t in [
        "contact",
        "player_name",
        "game",
        "game_insight",
        "puzzle_lecture",
        "_00_user_feature",
        "analysis",
    ] {
        r.stmt(
            &format!("sched.drift_count.{t}"),
            g,
            "apps/scheduler/src/drift.rs:292",
            &format!("SELECT count() AS total FROM {t} GROUP ALL"),
            json!({}),
        )
        .await;
    }
    // WhitePawn backends' queries from the 2026-11-16 slow log (app code, not sp00ky)
    r.stmt(
        "app.gamesync_dedup",
        g,
        "WhitePawn gamesync-api (prod slow log)",
        "SELECT VALUE source_id FROM game WHERE database = $coll AND source = $source",
        json!({ "coll": format!("r\"{}\"", rid("game_database", 0)), "source": "lichess" }),
    )
    .await;
    r.stmt(
        "app.collection_count",
        g,
        "WhitePawn analytics-api (prod slow log)",
        "SELECT VALUE count() FROM game WHERE database = $coll GROUP ALL",
        json!({ "coll": format!("r\"{}\"", rid("game_database", 0)) }),
    )
    .await;
    r.stmt(
        "app.puzzle_layout_none",
        g,
        "WhitePawn (prod slow log)",
        "SELECT VALUE id FROM puzzle WHERE layout = NONE LIMIT $n",
        json!({ "n": 100 }),
    )
    .await;
}

/// The job runner the SSP runs for each outbox job (ssp-node jobs/runner.rs).
async fn group_jobs(r: &mut Runner<'_>) {
    let ctx = r.ctx;
    let g = "jobs";
    use ssp_node::jobs as jr;
    r.time("ssp.job_lifecycle", g, "packages/ssp-node/src/jobs/runner.rs: set_assignee_helper, claim_processing, complete_success_helper", "UPDATE type::record($id) SET assignee = $assignee; UPDATE type::record($id) SET status = 'processing' ... WHERE status = 'pending' RETURN AFTER; complete", |_| async move {
        let n = ctx.next();
        let id = format!("job:bench_life_{n}");
        ctx.ssp("CREATE type::record($tb, $key) CONTENT $content", &[("tb", json!("job")), ("key", json!(format!("bench_life_{n}"))), ("content", json!({ "path": "/syncGames", "payload": {}, "status": "pending", "max_retries": 2, "retry_strategy": "linear" }))]).await?;
        jr::set_assignee_helper(&ctx.bench, &id, "ssp-0").await?;
        let claim = jr::claim_processing(&ctx.bench, &id, "ssp-0", 150).await?;
        let _ = claim;
        jr::complete_success_helper(&ctx.bench, &id, "{\"ok\":true}", Some(1)).await?;
        Ok(())
    })
    .await;
    r.time("ssp.job_claim_pending", g, "packages/ssp-node/src/jobs/runner.rs:687", "UPDATE type::record($id) SET status = 'processing', assignee = $me, ... WHERE status = 'pending' RETURN AFTER", |i| async move {
        let id = ctx.rec("job", 100 + i as u64 * 17);
        jr::claim_processing(&ctx.bench, &id, "ssp-1", 150).await.map(|_| ())
    })
    .await;
    r.time("ssp.job_load_record", g, "packages/ssp-node/src/jobs/runner.rs:1031", "SELECT status, path, payload, retries, max_retries, retry_strategy, timeout FROM ONLY type::record($id)", |i| async move {
        jr::load_job_record(&ctx.bench, &ctx.rec("job", i as u64 * 31)).await.map(|_| ())
    })
    .await;
}

/// The DB functions clients and admins call, and reads as a record user.
async fn group_functions(r: &mut Runner<'_>) {
    let ctx = r.ctx;
    let g = "functions";
    r.stmt(
        "fn.feature.materialize",
        g,
        "apps/cli/src/meta_tables_remote.surql fn::feature::materialize",
        "RETURN fn::feature::materialize($key);",
        json!({ "key": gen::FLAG_KEYS[2] }),
    )
    .await;
    r.stmt(
        "fn.feature.reassign",
        g,
        "apps/cli/src/meta_tables_remote.surql fn::feature::reassign",
        "RETURN fn::feature::reassign($key, $user);",
        json!({ "key": gen::FLAG_KEYS[2], "user": format!("r\"{}\"", rid("user", 3)) }),
    )
    .await;
    r.stmt(
        "fn.feature.allow",
        g,
        "apps/cli/src/meta_tables_remote.surql fn::feature::allow",
        "RETURN fn::feature::allow($key, 'on', $user);",
        json!({ "key": gen::FLAG_KEYS[4], "user": format!("r\"{}\"", rid("user", 4)) }),
    )
    .await;

    // record users over WebSocket, as the clients connect
    let user = 0u64;
    let session = match user_session(ctx, user).await {
        Ok(s) => s,
        Err(e) => {
            eprintln!("  [{}] record signin failed: {e:#}", ctx.target.label);
            return;
        }
    };
    let table = views::list_ref_table(user);
    let my_view = ctx.views.iter().find(|v| v.user == Some(user)).cloned();
    let s = &session;
    r.time(
        "fn.query.heartbeat",
        g,
        "apps/cli/src/functions_remote.surql:30",
        "fn::query::heartbeat($id)",
        |_| {
            let v = my_view.clone();
            async move {
                let v = v.ok_or_else(|| anyhow!("no view"))?;
                db::query(
                    s,
                    "RETURN fn::query::heartbeat($id);",
                    &json!({ "id": format!("r\"_00_query:⟨{}⟩\"", v.key) }),
                )
                .await
                .map(|_| ())
            }
        },
    )
    .await;
    let reads = [
        ("perm.read.game_window", "SELECT id, date, white, black, result, sort_index FROM game WHERE database = $database ORDER BY sort_index ASC LIMIT 50", json!({ "database": format!("r\"{}\"", rid("game_database", user)) })),
        ("perm.read.game_owner_count", "SELECT count() FROM game WHERE owner = $auth.id GROUP ALL", json!({})),
        ("perm.read.notifications", "SELECT * FROM notification WHERE recipient = $auth.id ORDER BY created_ms DESC LIMIT 100", json!({})),
        ("perm.read.my_views", "SELECT id, state, rowCount FROM _00_query WHERE auth_id = <string>$auth.id", json!({})),
    ];
    for (id, sql, vars) in reads {
        let sql = sql.to_string();
        r.time(
            id,
            g,
            "record user (account access) over WebSocket",
            &sql.clone(),
            |_| {
                let sql = sql.clone();
                let vars = vars.clone();
                async move { db::query(s, &sql, &vars).await.map(|_| ()) }
            },
        )
        .await;
    }
    if let Some(v) = &my_view {
        let sql =
            format!("SELECT * FROM {table} WHERE in = type::record('_00_query', $q) FETCH out");
        let key = v.key.clone();
        r.time(
            "perm.read.list_ref_membership_fetch",
            g,
            "packages/core (membership read of the user's list_ref table)",
            &sql.clone(),
            |_| {
                let sql = sql.clone();
                let key = key.clone();
                async move { db::query(s, &sql, &json!({ "q": key })).await.map(|_| ()) }
            },
        )
        .await;
    }
    // LIVE SELECT with FETCH out on the user's list_ref table, notified by
    // the SSP's edge publication (root) - latency to the record user
    if r.wanted("live.list_ref_fetch_out") {
        if let Err(e) = live_latency(r, &session, user).await {
            eprintln!("  [{}] live: {e:#}", ctx.target.label);
        }
    }
}

async fn user_session(ctx: &Ctx, user: u64) -> Result<Surreal<Any>> {
    let ws = ctx.target.url().replacen("http://", "ws://", 1);
    let s = surrealdb::engine::any::connect(ws).await?;
    s.signin(surrealdb::opt::auth::Record {
        namespace: db::NS.to_string(),
        database: db::DB.to_string(),
        access: "account".to_string(),
        params: json!({ "email": format!("bench{user}@example.test"), "password": gen::BENCH_PASSWORD }),
    })
    .await
    .context("record signin")?;
    Ok(s)
}

async fn live_latency(r: &mut Runner<'_>, session: &Surreal<Any>, user: u64) -> Result<()> {
    use futures::StreamExt;
    let ctx = r.ctx;
    let table = views::list_ref_table(user);
    let mut resp = session
        .query(format!("LIVE SELECT * FROM {table} FETCH out"))
        .await?;
    let mut stream = resp.stream::<surrealdb::Notification<surrealdb::types::Value>>(0)?;
    let view = ctx
        .views
        .iter()
        .find(|v| v.user == Some(user) && v.edges.len() >= 20)
        .cloned()
        .ok_or_else(|| anyhow!("no view"))?;
    let mut samples = Vec::new();
    for i in 0..r.opts.reps.min(30) {
        let rec = view.edges[i % view.edges.len()].0.clone();
        let txs = edge_txs(
            &[view.clone()],
            |v| delta(v, vec![], vec![], vec![rec.clone()], false),
            100 + i as i64,
        );
        let started = Instant::now();
        for (sql, binds) in txs {
            ctx.ssp(
                &sql,
                &binds
                    .iter()
                    .map(|(k, v)| (k.as_str(), json!(v)))
                    .collect::<Vec<_>>(),
            )
            .await?;
        }
        let got = tokio::time::timeout(Duration::from_secs(30), stream.next()).await;
        samples.push((started.elapsed().as_secs_f64() * 1000.0, got.is_ok()));
        // drain anything else this write produced
        while let Ok(Some(_)) = tokio::time::timeout(Duration::from_millis(50), stream.next()).await
        {
        }
    }
    let mut ms: Vec<f64> = samples.iter().map(|s| s.0).collect();
    let misses = samples.iter().filter(|s| !s.1).count();
    ms.sort_by(|a, b| a.partial_cmp(b).unwrap());
    r.out.push(WorkloadResult {
        phase: String::new(),
        id: "live.list_ref_fetch_out".into(),
        group: "functions".into(),
        source: "client LIVE SELECT * FROM _00_list_ref_user_<uid> FETCH out (packages/core/src/query/sql.ts:84), notified by edge publication".into(),
        sql: format!("LIVE SELECT * FROM {table} FETCH out"),
        p50: pct(&ms, 0.5),
        p95: pct(&ms, 0.95),
        ms,
        errors: misses,
        first_error: (misses > 0).then(|| format!("{misses} notifications did not arrive within 30 s")),
        explain: None,
        note: Some("write-to-notification latency of a version bump publication".into()),
    });
    eprintln!(
        "  [{}] live.list_ref_fetch_out: p50 {:.1} ms",
        ctx.target.label,
        r.out.last().unwrap().p50
    );
    Ok(())
}

/// A WhitePawn client's own writes, as the signed-in record user over
/// WebSocket: sp00ky's batch (`remoteBatch`) on games the user owns. Each rep
/// of a `first` workload takes a game no earlier rep touched, so before the
/// ledger migration its `_00_version` row is a legacy one.
async fn group_user(r: &mut Runner<'_>, phase: &str) {
    let ctx = r.ctx;
    let g = "user";
    let user = 0u64;
    let Some(mine) = ctx.games_by_user.get(&user).cloned() else {
        return;
    };
    let session = match user_session(ctx, user).await {
        Ok(s) => s,
        Err(e) => {
            eprintln!("  [{}] record signin failed: {e:#}", ctx.target.label);
            return;
        }
    };
    let s = &session;
    let base = if phase == "before" { 1_000 } else { 3_000 };
    let mine = &mine;
    let merge = "LET $ok0 = { UPDATE $id0 MERGE $data0; true }";
    // the games the first writes touched (a slow first write cuts the
    // samples to three; the second writes must revisit those, not new ones)
    let touched = r
        .time("user.game_merge.first_write", g, "packages/core/src/mutation/rows.ts:399 (remoteBatch), as the game's owner", merge, |i| async move {
            let game = rid("game", mine[(base + i) % mine.len()]);
            db::query(s, merge, &json!({ "id0": format!("r\"{game}\""), "data0": { "event": format!("bench {i}") } })).await.map(|_| ())
        })
        .await
        .map_or(1, |w| w.ms.len().max(1));
    r.time("user.game_merge.again", g, "packages/core/src/mutation/rows.ts:399 (remoteBatch), as the game's owner", merge, |i| async move {
        let game = rid("game", mine[(base + i % touched) % mine.len()]);
        db::query(s, merge, &json!({ "id0": format!("r\"{game}\""), "data0": { "event": format!("again {i}") } })).await.map(|_| ())
    })
    .await;
    let del = "LET $ok0 = { DELETE $id0; true }";
    r.time(
        "user.game_delete",
        g,
        "packages/core/src/mutation/rows.ts:371 (remoteBatch delete), as the game's owner",
        del,
        |i| async move {
            let game = rid("game", mine[(base + 500 + i) % mine.len()]);
            db::query(s, del, &json!({ "id0": format!("r\"{game}\"") }))
                .await
                .map(|_| ())
        },
    )
    .await;
    let create = "LET $ok0 = { CREATE ONLY $id0 SET owner = $d0_owner, database = $d0_database, white = $d0_white, black = $d0_black, pgn = $d0_pgn, result = $d0_result, status = $d0_status, date = $d0_date, sort_index = $d0_sort_index; true }";
    r.time(
        "user.game_create",
        g,
        "packages/core/src/mutation/rows.ts:358 (remoteBatch create), as the user",
        create,
        |_| async move {
            let n = ctx.next();
            db::query(
                s,
                create,
                &json!({
                    "id0": format!("r\"game:bench_u{n}_{}\"", ctx.target.key),
                    "d0_owner": format!("r\"{}\"", rid("user", user)),
                    "d0_database": format!("r\"{}\"", rid("game_database", user)),
                    "d0_white": format!("r\"{}\"", rid("player_name", user)),
                    "d0_black": format!("r\"{}\"", rid("player_name", user + 5000)),
                    "d0_pgn": "1. e4 e5 1-0", "d0_result": "1-0", "d0_status": "finished",
                    "d0_date": "d\"2026-11-15T10:00:00Z\"", "d0_sort_index": -1_794_700_000i64,
                }),
            )
            .await
            .map(|_| ())
        },
    )
    .await;
}

/// The ledger as the migration leaves it, without running it: the table
/// dropped (a range delete) and reloaded with keyed rows from the generator.
async fn rekey_ledger(r: &mut Runner<'_>) -> Result<()> {
    let ctx = r.ctx;
    let started = Instant::now();
    let db = ctx.http.last("INFO FOR DB").await?;
    let table = db
        .pointer("/tables/_00_version")
        .and_then(Value::as_str)
        .context("no _00_version table")?
        .to_string();
    let info = ctx.http.last("INFO FOR TABLE _00_version").await?;
    let mut ddl = vec![table];
    for kind in ["fields", "indexes", "events"] {
        if let Some(m) = info.get(kind).and_then(Value::as_object) {
            ddl.extend(m.values().filter_map(Value::as_str).map(String::from));
        }
    }
    ctx.http.sql("REMOVE TABLE _00_version;").await?;
    ctx.http
        .sql(&(ddl.join(";\n") + ";"))
        .await
        .context("redefine _00_version")?;
    let mut plan = ctx.gen.plan.clone();
    plan.legacy_ledger = 0.0;
    let g = Arc::new(Gen::new(plan, ctx.gen.schema.clone()));
    let rep = gen::load_table(&ctx.http, g, "_00_version", 6).await?;
    let secs = started.elapsed().as_secs_f64();
    eprintln!(
        "  [{}] ledger rekeyed: {} rows in {secs:.0}s",
        ctx.target.label, rep.rows
    );
    r.out.push(WorkloadResult {
        phase: String::new(),
        id: "sched.ledger_rekey".into(),
        group: "pages".into(),
        source: "sdb-parity (the state the migration leaves, without its deletes)".into(),
        sql: "REMOVE TABLE _00_version; DEFINE ...; INSERT keyed rows".into(),
        p50: secs * 1000.0,
        p95: secs * 1000.0,
        ms: vec![secs * 1000.0],
        errors: 0,
        first_error: None,
        explain: None,
        note: Some(format!("{} rows", rep.rows)),
    });
    Ok(())
}

/// The ledger migration the scheduler runs at boot (minutes on 3M rows).
async fn migrate_ledger(r: &mut Runner<'_>) {
    let ctx = r.ctx;
    if !r.wanted("sched.ledger_migration") {
        return;
    }
    struct L<'a>(&'a Ctx);
    impl ssp_protocol::version_ledger::LedgerDb for L<'_> {
        fn query_last(
            &self,
            sql: &str,
        ) -> impl Future<Output = std::result::Result<Value, String>> + Send {
            let sql = sql.to_string();
            let db = self.0.root.clone();
            async move {
                let out = db::query(&db, &sql, &json!({}))
                    .await
                    .map_err(|e| format!("{e:#}"))?;
                Ok(out.into_iter().last().unwrap_or(Value::Null))
            }
        }
        fn pause(&self, ms: u64) -> impl Future<Output = ()> + Send {
            tokio::time::sleep(Duration::from_millis(ms))
        }
    }
    let started = Instant::now();
    let mut steps = Vec::new();
    let mut last = Instant::now();
    let opts = ssp_protocol::version_ledger::MigrationOptions {
        batch: 1000,
        pause_ms: 0,
        ..Default::default()
    };
    let res = ssp_protocol::version_ledger::migrate(&L(ctx), &opts, |_p| {
        steps.push(last.elapsed().as_secs_f64() * 1000.0);
        last = Instant::now();
    })
    .await;
    let mut sorted = steps.clone();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());
    eprintln!(
        "  [{}] ledger migration: {:.0}s, {} steps",
        ctx.target.label,
        started.elapsed().as_secs_f64(),
        steps.len()
    );
    r.out.push(WorkloadResult {
        phase: String::new(),
        id: "sched.ledger_migration.step_1000".into(),
        group: "pages".into(),
        source: "packages/ssp-protocol/src/version_ledger.rs:214 migrate (scheduler boot)".into(),
        sql: "BEGIN; LET $spky_batch = (SELECT id, record_id, version FROM _00_version WHERE id > <cursor> LIMIT 1000); ... COMMIT".into(),
        p50: pct(&sorted, 0.5),
        p95: pct(&sorted, 0.95),
        ms: vec![],
        errors: res.is_err() as usize,
        first_error: res.err(),
        explain: None,
        note: Some(format!("{} steps, {:.0} s in all", steps.len(), started.elapsed().as_secs_f64())),
    });
}

// ------------------------------------------------------------------ mixed load

/// A few minutes of everything at once, at production cadences (the
/// periodic loops at their real intervals, the event-driven paths at a low
/// request rate), with the server's CPU sampled from `docker stats`.
async fn mixed(ctx: Arc<Ctx>, secs: u64) -> Result<MixedResult> {
    let sampler = docker::StatsSampler::start(&ctx.target.container())?;
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let stats: Arc<std::sync::Mutex<BTreeMap<String, Vec<f64>>>> = Default::default();
    let errors: Arc<std::sync::Mutex<BTreeMap<String, (u64, String)>>> = Default::default();
    let mut tasks = Vec::new();
    type Job = Arc<
        dyn Fn(Arc<Ctx>, u64) -> std::pin::Pin<Box<dyn Future<Output = Result<()>> + Send>>
            + Send
            + Sync,
    >;
    let mut loops: Vec<(&str, Duration, Job)> = Vec::new();
    // scheduler
    let engine = Arc::new(schedule_core::engine::ScheduleEngine::new(
        Arc::new(ctx.bench.clone()) as Arc<dyn schedule_core::db::ScheduleDb>,
        Arc::new(schedule_core::kill::NoopJobKill),
        schedule_core::engine::EngineConfig::default(),
    ));
    loops.push((
        "sched.tick",
        Duration::from_secs(5),
        Arc::new(move |_c, _| {
            let e = engine.clone();
            Box::pin(async move { e.tick_pass().await.map(|_| ()) })
        }),
    ));
    loops.push((
        "sched.jobs_sampler",
        Duration::from_secs(15),
        Arc::new(|c, _| {
            Box::pin(async move {
                schedule_core::admin::snapshot(&c.bench)
                    .await
                    .map(|_| ())
                    .map_err(|e| anyhow!("{e:?}"))
            })
        }),
    ));
    loops.push(("sched.flag_sweep", Duration::from_secs(30), Arc::new(|c, _| Box::pin(async move {
        c.q("SELECT key, enabled, default_variant, variants, rules, payloads FROM _00_feature_flag;", json!({})).await?;
        c.q("SELECT VALUE <string>id FROM user;", json!({})).await?;
        for k in gen::FLAG_KEYS {
            c.q("SELECT VALUE <string>user FROM _00_user_feature WHERE key = $key;", json!({ "key": k })).await?;
        }
        Ok(())
    }))));
    loops.push(("sched.presence", Duration::from_secs(15), Arc::new(|c, _| Box::pin(async move {
        c.q("SELECT type::string(id) AS id, auth_id, clientId AS client_id, array::len(subscribers ?? []) AS subscribers, materializationP99 AS p99, errorCount AS errors, rowCount AS row_count, updateCount AS updates FROM _00_query WHERE lastActiveAt + ttl > time::now() LIMIT 20001;", json!({})).await.map(|_| ())
    }))));
    loops.push(("sched.job_recovery", Duration::from_secs(30), Arc::new(|c, _| Box::pin(async move {
        for t in ["job", "gameanalysis_job", "statistics_job", "email_job", "linkpreview_job"] {
            c.q(&format!("SELECT type::string(id) AS id, assignee, ((created_at + <duration>(string::concat(<string>(delay ?? 0), 'ms'))) <= time::now() - 600s) AS long_overdue FROM {t} WHERE status = 'pending' AND updated_at < time::now() - 30s AND (created_at + <duration>(string::concat(<string>(delay ?? 0), 'ms'))) <= time::now()"), json!({})).await?;
            c.q(&format!("SELECT type::string(id) AS id, assignee, lease_until, updated_at FROM {t} WHERE status = 'processing' AND ({} OR assignee != NONE)", schedule_core::sql::LEASE_EXPIRED), json!({})).await?;
        }
        Ok(())
    }))));
    loops.push((
        "sched.drift_counts",
        Duration::from_secs(300),
        Arc::new(|c, _| {
            Box::pin(async move {
                for t in [
                    "game",
                    "player_name",
                    "contact",
                    "game_insight",
                    "notification",
                    "analysis",
                ] {
                    c.q(
                        &format!("SELECT count() AS total FROM {t} GROUP ALL"),
                        json!({}),
                    )
                    .await?;
                }
                Ok(())
            })
        }),
    ));
    loops.push((
        "sched.changefeed_poll",
        Duration::from_secs(2),
        Arc::new(|c, _| {
            Box::pin(async move {
                // packages/maintenance/src/changefeed.rs: the versionstamp is
                // inlined (SINCE takes no parameter) and is `unix_ms << 16`;
                // the poller's cursor trails the newest commit by a poll or two
                let now_ms = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)?
                    .as_millis() as u64;
                let since = (now_ms - 4_000) << 16;
                c.q(&format!("SHOW CHANGES FOR DATABASE SINCE {since} LIMIT 500;"), json!({}))
                    .await
                    .map(|_| ())
            })
        }),
    ));
    // SSP
    loops.push(("ssp.ttl_sweep", Duration::from_secs(60), Arc::new(|c, _| Box::pin(async move {
        c.ssp("UPDATE _00_query SET subscribers = array::filter(subscribers ?? [], |$s| <datetime>$s.seenAt + ttl > time::now()) WHERE array::len(array::filter(subscribers ?? [], |$s| <datetime>$s.seenAt + ttl <= time::now())) > 0", &[]).await?;
        c.ssp("SELECT VALUE (<string>id + '|' + <string>(auth_id OR '')) FROM _00_query WHERE lastActiveAt + ttl < time::now() OR reaping = true", &[]).await?;
        c.ssp("SELECT VALUE auth_id FROM _00_query", &[]).await.map(|_| ())
    }))));
    loops.push((
        "ssp.schema_probe",
        Duration::from_secs(15),
        Arc::new(|c, _| Box::pin(async move { c.ssp("INFO FOR DB", &[]).await.map(|_| ()) })),
    ));
    loops.push((
        "ssp.edge_publish",
        Duration::from_millis(500),
        Arc::new(|c, i| {
            Box::pin(async move {
                let vs: Vec<View> = c
                    .views
                    .iter()
                    .filter(|v| v.user.is_some() && !v.edges.is_empty())
                    .skip((i as usize * 7) % 900)
                    .take(4)
                    .cloned()
                    .collect();
                for (sql, b) in edge_txs(
                    &vs,
                    |v| {
                        delta(
                            v,
                            vec![],
                            vec![],
                            vec![v.edges[i as usize % v.edges.len()].0.clone()],
                            false,
                        )
                    },
                    200 + i as i64,
                ) {
                    c.ssp(
                        &sql,
                        &b.iter()
                            .map(|(k, v)| (k.as_str(), json!(v)))
                            .collect::<Vec<_>>(),
                    )
                    .await?;
                }
                Ok(())
            })
        }),
    ));
    loops.push(("ssp.metrics_flush", Duration::from_secs(2), Arc::new(|c, i| Box::pin(async move {
        let v = &c.views[(i as usize * 13) % c.views.len()];
        c.ssp("UPDATE type::record($id_0) SET rowCount = <int>$rows_0, updateCount = (IF metricsFlush = $flush { updateCount } ELSE { (updateCount ?? 0) + <int>$updates_0 }), metricsFlush = $flush RETURN NONE;", &[("id_0", json!(format!("_00_query:{}", v.key))), ("rows_0", json!(50)), ("updates_0", json!(1)), ("flush", json!(format!("m{i}")))]).await.map(|_| ())
    }))));
    // clients and backends
    loops.push(("client.game_merge", Duration::from_millis(1000), Arc::new(|c, i| Box::pin(async move {
        c.q("LET $ok0 = { UPDATE $id0 MERGE $data0; true }", json!({ "id0": format!("r\"{}\"", c.rec("game", 20_000 + i * 53)), "data0": { "result": "1-0" } })).await.map(|_| ())
    }))));
    loops.push(("backend.insights_done", Duration::from_millis(2000), Arc::new(|c, i| Box::pin(async move {
        c.q("LET $before = (UPDATE type::record($id) SET insights_computed_at = time::now(), insights_attempts = NONE, insights_next_attempt_at = NONE RETURN BEFORE)[0];", json!({ "id": c.rec("game", 40_000 + i * 97) })).await.map(|_| ())
    }))));
    loops.push((
        "backend.analysis_write",
        Duration::from_millis(250),
        Arc::new(|c, i| {
            Box::pin(async move {
                c.q(
                    "UPDATE type::record($id) SET depth = $d, updated_at = time::now()",
                    json!({ "id": c.rec("analysis", 3_000_000 + i * 7919), "d": 24 }),
                )
                .await
                .map(|_| ())
            })
        }),
    ));
    loops.push((
        "client.view_heartbeat",
        Duration::from_millis(1000),
        Arc::new(|c, i| {
            Box::pin(async move {
                let v = &c.views[(i as usize * 31) % c.views.len()];
                c.ssp(
                    "UPDATE type::record($id) SET lastActiveAt = time::now()",
                    &[("id", json!(format!("_00_query:{}", v.key)))],
                )
                .await
                .map(|_| ())
            })
        }),
    ));
    let started = Instant::now();
    // record users editing their own games, as WhitePawn's clients do
    // (sp00ky's `remoteBatch`), each write on a game not written before in
    // this run: before the fixes that is a legacy ledger row, the lookup a
    // record user's write scans the ledger for on 4.0.1
    let mut writers = Vec::new();
    for u in 0..5u64 {
        if let Ok(s) = user_session(&ctx, u).await {
            writers.push((u, s));
        }
    }
    let writers = Arc::new(writers);
    if !writers.is_empty() {
        let writers = writers.clone();
        loops.push((
            "user.record_write",
            Duration::from_millis(2000),
            Arc::new(move |c, _| {
                let writers = writers.clone();
                Box::pin(async move {
                    let k = c.next();
                    let (u, s) = &writers[(k % writers.len() as u64) as usize];
                    let Some(mine) = c.games_by_user.get(u).filter(|m| !m.is_empty()) else {
                        return Ok(());
                    };
                    let game = rid("game", mine[(k / writers.len() as u64) as usize % mine.len()]);
                    db::query(
                        s,
                        "LET $ok0 = { UPDATE $id0 MERGE $data0; true }",
                        &json!({ "id0": format!("r\"{game}\""), "data0": { "event": format!("mixed {k}") } }),
                    )
                    .await
                    .map(|_| ())
                })
            }),
        ));
    }
    for (name, every, job) in loops {
        let ctx = ctx.clone();
        let stop = stop.clone();
        let stats = stats.clone();
        let errors = errors.clone();
        tasks.push(tokio::spawn(async move {
            let mut i = 0u64;
            let mut tick = tokio::time::interval(every);
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            while !stop.load(Ordering::Relaxed) {
                tick.tick().await;
                if stop.load(Ordering::Relaxed) {
                    break;
                }
                let t0 = Instant::now();
                let r = tokio::time::timeout(Duration::from_secs(60), job(ctx.clone(), i)).await;
                let failed = match r {
                    Ok(Ok(())) => None,
                    Ok(Err(e)) => Some(db::truncate(&format!("{e:#}"), 300)),
                    Err(_) => Some("timed out after 60 s".to_string()),
                };
                if let Some(e) = failed {
                    let mut errors = errors.lock().unwrap();
                    let entry = errors.entry(name.to_string()).or_insert((0, e));
                    entry.0 += 1;
                }
                stats
                    .lock()
                    .unwrap()
                    .entry(name.to_string())
                    .or_default()
                    .push(t0.elapsed().as_secs_f64() * 1000.0);
                i += 1;
            }
        }));
    }
    // a few record users holding LIVE queries on their list_ref tables
    let mut lives = Vec::new();
    for u in 0..5u64 {
        if let Ok(s) = user_session(&ctx, u).await {
            if let Ok(mut resp) = s
                .query(format!(
                    "LIVE SELECT * FROM {} FETCH out",
                    views::list_ref_table(u)
                ))
                .await
            {
                if let Ok(stream) = resp.stream::<surrealdb::types::Value>(0) {
                    let stop = stop.clone();
                    lives.push(tokio::spawn(async move {
                        use futures::StreamExt;
                        let mut stream = stream;
                        let _s = s;
                        while !stop.load(Ordering::Relaxed) {
                            let _ =
                                tokio::time::timeout(Duration::from_secs(1), stream.next()).await;
                        }
                    }));
                }
            }
        }
    }
    tokio::time::sleep(Duration::from_secs(secs)).await;
    stop.store(true, Ordering::Relaxed);
    for t in tasks {
        let _ = tokio::time::timeout(Duration::from_secs(90), t).await;
    }
    for l in lives {
        l.abort();
    }
    let samples = sampler.stop().await;
    let errors_by_task = errors.lock().unwrap().clone();
    let cpu: Vec<f64> = samples.iter().map(|s| s.cpu).collect();
    let mut sorted = cpu.clone();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let stats = stats.lock().unwrap().clone();
    let per_task = stats
        .iter()
        .map(|(k, v)| {
            let mut s = v.clone();
            s.sort_by(|a, b| a.partial_cmp(b).unwrap());
            (k.clone(), (v.len() as u64, pct(&s, 0.5), pct(&s, 0.95)))
        })
        .collect();
    Ok(MixedResult {
        secs: started.elapsed().as_secs(),
        requests: stats.values().map(|v| v.len() as u64).sum(),
        errors: errors_by_task.values().map(|e| e.0).sum(),
        cpu_mean: if cpu.is_empty() {
            0.0
        } else {
            cpu.iter().sum::<f64>() / cpu.len() as f64
        },
        cpu_p95: pct(&sorted, 0.95),
        cpu_max: sorted.last().copied().unwrap_or(0.0),
        mem_max_mib: samples.iter().map(|s| s.mem_mib).fold(0.0, f64::max),
        per_task,
        cpu_series: samples.iter().map(|s| (s.t_ms / 1000, s.cpu)).collect(),
        errors_by_task,
        drain_secs: None,
    })
}

// ------------------------------------------------------------------ driver

/// Wait until no index is still being built (the patch builds CONCURRENTLY).
async fn wait_for_indexes(ctx: &Ctx) -> Result<()> {
    let started = Instant::now();
    loop {
        let mut building = 0;
        for (t, _) in ctx.gen.schema.iter() {
            let info = ctx.http.last(&format!("INFO FOR TABLE `{t}`")).await?;
            let Some(ix) = info.get("indexes").and_then(Value::as_object) else {
                continue;
            };
            for name in ix.keys() {
                let i = ctx
                    .http
                    .last(&format!("INFO FOR INDEX `{name}` ON `{t}`"))
                    .await
                    .unwrap_or(Value::Null);
                if let Some(st) = i.pointer("/building/status").and_then(Value::as_str) {
                    if st != "ready" && !st.is_empty() {
                        building += 1;
                    }
                }
            }
        }
        if building == 0 {
            eprintln!(
                "  [{}] indexes ready after {:.0}s",
                ctx.target.label,
                started.elapsed().as_secs_f64()
            );
            return Ok(());
        }
        tokio::time::sleep(Duration::from_secs(5)).await;
    }
}

async fn slow_log(target: &Target) -> Vec<(String, u64, f64)> {
    slow_log_of(&docker::logs(&target.container(), None).await)
}

/// The slow-query lines of a server log, grouped by statement, by total time.
pub fn slow_log_of(logs: &str) -> Vec<(String, u64, f64)> {
    let mut by: BTreeMap<String, (u64, f64)> = BTreeMap::new();
    for line in logs.lines() {
        let Some(i) = line.find("Slow query detected - time: ") else {
            continue;
        };
        let rest = &line[i + 28..];
        let Some((time, q)) = rest.split_once(" - query: ") else {
            continue;
        };
        let ms = parse_duration_ms(time);
        let q = q.split(" - params:").next().unwrap_or(q);
        let shape = db::truncate(q, 220);
        let e = by.entry(shape).or_default();
        e.0 += 1;
        e.1 += ms;
    }
    let mut out: Vec<(String, u64, f64)> = by.into_iter().map(|(k, (n, ms))| (k, n, ms)).collect();
    out.sort_by(|a, b| b.2.partial_cmp(&a.2).unwrap());
    out.truncate(25);
    out
}

fn parse_duration_ms(s: &str) -> f64 {
    let s = s.trim();
    let (num, unit) = s.split_at(
        s.find(|c: char| c.is_ascii_alphabetic() || c == 'µ')
            .unwrap_or(s.len()),
    );
    let n: f64 = num.parse().unwrap_or(0.0);
    match unit {
        "s" => n * 1000.0,
        "ms" => n,
        "µs" | "us" => n / 1000.0,
        "ns" => n / 1e6,
        "m" => n * 60_000.0,
        _ => n,
    }
}

pub async fn run_all(targets: &[Target], opts: &RunOptions) -> Result<Value> {
    let stub = crate::stub::serve(crate::prepare::ENDPOINT_PORT).await?;
    let mut results = Vec::new();
    for t in targets {
        match run_target(t, opts).await {
            Ok(r) => results.push(r),
            Err(e) => {
                eprintln!("[{}] failed: {e:#}", t.label);
                docker::stop(t).await;
                return Err(e);
            }
        }
    }
    stub.abort();
    Ok(serde_json::to_value(results)?)
}

async fn run_target(t: &Target, opts: &RunOptions) -> Result<TargetResult> {
    // a server left running by an interrupted run must not see its volume
    // replaced under it
    docker::stop(t).await;
    let volume = if opts.reset {
        docker::copy_volume(&t.data_volume(), &t.run_volume()).await?;
        t.run_volume()
    } else {
        t.data_volume()
    };
    eprintln!("[{}] starting on {volume}", t.label);
    docker::start(t, &volume, Mode::Run).await?;
    let result = run_on(t, opts).await;
    docker::stop(t).await;
    result
}

async fn run_on(t: &Target, opts: &RunOptions) -> Result<TargetResult> {
    let http = Http::new(&t.url());
    let plan = crate::prepare::plan_of(&http).await?;
    let schema = gen::read_schema(&http).await?;
    let gen = Arc::new(Gen::new(plan, schema));
    let views = views::plan_views(&gen);
    let games_by_user = views::games_of_view_users(&gen);
    let root = db::root_http(&t.url()).await?;
    let ctx = Arc::new(Ctx {
        target: t.clone(),
        http: http.clone(),
        bench: BenchDb { db: root.clone() },
        root,
        gen,
        views,
        games_by_user,
        seq: AtomicU64::new(
            (std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)?
                .as_millis() as u64)
                % 1_000_000_000,
        ),
    });
    freshen(&ctx).await?;
    let server = http.version().await.unwrap_or_default();
    let fixes = opts.apply_schema.is_some()
        || opts.app_patch.is_some()
        || opts.migrate_ledger
        || opts.rekey_ledger;
    let mut phases: Vec<&str> = if fixes {
        vec!["before", "after"]
    } else {
        vec!["before"]
    };
    if let Some(only) = &opts.phases {
        phases.retain(|p| only.iter().any(|o| o == p));
    }
    let mut out = TargetResult {
        target: t.key.to_string(),
        label: t.label.to_string(),
        server,
        workloads: Vec::new(),
        mixed: Vec::new(),
        slow_log: Vec::new(),
    };
    for phase in phases {
        let mut r = Runner {
            ctx: &ctx,
            opts,
            out: Vec::new(),
        };
        let mut ledger = "legacy";
        let waited = docker::settle(&t.container(), 30.0, Duration::from_secs(900)).await;
        if waited > Duration::from_secs(10) {
            eprintln!("[{}] server busy for {:.0}s before the {phase} phase", t.label, waited.as_secs_f64());
        }
        ctx.reauth().await;
        if phase == "after" {
            eprintln!("[{}] applying the fixes", t.label);
            if let Some(spky) = &opts.apply_schema {
                apply_schema(&ctx, spky, &opts.app).await?;
            }
            if let Some(patch) = &opts.app_patch {
                let sql = std::fs::read_to_string(patch)?;
                ctx.http.sql(&sql).await.context("app patch")?;
                wait_for_indexes(&ctx).await?;
                eprintln!("  [{}] applied {}", t.label, patch.display());
            }
            if opts.migrate_ledger {
                migrate_ledger(&mut r).await;
                ledger = "keyed";
            } else if opts.rekey_ledger {
                rekey_ledger(&mut r).await?;
                ledger = "keyed";
            }
        }
        // the record user's writes last, on a warm server: right after a
        // start SurrealKV's block cache is empty and every first read is slow
        ctx.reauth().await;
        group_pages(&mut r, ledger).await;
        ctx.reauth().await;
        group_ssp(&mut r).await;
        ctx.reauth().await;
        group_scheduler(&mut r).await;
        ctx.reauth().await;
        group_jobs(&mut r).await;
        group_functions(&mut r).await;
        ctx.reauth().await;
        group_writes(&mut r).await;
        group_user(&mut r, phase).await;
        for mut w in r.out {
            w.phase = phase.to_string();
            out.workloads.push(w);
        }
        if opts.mixed_secs > 0
            && opts
                .only
                .as_ref()
                .map_or(true, |o| o.iter().any(|s| s == "mixed"))
        {
            eprintln!(
                "[{}] {phase}: mixed load for {} s",
                t.label, opts.mixed_secs
            );
            ctx.reauth().await;
            let mut m = mixed(ctx.clone(), opts.mixed_secs).await?;
            let drain = docker::settle(&t.container(), 30.0, Duration::from_secs(900)).await;
            m.drain_secs = Some(drain.as_secs_f64());
            eprintln!("[{}] {phase}: busy for {:.0}s after the load stopped", t.label, drain.as_secs_f64());
            eprintln!(
                "[{}] {phase}: mixed cpu mean {:.0}% p95 {:.0}% max {:.0}%, {} requests, {} errors",
                t.label, m.cpu_mean, m.cpu_p95, m.cpu_max, m.requests, m.errors
            );
            for (task, (n, e)) in &m.errors_by_task {
                eprintln!("  [{}] {phase}: {task}: {n} errors, first: {e}", t.label);
            }
            out.mixed.push((phase.to_string(), m));
        }
    }
    out.slow_log = slow_log(t).await;
    Ok(out)
}

#[cfg(test)]
mod tests {
    /// Queries copied from sp00ky's private code must match their source,
    /// whitespace-normalised, so the suite measures what the code sends.
    #[test]
    fn queries_match_their_source() {
        let root = concat!(env!("CARGO_MANIFEST_DIR"), "/../..");
        let norm = |s: &str| {
            s.split_whitespace()
                .collect::<Vec<_>>()
                .join(" ")
                .replace("\\ ", "")
        };
        let checks: &[(&str, &str)] = &[
            ("packages/ssp-node/src/node.rs", "LET $spky_rid = type::record($rid); RETURN type::record('_00_version', [$spky_rid]).version ?? (SELECT VALUE version FROM ONLY _00_version WHERE record_id = $spky_rid LIMIT 1);"),
            ("packages/ssp-node/src/node.rs", "SELECT VALUE (<string>id + '|' + <string>(auth_id OR '')) FROM _00_query WHERE lastActiveAt + ttl < time::now() OR reaping = true"),
            ("packages/ssp-node/src/tables.rs", "SELECT VALUE count() FROM _00_query WHERE auth_id = $a GROUP ALL"),
            ("apps/scheduler/src/feature_flags.rs", "SELECT VALUE <string>user FROM _00_user_feature WHERE key = $key"),
            ("apps/scheduler/src/feature_flags.rs", "SELECT key, enabled, default_variant, variants, rules, payloads FROM _00_feature_flag"),
            ("apps/scheduler/src/admin/presence.rs", "FROM _00_query WHERE lastActiveAt + ttl > time::now() LIMIT {};"),
            ("apps/scheduler/src/drift.rs", "SELECT count() AS total FROM {} GROUP ALL"),
            ("apps/scheduler/src/job_scheduler.rs", "WHERE status = 'processing' AND ({expired} OR assignee != NONE)"),
        ];
        for (file, q) in checks {
            let src = std::fs::read_to_string(format!("{root}/{file}"))
                .unwrap_or_else(|e| panic!("{file}: {e}"));
            let src = norm(&src.replace("\\\n", " "));
            assert!(src.contains(&norm(q)), "{file} no longer contains: {q}");
        }
    }
}

/// Diagnostics: the same edit as the WebSocket session the suite uses and as
/// an HTTP request with the user's token.
pub async fn probe_user_write(url: &str, start: u64) -> Result<()> {
    let root = db::root_http(url).await?;
    let user = db::query(
        &root,
        "SELECT VALUE id FROM ONLY user WHERE email = 'bench0@example.test' LIMIT 1",
        &json!({}),
    )
    .await?
    .pop()
    .unwrap_or_default();
    let games = db::query(
        &root,
        &format!(
            "SELECT VALUE id FROM game WHERE owner = {} LIMIT 4 START {start}",
            user.as_str().unwrap_or("")
        ),
        &json!({}),
    )
    .await?
    .pop()
    .unwrap_or_default();
    let ws = surrealdb::engine::any::connect(url.replacen("http://", "ws://", 1)).await?;
    ws.signin(surrealdb::opt::auth::Record {
        namespace: db::NS.into(),
        database: db::DB.into(),
        access: "account".into(),
        params: json!({ "email": "bench0@example.test", "password": gen::BENCH_PASSWORD }),
    })
    .await?;
    let merge = "LET $ok0 = { UPDATE $id0 MERGE $data0; true }";
    for (i, g) in games
        .as_array()
        .cloned()
        .unwrap_or_default()
        .iter()
        .enumerate()
    {
        let g = g.as_str().unwrap_or("").to_string();
        let mut line = format!("{g}:");
        for k in 0..3 {
            let t0 = Instant::now();
            let r = db::query(&ws, merge, &json!({ "id0": format!("r\"{g}\""), "data0": { "event": format!("probe {i} {k}") } })).await;
            line.push_str(&format!(
                " ws#{k} {:.0} ms{}",
                t0.elapsed().as_secs_f64() * 1000.0,
                match &r {
                    Err(e) => format!(" ERR {}", db::truncate(&format!("{e:#}"), 200)),
                    Ok(v) => format!(" -> {}", db::truncate(&serde_json::to_string(v).unwrap_or_default(), 80)),
                }
            ));
            let ledger = db::query(
                &root,
                &format!("RETURN (SELECT id, version FROM _00_version WHERE record_id = {g});"),
                &json!({}),
            )
            .await
            .map(|v| serde_json::to_string(&v).unwrap_or_default())
            .unwrap_or_else(|e| format!("{e:#}"));
            line.push_str(&format!(" [ledger {}]", db::truncate(&ledger, 160)));
        }
        let t0 = Instant::now();
        let _ = db::query(&ws, "RETURN 1", &json!({})).await;
        line.push_str(&format!(
            " | ws RETURN 1 {:.0} ms",
            t0.elapsed().as_secs_f64() * 1000.0
        ));
        println!("{line}");
    }
    Ok(())
}
