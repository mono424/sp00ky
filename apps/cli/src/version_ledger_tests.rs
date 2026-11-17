//! The `_00_version` ledger at a realistic size, against the schema the CLI
//! generates: the cost of one synced write through the generated events, the
//! cost of the per-row `_00_rv` projection the SSP and the scheduler scan
//! with, and the migration of legacy (random id) ledger rows to keyed ones.
//!
//! The bug this guards against (2026-11-16, WhitePawn on SurrealDB 4.0.1): a
//! ledger lookup by `record_id` inside an event body was planned as a table
//! scan, so ONE write cost a scan of the whole ledger (14 s on 3M rows). The
//! cost assertions here are relative to a write without events, so a template
//! or a SurrealDB release that turns a ledger access back into a scan fails
//! them, on the embedded engine and on the servers alike. See
//! `docs/version-ledger.md`.
//!
//! The embedded SurrealDB 4.0.1 (kv-mem) tests run with `cargo test`. The same
//! checks against the 3.3.5 and 4.0.1 servers need Docker and are ignored by
//! default; they skip (pass) when Docker is unavailable:
//!
//! ```sh
//! cargo test -p sp00ky-cli version_ledger -- --include-ignored --test-threads=1
//! ```

use std::collections::HashMap;
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde_json::Value;
use ssp_protocol::version_ledger::{self, LedgerDb, MigrationOptions};
use surrealdb::engine::local::{Db, Mem};
use surrealdb::Surreal;

use crate::backend::DeployMode;
use crate::schema_builder::{build_server_events, build_server_schema, SchemaBuilderConfig};

/// Ledger rows of records other than the test's own: the bulk a long-lived
/// app accumulates. 100k is enough for a scan to cost two orders of magnitude
/// more than a key read on every engine tested.
const FILLER_ROWS: usize = 100_000;

/// A small app: one synced table.
const APP_SCHEMA: &str = "DEFINE TABLE job SCHEMAFULL PERMISSIONS FULL;\n\
DEFINE FIELD status ON TABLE job TYPE string;\n\
DEFINE FIELD n ON TABLE job TYPE int;\n";

/// The internal schema `spky` applies for [`APP_SCHEMA`], split where an
/// existing app's upgrade splits it: the tables (applied first, then data
/// written the old way), and the generated events (applied last). Cluster
/// mode with the changefeed transport, so the events post nothing: there is
/// no SSP in this test, and the ledger statements are the same under both
/// transports.
fn generated_schema() -> (String, String) {
    let dir = tempfile::tempdir().expect("tempdir");
    let schema = dir.path().join("schema.surql");
    std::fs::write(&schema, APP_SCHEMA).unwrap();
    let config = dir.path().join("sp00ky.yml");
    std::fs::write(&config, "sync:\n  transport: changefeed\n").unwrap();
    let builder = SchemaBuilderConfig {
        input_path: schema,
        config_path: Some(config),
        mode: DeployMode::Cluster,
        endpoint: None,
        secret: None,
        include_functions: false,
    };
    let tables = build_server_schema(&builder).expect("server schema");
    let events = build_server_events(&builder).expect("server events");
    assert!(
        events.contains("DEFINE EVENT OVERWRITE _00_job_mutation ON TABLE job"),
        "{events}"
    );
    (tables, events)
}

/// Where the statements run: the embedded engine or a SurrealDB server.
#[derive(Clone)]
enum Target {
    Embedded(Surreal<Db>),
    Server { port: u16 },
}

impl Target {
    async fn embedded() -> Self {
        let db = Surreal::new::<Mem>(()).await.expect("mem db");
        db.use_ns("t").use_db("t").await.expect("ns/db");
        Target::Embedded(db)
    }

    /// Every statement's value, or the first error.
    async fn sql(&self, sql: &str) -> Result<Vec<Value>, String> {
        match self {
            Target::Embedded(db) => {
                let mut response = db.query(sql).await.map_err(|e| e.to_string())?;
                let n = response.num_statements();
                let mut out = Vec::with_capacity(n);
                let mut errors = Vec::new();
                for i in 0..n {
                    match response.take::<surrealdb::types::Value>(i) {
                        Ok(v) => out.push(v.into_json_value()),
                        Err(e) => errors.push(e.to_string()),
                    }
                }
                match version_ledger::cause(&errors) {
                    Some(e) => Err(e),
                    None => Ok(out),
                }
            }
            Target::Server { port } => {
                let (port, sql) = (*port, sql.to_string());
                tokio::task::spawn_blocking(move || server_sql(port, &sql))
                    .await
                    .map_err(|e| e.to_string())?
            }
        }
    }

    async fn ok(&self, sql: &str) -> Vec<Value> {
        self.sql(sql)
            .await
            .unwrap_or_else(|e| panic!("{}\n-> {e}", short(sql)))
    }

    async fn last(&self, sql: &str) -> Value {
        self.ok(sql).await.pop().unwrap_or(Value::Null)
    }

    /// What the server reports from `/version`, as the SSP and scheduler read it.
    async fn server_version(&self) -> String {
        match self {
            Target::Embedded(db) => db
                .version()
                .await
                .map(|v| v.to_string())
                .unwrap_or_default(),
            Target::Server { port } => {
                let url = format!("http://127.0.0.1:{port}/version");
                ureq::get(&url)
                    .call()
                    .ok()
                    .and_then(|r| r.into_string().ok())
                    .unwrap_or_default()
            }
        }
    }

    /// The [`ssp_protocol::VersionLookup`] a scan resolves to right now, the
    /// way the SSP and scheduler resolve it.
    async fn version_lookup(&self) -> ssp_protocol::VersionLookup {
        let legacy = version_ledger::head_is_legacy(&self.last(version_ledger::HEAD_QUERY).await);
        let version = self.server_version().await;
        ssp_protocol::VersionLookup::for_ledger(Some(&version), legacy)
    }
}

impl LedgerDb for Target {
    async fn query_last(&self, sql: &str) -> Result<Value, String> {
        self.sql(sql)
            .await
            .map(|mut v| v.pop().unwrap_or(Value::Null))
    }

    async fn pause(&self, ms: u64) {
        tokio::time::sleep(Duration::from_millis(ms)).await;
    }
}

fn short(sql: &str) -> &str {
    &sql[..sql.len().min(400)]
}

fn server_sql(port: u16, sql: &str) -> Result<Vec<Value>, String> {
    let url = format!("http://127.0.0.1:{port}/sql");
    let body = match ureq::post(&url)
        .set("Authorization", "Basic cm9vdDpyb290")
        .set("surreal-ns", "t")
        .set("surreal-db", "t")
        .set("Accept", "application/json")
        .timeout(Duration::from_secs(600))
        .send_string(sql)
    {
        Ok(r) => r.into_string().map_err(|e| e.to_string())?,
        Err(ureq::Error::Status(_, r)) => r.into_string().map_err(|e| e.to_string())?,
        Err(e) => return Err(e.to_string()),
    };
    let parsed: Value = serde_json::from_str(&body).map_err(|e| format!("{e}: {body}"))?;
    let Value::Array(results) = parsed else {
        return Err(body);
    };
    let mut out = Vec::with_capacity(results.len());
    let mut errors = Vec::new();
    for r in results {
        let result = r.get("result").cloned().unwrap_or(Value::Null);
        match r.get("status").and_then(Value::as_str) {
            Some("OK") => out.push(result),
            _ => errors.push(
                result
                    .as_str()
                    .map(str::to_owned)
                    .unwrap_or_else(|| result.to_string()),
            ),
        }
    }
    match version_ledger::cause(&errors) {
        Some(e) => Err(e),
        None => Ok(out),
    }
}

/// An existing app being upgraded: the tables, `jobs` job rows each with a
/// legacy ledger row (version 5), with `filler` [`FILLER_ROWS`] more legacy
/// ledger rows, and only then the generated events. A table without events,
/// `plain`, is the baseline a page is measured against.
async fn upgraded_app(t: &Target, jobs: usize, filler: bool) {
    let (tables, events) = generated_schema();
    t.ok(&tables).await;
    t.ok("DEFINE TABLE plain SCHEMALESS PERMISSIONS FULL;")
        .await;
    seed_rows(t, "plain", jobs).await;
    for start in (0..jobs).step_by(5000) {
        let end = (start + 5000).min(jobs);
        t.ok(&format!(
            "INSERT INTO job (array::map(array::range({start}, {end}), |$i| {{ id: type::record('job', 'j' + <string>$i), status: 'pending', n: 0 }})) RETURN NONE;\n\
             INSERT INTO _00_version (array::map(array::range({start}, {end}), |$i| {{ record_id: type::record('job', 'j' + <string>$i), version: 5 }})) RETURN NONE;"
        ))
        .await;
    }
    if filler {
        seed_filler(t).await;
    }
    t.ok(&events).await;
    let head = t.last(version_ledger::HEAD_QUERY).await;
    assert!(
        version_ledger::head_is_legacy(&head),
        "the seeded ledger is a legacy one: {head}"
    );
}

/// [`FILLER_ROWS`] legacy ledger rows of records other than the test's own.
async fn seed_filler(t: &Target) {
    for start in (0..FILLER_ROWS).step_by(5000) {
        let end = (start + 5000).min(FILLER_ROWS);
        t.ok(&format!(
            "INSERT INTO _00_version (array::map(array::range({start}, {end}), |$i| {{ record_id: type::record('filler', 'f' + <string>$i), version: 3 }})) RETURN NONE;"
        ))
        .await;
    }
}

async fn seed_rows(t: &Target, table: &str, n: usize) {
    for start in (0..n).step_by(5000) {
        let end = (start + 5000).min(n);
        t.ok(&format!(
            "INSERT INTO {table} (array::map(array::range({start}, {end}), |$i| {{ id: type::record('{table}', 'j' + <string>$i), status: 'pending', n: 0 }})) RETURN NONE;"
        ))
        .await;
    }
}

/// The ledger version of `record` (a literal record id), keyed or legacy.
async fn version_of(t: &Target, record: &str) -> Option<i64> {
    let v = t
        .last(&format!(
            "RETURN type::record('_00_version', [{record}]).version ?? (SELECT VALUE version FROM _00_version WHERE record_id = {record})[0];"
        ))
        .await;
    v.as_i64()
}

/// The median time of `n` writes, the `i`-th one being `sql(i)`.
async fn median_write(t: &Target, n: usize, sql: impl Fn(usize) -> String) -> Duration {
    let mut times = Vec::with_capacity(n);
    for i in 0..n {
        let q = sql(i);
        let start = Instant::now();
        t.ok(&q).await;
        times.push(start.elapsed());
    }
    times.sort();
    times[n / 2]
}

/// The event paths a synced write can take, each timed over `N` records from
/// `o` on (`job:j{o}..`, `job:new{o}..`, `job:j{500 + o}..`).
const N: usize = 5;
const PATHS: [&str; 5] = [
    "UPDATE, legacy ledger row (first write)",
    "UPDATE, keyed ledger row",
    "CREATE",
    "DELETE, keyed ledger row",
    "DELETE, legacy ledger row",
];

async fn write_paths(t: &Target, o: usize) -> [Duration; 5] {
    [
        median_write(t, N, |i| {
            format!("UPDATE job:j{} SET n += 1 RETURN NONE;", o + i)
        })
        .await,
        median_write(t, N, |i| {
            format!("UPDATE job:j{} SET n += 1 RETURN NONE;", o + i)
        })
        .await,
        median_write(t, N, |i| {
            format!(
                "CREATE job:new{} SET status = 'pending', n = 0 RETURN NONE;",
                o + i
            )
        })
        .await,
        median_write(t, N, |i| format!("DELETE job:new{} RETURN NONE;", o + i)).await,
        median_write(t, N, |i| {
            format!("DELETE job:j{} RETURN NONE;", 500 + o + i)
        })
        .await,
    ]
}

/// The versions [`write_paths`] leaves behind for its records.
async fn check_write_paths(t: &Target, o: usize) {
    for i in o..o + N {
        assert_eq!(
            version_of(t, &format!("job:j{i}")).await,
            Some(7),
            "job:j{i}: 5, carried over and bumped twice"
        );
        let keyed = t
            .last(&format!("RETURN record::exists(_00_version:[job:j{i}]);"))
            .await;
        assert_eq!(
            keyed,
            Value::Bool(true),
            "job:j{i}: the first write moves the row to its keyed id"
        );
        assert_eq!(
            version_of(t, &format!("job:new{i}")).await,
            None,
            "a deleted record leaves no ledger row"
        );
        assert_eq!(
            version_of(t, &format!("job:j{}", 500 + i)).await,
            None,
            "nor does a legacy one"
        );
    }
}

/// Every event path costs the same on a [`FILLER_ROWS`] ledger as on a
/// 1000-row one: no ledger access in the generated events depends on the
/// ledger's size, which a scan does (measured with the templates this
/// replaced, embedded 4.0.1: 6.1 s per UPDATE at 100k rows). The versions come
/// out right on every path.
async fn synced_writes_stay_cheap(t: &Target) {
    upgraded_app(t, 1000, false).await;
    let small = write_paths(t, 0).await;
    seed_filler(t).await;
    let large = write_paths(t, 100).await;
    for ((what, small), large) in PATHS.iter().zip(small).zip(large) {
        let bound = small * 2 + Duration::from_millis(20);
        eprintln!(
            "  {what}: {large:?} on {FILLER_ROWS} ledger rows, {small:?} on 1000 (bound {bound:?})"
        );
        assert!(
            large <= bound,
            "{what} took {large:?} on a {FILLER_ROWS}-row ledger and {small:?} on a 1000-row one: \
             a ledger access in the generated events is scanning the ledger"
        );
    }
    check_write_paths(t, 0).await;
    check_write_paths(t, 100).await;
    // A re-created record starts over.
    t.ok("CREATE job:new0 SET status = 'pending', n = 0 RETURN NONE; UPDATE job:new0 SET n = 1 RETURN NONE;").await;
    assert_eq!(version_of(t, "job:new0").await, Some(2));
}

/// The projection an SSP bootstrap / scheduler clone page reads, over 1000
/// rows of a [`FILLER_ROWS`] ledger: before the migration (keyed read, legacy
/// fallback in this server's form) and after it (keyed only).
async fn version_projection_stays_cheap(t: &Target) {
    upgraded_app(t, 1000, true).await;
    let page = "SELECT * FROM job ORDER BY id LIMIT 1000";
    let started = Instant::now();
    let plain = t.last("SELECT * FROM plain ORDER BY id LIMIT 1000").await;
    let baseline = started.elapsed();
    assert_eq!(plain.as_array().map(Vec::len), Some(1000));

    let check = |rows: &Value, want: i64| {
        let rows = rows.as_array().expect("rows");
        assert_eq!(rows.len(), 1000);
        for row in rows {
            assert_eq!(
                row.get("_00_rv").and_then(Value::as_i64),
                Some(want),
                "{row}"
            );
        }
    };

    let lookup = t.version_lookup().await;
    assert_ne!(
        lookup,
        ssp_protocol::VersionLookup::Keyed,
        "legacy rows remain"
    );
    let q = ssp_protocol::with_durable_row_versions_using(page, lookup);
    let started = Instant::now();
    let rows = t.last(&q).await;
    let legacy_cost = started.elapsed();
    check(&rows, 5);
    eprintln!("  1000-row page before the migration ({lookup:?}): {legacy_cost:?}");

    let progress = version_ledger::migrate(
        t,
        &MigrationOptions {
            pause_ms: 0,
            ..Default::default()
        },
        |_| {},
    )
    .await
    .expect("migration");
    assert!(progress.done);
    assert_eq!(progress.moved, (1000 + FILLER_ROWS) as u64);

    let lookup = t.version_lookup().await;
    assert_eq!(
        lookup,
        ssp_protocol::VersionLookup::Keyed,
        "no legacy row left"
    );
    let q = ssp_protocol::with_durable_row_versions_using(page, lookup);
    let started = Instant::now();
    let rows = t.last(&q).await;
    let keyed_cost = started.elapsed();
    check(&rows, 5);
    let bound = baseline * 20 + Duration::from_millis(100);
    eprintln!("  1000-row page after the migration (Keyed): {keyed_cost:?} (plain page {baseline:?}, bound {bound:?})");
    assert!(
        keyed_cost <= bound,
        "a 1000-row page with keyed versions took {keyed_cost:?}, over {bound:?} (the same page without versions: {baseline:?})"
    );
}

/// Legacy rows become keyed with their versions, a second run finds nothing,
/// and the readers stop falling back.
async fn migration_keeps_versions(t: &Target) {
    let (tables, events) = generated_schema();
    t.ok(&tables).await;
    t.ok("INSERT INTO job (array::map(array::range(0, 3000), |$i| { id: type::record('job', 'j' + <string>$i), status: 'pending', n: 0 })) RETURN NONE;\n\
          INSERT INTO _00_version (array::map(array::range(0, 3000), |$i| { record_id: type::record('job', 'j' + <string>$i), version: $i % 37 + 1 })) RETURN NONE;\n\
          INSERT INTO _00_version [{ id: 42, record_id: job:numeric, version: 9 }];").await;
    t.ok(&events).await;
    let mut batches = 0;
    let opts = MigrationOptions {
        batch: 250,
        pause_ms: 0,
        ..Default::default()
    };
    let progress = version_ledger::migrate(t, &opts, |_| batches += 1)
        .await
        .expect("migration");
    assert!(progress.done, "{progress:?}");
    assert_eq!(progress.moved, 3001);
    assert!(batches >= 3001 / 250, "batched: {batches}");
    let counts = t
        .last("RETURN { all: count(SELECT id FROM _00_version), keyed: count(SELECT id FROM _00_version WHERE type::is_array(record::id(id))) };")
        .await;
    assert_eq!(counts["all"], 3001, "{counts}");
    assert_eq!(counts["keyed"], 3001, "{counts}");
    for i in [0usize, 1, 36, 37, 2999] {
        let v = t
            .last(&format!("RETURN _00_version:[job:j{i}].version;"))
            .await;
        assert_eq!(v.as_i64(), Some((i % 37 + 1) as i64), "job:j{i}");
    }
    assert_eq!(
        t.last("RETURN _00_version:[job:numeric].version;")
            .await
            .as_i64(),
        Some(9)
    );
    let again = version_ledger::migrate(t, &opts, |_| {})
        .await
        .expect("second run");
    assert!(again.done);
    assert_eq!(again.moved, 0, "idempotent");
    assert_eq!(t.version_lookup().await, ssp_protocol::VersionLookup::Keyed);
}

/// Writes through the generated events while the migration runs: no write is
/// lost (each record's version is its seeded one plus its committed writes),
/// one ledger row per record, and nothing fails but retryable conflicts.
async fn migration_under_concurrent_writes(t: &Target) {
    const JOBS: usize = 4000;
    const WRITERS: usize = 4;
    const WRITES: usize = 250;
    let (tables, events) = generated_schema();
    t.ok(&tables).await;
    t.ok(&format!(
        "INSERT INTO job (array::map(array::range(0, {JOBS}), |$i| {{ id: type::record('job', 'j' + <string>$i), status: 'pending', n: 0 }})) RETURN NONE;\n\
         INSERT INTO _00_version (array::map(array::range(0, {JOBS}), |$i| {{ record_id: type::record('job', 'j' + <string>$i), version: $i % 11 + 1 }})) RETURN NONE;"
    ))
    .await;
    t.ok(&events).await;

    let conflicts = Arc::new(AtomicU64::new(0));
    let mut writers = Vec::new();
    for w in 0..WRITERS {
        let t = t.clone();
        let conflicts = conflicts.clone();
        writers.push(tokio::spawn(async move {
            let mut committed: HashMap<usize, i64> = HashMap::new();
            // Spread over the key space, so writes land behind, at and ahead
            // of the migration's cursor.
            let mut k = w * 7919 % JOBS;
            for _ in 0..WRITES {
                k = (k * 31 + 17) % JOBS;
                loop {
                    match t
                        .sql(&format!("UPDATE job:j{k} SET n += 1 RETURN NONE;"))
                        .await
                    {
                        Ok(_) => break,
                        Err(e) if version_ledger::is_conflict(&e) => {
                            conflicts.fetch_add(1, Ordering::Relaxed);
                        }
                        Err(e) => panic!("write job:j{k} failed during the migration: {e}"),
                    }
                }
                *committed.entry(k).or_default() += 1;
            }
            committed
        }));
    }
    let opts = MigrationOptions {
        batch: 100,
        pause_ms: 0,
        ..Default::default()
    };
    let progress = version_ledger::migrate(t, &opts, |_| {})
        .await
        .expect("migration");
    let mut committed: HashMap<usize, i64> = HashMap::new();
    for w in writers {
        for (k, n) in w.await.expect("writer") {
            *committed.entry(k).or_default() += n;
        }
    }
    // A write behind the cursor can leave nothing; a run over a finished
    // ledger confirms it.
    let rest = version_ledger::migrate(t, &opts, |_| {})
        .await
        .expect("second run");
    eprintln!(
        "  migration: {progress:?}, then {rest:?}; writer conflicts retried: {}",
        conflicts.load(Ordering::Relaxed)
    );
    assert!(rest.done);

    let versions = t
        .last("SELECT VALUE [record_id, version, type::is_array(record::id(id))] FROM _00_version;")
        .await;
    let versions = versions.as_array().expect("ledger rows");
    assert_eq!(versions.len(), JOBS, "one ledger row per record");
    let mut seen = HashMap::new();
    for v in versions {
        let rid = v[0].as_str().expect("record_id").to_string();
        assert_eq!(v[2], Value::Bool(true), "{rid} is still a legacy row");
        assert!(
            seen.insert(rid.clone(), v[1].as_i64().unwrap()).is_none(),
            "two ledger rows for {rid}"
        );
    }
    for k in 0..JOBS {
        let want = (k % 11 + 1) as i64 + committed.get(&k).copied().unwrap_or(0);
        assert_eq!(
            seen.get(&format!("job:j{k}")).copied(),
            Some(want),
            "job:j{k}: a version bump was lost"
        );
    }
}

/// A multi-threaded runtime with the stack embedded SurrealDB 4 needs.
fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .thread_stack_size(8 << 20)
        .enable_all()
        .build()
        .unwrap()
}

#[test]
fn embedded_4_synced_writes_stay_cheap_on_a_large_ledger() {
    runtime().block_on(async { synced_writes_stay_cheap(&Target::embedded().await).await });
}

#[test]
fn embedded_4_version_projection_stays_cheap_on_a_large_ledger() {
    runtime().block_on(async { version_projection_stays_cheap(&Target::embedded().await).await });
}

#[test]
fn embedded_4_migration_keeps_versions_and_is_idempotent() {
    runtime().block_on(async { migration_keeps_versions(&Target::embedded().await).await });
}

#[test]
fn embedded_4_migration_under_concurrent_writes_loses_nothing() {
    runtime()
        .block_on(async { migration_under_concurrent_writes(&Target::embedded().await).await });
}

// ---------------------------------------------------------------------------
// The same against SurrealDB servers (Docker).
// ---------------------------------------------------------------------------

fn docker_available() -> bool {
    Command::new("docker")
        .arg("version")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

/// A throwaway in-memory SurrealDB server, removed on drop.
struct Server {
    name: String,
    port: u16,
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = Command::new("docker")
            .args(["rm", "-f", &self.name])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
    }
}

/// Start `surrealdb/surrealdb:<tag>` on `port`, or `None` without Docker.
fn server(tag: &str, port: u16) -> Option<Server> {
    if !docker_available() {
        eprintln!("docker unavailable; skipping the SurrealDB {tag} server test");
        return None;
    }
    let name = format!("spky-version-ledger-{}", tag.replace('.', "-"));
    let srv = Server { name, port };
    drop(
        Command::new("docker")
            .args(["rm", "-f", &srv.name])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status(),
    );
    let started = Command::new("docker")
        .args([
            "run",
            "-d",
            "--name",
            &srv.name,
            "-p",
            &format!("127.0.0.1:{port}:8000"),
        ])
        .arg(format!("surrealdb/surrealdb:{tag}"))
        .args(["start", "--user", "root", "--pass", "root", "memory"])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .expect("docker run");
    assert!(
        started.success(),
        "docker run surrealdb/surrealdb:{tag} failed"
    );
    let deadline = Instant::now() + Duration::from_secs(90);
    while server_sql(port, "RETURN 1;").is_err() {
        assert!(Instant::now() < deadline, "SurrealDB {tag} did not come up");
        std::thread::sleep(Duration::from_millis(300));
    }
    server_sql(
        port,
        "DEFINE NAMESPACE IF NOT EXISTS t; USE NS t; DEFINE DATABASE IF NOT EXISTS t;",
    )
    .expect("namespace and database");
    Some(srv)
}

/// Every server check on a fresh server per check.
fn on_servers(check: impl Fn(Target) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()>>>) {
    for (tag, port) in [("v3.3.5", 18971u16), ("v4.0.1", 18972u16)] {
        let Some(srv) = server(tag, port) else { return };
        eprintln!("SurrealDB {tag}:");
        runtime().block_on(check(Target::Server { port: srv.port }));
    }
}

#[test]
#[ignore = "needs Docker: cargo test -p sp00ky-cli version_ledger -- --include-ignored --test-threads=1"]
fn servers_synced_writes_stay_cheap_on_a_large_ledger() {
    on_servers(|t| Box::pin(async move { synced_writes_stay_cheap(&t).await }));
}

#[test]
#[ignore = "needs Docker: cargo test -p sp00ky-cli version_ledger -- --include-ignored --test-threads=1"]
fn servers_version_projection_stays_cheap_on_a_large_ledger() {
    on_servers(|t| Box::pin(async move { version_projection_stays_cheap(&t).await }));
}

#[test]
#[ignore = "needs Docker: cargo test -p sp00ky-cli version_ledger -- --include-ignored --test-threads=1"]
fn servers_migration_keeps_versions_and_is_idempotent() {
    on_servers(|t| Box::pin(async move { migration_keeps_versions(&t).await }));
}

#[test]
#[ignore = "needs Docker: cargo test -p sp00ky-cli version_ledger -- --include-ignored --test-threads=1"]
fn servers_migration_under_concurrent_writes_loses_nothing() {
    on_servers(|t| Box::pin(async move { migration_under_concurrent_writes(&t).await }));
}
