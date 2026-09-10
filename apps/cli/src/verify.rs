//! `spky verify` — compare per-table record counts AND content hashes
//! across the upstream SurrealDB, the scheduler's replica snapshot, and the
//! SSP's circuit store.
//!
//! Hashes are computed via the same `ssp_protocol::snapshot_hash` helper
//! the scheduler and SSP use, so any divergence (count or content) shows
//! up here. With `--fix`, the remedy is picked by which pair disagrees:
//! main-vs-replica divergence means the scheduler's snapshot itself is bad,
//! so we POST `/admin/resync` (re-clone from upstream, which also flags all
//! SSPs); replica-vs-SSP-only divergence just needs `/admin/ssp/resync-all`
//! so every SSP re-bootstraps from the current frozen snapshot.

use anyhow::{bail, Context, Result};
use serde_json::Value;
use ssp_protocol::snapshot_hash;
use std::collections::BTreeMap;
use std::path::Path;
use std::time::Duration;

use crate::backend::{self, DEFAULT_CONFIG_PATH};
use crate::dev::{SCHEDULER_PORT, SSP_PORT, SURREAL_PORT};

const HTTP_TIMEOUT: Duration = Duration::from_secs(10);
const PREFIX: &str = "[sp00ky verify]";
const HASH_DISPLAY_LEN: usize = 11; // "b3:" + 8 hex chars

#[derive(Default, Clone)]
struct TableStat {
    count: usize,
    hash: Option<String>,
}

pub fn run(fix: bool) -> Result<()> {
    let config = backend::load_config(Path::new(DEFAULT_CONFIG_PATH));
    let surreal = config.resolved_surrealdb();

    // Name the endpoints. Every column comes from localhost — this command
    // reads the LOCAL dev stack, and run inside a cloud-linked project it
    // otherwise looks like a verdict on the deployment: the counts are a
    // different database's, and its "mismatches" send you chasing drift that
    // does not exist.
    println!(
        "{} Comparing record counts and hashes across the LOCAL dev stack \
         (SurrealDB localhost:{}, scheduler localhost:{}, SSP localhost:{}).\n\
         {} This says nothing about a cloud deployment — for that, read the \
         scheduler's own drift verdict on the admin dashboard.\n",
        PREFIX, SURREAL_PORT, SCHEDULER_PORT, SSP_PORT, PREFIX
    );

    let main = fetch_main_stats(
        &surreal.namespace,
        &surreal.database,
        &surreal.username_literal(),
        &surreal.password_literal(),
    )
    .context("Failed to fetch counts/hashes from SurrealDB")?;
    let replica = fetch_scheduler_stats().unwrap_or_else(|e| {
        eprintln!("{} Warning: scheduler unavailable: {:#}", PREFIX, e);
        BTreeMap::new()
    });
    print_scheduler_drift();
    let circuit = fetch_ssp_stats().unwrap_or_else(|e| {
        eprintln!("{} Warning: SSP unavailable: {:#}", PREFIX, e);
        BTreeMap::new()
    });

    print_table(&main, &replica, &circuit);

    let mismatches = count_mismatches(&main, &replica, &circuit);
    println!();
    if mismatches == 0 {
        println!("{} OK — all sources agree.", PREFIX);
        return Ok(());
    }

    if fix {
        // Classify which pair disagrees to pick the right remedy. A bad
        // replica can't be fixed by re-bootstrapping SSPs from it — they'd
        // just refetch the same diverging state.
        let main_vs_replica = !replica.is_empty() && count_pair_mismatches(&main, &replica) > 0;
        if main_vs_replica {
            eprintln!(
                "{} Main DB and scheduler replica disagree — calling /admin/resync (re-clone from upstream)",
                PREFIX
            );
            match force_scheduler_resync("reclone") {
                Ok(marked) => println!(
                    "{} Replica re-cloned from upstream; {} SSP(s) flagged for re-bootstrap. Re-run `spky verify` after they've reconnected.",
                    PREFIX, marked
                ),
                Err(e) => eprintln!("{} --fix failed: {:#}", PREFIX, e),
            }
        } else {
            eprintln!(
                "{} {} table(s) out of sync — calling /admin/ssp/resync-all",
                PREFIX, mismatches
            );
            match force_resync_ssps() {
                Ok(n) => println!(
                    "{} Flagged {} SSP(s) for forced re-bootstrap. Re-run `spky verify` after they've reconnected.",
                    PREFIX, n
                ),
                Err(e) => eprintln!("{} --fix failed: {:#}", PREFIX, e),
            }
        }
    }

    bail!("{} {} table(s) out of sync", PREFIX, mismatches)
}

/// Counts + hashes per non-`_00_` table from the upstream SurrealDB.
fn fetch_main_stats(
    ns: &str,
    db: &str,
    user: &str,
    pass: &str,
) -> Result<BTreeMap<String, TableStat>> {
    let url = format!("http://localhost:{}/sql", SURREAL_PORT);
    let info: Value = surreal_query(&url, ns, db, user, pass, "INFO FOR DB")?;
    let tables = extract_tables(&info)?;

    let mut out = BTreeMap::new();
    for table in tables {
        // GROUP ALL returns [] when the table is empty.
        let count_q = format!("SELECT count() AS total FROM {} GROUP ALL", table);
        let count_res: Value = surreal_query(&url, ns, db, user, pass, &count_q)?;
        let count = count_res
            .as_array()
            .and_then(|arr| arr.first())
            .and_then(|row| row.get("total"))
            .and_then(|v| v.as_u64())
            .unwrap_or(0) as usize;

        // Project out opaque fields (`@nosync`/`@crdt`/`@opaque` on a DEFINE
        // FIELD) exactly as the scheduler replica and the SSP bootstrap do.
        // Without this, `spky verify` reports main≠SSP for every table carrying
        // such a field — a mismatch no re-clone can fix, because the two sides
        // are correctly holding different key sets.
        let table_info: Value =
            surreal_query(&url, ns, db, user, pass, &format!("INFO FOR TABLE {}", table))
                .unwrap_or(Value::Null);
        let omit = ssp_protocol::omit_clause(&ssp_protocol::opaque_fields_from_info(&table_info));

        let select_q = format!("SELECT *{} FROM {}", omit, table);
        let rows_val: Value = surreal_query(&url, ns, db, user, pass, &select_q)?;
        let pairs: Vec<(String, Value)> = rows_val
            .as_array()
            .map(|arr| {
                arr.iter()
                    .filter_map(|row| {
                        let id = row.get("id")?.as_str()?;
                        let raw_id = id
                            .strip_prefix(&format!("{}:", table))
                            .unwrap_or(id)
                            .to_string();
                        Some((raw_id, row.clone()))
                    })
                    .collect()
            })
            .unwrap_or_default();
        // The `x3:` set-hash: what the scheduler maintains per event and the
        // SSP keeps per collection, so all three sides compare like for like.
        let hash = snapshot_hash::xor_table_hash(pairs);

        out.insert(
            table,
            TableStat {
                count,
                hash: Some(hash),
            },
        );
    }
    Ok(out)
}

fn extract_tables(info: &Value) -> Result<Vec<String>> {
    let tables_obj = info
        .get("tables")
        .and_then(|v| v.as_object())
        .context("INFO FOR DB response missing `tables`")?;
    Ok(tables_obj
        .keys()
        .filter(|name| !name.starts_with("_00_"))
        .cloned()
        .collect())
}

/// Wrap a single SurrealQL statement, post it to /sql, and return the first
/// statement's result payload.
fn surreal_query(
    url: &str,
    ns: &str,
    db: &str,
    user: &str,
    pass: &str,
    sql: &str,
) -> Result<Value> {
    let resp = ureq::post(url)
        .set("Surreal-NS", ns)
        .set("Surreal-DB", db)
        .set("Accept", "application/json")
        .set("Content-Type", "text/plain")
        .set(
            "Authorization",
            &format!("Basic {}", base64_encode(&format!("{}:{}", user, pass))),
        )
        .timeout(HTTP_TIMEOUT)
        .send_string(sql)
        .map_err(|e| anyhow::anyhow!("SurrealDB HTTP error: {}", e))?;
    let body: Vec<Value> = resp
        .into_json()
        .context("Failed to parse SurrealDB response")?;
    let first = body
        .into_iter()
        .next()
        .context("SurrealDB returned no statement results")?;
    if first.get("status").and_then(|v| v.as_str()) != Some("OK") {
        bail!("SurrealDB query failed: {}", first);
    }
    Ok(first.get("result").cloned().unwrap_or(Value::Null))
}

/// Minimal base64 (basic-auth header) — we don't pull in the `base64` crate
/// just for this one call.
fn base64_encode(input: &str) -> String {
    const TABLE: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let bytes = input.as_bytes();
    let mut out = String::with_capacity((bytes.len() + 2) / 3 * 4);
    for chunk in bytes.chunks(3) {
        let b = [
            chunk[0],
            if chunk.len() > 1 { chunk[1] } else { 0 },
            if chunk.len() > 2 { chunk[2] } else { 0 },
        ];
        let n = ((b[0] as u32) << 16) | ((b[1] as u32) << 8) | (b[2] as u32);
        out.push(TABLE[((n >> 18) & 0x3f) as usize] as char);
        out.push(TABLE[((n >> 12) & 0x3f) as usize] as char);
        out.push(if chunk.len() > 1 {
            TABLE[((n >> 6) & 0x3f) as usize] as char
        } else {
            '='
        });
        out.push(if chunk.len() > 2 {
            TABLE[(n & 0x3f) as usize] as char
        } else {
            '='
        });
    }
    out
}

/// The scheduler's own replica-vs-upstream verdict (`drift` block of
/// `/health/snapshot`, written by its periodic drift check). Printed next to
/// this tool's comparison so an operator can tell "the scheduler already knows
/// and is acting" from "the scheduler has not noticed". Silent when the
/// scheduler is unreachable or predates the field.
fn print_scheduler_drift() {
    let url = format!("http://localhost:{}/health/snapshot", SCHEDULER_PORT);
    let body: Value = match ureq::get(&url).timeout(HTTP_TIMEOUT).call() {
        Ok(resp) => match resp.into_json() {
            Ok(b) => b,
            Err(_) => return,
        },
        Err(_) => return,
    };
    let Some(drift) = body.get("drift") else { return };
    let list = |key: &str| -> Vec<String> {
        drift
            .get(key)
            .and_then(|v| v.as_array())
            .map(|a| a.iter().filter_map(|v| v.as_str().map(str::to_string)).collect())
            .unwrap_or_default()
    };
    let mismatched = list("mismatched");
    let stuck = list("stuck");
    let auto = drift.get("auto_reclone_enabled").and_then(|v| v.as_bool()).unwrap_or(false);
    let reclones = drift.get("auto_reclones").and_then(|v| v.as_u64()).unwrap_or(0);
    let mode = if auto { "on" } else { "off" };
    if mismatched.is_empty() && stuck.is_empty() {
        println!(
            "{} Scheduler drift check: clean (auto-reclone {}, {} automatic re-clone(s) so far)",
            PREFIX, mode, reclones
        );
    } else {
        println!(
            "{} Scheduler drift check: mismatched {:?}, stuck {:?} (auto-reclone {}, {} automatic re-clone(s) so far)",
            PREFIX, mismatched, stuck, mode, reclones
        );
    }
}

/// Per-table counts + hashes from the scheduler's `/health/snapshot` endpoint.
fn fetch_scheduler_stats() -> Result<BTreeMap<String, TableStat>> {
    let url = format!("http://localhost:{}/health/snapshot", SCHEDULER_PORT);
    let resp = ureq::get(&url)
        .timeout(HTTP_TIMEOUT)
        .call()
        .map_err(|e| anyhow::anyhow!("Scheduler HTTP error: {}", e))?;
    let body: Value = resp
        .into_json()
        .context("Failed to parse scheduler response")?;

    let tables = body
        .get("tables")
        .and_then(|v| v.as_object())
        .context("Scheduler /health/snapshot missing `tables`")?;
    let hashes = body.get("hashes").and_then(|v| v.as_object());

    let mut out = BTreeMap::new();
    for (name, count_v) in tables.iter() {
        let count = count_v.as_u64().unwrap_or(0) as usize;
        let hash = hashes
            .and_then(|h| h.get(name))
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        out.insert(name.clone(), TableStat { count, hash });
    }
    Ok(out)
}

/// Per-table counts + hashes from the SSP's `/info` `circuit_tables` and
/// `circuit_hashes` fields.
fn fetch_ssp_stats() -> Result<BTreeMap<String, TableStat>> {
    let url = format!("http://localhost:{}/info", SSP_PORT);
    let resp = ureq::get(&url)
        .timeout(HTTP_TIMEOUT)
        .call()
        .map_err(|e| anyhow::anyhow!("SSP HTTP error: {}", e))?;
    let body: Value = resp.into_json().context("Failed to parse SSP response")?;
    let entities = body
        .as_array()
        .context("SSP /info should return an array")?;
    let ssp_entry = entities
        .iter()
        .find(|e| e.get("entity").and_then(|v| v.as_str()) == Some("ssp"))
        .context("SSP /info has no ssp entity")?;

    let tables = ssp_entry
        .get("circuit_tables")
        .and_then(|v| v.as_object())
        .context("SSP /info missing circuit_tables (rebuild the SSP image)")?;
    let hashes = ssp_entry.get("circuit_hashes").and_then(|v| v.as_object());

    let mut out = BTreeMap::new();
    for (name, count_v) in tables.iter() {
        let count = count_v.as_u64().unwrap_or(0) as usize;
        let hash = hashes
            .and_then(|h| h.get(name))
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        out.insert(name.clone(), TableStat { count, hash });
    }
    Ok(out)
}

/// POST /admin/ssp/resync-all on the scheduler. Returns the count of SSPs
/// that were flagged for forced re-bootstrap.
fn force_resync_ssps() -> Result<u64> {
    let url = format!("http://localhost:{}/admin/ssp/resync-all", SCHEDULER_PORT);
    let resp = ureq::post(&url)
        .timeout(HTTP_TIMEOUT)
        .send_string("")
        .map_err(|e| anyhow::anyhow!("Scheduler /admin/ssp/resync-all error: {}", e))?;
    let body: Value = resp
        .into_json()
        .context("Failed to parse resync-all response")?;
    Ok(body
        .get("marked_for_resync")
        .and_then(|v| v.as_u64())
        .unwrap_or(0))
}

/// Count tables where two specific sources disagree (count, hash, or one side
/// missing the table entirely). Used by `--fix` to distinguish a bad replica
/// (main-vs-replica) from merely stale SSPs (replica-vs-SSP).
fn count_pair_mismatches(
    a: &BTreeMap<String, TableStat>,
    b: &BTreeMap<String, TableStat>,
) -> usize {
    let all_tables: std::collections::BTreeSet<&String> = a.keys().chain(b.keys()).collect();
    all_tables
        .into_iter()
        .filter(|t| {
            let (sa, sb) = (a.get(*t), b.get(*t));
            match (sa, sb) {
                (Some(sa), Some(sb)) => {
                    sa.count != sb.count
                        || matches!(
                            (&sa.hash, &sb.hash),
                            (Some(ha), Some(hb)) if ha != hb
                        )
                }
                // Table missing on one side counts as a mismatch.
                _ => true,
            }
        })
        .count()
}

/// POST /admin/resync on the scheduler with the given mode. A `reclone` is a
/// full upstream re-ingest and can take a while on a big DB, hence the long
/// timeout. Returns the count of SSPs flagged for re-bootstrap.
fn force_scheduler_resync(mode: &str) -> Result<u64> {
    let url = format!("http://localhost:{}/admin/resync", SCHEDULER_PORT);
    let resp = ureq::post(&url)
        .set("Content-Type", "application/json")
        .timeout(Duration::from_secs(600))
        .send_string(&format!(r#"{{"mode":"{}"}}"#, mode))
        .map_err(|e| anyhow::anyhow!("Scheduler /admin/resync error: {}", e))?;
    let body: Value = resp
        .into_json()
        .context("Failed to parse /admin/resync response")?;
    Ok(body
        .get("marked_for_resync")
        .and_then(|v| v.as_u64())
        .unwrap_or(0))
}

fn count_mismatches(
    main: &BTreeMap<String, TableStat>,
    replica: &BTreeMap<String, TableStat>,
    circuit: &BTreeMap<String, TableStat>,
) -> usize {
    let all_tables: std::collections::BTreeSet<&String> = main
        .keys()
        .chain(replica.keys())
        .chain(circuit.keys())
        .collect();
    all_tables
        .into_iter()
        .filter(|t| !is_table_match(*t, main, replica, circuit))
        .count()
}

fn is_table_match(
    table: &str,
    main: &BTreeMap<String, TableStat>,
    replica: &BTreeMap<String, TableStat>,
    circuit: &BTreeMap<String, TableStat>,
) -> bool {
    let m = main.get(table);
    let r = replica.get(table);
    let c = circuit.get(table);

    let counts_match = matches!(
        (m.map(|s| s.count), r.map(|s| s.count), c.map(|s| s.count)),
        (Some(a), Some(b), Some(d)) if a == b && b == d
    );

    // For hashes, only fail if all three sides report a hash and any two
    // disagree. If a side is missing a hash (e.g. older binary), we don't
    // consider that a hash mismatch — counts still gate.
    let hashes: Vec<&String> = [m, r, c]
        .iter()
        .filter_map(|s| s.and_then(|t| t.hash.as_ref()))
        .collect();
    let hashes_match = hashes.windows(2).all(|w| w[0] == w[1]);

    counts_match && hashes_match
}

fn print_table(
    main: &BTreeMap<String, TableStat>,
    replica: &BTreeMap<String, TableStat>,
    circuit: &BTreeMap<String, TableStat>,
) {
    let all_tables: std::collections::BTreeSet<&String> = main
        .keys()
        .chain(replica.keys())
        .chain(circuit.keys())
        .collect();

    let table_w = all_tables.iter().map(|t| t.len()).max().unwrap_or(8).max(8);
    println!(
        "  {:<tw$}  {:>6}  {:<hw$}  {:>6}  {:<hw$}  {:>6}  {:<hw$}  status",
        "table",
        "main#",
        "main hash",
        "rep#",
        "replica hash",
        "ssp#",
        "ssp hash",
        tw = table_w,
        hw = HASH_DISPLAY_LEN,
    );
    println!(
        "  {:-<tw$}  {:->6}  {:-<hw$}  {:->6}  {:-<hw$}  {:->6}  {:-<hw$}  ------",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        tw = table_w,
        hw = HASH_DISPLAY_LEN,
    );

    for table in &all_tables {
        let status = if is_table_match(table, main, replica, circuit) {
            "OK"
        } else {
            "MISMATCH"
        };
        println!(
            "  {:<tw$}  {:>6}  {:<hw$}  {:>6}  {:<hw$}  {:>6}  {:<hw$}  {}",
            table,
            fmt_count(main.get(*table).map(|s| s.count)),
            fmt_hash(main.get(*table).and_then(|s| s.hash.as_deref())),
            fmt_count(replica.get(*table).map(|s| s.count)),
            fmt_hash(replica.get(*table).and_then(|s| s.hash.as_deref())),
            fmt_count(circuit.get(*table).map(|s| s.count)),
            fmt_hash(circuit.get(*table).and_then(|s| s.hash.as_deref())),
            status,
            tw = table_w,
            hw = HASH_DISPLAY_LEN,
        );
    }
}

fn fmt_count(c: Option<usize>) -> String {
    match c {
        Some(n) => n.to_string(),
        None => "-".to_string(),
    }
}

fn fmt_hash(h: Option<&str>) -> String {
    match h {
        Some(s) if s.len() >= HASH_DISPLAY_LEN => s[..HASH_DISPLAY_LEN].to_string(),
        Some(s) => s.to_string(),
        None => "-".to_string(),
    }
}
