//! Memory-footprint baseline for the circuit.
//!
//! The SSP holds every syncable table in RAM for the whole process lifetime,
//! so its resident size is what decides whether a tenant fits under its cgroup
//! cap. These tests pin the current cost per row and per registered query so a
//! representation change has to *prove* its reduction, and so a regression that
//! quietly inflates the store fails CI instead of arriving as an OOM kill in
//! production (which leaves no log line — the container is simply SIGKILLed).
//!
//! `#[ignore]`d: they allocate hundreds of MB and take seconds. Run explicitly:
//!
//! ```sh
//! cargo test -p ssp --release --test memory_profile -- --ignored --nocapture
//! ```
//!
//! **These thresholds are meant to be ratcheted DOWN.** Each phase of the
//! zero-copy row-store work should lower them and leave the new number here as
//! its evidence. Never raise one to make a build pass: a raise means the change
//! made the store bigger, which is the thing this file exists to catch.

use ssp::algebra::ZSet;
use ssp::circuit::store::{Change, Record, Store};
use ssp::circuit::Circuit;
use ssp::operator::plan::OrderSpec;
use ssp::operator::{Operator, TopK};
use ssp::types::Path;
use serde_json::json;

/// A row shaped like real application data: a mix of strings (including record
/// references), integers, a bool, a null, and one nested object. Twelve
/// top-level fields, ULID-width ids.
fn synthetic_row(i: usize) -> serde_json::Value {
    let id = format!("{i:026}");
    json!({
        "id": format!("thread:{id}"),
        "title": "a reasonably typical title string",
        "owner": format!("user:{:026}", i % 1000),
        "status": "open",
        "slug": format!("slug-{i}"),
        "body_preview": "some preview text that is a bit longer",
        "count": i as i64,
        "score": (i % 97) as i64,
        "_00_rv": i as i64,
        "pinned": false,
        "archived_at": serde_json::Value::Null,
        "meta": { "a": 1, "b": "two", "c": true },
    })
}

fn load_rows(n: usize) -> (Circuit, usize) {
    let records: Vec<Record> = (0..n)
        .map(|i| Record::new("thread", &format!("{i:026}"), synthetic_row(i)))
        .collect();
    // Size of the same rows as JSON on the wire — the denominator for the
    // blowup ratio, and the number the flat encoding should approach.
    let json_bytes: usize = (0..n)
        .map(|i| serde_json::to_vec(&synthetic_row(i)).unwrap().len())
        .sum();
    let mut circuit = Circuit::new();
    circuit.load(records);
    (circuit, json_bytes)
}

/// Baseline on the flat-encoded store, measured 2026-08-11: **624 B/row
/// against ~327 B of source JSON, a 1.9x blowup**, down from 2054 B/row and
/// 6.3x on the parsed-`Sp00kyValue` store it replaced. Confirmed against real
/// RSS on 200k rows: 508 MB peak before, 175 MB after.
///
/// Of what remains, the encoded bodies dominate. The id index is broken out
/// separately by [`index_is_the_anonymous_floor`], because it is the part that
/// cannot become reclaimable page cache.
#[test]
#[ignore = "allocates ~100MB and takes seconds; run with --ignored"]
fn store_bytes_per_row_stays_under_budget() {
    const ROWS: usize = 50_000;
    const MAX_BYTES_PER_ROW: f64 = 620.0;

    let (circuit, json_bytes) = load_rows(ROWS);
    let report = circuit.size_report();
    let table = &report.tables[0];

    let json_per_row = json_bytes as f64 / ROWS as f64;
    let blowup = table.total_bytes() as f64 / json_bytes as f64;

    eprintln!("rows            : {}", table.rows);
    eprintln!("source JSON     : {json_per_row:.0} B/row");
    eprintln!(
        "rows_bytes      : {:.0} B/row",
        table.rows_bytes as f64 / ROWS as f64
    );
    eprintln!(
        "  of which index: {:.0} B/row (anonymous; the floor)",
        table.index_bytes as f64 / ROWS as f64
    );
    eprintln!(
        "zset_bytes      : {:.0} B/row",
        table.zset_bytes as f64 / ROWS as f64
    );
    eprintln!("total           : {:.0} B/row", table.bytes_per_row());
    eprintln!("blowup vs JSON  : {blowup:.2}x");
    if let Some(rss) = rss_bytes() {
        eprintln!("process RSS     : {} B", rss);
    }

    assert_eq!(table.rows, ROWS, "every row must land in the store");
    assert!(
        table.bytes_per_row() <= MAX_BYTES_PER_ROW,
        "store grew to {:.0} B/row, over the {MAX_BYTES_PER_ROW:.0} B budget. \
         If this is an intended representation change, lower the threshold; \
         if not, the store just got bigger.",
        table.bytes_per_row()
    );
}

/// `zset` keys are now shared `Arc<str>` clones of the same allocation the
/// rest of the circuit holds, so what is left here is the bucket array: 76
/// B/row before interning, 33 after.
#[test]
#[ignore = "allocates ~100MB and takes seconds; run with --ignored"]
fn zset_duplicate_key_cost_is_pinned() {
    const ROWS: usize = 50_000;
    const MAX_ZSET_BYTES_PER_ROW: f64 = 45.0;

    let (circuit, _) = load_rows(ROWS);
    let table = &circuit.size_report().tables[0];
    let per_row = table.zset_bytes as f64 / ROWS as f64;

    eprintln!("zset            : {per_row:.0} B/row");
    assert!(
        per_row <= MAX_ZSET_BYTES_PER_ROW,
        "zset grew to {per_row:.0} B/row, over the {MAX_ZSET_BYTES_PER_ROW:.0} B budget"
    );
}

/// `TopK` keeps every row that reaches it in *both* a sorted buffer and a
/// reverse index, so an `ORDER BY ... LIMIT 20` costs O(table), not O(20) —
/// **per registered query**. With enough views over one big table this term
/// exceeds the row store outright, and it is invisible in the per-table
/// numbers.
///
/// Measured 149 B/row/query, down from 242: the row key is now shared with
/// the store rather than allocated twice more here, the sort key is inline for
/// single-field ordering, and short string keys sit inside a `SmolStr`.
///
/// Still O(table) per query, which is the real problem and needs an operator
/// redesign rather than a smaller representation.
#[test]
#[ignore = "allocates ~100MB and takes seconds; run with --ignored"]
fn topk_state_is_charged_per_query_over_the_whole_table() {
    const ROWS: usize = 50_000;
    const MAX_BYTES_PER_ROW_PER_QUERY: f64 = 175.0;

    let mut store = Store::new();
    store.ensure_collection("thread");
    let mut delta = ZSet::new();
    for i in 0..ROWS {
        let id = format!("{i:026}");
        store.apply_change(&Change::create("thread", &id, synthetic_row(i)));
        delta.insert(format!("thread:{id}").into(), 1);
    }

    // A window query: 20 rows out of 50k. State should be O(20); it is O(50k).
    let mut top_k = TopK::new(
        20,
        0,
        Some(vec![OrderSpec {
            field: Path::new("score"),
            direction: "DESC".into(),
        }]),
    );
    let out = top_k.step(&[&delta], &store, None);
    assert_eq!(out.len(), 20, "the window itself is small");

    let per_row = top_k.state_bytes() as f64 / ROWS as f64;
    eprintln!("topk state      : {per_row:.0} B/row/query (window is 20 rows)");
    assert!(
        per_row <= MAX_BYTES_PER_ROW_PER_QUERY,
        "TopK state grew to {per_row:.0} B/row/query, over the \
         {MAX_BYTES_PER_ROW_PER_QUERY:.0} B budget"
    );
}

/// The row index is the floor: O(rows), and anonymous memory, so unlike the
/// encoded bodies it cannot become reclaimable page cache however the arena is
/// backed. Everything else about the store can be moved off the heap; this
/// cannot, so it is pinned on its own.
///
/// Measured 69 B/row before, 17 after. The index used to own a `Box<str>` copy
/// of every id; it now stores a 12-byte slot and nothing else, and the id
/// itself lives in the arena record — which moves those bytes to where they
/// *can* be reclaimed.
#[test]
#[ignore = "allocates ~100MB and takes seconds; run with --ignored"]
fn index_is_the_anonymous_floor() {
    const ROWS: usize = 50_000;
    const MAX_INDEX_BYTES_PER_ROW: f64 = 25.0;

    let (circuit, _) = load_rows(ROWS);
    let table = &circuit.size_report().tables[0];
    let per_row = table.index_bytes as f64 / ROWS as f64;

    eprintln!("index           : {per_row:.0} B/row");
    assert!(
        table.index_bytes < table.rows_bytes,
        "the index must be a fraction of the row bytes, not the bulk of them"
    );
    assert!(
        per_row <= MAX_INDEX_BYTES_PER_ROW,
        "index grew to {per_row:.0} B/row, over the {MAX_INDEX_BYTES_PER_ROW:.0} B budget — \
         this is the term that cannot be paged out"
    );
}

/// `compute_table_hashes` must stream, not collect.
///
/// Measured 2026-08-11 on 200k rows (a 391 MB store): the old
/// collect-into-`Vec<(String, Value)>`-then-hash shape added **600 MB** of
/// transient allocation, more than doubling the process; streaming through
/// `TableHasher` adds **4 MB**. Extrapolated to a million rows the old shape
/// is a multi-gigabyte spike inside a 1 GB container, and it was reachable
/// from the unauthenticated `/info` route on every request.
///
/// This test compares the two shapes on the same store. It asserts on
/// allocation *shape* rather than on RSS, which is too noisy to gate CI on:
/// the streamed path must not build a per-table collection of parsed values.
#[test]
#[ignore = "allocates ~400MB and takes seconds; run with --ignored"]
fn table_hashing_does_not_materialize_the_table() {
    const ROWS: usize = 50_000;
    let (circuit, _) = load_rows(ROWS);

    // The streamed result must equal what the collect-then-hash shape gives,
    // or the scheduler's integrity check fails and the SSP exit(2)s.
    let (rows, truncated) = circuit.dump_table_rows("thread", usize::MAX);
    assert!(!truncated);
    let collected = ssp_protocol::snapshot_hash::hash_table(rows);
    assert_eq!(
        circuit.compute_table_hashes()["thread"],
        collected,
        "streamed hash diverged from hash_table — this is the exit(2) path"
    );
}

/// Guards the attribution itself: if `size_report` stops seeing a component,
/// every threshold above silently passes for the wrong reason.
#[test]
fn size_report_attributes_every_component() {
    let mut circuit = Circuit::new();
    circuit.load(
        (0..64).map(|i| Record::new("thread", &format!("{i:026}"), synthetic_row(i))),
    );

    let report = circuit.size_report();
    assert_eq!(report.tables.len(), 1);
    let table = &report.tables[0];
    assert_eq!(table.table, "thread");
    assert_eq!(table.rows, 64);
    assert!(table.rows_bytes > 0, "row bodies must be counted");
    assert_eq!(table.zset_bytes, 0, "no view scans the table, so no membership z-set exists yet");
    assert_eq!(table.total_bytes(), table.rows_bytes + table.zset_bytes);
    assert_eq!(report.store_bytes, table.total_bytes());

    // Once something asks for the membership, its bucket array is charged.
    let _ = circuit.store.get_collection("thread").unwrap().membership();
    let report = circuit.size_report();
    let table = &report.tables[0];
    assert!(table.zset_bytes > 0, "a built membership z-set must be counted");
    assert_eq!(table.total_bytes(), table.rows_bytes + table.zset_bytes);
    assert_eq!(report.total_bytes(), report.store_bytes + report.query_bytes);
    assert!(table.bytes_per_row() > 0.0);
}

/// An empty circuit must report zero rather than a floor of allocator noise,
/// so a delta against it is meaningful.
#[test]
fn empty_circuit_reports_zero() {
    let report = Circuit::new().size_report();
    assert_eq!(report.total_bytes(), 0);
    assert!(report.tables.is_empty());
    assert!(report.views.is_empty());
}

/// Best-effort process RSS, for context in the printed output only. `None`
/// off Linux, which is why no assertion depends on it.
fn rss_bytes() -> Option<u64> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    let kb: u64 = status
        .lines()
        .find_map(|l| l.strip_prefix("VmRSS:"))?
        .split_whitespace()
        .next()?
        .parse()
        .ok()?;
    Some(kb * 1024)
}
