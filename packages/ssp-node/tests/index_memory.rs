//! A replacement SSP rebuilds, before it serves, the indexes the SSP before
//! it held, from the record that one kept in the database
//! (`ssp_node::index_memory`), against a real embedded SurrealDB.

use std::sync::Arc;
use std::time::Duration;

use serde_json::{json, Value};
use ssp::circuit::index::IndexDef;
use ssp::circuit::{Circuit, OutputFormat, Record, TableMeta};
use ssp_node::index_memory::{
    prebuild, prebuild_in_background, read_records, select, write_record, IndexRecorder, Lineage, PrebuildLimits,
    SkipReason, SspIndexRecord, RECORD_FORMAT, RECORD_KEEP,
};
use ssp_node::ports::{Db, DbError};
use surrealdb::engine::local::{Db as MemEngine, Mem};
use surrealdb::Surreal;
use tokio::sync::RwLock;

struct MemDb(Surreal<MemEngine>);

#[async_trait::async_trait]
impl Db for MemDb {
    async fn query(&self, surql: &str, binds: &[(&str, Value)]) -> Result<Vec<Value>, DbError> {
        let mut q = self.0.query(surql);
        for (name, value) in binds {
            q = q.bind(((*name).to_string(), value.clone()));
        }
        let mut response = q.await.map_err(|e| DbError::Transport(e.to_string()))?;
        let mut out = Vec::new();
        for i in 0..response.num_statements() {
            let val: surrealdb::types::Value = response.take(i).map_err(|e| DbError::Query(e.to_string()))?;
            out.push(val.into_json_value());
        }
        Ok(out)
    }
    async fn version(&self) -> Result<String, DbError> {
        Ok("mem".into())
    }
}

async fn mem_db() -> MemDb {
    let db = Surreal::new::<Mem>(()).await.unwrap();
    db.use_ns("t").use_db("t").await.unwrap();
    MemDb(db)
}

fn def(name: &str, fields: &[&str]) -> IndexDef {
    IndexDef { name: name.into(), fields: fields.iter().map(|f| f.to_string()).collect() }
}

fn game_indexes() -> Vec<IndexDef> {
    vec![
        def("game_white", &["white", "date"]),
        def("game_black", &["black", "date"]),
        def("game_owner_source", &["owner", "source"]),
    ]
}

/// One SSP's circuit after its bootstrap: the same rows and mirrored schema
/// every SSP of the project has.
fn bootstrapped(indexes: Vec<IndexDef>) -> Arc<RwLock<Circuit>> {
    bootstrapped_rows(indexes, 400)
}

fn bootstrapped_rows(indexes: Vec<IndexDef>, rows: usize) -> Arc<RwLock<Circuit>> {
    let mut circuit = Circuit::new();
    circuit.set_table_meta("game", TableMeta { permission: "true".into(), indexes, ..Default::default() });
    for i in 0..rows {
        circuit.load([Record::new("game", &format!("g{i:04}"), json!({
            "white": format!("user:{}", i % 7),
            "black": format!("user:{}", i % 5),
            "owner": format!("user:{}", i % 3),
            "source": if i % 2 == 0 { "lichess" } else { "chesscom" },
            "date": 1_700_000_000 + i,
        }))]);
    }
    Arc::new(RwLock::new(circuit))
}

fn registration(id: &str, surql: &str) -> Value {
    json!({
        "id": id, "surql": surql, "clientId": "tab", "authId": "user:1", "ttl": "30m",
        "lastActiveAt": "", "params": { "player": "user:1" },
    })
}

/// Register a client's view; returns its id and how many indexes the
/// registration had to build.
fn register(circuit: &mut Circuit, id: &str, surql: &str) -> (String, usize) {
    let data = ssp::service::view::prepare_registration_dbsp(
        registration(id, surql),
        circuit.permissions(),
        circuit.link_targets(),
        circuit.opaque_fields(),
    )
    .expect("registration prepares");
    let query_id = data.plan.id.clone();
    let (_, timings) =
        circuit.add_query_with_auth_timed(data.plan, data.safe_params, Some(OutputFormat::Streaming), "user:1".into());
    (query_id, timings.indexes_built)
}

const WHITE: &str = "SELECT * FROM game WHERE white = $player ORDER BY date DESC LIMIT 20";
const BLACK: &str = "SELECT * FROM game WHERE black = $player ORDER BY date DESC LIMIT 20";

fn built(circuit: &Circuit) -> Vec<String> {
    circuit.built_indexes().get("game").cloned().unwrap_or_default()
}

#[tokio::test]
async fn a_replacement_ssp_builds_what_the_one_before_it_held_before_it_serves() {
    let db = mem_db().await;
    assert!(read_records(&db).await.unwrap().is_empty(), "no table yet: nothing recorded");

    // Blue serves: one client reads its white games, another read its black
    // games and went (that index sits idle in its grace).
    let blue = bootstrapped(game_indexes());
    {
        let mut c = blue.write().await;
        assert_eq!(register(&mut c, "white", WHITE).1, 1, "first reader builds game_white");
        let (black, builds) = register(&mut c, "black", BLACK);
        assert_eq!(builds, 1);
        c.remove_query(&black);
        assert_eq!(built(&c), vec!["game_black", "game_white"]);
    }
    tokio::time::sleep(Duration::from_millis(5)).await;
    let recorder = IndexRecorder::new("ssp-0");
    assert!(recorder.record(&db, &blue, false).await.unwrap(), "first record");
    assert!(!recorder.record(&db, &blue, false).await.unwrap(), "unchanged: not rewritten");

    let records = read_records(&db).await.unwrap();
    assert_eq!(records.len(), 1);
    let record = &records[0];
    assert_eq!((record.ssp_id.as_str(), record.format), ("ssp-0", RECORD_FORMAT));
    let listed: Vec<(&str, bool)> = record.indexes.iter().map(|i| (i.name.as_str(), i.idle_ms > 0)).collect();
    assert_eq!(listed, vec![("game_black", true), ("game_white", false)]);
    assert!(record.indexes.iter().all(|i| i.rows == 400 && i.bytes > 0));

    // Green: a recreated SSP on a fresh volume, bootstrapped over the same
    // rows. Before Ready it rebuilds both from blue's record.
    let green = bootstrapped(game_indexes());
    assert!(built(&*green.read().await).is_empty());
    let limits = PrebuildLimits { time: Duration::from_secs(60), max_bytes: None, idle_grace: green.read().await.index_idle_release() };
    let selection = select(&records, ssp_node::now_epoch_ms(), &limits, &green_line());
    let report = prebuild(&green, selection, limits.time).await;
    assert_eq!(report.built.iter().map(|(label, _)| label.as_str()).collect::<Vec<_>>(), vec!["game.game_white", "game.game_black"],
        "most recently read first");
    assert!(report.skipped.is_empty(), "{}", report.skipped_summary());
    assert_eq!(report.stats.rows_indexed, 800);
    assert_eq!(built(&*green.read().await), vec!["game_black", "game_white"]);

    // Clients that come back after the handover pay no build.
    {
        let mut c = green.write().await;
        assert_eq!(register(&mut c, "white", WHITE).1, 0);
        assert_eq!(register(&mut c, "black-again", BLACK).1, 0);
        assert_eq!(register(&mut c, "owner", "SELECT * FROM game WHERE owner = $player ORDER BY source LIMIT 5").1, 1,
            "nobody read game_owner_source before: still built lazily");
    }

    // Green writes its own record once promoted. A record nobody has
    // written for longer than RECORD_KEEP goes with that write.
    let now = ssp_node::now_epoch_ms();
    let ancient = SspIndexRecord {
        format: RECORD_FORMAT,
        ssp_id: "ssp-0-gone".into(),
        at_ms: now - RECORD_KEEP.as_millis() as u64 - 60_000,
        indexes: Vec::new(),
    };
    db.query(
        "UPSERT _00_sync_evidence:⟨ssp_indexes:ssp-0-gone⟩ CONTENT $r RETURN NONE",
        &[("r", { let mut v = serde_json::to_value(&ancient).unwrap(); v["kind"] = json!("ssp_indexes"); v })],
    )
    .await
    .unwrap();
    assert_eq!(read_records(&db).await.unwrap().len(), 2);
    assert!(IndexRecorder::new("ssp-0-g1").record(&db, &green, false).await.unwrap());
    let mut ids: Vec<String> = read_records(&db).await.unwrap().into_iter().map(|r| r.ssp_id).collect();
    ids.sort();
    assert_eq!(ids, vec!["ssp-0", "ssp-0-g1"], "the ancient record was pruned");
    // The slow-operation samples in the same table are untouched by either.
    db.query("UPSERT _00_sync_evidence:slow_operations SET samples = [1]", &[]).await.unwrap();
    write_record(&db, &SspIndexRecord { format: RECORD_FORMAT, ssp_id: "ssp-0-g1".into(), at_ms: now, indexes: Vec::new() })
        .await
        .unwrap();
    let samples = db.query("SELECT samples FROM ONLY _00_sync_evidence:slow_operations", &[]).await.unwrap();
    assert_eq!(samples[0]["samples"], json!([1]));
}

#[tokio::test]
async fn a_replacement_skips_what_the_schema_dropped_or_redefined_and_what_aged_out() {
    let db = mem_db().await;
    let blue = bootstrapped(game_indexes());
    {
        let mut c = blue.write().await;
        register(&mut c, "white", WHITE);
        register(&mut c, "black", BLACK);
        register(&mut c, "owner", "SELECT * FROM game WHERE owner = $player ORDER BY source LIMIT 5");
    }
    IndexRecorder::new("ssp-0").record(&db, &blue, false).await.unwrap();
    let records = read_records(&db).await.unwrap();

    // The deploy that recreates the SSP also migrated the schema: game_black
    // is gone, game_white now covers other fields.
    let green = bootstrapped(vec![def("game_white", &["white", "source"]), def("game_owner_source", &["owner", "source"])]);
    let limits = PrebuildLimits { time: Duration::from_secs(60), max_bytes: None, idle_grace: Duration::from_secs(3600) };
    let report = prebuild(&green, select(&records, ssp_node::now_epoch_ms(), &limits, &green_line()), limits.time).await;
    assert_eq!(report.built.iter().map(|(l, _)| l.as_str()).collect::<Vec<_>>(), vec!["game.game_owner_source"]);
    let mut skipped = report.skipped.clone();
    skipped.sort_by(|a, b| a.0.cmp(&b.0));
    assert_eq!(skipped, vec![
        ("game.game_black".to_string(), SkipReason::NotDefined),
        ("game.game_white".to_string(), SkipReason::Redefined),
    ]);

    // An SSP replaced long after its predecessor's last record (it crashed,
    // nobody wrote since) rebuilds nothing from it: the record aged out.
    let much_later = ssp_node::now_epoch_ms() + 2 * 3600 * 1000;
    let later = bootstrapped(game_indexes());
    let report = prebuild(&later, select(&records, much_later, &limits, &green_line()), limits.time).await;
    assert!(report.built.is_empty());
    assert!(report.skipped.iter().all(|(_, reason)| *reason == SkipReason::IdlePastGrace), "{}", report.skipped_summary());

    // No time left: nothing starts, everything is reported.
    let rushed = bootstrapped(game_indexes());
    let report = prebuild(&rushed, select(&records, ssp_node::now_epoch_ms(), &limits, &green_line()), Duration::ZERO).await;
    assert!(report.built.is_empty());
    assert_eq!(report.skipped.len(), 3);
    assert!(report.skipped.iter().all(|(_, reason)| *reason == SkipReason::TimeBudget));
    assert!(built(&*rushed.read().await).is_empty());
}

/// Blue's record (game_white read by a view, game_black idle), for a table
/// of `rows` games.
async fn blue_record(db: &MemDb, rows: usize) -> Vec<SspIndexRecord> {
    let blue = bootstrapped_rows(game_indexes(), rows);
    {
        let mut c = blue.write().await;
        register(&mut c, "white", WHITE);
        let (black, _) = register(&mut c, "black", BLACK);
        c.remove_query(&black);
    }
    tokio::time::sleep(Duration::from_millis(5)).await;
    IndexRecorder::new("ssp-0").record(db, &blue, false).await.unwrap();
    read_records(db).await.unwrap()
}

/// The SSP that replaces blue's `ssp-0` in these tests.
fn green_line() -> Lineage {
    Lineage::new("ssp-0-g1", Some("ssp-0".into()))
}

fn limits() -> PrebuildLimits {
    PrebuildLimits { time: Duration::from_secs(60), max_bytes: None, idle_grace: Duration::from_secs(3600) }
}

/// What `SspNode`'s registration does before it takes the circuit's write
/// lock: build the view's indexes a step at a time under the read lock, then
/// install the view. Returns the rows its steps read and whether the steps
/// or the install reported building an index.
async fn register_like_a_client(circuit: &RwLock<Circuit>, id: &str, surql: &str) -> (usize, usize) {
    let data = {
        let c = circuit.read().await;
        ssp::service::view::prepare_registration_dbsp(registration(id, surql), c.permissions(), c.link_targets(), c.opaque_fields())
            .expect("registration prepares")
    };
    let (mut rows, mut builds) = (0, 0);
    loop {
        let (done, stats) = circuit.read().await.prewarm_query_indexes_step(&data.plan, data.safe_params.as_ref(), Duration::ZERO);
        rows += stats.rows_indexed;
        builds += stats.indexes_built;
        if done {
            break;
        }
        tokio::task::yield_now().await;
    }
    let (_, timings) = circuit.write().await.add_query_with_auth_timed(
        data.plan,
        data.safe_params,
        Some(OutputFormat::Streaming),
        "user:1".into(),
    );
    (rows, builds + timings.indexes_built)
}

/// A serving SSP prebuilds in the background while a client registers a view
/// over the index it is building: they advance one part-built index, so every
/// row is read once and the index is built once.
#[tokio::test(flavor = "current_thread")]
async fn a_background_prebuild_and_a_registration_of_the_same_index_build_it_once() {
    const ROWS: usize = 6_000;
    let db = mem_db().await;
    let records = blue_record(&db, ROWS).await;
    let green = bootstrapped_rows(game_indexes(), ROWS);
    let selection = select(&records, ssp_node::now_epoch_ms(), &limits(), &green_line());
    assert_eq!(selection.build[0].name, "game_white", "the index the client needs goes first");

    // One 512-row step each, taking turns (both yield after every step).
    let background = prebuild_in_background(&green, selection, Duration::from_secs(60), Duration::ZERO, || false);
    let client = register_like_a_client(&green, "white", WHITE);
    let (report, (client_rows, client_builds)) = tokio::join!(background, client);

    assert!(client_rows > 0, "the registration took turns building game_white");
    assert!(client_rows < ROWS, "...and did not build it alone");
    assert_eq!(report.stats.rows_indexed + client_rows, 2 * ROWS, "game_white once between them, game_black once");
    let built: Vec<&str> = report.built.iter().map(|(label, _)| label.as_str()).collect();
    assert_eq!(built, vec!["game.game_white", "game.game_black"], "{}", report.skipped_summary());
    assert!(client_builds <= 1);
    let held = green.read().await.held_indexes();
    assert!(held.iter().all(|index| index.rows == ROWS), "{held:?}");
    assert_eq!(held.iter().find(|i| i.name == "game_white").unwrap().idle_ms, 0, "read by the client's view");
    assert!(held.iter().find(|i| i.name == "game_black").unwrap().idle_ms > 0, "unread: idle age carried over");
}

/// A background prebuild stops between two steps when asked (shutdown, the
/// SSP leaving Ready): the rest is reported, the part-built index stays, and
/// the next registration that needs it finishes it without reading a row
/// twice.
#[tokio::test(flavor = "current_thread")]
async fn a_background_prebuild_stops_between_steps_and_leaves_the_rest_to_the_next_reader() {
    const ROWS: usize = 6_000;
    let db = mem_db().await;
    let records = blue_record(&db, ROWS).await;
    let green = bootstrapped_rows(game_indexes(), ROWS);
    let asked = std::sync::atomic::AtomicUsize::new(0);
    let report = prebuild_in_background(
        &green,
        select(&records, ssp_node::now_epoch_ms(), &limits(), &green_line()),
        Duration::from_secs(60),
        Duration::ZERO,
        || asked.fetch_add(1, std::sync::atomic::Ordering::SeqCst) >= 3,
    )
    .await;
    assert!(report.built.is_empty());
    assert_eq!(report.skipped, vec![
        ("game.game_white".to_string(), SkipReason::Stopped),
        ("game.game_black".to_string(), SkipReason::Stopped),
    ]);
    let partial = report.stats.rows_indexed;
    assert!(partial > 0 && partial < ROWS, "stopped part way: {partial}");
    tokio::time::sleep(Duration::from_millis(5)).await;
    let held = green.read().await.held_indexes();
    assert_eq!(held.len(), 1);
    assert_eq!((held[0].name.as_str(), held[0].rows), ("game_white", partial));
    assert!(held[0].idle_ms >= 5, "left part built and unread: idle, not in use");

    let (client_rows, client_builds) = register_like_a_client(&green, "white", WHITE).await;
    assert_eq!(partial + client_rows, ROWS, "finished from where it stopped");
    assert_eq!(client_builds, 1, "the registration's step completed it");
    assert_eq!(green.read().await.held_indexes()[0].rows, ROWS);
}
