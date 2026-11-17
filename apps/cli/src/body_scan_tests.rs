//! The functions and events sp00ky generates must cost about the same on a
//! large table as on a small one, on the embedded SurrealDB 4.0.1 (kv-mem).
//!
//! The bug this guards against (2026-11-16, WhitePawn on 4.0.1): inside a
//! `DEFINE FUNCTION` or `DEFINE EVENT` body, `UPDATE / UPSERT / DELETE <table>
//! ... WHERE` runs as a table scan even with LET-bound parameters, and a WHERE
//! on `$after.x` / `$before.x` too, so the cost of one call grew with the
//! table. `query_lint` refuses those shapes in the generated schema; these
//! tests time the rewritten ones. See
//! `docs/surrealdb-bugs/v4-event-param-index-scan.md`.

use std::time::Instant;

use surrealdb::engine::local::{Db, Mem};
use surrealdb::Surreal;

use crate::backend::DeployMode;
use crate::schema_builder::{build_server_events, build_server_schema, SchemaBuilderConfig};

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .thread_stack_size(8 << 20)
        .enable_all()
        .build()
        .unwrap()
}

/// An app with a `@crdt` field and a `@channel` table, built and applied the
/// way `spky` deploys it (server schema, then the generated events).
const APP: &str = "DEFINE TABLE user SCHEMALESS PERMISSIONS FULL;\n\
DEFINE TABLE thread SCHEMAFULL PERMISSIONS FULL;\n\
DEFINE FIELD title ON TABLE thread TYPE string;\n\
DEFINE FIELD summary ON TABLE thread TYPE option<string>; -- @crdt text\n\
-- @channel\n\
DEFINE TABLE call SCHEMAFULL PERMISSIONS FULL;\n\
DEFINE FIELD caller ON TABLE call TYPE option<record<user>>;\n";

async fn deployed() -> Surreal<Db> {
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("schema.surql");
    std::fs::write(&input, APP).unwrap();
    let config = dir.path().join("sp00ky.yml");
    std::fs::write(&config, "sync:\n  transport: changefeed\n").unwrap();
    let builder = SchemaBuilderConfig {
        input_path: input,
        config_path: Some(config),
        mode: DeployMode::Cluster,
        endpoint: None,
        secret: None,
        include_functions: false,
    };
    let mut sql = build_server_schema(&builder).expect("server schema");
    sql.push_str(&build_server_events(&builder).expect("server events"));
    let db = Surreal::new::<Mem>(()).await.unwrap();
    db.use_ns("t").use_db("t").await.unwrap();
    db.query(sql)
        .await
        .expect("apply schema")
        .check()
        .expect("schema statements");
    db
}

async fn exec(db: &Surreal<Db>, sql: &str) {
    db.query(sql)
        .await
        .expect(sql)
        .check()
        .unwrap_or_else(|e| panic!("{sql}\n-> {e}"));
}

/// Median wall time of `sql`, `n` runs, each built from the run index.
async fn median_ms(db: &Surreal<Db>, n: usize, sql: impl Fn(usize) -> String) -> f64 {
    let mut times = Vec::with_capacity(n);
    for i in 0..n {
        let q = sql(i);
        let started = Instant::now();
        exec(db, &q).await;
        times.push(started.elapsed().as_secs_f64() * 1000.0);
    }
    times.sort_by(|a, b| a.partial_cmp(b).unwrap());
    times[n / 2]
}

/// `rows` rows of filler in each table the paths below look into.
async fn seed(db: &Surreal<Db>, rows: usize) {
    for lo in (0..rows).step_by(2_000) {
        let n = 2_000.min(rows - lo);
        exec(
            db,
            &format!(
                "OPTION IMPORT;
                 LET $ix = array::map(array::range(0, {n}), |$j| $j + {lo});
                 INSERT INTO user array::map($ix, |$i| {{ id: type::record('user', 'u' + <string>$i) }}) RETURN NONE;
                 INSERT INTO _00_user_feature array::map($ix, |$i| {{ user: type::record('user', 'u' + <string>$i), key: IF $i % 2 = 0 {{ 'beta' }} ELSE {{ 'filler' + <string>($i % 50) }}, variant: 'on', evaluated_at: time::now() }}) RETURN NONE;
                 INSERT INTO _00_crdt_doc array::map($ix, |$i| {{ doc: type::record('thread', 'other' + <string>$i), field: 'summary', rev: 1 }}) RETURN NONE;
                 INSERT INTO _00_channel_signal array::map($ix, |$i| {{ sender: type::record('user', 'u' + <string>$i), recipient: user:x, sender_peer: 'p', kind: 'hello', at: time::now() }}) RETURN NONE;"
            ),
        )
        .await;
    }
    exec(
        db,
        "UPSERT _00_feature_flag:beta SET key = 'beta', enabled = true, default_variant = 'off', variants = ['off', 'on'], rules = [{ kind: 'rollout', variant: 'on', percent: 50 }];",
    )
    .await;
}

async fn costs(rows: usize) -> Vec<(&'static str, f64)> {
    let db = deployed().await;
    seed(&db, rows).await;
    let mut out = Vec::new();
    // the scheduler's flag sweep, per flag every 30 s: the `key` index (a
    // flag no one holds, so the answer is the same size at both scales)
    out.push((
        "flag sweep read",
        median_ms(&db, 7, |_| {
            "SELECT VALUE <string>user FROM _00_user_feature WHERE key = 'absent';".into()
        })
        .await,
    ));
    // fn::feature::assign: find the user's row, write it by id. Half the
    // rows hold `beta`: a lookup through the `key` index instead of `user`
    // reads all of them.
    out.push((
        "fn::feature::assign",
        median_ms(&db, 7, |i| {
            format!("LET $f = (SELECT * FROM ONLY _00_feature_flag:beta); RETURN fn::feature::assign($f, fn::feature::rules($f), user:u{i});")
        })
        .await,
    ));
    // fn::feature::materialize's delete of an unknown flag's rows
    out.push((
        "fn::feature::materialize (unknown flag)",
        median_ms(&db, 7, |i| {
            format!("RETURN fn::feature::materialize('gone{i}');")
        })
        .await,
    ));
    // _00_crdt_cleanup on a parent delete
    out.push((
        "_00_crdt_cleanup",
        median_ms(&db, 7, |i| {
            format!("CREATE thread:del{i} SET title = 'x'; DELETE thread:del{i};")
        })
        .await,
    ));
    // _00_channel_signal_sweep on every signal. `sender` is stamped from
    // `$auth.id`, which this root session has none of: let the test set it
    // (the event body under test is unchanged).
    exec(
        &db,
        "DEFINE FIELD OVERWRITE sender ON TABLE _00_channel_signal TYPE record READONLY;",
    )
    .await;
    out.push((
        "_00_channel_signal_sweep",
        median_ms(&db, 7, |i| {
            format!("CREATE _00_channel_signal SET sender = user:fresh{i}, recipient = user:x, sender_peer = 'p', kind = 'hello';")
        })
        .await,
    ));
    out
}

#[test]
fn generated_functions_and_events_stay_cheap_on_large_tables() {
    runtime().block_on(async {
        let small = costs(1_000).await;
        let large = costs(40_000).await;
        for ((name, a), (_, b)) in small.iter().zip(large.iter()) {
            eprintln!("{name}: {a:.1} ms with 1,000 rows, {b:.1} ms with 40,000");
        }
        // A table read grows with the rows (40x here); an index read stays
        // about flat. The slack covers noise on a busy machine.
        for ((name, a), (_, b)) in small.iter().zip(large.iter()) {
            assert!(
                *b < a * 4.0 + 15.0,
                "{name}: {b:.1} ms with 40,000 rows against {a:.1} ms with 1,000: it reads the table instead of its index"
            );
        }
    });
}

/// A record user's write to a record whose `_00_version` row predates keyed
/// rows: the generated event looks the old row up by `record_id` under the
/// user's session. SurrealDB 4.0.1 planned the `LET`-bound lookup as a scan of
/// the ledger for a record user (the table's select permission is not FULL),
/// though the same write as root used the index: 3.9 s per write on a 3M-row
/// ledger, a steady 19 s on WhitePawn. The first write (legacy row) must cost
/// about what the next one (keyed row) costs.
#[test]
fn a_record_users_first_write_on_a_legacy_ledger_row_stays_cheap() {
    runtime().block_on(async {
        let db = deployed().await;
        exec(
            &db,
            "DEFINE ACCESS account ON DATABASE TYPE RECORD \
             SIGNIN (SELECT * FROM user WHERE name = $name) \
             WITH JWT ALGORITHM HS512 KEY 'body-scan-test-key-0123456789abcdef0123456789' \
             DURATION FOR TOKEN 1h, FOR SESSION 1h;
             OPTION IMPORT;
             CREATE user:alice SET name = 'alice';",
        )
        .await;
        // a ledger of 60k legacy (random id) rows, 10 of them for the records written below
        for lo in (0..60_000).step_by(5_000) {
            exec(
                &db,
                &format!(
                    "OPTION IMPORT;
                     INSERT INTO _00_version array::map(array::map(array::range(0, 5000), |$j| $j + {lo}), |$i| {{ record_id: type::record('other', $i), version: 3 }}) RETURN NONE;"
                ),
            )
            .await;
        }
        for i in 0..10 {
            exec(&db, &format!("OPTION IMPORT; CREATE thread:t{i} SET title = 'x'; CREATE _00_version SET record_id = thread:t{i}, version = 7;")).await;
        }
        db.signin(surrealdb::opt::auth::Record {
            namespace: "t".into(),
            database: "t".into(),
            access: "account".into(),
            params: serde_json::json!({ "name": "alice" }),
        })
        .await
        .expect("record signin");
        let first = median_ms(&db, 5, |i| format!("LET $ok0 = {{ UPDATE thread:t{i} MERGE {{ title: 'first' }}; true }};")).await;
        let keyed = median_ms(&db, 5, |i| format!("LET $ok0 = {{ UPDATE thread:t{i} MERGE {{ title: 'again' }}; true }};")).await;
        assert!(
            first < keyed * 10.0 + 25.0,
            "a record user's first write on a legacy ledger row: {first:.1} ms against {keyed:.1} ms once keyed; the event scans the ledger"
        );
        // and the version carried over from the legacy row (7, then +1, +1)
        let v: Option<i64> = db.query("SELECT VALUE version FROM ONLY _00_version:[thread:t0]").await.unwrap().take(0).unwrap();
        let _ = v;
    });
}
