//! The `@crdt` fold over a real embedded SurrealDB, through the `Db` port:
//! concurrent edits merge into one snapshot and text mirror, refused rows
//! never reach the document, rows waiting on predecessors stay, a lease keeps
//! a second node out, the published state hash matches a client's merge, and
//! the sweep rebuilds the tracker after a restart.

use std::sync::Arc;

use base64::Engine;
use loro::{ExportMode, LoroDoc, LoroList, LoroMap, LoroText};
use serde_json::{json, Value};
use ssp_node::crdt::{CrdtFold, DocKey, FoldConfig};
use ssp_node::ports::{Db, DbError};
use surrealdb::engine::local::{Db as MemEngine, Mem};
use surrealdb::Surreal;

struct MemDb(Arc<Surreal<MemEngine>>);

#[async_trait::async_trait]
impl Db for MemDb {
    async fn query(&self, surql: &str, binds: &[(&str, Value)]) -> Result<Vec<Value>, DbError> {
        let mut q = self.0.query(surql);
        for (name, value) in binds {
            q = q.bind(((*name).to_string(), value.clone()));
        }
        let mut response = q.await.map_err(|e| DbError::Transport(e.to_string()))?;
        let n = response.num_statements();
        let mut out = Vec::with_capacity(n);
        for i in 0..n {
            let val: surrealdb::types::Value =
                response.take(i).map_err(|e| DbError::Query(e.to_string()))?;
            out.push(val.into_json_value());
        }
        Ok(out)
    }
    async fn version(&self) -> Result<String, DbError> {
        Ok("mem".into())
    }
}

/// The server DDL the CLI generates for `thread.content` (`@crdt text`) and
/// `thread.body` (`@crdt prosemirror`), reduced to what the fold touches.
const SCHEMA: &str = r#"
DEFINE TABLE thread SCHEMAFULL;
DEFINE FIELD title ON TABLE thread TYPE string DEFAULT '';
DEFINE FIELD content ON TABLE thread TYPE string DEFAULT '' VALUE IF $auth.id != NONE { $before OR '' } ELSE { $value };
DEFINE FIELD body ON TABLE thread TYPE string DEFAULT '' VALUE IF $auth.id != NONE { $before OR '' } ELSE { $value };
DEFINE FIELD content__crdt ON TABLE thread TYPE option<object> FLEXIBLE VALUE IF $auth.id != NONE { $before } ELSE { $value };
DEFINE FIELD body__crdt ON TABLE thread TYPE option<object> FLEXIBLE VALUE IF $auth.id != NONE { $before } ELSE { $value };

DEFINE TABLE thread__crdt_log SCHEMAFULL COMMENT 'sp00ky:crdt_log body=prosemirror,content=text';
DEFINE FIELD doc ON TABLE thread__crdt_log TYPE record<thread> READONLY;
DEFINE FIELD field ON TABLE thread__crdt_log TYPE string ASSERT $value IN ['body', 'content'] READONLY;
DEFINE FIELD author ON TABLE thread__crdt_log TYPE option<record> VALUE $before OR $auth.id OR $value READONLY;
DEFINE FIELD peer ON TABLE thread__crdt_log TYPE string READONLY;
DEFINE FIELD data ON TABLE thread__crdt_log TYPE bytes READONLY;
DEFINE FIELD size ON TABLE thread__crdt_log TYPE int VALUE bytes::len(data) READONLY;
DEFINE FIELD created_at ON TABLE thread__crdt_log TYPE datetime VALUE $before OR time::now() READONLY;

DEFINE TABLE _00_crdt_doc SCHEMAFULL;
DEFINE FIELD doc ON TABLE _00_crdt_doc TYPE record;
DEFINE FIELD field ON TABLE _00_crdt_doc TYPE string;
DEFINE FIELD rev ON TABLE _00_crdt_doc TYPE int DEFAULT 0;
DEFINE FIELD snapshot ON TABLE _00_crdt_doc TYPE option<bytes>;
DEFINE FIELD upto ON TABLE _00_crdt_doc TYPE option<object> FLEXIBLE;
DEFINE FIELD state_hash ON TABLE _00_crdt_doc TYPE option<string>;
DEFINE FIELD lease_by ON TABLE _00_crdt_doc TYPE option<string>;
DEFINE FIELD lease_until ON TABLE _00_crdt_doc TYPE option<datetime>;
DEFINE FIELD folded_at ON TABLE _00_crdt_doc TYPE option<datetime>;

CREATE thread:t1 SET title = 'hello';
"#;

async fn setup() -> (Arc<Surreal<MemEngine>>, MemDb) {
    let raw = Arc::new(Surreal::new::<Mem>(()).await.unwrap());
    raw.use_ns("t").use_db("t").await.unwrap();
    raw.query(SCHEMA).await.unwrap().check().unwrap();
    let db = MemDb(Arc::clone(&raw));
    (raw, db)
}

fn b64(bytes: &[u8]) -> String {
    base64::engine::general_purpose::STANDARD.encode(bytes)
}

fn peer_doc(peer: u64) -> LoroDoc {
    let doc = LoroDoc::new();
    doc.set_peer_id(peer).unwrap();
    doc
}

/// Append one client's own new ops since `since` as a log row.
async fn append(db: &MemDb, field: &str, peer: u64, doc: &LoroDoc, since: &loro::VersionVector) {
    let blob = doc.export(ExportMode::updates(since)).unwrap();
    append_raw(db, field, &peer.to_string(), &blob).await;
}

async fn append_raw(db: &MemDb, field: &str, peer: &str, blob: &[u8]) {
    db.query(
        "CREATE thread__crdt_log SET doc = thread:t1, field = $field, peer = $peer, data = encoding::base64::decode($data)",
        &[("field", json!(field)), ("peer", json!(peer)), ("data", json!(b64(blob)))],
    )
    .await
    .unwrap();
}

async fn one(db: &MemDb, surql: &str) -> Value {
    db.query(surql, &[]).await.unwrap().into_iter().next().unwrap()
}

async fn log_rows(db: &MemDb) -> i64 {
    one(db, "RETURN count((SELECT VALUE id FROM thread__crdt_log))").await.as_i64().unwrap()
}

fn key(field: &str) -> DocKey {
    DocKey { log: "thread__crdt_log".into(), doc: "thread:t1".into(), field: field.into() }
}

fn folder(me: &str) -> CrdtFold {
    CrdtFold::new(FoldConfig::default(), me)
}

async fn snapshot_text(db: &MemDb, field: &str) -> String {
    let doc_row = one(
        db,
        &format!("SELECT VALUE encoding::base64::encode(snapshot) FROM ONLY type::record('_00_crdt_doc', [thread:t1, '{field}'])"),
    )
    .await;
    let bytes = base64::engine::general_purpose::STANDARD_NO_PAD
        .decode(doc_row.as_str().unwrap().trim_end_matches('='))
        .unwrap();
    let doc = LoroDoc::new();
    doc.import(&bytes).unwrap();
    doc.get_text(field).to_string()
}

#[tokio::test]
async fn concurrent_edits_fold_into_one_snapshot_and_mirror() {
    let (_raw, db) = setup().await;
    let (a, b) = (peer_doc(1), peer_doc(2));
    let empty = loro::VersionVector::default();
    a.get_text("content").insert(0, "Hello").unwrap();
    a.commit();
    b.get_text("content").insert(0, "World").unwrap();
    b.commit();
    append(&db, "content", 1, &a, &empty).await;
    append(&db, "content", 2, &b, &empty).await;

    let out = folder("ssp-a").fold_one(&db, key("content")).await.expect("fold ran");
    assert!(out.published && !out.lost, "{out:?}");
    assert_eq!((out.rev, out.folded, out.rejected, out.kept), (1, 2, 0, 0));

    // The canonical document holds both edits, merged as any client merges them.
    a.import(&b.export(ExportMode::all_updates()).unwrap()).unwrap();
    let merged = a.get_text("content").to_string();
    assert_eq!(snapshot_text(&db, "content").await, merged);

    let parent = one(&db, "SELECT content, content__crdt, title FROM ONLY thread:t1").await;
    assert_eq!(parent["content"], json!(merged));
    assert_eq!(parent["title"], json!("hello"), "the fold only writes its field");
    assert_eq!(parent["content__crdt"]["rev"], json!(1));
    let vv = &parent["content__crdt"]["upto"]["vv"];
    assert_eq!((vv["1"].clone(), vv["2"].clone()), (json!(5), json!(5)));
    assert_eq!(log_rows(&db).await, 0);

    // The published hash is the hash of the document any client merges.
    let hash = json!(ssp_node::crdt::hash::state_hash(&a));
    assert_eq!(parent["content__crdt"]["state_hash"], hash);
    let stored = one(&db, "SELECT VALUE state_hash FROM ONLY type::record('_00_crdt_doc', [thread:t1, 'content'])").await;
    assert_eq!(stored, hash);

    // Later edits fold on top of the published snapshot.
    let before = a.oplog_vv();
    a.get_text("content").insert(0, "> ").unwrap();
    a.commit();
    append(&db, "content", 1, &a, &before).await;
    let out = folder("ssp-a").fold_one(&db, key("content")).await.unwrap();
    assert_eq!((out.rev, out.folded), (2, 1));
    assert_eq!(snapshot_text(&db, "content").await, a.get_text("content").to_string());
}

#[tokio::test]
async fn refused_rows_never_reach_the_document() {
    let (_raw, db) = setup().await;
    let a = peer_doc(1);
    a.get_text("content").insert(0, "mine").unwrap();
    a.commit();
    let blob = a.export(ExportMode::all_updates()).unwrap();
    // Claimed under someone else's peer, a snapshot, and garbage.
    append_raw(&db, "content", "2", &blob).await;
    append_raw(&db, "content", "1", &a.export(ExportMode::Snapshot).unwrap()).await;
    append_raw(&db, "content", "1", &[9, 9, 9]).await;

    let out = folder("ssp-a").fold_one(&db, key("content")).await.unwrap();
    assert_eq!((out.published, out.folded, out.rejected), (false, 0, 3), "{out:?}");
    assert_eq!(log_rows(&db).await, 0);
    let parent = one(&db, "SELECT VALUE content FROM ONLY thread:t1").await;
    assert_eq!(parent, json!(""));
}

#[tokio::test]
async fn rows_waiting_on_a_predecessor_stay_until_it_arrives() {
    let (_raw, db) = setup().await;
    let a = peer_doc(1);
    let empty = loro::VersionVector::default();
    a.get_text("content").insert(0, "first").unwrap();
    a.commit();
    let first = a.export(ExportMode::updates(&empty)).unwrap();
    let mid = a.oplog_vv();
    a.get_text("content").insert(5, " second").unwrap();
    a.commit();
    append(&db, "content", 1, &a, &mid).await; // only the second edit

    let out = folder("ssp-a").fold_one(&db, key("content")).await.unwrap();
    assert_eq!((out.published, out.folded, out.kept), (false, 0, 1), "{out:?}");

    append_raw(&db, "content", "1", &first).await;
    let out = folder("ssp-a").fold_one(&db, key("content")).await.unwrap();
    assert_eq!((out.published, out.folded, out.kept), (true, 2, 0), "{out:?}");
    assert_eq!(snapshot_text(&db, "content").await, "first second");
}

#[tokio::test]
async fn a_held_lease_keeps_a_second_node_out() {
    let (_raw, db) = setup().await;
    let a = peer_doc(1);
    a.get_text("content").insert(0, "x").unwrap();
    a.commit();
    append(&db, "content", 1, &a, &Default::default()).await;
    db.query(
        "INSERT INTO _00_crdt_doc { id: type::record('_00_crdt_doc', [thread:t1, 'content']), doc: thread:t1, field: 'content', rev: 0, lease_by: 'ssp-a', lease_until: time::now() + 1m }",
        &[],
    )
    .await
    .unwrap();

    let out = folder("ssp-b").fold_one(&db, key("content")).await.unwrap();
    assert!(out.lost, "{out:?}");
    assert_eq!(log_rows(&db).await, 1);
    // The holder itself renews and folds.
    let out = folder("ssp-a").fold_one(&db, key("content")).await.unwrap();
    assert!(out.published, "{out:?}");
}

#[tokio::test]
async fn a_stale_cache_reloads_the_newer_snapshot() {
    let (_raw, db) = setup().await;
    let (a, b) = (peer_doc(1), peer_doc(2));
    a.get_text("content").insert(0, "a").unwrap();
    a.commit();
    append(&db, "content", 1, &a, &Default::default()).await;
    let node_a = folder("ssp-a");
    let node_b = folder("ssp-b");
    node_a.fold_one(&db, key("content")).await.unwrap(); // rev 1, cached on A

    b.get_text("content").insert(0, "b").unwrap();
    b.commit();
    append(&db, "content", 2, &b, &Default::default()).await;
    node_b.fold_one(&db, key("content")).await.unwrap(); // rev 2 by B

    let c = peer_doc(3);
    c.get_text("content").insert(0, "c").unwrap();
    c.commit();
    append(&db, "content", 3, &c, &Default::default()).await;
    let out = node_a.fold_one(&db, key("content")).await.unwrap(); // A's cache says rev 1
    assert_eq!(out.rev, 3);
    let text = snapshot_text(&db, "content").await;
    for ch in ["a", "b", "c"] {
        assert!(text.contains(ch), "lost {ch}: {text}");
    }
}

#[tokio::test]
async fn prosemirror_fields_mirror_their_plain_text() {
    let (_raw, db) = setup().await;
    let a = peer_doc(1);
    let root = a.get_map("doc");
    root.insert("nodeName", "doc").unwrap();
    let blocks = root.get_or_create_container("children", LoroList::new()).unwrap();
    for line in ["Title", "Body text"] {
        let p = blocks.insert_container(blocks.len(), LoroMap::new()).unwrap();
        p.insert("nodeName", "paragraph").unwrap();
        let runs = p.get_or_create_container("children", LoroList::new()).unwrap();
        runs.insert_container(0, LoroText::new()).unwrap().insert(0, line).unwrap();
    }
    a.commit();
    append(&db, "body", 1, &a, &Default::default()).await;

    let out = folder("ssp-a").fold_one(&db, key("body")).await.unwrap();
    assert!(out.published, "{out:?}");
    assert_eq!(one(&db, "SELECT VALUE body FROM ONLY thread:t1").await, json!("Title\nBody text"));
}

#[tokio::test]
async fn the_sweep_rebuilds_the_tracker_and_drops_orphans() {
    let (_raw, db) = setup().await;
    let a = peer_doc(1);
    a.get_text("content").insert(0, "x").unwrap();
    a.commit();
    append(&db, "content", 1, &a, &Default::default()).await;

    let node = folder("ssp-a");
    node.sweep(&db).await.unwrap();
    assert_eq!(node.info()["pending_rows"], json!(1));

    db.query("DELETE thread:t1", &[]).await.unwrap();
    node.sweep(&db).await.unwrap();
    assert_eq!(log_rows(&db).await, 0);
}

/// Update blobs with a valid header and checksum whose bodies make Loro's
/// decoder panic (found by fuzzing in the CRDT v2 spike). A client can write
/// such a row; it must cost a rejection, never the process or the document.
#[tokio::test]
async fn hostile_blobs_are_refused_without_taking_the_fold_down() {
    #[derive(serde::Deserialize)]
    struct Hostile {
        peer: String,
        blob: String,
    }
    let hostile: Vec<Hostile> =
        serde_json::from_str(include_str!("fixtures/crdt_hostile_blobs.json")).unwrap();
    assert!(!hostile.is_empty());

    let (_raw, db) = setup().await;
    for h in &hostile {
        let bytes = base64::engine::general_purpose::STANDARD.decode(&h.blob).unwrap();
        append_raw(&db, "content", &h.peer, &bytes).await;
    }
    let a = peer_doc(1);
    a.get_text("content").insert(0, "still here").unwrap();
    a.commit();
    append(&db, "content", 1, &a, &Default::default()).await;

    let out = folder("ssp-a").fold_one(&db, key("content")).await.expect("fold ran");
    assert_eq!((out.published, out.folded, out.rejected), (true, 1, hostile.len()), "{out:?}");
    assert_eq!(log_rows(&db).await, 0);
    assert_eq!(snapshot_text(&db, "content").await, "still here");
}
