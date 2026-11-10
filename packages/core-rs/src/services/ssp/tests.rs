use super::*;
use crate::kernel::ports::{LocalPort, WriteMode};
use crate::services::local::{LocalStoreAdapter, LocalStoreHolder};
use crate::testing::row;
use crate::types::StoreType;
use serde_json::json;

fn ids(array: &RecordVersionArray) -> Vec<String> {
    array.iter().map(|(id, _)| id.clone()).collect()
}

fn id_set(array: &RecordVersionArray) -> std::collections::BTreeSet<String> {
    array.iter().map(|(id, _)| id.clone()).collect()
}

fn view(id: &str) -> Json {
    view_of(id, "SELECT * FROM thread")
}

fn view_of(id: &str, surql: &str) -> Json {
    json!({
        "id": id,
        "surql": surql,
        "params": {},
        "clientId": "local",
        "ttl": "10m",
        "lastActiveAt": "2026-01-01T00:00:00.000Z",
    })
}

fn thread(id: &str, rv: i64, title: &str) -> IngestRecord {
    IngestRecord::new(
        "thread",
        IngestOp::Create,
        format!("thread:{id}"),
        row(json!({"id": format!("thread:{id}"), "title": title, "_00_rv": rv})),
    )
}

fn plan(hash: &str, surql: &str, params: serde_json::Value) -> QueryPlanConfig {
    QueryPlanConfig {
        query_hash: hash.into(),
        surql: surql.into(),
        params: row(params),
        ttl: "10m".into(),
        last_active_at: DateTime::parse_from_rfc3339("2026-01-01T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc),
    }
}

/// A service with permissions seeded and a receiver capturing every update.
fn service(schema: &str) -> (Rc<StreamProcessorService>, Rc<RefCell<Vec<StreamUpdate>>>) {
    let svc = Rc::new(StreamProcessorService::new());
    svc.init();
    svc.seed_permissions_from_schema(schema);
    let received = Rc::new(RefCell::new(Vec::new()));
    let sink = received.clone();
    svc.add_receiver(Rc::new(move |u| sink.borrow_mut().push(u)));
    (svc, received)
}

const THREAD_SCHEMA: &str = "DEFINE TABLE thread SCHEMAFULL PERMISSIONS FOR select WHERE true;";

// ---- ffi/stream_processor_test.dart -------------------------------------------------

#[test]
fn registers_a_view_and_ingests_a_matching_record() {
    let mut sp = Processor::new();
    sp.set_permission("thread", "true");
    let initial = sp.register_view(view("q1")).unwrap();
    assert_eq!(initial.update.query_hash, "q1");
    assert!(initial.update.local_array.is_empty());
    assert!(initial.missing_fields.is_empty());

    let updates = sp
        .ingest(
            "thread",
            IngestOp::Create,
            "thread:a",
            &row(json!({"id": "thread:a", "title": "hello"})),
        )
        .unwrap();
    let u = updates.iter().find(|u| u.query_hash == "q1").unwrap();
    assert!(ids(&u.local_array).contains(&"thread:a".to_string()));
    assert_eq!(u.op.as_deref(), Some("CREATE"));
    // Set by the service, not the processor.
    assert_eq!(u.materialization_time_ms, None);
}

#[test]
fn save_load_state_round_trips() {
    let mut sp = Processor::new();
    sp.set_permission("thread", "true");
    sp.register_view(view("q2")).unwrap();
    sp.ingest(
        "thread",
        IngestOp::Create,
        "thread:b",
        &row(json!({"id": "thread:b", "title": "x"})),
    )
    .unwrap();
    let state = sp.save_state().unwrap();
    assert!(!state.is_empty());
    let mut sp2 = Processor::new();
    sp2.load_state(&state).unwrap();
}

#[test]
fn accepts_a_related_shaped_subquery_view_and_tracks_both_tables() {
    // The client registers the FULL surql (subquery projections included), and
    // a change to a CHILD row has to re-emit the parent view: otherwise a
    // relation would only refresh when its parent happened to change.
    let mut sp = Processor::new();
    sp.set_permission("thread", "true");
    sp.set_permission("comment", "true");
    sp.register_view(view_of(
        "q3",
        "SELECT *, (SELECT * FROM comment WHERE thread=$parent.id) AS comments FROM thread",
    ))
    .unwrap();

    let after_parent = sp
        .ingest(
            "thread",
            IngestOp::Create,
            "thread:a",
            &row(json!({"id": "thread:a", "title": "t"})),
        )
        .unwrap();
    let u = after_parent.iter().find(|u| u.query_hash == "q3").unwrap();
    assert!(ids(&u.local_array).contains(&"thread:a".to_string()));

    let after_child = sp
        .ingest(
            "comment",
            IngestOp::Create,
            "comment:c1",
            &row(json!({"id": "comment:c1", "thread": "thread:a", "body": "b"})),
        )
        .unwrap();
    assert!(
        after_child.iter().any(|u| u.query_hash == "q3"),
        "a child change must re-emit the parent view"
    );
}

#[test]
fn surfaces_circuit_errors() {
    let mut sp = Processor::new();
    let err = sp
        .register_view(json!({"surql": "SELECT * FROM thread"}))
        .unwrap_err();
    assert!(matches!(err, Error::Ssp(_)), "{err:?}");
}

// ---- ffi/stream_processor_store_test.dart -------------------------------------------

fn store_processor() -> Processor {
    let mut sp = Processor::new();
    sp.set_permission("thread", "true");
    sp
}

#[test]
fn ingest_many_applies_a_whole_batch_in_one_step_in_order() {
    let mut sp = store_processor();
    sp.register_view(view("q1")).unwrap();
    let mut last = thread("a", 2, "last");
    last.op = IngestOp::Update;
    let updates = sp
        .ingest_many(&[thread("a", 1, "t"), thread("b", 1, "t"), last])
        .unwrap();
    assert_eq!(updates.len(), 1, "one coalesced update per view");
    let u = &updates[0];
    assert_eq!(u.query_hash, "q1");
    assert_eq!(
        id_set(&u.local_array),
        ["thread:a".to_string(), "thread:b".to_string()].into()
    );
    assert!(
        u.local_array.contains(&("thread:a".to_string(), 2)),
        "last write wins"
    );
    assert!(sp.ingest_many(&[]).unwrap().is_empty());
}

#[test]
fn a_saved_store_restores_under_views_registered_after_it() {
    let mut sp = store_processor();
    sp.register_view(view("q1")).unwrap();
    sp.ingest_many(&[thread("a", 1, "t"), thread("b", 1, "t")])
        .unwrap();
    let snapshot = sp.save_store_state().unwrap();
    assert!(!snapshot.is_empty());

    // A fresh circuit registers its view against an empty store, then the
    // snapshot lands underneath it and the view catches up.
    let mut restored = store_processor();
    let reg = restored.register_view(view("q1")).unwrap();
    assert!(reg.update.local_array.is_empty());
    let updates = restored.load_store_state(&snapshot).unwrap();
    let caught = updates.iter().find(|u| u.query_hash == "q1").unwrap();
    assert_eq!(
        id_set(&caught.local_array),
        ["thread:a".to_string(), "thread:b".to_string()].into()
    );
    assert!(restored.load_store_state(&[]).unwrap().is_empty());
}

#[test]
fn loading_a_corrupt_snapshot_reports_an_error_rather_than_crashing() {
    let mut sp = store_processor();
    let mut bytes = sp.save_store_state().unwrap();
    bytes[0] = 0x00;
    assert!(matches!(sp.load_store_state(&bytes), Err(Error::Ssp(_))));
    assert!(sp.load_store_state(&[1, 2, 3]).is_err());
}

#[test]
fn reconcile_deletes_what_the_caller_no_longer_has_and_asks_for_the_rest() {
    let mut sp = store_processor();
    sp.register_view(view("q1")).unwrap();
    sp.ingest_many(&[thread("a", 1, "t"), thread("b", 1, "t")])
        .unwrap();

    let result = sp
        .reconcile(
            "thread",
            &[
                ("thread:b".into(), 2), // the caller holds a newer body
                ("thread:c".into(), 1), // the circuit has never seen it
            ],
        )
        .unwrap();
    let fetch: std::collections::BTreeSet<_> = result.fetch.iter().cloned().collect();
    assert_eq!(
        fetch,
        ["thread:b".to_string(), "thread:c".to_string()].into()
    );
    assert_eq!(result.deleted, 1, "thread:a is not in the list any more");
    let after = result
        .updates
        .iter()
        .find(|u| u.query_hash == "q1")
        .unwrap();
    assert!(!ids(&after.local_array).contains(&"thread:a".to_string()));
}

#[test]
fn max_row_versions_reports_the_highest_rv_folded_into_each_table() {
    let mut sp = store_processor();
    sp.register_view(view("q1")).unwrap();
    sp.ingest_many(&[thread("a", 3, "t"), thread("b", 7, "t")])
        .unwrap();
    assert_eq!(sp.max_row_versions()["thread"], 7);
}

#[test]
fn projection_can_be_turned_on_before_the_first_ingest() {
    let mut sp = store_processor();
    sp.set_projection(true);
    sp.register_view(view("q1")).unwrap();
    let updates = sp.ingest_many(&[thread("a", 1, "t")]).unwrap();
    assert_eq!(ids(&updates[0].local_array), vec!["thread:a".to_string()]);
    sp.set_projection(false);
}

#[test]
fn registration_timings_come_back_on_the_register_path() {
    let mut sp = store_processor();
    let reg = sp.register_view(view("q1")).unwrap();
    assert!(reg.update.parse_ms.is_some());
    // The ingest side is not measured by a registration.
    assert_eq!(reg.update.store_apply_ms, None);
}

#[test]
fn bytes_are_ingested_as_null() {
    let mut record = row(json!({"id": "thread:a", "title": "t"}));
    record.insert("_00_crdt".into(), Value::Bytes(vec![1, 2, 3]));
    record.insert(
        "author".into(),
        Value::RecordId(crate::RecordId::new("user", "k")),
    );
    let normalized = normalize_value(&Value::Object(record));
    assert_eq!(normalized["_00_crdt"], Json::Null);
    assert_eq!(normalized["author"], json!("user:k"));
}

// ---- stream_processor_batch_test.dart -----------------------------------------------

fn batch_service() -> (Rc<StreamProcessorService>, Rc<RefCell<Vec<StreamUpdate>>>) {
    let (svc, received) =
        service("DEFINE TABLE user SCHEMAFULL PERMISSIONS FOR select WHERE true;");
    svc.register_query_plan(&plan("q1", "SELECT * FROM user", json!({})))
        .unwrap();
    received.borrow_mut().clear();
    (svc, received)
}

fn ingest_user(svc: &StreamProcessorService, n: u32) {
    svc.ingest(
        "user",
        IngestOp::Create,
        &format!("user:{n}"),
        &row(json!({"id": format!("user:{n}"), "name": format!("u{n}")})),
    );
}

#[test]
fn emits_one_update_per_record_when_not_batching() {
    let (svc, received) = batch_service();
    ingest_user(&svc, 1);
    ingest_user(&svc, 2);
    ingest_user(&svc, 3);
    assert_eq!(received.borrow().len(), 3);
}

#[test]
fn coalesces_a_batch_into_a_single_update_with_the_final_array() {
    let (svc, received) = batch_service();
    svc.begin_batch();
    ingest_user(&svc, 1);
    ingest_user(&svc, 2);
    ingest_user(&svc, 3);
    assert!(
        received.borrow().is_empty(),
        "nothing dispatched until end_batch"
    );
    svc.end_batch();

    let received = received.borrow();
    assert_eq!(received.len(), 1);
    let update = &received[0];
    assert_eq!(update.query_hash, "q1");
    assert_eq!(
        id_set(&update.local_array),
        [
            "user:1".to_string(),
            "user:2".to_string(),
            "user:3".to_string()
        ]
        .into()
    );
    assert_eq!(update.local_array.len(), 3);
    // Coalesced updates take the immediate (non-debounced) path.
    assert_eq!(update.op.as_deref(), Some("CREATE"));
    assert!(update.materialization_time_ms.is_some());
}

#[test]
fn batch_ingest_of_new_records_matching_a_filtered_query_emits_them() {
    // The initial down-sync path: register a record-filtered query, then
    // bulk-fetch the matching rows and ingest them as ONE batch.
    let (svc, received) = batch_service();
    svc.seed_permissions_from_schema("DEFINE TABLE game PERMISSIONS FOR select WHERE true;");
    svc.register_query_plan(&plan(
        "qg",
        "SELECT * FROM game WHERE database = $db ORDER BY sort_index ASC",
        // The sanitized string form the local circuit receives.
        json!({"db": "game_database:DBS_x"}),
    ))
    .unwrap();
    received.borrow_mut().clear();

    svc.begin_batch();
    for i in 0..4 {
        svc.ingest(
            "game",
            IngestOp::Create,
            &format!("game:g{i}"),
            &row(json!({
                "id": format!("game:g{i}"),
                "database": "game_database:DBS_x",
                "white": "W",
                "black": "B",
                "pgn": "1. e4 e5",
                "result": "1-0",
                "status": "Finished",
                "sort_index": -i,
            })),
        );
    }
    svc.end_batch();

    let received = received.borrow();
    let last = received
        .iter()
        .rev()
        .find(|u| u.query_hash == "qg")
        .expect("filtered query emits");
    assert_eq!(last.local_array.len(), 4);
    for i in 0..4 {
        assert!(ids(&last.local_array).contains(&format!("game:g{i}")));
    }
}

#[test]
fn end_batch_is_safe_with_no_buffered_updates_and_begin_batch_is_idempotent() {
    let (svc, received) = batch_service();
    svc.begin_batch();
    svc.begin_batch();
    svc.end_batch();
    assert!(received.borrow().is_empty());
    // A later ingest dispatches normally (window closed).
    ingest_user(&svc, 1);
    assert_eq!(received.borrow().len(), 1);
}

#[test]
fn ingest_many_coalesces_inside_a_batch_and_sums_times() {
    let (svc, received) = batch_service();
    svc.begin_batch();
    ingest_user(&svc, 1);
    svc.ingest_many(vec![IngestRecord::new(
        "user",
        IngestOp::Create,
        "user:2",
        row(json!({"id": "user:2", "name": "u2"})),
    )]);
    svc.end_batch();
    let received = received.borrow();
    assert_eq!(received.len(), 1);
    assert_eq!(received[0].local_array.len(), 2);
}

// ---- feature_flag_permission_test.dart -----------------------------------------------

const FEATURE_QUERY: &str = "SELECT key, variant, payload FROM _00_user_feature";

fn feature_row(key: &str, variant: &str) -> Row {
    row(json!({
        "id": format!("_00_user_feature:{key}"),
        "user": "user:alice",
        "key": key,
        "variant": variant,
    }))
}

#[test]
fn with_the_permission_seeded_the_assignment_materializes() {
    let mut sp = Processor::new();
    sp.set_permission("_00_user_feature", "true");
    sp.register_view(view_of("ff", FEATURE_QUERY)).unwrap();
    let updates = sp
        .ingest(
            "_00_user_feature",
            IngestOp::Create,
            "_00_user_feature:demo",
            &feature_row("demo", "on"),
        )
        .unwrap();
    let u = updates.iter().find(|u| u.query_hash == "ff").unwrap();
    assert!(ids(&u.local_array).contains(&"_00_user_feature:demo".to_string()));
}

#[test]
fn an_explicit_deny_is_enforced() {
    // The circuit gates on the permission text: `false` rejects the view
    // outright. This is why the built-in `true` seed matters.
    let mut sp = Processor::new();
    sp.set_permission("_00_user_feature", "false");
    assert!(matches!(
        sp.register_view(view_of("ff", FEATURE_QUERY)),
        Err(Error::Ssp(_))
    ));
}

#[test]
fn seed_permits_user_feature_even_when_the_schema_omits_it() {
    let (svc, _) = service(THREAD_SCHEMA);
    svc.register_query_plan(&plan("ff", FEATURE_QUERY, json!({})))
        .unwrap();
    let updates = svc.ingest(
        "_00_user_feature",
        IngestOp::Create,
        "_00_user_feature:demo",
        &feature_row("demo", "on"),
    );
    let u = updates.iter().find(|u| u.query_hash == "ff").unwrap();
    assert!(
        ids(&u.local_array).contains(&"_00_user_feature:demo".to_string()),
        "built-in seed must let the assignment through the view"
    );
    // The release table is seeded too.
    svc.register_query_plan(&plan("rel", "SELECT * FROM _00_app_release", json!({})))
        .unwrap();
}

// ---- services_test.dart: StreamProcessorService ---------------------------------------

#[test]
fn seeds_permissions_then_registers_and_ingests_through_receivers() {
    let (svc, captured) = service(THREAD_SCHEMA);
    let initial = svc
        .register_query_plan(&plan("q1", "SELECT * FROM thread", json!({})))
        .unwrap();
    assert_eq!(initial.query_hash, "q1");
    svc.ingest(
        "thread",
        IngestOp::Create,
        "thread:a",
        &row(json!({"id": "thread:a", "title": "t"})),
    );
    let captured = captured.borrow();
    let last = captured.last().unwrap();
    assert!(ids(&last.local_array).contains(&"thread:a".to_string()));
    assert!(last.materialization_time_ms.is_some());
    assert!(svc.snapshot_dirty());
    assert_eq!(svc.dirty_rows(), 1);
}

#[test]
fn an_uninitialized_service_registers_nothing() {
    let svc = StreamProcessorService::new();
    assert!(svc
        .register_query_plan(&plan("q1", "SELECT * FROM thread", json!({})))
        .is_err());
    assert!(svc
        .ingest(
            "thread",
            IngestOp::Create,
            "thread:a",
            &row(json!({"id": "thread:a"}))
        )
        .is_empty());
    assert!(svc.ingest_many(vec![thread("a", 1, "t")]).is_empty());
    assert!(svc.save_store_snapshot().is_none());
    svc.unregister_query_plan("q1");
}

#[test]
fn session_auth_is_injected_into_registrations() {
    let (svc, _) =
        service("DEFINE TABLE note SCHEMAFULL PERMISSIONS FOR select WHERE owner = $auth.id;");
    assert_eq!(svc.session_auth(), (String::new(), String::new()));
    svc.set_session_auth(Some("user:alice"), Some("account"));
    assert_eq!(
        svc.session_auth(),
        ("user:alice".to_string(), "account".to_string())
    );
    svc.register_query_plan(&plan("n", "SELECT * FROM note", json!({})))
        .unwrap();
    let updates = svc.ingest_many(vec![
        IngestRecord::new(
            "note",
            IngestOp::Create,
            "note:mine",
            row(json!({"id": "note:mine", "owner": "user:alice"})),
        ),
        IngestRecord::new(
            "note",
            IngestOp::Create,
            "note:theirs",
            row(json!({"id": "note:theirs", "owner": "user:bob"})),
        ),
    ]);
    let u = updates.iter().find(|u| u.query_hash == "n").unwrap();
    assert_eq!(ids(&u.local_array), vec!["note:mine".to_string()]);
    svc.set_session_auth(None, None);
    assert_eq!(svc.session_auth(), (String::new(), String::new()));
}

#[test]
fn reset_circuit_starts_empty_and_clean() {
    let (svc, received) = service(THREAD_SCHEMA);
    svc.register_query_plan(&plan("q1", "SELECT * FROM thread", json!({})))
        .unwrap();
    svc.ingest_many(vec![thread("a", 1, "t")]);
    svc.begin_batch();
    assert!(svc.snapshot_dirty());
    svc.set_session_auth(Some("user:a"), None);

    svc.reset_circuit();
    assert!(!svc.snapshot_dirty());
    assert_eq!(
        svc.session_auth().0,
        "user:a",
        "the session identity survives"
    );
    assert!(svc.max_row_versions().is_empty(), "the rows are gone");
    // Permissions are seeded again by the switch.
    svc.seed_permissions_from_schema(THREAD_SCHEMA);
    received.borrow_mut().clear();
    let initial = svc
        .register_query_plan(&plan("q1", "SELECT * FROM thread", json!({})))
        .unwrap();
    assert!(initial.local_array.is_empty());
    // The batch window was closed by the reset.
    svc.ingest_many(vec![thread("b", 1, "t")]);
    assert_eq!(received.borrow().len(), 1);
}

#[test]
fn a_receiver_may_call_back_into_the_service() {
    let (svc, _) = service(THREAD_SCHEMA);
    svc.register_query_plan(&plan("q1", "SELECT * FROM thread", json!({})))
        .unwrap();
    let weak = Rc::downgrade(&svc);
    let seen = Rc::new(Cell::new(0usize));
    let s = seen.clone();
    svc.set_receivers(vec![Rc::new(move |_u| {
        s.set(s.get() + 1);
        if let Some(svc) = weak.upgrade() {
            let _ = svc.max_row_versions();
            let _ = svc.save_store_snapshot();
        }
    })]);
    svc.ingest_many(vec![thread("a", 1, "t")]);
    assert_eq!(seen.get(), 1);
}

#[test]
fn unregister_stops_updates_for_that_view() {
    let (svc, received) = service(THREAD_SCHEMA);
    svc.register_query_plan(&plan("q1", "SELECT * FROM thread", json!({})))
        .unwrap();
    svc.unregister_query_plan("q1");
    received.borrow_mut().clear();
    svc.ingest_many(vec![thread("a", 1, "t")]);
    assert!(received.borrow().iter().all(|u| u.query_hash != "q1"));
}

// ---- services/circuit_prime_test.dart ---------------------------------------------------

/// A file store, the way a client opens it.
struct Store {
    _dir: tempfile::TempDir,
    store: StoreType,
}

impl Store {
    fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let store = StoreType::File(dir.path().join("spooky.db"));
        Store { _dir: dir, store }
    }

    fn holder(&self) -> Rc<LocalStoreHolder> {
        let holder = Rc::new(LocalStoreHolder::new(self.store.clone()));
        holder.connect("anon").unwrap();
        holder
    }

    /// The anon store opened directly (a closed client's file).
    fn raw(&self) -> LocalDatabaseService {
        let StoreType::File(base) = &self.store else {
            unreachable!()
        };
        let db =
            LocalDatabaseService::open_file(&crate::services::local::bucket_path(base, "anon"))
                .unwrap();
        db.provision().unwrap();
        db
    }
}

fn tables() -> Vec<String> {
    vec!["thread".to_string()]
}

/// Boot a circuit over the holder's store: init, permissions, prime.
fn boot(holder: &LocalStoreHolder) -> (Rc<StreamProcessorService>, PrimeStats) {
    let (svc, _) = service(THREAD_SCHEMA);
    let stats = svc
        .prime_from_store(
            &holder.db().unwrap(),
            &tables(),
            &BTreeMap::new(),
            &HashSet::new(),
            None,
        )
        .unwrap();
    (svc, stats)
}

/// What a create does to the store and the circuit.
fn create(holder: &LocalStoreHolder, svc: &StreamProcessorService, id: &str, title: &str, rv: i64) {
    let doc = row(json!({"title": title, "_00_rv": rv}));
    holder.db().unwrap().create(id, doc.clone()).unwrap();
    let mut record = doc;
    record.insert("id".into(), Value::from(id));
    svc.ingest_many(vec![IngestRecord::new(
        "thread",
        IngestOp::Create,
        id,
        record,
    )]);
}

#[test]
fn a_restarted_client_sees_its_rows_without_touching_the_network() {
    let store = Store::new();
    let holder = store.holder();
    let (svc, _) = boot(&holder);
    create(&holder, &svc, "thread:a", "kept", 1);
    // The client writes a snapshot on close.
    assert!(svc.checkpoint_into(&holder.db().unwrap()).unwrap());
    svc.close();
    holder.close();

    let holder = store.holder();
    assert!(holder.db().unwrap().get_snapshot().unwrap().is_some());
    let (svc, stats) = boot(&holder);
    assert!(stats.restored);
    assert_eq!(stats.ingested, 0, "nothing changed since the snapshot");

    // The query's local window comes from the primed circuit.
    let update = svc
        .register_query_plan(&plan("q", "SELECT * FROM thread", json!({})))
        .unwrap();
    assert_eq!(ids(&update.local_array), vec!["thread:a".to_string()]);
    let rows = holder
        .db()
        .unwrap()
        .get_docs("thread", &ids(&update.local_array))
        .unwrap();
    assert_eq!(rows[0]["title"], Value::from("kept"));
}

#[test]
fn a_store_rebuilt_from_rows_is_due_a_checkpoint() {
    // Rows on disk and no snapshot: what a killed app leaves behind.
    let store = Store::new();
    let raw = store.raw();
    raw.put_doc(
        "thread",
        "thread:a",
        row(json!({"title": "a", "_00_rv": 1})),
        false,
        &[],
    )
    .unwrap();
    raw.close();

    let holder = store.holder();
    let db = holder.db().unwrap();
    let (svc, stats) = boot(&holder);
    assert!(!stats.restored);
    assert_eq!(stats.ingested, 1);
    assert!(db.get_snapshot().unwrap().is_none());
    assert!(
        svc.dirty_rows() >= CHECKPOINT_FLOOR,
        "the checkpoint timer writes it, no close needed"
    );
    assert!(svc.checkpoint_into(&db).unwrap());
    assert!(db.get_snapshot().unwrap().is_some());
}

#[test]
fn a_checkpoint_of_an_unchanged_circuit_writes_nothing() {
    let store = Store::new();
    let holder = store.holder();
    let db = holder.db().unwrap();
    let (svc, _) = boot(&holder);
    create(&holder, &svc, "thread:a", "a", 1);
    svc.register_query_plan(&plan("q", "SELECT * FROM thread", json!({})))
        .unwrap();
    assert!(svc.checkpoint_into(&db).unwrap());
    assert!(db.get_snapshot().unwrap().is_some());

    db.clear_snapshot().unwrap();
    assert!(!svc.checkpoint_into(&db).unwrap());
    assert!(
        db.get_snapshot().unwrap().is_none(),
        "nothing changed since the last snapshot"
    );

    create(&holder, &svc, "thread:b", "b", 1);
    assert!(svc.checkpoint_into(&db).unwrap());
    assert!(db.get_snapshot().unwrap().is_some());
    // The full circuit is never persisted any more.
    assert!(db.kv_get("_00_stream_processor_state").unwrap().is_none());
}

fn snapshot_mode_rebuilds_from_authoritative_rows(mode: &str) {
    let store = Store::new();
    let holder = store.holder();
    let (svc, _) = boot(&holder);
    create(&holder, &svc, "thread:a", "original", 1);
    create(&holder, &svc, "thread:b", "deleted", 1);
    assert!(svc.checkpoint_into(&holder.db().unwrap()).unwrap());
    svc.close();
    holder.close();

    // Edit the closed store directly: writes after an older checkpoint.
    let raw = store.raw();
    match mode {
        "missing" => raw.clear_snapshot().unwrap(),
        "corrupt" => raw.put_snapshot(&[1, 2, 3]).unwrap(),
        _ => {}
    }
    raw.put_doc(
        "thread",
        "thread:a",
        row(json!({"title": "latest", "_00_rv": 5})),
        false,
        &[],
    )
    .unwrap();
    raw.delete_doc("thread", "thread:b").unwrap();
    raw.close();

    let holder = store.holder();
    let (svc, stats) = boot(&holder);
    assert_eq!(stats.restored, mode == "stale", "{mode}");
    let update = svc
        .register_query_plan(&plan("q", "SELECT * FROM thread", json!({})))
        .unwrap();
    assert_eq!(
        update.local_array,
        vec![("thread:a".to_string(), 5)],
        "{mode}"
    );
    let rows = holder
        .db()
        .unwrap()
        .get_docs("thread", &ids(&update.local_array))
        .unwrap();
    assert_eq!(rows[0]["title"], Value::from("latest"));
}

#[test]
fn missing_snapshot_rebuilds_from_authoritative_rows() {
    snapshot_mode_rebuilds_from_authoritative_rows("missing");
}

#[test]
fn corrupt_snapshot_rebuilds_from_authoritative_rows() {
    snapshot_mode_rebuilds_from_authoritative_rows("corrupt");
}

#[test]
fn stale_snapshot_rebuilds_from_authoritative_rows() {
    snapshot_mode_rebuilds_from_authoritative_rows("stale");
}

#[test]
fn prime_catches_up_views_registered_before_it_and_reports_settled_versions() {
    let store = Store::new();
    let holder = store.holder();
    let (svc, _) = boot(&holder);
    create(&holder, &svc, "thread:a", "a", 2);
    create(&holder, &svc, "thread:b", "b", 3);
    svc.checkpoint_into(&holder.db().unwrap()).unwrap();
    svc.close();
    holder.close();

    let holder = store.holder();
    let (svc, received) = service(THREAD_SCHEMA);
    // A query registers against the empty circuit before the prime runs.
    let initial = svc
        .register_query_plan(&plan("q", "SELECT * FROM thread", json!({})))
        .unwrap();
    assert!(initial.local_array.is_empty());

    let reported = Rc::new(RefCell::new(Vec::new()));
    let sink = reported.clone();
    let on_versions = move |table: &str, entries: Vec<(String, i64)>| {
        sink.borrow_mut().push((table.to_string(), entries))
    };
    let pending: HashSet<String> = ["thread:b".to_string()].into();
    let stats = svc
        .prime_from_store(
            &holder.db().unwrap(),
            &tables(),
            &BTreeMap::new(),
            &pending,
            Some(&on_versions),
        )
        .unwrap();
    assert!(stats.restored);
    let caught = received
        .borrow()
        .iter()
        .rev()
        .find(|u| u.query_hash == "q")
        .cloned()
        .unwrap();
    assert_eq!(
        id_set(&caught.local_array),
        ["thread:a".to_string(), "thread:b".to_string()].into()
    );
    // A row with a queued local write is not reported at the server's version.
    assert_eq!(
        *reported.borrow(),
        vec![("thread".to_string(), vec![("thread:a".to_string(), 2)])]
    );
}

#[test]
fn prime_from_store_resets_changed_column_sets_first() {
    let store = Store::new();
    let holder = store.holder();
    let db = holder.db().unwrap();
    db.create("thread:a", row(json!({"title": "a", "_00_rv": 4})))
        .unwrap();
    let (svc, _) = service(THREAD_SCHEMA);
    let sets: BTreeMap<String, String> = [("thread".to_string(), "title".to_string())].into();
    let reported = Rc::new(RefCell::new(Vec::new()));
    let sink = reported.clone();
    let on_versions = move |_: &str, entries: Vec<(String, i64)>| sink.borrow_mut().extend(entries);
    svc.prime_from_store(&db, &tables(), &sets, &HashSet::new(), Some(&on_versions))
        .unwrap();
    assert_eq!(
        *reported.borrow(),
        vec![("thread:a".to_string(), 0)],
        "first boot reads as stale"
    );
    let update = svc
        .register_query_plan(&plan("q", "SELECT * FROM thread", json!({})))
        .unwrap();
    assert_eq!(ids(&update.local_array), vec!["thread:a".to_string()]);
}

// ---- local_read_write_test.dart, at the port level --------------------------------------

/// The local store and the circuit wired the way the runtime drives them.
fn ports() -> (
    LocalStoreAdapter,
    SspAdapter,
    Rc<RefCell<Vec<StreamUpdate>>>,
) {
    let holder = Rc::new(LocalStoreHolder::new(StoreType::Memory));
    holder.connect("anon").unwrap();
    let (svc, received) = service(THREAD_SCHEMA);
    (
        LocalStoreAdapter::new(holder),
        SspAdapter::new(svc),
        received,
    )
}

fn register(ssp: &SspAdapter, hash: &str) -> RegisterResult {
    ssp.register(RegisterPlan {
        query_hash: hash.into(),
        surql: "SELECT * FROM thread".into(),
        params: Object::new(),
        ttl: "10m".into(),
        table_name: "thread".into(),
    })
    .unwrap()
}

/// A local write and its circuit ingest, as the write saga commits them.
fn write(
    local: &LocalStoreAdapter,
    ssp: &SspAdapter,
    op: IngestOp,
    id: &str,
    data: serde_json::Value,
) {
    match op {
        IngestOp::Delete => {
            local.delete("thread", id).unwrap();
            ssp.ingest_many(vec![IngestRecord::delete(id)]).unwrap();
        }
        _ => {
            local
                .put("thread", id, row(data), WriteMode::Merge)
                .unwrap();
            let stored = local.get("thread", id).unwrap().unwrap();
            ssp.ingest_many(vec![IngestRecord::new("thread", op, id, stored)])
                .unwrap();
        }
    }
}

fn latest(received: &Rc<RefCell<Vec<StreamUpdate>>>, hash: &str) -> RecordVersionArray {
    received
        .borrow()
        .iter()
        .rev()
        .find(|u| u.query_hash == hash)
        .unwrap()
        .local_array
        .clone()
}

#[test]
fn a_registered_query_reflects_a_create() {
    let (local, ssp, received) = ports();
    let reg = register(&ssp, "q");
    assert!(reg.local_array.is_empty());
    assert!(reg.timings.parse_ms.is_some());

    write(
        &local,
        &ssp,
        IngestOp::Create,
        "thread:a",
        json!({"title": "hello", "_00_rv": 1}),
    );
    let array = latest(&received, "q");
    let rows = local.get_many("thread", &ids(&array)).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["title"], Value::from("hello"));
    assert_eq!(rows[0]["id"], Value::from("thread:a"));
}

#[test]
fn an_update_is_reflected_with_its_version() {
    let (local, ssp, received) = ports();
    register(&ssp, "q");
    write(
        &local,
        &ssp,
        IngestOp::Create,
        "thread:b",
        json!({"title": "first", "_00_rv": 1}),
    );
    write(
        &local,
        &ssp,
        IngestOp::Update,
        "thread:b",
        json!({"title": "second", "_00_rv": 2}),
    );
    let array = latest(&received, "q");
    assert_eq!(array, vec![("thread:b".to_string(), 2)]);
    let rows = local.get_many("thread", &ids(&array)).unwrap();
    assert_eq!(rows[0]["title"], Value::from("second"));
    assert_eq!(rows[0]["_00_rv"], Value::Int(2));
}

#[test]
fn a_delete_removes_the_record() {
    let (local, ssp, received) = ports();
    register(&ssp, "q");
    write(
        &local,
        &ssp,
        IngestOp::Create,
        "thread:c",
        json!({"title": "x", "_00_rv": 1}),
    );
    write(&local, &ssp, IngestOp::Delete, "thread:c", json!({}));
    assert!(latest(&received, "q").is_empty());
    assert!(local.get("thread", "thread:c").unwrap().is_none());
}

#[test]
fn ssp_adapter_unregister_and_register_errors() {
    let (_local, ssp, received) = ports();
    register(&ssp, "q");
    ssp.unregister("q").unwrap();
    received.borrow_mut().clear();
    ssp.ingest_many(vec![thread("a", 1, "t")]).unwrap();
    assert!(received.borrow().is_empty());
    let err = ssp
        .register(RegisterPlan {
            query_hash: "bad".into(),
            surql: "SELECT * FROM secret".into(),
            params: Object::new(),
            ttl: "10m".into(),
            table_name: "secret".into(),
        })
        .unwrap_err();
    assert!(matches!(err, Error::Ssp(_)), "{err:?}");
}
