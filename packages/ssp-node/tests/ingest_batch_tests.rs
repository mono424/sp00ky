use super::*;

fn row(op: &str, id: &str, n: i64) -> Value {
    json!({"table":"thread", "op":op, "id":id, "record":{"n":n, "owner":"user:a", "_00_rv":n+1}})
}

#[tokio::test]
async fn batch_matches_serial_membership_hash_and_versions_after_churn() {
    let serial = build(Default::default()).await;
    let batch = build(Default::default()).await;
    for h in [&serial, &batch] {
        h.node
            .processor
            .write()
            .await
            .set_permission("thread", "true");
        for (id, sql) in [
            ("all", "SELECT * FROM thread"),
            (
                "window",
                "SELECT * FROM thread WHERE n >= 3 ORDER BY n LIMIT 2",
            ),
        ] {
            let mut registration = thread_register(id);
            registration["surql"] = json!(sql);
            assert_eq!(
                h.node
                    .route(authed(Method::Post, "/view/register", registration))
                    .await
                    .unwrap()
                    .status,
                200
            );
        }
    }
    let mut rows = Vec::new();
    for i in 0..40 {
        rows.push(row("CREATE", &format!("thread:{i}"), i));
    }
    for i in 0..20 {
        rows.push(row("UPDATE", &format!("thread:{i}"), 100 + i));
    }
    rows.extend([
        row("DELETE", "thread:3", 103),
        row("CREATE", "thread:3", 200),
        row("MERGE", "thread:7", 300),
        row("DELETE", "thread:9", 109),
    ]);
    for body in &rows {
        assert_eq!(
            serial
                .node
                .route(authed(Method::Post, "/ingest", body.clone()))
                .await
                .unwrap()
                .status,
            200
        );
    }
    assert_eq!(
        batch
            .node
            .route(authed(
                Method::Post,
                "/ingest/batch",
                json!({"records":rows})
            ))
            .await
            .unwrap()
            .status,
        200
    );
    let left = serial.node.processor.read().await;
    let right = batch.node.processor.read().await;
    assert_eq!(left.compute_table_hashes(), right.compute_table_hashes());
    for id in ["all", "window"] {
        let a = left.get_view(id).unwrap();
        let b = right.get_view(id).unwrap();
        assert_eq!(a.cache, b.cache, "membership: {id}");
        assert_eq!(a.last_hash, b.last_hash, "result hash: {id}");
    }
    assert_eq!(right.store.get_record_version_by_key("thread:3"), Some(201));
    assert!(right.store.get_record_version_by_key("thread:9").is_none());
    let metrics = batch.node.view_metrics.read().await;
    assert!(metrics["all"].ingest.is_some());
}

#[tokio::test]
async fn invalid_batch_is_rejected_before_any_record_or_job_is_applied() {
    let h = build(HarnessOpts {
        job_tables: vec![("job", "http://worker")],
        ..Default::default()
    })
    .await;
    for bad in [
        json!({"table":"thread","op":"NOPE","id":"thread:x","record":{}}),
        json!({"table":"job","op":"CREATE","id":"job:x","record":{"status":"pending"}}),
        json!({"table":"user","op":"DELETE","id":"user:x","record":{}}),
        json!({"table":"_00_query_allowlist","op":"UPDATE","id":"_00_query_allowlist:x","record":{}}),
    ] {
        let result = h
            .node
            .route(authed(
                Method::Post,
                "/ingest/batch",
                json!({"records":[row("CREATE","thread:ok",1),bad]}),
            ))
            .await
            .unwrap();
        assert_eq!(result.status, 400);
        assert!(h
            .node
            .processor
            .read()
            .await
            .store
            .get_record_version_by_key("thread:ok")
            .is_none());
        assert!(h.job_rx.lock().await.try_recv().is_err());
        assert_eq!(h.node.edge_update_tx.snapshot().pending_batches, 0);
    }
}

#[tokio::test]
async fn batch_enforces_auth_readiness_count_bytes_and_backpressure() {
    let h = build(Default::default()).await;
    let body = json!({"records":[row("CREATE","thread:a",1)]});
    assert_eq!(
        h.node
            .route(req(Method::Post, "/ingest/batch", None, body.clone()))
            .await
            .unwrap()
            .status,
        401
    );
    *h.node.status.write().await = SspStatus::Bootstrapping;
    assert_eq!(
        h.node
            .route(authed(Method::Post, "/ingest/batch", body.clone()))
            .await
            .unwrap()
            .status,
        503
    );
    *h.node.status.write().await = SspStatus::Ready;
    for invalid in [
        json!({"records":[]}),
        json!({"records":vec![row("CREATE","thread:a",1);129]}),
        json!({}),
    ] {
        assert_eq!(
            h.node
                .route(authed(Method::Post, "/ingest/batch", invalid))
                .await
                .unwrap()
                .status,
            400
        );
    }
    let oversized = json!({"records":[{"table":"thread","op":"CREATE","id":"thread:a","record":{"blob":"x".repeat(ssp_protocol::MAX_INGEST_BATCH_BYTES)}}]});
    assert_eq!(
        h.node
            .route(authed(Method::Post, "/ingest/batch", oversized))
            .await
            .unwrap()
            .status,
        413
    );
    let mut permits = Vec::new();
    while let Some(permit) = h.node.edge_update_tx.try_reserve(0) {
        permits.push(permit);
    }
    assert_eq!(
        h.node
            .route(authed(Method::Post, "/ingest/batch", body.clone()))
            .await
            .unwrap()
            .status,
        503
    );
    assert!(h
        .node
        .processor
        .read()
        .await
        .store
        .get_record_version_by_key("thread:a")
        .is_none());
    drop(permits);
    assert_eq!(
        h.node
            .route(authed(Method::Post, "/ingest/batch", body))
            .await
            .unwrap()
            .status,
        200
    );
    let full = (0..128)
        .map(|i| row("CREATE", &format!("thread:{i}"), i))
        .collect::<Vec<_>>();
    assert_eq!(
        h.node
            .route(authed(
                Method::Post,
                "/ingest/batch",
                json!({"records":full})
            ))
            .await
            .unwrap()
            .status,
        200
    );
}

#[tokio::test]
async fn standby_batch_updates_local_state_without_publication() {
    let h = build(Default::default()).await;
    h.node
        .processor
        .write()
        .await
        .set_permission("thread", "true");
    assert_eq!(
        h.node
            .route(authed(
                Method::Post,
                "/view/register",
                thread_register("all")
            ))
            .await
            .unwrap()
            .status,
        200
    );
    h.node.edge_update_tx.set_standby(true);
    let pending_before = h.node.edge_update_tx.snapshot().pending_batches;
    assert_eq!(
        h.node
            .route(authed(
                Method::Post,
                "/ingest/batch",
                json!({"records":[row("CREATE","thread:a",1)]})
            ))
            .await
            .unwrap()
            .status,
        200
    );
    assert_eq!(
        h.node
            .processor
            .read()
            .await
            .get_view("all")
            .unwrap()
            .cache
            .len(),
        1
    );
    assert_eq!(
        h.node.edge_update_tx.snapshot().pending_batches,
        pending_before
    );
}

#[tokio::test]
async fn batch_delete_recreate_restores_rows_children_and_merged_subscriber_edges() {
    let h = build(HarnessOpts {
        merge_views: true,
        ..Default::default()
    })
    .await;
    h.raw_db
        .query(
            "DEFINE TABLE thread SCHEMALESS PERMISSIONS FOR select FULL; \
        DEFINE TABLE note SCHEMALESS PERMISSIONS FOR select FULL; \
        CREATE thread:a SET owner = user:a, n = 1; CREATE thread:keep SET owner = user:a, n = 2; \
        CREATE note:c SET thread = thread:a, owner = user:a, text = 'old'; \
        CREATE note:d SET thread = thread:a, owner = user:a, text = 'retained';",
        )
        .await
        .unwrap()
        .check()
        .unwrap();
    {
        let mut circuit = h.node.processor.write().await;
        circuit.set_permission("thread", "true");
        circuit.set_permission("note", "true");
    }
    let note = |op: &str, id: &str, text: &str| {
        json!({"table":"note", "op":op, "id":id,
        "record":{"thread":"thread:a", "owner":"user:a", "text":text}})
    };
    let seed = json!({"records":[row("CREATE","thread:a",1), row("CREATE","thread:keep",2),
        note("CREATE","note:c","old"), note("CREATE","note:d","retained")]});
    assert_eq!(
        h.node
            .route(authed(Method::Post, "/ingest/batch", seed))
            .await
            .unwrap()
            .status,
        200
    );
    for (id, client) in [("owner", "tab-a"), ("subscriber", "tab-b")] {
        let mut registration = thread_register(id);
        registration["clientId"] = json!(client);
        registration["surql"] =
            json!("SELECT *, (SELECT * FROM note WHERE thread=$parent.id) AS notes FROM thread");
        let response = h
            .node
            .route(authed(Method::Post, "/view/register", registration))
            .await
            .unwrap();
        assert_eq!(response.status, 200, "{:?}", json_of(&response));
    }
    publication_drained(&h).await;
    assert_eq!(
        h.node.processor.read().await.view_count(),
        1,
        "subscriber shares the graph"
    );
    for id in ["owner", "subscriber"] {
        assert_eq!(edge_count_of(&h, id).await, 4);
    }
    let mut before = h
        .raw_db
        .query(
            "SELECT VALUE <string>id FROM ONLY _00_list_ref \
        WHERE in = _00_query:owner AND out = thread:keep LIMIT 1",
        )
        .await
        .unwrap();
    let retained_edge: Option<String> = before.take(0).unwrap();

    // Model the actual source transaction: SurrealDB removes a deleted row's
    // graph edges, while the child's parent pointer can still name an old edge.
    h.raw_db
        .query(
            "DELETE thread:a; CREATE thread:a SET owner = user:a, n = 10; \
        DELETE note:c; CREATE note:c SET thread = thread:a, owner = user:a, text = 'new';",
        )
        .await
        .unwrap()
        .check()
        .unwrap();
    let body = json!({"records":[row("DELETE","thread:a",1), row("CREATE","thread:a",10),
        note("DELETE","note:c","old"), note("CREATE","note:c","new")]});
    assert_eq!(
        h.node
            .route(authed(Method::Post, "/ingest/batch", body.clone()))
            .await
            .unwrap()
            .status,
        200
    );
    publication_drained(&h).await;
    // A lost HTTP acknowledgement replays the same batch without repeating
    // the source transaction. Recreated edges must remain unique.
    assert_eq!(
        h.node
            .route(authed(Method::Post, "/ingest/batch", body))
            .await
            .unwrap()
            .status,
        200
    );
    publication_drained(&h).await;
    for id in ["owner", "subscriber"] {
        assert_eq!(
            edge_count_of(&h, id).await,
            4,
            "no survivor edge is lost or duplicated for {id}"
        );
        let mut parent = h
            .raw_db
            .query(format!(
                "SELECT VALUE <string>id FROM ONLY _00_list_ref \
            WHERE in = _00_query:{id} AND out = thread:a LIMIT 1"
            ))
            .await
            .unwrap();
        let parent: Option<String> = parent.take(0).unwrap();
        let parent = parent.expect("recreated parent relation exists");
        let mut children = h
            .raw_db
            .query(format!(
                "SELECT <string>out AS child, <string>parent AS parent \
            FROM _00_list_ref WHERE in = _00_query:{id} AND parent_rel = 'notes'"
            ))
            .await
            .unwrap();
        let mut children: Vec<Value> = children.take(0).unwrap();
        children.sort_by(|a, b| a["child"].as_str().cmp(&b["child"].as_str()));
        assert_eq!(
            children,
            json!([{"child":"note:c","parent":parent},{"child":"note:d","parent":parent}])
                .as_array()
                .unwrap()
                .clone(),
            "retained and recreated children bind to the current parent edge"
        );
        assert_eq!(row_count_of(&h, id).await, 2);
    }
    let mut after = h
        .raw_db
        .query(
            "SELECT VALUE <string>id FROM ONLY _00_list_ref \
        WHERE in = _00_query:owner AND out = thread:keep LIMIT 1",
        )
        .await
        .unwrap();
    let after: Option<String> = after.take(0).unwrap();
    assert_eq!(
        after, retained_edge,
        "unrelated members are not republished"
    );
}
