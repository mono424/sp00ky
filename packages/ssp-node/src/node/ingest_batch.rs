use super::*;
use ssp::circuit::{
    Change, ChangeSet, Circuit, Operation, SubqueryDeltaItem, SubqueryOp, ViewDelta,
};
use ssp_protocol::{IngestBatchRequest, MAX_INGEST_BATCH_BYTES, MAX_INGEST_BATCH_RECORDS};

/// A source delete removes its published edges even when a later write in the
/// same step restores membership. Recreate only those surviving keys; scanning
/// the full view cache here would undo the benefit of bounded ingestion.
fn restore_recreated_edges(
    circuit: &Circuit,
    deltas: &mut Vec<ViewDelta>,
    deleted: &std::collections::HashSet<String>,
    mode: ssp_protocol::RefMode,
    cleanup: &mut Vec<crate::edges::PublicationCleanup>,
) {
    if deleted.is_empty() {
        return;
    }
    let surviving: Vec<&str> = deleted
        .iter()
        .filter(|key| {
            key.split_once(':').is_some_and(|(table, _)| {
                circuit
                    .store
                    .get_collection(table)
                    .is_some_and(|collection| collection.has_key(key))
            })
        })
        .map(String::as_str)
        .collect();
    if surviving.is_empty() {
        return;
    }
    let mut positions: std::collections::HashMap<String, usize> = deltas
        .iter()
        .enumerate()
        .map(|(index, delta)| (delta.query_id.clone(), index))
        .collect();
    let mut drops: std::collections::HashSet<(String, String)> = cleanup
        .iter()
        .filter_map(|item| match item {
            crate::edges::PublicationCleanup::DropEdgesTo { table, record } => {
                Some((table.clone(), record.clone()))
            }
            _ => None,
        })
        .collect();
    for owner in circuit.view_ids() {
        let Some(view) = circuit.get_view(&owner) else {
            continue;
        };
        let additions: Vec<String> = surviving
            .iter()
            .filter(|key| view.cache.contains_key(**key))
            .map(|key| (*key).to_string())
            .collect();
        let mut children: std::collections::HashMap<String, SubqueryDeltaItem> = surviving
            .iter()
            .filter_map(|key| {
                view.subquery_cache
                    .get(*key)
                    .map(|(parent, alias)| SubqueryDeltaItem {
                        id: (*key).to_string(),
                        parent_key: parent.to_string(),
                        alias: alias.clone(),
                        op: SubqueryOp::Add,
                    })
            })
            .map(|item| (item.id.clone(), item))
            .collect();
        if !additions.is_empty() {
            // Recreated parent edges have new IDs. Their retained children
            // must also replace the old parent pointer. Only this rare path
            // scans the child cache; ordinary batches use direct lookups.
            let parents: std::collections::HashSet<&str> =
                additions.iter().map(String::as_str).collect();
            for (child, (parent, alias)) in &view.subquery_cache {
                if parents.contains(parent.as_ref()) {
                    children
                        .entry(child.to_string())
                        .or_insert_with(|| SubqueryDeltaItem {
                            id: child.to_string(),
                            parent_key: parent.to_string(),
                            alias: alias.clone(),
                            op: SubqueryOp::Add,
                        });
                }
            }
        }
        if additions.is_empty() && children.is_empty() {
            continue;
        }
        let targets = circuit
            .is_registered(&owner)
            .then_some((owner.clone(), view.auth_id.clone()))
            .into_iter()
            .chain(
                circuit
                    .subscribers_of(&owner)
                    .iter()
                    .map(|subscriber| (subscriber.query_id.clone(), subscriber.auth_id.clone())),
            );
        for (query_id, auth_id) in targets {
            // A retried batch does not repeat the source DB deletion. Remove
            // previous edges in every affected publication table before the
            // additions, including public views held by another owner.
            let table = crate::tables::list_ref_table(mode, &auth_id);
            for key in &additions {
                if drops.insert((table.clone(), key.clone())) {
                    cleanup.push(crate::edges::PublicationCleanup::DropEdgesTo {
                        table: table.clone(),
                        record: key.clone(),
                    });
                }
            }
            let index = *positions.entry(query_id.clone()).or_insert_with(|| {
                deltas.push(ViewDelta {
                    query_id,
                    additions: vec![],
                    removals: vec![],
                    updates: vec![],
                    row_count: view.cache.len(),
                    result_hash: view.last_hash.clone(),
                    subquery_items: vec![],
                    auth_id,
                    initial: false,
                });
                deltas.len() - 1
            });
            let delta = &mut deltas[index];
            for key in &additions {
                delta.updates.retain(|id| id != key);
                delta.removals.retain(|id| id != key);
                if !delta.additions.contains(key) {
                    delta.additions.push(key.clone());
                }
            }
            for child in children.values() {
                delta.subquery_items.retain(|item| item.id != child.id);
                delta.subquery_items.push(SubqueryDeltaItem {
                    op: SubqueryOp::Remove,
                    ..child.clone()
                });
                delta.subquery_items.push(child.clone());
            }
        }
    }
}

impl SspNode {
    /// Bounded ordinary-row delivery. Lifecycle and job events use `/ingest`.
    pub(super) async fn ingest_batch_handler(&self, req: &ApiRequest) -> Option<ApiResponse> {
        let started = web_time::Instant::now();
        if let Some(gate) = self.ready_gate().await {
            return Some(gate);
        }
        if req.body.len() > MAX_INGEST_BATCH_BYTES {
            return Some(err_json(
                413,
                "ingest_batch_size",
                "Ingest batch exceeds the byte limit",
            ));
        }
        let Ok(payload) = serde_json::from_slice::<IngestBatchRequest>(&req.body) else {
            return Some(err_json(400, "ingest_batch", "Invalid ingest batch"));
        };
        if payload.records.is_empty() || payload.records.len() > MAX_INGEST_BATCH_RECORDS {
            return Some(err_json(
                400,
                "ingest_batch_size",
                "Ingest batch must contain 1 to 128 records",
            ));
        }
        // Validate everything before acquiring capacity or causing any side effect.
        let mut changes = Vec::with_capacity(payload.records.len());
        let mut sources = std::collections::HashMap::new();
        let mut deleted = std::collections::HashSet::new();
        let mut cleanup = Vec::new();
        for row in &payload.records {
            if row.table.is_empty()
                || row.id.is_empty()
                || row.table == "user"
                || row.table.starts_with("_00_")
                || self.job_config.job_tables.contains_key(&row.table)
            {
                return Some(err_json(
                    400,
                    "ingest_batch_table",
                    "Lifecycle and job records require /ingest",
                ));
            }
            let Some(op) = Operation::from_str(&row.op) else {
                return Some(err_json(400, "ingest_batch_op", "Invalid ingest operation"));
            };
            let clean = ssp::sanitizer::normalize_record_ref(&row.record);
            changes.push(match op {
                Operation::Create => Change::create(&row.table, &row.id, clean),
                Operation::Update => Change::update(&row.table, &row.id, clean),
                Operation::Merge => Change::merge(&row.table, &row.id, clean),
                Operation::Delete => Change::delete(&row.table, &row.id),
            });
            let key = ssp::types::make_key(&row.table, &row.id).to_string();
            if op == Operation::Delete {
                sources.remove(&key);
                deleted.insert(key.clone());
                cleanup.extend(delete_cleanup(
                    self.ref_mode,
                    self.anonymous_live_queries,
                    &key,
                    &row.record,
                ));
            } else if let Some(version) = row
                .record
                .get("_00_rv")
                .and_then(Value::as_i64)
                .filter(|v| *v > 0)
            {
                sources
                    .entry(key)
                    .and_modify(|current: &mut i64| *current = (*current).max(version))
                    .or_insert(version);
            }
        }
        let Some(permit) = self.publication_admission(req.body.len()) else {
            return Some(err_json(
                503,
                "publication_backlog",
                "Publication backlog is full; retry this request",
            ));
        };
        let mut stages = crate::view_metrics::IngestStages::default();
        let waiting = web_time::Instant::now();
        let (counts, ids, synthesized, shapes, standby) = {
            let mut circuit = self.processor.write().await;
            stages.lock_wait_ms = waiting.elapsed().as_secs_f64() * 1000.0;
            let hold = web_time::Instant::now();
            if !self.edge_update_tx.is_current(&permit)
                || *self.status.read().await != SspStatus::Ready
            {
                return Some(err_json(
                    503,
                    "publication_epoch",
                    "Circuit restarted; retry ingest",
                ));
            }
            let standby = self.is_standby();
            let before = circuit.synthesized_row_versions();
            let (mut deltas, timing) = circuit.step_timed(ChangeSet { changes });
            restore_recreated_edges(&circuit, &mut deltas, &deleted, self.ref_mode, &mut cleanup);
            stages.store_apply_ms = timing.store_apply_ms;
            stages.circuit_step_ms = timing.circuit_step_ms;
            let counts = deltas.iter().map(|d| d.row_count).collect::<Vec<_>>();
            let ids = deltas
                .iter()
                .map(|d| d.query_id.clone())
                .collect::<Vec<_>>();
            let enqueue = web_time::Instant::now();
            self.edge_update_tx.enqueue_sources(
                permit,
                deltas,
                &circuit,
                sources.into_iter().collect(),
                false,
                cleanup,
            );
            stages.enqueue_ms = enqueue.elapsed().as_secs_f64() * 1000.0;
            stages.lock_hold_ms = hold.elapsed().as_secs_f64() * 1000.0;
            let shapes: Vec<Value> = if started.elapsed().as_secs_f64() * 1000.0
                >= crate::view_metrics::SLOW_REGISTRATION_MS
            {
                ids.iter()
                    .take(8)
                    .filter_map(|id| circuit.get_view(id))
                    .map(|view| {
                        serde_json::to_value(ssp::allowlist::Shape::of(&view.plan.root))
                            .unwrap_or(Value::Null)
                    })
                    .filter(|shape| shape.to_string().len() <= 4096)
                    .collect()
            } else {
                Vec::new()
            };
            (
                counts,
                ids,
                circuit.synthesized_row_versions() - before,
                shapes,
                standby,
            )
        };
        let materialization_ms = waiting.elapsed().as_secs_f64() * 1000.0;
        if !standby {
            for row in &payload.records {
                self.observe_push(&row.table, &row.op, &row.id, &row.record);
            }
        }
        self.platform
            .telemetry
            .counter("ingest", payload.records.len() as u64);
        self.platform.telemetry.counter("ingest_batch", 1);
        self.platform
            .telemetry
            .counter("ingest_rv_synthesized", synthesized);
        if !ids.is_empty() {
            note_view_metrics(&self.view_metrics, counts, ids.clone(), materialization_ms).await;
            stages.request_ms = started.elapsed().as_secs_f64() * 1000.0;
            let mut metrics = self.view_metrics.write().await;
            for id in &ids {
                if let Some(state) = metrics.get_mut(id) {
                    state.ingest = Some(stages.clone());
                }
            }
        }
        stages.request_ms = started.elapsed().as_secs_f64() * 1000.0;
        self.platform
            .telemetry
            .histogram_ms("ingest_duration", stages.request_ms);
        if !standby && stages.request_ms >= crate::view_metrics::SLOW_REGISTRATION_MS {
            let evidence = json!({"at_ms": crate::now_epoch_ms(), "kind": "ingest_batch", "version": self.version,
                "records": payload.records.len(), "affected_views": ids.len(), "shapes": shapes, "stages": stages});
            crate::view_metrics::spawn_slow_evidence(&self.platform, evidence);
        }
        Some(ApiResponse::json(200, Value::Null))
    }
}
