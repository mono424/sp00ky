//! Optimistic writes, debounced flushes and channel peer writes.
//!
//! PORT: `packages/spooky_core/lib/src/mutation/write_saga.dart`.

use crate::error::{Error, Result};
use crate::kernel::events::{LogLevel, MutationEvent, OutEvent, RuntimeEvent};
use crate::kernel::ports::{IdScope, IngestOp, IngestRecord};
use crate::kernel::Ctx;
use crate::mutation::rows::{
    log_data, num_i64, plan_create_tx, plan_deferred_outbox_row_tx, plan_delete_tx,
    plan_local_only_update_tx, plan_peer_create_tx, plan_peer_undo_tx, plan_update_tx,
    WritePlanInput,
};
use crate::query::env::SagaEnv;
use crate::state::reducers as r;
use crate::state::{OutboxItem, OutboxStatus, PeerWrite, PeerWriteKind, PendingWrite};
use crate::types::{DebounceKey, MutationEventType, UpdateOptions};
use crate::utils::parse_query_params;
use crate::value::{table_of, Object, Row, Value};

/// One optimistic write, as the facade asks for it.
#[derive(Clone, Debug, PartialEq)]
pub struct WriteInput {
    pub kind: MutationEventType,
    pub record_id: String,
    pub data: Option<Object>,
    pub options: Option<UpdateOptions>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct WriteResult {
    /// The outbox row's id; empty for a debounced update (its row is written
    /// on flush).
    pub mutation_id: String,
    /// The row as the local store holds it after the write; `None` for a
    /// delete.
    pub record: Option<Row>,
}

const DEFAULT_DEBOUNCE_MS: u64 = 300;

fn debounce_key(input: &WriteInput) -> Option<String> {
    let d = input.options.as_ref()?.debounced?;
    if d.key == DebounceKey::RecordId {
        return Some(input.record_id.clone());
    }
    // Object keys are already sorted.
    let fields: Vec<&str> = input
        .data
        .iter()
        .flat_map(|d| d.keys())
        .map(String::as_str)
        .collect();
    Some(format!("{}::{}", input.record_id, fields.join(",")))
}

fn debounce_delay(input: &WriteInput) -> u64 {
    input
        .options
        .as_ref()
        .and_then(|o| o.debounced)
        .and_then(|d| d.delay_ms)
        .unwrap_or(DEFAULT_DEBOUNCE_MS)
}

fn error_data(record_id: &str, error: &Error) -> Option<Value> {
    log_data(vec![
        ("recordId", record_id.into()),
        ("error", error.to_string().into()),
    ])
}

/// One optimistic write: read the previous row (update/delete), run the local
/// transaction that writes the row AND its outbox entry, publish the outbox
/// item (which puts the id into the overlay and dirties the table's queries),
/// feed the circuit, and start a drain. A debounced update applies locally at
/// once but accumulates into one outbox row written on flush.
pub async fn write(ctx: &Ctx, env: &SagaEnv, input: WriteInput) -> Result<WriteResult> {
    let table = table_of(&input.record_id).to_string();
    let columns = env
        .columns(&table)
        .ok_or_else(|| Error::InvalidArgument(format!("Table {table} not found")))?;
    let params = input
        .data
        .as_ref()
        .map(|d| parse_query_params(columns, d))
        .transpose()?;
    if input.kind == MutationEventType::Update {
        if let Some(key) = debounce_key(&input) {
            return debounced_update(ctx, &input, key, &table, params.unwrap_or_default()).await;
        }
    }
    let mutation_id = ctx.id(IdScope::Mutation)?;
    let now = ctx.now()?;
    let before = if input.kind == MutationEventType::Create {
        None
    } else {
        ctx.local_get(&table, &input.record_id)?
    };
    let plan = WritePlanInput {
        record_id: input.record_id.clone(),
        mutation_id: mutation_id.clone(),
        table: table.clone(),
        data: params.clone(),
        before: before.clone(),
        now,
    };
    let ops = match input.kind {
        MutationEventType::Create => plan_create_tx(&plan),
        MutationEventType::Update => plan_update_tx(&plan),
        MutationEventType::Delete => plan_delete_tx(&plan),
    };
    ctx.local_tx(ops, None)?;

    // The local store has no RETURN clause, so the written row is read back
    // rather than carried out of the transaction.
    let record = if input.kind == MutationEventType::Delete {
        None
    } else {
        ctx.local_get(&table, &input.record_id)?
    };

    ctx.update(|s| {
        r::outbox_push(
            s,
            OutboxItem {
                id: mutation_id.clone(),
                kind: input.kind,
                record_id: input.record_id.clone(),
                table: table.clone(),
                status: OutboxStatus::Pending,
                acked_at: None,
                attempts: 0,
                in_doubt: false,
            },
        )
    })?;

    let version = match input.kind {
        MutationEventType::Create => 1,
        _ => num_i64(record.as_ref().and_then(|r| r.get("_00_rv"))).unwrap_or_else(|| {
            num_i64(before.as_ref().and_then(|b| b.get("_00_rv"))).unwrap_or(0) + 1
        }),
    };
    let circuit_record = if input.kind == MutationEventType::Delete {
        before.clone().unwrap_or_default()
    } else {
        let mut r = record.clone().unwrap_or_default();
        r.insert("_00_rv".into(), Value::Int(version));
        r
    };
    let op = match input.kind {
        MutationEventType::Create => IngestOp::Create,
        MutationEventType::Update => IngestOp::Update,
        MutationEventType::Delete => IngestOp::Delete,
    };
    let ingested = ctx
        .ssp_ingest(vec![IngestRecord::new(
            table.clone(),
            op,
            input.record_id.clone(),
            circuit_record,
        )])
        .and_then(|()| {
            ctx.update(|s| {
                if input.kind == MutationEventType::Delete {
                    r::delete_versions(s, [&input.record_id]);
                } else {
                    r::set_versions(s, &[(input.record_id.clone(), version)]);
                }
            })
        });
    if let Err(error) = ingested {
        ctx.log(
            LogLevel::Error,
            "circuit ingest failed after a local write",
            error_data(&input.record_id, &error),
        )?;
    }
    ctx.emit(OutEvent::MutationEmitted(MutationEvent {
        kind: input.kind,
        mutation_id: mutation_id.clone(),
        record_id: input.record_id.clone(),
        data: params,
        record: record.clone(),
        created_at: now,
    }))?;
    ctx.dispatch(RuntimeEvent::Drain)?;
    Ok(WriteResult {
        mutation_id,
        record,
    })
}

/// Apply locally now, remember the merged patch, write the outbox row on flush.
async fn debounced_update(
    ctx: &Ctx,
    input: &WriteInput,
    key: String,
    table: &str,
    params: Object,
) -> Result<WriteResult> {
    let existing = ctx.read(|s| {
        s.pending_writes
            .get(&key)
            .map(|w| (w.before.clone(), w.first_at))
    })?;
    let now = ctx.now()?;
    // A merged write never re-reads the before row: the first one wins.
    let (before, first_at) = match existing {
        Some((before, first_at)) => (before, first_at),
        None => (ctx.local_get(table, &input.record_id)?, now),
    };
    ctx.local_tx(
        plan_local_only_update_tx(table, &input.record_id, params.clone()),
        None,
    )?;
    let record = ctx.local_get(table, &input.record_id)?;
    ctx.update(|s| {
        r::merge_pending_write(
            s,
            PendingWrite {
                key: key.clone(),
                table: table.to_string(),
                record_id: input.record_id.clone(),
                data: params,
                before,
                first_at,
            },
        )
    })?;
    ctx.update(|s| r::mark_table_dirty(s, table))?;
    let version = num_i64(record.as_ref().and_then(|r| r.get("_00_rv"))).unwrap_or(0);
    let mut circuit_record = record.clone().unwrap_or_default();
    circuit_record.insert("_00_rv".into(), Value::Int(version));
    let ingested = ctx
        .ssp_ingest(vec![IngestRecord::new(
            table,
            IngestOp::Update,
            input.record_id.clone(),
            circuit_record,
        )])
        .and_then(|()| ctx.update(|s| r::set_versions(s, &[(input.record_id.clone(), version)])));
    if let Err(error) = ingested {
        ctx.log(
            LogLevel::Error,
            "circuit ingest failed after a local write",
            error_data(&input.record_id, &error),
        )?;
    }
    ctx.timer_set(
        format!("debounce:{key}"),
        debounce_delay(input),
        RuntimeEvent::FlushWrite { key },
    )?;
    Ok(WriteResult {
        mutation_id: String::new(),
        record,
    })
}

/// Timer target: turn the accumulated patch into one outbox row and drain.
pub async fn flush_write(ctx: &Ctx, _env: &SagaEnv, key: &str) -> Result<()> {
    let Some(pending) = ctx.read(|s| s.pending_writes.get(key).cloned())? else {
        return Ok(());
    };
    ctx.update(|s| r::clear_pending_write(s, key))?;
    let mutation_id = ctx.id(IdScope::Mutation)?;
    let now = ctx.now()?;
    ctx.local_tx(
        plan_deferred_outbox_row_tx(&WritePlanInput {
            record_id: pending.record_id.clone(),
            mutation_id: mutation_id.clone(),
            table: pending.table.clone(),
            data: Some(pending.data.clone()),
            before: pending.before.clone(),
            now,
        }),
        None,
    )?;
    ctx.update(|s| {
        r::outbox_push(
            s,
            OutboxItem {
                id: mutation_id.clone(),
                kind: MutationEventType::Update,
                record_id: pending.record_id.clone(),
                table: pending.table.clone(),
                status: OutboxStatus::Pending,
                acked_at: None,
                attempts: 0,
                in_doubt: false,
            },
        )
    })?;
    ctx.emit(OutEvent::MutationEmitted(MutationEvent {
        kind: MutationEventType::Update,
        mutation_id,
        record_id: pending.record_id,
        data: Some(pending.data),
        record: None,
        created_at: now,
    }))?;
    ctx.dispatch(RuntimeEvent::Drain)?;
    Ok(())
}

// ---- channel peers' creates and deletes -------------------------------------

/// The row as the local store holds it, or `None`: what a channel checks its
/// filter against.
pub async fn read_row(ctx: &Ctx, id: &str) -> Result<Option<Row>> {
    ctx.local_get(table_of(id), id)
}

/// A channel peer's create or delete.
#[derive(Clone, Debug, PartialEq)]
pub struct PeerWriteInput {
    pub record_id: String,
    /// The row's fields, for a create.
    pub data: Option<Object>,
    /// How long it shows without the server confirming it.
    pub ttl_ms: u64,
}

/// A channel peer created a row: put it in the store and the circuit like an
/// own optimistic create, minus the outbox entry (the peer pushes it), and show
/// it through the overlay. `_00_rv` and `ClientState::versions` stay unset on
/// purpose: the server's body (with its computed fields) still downloads and
/// replaces this one. Refuses an id this client already holds a confirmed row
/// or an own pending write for, so a peer can never overwrite real data.
/// Returns whether it applied.
pub async fn peer_create(ctx: &Ctx, env: &SagaEnv, input: PeerWriteInput) -> Result<bool> {
    let table = table_of(&input.record_id).to_string();
    let Some(columns) = env.columns(&table) else {
        return Ok(false);
    };
    let id = input.record_id.clone();
    let (confirmed, own_pending, peer_pending) = ctx.read(|s| {
        (
            s.versions.contains_key(&id),
            s.outbox.iter().any(|i| i.record_id == id),
            s.peer_writes.contains_key(&id),
        )
    })?;
    if confirmed || own_pending || peer_pending {
        return Ok(false);
    }
    let mut record = ctx.local_get(&table, &id)?;
    if record.is_none() {
        let params = parse_query_params(columns, &input.data.clone().unwrap_or_default())?;
        let created = match ctx.local_tx(plan_peer_create_tx(&table, &id, params), None) {
            Ok(()) => ctx.local_get(&table, &id),
            Err(e) => Err(e),
        };
        match created {
            Ok(r) => record = r,
            Err(error) => {
                ctx.log(
                    LogLevel::Debug,
                    "peer create refused by the local store",
                    error_data(&id, &error),
                )?;
                return Ok(false);
            }
        }
    }
    let Some(mut record) = record else {
        return Ok(false);
    };
    record.insert("_00_rv".into(), Value::Int(0));
    if let Err(error) = ctx.ssp_ingest(vec![IngestRecord::new(
        table.clone(),
        IngestOp::Create,
        id.clone(),
        record,
    )]) {
        ctx.log(
            LogLevel::Error,
            "circuit ingest failed after a peer create",
            error_data(&id, &error),
        )?;
    }
    let now = ctx.now()?;
    ctx.update(|s| {
        r::set_peer_write(
            s,
            &id,
            PeerWrite {
                kind: PeerWriteKind::Create,
                table: table.clone(),
                expires_at: now + input.ttl_ms as i64,
            },
        )
    })?;
    ctx.timer_set(
        format!("peer:{id}"),
        input.ttl_ms,
        RuntimeEvent::PeerWriteExpired { id: id.clone() },
    )?;
    Ok(true)
}

/// A channel peer deleted a row: hide it through the overlay, keep it in the
/// store. Undoing is then just forgetting the entry; a confirmed delete has
/// taken the row out of the server's membership by the time it expires.
pub async fn peer_delete(ctx: &Ctx, env: &SagaEnv, input: PeerWriteInput) -> Result<bool> {
    let table = table_of(&input.record_id).to_string();
    if env.columns(&table).is_none() {
        return Ok(false);
    }
    let id = input.record_id.clone();
    let (known, confirmed) = ctx.read(|s| {
        (
            s.peer_writes.get(&id).map(|w| w.kind),
            s.versions.contains_key(&id),
        )
    })?;
    if known == Some(PeerWriteKind::Create) && !confirmed {
        // Deleting a row the same peer created and nobody confirmed: just undo it.
        ctx.timer_clear(&format!("peer:{id}"))?;
        undo_peer_create(ctx, &table, &id).await?;
        return Ok(true);
    }
    if ctx.local_get(&table, &id)?.is_none() {
        return Ok(false);
    }
    let now = ctx.now()?;
    ctx.update(|s| {
        r::set_peer_write(
            s,
            &id,
            PeerWrite {
                kind: PeerWriteKind::Delete,
                table: table.clone(),
                expires_at: now + input.ttl_ms as i64,
            },
        )
    })?;
    ctx.timer_set(
        format!("peer:{id}"),
        input.ttl_ms,
        RuntimeEvent::PeerWriteExpired { id: id.clone() },
    )?;
    Ok(true)
}

/// Timer target: a peer's write stops showing as unconfirmed.
pub async fn peer_write_expired(ctx: &Ctx, id: &str) -> Result<()> {
    let (write, confirmed, own_pending) = ctx.read(|s| {
        (
            s.peer_writes.get(id).cloned(),
            s.versions.contains_key(id),
            s.outbox.iter().any(|i| i.record_id == id),
        )
    })?;
    let Some(write) = write else {
        return Ok(());
    };
    if write.kind == PeerWriteKind::Create && !confirmed && !own_pending {
        return undo_peer_create(ctx, &write.table, id).await;
    }
    ctx.update(|s| r::clear_peer_write(s, id))?;
    Ok(())
}

async fn undo_peer_create(ctx: &Ctx, table: &str, id: &str) -> Result<()> {
    let before = ctx.local_get(table, id)?;
    let undone = ctx
        .local_tx(plan_peer_undo_tx(table, id), None)
        .and_then(|()| match before {
            Some(before) => {
                ctx.ssp_ingest(vec![IngestRecord::new(table, IngestOp::Delete, id, before)])
            }
            None => Ok(()),
        });
    if let Err(error) = undone {
        ctx.log(
            LogLevel::Warn,
            "could not undo an unconfirmed peer create",
            error_data(id, &error),
        )?;
    }
    ctx.update(|s| r::clear_peer_write(s, id))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kernel::events::LogLevel;
    use crate::kernel::ports::LocalOp;
    use crate::mutation::mutation_id::PENDING_TABLE;
    use crate::mutation::test_support::{new_rig, record_events, state_with, test_env};
    use crate::state::selectors::overlay;
    use crate::state::ClientState;
    use crate::testing::{
        build_definition, build_entry, build_outbox_item, results, row, run_local, TestRig,
    };
    use crate::types::DebounceOptions;
    use serde_json::json;

    fn create(id: &str, data: serde_json::Value) -> WriteInput {
        WriteInput {
            kind: MutationEventType::Create,
            record_id: id.into(),
            data: Some(row(data)),
            options: None,
        }
    }

    fn update(id: &str, data: serde_json::Value, options: Option<UpdateOptions>) -> WriteInput {
        WriteInput {
            kind: MutationEventType::Update,
            record_id: id.into(),
            data: Some(row(data)),
            options,
        }
    }

    async fn run_write(rig: &TestRig, input: WriteInput) -> Result<WriteResult> {
        rig.run(|ctx| async move { write(&ctx, &test_env(), input).await })
            .await
    }

    /// A drain the write dispatched shows up as a push once it runs.
    async fn drained(rig: &TestRig) -> bool {
        rig.remote
            .answer_with(|_, _| Ok(results(vec![Ok(json!(null))])));
        rig.settle().await;
        rig.log
            .named("remote.write")
            .iter()
            .any(|c| c.args.contains("LET $ok0"))
    }

    #[tokio::test]
    async fn rejects_a_table_the_schema_does_not_know() {
        run_local(async {
            let (rig, _) = new_rig(test_env(), ClientState::empty("tab"));
            let out = run_write(&rig, create("ghost:1", json!({}))).await;
            assert!(matches!(out, Err(Error::InvalidArgument(_))));
        })
        .await;
    }

    #[tokio::test]
    async fn create_runs_the_local_tx_queues_ingests_at_version_1_and_drains() {
        run_local(async {
            let (rig, _) = new_rig(
                test_env(),
                state_with(vec![build_entry(build_definition("a", "thing"))]),
            );
            let events = record_events(&rig);
            let out = run_write(
                &rig,
                create("thing:1", json!({"title": "hello", "owner": "user:u1"})),
            )
            .await
            .unwrap();
            assert_eq!(rig.log.count("local.tx"), 1);
            let stored = rig.local.row("thing", "thing:1").unwrap();
            assert_eq!(stored["_00_rv"], Value::Int(1));
            // The schema re-types the record link before it is stored.
            assert!(matches!(stored["owner"], Value::RecordId(_)));
            let outbox = rig.local.row(PENDING_TABLE, &out.mutation_id).unwrap();
            assert_eq!(outbox["mutationType"], Value::from("create"));

            let st = rig.state();
            assert_eq!(st.outbox.len(), 1);
            assert_eq!(st.outbox[0].id, out.mutation_id);
            assert_eq!(st.outbox[0].status, OutboxStatus::Pending);
            assert_eq!(st.versions.get("thing:1"), Some(&1));
            // The overlay changed, so the table re-renders.
            assert!(st.dirty.contains("a"));
            assert_eq!(events.count("mutation:event"), 1);
            assert_eq!(rig.ssp.ingested.borrow()[0].op, IngestOp::Create);
            assert!(drained(&rig).await);
        })
        .await;
    }

    #[tokio::test]
    async fn update_reads_the_previous_row_and_takes_the_version_off_the_new_one() {
        run_local(async {
            let (rig, _) = new_rig(test_env(), ClientState::empty("tab"));
            rig.local.seed(
                "thing",
                "thing:1",
                row(json!({"id": "thing:1", "title": "old", "_00_rv": 6})),
            );
            run_write(&rig, update("thing:1", json!({"title": "renamed"}), None))
                .await
                .unwrap();
            let tx = &rig.log.named("local.tx")[0].args;
            assert!(tx.starts_with("[BumpRv"), "{tx}");
            assert!(tx.contains("mode: Merge"), "{tx}");
            let item = rig.state().outbox[0].id.clone();
            let outbox = rig.local.row(PENDING_TABLE, &item).unwrap();
            assert_eq!(
                outbox["beforeRecord"].get("title"),
                Some(&Value::from("old"))
            );
            assert_eq!(rig.state().versions.get("thing:1"), Some(&7));
            assert_eq!(
                rig.local.row("thing", "thing:1").unwrap()["title"],
                Value::from("renamed")
            );
        })
        .await;
    }

    #[tokio::test]
    async fn delete_feeds_the_circuit_with_the_row_as_it_was() {
        run_local(async {
            let (rig, _) = new_rig(test_env(), ClientState::empty("tab"));
            rig.local.seed(
                "thing",
                "thing:1",
                row(json!({"id": "thing:1", "title": "gone", "_00_rv": 3})),
            );
            let out = run_write(
                &rig,
                WriteInput {
                    kind: MutationEventType::Delete,
                    record_id: "thing:1".into(),
                    data: None,
                    options: None,
                },
            )
            .await
            .unwrap();
            assert_eq!(out.record, None);
            assert!(!rig.state().versions.contains_key("thing:1"));
            assert!(rig.log.named("local.tx")[0].args.starts_with("[Delete"));
            let ingested = rig.ssp.ingested.borrow();
            assert_eq!(ingested[0].op, IngestOp::Delete);
            assert_eq!(ingested[0].record["title"], Value::from("gone"));
        })
        .await;
    }

    #[tokio::test]
    async fn a_failing_circuit_ingest_is_logged_and_the_local_write_stands() {
        run_local(async {
            let (rig, faults) = new_rig(test_env(), ClientState::empty("tab"));
            faults.fail_ssp_ingest();
            let events = record_events(&rig);
            run_write(&rig, create("thing:1", json!({"title": "x"})))
                .await
                .unwrap();
            assert_eq!(rig.state().outbox.len(), 1);
            let logs = events.logs();
            assert_eq!(logs.len(), 1);
            assert_eq!(logs[0].0, LogLevel::Error);
        })
        .await;
    }

    #[tokio::test]
    async fn debounced_applies_locally_merges_per_key_and_arms_the_flush_timer() {
        run_local(async {
            let (rig, _) = new_rig(test_env(), ClientState::empty("tab"));
            rig.local.seed(
                "thing",
                "thing:1",
                row(json!({"id": "thing:1", "_00_rv": 1})),
            );
            let first = run_write(
                &rig,
                update(
                    "thing:1",
                    json!({"title": "a"}),
                    Some(UpdateOptions::debounced()),
                ),
            )
            .await
            .unwrap();
            assert_eq!(first.mutation_id, "");
            // The outbox row is written on flush, not now.
            assert!(rig.state().outbox.is_empty());
            let key = rig.state().pending_writes.keys().next().cloned().unwrap();
            assert_eq!(key, "thing:1::title");
            assert_eq!(rig.timers.delay(&format!("debounce:{key}")), Some(300));
            assert_eq!(rig.state().versions.get("thing:1"), Some(&2));

            let options = UpdateOptions {
                debounced: Some(DebounceOptions {
                    key: DebounceKey::RecordId,
                    delay_ms: Some(50),
                }),
            };
            run_write(
                &rig,
                update("thing:1", json!({"title": "b"}), Some(options)),
            )
            .await
            .unwrap();
            assert_eq!(rig.timers.delay("debounce:thing:1"), Some(50));
        })
        .await;
    }

    #[tokio::test]
    async fn the_first_before_record_survives_the_merge() {
        run_local(async {
            let (rig, _) = new_rig(test_env(), ClientState::empty("tab"));
            rig.local.seed(
                "thing",
                "thing:1",
                row(json!({"id": "thing:1", "title": "original"})),
            );
            let options = Some(UpdateOptions {
                debounced: Some(DebounceOptions {
                    key: DebounceKey::RecordId,
                    delay_ms: None,
                }),
            });
            run_write(&rig, update("thing:1", json!({"title": "a"}), options))
                .await
                .unwrap();
            rig.log.clear();
            run_write(&rig, update("thing:1", json!({"title": "b"}), options))
                .await
                .unwrap();
            let st = rig.state();
            let pending = &st.pending_writes["thing:1"];
            assert_eq!(
                pending.before.as_ref().unwrap()["title"],
                Value::from("original")
            );
            assert_eq!(pending.data["title"], Value::from("b"));
            // The only read is the post-write read-back: a merged write never
            // re-reads the before row, which is what keeps the original.
            let names = rig.log.names();
            assert_eq!(rig.log.count("local.get"), 1);
            let tx = names.iter().position(|n| n == "local.tx").unwrap();
            let get = names.iter().position(|n| n == "local.get").unwrap();
            assert!(tx < get);
        })
        .await;
    }

    #[tokio::test]
    async fn flush_writes_exactly_one_outbox_row_and_drains() {
        run_local(async {
            let mut state = ClientState::empty("tab");
            r::merge_pending_write(
                &mut state,
                PendingWrite {
                    key: "k".into(),
                    table: "thing".into(),
                    record_id: "thing:1".into(),
                    data: row(json!({"title": "final"})),
                    before: Some(row(json!({"title": "original"}))),
                    first_at: 1,
                },
            );
            let (rig, _) = new_rig(test_env(), state);
            rig.run(|ctx| async move { flush_write(&ctx, &test_env(), "k").await })
                .await
                .unwrap();
            let st = rig.state();
            assert!(st.pending_writes.is_empty());
            let txs = rig.log.named("local.tx");
            assert_eq!(txs.len(), 1);
            let doc = rig.local.row(PENDING_TABLE, &st.outbox[0].id).unwrap();
            assert_eq!(doc["data"], Value::Object(row(json!({"title": "final"}))));
            assert_eq!(
                doc["beforeRecord"],
                Value::Object(row(json!({"title": "original"})))
            );
            assert_eq!(st.outbox.len(), 1);
            assert_eq!(st.outbox[0].kind, MutationEventType::Update);
            assert!(drained(&rig).await);
        })
        .await;
    }

    #[tokio::test]
    async fn flushing_an_unknown_key_is_a_no_op() {
        run_local(async {
            let (rig, _) = new_rig(test_env(), ClientState::empty("tab"));
            rig.run(|ctx| async move { flush_write(&ctx, &test_env(), "nope").await })
                .await
                .unwrap();
            assert_eq!(rig.log.count("local.tx"), 0);
        })
        .await;
    }

    // ---- peers ----------------------------------------------------------------

    fn peer(id: &str, data: Option<serde_json::Value>) -> PeerWriteInput {
        PeerWriteInput {
            record_id: id.into(),
            data: data.map(row),
            ttl_ms: 5000,
        }
    }

    async fn run_peer_create(rig: &TestRig, input: PeerWriteInput) -> bool {
        rig.run(|ctx| async move { peer_create(&ctx, &test_env(), input).await })
            .await
            .unwrap()
    }

    async fn run_peer_delete(rig: &TestRig, input: PeerWriteInput) -> bool {
        rig.run(|ctx| async move { peer_delete(&ctx, &test_env(), input).await })
            .await
            .unwrap()
    }

    async fn run_expired(rig: &TestRig, id: &'static str) {
        rig.run(|ctx| async move { peer_write_expired(&ctx, id).await })
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn peer_create_shows_the_row_without_rv_or_an_outbox_entry() {
        run_local(async {
            let (rig, _) = new_rig(
                test_env(),
                state_with(vec![build_entry(build_definition("a", "thing"))]),
            );
            assert!(
                run_peer_create(
                    &rig,
                    peer("thing:1", Some(json!({"title": "hi", "owner": "user:u1"})))
                )
                .await
            );
            let txs = rig.log.named("local.tx");
            // The peer pushes it, not us.
            assert_eq!(txs.len(), 1);
            let stored = rig.local.row("thing", "thing:1").unwrap();
            // The server's body must still download.
            assert!(!stored.contains_key("_00_rv"));
            assert!(matches!(stored["owner"], Value::RecordId(_)));
            let ingested = rig.ssp.ingested.borrow().clone();
            assert_eq!(ingested.len(), 1);
            assert_eq!(ingested[0].op, IngestOp::Create);
            assert_eq!(ingested[0].record["_00_rv"], Value::Int(0));
            let st = rig.state();
            assert!(!st.versions.contains_key("thing:1"));
            assert!(st.outbox.is_empty());
            assert_eq!(st.peer_writes["thing:1"].kind, PeerWriteKind::Create);
            assert_eq!(
                st.peer_writes["thing:1"].expires_at,
                1_700_000_000_000 + 5000
            );
            assert!(overlay(&st).writes.contains("thing:1"));
            assert!(st.dirty.contains("a"));
            assert_eq!(rig.timers.delay("peer:thing:1"), Some(5000));
        })
        .await;
    }

    #[tokio::test]
    async fn peer_create_refuses_confirmed_own_pending_peer_pending_and_unknown() {
        run_local(async {
            let attempt = |state: ClientState, id: &'static str| async move {
                let (rig, _) = new_rig(test_env(), state);
                run_peer_create(&rig, peer(id, Some(json!({})))).await
            };
            let base = || ClientState::empty("tab");
            let mut confirmed = base();
            r::set_versions(&mut confirmed, &[("thing:1".into(), 3)]);
            assert!(!attempt(confirmed, "thing:1").await);
            let mut own = base();
            r::outbox_push(
                &mut own,
                build_outbox_item("m1", MutationEventType::Create, "thing:1"),
            );
            assert!(!attempt(own, "thing:1").await);
            let mut peer_pending = base();
            r::set_peer_write(
                &mut peer_pending,
                "thing:1",
                PeerWrite {
                    kind: PeerWriteKind::Delete,
                    table: "thing".into(),
                    expires_at: 0,
                },
            );
            assert!(!attempt(peer_pending, "thing:1").await);
            assert!(!attempt(base(), "ghost:1").await);
        })
        .await;
    }

    #[tokio::test]
    async fn peer_delete_hides_a_held_row_and_keeps_it_stored() {
        run_local(async {
            let (rig, _) = new_rig(test_env(), ClientState::empty("tab"));
            rig.local.seed(
                "thing",
                "thing:1",
                row(json!({"id": "thing:1", "title": "hi"})),
            );
            assert!(run_peer_delete(&rig, peer("thing:1", None)).await);
            assert_eq!(rig.log.count("local.tx"), 0);
            assert!(rig.local.row("thing", "thing:1").is_some());
            assert!(overlay(&rig.state()).deletes.contains("thing:1"));
            assert!(rig.timers.has("peer:thing:1"));

            assert!(!run_peer_delete(&rig, peer("thing:9", None)).await);
        })
        .await;
    }

    #[tokio::test]
    async fn peer_delete_of_an_unconfirmed_peer_create_just_undoes_it() {
        run_local(async {
            let (rig, _) = new_rig(test_env(), ClientState::empty("tab"));
            assert!(run_peer_create(&rig, peer("thing:1", Some(json!({"title": "x"})))).await);
            rig.ssp.ingested.borrow_mut().clear();
            assert!(run_peer_delete(&rig, peer("thing:1", None)).await);
            assert!(rig.local.row("thing", "thing:1").is_none());
            assert!(rig.state().peer_writes.is_empty());
            let ingested = rig.ssp.ingested.borrow();
            assert_eq!(ingested.len(), 1);
            assert_eq!(ingested[0].op, IngestOp::Delete);
            assert!(!rig.timers.has("peer:thing:1"));
        })
        .await;
    }

    #[tokio::test]
    async fn expiry_undoes_an_unconfirmed_create_keeps_a_confirmed_one_forgets_a_delete() {
        run_local(async {
            let (rig, _) = new_rig(test_env(), ClientState::empty("tab"));
            assert!(run_peer_create(&rig, peer("thing:1", Some(json!({"title": "x"})))).await);
            run_expired(&rig, "thing:1").await;
            assert!(rig.local.row("thing", "thing:1").is_none());
            assert!(rig.state().peer_writes.is_empty());

            // The server's body landed.
            assert!(run_peer_create(&rig, peer("thing:2", Some(json!({"title": "x"})))).await);
            rig.runtime
                .update(|s| r::set_versions(s, &[("thing:2".into(), 1)]));
            rig.log.clear();
            run_expired(&rig, "thing:2").await;
            assert!(rig.local.row("thing", "thing:2").is_some());
            assert!(rig.state().peer_writes.is_empty());
            assert_eq!(rig.log.count("local.tx"), 0);

            rig.runtime.update(|s| {
                r::set_peer_write(
                    s,
                    "thing:3",
                    PeerWrite {
                        kind: PeerWriteKind::Delete,
                        table: "thing".into(),
                        expires_at: 0,
                    },
                )
            });
            run_expired(&rig, "thing:3").await;
            assert!(rig.state().peer_writes.is_empty());
            assert_eq!(rig.log.count("local.tx"), 0);

            run_expired(&rig, "thing:4").await;
            assert!(rig.log.calls().is_empty(), "an unknown id touches nothing");
        })
        .await;
    }

    #[test]
    fn a_bucket_switch_forgets_peer_writes() {
        let mut s = ClientState::empty("tab");
        r::set_peer_write(
            &mut s,
            "thing:1",
            PeerWrite {
                kind: PeerWriteKind::Create,
                table: "thing".into(),
                expires_at: 0,
            },
        );
        r::clear_bucket_state(&mut s);
        assert!(s.peer_writes.is_empty());
    }

    #[tokio::test]
    async fn read_row_reads_the_local_store() {
        run_local(async {
            let (rig, _) = new_rig(test_env(), ClientState::empty("tab"));
            rig.local
                .seed("thing", "thing:1", row(json!({"id": "thing:1"})));
            let out = rig
                .run(|ctx| async move { read_row(&ctx, "thing:1").await })
                .await
                .unwrap();
            assert_eq!(out, Some(row(json!({"id": "thing:1"}))));
        })
        .await;
    }

    #[test]
    fn the_peer_create_tx_has_no_outbox_entry() {
        let ops = plan_peer_create_tx("thing", "thing:1", row(json!({"title": "x"})));
        assert!(matches!(ops.as_slice(), [LocalOp::Put { table, .. }] if table == "thing"));
    }
}
