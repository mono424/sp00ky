//! The outbox: loading it from the store, draining it to the server, and
//! rolling back what the server rejects.
//!
//! PORT: `packages/spooky_core/lib/src/mutation/push_saga.dart`.

use std::collections::BTreeSet;

use crate::error::Result;
use crate::kernel::constants::{backoff_ms, ACK_GRACE_MS};
use crate::kernel::events::{LogLevel, OutEvent, RuntimeEvent};
use crate::kernel::ports::IngestOp;
use crate::kernel::Ctx;
use crate::mutation::mutation_id::PENDING_TABLE;
use crate::mutation::rows::{
    build_failed_row, delete_pending_row, hydrate_row_data, log_data, move_to_failed_tx, num_i64,
    parse_pending_row, plan_revert, remote_batch, sort_pending_rows, stored_id_string,
    to_outbox_item, FailedErrorKind, FailedMutationError, PendingMutationRow, BATCH_SKIPPED,
    FAILED_TABLE,
};
use crate::query::env::SagaEnv;
use crate::state::reducers as r;
use crate::state::{OutboxItem, OutboxStatus};
use crate::types::MutationEventType;
use crate::utils::{classify_error, classify_sync_error, ErrorKind};
use crate::value::{table_of, Object, RecordId, Value};

/// Boot or bucket switch: mirror the outbox table into state. Rows that can
/// never be replayed (a create without its payload) go straight to the tray.
pub async fn load_outbox(ctx: &Ctx, env: &SagaEnv) -> Result<()> {
    let raw = ctx.local_get_all(PENDING_TABLE)?;
    let mut replayable = Vec::new();
    let mut unreplayable = Vec::new();
    for row in raw {
        match parse_pending_row(&row) {
            Some(parsed) => replayable.push(parsed),
            None => unreplayable.push(row),
        }
    }
    // Drain order is id order, which is mint order.
    let items: Vec<OutboxItem> = sort_pending_rows(replayable)
        .iter()
        .map(to_outbox_item)
        .collect();
    let has_items = !items.is_empty();
    ctx.update(|s| r::outbox_replace(s, items))?;
    for row in unreplayable {
        let record_id = row
            .get("recordId")
            .map(Value::to_id_string)
            .unwrap_or_default();
        let table_name = row
            .get("tableName")
            .map(Value::to_id_string)
            .unwrap_or_else(|| table_of(&record_id).to_string());
        let pending = PendingMutationRow {
            id: row
                .get("id")
                .map(|id| stored_id_string(Some(id)))
                .unwrap_or_default(),
            mutation_type: MutationEventType::Create,
            record_id,
            table_name,
            data: None,
            before_record: None,
            created_at: 0,
            v: 1,
        };
        let error = FailedMutationError {
            message: "pending mutation is not replayable".into(),
            kind: FailedErrorKind::Unreplayable,
        };
        rollback(ctx, env, &pending, error).await?;
    }
    refresh_failed_count(ctx).await?;
    if has_items {
        ctx.dispatch(RuntimeEvent::Drain)?;
    }
    Ok(())
}

pub async fn refresh_failed_count(ctx: &Ctx) -> Result<()> {
    // The tray table may not exist yet on a fresh store; the count stays as is.
    if let Ok(rows) = ctx.local_get_all(FAILED_TABLE) {
        ctx.update(|s| r::set_failed_count(s, rows.len()))?;
    }
    Ok(())
}

fn sync_outcome(ok: bool, error: Option<String>) -> RuntimeEvent {
    RuntimeEvent::SyncOutcome { ok, error }
}

/// Drain the outbox: one serial lane, FIFO, up to `env.outbox_batch_size`
/// statements per request, per-statement outcome. Accepted rows are acked
/// (they stay in the overlay until membership names them), rejected rows are
/// rolled back, a transport failure leaves the unsent tail queued behind a
/// backoff timer. The server stops a batch at its first failed statement
/// ([`remote_batch`]), so the rows after a rejection are sent again at once,
/// and a re-sent row behind a retryable failure never ran before, so it cannot
/// read "already exists".
///
/// A request that throws a transport error (the push timeout, a dropped
/// socket) may still have committed on the server, and so may one sent by a
/// run that died before its answer. Those rows are marked in doubt
/// (`OutboxItem::in_doubt`, every row loaded from the store is), and their
/// re-sent CREATE accepts the row already on the server when it holds what
/// the CREATE would write, instead of failing "already exists" and rolling
/// back a write the server kept.
pub async fn drain(ctx: &Ctx, env: &SagaEnv) -> Result<()> {
    // A client with no endpoint has nothing to push to. Draining anyway would
    // fail every statement in a way the classifier reads as a rejection, and a
    // rejection rolls the write back: the local-first write would delete itself.
    if !env.has_remote {
        return Ok(());
    }
    loop {
        let batch: Vec<OutboxItem> = ctx.read(|s| {
            s.outbox
                .iter()
                .filter(|i| i.status == OutboxStatus::Pending)
                .take(env.outbox_batch_size.max(1))
                .cloned()
                .collect()
        })?;
        if batch.is_empty() {
            return Ok(());
        }

        let mut rows = Vec::with_capacity(batch.len());
        for item in &batch {
            let parsed = ctx.local_get(PENDING_TABLE, &item.id)?.and_then(|mut raw| {
                // Stores hand the id back on the document; fill it in for one
                // that does not.
                raw.entry("id".to_string())
                    .or_insert_with(|| item.id.clone().into());
                parse_pending_row(&raw)
            });
            match parsed {
                Some(row) => {
                    let columns = env.columns(&row.table_name);
                    rows.push(hydrate_row_data(row, columns));
                }
                None => {
                    // The row is already gone from the store.
                    ctx.update(|s| r::outbox_remove(s, &item.id))?;
                }
            }
        }
        if rows.is_empty() {
            continue;
        }
        let attempts_of = |id: &str| {
            batch
                .iter()
                .find(|i| i.id == id)
                .map(|i| i.attempts)
                .unwrap_or(0)
        };

        let in_doubt: BTreeSet<String> = batch
            .iter()
            .filter(|i| i.in_doubt)
            .map(|i| i.id.clone())
            .collect();
        let (sql, vars) = remote_batch(&rows, &in_doubt);
        let results = match ctx
            .remote_write(&sql, Some(vars), Some(env.push_timeout_ms))
            .await
        {
            Ok(results) => results,
            Err(error) => {
                if classify_error(&error) == ErrorKind::Application {
                    // The whole request was rejected before any statement ran:
                    // treat the head as rejected and keep the rest for the next
                    // round.
                    rollback(
                        ctx,
                        env,
                        &rows[0],
                        FailedMutationError::application(error.to_string()),
                    )
                    .await?;
                    ctx.dispatch(sync_outcome(false, Some(error.to_string())))?;
                    continue;
                }
                // No answer: the server may have committed any prefix of the batch.
                let head = rows[0].id.clone();
                let attempts = attempts_of(&head);
                ctx.update(|s| {
                    r::outbox_mark_in_doubt(s, rows.iter().map(|row| &row.id));
                    r::outbox_bump_attempts(s, &head);
                })?;
                ctx.dispatch(sync_outcome(false, Some(error.to_string())))?;
                ctx.timer_set("outbox", backoff_ms(attempts), RuntimeEvent::Drain)?;
                return Ok(());
            }
        };

        let now = ctx.now()?;
        let mut cut_at: Option<usize> = None;
        for (i, row) in rows.iter().enumerate() {
            let Some(res) = results.get(i) else {
                // Statements after a transport-level cut: no answer says
                // whether they ran, so they are in doubt.
                ctx.update(|s| r::outbox_mark_in_doubt(s, rows[i..].iter().map(|row| &row.id)))?;
                cut_at = Some(i);
                break;
            };
            match res.error() {
                None => {
                    if let Err(error) = ctx.local_tx(vec![delete_pending_row(&row.id)], None) {
                        ctx.log(
                            LogLevel::Error,
                            "outbox row delete failed after a successful push",
                            log_data(vec![
                                ("id", row.id.clone().into()),
                                ("error", error.to_string().into()),
                            ]),
                        )?;
                    }
                    ctx.update(|s| r::outbox_ack(s, &row.id, now))?;
                    ctx.emit(OutEvent::MutationSettled {
                        mutation_id: row.id.clone(),
                        record_id: row.record_id.clone(),
                        kind: row.mutation_type,
                    })?;
                }
                // An earlier statement in this batch was rejected, so this one
                // never ran: the next round sends it again.
                Some(message) if message.contains(BATCH_SKIPPED) => break,
                Some(message) if classify_sync_error(message) == ErrorKind::Network => {
                    cut_at = Some(i);
                    break;
                }
                Some(message) => {
                    rollback(ctx, env, row, FailedMutationError::application(message)).await?;
                }
            }
        }
        ctx.timer_set("ack-prune", ACK_GRACE_MS, RuntimeEvent::AckPrune)?;
        ctx.dispatch(sync_outcome(
            cut_at.is_none(),
            cut_at.map(|_| "push interrupted".to_string()),
        ))?;
        if let Some(cut) = cut_at {
            let id = rows[cut].id.clone();
            let attempts = attempts_of(&id);
            ctx.update(|s| r::outbox_bump_attempts(s, &id))?;
            ctx.timer_set("outbox", backoff_ms(attempts), RuntimeEvent::Drain)?;
            return Ok(());
        }
    }
}

/// Undo one rejected mutation. Order matters: the tray row is written and the
/// pending row deleted in ONE transaction before any local revert, so a crash
/// mid-way leaves the mutation in the tray, never silently re-applied.
pub async fn rollback(
    ctx: &Ctx,
    env: &SagaEnv,
    row: &PendingMutationRow,
    error: FailedMutationError,
) -> Result<()> {
    let attempts = ctx.read(|s| {
        s.outbox
            .iter()
            .find(|i| i.id == row.id)
            .map(|i| i.attempts)
            .unwrap_or(0)
    })?;
    let now = ctx.now()?;
    let mut before = row.before_record.clone();
    if before.is_none() && row.mutation_type != MutationEventType::Create {
        // Legacy row: nothing captured locally, so ask the server for it.
        let mut vars = Object::new();
        vars.insert(
            "id".into(),
            Value::RecordId(RecordId::parse(&row.record_id)),
        );
        before = match ctx
            .remote_query(
                "SELECT * FROM ONLY $id",
                Some(vars),
                Some(env.remote_timeout_ms),
            )
            .await
        {
            Ok(results) => results
                .into_iter()
                .next()
                .and_then(|r| r.into_value())
                .and_then(Value::into_object),
            Err(_) => None,
        };
    }
    let plan = plan_revert(row, before.as_ref());
    let message = error.message.clone();
    let failed = build_failed_row(row, error, before.clone(), attempts, now, plan.revert);
    let id_and = |e: &crate::Error| {
        log_data(vec![
            ("id", row.id.clone().into()),
            ("error", e.to_string().into()),
        ])
    };
    if let Err(e) = ctx.local_tx(move_to_failed_tx(&failed), None) {
        ctx.log(
            LogLevel::Error,
            "failed to move a rejected mutation to the tray",
            id_and(&e),
        )?;
    }
    ctx.update(|s| {
        r::outbox_remove(s, &row.id);
        let count = s.failed_count + 1;
        r::set_failed_count(s, count);
    })?;
    if !plan.ops.is_empty() {
        if let Err(e) = ctx.local_tx(plan.ops.clone(), None) {
            ctx.log(LogLevel::Error, "local revert failed", id_and(&e))?;
        }
    }
    let circuit_op = plan.circuit.op;
    if let Err(e) = ctx.ssp_ingest(vec![plan.circuit]) {
        ctx.log(LogLevel::Warn, "circuit revert failed", id_and(&e))?;
    }
    if circuit_op == IngestOp::Delete {
        ctx.update(|s| r::delete_versions(s, [&row.record_id]))?;
    } else if let Some(rv) = num_i64(before.as_ref().and_then(|b| b.get("_00_rv"))) {
        ctx.update(|s| r::set_versions(s, &[(row.record_id.clone(), rv)]))?;
    }
    ctx.update(|s| r::mark_table_dirty(s, &row.table_name))?;
    ctx.emit(OutEvent::MutationRolledBack {
        mutation_id: row.id.clone(),
        record_id: row.record_id.clone(),
        kind: row.mutation_type,
        error: message,
    })?;
    let count = ctx.read(|s| s.failed_count)?;
    ctx.emit(OutEvent::TrayChanged { count })?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::RefCell;
    use std::rc::Rc;

    use crate::error::Error;
    use crate::kernel::events::LogLevel;
    use crate::mutation::test_support::{new_rig, record_events, state_with, test_env};
    use crate::state::ClientState;
    use crate::testing::{
        build_definition, build_entry, build_outbox_item, results, row, run_local, TestRig,
    };
    use crate::value::Row;
    use serde_json::json;

    fn pending_doc(id: &str, kind: &str, record_id: &str, data: Option<serde_json::Value>) -> Row {
        let mut doc = row(json!({
            "id": format!("{PENDING_TABLE}:{id}"),
            "mutationType": kind,
            "recordId": record_id,
            "tableName": "thing",
            "createdAt": 1,
            "v": 2,
        }));
        let data = data.or_else(|| (kind == "create").then(|| json!({"title": "x"})));
        if let Some(data) = data {
            doc.insert("data".into(), Value::Object(row(data)));
        }
        doc
    }

    /// A rig whose outbox holds `docs` (state items plus their stored rows).
    fn queued(docs: Vec<Row>) -> TestRig {
        let mut state = ClientState::empty("tab");
        let items = docs
            .iter()
            .map(|d| {
                let id = d["id"].to_id_string();
                let kind = MutationEventType::parse(d["mutationType"].as_str().unwrap()).unwrap();
                build_outbox_item(&id, kind, d["recordId"].as_str().unwrap())
            })
            .collect();
        r::outbox_replace(&mut state, items);
        let (rig, _) = new_rig(test_env(), state);
        for d in docs {
            rig.local.seed(PENDING_TABLE, &d["id"].to_id_string(), d);
        }
        rig
    }

    async fn run_drain(rig: &TestRig) {
        rig.run(|ctx| async move { drain(&ctx, &test_env()).await })
            .await
            .unwrap()
    }

    fn pid(id: &str) -> String {
        format!("{PENDING_TABLE}:{id}")
    }

    #[tokio::test]
    async fn pushes_in_fifo_order_acks_each_accepted_row_and_deletes_it() {
        run_local(async {
            let rig = queued(vec![
                pending_doc("m1", "create", "thing:1", None),
                pending_doc("m2", "create", "thing:2", None),
            ]);
            let events = record_events(&rig);
            rig.remote
                .answer_with(|_, _| Ok(results(vec![Ok(json!(null)), Ok(json!(null))])));
            run_drain(&rig).await;
            // A push must never overlap another on the socket.
            let sent = rig.log.named("remote.write");
            assert_eq!(sent.len(), 1);
            assert!(!rig.log.contains("remote.query"));
            assert_eq!(sent[0].args.matches("LET $ok").count(), 2);
            let st = rig.state();
            assert!(st.outbox.iter().all(|i| i.status == OutboxStatus::Acked));
            assert_eq!(rig.log.count("local.tx"), 2);
            assert!(rig.local.row(PENDING_TABLE, &pid("m1")).is_none());
            assert_eq!(events.count("mutation:settled"), 2);
            assert_eq!(rig.timers.delay("ack-prune"), Some(30_000));
            assert!(!rig.timers.has("outbox"));
        })
        .await;
    }

    #[tokio::test]
    async fn a_rejected_statement_rolls_back_only_that_row_and_the_skipped_rest_goes_out_again() {
        run_local(async {
            let rig = queued(vec![
                pending_doc("m1", "create", "thing:1", None),
                pending_doc("m2", "create", "thing:2", None),
            ]);
            let events = record_events(&rig);
            let pushed: Rc<RefCell<Vec<Vec<String>>>> = Rc::default();
            let seen = pushed.clone();
            rig.remote.answer_with(move |_, vars| {
                let vars = vars.unwrap();
                let ids: Vec<String> = vars
                    .iter()
                    .filter(|(k, _)| k.starts_with("id"))
                    .map(|(_, v)| crate::value::key_of(&v.to_id_string()).to_string())
                    .collect();
                seen.borrow_mut().push(ids);
                Ok(if seen.borrow().len() == 1 {
                    vec![
                        crate::kernel::ports::StatementResult::err("permission denied"),
                        crate::kernel::ports::StatementResult::err(format!(
                            "An error occurred: {BATCH_SKIPPED}"
                        )),
                    ]
                } else {
                    results(vec![Ok(json!(null))])
                })
            });
            run_drain(&rig).await;
            assert_eq!(*pushed.borrow(), vec![vec!["1", "2"], vec!["2"]]);
            let st = rig.state();
            assert_eq!(st.outbox.len(), 1);
            assert_eq!(st.outbox[0].id, pid("m2"));
            assert_eq!(st.outbox[0].status, OutboxStatus::Acked);
            assert_eq!(st.outbox[0].attempts, 0);
            assert_eq!(st.failed_count, 1);
            let rolled: Vec<String> = events
                .all()
                .iter()
                .filter_map(|e| match e {
                    OutEvent::MutationRolledBack { mutation_id, .. } => Some(mutation_id.clone()),
                    _ => None,
                })
                .collect();
            assert_eq!(rolled, [pid("m1")]);
            assert!(!rig.timers.has("outbox"));
            assert!(rig
                .local
                .row(FAILED_TABLE, "_00_failed_mutations:m1")
                .is_some());
        })
        .await;
    }

    #[tokio::test]
    async fn a_transient_server_state_keeps_the_tail_queued_instead_of_dropping_it() {
        // The 2026-09-08 import loss: these read as application errors before,
        // so every queued CREATE behind a stalled database was rolled back.
        run_local(async {
            for message in [
                "Transaction conflict: Resource busy",
                "Specify a namespace to use",
                // sp00ky's own ingest event could not post to the scheduler
                // (down, past its TIMEOUT, 5xx): an outage, not the mutation's
                // fault.
                "Error while processing event _00_thing_mutation: There was an error processing a remote HTTP request: error sending request for url (http://scheduler:9667/ingest)",
                "Error while processing event _00_thing_mutation: There was an error processing a remote HTTP request: 503 Service Unavailable",
            ] {
                let rig = queued(vec![pending_doc("m1", "create", "thing:1", None)]);
                rig.remote.answer_with(move |_, _| Ok(results(vec![Err(message)])));
                run_drain(&rig).await;
                let st = rig.state();
                assert_eq!(st.outbox.len(), 1, "{message}");
                assert_eq!(st.failed_count, 0, "{message}");
                assert!(rig.timers.has("outbox"), "{message}");
            }
        })
        .await;
    }

    #[tokio::test]
    async fn a_thrown_transport_failure_backs_off_and_stops() {
        run_local(async {
            let rig = queued(vec![pending_doc("m1", "create", "thing:1", None)]);
            rig.remote
                .answer_with(|_, _| Err(Error::Network("socket closed".into())));
            run_drain(&rig).await;
            let st = rig.state();
            assert_eq!(st.outbox[0].attempts, 1);
            assert_eq!(rig.timers.delay("outbox"), Some(500));
            assert_eq!(st.failed_count, 0);
            // No answer: the server may have committed it.
            assert!(st.outbox[0].in_doubt);
        })
        .await;
    }

    #[tokio::test]
    async fn a_push_whose_answer_was_lost_goes_out_again_with_creates_that_accept_their_own_commit()
    {
        run_local(async {
            let rig = queued(vec![
                pending_doc("m1", "create", "thing:1", None),
                pending_doc("m2", "create", "thing:2", None),
            ]);
            let events = record_events(&rig);
            let pushed: Rc<RefCell<Vec<String>>> = Rc::default();
            let seen = pushed.clone();
            rig.remote.answer_with(move |sql, _| {
                seen.borrow_mut().push(sql.to_string());
                // The server ran the batch; the answer never came back.
                if seen.borrow().len() == 1 {
                    return Err(Error::Timeout(15_000));
                }
                Ok(results(vec![Ok(json!(null)), Ok(json!(null))]))
            });
            run_drain(&rig).await;
            assert!(rig.state().outbox.iter().all(|i| i.in_doubt));
            assert_eq!(rig.timers.delay("outbox"), Some(500));
            run_drain(&rig).await;
            let tolerant = |i: usize| {
                format!(
                    "LET $was{i} = (SELECT * FROM ONLY $id{i}); IF ($was{i} = NONE) OR ($was{i}.title != $d{i}_title) {{ CREATE ONLY $id{i} SET title = $d{i}_title }}"
                )
            };
            let pushed = pushed.borrow();
            assert!(!pushed[0].contains("$was"));
            assert_eq!(
                pushed.last().unwrap().split(";\n").collect::<Vec<_>>(),
                vec![
                    format!("LET $ok0 = {{ {}; true }}", tolerant(0)),
                    format!("LET $ok1 = {{ IF !$ok0 {{ THROW \"{BATCH_SKIPPED}\" }}; {}; true }}", tolerant(1)),
                ]
            );
            assert_eq!(events.count("mutation:rolled-back"), 0);
            assert!(rig.state().outbox.iter().all(|i| i.status == OutboxStatus::Acked));
        })
        .await;
    }

    #[tokio::test]
    async fn a_short_result_list_is_a_cut_and_the_rest_stays_queued() {
        run_local(async {
            let rig = queued(vec![
                pending_doc("m1", "create", "thing:1", None),
                pending_doc("m2", "create", "thing:2", None),
            ]);
            rig.remote
                .answer_with(|_, _| Ok(results(vec![Ok(json!(null))])));
            run_drain(&rig).await;
            let st = rig.state();
            let statuses: Vec<_> = st.outbox.iter().map(|i| i.status).collect();
            assert_eq!(statuses, [OutboxStatus::Acked, OutboxStatus::Pending]);
            // No answer says whether the second one ran.
            let doubt: Vec<_> = st.outbox.iter().map(|i| i.in_doubt).collect();
            assert_eq!(doubt, [false, true]);
            assert_eq!(st.outbox[1].attempts, 1);
            assert!(rig.timers.has("outbox"));
        })
        .await;
    }

    #[tokio::test]
    async fn a_request_rejected_as_a_whole_rolls_back_the_head_and_continues() {
        run_local(async {
            let rig = queued(vec![pending_doc("m1", "create", "thing:1", None)]);
            rig.remote.answer_with(|_, _| {
                Err(Error::Rpc {
                    code: -32000,
                    message: "parse error in query".into(),
                })
            });
            run_drain(&rig).await;
            let st = rig.state();
            assert!(st.outbox.is_empty());
            assert_eq!(st.failed_count, 1);
        })
        .await;
    }

    #[tokio::test]
    async fn a_pending_row_the_store_no_longer_has_is_dropped() {
        run_local(async {
            let mut state = ClientState::empty("tab");
            r::outbox_replace(
                &mut state,
                vec![build_outbox_item(
                    &pid("gone"),
                    MutationEventType::Create,
                    "thing:1",
                )],
            );
            let (rig, _) = new_rig(test_env(), state);
            run_drain(&rig).await;
            assert!(rig.state().outbox.is_empty());
            assert!(!rig.log.contains("remote.write"));
        })
        .await;
    }

    #[tokio::test]
    async fn an_empty_outbox_does_nothing() {
        run_local(async {
            let (rig, _) = new_rig(test_env(), ClientState::empty("tab"));
            run_drain(&rig).await;
            assert!(rig.log.calls().is_empty());
        })
        .await;
    }

    #[tokio::test]
    async fn a_client_with_no_endpoint_never_drains() {
        // Draining against nothing would read as a rejection and roll the
        // local-first write back.
        run_local(async {
            let rig = queued(vec![pending_doc("m1", "create", "thing:1", None)]);
            let env = SagaEnv {
                has_remote: false,
                ..test_env()
            };
            rig.run(|ctx| async move { drain(&ctx, &env).await })
                .await
                .unwrap();
            assert!(rig.log.calls().is_empty());
            assert_eq!(rig.state().outbox.len(), 1);
        })
        .await;
    }

    #[tokio::test]
    async fn stored_payloads_are_retyped_from_the_schema_before_pushing() {
        run_local(async {
            let rig = queued(vec![pending_doc(
                "m1",
                "update",
                "thing:1",
                Some(json!({"owner": "user:u1"})),
            )]);
            let sent: Rc<RefCell<Option<Object>>> = Rc::default();
            let seen = sent.clone();
            rig.remote.answer_with(move |_, vars| {
                *seen.borrow_mut() = vars.cloned();
                Ok(results(vec![Ok(json!(null))]))
            });
            run_drain(&rig).await;
            let vars = sent.borrow().clone().unwrap();
            let data = vars["data0"].as_object().unwrap();
            assert!(matches!(data["owner"], Value::RecordId(_)));
        })
        .await;
    }

    #[tokio::test]
    async fn batches_are_capped_at_the_configured_size() {
        run_local(async {
            let rig = queued(vec![
                pending_doc("m1", "create", "thing:1", None),
                pending_doc("m2", "create", "thing:2", None),
                pending_doc("m3", "create", "thing:3", None),
            ]);
            rig.remote.answer_with(|sql, _| {
                Ok(results(sql.split(";\n").map(|_| Ok(json!(null))).collect()))
            });
            let env = SagaEnv {
                outbox_batch_size: 2,
                ..test_env()
            };
            rig.run(|ctx| async move { drain(&ctx, &env).await })
                .await
                .unwrap();
            let sizes: Vec<usize> = rig
                .log
                .named("remote.write")
                .iter()
                .map(|c| c.args.matches("LET $ok").count())
                .collect();
            assert_eq!(sizes, [2, 1]);
            assert!(rig
                .state()
                .outbox
                .iter()
                .all(|i| i.status == OutboxStatus::Acked));
        })
        .await;
    }

    // ---- rollback ---------------------------------------------------------------

    fn rollback_row(
        kind: MutationEventType,
        before: Option<serde_json::Value>,
    ) -> PendingMutationRow {
        PendingMutationRow {
            id: pid("m1"),
            mutation_type: kind,
            record_id: "thing:1".into(),
            table_name: "thing".into(),
            data: Some(row(json!({"title": "x"}))),
            before_record: before.map(row),
            created_at: 1,
            v: 2,
        }
    }

    async fn run_rollback(rig: &TestRig, row: PendingMutationRow) {
        rig.run(|ctx| async move {
            rollback(
                &ctx,
                &test_env(),
                &row,
                FailedMutationError::application("nope"),
            )
            .await
        })
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn a_create_moves_to_the_tray_first_then_deletes_locally_and_in_the_circuit() {
        run_local(async {
            let mut state = state_with(vec![build_entry(build_definition("a", "thing"))]);
            r::set_versions(&mut state, &[("thing:1".into(), 1)]);
            r::outbox_replace(
                &mut state,
                vec![build_outbox_item(
                    &pid("m1"),
                    MutationEventType::Create,
                    "thing:1",
                )],
            );
            state.dirty.clear();
            let (rig, _) = new_rig(test_env(), state);
            let events = record_events(&rig);
            run_rollback(&rig, rollback_row(MutationEventType::Create, None)).await;
            let txs = rig.log.named("local.tx");
            assert_eq!(txs.len(), 2);
            // The tray row is written before anything is undone.
            assert!(txs[0].args.contains(FAILED_TABLE), "{}", txs[0].args);
            assert!(txs[1].args.starts_with("[Delete"), "{}", txs[1].args);
            assert_eq!(rig.ssp.ingested.borrow()[0].op, IngestOp::Delete);
            let st = rig.state();
            assert!(!st.versions.contains_key("thing:1"));
            assert!(st.outbox.is_empty());
            assert_eq!(st.failed_count, 1);
            assert!(st.dirty.contains("a"));
            let trays: Vec<usize> = events
                .all()
                .iter()
                .filter_map(|e| match e {
                    OutEvent::TrayChanged { count } => Some(*count),
                    _ => None,
                })
                .collect();
            assert_eq!(trays, [1]);
        })
        .await;
    }

    #[tokio::test]
    async fn an_update_restores_before_and_its_version() {
        run_local(async {
            let (rig, _) = new_rig(test_env(), ClientState::empty("tab"));
            run_rollback(
                &rig,
                rollback_row(
                    MutationEventType::Update,
                    Some(json!({"id": "thing:1", "title": "old", "_00_rv": 4})),
                ),
            )
            .await;
            assert_eq!(
                rig.local.row("thing", "thing:1").unwrap()["title"],
                Value::from("old")
            );
            assert_eq!(rig.state().versions.get("thing:1"), Some(&4));
            let failed = rig
                .local
                .row(FAILED_TABLE, "_00_failed_mutations:m1")
                .unwrap();
            assert_eq!(failed["revert"], Value::from("full"));
        })
        .await;
    }

    #[tokio::test]
    async fn a_legacy_row_with_no_before_asks_the_server_for_it() {
        run_local(async {
            let (rig, _) = new_rig(test_env(), ClientState::empty("tab"));
            rig.remote.answer_with(|_, _| {
                Ok(results(vec![Ok(
                    json!({"id": "thing:1", "title": "server", "_00_rv": 9}),
                )]))
            });
            run_rollback(&rig, rollback_row(MutationEventType::Update, None)).await;
            assert_eq!(rig.state().versions.get("thing:1"), Some(&9));
            assert!(rig.log.contains("remote.query"));
        })
        .await;
    }

    #[tokio::test]
    async fn an_unreachable_server_leaves_the_revert_partial_not_wrong() {
        run_local(async {
            let (rig, _) = new_rig(test_env(), ClientState::empty("tab"));
            rig.remote
                .answer_with(|_, _| Err(Error::Network("socket closed".into())));
            run_rollback(&rig, rollback_row(MutationEventType::Update, None)).await;
            // Only the tray move ran: nothing can be restored without a before row.
            assert_eq!(rig.log.count("local.tx"), 1);
            assert_eq!(rig.state().failed_count, 1);
            let failed = rig
                .local
                .row(FAILED_TABLE, "_00_failed_mutations:m1")
                .unwrap();
            assert_eq!(failed["revert"], Value::from("partial"));
        })
        .await;
    }

    #[tokio::test]
    async fn every_local_step_is_best_effort() {
        run_local(async {
            let (rig, faults) = new_rig(test_env(), ClientState::empty("tab"));
            *rig.local.fail_writes.borrow_mut() = Some("disk full".into());
            faults.fail_ssp_ingest();
            let events = record_events(&rig);
            run_rollback(&rig, rollback_row(MutationEventType::Create, None)).await;
            assert_eq!(rig.state().failed_count, 1);
            let levels: Vec<LogLevel> = events.logs().into_iter().map(|(l, _)| l).collect();
            assert_eq!(levels, [LogLevel::Error, LogLevel::Error, LogLevel::Warn]);
        })
        .await;
    }

    // ---- load_outbox ---------------------------------------------------------------

    #[tokio::test]
    async fn load_mirrors_pending_rows_trays_unreplayable_ones_and_starts_a_drain() {
        run_local(async {
            let (rig, _) = new_rig(test_env(), ClientState::empty("tab"));
            rig.local.seed(
                PENDING_TABLE,
                &pid("m2"),
                pending_doc("m2", "create", "thing:1", None),
            );
            rig.local.seed(
                PENDING_TABLE,
                &pid("m1"),
                pending_doc("m1", "create", "thing:2", None),
            );
            // A create with no payload can never be replayed.
            rig.local.seed(
                PENDING_TABLE,
                &pid("m3"),
                row(json!({"id": pid("m3"), "mutationType": "create", "recordId": "thing:3"})),
            );
            let events = record_events(&rig);
            rig.run(|ctx| async move { load_outbox(&ctx, &test_env()).await })
                .await
                .unwrap();
            let st = rig.state();
            // The drain order is id order.
            let ids: Vec<&str> = st.outbox.iter().map(|i| i.id.as_str()).collect();
            assert_eq!(ids, [pid("m1"), pid("m2")]);
            // A run that died mid-push may have sent them.
            assert!(st.outbox.iter().all(|i| i.in_doubt));
            assert_eq!(events.count("mutation:rolled-back"), 1);
            assert_eq!(st.failed_count, 1);
            let failed = rig
                .local
                .row(FAILED_TABLE, "_00_failed_mutations:m3")
                .unwrap();
            assert_eq!(
                failed["error"].get("kind"),
                Some(&Value::from("unreplayable"))
            );
            assert!(rig.local.row(PENDING_TABLE, &pid("m3")).is_none());

            rig.remote
                .answer_with(|_, _| Ok(results(vec![Ok(json!(null)), Ok(json!(null))])));
            rig.settle().await;
            assert_eq!(
                rig.log.count("remote.write"),
                1,
                "the dispatched drain pushed"
            );
        })
        .await;
    }

    #[tokio::test]
    async fn an_empty_outbox_starts_no_drain() {
        run_local(async {
            let (rig, _) = new_rig(test_env(), ClientState::empty("tab"));
            rig.run(|ctx| async move { load_outbox(&ctx, &test_env()).await })
                .await
                .unwrap();
            // Queue a row now: a dispatched drain would push it.
            rig.runtime.update(|s| {
                r::outbox_push(
                    s,
                    build_outbox_item("m1", MutationEventType::Create, "thing:1"),
                )
            });
            rig.local.seed(
                PENDING_TABLE,
                "m1",
                pending_doc("m1", "create", "thing:1", None),
            );
            rig.settle().await;
            assert!(!rig.log.contains("remote.write"));
        })
        .await;
    }

    #[tokio::test]
    async fn refresh_failed_count_reads_the_tray() {
        run_local(async {
            let (rig, faults) = new_rig(test_env(), ClientState::empty("tab"));
            rig.local
                .seed(FAILED_TABLE, "_00_failed_mutations:a", Row::new());
            rig.local
                .seed(FAILED_TABLE, "_00_failed_mutations:b", Row::new());
            rig.run(|ctx| async move { refresh_failed_count(&ctx).await })
                .await
                .unwrap();
            assert_eq!(rig.state().failed_count, 2);
            // The tray table may not exist yet on a fresh store.
            faults.fail_local_get_all();
            rig.runtime.update(|s| s.failed_count = 5);
            rig.run(|ctx| async move { refresh_failed_count(&ctx).await })
                .await
                .unwrap();
            assert_eq!(rig.state().failed_count, 5);
        })
        .await;
    }
}
