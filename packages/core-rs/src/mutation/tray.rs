//! The failed-write tray: list, retry, discard.
//!
//! PORT: `packages/spooky_core/lib/src/mutation/tray_saga.dart`.

use crate::error::Result;
use crate::kernel::events::OutEvent;
use crate::kernel::Ctx;
use crate::mutation::rows::{
    delete_failed_row, failed_record_id, parse_failed_row, sort_failed_rows, FailedMutationRow,
    FAILED_TABLE,
};
use crate::mutation::write::{write, WriteInput};
use crate::query::env::SagaEnv;
use crate::state::reducers as r;

/// Every rejected mutation still in the tray, oldest first.
pub async fn list_failed(ctx: &Ctx) -> Result<Vec<FailedMutationRow>> {
    let raw = ctx.local_get_all(FAILED_TABLE)?;
    Ok(sort_failed_rows(
        raw.iter().filter_map(parse_failed_row).collect(),
    ))
}

fn load_failed(ctx: &Ctx, mutation_id: &str) -> Result<Option<FailedMutationRow>> {
    let id = failed_record_id(mutation_id);
    Ok(ctx.local_get(FAILED_TABLE, &id)?.and_then(|mut raw| {
        raw.entry("id".to_string())
            .or_insert_with(|| id.clone().into());
        parse_failed_row(&raw)
    }))
}

fn drop_failed(ctx: &Ctx, mutation_id: &str) -> Result<()> {
    ctx.local_tx(vec![delete_failed_row(mutation_id)], None)?;
    let count = ctx.update(|s| {
        let count = s.failed_count.saturating_sub(1);
        r::set_failed_count(s, count);
        count
    })?;
    ctx.emit(OutEvent::TrayChanged { count })
}

/// Re-apply a rejected mutation as a NEW optimistic write (fresh mutation id,
/// the optimistic local change comes back, the outbox drains), then drop the
/// tray row.
pub async fn retry_failed(ctx: &Ctx, env: &SagaEnv, mutation_id: &str) -> Result<bool> {
    let Some(failed) = load_failed(ctx, mutation_id)? else {
        return Ok(false);
    };
    write(
        ctx,
        env,
        WriteInput {
            kind: failed.mutation_type,
            record_id: failed.record_id,
            data: failed.data,
            options: None,
        },
    )
    .await?;
    drop_failed(ctx, mutation_id)?;
    Ok(true)
}

pub async fn discard_failed(ctx: &Ctx, mutation_id: &str) -> Result<bool> {
    if load_failed(ctx, mutation_id)?.is_none() {
        return Ok(false);
    }
    drop_failed(ctx, mutation_id)?;
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mutation::mutation_id::PENDING_TABLE;
    use crate::mutation::test_support::{new_rig, record_events, test_env};
    use crate::state::ClientState;
    use crate::testing::{row, run_local};
    use crate::value::{Row, Value};
    use serde_json::json;

    fn failed_doc(id: &str, failed_at: i64) -> Row {
        row(json!({
            "id": format!("{FAILED_TABLE}:{id}"),
            "mutationType": "update",
            "recordId": "thing:1",
            "tableName": "thing",
            "data": {"title": "x"},
            "beforeRecord": {"id": "thing:1", "title": "old"},
            "error": {"message": "nope", "kind": "application"},
            "attempts": 1,
            "createdAt": 1,
            "failedAt": failed_at,
            "revert": "full",
        }))
    }

    fn trays(events: &crate::mutation::test_support::Events) -> Vec<usize> {
        events
            .all()
            .iter()
            .filter_map(|e| match e {
                OutEvent::TrayChanged { count } => Some(*count),
                _ => None,
            })
            .collect()
    }

    #[tokio::test]
    async fn lists_parsed_rows_oldest_first_skipping_junk() {
        run_local(async {
            let (rig, _) = new_rig(test_env(), ClientState::empty("tab"));
            rig.local
                .seed(FAILED_TABLE, "_00_failed_mutations:b", failed_doc("b", 20));
            rig.local
                .seed(FAILED_TABLE, "_00_failed_mutations:a", failed_doc("a", 10));
            rig.local.seed(
                FAILED_TABLE,
                "_00_failed_mutations:junk",
                row(json!({"garbage": true})),
            );
            let out = rig
                .run(|ctx| async move { list_failed(&ctx).await })
                .await
                .unwrap();
            let at: Vec<i64> = out.iter().map(|f| f.failed_at).collect();
            assert_eq!(at, [10, 20]);
            assert_eq!(out[0].id, format!("{PENDING_TABLE}:a"));
        })
        .await;
    }

    #[tokio::test]
    async fn retry_reapplies_the_write_with_a_new_id_and_drops_the_tray_row() {
        run_local(async {
            let mut state = ClientState::empty("tab");
            state.failed_count = 1;
            let (rig, _) = new_rig(test_env(), state);
            rig.local
                .seed(FAILED_TABLE, "_00_failed_mutations:a", failed_doc("a", 1));
            rig.local.seed(
                "thing",
                "thing:1",
                row(json!({"id": "thing:1", "_00_rv": 2})),
            );
            let events = record_events(&rig);
            let pending_id = format!("{PENDING_TABLE}:a");
            let retried = rig
                .run(|ctx| async move { retry_failed(&ctx, &test_env(), &pending_id).await })
                .await
                .unwrap();
            assert!(retried);
            let st = rig.state();
            assert_eq!(st.outbox.len(), 1);
            // A retry is a new write, with a fresh mutation id.
            assert_ne!(st.outbox[0].id, format!("{PENDING_TABLE}:a"));
            assert_eq!(st.failed_count, 0);
            assert_eq!(trays(&events), [0]);
            assert!(rig
                .local
                .row(FAILED_TABLE, "_00_failed_mutations:a")
                .is_none());
            assert_eq!(
                rig.local.row("thing", "thing:1").unwrap()["title"],
                Value::from("x")
            );
        })
        .await;
    }

    #[tokio::test]
    async fn discard_drops_the_row_and_never_goes_below_zero() {
        run_local(async {
            let (rig, _) = new_rig(test_env(), ClientState::empty("tab"));
            rig.local
                .seed(FAILED_TABLE, "_00_failed_mutations:a", failed_doc("a", 1));
            let id = format!("{PENDING_TABLE}:a");
            assert!(rig
                .run(|ctx| async move { discard_failed(&ctx, &id).await })
                .await
                .unwrap());
            assert_eq!(rig.state().failed_count, 0);
            assert_eq!(rig.log.count("local.tx"), 1);
        })
        .await;
    }

    #[tokio::test]
    async fn a_missing_row_returns_false_and_changes_nothing() {
        run_local(async {
            let (rig, _) = new_rig(test_env(), ClientState::empty("tab"));
            let id = format!("{PENDING_TABLE}:gone");
            let retry_id = id.clone();
            assert!(!rig
                .run(|ctx| async move { retry_failed(&ctx, &test_env(), &retry_id).await })
                .await
                .unwrap());
            assert_eq!(rig.log.count("local.tx"), 0);
            assert!(!rig
                .run(|ctx| async move { discard_failed(&ctx, &id).await })
                .await
                .unwrap());
        })
        .await;
    }
}
