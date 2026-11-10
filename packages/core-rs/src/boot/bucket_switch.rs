//! Switch the local store to another principal's bucket.
//!
//! PORT: `packages/spooky_core/lib/src/boot/bucket_switch_saga.dart`.

use crate::boot::boot::prime_circuit;
use crate::error::Result;
use crate::kernel::events::{LogLevel, OutEvent, RuntimeEvent};
use crate::kernel::ports::RegisterPlan;
use crate::kernel::Ctx;
use crate::mutation::push::load_outbox;
use crate::mutation::rows::log_data;
use crate::query::env::SagaEnv;
use crate::query::hash::{query_hash_input, QueryKeyInput};
use crate::query::membership::{is_resolved_before, parse_view_row};
use crate::query::sql::{view_record_id, VIEW_TABLE};
use crate::state::lifecycle::{is_authoritative, seed_lifecycle};
use crate::state::reducers::{self as r, Identity};
use crate::types::RecordVersionArray;
use crate::value::{RecordId, Value};

/// The timers that belong to the outgoing bucket.
const BUCKET_TIMERS: [&str; 5] = ["poll", "outbox", "membership", "fetch", "ack-prune"];

/// Move the client to another principal's local store: swap and rebind.
///
/// Runs on the serial `bucket` lane; a switch that wakes up to a newer
/// `pending_bucket` steps aside. Every active query keeps its hash and is
/// re-seeded from the new store's `_00_view` rows, then re-registered.
pub async fn bucket_switch(ctx: &Ctx, env: &SagaEnv, target: &str) -> Result<()> {
    let pending = ctx.read(|s| s.pending_bucket.clone())?;
    let current = ctx.service_sync(|s| s.local_current_bucket_id())?;
    if pending.as_deref() != Some(target) || current == target {
        return Ok(());
    }

    for key in BUCKET_TIMERS {
        ctx.timer_clear(key)?;
    }
    ctx.update(r::clear_bucket_state)?;
    ctx.service_sync(|s| s.crdt_close_all(false))?;

    {
        let target = target.to_string();
        ctx.service(|s| async move { s.local_switch_store(&target).await })
            .await?;
    }
    ctx.service(|s| async move { s.migrator_provision().await })
        .await?;
    ctx.service(|s| async move { s.ssp_reset().await }).await?;
    ctx.service_sync(|s| s.ssp_set_permissions())??;
    let auth_id = ctx.service_sync(|s| s.auth_session_auth_id())?;
    let access = ctx.service_sync(|s| s.auth_access())?;
    ctx.service_sync(|s| s.ssp_set_session_auth(auth_id.as_deref(), access.as_deref()))?;
    ctx.update(|s| {
        r::set_identity(
            s,
            Identity {
                bucket_id: Some(Some(target.to_string())),
                ..Default::default()
            },
        )
    })?;
    load_outbox(ctx, env).await?;
    prime_circuit(ctx).await?;

    if let Some(token) = ctx.service_sync(|s| s.auth_token())? {
        let persisted = ctx
            .service(|s| async move {
                s.persistence_set("sp00ky_auth_token", Some(Value::String(token)))
                    .await
            })
            .await;
        if let Err(error) = persisted {
            ctx.log(
                LogLevel::Warn,
                "failed to re-persist the auth token",
                log_data(vec![("error", error.to_string().into())]),
            )?;
        }
    }
    rebind_queries(ctx).await?;
    ctx.dispatch(RuntimeEvent::ensure_registered())?;
    ctx.dispatch(RuntimeEvent::LiveStart)?;
    // The switch cleared the poll timer, and only the tick itself re-arms it: a
    // sign-in (auth flip -> bucket switch) otherwise left the client running on
    // LIVE alone for the rest of the session, so any membership change LIVE did
    // not deliver was never noticed.
    ctx.dispatch(RuntimeEvent::PollTick)?;
    ctx.dispatch(RuntimeEvent::Drain)?;
    Ok(())
}

/// Re-seed every active query from the new store and rebuild its SSP view.
pub async fn rebind_queries(ctx: &Ctx) -> Result<()> {
    let entries: Vec<_> = ctx.read(|s| {
        s.queries
            .values()
            .map(|e| (e.def.clone(), is_authoritative(&e.lifecycle)))
            .collect()
    })?;
    let session_id = ctx.read(|s| s.session_id.clone())?;
    for (def, was_authoritative) in entries {
        let view = ctx
            .local_get(VIEW_TABLE, &view_record_id(&def.view_key))
            .ok()
            .and_then(|row| parse_view_row(row.as_ref()));
        let hash = ctx.hash(&query_hash_input(
            &QueryKeyInput {
                surql: &def.surql,
                params: &def.params,
            },
            session_id.as_deref(),
        ))?;
        let registered = ctx.ssp_register(RegisterPlan {
            query_hash: def.hash.clone(),
            surql: def.surql.clone(),
            params: def.params.clone(),
            ttl: def.ttl.clone(),
            table_name: def.table_name.clone(),
        });
        let local_array: RecordVersionArray = match registered {
            Ok(reg) => reg.local_array,
            Err(error) => {
                ctx.log(
                    LogLevel::Warn,
                    "local view rebuild failed after bucket switch",
                    log_data(vec![
                        ("hash", def.hash.clone().into()),
                        ("error", error.to_string().into()),
                    ]),
                )?;
                Vec::new()
            }
        };
        let lifecycle = seed_lifecycle(is_resolved_before(view.as_ref()));
        let remote_array = view.map(|v| v.ids).unwrap_or_default();
        ctx.update(|s| {
            r::rebind_query(
                s,
                &def.hash,
                RecordId::new("_00_query", hash),
                lifecycle,
                remote_array,
                local_array,
            )
        })?;
        let known = is_authoritative(&lifecycle);
        if was_authoritative != known {
            ctx.emit(OutEvent::QueryAuthority {
                hash: def.hash,
                known,
            })?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kernel::events::LogLevel;
    use crate::mutation::test_support::{
        in_order, new_rig, record_events, service_calls, state_with, test_env,
    };
    use crate::state::lifecycle::{QueryLifecycle, QueryPhase};
    use crate::state::{ClientState, RemotePhase};
    use crate::testing::{build_definition, build_entry, build_outbox_item, row, run_local};
    use crate::types::MutationEventType;
    use serde_json::json;
    use std::sync::Arc;

    #[tokio::test]
    async fn steps_aside_when_a_newer_target_won_the_race() {
        run_local(async {
            let mut state = ClientState::empty("tab");
            state.pending_bucket = Some("u2".into());
            let (rig, _) = new_rig(test_env(), state);
            rig.run(|ctx| async move { bucket_switch(&ctx, &test_env(), "u1").await })
                .await
                .unwrap();
            assert_eq!(service_calls(&rig), ["localCurrentBucketId"]);
        })
        .await;
    }

    #[tokio::test]
    async fn clears_the_per_bucket_slices_swaps_the_store_and_re_drives() {
        run_local(async {
            let mut state = ClientState::empty("tab");
            state.pending_bucket = Some("u1".into());
            r::set_versions(&mut state, &[("thing:1".into(), 1)]);
            r::outbox_replace(
                &mut state,
                vec![build_outbox_item(
                    "m1",
                    MutationEventType::Create,
                    "thing:1",
                )],
            );
            let (rig, _) = new_rig(test_env(), state);
            *rig.services.auth_token.borrow_mut() = Some("jwt".into());
            let ctx = rig.ctx();
            for key in BUCKET_TIMERS {
                ctx.timer_set(key, 1000, RuntimeEvent::Drain).unwrap();
            }
            rig.run(|ctx| async move { bucket_switch(&ctx, &test_env(), "u1").await })
                .await
                .unwrap();
            let st = rig.state();
            assert!(st.versions.is_empty());
            assert!(st.outbox.is_empty());
            assert_eq!(st.bucket_id.as_deref(), Some("u1"));
            assert!(st.primed);
            assert!(in_order(
                &service_calls(&rig),
                &[
                    "crdtCloseAll",
                    "localSwitchStore",
                    "migratorProvision",
                    "sspReset",
                    "sspSetPermissions",
                    "persistenceSet"
                ]
            ));
            assert_eq!(
                rig.services.persisted.borrow().get("sp00ky_auth_token"),
                Some(&Some(Value::from("jwt")))
            );
            // The poll timer is cleared by the switch, so only the tick re-arms it.
            for key in BUCKET_TIMERS {
                assert!(!rig.timers.has(key), "{key} still armed");
            }
        })
        .await;
    }

    #[tokio::test]
    async fn rebind_reseeds_every_query_from_the_new_store() {
        run_local(async {
            let mut entry = build_entry(build_definition("a", "thing"));
            entry.lifecycle = QueryLifecycle {
                phase: QueryPhase::Live,
                remote: RemotePhase::Registered,
                fetch_depth: 0,
                background_depth: 0,
                notified: true,
            };
            entry.remote_array = vec![("thing:old".into(), 1)];
            entry.records = Arc::new(vec![row(json!({"id": "thing:old"}))]);
            let (rig, _) = new_rig(test_env(), state_with(vec![entry]));
            rig.ssp.local_arrays(|_| vec![("thing:new".into(), 1)]);
            rig.local.seed(
                VIEW_TABLE,
                &view_record_id("view-a"),
                row(json!({"ids": [["thing:new", 1]], "confirmed": true})),
            );
            let events = record_events(&rig);
            rig.run(|ctx| async move { rebind_queries(&ctx).await })
                .await
                .unwrap();
            let st = rig.state();
            let e = &st.queries["a"];
            // The hash is the identity.
            assert_eq!(e.def.hash, "a");
            assert_ne!(e.def.id, RecordId::new("_00_query", "a"));
            assert_eq!(e.remote_array, vec![("thing:new".to_string(), 1)]);
            assert_eq!(e.local_array, vec![("thing:new".to_string(), 1)]);
            assert!(e.records.is_empty());
            assert_eq!(e.lifecycle.phase, QueryPhase::Cached);
            assert_eq!(e.lifecycle.remote, RemotePhase::Unregistered);
            // Authoritative before and after: no authority flip.
            assert_eq!(events.count("query:authority"), 0);
        })
        .await;
    }

    #[tokio::test]
    async fn rebind_tells_subscribers_when_authority_flips() {
        run_local(async {
            let mut entry = build_entry(build_definition("a", "thing"));
            entry.lifecycle.phase = QueryPhase::Live;
            let (rig, _) = new_rig(test_env(), state_with(vec![entry]));
            let events = record_events(&rig);
            rig.run(|ctx| async move { rebind_queries(&ctx).await })
                .await
                .unwrap();
            assert_eq!(rig.state().queries["a"].lifecycle.phase, QueryPhase::Cold);
            assert!(events.all().iter().any(
                |e| matches!(e, OutEvent::QueryAuthority { hash, known: false } if hash == "a")
            ));
        })
        .await;
    }

    #[tokio::test]
    async fn a_failing_local_view_rebuild_leaves_an_empty_window() {
        run_local(async {
            let (rig, faults) = new_rig(
                test_env(),
                state_with(vec![build_entry(build_definition("a", "thing"))]),
            );
            rig.ssp.local_arrays(|_| vec![("thing:1".into(), 1)]);
            faults.fail_ssp_register();
            let events = record_events(&rig);
            rig.run(|ctx| async move { rebind_queries(&ctx).await })
                .await
                .unwrap();
            assert!(rig.state().queries["a"].local_array.is_empty());
            let logs = events.logs();
            assert_eq!(logs.len(), 1);
            assert_eq!(logs[0].0, LogLevel::Warn);
        })
        .await;
    }
}
