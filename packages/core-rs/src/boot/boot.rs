//! Boot: the local-first half (awaited) and the network half (dispatched).
//!
//! PORT: `packages/spooky_core/lib/src/boot/boot_saga.dart`.

use crate::error::Result;
use crate::kernel::constants::GC_BOOT_DELAY_MS;
use crate::kernel::events::{LogLevel, RuntimeEvent};
use crate::kernel::ports::{IdScope, WriteMode};
use crate::kernel::Ctx;
use crate::modules::ref_tables::ANON_USER_ID;
use crate::mutation::push::load_outbox;
use crate::mutation::rows::log_data;
use crate::query::env::SagaEnv;
use crate::query::sql::{view_record_id, LEGACY_VIEW_TABLE, VIEW_TABLE};
use crate::state::reducers::{self as r, Identity};
use crate::state::RemotePhase;
use crate::value::{Row, Value};

fn error_data(error: &crate::Error) -> Option<Value> {
    log_data(vec![("error", error.to_string().into())])
}

/// Local boot. Everything awaited here is network-free: the store opens, the
/// schema provisions, the SSP starts, the session is restored from the cached
/// token, the outbox is mirrored. `local_ready` flips at the end and the
/// network half runs in the background ([`start_remote`], dispatched by the
/// client only when it has an endpoint).
pub async fn boot(ctx: &Ctx, env: &SagaEnv) -> Result<()> {
    let bucket = ctx
        .service(|s| async move { s.hint_read().await })
        .await?
        .unwrap_or_else(|| ANON_USER_ID.to_string());
    {
        let bucket = bucket.clone();
        ctx.service(|s| async move { s.local_connect(&bucket).await })
            .await?;
    }
    ctx.update(|s| {
        r::set_identity(
            s,
            Identity {
                bucket_id: Some(Some(bucket)),
                ..Default::default()
            },
        )
    })?;
    ctx.service(|s| async move { s.migrator_provision().await })
        .await?;
    migrate_window_to_view(ctx).await?;
    ctx.service(|s| async move { s.ssp_init().await }).await?;
    ctx.service_sync(|s| s.ssp_set_permissions())??;

    let restored_user_id = ctx
        .service(|s| async move { s.auth_restore_session().await })
        .await?;
    let salt = ctx.id(IdScope::Salt)?;
    let owner_secret = ctx.id(IdScope::Owner)?;
    let auth_id = ctx.service_sync(|s| s.auth_session_auth_id())?;
    ctx.update(|s| {
        r::set_identity(
            s,
            Identity {
                session_id: Some(Some(salt.clone())),
                user_id: Some(restored_user_id.clone()),
                salt_user_id: Some(auth_id.clone()),
                owner_secret: Some(owner_secret),
                ..Default::default()
            },
        )
    })?;
    ctx.service_sync(|s| s.crdt_set_session_id(&salt))?;
    if restored_user_id.is_some() {
        let access = ctx.service_sync(|s| s.auth_access())?;
        ctx.service_sync(|s| s.ssp_set_session_auth(auth_id.as_deref(), access.as_deref()))?;
    }

    load_outbox(ctx, env).await?;
    prime_circuit(ctx).await?;
    ctx.service_sync(|s| s.features_init())?;
    ctx.service_sync(|s| s.releases_init())?;
    ctx.dispatch(RuntimeEvent::LifecycleTick)?;
    // Not at once: right after the prime `versions` names every local body
    // while no query has answered yet, so a sweep now could only judge by
    // `_00_view`.
    ctx.timer_set("gc", GC_BOOT_DELAY_MS, RuntimeEvent::GcTick)?;
    ctx.update(|s| {
        r::set_identity(
            s,
            Identity {
                local_ready: Some(true),
                ..Default::default()
            },
        )
    })?;
    Ok(())
}

/// One-time copy of the legacy `_00_window` rows into `_00_view`.
pub async fn migrate_window_to_view(ctx: &Ctx) -> Result<()> {
    let migrated: Result<()> = (|| {
        if !ctx.local_get_all(VIEW_TABLE)?.is_empty() {
            return Ok(());
        }
        for row in ctx.local_get_all(LEGACY_VIEW_TABLE)? {
            let Some(id) = row
                .get("id")
                .filter(|v| !v.is_nullish())
                .map(Value::to_id_string)
            else {
                continue;
            };
            let Some(ids @ Value::Array(_)) = row.get("ids") else {
                continue;
            };
            let key = id.split_once(':').map(|(_, k)| k).unwrap_or(&id);
            let mut doc = Row::new();
            doc.insert("ids".into(), ids.clone());
            doc.insert(
                "confirmed".into(),
                Value::Bool(row.get("confirmed").and_then(Value::as_bool) == Some(true)),
            );
            doc.insert(
                "updatedAt".into(),
                row.get("updatedAt")
                    .filter(|v| !v.is_nullish())
                    .cloned()
                    .unwrap_or(Value::Int(0)),
            );
            ctx.local_put(
                VIEW_TABLE,
                &view_record_id(key),
                doc,
                WriteMode::Replace,
                None,
            )?;
        }
        Ok(())
    })();
    if let Err(error) = migrated {
        ctx.log(
            LogLevel::Debug,
            "window -> view migration skipped",
            error_data(&error),
        )?;
    }
    Ok(())
}

/// The network half of boot: connect, verify the restored session, then let
/// membership, LIVE, the poll and the outbox start. Every step is best-effort.
pub async fn start_remote(ctx: &Ctx) -> Result<()> {
    if let Err(error) = ctx
        .service(|s| async move { s.remote_connect().await })
        .await
    {
        ctx.log(
            LogLevel::Warn,
            "remote connect failed; running from the local store",
            error_data(&error),
        )?;
    }
    ctx.service_sync(|s| s.supervisor_start())?;
    if let Err(error) = ctx.service(|s| async move { s.auth_init().await }).await {
        ctx.log(
            LogLevel::Warn,
            "auth verification failed; keeping the restored session",
            error_data(&error),
        )?;
    }
    ctx.dispatch(RuntimeEvent::ensure_registered())?;
    ctx.dispatch(RuntimeEvent::LiveStart)?;
    ctx.dispatch(RuntimeEvent::PollTick)?;
    ctx.dispatch(RuntimeEvent::Drain)?;
    Ok(())
}

/// Fill the circuit from the local store; `primed` flips even on failure so
/// fetches never hang.
pub async fn prime_circuit(ctx: &Ctx) -> Result<()> {
    let pending: Vec<String> =
        ctx.read(|s| s.outbox.iter().map(|i| i.record_id.clone()).collect())?;
    let logged = match ctx
        .service(|s| async move { s.ssp_prime(pending).await })
        .await
    {
        Ok(()) => Ok(()),
        Err(error) => ctx.log(
            LogLevel::Warn,
            "circuit prime failed; starting empty",
            error_data(&error),
        ),
    };
    ctx.update(|s| {
        r::set_identity(
            s,
            Identity {
                primed: Some(true),
                ..Default::default()
            },
        )
    })?;
    logged
}

pub async fn versions_primed(ctx: &Ctx, entries: Vec<(String, i64)>) -> Result<()> {
    ctx.update(|s| r::set_versions(s, &entries))
}

/// Hand this client's views back to the server when the app goes away for good.
pub async fn app_detached(ctx: &Ctx) -> Result<()> {
    let ids: Vec<String> = ctx.read(|s| {
        s.queries
            .values()
            .filter(|e| e.lifecycle.remote == RemotePhase::Registered)
            .map(|e| e.def.id.to_string())
            .collect()
    })?;
    if !ids.is_empty() {
        ctx.service_sync(|s| s.remote_release_views(ids))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kernel::events::LogLevel;
    use crate::mutation::mutation_id::PENDING_TABLE;
    use crate::mutation::test_support::{
        in_order, new_rig, record_events, service_calls, state_with, test_env,
    };
    use crate::state::lifecycle::{QueryLifecycle, QueryPhase};
    use crate::state::ClientState;
    use crate::testing::{
        build_definition, build_entry, build_outbox_item, results, row, run_local,
    };
    use crate::types::MutationEventType;
    use serde_json::json;

    #[tokio::test]
    async fn opens_the_store_primes_restores_the_session() {
        run_local(async {
            let (rig, _) = new_rig(test_env(), ClientState::empty("tab"));
            *rig.services.hint.borrow_mut() = Some("u1".into());
            *rig.services.restored_user.borrow_mut() = Some("user:u1".into());
            *rig.services.auth_user.borrow_mut() = Some(row(json!({"id": "user:u1"})));
            *rig.services.auth_access.borrow_mut() = Some("account".into());
            rig.run(|ctx| async move { boot(&ctx, &test_env()).await })
                .await
                .unwrap();
            let names = service_calls(&rig);
            assert!(in_order(
                &names,
                &[
                    "hintRead",
                    "localConnect",
                    "migratorProvision",
                    "sspInit",
                    "sspSetPermissions",
                    "authRestoreSession"
                ]
            ));
            let st = rig.state();
            assert_eq!(st.bucket_id.as_deref(), Some("u1"));
            assert_eq!(st.user_id.as_deref(), Some("user:u1"));
            assert!(st.session_id.is_some());
            assert_eq!(st.salt_user_id.as_deref(), Some("user:u1"));
            assert!(st.owner_secret.is_some());
            assert_ne!(st.owner_secret, st.session_id);
            assert!(names.contains(&"sspSetSessionAuth".to_string()));
            assert!(st.local_ready);
            assert!(st.primed);
            // The orphan sweep waits for the first answers, never at boot.
            assert_eq!(rig.timers.delay("gc"), Some(30_000));
        })
        .await;
    }

    #[tokio::test]
    async fn no_hint_and_no_session_is_the_anon_bucket() {
        run_local(async {
            let (rig, _) = new_rig(test_env(), ClientState::empty("tab"));
            rig.run(|ctx| async move { boot(&ctx, &test_env()).await })
                .await
                .unwrap();
            let st = rig.state();
            assert_eq!(st.bucket_id.as_deref(), Some("anon"));
            assert_eq!(st.user_id, None);
            assert!(!service_calls(&rig).contains(&"sspSetSessionAuth".to_string()));
        })
        .await;
    }

    #[tokio::test]
    async fn a_cached_session_boots_from_the_local_store_without_the_network() {
        // `cached_session_test.dart` at the saga level: the hint names the
        // cached bucket, the token restores the user, the queued write is
        // mirrored, and nothing waits on the server.
        run_local(async {
            let (rig, _) = new_rig(test_env(), ClientState::empty("tab"));
            *rig.services.hint.borrow_mut() = Some("cached".into());
            *rig.services.restored_user.borrow_mut() = Some("user:cached".into());
            *rig.services.auth_user.borrow_mut() = Some(row(json!({"id": "user:cached"})));
            rig.local.seed(
                PENDING_TABLE,
                "_00_pending_mutations:m1",
                row(json!({
                    "id": "_00_pending_mutations:m1",
                    "mutationType": "create",
                    "recordId": "thing:pending",
                    "tableName": "thing",
                    "data": {"title": "queued"},
                    "createdAt": 1,
                    "v": 2,
                })),
            );
            rig.run(|ctx| async move { boot(&ctx, &test_env()).await })
                .await
                .unwrap();
            let st = rig.state();
            assert_eq!(st.bucket_id.as_deref(), Some("cached"));
            assert_eq!(st.user_id.as_deref(), Some("user:cached"));
            assert_eq!(st.outbox.len(), 1);
            assert!(st.local_ready);
            assert!(!rig.log.contains("remote.query") && !rig.log.contains("remote.write"));
            assert!(!service_calls(&rig).contains(&"remoteConnect".to_string()));
        })
        .await;
    }

    #[tokio::test]
    async fn the_legacy_window_table_is_copied_into_view_exactly_once() {
        run_local(async {
            let (rig, _) = new_rig(test_env(), ClientState::empty("tab"));
            rig.local.seed(
                LEGACY_VIEW_TABLE,
                "_00_window:k1",
                row(json!({"id": "_00_window:k1", "ids": [["thing:1", 2]], "confirmed": true, "updatedAt": 7})),
            );
            rig.run(|ctx| async move { migrate_window_to_view(&ctx).await }).await.unwrap();
            let puts = rig.log.named("local.put");
            assert_eq!(puts.len(), 1);
            let view = rig.local.row(VIEW_TABLE, &view_record_id("k1")).unwrap();
            assert_eq!(view["confirmed"], Value::Bool(true));
            assert_eq!(view["updatedAt"], Value::Int(7));

            rig.log.clear();
            rig.run(|ctx| async move { migrate_window_to_view(&ctx).await }).await.unwrap();
            assert_eq!(rig.log.count("local.put"), 0);
        })
        .await;
    }

    #[tokio::test]
    async fn a_failing_migration_is_skipped_not_fatal() {
        run_local(async {
            let (rig, faults) = new_rig(test_env(), ClientState::empty("tab"));
            faults.fail_local_get_all();
            let events = record_events(&rig);
            rig.run(|ctx| async move { migrate_window_to_view(&ctx).await })
                .await
                .unwrap();
            let logs = events.logs();
            assert_eq!(logs.len(), 1);
            assert_eq!(logs[0].0, LogLevel::Debug);
        })
        .await;
    }

    #[tokio::test]
    async fn start_remote_is_best_effort_and_still_starts_the_engine() {
        run_local(async {
            let (rig, faults) = new_rig(test_env(), ClientState::empty("tab"));
            faults.fail_service("remoteConnect");
            faults.fail_service("authInit");
            let events = record_events(&rig);
            // A queued write, so the dispatched drain shows up as a push.
            rig.runtime.update(|s| r::outbox_push(s, build_outbox_item("m1", MutationEventType::Create, "thing:1")));
            rig.local.seed(
                PENDING_TABLE,
                "m1",
                row(json!({"id": "m1", "mutationType": "create", "recordId": "thing:1", "data": {"title": "x"}})),
            );
            rig.remote.answer_with(|_, _| Ok(results(vec![Ok(json!(null))])));
            rig.run(|ctx| async move { start_remote(&ctx).await }).await.unwrap();
            assert_eq!(events.logs().len(), 2);
            assert!(service_calls(&rig).contains(&"supervisorStart".to_string()));
            rig.settle().await;
            assert!(rig
                .log
                .named("remote.write")
                .iter()
                .any(|c| c.args.contains("CREATE ONLY")));
        })
        .await;
    }

    #[tokio::test]
    async fn prime_passes_the_pending_ids_and_flips_primed_even_when_it_fails() {
        run_local(async {
            let (ok, _) = new_rig(test_env(), ClientState::empty("tab"));
            ok.runtime.update(|s| {
                r::outbox_replace(
                    s,
                    vec![build_outbox_item(
                        "m1",
                        MutationEventType::Create,
                        "thing:1",
                    )],
                )
            });
            ok.run(|ctx| async move { prime_circuit(&ctx).await })
                .await
                .unwrap();
            assert!(ok.state().primed);
            assert_eq!(ok.log.named("service.sspPrime")[0].args, r#"["thing:1"]"#);

            let (failed, faults) = new_rig(test_env(), ClientState::empty("tab"));
            faults.fail_service("sspPrime");
            let events = record_events(&failed);
            failed
                .run(|ctx| async move { prime_circuit(&ctx).await })
                .await
                .unwrap();
            // A fetch that waits on `primed` must never hang.
            assert!(failed.state().primed);
            let logs = events.logs();
            assert_eq!(logs.len(), 1);
            assert_eq!(logs[0].0, LogLevel::Warn);
        })
        .await;
    }

    #[tokio::test]
    async fn versions_primed_records_versions() {
        run_local(async {
            let (rig, _) = new_rig(test_env(), ClientState::empty("tab"));
            rig.run(|ctx| async move { versions_primed(&ctx, vec![("thing:1".into(), 3)]).await })
                .await
                .unwrap();
            assert_eq!(rig.state().versions.get("thing:1"), Some(&3));
        })
        .await;
    }

    #[tokio::test]
    async fn app_detached_hands_back_only_the_views_the_server_holds() {
        run_local(async {
            let mut registered = build_entry(build_definition("a", "thing"));
            registered.lifecycle = QueryLifecycle {
                phase: QueryPhase::Live,
                remote: RemotePhase::Registered,
                fetch_depth: 0,
                background_depth: 0,
                notified: false,
            };
            let (rig, _) = new_rig(
                test_env(),
                state_with(vec![
                    registered,
                    build_entry(build_definition("b", "thing")),
                ]),
            );
            rig.run(|ctx| async move { app_detached(&ctx).await })
                .await
                .unwrap();
            assert_eq!(service_calls(&rig), ["remoteReleaseViews"]);
            assert_eq!(
                rig.log.named("service.remoteReleaseViews")[0].args,
                r#"["_00_query:a"]"#
            );

            let (none, _) = new_rig(test_env(), ClientState::empty("tab"));
            none.run(|ctx| async move { app_detached(&ctx).await })
                .await
                .unwrap();
            assert!(service_calls(&none).is_empty());
        })
        .await;
    }
}
