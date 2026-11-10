//! The signed-in principal changed.
//!
//! PORT: `packages/spooky_core/lib/src/boot/auth_flip_saga.dart`.

use crate::boot::bucket_switch::bucket_switch;
use crate::error::Result;
use crate::kernel::events::LogLevel;
use crate::kernel::ports::{IdScope, IngestOp, IngestRecord, LocalOp};
use crate::kernel::Ctx;
use crate::modules::ref_tables::bucket_id_for_user;
use crate::mutation::rows::log_data;
use crate::query::env::SagaEnv;
use crate::state::reducers::{self as r, Identity};
use crate::utils::clean_record;
use crate::value::{table_of, Value};

/// The signed-in principal changed (sign-in, sign-out, boot verification).
/// Identity first (routing and `$auth` for the local SSP), then the bucket,
/// then the salt if the principal really changed, then the verified user row.
pub async fn auth_flip(ctx: &Ctx, env: &SagaEnv, user_id: Option<String>) -> Result<()> {
    let auth_id = ctx.service_sync(|s| s.auth_session_auth_id())?;
    let access = ctx.service_sync(|s| s.auth_access())?;
    let target = bucket_id_for_user(user_id.as_deref());
    ctx.update(|s| {
        r::set_identity(
            s,
            Identity {
                user_id: Some(user_id),
                ..Default::default()
            },
        )
    })?;
    ctx.service_sync(|s| s.ssp_set_session_auth(auth_id.as_deref(), access.as_deref()))?;
    {
        let target = target.clone();
        ctx.service(|s| async move { s.hint_write(&target).await })
            .await?;
    }
    ctx.update(|s| {
        r::set_identity(
            s,
            Identity {
                pending_bucket: Some(Some(target.clone())),
                ..Default::default()
            },
        )
    })?;
    bucket_switch(ctx, env, &target).await?;
    let salt_user_id = ctx.read(|s| s.salt_user_id.clone())?;
    if auth_id != salt_user_id {
        let salt = ctx.id(IdScope::Salt)?;
        ctx.update(|s| {
            r::set_identity(
                s,
                Identity {
                    session_id: Some(Some(salt.clone())),
                    salt_user_id: Some(auth_id),
                    ..Default::default()
                },
            )
        })?;
        ctx.service_sync(|s| s.crdt_set_session_id(&salt))?;
    }
    persist_verified_user(ctx, env).await
}

/// Write the user's own row, as the server just returned it, into the local
/// store so `user` queries see `email_verified` and friends without waiting for
/// membership. A field refresh, not a version claim.
pub async fn persist_verified_user(ctx: &Ctx, env: &SagaEnv) -> Result<()> {
    let Some(row) = ctx.service_sync(|s| s.auth_current_user())? else {
        return Ok(());
    };
    let Some(id) = row
        .get("id")
        .filter(|v| !v.is_nullish())
        .map(Value::to_id_string)
    else {
        return Ok(());
    };
    if row.len() <= 1 {
        return Ok(());
    }
    let table = table_of(&id).to_string();
    let Some(columns) = env.columns(&table) else {
        return Ok(());
    };
    let version = ctx.read(|s| s.versions.get(&id).copied())?.unwrap_or(1);
    let mut body = if columns.is_empty() {
        row
    } else {
        clean_record(columns, &row)
    };
    body.insert("id".into(), id.clone().into());
    body.insert("_00_rv".into(), Value::Int(version));
    let persisted = ctx
        .local_tx(
            vec![LocalOp::merge(
                table.clone(),
                id.clone(),
                body.clone(),
                Vec::new(),
            )],
            None,
        )
        .and_then(|()| {
            ctx.ssp_ingest(vec![IngestRecord::new(
                table.clone(),
                IngestOp::Update,
                id.clone(),
                body,
            )])
        })
        .and_then(|()| {
            ctx.update(|s| {
                r::set_versions(s, &[(id.clone(), version)]);
                r::mark_table_dirty(s, &table);
            })
        });
    if let Err(error) = persisted {
        ctx.log(
            LogLevel::Warn,
            "could not persist the verified user row",
            log_data(vec![("error", error.to_string().into())]),
        )?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mutation::test_support::{
        in_order, new_rig, service_calls, state_with, test_env, test_schema,
    };
    use crate::state::ClientState;
    use crate::testing::{build_definition, build_entry, row, run_local};
    use crate::types::ColumnSchema;
    use serde_json::json;
    use std::sync::Arc;

    #[tokio::test]
    async fn sets_identity_and_auth_first_then_switches_bucket_and_rotates_salt() {
        run_local(async {
            let (rig, _) = new_rig(test_env(), ClientState::empty("tab"));
            *rig.services.auth_user.borrow_mut() = Some(row(json!({"id": "user:u1"})));
            *rig.services.auth_access.borrow_mut() = Some("account".into());
            rig.run(
                |ctx| async move { auth_flip(&ctx, &test_env(), Some("user:u1".into())).await },
            )
            .await
            .unwrap();
            assert!(in_order(
                &service_calls(&rig),
                &[
                    "sspSetSessionAuth",
                    "hintWrite",
                    "localSwitchStore",
                    "crdtSetSessionId"
                ]
            ));
            let st = rig.state();
            assert_eq!(st.user_id.as_deref(), Some("user:u1"));
            assert_eq!(st.bucket_id.as_deref(), Some("u1"));
            assert_eq!(st.salt_user_id.as_deref(), Some("user:u1"));
            assert!(st.session_id.is_some());
        })
        .await;
    }

    #[tokio::test]
    async fn the_same_principal_on_the_same_bucket_switches_nothing() {
        run_local(async {
            let mut state = ClientState::empty("tab");
            state.salt_user_id = Some("user:u1".into());
            let (rig, _) = new_rig(test_env(), state);
            *rig.services.auth_user.borrow_mut() = Some(row(json!({"id": "user:u1"})));
            *rig.services.bucket.borrow_mut() = "u1".into();
            rig.run(
                |ctx| async move { auth_flip(&ctx, &test_env(), Some("user:u1".into())).await },
            )
            .await
            .unwrap();
            let names = service_calls(&rig);
            assert!(!names.contains(&"localSwitchStore".to_string()));
            assert!(!names.contains(&"crdtSetSessionId".to_string()));
            assert_eq!(rig.state().session_id, None);
        })
        .await;
    }

    /// `session_transition_test.dart` at the saga level: every account keeps
    /// its own store, queries rebind across switches, and each account's
    /// queued writes come back with it.
    #[tokio::test]
    async fn account_changes_rebind_queries_and_preserve_each_outbox() {
        run_local(async {
            // No endpoint: the writes stay queued, as with the Dart test's dead
            // sync socket.
            let env = SagaEnv {
                has_remote: false,
                ..test_env()
            };
            let (rig, faults) = new_rig(env.clone(), ClientState::empty("tab"));
            faults.bucket_stores();
            let boot_env = env.clone();
            rig.run(|ctx| async move { crate::boot::boot::boot(&ctx, &boot_env).await })
                .await
                .unwrap();
            let flip = |name: Option<&str>| {
                let user = name.map(|n| format!("user:{n}"));
                *rig.services.auth_user.borrow_mut() =
                    user.as_ref().map(|id| row(json!({ "id": id })));
                let env = env.clone();
                let rig = &rig;
                async move {
                    rig.run(|ctx| async move { auth_flip(&ctx, &env, user).await })
                        .await
                        .unwrap();
                    rig.state().bucket_id
                }
            };
            let write = |id: &'static str, title: &'static str| {
                let env = env.clone();
                let rig = &rig;
                async move {
                    let input = crate::mutation::write::WriteInput {
                        kind: crate::types::MutationEventType::Create,
                        record_id: id.into(),
                        data: Some(row(json!({ "title": title }))),
                        options: None,
                    };
                    rig.run(
                        |ctx| async move { crate::mutation::write::write(&ctx, &env, input).await },
                    )
                    .await
                    .unwrap();
                }
            };

            assert_eq!(flip(Some("alice")).await.as_deref(), Some("alice"));
            rig.runtime.update(|s| {
                crate::state::reducers::put_query(s, build_entry(build_definition("q", "thing")))
            });
            write("thing:a", "Alice").await;
            rig.runtime.update(|s| {
                crate::state::reducers::set_records(
                    s,
                    "q",
                    Arc::new(vec![row(json!({"id": "thing:a", "title": "Alice"}))]),
                    true,
                    None,
                )
            });

            assert_eq!(flip(Some("bob")).await.as_deref(), Some("bob"));
            assert!(rig.local.row("thing", "thing:a").is_none());
            assert!(rig.state().queries["q"].records.is_empty());
            assert!(rig.state().outbox.is_empty());
            write("thing:b", "Bob").await;

            assert_eq!(flip(Some("alice")).await.as_deref(), Some("alice"));
            assert_eq!(
                rig.local.row("thing", "thing:a").unwrap()["title"],
                Value::from("Alice")
            );
            assert!(rig.local.row("thing", "thing:b").is_none());
            let st = rig.state();
            assert_eq!(crate::state::selectors::pending_mutation_count(&st), 1);
            assert_eq!(st.outbox[0].record_id, "thing:a");

            assert_eq!(flip(None).await.as_deref(), Some("anon"));
        })
        .await;
    }

    fn user_env() -> SagaEnv {
        let schema = test_schema().with_table("user", Vec::<(String, ColumnSchema)>::new());
        SagaEnv::new(Arc::new(schema))
    }

    #[tokio::test]
    async fn persist_writes_the_server_row_and_dirties_its_table() {
        run_local(async {
            let (rig, _) = new_rig(
                user_env(),
                state_with(vec![build_entry(build_definition("a", "user"))]),
            );
            *rig.services.auth_user.borrow_mut() =
                Some(row(json!({"id": "user:u1", "email_verified": true})));
            rig.run(|ctx| async move { persist_verified_user(&ctx, &user_env()).await })
                .await
                .unwrap();
            assert_eq!(rig.log.count("local.tx"), 1);
            let stored = rig.local.row("user", "user:u1").unwrap();
            assert_eq!(stored["email_verified"], Value::Bool(true));
            let st = rig.state();
            assert_eq!(st.versions.get("user:u1"), Some(&1));
            assert!(st.dirty.contains("a"));
        })
        .await;
    }

    #[tokio::test]
    async fn persist_skips_a_bare_id_a_missing_row_or_an_unknown_table() {
        run_local(async {
            for user in [
                None,
                Some(json!({"id": "user:u1"})),
                Some(json!({"id": "ghost:1", "x": 1})),
            ] {
                let (rig, _) = new_rig(test_env(), ClientState::empty("tab"));
                *rig.services.auth_user.borrow_mut() = user.clone().map(row);
                rig.run(|ctx| async move { persist_verified_user(&ctx, &test_env()).await })
                    .await
                    .unwrap();
                assert_eq!(rig.log.count("local.tx"), 0, "{user:?}");
            }
        })
        .await;
    }
}
