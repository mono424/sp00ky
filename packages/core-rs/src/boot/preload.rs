//! Preload: a registered query nobody subscribes to.
//!
//! PORT: `packages/spooky_core/lib/src/boot/preload_saga.dart`.

use crate::error::{Error, Result};
use crate::kernel::Ctx;
use crate::query::env::{receives_membership, SagaEnv};
use crate::query::register::{register_local, RegisterInput};
use crate::state::lifecycle::QueryPhase;
use crate::state::selectors::{settle_failed, settled};
use crate::types::QueryHash;

/// Preload = a registered query nobody subscribes to.
///
/// Resolved before on this device: returns as soon as the entry exists (its
/// rows paint from cache). Never resolved: blocks until the server's membership
/// and every body are local and the first materialization ran. The entry is
/// evicted like any other query a ttl after it was last watched (or
/// registered). Returns the hash and whether it waited; a registration that
/// fails to settle is [`Error::PreloadFailed`] (Dart `PreloadFailedError`).
///
/// A session that cannot be sent membership (signed out, without anonymous
/// live queries) does not wait either: a view with rows would never settle,
/// and an app that gates its first paint on a preload stayed on its loading
/// screen forever. Nor does an offline device: the membership it waits for
/// cannot arrive before the network does, and an app started offline before
/// its preload ever settled stayed on its loading screen. It paints from what
/// is local instead, and the query fills in once the registration lands. The
/// core cannot see connectivity, so offline means the host said so through
/// `Sp00kyClient::set_offline`.
pub async fn preload(ctx: &Ctx, env: &SagaEnv, input: RegisterInput) -> Result<(QueryHash, bool)> {
    let hash = register_local(ctx, env, input).await?;
    let phase = ctx.read(|s| s.queries.get(&hash).map(|e| e.lifecycle.phase))?;
    let reachable = ctx.read(|s| receives_membership(env, s))?;
    if phase != Some(QueryPhase::Cold) || !reachable {
        return Ok((hash, false));
    }
    if ctx.service_sync(|s| s.net_offline())? {
        return Ok((hash, false));
    }
    let gate = hash.clone();
    ctx.wait(move |s| settled(s, &gate) || settle_failed(s, &gate))
        .await?;
    if ctx.read(|s| settle_failed(s, &hash))? {
        return Err(Error::PreloadFailed { hash });
    }
    Ok((hash, true))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mutation::test_support::{new_rig, test_env, test_schema};
    use crate::query::hash::{view_key_input, QueryKeyInput};
    use crate::query::sql::{view_record_id, VIEW_TABLE};
    use crate::state::lifecycle::LifecycleEvent;
    use crate::state::reducers as r;
    use crate::state::ClientState;
    use crate::testing::{row, run_local};
    use crate::value::{sha256_hex, Object};
    use serde_json::json;
    use std::sync::Arc;

    fn input() -> RegisterInput {
        RegisterInput {
            table_name: "thing".into(),
            surql: "SELECT * FROM thing".into(),
            params: Object::new(),
            ttl: "10m".into(),
            has_explicit_order: false,
            relations: Vec::new(),
        }
    }

    fn signed_in() -> ClientState {
        let mut s = ClientState::empty("tab");
        s.user_id = Some("user:alice".into());
        s
    }

    #[tokio::test]
    #[ignore = "needs register_local"]
    async fn a_query_resolved_before_returns_as_soon_as_the_entry_exists() {
        run_local(async {
            let (rig, _) = new_rig(test_env(), signed_in());
            let params = Object::new();
            let key = sha256_hex(&view_key_input(&QueryKeyInput {
                surql: "SELECT * FROM thing",
                params: &params,
            }));
            rig.local.seed(
                VIEW_TABLE,
                &view_record_id(&key),
                row(json!({"ids": [], "confirmed": true})),
            );
            let (_, waited) = rig
                .run(|ctx| async move { preload(&ctx, &test_env(), input()).await })
                .await
                .unwrap();
            assert!(!waited);
        })
        .await;
    }

    #[tokio::test]
    #[ignore = "needs register_local"]
    async fn a_cold_query_blocks_on_the_settle_gate() {
        run_local(async {
            let anon_live = SagaEnv {
                anon_live: true,
                ..SagaEnv::new(Arc::new(test_schema()))
            };
            for (env, state) in [
                (test_env(), signed_in()),
                (anon_live, ClientState::empty("tab")),
            ] {
                let (rig, _) = new_rig(env.clone(), state);
                let rt = rig.runtime.clone();
                let task = tokio::task::spawn_local(async move {
                    rt.run_unfenced(
                        None,
                        |ctx| async move { preload(&ctx, &env, input()).await },
                    )
                    .await
                });
                rig.settle().await;
                assert!(
                    !task.is_finished(),
                    "a cold preload waits for the settle gate"
                );
                let hash = rig.state().queries.keys().next().cloned().unwrap();
                rig.runtime
                    .update(|s| r::apply_lifecycle(s, &hash, LifecycleEvent::RemoteFailed));
                let out = task.await.unwrap();
                assert!(matches!(out, Err(Error::PreloadFailed { hash: h }) if h == hash));
            }
        })
        .await;
    }

    #[tokio::test]
    #[ignore = "needs register_local"]
    async fn signed_out_without_anonymous_live_queries_never_waits() {
        // No membership can reach this session.
        run_local(async {
            let (rig, _) = new_rig(test_env(), ClientState::empty("tab"));
            let (_, waited) = rig
                .run(|ctx| async move { preload(&ctx, &test_env(), input()).await })
                .await
                .unwrap();
            assert!(!waited);
        })
        .await;
    }

    #[tokio::test]
    #[ignore = "needs register_local"]
    async fn offline_registers_but_never_waits() {
        // A launch without a network paints from what is local.
        run_local(async {
            let (rig, _) = new_rig(test_env(), signed_in());
            rig.services.offline.set(true);
            let (_, waited) = rig
                .run(|ctx| async move { preload(&ctx, &test_env(), input()).await })
                .await
                .unwrap();
            assert!(!waited);
            assert_eq!(rig.log.count("ssp.register"), 1);
        })
        .await;
    }
}
