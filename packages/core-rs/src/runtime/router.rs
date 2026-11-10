//! One table: which saga handles an event, and on which lane.

use futures::future::{FutureExt, LocalBoxFuture};

use crate::boot::{auth_flip, boot as boot_saga, bucket_switch};
use crate::error::Result;
use crate::kernel::events::RuntimeEvent;
use crate::kernel::lanes::Lane;
use crate::kernel::Ctx;
use crate::mutation::{push, write};
use crate::query::env::SagaEnv;
use crate::query::{fetch, lifecycle, materialize, membership_saga, register};
use crate::sync::{connection, live, poll};

/// A routed saga, ready to run.
pub type SagaFn = Box<dyn FnOnce(Ctx) -> LocalBoxFuture<'static, Result<()>>>;

pub struct Route {
    pub saga: SagaFn,
    pub lane: Option<Lane>,
}

fn route_to<F, Fut>(lane: Option<Lane>, f: F) -> Route
where
    F: FnOnce(Ctx) -> Fut + 'static,
    Fut: std::future::Future<Output = Result<()>> + 'static,
{
    Route {
        saga: Box::new(move |ctx| f(ctx).boxed_local()),
        lane,
    }
}

async fn materialize_dirty(ctx: Ctx) -> Result<()> {
    let dirty: Vec<String> = ctx.read(|s| s.dirty.iter().cloned().collect())?;
    for hash in dirty {
        ctx.dispatch(RuntimeEvent::Materialize { hash })?;
    }
    Ok(())
}

pub fn route(env: &SagaEnv, event: RuntimeEvent) -> Route {
    use RuntimeEvent as E;
    let env = env.clone();
    match event {
        E::EnsureRegistered { require_auth, attempt } => route_to(Some(Lane::serial("ensure")), move |ctx| async move {
            register::ensure_registered(&ctx, &env, require_auth, attempt).await
        }),
        E::RegisterRemote { hash, retry } => {
            route_to(Some(Lane::dedupe(format!("register:{hash}"))), move |ctx| async move {
                register::register_remote(&ctx, &env, &hash, retry).await
            })
        }
        E::RecoverLostView { hash } => route_to(Some(Lane::dedupe(format!("view-lost:{hash}"))), move |ctx| async move {
            membership_saga::recover_lost_view(&ctx, &hash).await
        }),
        E::ReadDirtyMembership => route_to(Some(Lane::serial("membership")), move |ctx| async move {
            membership_saga::read_dirty_membership(&ctx, &env).await
        }),
        E::ReadMembership { hashes } => route_to(Some(Lane::serial("membership")), move |ctx| async move {
            membership_saga::read_membership(&ctx, &env, &hashes, true).await.map(|_| ())
        }),
        E::FetchRows => route_to(Some(Lane::serial("fetch")), move |ctx| async move { fetch::fetch_rows(&ctx, &env).await }),
        E::Materialize { hash } => route_to(Some(Lane::serial(format!("mat:{hash}"))), move |ctx| async move {
            materialize::materialize(&ctx, &hash).await
        }),
        E::MaterializeDirty => route_to(None, materialize_dirty),
        E::StreamUpdate { update } => {
            route_to(Some(Lane::serial(format!("stream:{}", update.query_hash))), move |ctx| async move {
                materialize::stream_update(&ctx, *update).await
            })
        }
        E::LifecycleTick | E::HeartbeatNow => route_to(Some(Lane::dedupe("lifecycle")), move |ctx| async move {
            lifecycle::lifecycle_tick(&ctx, &env).await
        }),
        E::AckPrune => route_to(Some(Lane::dedupe("ack-prune")), |ctx| async move { lifecycle::ack_prune(&ctx).await }),
        E::GcTick => route_to(Some(Lane::dedupe("gc")), |ctx| async move { lifecycle::gc_tick(&ctx).await }),
        E::Drain => route_to(Some(Lane::serial("outbox")), move |ctx| async move { push::drain(&ctx, &env).await }),
        E::FlushWrite { key } => route_to(Some(Lane::serial("outbox-write")), move |ctx| async move {
            write::flush_write(&ctx, &env, &key).await
        }),
        E::PeerWriteExpired { id } => route_to(Some(Lane::serial(format!("peer:{id}"))), move |ctx| async move {
            write::peer_write_expired(&ctx, &id).await
        }),
        E::PollTick => route_to(Some(Lane::dedupe("poll")), move |ctx| async move { poll::poll_tick(&ctx, &env).await }),
        E::SelfHealTick => route_to(Some(Lane::dedupe("heal")), move |ctx| async move {
            connection::self_heal_tick(&ctx, &env).await
        }),
        E::SyncOutcome { ok, error } => route_to(Some(Lane::serial("health")), move |ctx| async move {
            connection::sync_outcome(&ctx, &env, ok, error).await
        }),
        E::ConnectionChanged { state } => route_to(Some(Lane::serial("connection")), move |ctx| async move {
            connection::connection_changed(&ctx, &env, state).await
        }),
        E::LiveStart => route_to(Some(Lane::serial("live")), move |ctx| async move { live::live_start(&ctx, &env).await }),
        E::LiveChange { hashes, rows } => route_to(Some(Lane::serial("live-change")), move |ctx| async move {
            live::live_change(&ctx, &env, hashes, rows).await
        }),
        E::StartRemote => route_to(Some(Lane::dedupe("start-remote")), |ctx| async move { boot_saga::start_remote(&ctx).await }),
        E::PrimeCircuit => route_to(Some(Lane::dedupe("prime")), |ctx| async move { boot_saga::prime_circuit(&ctx).await }),
        E::VersionsPrimed { entries } => route_to(None, move |ctx| async move { boot_saga::versions_primed(&ctx, entries).await }),
        E::AuthFlip { user_id } => route_to(Some(Lane::serial("bucket")), move |ctx| async move {
            auth_flip::auth_flip(&ctx, &env, user_id).await
        }),
        E::BucketSwitch { target } => route_to(Some(Lane::serial("bucket")), move |ctx| async move {
            bucket_switch::bucket_switch(&ctx, &env, &target).await
        }),
        E::AppDetached => route_to(None, |ctx| async move { boot_saga::app_detached(&ctx).await }),
    }
}
