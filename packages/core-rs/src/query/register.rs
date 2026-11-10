//! Query registration: local (keys, durable seed, circuit view) and remote
//! (`fn::query::register` plus the membership read-back).
//!
//! PORT: `packages/spooky_core/lib/src/query/register_saga.dart`.

use std::sync::Arc;

use serde_json::json;

use crate::error::{Error, Result};
use crate::kernel::constants::{
    backoff_ms, AUTH_READY_MAX_ATTEMPTS, AUTH_READY_RETRY_MS, MATERIALIZING_REREAD_LADDER_MS,
    REGISTER_MAX_RETRIES,
};
use crate::kernel::events::{LogLevel, RuntimeEvent};
use crate::kernel::ports::RegisterPlan;
use crate::kernel::Ctx;
use crate::modules::query_builder::RelationPlan;
use crate::query::env::{list_ref_table, receives_membership, SagaEnv};
use crate::query::hash::{query_hash_input, view_key_input, QueryKeyInput};
use crate::query::membership::{
    is_resolved_before, parse_view_row, snapshot_from_single, Answer, MembershipOutcome,
};
use crate::query::membership_saga::{
    apply_membership, apply_subquery_children, mark_membership_dirty,
};
use crate::query::sql;
use crate::state::lifecycle::{seed_lifecycle, LifecycleEvent, QueryPhase, RemotePhase};
use crate::state::reducers as r;
use crate::state::selectors::desired_registrations;
use crate::state::{QueryDefinition, QueryEntry, QueryTelemetry};
use crate::types::{parse_ttl_ms, QueryHash, QueryTimeToLive};
use crate::value::{Object, RecordId};

/// What `register_local` needs to know about a query.
#[derive(Clone, Debug, PartialEq)]
pub struct RegisterInput {
    pub table_name: String,
    pub surql: String,
    pub params: Object,
    pub ttl: QueryTimeToLive,
    pub has_explicit_order: bool,
    pub relations: Vec<RelationPlan>,
}

/// Register a query locally: compute its keys, read the durable `_00_view`
/// row, build the circuit's local view and publish the entry. Returns as soon
/// as the entry exists (the first paint comes from `materialize`); the remote
/// registration is dispatched, never awaited.
pub async fn register_local(ctx: &Ctx, env: &SagaEnv, input: RegisterInput) -> Result<QueryHash> {
    let _ = env;
    let key = QueryKeyInput {
        surql: &input.surql,
        params: &input.params,
    };
    let session_id = ctx.read(|s| s.session_id.clone())?;
    let hash = ctx.hash(&query_hash_input(&key, session_id.as_deref()))?;
    let (active, pending) =
        ctx.read(|s| (s.queries.contains_key(&hash), s.registering.contains(&hash)))?;
    if active || pending {
        if !active {
            let h = hash.clone();
            ctx.wait(move |s| !s.registering.contains(&h)).await?;
        }
        // The hash ignores the `.related()` plan (the surql already encodes the
        // shape), so a plan-less first registrant, a preload, would otherwise
        // leave the joins unresolved for every later subscriber.
        if !input.relations.is_empty() {
            let bare = ctx.read(|s| {
                s.queries
                    .get(&hash)
                    .is_some_and(|e| e.def.relations.is_empty())
            })?;
            if bare {
                ctx.update(|s| r::set_relations(s, &hash, input.relations.clone()))?;
            }
        }
        return Ok(hash);
    }
    ctx.update(|s| r::begin_registering(s, &hash))?;
    let out = register_new(ctx, &hash, &input).await;
    // Unfenced: a marker left behind by a fenced-off registration would hang
    // the next registrant of this hash in its wait above.
    ctx.runtime().update(|s| r::end_registering(s, &hash));
    out.map(|()| hash)
}

async fn register_new(ctx: &Ctx, hash: &str, input: &RegisterInput) -> Result<()> {
    let key = QueryKeyInput {
        surql: &input.surql,
        params: &input.params,
    };
    let view_key = ctx.hash(&view_key_input(&key))?;
    // A row the store cannot read is a row we do not have.
    let view_row = ctx
        .local_get(sql::VIEW_TABLE, &sql::view_record_id(&view_key))
        .unwrap_or(None);
    let view = parse_view_row(view_row.as_ref());
    let now = ctx.now()?;
    let reg = ctx.ssp_register(RegisterPlan {
        query_hash: hash.to_string(),
        surql: input.surql.clone(),
        params: input.params.clone(),
        ttl: input.ttl.clone(),
        table_name: input.table_name.clone(),
    })?;
    let entry = QueryEntry {
        def: QueryDefinition {
            id: RecordId::new("_00_query", hash),
            hash: hash.to_string(),
            view_key,
            surql: input.surql.clone(),
            params: input.params.clone(),
            ttl: input.ttl.clone(),
            ttl_ms: parse_ttl_ms(&input.ttl),
            table_name: input.table_name.clone(),
            created_at: now,
            relations: input.relations.clone(),
            has_explicit_order: input.has_explicit_order,
        },
        lifecycle: seed_lifecycle(is_resolved_before(view.as_ref())),
        remote_array: view.map(|v| v.ids).unwrap_or_default(),
        local_array: reg.local_array,
        subquery_remote_array: Vec::new(),
        records: Arc::new(Vec::new()),
        server_state: None,
        subscribers: 0,
        last_subscriber_left_at: Some(now),
        last_heartbeat_at: None,
        last_polled_at: None,
        register_attempts: 0,
        view_lost_count: 0,
        view_lost_retry_at: None,
        telemetry: QueryTelemetry {
            registration_timings: reg.timings,
            ..Default::default()
        },
    };
    ctx.update(|s| r::put_query(s, entry))?;
    ctx.dispatch(RuntimeEvent::ensure_registered())
}

/// Make the remote match the desired set: start a registration for every
/// query that has none. Idempotent. With `require_auth` (after a reconnect or
/// while degraded) it first waits for `$auth.id` to be visible on the socket:
/// a register issued before that stamps the view with an empty identity.
pub async fn ensure_registered(
    ctx: &Ctx,
    env: &SagaEnv,
    require_auth: bool,
    attempt: u32,
) -> Result<()> {
    let (hashes, user_id) = ctx.read(|s| (desired_registrations(s), s.user_id.clone()))?;
    if hashes.is_empty() {
        return Ok(());
    }
    if require_auth && user_id.is_some() {
        let authed = match ctx
            .remote_query("RETURN $auth.id", None, Some(env.remote_timeout_ms))
            .await
        {
            Ok(res) => res
                .first()
                .and_then(|r| r.value())
                .is_some_and(|v| !v.is_nullish()),
            Err(_) => false,
        };
        if !authed {
            let next = attempt + 1;
            if next >= AUTH_READY_MAX_ATTEMPTS {
                return ctx.log(
                    LogLevel::Warn,
                    "auth identity never became visible; not registering",
                    Some(json!({"attempt": next}).into()),
                );
            }
            return ctx.timer_set(
                "ensure-registered",
                AUTH_READY_RETRY_MS,
                RuntimeEvent::EnsureRegistered {
                    require_auth: true,
                    attempt: next,
                },
            );
        }
    }
    for hash in hashes {
        ctx.dispatch(RuntimeEvent::RegisterRemote { hash, retry: false })?;
    }
    Ok(())
}

/// One remote registration: `fn::query::register` plus the edge, meta and
/// children read in ONE request, then membership application. Concurrent with
/// every other registration; retried on its own backoff; stops when the entry
/// is gone.
pub async fn register_remote(ctx: &Ctx, env: &SagaEnv, hash: &str, retry: bool) -> Result<()> {
    let found = ctx.read(|s| {
        s.queries.get(hash).map(|e| {
            let payload = sql::RegisterPayload {
                id: e.def.id.clone(),
                surql: e.def.surql.clone(),
                params: e.def.params.clone(),
                ttl: e.def.ttl.clone(),
                owner: s.owner_secret.clone(),
            };
            (e.lifecycle, payload, list_ref_table(env, s))
        })
    })?;
    let Some((lifecycle, payload, table)) = found else {
        return Ok(());
    };
    match lifecycle.remote {
        RemotePhase::Registered | RemotePhase::Failed => return Ok(()),
        RemotePhase::Registering if !retry => return Ok(()),
        _ => {}
    }
    // A query that already shows rows (cached or live) only re-verifies them
    // here: the cycle holds `fetching` but is not traffic the user waits on.
    let background = lifecycle.phase != QueryPhase::Cold;
    ctx.update(|s| {
        r::apply_lifecycle(s, hash, LifecycleEvent::RemoteRegistering);
        r::apply_lifecycle(s, hash, LifecycleEvent::FetchBegin { background });
    })?;
    let out = match register_round(ctx, env, hash, &table, &payload).await {
        Ok(()) => Ok(()),
        Err(error) => register_failed(ctx, hash, error),
    };
    // finally
    if ctx.read(|s| s.queries.contains_key(hash))? {
        ctx.update(|s| r::apply_lifecycle(s, hash, LifecycleEvent::FetchEnd { background }))?;
    }
    out
}

async fn register_round(
    ctx: &Ctx,
    env: &SagaEnv,
    hash: &str,
    table: &str,
    payload: &sql::RegisterPayload,
) -> Result<()> {
    let results = ctx
        .remote_query(
            &sql::register_select(table),
            Some(sql::register_vars(payload)),
            Some(env.remote_timeout_ms),
        )
        .await?;
    if !ctx.read(|s| s.queries.contains_key(hash))? {
        return Ok(());
    }
    match sql::stmt(&results, 0) {
        Some(register) if register.is_ok() => {}
        other => {
            return Err(Error::Statement(
                other
                    .and_then(|r| r.error())
                    .unwrap_or("register returned nothing")
                    .to_string(),
            ))
        }
    }
    let snap = snapshot_from_single(
        Answer::of(sql::stmt(&results, 1)),
        Answer::of(sql::stmt(&results, 2)),
        Answer::of(sql::stmt(&results, 3)),
    );
    let Some(snap) = snap else {
        // Registered, but the read-back did not answer. That says nothing
        // about the row, so read membership again rather than guess: a guess
        // of "gone" re-registered the query, and under load the next read-back
        // failed the same way.
        ctx.update(|s| {
            r::apply_lifecycle(s, hash, LifecycleEvent::RemoteRegistered);
            r::reset_register_attempts(s, hash);
        })?;
        ctx.log(
            LogLevel::Debug,
            "registration read-back did not answer; re-reading membership",
            Some(json!({"hash": hash}).into()),
        )?;
        mark_membership_dirty(ctx, &[hash.to_string()]).await?;
        return ctx.dispatch(RuntimeEvent::SyncOutcome {
            ok: false,
            error: Some("registration read-back failed".into()),
        });
    };
    let outcome =
        apply_membership(ctx, hash, snap.primary.clone(), Some(&snap.meta), false).await?;
    if outcome == MembershipOutcome::Ignored
        && snap.meta.present
        && snap.meta.state.as_deref() == Some("materializing")
    {
        ctx.update(|s| {
            r::mark_membership_dirty(s, [&hash.to_string()]);
            r::set_membership_reread(s, hash, Some(1));
        })?;
        ctx.timer_set(
            "membership",
            MATERIALIZING_REREAD_LADDER_MS[0],
            RuntimeEvent::ReadDirtyMembership,
        )?;
    }
    apply_subquery_children(ctx, hash, snap.subquery).await?;
    ctx.update(|s| {
        r::apply_lifecycle(s, hash, LifecycleEvent::RemoteRegistered);
        r::reset_register_attempts(s, hash);
    })?;
    // A brand-new account's `_00_list_ref_user_<id>` table only exists once
    // the SSP has filed its first view, so the LIVE subscription made at
    // sign-in failed with "table does not exist" and only the poll covered
    // membership. A registration that just answered means the table is there
    // now: subscribe (a no-op when LIVE is already up).
    if ctx.read(|s| receives_membership(env, s) && s.sync.live_uuid.is_none())? {
        ctx.dispatch(RuntimeEvent::LiveStart)?;
    }
    ctx.dispatch(RuntimeEvent::SyncOutcome {
        ok: true,
        error: None,
    })
}

/// The registration failed: count it, report the round, and either retry on
/// the backoff or give up as `failed`. The phase stays `registering` while a
/// retry is armed: the retry passes `retry: true` to get past the in-flight
/// guard.
fn register_failed(ctx: &Ctx, hash: &str, error: Error) -> Result<()> {
    let Some(previous) = ctx.read(|s| s.queries.get(hash).map(|e| e.register_attempts))? else {
        return Ok(());
    };
    let attempts = previous + 1;
    ctx.update(|s| r::bump_register_attempts(s, hash))?;
    let message = error.to_string();
    ctx.log(
        LogLevel::Warn,
        "remote registration failed",
        Some(json!({"hash": hash, "attempts": attempts, "error": message}).into()),
    )?;
    ctx.dispatch(RuntimeEvent::SyncOutcome {
        ok: false,
        error: Some(message),
    })?;
    if attempts >= REGISTER_MAX_RETRIES {
        ctx.update(|s| r::apply_lifecycle(s, hash, LifecycleEvent::RemoteFailed))
    } else {
        ctx.timer_set(
            format!("register:{hash}"),
            backoff_ms(attempts),
            RuntimeEvent::RegisterRemote {
                hash: hash.to_string(),
                retry: true,
            },
        )
    }
}

#[cfg(test)]
mod tests;
