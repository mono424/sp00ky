//! Operator surface for machine pools.
//!
//! Reads come straight from `_00_pool` / `_00_machine`; the two writes are the
//! operator-owned fields and nothing else (`paused` on a pool, and asking one
//! machine to drain), so they cannot collide with deploy or with the engine.
//! Everything that actually moves a machine is left to the pool sweep: an
//! operator states an intent here and the next pass carries it out, the same way
//! `schedule pause` works.

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::{Extension, Json};
use serde_json::{json, Value};

use super::{api_error, db_unavailable, esc, rows, AdminState, ApiError, CurrentSession};

/// `GET /admin/api/pools`: every pool with its spec, its live machines by state
/// and how many jobs are waiting for one.
pub async fn list_pools(State(state): State<AdminState>) -> Result<Json<Value>, ApiError> {
    let db = state.db().ok_or_else(db_unavailable)?;
    let pools = rows(
        &db,
        "SELECT name, provider, machine_type, slots, min, max, buffer, autoscale, paused, \
         lease_secs, idle_timeout_secs, backend, target_table, boot_failures, last_error, \
         (breaker_until != NONE AND breaker_until > time::now()) AS breaker_open \
         FROM _00_pool ORDER BY name ASC;",
    )
    .await?;
    let machines = rows(
        &db,
        "SELECT pool, state, count() AS n, math::sum(busy_slots) AS busy FROM _00_machine \
         WHERE state NOT IN ['gone', 'failed'] GROUP BY pool, state;",
    )
    .await?;

    let mut out = Vec::with_capacity(pools.len());
    for mut pool in pools {
        let name = pool
            .get("name")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        let mut by_state = serde_json::Map::new();
        let mut busy = 0;
        for m in machines
            .iter()
            .filter(|m| m.get("pool").and_then(Value::as_str) == Some(&name))
        {
            if let Some(s) = m.get("state").and_then(Value::as_str) {
                by_state.insert(s.to_string(), m.get("n").cloned().unwrap_or(json!(0)));
            }
            busy += m.get("busy").and_then(Value::as_i64).unwrap_or(0);
        }
        // The table name was validated by deploy and again by the engine; it is
        // still only interpolated when it is a plain identifier.
        let queued = match pool.get("target_table").and_then(Value::as_str) {
            Some(t) if schedule_core::sql::is_plain_identifier(t) => rows(
                &db,
                &format!("SELECT count() AS n FROM {t} WHERE status = 'pending' GROUP ALL;"),
            )
            .await
            .ok()
            .and_then(|r| r.first().and_then(|r| r.get("n").and_then(Value::as_i64)))
            .unwrap_or(0),
            _ => 0,
        };
        pool["machines"] = Value::Object(by_state);
        pool["busy_slots"] = json!(busy);
        pool["queued_jobs"] = json!(queued);
        out.push(pool);
    }
    Ok(Json(json!({ "pools": out })))
}

/// `GET /admin/api/pools/:name/machines`: that pool's machines, newest first,
/// terminal ones included (they carry the `reason` an operator is looking for).
pub async fn list_machines(
    State(state): State<AdminState>,
    Path(name): Path<String>,
) -> Result<Json<Value>, ApiError> {
    let db = state.db().ok_or_else(db_unavailable)?;
    let machines = rows(
        &db,
        &format!(
            "SELECT id, state, provider, provider_id, slots, busy_slots, spec_hash, created_at, \
             ready_at, last_seen, idle_since, ended_at, reason, failure, agent FROM _00_machine \
             WHERE pool = '{}' ORDER BY created_at DESC LIMIT 200;",
            esc(&name)
        ),
    )
    .await?;
    Ok(Json(json!({ "pool": name, "machines": machines })))
}

async fn set_paused(
    state: &AdminState,
    session: &CurrentSession,
    name: &str,
    paused: bool,
) -> Result<Json<Value>, ApiError> {
    let db = state.db().ok_or_else(db_unavailable)?;
    let updated = rows(
        &db,
        &format!(
            "UPDATE _00_pool SET paused = {paused} WHERE name = '{}' RETURN name;",
            esc(name)
        ),
    )
    .await?;
    if updated.is_empty() {
        return Err(api_error(
            StatusCode::NOT_FOUND,
            format!("No pool named '{name}'"),
        ));
    }
    tracing::info!(pool = %name, paused, by = %session.0.subject, "Pool pause state changed from the dashboard");
    Ok(Json(json!({ "name": name, "paused": paused })))
}

/// `POST /admin/api/pools/:name/pause`: stop assigning jobs and stop creating
/// machines. Jobs already running finish.
pub async fn pool_pause(
    State(state): State<AdminState>,
    Extension(session): Extension<CurrentSession>,
    Path(name): Path<String>,
) -> Result<Json<Value>, ApiError> {
    set_paused(&state, &session, &name, true).await
}

/// `POST /admin/api/pools/:name/resume`
pub async fn pool_resume(
    State(state): State<AdminState>,
    Extension(session): Extension<CurrentSession>,
    Path(name): Path<String>,
) -> Result<Json<Value>, ApiError> {
    set_paused(&state, &session, &name, false).await
}

/// `POST /admin/api/machines/:id/drain`: take no new jobs, finish what is
/// running, then be destroyed. The pool replaces it if it still needs the capacity.
pub async fn machine_drain(
    State(state): State<AdminState>,
    Extension(session): Extension<CurrentSession>,
    Path(id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    let db = state.db().ok_or_else(db_unavailable)?;
    let key = id.strip_prefix("_00_machine:").unwrap_or(&id);
    if key.is_empty() || !key.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
        return Err(api_error(StatusCode::BAD_REQUEST, "Not a machine id"));
    }
    let moved = rows(
        &db,
        &format!("UPDATE _00_machine:{key} SET state = 'draining' WHERE state = 'ready' RETURN id, state;"),
    )
    .await?;
    if moved.is_empty() {
        return Err(api_error(
            StatusCode::CONFLICT,
            "Only a ready machine can be drained (it may already be draining or gone)",
        ));
    }
    tracing::info!(machine = %id, by = %session.0.subject, "Machine drain requested from the dashboard");
    Ok(Json(
        json!({ "id": format!("_00_machine:{key}"), "state": "draining" }),
    ))
}
