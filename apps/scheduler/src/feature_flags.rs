//! Feature flag evaluator: periodic sweep that materializes per-user
//! assignments into `_00_user_feature` from the definitions in
//! `_00_feature_flag`.
//!
//! The CLI evaluates inline on every `spky flag ...` write and materializes
//! every existing user, so this sweep only fills in **users who signed up
//! since the last write**: each tick it skips any (user, flag) pair that
//! already has a row and evaluates the rest. Steady-state (no new users) it
//! writes nothing. It also self-heals if a CLI run was interrupted.
//!
//! The bucketing must match `apps/cli/src/flag.rs::rollout_hash`. Both take
//! SHA-256("<key>:<user_id>"), read the first 8 hex chars as a big-endian
//! u32, then modulo 100, so a CLI write and a scheduler sweep produce
//! identical assignments for the same inputs.

use anyhow::{Context, Result};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;
use surrealdb::engine::remote::http::Client;
use surrealdb::Surreal;
use tracing::{debug, error, info, warn};

const RULE_ALLOWLIST: &str = "allowlist";
const RULE_ROLLOUT: &str = "rollout";

/// Spawn a periodic task that fills in feature-flag assignments for users
/// who don't yet have them. Idempotent: each tick skips (user, flag) pairs
/// that already have a `_00_user_feature` row, so it only writes for new
/// signups.
///
/// Default interval is 30 seconds: a newly-signed-up user gets their flag
/// assignments within half a minute, and a quiet table costs one cheap
/// SELECT-per-flag per tick.
pub fn spawn(db: Arc<maintenance::db::ReconnectingDb>, interval_secs: u64) {
    crate::handover::spawn_singleton("feature-flag-sweep", async move {
        let mut interval = tokio::time::interval(Duration::from_secs(interval_secs));
        interval.tick().await; // skip the immediate first tick
        loop {
            interval.tick().await;
            // Re-read the handle every tick: it is replaced wholesale when
            // SurrealDB restarts and invalidates its session.
            if let Err(err) = tick(&db.handle()).await {
                db.note_error(&format!("{err:#}"));
                warn!(error = %err, "Feature flag sweep failed; will retry on next tick");
            }
        }
    });
}

/// One sweep: for every flag, materialize a `_00_user_feature` row for any
/// user that doesn't already have one. Existing rows are left untouched —
/// the CLI keeps them current on every write.
pub async fn tick(db: &Surreal<Client>) -> Result<()> {
    let flags: Vec<Value> = db
        .query("SELECT key, enabled, default_variant, variants, rules, payloads FROM _00_feature_flag;")
        .await
        .context("Failed to query _00_feature_flag")?
        .take(0)
        .unwrap_or_default();

    if flags.is_empty() {
        return Ok(());
    }

    let user_rows: Vec<Value> = db
        .query("SELECT VALUE <string>id FROM user;")
        .await
        .context("Failed to enumerate users")?
        .take(0)
        .unwrap_or_default();

    let users: Vec<String> = user_rows
        .into_iter()
        .filter_map(|v| v.as_str().map(|s| s.to_string()))
        .collect();

    // Stamp the root-enumerated user count so `fn::feature::materialize` can
    // detect a permission-truncated `SELECT VALUE id FROM user`. That function
    // runs under the CALLER's permissions (a SurrealDB custom function is not
    // security-definer), so an admin flipping a flag from the DevTools panel
    // in an app whose `user` table hides rows would silently materialize only
    // themselves. This sweep is the only place that sees the true count.
    //
    // Written after the `flags.is_empty()` early return, so a project with no
    // flags pays nothing: materialize is only ever called for a flag that
    // exists, and then the stamp is at most one sweep old.
    if let Err(err) = db
        .query("UPSERT _00_module_state:feature_user_count SET count = $count;")
        .bind(("count", users.len() as i64))
        .await
    {
        // Non-fatal: the guard is a safety net, not a precondition for the
        // sweep's own work.
        warn!(error = %err, "Failed to stamp the feature-flag user count");
    }

    if users.is_empty() {
        return Ok(());
    }

    let mut total = 0usize;
    for flag in &flags {
        let key = match flag.get("key").and_then(Value::as_str) {
            Some(k) => k.to_string(),
            None => continue,
        };

        // Users already materialized for this flag. The CLI writes every
        // existing user on each `spky flag` mutation, so this set covers
        // everyone except brand-new signups.
        let assigned_rows: Vec<Value> = db
            .query("SELECT VALUE <string>user FROM _00_user_feature WHERE key = $key;")
            .bind(("key", key.clone()))
            .await
            .with_context(|| format!("Failed to load assignments for flag '{}'", key))?
            .take(0)
            .unwrap_or_default();
        let assigned: HashSet<String> = assigned_rows
            .into_iter()
            .filter_map(|v| v.as_str().map(|s| s.to_string()))
            .collect();

        for user_id in &users {
            if assigned.contains(user_id) {
                continue;
            }
            let (variant, payload) = evaluate_one(flag, user_id);

            // `user_id` is a DB-sourced record id (e.g. `user:abc`) and is
            // interpolated as a record reference, mirroring how replica.rs
            // handles thing ids. Every value is bound, never interpolated.
            let result = match payload {
                Some(payload) => {
                    db.query(format!(
                        "UPSERT _00_user_feature SET user = {uid}, key = $key, variant = $variant, payload = $payload \
                         WHERE user = {uid} AND key = $key;",
                        uid = user_id
                    ))
                    .bind(("key", key.clone()))
                    .bind(("variant", variant))
                    .bind(("payload", payload))
                    .await
                }
                None => {
                    db.query(format!(
                        "UPSERT _00_user_feature SET user = {uid}, key = $key, variant = $variant \
                         WHERE user = {uid} AND key = $key;",
                        uid = user_id
                    ))
                    .bind(("key", key.clone()))
                    .bind(("variant", variant))
                    .await
                }
            };

            match result {
                Ok(_) => total += 1,
                Err(err) => {
                    error!(
                        error = %err,
                        flag = %key,
                        user = %user_id,
                        "Failed to upsert feature flag assignment"
                    );
                }
            }
        }
    }

    if total > 0 {
        info!(upserts = total, "Materialized feature flag assignments for new users");
    } else {
        debug!(
            flags = flags.len(),
            users = users.len(),
            "Feature flag sweep: nothing new"
        );
    }
    Ok(())
}

/// Evaluator: identical semantics to `apps/cli/src/flag.rs::evaluate_one`.
fn evaluate_one(flag: &Value, user_id: &str) -> (String, Option<Value>) {
    let default = flag
        .get("default_variant")
        .and_then(Value::as_str)
        .unwrap_or("off")
        .to_string();
    let enabled = flag.get("enabled").and_then(Value::as_bool).unwrap_or(true);
    let payloads = flag.get("payloads").cloned();
    let resolve = |variant: &str| -> Option<Value> {
        payloads.as_ref().and_then(|p| p.get(variant).cloned())
    };

    if !enabled {
        let payload = resolve(&default);
        return (default, payload);
    }

    let key = flag.get("key").and_then(Value::as_str).unwrap_or("");
    let empty: Vec<Value> = vec![];
    let mut rules: Vec<&Value> = flag
        .get("rules")
        .and_then(Value::as_array)
        .unwrap_or(&empty)
        .iter()
        .collect();
    rules.sort_by_key(|r| r.get("priority").and_then(Value::as_i64).unwrap_or(100));

    for rule in rules {
        let kind = rule.get("kind").and_then(Value::as_str).unwrap_or("");
        let variant = match rule.get("variant").and_then(Value::as_str) {
            Some(v) => v.to_string(),
            None => continue,
        };
        match kind {
            x if x == RULE_ALLOWLIST => {
                let hit = rule
                    .get("users")
                    .and_then(Value::as_array)
                    .map(|users| users.iter().any(|u| u.as_str() == Some(user_id)))
                    .unwrap_or(false);
                if hit {
                    return (variant.clone(), resolve(&variant));
                }
            }
            x if x == RULE_ROLLOUT => {
                let pct = rule.get("percent").and_then(Value::as_i64).unwrap_or(0);
                if pct > 0 && rollout_hash(key, user_id) < pct {
                    return (variant.clone(), resolve(&variant));
                }
            }
            _ => {}
        }
    }

    let payload = resolve(&default);
    (default, payload)
}

fn rollout_hash(key: &str, user_id: &str) -> i64 {
    let mut hasher = Sha256::new();
    hasher.update(key.as_bytes());
    hasher.update(b":");
    hasher.update(user_id.as_bytes());
    let digest = hasher.finalize();
    let prefix = u32::from_be_bytes([digest[0], digest[1], digest[2], digest[3]]);
    (prefix as i64) % 100
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn rollout_hash_is_deterministic_and_bounded() {
        for i in 0..1000 {
            let user = format!("user:{}", i);
            let h = rollout_hash("flag-x", &user);
            assert!((0..100).contains(&h), "hash {} out of range", h);
            assert_eq!(h, rollout_hash("flag-x", &user));
        }
    }

    /// Golden vectors shared verbatim with the CLI evaluator
    /// (`apps/cli/src/flag.rs`) and `fn::feature::hash` in
    /// `meta_tables_remote.surql`. Keep all three in lockstep: if the scheduler
    /// and CLI ever disagree on a bucket, a CLI write and a scheduler sweep
    /// would assign the same user different variants for a percentage rollout.
    #[test]
    fn rollout_hash_matches_golden_vectors() {
        assert_eq!(rollout_hash("checkout-v2", "user:alice"), 8);
        assert_eq!(rollout_hash("checkout-v2", "user:bob"), 62);
        assert_eq!(rollout_hash("flag-x", "user:abc"), 0);
        assert_eq!(rollout_hash("beta", "user:42"), 90);
        assert_eq!(rollout_hash("rollout", "user:zzz"), 17);
    }

    #[test]
    fn rollout_at_zero_never_matches() {
        let flag = json!({
            "key": "x",
            "enabled": true,
            "default_variant": "off",
            "rules": [{ "kind": "rollout", "variant": "on", "percent": 0, "priority": 50 }],
        });
        for i in 0..50 {
            let (variant, _) = evaluate_one(&flag, &format!("user:{}", i));
            assert_eq!(variant, "off");
        }
    }

    #[test]
    fn rollout_at_hundred_always_matches() {
        let flag = json!({
            "key": "x",
            "enabled": true,
            "default_variant": "off",
            "rules": [{ "kind": "rollout", "variant": "on", "percent": 100, "priority": 50 }],
        });
        for i in 0..50 {
            let (variant, _) = evaluate_one(&flag, &format!("user:{}", i));
            assert_eq!(variant, "on");
        }
    }

    #[test]
    fn allowlist_beats_lower_priority_rollout() {
        let flag = json!({
            "key": "x",
            "enabled": true,
            "default_variant": "off",
            "rules": [
                { "kind": "rollout", "variant": "on", "percent": 0, "priority": 50 },
                { "kind": "allowlist", "variant": "on", "users": ["user:abc"], "priority": 10 },
            ],
        });
        let (v, _) = evaluate_one(&flag, "user:abc");
        assert_eq!(v, "on");
        let (v, _) = evaluate_one(&flag, "user:xyz");
        assert_eq!(v, "off");
    }

    #[test]
    fn disabled_flag_returns_default_regardless_of_rules() {
        let flag = json!({
            "key": "x",
            "enabled": false,
            "default_variant": "off",
            "rules": [
                { "kind": "allowlist", "variant": "on", "users": ["user:abc"], "priority": 10 },
            ],
        });
        let (v, _) = evaluate_one(&flag, "user:abc");
        assert_eq!(v, "off");
    }

    #[test]
    fn payload_resolves_for_variant() {
        let flag = json!({
            "key": "x",
            "enabled": true,
            "default_variant": "off",
            "rules": [
                { "kind": "allowlist", "variant": "treatment", "users": ["user:abc"], "priority": 10 },
            ],
            "payloads": { "treatment": { "copy": "Hello" } },
        });
        let (v, p) = evaluate_one(&flag, "user:abc");
        assert_eq!(v, "treatment");
        assert_eq!(p, Some(json!({ "copy": "Hello" })));
    }
}
