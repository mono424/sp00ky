use crate::{converter, permission_inject, sanitizer};
use anyhow::{anyhow, Result};
use serde_json::{json, Value};
use std::collections::{BTreeSet, HashMap};
use web_time::Instant;

pub mod view {
    use super::*;

    /// Parsed registration data using new DBSP types
    pub struct DbspRegistrationData {
        pub plan: crate::operator::plan::QueryPlan,
        pub safe_params: Option<Value>,
        pub metadata: Value,
        pub format: Option<crate::circuit::view::OutputFormat>,
        /// Time (ms) spent converting/parsing the surql into a plan and
        /// injecting permissions. Surfaced to DevTools as the SSP "parse" phase.
        pub parse_ms: f64,
        /// Identity of the COMPUTATION this registration performs: the injected
        /// plan plus the value of every param that plan dereferences. Two
        /// registrations sharing this may share one operator graph.
        ///
        /// Computed here because this is the only place holding both the
        /// post-injection plan and `safe_params`. See `crate::merge_key` for why
        /// the plan alone is not sufficient (plans reference params, they never
        /// bake them, so two different users produce identical plans).
        pub merge_key: String,
        /// The client's query shape BEFORE permission injection, for the
        /// allowlist. Post-injection the plan carries the tenant's
        /// PERMISSIONS filters, which are not part of what the app asked for.
        pub shape: crate::allowlist::Shape,
    }

    /// Prepares a view registration request using DBSP types.
    ///
    /// Runs the converter on the user's surql, then injects each table's
    /// permission predicate per scan via `permission_inject::inject_permissions`.
    /// Errors abort registration so callers can surface them (typically as
    /// HTTP 400) with the offending table named.
    pub fn prepare_registration_dbsp(
        config: Value,
        permissions: &HashMap<String, String>,
        links: &HashMap<String, HashMap<String, String>>,
        opaque_fields: &HashMap<String, BTreeSet<String>>,
    ) -> Result<DbspRegistrationData> {
        use crate::circuit::view::OutputFormat;
        use crate::operator::plan::{OperatorPlan, QueryPlan};

        let id = config
            .get("id")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow!("Missing or invalid 'id'"))?
            .to_string();

        let surreal_ql = config
            .get("surql")
            .or_else(|| config.get("surreal_ql"))
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow!("Missing or invalid 'surql'"))?
            .to_string();

        let client_id = config
            .get("clientId")
            .or_else(|| config.get("client_id"))
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow!("Missing or invalid 'clientId'"))?
            .to_string();

        let ttl = config
            .get("ttl")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow!("Missing or invalid 'ttl'"))?
            .to_string();

        let last_active_at = config
            .get("lastActiveAt")
            .or_else(|| config.get("last_active_at"))
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow!("Missing or invalid 'lastActiveAt'"))?
            .to_string();

        let params = config.get("params").cloned().unwrap_or(json!({}));

        let format = config
            .get("format")
            .or_else(|| config.get("resultFormat"))
            .and_then(|v| v.as_str())
            .and_then(|s| match s.to_lowercase().as_str() {
                "streaming" => Some(OutputFormat::Streaming),
                "tree" => Some(OutputFormat::Tree),
                "flat" => Some(OutputFormat::Flat),
                _ => None,
            });

        // Time the convert→plan→permission-inject path as the SSP "parse" phase.
        let parse_start = Instant::now();

        let root_op_val = converter::convert_surql_to_dbsp_with_links(&surreal_ql, links)
            .or_else(|_| {
                serde_json::from_str::<Value>(&surreal_ql).map_err(anyhow::Error::from)
            })
            .map_err(|_| anyhow!("Invalid Query Plan"))?;

        let mut root_op: OperatorPlan = serde_json::from_value(root_op_val)
            .map_err(|e| anyhow!("Invalid Operator JSON: {}", e))?;

        let shape = crate::allowlist::Shape::of(&root_op);

        let safe_params = sanitizer::parse_params(params.clone());
        let safe_params_val = safe_params.clone().unwrap_or(json!({}));

        // Extract the user-scoped auth identity from the injected params.
        // `fn::query::register` injects `params.auth.id = <string>$auth.id`
        // server-side, so by the time we get here `safe_params.auth.id`
        // is the authenticated caller's user record id (as a string like
        // "user:abc"). We carry this forward as `auth_id` on `_00_query`
        // and `_00_list_ref` so cross-session LIVE delivery on
        // `_00_list_ref` can gate on `auth_id = $auth.id` (stable
        // across re-auth / reconnect) instead of the per-connection
        // `session::id()`.
        //
        // Note: we cannot read this from a top-level `authId` field on
        // the registration payload because SurrealDB's `http::post`
        // silently strips object keys that aren't in the runtime's
        // recognized set for that call site. The `params.auth.id` path
        // is the only channel that already survives that filtering.
        let auth_id = safe_params_val
            .get("auth")
            .and_then(|a| a.get("id"))
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();

        permission_inject::inject_permissions_with_links(
            &mut root_op,
            permissions,
            safe_params.as_ref(),
            links,
        )?;

        // Reject AFTER permission injection, so a permission expression that
        // itself touches an opaque field is caught too — that is the dangerous
        // case, because it silently empties the view for every caller rather than
        // for the one who wrote the query.
        reject_opaque_field_evaluation(&root_op, opaque_fields)?;

        let parse_ms = parse_start.elapsed().as_secs_f64() * 1000.0;

        let plan = QueryPlan {
            id: id.clone(),
            root: root_op,
        };

        // Deliberately over `plan.root` (post-injection) rather than the raw
        // surql: the permission filters are part of the computation, and a
        // schema change that alters injection must produce a different key.
        let merge_key = crate::merge_key::compute(&plan.root, safe_params.as_ref());

        let metadata = json!({
            "id": id,
            "clientId": client_id,
            "authId": auth_id,
            "sql": surreal_ql,
            "params": params,
            "safe_params": safe_params_val,
            "ttl": ttl,
            "lastActiveAt": last_active_at
        });

        Ok(DbspRegistrationData {
            plan,
            safe_params,
            metadata,
            format,
            parse_ms,
            merge_key,
            shape,
        })
    }

    /// Fail registration when the plan would evaluate a field the circuit does
    /// not hold (`sp00ky:opaque`: `-- @nosync`, `-- @crdt`, `-- @opaque`).
    ///
    /// Failing loudly here is the whole point. The alternative is not "the query
    /// doesn't work" but "the query returns nothing, forever, with no error":
    /// `resolve_field` yields `None` for the absent key, the comparison
    /// evaluates false, and the row never enters the membership set. When the
    /// offending predicate came from a table permission, every caller of that
    /// table sees an empty result and the rows read as deleted.
    fn reject_opaque_field_evaluation(
        root: &crate::operator::plan::OperatorPlan,
        opaque_fields: &HashMap<String, BTreeSet<String>>,
    ) -> Result<()> {
        if opaque_fields.is_empty() {
            return Ok(());
        }
        for (table, field) in root.evaluated_field_refs() {
            if opaque_fields
                .get(&table)
                .is_some_and(|fields| fields.contains(&field))
            {
                return Err(anyhow!(
                    "Field '{table}.{field}' cannot be used in WHERE, ORDER BY, a join, \
                     or a table permission: it is marked @opaque/@nosync/@crdt, so the sync \
                     engine never stores its value and any comparison against it would \
                     silently match nothing"
                ));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod opaque_field_gate_tests {
    use super::view::prepare_registration_dbsp;
    use serde_json::json;
    use std::collections::{BTreeSet, HashMap};

    fn opaque(table: &str, fields: &[&str]) -> HashMap<String, BTreeSet<String>> {
        let mut m = HashMap::new();
        m.insert(
            table.to_string(),
            fields.iter().map(|s| s.to_string()).collect(),
        );
        m
    }

    fn payload(surql: &str) -> serde_json::Value {
        json!({
            "id": "_00_query:t",
            "surql": surql,
            "params": {},
            "clientId": "c", "ttl": "10m", "lastActiveAt": "",
        })
    }

    /// Registration is default-deny for a table with no registered
    /// PERMISSIONS, which would fail every case here before the opaque gate is
    /// even reached.
    fn allow(table: &str) -> HashMap<String, String> {
        let mut m = HashMap::new();
        m.insert(table.to_string(), "true".to_string());
        m
    }

    #[test]
    fn where_on_opaque_field_is_rejected() {
        let err = prepare_registration_dbsp(
            payload("SELECT * FROM user WHERE secret_token = 'x';"),
            &allow("user"),
            &HashMap::new(),
            &opaque("user", &["secret_token"]),
        )
        .err().expect("must reject");
        assert!(err.to_string().contains("user.secret_token"), "got: {err}");
    }

    #[test]
    fn order_by_on_opaque_field_is_rejected() {
        let err = prepare_registration_dbsp(
            payload("SELECT * FROM user ORDER BY secret_token asc LIMIT 10;"),
            &allow("user"),
            &HashMap::new(),
            &opaque("user", &["secret_token"]),
        )
        .err().expect("must reject");
        assert!(err.to_string().contains("user.secret_token"), "got: {err}");
    }

    #[test]
    fn opaque_field_in_a_table_permission_is_rejected() {
        // The dangerous case: nothing in the user's query mentions the field, so
        // without this gate every caller of `user` silently sees zero rows.
        let mut perms = HashMap::new();
        perms.insert("user".to_string(), "secret_token = 'x'".to_string());
        let err = prepare_registration_dbsp(
            payload("SELECT * FROM user;"),
            &perms,
            &HashMap::new(),
            &opaque("user", &["secret_token"]),
        )
        .err().expect("must reject");
        assert!(err.to_string().contains("user.secret_token"), "got: {err}");
    }

    #[test]
    fn projecting_an_opaque_field_is_allowed() {
        // Projections are pass-through — the client reads the value from
        // SurrealDB, not from the circuit — so selecting it must still work.
        // This is the whole point of @opaque as distinct from @nosync.
        prepare_registration_dbsp(
            payload("SELECT id, secret_token FROM user;"),
            &allow("user"),
            &HashMap::new(),
            &opaque("user", &["secret_token"]),
        )
        .expect("projection must be allowed");
    }

    #[test]
    fn select_star_is_allowed() {
        prepare_registration_dbsp(
            payload("SELECT * FROM user;"),
            &allow("user"),
            &HashMap::new(),
            &opaque("user", &["secret_token"]),
        )
        .expect("SELECT * must be allowed");
    }

    #[test]
    fn a_non_opaque_predicate_on_the_same_table_still_registers() {
        prepare_registration_dbsp(
            payload("SELECT * FROM user WHERE email = 'a@b.c';"),
            &allow("user"),
            &HashMap::new(),
            &opaque("user", &["secret_token"]),
        )
        .expect("unrelated predicate must be allowed");
    }

    #[test]
    fn an_opaque_field_on_a_different_table_does_not_block() {
        // Field names are not globally unique; the gate must key on (table, field).
        prepare_registration_dbsp(
            payload("SELECT * FROM user WHERE secret_token = 'x';"),
            &allow("user"),
            &HashMap::new(),
            &opaque("other_table", &["secret_token"]),
        )
        .expect("must not block a same-named field on another table");
    }
}

#[cfg(test)]
mod start_window_isolation_tests {
    use super::view::prepare_registration_dbsp;
    use crate::circuit::view::OutputFormat;
    use crate::circuit::{Circuit, Record};
    use serde_json::json;
    use std::collections::HashMap;

    // Mirrors register_view_handler: surql -> prepare_registration_dbsp ->
    // add_query_with_auth(Streaming) -> the returned ViewDelta.records is what the
    // SSP writes into _00_list_ref. For START 50 over 120 rows it must be the
    // window [50,100), not the top-50.
    #[test]
    fn ssp_register_applies_start_offset() {
        let mut recs = vec![];
        for i in 0..120u32 {
            let id = format!("g{:03}", i);
            recs.push(Record::new(
                "game",
                &id,
                json!({ "id": format!("game:{}", id), "database": "game_database:c1",
                        "sort_index": i, "date": "2024-01-01T00:00:00Z" }),
            ));
        }
        let mut circuit = Circuit::new();
        circuit.load(recs);

        let mut perms = HashMap::new();
        perms.insert("game".to_string(), "true".to_string());

        let payload = json!({
            "id": "_00_query:test",
            "surql": "SELECT * FROM game WHERE database = $database ORDER BY sort_index asc, date desc, id asc LIMIT 50 START 50;",
            "params": { "database": "game_database:c1" },
            "clientId": "c", "ttl": "10m", "lastActiveAt": "", "format": "streaming"
        });
        let data = prepare_registration_dbsp(payload, &perms, &HashMap::new(), &HashMap::new()).expect("prep");
        let update = circuit
            .add_query_with_auth(data.plan, data.safe_params, Some(OutputFormat::Streaming), String::new())
            .expect("delta");
        let mut got = update.records.clone();
        got.sort();
        eprintln!("records.len={} first={:?} last={:?}", got.len(), got.first(), got.last());
        assert_eq!(got.len(), 50, "window size");
        assert_eq!(got.first().map(String::as_str), Some("game:g050"), "window must START at offset 50");
        assert_eq!(got.last().map(String::as_str), Some("game:g099"));
    }
}

#[cfg(test)]
mod start_window_register_before_ingest_tests {
    use super::view::prepare_registration_dbsp;
    use crate::circuit::view::OutputFormat;
    use crate::circuit::{Circuit, Change, ChangeSet};
    use serde_json::json;
    use std::collections::HashMap;

    // RUNTIME ORDER: the SSP registers the view, THEN games stream in via ingest.
    // (The passing isolation test loads data first, then registers.) For START 50
    // the view must still settle to the window [50,100), not the top-50.
    #[test]
    fn ssp_offset_window_correct_when_registered_before_ingest() {
        let mut circuit = Circuit::new();
        let mut perms = HashMap::new();
        perms.insert("game".to_string(), "true".to_string());
        let payload = json!({
            "id": "_00_query:test",
            "surql": "SELECT * FROM game WHERE database = $database ORDER BY sort_index asc, date desc, id asc LIMIT 50 START 50;",
            "params": { "database": "game_database:c1" },
            "clientId": "c", "ttl": "10m", "lastActiveAt": "", "format": "streaming"
        });
        let data = prepare_registration_dbsp(payload, &perms, &HashMap::new(), &HashMap::new()).expect("prep");
        circuit.add_query_with_auth(data.plan, data.safe_params, Some(OutputFormat::Streaming), String::new());

        let changes: Vec<Change> = (0..160u32)
            .map(|i| {
                let id = format!("g{:03}", i);
                Change::create(
                    "game",
                    &id,
                    json!({ "id": format!("game:{}", id), "database": "game_database:c1",
                            "sort_index": i, "date": "2024-01-01T00:00:00Z" }),
                )
            })
            .collect();
        circuit.step(ChangeSet { changes });

        let view = circuit.get_view("_00_query:test").expect("view");
        let mut recs: Vec<String> = view.cache.keys().map(|k| k.to_string()).collect();
        recs.sort();
        eprintln!("len={} first={:?} last={:?}", recs.len(), recs.first(), recs.last());
        assert_eq!(recs.len(), 50, "window size");
        assert_eq!(recs.first().map(String::as_str), Some("game:g050"), "must START at offset 50");
    }
}

#[cfg(test)]
mod link_traversal_permission_tests {
    //! Reproduction for the outbox-table live-query bug: an outbox table's
    //! SELECT permission traverses a record link (`assigned_to.owner.id =
    //! $auth.id`, job -> connection -> owner). The SSP injects that permission
    //! as a flat Filter, but the Filter operator can't dereference a link into
    //! another table's row, so `assigned_to.owner` resolves to NULL and EVERY
    //! row is filtered out — the view materializes zero rows, no `_00_list_ref`
    //! edges are written, and the client's query never receives live updates.
    //!
    //! With the link map (`job.assigned_to -> connection`) the converter lowers
    //! the traversal into a SemiJoin against `connection`, so the rows survive.
    use super::view::prepare_registration_dbsp;
    use crate::circuit::view::OutputFormat;
    use crate::circuit::{Circuit, Record};
    use serde_json::json;
    use std::collections::HashMap;

    // The real gamesync outbox permission (see packages/schema/src/outbox/gamesync.surql).
    const JOB_PERM: &str = "$access = \"account\" AND assigned_to.owner.id = $auth.id";

    fn store_with_job() -> Circuit {
        let mut circuit = Circuit::new();
        circuit.load(vec![
            // A job assigned to connection:c1, owned (via the connection) by user:u1.
            Record::new(
                "job",
                "j1",
                json!({ "id": "job:j1", "assigned_to": "connection:c1", "path": "/syncGames", "status": "pending" }),
            ),
            // Another user's job — must NOT leak into u1's view.
            Record::new(
                "job",
                "j2",
                json!({ "id": "job:j2", "assigned_to": "connection:c2", "path": "/syncGames", "status": "pending" }),
            ),
            Record::new("connection", "c1", json!({ "id": "connection:c1", "owner": "user:u1" })),
            Record::new("connection", "c2", json!({ "id": "connection:c2", "owner": "user:u2" })),
        ]);
        circuit
    }

    fn register_job_view(circuit: &mut Circuit, links: &HashMap<String, HashMap<String, String>>) -> Vec<String> {
        let mut perms = HashMap::new();
        perms.insert("job".to_string(), JOB_PERM.to_string());

        let payload = json!({
            "id": "_00_query:jobview",
            "surql": "SELECT * FROM job WHERE assigned_to = $assigned_to AND path = $path;",
            "params": {
                "assigned_to": "connection:c1",
                "path": "/syncGames",
                "auth": { "id": "user:u1" },
                "access": "account",
            },
            "clientId": "c", "ttl": "10m", "lastActiveAt": "", "format": "streaming",
        });

        let data = prepare_registration_dbsp(payload, &perms, links, &HashMap::new()).expect("prep");
        circuit.add_query_with_auth(
            data.plan,
            data.safe_params,
            Some(OutputFormat::Streaming),
            "user:u1".to_string(),
        );
        let mut recs: Vec<String> = circuit
            .get_view("_00_query:jobview")
            .expect("view")
            .cache
            .keys()
            .map(|k| k.to_string())
            .collect();
        recs.sort();
        recs
    }

    /// REPRODUCTION: with no link map the traversal permission stays a flat
    /// Filter and the view is empty even though a matching, permitted row exists.
    /// (Passes today's buggy behavior; documents the failure mode.)
    #[test]
    fn traversal_permission_without_link_map_yields_zero_rows() {
        let mut circuit = store_with_job();
        let recs = register_job_view(&mut circuit, &HashMap::new());
        assert!(
            recs.is_empty(),
            "without the link map the link-traversal Filter drops all rows, got {recs:?}"
        );
    }

    /// FIX: with `job.assigned_to -> connection` the permission lowers to a
    /// SemiJoin, so u1's own job is visible and u2's is not.
    #[test]
    fn traversal_permission_with_link_map_admits_owned_row() {
        let mut circuit = store_with_job();
        let mut links: HashMap<String, HashMap<String, String>> = HashMap::new();
        links
            .entry("job".to_string())
            .or_default()
            .insert("assigned_to".to_string(), "connection".to_string());

        let recs = register_job_view(&mut circuit, &links);
        assert_eq!(recs, vec!["job:j1".to_string()], "u1 sees only their own job");
    }
}
