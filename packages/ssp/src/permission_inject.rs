//! Per-Scan permission injection.
//!
//! For each `Scan { table }` in a registered query's plan, look up the table's
//! raw `PERMISSIONS FOR select WHERE <expr>` text, parse it through the same
//! `convert_surql_to_dbsp` pipeline that handles user queries, and AND-fold the
//! resulting predicate into the existing `Filter` directly above the scan (or
//! wrap the scan in a new `Filter`). The SSP runs as root and bypasses
//! SurrealDB's own permission enforcement, so this is what stops it from
//! over-sharing rows back to clients.
//!
//! Failures are surfaced at registration time. Callers map them to HTTP 400
//! with the offending table named in the error so the client can fix its
//! schema or supply the missing `auth` param. Default-deny: a table with no
//! registered permission is rejected, except `_00_*` meta tables (which the
//! boot loader filters out and the SSP itself accesses directly).

use std::collections::HashMap;

use anyhow::{anyhow, Result};
use serde_json::Value;

use crate::converter;
use crate::operator::plan::{JoinCondition, OperatorPlan, Projection};
use crate::operator::predicate::Predicate;
use crate::types::Path;

/// The result of lowering a table's SELECT permission. A permission that is a
/// flat boolean over the scanned table is a [`Predicate`] (AND-folded into the
/// existing Filter). One that references another table via `IN (subquery)`
/// lowers to a whole [`OperatorPlan`] over `Scan{table}` (SemiJoin / Union /
/// Distinct) that yields the allowed keys; it's composed with the view by a
/// `SemiJoin(view, perm, on: id = id)` — i.e. keep view rows whose id the
/// permission also admits.
enum PermInjection {
    Pred(Predicate),
    Plan(OperatorPlan),
}

/// Walk `plan` and inject each scanned table's permission predicate. Errors
/// abort registration; partial injection is never visible.
///
/// Link-schema-free entry point (permissions with record-link traversals stay
/// flat). Kept for callers/tests that don't need link lowering.
pub fn inject_permissions(
    plan: &mut OperatorPlan,
    perms: &HashMap<String, String>,
    params: Option<&Value>,
) -> Result<()> {
    inject_permissions_with_links(plan, perms, params, &HashMap::new())
}

/// As [`inject_permissions`], but resolves record-link target tables via `links`
/// so a permission like `assigned_to.owner.id = $auth.id` lowers to a `SemiJoin`
/// instead of an unevaluable flat `Filter`. See [`crate::converter::LinkMap`].
pub fn inject_permissions_with_links(
    plan: &mut OperatorPlan,
    perms: &HashMap<String, String>,
    params: Option<&Value>,
    links: &HashMap<String, HashMap<String, String>>,
) -> Result<()> {
    inject_node(plan, perms, params, links)
}

fn inject_node(
    plan: &mut OperatorPlan,
    perms: &HashMap<String, String>,
    params: Option<&Value>,
    links: &HashMap<String, HashMap<String, String>>,
) -> Result<()> {
    match plan {
        OperatorPlan::Scan { table } => {
            let table_name = table.clone();
            let Some(injected) = build_injection(&table_name, perms, params, links)? else {
                return Ok(());
            };
            let scan = OperatorPlan::Scan { table: table_name };
            *plan = match injected {
                PermInjection::Pred(predicate) => OperatorPlan::Filter {
                    input: Box::new(scan),
                    predicate,
                },
                PermInjection::Plan(perm) => intersect_with_permission(scan, perm),
            };
        }
        OperatorPlan::Filter { input, predicate } => {
            if let OperatorPlan::Scan { table } = input.as_ref() {
                let table_name = table.clone();
                let Some(injected) = build_injection(&table_name, perms, params, links)? else {
                    return Ok(());
                };
                match injected {
                    PermInjection::Pred(p) => {
                        let original = std::mem::replace(predicate, Predicate::True);
                        *predicate = and_predicates(original, p);
                    }
                    PermInjection::Plan(perm) => {
                        // Keep the view's own Filter{Scan} as the semi-join left,
                        // so both the view predicate AND the permission apply.
                        let view = std::mem::replace(plan, OperatorPlan::Scan { table: table_name });
                        *plan = intersect_with_permission(view, perm);
                    }
                }
                return Ok(());
            }
            inject_node(input, perms, params, links)?;
        }
        OperatorPlan::Join { left, right, .. }
        | OperatorPlan::SemiJoin { left, right, .. }
        | OperatorPlan::AntiJoin { left, right, .. }
        | OperatorPlan::Union { left, right } => {
            inject_node(left, perms, params, links)?;
            inject_node(right, perms, params, links)?;
        }
        OperatorPlan::Project { input, projections } => {
            inject_node(input, perms, params, links)?;
            for proj in projections.iter_mut() {
                if let Projection::Subquery { alias, plan, .. } = proj {
                    // A subquery over a table this node can't authorize must NOT
                    // abort the whole registration — that would take its sibling
                    // subqueries down with it. The classic case: a free/Cloudflare
                    // deployment lacks the devOnly outbox `job` table, but the
                    // client's ThreadDetail query still projects `jobs` alongside
                    // `author` + `comments`; rejecting the view made comments never
                    // sync (they only survived via preload, vanishing on reload).
                    // Degrade this subquery to EMPTY (fail-closed: `Filter{.., False}`
                    // yields no rows even if the table exists but is denied) and keep
                    // the rest of the view working.
                    if let Err(e) = inject_node(plan, perms, params, links) {
                        tracing::warn!(
                            target: "ssp::policy",
                            alias = %alias,
                            error = %e,
                            "subquery permission injection failed — yielding an empty subquery instead of rejecting the view"
                        );
                        let inner = std::mem::replace(
                            plan.as_mut(),
                            OperatorPlan::Scan { table: String::new() },
                        );
                        **plan = OperatorPlan::Filter {
                            input: Box::new(inner),
                            predicate: Predicate::False,
                        };
                    }
                }
            }
        }
        OperatorPlan::Limit { input, .. } | OperatorPlan::Distinct { input } => {
            inject_node(input, perms, params, links)?;
        }
    }
    Ok(())
}

/// Look up the permission text for `table` and return the predicate to AND in.
/// Returns `Ok(None)` for permissions that resolve to `true` (no filtering
/// needed) or for `_00_*` meta tables that the SSP accesses directly. Returns
/// `Err` for default-deny, unsupported constructs, missing `$auth`, or
/// converter parse failures.
/// Compose a permission subplan with the view: keep view rows whose `id` the
/// permission also admits.
fn intersect_with_permission(view: OperatorPlan, perm: OperatorPlan) -> OperatorPlan {
    OperatorPlan::SemiJoin {
        left: Box::new(view),
        right: Box::new(perm),
        on: JoinCondition {
            left_field: Path::new("id"),
            right_field: Path::new("id"),
        },
    }
}

fn build_injection(
    table: &str,
    perms: &HashMap<String, String>,
    params: Option<&Value>,
    links: &HashMap<String, HashMap<String, String>>,
) -> Result<Option<PermInjection>> {
    let raw = match perms.get(table) {
        Some(t) => t.trim(),
        None => {
            if table.starts_with("_00_") {
                return Ok(None);
            }
            return Err(anyhow!(
                "table `{table}` has no PERMISSIONS clause registered; default-deny"
            ));
        }
    };

    if raw.eq_ignore_ascii_case("true") {
        return Ok(None);
    }
    if raw.eq_ignore_ascii_case("false") {
        return Err(anyhow!("table `{table}` permission is FALSE; access denied"));
    }

    if raw.contains("$auth") && !params_have_auth(params) {
        return Err(anyhow!(
            "permission for `{table}` requires $auth but registration params lack it"
        ));
    }

    if let Some(snippet) = unsupported_construct(raw) {
        return Err(anyhow!(
            "permission for `{table}` contains unsupported construct: {snippet}"
        ));
    }

    let synthetic = format!("SELECT * FROM {table} WHERE {raw}");
    let plan_val = converter::convert_surql_to_dbsp_with_links(&synthetic, links)
        .map_err(|e| anyhow!("permission for `{table}` failed to parse: {e}"))?;

    let parsed: OperatorPlan = serde_json::from_value(plan_val).map_err(|e| {
        anyhow!("permission for `{table}` deserialize error: {e}")
    })?;

    match parsed {
        OperatorPlan::Filter { input, predicate } if matches!(*input, OperatorPlan::Scan { .. }) => {
            Ok(Some(PermInjection::Pred(predicate)))
        }
        // An `IN (subquery)` permission lowers to a SemiJoin / Union / Distinct
        // subplan over `Scan{table}` — compose it whole (see PermInjection).
        plan @ (OperatorPlan::SemiJoin { .. }
        | OperatorPlan::Union { .. }
        | OperatorPlan::Distinct { .. }) => {
            // A scan with no table name can never match a row, so a permission
            // containing one silently admits NOTHING — the view materialises
            // empty and every client renders an empty list with no error
            // anywhere. That cost two debugging sessions on whitepawn's
            // `statistics` table; refuse it at registration instead.
            if let Some(bad) = scans_unnamed_table(&plan) {
                return Err(anyhow!(
                    "permission for `{table}` lowered to a scan of an unnamed table                      ({bad}) — it would admit no rows at all"
                ));
            }
            Ok(Some(PermInjection::Plan(plan)))
        }
        OperatorPlan::Filter { .. } => Err(anyhow!(
            "permission for `{table}` produced a non-flat plan (likely identifier-vs-identifier comparison)"
        )),
        _ => Err(anyhow!(
            "permission for `{table}` produced a non-filter plan (likely a join or subquery)"
        )),
    }
}

/// Find a `Scan` with an empty table name anywhere in `plan`. Such a scan is
/// always empty at runtime, so any permission built on one denies everything —
/// the failure mode this guards is silence, not an error.
fn scans_unnamed_table(plan: &OperatorPlan) -> Option<String> {
    match plan {
        OperatorPlan::Scan { table } => {
            if table.trim().is_empty() {
                Some("Scan{table: \"\"}".to_string())
            } else {
                None
            }
        }
        OperatorPlan::Filter { input, .. }
        | OperatorPlan::Distinct { input }
        | OperatorPlan::Project { input, .. } => scans_unnamed_table(input),
        OperatorPlan::SemiJoin { left, right, .. } | OperatorPlan::Union { left, right } => {
            scans_unnamed_table(left).or_else(|| scans_unnamed_table(right))
        }
        _ => None,
    }
}

fn params_have_auth(params: Option<&Value>) -> bool {
    params
        .and_then(|p| p.get("auth"))
        .map(|v| !v.is_null())
        .unwrap_or(false)
}

/// Substring scan for permission constructs the SSP cannot represent. Returns
/// the matched snippet for error messages. We use case-insensitive matching
/// because SurrealDB doesn't care about keyword case.
fn unsupported_construct(raw: &str) -> Option<&'static str> {
    let lower = raw.to_lowercase();
    // `IN (SELECT VALUE ...)` is now supported (lowered to a SemiJoin by the
    // converter), so it is no longer denylisted. EXISTS-subqueries and $parent
    // correlation are still unsupported.
    const NEEDLES: &[&str] = &["exists (select", "$parent"];
    for needle in NEEDLES {
        if lower.contains(needle) {
            return Some(needle);
        }
    }
    None
}

/// AND two predicates, flattening when either is already an `And` and dropping
/// `True` operands. Short-circuits to `False` when either side is `False`.
fn and_predicates(a: Predicate, b: Predicate) -> Predicate {
    if matches!(a, Predicate::False) || matches!(b, Predicate::False) {
        return Predicate::False;
    }
    let mut out: Vec<Predicate> = Vec::new();
    push_and_operand(&mut out, a);
    push_and_operand(&mut out, b);
    match out.len() {
        0 => Predicate::True,
        1 => out.into_iter().next().unwrap(),
        _ => Predicate::And { predicates: out },
    }
}

fn push_and_operand(out: &mut Vec<Predicate>, p: Predicate) {
    match p {
        Predicate::True => {}
        Predicate::And { predicates } => {
            for inner in predicates {
                push_and_operand(out, inner);
            }
        }
        other => out.push(other),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::operator::plan::{OperatorPlan, Projection};
    use crate::types::Path;
    use serde_json::json;

    fn perms_with(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs.iter().map(|(t, p)| (t.to_string(), p.to_string())).collect()
    }

    fn links_with(pairs: &[(&str, &str, &str)]) -> HashMap<String, HashMap<String, String>> {
        let mut m: HashMap<String, HashMap<String, String>> = HashMap::new();
        for (t, f, target) in pairs {
            m.entry(t.to_string()).or_default().insert(f.to_string(), target.to_string());
        }
        m
    }

    /// Recursively collect every `Projection::Subquery { alias, plan }` in a plan.
    fn collect_subqueries<'a>(plan: &'a OperatorPlan, out: &mut Vec<(String, &'a OperatorPlan)>) {
        match plan {
            OperatorPlan::Project { input, projections } => {
                collect_subqueries(input, out);
                for p in projections {
                    if let Projection::Subquery { alias, plan, .. } = p {
                        out.push((alias.clone(), plan.as_ref()));
                        collect_subqueries(plan, out);
                    }
                }
            }
            OperatorPlan::Filter { input, .. }
            | OperatorPlan::Limit { input, .. }
            | OperatorPlan::Distinct { input } => collect_subqueries(input, out),
            OperatorPlan::Join { left, right, .. }
            | OperatorPlan::SemiJoin { left, right, .. }
            | OperatorPlan::AntiJoin { left, right, .. }
            | OperatorPlan::Union { left, right } => {
                collect_subqueries(left, out);
                collect_subqueries(right, out);
            }
            OperatorPlan::Scan { .. } => {}
        }
    }

    /// A subquery over a table with NO registered permission (e.g. the devOnly
    /// outbox `job` absent from a free/Cloudflare deployment) must NOT reject the
    /// whole view — it degrades to an empty (`Filter{.., False}`) subquery while
    /// sibling subqueries (`author`, `comments`) keep working. Regression for
    /// "comments vanish on reload": the client's ThreadDetail query projects
    /// `jobs` next to `comments`, and rejecting it made the comments view never
    /// register (comments only survived via preload).
    #[test]
    fn unpermitted_subquery_table_degrades_to_empty_not_rejection() {
        let sql = "SELECT *, \
            (SELECT * FROM user WHERE id=$parent.author LIMIT 1)[0] AS author, \
            (SELECT * FROM comment WHERE thread=$parent.id ORDER BY created_at desc LIMIT 10) AS comments, \
            (SELECT * FROM job WHERE assigned_to=$parent.id LIMIT 1) AS jobs \
            FROM thread WHERE id = $id";
        let mut plan: OperatorPlan =
            serde_json::from_value(converter::convert_surql_to_dbsp(sql).unwrap()).unwrap();

        // thread/user/comment are permitted; `job` is deliberately ABSENT.
        let perms = perms_with(&[("thread", "true"), ("user", "true"), ("comment", "true")]);
        let params = json!({ "auth": { "id": "user:u" }, "id": "thread:t" });

        // Before the fix this returned Err (default-deny on `job`) and the whole
        // registration aborted. It must now succeed.
        inject_permissions(&mut plan, &perms, Some(&params))
            .expect("view with an unpermitted subquery table must still register");

        let mut subs = Vec::new();
        collect_subqueries(&plan, &mut subs);
        let jobs = subs.iter().find(|(a, _)| a == "jobs").expect("jobs subquery present");
        assert!(
            matches!(jobs.1, OperatorPlan::Filter { predicate: Predicate::False, .. }),
            "jobs subquery must be forced empty (Filter False); got {:?}",
            jobs.1
        );
        let comments = subs.iter().find(|(a, _)| a == "comments").expect("comments subquery present");
        assert!(
            !matches!(comments.1, OperatorPlan::Filter { predicate: Predicate::False, .. }),
            "comments subquery must NOT be emptied — it is permitted"
        );
    }

    /// An outbox permission that traverses a record link
    /// (`assigned_to.owner.id = $auth.id`) must inject as a SemiJoin
    /// composition (via the link map), not a flat unevaluable Filter.
    #[test]
    fn link_traversal_permission_injects_semijoin() {
        // User view: SELECT * FROM job WHERE assigned_to = $assigned_to
        let mut plan = OperatorPlan::Filter {
            input: Box::new(OperatorPlan::Scan { table: "job".into() }),
            predicate: Predicate::Eq {
                field: Path::new("assigned_to"),
                value: json!({ "$param": "assigned_to" }),
            },
        };
        let perms = perms_with(&[("job", "$access = \"account\" AND assigned_to.owner.id = $auth.id")]);
        let links = links_with(&[("job", "assigned_to", "connection")]);
        let params = json!({ "auth": { "id": "user:u1" }, "access": "account", "assigned_to": "connection:c1" });

        inject_permissions_with_links(&mut plan, &perms, Some(&params), &links).unwrap();

        // Root is the id=id intersection wrapper: SemiJoin(view, perm).
        match plan {
            OperatorPlan::SemiJoin { left, right, on } => {
                assert!(matches!(*left, OperatorPlan::Filter { .. }), "view filter kept as left");
                assert_eq!(on.left_field.segments(), &["id".to_string()]);
                assert_eq!(on.right_field.segments(), &["id".to_string()]);
                // right = permission SemiJoin(Filter(Scan{job}, $access), Filter(Scan{connection}), on assigned_to=id)
                match *right {
                    OperatorPlan::SemiJoin { right: perm_right, on: perm_on, .. } => {
                        assert_eq!(perm_on.left_field.segments(), &["assigned_to".to_string()]);
                        assert_eq!(perm_on.right_field.segments(), &["id".to_string()]);
                        match *perm_right {
                            OperatorPlan::Filter { input, .. } => {
                                assert!(matches!(*input, OperatorPlan::Scan { table } if table == "connection"));
                            }
                            other => panic!("expected Filter(Scan connection), got {other:?}"),
                        }
                    }
                    other => panic!("expected permission SemiJoin, got {other:?}"),
                }
            }
            other => panic!("expected SemiJoin at root, got {other:?}"),
        }
    }

    /// Without a link map (or for an unmapped link) the same permission stays a
    /// flat Filter — the pre-fix behavior that drops every row.
    #[test]
    fn link_traversal_permission_without_map_stays_flat() {
        let mut plan = OperatorPlan::Scan { table: "job".into() };
        let perms = perms_with(&[("job", "$access = \"account\" AND assigned_to.owner.id = $auth.id")]);
        let params = json!({ "auth": { "id": "user:u1" }, "access": "account" });

        inject_permissions(&mut plan, &perms, Some(&params)).unwrap();
        assert!(matches!(plan, OperatorPlan::Filter { .. }), "no link map → flat Filter");
    }

    #[test]
    fn missing_table_is_default_deny() {
        let mut plan = OperatorPlan::Scan {
            table: "thread".into(),
        };
        let perms = HashMap::new();
        let err = inject_permissions(&mut plan, &perms, None).unwrap_err();
        assert!(err.to_string().contains("thread"));
        assert!(err.to_string().contains("default-deny"));
    }

    #[test]
    fn meta_tables_pass_through_when_missing() {
        let mut plan = OperatorPlan::Scan {
            table: "_00_query".into(),
        };
        let perms = HashMap::new();
        inject_permissions(&mut plan, &perms, None).unwrap();
        assert!(matches!(plan, OperatorPlan::Scan { .. }));
    }

    #[test]
    fn false_permission_errors() {
        let mut plan = OperatorPlan::Scan {
            table: "thread".into(),
        };
        let perms = perms_with(&[("thread", "false")]);
        let err = inject_permissions(&mut plan, &perms, None).unwrap_err();
        assert!(err.to_string().contains("FALSE"));
    }

    #[test]
    fn true_permission_is_noop() {
        let mut plan = OperatorPlan::Scan {
            table: "thread".into(),
        };
        let perms = perms_with(&[("thread", "true")]);
        inject_permissions(&mut plan, &perms, None).unwrap();
        assert!(matches!(plan, OperatorPlan::Scan { .. }));
    }

    #[test]
    fn auth_required_but_missing_errors() {
        let mut plan = OperatorPlan::Scan {
            table: "thread".into(),
        };
        let perms = perms_with(&[("thread", "author.id = $auth.id")]);
        let err = inject_permissions(&mut plan, &perms, None).unwrap_err();
        assert!(err.to_string().contains("$auth"));
    }

    #[test]
    fn exists_subquery_still_unsupported() {
        let mut plan = OperatorPlan::Scan {
            table: "thread".into(),
        };
        let perms = perms_with(&[("thread", "EXISTS (SELECT VALUE in FROM collaborates_on)")]);
        let params = json!({"auth": {"id": "user:a"}});
        let err = inject_permissions(&mut plan, &perms, Some(&params)).unwrap_err();
        assert!(err.to_string().contains("unsupported construct"));
    }

    #[test]
    fn single_hop_in_subquery_injects_semijoin() {
        // `owner IN (SELECT VALUE owner FROM broadcast WHERE share_visibility = 'public')`
        // over a plain scan → SemiJoin(Scan{thread}, Filter(Scan{broadcast}, ...)).
        let mut plan = OperatorPlan::Scan {
            table: "thread".into(),
        };
        let perms = perms_with(&[(
            "thread",
            "owner IN (SELECT VALUE owner FROM broadcast WHERE share_visibility = 'public')",
        )]);
        inject_permissions(&mut plan, &perms, None).unwrap();
        // Root is the id=id intersection wrapper: SemiJoin(view, perm).
        match plan {
            OperatorPlan::SemiJoin { left, right, on } => {
                assert!(matches!(*left, OperatorPlan::Scan { .. }), "left is the view scan");
                assert_eq!(on.left_field.segments(), &["id".to_string()]);
                assert_eq!(on.right_field.segments(), &["id".to_string()]);
                // right is the permission plan: SemiJoin(Scan{thread}, Filter(Scan{broadcast})).
                match *right {
                    OperatorPlan::SemiJoin { right: perm_right, on: perm_on, .. } => {
                        assert_eq!(perm_on.left_field.segments(), &["owner".to_string()]);
                        assert_eq!(perm_on.right_field.segments(), &["owner".to_string()]);
                        match *perm_right {
                            OperatorPlan::Filter { input, .. } => {
                                assert!(matches!(*input, OperatorPlan::Scan { table } if table == "broadcast"));
                            }
                            other => panic!("expected Filter(Scan broadcast), got {:?}", other),
                        }
                    }
                    other => panic!("expected permission SemiJoin, got {:?}", other),
                }
            }
            other => panic!("expected SemiJoin at root, got {:?}", other),
        }
    }

    #[test]
    fn inside_canonical_form_is_supported() {
        // SurrealDB stores `IN` as `INSIDE`; the injector must handle both.
        let mut plan = OperatorPlan::Scan {
            table: "thread".into(),
        };
        let perms = perms_with(&[(
            "thread",
            "owner INSIDE (SELECT VALUE owner FROM broadcast WHERE share_visibility = 'public')",
        )]);
        inject_permissions(&mut plan, &perms, None).unwrap();
        assert!(matches!(plan, OperatorPlan::SemiJoin { .. }), "INSIDE lowered to SemiJoin");
    }

    #[test]
    fn or_of_flat_and_subquery_keeps_both_branches() {
        // whitepawn's `statistics` permission, verbatim. 70 rows owned by the
        // caller materialised as ZERO on staging, so the `owner = $auth.id`
        // branch is being lost somewhere in the OR.
        let mut plan = OperatorPlan::Scan { table: "statistics".into() };
        let perms = perms_with(&[(
            "statistics",
            "$access = 'account' AND (owner = $auth.id OR database INSIDE \
             (SELECT VALUE collection FROM collection_share WHERE user = $auth.id AND role = 'editor'))",
        )]);
        let params = json!({"auth": {"id": "user:a"}, "access": "account"});
        inject_permissions(&mut plan, &perms, Some(&params))
            .expect("whitepawn's statistics permission must lower");

        // Both OR branches must survive. Before the fix the AND branch handed
        // the nested `or` node straight to `semijoin_for_subquery`, which read
        // `table` off a node that has none — producing `Scan{table: ""}` and
        // dropping `owner = $auth.id` entirely, so the permission admitted no
        // rows at all.
        let rendered = format!("{plan:?}");
        assert!(
            rendered.contains("collection_share"),
            "subquery branch lost its source table: {rendered}"
        );
        assert!(
            !rendered.contains("table: \"\""),
            "permission lowered to a scan of an unnamed table: {rendered}"
        );
        assert!(
            rendered.contains("Union"),
            "the OR must lower to a Union of both branches: {rendered}"
        );
        assert!(
            rendered.contains("\"owner\""),
            "the `owner = $auth.id` branch must survive: {rendered}"
        );
    }

    #[test]
    fn a_permission_that_admits_nothing_is_rejected() {
        // The safety net for the class of bug above: whatever the lowering
        // does, a permission that can only ever match zero rows must fail
        // registration loudly rather than install and render empty lists.
        let bad = OperatorPlan::SemiJoin {
            left: Box::new(OperatorPlan::Scan { table: "statistics".into() }),
            right: Box::new(OperatorPlan::Scan { table: String::new() }),
            on: crate::operator::plan::JoinCondition {
                left_field: Path::new("id"),
                right_field: Path::new("id"),
            },
        };
        assert!(scans_unnamed_table(&bad).is_some(), "walker must spot the empty scan");
        assert!(
            scans_unnamed_table(&OperatorPlan::Scan { table: "statistics".into() }).is_none(),
            "a named scan is fine"
        );
    }

    #[test]
    fn two_hop_in_subquery_injects_nested_semijoin() {
        // `owner IN (SELECT VALUE broadcast.owner FROM broadcast_share WHERE ...)`
        // → SemiJoin(view, SemiJoin(Scan{broadcast}, Filter(Scan{broadcast_share})))
        let mut plan = OperatorPlan::Scan {
            table: "stream_presence".into(),
        };
        let perms = perms_with(&[(
            "stream_presence",
            "owner IN (SELECT VALUE broadcast.owner FROM broadcast_share WHERE user = $auth.id AND role = 'admin')",
        )]);
        let params = json!({"auth": {"id": "user:a"}});
        inject_permissions(&mut plan, &perms, Some(&params)).unwrap();
        // Root: id=id intersection wrapper. right = permission plan
        // SemiJoin(Scan{stream_presence}, inner, on owner=owner), where inner
        // resolves admin-shared broadcasts.
        match plan {
            OperatorPlan::SemiJoin { right, on, .. } => {
                assert_eq!(on.left_field.segments(), &["id".to_string()]);
                assert_eq!(on.right_field.segments(), &["id".to_string()]);
                match *right {
                    OperatorPlan::SemiJoin { right: perm_right, on: perm_on, .. } => {
                        assert_eq!(perm_on.left_field.segments(), &["owner".to_string()]);
                        assert_eq!(perm_on.right_field.segments(), &["owner".to_string()]);
                        // inner: SemiJoin(Scan{broadcast}, Filter(Scan{broadcast_share}), on id=broadcast)
                        match *perm_right {
                            OperatorPlan::SemiJoin { left, on: inner_on, .. } => {
                                assert!(matches!(*left, OperatorPlan::Scan { table } if table == "broadcast"));
                                assert_eq!(inner_on.left_field.segments(), &["id".to_string()]);
                                assert_eq!(inner_on.right_field.segments(), &["broadcast".to_string()]);
                            }
                            other => panic!("expected inner SemiJoin, got {:?}", other),
                        }
                    }
                    other => panic!("expected permission SemiJoin, got {:?}", other),
                }
            }
            other => panic!("expected SemiJoin at root, got {:?}", other),
        }
    }

    #[test]
    fn full_stream_presence_permission_injects_distinct_union() {
        // The real schema permission: owner OR public OR admin. The whole thing
        // must compose without error into SemiJoin(view, Distinct(Union(...))).
        let mut plan = OperatorPlan::Filter {
            input: Box::new(OperatorPlan::Scan {
                table: "stream_presence".into(),
            }),
            predicate: Predicate::Eq {
                field: Path::new("owner"),
                value: json!({"$param": "owner"}),
            },
        };
        let perms = perms_with(&[(
            "stream_presence",
            "( $access = \"account\" AND owner = $auth.id ) OR owner IN (SELECT VALUE owner FROM broadcast WHERE share_visibility = 'public') OR ( $access = \"account\" AND owner IN (SELECT VALUE broadcast.owner FROM broadcast_share WHERE user = $auth.id AND role = 'admin') )",
        )]);
        let params = json!({"auth": {"id": "user:a"}, "access": "account", "owner": "user:a"});
        inject_permissions(&mut plan, &perms, Some(&params)).unwrap();
        match plan {
            OperatorPlan::SemiJoin { left, right, on } => {
                assert!(matches!(*left, OperatorPlan::Filter { .. }), "view filter kept as left");
                assert_eq!(on.left_field.segments(), &["id".to_string()]);
                assert_eq!(on.right_field.segments(), &["id".to_string()]);
                assert!(matches!(*right, OperatorPlan::Distinct { .. }), "permission is Distinct(Union(...))");
            }
            other => panic!("expected SemiJoin at root, got {:?}", other),
        }
    }

    #[test]
    fn simple_eq_wraps_scan_in_filter() {
        let mut plan = OperatorPlan::Scan {
            table: "thread".into(),
        };
        let perms = perms_with(&[("thread", "author.id = $auth.id")]);
        let params = json!({"auth": {"id": "user:a"}});
        inject_permissions(&mut plan, &perms, Some(&params)).unwrap();

        match plan {
            OperatorPlan::Filter { input, predicate } => {
                assert!(matches!(*input, OperatorPlan::Scan { .. }));
                match predicate {
                    Predicate::Eq { field, value } => {
                        assert_eq!(field.segments(), &["author".to_string(), "id".to_string()]);
                        assert_eq!(value, json!({"$param": "auth.id"}));
                    }
                    other => panic!("expected Eq, got {:?}", other),
                }
            }
            _ => panic!("expected Filter at root"),
        }
    }

    #[test]
    fn existing_filter_is_anded_not_double_wrapped() {
        let user_pred = Predicate::Eq {
            field: Path::new("active"),
            value: json!(true),
        };
        let mut plan = OperatorPlan::Filter {
            input: Box::new(OperatorPlan::Scan {
                table: "thread".into(),
            }),
            predicate: user_pred,
        };
        let perms = perms_with(&[("thread", "author.id = $auth.id")]);
        let params = json!({"auth": {"id": "user:a"}});
        inject_permissions(&mut plan, &perms, Some(&params)).unwrap();

        match plan {
            OperatorPlan::Filter { predicate, .. } => match predicate {
                Predicate::And { predicates } => assert_eq!(predicates.len(), 2),
                other => panic!("expected And, got {:?}", other),
            },
            _ => panic!("expected Filter"),
        }
    }

    #[test]
    fn subquery_inner_scan_is_injected() {
        let inner_scan = OperatorPlan::Scan {
            table: "comment".into(),
        };
        let projection = Projection::Subquery {
            alias: "comments".into(),
            plan: Box::new(inner_scan),
            parent_key: None,
        };
        let mut plan = OperatorPlan::Project {
            input: Box::new(OperatorPlan::Scan {
                table: "thread".into(),
            }),
            projections: vec![projection],
        };
        let perms = perms_with(&[("thread", "true"), ("comment", "author.id = $auth.id")]);
        let params = json!({"auth": {"id": "user:a"}});
        inject_permissions(&mut plan, &perms, Some(&params)).unwrap();

        if let OperatorPlan::Project { projections, .. } = &plan {
            if let Projection::Subquery { plan, .. } = &projections[0] {
                assert!(matches!(plan.as_ref(), OperatorPlan::Filter { .. }));
                return;
            }
        }
        panic!("expected Project with Subquery containing a Filter");
    }

    #[test]
    fn or_with_param_lhs_parses_through_shared_converter() {
        // Shape used by the example app's thread permission.
        let mut plan = OperatorPlan::Scan {
            table: "thread".into(),
        };
        let perms = perms_with(&[(
            "thread",
            "published = true OR $access = \"account\" AND author.id = $auth.id",
        )]);
        let params = json!({"auth": {"id": "user:a"}, "access": "account"});
        inject_permissions(&mut plan, &perms, Some(&params)).unwrap();

        match plan {
            OperatorPlan::Filter { predicate, .. } => {
                assert!(matches!(predicate, Predicate::Or { .. }));
            }
            _ => panic!("expected Filter"),
        }
    }

    /// The exact `renderer_device` permission that shipped with impersonation.
    /// It used to fail to parse, and `prepare_registration_dbsp` turns a failed
    /// parse into a rejected view registration, so every live query on the
    /// table went down at once for every client. Four of whitepawn's fourteen
    /// affected tables were livestream tables; the rest were direct messages,
    /// linkups, presence, saved puzzles and statistics.
    #[test]
    fn impersonation_membership_permission_injects() {
        let mut plan = OperatorPlan::Scan {
            table: "renderer_device".into(),
        };
        let perms = perms_with(&[(
            "renderer_device",
            "$access INSIDE ['account', '_00_impersonate'] AND owner = $auth.id",
        )]);
        let params = json!({"auth": {"id": "user:a"}, "access": "account"});
        inject_permissions(&mut plan, &perms, Some(&params)).unwrap();

        match plan {
            OperatorPlan::Filter { input, .. } => {
                assert!(matches!(*input, OperatorPlan::Scan { .. }));
            }
            other => panic!("expected a flat Filter over the scan, got {other:?}"),
        }
    }

    /// Identifier-vs-identifier in a permission expression (e.g. `author = otherfield`)
    /// gets routed through the converter as a `__JOIN_CANDIDATE__` and lifted
    /// into a `Join` operator. We can't AND a Join into a scan filter, so the
    /// non-flat plan must surface as an error.
    #[test]
    fn join_candidate_permission_errors() {
        let mut plan = OperatorPlan::Scan {
            table: "thread".into(),
        };
        let perms = perms_with(&[("thread", "author = otherfield")]);
        let err = inject_permissions(&mut plan, &perms, None).unwrap_err();
        assert!(
            err.to_string().contains("non-filter") || err.to_string().contains("non-flat"),
            "expected join/non-filter error, got {err}"
        );
    }

    /// $auth check looks at a top-level `auth` field on the params object.
    /// `auth = null` must be treated as missing — null can't grant access.
    #[test]
    fn null_auth_param_is_treated_as_missing() {
        let mut plan = OperatorPlan::Scan {
            table: "thread".into(),
        };
        let perms = perms_with(&[("thread", "author.id = $auth.id")]);
        let params = json!({"auth": null});
        let err = inject_permissions(&mut plan, &perms, Some(&params)).unwrap_err();
        assert!(err.to_string().contains("$auth"));
    }

    /// A predicate that doesn't reference `$auth` at all should register fine
    /// even with no auth params (e.g. `published = true`).
    #[test]
    fn no_auth_required_works_without_params() {
        let mut plan = OperatorPlan::Scan {
            table: "thread".into(),
        };
        let perms = perms_with(&[("thread", "published = true")]);
        inject_permissions(&mut plan, &perms, None).unwrap();
        assert!(matches!(plan, OperatorPlan::Filter { .. }));
    }

    /// A join in the user query: each side scan must get its own permission
    /// applied independently. Verify by tracking both wrap-events.
    #[test]
    fn join_query_injects_both_sides() {
        use crate::operator::plan::JoinCondition;
        let mut plan = OperatorPlan::Join {
            left: Box::new(OperatorPlan::Scan {
                table: "thread".into(),
            }),
            right: Box::new(OperatorPlan::Scan {
                table: "user".into(),
            }),
            on: JoinCondition {
                left_field: Path::new("author"),
                right_field: Path::new("id"),
            },
        };
        let perms = perms_with(&[("thread", "true"), ("user", "id = $auth.id")]);
        let params = json!({"auth": {"id": "user:a"}});
        inject_permissions(&mut plan, &perms, Some(&params)).unwrap();

        // Left side `thread` is `true` so unchanged. Right side `user` must
        // now be wrapped in Filter.
        if let OperatorPlan::Join { right, .. } = &plan {
            assert!(
                matches!(right.as_ref(), OperatorPlan::Filter { .. }),
                "right side must be Filter-wrapped, got {right:?}"
            );
        } else {
            panic!("expected Join");
        }
    }

    /// Limit / Distinct wrappers around a scan must not block injection: the
    /// walker has to descend through them.
    #[test]
    fn injection_descends_through_limit_and_distinct() {
        let mut plan = OperatorPlan::Limit {
            input: Box::new(OperatorPlan::Distinct {
                input: Box::new(OperatorPlan::Scan {
                    table: "thread".into(),
                }),
            }),
            limit: 10,
            start: 0,
            order_by: None,
        };
        let perms = perms_with(&[("thread", "published = true")]);
        inject_permissions(&mut plan, &perms, None).unwrap();

        // Walk down to find the wrapped scan.
        if let OperatorPlan::Limit { input, .. } = &plan {
            if let OperatorPlan::Distinct { input } = input.as_ref() {
                assert!(
                    matches!(input.as_ref(), OperatorPlan::Filter { .. }),
                    "scan must be wrapped"
                );
                return;
            }
        }
        panic!("walker did not descend through Limit/Distinct");
    }

    /// Permission text with leading/trailing whitespace must still parse —
    /// the boot loader trims when extracting, but a manual `set_permission`
    /// caller could pass whitespace.
    #[test]
    fn whitespace_around_permission_text_is_tolerated() {
        let mut plan = OperatorPlan::Scan {
            table: "thread".into(),
        };
        let perms = perms_with(&[("thread", "  published = true  ")]);
        inject_permissions(&mut plan, &perms, None).unwrap();
        assert!(matches!(plan, OperatorPlan::Filter { .. }));
    }

    /// AND-folding must short-circuit to `False` if either operand is False
    /// (e.g. the user predicate already evaluated to false statically).
    #[test]
    fn and_with_false_collapses_to_false() {
        let user_pred = Predicate::False;
        let mut plan = OperatorPlan::Filter {
            input: Box::new(OperatorPlan::Scan {
                table: "thread".into(),
            }),
            predicate: user_pred,
        };
        let perms = perms_with(&[("thread", "published = true")]);
        inject_permissions(&mut plan, &perms, None).unwrap();
        if let OperatorPlan::Filter { predicate, .. } = plan {
            assert!(matches!(predicate, Predicate::False));
        } else {
            panic!("expected Filter");
        }
    }

    /// Empty perms HashMap behaves the same as missing entries: every scan
    /// default-denies (except `_00_*`).
    #[test]
    fn empty_perms_map_default_denies_user_tables() {
        let mut plan = OperatorPlan::Scan {
            table: "user_table".into(),
        };
        let perms: HashMap<String, String> = HashMap::new();
        let err = inject_permissions(&mut plan, &perms, None).unwrap_err();
        assert!(err.to_string().contains("default-deny"));
    }

    /// Subquery whose plan is itself a Filter+Scan: the existing inner Filter
    /// must be ANDed, not double-wrapped (mirrors the top-level case).
    #[test]
    fn subquery_existing_filter_is_anded() {
        let inner_filter = OperatorPlan::Filter {
            input: Box::new(OperatorPlan::Scan {
                table: "comment".into(),
            }),
            predicate: Predicate::Eq {
                field: Path::new("active"),
                value: json!(true),
            },
        };
        let projection = Projection::Subquery {
            alias: "comments".into(),
            plan: Box::new(inner_filter),
            parent_key: None,
        };
        let mut plan = OperatorPlan::Project {
            input: Box::new(OperatorPlan::Scan {
                table: "thread".into(),
            }),
            projections: vec![projection],
        };
        let perms = perms_with(&[("thread", "true"), ("comment", "author.id = $auth.id")]);
        let params = json!({"auth": {"id": "user:a"}});
        inject_permissions(&mut plan, &perms, Some(&params)).unwrap();

        if let OperatorPlan::Project { projections, .. } = &plan {
            if let Projection::Subquery { plan, .. } = &projections[0] {
                if let OperatorPlan::Filter { predicate, .. } = plan.as_ref() {
                    match predicate {
                        Predicate::And { predicates } => {
                            assert_eq!(predicates.len(), 2);
                            return;
                        }
                        other => panic!("expected And, got {other:?}"),
                    }
                }
            }
        }
        panic!("did not find expected nested Filter");
    }
}
