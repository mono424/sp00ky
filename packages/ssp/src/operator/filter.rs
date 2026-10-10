use crate::algebra::ZSet;
use crate::circuit::store::Store;
use crate::eval::value_ops::{compare_values, resolve_field};
use crate::eval::value_ref::ValueRef;
use crate::operator::predicate::Predicate;
use crate::types::{Path, Sp00kyValue};
use serde_json::Value;
use std::cmp::Ordering;
use std::collections::HashMap;

/// Filter operator: selects records matching a predicate.
///
/// Stateless (arity 1). The delta rule is identical to the snapshot rule:
/// `delta_out = filter(delta_in, predicate)`
///
/// This works because filter is a linear operator in DBSP:
/// `filter(A + B) = filter(A) + filter(B)`
#[derive(Debug)]
pub struct Filter {
    pub predicate: Predicate,
}

impl Filter {
    pub fn new(predicate: Predicate) -> Self {
        Self { predicate }
    }

    fn check_predicate(&self, key: &str, store: &Store, ctx: Option<&Sp00kyValue>) -> bool {
        check_predicate_recursive(&self.predicate, key, store, ctx)
    }
}

impl super::Operator for Filter {
    fn snapshot(&self, inputs: &[&ZSet], store: &Store, ctx: Option<&Sp00kyValue>) -> ZSet {
        let upstream = inputs[0];
        let mut out = HashMap::new();
        for (key, &weight) in upstream.iter() {
            if self.check_predicate(key, store, ctx) {
                out.insert(key.clone(), weight);
            }
        }
        out
    }

    fn step(
        &mut self,
        input_deltas: &[&ZSet],
        store: &Store,
        ctx: Option<&Sp00kyValue>,
    ) -> ZSet {
        // Filter is stateless: delta rule = apply predicate to delta
        self.snapshot(input_deltas, store, ctx)
    }

    fn arity(&self) -> usize {
        1
    }

    fn reset(&mut self) {}

    fn evaluate_key(
        &self,
        key: &str,
        input_evals: &[bool],
        store: &Store,
        ctx: Option<&Sp00kyValue>,
    ) -> bool {
        // Filter is the membership-changing operator that breaks
        // cross-user Update propagation: the upstream Scan emits
        // nothing for Operation::Update, so the standard delta path
        // never re-evaluates the predicate. Here we check both that
        // the key is reachable upstream AND that the predicate holds
        // on the current row data.
        input_evals.first().copied().unwrap_or(false)
            && self.check_predicate(key, store, ctx)
    }
}

/// Resolve a predicate value, handling $param references.
///
/// Called per row, per predicate, per step, and allocates every time: a
/// literal is converted out of `serde_json::Value` afresh, and a `$param`
/// reference builds a `Path` (a `Vec<String>`) before cloning what it
/// resolves to. Both results are row-independent — the literal is fixed at
/// plan time and `ctx` is fixed per view — so this is pure repeated work.
///
/// Hoisting it needs a compiled predicate tree carrying pre-converted
/// operands, which is deferred to the `ValueRef` migration that rewrites these
/// call sites anyway.
pub(crate) fn resolve_predicate_value(value: &Value, ctx: Option<&Sp00kyValue>) -> Option<Sp00kyValue> {
    if let Some(obj) = value.as_object() {
        if let Some(param_path) = obj.get("$param") {
            let ctx = ctx?;
            let path_str = param_path.as_str().unwrap_or("");
            let effective_path = if let Some(rest) = path_str.strip_prefix("parent.") {
                rest
            } else {
                path_str
            };
            let path = Path::new(effective_path);
            let resolved = resolve_field(ValueRef::from_value(ctx), &path);
            // An unresolvable `$param` yields None so the caller fails closed,
            // which is why this is not simply `to_owned_value()` (that maps
            // Missing to Null and would compare as a real value).
            if resolved.is_missing() {
                None
            } else {
                Some(resolved.to_owned_value())
            }
        } else {
            Some(Sp00kyValue::from(value.clone()))
        }
    } else {
        Some(Sp00kyValue::from(value.clone()))
    }
}

pub(crate) fn check_predicate_recursive(
    pred: &Predicate,
    key: &str,
    store: &Store,
    ctx: Option<&Sp00kyValue>,
) -> bool {
    match pred {
        Predicate::True => true,
        Predicate::False => false,
        Predicate::And { predicates } => predicates
            .iter()
            .all(|p| check_predicate_recursive(p, key, store, ctx)),
        Predicate::Or { predicates } => predicates
            .iter()
            .any(|p| check_predicate_recursive(p, key, store, ctx)),
        Predicate::Prefix { field, prefix } => {
            if field.segments().len() == 1 && field.segments()[0] == "id" {
                return key.starts_with(prefix.as_str());
            }
            let row = store.get_row_by_key_or_deleted(key);
            match resolve_field(row, field).as_str() {
                Some(s) => s.starts_with(prefix.as_str()),
                None => false,
            }
        }
        Predicate::Eq { field, value }
        | Predicate::Neq { field, value }
        | Predicate::Gt { field, value }
        | Predicate::Gte { field, value }
        | Predicate::Lt { field, value }
        | Predicate::Lte { field, value } => {
            let target = match resolve_predicate_value(value, ctx) {
                Some(v) => v,
                None => return false,
            };

            // `id` is the record KEY, not a body field — the row object carries
            // no "id" entry, so `resolve_field` returns None and every
            // `WHERE id = <record>` comparison would fail (threads never match,
            // so a parent-filtered detail query registers an empty view and its
            // reverse subqueries — e.g. `comments` — never sync). Mirror the
            // `Prefix` branch above and compare against the key directly.
            //
            // The resolved field is BORROWED, not cloned. `compare_values`
            // only needs a reference, and the previous `.cloned()` deep-copied
            // whatever the path landed on — a whole nested object or array if
            // the predicate targeted one — once per row, per predicate, per
            // step. Only the `id` branch needs to own anything, and only
            // because it synthesizes a value that isn't in the row.
            let actual = if field.segments().len() == 1 && field.segments()[0] == "id" {
                ValueRef::Str(key)
            } else {
                resolve_field(store.get_row_by_key_or_deleted(key), field)
            };

            if actual.is_missing() {
                return false;
            }
            let target = ValueRef::from_value(&target);
            let ord = compare_values(actual, target);
            match pred {
                // Equality needs the same kind of value, as SurrealDB has it:
                // `compare_values` is an ordering and folds a type mismatch to
                // `Equal`, which made `7 = "x"` hold, so a filter on a field
                // matched every row holding another type there.
                Predicate::Eq { .. } => ord == Ordering::Equal && same_kind(actual, target),
                Predicate::Neq { .. } => ord != Ordering::Equal || !same_kind(actual, target),
                Predicate::Gt { .. } => ord == Ordering::Greater,
                Predicate::Gte { .. } => ord != Ordering::Less,
                Predicate::Lt { .. } => ord == Ordering::Less,
                Predicate::Lte { .. } => ord != Ordering::Greater,
                _ => false,
            }
        }
        Predicate::ParamEq { param, value }
        | Predicate::ParamNeq { param, value }
        | Predicate::ParamGt { param, value }
        | Predicate::ParamGte { param, value }
        | Predicate::ParamLt { param, value }
        | Predicate::ParamLte { param, value } => {
            // LHS is a $param: resolve from ctx the same way RHS $param refs are
            // resolved in `resolve_predicate_value`. RHS may itself be a literal or
            // a $param.
            let left = match resolve_param_lookup(param, ctx) {
                Some(v) => v,
                None => return false,
            };
            let right = match resolve_predicate_value(value, ctx) {
                Some(v) => v,
                None => return false,
            };
            let ord = compare_values(ValueRef::from_value(&left), ValueRef::from_value(&right));
            match pred {
                Predicate::ParamEq { .. } => ord == Ordering::Equal,
                Predicate::ParamNeq { .. } => ord != Ordering::Equal,
                Predicate::ParamGt { .. } => ord == Ordering::Greater,
                Predicate::ParamGte { .. } => ord != Ordering::Less,
                Predicate::ParamLt { .. } => ord == Ordering::Less,
                Predicate::ParamLte { .. } => ord != Ordering::Greater,
                _ => false,
            }
        }
    }
}

/// Whether two values are the same kind of value, numbers counting as one
/// kind (`5 = 5.0`).
fn same_kind(a: ValueRef<'_>, b: ValueRef<'_>) -> bool {
    matches!(
        (a, b),
        (ValueRef::Missing, ValueRef::Missing)
            | (ValueRef::Null, ValueRef::Null)
            | (ValueRef::Bool(_), ValueRef::Bool(_))
            | (ValueRef::Int(_) | ValueRef::Float(_), ValueRef::Int(_) | ValueRef::Float(_))
            | (ValueRef::Str(_), ValueRef::Str(_))
            | (ValueRef::Arr(_), ValueRef::Arr(_))
            | (ValueRef::Obj(_), ValueRef::Obj(_))
    )
}

/// Look up a `$param` reference (e.g. "auth.id", "access") against the
/// per-registration context. Mirrors the RHS resolution path in
/// `resolve_predicate_value` but takes the already-extracted param name.
fn resolve_param_lookup(param: &str, ctx: Option<&Sp00kyValue>) -> Option<Sp00kyValue> {
    let ctx = ctx?;
    let effective_path = param.strip_prefix("parent.").unwrap_or(param);
    let resolved = resolve_field(ValueRef::from_value(ctx), &Path::new(effective_path));
    if resolved.is_missing() {
        None
    } else {
        Some(resolved.to_owned_value())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::operator::Operator;
    use crate::circuit::store::Change;
    use serde_json::json;

    fn zset(items: &[(&str, i64)]) -> ZSet {
        items.iter().map(|(k, w)| ((*k).into(), *w)).collect()
    }

    #[test]
    fn equality_never_holds_across_value_types() {
        let mut store = Store::new();
        store.ensure_collection("g");
        store.apply_change(&Change::create("g", "a", json!({ "db": 7 })));
        store.apply_change(&Change::create("g", "b", json!({ "db": "game_database:x" })));
        store.apply_change(&Change::create("g", "c", json!({ "db": { "tb": "game_database" } })));
        store.apply_change(&Change::create("g", "d", json!({ "db": 5.0 })));
        let input = zset(&[("g:a", 1), ("g:b", 1), ("g:c", 1), ("g:d", 1)]);
        let keys = |pred: Predicate| {
            let mut keys: Vec<String> = Filter::new(pred).snapshot(&[&input], &store, None).keys().map(|k| k.to_string()).collect();
            keys.sort();
            keys
        };
        assert_eq!(keys(Predicate::Eq { field: Path::new("db"), value: json!("game_database:x") }), vec!["g:b"]);
        assert_eq!(keys(Predicate::Neq { field: Path::new("db"), value: json!("game_database:x") }), vec!["g:a", "g:c", "g:d"]);
        // Numbers are one kind.
        assert_eq!(keys(Predicate::Eq { field: Path::new("db"), value: json!(5) }), vec!["g:d"]);
    }

    #[test]
    fn snapshot_filters_matching_records() {
        let mut store = Store::new();
        store.ensure_collection("users");
        store.apply_change(&Change::create("users", "user:1", json!({"level": 10})));
        store.apply_change(&Change::create("users", "user:2", json!({"level": 3})));

        let pred = Predicate::Gte {
            field: Path::new("level"),
            value: json!(5),
        };
        let filter = Filter::new(pred);
        let input = zset(&[("users:1", 1), ("users:2", 1)]);

        let result = filter.snapshot(&[&input], &store, None);
        assert_eq!(result.get("users:1"), Some(&1));
        assert!(!result.contains_key("users:2"));
    }

    #[test]
    fn step_is_identical_to_snapshot() {
        let mut store = Store::new();
        store.ensure_collection("users");
        store.apply_change(&Change::create("users", "user:1", json!({"level": 10})));
        store.apply_change(&Change::create("users", "user:2", json!({"level": 3})));

        let pred = Predicate::Gte {
            field: Path::new("level"),
            value: json!(5),
        };
        let delta = zset(&[("users:1", 1), ("users:2", 1)]);

        let snap = Filter::new(pred.clone()).snapshot(&[&delta], &store, None);
        let incr = Filter::new(pred).step(&[&delta], &store, None);
        assert_eq!(snap, incr);
    }

    #[test]
    fn step_preserves_negative_weights() {
        let mut store = Store::new();
        store.ensure_collection("users");
        store.apply_change(&Change::create("users", "user:1", json!({"level": 10})));

        let pred = Predicate::Gte {
            field: Path::new("level"),
            value: json!(5),
        };
        let mut filter = Filter::new(pred);
        let delta = zset(&[("users:1", -1)]);

        let result = filter.step(&[&delta], &store, None);
        assert_eq!(result.get("users:1"), Some(&-1));
    }

    #[test]
    fn param_eq_matches_when_ctx_param_equals_literal() {
        // Mirrors `$access = "account"` on the LHS. The per-row context carries
        // the registration's params; for ParamEq the comparison is row-independent.
        let mut store = Store::new();
        store.ensure_collection("threads");
        store.apply_change(&Change::create(
            "threads",
            "thread:1",
            json!({"title": "t1"}),
        ));
        let pred = Predicate::ParamEq {
            param: "access".into(),
            value: json!("account"),
        };
        let filter = Filter::new(pred);
        let input = zset(&[("threads:1", 1)]);
        let ctx = Sp00kyValue::from(json!({"access": "account"}));
        let result = filter.snapshot(&[&input], &store, Some(&ctx));
        assert_eq!(result.get("threads:1"), Some(&1));
    }

    #[test]
    fn param_eq_filters_out_when_ctx_param_differs() {
        let mut store = Store::new();
        store.ensure_collection("threads");
        store.apply_change(&Change::create(
            "threads",
            "thread:1",
            json!({"title": "t1"}),
        ));
        let pred = Predicate::ParamEq {
            param: "access".into(),
            value: json!("account"),
        };
        let filter = Filter::new(pred);
        let input = zset(&[("threads:1", 1)]);
        let ctx = Sp00kyValue::from(json!({"access": "system"}));
        let result = filter.snapshot(&[&input], &store, Some(&ctx));
        assert!(!result.contains_key("threads:1"));
    }

    #[test]
    fn param_eq_falls_closed_when_ctx_missing() {
        let mut store = Store::new();
        store.ensure_collection("threads");
        store.apply_change(&Change::create(
            "threads",
            "thread:1",
            json!({"title": "t1"}),
        ));
        let pred = Predicate::ParamEq {
            param: "access".into(),
            value: json!("account"),
        };
        let filter = Filter::new(pred);
        let input = zset(&[("threads:1", 1)]);
        // No ctx at all => param can't be resolved => row drops out (fail-closed).
        let result = filter.snapshot(&[&input], &store, None);
        assert!(!result.contains_key("threads:1"));
    }

    #[test]
    fn param_eq_resolves_dotted_path() {
        // `$auth.id = "user:abc"` — dotted param paths must resolve through the
        // ctx object (mirrors how RHS `{"$param": "auth.id"}` already resolves).
        let mut store = Store::new();
        store.ensure_collection("threads");
        store.apply_change(&Change::create(
            "threads",
            "thread:1",
            json!({"title": "t1"}),
        ));
        let pred = Predicate::ParamEq {
            param: "auth.id".into(),
            value: json!("user:abc"),
        };
        let filter = Filter::new(pred);
        let input = zset(&[("threads:1", 1)]);
        let ctx = Sp00kyValue::from(json!({"auth": {"id": "user:abc"}}));
        let result = filter.snapshot(&[&input], &store, Some(&ctx));
        assert_eq!(result.get("threads:1"), Some(&1));
    }
}
