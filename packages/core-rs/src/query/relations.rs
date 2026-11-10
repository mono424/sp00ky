//! `.related()` resolution from the local cache.
//!
//! PORT: `packages/spooky_core/lib/src/query/relation_resolver.dart`.

use std::collections::{BTreeMap, BTreeSet};

use crate::error::{Error, Result};
use crate::kernel::Ctx;
use crate::modules::query_builder::RelationPlan;
use crate::utils::{sort_rows, stable_key};
use crate::value::{Object, Row, Value};

/// Nesting depth beyond which a relation tree is treated as cyclic (TS
/// `MAX_RELATION_DEPTH`).
pub const MAX_RELATION_DEPTH: usize = 12;

/// The error a `.related()` tree nesting past [`MAX_RELATION_DEPTH`] fails
/// with (TS `RelationCycleError`).
pub fn relation_cycle_error(path: &[String]) -> Error {
    Error::Other(format!(
        "RelationCycleError: relation nesting exceeded {MAX_RELATION_DEPTH} at {}",
        path.join(" -> ")
    ))
}

/// Fetches the candidate child rows of one relation level.
pub trait RelationFetcher {
    /// Rows of `table` whose `match_field` is one of `keys`.
    fn fetch_relation(&self, table: &str, match_field: &str, keys: &[Value]) -> Result<Vec<Row>>;
}

/// [`RelationFetcher`] over the local store, through the saga context.
///
/// Divergence from the browser core, which pushes the match into a local
/// SurrealQL query: this scans the logical table and filters here. The local
/// store is keyed only by `(table, id)`, so a field match has no index to use
/// either way. Fine for a client-sized cache.
pub struct CtxRelationFetcher<'a>(pub &'a Ctx);

impl RelationFetcher for CtxRelationFetcher<'_> {
    fn fetch_relation(&self, table: &str, match_field: &str, keys: &[Value]) -> Result<Vec<Row>> {
        let wanted: BTreeSet<String> = keys.iter().map(|k| stable_key(Some(k))).collect();
        let rows = self.0.local_get_all(table)?;
        Ok(rows
            .into_iter()
            .filter(|row| wanted.contains(&stable_key(row.get(match_field))))
            .collect())
    }
}

/// Resolve a query's `.related()` tree by level-ordered, batched fan-out: the
/// engine-neutral replacement for SurrealQL's nested `(SELECT ...) AS alias`
/// projections. One fetch per relation per level, never per row.
///
/// Correlation mirrors the emitted subquery:
/// - `one`: `parent[foreign_key_field] == child.id`, attach the first match or null
/// - `many`: `child[foreign_key_field] == parent.id`, attach the list
///
/// `parents` is mutated in place. Fails with [`relation_cycle_error`] when
/// nesting exceeds [`MAX_RELATION_DEPTH`].
pub fn resolve_relations(
    parents: &mut [Row],
    relations: &[RelationPlan],
    fetcher: &dyn RelationFetcher,
) -> Result<()> {
    resolve_at(parents, relations, fetcher, 0, &[])
}

fn resolve_at(
    parents: &mut [Row],
    relations: &[RelationPlan],
    fetcher: &dyn RelationFetcher,
    depth: usize,
    path: &[String],
) -> Result<()> {
    if relations.is_empty() || parents.is_empty() {
        return Ok(());
    }
    if depth >= MAX_RELATION_DEPTH {
        let mut full = path.to_vec();
        full.push(
            relations
                .iter()
                .map(|r| r.alias.as_str())
                .collect::<Vec<_>>()
                .join("|"),
        );
        return Err(relation_cycle_error(&full));
    }
    for relation in relations {
        resolve_one(parents, relation, fetcher, depth, path)?;
    }
    Ok(())
}

fn resolve_one(
    parents: &mut [Row],
    relation: &RelationPlan,
    fetcher: &dyn RelationFetcher,
    depth: usize,
    path: &[String],
) -> Result<()> {
    let is_one = relation.is_one();
    // The child field to match parent keys against.
    let match_field = if is_one {
        "id"
    } else {
        relation.foreign_key_field.as_str()
    };
    let parent_key_field = if is_one {
        relation.foreign_key_field.as_str()
    } else {
        "id"
    };
    let parent_key = |parent: &Row| {
        parent
            .get(parent_key_field)
            .filter(|v| !v.is_nullish())
            .cloned()
    };

    // Distinct, non-null correlation keys: an absent foreign key contributes
    // nothing, so it never triggers a spurious match-everything fetch.
    let mut keys: BTreeMap<String, Value> = BTreeMap::new();
    for parent in parents.iter() {
        if let Some(key) = parent_key(parent) {
            keys.entry(stable_key(Some(&key))).or_insert(key);
        }
    }

    let mut grouped: BTreeMap<String, Vec<Row>> = BTreeMap::new();
    if !keys.is_empty() {
        let keys: Vec<Value> = keys.into_values().collect();
        let mut children = fetcher.fetch_relation(&relation.table, match_field, &keys)?;
        // Recurse BEFORE grouping: each child is itself a parent for its own
        // relations, and resolving the deduped child set once keeps nested
        // fan-out at O(depth) batches. Children later dropped by a per-parent
        // limit carry resolved nested data harmlessly.
        let mut child_path = path.to_vec();
        child_path.push(relation.alias.clone());
        resolve_at(
            &mut children,
            &relation.relations,
            fetcher,
            depth + 1,
            &child_path,
        )?;
        for child in children {
            grouped
                .entry(stable_key(child.get(match_field)))
                .or_default()
                .push(child);
        }
    }

    for parent in parents.iter_mut() {
        let mut bucket: Vec<Row> = match parent_key(parent) {
            Some(key) => grouped
                .get(&stable_key(Some(&key)))
                .cloned()
                .unwrap_or_default(),
            None => Vec::new(),
        };
        // Filter, then order, then limit, PER PARENT: a shared batch fetch
        // cannot express "top N per parent".
        if !relation.where_.is_empty() {
            bucket.retain(|row| matches_where(row, &relation.where_));
        }
        if !relation.order_by.is_empty() {
            sort_rows(&mut bucket, &relation.order_by);
        }
        if let Some(limit) = relation.limit {
            bucket.truncate(limit);
        }
        let value = if is_one {
            bucket
                .into_iter()
                .next()
                .map(Value::Object)
                .unwrap_or(Value::Null)
        } else {
            Value::Array(bucket.into_iter().map(Value::Object).collect())
        };
        parent.insert(relation.alias.clone(), value);
    }
    Ok(())
}

fn matches_where(row: &Row, where_: &Object) -> bool {
    where_
        .iter()
        .all(|(k, v)| stable_key(row.get(k)) == stable_key(Some(v)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::row;
    use crate::types::Cardinality;
    use serde_json::json;
    use std::cell::Cell;

    /// A fetcher straight over a table map, counting its calls.
    struct MapFetcher {
        tables: BTreeMap<String, Vec<Row>>,
        calls: Cell<usize>,
    }

    impl RelationFetcher for MapFetcher {
        fn fetch_relation(
            &self,
            table: &str,
            match_field: &str,
            keys: &[Value],
        ) -> Result<Vec<Row>> {
            self.calls.set(self.calls.get() + 1);
            let wanted: BTreeSet<String> = keys.iter().map(|k| stable_key(Some(k))).collect();
            Ok(self
                .tables
                .get(table)
                .map(|rows| {
                    rows.iter()
                        .filter(|r| wanted.contains(&stable_key(r.get(match_field))))
                        .cloned()
                        .collect()
                })
                .unwrap_or_default())
        }
    }

    fn fetcher() -> MapFetcher {
        let mut tables: BTreeMap<String, Vec<Row>> = BTreeMap::new();
        tables.insert(
            "user".into(),
            vec![
                row(json!({"id": "user:u1", "name": "Ada"})),
                row(json!({"id": "user:u2", "name": "Linus"})),
            ],
        );
        tables.insert(
            "thread".into(),
            vec![
                row(json!({"id": "thread:t1", "title": "first", "author": "user:u1"})),
                row(json!({"id": "thread:t2", "title": "second", "author": "user:u2"})),
                row(json!({"id": "thread:t3", "title": "orphan"})),
            ],
        );
        tables.insert(
            "comment".into(),
            [("comment:c1", "thread:t1", 3, "user:u1"), ("comment:c2", "thread:t1", 1, "user:u2"), ("comment:c3", "thread:t1", 2, "user:u1"), ("comment:c4", "thread:t2", 9, "user:u2")]
                .into_iter()
                .map(|(id, thread, score, author)| row(json!({"id": id, "body": id, "thread": thread, "score": score, "author": author})))
                .collect(),
        );
        MapFetcher {
            tables,
            calls: Cell::new(0),
        }
    }

    fn threads(f: &MapFetcher, ids: &[&str]) -> Vec<Row> {
        ids.iter()
            .map(|id| {
                f.tables["thread"]
                    .iter()
                    .find(|r| r["id"] == Value::from(*id))
                    .unwrap()
                    .clone()
            })
            .collect()
    }

    fn plan(alias: &str, table: &str, cardinality: Cardinality, fk: &str) -> RelationPlan {
        RelationPlan {
            alias: alias.into(),
            table: table.into(),
            cardinality,
            foreign_key_field: fk.into(),
            select: Vec::new(),
            where_: Object::new(),
            order_by: Vec::new(),
            limit: None,
            relations: Vec::new(),
        }
    }

    fn child_ids(v: &Value) -> Vec<String> {
        v.as_array()
            .unwrap()
            .iter()
            .map(|c| c.get("id").unwrap().to_id_string())
            .collect()
    }

    #[test]
    fn a_one_relation_attaches_the_single_row_or_null() {
        let f = fetcher();
        let mut rows = threads(&f, &["thread:t1", "thread:t3"]);
        resolve_relations(
            &mut rows,
            &[plan("author", "user", Cardinality::One, "author")],
            &f,
        )
        .unwrap();
        assert_eq!(rows[0]["author"].get("name"), Some(&Value::from("Ada")));
        assert_eq!(
            rows[1]["author"],
            Value::Null,
            "no foreign key attaches null"
        );
        assert_eq!(f.calls.get(), 1, "one fetch per relation per level");
    }

    #[test]
    fn a_many_relation_attaches_matching_children_only() {
        let f = fetcher();
        let mut rows = threads(&f, &["thread:t1", "thread:t2", "thread:t3"]);
        resolve_relations(
            &mut rows,
            &[plan("comments", "comment", Cardinality::Many, "thread")],
            &f,
        )
        .unwrap();
        assert_eq!(
            child_ids(&rows[0]["comments"]),
            ["comment:c1", "comment:c2", "comment:c3"]
        );
        assert_eq!(child_ids(&rows[1]["comments"]), ["comment:c4"]);
        assert_eq!(rows[2]["comments"], Value::Array(vec![]));
    }

    #[test]
    fn order_and_limit_apply_per_parent() {
        let f = fetcher();
        let mut rows = threads(&f, &["thread:t1", "thread:t2"]);
        let mut p = plan("comments", "comment", Cardinality::Many, "thread");
        p.order_by = vec![("score".into(), "desc".into())];
        p.limit = Some(2);
        resolve_relations(&mut rows, &[p.clone()], &f).unwrap();
        // Top 2 of thread:t1 by score, not the global top 2 (which would hold c4).
        assert_eq!(
            child_ids(&rows[0]["comments"]),
            ["comment:c1", "comment:c3"]
        );
        assert_eq!(child_ids(&rows[1]["comments"]), ["comment:c4"]);

        // A direction spelled as the builder emits it (DESC) still sorts down.
        let mut rows = threads(&f, &["thread:t1"]);
        p.order_by = vec![("score".into(), "DESC".into())];
        p.limit = Some(1);
        resolve_relations(&mut rows, &[p], &f).unwrap();
        assert_eq!(child_ids(&rows[0]["comments"]), ["comment:c1"]);
    }

    #[test]
    fn a_sub_where_filters_the_children() {
        let f = fetcher();
        let mut rows = threads(&f, &["thread:t1"]);
        let mut p = plan("comments", "comment", Cardinality::Many, "thread");
        p.where_.insert(
            "author".into(),
            Value::RecordId(crate::value::RecordId::new("user", "u1")),
        );
        resolve_relations(&mut rows, &[p], &f).unwrap();
        assert_eq!(
            child_ids(&rows[0]["comments"]),
            ["comment:c1", "comment:c3"]
        );
    }

    #[test]
    fn nested_relations_resolve_a_second_level() {
        let f = fetcher();
        let mut rows = threads(&f, &["thread:t1"]);
        let mut p = plan("comments", "comment", Cardinality::Many, "thread");
        p.order_by = vec![("score".into(), "asc".into())];
        p.relations = vec![plan("author", "user", Cardinality::One, "author")];
        resolve_relations(&mut rows, &[p], &f).unwrap();
        let first = &rows[0]["comments"].as_array().unwrap()[0];
        assert_eq!(
            first.get("author").and_then(|a| a.get("name")),
            Some(&Value::from("Linus"))
        );
        assert_eq!(f.calls.get(), 2);
    }

    #[test]
    fn nesting_past_the_depth_cap_fails() {
        // Self-join on `id`, so every level keeps matching and the recursion
        // can actually reach the cap.
        fn chain(depth: usize) -> RelationPlan {
            let mut p = plan("self", "thread", Cardinality::One, "id");
            if depth > 0 {
                p.relations = vec![chain(depth - 1)];
            }
            p
        }
        let f = fetcher();
        let err = resolve_relations(
            &mut threads(&f, &["thread:t1"]),
            &[chain(MAX_RELATION_DEPTH)],
            &f,
        )
        .unwrap_err();
        assert!(err.to_string().starts_with("RelationCycleError"), "{err}");
        resolve_relations(
            &mut threads(&f, &["thread:t1"]),
            &[chain(MAX_RELATION_DEPTH - 2)],
            &f,
        )
        .unwrap();
    }

    #[test]
    fn an_empty_parent_set_or_plan_is_a_no_op() {
        let f = fetcher();
        resolve_relations(
            &mut [],
            &[plan("author", "user", Cardinality::One, "author")],
            &f,
        )
        .unwrap();
        let mut rows = threads(&f, &["thread:t1"]);
        resolve_relations(&mut rows, &[], &f).unwrap();
        assert_eq!(rows[0]["author"], Value::from("user:u1"));
        assert_eq!(f.calls.get(), 0);
    }
}
