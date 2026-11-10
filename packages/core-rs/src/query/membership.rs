//! Pure membership decisions: what a server id-set read may do, and how the
//! batched `_00_list_ref` reads fold into per-query snapshots.

use std::collections::{BTreeMap, BTreeSet};

use crate::state::lifecycle::QueryPhase;
use crate::types::{RecordVersionArray, ServerViewMeta};
use crate::value::{Row, Value};

/// What a membership read may do with the id-set it got back.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MembershipOutcome {
    /// The set is the server's answer; commit it.
    Applied,
    /// The server's `_00_query` row is gone while we hold rows.
    ViewLost,
    /// The read proves nothing; keep what we have.
    Ignored,
}

/// The durable `_00_view` row: the last server-confirmed membership.
#[derive(Clone, Debug, PartialEq)]
pub struct DurableView {
    pub ids: RecordVersionArray,
    pub confirmed: bool,
}

pub fn parse_view_row(row: Option<&Row>) -> Option<DurableView> {
    let row = row?;
    let Some(Value::Array(_)) = row.get("ids") else {
        return None;
    };
    Some(DurableView {
        ids: crate::query::sql::decode_view_ids(row.get("ids")),
        confirmed: row.get("confirmed").and_then(Value::as_bool) == Some(true),
    })
}

/// "Resolved before": the server answered this query on this device at some
/// point. An unconfirmed empty row does not count.
pub fn is_resolved_before(view: Option<&DurableView>) -> bool {
    view.is_some_and(|v| !v.ids.is_empty() || v.confirmed)
}

pub fn meta_from_row(row: Option<&Value>) -> ServerViewMeta {
    let Some(Value::Object(row)) = row else {
        return ServerViewMeta::ABSENT;
    };
    let state = match row.get("state").and_then(Value::as_str) {
        Some(s @ ("materializing" | "ready")) => Some(s.to_string()),
        _ => None,
    };
    ServerViewMeta {
        present: true,
        row_count: row.get("rowCount").and_then(Value::as_f64).map(|f| f as i64),
        state,
    }
}

/// Collapse duplicate `(id, version)` pairs to one per id, keeping the highest
/// version (first-seen order).
pub fn dedupe_record_versions(pairs: RecordVersionArray) -> RecordVersionArray {
    if pairs.len() < 2 {
        return pairs;
    }
    let mut best: BTreeMap<&str, i64> = BTreeMap::new();
    for (id, v) in &pairs {
        let e = best.entry(id.as_str()).or_insert(*v);
        if *v > *e {
            *e = *v;
        }
    }
    if best.len() == pairs.len() {
        return pairs;
    }
    let mut seen = BTreeSet::new();
    pairs
        .iter()
        .filter(|(id, _)| seen.insert(id.as_str()))
        .map(|(id, _)| (id.clone(), best[id.as_str()]))
        .collect()
}

#[derive(Clone, Debug)]
pub struct MembershipDecisionInput<'a> {
    pub phase: QueryPhase,
    /// Number of ids currently held as membership.
    pub held: usize,
    pub remote_array: &'a RecordVersionArray,
    pub meta: Option<&'a ServerViewMeta>,
    pub verified_removal: bool,
}

/// Whether a server id-set may be applied. A NON-EMPTY set is always the
/// server's answer. An EMPTY set is believed only when the server stands
/// behind it: the `_00_query` row is present, `ready` (or pre-`state`), and
/// reports zero rows. A missing row while we hold membership is a lost view.
pub fn decide_membership_outcome(input: &MembershipDecisionInput<'_>) -> MembershipOutcome {
    if !input.remote_array.is_empty() || input.verified_removal {
        return MembershipOutcome::Applied;
    }
    match input.meta {
        Some(meta) if meta.present => {
            let known_empty = meta.row_count == Some(0) && meta.state.as_deref() != Some("materializing");
            if known_empty {
                MembershipOutcome::Applied
            } else {
                MembershipOutcome::Ignored
            }
        }
        _ => {
            if input.held > 0 || input.phase != QueryPhase::Cold {
                MembershipOutcome::ViewLost
            } else {
                MembershipOutcome::Ignored
            }
        }
    }
}

// ---- list_ref snapshots ----------------------------------------------------------------

/// One statement's answer as a snapshot reads it: `Failed` (an ERR, or no
/// result at all) is kept apart from `Missing`/a null result, which is the
/// server saying "no such row". Reading a failed statement as a missing row
/// turned every timeout under load into a lost view (whitepawn, 2026-09-16).
#[derive(Clone, Copy, Debug)]
pub enum Answer<'a> {
    Failed,
    Value(Option<&'a Value>),
}

impl<'a> Answer<'a> {
    pub fn of(res: Option<&'a crate::kernel::ports::StatementResult>) -> Self {
        match res {
            Some(r) if r.is_ok() => Answer::Value(r.value()),
            _ => Answer::Failed,
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct ListRefSnapshot {
    pub primary: RecordVersionArray,
    pub subquery: RecordVersionArray,
    pub meta: ServerViewMeta,
}

fn to_pairs(rows: &[Value]) -> RecordVersionArray {
    dedupe_record_versions(
        rows.iter()
            .filter_map(|r| {
                let o = r.as_object()?;
                Some((
                    o.get("out").map(Value::to_id_string).unwrap_or_default(),
                    o.get("version").and_then(Value::as_i64).unwrap_or(0),
                ))
            })
            .collect(),
    )
}

/// Fold the single-query batch (edges, meta, children) into a snapshot.
/// `None` when any of the three did not answer: half an answer is not one.
pub fn snapshot_from_single(items: Answer<'_>, meta_row: Answer<'_>, children: Answer<'_>) -> Option<ListRefSnapshot> {
    let Answer::Value(Some(Value::Array(items))) = items else {
        return None;
    };
    let Answer::Value(meta_row) = meta_row else {
        return None;
    };
    let Answer::Value(children) = children else {
        return None;
    };
    Some(ListRefSnapshot {
        primary: to_pairs(items),
        subquery: children.and_then(Value::as_array).map(|c| to_pairs(c)).unwrap_or_default(),
        meta: meta_from_row(meta_row),
    })
}

/// Fold the many-query batch into one snapshot per hash. Every hash in
/// `hash_by_id` gets a snapshot; a query whose row did not come back reads
/// `present: false`. Empty when either statement did not answer.
pub fn snapshots_from_batch(
    edges: Answer<'_>,
    counts: Answer<'_>,
    hash_by_id: &BTreeMap<String, String>,
) -> BTreeMap<String, ListRefSnapshot> {
    let mut out = BTreeMap::new();
    let (Answer::Value(Some(Value::Array(edges))), Answer::Value(counts)) = (edges, counts) else {
        return out;
    };
    for hash in hash_by_id.values() {
        out.insert(hash.clone(), ListRefSnapshot::default());
    }
    let mut primary: BTreeMap<String, RecordVersionArray> = BTreeMap::new();
    let mut subquery: BTreeMap<String, RecordVersionArray> = BTreeMap::new();
    for row in edges {
        let Some(o) = row.as_object() else { continue };
        let Some(hash) = o.get("in").and_then(|v| hash_by_id.get(&v.to_id_string())) else {
            continue;
        };
        let pair = (
            o.get("out").map(Value::to_id_string).unwrap_or_default(),
            o.get("version").and_then(Value::as_i64).unwrap_or(0),
        );
        if o.get("parent").is_none_or(Value::is_nullish) {
            primary.entry(hash.clone()).or_default().push(pair);
        } else {
            subquery.entry(hash.clone()).or_default().push(pair);
        }
    }
    for (hash, snap) in out.iter_mut() {
        snap.primary = dedupe_record_versions(primary.remove(hash).unwrap_or_default());
        snap.subquery = dedupe_record_versions(subquery.remove(hash).unwrap_or_default());
    }
    if let Some(Value::Array(counts)) = counts {
        for count in counts {
            let Some(o) = count.as_object() else { continue };
            let Some(id) = o.get("id").filter(|v| !v.is_nullish()) else {
                continue;
            };
            if let Some(hash) = hash_by_id.get(&id.to_id_string()) {
                if let Some(snap) = out.get_mut(hash) {
                    snap.meta = meta_from_row(Some(count));
                }
            }
        }
    }
    out
}

/// A batch that must not be believed at face value: a query we hold rows for
/// whose `_00_query` row did not come back, or no edge at all while the server
/// still reports rows for a held query. Such hashes are re-read one at a time.
pub fn suspect_hashes(
    snapshots: &BTreeMap<String, ListRefSnapshot>,
    held_counts: &BTreeMap<String, usize>,
    edge_count: usize,
) -> Vec<String> {
    snapshots
        .iter()
        .filter(|(hash, snap)| {
            let held = held_counts.get(*hash).copied().unwrap_or(0);
            held > 0 && (!snap.meta.present || (edge_count == 0 && snap.meta.row_count.unwrap_or(0) > 0))
        })
        .map(|(h, _)| h.clone())
        .collect()
}

/// A stored `_00_view` row as the orphan collector sees it.
#[derive(Clone, Debug, PartialEq)]
pub struct ViewIndexRow {
    /// The view key; `None` when the row carries no id (never retired).
    pub key: Option<String>,
    /// Every id it vouches for (members and subquery children).
    pub ids: BTreeSet<String>,
    /// When a server answer last wrote it.
    pub updated_at: i64,
}

pub fn parse_view_index_row(row: &Row) -> ViewIndexRow {
    let key = row
        .get("id")
        .filter(|v| !v.is_nullish())
        .map(|v| {
            let id = v.to_id_string();
            match id.split_once(':') {
                Some((_, k)) => k.to_string(),
                None => id,
            }
        });
    let mut ids = BTreeSet::new();
    for field in ["ids", "children"] {
        if let Some(Value::Array(items)) = row.get(field) {
            for entry in items {
                if let Some(first) = entry.as_array().and_then(|a| a.first()) {
                    ids.insert(first.to_id_string());
                }
            }
        }
    }
    ViewIndexRow {
        key,
        ids,
        updated_at: row.get("updatedAt").and_then(Value::as_f64).map(|f| f as i64).unwrap_or(0),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::value::object_from_json;
    use serde_json::json;

    fn meta(present: bool, rows: Option<i64>, state: Option<&str>) -> ServerViewMeta {
        ServerViewMeta {
            present,
            row_count: rows,
            state: state.map(str::to_string),
        }
    }

    #[test]
    fn decisions() {
        let empty = vec![];
        let some = vec![("t:1".to_string(), 1)];
        let d = |phase, held, arr, m: Option<&ServerViewMeta>| {
            decide_membership_outcome(&MembershipDecisionInput {
                phase,
                held,
                remote_array: arr,
                meta: m,
                verified_removal: false,
            })
        };
        assert_eq!(d(QueryPhase::Cold, 0, &some, None), MembershipOutcome::Applied);
        assert_eq!(d(QueryPhase::Cold, 0, &empty, None), MembershipOutcome::Ignored);
        assert_eq!(d(QueryPhase::Live, 0, &empty, None), MembershipOutcome::ViewLost);
        let ready0 = meta(true, Some(0), Some("ready"));
        assert_eq!(d(QueryPhase::Live, 3, &empty, Some(&ready0)), MembershipOutcome::Applied);
        let mat = meta(true, Some(0), Some("materializing"));
        assert_eq!(d(QueryPhase::Live, 3, &empty, Some(&mat)), MembershipOutcome::Ignored);
        let rows = meta(true, Some(4), None);
        assert_eq!(d(QueryPhase::Live, 3, &empty, Some(&rows)), MembershipOutcome::Ignored);
    }

    #[test]
    fn single_snapshot_requires_all_three() {
        let edges = Value::from(json!([{"out": "t:1", "version": 2}, {"out": "t:1", "version": 3}]));
        let m = Value::from(json!({"rowCount": 1, "state": "ready"}));
        let s = snapshot_from_single(Answer::Value(Some(&edges)), Answer::Value(Some(&m)), Answer::Value(None)).unwrap();
        assert_eq!(s.primary, vec![("t:1".to_string(), 3)]);
        assert!(s.meta.present);
        assert!(snapshot_from_single(Answer::Value(Some(&edges)), Answer::Failed, Answer::Value(None)).is_none());
        let absent = snapshot_from_single(Answer::Value(Some(&edges)), Answer::Value(None), Answer::Value(None)).unwrap();
        assert!(!absent.meta.present);
    }

    #[test]
    fn batch_snapshot() {
        let edges = Value::from(json!([
            {"in": "_00_query:h1", "out": "t:1", "version": 1},
            {"in": "_00_query:h1", "out": "u:1", "version": 1, "parent": "t:1"},
            {"in": "_00_query:h9", "out": "t:9", "version": 1}
        ]));
        let counts = Value::from(json!([{"id": "_00_query:h1", "rowCount": 1, "state": "ready"}]));
        let mut by_id = BTreeMap::new();
        by_id.insert("_00_query:h1".to_string(), "h1".to_string());
        by_id.insert("_00_query:h2".to_string(), "h2".to_string());
        let snaps = snapshots_from_batch(Answer::Value(Some(&edges)), Answer::Value(Some(&counts)), &by_id);
        assert_eq!(snaps["h1"].primary.len(), 1);
        assert_eq!(snaps["h1"].subquery.len(), 1);
        assert!(snaps["h1"].meta.present);
        assert!(!snaps["h2"].meta.present);
        let mut held = BTreeMap::new();
        held.insert("h2".to_string(), 3usize);
        assert_eq!(suspect_hashes(&snaps, &held, 3), vec!["h2".to_string()]);
    }

    #[test]
    fn view_index_row() {
        let row = object_from_json(json!({"id": "_00_view:abc", "ids": [["t:1", 1]], "children": [["u:1", 2]], "updatedAt": 9}));
        let v = parse_view_index_row(&row);
        assert_eq!(v.key.as_deref(), Some("abc"));
        assert_eq!(v.ids.len(), 2);
        assert_eq!(v.updated_at, 9);
    }
}
