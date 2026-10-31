//! The admin plane's live protocol (`/admin/api/v2/live`).
//!
//! The dashboard subscribes to *topics* over one WebSocket. A topic is a piece
//! of server-side state, computed once however many people watch it, and sent
//! as a snapshot followed by small patches. This module is the pure half: the
//! wire messages and the diff/apply functions. Hosts (the scheduler, later the
//! SSP node) own the topic catalog and the socket.
//!
//! # Three shapes, one rule
//!
//! Every topic is one of three shapes, and the host never writes a delta by
//! hand: a producer computes the topic's next full (bounded) state and
//! [`diff`] derives the patch from the previous one. That keeps a producer a
//! pure function of what it reads, and keeps "the client's copy equals the
//! server's" a property this module can test once, instead of a promise every
//! producer has to keep.
//!
//! * [`Shape::Doc`]: a JSON object. The patch is an RFC 7396 merge patch.
//!   Merge patch spells "delete" as `null`, so a doc's canonical form has no
//!   `null` object members ([`canonical`]); a reader treats a missing member as
//!   null. Arrays are replaced whole, so a doc keeps large or keyed
//!   collections as objects keyed by id, where a change patches one entry.
//! * [`Shape::Window`]: `{rows: [...], meta: {...}}`, rows keyed by one field
//!   and kept in server order. The patch carries changed rows, removed keys,
//!   the new key order only when it changed, and a merge patch of `meta`.
//! * [`Shape::Series`]: `{items: [...], keep: n, meta: {...}}`, items keyed by
//!   a field that only grows. The patch appends; the reader trims to `keep`.
//!   A series that went backwards (a ring reset) cannot be patched, and
//!   [`diff`] says so with [`Delta::Reset`]: the host sends a snapshot.
//!
//! # Sequence numbers
//!
//! Every state a host publishes gets a `seq` from one counter per process,
//! and every patch names the `from` it applies on top of. A reader whose own
//! seq is not `from` has missed something and resubscribes with `since`, which
//! the host answers with the missing patches when it still has them and with
//! a snapshot otherwise. `boot` (in [`ServerMsg::Ready`]) changes when the
//! process does, and a `since` from another boot is never honoured.

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::collections::{HashMap, HashSet};

/// Version of the protocol this module speaks. Advertised by `GET /config` as
/// `live`, so a dashboard can tell a host it can subscribe to from one it
/// has to poll.
pub const PROTOCOL_VERSION: u32 = 1;

/// The WebSocket close code for "your session is gone or never was".
pub const CLOSE_UNAUTHORIZED: u16 = 4401;
/// Close code for a connection that did not authenticate in time.
pub const CLOSE_AUTH_TIMEOUT: u16 = 4408;
/// Close code for a message the host could not read, or a limit exceeded.
pub const CLOSE_PROTOCOL: u16 = 4400;
/// Close code for "the host is at its connection limit".
pub const CLOSE_BUSY: u16 = 4429;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Shape {
    Doc,
    Window,
    Series,
}

/* ------------------------------------------------------------------ */
/* Wire messages                                                        */
/* ------------------------------------------------------------------ */

/// What a dashboard sends.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
#[serde(tag = "op", rename_all = "lowercase")]
pub enum ClientMsg {
    /// First message on every connection, and again whenever the session is
    /// renewed (the host answers [`ServerMsg::Expiring`] ahead of expiry).
    /// The token never travels in the URL: query strings end up in proxy logs.
    Auth { token: String },
    /// Subscribe to `topic` with `args`. `ref` is the client's own handle,
    /// echoed in [`ServerMsg::Subscribed`]. `since` is the seq the client
    /// already holds for this topic, when resuming after a reconnect.
    Sub {
        #[serde(rename = "ref")]
        reference: String,
        topic: String,
        #[serde(default)]
        args: Value,
        #[serde(default)]
        since: Option<u64>,
    },
    /// Stop receiving `key`.
    Unsub { key: String },
    Ping,
}

/// The identity a connection is acting as, as the host reports it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SessionInfo {
    pub subject: String,
    pub label: String,
    pub mode: String,
    pub scope: String,
    /// Epoch seconds.
    pub expires_at: u64,
}

/// What a host sends.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "lowercase")]
pub enum ServerMsg {
    /// The connection is authenticated. `boot` identifies this host process;
    /// `topics` is what it can serve.
    Ready {
        boot: String,
        session: SessionInfo,
        topics: Vec<String>,
    },
    /// A `sub` was accepted. Several refs can share one key (same topic, same
    /// args); frames carry the key.
    Subscribed {
        #[serde(rename = "ref")]
        reference: String,
        key: String,
        shape: Shape,
    },
    /// The full state of `key` at `seq`.
    Snap { key: String, seq: u64, data: Value },
    /// The patch that turns the state at `from` into the state at `seq`.
    Diff {
        key: String,
        from: u64,
        seq: u64,
        patch: Value,
    },
    /// The session expires at `at` (epoch seconds); send a fresh `auth`.
    Expiring { at: u64 },
    Error {
        #[serde(rename = "ref", skip_serializing_if = "Option::is_none", default)]
        reference: Option<String>,
        code: String,
        message: String,
    },
    Pong,
}

/// A topic's instance key: the topic name, plus its arguments in canonical
/// form when it has any. Two subscriptions with the same key share one
/// producer.
pub fn topic_key(topic: &str, args: &Value) -> String {
    match args {
        Value::Null => topic.to_string(),
        Value::Object(map) if map.is_empty() => topic.to_string(),
        other => format!("{topic}?{}", canonical_json(other)),
    }
}

/// JSON with object keys sorted at every depth, so equal values print equal.
pub fn canonical_json(v: &Value) -> String {
    fn sorted(v: &Value) -> Value {
        match v {
            Value::Object(map) => {
                let mut keys: Vec<&String> = map.keys().collect();
                keys.sort();
                let mut out = Map::new();
                for k in keys {
                    out.insert(k.clone(), sorted(&map[k]));
                }
                Value::Object(out)
            }
            Value::Array(items) => Value::Array(items.iter().map(sorted).collect()),
            other => other.clone(),
        }
    }
    // `serde_json` keeps insertion order only with `preserve_order`; without
    // it maps are already sorted, and with it `sorted` makes them so.
    sorted(v).to_string()
}

/* ------------------------------------------------------------------ */
/* Diff and apply                                                       */
/* ------------------------------------------------------------------ */

/// What [`diff`] found.
#[derive(Debug, Clone, PartialEq)]
pub enum Delta {
    /// Nothing a reader could observe changed: send nothing.
    Unchanged,
    /// Send this patch.
    Patch(Value),
    /// The new state does not follow from the old one: send a snapshot.
    Reset,
}

/// Why [`apply`] refused a patch. A reader that gets one resubscribes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApplyError(pub String);

impl std::fmt::Display for ApplyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// The canonical form of a state of `shape`: what a reader holds after
/// applying patches, and so what a host must diff and snapshot.
///
/// Object members that are `null` are dropped at every depth (merge patch
/// cannot tell "set to null" from "delete"), including inside window rows and
/// series items, which are themselves objects.
pub fn canonical(v: Value) -> Value {
    match v {
        Value::Object(map) => Value::Object(
            map.into_iter()
                .filter(|(_, v)| !v.is_null())
                .map(|(k, v)| (k, canonical(v)))
                .collect(),
        ),
        Value::Array(items) => Value::Array(items.into_iter().map(canonical).collect()),
        other => other,
    }
}

/// The patch from `prev` to `next`, both canonical states of `shape`.
/// `key` names the row/item key field; docs ignore it.
pub fn diff(shape: Shape, key: &str, prev: &Value, next: &Value) -> Delta {
    if prev == next {
        return Delta::Unchanged;
    }
    match shape {
        Shape::Doc => match merge_diff(prev, next) {
            Some(patch) => Delta::Patch(patch),
            None => Delta::Unchanged,
        },
        Shape::Window => window_diff(key, prev, next),
        Shape::Series => series_diff(key, prev, next),
    }
}

/// Apply `patch` to `state` in place.
pub fn apply(shape: Shape, key: &str, state: &mut Value, patch: &Value) -> Result<(), ApplyError> {
    match shape {
        Shape::Doc => {
            merge_apply(state, patch);
            Ok(())
        }
        Shape::Window => window_apply(key, state, patch),
        Shape::Series => series_apply(key, state, patch),
    }
}

/* ---------- doc: RFC 7396 merge patch ---------- */

/// The merge patch turning `prev` into `next`, or `None` when they are equal.
/// Non-object values (and arrays) are replaced whole.
pub fn merge_diff(prev: &Value, next: &Value) -> Option<Value> {
    if prev == next {
        return None;
    }
    match (prev, next) {
        (Value::Object(a), Value::Object(b)) => {
            let mut patch = Map::new();
            for (k, old) in a {
                match b.get(k) {
                    None => {
                        patch.insert(k.clone(), Value::Null);
                    }
                    Some(new) => {
                        if let Some(p) = merge_diff(old, new) {
                            patch.insert(k.clone(), p);
                        }
                    }
                }
            }
            for (k, new) in b {
                if !a.contains_key(k) {
                    patch.insert(k.clone(), new.clone());
                }
            }
            if patch.is_empty() {
                None
            } else {
                Some(Value::Object(patch))
            }
        }
        // A new object over a non-object: the whole object is the patch, and
        // applying it onto anything non-object replaces it (RFC 7396 §2).
        _ => Some(next.clone()),
    }
}

/// RFC 7396 `MergePatch(target, patch)`, in place.
pub fn merge_apply(target: &mut Value, patch: &Value) {
    let Value::Object(patch) = patch else {
        *target = patch.clone();
        return;
    };
    if !target.is_object() {
        *target = Value::Object(Map::new());
    }
    let map = target.as_object_mut().expect("just made an object");
    for (k, v) in patch {
        if v.is_null() {
            map.remove(k);
        } else {
            let slot = map.entry(k.clone()).or_insert(Value::Null);
            merge_apply(slot, v);
        }
    }
}

/* ---------- window: keyed rows ---------- */

/// A row's key as a string: strings verbatim, anything else as JSON.
fn key_of(row: &Value, key: &str) -> Option<String> {
    match row.get(key)? {
        Value::String(s) => Some(s.clone()),
        Value::Null => None,
        other => Some(other.to_string()),
    }
}

fn rows_of(v: &Value) -> &[Value] {
    v.get("rows").and_then(Value::as_array).map(Vec::as_slice).unwrap_or(&[])
}

fn meta_of(v: &Value) -> Value {
    v.get("meta").cloned().unwrap_or_else(|| Value::Object(Map::new()))
}

fn window_diff(key: &str, prev: &Value, next: &Value) -> Delta {
    let old_rows = rows_of(prev);
    let new_rows = rows_of(next);

    let mut old_by_key: HashMap<String, &Value> = HashMap::with_capacity(old_rows.len());
    let mut old_order = Vec::with_capacity(old_rows.len());
    for row in old_rows {
        let Some(k) = key_of(row, key) else { return Delta::Reset };
        old_order.push(k.clone());
        old_by_key.insert(k, row);
    }

    let mut new_keys = HashSet::with_capacity(new_rows.len());
    let mut new_order = Vec::with_capacity(new_rows.len());
    let mut up = Vec::new();
    for row in new_rows {
        // A row without its key cannot be addressed by a later patch, and two
        // rows under one key cannot both be held: either way the window has to
        // travel whole.
        let Some(k) = key_of(row, key) else { return Delta::Reset };
        if !new_keys.insert(k.clone()) {
            return Delta::Reset;
        }
        if old_by_key.get(&k).map_or(true, |old| *old != row) {
            up.push(row.clone());
        }
        new_order.push(k);
    }
    let rm: Vec<String> = old_order.iter().filter(|k| !new_keys.contains(*k)).cloned().collect();

    let mut patch = Map::new();
    if !up.is_empty() {
        patch.insert("up".into(), Value::Array(up));
    }
    if !rm.is_empty() {
        patch.insert("rm".into(), Value::Array(rm.into_iter().map(Value::String).collect()));
    }
    // The order travels only when applying `up` and `rm` alone would not
    // reproduce it: survivors keep their places and new rows land at the end.
    if implied_order(&old_order, &new_keys, &new_order) != new_order {
        patch.insert(
            "order".into(),
            Value::Array(new_order.iter().cloned().map(Value::String).collect()),
        );
    }
    if let Some(m) = merge_diff(&meta_of(prev), &meta_of(next)) {
        patch.insert("meta".into(), m);
    }
    if patch.is_empty() {
        Delta::Unchanged
    } else {
        Delta::Patch(Value::Object(patch))
    }
}

/// The order a reader arrives at without an explicit `order`: the old order
/// minus removed keys, then keys it has not seen, in their new order.
fn implied_order(old_order: &[String], new_keys: &HashSet<String>, new_order: &[String]) -> Vec<String> {
    let kept: HashSet<&String> = old_order.iter().collect();
    let mut out: Vec<String> = old_order.iter().filter(|k| new_keys.contains(*k)).cloned().collect();
    out.extend(new_order.iter().filter(|k| !kept.contains(k)).cloned());
    out
}

fn window_apply(key: &str, state: &mut Value, patch: &Value) -> Result<(), ApplyError> {
    let Value::Object(patch) = patch else {
        return Err(ApplyError("window patch must be an object".into()));
    };
    let rows = rows_of(state).to_vec();
    let mut order: Vec<String> = Vec::with_capacity(rows.len());
    let mut by_key: HashMap<String, Value> = HashMap::with_capacity(rows.len());
    for row in rows {
        let k = key_of(&row, key).ok_or_else(|| ApplyError("row without a key".into()))?;
        order.push(k.clone());
        by_key.insert(k, row);
    }
    if let Some(rm) = patch.get("rm").and_then(Value::as_array) {
        let gone: HashSet<&str> = rm.iter().filter_map(Value::as_str).collect();
        order.retain(|k| !gone.contains(k.as_str()));
        for k in gone {
            by_key.remove(k);
        }
    }
    if let Some(up) = patch.get("up").and_then(Value::as_array) {
        for row in up {
            let k = key_of(row, key).ok_or_else(|| ApplyError("row without a key".into()))?;
            if by_key.insert(k.clone(), row.clone()).is_none() {
                order.push(k);
            }
        }
    }
    if let Some(explicit) = patch.get("order").and_then(Value::as_array) {
        let explicit: Vec<String> = explicit.iter().filter_map(|v| v.as_str().map(str::to_string)).collect();
        if explicit.len() != by_key.len() || explicit.iter().any(|k| !by_key.contains_key(k)) {
            return Err(ApplyError("order names rows the window does not hold".into()));
        }
        order = explicit;
    }
    let rows: Vec<Value> = order
        .into_iter()
        .map(|k| by_key.remove(&k).expect("order and rows agree"))
        .collect();
    set_member(state, "rows", Value::Array(rows));
    if let Some(m) = patch.get("meta") {
        merge_member(state, "meta", m);
    }
    Ok(())
}

/// Set `name` on an object state, making it one if it is not.
fn set_member(state: &mut Value, name: &str, value: Value) {
    if !state.is_object() {
        *state = Value::Object(Map::new());
    }
    state.as_object_mut().expect("just made an object").insert(name.into(), value);
}

/// Merge-patch the member `name` of an object state. Only touched when the
/// patch names it, so a state without `meta` keeps not having one.
fn merge_member(state: &mut Value, name: &str, patch: &Value) {
    if !state.is_object() {
        *state = Value::Object(Map::new());
    }
    let map = state.as_object_mut().expect("just made an object");
    let slot = map.entry(name.to_string()).or_insert_with(|| Value::Object(Map::new()));
    merge_apply(slot, patch);
}

/* ---------- series: append-only ---------- */

fn items_of(v: &Value) -> &[Value] {
    v.get("items").and_then(Value::as_array).map(Vec::as_slice).unwrap_or(&[])
}

fn keep_of(v: &Value) -> usize {
    v.get("keep").and_then(Value::as_u64).map(|n| n as usize).unwrap_or(usize::MAX)
}

fn series_key(item: &Value, key: &str) -> Option<f64> {
    item.get(key).and_then(Value::as_f64)
}

fn series_diff(key: &str, prev: &Value, next: &Value) -> Delta {
    if keep_of(prev) != keep_of(next) {
        return Delta::Reset;
    }
    let old = items_of(prev);
    let last = match old.last() {
        Some(item) => match series_key(item, key) {
            Some(k) => Some(k),
            None => return Delta::Reset,
        },
        None => None,
    };
    let mut append = Vec::new();
    for item in items_of(next) {
        let Some(k) = series_key(item, key) else { return Delta::Reset };
        if last.map_or(true, |l| k > l) {
            append.push(item.clone());
        }
    }
    let mut patch = Map::new();
    if !append.is_empty() {
        patch.insert("append".into(), Value::Array(append));
    }
    if let Some(m) = merge_diff(&meta_of(prev), &meta_of(next)) {
        patch.insert("meta".into(), m);
    }
    if patch.is_empty() {
        // Same tail, different items: something was rewritten in place.
        return Delta::Reset;
    }
    let patch = Value::Object(patch);
    // A series is only patchable when it is a sliding window over one growing
    // stream. Rather than trust every producer to keep that promise, check it:
    // the states are bounded, so this costs one small copy.
    let mut check = prev.clone();
    if series_apply(key, &mut check, &patch).is_err() || &check != next {
        return Delta::Reset;
    }
    Delta::Patch(patch)
}

fn series_apply(key: &str, state: &mut Value, patch: &Value) -> Result<(), ApplyError> {
    let Value::Object(patch) = patch else {
        return Err(ApplyError("series patch must be an object".into()));
    };
    let keep = keep_of(state);
    let mut items = items_of(state).to_vec();
    if let Some(append) = patch.get("append").and_then(Value::as_array) {
        let mut last = items.last().and_then(|i| series_key(i, key));
        for item in append {
            let k = series_key(item, key).ok_or_else(|| ApplyError("item without a key".into()))?;
            if last.map_or(false, |l| k <= l) {
                return Err(ApplyError("series went backwards".into()));
            }
            last = Some(k);
            items.push(item.clone());
        }
    }
    if items.len() > keep {
        items.drain(..items.len() - keep);
    }
    set_member(state, "items", Value::Array(items));
    if let Some(m) = patch.get("meta") {
        merge_member(state, "meta", m);
    }
    Ok(())
}

/// A window state, canonical.
pub fn window(rows: Vec<Value>, meta: Value) -> Value {
    canonical(serde_json::json!({ "rows": rows, "meta": meta_or_empty(meta) }))
}

/// A series state, canonical: the last `keep` of `items`.
pub fn series(mut items: Vec<Value>, keep: usize, meta: Value) -> Value {
    if items.len() > keep {
        items.drain(..items.len() - keep);
    }
    canonical(serde_json::json!({ "items": items, "keep": keep, "meta": meta_or_empty(meta) }))
}

fn meta_or_empty(meta: Value) -> Value {
    if meta.is_object() {
        meta
    } else {
        Value::Object(Map::new())
    }
}

/* ------------------------------------------------------------------ */
/* Tests                                                                */
/* ------------------------------------------------------------------ */

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// The property the whole protocol rests on: for any two states,
    /// `apply(prev, diff(prev, next)) == next`, or `diff` asked for a snapshot.
    fn roundtrip(shape: Shape, key: &str, prev: Value, next: Value) -> Delta {
        let prev = canonical(prev);
        let next = canonical(next);
        let delta = diff(shape, key, &prev, &next);
        match &delta {
            Delta::Unchanged => assert_eq!(prev, next, "unchanged but states differ"),
            Delta::Patch(p) => {
                let mut state = prev.clone();
                apply(shape, key, &mut state, p).expect("patch applies");
                assert_eq!(state, next, "patch {p} does not reproduce next");
            }
            Delta::Reset => {}
        }
        delta
    }

    /// The golden vectors the TypeScript reader is tested against, so the two
    /// implementations cannot drift apart silently.
    #[test]
    fn golden_vectors_hold() {
        let raw = include_str!("../tests/admin_live_vectors.json");
        let cases: Vec<Value> = serde_json::from_str(raw).expect("vectors parse");
        assert!(!cases.is_empty());
        for case in cases {
            let shape: Shape = serde_json::from_value(case["shape"].clone()).unwrap();
            let key = case["key"].as_str().unwrap_or("id");
            let prev = case["prev"].clone();
            let next = case["next"].clone();
            let delta = roundtrip(shape, key, prev, next);
            match (&delta, &case["patch"]) {
                (Delta::Patch(p), expected) if !expected.is_null() && expected != "reset" => {
                    assert_eq!(p, expected, "case {}", case["name"]);
                }
                (Delta::Reset, expected) => assert_eq!(expected, "reset", "case {}", case["name"]),
                (Delta::Unchanged, expected) => assert!(expected.is_null(), "case {}", case["name"]),
                (Delta::Patch(p), expected) => panic!("case {}: got {p}, expected {expected}", case["name"]),
            }
        }
    }

    #[test]
    fn doc_patches_only_what_changed() {
        let prev = json!({"a": 1, "ssps": {"ssp-0": {"status": "ready", "views": 3}, "ssp-1": {"status": "ready"}}});
        let next = json!({"a": 1, "ssps": {"ssp-0": {"status": "lagging", "views": 3}}});
        assert_eq!(
            roundtrip(Shape::Doc, "", prev, next),
            Delta::Patch(json!({"ssps": {"ssp-0": {"status": "lagging"}, "ssp-1": null}}))
        );
    }

    #[test]
    fn doc_nulls_are_absence() {
        // `null` in a fresh state is the same thing as a missing member.
        assert_eq!(roundtrip(Shape::Doc, "", json!({"a": 1, "b": null}), json!({"a": 1})), Delta::Unchanged);
        assert_eq!(
            roundtrip(Shape::Doc, "", json!({"a": 1, "b": 2}), json!({"a": 1, "b": null})),
            Delta::Patch(json!({"b": null}))
        );
    }

    #[test]
    fn doc_type_changes_replace() {
        roundtrip(Shape::Doc, "", json!({"a": {"x": 1}}), json!({"a": [1, 2]}));
        roundtrip(Shape::Doc, "", json!({"a": [1, 2]}), json!({"a": {"x": 1}}));
        roundtrip(Shape::Doc, "", json!({"a": 1}), json!({"a": {"x": {"y": 1}}}));
    }

    #[test]
    fn window_sends_changed_rows_and_removals() {
        let prev = json!({"rows": [{"id": "a", "n": 1}, {"id": "b", "n": 1}, {"id": "c", "n": 1}], "meta": {"total": 3}});
        let next = json!({"rows": [{"id": "a", "n": 2}, {"id": "c", "n": 1}, {"id": "d", "n": 1}], "meta": {"total": 3}});
        assert_eq!(
            roundtrip(Shape::Window, "id", prev, next),
            Delta::Patch(json!({"up": [{"id": "a", "n": 2}, {"id": "d", "n": 1}], "rm": ["b"]}))
        );
    }

    #[test]
    fn window_sends_order_only_when_needed() {
        // Newest first: a new row lands at the top, which append-at-end would
        // get wrong, so the order travels.
        let prev = json!({"rows": [{"id": "b"}, {"id": "a"}]});
        let next = json!({"rows": [{"id": "c"}, {"id": "b"}, {"id": "a"}]});
        match roundtrip(Shape::Window, "id", prev, next) {
            Delta::Patch(p) => assert_eq!(p["order"], json!(["c", "b", "a"])),
            other => panic!("{other:?}"),
        }
        // A pure update keeps its place: no order.
        let prev = json!({"rows": [{"id": "b", "s": 1}, {"id": "a"}]});
        let next = json!({"rows": [{"id": "b", "s": 2}, {"id": "a"}]});
        match roundtrip(Shape::Window, "id", prev, next) {
            Delta::Patch(p) => assert!(p.get("order").is_none()),
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn window_numeric_keys_and_meta() {
        let prev = json!({"rows": [{"n": 1}, {"n": 2}], "meta": {"total": 2, "x": {"y": 1}}});
        let next = json!({"rows": [{"n": 2}, {"n": 3}], "meta": {"total": 9, "x": {"y": 1}}});
        assert_eq!(
            roundtrip(Shape::Window, "n", prev, next),
            Delta::Patch(json!({"up": [{"n": 3}], "rm": ["1"], "meta": {"total": 9}}))
        );
    }

    #[test]
    fn window_without_keys_resets() {
        assert_eq!(roundtrip(Shape::Window, "id", json!({"rows": [{"id": "a"}]}), json!({"rows": [{"x": 1}]})), Delta::Reset);
        assert_eq!(
            roundtrip(Shape::Window, "id", json!({"rows": []}), json!({"rows": [{"id": "a"}, {"id": "a"}]})),
            Delta::Reset
        );
    }

    #[test]
    fn series_appends_and_trims() {
        let prev = series(vec![json!({"t": 1}), json!({"t": 2}), json!({"t": 3})], 3, json!({}));
        let next = series(vec![json!({"t": 2}), json!({"t": 3}), json!({"t": 4}), json!({"t": 5})], 3, json!({}));
        assert_eq!(
            roundtrip(Shape::Series, "t", prev, next),
            Delta::Patch(json!({"append": [{"t": 4}, {"t": 5}]}))
        );
    }

    #[test]
    fn series_that_went_backwards_resets() {
        let prev = series(vec![json!({"t": 5}), json!({"t": 6})], 10, json!({}));
        let next = series(vec![json!({"t": 1})], 10, json!({}));
        assert_eq!(roundtrip(Shape::Series, "t", prev, next), Delta::Reset);
        // Same tail, rewritten history: not an append.
        let prev = series(vec![json!({"t": 1, "v": 1}), json!({"t": 2})], 10, json!({}));
        let next = series(vec![json!({"t": 1, "v": 9}), json!({"t": 2})], 10, json!({}));
        assert_eq!(roundtrip(Shape::Series, "t", prev, next), Delta::Reset);
    }

    #[test]
    fn series_meta_only_change() {
        let prev = series(vec![json!({"t": 1})], 10, json!({"dropped": 0}));
        let next = series(vec![json!({"t": 1})], 10, json!({"dropped": 4}));
        assert_eq!(roundtrip(Shape::Series, "t", prev, next), Delta::Patch(json!({"meta": {"dropped": 4}})));
    }

    #[test]
    fn randomised_windows_roundtrip() {
        // A small deterministic generator: no new dev-dependency for this.
        let mut seed: u64 = 0x5eed;
        let mut rand = move |n: u64| {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed % n
        };
        for _ in 0..2000 {
            let make = |rand: &mut dyn FnMut(u64) -> u64| {
                let mut keys: Vec<u64> = (0..rand(8)).map(|_| rand(12)).collect();
                keys.sort();
                keys.dedup();
                if rand(2) == 0 {
                    keys.reverse();
                }
                let rows: Vec<Value> = keys.iter().map(|k| json!({"id": format!("k{k}"), "v": rand(3)})).collect();
                json!({"rows": rows, "meta": {"total": rand(4), "nested": {"a": rand(2)}}})
            };
            let prev = make(&mut rand);
            let next = make(&mut rand);
            let delta = roundtrip(Shape::Window, "id", prev, next);
            assert_ne!(delta, Delta::Reset, "keyed windows never need a reset");
        }
    }

    #[test]
    fn randomised_docs_roundtrip() {
        let mut seed: u64 = 0xd0c;
        let mut rand = move |n: u64| {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed % n
        };
        fn build(rand: &mut dyn FnMut(u64) -> u64, depth: u32) -> Value {
            match rand(if depth > 2 { 4 } else { 6 }) {
                0 => Value::Null,
                1 => json!(rand(5)),
                2 => json!(format!("s{}", rand(3))),
                3 => json!([rand(3), rand(3)]),
                _ => {
                    let mut map = Map::new();
                    for _ in 0..rand(4) {
                        map.insert(format!("k{}", rand(5)), build(rand, depth + 1));
                    }
                    Value::Object(map)
                }
            }
        }
        for _ in 0..3000 {
            let mut prev = build(&mut rand, 0);
            let mut next = build(&mut rand, 0);
            // A doc is an object at the top.
            if !prev.is_object() {
                prev = json!({ "v": prev });
            }
            if !next.is_object() {
                next = json!({ "v": next });
            }
            let delta = roundtrip(Shape::Doc, "", prev, next);
            assert_ne!(delta, Delta::Reset);
        }
    }

    #[test]
    fn topic_keys_are_canonical() {
        assert_eq!(topic_key("overview", &Value::Null), "overview");
        assert_eq!(topic_key("overview", &json!({})), "overview");
        assert_eq!(
            topic_key("metrics", &json!({"name": "sync", "b": [2, 1]})),
            topic_key("metrics", &json!({"b": [2, 1], "name": "sync"}))
        );
        assert_eq!(topic_key("metrics", &json!({"name": "sync"})), r#"metrics?{"name":"sync"}"#);
    }

    #[test]
    fn messages_have_the_documented_shape() {
        let sub: ClientMsg = serde_json::from_str(r#"{"op":"sub","ref":"r1","topic":"overview"}"#).unwrap();
        assert_eq!(
            sub,
            ClientMsg::Sub { reference: "r1".into(), topic: "overview".into(), args: Value::Null, since: None }
        );
        let diff = ServerMsg::Diff { key: "overview".into(), from: 1, seq: 2, patch: json!({"a": 1}) };
        assert_eq!(
            serde_json::to_value(&diff).unwrap(),
            json!({"op": "diff", "key": "overview", "from": 1, "seq": 2, "patch": {"a": 1}})
        );
        let err = ServerMsg::Error { reference: None, code: "x".into(), message: "y".into() };
        assert_eq!(serde_json::to_value(&err).unwrap(), json!({"op": "error", "code": "x", "message": "y"}));
    }
}
