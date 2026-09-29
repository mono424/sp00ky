//! Everything about one rule and one row that needs no I/O: conditions,
//! `maxAge`, the dedupe key, recipient extraction, the template context,
//! payload rendering and the payload size cap.
//!
//! The engine does the I/O around it (`with` / `to.query`, subscriptions,
//! delivery); keeping this half pure keeps it exhaustively testable.

use std::cmp::Ordering;
use std::collections::{BTreeMap, HashSet};

use serde_json::{Map, Value};

use crate::config::{
    Condition, ConditionOps, DataSpec, MaxAge, NotificationTemplate, OneOrMany, Op, PayloadKind,
    PushPayload, Rule, RuleDefaults, Target, PAYLOAD_VERSION,
};
use crate::ece::MAX_PLAINTEXT;
use crate::template;
use crate::util::{
    b64url, canonical_record_id, collapse_record_ids, epoch_millis, record_id_object, sha256,
    stable_hash,
};

/// Longest topic kept in the payload; the header form is hashed anyway.
pub const MAX_TOPIC_CHARS: usize = 256;

// ── Field paths ──────────────────────────────────────────────────────────

/// Strict dot-path lookup: object keys and array indices, nothing clever.
/// `None` when a segment is missing (which conditions tell apart from null).
pub fn get_path<'a>(v: &'a Value, path: &str) -> Option<&'a Value> {
    let mut cur = v;
    for seg in path.split('.') {
        cur = match cur {
            Value::Object(map) => map.get(seg)?,
            Value::Array(items) => items.get(seg.parse::<usize>().ok()?)?,
            _ => return None,
        };
    }
    Some(cur)
}

// ── Conditions ───────────────────────────────────────────────────────────

/// Equality as a manifest author means it: `3 == 3.0`, and a record id equals
/// itself however it was printed (`user:⟨a-b⟩`, `user:a-b`, `{tb, id}`).
pub fn loose_eq(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Number(x), Value::Number(y)) => match (x.as_f64(), y.as_f64()) {
            (Some(x), Some(y)) => x == y,
            _ => x == y,
        },
        (Value::String(x), Value::String(y)) => {
            x == y
                || matches!((canonical_record_id(x), canonical_record_id(y)), (Some(x), Some(y)) if x == y)
        }
        (Value::Object(_), Value::String(s)) | (Value::String(s), Value::Object(_)) => {
            let obj = if a.is_object() { a } else { b };
            match (record_id_object(obj), canonical_record_id(s)) {
                (Some(x), Some(y)) => x == y,
                _ => false,
            }
        }
        (Value::Array(x), Value::Array(y)) => {
            x.len() == y.len() && x.iter().zip(y).all(|(x, y)| loose_eq(x, y))
        }
        _ => a == b,
    }
}

fn compare(a: &Value, b: &Value) -> Option<Ordering> {
    match (a, b) {
        (Value::Number(x), Value::Number(y)) => x.as_f64()?.partial_cmp(&y.as_f64()?),
        (Value::String(x), Value::String(y)) => Some(x.cmp(y)),
        _ => None,
    }
}

fn is_null(v: Option<&Value>) -> bool {
    matches!(v, None | Some(Value::Null))
}

/// `field: value`. `null` matches a missing field and a null one alike; any
/// other value needs the field present and equal.
fn eq_holds(expected: &Value, actual: Option<&Value>) -> bool {
    match (expected, actual) {
        (Value::Null, a) => is_null(a),
        (_, None) => false,
        (e, Some(a)) => loose_eq(e, a),
    }
}

fn ops_hold(ops: &ConditionOps, actual: Option<&Value>) -> bool {
    if let Some(e) = &ops.eq {
        if !eq_holds(e, actual) {
            return false;
        }
    }
    if let Some(e) = &ops.ne {
        if eq_holds(e, actual) {
            return false;
        }
    }
    if let Some(list) = &ops.in_ {
        if !list.iter().any(|e| eq_holds(e, actual)) {
            return false;
        }
    }
    if let Some(list) = &ops.nin {
        if list.iter().any(|e| eq_holds(e, actual)) {
            return false;
        }
    }
    let ordered = [
        (&ops.gt, &[Ordering::Greater][..]),
        (&ops.gte, &[Ordering::Greater, Ordering::Equal][..]),
        (&ops.lt, &[Ordering::Less][..]),
        (&ops.lte, &[Ordering::Less, Ordering::Equal][..]),
    ];
    for (bound, accepted) in ordered {
        if let Some(bound) = bound {
            match actual.and_then(|a| compare(a, bound)) {
                Some(ord) if accepted.contains(&ord) => {}
                _ => return false,
            }
        }
    }
    if let Some(exists) = ops.exists {
        if exists == is_null(actual) {
            return false;
        }
    }
    if let Some(needle) = &ops.contains {
        let hit = match actual {
            Some(Value::Array(items)) => items.iter().any(|i| loose_eq(i, needle)),
            Some(Value::String(s)) => match needle {
                Value::String(n) => s.contains(n.as_str()),
                Value::Number(n) => s.contains(&n.to_string()),
                _ => false,
            },
            _ => false,
        };
        if !hit {
            return false;
        }
    }
    if let Some(prefix) = &ops.starts_with {
        match actual {
            Some(Value::String(s)) if s.starts_with(prefix.as_str()) => {}
            _ => return false,
        }
    }
    true
}

/// One condition against one field value (`None` = the field is missing).
pub fn condition_holds(cond: &Condition, actual: Option<&Value>) -> bool {
    match cond {
        Condition::Eq(e) => eq_holds(e, actual),
        Condition::Any(list) => list.iter().any(|e| eq_holds(e, actual)),
        Condition::Ops(ops) => ops_hold(ops, actual),
    }
}

/// Every `when` condition against the row.
pub fn when_matches(when: &BTreeMap<String, Condition>, record: &Value) -> bool {
    when.iter()
        .all(|(path, cond)| condition_holds(cond, get_path(record, path)))
}

/// `maxAge`: the row's timestamp field is at most `within` old. A missing or
/// unreadable field passes: a typo in the manifest must not silently mute a
/// rule forever (lint cannot know the field's type).
pub fn max_age_ok(max_age: &MaxAge, record: &Value, now_ms: u64) -> bool {
    match get_path(record, &max_age.field).and_then(epoch_millis) {
        Some(ts) => (now_ms as i64).saturating_sub(ts) <= max_age.within.as_millis() as i64,
        None => true,
    }
}

// ── Dedupe ───────────────────────────────────────────────────────────────

/// What makes two observations of a row "the same push" for one rule.
///
/// - `once: [fields]`: the rule, the record and those fields' values, so a
///   row pushes when it reaches a state and not again while it stays there.
/// - otherwise the record version: `_00_rv` when the row carries it, else a
///   hash of the row. The op is part of it so a delete (whose before-image
///   equals the last update's row) is not mistaken for that update.
///
/// Re-ingested rows (changefeed look-back, WAL replay, retry re-polls) produce
/// the same key and are dropped.
pub fn dedupe_key(rule_name: &str, rule: &Rule, id: &str, op: Op, record: &Value) -> String {
    match &rule.once {
        Some(fields) => {
            let values: Vec<Value> = fields
                .iter()
                .map(|f| {
                    get_path(record, f)
                        .cloned()
                        .map(collapse_record_ids)
                        .unwrap_or(Value::Null)
                })
                .collect();
            format!(
                "{rule_name}|{id}|once|{}",
                stable_hash(&Value::Array(values))
            )
        }
        None => match record.get("_00_rv") {
            Some(rv) if !rv.is_null() => format!("{rule_name}|{id}|{}|rv:{rv}", op.as_str()),
            _ => format!("{rule_name}|{id}|{}|h:{}", op.as_str(), stable_hash(record)),
        },
    }
}

// ── Recipients ───────────────────────────────────────────────────────────

/// Canonical `table:key` of a user id string; `None` for anything that is not
/// a record id (plain text without `:`, empty strings).
pub fn normalize_recipient(s: &str) -> Option<String> {
    canonical_record_id(s)
}

/// Collect user ids from any value a field or a query can hold: id strings,
/// `{tb, id}` objects, rows with an `id`, one-field rows (`SELECT user
/// FROM ...`), and arrays of any of those.
pub fn collect_ids(v: &Value, out: &mut Vec<String>) {
    match v {
        Value::String(s) => {
            if let Some(id) = normalize_recipient(s) {
                out.push(id);
            }
        }
        Value::Array(items) => items.iter().for_each(|i| collect_ids(i, out)),
        Value::Object(map) => {
            if let Some(id) = record_id_object(v) {
                if let Some(id) = normalize_recipient(&id) {
                    out.push(id);
                }
            } else if let Some(id) = map.get("id") {
                collect_ids(id, out);
            } else if map.len() == 1 {
                if let Some(only) = map.values().next() {
                    collect_ids(only, out);
                }
            }
        }
        _ => {}
    }
}

/// Recipients named by row fields (`to: [owner, members]`, `except:`).
pub fn field_ids(paths: &OneOrMany<String>, record: &Value) -> Vec<String> {
    let mut out = Vec::new();
    for p in paths.iter() {
        if let Some(v) = get_path(record, p) {
            collect_ids(v, &mut out);
        }
    }
    out
}

/// Field recipients of a rule; `None` for a `to.query` rule (the engine runs
/// the query and feeds its result through [`collect_ids`]).
pub fn target_field_ids(rule: &Rule, record: &Value) -> Option<Vec<String>> {
    match &rule.to {
        Target::Fields(paths) => Some(field_ids(paths, record)),
        Target::Query { .. } => None,
    }
}

/// Order-preserving dedupe, minus `except`, capped. Returns the list and
/// whether the cap cut it.
pub fn finalize_recipients(ids: Vec<String>, except: &[String], cap: usize) -> (Vec<String>, bool) {
    let except: HashSet<&str> = except.iter().map(String::as_str).collect();
    let mut seen = HashSet::new();
    let mut out = Vec::new();
    let mut capped = false;
    for id in ids {
        if except.contains(id.as_str()) || !seen.insert(id.clone()) {
            continue;
        }
        if out.len() >= cap {
            capped = true;
            break;
        }
        out.push(id);
    }
    (out, capped)
}

// ── Context and rendering ────────────────────────────────────────────────

/// The row an observation is about.
#[derive(Debug, Clone, Copy)]
pub struct RowRef<'a> {
    pub table: &'a str,
    /// Canonical `table:key`.
    pub id: &'a str,
    pub op: Op,
    pub record: &'a Value,
}

/// Template context: row fields at the top level, then `row`, `id`, `table`,
/// `op`, `rule`, `now` (epoch ms), then every `with` name, each layer winning
/// over the one before. `recipient` is added per recipient by
/// [`with_recipient`].
pub fn context(row: RowRef<'_>, rule_name: &str, with: &Map<String, Value>, now_ms: u64) -> Value {
    let mut map = row.record.as_object().cloned().unwrap_or_default();
    map.insert("row".into(), row.record.clone());
    map.insert("id".into(), Value::String(row.id.to_string()));
    map.insert("table".into(), Value::String(row.table.to_string()));
    map.insert("op".into(), Value::String(row.op.as_str().to_string()));
    map.insert("rule".into(), Value::String(rule_name.to_string()));
    map.insert("now".into(), Value::from(now_ms));
    for (k, v) in with {
        map.insert(k.clone(), v.clone());
    }
    Value::Object(map)
}

pub fn with_recipient(ctx: &Value, recipient: &str) -> Value {
    let mut ctx = ctx.clone();
    if let Value::Object(map) = &mut ctx {
        map.insert("recipient".into(), Value::String(recipient.to_string()));
    }
    ctx
}

/// The notification a rule shows: its own template over `defaults`. `None`
/// for a nudge (defaults never turn a nudge into a notification).
pub fn effective_notification(
    rule: &Rule,
    defaults: &RuleDefaults,
) -> Option<NotificationTemplate> {
    let own = rule.notification.as_ref()?;
    Some(match &defaults.notification {
        Some(base) => own.over(base),
        None => own.clone(),
    })
}

/// Does anything the rule renders use `{{recipient}}`? Then it renders once
/// per recipient instead of once per row.
pub fn rule_references_recipient(rule: &Rule, defaults: &RuleDefaults) -> bool {
    let root = "recipient";
    if rule
        .topic
        .as_deref()
        .is_some_and(|t| template::references(t, root))
    {
        return true;
    }
    if let Some(n) = effective_notification(rule, defaults) {
        if serde_json::to_value(&n).is_ok_and(|v| template::value_references(&v, root)) {
            return true;
        }
    }
    match &rule.data {
        Some(DataSpec::Map(map)) => map.values().any(|v| template::value_references(v, root)),
        Some(DataSpec::Fields(paths)) => paths
            .iter()
            .any(|p| p == root || p.starts_with("recipient.")),
        None => false,
    }
}

/// The rule's topic (collapse key), defaulting to the record id.
pub fn render_topic(rule: &Rule, ctx: &Value, id: &str) -> String {
    let topic = rule
        .topic
        .as_deref()
        .map(|t| template::render(t, ctx))
        .unwrap_or_default();
    let topic = topic.trim();
    let topic = if topic.is_empty() { id } else { topic };
    template::truncate(topic, MAX_TOPIC_CHARS)
}

/// Keys whose template renders to text, never to another JSON type.
const TEXT_KEYS: [&str; 9] = [
    "title", "body", "icon", "badge", "image", "lang", "dir", "tag", "url",
];

/// Render a notification template to what `showNotification` gets. Text keys
/// render to strings (empty optional ones are dropped), everything else
/// (`data`, pass-through keys) keeps its JSON type. `tag` defaults to the
/// topic, so a newer push replaces the older notification on the device.
pub fn render_notification(tpl: &NotificationTemplate, ctx: &Value, topic: Option<&str>) -> Value {
    let raw = match serde_json::to_value(tpl) {
        Ok(Value::Object(map)) => map,
        _ => Map::new(),
    };
    let mut out = Map::new();
    for (k, v) in raw {
        let rendered = match (TEXT_KEYS.contains(&k.as_str()), &v) {
            (true, Value::String(s)) => {
                let text = template::render(s, ctx);
                if text.is_empty() && k != "title" {
                    continue;
                }
                Value::String(text)
            }
            _ if k == "actions" => render_actions(&v, ctx),
            _ => template::render_value(&v, ctx),
        };
        out.insert(k, rendered);
    }
    if !out.contains_key("tag") {
        if let Some(topic) = topic.filter(|t| !t.is_empty()) {
            out.insert("tag".into(), Value::String(topic.to_string()));
        }
    }
    Value::Object(out)
}

fn render_actions(v: &Value, ctx: &Value) -> Value {
    let Value::Array(items) = v else {
        return template::render_value(v, ctx);
    };
    Value::Array(
        items
            .iter()
            .map(|a| match a {
                Value::Object(map) => Value::Object(
                    map.iter()
                        .map(|(k, v)| {
                            let r = match (k.as_str(), v) {
                                ("action" | "title" | "icon", Value::String(s)) => {
                                    Value::String(template::render(s, ctx))
                                }
                                _ => template::render_value(v, ctx),
                            };
                            (k.clone(), r)
                        })
                        .collect(),
                ),
                other => template::render_value(other, ctx),
            })
            .collect(),
    )
}

/// `payload.data`: copied fields (keyed by their path) or rendered templates.
pub fn render_data(spec: &DataSpec, ctx: &Value) -> Value {
    match spec {
        DataSpec::Fields(paths) => Value::Object(
            paths
                .iter()
                .map(|p| {
                    let v = template::lookup(ctx, p)
                        .cloned()
                        .map(collapse_record_ids)
                        .unwrap_or(Value::Null);
                    (p.clone(), v)
                })
                .collect(),
        ),
        DataSpec::Map(map) => {
            template::render_value(&Value::Object(map.clone().into_iter().collect()), ctx)
        }
    }
}

/// The payload of one rule push, before encoding.
pub fn rule_payload(
    rule_name: &str,
    rule: &Rule,
    defaults: &RuleDefaults,
    row: RowRef<'_>,
    ctx: &Value,
    topic: &str,
    now_ms: u64,
) -> PushPayload {
    let notification =
        effective_notification(rule, defaults).map(|n| render_notification(&n, ctx, Some(topic)));
    let data = rule.data.as_ref().map(|d| render_data(d, ctx));
    PushPayload {
        v: PAYLOAD_VERSION,
        kind: PayloadKind::Rule,
        rule: Some(rule_name.to_string()),
        message: None,
        table: Some(row.table.to_string()),
        id: Some(row.id.to_string()),
        op: Some(row.op),
        topic: Some(topic.to_string()),
        notification,
        data,
        ts: now_ms,
    }
}

/// The payload of a `_00_push_message` row. Its notification is shown as
/// written (no templating), over the manifest's `defaults.notification`.
pub fn message_payload(
    message_id: &str,
    row: &Value,
    defaults: &RuleDefaults,
    now_ms: u64,
) -> PushPayload {
    let topic = row
        .get("topic")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|t| !t.is_empty())
        .map(|t| template::truncate(t, MAX_TOPIC_CHARS));
    let notification = match row.get("notification") {
        Some(Value::Object(own)) => {
            let mut merged = match &defaults.notification {
                Some(base) => {
                    let ctx = serde_json::json!({ "id": message_id, "table": "_00_push_message", "now": now_ms });
                    match render_notification(base, &ctx, None) {
                        Value::Object(map) => map,
                        _ => Map::new(),
                    }
                }
                None => Map::new(),
            };
            for (k, v) in own {
                if !v.is_null() {
                    merged.insert(k.clone(), v.clone());
                }
            }
            if !merged.contains_key("tag") {
                if let Some(t) = &topic {
                    merged.insert("tag".into(), Value::String(t.clone()));
                }
            }
            Some(Value::Object(merged))
        }
        _ => None,
    };
    let data = row.get("data").filter(|d| !d.is_null()).cloned();
    PushPayload {
        v: PAYLOAD_VERSION,
        kind: PayloadKind::Message,
        rule: None,
        message: Some(message_id.to_string()),
        table: None,
        id: None,
        op: None,
        topic,
        notification,
        data,
        ts: now_ms,
    }
}

// ── Encoding and the size cap ────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq)]
pub struct Encoded {
    pub bytes: Vec<u8>,
    /// What had to go to fit ([`MAX_PLAINTEXT`]): `data`, `body`,
    /// `notification`, `topic`, in that order.
    pub degraded: Vec<&'static str>,
}

/// JSON-encode a payload within [`MAX_PLAINTEXT`] bytes. Over the cap it
/// degrades in a fixed order: drop `data`, then shorten `notification.body`,
/// then drop the notification (the push becomes a nudge, which the service
/// worker can still render from live data), then the topic.
pub fn encode_payload(payload: &PushPayload) -> Result<Encoded, String> {
    let mut p = payload.clone();
    let mut degraded = Vec::new();
    let encode = |p: &PushPayload| serde_json::to_vec(p).map_err(|e| e.to_string());
    let mut bytes = encode(&p)?;
    if bytes.len() <= MAX_PLAINTEXT {
        return Ok(Encoded { bytes, degraded });
    }
    if p.data.take().is_some() {
        degraded.push("data");
        bytes = encode(&p)?;
        if bytes.len() <= MAX_PLAINTEXT {
            return Ok(Encoded { bytes, degraded });
        }
    }
    let body = p
        .notification
        .as_ref()
        .and_then(|n| n.get("body"))
        .and_then(Value::as_str)
        .map(str::to_string);
    if let Some(mut body) = body {
        degraded.push("body");
        // Cut by encoded cost, re-measure, and cut again if escaping elsewhere
        // still leaves it over; one pass is normally enough.
        for _ in 0..8 {
            let over = bytes.len().saturating_sub(MAX_PLAINTEXT);
            if over == 0 || body.is_empty() {
                break;
            }
            // Budget in encoded bytes: what the body costs now, minus the
            // overflow and the "..." that gets appended.
            let budget = body
                .chars()
                .map(json_cost)
                .sum::<usize>()
                .saturating_sub(over + 3);
            let mut spent = 0;
            let cut: String = body
                .chars()
                .take_while(|c| {
                    spent += json_cost(*c);
                    spent <= budget
                })
                .collect();
            let cut = cut.trim_end();
            body = if cut.is_empty() {
                String::new()
            } else {
                format!("{cut}...")
            };
            if let Some(Value::Object(n)) = p.notification.as_mut() {
                n.insert("body".into(), Value::String(body.clone()));
            }
            bytes = encode(&p)?;
        }
        if bytes.len() <= MAX_PLAINTEXT {
            return Ok(Encoded { bytes, degraded });
        }
    }
    if p.notification.take().is_some() {
        degraded.push("notification");
        bytes = encode(&p)?;
        if bytes.len() <= MAX_PLAINTEXT {
            return Ok(Encoded { bytes, degraded });
        }
    }
    if p.topic.take().is_some() {
        degraded.push("topic");
        bytes = encode(&p)?;
        if bytes.len() <= MAX_PLAINTEXT {
            return Ok(Encoded { bytes, degraded });
        }
    }
    Err(format!(
        "payload is {} bytes even as a bare nudge",
        bytes.len()
    ))
}

/// Bytes a character takes inside a JSON string as serde_json writes it.
fn json_cost(c: char) -> usize {
    match c {
        '"' | '\\' | '\n' | '\r' | '\t' | '\u{8}' | '\u{c}' => 2,
        c if (c as u32) < 0x20 => 6,
        c => c.len_utf8(),
    }
}

/// RFC 8030 `Topic` header: at most 32 characters of the base64url alphabet,
/// so any topic is hashed. The readable topic travels inside the payload.
pub fn topic_header(topic: &str) -> String {
    b64url(&sha256(topic.as_bytes()))[..32].to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::PushConfig;
    use serde_json::json;

    fn cond(yaml: &str) -> Condition {
        serde_yaml::from_str(yaml).unwrap()
    }

    fn holds(yaml: &str, v: Option<Value>) -> bool {
        condition_holds(&cond(yaml), v.as_ref())
    }

    #[test]
    fn scalar_and_list_conditions() {
        assert!(holds("ready", Some(json!("ready"))));
        assert!(!holds("ready", Some(json!("draft"))));
        assert!(!holds("ready", None));
        assert!(holds("3", Some(json!(3.0))));
        assert!(holds("false", Some(json!(false))));
        assert!(!holds("false", None));
        assert!(holds("null", None));
        assert!(holds("null", Some(json!(null))));
        assert!(!holds("null", Some(json!(0))));
        assert!(holds("[ready, urgent]", Some(json!("urgent"))));
        assert!(!holds("[ready, urgent]", Some(json!("x"))));
        assert!(holds("[ready, null]", None));
        assert!(!holds("[]", Some(json!("x"))));
        assert!(holds("\"user:abc\"", Some(json!("user:⟨abc⟩"))));
        assert!(holds(
            "\"user:abc\"",
            Some(json!({"tb": "user", "id": "abc"}))
        ));
    }

    #[test]
    fn operator_conditions() {
        assert!(holds("{ ne: x }", Some(json!("y"))));
        assert!(holds("{ ne: x }", None));
        assert!(!holds("{ ne: x }", Some(json!("x"))));
        assert!(holds("{ in: [a, b] }", Some(json!("b"))));
        assert!(!holds("{ nin: [a, b] }", Some(json!("b"))));
        assert!(holds("{ nin: [a, b] }", None));
        assert!(holds("{ gt: 2 }", Some(json!(3))));
        assert!(!holds("{ gt: 3 }", Some(json!(3))));
        assert!(holds("{ gte: 3 }", Some(json!(3))));
        assert!(holds("{ gte: 2, lt: 10 }", Some(json!(9.5))));
        assert!(!holds("{ gte: 2, lt: 10 }", Some(json!(10))));
        assert!(holds("{ lte: 1.5 }", Some(json!(1.5))));
        assert!(!holds("{ lt: 5 }", None));
        assert!(
            !holds("{ lt: 5 }", Some(json!("4"))),
            "no string/number coercion"
        );
        assert!(holds("{ gt: b }", Some(json!("c"))));
        assert!(holds(
            "{ lt: \"2024-06-01\" }",
            Some(json!("2024-05-31T23:00:00Z"))
        ));
        assert!(holds("{ exists: true }", Some(json!(0))));
        assert!(!holds("{ exists: true }", Some(json!(null))));
        assert!(!holds("{ exists: true }", None));
        assert!(holds("{ exists: false }", None));
        assert!(holds("{ exists: false }", Some(json!(null))));
        assert!(!holds("{ exists: false }", Some(json!(""))));
        assert!(holds("{ contains: b }", Some(json!(["a", "b"]))));
        assert!(holds(
            "{ contains: \"user:b\" }",
            Some(json!(["user:a", "user:⟨b⟩"]))
        ));
        assert!(holds("{ contains: ell }", Some(json!("hello"))));
        assert!(holds("{ contains: 42 }", Some(json!("x42y"))));
        assert!(!holds("{ contains: z }", Some(json!(["a"]))));
        assert!(!holds("{ contains: z }", None));
        assert!(holds("{ startsWith: \"dm:\" }", Some(json!("dm:1"))));
        assert!(!holds("{ startsWith: \"dm:\" }", Some(json!(5))));
        assert!(holds("{ eq: 5 }", Some(json!(5))));
        assert!(!holds("{ eq: 5, ne: 5 }", Some(json!(5))));
    }

    #[test]
    fn when_uses_paths_and_indices() {
        let when: BTreeMap<String, Condition> = serde_yaml::from_str(
            "{ kind: text, data.level: { gte: 2 }, tags.0: a, muted: { exists: false } }",
        )
        .unwrap();
        assert!(when_matches(
            &when,
            &json!({"kind": "text", "data": {"level": 3}, "tags": ["a"]})
        ));
        assert!(!when_matches(
            &when,
            &json!({"kind": "text", "data": {"level": 1}, "tags": ["a"]})
        ));
        assert!(!when_matches(
            &when,
            &json!({"kind": "text", "data": {"level": 3}, "tags": ["a"], "muted": true})
        ));
        assert!(when_matches(&BTreeMap::new(), &json!({})));
    }

    #[test]
    fn max_age() {
        let ma: MaxAge = serde_yaml::from_str("{ field: created_at, within: 45s }").unwrap();
        let now = 1_704_067_200_000u64;
        assert!(max_age_ok(
            &ma,
            &json!({"created_at": "2024-01-01T00:00:00Z"}),
            now + 1_000
        ));
        assert!(!max_age_ok(
            &ma,
            &json!({"created_at": "2024-01-01T00:00:00Z"}),
            now + 46_000
        ));
        assert!(max_age_ok(&ma, &json!({"created_at": now - 10_000}), now));
        assert!(!max_age_ok(
            &ma,
            &json!({"created_at": (now - 60_000) / 1000}),
            now
        ));
        assert!(
            max_age_ok(&ma, &json!({"created_at": now + 60_000}), now),
            "future timestamps pass"
        );
        assert!(max_age_ok(&ma, &json!({}), now), "missing field passes");
    }

    fn rule(yaml: &str) -> Rule {
        serde_yaml::from_str(yaml).unwrap()
    }

    #[test]
    fn dedupe_keys() {
        let plain = rule("{ table: t, to: u }");
        let a = dedupe_key(
            "r",
            &plain,
            "t:1",
            Op::Update,
            &json!({"x": 1, "_00_rv": 4}),
        );
        let b = dedupe_key(
            "r",
            &plain,
            "t:1",
            Op::Update,
            &json!({"x": 2, "_00_rv": 4}),
        );
        let c = dedupe_key(
            "r",
            &plain,
            "t:1",
            Op::Update,
            &json!({"x": 1, "_00_rv": 5}),
        );
        assert_eq!(a, b, "same version, same push");
        assert_ne!(a, c);
        let h1 = dedupe_key("r", &plain, "t:1", Op::Update, &json!({"x": 1}));
        let h2 = dedupe_key("r", &plain, "t:1", Op::Delete, &json!({"x": 1}));
        assert_ne!(h1, h2, "a delete is not the last update again");
        let once = rule("{ table: t, to: u, once: [state] }");
        let o1 = dedupe_key(
            "r",
            &once,
            "t:1",
            Op::Update,
            &json!({"state": "ready", "n": 1, "_00_rv": 1}),
        );
        let o2 = dedupe_key(
            "r",
            &once,
            "t:1",
            Op::Update,
            &json!({"state": "ready", "n": 2, "_00_rv": 2}),
        );
        let o3 = dedupe_key(
            "r",
            &once,
            "t:1",
            Op::Update,
            &json!({"state": "done", "_00_rv": 3}),
        );
        assert_eq!(o1, o2);
        assert_ne!(o1, o3);
        assert_ne!(
            dedupe_key("r2", &once, "t:1", Op::Update, &json!({"state": "ready"})),
            o1
        );
    }

    #[test]
    fn recipients_normalize_and_flatten() {
        let rec = json!({
            "recipient": " user:a ",
            "members": ["user:b", {"tb": "user", "id": "c"}, "not an id", "", 5, null, ["user:⟨d⟩"]],
            "owner": {"id": "user:e", "name": "E"},
            "sender": "user:a",
        });
        let to: OneOrMany<String> =
            serde_yaml::from_str("[recipient, members, owner, missing]").unwrap();
        let ids = field_ids(&to, &rec);
        assert_eq!(ids, vec!["user:a", "user:b", "user:c", "user:d", "user:e"]);
        let except = field_ids(&OneOrMany::One("sender".into()), &rec);
        let (final_ids, capped) = finalize_recipients(ids.clone(), &except, 10);
        assert_eq!(final_ids, vec!["user:b", "user:c", "user:d", "user:e"]);
        assert!(!capped);
        let (capped_ids, capped) = finalize_recipients(ids, &[], 2);
        assert_eq!(capped_ids, vec!["user:a", "user:b"]);
        assert!(capped);
        let (dups, _) =
            finalize_recipients(vec!["u:1".into(), "u:1".into(), "u:2".into()], &[], 10);
        assert_eq!(dups, vec!["u:1", "u:2"]);
        let mut from_query = Vec::new();
        collect_ids(
            &json!([{"user": "user:x"}, {"id": "user:y", "n": 1}, "user:z", [{"tb": "user", "id": 7}]]),
            &mut from_query,
        );
        assert_eq!(from_query, vec!["user:x", "user:y", "user:z", "user:7"]);
    }

    fn cfg(yaml: &str) -> PushConfig {
        let c: PushConfig = serde_yaml::from_str(yaml).unwrap();
        assert!(c.is_valid(), "{:?}", c.validate());
        c
    }

    #[test]
    fn renders_a_full_rule_payload() {
        let c = cfg(r#"
defaults:
  notification: { icon: /icon.png, badge: "/b/{{table}}.png", title: Fallback }
rules:
  new-message:
    table: message
    to: recipient
    topic: "dm:{{conversation | key}}"
    notification:
      title: "{{sender.username}}"
      body: "{{text | truncate(10)}}"
      url: "/m/{{conversation | key}}"
      image: "{{nope}}"
      vibrate: [100, 50]
      timestamp: "{{created}}"
      actions: [{ action: "open-{{id | key}}", title: "Open {{sender.username}}" }]
    data: [conversation, sender.username]
"#);
        let rule = &c.rules["new-message"];
        let record = json!({"text": "hello world, again", "conversation": "conversation:c1", "recipient": "user:b", "created": 1234});
        let row = RowRef {
            table: "message",
            id: "message:m1",
            op: Op::Create,
            record: &record,
        };
        let mut with = Map::new();
        with.insert("sender".into(), json!({"username": "ada"}));
        let ctx = context(row, "new-message", &with, 99);
        let topic = render_topic(rule, &ctx, row.id);
        assert_eq!(topic, "dm:c1");
        let p = rule_payload("new-message", rule, &c.defaults, row, &ctx, &topic, 99);
        assert_eq!(p.kind, PayloadKind::Rule);
        assert_eq!(p.rule.as_deref(), Some("new-message"));
        assert_eq!(p.id.as_deref(), Some("message:m1"));
        assert_eq!(p.table.as_deref(), Some("message"));
        assert_eq!(p.op, Some(Op::Create));
        assert_eq!(p.ts, 99);
        assert_eq!(
            p.notification.unwrap(),
            json!({
                "title": "ada",
                "body": "hello w...",
                "icon": "/icon.png",
                "badge": "/b/message.png",
                "url": "/m/c1",
                "tag": "dm:c1",
                "vibrate": [100, 50],
                "timestamp": 1234,
                "actions": [{"action": "open-m1", "title": "Open ada"}],
            })
        );
        assert_eq!(
            p.data.unwrap(),
            json!({"conversation": "conversation:c1", "sender.username": "ada"})
        );
        assert!(!rule_references_recipient(rule, &c.defaults));
    }

    #[test]
    fn nudges_skip_defaults_and_topics_default_to_the_id() {
        let c = cfg("defaults: { notification: { icon: /i.png } }\nrules: { n: { table: t, to: u, data: { who: '{{recipient}}' } } }");
        let rule = &c.rules["n"];
        let record = json!({"u": "user:a"});
        let row = RowRef {
            table: "t",
            id: "t:1",
            op: Op::Update,
            record: &record,
        };
        let ctx = with_recipient(&context(row, "n", &Map::new(), 5), "user:a");
        let topic = render_topic(rule, &ctx, row.id);
        assert_eq!(topic, "t:1");
        let p = rule_payload("n", rule, &c.defaults, row, &ctx, &topic, 5);
        assert!(p.notification.is_none());
        assert_eq!(p.data.unwrap(), json!({"who": "user:a"}));
        assert!(rule_references_recipient(rule, &c.defaults));
    }

    #[test]
    fn with_names_and_builtins_win_over_row_fields() {
        let record = json!({"id": "t:1", "table": "shadowed", "sender": "user:a", "x": 1});
        let row = RowRef {
            table: "t",
            id: "t:1",
            op: Op::Create,
            record: &record,
        };
        let mut with = Map::new();
        with.insert("sender".into(), json!({"username": "ada"}));
        let ctx = context(row, "r", &with, 7);
        assert_eq!(ctx["table"], json!("t"));
        assert_eq!(ctx["sender"], json!({"username": "ada"}));
        assert_eq!(ctx["row"]["sender"], json!("user:a"));
        assert_eq!(ctx["x"], json!(1));
        assert_eq!(ctx["now"], json!(7));
        assert_eq!(ctx["rule"], json!("r"));
        assert_eq!(ctx["op"], json!("create"));
    }

    #[test]
    fn message_payloads_merge_defaults() {
        let defaults: RuleDefaults =
            serde_yaml::from_str("notification: { icon: /i.png, title: Default }").unwrap();
        let row =
            json!({"notification": {"title": "Hi", "body": null}, "data": {"a": 1}, "topic": "t1"});
        let p = message_payload("_00_push_message:x", &row, &defaults, 3);
        assert_eq!(p.kind, PayloadKind::Message);
        assert_eq!(p.message.as_deref(), Some("_00_push_message:x"));
        assert_eq!(
            p.notification.unwrap(),
            json!({"title": "Hi", "icon": "/i.png", "tag": "t1"})
        );
        assert_eq!(p.data.unwrap(), json!({"a": 1}));
        let nudge = message_payload("m:1", &json!({"data": null}), &defaults, 3);
        assert!(nudge.notification.is_none() && nudge.data.is_none() && nudge.topic.is_none());
        let wire = serde_json::to_value(&nudge).unwrap();
        assert_eq!(
            wire,
            json!({"v": 1, "kind": "message", "message": "m:1", "ts": 3})
        );
    }

    fn big_payload(body_len: usize, data_len: usize) -> PushPayload {
        PushPayload {
            v: 1,
            kind: PayloadKind::Rule,
            rule: Some("r".into()),
            message: None,
            table: Some("t".into()),
            id: Some("t:1".into()),
            op: Some(Op::Create),
            topic: Some("t:1".into()),
            notification: Some(json!({"title": "T", "body": "é\"".repeat(body_len / 3)})),
            data: Some(json!({"blob": "x".repeat(data_len)})),
            ts: 1,
        }
    }

    #[test]
    fn size_cap_degrades_in_order() {
        let small = encode_payload(&big_payload(30, 30)).unwrap();
        assert!(small.degraded.is_empty());

        let no_data = encode_payload(&big_payload(30, 5000)).unwrap();
        assert_eq!(no_data.degraded, vec!["data"]);
        let v: PushPayload = serde_json::from_slice(&no_data.bytes).unwrap();
        assert!(v.data.is_none() && v.notification.is_some());

        let cut = encode_payload(&big_payload(9000, 10)).unwrap();
        assert_eq!(cut.degraded, vec!["data", "body"]);
        assert!(cut.bytes.len() <= MAX_PLAINTEXT);
        let v: PushPayload = serde_json::from_slice(&cut.bytes).unwrap();
        let body = v.notification.unwrap()["body"]
            .as_str()
            .unwrap()
            .to_string();
        assert!(body.ends_with("..."));
        assert!(body.len() > 1000, "cut only as much as needed");
        assert!(
            cut.bytes.len() > MAX_PLAINTEXT - 16,
            "{} bytes",
            cut.bytes.len()
        );

        let mut huge_title = big_payload(10, 10);
        huge_title.notification = Some(json!({"title": "x".repeat(5000)}));
        let nudge = encode_payload(&huge_title).unwrap();
        assert_eq!(nudge.degraded, vec!["data", "notification"]);
        let v: PushPayload = serde_json::from_slice(&nudge.bytes).unwrap();
        assert!(v.notification.is_none());
    }

    #[test]
    fn topic_headers_are_short_and_safe() {
        let h = topic_header("dm:conversation:⟨weird key⟩ with spaces");
        assert_eq!(h.len(), 32);
        assert!(h
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_'));
        assert_eq!(h, topic_header("dm:conversation:⟨weird key⟩ with spaces"));
        assert_ne!(h, topic_header("other"));
    }
}
