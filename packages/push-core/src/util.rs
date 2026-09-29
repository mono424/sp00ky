//! Small helpers every module needs: record ids in their several wire shapes,
//! timestamps in their several wire shapes, base64 in its several alphabets.

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use serde_json::Value;
use sha2::{Digest, Sha256};

/// Wall clock in epoch milliseconds. `web-time` so the crate needs no clock
/// backend of its own on wasm32.
pub fn now_ms() -> u64 {
    web_time::SystemTime::now()
        .duration_since(web_time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

pub fn b64url(bytes: &[u8]) -> String {
    URL_SAFE_NO_PAD.encode(bytes)
}

/// Base64 in whatever form a browser or a library printed it: url-safe or
/// standard alphabet, with or without padding, possibly wrapped.
pub fn decode_b64_any(s: &str) -> Result<Vec<u8>, String> {
    let cleaned: String = s
        .chars()
        .filter(|c| !c.is_whitespace() && *c != '=')
        .map(|c| match c {
            '+' => '-',
            '/' => '_',
            other => other,
        })
        .collect();
    URL_SAFE_NO_PAD
        .decode(cleaned.as_bytes())
        .map_err(|e| format!("invalid base64: {e}"))
}

pub fn sha256(bytes: &[u8]) -> [u8; 32] {
    Sha256::digest(bytes).into()
}

pub fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

// ── Record ids ───────────────────────────────────────────────────────────

/// Strip the quoting SurrealDB puts around keys that need escaping
/// (`user:⟨a-b⟩`, ``user:`a-b` ``). Two sides that print the same id
/// differently must still compare equal.
pub fn unquote_key(key: &str) -> &str {
    if let Some(inner) = key.strip_prefix('⟨').and_then(|k| k.strip_suffix('⟩')) {
        return inner;
    }
    if let Some(inner) = key.strip_prefix('`').and_then(|k| k.strip_suffix('`')) {
        return inner;
    }
    key
}

fn valid_table(table: &str) -> bool {
    !table.is_empty() && table.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
}

/// `(table, key)` of a record id string, key unquoted. `None` for anything
/// that is not shaped like one (no `:`, empty parts, a table with spaces).
pub fn split_record_id(s: &str) -> Option<(&str, &str)> {
    let s = s.trim();
    let (table, raw_key) = s.split_once(':')?;
    let key = unquote_key(raw_key);
    let quoted = key.len() != raw_key.len();
    // An unquoted key never holds whitespace: "Note: hi" is text, not an id.
    if !valid_table(table) || key.is_empty() || (!quoted && key.contains(char::is_whitespace)) {
        return None;
    }
    Some((table, key))
}

/// Canonical `table:key` (key unquoted), or `None` when `s` is not a record id.
pub fn canonical_record_id(s: &str) -> Option<String> {
    split_record_id(s).map(|(t, k)| format!("{t}:{k}"))
}

/// A record id in object form: `{tb, id}` (the SDKs) or `{table, key}`.
/// Exactly those two keys, so an ordinary row that happens to have an `id`
/// and a `tb` column among others is not mistaken for one.
pub fn record_id_object(v: &Value) -> Option<String> {
    let map = v.as_object()?;
    if map.len() != 2 {
        return None;
    }
    let (tb, id) = if let (Some(tb), Some(id)) = (map.get("tb"), map.get("id")) {
        (tb, id)
    } else if let (Some(tb), Some(id)) = (map.get("table"), map.get("key")) {
        (tb, id)
    } else {
        return None;
    };
    let tb = tb.as_str()?;
    let id = match id {
        Value::String(s) => unquote_key(s).to_string(),
        Value::Number(n) => n.to_string(),
        other => other.to_string(),
    };
    Some(format!("{tb}:{id}"))
}

/// Replace record-id objects with their `table:key` string, at any depth.
pub fn collapse_record_ids(v: Value) -> Value {
    if let Some(id) = record_id_object(&v) {
        return Value::String(id);
    }
    match v {
        Value::Array(items) => Value::Array(items.into_iter().map(collapse_record_ids).collect()),
        Value::Object(map) => Value::Object(
            map.into_iter()
                .map(|(k, v)| (k, collapse_record_ids(v)))
                .collect(),
        ),
        other => other,
    }
}

/// The key bound for `type::record($tb, $key)`, from the key as SurrealDB
/// printed it. An unquoted all-digit key is an integer key (`message:123`); a
/// quoted one (``message:`123` ``) is a string that only looks numeric.
/// Binding the wrong type would address a different record.
pub fn key_value(raw_key: &str) -> Value {
    let key = unquote_key(raw_key);
    let quoted = key.len() != raw_key.len();
    if !quoted && !key.is_empty() && key.len() < 19 && key.chars().all(|c| c.is_ascii_digit()) {
        if let Ok(n) = key.parse::<i64>() {
            return Value::from(n);
        }
    }
    Value::String(key.to_string())
}

/// `(table, key)` of a record id string, ready to bind for
/// `type::record($tb, $key)`.
pub fn record_binding(s: &str) -> Option<(String, Value)> {
    let s = s.trim();
    let (table, _) = split_record_id(s)?;
    let raw_key = &s[table.len() + 1..];
    Some((table.to_string(), key_value(raw_key)))
}

// ── Time ─────────────────────────────────────────────────────────────────

/// A timestamp in epoch ms: an RFC 3339 datetime (what flattened SurrealDB
/// datetimes look like), or a number (epoch ms when > 1e11, else seconds),
/// or a numeric string.
pub fn epoch_millis(v: &Value) -> Option<i64> {
    match v {
        Value::Number(n) => {
            let f = n.as_f64()?;
            if !f.is_finite() {
                return None;
            }
            Some(if f.abs() > 1e11 {
                f as i64
            } else {
                (f * 1000.0) as i64
            })
        }
        Value::String(s) => {
            let s = s.trim();
            let s = s
                .strip_prefix("d'")
                .and_then(|s| s.strip_suffix('\''))
                .unwrap_or(s);
            if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(s) {
                return Some(dt.timestamp_millis());
            }
            if let Ok(f) = s.parse::<f64>() {
                return epoch_millis(&serde_json::json!(f));
            }
            None
        }
        _ => None,
    }
}

// ── Hashing ──────────────────────────────────────────────────────────────

/// Hash of a JSON value that does not depend on object key order (serde_json
/// may be built with `preserve_order` somewhere in the workspace).
pub fn stable_hash(v: &Value) -> String {
    let mut buf = String::new();
    canonical_json(v, &mut buf);
    hex(&sha256(buf.as_bytes())[..16])
}

fn canonical_json(v: &Value, out: &mut String) {
    match v {
        Value::Object(map) => {
            let mut keys: Vec<&String> = map.keys().collect();
            keys.sort();
            out.push('{');
            for (i, k) in keys.into_iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                out.push_str(&Value::String(k.clone()).to_string());
                out.push(':');
                canonical_json(&map[k], out);
            }
            out.push('}');
        }
        Value::Array(items) => {
            out.push('[');
            for (i, item) in items.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                canonical_json(item, out);
            }
            out.push(']');
        }
        other => out.push_str(&other.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn record_ids_in_every_shape() {
        assert_eq!(canonical_record_id("user:abc").as_deref(), Some("user:abc"));
        assert_eq!(
            canonical_record_id(" user:⟨a-b⟩ ").as_deref(),
            Some("user:a-b")
        );
        assert_eq!(
            canonical_record_id("user:`a-b`").as_deref(),
            Some("user:a-b")
        );
        assert_eq!(canonical_record_id("hello"), None);
        assert_eq!(canonical_record_id("Note: hi"), None);
        assert_eq!(
            canonical_record_id("user:⟨a b⟩").as_deref(),
            Some("user:a b")
        );
        assert_eq!(canonical_record_id("two words:x"), None);
        assert_eq!(canonical_record_id(":x"), None);
        assert_eq!(canonical_record_id("user:"), None);
        assert_eq!(
            record_id_object(&json!({"tb": "user", "id": "abc"})).as_deref(),
            Some("user:abc")
        );
        assert_eq!(
            record_id_object(&json!({"table": "user", "key": 7})).as_deref(),
            Some("user:7")
        );
        assert_eq!(
            record_id_object(&json!({"tb": "user", "id": "abc", "x": 1})),
            None
        );
        assert_eq!(
            collapse_record_ids(json!({"a": [{"tb": "u", "id": "1"}], "b": 2})),
            json!({"a": ["u:1"], "b": 2})
        );
    }

    #[test]
    fn key_values_keep_numeric_keys_numeric() {
        assert_eq!(key_value("123"), json!(123));
        assert_eq!(key_value("`123`"), json!("123"));
        assert_eq!(key_value("⟨123⟩"), json!("123"));
        assert_eq!(key_value("abc"), json!("abc"));
        assert_eq!(key_value("0123abc"), json!("0123abc"));
        assert_eq!(record_binding("m:`0a1`"), Some(("m".into(), json!("0a1"))));
        assert_eq!(record_binding("m:42"), Some(("m".into(), json!(42))));
        assert_eq!(record_binding("nope"), None);
    }

    #[test]
    fn timestamps_in_every_shape() {
        assert_eq!(
            epoch_millis(&json!("2024-01-01T00:00:00Z")),
            Some(1_704_067_200_000)
        );
        assert_eq!(
            epoch_millis(&json!("2024-01-01T00:00:00.5Z")),
            Some(1_704_067_200_500)
        );
        assert_eq!(
            epoch_millis(&json!("d'2024-01-01T00:00:00Z'")),
            Some(1_704_067_200_000)
        );
        assert_eq!(
            epoch_millis(&json!(1_704_067_200_000u64)),
            Some(1_704_067_200_000)
        );
        assert_eq!(
            epoch_millis(&json!(1_704_067_200u64)),
            Some(1_704_067_200_000)
        );
        assert_eq!(epoch_millis(&json!("1704067200")), Some(1_704_067_200_000));
        assert_eq!(epoch_millis(&json!("yesterday")), None);
        assert_eq!(epoch_millis(&json!(null)), None);
    }

    #[test]
    fn base64_in_every_alphabet() {
        let bytes = vec![0xfb, 0xff, 0xfe, 0x01];
        assert_eq!(decode_b64_any("-__-AQ").unwrap(), bytes);
        assert_eq!(decode_b64_any("+//+AQ==").unwrap(), bytes);
        assert_eq!(decode_b64_any("+//+\nAQ").unwrap(), bytes);
        assert!(decode_b64_any("*").is_err());
    }

    #[test]
    fn stable_hash_ignores_key_order() {
        let a: Value = serde_json::from_str(r#"{"a":1,"b":{"x":[1,2],"y":null}}"#).unwrap();
        let b: Value = serde_json::from_str(r#"{"b":{"y":null,"x":[1,2]},"a":1}"#).unwrap();
        assert_eq!(stable_hash(&a), stable_hash(&b));
        assert_ne!(stable_hash(&a), stable_hash(&json!({"a": 2})));
    }
}
