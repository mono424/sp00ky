//! Canonical state hash of a folded document, so a client can check that the
//! document it built from the same rows is the one the server published.
//! Mirror: `packages/core/src/modules/crdt/hash.ts`; both are checked against
//! `tests/fixtures/crdt_state_hash.json`.
//!
//!   state_hash(doc) = hex(SHA-256(utf8(JCS(roots(doc.get_deep_value())))))
//!
//! JCS is RFC 8785, with this mapping of Loro values:
//! - object keys sorted by UTF-16 code units, strings escaped like
//!   ECMAScript `JSON.stringify`, numbers formatted like `Number#toString`
//!   (`ryu-js`; std's shortest digits round ties the other way);
//! - `I64` as the nearest `f64` (what `loro-crdt` hands to JS), NaN and the
//!   infinities as `{"$f64": "NaN" | "Infinity" | "-Infinity"}`, binary as
//!   `{"$bytes": "<padded standard base64>"}`;
//! - a text is its string (marks are not part of the hash);
//! - root keys holding `""`, `{}`, `[]` or `0` are dropped: a root exists as
//!   soon as code touches it and snapshots keep it, but a document built from
//!   update blobs alone does not have it.

use base64::Engine;
use loro::{LoroDoc, LoroValue};
use sha2::{Digest, Sha256};
use std::cmp::Ordering;
use std::fmt::Write;

/// Hex SHA-256 of the document's canonical JSON.
pub fn state_hash(doc: &LoroDoc) -> String {
    let digest = Sha256::digest(canonical_state(doc).as_bytes());
    let mut s = String::with_capacity(64);
    for b in digest {
        let _ = write!(s, "{b:02x}");
    }
    s
}

/// The canonical JSON the hash is taken over.
pub fn canonical_state(doc: &LoroDoc) -> String {
    let mut out = String::new();
    match doc.get_deep_value() {
        LoroValue::Map(m) => {
            let mut keys: Vec<&String> = m.keys().filter(|k| !is_empty_root(&m[*k])).collect();
            keys.sort_by(|a, b| cmp_utf16(a, b));
            write_object(keys.into_iter().map(|k| (k.as_str(), &m[k])), &mut out);
        }
        other => write_value(&other, &mut out),
    }
    out
}

fn is_empty_root(v: &LoroValue) -> bool {
    match v {
        LoroValue::String(s) => s.is_empty(),
        LoroValue::List(l) => l.is_empty(),
        LoroValue::Map(m) => m.is_empty(),
        LoroValue::Double(d) => *d == 0.0,
        LoroValue::I64(i) => *i == 0,
        _ => false,
    }
}

fn write_value(v: &LoroValue, out: &mut String) {
    match v {
        LoroValue::Null => out.push_str("null"),
        LoroValue::Bool(b) => out.push_str(if *b { "true" } else { "false" }),
        LoroValue::Double(d) => write_number(*d, out),
        LoroValue::I64(i) => write_number(*i as f64, out),
        LoroValue::String(s) => write_string(s, out),
        LoroValue::Binary(b) => {
            let encoded = base64::engine::general_purpose::STANDARD.encode(b.as_slice());
            write_object([("$bytes", &LoroValue::String(encoded.into()))].into_iter(), out);
        }
        LoroValue::List(l) => {
            out.push('[');
            for (i, item) in l.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                write_value(item, out);
            }
            out.push(']');
        }
        LoroValue::Map(m) => {
            let mut keys: Vec<&String> = m.keys().collect();
            keys.sort_by(|a, b| cmp_utf16(a, b));
            write_object(keys.into_iter().map(|k| (k.as_str(), &m[k])), out);
        }
        // `get_deep_value` resolves every container; an id here would be a bug.
        LoroValue::Container(id) => write_string(&id.to_string(), out),
    }
}

fn write_object<'a>(members: impl Iterator<Item = (&'a str, &'a LoroValue)>, out: &mut String) {
    out.push('{');
    for (i, (k, v)) in members.enumerate() {
        if i > 0 {
            out.push(',');
        }
        write_string(k, out);
        out.push(':');
        write_value(v, out);
    }
    out.push('}');
}

fn write_number(x: f64, out: &mut String) {
    if x == 0.0 {
        out.push('0'); // also -0
    } else if x.is_finite() {
        out.push_str(ryu_js::Buffer::new().format_finite(x));
    } else {
        let tag = if x.is_nan() {
            "NaN"
        } else if x > 0.0 {
            "Infinity"
        } else {
            "-Infinity"
        };
        out.push_str("{\"$f64\":");
        write_string(tag, out);
        out.push('}');
    }
}

/// ECMAScript `JSON.stringify` string quoting (RFC 8785 strings).
fn write_string(s: &str, out: &mut String) {
    out.push('"');
    for ch in s.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\u{08}' => out.push_str("\\b"),
            '\t' => out.push_str("\\t"),
            '\n' => out.push_str("\\n"),
            '\u{0C}' => out.push_str("\\f"),
            '\r' => out.push_str("\\r"),
            c if (c as u32) < 0x20 => {
                let _ = write!(out, "\\u{:04x}", c as u32);
            }
            c => out.push(c),
        }
    }
    out.push('"');
}

fn cmp_utf16(a: &str, b: &str) -> Ordering {
    a.encode_utf16().cmp(b.encode_utf16())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(serde::Deserialize)]
    struct Vector {
        name: String,
        snapshot: String,
        canonical: String,
        hash: String,
    }

    #[test]
    fn matches_the_shared_vectors() {
        let raw = include_str!("../../tests/fixtures/crdt_state_hash.json");
        let vectors: Vec<Vector> = serde_json::from_str(raw).unwrap();
        assert!(!vectors.is_empty());
        for v in vectors {
            let bytes = base64::engine::general_purpose::STANDARD.decode(&v.snapshot).unwrap();
            let doc = LoroDoc::from_snapshot(&bytes).unwrap();
            assert_eq!(canonical_state(&doc), v.canonical, "{}", v.name);
            assert_eq!(state_hash(&doc), v.hash, "{}", v.name);
        }
    }

    #[test]
    fn numbers_format_like_ecmascript() {
        let cases = [(1e21, "1e+21"), (1e-7, "1e-7"), (0.1 + 0.2, "0.30000000000000004"), (-0.0, "0"), (1e15 + 0.25, "1000000000000000.2")];
        for (x, want) in cases {
            let mut s = String::new();
            write_number(x, &mut s);
            assert_eq!(s, want);
        }
    }
}
