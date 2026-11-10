//! SurrealDB's CBOR protocol carries records, datetimes, etc. as custom tags
//! (mirrors `surrealdb.js`). The JSON-RPC wire could only send a record id as a
//! plain string, which SurrealDB won't match against a `record<...>` field, so
//! record-filtered queries (`WHERE database = $db`, list_ref `$in->_00_list_ref...`,
//! `SELECT * FROM $ids`) silently matched nothing. CBOR sends real records.
//!
//! On the way OUT typed values ([`Value::RecordId`], [`Value::Datetime`],
//! [`Value::Duration`]) map to their tagged forms; on the way IN everything is
//! normalized back to the JSON shapes the rest of the core reads: records as
//! `"table:id"` strings, datetimes as ISO-8601 strings, uuids, decimals and
//! durations as strings.

use ciborium::value::{Integer, Value as Cbor};

use crate::error::{Error, Result};
use crate::value::{iso8601, Object, RecordId, RecordKey, SurrealDuration, Value};

/// The SurrealDB CBOR tags.
pub mod tag {
    /// RFC 3339 string (CBOR standard tag 0).
    pub const SPEC_DATETIME: u64 = 0;
    /// Epoch seconds (CBOR standard tag 1).
    pub const EPOCH_DATETIME: u64 = 1;
    pub const NONE: u64 = 6;
    pub const TABLE: u64 = 7;
    /// `[table, id]`.
    pub const RECORD_ID: u64 = 8;
    pub const STRING_UUID: u64 = 9;
    pub const STRING_DECIMAL: u64 = 10;
    /// `[seconds, nanoseconds]`.
    pub const CUSTOM_DATETIME: u64 = 12;
    pub const STRING_DURATION: u64 = 13;
    /// `[seconds, nanoseconds]`.
    pub const CUSTOM_DURATION: u64 = 14;
    /// 16 raw bytes.
    pub const SPEC_UUID: u64 = 37;
}

/// Encode a value to SurrealDB-flavored CBOR bytes.
pub fn encode(value: &Value) -> Vec<u8> {
    let mut out = Vec::new();
    // Writing into a Vec cannot fail, and every `Cbor` we build is encodable.
    ciborium::into_writer(&to_cbor(value), &mut out).expect("CBOR encoding into a Vec");
    out
}

/// Decode SurrealDB CBOR bytes to a normalized value (records and datetimes
/// as strings, maps as [`Object`]s).
pub fn decode(bytes: &[u8]) -> Result<Value> {
    let raw: Cbor = ciborium::from_reader(bytes)
        .map_err(|e| Error::Network(format!("invalid CBOR frame: {e}")))?;
    Ok(from_cbor(raw))
}

fn int(i: impl Into<Integer>) -> Cbor {
    Cbor::Integer(i.into())
}

/// A value as CBOR, with SurrealDB tags for the typed variants.
///
/// A `None` map entry is left out (SurrealDB reads a missing field as `NONE`);
/// a `None` anywhere else is the `NONE` tag.
pub fn to_cbor(v: &Value) -> Cbor {
    match v {
        Value::None => Cbor::Tag(tag::NONE, Box::new(Cbor::Null)),
        Value::Null => Cbor::Null,
        Value::Bool(b) => Cbor::Bool(*b),
        Value::Int(i) => int(*i),
        Value::Float(f) => Cbor::Float(*f),
        Value::String(s) => Cbor::Text(s.clone()),
        // A byte buffer is a SurrealDB `bytes` value, not an array of ints: the
        // latter is what `f"...".put()` would store, and it is not a file.
        Value::Bytes(b) => Cbor::Bytes(b.clone()),
        Value::Array(a) => Cbor::Array(a.iter().map(to_cbor).collect()),
        Value::Object(o) => Cbor::Map(
            o.iter()
                .filter(|(_, v)| !v.is_none())
                .map(|(k, v)| (Cbor::Text(k.clone()), to_cbor(v)))
                .collect(),
        ),
        Value::RecordId(r) => record_to_cbor(r),
        Value::Datetime(d) => {
            // Floor seconds plus a non-negative nanosecond part, so a datetime
            // before the epoch survives too.
            let parts = vec![int(d.timestamp()), int(d.timestamp_subsec_nanos())];
            Cbor::Tag(tag::CUSTOM_DATETIME, Box::new(Cbor::Array(parts)))
        }
        Value::Duration(d) => duration_to_cbor(d),
    }
}

fn record_to_cbor(r: &RecordId) -> Cbor {
    let key = match &r.key {
        RecordKey::Int(i) => int(*i),
        RecordKey::String(s) => Cbor::Text(s.clone()),
    };
    Cbor::Tag(
        tag::RECORD_ID,
        Box::new(Cbor::Array(vec![Cbor::Text(r.table.clone()), key])),
    )
}

/// SurrealDB 4 refuses the string form (tag 13) with a 400 for the whole
/// request; `[seconds, nanoseconds]` works on 3.x too. A string the parser does
/// not know (it is not a SurrealQL duration either) keeps tag 13, which 3.x
/// parses and 4.x refuses as before.
fn duration_to_cbor(d: &SurrealDuration) -> Cbor {
    match d.parts() {
        Some((secs, nanos)) => Cbor::Tag(
            tag::CUSTOM_DURATION,
            Box::new(Cbor::Array(vec![int(secs), int(nanos)])),
        ),
        None => Cbor::Tag(tag::STRING_DURATION, Box::new(Cbor::Text(d.0.clone()))),
    }
}

fn integer_value(i: Integer) -> Value {
    let wide = i128::from(i);
    i64::try_from(wide)
        .map(Value::Int)
        .unwrap_or(Value::Float(wide as f64))
}

fn integer_i64(v: &Cbor) -> Option<i64> {
    match v {
        Cbor::Integer(i) => i64::try_from(i128::from(*i)).ok(),
        _ => None,
    }
}

/// `secs` + `nanos` since the epoch as Dart's `toIso8601String` spells it,
/// at the microsecond precision a Dart `DateTime` holds.
fn epoch_iso(secs: i64, nanos: i64) -> Option<String> {
    let micros = secs
        .checked_mul(1_000_000)?
        .checked_add(nanos.div_euclid(1000))?;
    chrono::DateTime::from_timestamp_micros(micros).map(|d| iso8601(&d))
}

/// CBOR to a normalized value (the inverse of [`to_cbor`]).
pub fn from_cbor(v: Cbor) -> Value {
    match v {
        Cbor::Tag(t, inner) => from_tagged(t, *inner),
        Cbor::Null => Value::Null,
        Cbor::Bool(b) => Value::Bool(b),
        Cbor::Integer(i) => integer_value(i),
        Cbor::Float(f) => Value::Float(f),
        Cbor::Text(s) => Value::String(s),
        Cbor::Bytes(b) => Value::Bytes(b),
        Cbor::Array(a) => Value::Array(a.into_iter().map(from_cbor).collect()),
        Cbor::Map(m) => {
            let mut out = Object::new();
            for (k, v) in m {
                out.insert(from_cbor(k).to_id_string(), from_cbor(v));
            }
            Value::Object(out)
        }
        // Anything ciborium adds later reads as nothing rather than failing the frame.
        _ => Value::Null,
    }
}

fn from_tagged(t: u64, inner: Cbor) -> Value {
    match (t, inner) {
        (tag::NONE, _) => Value::None,
        (tag::RECORD_ID, Cbor::Array(parts)) if parts.len() >= 2 => {
            let mut parts = parts.into_iter();
            let table = from_cbor(parts.next().unwrap_or(Cbor::Null)).to_id_string();
            let key = from_cbor(parts.next().unwrap_or(Cbor::Null)).to_id_string();
            Value::String(format!("{table}:{key}"))
        }
        // The `StringRecordId` form.
        (tag::RECORD_ID, Cbor::Text(s)) => Value::String(s),
        (tag::CUSTOM_DATETIME, Cbor::Array(parts))
            if parts.len() >= 2
                && integer_i64(&parts[0]).is_some()
                && integer_i64(&parts[1]).is_some() =>
        {
            let secs = integer_i64(&parts[0]).unwrap_or(0);
            let nanos = integer_i64(&parts[1]).unwrap_or(0);
            epoch_iso(secs, nanos)
                .map(Value::String)
                .unwrap_or(Value::Null)
        }
        (tag::SPEC_DATETIME | tag::EPOCH_DATETIME, Cbor::Text(s)) => Value::String(s),
        (tag::SPEC_DATETIME | tag::EPOCH_DATETIME, Cbor::Integer(i)) => {
            i64::try_from(i128::from(i))
                .ok()
                .and_then(|secs| epoch_iso(secs, 0))
                .map(Value::String)
                .unwrap_or(Value::Null)
        }
        (
            tag::TABLE | tag::STRING_UUID | tag::STRING_DECIMAL | tag::STRING_DURATION,
            Cbor::Text(s),
        ) => Value::String(s),
        // The server's own duration form, compact (`[5400]`, `[0, 250000000]`):
        // read back as the SurrealQL text a `SurrealDuration` holds.
        (tag::CUSTOM_DURATION, Cbor::Array(parts))
            if parts.len() <= 2 && parts.iter().all(|p| integer_i64(p).is_some_and(|n| n >= 0)) =>
        {
            let part = |i: usize| parts.get(i).and_then(integer_i64).unwrap_or(0) as u64;
            Value::String(duration_text(part(0), part(1)))
        }
        // A binary uuid, e.g. a LIVE query id. Rendered canonically so it
        // round-trips back to SurrealDB (KILL) and matches notification ids.
        (tag::SPEC_UUID, Cbor::Bytes(b)) => Value::String(bytes_to_uuid(&b)),
        // Untagged in effect: convert by structural type.
        (_, inner) => from_cbor(inner),
    }
}

/// `(seconds, nanoseconds)` as SurrealQL spells a duration (`1h30m`,
/// `1s250ms`, `0ns`), largest unit first, a year being 365 days.
pub fn duration_text(secs: u64, nanos: u64) -> String {
    use std::fmt::Write;
    let mut secs = secs.saturating_add(nanos / 1_000_000_000);
    let nanos = nanos % 1_000_000_000;
    if secs == 0 && nanos == 0 {
        return "0ns".into();
    }
    let mut out = String::new();
    for (unit, size) in [
        ("y", 31_536_000),
        ("w", 604_800),
        ("d", 86_400),
        ("h", 3_600),
        ("m", 60),
        ("s", 1),
    ] {
        let n = secs / size;
        secs %= size;
        if n > 0 {
            let _ = write!(out, "{n}{unit}");
        }
    }
    for (n, unit) in [
        (nanos / 1_000_000, "ms"),
        (nanos / 1_000 % 1_000, "µs"),
        (nanos % 1_000, "ns"),
    ] {
        if n > 0 {
            let _ = write!(out, "{n}{unit}");
        }
    }
    out
}

/// 16 raw bytes as a canonical uuid (`8-4-4-4-12`); any other length as plain
/// hex.
fn bytes_to_uuid(bytes: &[u8]) -> String {
    let hex = hex::encode(bytes);
    if hex.len() != 32 {
        return hex;
    }
    format!(
        "{}-{}-{}-{}-{}",
        &hex[0..8],
        &hex[8..12],
        &hex[12..16],
        &hex[16..20],
        &hex[20..]
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{TimeZone, Utc};
    use serde_json::json;

    /// A field of a raw-decoded CBOR map, tags intact.
    fn raw_field(bytes: &[u8], key: &str) -> Cbor {
        let raw: Cbor = ciborium::from_reader(bytes).unwrap();
        let Cbor::Map(entries) = raw else {
            panic!("not a map")
        };
        entries
            .into_iter()
            .find(|(k, _)| matches!(k, Cbor::Text(t) if t == key))
            .map(|(_, v)| v)
            .unwrap_or_else(|| panic!("no field {key:?} in decoded map"))
    }

    fn obj(entries: Vec<(&str, Value)>) -> Value {
        Value::Object(
            entries
                .into_iter()
                .map(|(k, v)| (k.to_string(), v))
                .collect(),
        )
    }

    fn tag_of(v: &Cbor) -> Option<u64> {
        match v {
            Cbor::Tag(t, _) => Some(*t),
            _ => None,
        }
    }

    #[test]
    fn record_id_is_tag_8_and_decodes_to_table_id() {
        let bytes = encode(&obj(vec![("id", RecordId::new("game", "abc").into())]));
        // On the wire it is a tagged record, NOT a string: the whole point.
        assert_eq!(tag_of(&raw_field(&bytes, "id")), Some(tag::RECORD_ID));
        // The normalized decode gives the "table:id" string downstream expects.
        assert_eq!(
            decode(&bytes).unwrap().get("id"),
            Some(&Value::from("game:abc"))
        );
        // Integer keys stay integers on the wire.
        let bytes = encode(&obj(vec![("id", RecordId::new("n", 7i64).into())]));
        let Cbor::Tag(_, inner) = raw_field(&bytes, "id") else {
            panic!()
        };
        assert_eq!(*inner, Cbor::Array(vec![Cbor::Text("n".into()), int(7)]));
        assert_eq!(decode(&bytes).unwrap().get("id"), Some(&Value::from("n:7")));
    }

    #[test]
    fn datetime_is_tag_12_and_decodes_to_iso() {
        let dt =
            Utc.with_ymd_and_hms(2026, 1, 2, 3, 4, 5).unwrap() + chrono::Duration::milliseconds(6);
        let bytes = encode(&obj(vec![("when", dt.into())]));
        let raw = raw_field(&bytes, "when");
        assert_eq!(tag_of(&raw), Some(tag::CUSTOM_DATETIME));
        let Cbor::Tag(_, inner) = raw else { panic!() };
        assert_eq!(
            *inner,
            Cbor::Array(vec![int(dt.timestamp()), int(6_000_000)])
        );
        assert_eq!(
            decode(&bytes).unwrap().get("when"),
            Some(&Value::from("2026-01-02T03:04:05.006Z"))
        );
        // Microseconds survive, as a Dart DateTime keeps them.
        let fine = dt + chrono::Duration::microseconds(7);
        let bytes = encode(&obj(vec![("when", fine.into())]));
        assert_eq!(
            decode(&bytes).unwrap().get("when"),
            Some(&Value::from("2026-01-02T03:04:05.006007Z"))
        );
    }

    #[test]
    fn datetime_before_the_epoch() {
        let dt = Utc.with_ymd_and_hms(1969, 12, 31, 23, 59, 58).unwrap()
            + chrono::Duration::milliseconds(500);
        let bytes = encode(&obj(vec![("when", dt.into())]));
        assert_eq!(
            decode(&bytes).unwrap().get("when"),
            Some(&Value::from("1969-12-31T23:59:58.500Z"))
        );
    }

    #[test]
    fn duration_is_seconds_and_nanos_tag_14() {
        // SurrealDB 4 answers 400 to the string form (tag 13).
        fn parts(text: &str) -> Vec<i128> {
            let raw = raw_field(
                &encode(&obj(vec![("d", SurrealDuration::new(text).into())])),
                "d",
            );
            let Cbor::Tag(t, inner) = raw else {
                panic!("untagged")
            };
            assert_eq!(t, tag::CUSTOM_DURATION);
            let Cbor::Array(items) = *inner else {
                panic!("not a list")
            };
            items
                .into_iter()
                .map(|i| match i {
                    Cbor::Integer(i) => i128::from(i),
                    other => panic!("{other:?}"),
                })
                .collect()
        }
        assert_eq!(parts("5m"), vec![300, 0]);
        assert_eq!(parts("1h30m"), vec![5400, 0]);
        assert_eq!(parts("1s250ms"), vec![1, 250_000_000]);
        assert_eq!(parts("2w1d"), vec![1_296_000, 0]);
        assert_eq!(parts("1y"), vec![31_536_000, 0]);
        assert_eq!(parts("7ns"), vec![0, 7]);
    }

    #[test]
    fn unreadable_duration_keeps_the_string_form() {
        let raw = raw_field(
            &encode(&obj(vec![("d", SurrealDuration::new("soon").into())])),
            "d",
        );
        assert_eq!(tag_of(&raw), Some(tag::STRING_DURATION));
        // And decodes back to its text.
        let bytes = encode(&obj(vec![("d", SurrealDuration::new("soon").into())]));
        assert_eq!(decode(&bytes).unwrap().get("d"), Some(&Value::from("soon")));
    }

    #[test]
    fn primitives_and_nested_round_trip() {
        let mut v = Value::from(json!({
            "s": "hi",
            "n": 42,
            "big": -1735689600,
            "f": 1.5,
            "b": true,
            "nil": null,
        }));
        let o = v.as_object_mut().unwrap();
        o.insert(
            "list".into(),
            Value::Array(vec![
                1i64.into(),
                "two".into(),
                RecordId::new("t", "x").into(),
            ]),
        );
        o.insert(
            "nested".into(),
            obj(vec![("a", RecordId::new("u", "1").into())]),
        );
        let out = decode(&encode(&v)).unwrap();
        assert_eq!(out.get("s"), Some(&Value::from("hi")));
        assert_eq!(out.get("n"), Some(&Value::Int(42)));
        assert_eq!(out.get("big"), Some(&Value::Int(-1735689600)));
        assert_eq!(out.get("f"), Some(&Value::Float(1.5)));
        assert_eq!(out.get("b"), Some(&Value::Bool(true)));
        assert_eq!(out.get("nil"), Some(&Value::Null));
        assert_eq!(
            out.get("list").unwrap().as_array().unwrap()[2],
            Value::from("t:x")
        );
        assert_eq!(
            out.get("nested").unwrap().get("a"),
            Some(&Value::from("u:1"))
        );
    }

    #[test]
    fn none_entries_are_omitted_and_none_elsewhere_is_tag_6() {
        let mut o = Object::new();
        o.insert("gone".into(), Value::None);
        o.insert("kept".into(), Value::Null);
        o.insert("list".into(), Value::Array(vec![Value::None]));
        let bytes = encode(&Value::Object(o));
        let raw: Cbor = ciborium::from_reader(&bytes[..]).unwrap();
        let Cbor::Map(entries) = raw else { panic!() };
        let keys: Vec<_> = entries.iter().map(|(k, _)| k.clone()).collect();
        assert_eq!(
            keys,
            vec![Cbor::Text("kept".into()), Cbor::Text("list".into())]
        );
        let out = decode(&bytes).unwrap();
        assert_eq!(out.get("kept"), Some(&Value::Null));
        assert_eq!(out.get("list").unwrap().as_array().unwrap()[0], Value::None);
    }

    #[test]
    fn bytes_round_trip_as_a_byte_string() {
        let payload = vec![0u8, 137, 80, 78, 255];
        let bytes = encode(&obj(vec![("content", Value::Bytes(payload.clone()))]));
        assert!(matches!(raw_field(&bytes, "content"), Cbor::Bytes(_)));
        assert_eq!(
            decode(&bytes).unwrap().get("content"),
            Some(&Value::Bytes(payload))
        );
    }

    #[test]
    fn rpc_envelope_round_trips() {
        let bytes = encode(&obj(vec![
            ("id", "1".into()),
            ("method", "query".into()),
            (
                "params",
                Value::Array(vec![
                    "SELECT * FROM $id".into(),
                    obj(vec![("id", RecordId::new("thing", "x").into())]),
                ]),
            ),
        ]));
        let out = decode(&bytes).unwrap();
        assert_eq!(out.get("method"), Some(&Value::from("query")));
        assert_eq!(
            out.get("params").unwrap().as_array().unwrap()[1].get("id"),
            Some(&Value::from("thing:x"))
        );
    }

    #[test]
    fn inbound_tags_normalize() {
        let uuid = Cbor::Tag(
            tag::SPEC_UUID,
            Box::new(Cbor::Bytes(
                hex::decode("0193f1d2a4b67c8d9e0f112233445566").unwrap(),
            )),
        );
        assert_eq!(
            from_cbor(uuid),
            Value::from("0193f1d2-a4b6-7c8d-9e0f-112233445566")
        );
        assert_eq!(
            from_cbor(Cbor::Tag(tag::NONE, Box::new(Cbor::Null))),
            Value::None
        );
        for t in [
            tag::TABLE,
            tag::STRING_UUID,
            tag::STRING_DECIMAL,
            tag::STRING_DURATION,
        ] {
            assert_eq!(
                from_cbor(Cbor::Tag(t, Box::new(Cbor::Text("x".into())))),
                Value::from("x")
            );
        }
        assert_eq!(
            from_cbor(Cbor::Tag(
                tag::RECORD_ID,
                Box::new(Cbor::Text("a:b".into()))
            )),
            Value::from("a:b")
        );
        assert_eq!(
            from_cbor(Cbor::Tag(
                tag::SPEC_DATETIME,
                Box::new(Cbor::Text("2026-01-01T00:00:00Z".into()))
            )),
            Value::from("2026-01-01T00:00:00Z")
        );
        assert_eq!(
            from_cbor(Cbor::Tag(tag::EPOCH_DATETIME, Box::new(int(86_400)))),
            Value::from("1970-01-02T00:00:00.000Z")
        );
        // An unknown tag converts its content structurally.
        assert_eq!(from_cbor(Cbor::Tag(99, Box::new(int(3)))), Value::Int(3));
    }

    #[test]
    fn server_durations_read_back_as_text() {
        let compact = |parts: Vec<i64>| {
            from_cbor(Cbor::Tag(
                tag::CUSTOM_DURATION,
                Box::new(Cbor::Array(parts.into_iter().map(int).collect())),
            ))
        };
        // As SurrealDB 3 sends them.
        assert_eq!(compact(vec![5400]), Value::from("1h30m"));
        assert_eq!(compact(vec![0, 250_000_000]), Value::from("250ms"));
        assert_eq!(compact(vec![]), Value::from("0ns"));
        assert_eq!(compact(vec![1, 1_001_001]), Value::from("1s1ms1µs1ns"));
        assert_eq!(compact(vec![31_536_000 + 691_200]), Value::from("1y1w1d"));
        // And the text parses back to the same parts.
        for text in ["1h30m", "1s1ms1µs1ns", "1y1w1d", "7ns"] {
            let (s, n) = SurrealDuration::new(text).parts().unwrap();
            assert_eq!(duration_text(s, n), text);
        }
        // Not a duration shape: structural.
        assert_eq!(
            compact(vec![-1]),
            Value::Array(vec![Value::Int(-1)]),
            "negative parts are not a duration"
        );
    }
}
