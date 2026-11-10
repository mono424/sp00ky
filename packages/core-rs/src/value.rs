//! The dynamic value every row, parameter and RPC result is made of.
//!
//! Mirrors the `dynamic` the Dart core passes around: rows read back from the
//! server or the local store hold only the JSON shapes (records as `"table:id"`
//! strings, datetimes as ISO-8601 strings) plus raw bytes, while values on the
//! way OUT may be typed ([`RecordId`], datetimes, durations), which the CBOR
//! codec sends as SurrealDB's tagged forms. A `record<user>` field therefore
//! has to be a [`Value::RecordId`] in a query's parameters, or SurrealDB will not
//! match it; [`crate::utils::parse_query_params`] re-types a row from the
//! schema before it is sent.
//!
//! Convert from `serde_json::Value` (so `serde_json::json!` works for literals)
//! and from any `Serialize` type with [`to_value`]; read rows into your own
//! types with [`from_value`].

use std::collections::BTreeMap;
use std::fmt;

use chrono::{DateTime, SecondsFormat, Utc};
use serde::de::{self, DeserializeOwned, MapAccess, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::error::{Error, Result};

/// A row or any other string-keyed map. Sorted, so its JSON spelling (which the
/// query keys hash) is stable.
pub type Object = BTreeMap<String, Value>;

/// One row as the engine stores and renders it.
pub type Row = Object;

/// A SurrealDB value.
#[derive(Clone, Debug, Default)]
pub enum Value {
    /// SurrealDB `NONE` / Dart `null`: the absent value. A map entry holding it
    /// is omitted on the wire.
    #[default]
    None,
    /// SurrealDB `NULL`.
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    String(String),
    Bytes(Vec<u8>),
    Array(Vec<Value>),
    Object(Object),
    /// A record link (`table:id`). Inbound records arrive as strings.
    RecordId(RecordId),
    /// A datetime. Inbound datetimes arrive as ISO-8601 strings.
    Datetime(DateTime<Utc>),
    /// A SurrealQL duration such as `10m` or `1h30m`.
    Duration(SurrealDuration),
}

impl Value {
    pub fn is_none(&self) -> bool {
        matches!(self, Value::None)
    }

    /// `None` or `Null`: what Dart reads as `null`.
    pub fn is_nullish(&self) -> bool {
        matches!(self, Value::None | Value::Null)
    }

    pub fn as_str(&self) -> Option<&str> {
        match self {
            Value::String(s) => Some(s),
            _ => None,
        }
    }

    pub fn as_bool(&self) -> Option<bool> {
        match self {
            Value::Bool(b) => Some(*b),
            _ => None,
        }
    }

    /// An integer, also from a float with no fractional part (JSON numbers).
    pub fn as_i64(&self) -> Option<i64> {
        match self {
            Value::Int(i) => Some(*i),
            Value::Float(f) if f.fract() == 0.0 && f.is_finite() => Some(*f as i64),
            _ => None,
        }
    }

    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Value::Int(i) => Some(*i as f64),
            Value::Float(f) => Some(*f),
            _ => None,
        }
    }

    pub fn as_array(&self) -> Option<&Vec<Value>> {
        match self {
            Value::Array(a) => Some(a),
            _ => None,
        }
    }

    pub fn as_object(&self) -> Option<&Object> {
        match self {
            Value::Object(o) => Some(o),
            _ => None,
        }
    }

    pub fn as_object_mut(&mut self) -> Option<&mut Object> {
        match self {
            Value::Object(o) => Some(o),
            _ => None,
        }
    }

    pub fn into_object(self) -> Option<Object> {
        match self {
            Value::Object(o) => Some(o),
            _ => None,
        }
    }

    pub fn into_array(self) -> Option<Vec<Value>> {
        match self {
            Value::Array(a) => Some(a),
            _ => None,
        }
    }

    /// `value[key]`, `None` when this is not a map or has no such key.
    pub fn get(&self, key: &str) -> Option<&Value> {
        self.as_object().and_then(|o| o.get(key))
    }

    /// The string form Dart's `toString()` gives an id-ish value: a record id
    /// or a string as is, a number without a trailing `.0`.
    pub fn to_id_string(&self) -> String {
        match self {
            Value::String(s) => s.clone(),
            Value::RecordId(r) => r.to_string(),
            Value::Int(i) => i.to_string(),
            Value::Float(f) if f.fract() == 0.0 => format!("{}", *f as i64),
            Value::Float(f) => f.to_string(),
            Value::Bool(b) => b.to_string(),
            Value::None | Value::Null => "null".to_string(),
            Value::Datetime(d) => iso8601(d),
            Value::Duration(d) => d.0.clone(),
            other => other.to_json().to_string(),
        }
    }

    /// The JSON shape of this value, as the local store keeps it and as the
    /// query keys hash it (Dart `jsonSafe`): records become `table:id`,
    /// datetimes ISO-8601, durations their text, bytes an array of numbers.
    /// `None` map entries are dropped.
    pub fn to_json(&self) -> serde_json::Value {
        use serde_json::Value as J;
        match self {
            Value::None | Value::Null => J::Null,
            Value::Bool(b) => J::Bool(*b),
            Value::Int(i) => J::from(*i),
            Value::Float(f) => serde_json::Number::from_f64(*f)
                .map(J::Number)
                .unwrap_or(J::Null),
            Value::String(s) => J::String(s.clone()),
            Value::Bytes(b) => J::Array(b.iter().map(|x| J::from(*x)).collect()),
            Value::Array(a) => J::Array(a.iter().map(Value::to_json).collect()),
            Value::Object(o) => J::Object(
                o.iter()
                    .filter(|(_, v)| !v.is_none())
                    .map(|(k, v)| (k.clone(), v.to_json()))
                    .collect(),
            ),
            Value::RecordId(r) => J::String(r.to_string()),
            Value::Datetime(d) => J::String(iso8601(d)),
            Value::Duration(d) => J::String(d.0.clone()),
        }
    }

    /// [`Value::to_json`] encoded as a string.
    pub fn to_json_string(&self) -> String {
        self.to_json().to_string()
    }
}

/// `2026-01-02T03:04:05.678Z`, the spelling Dart's `toIso8601String` gives a
/// UTC datetime (millisecond or microsecond precision as the value holds).
pub fn iso8601(d: &DateTime<Utc>) -> String {
    let format = if d.timestamp_subsec_nanos() % 1_000_000 == 0 {
        SecondsFormat::Millis
    } else {
        SecondsFormat::Micros
    };
    d.to_rfc3339_opts(format, true)
}

impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        use Value::*;
        match (self, other) {
            (None, None) | (Null, Null) => true,
            (Bool(a), Bool(b)) => a == b,
            (Int(a), Int(b)) => a == b,
            (Float(a), Float(b)) => a == b,
            (Int(a), Float(b)) | (Float(b), Int(a)) => (*a as f64) == *b,
            (String(a), String(b)) => a == b,
            (Bytes(a), Bytes(b)) => a == b,
            (Array(a), Array(b)) => a == b,
            (Object(a), Object(b)) => a == b,
            (RecordId(a), RecordId(b)) => a == b,
            (Datetime(a), Datetime(b)) => a == b,
            (Duration(a), Duration(b)) => a == b,
            // A record compares equal to its string spelling, as Dart's
            // `compareRecordIds` does.
            (RecordId(a), String(b)) | (String(b), RecordId(a)) => a.to_string() == *b,
            _ => false,
        }
    }
}

impl From<serde_json::Value> for Value {
    fn from(v: serde_json::Value) -> Self {
        use serde_json::Value as J;
        match v {
            J::Null => Value::Null,
            J::Bool(b) => Value::Bool(b),
            J::Number(n) => match n.as_i64() {
                Some(i) => Value::Int(i),
                None => Value::Float(n.as_f64().unwrap_or(f64::NAN)),
            },
            J::String(s) => Value::String(s),
            J::Array(a) => Value::Array(a.into_iter().map(Value::from).collect()),
            J::Object(o) => Value::Object(o.into_iter().map(|(k, v)| (k, Value::from(v))).collect()),
        }
    }
}

impl From<&serde_json::Value> for Value {
    fn from(v: &serde_json::Value) -> Self {
        Value::from(v.clone())
    }
}

macro_rules! from_scalar {
    ($($t:ty => $variant:ident as $cast:ty),* $(,)?) => {
        $(impl From<$t> for Value {
            fn from(v: $t) -> Self { Value::$variant(v as $cast) }
        })*
    };
}
from_scalar!(i64 => Int as i64, i32 => Int as i64, u32 => Int as i64, usize => Int as i64, u8 => Int as i64, f64 => Float as f64, f32 => Float as f64);

impl From<bool> for Value {
    fn from(v: bool) -> Self {
        Value::Bool(v)
    }
}
impl From<String> for Value {
    fn from(v: String) -> Self {
        Value::String(v)
    }
}
impl From<&str> for Value {
    fn from(v: &str) -> Self {
        Value::String(v.to_string())
    }
}
impl From<Vec<u8>> for Value {
    fn from(v: Vec<u8>) -> Self {
        Value::Bytes(v)
    }
}
impl From<Vec<Value>> for Value {
    fn from(v: Vec<Value>) -> Self {
        Value::Array(v)
    }
}
impl From<Object> for Value {
    fn from(v: Object) -> Self {
        Value::Object(v)
    }
}
impl From<RecordId> for Value {
    fn from(v: RecordId) -> Self {
        Value::RecordId(v)
    }
}
impl From<DateTime<Utc>> for Value {
    fn from(v: DateTime<Utc>) -> Self {
        Value::Datetime(v)
    }
}
impl From<SurrealDuration> for Value {
    fn from(v: SurrealDuration) -> Self {
        Value::Duration(v)
    }
}
impl<T: Into<Value>> From<Option<T>> for Value {
    fn from(v: Option<T>) -> Self {
        v.map(Into::into).unwrap_or(Value::None)
    }
}

/// A row from a `serde_json::json!({...})` literal. Non-object JSON gives an
/// empty row.
pub fn object_from_json(v: serde_json::Value) -> Object {
    Value::from(v).into_object().unwrap_or_default()
}

/// Any `Serialize` value as a [`Value`] (through its JSON form, so a
/// [`RecordId`] field becomes its `table:id` string; the schema re-types it on
/// the way out).
pub fn to_value<T: Serialize + ?Sized>(value: &T) -> Result<Value> {
    serde_json::to_value(value)
        .map(Value::from)
        .map_err(|e| Error::InvalidArgument(e.to_string()))
}

/// A [`Value`] (usually a row) as your own type.
pub fn from_value<T: DeserializeOwned>(value: Value) -> Result<T> {
    serde_json::from_value(value.to_json()).map_err(|e| Error::InvalidArgument(e.to_string()))
}

/// A row as your own type.
pub fn from_row<T: DeserializeOwned>(row: &Row) -> Result<T> {
    from_value(Value::Object(row.clone()))
}

impl Serialize for Value {
    fn serialize<S: Serializer>(&self, s: S) -> std::result::Result<S::Ok, S::Error> {
        match self {
            Value::None | Value::Null => s.serialize_none(),
            Value::Bool(b) => s.serialize_bool(*b),
            Value::Int(i) => s.serialize_i64(*i),
            Value::Float(f) => s.serialize_f64(*f),
            Value::String(v) => s.serialize_str(v),
            Value::Bytes(b) => s.serialize_bytes(b),
            Value::Array(a) => a.serialize(s),
            Value::Object(o) => {
                use serde::ser::SerializeMap;
                let mut m = s.serialize_map(None)?;
                for (k, v) in o.iter().filter(|(_, v)| !v.is_none()) {
                    m.serialize_entry(k, v)?;
                }
                m.end()
            }
            Value::RecordId(r) => s.serialize_str(&r.to_string()),
            Value::Datetime(d) => s.serialize_str(&iso8601(d)),
            Value::Duration(d) => s.serialize_str(&d.0),
        }
    }
}

impl<'de> Deserialize<'de> for Value {
    fn deserialize<D: Deserializer<'de>>(d: D) -> std::result::Result<Value, D::Error> {
        struct V;
        impl<'de> Visitor<'de> for V {
            type Value = Value;
            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                f.write_str("any value")
            }
            fn visit_bool<E>(self, v: bool) -> std::result::Result<Value, E> {
                Ok(Value::Bool(v))
            }
            fn visit_i64<E>(self, v: i64) -> std::result::Result<Value, E> {
                Ok(Value::Int(v))
            }
            fn visit_u64<E>(self, v: u64) -> std::result::Result<Value, E> {
                Ok(i64::try_from(v).map(Value::Int).unwrap_or(Value::Float(v as f64)))
            }
            fn visit_f64<E>(self, v: f64) -> std::result::Result<Value, E> {
                Ok(Value::Float(v))
            }
            fn visit_str<E>(self, v: &str) -> std::result::Result<Value, E> {
                Ok(Value::String(v.to_string()))
            }
            fn visit_string<E>(self, v: String) -> std::result::Result<Value, E> {
                Ok(Value::String(v))
            }
            fn visit_bytes<E>(self, v: &[u8]) -> std::result::Result<Value, E> {
                Ok(Value::Bytes(v.to_vec()))
            }
            fn visit_byte_buf<E>(self, v: Vec<u8>) -> std::result::Result<Value, E> {
                Ok(Value::Bytes(v))
            }
            fn visit_none<E>(self) -> std::result::Result<Value, E> {
                Ok(Value::Null)
            }
            fn visit_unit<E>(self) -> std::result::Result<Value, E> {
                Ok(Value::Null)
            }
            fn visit_some<D: Deserializer<'de>>(self, d: D) -> std::result::Result<Value, D::Error> {
                Value::deserialize(d)
            }
            fn visit_seq<A: SeqAccess<'de>>(self, mut a: A) -> std::result::Result<Value, A::Error> {
                let mut out = Vec::new();
                while let Some(v) = a.next_element()? {
                    out.push(v);
                }
                Ok(Value::Array(out))
            }
            fn visit_map<A: MapAccess<'de>>(self, mut a: A) -> std::result::Result<Value, A::Error> {
                let mut out = Object::new();
                while let Some((k, v)) = a.next_entry::<String, Value>()? {
                    out.insert(k, v);
                }
                Ok(Value::Object(out))
            }
        }
        d.deserialize_any(V)
    }
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.to_id_string())
    }
}

// ---- record ids --------------------------------------------------------------

/// The id part of a [`RecordId`]: SurrealDB ids may be strings or integers.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum RecordKey {
    String(String),
    Int(i64),
}

impl fmt::Display for RecordKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RecordKey::String(s) => f.write_str(s),
            RecordKey::Int(i) => write!(f, "{i}"),
        }
    }
}

impl From<&str> for RecordKey {
    fn from(v: &str) -> Self {
        RecordKey::String(v.to_string())
    }
}
impl From<String> for RecordKey {
    fn from(v: String) -> Self {
        RecordKey::String(v)
    }
}
impl From<i64> for RecordKey {
    fn from(v: i64) -> Self {
        RecordKey::Int(v)
    }
}

/// A SurrealDB record identifier (`table:id`).
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct RecordId {
    pub table: String,
    pub key: RecordKey,
}

impl RecordId {
    pub fn new(table: impl Into<String>, key: impl Into<RecordKey>) -> Self {
        RecordId {
            table: table.into(),
            key: key.into(),
        }
    }

    /// Parse `table:id`. The id keeps any further colons (TS
    /// `parseRecordIdString`); a string with no colon is a table with an empty id.
    pub fn parse(id: &str) -> Self {
        match id.split_once(':') {
            Some((table, key)) => RecordId::new(table, key),
            None => RecordId::new(id, ""),
        }
    }

    /// A fresh id for `table` (TS `generateNewTableId`).
    pub fn generate(table: impl Into<String>) -> Self {
        RecordId::new(table, generate_id())
    }
}

impl fmt::Display for RecordId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.table, self.key)
    }
}

impl Serialize for RecordId {
    fn serialize<S: Serializer>(&self, s: S) -> std::result::Result<S::Ok, S::Error> {
        s.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for RecordId {
    fn deserialize<D: Deserializer<'de>>(d: D) -> std::result::Result<Self, D::Error> {
        let s = String::deserialize(d)?;
        if !s.contains(':') {
            return Err(de::Error::custom(format!("not a record id: {s}")));
        }
        Ok(RecordId::parse(&s))
    }
}

/// `table:id` -> table (TS `extractTablePart`).
pub fn table_of(id: &str) -> &str {
    id.split_once(':').map(|(t, _)| t).unwrap_or(id)
}

/// `table:id` -> id (TS `extractIdPart`).
pub fn key_of(id: &str) -> &str {
    id.split_once(':').map(|(_, k)| k).unwrap_or("")
}

/// A SurrealQL duration (`10m`, `1h30m`), kept as text.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct SurrealDuration(pub String);

impl SurrealDuration {
    pub fn new(text: impl Into<String>) -> Self {
        SurrealDuration(text.into())
    }

    /// `(seconds, nanoseconds)`, or `None` when the text is not a SurrealQL
    /// duration. A year is 365 days, as in SurrealQL.
    pub fn parts(&self) -> Option<(u64, u64)> {
        let text = self.0.as_str();
        if text.is_empty() {
            return None;
        }
        let mut total: u128 = 0;
        let mut rest = text;
        while !rest.is_empty() {
            let digits = rest.chars().take_while(|c| c.is_ascii_digit()).count();
            if digits == 0 {
                return None;
            }
            let n: u128 = rest[..digits].parse().ok()?;
            rest = &rest[digits..];
            let (unit, len): (u128, usize) = if rest.starts_with("ns") {
                (1, 2)
            } else if rest.starts_with("us") {
                (1_000, 2)
            } else if rest.starts_with("µs") {
                (1_000, "µs".len())
            } else if rest.starts_with("ms") {
                (1_000_000, 2)
            } else if rest.starts_with('s') {
                (1_000_000_000, 1)
            } else if rest.starts_with('m') {
                (60_000_000_000, 1)
            } else if rest.starts_with('h') {
                (3_600_000_000_000, 1)
            } else if rest.starts_with('d') {
                (86_400_000_000_000, 1)
            } else if rest.starts_with('w') {
                (604_800_000_000_000, 1)
            } else if rest.starts_with('y') {
                (31_536_000_000_000_000, 1)
            } else {
                return None;
            };
            total += n * unit;
            rest = &rest[len..];
        }
        Some(((total / 1_000_000_000) as u64, (total % 1_000_000_000) as u64))
    }
}

impl fmt::Display for SurrealDuration {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

// ---- ids -------------------------------------------------------------------

/// A UUID v4 with the hyphens stripped (TS `generateId`).
pub fn generate_id() -> String {
    uuid::Uuid::new_v4().simple().to_string()
}

/// 32 random bytes, hex: the secret a client proves it owns its anonymous views
/// with (`$config.owner`; TS `mintOwnerSecret`).
pub fn generate_owner_secret() -> String {
    use rand::RngCore;
    let mut bytes = [0u8; 32];
    rand::rng().fill_bytes(&mut bytes);
    hex::encode(bytes)
}

/// Lowercase hex sha256 of `input`.
pub fn sha256_hex(input: &str) -> String {
    use sha2::{Digest, Sha256};
    hex::encode(Sha256::digest(input.as_bytes()))
}

/// Deep merge `patch` into `base` (maps merge recursively, everything else is
/// replaced). The local store's `MERGE`.
pub fn deep_merge(base: &mut Object, patch: &Object) {
    for (key, value) in patch {
        match (base.get_mut(key), value) {
            (Some(Value::Object(existing)), Value::Object(incoming)) => deep_merge(existing, incoming),
            _ => {
                base.insert(key.clone(), value.clone());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn record_id_parse_keeps_colons() {
        let r = RecordId::parse("thread:a:b");
        assert_eq!(r.table, "thread");
        assert_eq!(r.key, RecordKey::String("a:b".into()));
        assert_eq!(r.to_string(), "thread:a:b");
    }

    #[test]
    fn json_round_trip_drops_none() {
        let mut o = object_from_json(json!({"a": 1, "b": "x", "c": [true, null]}));
        o.insert("d".into(), Value::None);
        o.insert("e".into(), Value::RecordId(RecordId::new("user", "k1")));
        let j = Value::Object(o).to_json();
        assert_eq!(j, json!({"a": 1, "b": "x", "c": [true, null], "e": "user:k1"}));
    }

    #[test]
    fn duration_parts() {
        assert_eq!(SurrealDuration::new("1h30m").parts(), Some((5400, 0)));
        assert_eq!(SurrealDuration::new("250ms").parts(), Some((0, 250_000_000)));
        assert_eq!(SurrealDuration::new("abc").parts(), None);
        assert_eq!(SurrealDuration::new("1x").parts(), None);
    }

    #[test]
    fn typed_round_trip() {
        #[derive(Serialize, Deserialize, PartialEq, Debug)]
        struct Doc {
            id: String,
            n: i64,
        }
        let v = to_value(&Doc { id: "doc:1".into(), n: 3 }).unwrap();
        let back: Doc = from_value(v).unwrap();
        assert_eq!(back, Doc { id: "doc:1".into(), n: 3 });
    }

    #[test]
    fn iso_millis() {
        let d = DateTime::parse_from_rfc3339("2026-01-02T03:04:05.678Z").unwrap().with_timezone(&Utc);
        assert_eq!(iso8601(&d), "2026-01-02T03:04:05.678Z");
    }
}
