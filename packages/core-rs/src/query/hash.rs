//! Hash inputs for the two query keys. Both are `sha256(json(input))`: the
//! salted key names the remote `_00_query` row, the unsalted one names the
//! durable `_00_view` row.

use crate::value::{Object, Value};

/// What a query is, for hashing.
#[derive(Clone, Debug)]
pub struct QueryKeyInput<'a> {
    pub surql: &'a str,
    pub params: &'a Object,
}

fn json_string(s: &str) -> String {
    serde_json::Value::String(s.to_string()).to_string()
}

/// Session-salted: two clients of one user must not share a remote view. Key
/// order matches the JavaScript object literal (`surql`, `params`,
/// `sessionId`).
pub fn query_hash_input(input: &QueryKeyInput<'_>, session_id: Option<&str>) -> String {
    format!(
        "{{\"surql\":{},\"params\":{},\"sessionId\":{}}}",
        json_string(input.surql),
        Value::Object(input.params.clone()).to_json(),
        session_id.map(json_string).unwrap_or_else(|| "null".into())
    )
}

/// Session-independent: survives restart and bucket switch.
pub fn view_key_input(input: &QueryKeyInput<'_>) -> String {
    format!(
        "{{\"surql\":{},\"params\":{}}}",
        json_string(input.surql),
        Value::Object(input.params.clone()).to_json()
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::value::{object_from_json, RecordId};

    #[test]
    fn shapes() {
        let mut params = object_from_json(serde_json::json!({"b": 1}));
        params.insert("a".into(), Value::RecordId(RecordId::new("user", "x")));
        let k = QueryKeyInput { surql: "SELECT * FROM t", params: &params };
        assert_eq!(view_key_input(&k), r#"{"surql":"SELECT * FROM t","params":{"a":"user:x","b":1}}"#);
        assert_eq!(
            query_hash_input(&k, Some("s1")),
            r#"{"surql":"SELECT * FROM t","params":{"a":"user:x","b":1},"sessionId":"s1"}"#
        );
        assert!(query_hash_input(&k, None).ends_with(r#""sessionId":null}"#));
    }
}
