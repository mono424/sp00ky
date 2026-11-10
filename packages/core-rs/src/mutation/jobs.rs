//! Backend jobs (`client.run(backend, path, payload)`).
//!
//! PORT: `packages/spooky_core/lib/src/mutation/jobs.dart`.

use crate::error::{Error, Result};
use crate::types::{RunOptions, Schema};
use crate::value::{Object, Value};

/// Build the outbox job record for a backend route and resolve its table.
/// Every job is a single execution; recurring work is declared server-side.
pub fn build_job_record(
    schema: &Schema,
    backend: &str,
    path: &str,
    payload: &Object,
    options: &RunOptions,
) -> Result<(String, Object)> {
    let def = schema
        .backends
        .get(backend)
        .ok_or_else(|| Error::InvalidArgument(format!("Backend {backend} not found")))?;
    let route = def
        .routes
        .get(path)
        .ok_or_else(|| Error::InvalidArgument(format!("Route {backend}.{path} not found")))?;
    if def.outbox_table.is_empty() {
        return Err(Error::InvalidArgument(format!(
            "Outbox table for backend {backend} not found"
        )));
    }

    let mut args = Object::new();
    for (name, arg) in &route.args {
        if !payload.contains_key(name) && !arg.optional {
            return Err(Error::InvalidArgument(format!(
                "Missing required argument {name}"
            )));
        }
        // An optional argument the caller left out is sent as null, so the
        // route sees every declared key.
        let value = payload
            .get(name)
            .filter(|v| !v.is_none())
            .cloned()
            .unwrap_or(Value::Null);
        args.insert(name.clone(), value);
    }

    let mut record = Object::new();
    record.insert("path".into(), path.into());
    record.insert(
        "payload".into(),
        Value::Object(args).to_json_string().into(),
    );
    // Explicit: the schema's server-side DEFAULT does not apply to an
    // optimistic local create, and in-flight indicators key on `pending`.
    record.insert("status".into(), "pending".into());
    record.insert(
        "max_retries".into(),
        Value::Int(i64::from(options.max_retries.unwrap_or(3))),
    );
    record.insert(
        "retry_strategy".into(),
        options
            .retry_strategy
            .clone()
            .unwrap_or_else(|| "linear".into())
            .into(),
    );
    if let Some(timeout) = options.timeout {
        record.insert("timeout".into(), Value::Int(timeout as i64));
    }
    if let Some(delay) = options.delay {
        record.insert("delay".into(), Value::Int(delay as i64));
    }
    if let Some(assigned_to) = &options.assigned_to {
        record.insert("assigned_to".into(), assigned_to.clone().into());
    }
    Ok((def.outbox_table.clone(), record))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::row;
    use serde_json::json;

    fn schema() -> Schema {
        serde_json::from_value(json!({
            "backends": {
                "api": {
                    "outboxTable": "_job_api",
                    "routes": {
                        "/spookify": {
                            "args": {
                                "id": {"optional": false},
                                "note": {"optional": true},
                            }
                        }
                    }
                }
            }
        }))
        .unwrap()
    }

    #[test]
    fn builds_the_job_row_and_resolves_the_outbox_table() {
        let (table, record) = build_job_record(
            &schema(),
            "api",
            "/spookify",
            &row(json!({"id": "thing:1"})),
            &RunOptions::default(),
        )
        .unwrap();
        assert_eq!(table, "_job_api");
        assert_eq!(record["path"], Value::from("/spookify"));
        assert_eq!(record["status"], Value::from("pending"));
        assert_eq!(record["max_retries"], Value::Int(3));
        assert_eq!(record["retry_strategy"], Value::from("linear"));
        let payload: serde_json::Value =
            serde_json::from_str(record["payload"].as_str().unwrap()).unwrap();
        assert_eq!(payload, json!({"id": "thing:1", "note": null}));
    }

    #[test]
    fn carries_the_options_that_are_set_and_only_those() {
        let options = RunOptions {
            max_retries: Some(7),
            retry_strategy: Some("exponential".into()),
            delay: Some(500),
            ..Default::default()
        };
        let (_, record) = build_job_record(
            &schema(),
            "api",
            "/spookify",
            &row(json!({"id": "thing:1"})),
            &options,
        )
        .unwrap();
        assert_eq!(record["max_retries"], Value::Int(7));
        assert_eq!(record["retry_strategy"], Value::from("exponential"));
        assert_eq!(record["delay"], Value::Int(500));
        assert!(!record.contains_key("timeout"));
        assert!(!record.contains_key("assigned_to"));
    }

    #[test]
    fn rejects_an_unknown_backend_route_or_missing_required_argument() {
        let none = Object::new();
        let opts = RunOptions::default();
        assert!(matches!(
            build_job_record(&schema(), "nope", "/spookify", &none, &opts),
            Err(Error::InvalidArgument(_))
        ));
        assert!(matches!(
            build_job_record(&schema(), "api", "/nope", &none, &opts),
            Err(Error::InvalidArgument(_))
        ));
        assert!(matches!(
            build_job_record(&schema(), "api", "/spookify", &none, &opts),
            Err(Error::InvalidArgument(_))
        ));
        let no_table: Schema = serde_json::from_value(json!({
            "backends": {"api": {"routes": {"/x": {}}}}
        }))
        .unwrap();
        assert!(matches!(
            build_job_record(&no_table, "api", "/x", &none, &opts),
            Err(Error::InvalidArgument(_))
        ));
    }
}
