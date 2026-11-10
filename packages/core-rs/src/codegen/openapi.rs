//! OpenAPI spec -> a backend's typed routes.
//!
//! PORT: `packages/spooky_core/lib/src/codegen/openapi_parser.dart`.

use serde_yaml::Value as Yaml;

use crate::error::{Error, Result};

/// The JSON-schema type of a route argument.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ArgKind {
    String,
    Integer,
    Number,
    Boolean,
    /// Anything else (objects, arrays, no `type`).
    Any,
}

impl ArgKind {
    fn from_json_type(t: Option<&str>) -> Self {
        match t {
            Some("string") => ArgKind::String,
            Some("integer") => ArgKind::Integer,
            Some("number") => ArgKind::Number,
            Some("boolean") => ArgKind::Boolean,
            _ => ArgKind::Any,
        }
    }

    /// The JSON-schema spelling, as the runtime schema's route args carry it.
    pub fn json_type(self) -> Option<&'static str> {
        match self {
            ArgKind::String => Some("string"),
            ArgKind::Integer => Some("integer"),
            ArgKind::Number => Some("number"),
            ArgKind::Boolean => Some("boolean"),
            ArgKind::Any => None,
        }
    }
}

/// One argument of a backend route (a requestBody property).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OpenApiArg {
    pub name: String,
    pub kind: ArgKind,
    pub optional: bool,
}

/// One route (e.g. POST `/spookify`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OpenApiRoute {
    pub path: String,
    pub args: Vec<OpenApiArg>,
}

/// A backend: its name (the key in `run(name, ...)`), its outbox table and its
/// routes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OpenApiBackend {
    pub name: String,
    pub outbox_table: Option<String>,
    pub routes: Vec<OpenApiRoute>,
}

fn key_string(k: &Yaml) -> String {
    match k {
        Yaml::String(s) => s.clone(),
        other => serde_yaml::to_string(other)
            .unwrap_or_default()
            .trim()
            .to_string(),
    }
}

fn dig<'a>(root: &'a Yaml, path: &[&str]) -> Option<&'a Yaml> {
    path.iter().try_fold(root, |node, key| node.get(key))
}

fn args_from_schema(schema: Option<&Yaml>) -> Vec<OpenApiArg> {
    let Some(Yaml::Mapping(props)) = schema.and_then(|s| s.get("properties")) else {
        return Vec::new();
    };
    let required: Vec<String> = match schema.and_then(|s| s.get("required")) {
        Some(Yaml::Sequence(items)) => items.iter().map(key_string).collect(),
        _ => Vec::new(),
    };
    let mut args: Vec<OpenApiArg> = props
        .iter()
        .map(|(k, spec)| {
            let name = key_string(k);
            OpenApiArg {
                optional: !required.contains(&name),
                kind: ArgKind::from_json_type(spec.get("type").and_then(Yaml::as_str)),
                name,
            }
        })
        .collect();
    args.sort_by(|a, b| a.name.cmp(&b.name));
    args
}

/// Parse an OpenAPI document (YAML or JSON) into backend `name`'s routes.
///
/// Reads each `paths.<route>.post.requestBody.content.application/json.schema`,
/// taking `properties` (with `type`) and `required`. Only the post body is
/// considered (sp00ky backend routes are POST). Routes and args are sorted so
/// codegen output is deterministic.
pub fn parse_open_api(
    name: &str,
    source: &str,
    outbox_table: Option<&str>,
) -> Result<OpenApiBackend> {
    let doc: Yaml = serde_yaml::from_str(source)
        .map_err(|e| Error::InvalidArgument(format!("invalid OpenAPI spec for {name}: {e}")))?;
    let mut routes = Vec::new();
    if let Some(Yaml::Mapping(paths)) = doc.get("paths") {
        for (path, op) in paths {
            let Some(post) = op.get("post").filter(|p| p.is_mapping()) else {
                continue;
            };
            let schema = dig(
                post,
                &["requestBody", "content", "application/json", "schema"],
            );
            routes.push(OpenApiRoute {
                path: key_string(path),
                args: args_from_schema(schema),
            });
        }
    }
    routes.sort_by(|a, b| a.path.cmp(&b.path));
    Ok(OpenApiBackend {
        name: name.to_string(),
        outbox_table: outbox_table.map(str::to_string),
        routes,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const SPEC: &str = "
openapi: 3.1.0
paths:
  /spookify:
    post:
      requestBody:
        content:
          application/json:
            schema:
              type: object
              properties:
                id:
                  type: string
                count:
                  type: integer
              required:
                - id
  /noargs:
    post:
      responses:
        '200': { description: ok }
  /getonly:
    get:
      responses: {}
";

    #[test]
    fn parses_routes_and_typed_args_with_required_and_optional() {
        let backend = parse_open_api("api", SPEC, Some("job")).unwrap();
        assert_eq!(backend.name, "api");
        assert_eq!(backend.outbox_table.as_deref(), Some("job"));
        let paths: Vec<&str> = backend.routes.iter().map(|r| r.path.as_str()).collect();
        assert_eq!(paths, ["/noargs", "/spookify"]);

        let spookify = &backend.routes[1];
        let id = spookify.args.iter().find(|a| a.name == "id").unwrap();
        let count = spookify.args.iter().find(|a| a.name == "count").unwrap();
        assert_eq!(id.kind, ArgKind::String);
        assert!(!id.optional);
        assert_eq!(count.kind, ArgKind::Integer);
        assert!(count.optional);
        assert_eq!(spookify.args[0].name, "count", "args are sorted");
        assert!(backend.routes[0].args.is_empty());
    }

    #[test]
    fn json_specs_and_garbage() {
        let json = r#"{"paths": {"/x": {"post": {"requestBody": {"content": {"application/json": {"schema": {"properties": {"flag": {"type": "boolean"}, "data": {"type": "object"}}}}}}}}}}"#;
        let backend = parse_open_api("j", json, None).unwrap();
        let args = &backend.routes[0].args;
        assert_eq!(args[0].kind, ArgKind::Any);
        assert_eq!(args[1].kind, ArgKind::Boolean);
        assert!(parse_open_api("bad", "paths: [unclosed", None).is_err());
        assert!(parse_open_api("empty", "openapi: 3.1.0", None)
            .unwrap()
            .routes
            .is_empty());
    }
}
