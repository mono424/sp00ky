//! `{{ path | filter(arg) }}` templates over a JSON context.
//!
//! Deliberately small: a path into the context, then a chain of filters with
//! literal arguments. No conditionals, no loops, no arithmetic: anything
//! smarter belongs in a `with:` query, which has all of SurrealQL.
//!
//! ```text
//! {{ sender.username }}
//! {{ text | truncate(120) }}
//! {{ members.0 | key }}
//! {{ title | default("New post") | upper }}
//! ```
//!
//! - A missing path renders as the empty string (as `null` in a JSON value
//!   position, see [`render_value`]).
//! - A name segment applied to a one-element array reads that element, so the
//!   result of `SELECT name FROM user WHERE ...` (always an array) can be
//!   addressed as `{{ owner.name }}` without `.0`.
//! - Record ids arrive as `table:key` strings or `{tb, id}` / `{table, key}`
//!   objects; both render as `table:key`.
//!
//! Filters: `default(x)` (for null, missing or ""), `truncate(n)` (characters,
//! `...` appended only when cut, counted in `n`), `upper`, `lower`, `trim`,
//! `key` (`user:abc` -> `abc`), `table` (`user:abc` -> `user`), `json`, `len`,
//! `join(sep)` (default `", "`), `first`, `last`, `date(format)` (UTC,
//! strftime, default `%Y-%m-%d %H:%M`).

use std::collections::HashMap;
use std::fmt::Write as _;
use std::sync::{Arc, Mutex, OnceLock};

use serde_json::Value;

use crate::util::{epoch_millis, record_id_object, split_record_id, unquote_key};

/// Parse-check a template; `Err` carries a message for `spky lint`.
pub fn check(template: &str) -> Result<(), String> {
    parse_cached(template).map(|_| ())
}

/// Render to text. A template that does not parse renders verbatim, so a
/// broken manifest shows up on the device instead of vanishing (lint rejects
/// it long before that).
pub fn render(template: &str, ctx: &Value) -> String {
    match parse_cached(template) {
        Ok(parts) => {
            let mut out = String::new();
            for part in parts.iter() {
                match part {
                    Part::Text(t) => out.push_str(t),
                    Part::Expr(e) => out.push_str(&to_text(&eval(e, ctx))),
                }
            }
            out
        }
        Err(_) => template.to_string(),
    }
}

/// Render every string inside a JSON value. A string that is exactly one
/// `{{expr}}` keeps the JSON type of its result (`"{{count}}"` -> `3`,
/// `"{{tags}}"` -> `["a","b"]`, a missing path -> `null`); anything else
/// renders to text.
pub fn render_value(template: &Value, ctx: &Value) -> Value {
    match template {
        Value::String(s) => match parse_cached(s) {
            Ok(parts) => match parts.as_slice() {
                [Part::Expr(e)] => crate::util::collapse_record_ids(eval(e, ctx)),
                _ => Value::String(render(s, ctx)),
            },
            Err(_) => Value::String(s.clone()),
        },
        Value::Array(items) => Value::Array(items.iter().map(|i| render_value(i, ctx)).collect()),
        Value::Object(map) => Value::Object(
            map.iter()
                .map(|(k, v)| (k.clone(), render_value(v, ctx)))
                .collect(),
        ),
        other => other.clone(),
    }
}

/// Does any expression in `template` start at `root` (`recipient`,
/// `recipient.x`)? Decides whether a rule renders once per row or once per
/// recipient.
pub fn references(template: &str, root: &str) -> bool {
    match parse_cached(template) {
        Ok(parts) => parts.iter().any(
            |p| matches!(p, Part::Expr(e) if e.path.first().map(String::as_str) == Some(root)),
        ),
        Err(_) => false,
    }
}

/// [`references`] over every string inside a JSON value.
pub fn value_references(template: &Value, root: &str) -> bool {
    match template {
        Value::String(s) => references(s, root),
        Value::Array(items) => items.iter().any(|i| value_references(i, root)),
        Value::Object(map) => map.values().any(|v| value_references(v, root)),
        _ => false,
    }
}

/// Look a dot path up in a JSON value, leniently (see the module docs on
/// one-element arrays). `None` when any segment is missing.
pub fn lookup<'a>(root: &'a Value, path: &str) -> Option<&'a Value> {
    let segments: Vec<&str> = path.split('.').collect();
    lookup_segments(root, &segments)
}

fn lookup_segments<'a, S: AsRef<str>>(root: &'a Value, path: &[S]) -> Option<&'a Value> {
    let mut cur = root;
    for seg in path {
        let seg = seg.as_ref();
        cur = match cur {
            Value::Object(map) => map.get(seg)?,
            Value::Array(items) => match seg.parse::<usize>() {
                Ok(i) => items.get(i)?,
                Err(_) if items.len() == 1 => match &items[0] {
                    Value::Object(map) => map.get(seg)?,
                    _ => return None,
                },
                Err(_) => return None,
            },
            _ => return None,
        };
    }
    Some(cur)
}

/// Text form of a value, as it appears inside a rendered string.
pub fn to_text(v: &Value) -> String {
    match v {
        Value::Null => String::new(),
        Value::String(s) => s.clone(),
        Value::Bool(b) => b.to_string(),
        Value::Number(n) => n.to_string(),
        Value::Array(items) => items.iter().map(to_text).collect::<Vec<_>>().join(", "),
        Value::Object(_) => record_id_object(v).unwrap_or_else(|| v.to_string()),
    }
}

// ── AST ──────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq)]
enum Part {
    Text(String),
    Expr(Expr),
}

#[derive(Debug, Clone, PartialEq)]
struct Expr {
    path: Vec<String>,
    filters: Vec<Filter>,
}

#[derive(Debug, Clone, PartialEq)]
struct Filter {
    name: FilterName,
    args: Vec<Value>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum FilterName {
    Default,
    Truncate,
    Upper,
    Lower,
    Trim,
    Key,
    Table,
    Json,
    Len,
    Join,
    First,
    Last,
    Date,
}

impl FilterName {
    fn parse(name: &str) -> Option<FilterName> {
        Some(match name {
            "default" => FilterName::Default,
            "truncate" => FilterName::Truncate,
            "upper" => FilterName::Upper,
            "lower" => FilterName::Lower,
            "trim" => FilterName::Trim,
            "key" => FilterName::Key,
            "table" => FilterName::Table,
            "json" => FilterName::Json,
            "len" => FilterName::Len,
            "join" => FilterName::Join,
            "first" => FilterName::First,
            "last" => FilterName::Last,
            "date" => FilterName::Date,
            _ => return None,
        })
    }

    /// (min, max) number of arguments.
    fn arity(self) -> (usize, usize) {
        match self {
            FilterName::Default | FilterName::Truncate => (1, 1),
            FilterName::Join | FilterName::Date => (0, 1),
            _ => (0, 0),
        }
    }
}

const FILTER_NAMES: &str =
    "default, truncate, upper, lower, trim, key, table, json, len, join, first, last, date";

// ── Parser ───────────────────────────────────────────────────────────────

type Parsed = Arc<Vec<Part>>;

const CACHE_LIMIT: usize = 4096;

/// Templates come from the manifest, so the set is small and fixed per config;
/// parsing each once keeps per-row rendering to a walk over the AST. The cap
/// only guards against something rendering unbounded ad-hoc strings.
fn parse_cached(template: &str) -> Result<Parsed, String> {
    if !template.contains("{{") {
        return Ok(Arc::new(vec![Part::Text(template.to_string())]));
    }
    type Cache = Mutex<HashMap<String, Result<Parsed, String>>>;
    static CACHE: OnceLock<Cache> = OnceLock::new();
    let cache = CACHE.get_or_init(|| Mutex::new(HashMap::new()));
    if let Ok(map) = cache.lock() {
        if let Some(hit) = map.get(template) {
            return hit.clone();
        }
    }
    let parsed = parse(template).map(Arc::new);
    if let Ok(mut map) = cache.lock() {
        if map.len() >= CACHE_LIMIT {
            map.clear();
        }
        map.insert(template.to_string(), parsed.clone());
    }
    parsed
}

fn parse(template: &str) -> Result<Vec<Part>, String> {
    let mut parts = Vec::new();
    let mut rest = template;
    while let Some(start) = rest.find("{{") {
        if start > 0 {
            parts.push(Part::Text(rest[..start].to_string()));
        }
        let after = &rest[start + 2..];
        let (expr, consumed) = parse_expr(after).map_err(|e| format!("{e} in `{template}`"))?;
        parts.push(Part::Expr(expr));
        rest = &after[consumed..];
    }
    if !rest.is_empty() {
        parts.push(Part::Text(rest.to_string()));
    }
    Ok(parts)
}

struct Cursor<'a> {
    src: &'a str,
    pos: usize,
}

impl<'a> Cursor<'a> {
    fn peek(&self) -> Option<char> {
        self.src[self.pos..].chars().next()
    }
    fn bump(&mut self) -> Option<char> {
        let c = self.peek()?;
        self.pos += c.len_utf8();
        Some(c)
    }
    fn skip_ws(&mut self) {
        while matches!(self.peek(), Some(c) if c.is_whitespace()) {
            self.bump();
        }
    }
    fn starts_with(&self, s: &str) -> bool {
        self.src[self.pos..].starts_with(s)
    }
    fn take_while(&mut self, f: impl Fn(char) -> bool) -> &'a str {
        let start = self.pos;
        while matches!(self.peek(), Some(c) if f(c)) {
            self.bump();
        }
        &self.src[start..self.pos]
    }
}

/// Parse one expression up to and including its closing `}}`. Returns the
/// expression and how many bytes of `src` it used.
fn parse_expr(src: &str) -> Result<(Expr, usize), String> {
    let mut c = Cursor { src, pos: 0 };
    c.skip_ws();
    let path_text = c.take_while(|ch| {
        !(ch.is_whitespace() || matches!(ch, '|' | '}' | '(' | ')' | ',' | '"' | '\'' | '{'))
    });
    if path_text.is_empty() {
        return Err(if c.starts_with("}}") {
            "empty `{{ }}`".into()
        } else {
            "expected a field path after `{{`".into()
        });
    }
    let path: Vec<String> = path_text.split('.').map(str::to_string).collect();
    if path.iter().any(String::is_empty) {
        return Err(format!("`{path_text}` is not a field path"));
    }
    let mut filters = Vec::new();
    loop {
        c.skip_ws();
        if c.starts_with("}}") {
            c.pos += 2;
            return Ok((Expr { path, filters }, c.pos));
        }
        match c.peek() {
            None => return Err("unclosed `{{`".into()),
            Some('|') => {
                c.bump();
                c.skip_ws();
                let name = c.take_while(|ch| ch.is_ascii_alphanumeric() || ch == '_');
                if name.is_empty() {
                    return Err("expected a filter name after `|`".into());
                }
                let kind = FilterName::parse(name)
                    .ok_or_else(|| format!("unknown filter `{name}` (known: {FILTER_NAMES})"))?;
                c.skip_ws();
                let mut args = Vec::new();
                if c.peek() == Some('(') {
                    c.bump();
                    loop {
                        c.skip_ws();
                        if c.peek() == Some(')') {
                            c.bump();
                            break;
                        }
                        args.push(parse_literal(&mut c)?);
                        c.skip_ws();
                        match c.bump() {
                            Some(',') => continue,
                            Some(')') => break,
                            _ => {
                                return Err(format!(
                                    "expected `,` or `)` in the arguments of `{name}`"
                                ))
                            }
                        }
                    }
                }
                let (min, max) = kind.arity();
                if args.len() < min || args.len() > max {
                    return Err(if min == max {
                        format!("`{name}` takes {min} argument(s), got {}", args.len())
                    } else {
                        format!(
                            "`{name}` takes at most {max} argument(s), got {}",
                            args.len()
                        )
                    });
                }
                validate_args(kind, name, &args)?;
                filters.push(Filter { name: kind, args });
            }
            Some(other) => return Err(format!("unexpected `{other}` (filters go after `|`)")),
        }
    }
}

fn parse_literal(c: &mut Cursor<'_>) -> Result<Value, String> {
    match c.peek() {
        Some(q @ ('"' | '\'')) => {
            c.bump();
            let mut s = String::new();
            loop {
                match c.bump() {
                    None => return Err("unterminated string literal".into()),
                    Some('\\') => match c.bump() {
                        Some('n') => s.push('\n'),
                        Some('t') => s.push('\t'),
                        Some(other) => s.push(other),
                        None => return Err("unterminated string literal".into()),
                    },
                    Some(ch) if ch == q => break,
                    Some(ch) => s.push(ch),
                }
            }
            Ok(Value::String(s))
        }
        Some(ch) if ch == '-' || ch.is_ascii_digit() => {
            let text = c.take_while(|ch| ch == '-' || ch == '.' || ch.is_ascii_digit());
            if let Ok(i) = text.parse::<i64>() {
                return Ok(Value::from(i));
            }
            text.parse::<f64>()
                .ok()
                .and_then(serde_json::Number::from_f64)
                .map(Value::Number)
                .ok_or_else(|| format!("`{text}` is not a number"))
        }
        _ => {
            let word = c.take_while(|ch| ch.is_ascii_alphabetic());
            match word {
                "true" => Ok(Value::Bool(true)),
                "false" => Ok(Value::Bool(false)),
                "null" => Ok(Value::Null),
                "" => Err(
                    "expected an argument (a quoted string, a number, true, false or null)".into(),
                ),
                other => Err(format!("`{other}` is not a literal; quote strings")),
            }
        }
    }
}

fn validate_args(kind: FilterName, name: &str, args: &[Value]) -> Result<(), String> {
    match kind {
        FilterName::Truncate => match args[0].as_u64() {
            Some(n) if n >= 1 => Ok(()),
            _ => Err(format!("`{name}` takes a positive whole number")),
        },
        FilterName::Join | FilterName::Date => match args.first() {
            None => Ok(()),
            Some(Value::String(fmt)) => {
                if kind == FilterName::Date {
                    let bad = chrono::format::StrftimeItems::new(fmt)
                        .any(|item| matches!(item, chrono::format::Item::Error));
                    if bad {
                        return Err(format!("`{fmt}` is not a valid date format"));
                    }
                }
                Ok(())
            }
            Some(_) => Err(format!("`{name}` takes a string")),
        },
        _ => Ok(()),
    }
}

// ── Evaluation ───────────────────────────────────────────────────────────

fn eval(e: &Expr, ctx: &Value) -> Value {
    let mut v = lookup_segments(ctx, &e.path)
        .cloned()
        .unwrap_or(Value::Null);
    for f in &e.filters {
        v = apply(f, v);
    }
    v
}

fn is_blank(v: &Value) -> bool {
    match v {
        Value::Null => true,
        Value::String(s) => s.is_empty(),
        _ => false,
    }
}

/// String filters keep a missing value missing, so `| default(..)` still
/// works after them.
fn map_text(v: Value, f: impl Fn(&str) -> String) -> Value {
    match v {
        Value::Null => Value::Null,
        other => Value::String(f(&to_text(&other))),
    }
}

/// `key` / `table` of one value; arrays map element-wise so
/// `{{ members | key | join }}` works.
fn record_part(v: Value, want_key: bool) -> Value {
    match v {
        Value::Null => Value::Null,
        Value::Array(items) => Value::Array(
            items
                .into_iter()
                .map(|i| record_part(i, want_key))
                .collect(),
        ),
        other => {
            let text = record_id_object(&other).unwrap_or_else(|| to_text(&other));
            match split_record_id(&text) {
                Some((t, k)) => Value::String(if want_key {
                    k.to_string()
                } else {
                    t.to_string()
                }),
                None if want_key => Value::String(unquote_key(&text).to_string()),
                None => Value::String(String::new()),
            }
        }
    }
}

/// At most `n` characters, `...` included when the text had to be cut.
pub fn truncate(s: &str, n: usize) -> String {
    if s.chars().count() <= n {
        return s.to_string();
    }
    if n <= 3 {
        return s.chars().take(n).collect();
    }
    let cut: String = s.chars().take(n - 3).collect();
    format!("{}...", cut.trim_end())
}

fn apply(f: &Filter, v: Value) -> Value {
    match f.name {
        FilterName::Default => {
            if is_blank(&v) {
                f.args[0].clone()
            } else {
                v
            }
        }
        FilterName::Truncate => {
            let n = f.args[0].as_u64().unwrap_or(u64::MAX) as usize;
            map_text(v, |s| truncate(s, n))
        }
        FilterName::Upper => map_text(v, str::to_uppercase),
        FilterName::Lower => map_text(v, str::to_lowercase),
        FilterName::Trim => map_text(v, |s| s.trim().to_string()),
        FilterName::Key => record_part(v, true),
        FilterName::Table => record_part(v, false),
        FilterName::Json => Value::String(v.to_string()),
        FilterName::Len => Value::from(match &v {
            Value::Null => 0,
            Value::Array(items) => items.len(),
            Value::Object(map) => map.len(),
            Value::String(s) => s.chars().count(),
            other => to_text(other).chars().count(),
        }),
        FilterName::Join => {
            let sep = f.args.first().and_then(Value::as_str).unwrap_or(", ");
            match v {
                Value::Null => Value::Null,
                Value::Array(items) => {
                    Value::String(items.iter().map(to_text).collect::<Vec<_>>().join(sep))
                }
                other => Value::String(to_text(&other)),
            }
        }
        FilterName::First | FilterName::Last => {
            let first = f.name == FilterName::First;
            match v {
                Value::Array(items) => {
                    let pick = if first { items.first() } else { items.last() };
                    pick.cloned().unwrap_or(Value::Null)
                }
                Value::String(s) => {
                    let pick = if first {
                        s.chars().next()
                    } else {
                        s.chars().last()
                    };
                    pick.map(|c| Value::String(c.to_string()))
                        .unwrap_or(Value::Null)
                }
                other => other,
            }
        }
        FilterName::Date => {
            let fmt = f
                .args
                .first()
                .and_then(Value::as_str)
                .unwrap_or("%Y-%m-%d %H:%M");
            let Some(ms) = epoch_millis(&v) else {
                return v;
            };
            let Some(dt) = chrono::DateTime::<chrono::Utc>::from_timestamp_millis(ms) else {
                return v;
            };
            // `write!` instead of `to_string()`: a bad format is an `Err`
            // here, and a panic there.
            let mut out = String::new();
            match write!(out, "{}", dt.format(fmt)) {
                Ok(()) => Value::String(out),
                Err(_) => v,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn ctx() -> Value {
        json!({
            "text": "Hello there, this is a fairly long message body",
            "sender": { "username": "ada", "id": "user:ada" },
            "conversation": "conversation:⟨x-1⟩",
            "members": ["user:a", "user:b", {"tb": "user", "id": "c"}],
            "count": 3,
            "ratio": 1.5,
            "flag": false,
            "empty": "",
            "nothing": null,
            "rows": [{ "name": "only" }],
            "created_at": "2024-03-05T14:07:09Z",
            "ms": 1_709_647_629_000u64,
            "padded": "  hi  ",
            "rid": { "tb": "post", "id": "p9" },
        })
    }

    #[test]
    fn plain_text_passes_through() {
        assert_eq!(render("no templates here", &ctx()), "no templates here");
        assert_eq!(render("", &ctx()), "");
        assert_eq!(render("a } b }} c", &ctx()), "a } b }} c");
    }

    #[test]
    fn paths() {
        let c = ctx();
        assert_eq!(render("{{sender.username}}", &c), "ada");
        assert_eq!(render("{{ sender.username }} says hi", &c), "ada says hi");
        assert_eq!(render("{{members.1}}", &c), "user:b");
        assert_eq!(render("{{members.2}}", &c), "user:c");
        assert_eq!(render("{{rows.name}}", &c), "only");
        assert_eq!(render("{{rows.0.name}}", &c), "only");
        assert_eq!(render("[{{missing.deep.path}}]", &c), "[]");
        assert_eq!(
            render("{{count}} / {{ratio}} / {{flag}}", &c),
            "3 / 1.5 / false"
        );
        assert_eq!(render("{{members}}", &c), "user:a, user:b, user:c");
        assert_eq!(render("{{rid}}", &c), "post:p9");
        assert_eq!(render("{{nothing}}", &c), "");
        assert_eq!(render("{{members.9}}", &c), "");
        assert_eq!(lookup(&c, "sender.username"), Some(&json!("ada")));
        assert_eq!(lookup(&c, "sender.nope"), None);
    }

    #[test]
    fn filters() {
        let c = ctx();
        assert_eq!(render("{{missing | default('anon')}}", &c), "anon");
        assert_eq!(render("{{empty | default(\"x\")}}", &c), "x");
        assert_eq!(render("{{nothing | default(5)}}", &c), "5");
        assert_eq!(render("{{sender.username | default('x')}}", &c), "ada");
        assert_eq!(render("{{text | truncate(12)}}", &c), "Hello the...");
        assert_eq!(
            render("{{text | truncate(500)}}", &c),
            "Hello there, this is a fairly long message body"
        );
        assert_eq!(render("{{sender.username | truncate(2)}}", &c), "ad");
        assert_eq!(render("{{ sender.username | upper }}", &c), "ADA");
        assert_eq!(
            render("{{ SENDER | lower }}", &json!({"SENDER": "AdA"})),
            "ada"
        );
        assert_eq!(render("{{padded | trim}}!", &c), "hi!");
        assert_eq!(render("{{conversation | key}}", &c), "x-1");
        assert_eq!(render("{{conversation | table}}", &c), "conversation");
        assert_eq!(render("{{rid | key}}", &c), "p9");
        assert_eq!(render("{{rid | table}}", &c), "post");
        assert_eq!(render("{{members | key | join('+')}}", &c), "a+b+c");
        assert_eq!(render("{{sender.username | key}}", &c), "ada");
        assert_eq!(render("{{sender.username | table}}", &c), "");
        let json_out: Value = serde_json::from_str(&render("{{sender | json}}", &c)).unwrap();
        assert_eq!(json_out, c["sender"]);
        assert_eq!(
            render("{{members | len}} {{text | len}} {{nothing | len}}", &c),
            "3 47 0"
        );
        assert_eq!(render("{{members | join}}", &c), "user:a, user:b, user:c");
        assert_eq!(
            render("{{members | first}}..{{members | last}}", &c),
            "user:a..user:c"
        );
        assert_eq!(render("{{sender.username | last | upper}}", &c), "A");
        assert_eq!(render("{{created_at | date}}", &c), "2024-03-05 14:07");
        assert_eq!(render("{{ms | date('%H:%M:%S')}}", &c), "14:07:09");
        assert_eq!(render("{{text | date}}", &c), c["text"].as_str().unwrap());
        assert_eq!(render("{{missing | upper | default('-')}}", &c), "-");
    }

    #[test]
    fn bad_templates_render_verbatim() {
        assert_eq!(render("{{ 'x' }}", &ctx()), "{{ 'x' }}");
        assert_eq!(render("hi {{ a", &ctx()), "hi {{ a");
    }

    #[test]
    fn check_reports_mistakes() {
        assert!(check("{{a}} and {{ b.c | truncate(3) | default('x') }}").is_ok());
        assert!(check("plain").is_ok());
        assert!(check("{{a").unwrap_err().contains("unclosed"));
        assert!(check("{{}}").unwrap_err().contains("empty"));
        assert!(check("{{ a | nope }}")
            .unwrap_err()
            .contains("unknown filter `nope`"));
        assert!(check("{{ a | truncate }}").is_err());
        assert!(check("{{ a | truncate(0) }}").is_err());
        assert!(check("{{ a | truncate('x') }}").is_err());
        assert!(check("{{ a | upper(1) }}").is_err());
        assert!(check("{{ a | default(x) }}")
            .unwrap_err()
            .contains("quote strings"));
        assert!(check("{{ a | default('x' }}").is_err());
        assert!(check("{{ a b }}").is_err());
        assert!(check("{{ a..b }}").is_err());
        assert!(check("{{ a | join(1) }}").is_err());
        assert!(check("{{ a | date('%Q') }}").is_err());
        assert!(check("{{ a | default('}}') }}").is_ok());
        assert!(check("{{ 'x' }}").is_err());
        assert!(check("{{ a | join(', ') | truncate(10) }}").is_ok());
    }

    #[test]
    fn string_literals_may_contain_braces_and_escapes() {
        assert_eq!(
            render("{{ nope | default('}} \\' ok') }}", &ctx()),
            "}} ' ok"
        );
    }

    #[test]
    fn render_value_keeps_types() {
        let c = ctx();
        let t = json!({
            "count": "{{count}}",
            "members": "{{ members }}",
            "text": "n={{count}}",
            "missing": "{{nope}}",
            "nested": [{"k": "{{sender.username}}"}, 5, true],
            "rid": "{{rid}}",
        });
        assert_eq!(
            render_value(&t, &c),
            json!({
                "count": 3,
                "members": ["user:a", "user:b", "user:c"],
                "text": "n=3",
                "missing": null,
                "nested": [{"k": "ada"}, 5, true],
                "rid": "post:p9",
            })
        );
    }

    #[test]
    fn references_finds_roots() {
        assert!(references("Hi {{ recipient | key }}", "recipient"));
        assert!(references("{{recipient.name}}", "recipient"));
        assert!(!references("{{recipients}}", "recipient"));
        assert!(!references("recipient", "recipient"));
        assert!(!references("{{ x | default('recipient') }}", "recipient"));
        assert!(value_references(
            &json!({"a": ["{{recipient}}"]}),
            "recipient"
        ));
        assert!(!value_references(&json!({"a": [1, "x"]}), "recipient"));
    }

    #[test]
    fn unicode_is_cut_on_characters() {
        let c = json!({"t": "héllo wörld ünïcode"});
        assert_eq!(render("{{t | truncate(8)}}", &c), "héllo...");
        assert_eq!(render("{{t | first}}", &json!({"t": "ñx"})), "ñ");
    }
}
