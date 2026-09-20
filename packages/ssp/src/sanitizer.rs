use nom::{
    branch::alt,
    bytes::complete::{is_not, tag, take_while1},
    character::complete::{char, digit1, multispace1, none_of},
    combinator::{map, opt, recognize, value, verify},
    multi::many0,
    sequence::{delimited, pair, preceded, tuple},
    IResult,
};
use serde_json::Value;

// =============================================================================
// TEIL 1: DIE INTERNE NOM LOGIK
// =============================================================================

#[derive(Debug, Clone, PartialEq)]
enum Token {
    Keyword(String),
    Identifier(String),
    StringLit(String),
    BacktickLit(String),
    Number(String),
    Symbol(char),
    Whitespace,
}

fn parse_ws(input: &str) -> IResult<&str, Token> {
    value(Token::Whitespace, multispace1)(input)
}

fn parse_comment(input: &str) -> IResult<&str, ()> {
    alt((
        value((), pair(tag("--"), is_not("\n\r"))),
        value((), pair(tag("//"), is_not("\n\r"))),
        value((), tuple((tag("/*"), take_while1(|c| c != '*'), tag("*/")))),
    ))(input)
}

fn parse_string_lit(input: &str) -> IResult<&str, Token> {
    let parse_single = delimited(
        char('\''),
        recognize(many0(alt((tag("\\'"), is_not("'\\"))))),
        char('\''),
    );
    let parse_double = delimited(
        char('"'),
        recognize(many0(alt((tag("\\\""), is_not("\"\\"))))),
        char('"'),
    );
    map(alt((parse_single, parse_double)), |s: &str| {
        Token::StringLit(format!("'{}'", s))
    })(input)
}

fn parse_word(input: &str) -> IResult<&str, Token> {
    // FIX: Semikolon darf NIEMALS Teil eines Wortes sein!
    let allowed_chars =
        |c: char| (c.is_alphanumeric() || c == '_' || c == ':' || c == '⟨' || c == '⟩') && c != ';';

    map(take_while1(allowed_chars), |s: &str| {
        if is_keyword(s) {
            Token::Keyword(s.to_string())
        } else {
            Token::Identifier(s.to_string())
        }
    })(input)
}

fn is_keyword(s: &str) -> bool {
    let keywords = [
        "SELECT", "CREATE", "UPDATE", "DELETE", "RELATE", "FROM", "WHERE", "CONTENT", "SET",
        "RETURN", "TIMEOUT", "PARALLEL", "LIMIT", "START", "GROUP", "ORDER", "BY", "ASC", "DESC",
        "INSIDE", "CONTAINS", "NONE", "NULL", "TRUE", "FALSE", "AND", "OR", "NOT", "INFO", "DB",
        "NS",
    ];
    keywords.iter().any(|k| k.eq_ignore_ascii_case(s))
}

fn parse_number(input: &str) -> IResult<&str, Token> {
    map(
        recognize(tuple((
            opt(char('-')),
            digit1,
            opt(tuple((char('.'), digit1))),
        ))),
        |s: &str| Token::Number(s.to_string()),
    )(input)
}

fn parse_symbol(input: &str) -> IResult<&str, Token> {
    let safe_symbols = "=,()[]{}<>!+-*/";
    map(
        verify(none_of(" \t\r\n;"), |c| safe_symbols.contains(*c)),
        Token::Symbol,
    )(input)
}


fn parse_backtick_lit(input: &str) -> IResult<&str, Token> {
    delimited(
        char('`'),
        recognize(many0(is_not("`"))),
        char('`'),
    )(input)
    .map(|(rem, s)| (rem, Token::BacktickLit(s.to_string())))
}

fn parse_safe_query(input: &str) -> IResult<&str, Vec<Token>> {
    // Die Logik hier ist: Wir parsen Tokens, solange wir KEIN Semikolon sehen.
    let (remainder, tokens) = many0(preceded(
        many0(alt((parse_comment, value((), multispace1)))),
        alt((
            parse_string_lit,
            parse_backtick_lit, // Add support for `...`
            parse_number,
            parse_word,
            parse_symbol,
            parse_ws,
        )),
    ))(input)?;

    Ok((remainder, tokens))
}

fn rebuild_query(tokens: Vec<Token>) -> String {
    let mut out = String::new();
    let mut needs_space = false;

    let mut iter = tokens.iter().peekable();

    while let Some(token) = iter.next() {
        match token {
            Token::Keyword(s) => {
                let mut text = s.as_str();
                let mut captured_colon = false;
                if text.ends_with(':') {
                    text = &text[0..text.len() - 1];
                    captured_colon = true;
                }

                let next_is_colon = matches!(iter.peek(), Some(Token::Symbol(':')));
                let is_key = captured_colon || next_is_colon;

                if needs_space {
                    out.push(' ');
                }

                if is_key {
                    out.push('"');
                    out.push_str(text);
                    out.push('"');
                    if captured_colon {
                        out.push(':');
                        needs_space = true;
                    }
                } else {
                    out.push_str(text);
                }

                if !captured_colon {
                    needs_space = true;
                }
            }

            Token::Identifier(s) => {
                let mut text = s.as_str();
                let mut captured_colon = false;

                if text.ends_with(':') {
                    text = &text[0..text.len() - 1];
                    captured_colon = true;
                }
                
                // MERGE LOGIC for Complex IDs: thread:`foo`
                // If Ident ends with colon AND next is BacktickLit -> Merge to quoted string "thread:foo"
                if captured_colon {
                    if let Some(Token::BacktickLit(inner)) = iter.peek() {
                         iter.next(); // Consume backtick token
                         
                         if needs_space { out.push(' '); }
                         out.push('"');
                         out.push_str(text); // "thread:" (without colon? No, we need prefix)
                         // Wait, text was striped of colon above.
                         // Current logic: text = "thread"
                         // We want "thread:foo"
                         out.push(':'); 
                         out.push_str(inner); // "foo"
                         out.push('"');
                         needs_space = true;
                         continue; 
                    }
                }

                let next_is_colon = matches!(iter.peek(), Some(Token::Symbol(':')));
                let is_key = captured_colon || next_is_colon;

                if needs_space {
                    out.push(' ');
                }

                if is_key {
                    out.push('"');
                    out.push_str(text);
                    out.push('"');
                    if captured_colon {
                        out.push(':');
                        needs_space = true;
                    }
                } else {
                    if text.contains(':') {
                        out.push('"');
                        out.push_str(text);
                        out.push('"');
                    } else {
                        out.push_str(text);
                    }
                }

                if !captured_colon {
                    needs_space = true;
                }
            }

            Token::Number(s) => {
                if needs_space { out.push(' '); }
                out.push_str(s);
                needs_space = true;
            }
            Token::StringLit(s) => {
                if needs_space { out.push(' '); }
                let content = &s[1..s.len() - 1];
                out.push('"');
                out.push_str(content);
                out.push('"');
                needs_space = true;
            }
            Token::BacktickLit(s) => {
                // Standalone backtick -> quoted string
                if needs_space { out.push(' '); }
                out.push('"');
                out.push_str(s);
                out.push('"');
                needs_space = true;
            }
            Token::Symbol(c) => {
                if needs_space && c != &',' && c != &')' && c != &']' && c != &':' {
                    out.push(' ');
                }
                out.push(*c);
                if c == &',' || c == &':' {
                    needs_space = true;
                } else if c == &'(' || c == &'[' {
                    needs_space = false;
                } else {
                    needs_space = true;
                }
            }
            Token::Whitespace => {
                needs_space = true;
            }
        }
    }
    out.trim().to_string()
}

// =============================================================================
// TEIL 2: DIE PUBLIC API
// =============================================================================

pub fn sanitize_query(raw_input: &str) -> Result<String, String> {
    match parse_safe_query(raw_input) {
        Ok((remainder, tokens)) => {
            if tokens.is_empty() {
                // Leerer Input oder nur Kommentare/Müll
                if !raw_input.trim().is_empty()
                    && !remainder.contains(";")
                    && !raw_input.trim().starts_with("--")
                {
                    // Strict Mode könnte hier Fehler werfen
                }
            }
            // Wir ignorieren den remainder (alles ab dem Semikolon)
            Ok(rebuild_query(tokens))
        }
        Err(e) => Err(format!("Parsing Error: {}", e)),
    }
}

// -----------------------------------------------------------------------------
// LEGACY BRIDGE
// -----------------------------------------------------------------------------

pub fn fix_surql_json(s: &str) -> String {
    sanitize_query(s).unwrap_or_else(|_| String::new())
}

/// Whether a string is worth trying to parse as a JSON container.
fn looks_like_json_container(s: &str) -> bool {
    (s.starts_with('{') && s.ends_with('}')) || (s.starts_with('[') && s.ends_with(']'))
}

/// Normalize an incoming record for the circuit: collapse `{tb, id}` objects to
/// the `"tb:id"` record-id string at any depth.
///
/// A record that arrives as a JSON STRING is parsed, because the legacy bridge
/// hands the whole record over that way - but only at the TOP level. This used
/// to apply to every nested value too, so a user field of `TYPE string` that
/// happened to hold JSON (whitepawn's `broadcast.scene`, "a built-in id or
/// inline JSON") was silently retyped into an object on its way into the circuit.
///
/// Nothing else in the system does that. The SSP's own bootstrap reads the row
/// straight from SurrealDB and keeps the string; so does the scheduler's replica.
/// So the moment a row carrying such a field was UPDATED during a bootstrap, the
/// replayed event made it an object while both other copies said string, the
/// catch-up hash could never match, and the SSP was re-bootstrapped - into the
/// same disagreement. A livestream updates its `broadcast` row every few
/// seconds, so every restart failed five times, re-cloned the replica, and left
/// the tenant with no ready SSP for about nine minutes until the breaker gave
/// up and admitted it anyway.
///
/// A value's type is the database's to decide, not something to infer from what
/// the text looks like.
pub fn normalize_record(record: Value) -> Value {
    match record {
        Value::String(s) if looks_like_json_container(&s) => {
            match serde_json::from_str::<Value>(&s) {
                Ok(parsed) => normalize_fields(parsed),
                Err(_) => Value::String(s),
            }
        }
        other => normalize_fields(other),
    }
}

/// Borrowing counterpart to [`normalize_record`], for callers that still need
/// the original afterwards.
///
/// Those callers were writing `normalize_record(record.clone())`, which built
/// the tree twice: once for the clone, once for the rebuild that
/// `normalize_record` performs on every object and array regardless. Reading
/// through a reference produces the same output for one construction and no
/// clone.
///
/// The owned version is kept rather than forwarded to this one, because it
/// genuinely benefits from ownership on the leaf paths - a scalar or a string is
/// moved through untouched instead of being copied.
pub fn normalize_record_ref(record: &Value) -> Value {
    match record {
        Value::String(s) if looks_like_json_container(s) => {
            match serde_json::from_str::<Value>(s) {
                Ok(parsed) => normalize_fields(parsed),
                Err(_) => Value::String(s.clone()),
            }
        }
        other => normalize_fields_ref(other),
    }
}

/// `{tb, id}` -> `"tb:id"`, or `None` when the object is not a record id.
fn collapse_record_id(map: &serde_json::Map<String, Value>) -> Option<Value> {
    if map.len() != 2 {
        return None;
    }
    let tb = map.get("tb")?.as_str()?;
    let id = match map.get("id")? {
        Value::String(s) => s.clone(),
        Value::Number(n) => n.to_string(),
        other => other.to_string(),
    };
    Some(Value::String(format!("{tb}:{id}")))
}

/// Everything below the top level. Strings are left exactly as they are.
fn normalize_fields(value: Value) -> Value {
    match value {
        Value::Object(map) => match collapse_record_id(&map) {
            Some(id) => id,
            None => Value::Object(map.into_iter().map(|(k, v)| (k, normalize_fields(v))).collect()),
        },
        Value::Array(arr) => Value::Array(arr.into_iter().map(normalize_fields).collect()),
        other => other,
    }
}

fn normalize_fields_ref(value: &Value) -> Value {
    match value {
        Value::Object(map) => match collapse_record_id(map) {
            Some(id) => id,
            None => Value::Object(
                map.iter().map(|(k, v)| (k.clone(), normalize_fields_ref(v))).collect(),
            ),
        },
        Value::Array(arr) => Value::Array(arr.iter().map(normalize_fields_ref).collect()),
        other => other.clone(),
    }
}

pub fn parse_params(params: Value) -> Option<Value> {
    let s = match params {
        Value::String(s) => fix_surql_json(&s),
        _ => params.to_string(),
    };
    if let Ok(val) = serde_json::from_str(&s) {
        return Some(val);
    }
    None
}

#[cfg(test)]
mod normalize_ref_tests {
    use super::*;
    use serde_json::json;

    /// The borrowed and owned normalizers must agree exactly — the borrowed
    /// one exists only to avoid a clone, never to change behaviour.
    #[track_caller]
    fn assert_same(v: Value) {
        assert_eq!(
            normalize_record_ref(&v),
            normalize_record(v.clone()),
            "borrowed normalize diverged for {v}"
        );
    }

    #[test]
    fn agrees_on_scalars_and_containers() {
        assert_same(json!(null));
        assert_same(json!(5));
        assert_same(json!(5.5));
        assert_same(json!(true));
        assert_same(json!("plain string"));
        assert_same(json!([]));
        assert_same(json!({}));
        assert_same(json!({ "a": 1, "b": [1, 2, { "c": 3 }] }));
    }

    #[test]
    fn agrees_on_record_id_collapsing() {
        // The `{tb, id}` shape collapses to "tb:id" at any depth.
        assert_same(json!({ "tb": "user", "id": "abc" }));
        assert_same(json!({ "tb": "user", "id": 7 }));
        assert_same(json!({ "owner": { "tb": "user", "id": "abc" } }));
        assert_same(json!([{ "tb": "user", "id": "abc" }]));
        // A three-key object is NOT a record id.
        assert_same(json!({ "tb": "user", "id": "abc", "extra": 1 }));
    }

    /// whitepawn's `broadcast.scene` is `TYPE option<string>` holding inline JSON.
    /// Retyping it into an object here, while the bootstrap and the scheduler
    /// both keep the string, made every catch-up hash mismatch and took the
    /// tenant's only SSP out for nine minutes on every restart.
    #[test]
    fn a_string_field_holding_json_stays_a_string() {
        let scene = r#"{"id":"big-board","name":"Board Focus","canvas":{"width":1920,"height":1080}}"#;
        let row = json!({
            "id": "broadcast:abc",
            "scene": scene,
            "tags": "[1,2,3]",
            "nested": { "blob": "{\"a\":1}" },
            "list": ["{\"b\":2}"],
        });
        for out in [normalize_record(row.clone()), normalize_record_ref(&row)] {
            assert_eq!(out["scene"], json!(scene), "a string is the database's to type");
            assert_eq!(out["tags"], json!("[1,2,3]"));
            assert_eq!(out["nested"]["blob"], json!("{\"a\":1}"), "at any depth");
            assert_eq!(out["list"][0], json!("{\"b\":2}"), "and inside arrays");
        }
    }

    /// The one place a JSON string IS a record: the legacy bridge hands the whole
    /// thing over stringified. Record ids inside it still collapse.
    #[test]
    fn a_whole_record_arriving_as_a_string_is_still_parsed() {
        let whole = r#"{"id":"t:1","owner":{"tb":"user","id":"abc"},"scene":"{\"x\":1}"}"#;
        let out = normalize_record(json!(whole));
        assert_eq!(out["owner"], json!("user:abc"));
        assert_eq!(out["scene"], json!("{\"x\":1}"), "but its string fields are not");
        assert_eq!(out, normalize_record_ref(&json!(whole)));
    }

    #[test]
    fn agrees_on_embedded_json_strings() {
        assert_same(json!("{\"tb\":\"user\",\"id\":\"abc\"}"));
        assert_same(json!("[1,2,3]"));
        // Looks like JSON but is not — must stay a string.
        assert_same(json!("{not json}"));
        assert_same(json!("{"));
    }
}
