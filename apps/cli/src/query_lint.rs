//! Statement shapes SurrealDB 4.0.1 runs as table scans inside a `DEFINE
//! EVENT` or `DEFINE FUNCTION` body, found by reading the schema source.
//!
//! The same statement at the top level of a query uses its index; inside an
//! event or function body 4.0.1 plans it from what it knows when the body is
//! compiled. Measured on 4.0.1 and 3.3.5 (300k-row table, one index on `f`;
//! `docs/surrealdb-bugs/v4-event-param-index-scan.md`):
//!
//! | inside an event or function | 4.0.1 | 3.3.5 |
//! | --- | ---: | ---: |
//! | `SELECT ... WHERE f = $after.x` | scan | index |
//! | `LET $x = $after.x;` first, then `SELECT ... WHERE f = $x` (also in IF and FOR) | index | index |
//! | `UPDATE t ... WHERE f = $x` / `DELETE t WHERE f = $x`, `$x` LET-bound | scan | index |
//! | `UPDATE (SELECT VALUE id FROM t WHERE f = $x) ...` / `DELETE (SELECT VALUE id ...)` | index | index |
//! | `... WHERE f INSIDE $x`, even in the subquery form | scan | index |
//! | `SELECT ... WHERE a = $x` where `a` has only a composite index `(a, b)` (any prefix, unique or not; also in a top-level FOR loop) | scan | index |
//! | in a function that also calls another `fn::` and writes: its lookups (`DELETE (SELECT ...)`, `LET $row = (SELECT ... LIMIT 1)[0]; UPDATE $row`, ...) | scan | index |
//! | the lookups and writes in a helper function that calls no other `fn::` | index | index |
//!
//! So the rewrites below are what both lines plan as index reads, and none of
//! them is slower on 3.3.5. The lint reads text, not a parse tree: it finds
//! the bodies, splits them into statements and looks at each one's target
//! and WHERE clause. It errs towards reporting a statement it cannot place.

use std::fmt;

/// One statement the lint would rewrite.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Finding {
    /// `event <name> ON <table>` or `function fn::<name>`.
    pub owner: String,
    pub rule: Rule,
    /// The statement, whitespace-collapsed.
    pub statement: String,
    /// What to write instead.
    pub fix: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Rule {
    /// A WHERE compares a field to `$after.x` / `$before.x` / `$value` /
    /// `$input` (or another event value) directly.
    WhereOnEventValue,
    /// `UPDATE t ... WHERE`, `DELETE t WHERE`, `UPSERT t ... WHERE` with a
    /// parameter in the WHERE.
    WriteWhere,
    /// `WHERE f INSIDE $x` / `IN $x` / `CONTAINSANY $x`.
    InsideParam,
    /// A table or field select rule written `WHERE true` instead of `FULL`.
    SelectWhereTrue,
    /// A lookup on fields that only a composite index covers.
    CompositeIndex,
    /// A lookup by a parameter in a function body that also writes and calls
    /// another `fn::` function.
    LookupWriteWithCall,
}

impl Rule {
    pub fn code(self) -> &'static str {
        match self {
            Rule::WhereOnEventValue => "where-on-event-value",
            Rule::WriteWhere => "write-where",
            Rule::InsideParam => "inside-param",
            Rule::SelectWhereTrue => "select-where-true",
            Rule::CompositeIndex => "composite-index",
            Rule::LookupWriteWithCall => "lookup-write-with-call",
        }
    }
}

impl fmt::Display for Finding {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} [{}]: {}\n    fix: {}",
            self.owner,
            self.rule.code(),
            self.statement,
            self.fix
        )
    }
}

/// Parameters an event or function body receives from SurrealDB rather than
/// from a `LET` of its own. (`$parent` and `$this` are left out: inside a
/// subquery they are the outer document, the correlated form that keeps an
/// index, see `sp00ky::LEDGER_LOOKUP_ROWS`.)
const EVENT_VALUES: &[&str] = &["$after", "$before", "$value", "$input", "$event"];

/// Every finding in a schema source.
pub fn lint(source: &str) -> Vec<Finding> {
    let clean = strip_comments(source);
    let indexes = indexes(&clean);
    let mut out = Vec::new();
    for (owner, body) in bodies(&clean) {
        let stmts = statements(&body);
        for stmt in &stmts {
            out.extend(check_statement(&owner, stmt, &indexes));
        }
        if owner.starts_with("function ") && calls_a_function(&body) && writes(&body) {
            out.extend(stmts.iter().filter_map(|s| lookup_with_call(&owner, s)));
        }
    }
    out.extend(select_where_true(&clean));
    let allowed = allowed(source);
    out.retain(|f| {
        !allowed
            .iter()
            .any(|(owner, rule)| owner == &f.owner && rule == f.rule.code())
    });
    out
}

/// `-- spky-lint: allow <rule>[, <rule>]` on the lines right above a `DEFINE
/// EVENT` or `DEFINE FUNCTION` silences those rules for that event or
/// function: `(owner, rule)` pairs.
fn allowed(source: &str) -> Vec<(String, String)> {
    let mut out = Vec::new();
    let mut pending: Vec<String> = Vec::new();
    for line in source.lines() {
        let t = line.trim();
        if let Some(i) = t.find("spky-lint: allow ") {
            if t.starts_with("--") || t.starts_with("//") {
                pending.extend(
                    t[i + 17..]
                        .split(',')
                        .map(|r| r.trim().to_string())
                        .filter(|r| !r.is_empty()),
                );
                continue;
            }
        }
        if t.is_empty() || t.starts_with("--") || t.starts_with("//") {
            continue;
        }
        if !pending.is_empty() {
            if let Some((owner, _)) = bodies(&strip_comments(&format!("{t}\n}};")))
                .into_iter()
                .next()
            {
                let owner = if owner.starts_with("event ") {
                    owner
                } else {
                    owner.split('(').next().unwrap_or(&owner).to_string()
                };
                out.extend(pending.drain(..).map(|r| (owner.clone(), r)));
            } else if let Some(name) = function_name(t) {
                out.extend(pending.drain(..).map(|r| (format!("function {name}"), r)));
            }
            pending.clear();
        }
    }
    out
}

/// `fn::x` of a `DEFINE FUNCTION [OVERWRITE | IF NOT EXISTS] fn::x(...` line.
fn function_name(line: &str) -> Option<String> {
    let upper = line.to_ascii_uppercase();
    if !upper.starts_with("DEFINE FUNCTION ") {
        return None;
    }
    line.split_whitespace()
        .skip(2)
        .find(|w| {
            !matches!(
                w.to_ascii_uppercase().as_str(),
                "OVERWRITE" | "IF" | "NOT" | "EXISTS"
            )
        })
        .map(|w| w.split('(').next().unwrap_or(w).to_string())
}

/// The body calls a `fn::` function (a custom one; built-ins do not matter).
fn calls_a_function(body: &str) -> bool {
    let b = body.as_bytes();
    let mut i = 0;
    while i + 4 < b.len() {
        if b[i] == b'\'' || b[i] == b'"' || b[i] == b'`' {
            i = skip_string(b, i);
            continue;
        }
        if body[i..].starts_with("fn::")
            && (i == 0 || !(b[i - 1].is_ascii_alphanumeric() || b[i - 1] == b'_'))
        {
            return true;
        }
        i += 1;
    }
    false
}

/// The body writes: UPDATE, DELETE, UPSERT, CREATE, INSERT or RELATE.
fn writes(body: &str) -> bool {
    let b = body.as_bytes();
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'\'' || b[i] == b'"' || b[i] == b'`' {
            i = skip_string(b, i);
            continue;
        }
        let at_word = i == 0
            || !(b[i - 1].is_ascii_alphanumeric()
                || b[i - 1] == b'_'
                || b[i - 1] == b':'
                || b[i - 1] == b'$');
        if at_word {
            for kw in ["UPDATE", "DELETE", "UPSERT", "CREATE", "INSERT", "RELATE"] {
                let n = kw.len();
                if i + n < b.len()
                    && b[i..i + n].eq_ignore_ascii_case(kw.as_bytes())
                    && b[i + n].is_ascii_whitespace()
                {
                    return true;
                }
            }
        }
        i += 1;
    }
    false
}

/// A function body that calls another `fn::` function and writes: SurrealDB
/// 4.0.1 plans its lookups as table scans (measured on 40k rows: 20-60 ms per
/// call against about 1 ms; `fn::feature::assign` and `materialize`, whose
/// lookups kept their index once moved into helpers that call nothing).
/// Event bodies and functions without the call are not affected.
fn lookup_with_call(owner: &str, stmt: &str) -> Option<Finding> {
    let where_clause = own_where(stmt)?;
    if params_in(&without_subqueries(&where_clause)).is_empty() {
        return None;
    }
    Some(Finding {
        owner: owner.to_string(),
        rule: Rule::LookupWriteWithCall,
        statement: stmt.to_string(),
        fix: "move the lookups and the writes into a function of their own that calls no other `fn::` and call it \
              from here (4.0.1 reads the whole table for a lookup in a function that both writes and calls another function)"
            .to_string(),
    })
}

/// Columns of every index in a source, per table.
type Indexes = std::collections::HashMap<String, Vec<Vec<String>>>;

/// `DEFINE INDEX ... ON [TABLE] t FIELDS|COLUMNS a, b ...` statements.
fn indexes(src: &str) -> Indexes {
    let mut out = Indexes::new();
    let mut at = 0;
    while let Some(i) = find_ci(src, "DEFINE INDEX ", at) {
        let end = statement_end(src.as_bytes(), i);
        at = end.max(i + 13);
        let stmt = collapse(&src[i..end]);
        let words: Vec<&str> = stmt.split_whitespace().collect();
        let Some(on) = words.iter().position(|w| w.eq_ignore_ascii_case("ON")) else {
            continue;
        };
        let mut t = on + 1;
        if words
            .get(t)
            .is_some_and(|w| w.eq_ignore_ascii_case("TABLE"))
        {
            t += 1;
        }
        let Some(table) = words.get(t) else { continue };
        let Some(cols_at) = words
            .iter()
            .position(|w| w.eq_ignore_ascii_case("FIELDS") || w.eq_ignore_ascii_case("COLUMNS"))
        else {
            continue;
        };
        let mut cols = Vec::new();
        for w in &words[cols_at + 1..] {
            let ends = !w.ends_with(',');
            let name = w.trim_end_matches(',').trim_matches('`');
            if !name.is_empty() {
                cols.push(name.to_string());
            }
            if ends {
                break;
            }
        }
        if !cols.is_empty() {
            out.entry(table.trim_matches('`').to_string())
                .or_default()
                .push(cols);
        }
    }
    out
}

/// The table a SELECT reads: its own `FROM <table>`, `None` for a subquery,
/// a parameter, a record id or an object.
fn own_from(stmt: &str) -> Option<String> {
    let b = stmt.as_bytes();
    let mut depth = 0i32;
    let mut i = 0;
    while i < b.len() {
        match b[i] {
            b'\'' | b'"' | b'`' => {
                i = skip_string(b, i);
                continue;
            }
            b'(' | b'[' | b'{' => depth += 1,
            b')' | b']' | b'}' => depth -= 1,
            _ => {}
        }
        if depth == 0
            && (i == 0 || b[i - 1].is_ascii_whitespace())
            && stmt[i..].len() > 5
            && stmt[i..i + 5].eq_ignore_ascii_case("FROM ")
        {
            let mut words = stmt[i + 5..].split_whitespace();
            let mut t = words.next()?;
            if t.eq_ignore_ascii_case("ONLY") {
                t = words.next()?;
            }
            let t = t.trim_end_matches([',', ';']);
            if t.starts_with(['(', '$', '[', '{']) || t.contains([':', '(']) {
                return None;
            }
            return Some(t.trim_matches('`').to_string());
        }
        i += 1;
    }
    None
}

/// Fields a WHERE compares with a parameter (`f = $x`, `$x < f`, ...).
fn param_fields(where_own: &str) -> Vec<String> {
    let mut out = Vec::new();
    let upper = where_own.to_ascii_uppercase();
    let mut terms = vec![];
    let mut last = 0;
    for (i, _) in upper.match_indices(' ') {
        let rest = &upper[i..];
        let n = if rest.starts_with(" AND ")
            || rest.starts_with(" OR ")
            || rest.starts_with(" && ")
            || rest.starts_with(" || ")
        {
            rest[1..].find(' ').map(|k| k + 1)
        } else {
            None
        };
        if let Some(n) = n {
            terms.push(&where_own[last..i]);
            last = i + n;
        }
    }
    terms.push(&where_own[last..]);
    for term in terms {
        let term = term
            .trim()
            .trim_start_matches('(')
            .trim_end_matches(')')
            .trim();
        let Some((pos, len)) = ["==", "<=", ">=", "=", "<", ">"]
            .iter()
            .filter_map(|op| term.find(op).map(|p| (p, op.len())))
            .filter(|(p, _)| {
                *p == 0
                    || !matches!(
                        term.as_bytes()[p - 1],
                        b'!' | b'<' | b'>' | b'=' | b'?' | b'~'
                    )
            })
            .min_by_key(|(p, _)| *p)
        else {
            continue;
        };
        let (left, right) = (term[..pos].trim(), term[pos + len..].trim());
        let ident = |s: &str| {
            !s.is_empty()
                && s.bytes()
                    .all(|c| c.is_ascii_alphanumeric() || c == b'_' || c == b'.' || c == b'`')
        };
        let field = if right.contains('$') && ident(left) {
            left
        } else if left.contains('$') && ident(right) {
            right
        } else {
            continue;
        };
        out.push(field.trim_matches('`').to_string());
    }
    out
}

/// `FOR select WHERE true` on a table or a field. It is the same rule as
/// `FOR select FULL`, but SurrealDB 4.0.1 plans them differently for a record
/// user: with a select rule other than FULL on a field, the user's query
/// filtering on that field reads the whole table (the index is not used, on
/// 3.3.5 either); with one on a table, a lookup an event runs for a record
/// user's write reads the whole table (a WhitePawn game delete by its owner:
/// 37-115 s, 0.2-0.5 s with FULL).
fn select_where_true(src: &str) -> Vec<Finding> {
    let b = src.as_bytes();
    let mut out = Vec::new();
    let mut at = 0;
    while let Some(i) = find_ci(src, "DEFINE ", at) {
        at = i + 7;
        let kind = src[at..]
            .split_whitespace()
            .next()
            .unwrap_or("")
            .to_ascii_uppercase();
        if kind != "TABLE" && kind != "FIELD" {
            continue;
        }
        let end = statement_end(b, i);
        let stmt = collapse(&src[i..end]);
        at = end.max(at);
        let upper = stmt.to_ascii_uppercase();
        let Some(perm) = upper.find(" PERMISSIONS ") else {
            continue;
        };
        let rules = &stmt[perm + 13..];
        // each `FOR <ops> <rule>` clause
        for clause in rules
            .split(" FOR ")
            .chain(rules.strip_prefix("FOR ").into_iter().take(0))
        {
            let clause = clause
                .trim()
                .trim_start_matches("FOR ")
                .trim_end_matches(',')
                .trim();
            let lower = clause.to_ascii_lowercase();
            let Some(w) = lower.find(" where ") else {
                continue;
            };
            let ops: Vec<&str> = lower[..w].split(',').map(str::trim).collect();
            let cond = lower[w + 7..].trim().trim_end_matches(',').trim();
            let cond = cond
                .split(|c: char| c == ',' || c == ';')
                .next()
                .unwrap_or("")
                .trim();
            if ops.contains(&"select") && cond == "true" {
                let words: Vec<&str> = stmt.split_whitespace().collect();
                let owner = if kind == "TABLE" {
                    format!(
                        "table {}",
                        words
                            .iter()
                            .skip(2)
                            .find(|w| !matches!(
                                w.to_ascii_uppercase().as_str(),
                                "OVERWRITE" | "IF" | "NOT" | "EXISTS"
                            ))
                            .unwrap_or(&"?")
                    )
                } else {
                    let name = words
                        .iter()
                        .skip(2)
                        .find(|w| {
                            !matches!(
                                w.to_ascii_uppercase().as_str(),
                                "OVERWRITE" | "IF" | "NOT" | "EXISTS"
                            )
                        })
                        .unwrap_or(&"?");
                    let table = words
                        .windows(2)
                        .find(|w| w[0].eq_ignore_ascii_case("ON"))
                        .map(|w| {
                            if w[1].eq_ignore_ascii_case("TABLE") {
                                ""
                            } else {
                                w[1]
                            }
                        })
                        .unwrap_or("?");
                    let table = if table.is_empty() {
                        words
                            .windows(2)
                            .find(|w| {
                                w[0].eq_ignore_ascii_case("TABLE")
                                    && !w[1].eq_ignore_ascii_case("TYPE")
                            })
                            .map(|w| w[1])
                            .unwrap_or("?")
                    } else {
                        table
                    };
                    format!("field {name} ON {table}")
                };
                out.push(Finding {
                    owner,
                    rule: Rule::SelectWhereTrue,
                    statement: format!("FOR {} WHERE true", ops.join(", ")),
                    fix: format!(
                        "FOR {} FULL (the same rule; SurrealDB 4.0.1 gives up indexes for record users under `WHERE true`)",
                        ops.join(", ")
                    ),
                });
            }
        }
    }
    out
}

/// Findings per schema file of a project: the schema source and every
/// backend's outbox schema, as `(path relative to the config, findings)`.
pub fn lint_project(
    config: &crate::backend::Sp00kyConfig,
    base_dir: &std::path::Path,
) -> Vec<(String, Vec<Finding>)> {
    let mut files: Vec<String> = vec![config
        .resolved_schema()
        .schema
        .to_string_lossy()
        .into_owned()];
    for app in config.apps.values() {
        if let Some(m) = &app.method {
            files.push(m.schema.clone());
        }
    }
    files.sort();
    files.dedup();
    files
        .into_iter()
        .filter_map(|f| {
            let src = std::fs::read_to_string(base_dir.join(&f)).ok()?;
            let found = lint(&src);
            (!found.is_empty()).then_some((f, found))
        })
        .collect()
}

/// The source without `--` and `//` line comments and `/* */` blocks, string
/// literals left intact.
pub fn strip_comments(src: &str) -> String {
    let b = src.as_bytes();
    let mut out = String::with_capacity(src.len());
    let mut i = 0;
    while i < b.len() {
        let c = b[i];
        if c == b'\'' || c == b'"' || c == b'`' {
            let end = skip_string(b, i);
            out.push_str(&src[i..end]);
            i = end;
            continue;
        }
        if (c == b'-' && b.get(i + 1) == Some(&b'-')) || (c == b'/' && b.get(i + 1) == Some(&b'/'))
        {
            while i < b.len() && b[i] != b'\n' {
                i += 1;
            }
            continue;
        }
        if c == b'/' && b.get(i + 1) == Some(&b'*') {
            i += 2;
            while i + 1 < b.len() && !(b[i] == b'*' && b[i + 1] == b'/') {
                i += 1;
            }
            i += 2;
            continue;
        }
        let ch = src[i..].chars().next().unwrap();
        out.push(ch);
        i += ch.len_utf8();
    }
    out
}

fn skip_string(b: &[u8], start: usize) -> usize {
    let q = b[start];
    let mut i = start + 1;
    while i < b.len() {
        if b[i] == b'\\' {
            i += 2;
            continue;
        }
        if b[i] == q {
            return i + 1;
        }
        i += 1;
    }
    b.len()
}

/// The index of the brace closing the one at `open`.
fn matching_brace(b: &[u8], open: usize) -> Option<usize> {
    let mut depth = 0i32;
    let mut i = open;
    while i < b.len() {
        match b[i] {
            b'\'' | b'"' | b'`' => {
                i = skip_string(b, i);
                continue;
            }
            b'{' => depth += 1,
            b'}' => {
                depth -= 1;
                if depth == 0 {
                    return Some(i);
                }
            }
            _ => {}
        }
        i += 1;
    }
    None
}

fn find_ci(hay: &str, needle: &str, from: usize) -> Option<usize> {
    let h = hay.as_bytes();
    let n = needle.as_bytes();
    if n.is_empty() || from >= h.len() {
        return None;
    }
    (from..=h.len().saturating_sub(n.len())).find(|&i| h[i..i + n.len()].eq_ignore_ascii_case(n))
}

/// `(owner, body)` for every DEFINE EVENT and DEFINE FUNCTION.
fn bodies(src: &str) -> Vec<(String, String)> {
    let b = src.as_bytes();
    let mut out = Vec::new();
    let mut at = 0;
    while let Some(i) = find_ci(src, "DEFINE ", at) {
        at = i + 7;
        let rest = &src[at..];
        let mut words = rest.split_whitespace();
        let kind = words.next().unwrap_or("").to_ascii_uppercase();
        if kind != "EVENT" && kind != "FUNCTION" {
            continue;
        }
        // the header runs to the body's opening brace; an event whose THEN is
        // a single statement (no braces) ends at its own `;`
        let semi = statement_end(rest.as_bytes(), 0);
        let open_rel = match rest.find('{') {
            Some(o) if o < semi => o,
            _ => {
                let stmt = &rest[..semi];
                if let Some(then) = find_ci(stmt, "THEN ", 0) {
                    let mut names = stmt.split_whitespace().skip(1).filter(|t| {
                        !matches!(
                            t.to_ascii_uppercase().as_str(),
                            "OVERWRITE" | "IF" | "NOT" | "EXISTS"
                        )
                    });
                    let name = names.next().unwrap_or("?");
                    out.push((
                        format!("{} {name}", kind.to_ascii_lowercase()),
                        stmt[then + 5..].to_string(),
                    ));
                }
                at += semi;
                continue;
            }
        };
        let header = &rest[..open_rel];
        let mut tokens: Vec<&str> = header.split_whitespace().skip(1).collect();
        tokens.retain(|t| {
            !matches!(
                t.to_ascii_uppercase().as_str(),
                "OVERWRITE" | "IF" | "NOT" | "EXISTS"
            )
        });
        let name = tokens
            .first()
            .map(|t| t.split('(').next().unwrap_or(t))
            .unwrap_or("?");
        let owner = if kind == "EVENT" {
            let table = header
                .split_whitespace()
                .collect::<Vec<_>>()
                .windows(2)
                .find(|w| w[0].eq_ignore_ascii_case("ON"))
                .map(|w| {
                    if w[1].eq_ignore_ascii_case("TABLE") {
                        header
                            .split_whitespace()
                            .skip_while(|t| !t.eq_ignore_ascii_case("TABLE"))
                            .nth(1)
                            .unwrap_or("?")
                    } else {
                        w[1]
                    }
                })
                .unwrap_or("?");
            format!("event {name} ON {table}")
        } else {
            format!("function {name}")
        };
        let open = at + open_rel;
        let Some(close) = matching_brace(b, open) else {
            continue;
        };
        out.push((owner, src[open + 1..close].to_string()));
        at = close;
    }
    out
}

/// Statements of a body, nested blocks (IF, FOR, `{ }`) flattened: each
/// SELECT / UPDATE / DELETE / UPSERT with its text up to the end of the
/// statement or of the subquery it sits in.
fn statements(body: &str) -> Vec<String> {
    let b = body.as_bytes();
    let mut out = Vec::new();
    let mut i = 0;
    while i < b.len() {
        match b[i] {
            b'\'' | b'"' | b'`' => {
                i = skip_string(b, i);
                continue;
            }
            _ => {}
        }
        let at_word =
            i == 0 || !(b[i - 1].is_ascii_alphanumeric() || b[i - 1] == b'_' || b[i - 1] == b':');
        if at_word {
            for kw in ["SELECT", "UPDATE", "DELETE", "UPSERT"] {
                let n = kw.len();
                if i + n < b.len()
                    && b[i..i + n].eq_ignore_ascii_case(kw.as_bytes())
                    && !(b[i + n].is_ascii_alphanumeric() || b[i + n] == b'_' || b[i + n] == b':')
                {
                    let end = statement_end(b, i);
                    out.push(collapse(&body[i..end]));
                }
            }
        }
        i += 1;
    }
    out
}

/// End of the statement starting at `start`: the first `;` or unmatched
/// closing bracket at its own depth.
fn statement_end(b: &[u8], start: usize) -> usize {
    let mut depth = 0i32;
    let mut i = start;
    while i < b.len() {
        match b[i] {
            b'\'' | b'"' | b'`' => {
                i = skip_string(b, i);
                continue;
            }
            b'(' | b'[' | b'{' => depth += 1,
            b')' | b']' | b'}' => {
                if depth == 0 {
                    return i;
                }
                depth -= 1;
            }
            b';' if depth == 0 => return i,
            _ => {}
        }
        i += 1;
    }
    b.len()
}

fn collapse(s: &str) -> String {
    s.split_whitespace().collect::<Vec<_>>().join(" ")
}

/// The statement's own WHERE clause (not one inside a nested subquery), up to
/// the next clause keyword at its depth.
fn own_where(stmt: &str) -> Option<String> {
    let b = stmt.as_bytes();
    let mut depth = 0i32;
    let mut i = 0;
    let mut start = None;
    while i < b.len() {
        match b[i] {
            b'\'' | b'"' | b'`' => {
                i = skip_string(b, i);
                continue;
            }
            b'(' | b'[' | b'{' => depth += 1,
            b')' | b']' | b'}' => depth -= 1,
            _ => {}
        }
        if depth == 0 {
            let word_start = i == 0 || !(b[i - 1].is_ascii_alphanumeric() || b[i - 1] == b'_');
            if word_start {
                let rest = &stmt[i..];
                let upper = rest.get(..12).unwrap_or(rest).to_ascii_uppercase();
                if start.is_none() && upper.starts_with("WHERE ") {
                    start = Some(i + 6);
                } else if start.is_some()
                    && [
                        "ORDER BY", "LIMIT ", "GROUP ", "START ", "RETURN ", "FETCH ", "TIMEOUT ",
                        "SPLIT ", "PARALLEL", "EXPLAIN", "WITH ",
                    ]
                    .iter()
                    .any(|k| upper.starts_with(k))
                {
                    return Some(stmt[start.unwrap()..i].trim().to_string());
                }
            }
        }
        i += 1;
    }
    start.map(|s| stmt[s..].trim().to_string())
}

/// Text of `s` with nested `( ... )` groups that are subqueries removed, so a
/// check on a statement's own clause does not see its subqueries'.
fn without_subqueries(s: &str) -> String {
    let b = s.as_bytes();
    let mut out = String::new();
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'(' {
            let inner = s[i + 1..].trim_start().to_ascii_uppercase();
            if inner.starts_with("SELECT")
                || inner.starts_with("UPDATE")
                || inner.starts_with("DELETE")
            {
                let mut depth = 0;
                while i < b.len() {
                    match b[i] {
                        b'(' => depth += 1,
                        b')' => {
                            depth -= 1;
                            if depth == 0 {
                                break;
                            }
                        }
                        _ => {}
                    }
                    i += 1;
                }
                out.push_str("(…)");
                i += 1;
                continue;
            }
        }
        let ch = s[i..].chars().next().unwrap();
        out.push(ch);
        i += ch.len_utf8();
    }
    out
}

fn params_in(s: &str) -> Vec<String> {
    let b = s.as_bytes();
    let mut out = Vec::new();
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'\'' || b[i] == b'"' || b[i] == b'`' {
            i = skip_string(b, i);
            continue;
        }
        if b[i] == b'$' {
            let mut j = i + 1;
            while j < b.len() && (b[j].is_ascii_alphanumeric() || b[j] == b'_' || b[j] == b'.') {
                j += 1;
            }
            out.push(s[i..j].trim_end_matches('.').to_string());
            i = j;
            continue;
        }
        i += 1;
    }
    out
}

fn is_event_value(p: &str) -> bool {
    EVENT_VALUES
        .iter()
        .any(|v| p == *v || p.starts_with(&format!("{v}.")))
}

/// The write target of an UPDATE / DELETE / UPSERT: `Some(table)` when it is
/// a table name (the shape that scans), `None` for a subquery, a parameter,
/// a record id or a function call.
fn write_target(stmt: &str) -> Option<String> {
    let mut words = stmt.split_whitespace();
    let _verb = words.next()?;
    let mut target = words.next()?;
    if target.eq_ignore_ascii_case("FROM") || target.eq_ignore_ascii_case("ONLY") {
        target = words.next()?;
    }
    if target.eq_ignore_ascii_case("ONLY") {
        target = words.next()?;
    }
    let t = target.trim_end_matches(',');
    if t.starts_with('(')
        || t.starts_with('$')
        || t.contains(':')
        || t.contains('(')
        || t.starts_with('[')
    {
        return None;
    }
    Some(t.trim_matches('`').to_string())
}

fn check_statement(owner: &str, stmt: &str, indexes: &Indexes) -> Vec<Finding> {
    let mut out = Vec::new();
    let Some(where_clause) = own_where(stmt) else {
        return out;
    };
    let where_own = without_subqueries(&where_clause);
    let params = params_in(&where_own);
    if params.is_empty() {
        return out;
    }
    let verb = stmt
        .split_whitespace()
        .next()
        .unwrap_or("")
        .to_ascii_uppercase();
    let upper_where = where_own.to_ascii_uppercase();
    if [
        "INSIDE $",
        " IN $",
        "CONTAINSANY $",
        "ANYINSIDE $",
        "CONTAINSALL $",
        "ALLINSIDE $",
    ]
    .iter()
    .any(|k| upper_where.contains(k))
    {
        out.push(Finding {
            owner: owner.to_string(),
            rule: Rule::InsideParam,
            statement: stmt.to_string(),
            fix: "loop over the values and match each with `=`: `FOR $v IN $values { ... WHERE field = $v ... };` \
                  (4.0.1 scans `INSIDE $param` inside a body, even in a subquery)"
                .to_string(),
        });
    }
    if matches!(verb.as_str(), "UPDATE" | "DELETE" | "UPSERT") {
        if let Some(table) = write_target(stmt) {
            let fix = match verb.as_str() {
                "DELETE" => format!("LET $ids = (SELECT VALUE id FROM {table} WHERE {where_clause}); DELETE $ids;"),
                "UPDATE" => format!("LET $ids = (SELECT VALUE id FROM {table} WHERE {where_clause}); UPDATE $ids SET ...;"),
                _ => format!(
                    "key the row (`UPSERT type::record('{table}', [...]) SET ...`) or `LET $ids = (SELECT VALUE id FROM {table} WHERE {where_clause}); UPDATE $ids SET ...;` plus a CREATE when it matched nothing"
                ),
            };
            let bind = if params.iter().any(|p| is_event_value(p)) {
                " with every `$after.x` / `$before.x` first bound by a `LET` at the top of the body"
            } else {
                ""
            };
            out.push(Finding {
                owner: owner.to_string(),
                rule: Rule::WriteWhere,
                statement: stmt.to_string(),
                fix: format!("{fix}{bind} (4.0.1 scans `{verb} {table} ... WHERE` inside a body, even with LET-bound parameters)"),
            });
            return out;
        }
    }
    if verb == "SELECT" {
        if let Some(found) = composite_only(owner, stmt, &where_own, indexes) {
            out.push(found);
        }
    }
    let direct: Vec<&String> = params.iter().filter(|p| is_event_value(p)).collect();
    if !direct.is_empty() {
        let lets: Vec<String> = direct
            .iter()
            .map(|p| {
                format!(
                    "LET ${} = {p};",
                    p.trim_start_matches('$').replace('.', "_")
                )
            })
            .collect();
        out.push(Finding {
            owner: owner.to_string(),
            rule: Rule::WhereOnEventValue,
            statement: stmt.to_string(),
            fix: format!(
                "bind the value at the top of the body and filter on the variable: {} then `WHERE ... = ${}` \
                 (4.0.1 scans a WHERE on `{}` inside a body)",
                lets.join(" "),
                direct[0].trim_start_matches('$').replace('.', "_"),
                direct[0]
            ),
        });
    }
    out
}

/// A SELECT whose parameter-compared fields have a composite index and no
/// single-column one: 4.0.1 does not use a composite index for a parameter
/// inside a body (or a FOR loop), not even all of its columns matched with
/// `=` (`docs/surrealdb-bugs/v4-block-param-index-scan.md`).
fn composite_only(owner: &str, stmt: &str, where_own: &str, indexes: &Indexes) -> Option<Finding> {
    let table = own_from(stmt)?;
    let fields = param_fields(where_own);
    let defined = indexes.get(&table)?;
    if fields.is_empty()
        || fields
            .iter()
            .any(|f| defined.iter().any(|cols| cols.len() == 1 && &cols[0] == f))
    {
        return None;
    }
    let composite = defined
        .iter()
        .find(|cols| cols.len() > 1 && cols.iter().any(|c| fields.contains(c)))?;
    let field = composite.iter().find(|c| fields.contains(c)).unwrap();
    Some(Finding {
        owner: owner.to_string(),
        rule: Rule::CompositeIndex,
        statement: stmt.to_string(),
        fix: format!(
            "DEFINE INDEX idx_{table}_{} ON TABLE {table} COLUMNS {field}; (4.0.1 reads the whole table instead of the index ({}) for a parameter inside a body; a single-column index is used)",
            field.replace('.', "_"),
            composite.join(", ")
        ),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn allow_comments_silence_a_rule_for_one_definition() {
        let src = "-- the row may be invisible to the caller\n\
            -- spky-lint: allow write-where\n\
            DEFINE FUNCTION OVERWRITE fn::a($k: string) { UPSERT t SET v = 1 WHERE k = $k; };\n\
            DEFINE FUNCTION fn::b($k: string) { UPSERT t SET v = 1 WHERE k = $k; };\n\
            -- spky-lint: allow inside-param\n\
            DEFINE EVENT e ON t THEN { DELETE u WHERE k = $after.k; };";
        let found = lint(src);
        assert_eq!(
            found
                .iter()
                .map(|f| (f.owner.as_str(), f.rule))
                .collect::<Vec<_>>(),
            vec![
                ("function fn::b", Rule::WriteWhere),
                ("event e ON t", Rule::WriteWhere)
            ],
            "{found:?}"
        );
    }

    #[test]
    fn flags_lookups_in_functions_that_write_and_call_functions() {
        let src = "DEFINE FUNCTION fn::m($user: record) { \
            LET $row = (SELECT VALUE id FROM uf WHERE user = $user LIMIT 1)[0]; \
            IF $row != NONE { UPDATE $row SET v = fn::other(1); }; };\n\
            DEFINE FUNCTION fn::put($user: record) { LET $row = (SELECT VALUE id FROM uf WHERE user = $user LIMIT 1)[0]; \
            IF $row != NONE { UPDATE $row SET v = 1 } ELSE { CREATE uf SET user = $user }; };\n\
            DEFINE FUNCTION fn::read($user: record) { LET $x = fn::other(1); RETURN (SELECT * FROM uf WHERE user = $user); };\n\
            DEFINE FUNCTION fn::orchestrate($user: record) { LET $v = fn::other(1); RETURN fn::put($user); };\n\
            DEFINE EVENT e ON t THEN { LET $k = $after.k; DELETE (SELECT VALUE id FROM uf WHERE key = $k); LET $x = fn::other($k); };";
        let found = lint(src);
        assert_eq!(
            found
                .iter()
                .map(|f| (f.rule, f.owner.as_str()))
                .collect::<Vec<_>>(),
            vec![(Rule::LookupWriteWithCall, "function fn::m")],
            "{found:?}"
        );
    }

    #[test]
    fn flags_lookups_only_a_composite_index_covers() {
        let src = "DEFINE INDEX d ON TABLE doc FIELDS doc, field UNIQUE;\n\
            DEFINE INDEX OVERWRITE s ON sig COLUMNS sender, at;\n\
            DEFINE INDEX r ON sig FIELDS recipient;\n\
            DEFINE EVENT e ON thread WHEN $event = 'DELETE' THEN { LET $doc = $before.id; \
            DELETE (SELECT VALUE id FROM doc WHERE doc = $doc); \
            LET $a = (SELECT VALUE id FROM sig WHERE sender = $doc AND at < time::now()); \
            LET $b = (SELECT VALUE id FROM sig WHERE recipient = $doc AND sender = $doc); \
            LET $c = (SELECT VALUE id FROM doc WHERE doc = thread:x); };";
        let found = lint(src);
        assert_eq!(
            found.iter().map(|f| f.rule).collect::<Vec<_>>(),
            vec![Rule::CompositeIndex, Rule::CompositeIndex],
            "{found:?}"
        );
        assert!(
            found[0]
                .fix
                .starts_with("DEFINE INDEX idx_doc_doc ON TABLE doc COLUMNS doc;"),
            "{}",
            found[0].fix
        );
        assert!(
            found[1]
                .fix
                .starts_with("DEFINE INDEX idx_sig_sender ON TABLE sig COLUMNS sender;"),
            "{}",
            found[1].fix
        );
    }

    fn rules(src: &str) -> Vec<Rule> {
        lint(src).into_iter().map(|f| f.rule).collect()
    }

    #[test]
    fn flags_where_on_event_values() {
        let src = "DEFINE EVENT e ON game WHEN $event = 'UPDATE' THEN { \
            LET $n = (SELECT VALUE count() FROM game WHERE database = $after.database GROUP ALL)[0]; };";
        assert_eq!(rules(src), vec![Rule::WhereOnEventValue]);
        let f = &lint(src)[0];
        assert!(
            f.fix.contains("LET $after_database = $after.database;"),
            "{}",
            f.fix
        );
        assert_eq!(f.owner, "event e ON game");
    }

    #[test]
    fn let_bound_values_pass() {
        let src = "DEFINE EVENT e ON game THEN { LET $db = $after.database; \
            LET $n = (SELECT VALUE count() FROM game WHERE database = $db GROUP ALL)[0]; \
            IF $n > 0 { FOR $g IN [$db] { LET $m = (SELECT VALUE id FROM game WHERE database = $g); }; }; };";
        assert!(lint(src).is_empty(), "{:?}", lint(src));
    }

    #[test]
    fn flags_update_and_delete_where_even_when_bound() {
        let src = "DEFINE EVENT game_children_cleanup ON game WHEN $event = 'DELETE' THEN { \
            DELETE analysis WHERE game = $before.id; \
            LET $db = $before.database; UPDATE statistics SET analyzed_count = 0 WHERE database = $db; };";
        let found = lint(src);
        assert_eq!(
            found.iter().map(|f| f.rule).collect::<Vec<_>>(),
            vec![Rule::WriteWhere, Rule::WriteWhere]
        );
        assert!(
            found[0].fix.starts_with(
                "LET $ids = (SELECT VALUE id FROM analysis WHERE game = $before.id); DELETE $ids;"
            ),
            "{}",
            found[0].fix
        );
        assert!(found[0].fix.contains("bound by a `LET`"));
        assert!(found[1].fix.starts_with(
            "LET $ids = (SELECT VALUE id FROM statistics WHERE database = $db); UPDATE $ids SET"
        ));
    }

    #[test]
    fn subquery_and_key_writes_pass() {
        let src = "DEFINE FUNCTION fn::x($k: string) { LET $key = $k; \
            DELETE (SELECT VALUE id FROM t WHERE f = $key); \
            UPDATE (SELECT VALUE id FROM t WHERE f = $key) SET g = 1; \
            UPDATE type::record('t', $key) SET g = 1; DELETE $key; UPSERT t:abc SET g = 1; \
            RETURN (SELECT * FROM t WHERE f = $key); };";
        assert!(lint(src).is_empty(), "{:?}", lint(src));
    }

    #[test]
    fn single_statement_event_bodies_stay_in_their_event() {
        let src = "DEFINE EVENT a ON t WHEN $event = 'CREATE' THEN RELATE ($after.id)->x->($after.game);\n\
            DEFINE EVENT b ON t WHEN $event = 'DELETE' THEN { DELETE y WHERE t = $before.id; };";
        let found = lint(src);
        assert_eq!(found.len(), 1, "{found:?}");
        assert_eq!(found[0].owner, "event b ON t");
    }

    #[test]
    fn flags_select_where_true() {
        let src = "DEFINE TABLE game SCHEMAFULL PERMISSIONS FOR select WHERE true, FOR create, update WHERE owner = $auth.id;\n\
            DEFINE FIELD status ON TABLE job TYPE string PERMISSIONS FOR create, select WHERE true FOR update WHERE false;\n\
            DEFINE TABLE ok SCHEMAFULL PERMISSIONS FOR select FULL, FOR update WHERE true;\n\
            DEFINE FIELD f ON ok TYPE string PERMISSIONS FOR select WHERE $auth.id != NONE;";
        let found = lint(src);
        assert_eq!(
            found.iter().map(|f| f.owner.as_str()).collect::<Vec<_>>(),
            vec!["table game", "field status ON job"],
            "{found:?}"
        );
        assert!(found.iter().all(|f| f.rule == Rule::SelectWhereTrue));
        assert!(
            found[1].fix.starts_with("FOR create, select FULL"),
            "{}",
            found[1].fix
        );
    }

    #[test]
    fn flags_inside_param() {
        let src = "DEFINE FUNCTION fn::x($ids: array) { LET $i = $ids; DELETE (SELECT VALUE id FROM t WHERE f INSIDE $i); };";
        assert_eq!(rules(src), vec![Rule::InsideParam]);
    }

    #[test]
    fn literal_wheres_and_comments_pass() {
        let src = "-- DEFINE EVENT x ON t THEN { DELETE t WHERE f = $after.f };\n\
            DEFINE EVENT e ON t THEN { DELETE sink WHERE kind = 'stale'; \
            LET $s = 'DELETE t WHERE f = $after.f'; };";
        assert!(lint(src).is_empty(), "{:?}", lint(src));
    }

    /// Everything sp00ky itself deploys: the server schema of an app with a
    /// `@crdt` and a `@channel` table (meta tables, generated tables and their
    /// events, bucket and remote functions), the per-table ingest events, and
    /// the templates applied beside them. None of it may filter a write or a
    /// read in a shape SurrealDB 4.0.1 scans.
    #[test]
    fn sp00kys_own_schema_passes() {
        use crate::backend::DeployMode;
        use crate::schema_builder::{
            build_server_events, build_server_schema, SchemaBuilderConfig,
        };
        let app = "DEFINE TABLE user SCHEMAFULL PERMISSIONS FOR select FULL;\n\
            DEFINE FIELD name ON TABLE user TYPE string;\n\
            DEFINE TABLE thread SCHEMAFULL PERMISSIONS FULL;\n\
            DEFINE FIELD title ON TABLE thread TYPE string;\n\
            DEFINE FIELD summary ON TABLE thread TYPE option<string>; -- @crdt text\n\
            -- @channel\n\
            DEFINE TABLE call SCHEMAFULL PERMISSIONS FULL;\n\
            DEFINE FIELD caller ON TABLE call TYPE record<user>;\n";
        let dir = tempfile::tempdir().unwrap();
        let input = dir.path().join("schema.surql");
        std::fs::write(&input, app).unwrap();
        let config = dir.path().join("sp00ky.yml");
        std::fs::write(&config, "sync:\n  transport: changefeed\n").unwrap();
        let mut sql = String::new();
        for mode in [DeployMode::Cluster, DeployMode::Singlenode] {
            let builder = SchemaBuilderConfig {
                input_path: input.clone(),
                config_path: Some(config.clone()),
                mode,
                endpoint: Some("http://scheduler:9667".into()),
                secret: Some("secret".into()),
                include_functions: true,
            };
            sql.push_str(&build_server_schema(&builder).expect("server schema"));
            sql.push_str(&build_server_events(&builder).expect("server events"));
        }
        for template in [
            include_str!("meta_tables_remote.surql"),
            include_str!("functions_remote.surql"),
            include_str!("functions_remote_singlenode.surql"),
            include_str!("impersonation_remote.surql"),
            include_str!("push_tables.surql"),
            include_str!("schedule_tables.surql"),
            include_str!("pool_tables.surql"),
        ] {
            sql.push_str(template);
        }
        assert!(
            sql.contains("_00_crdt_cleanup") && sql.contains("_00_channel_signal_sweep"),
            "the generated events are covered"
        );
        assert!(
            sql.contains("fn::feature::assign") && sql.contains("fn::push::update"),
            "the functions are covered"
        );
        let found = lint(&sql);
        assert!(
            found.is_empty(),
            "{}",
            found
                .iter()
                .map(|f| f.to_string())
                .collect::<Vec<_>>()
                .join("\n")
        );
    }

    #[test]
    fn upsert_where_is_flagged() {
        let src = "DEFINE FUNCTION fn::feature::assign($user: record, $key: string) { \
            UPSERT _00_user_feature SET variant = 'on' WHERE user = $user AND key = $key; };";
        let f = lint(src);
        assert_eq!(f.len(), 1);
        assert_eq!(f[0].rule, Rule::WriteWhere);
        assert_eq!(f[0].owner, "function fn::feature::assign");
    }
}
