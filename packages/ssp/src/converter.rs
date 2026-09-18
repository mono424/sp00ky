use anyhow::{anyhow, Result};
use nom::{
    branch::alt,
    bytes::complete::{is_not, tag, tag_no_case, take_while},
    character::complete::{alpha1, char, digit1, multispace0},
    combinator::{cut, map, map_res, opt, recognize, value},
    multi::separated_list1,
    sequence::{delimited, pair, preceded, tuple},
    IResult,
};
use serde_json::{json, Value};
use std::collections::HashMap;

/// Per-table record-link field map: `table -> (field -> target_table)`.
///
/// Populated at bootstrap from `INFO FOR TABLE` (`DEFINE FIELD ... TYPE
/// record<X>` / `option<record<X>>`). Lets the converter lower a multi-hop
/// link-traversal equality (`assigned_to.owner.id = $auth.id`) into a
/// `SemiJoin` against the linked table, because the target table name is NOT
/// derivable from the field name (`assigned_to` links to `connection`). Without
/// it, such a predicate stays a flat `Filter` whose path can't be dereferenced
/// across rows, so the view matches zero rows (see `lower_link_traversals`).
pub type LinkMap = HashMap<String, HashMap<String, String>>;

/// Convert with no link schema (link-traversal predicates stay flat). Kept for
/// call sites / tests that don't need permission link lowering.
pub fn convert_surql_to_dbsp(sql: &str) -> Result<Value> {
    convert_surql_to_dbsp_with_links(sql, &LinkMap::new())
}

/// Convert `sql` to a DBSP plan, then lower any record-link-traversal equality
/// predicate (`link.….field = value`) into a `SemiJoin` using `links` to resolve
/// each link's target table. See [`lower_link_traversals`].
pub fn convert_surql_to_dbsp_with_links(sql: &str, links: &LinkMap) -> Result<Value> {
    let clean_sql = sql.trim().trim_end_matches(';');
    match parse_full_query(clean_sql) {
        Ok((_, mut plan)) => {
            lower_link_traversals(&mut plan, links);
            Ok(plan)
        }
        Err(e) => Err(anyhow!("SQL Parsing Error: {}", e)),
    }
}

// --- HELPERS ---

fn ws<'a, F, O, E: nom::error::ParseError<&'a str>>(
    inner: F,
) -> impl FnMut(&'a str) -> IResult<&'a str, O, E>
where
    F: FnMut(&'a str) -> IResult<&'a str, O, E>,
{
    delimited(multispace0, inner, multispace0)
}

// Identifier: Start with Alpha/_, then Alphanumeric/_/:/.
fn parse_identifier(input: &str) -> IResult<&str, String> {
    let parser = recognize(pair(
        alt((alpha1, tag("_"))),
        take_while(|c: char| c.is_alphanumeric() || c == '_' || c == ':' || c == '.'),
    ));
    map(parser, |s: &str| s.to_string())(input)
}

// --- VALUES ---

#[derive(Debug, Clone)]
enum ParsedValue {
    Json(Value),
    Identifier(String),
    Prefix(String),
}

fn parse_string_literal(input: &str) -> IResult<&str, ParsedValue> {
    let parse_content = |delimiter| {
        delimited(
            char(delimiter),
            is_not(if delimiter == '\'' { "'" } else { "\"" }),
            char(delimiter),
        )
    };
    map(alt((parse_content('\''), parse_content('"'))), |s: &str| {
        if s.ends_with('*') {
            ParsedValue::Prefix(s.trim_end_matches('*').to_string())
        } else {
            ParsedValue::Json(json!(s))
        }
    })(input)
}

fn parse_value_entry(input: &str) -> IResult<&str, ParsedValue> {
    alt((
        parse_string_literal,
        map(preceded(char('$'), parse_identifier), |s| {
            ParsedValue::Json(json!({ "$param": s }))
        }),
        value(ParsedValue::Json(json!(true)), tag_no_case("true")),
        value(ParsedValue::Json(json!(false)), tag_no_case("false")),
        // Numbers before Identifiers!
        map_res(digit1, |s: &str| {
            s.parse::<i64>().map(|n| ParsedValue::Json(json!(n)))
        }),
        map(parse_identifier, ParsedValue::Identifier),
    ))(input)
}

// --- LOGIC ---

fn parse_cmp_op(input: &str) -> IResult<&str, &str> {
    alt((
        tag(">="),
        tag("<="),
        tag("!="),
        tag("="),
        tag(">"),
        tag("<"),
        tag_no_case("CONTAINS"),
        tag_no_case("INSIDE"),
    ))(input)
}

/// Comparison operators accepted with a param on the LHS.
///
/// Separate from [`parse_cmp_op`] so membership can be accepted here without
/// also reaching the field-LHS leaf, where `field IN [...]` has different
/// semantics and would silently lower to an equality. `INSIDE` is tried before
/// `IN` because it is the longer keyword and shares its prefix.
fn parse_param_cmp_op(input: &str) -> IResult<&str, &str> {
    alt((
        tag(">="),
        tag("<="),
        tag("!="),
        tag("="),
        tag(">"),
        tag("<"),
        tag_no_case("INSIDE"),
        tag_no_case("IN"),
    ))(input)
}

/// Parse a bracketed list of string literals: `['account', '_00_impersonate']`.
///
/// Only reachable on the RHS of a `$param INSIDE [...]` membership test. Kept
/// out of [`parse_value_entry`] so an array literal cannot appear anywhere the
/// rest of the converter does not already handle one.
fn parse_string_array(input: &str) -> IResult<&str, Vec<Value>> {
    let (input, _) = ws(char('['))(input)?;
    let (input, items) = separated_list1(ws(char(',')), ws(parse_string_literal))(input)?;
    let (input, _) = ws(char(']'))(input)?;
    let mut values = Vec::with_capacity(items.len());
    for item in items {
        match item {
            ParsedValue::Json(v) => values.push(v),
            // A `*`-suffixed member would be a prefix match, which membership
            // does not mean. Fail the branch rather than silently widen it.
            _ => {
                return Err(nom::Err::Error(nom::error::Error::new(
                    input,
                    nom::error::ErrorKind::Tag,
                )))
            }
        }
    }
    Ok((input, values))
}

/// Parse `$param OP value` — comparison with the param on the LHS. Returns
/// a `paramcmp`-flavored predicate JSON ({ "type": "parameq", "param", "value" }).
/// CONTAINS is not supported with a param-LHS.
fn parse_leaf_param_lhs(input: &str) -> IResult<&str, Value> {
    let (input, _) = ws(char('$'))(input)?;
    let (input, param) = parse_identifier(input)?;
    let (input, op) = ws(parse_param_cmp_op)(input)?;

    // `$access INSIDE ['account', '_00_impersonate']` is how SurrealDB spells
    // "this access method is one of these", and it is what every account-scoped
    // permission became when impersonation shipped. Desugar it to an OR of
    // equalities: the plan stays a flat Filter, which is what
    // `permission_inject` accepts, and no new predicate type has to be taught
    // to the operators downstream.
    //
    // Without this the whole permission failed to parse and the SSP rejected
    // the view outright, which takes every live query on that table down at
    // once, for every client.
    if op.eq_ignore_ascii_case("INSIDE") || op.eq_ignore_ascii_case("IN") {
        let (input, values) = ws(parse_string_array)(input)?;
        let predicates: Vec<Value> = values
            .into_iter()
            .map(|value| json!({ "type": "parameq", "param": param, "value": value }))
            .collect();
        // One member is just an equality; wrapping it in an OR would only cost
        // the reader. Mirrors `parse_or_expression`.
        return Ok((
            input,
            if predicates.len() == 1 {
                predicates[0].clone()
            } else {
                json!({ "type": "or", "predicates": predicates })
            },
        ));
    }

    let (input, right) = ws(parse_value_entry)(input)?;

    let type_str = match op.to_uppercase().as_str() {
        "=" => "parameq",
        "!=" => "paramneq",
        ">" => "paramgt",
        ">=" => "paramgte",
        "<" => "paramlt",
        "<=" => "paramlte",
        _ => {
            // CONTAINS with a param-LHS isn't supported; fail this alt branch so
            // the outer parser can try the next alternative.
            return Err(nom::Err::Error(nom::error::Error::new(
                input,
                nom::error::ErrorKind::Tag,
            )));
        }
    };

    let val = match right {
        ParsedValue::Json(v) => v,
        // identifier/prefix on the RHS of a $param-LHS comparison isn't supported
        // in v1; bail.
        _ => {
            return Err(nom::Err::Error(nom::error::Error::new(
                input,
                nom::error::ErrorKind::Tag,
            )));
        }
    };

    Ok((
        input,
        json!({ "type": type_str, "param": param, "value": val }),
    ))
}

/// Parse `field OP value` — the historical leaf shape with a path-identifier
/// on the LHS. Kept verbatim apart from the op-tag extraction.
fn parse_leaf_field_lhs(input: &str) -> IResult<&str, Value> {
    let (input, (left, op, right)) = tuple((
        ws(parse_identifier),
        ws(parse_cmp_op),
        ws(parse_value_entry),
    ))(input)?;

    let type_str = match op.to_uppercase().as_str() {
        "=" => "eq",
        ">" => "gt",
        "<" => "lt",
        ">=" => "gte",
        "<=" => "lte",
        "!=" => "neq",
        _ => "eq",
    };

    match right {
        ParsedValue::Json(val) => Ok((
            input,
            json!({ "type": type_str, "field": left, "value": val }),
        )),
        ParsedValue::Prefix(val) => Ok((
            input,
            json!({ "type": "prefix", "field": left, "prefix": val }),
        )),
        ParsedValue::Identifier(right_field) => Ok((
            input,
            json!({ "type": "__JOIN_CANDIDATE__", "left": left, "right": right_field }),
        )),
    }
}

fn parse_leaf_predicate(input: &str) -> IResult<&str, Value> {
    // Try param-LHS first (it's distinguishable by the leading `$`); fall back
    // to the historical field-LHS shape.
    alt((parse_leaf_param_lhs, parse_leaf_field_lhs))(input)
}

// Recursive Expression Parser
// Logic: Or -> And -> Term (Leaf or Parens)

fn parse_term(input: &str) -> IResult<&str, Value> {
    alt((
        // `<path> IN (SELECT VALUE <col|link.field> FROM <table> [WHERE ...])`
        // must be tried before the parenthesised-expression and leaf branches:
        // it starts with an identifier like the leaf, but the `IN (SELECT ...`
        // tail is what distinguishes it. On any mismatch it backtracks cleanly.
        parse_in_subquery_leaf,
        delimited(ws(char('(')), parse_or_expression, ws(char(')'))),
        parse_leaf_predicate,
    ))(input)
}

/// Parse `<path> IN ( SELECT VALUE <proj> FROM <table> [WHERE <pred>] )`.
///
/// Emits an `in_subquery` marker node (not a `Predicate` — it lowers to a
/// `SemiJoin`, an operator, in [`lower_where_to_plan`]). `proj` is either a bare
/// column (single-hop: `owner`) or a `<link>.<field>` record-link traversal
/// (two-hop: `broadcast.owner`), handled in [`semijoin_for_subquery`].
fn parse_in_subquery_leaf(input: &str) -> IResult<&str, Value> {
    let (input, left) = ws(parse_identifier)(input)?;
    // SurrealDB canonicalises `IN` to `INSIDE` in stored permission text, so
    // accept both. INSIDE is tried first: it is the longer keyword, and matching
    // `IN` first would leave a dangling `SIDE` and fail the `(`.
    let (input, _) = ws(alt((tag_no_case("INSIDE"), tag_no_case("IN"))))(input)?;
    let (input, _) = ws(char('('))(input)?;
    let (input, _) = ws(tag_no_case("SELECT"))(input)?;
    let (input, _) = ws(tag_no_case("VALUE"))(input)?;
    let (input, proj) = ws(parse_identifier)(input)?;
    let (input, _) = ws(tag_no_case("FROM"))(input)?;
    let (input, table) = ws(parse_identifier)(input)?;
    let (input, where_logic) = opt(ws(parse_where_logic))(input)?;
    let (input, _) = ws(char(')'))(input)?;
    Ok((
        input,
        json!({
            "type": "in_subquery",
            "left": left,
            "proj": proj,
            "table": table,
            "where": where_logic,
        }),
    ))
}

fn parse_and_expression(input: &str) -> IResult<&str, Value> {
    let (input, terms) = separated_list1(ws(tag_no_case("AND")), parse_term)(input)?;
    if terms.len() == 1 {
        Ok((input, terms[0].clone()))
    } else {
        Ok((input, json!({ "type": "and", "predicates": terms })))
    }
}

fn parse_or_expression(input: &str) -> IResult<&str, Value> {
    let (input, terms) = separated_list1(ws(tag_no_case("OR")), parse_and_expression)(input)?;
    if terms.len() == 1 {
        Ok((input, terms[0].clone()))
    } else {
        Ok((input, json!({ "type": "or", "predicates": terms })))
    }
}

fn parse_where_logic(input: &str) -> IResult<&str, Value> {
    preceded(tag_no_case("WHERE"), cut(parse_or_expression))(input)
}

// --- MAIN QUERY ---

fn parse_limit_clause(input: &str) -> IResult<&str, usize> {
    preceded(
        tag_no_case("LIMIT"),
        ws(map_res(digit1, |s: &str| s.parse::<usize>())),
    )(input)
}

fn parse_start_clause(input: &str) -> IResult<&str, usize> {
    preceded(
        tag_no_case("START"),
        ws(map_res(digit1, |s: &str| s.parse::<usize>())),
    )(input)
}

fn parse_order_clause(input: &str) -> IResult<&str, Vec<Value>> {
    let single_order = map(
        tuple((
            ws(parse_identifier),
            opt(ws(alt((tag_no_case("ASC"), tag_no_case("DESC"))))),
        )),
        |(field, dir)| json!({ "field": field, "direction": dir.unwrap_or("ASC").to_uppercase() }),
    );
    preceded(
        tag_no_case("ORDER BY"),
        separated_list1(ws(char(',')), single_order),
    )(input)
}

// --- SELECT PROJECTION ---

/// Walk a predicate JSON tree to find $parent.* references.
/// Returns (child_field, parent_field) if found.
fn extract_parent_key_from_predicate(predicate: &Value) -> Option<(String, String)> {
    let obj = predicate.as_object()?;
    let pred_type = obj.get("type")?.as_str()?;

    match pred_type {
        "and" | "or" => {
            // Recurse into nested predicates, return first match
            let preds = obj.get("predicates")?.as_array()?;
            for p in preds {
                if let Some(result) = extract_parent_key_from_predicate(p) {
                    return Some(result);
                }
            }
            None
        }
        // Leaf predicate: check if value is { "$param": "parent.*" }
        _ => {
            let child_field = obj.get("field")?.as_str()?;
            let value = obj.get("value")?;
            let param = value.get("$param")?.as_str()?;
            let parent_field = param.strip_prefix("parent.")?;
            Some((child_field.to_string(), parent_field.to_string()))
        }
    }
}

/// Walk a plan JSON tree to find the filter predicate and extract $parent.* references.
fn extract_parent_key(plan: &Value) -> Option<Value> {
    let obj = plan.as_object()?;
    let op = obj.get("op")?.as_str()?;

    match op {
        "filter" => {
            let predicate = obj.get("predicate")?;
            let (child_field, parent_field) = extract_parent_key_from_predicate(predicate)?;
            Some(json!({ "child_field": child_field, "parent_field": parent_field }))
        }
        "limit" | "project" => {
            let input = obj.get("input")?;
            extract_parent_key(input)
        }
        _ => None,
    }
}

fn parse_subquery_projection(input: &str) -> IResult<&str, Value> {
    // (SELECT ... ) [optional_index] AS alias
    let (input, sub_plan) = delimited(ws(char('(')), parse_full_query, ws(char(')')))(input)?;

    // Parse optional array index like [0]
    let (input, _index) = opt(ws(delimited(
        char('['),
        map_res(digit1, |s: &str| s.parse::<usize>()),
        char(']'),
    )))(input)?;

    let (input, _) = ws(tag_no_case("AS"))(input)?;
    let (input, alias) = ws(parse_identifier)(input)?;

    let mut result = json!({ "type": "subquery", "alias": alias, "plan": sub_plan });
    if let Some(parent_key) = extract_parent_key(&sub_plan) {
        result
            .as_object_mut()
            .unwrap()
            .insert("parent_key".to_string(), parent_key);
    }

    Ok((input, result))
}

fn parse_field_projection(input: &str) -> IResult<&str, Value> {
    // field OR field AS alias (though we usually just use field name)
    // keeping it simple: just identifier for now, or *
    alt((
        map(tag("*"), |_| json!({ "type": "all" })),
        map(parse_identifier, |f| json!({ "type": "field", "name": f })),
    ))(input)
}

fn parse_projection_item(input: &str) -> IResult<&str, Value> {
    alt((parse_subquery_projection, parse_field_projection))(input)
}

fn parse_full_query(input: &str) -> IResult<&str, Value> {
    let (input, _) = ws(tag_no_case("SELECT"))(input)?;

    let (input, fields) = separated_list1(ws(char(',')), parse_projection_item)(input)?;

    let (input, _) = ws(tag_no_case("FROM"))(input)?;
    let (input, table) = ws(parse_identifier)(input)?;

    let (input, where_logic) = opt(ws(parse_where_logic))(input)?;

    let (input, order_by) = opt(ws(parse_order_clause))(input)?;
    let (input, limit) = opt(ws(parse_limit_clause))(input)?;
    // SurrealDB allows `LIMIT n START m`; offset is meaningless without a limit
    // window, so it's only consumed alongside `limit` in the tree-building step.
    let (input, start) = opt(ws(parse_start_clause))(input)?;

    // --- TREE BUILDING ---
    let mut current_op = json!({ "op": "scan", "table": table });

    if let Some(logic) = where_logic {
        if where_contains_subquery(&logic) {
            // A WHERE with an `IN (subquery)` term can't be a flat Filter — it
            // lowers to SemiJoin / Union / Distinct operators.
            current_op = lower_where_to_plan(&current_op, &logic);
        } else {
            current_op = wrap_conditions(current_op, logic);
        }
    }

    // Projections
    // If we have just one "type": "all", and nothing else, we skip projection technically
    // But let's be explicit if desired.
    // If fields contains any subquery or if fields is not just "*", we project.
    let needs_projection =
        fields.len() > 1 || fields[0].get("type").and_then(|t| t.as_str()) != Some("all");

    if needs_projection {
        current_op = json!({ "op": "project", "projections": fields, "input": current_op });
    }

    if let Some(l) = limit {
        let mut limit_op =
            json!({ "op": "limit", "limit": l, "start": start.unwrap_or(0), "input": current_op });
        if let Some(orders) = order_by {
            limit_op
                .as_object_mut()
                .unwrap()
                .insert("order_by".to_string(), json!(orders));
        }
        current_op = limit_op;
    }

    Ok((input, current_op))
}

/// True if `expr` contains an `in_subquery` marker anywhere in its AND/OR tree.
fn where_contains_subquery(expr: &Value) -> bool {
    match expr.get("type").and_then(|t| t.as_str()) {
        Some("in_subquery") => true,
        Some("and") | Some("or") => expr
            .get("predicates")
            .and_then(|p| p.as_array())
            .map(|list| list.iter().any(where_contains_subquery))
            .unwrap_or(false),
        _ => false,
    }
}

/// Lower a WHERE expression that contains `IN (subquery)` term(s) into an
/// operator plan (JSON) over `scan` that yields the outer table's allowed keys.
///
/// - `OR`  → `Distinct(Union(branch_plans...))` (Union merges Z-sets over the
///   same outer scan; Distinct clamps duplicate keys to weight 1).
/// - `AND` → apply the pure-predicate conjuncts as one `Filter` on the outer
///   scan, then thread each `IN (subquery)` conjunct as a `SemiJoin` on top.
/// - a bare `in_subquery` → a single `SemiJoin` (see [`semijoin_for_subquery`]).
/// - anything else (a plain predicate) → `Filter(scan, predicate)`.
fn lower_where_to_plan(scan: &Value, expr: &Value) -> Value {
    match expr.get("type").and_then(|t| t.as_str()) {
        Some("or") => {
            let branches = expr
                .get("predicates")
                .and_then(|p| p.as_array())
                .cloned()
                .unwrap_or_default();
            let mut plans = branches.iter().map(|b| lower_where_to_plan(scan, b));
            let first = plans.next().unwrap_or_else(|| scan.clone());
            let unioned = plans.fold(first, |acc, p| {
                json!({ "op": "union", "left": acc, "right": p })
            });
            json!({ "op": "distinct", "input": unioned })
        }
        Some("and") => {
            let list = expr
                .get("predicates")
                .and_then(|p| p.as_array())
                .cloned()
                .unwrap_or_default();
            let (subs, preds): (Vec<Value>, Vec<Value>) =
                list.into_iter().partition(where_contains_subquery);
            let mut base = scan.clone();
            if !preds.is_empty() {
                let pred = if preds.len() == 1 {
                    preds[0].clone()
                } else {
                    json!({ "type": "and", "predicates": preds })
                };
                base = json!({ "op": "filter", "predicate": pred, "input": base });
            }
            for sub in subs {
                // `where_contains_subquery` is recursive, so a conjunct here is
                // not necessarily an `in_subquery` LEAF — it can be a nested
                // and/or that merely contains one. Handing such a node to
                // `semijoin_for_subquery` read `table`/`left`/`proj` off a node
                // that has none, yielding `Scan{table: ""}` and silently
                // dropping the rest of the branch: whitepawn's
                // `owner = $auth.id OR database INSIDE (…)` permission admitted
                // NOTHING, so a table with 70 readable rows materialised empty.
                base = if sub.get("type").and_then(|t| t.as_str()) == Some("in_subquery") {
                    semijoin_for_subquery(&base, &sub)
                } else {
                    lower_where_to_plan(&base, &sub)
                };
            }
            base
        }
        Some("in_subquery") => semijoin_for_subquery(scan, expr),
        _ => json!({ "op": "filter", "predicate": expr.clone(), "input": scan.clone() }),
    }
}

/// Build the `SemiJoin` plan for one `IN (subquery)` term whose left is `base`.
///
/// Single-hop (`proj = "owner"`):
///   `SemiJoin(base, Filter(Scan{table}, where), on: <left> = owner)`
///
/// Two-hop (`proj = "broadcast.owner"`): the projected value lives on the record
/// linked from `table.<link>`, which the runtime can't dereference across rows.
/// So we first resolve the linked rows with an inner semi-join, then match on
/// the target field:
///   inner  = SemiJoin(Scan{link}, Filter(Scan{table}, where), on: id = <link>)
///   result = SemiJoin(base, inner, on: <left> = <field>)
/// The link's target table is taken to be the link field's name (the schema
/// convention here: `broadcast_share.broadcast` is `record<broadcast>`).
fn semijoin_for_subquery(base: &Value, sub: &Value) -> Value {
    let left = sub.get("left").and_then(|v| v.as_str()).unwrap_or("id");
    let proj = sub.get("proj").and_then(|v| v.as_str()).unwrap_or("id");
    let table = sub.get("table").and_then(|v| v.as_str()).unwrap_or("");
    let where_logic = sub.get("where").filter(|w| !w.is_null());

    let mut inner_scan = json!({ "op": "scan", "table": table });
    if let Some(w) = where_logic {
        inner_scan = json!({ "op": "filter", "predicate": w.clone(), "input": inner_scan });
    }

    match proj.split_once('.') {
        Some((link, field)) => {
            let inner = json!({
                "op": "semijoin",
                "left": { "op": "scan", "table": link },
                "right": inner_scan,
                "on": { "left_field": "id", "right_field": link },
            });
            json!({
                "op": "semijoin",
                "left": base.clone(),
                "right": inner,
                "on": { "left_field": left, "right_field": field },
            })
        }
        None => json!({
            "op": "semijoin",
            "left": base.clone(),
            "right": inner_scan,
            "on": { "left_field": left, "right_field": proj },
        }),
    }
}

/// Post-pass over a converted plan (JSON): rewrite any `Filter{Scan{table}}`
/// whose predicate compares a *record-link traversal* path (`link.….field`) into
/// a `SemiJoin` against the linked table, so the cross-row dereference the
/// `Filter` operator can't do becomes a proper DBSP join.
///
/// Why this is needed: the `Filter` operator resolves a field path on a single
/// row. For `assigned_to.owner.id` on a `job` row, `assigned_to` is a record-ref
/// string (`connection:…`); the next segment `owner` can't be read off a string,
/// so the path resolves to NULL and the predicate is always false. Lowering
/// `assigned_to.owner.id = $auth.id` to
/// `SemiJoin(Scan{job}, Filter(Scan{connection}, owner.id = $auth.id),
///          on: assigned_to = id)` makes the deref a join the runtime can do.
///
/// A trailing `.id` on a link (`owner.id`) is left flat — `resolve_field` reads
/// the id straight off the record-ref string, so it needs no join. Only a
/// genuine cross-table hop (a link segment followed by more than `.id`) is
/// lowered. The rewrite recurses into its own output, so multi-hop chains
/// (`a.b.c`, each of `a`,`b` a link) unfold one join per hop.
fn lower_link_traversals(plan: &mut Value, links: &LinkMap) {
    if links.is_empty() {
        return; // no schema → nothing to lower; identical to prior behavior
    }
    let op = plan.get("op").and_then(|v| v.as_str()).map(str::to_string);
    match op.as_deref() {
        Some("filter") => {
            if let Some(input) = plan.get_mut("input") {
                lower_link_traversals(input, links);
            }
            let table = plan
                .get("input")
                .filter(|i| i.get("op").and_then(|o| o.as_str()) == Some("scan"))
                .and_then(|i| i.get("table"))
                .and_then(|t| t.as_str())
                .map(str::to_string);
            if let Some(table) = table {
                if let Some(pred) = plan.get("predicate").cloned() {
                    if predicate_contains_link_leaf(&pred, &table, links) {
                        let scan = plan.get("input").cloned().unwrap();
                        *plan = lower_predicate_to_plan(&scan, &pred, &table, links);
                        // Recurse into the rewritten plan so deeper hops (a link
                        // in the inner Filter's WHERE) also unfold.
                        lower_link_traversals(plan, links);
                    }
                }
            }
        }
        Some("scan") | None => {}
        _ => {
            for key in ["input", "left", "right"] {
                if let Some(child) = plan.get_mut(key) {
                    lower_link_traversals(child, links);
                }
            }
            if let Some(projs) = plan.get_mut("projections").and_then(|p| p.as_array_mut()) {
                for p in projs {
                    if let Some(sub) = p.get_mut("plan") {
                        lower_link_traversals(sub, links);
                    }
                }
            }
        }
    }
}

/// Lower a WHERE predicate over `scan` (a `Scan{table}`) into a plan, treating
/// record-link-traversal `eq` leaves like `IN (subquery)` markers. Mirrors
/// [`lower_where_to_plan`] (OR → Distinct(Union), AND → Filter + SemiJoins) but
/// additionally converts link leaves via [`link_leaf_to_marker`].
fn lower_predicate_to_plan(scan: &Value, expr: &Value, table: &str, links: &LinkMap) -> Value {
    match expr.get("type").and_then(|t| t.as_str()) {
        Some("or") => {
            let branches = expr
                .get("predicates")
                .and_then(|p| p.as_array())
                .cloned()
                .unwrap_or_default();
            let mut plans = branches.iter().map(|b| lower_predicate_to_plan(scan, b, table, links));
            let first = plans.next().unwrap_or_else(|| scan.clone());
            let unioned = plans.fold(first, |acc, p| json!({ "op": "union", "left": acc, "right": p }));
            json!({ "op": "distinct", "input": unioned })
        }
        Some("and") => {
            let list = expr
                .get("predicates")
                .and_then(|p| p.as_array())
                .cloned()
                .unwrap_or_default();
            // Three buckets, not two: leaf markers become SemiJoins, nested
            // and/or nodes that CONTAIN a marker must recurse (folding them into
            // the flat predicate would emit an `in_subquery` marker inside a
            // `Predicate`, which cannot deserialize), and the rest is one Filter.
            let (markers, rest): (Vec<Value>, Vec<Value>) =
                list.into_iter().partition(|p| is_marker_leaf(p, table, links));
            let (nested, preds): (Vec<Value>, Vec<Value>) = rest
                .into_iter()
                .partition(|p| contains_marker(p, table, links));
            let mut base = scan.clone();
            if !preds.is_empty() {
                let pred = if preds.len() == 1 {
                    preds[0].clone()
                } else {
                    json!({ "type": "and", "predicates": preds })
                };
                base = json!({ "op": "filter", "predicate": pred, "input": base });
            }
            for m in markers {
                base = semijoin_for_subquery(&base, &to_marker(&m, table, links));
            }
            for n in nested {
                base = lower_predicate_to_plan(&base, &n, table, links);
            }
            base
        }
        Some("in_subquery") => semijoin_for_subquery(scan, expr),
        _ if is_link_leaf(expr, table, links) => {
            semijoin_for_subquery(scan, &link_leaf_to_marker(expr, table, links))
        }
        _ => json!({ "op": "filter", "predicate": expr.clone(), "input": scan.clone() }),
    }
}

/// True if `expr` (any AND/OR depth) contains a record-link-traversal `eq` leaf.
fn predicate_contains_link_leaf(expr: &Value, table: &str, links: &LinkMap) -> bool {
    match expr.get("type").and_then(|t| t.as_str()) {
        Some("and") | Some("or") => expr
            .get("predicates")
            .and_then(|p| p.as_array())
            .map(|list| list.iter().any(|p| predicate_contains_link_leaf(p, table, links)))
            .unwrap_or(false),
        _ => is_link_leaf(expr, table, links),
    }
}

/// True if `expr` contains a marker leaf anywhere in its and/or tree — the
/// recursive counterpart of [`is_marker_leaf`], used to decide whether a
/// conjunct needs plan lowering rather than flat-predicate treatment.
fn contains_marker(
    expr: &Value,
    table: &str,
    links: &HashMap<String, HashMap<String, String>>,
) -> bool {
    if is_marker_leaf(expr, table, links) {
        return true;
    }
    match expr.get("type").and_then(|t| t.as_str()) {
        Some("and") | Some("or") => expr
            .get("predicates")
            .and_then(|p| p.as_array())
            .map(|list| list.iter().any(|p| contains_marker(p, table, links)))
            .unwrap_or(false),
        _ => false,
    }
}

/// A leaf that must be threaded as a SemiJoin: an existing `in_subquery` marker
/// or a record-link-traversal `eq`.
fn is_marker_leaf(leaf: &Value, table: &str, links: &LinkMap) -> bool {
    leaf.get("type").and_then(|t| t.as_str()) == Some("in_subquery")
        || is_link_leaf(leaf, table, links)
}

fn to_marker(leaf: &Value, table: &str, links: &LinkMap) -> Value {
    if leaf.get("type").and_then(|t| t.as_str()) == Some("in_subquery") {
        leaf.clone()
    } else {
        link_leaf_to_marker(leaf, table, links)
    }
}

/// True iff `leaf` is `<link>.<rest…> = value` where `<link>` is a record link
/// on `table` and `<rest…>` is more than a bare `id` — i.e. a cross-table hop
/// the `Filter` operator cannot dereference. `<link>.id` is excluded: the id is
/// readable off the record-ref string, so it stays a flat filter.
fn is_link_leaf(leaf: &Value, table: &str, links: &LinkMap) -> bool {
    if leaf.get("type").and_then(|t| t.as_str()) != Some("eq") {
        return false;
    }
    let Some(field) = leaf.get("field").and_then(|f| f.as_str()) else {
        return false;
    };
    let segs: Vec<&str> = field.split('.').collect();
    if segs.len() < 2 {
        return false;
    }
    if links.get(table).and_then(|m| m.get(segs[0])).is_none() {
        return false;
    }
    !(segs.len() == 2 && segs[1] == "id")
}

/// Convert a link-traversal `eq` leaf into an `in_subquery` marker so
/// [`semijoin_for_subquery`] can lower it: `link.rest = v` on `table` becomes
/// `link IN (SELECT VALUE id FROM <target> WHERE rest = v)`.
fn link_leaf_to_marker(leaf: &Value, table: &str, links: &LinkMap) -> Value {
    let field = leaf.get("field").and_then(|f| f.as_str()).unwrap_or("");
    let value = leaf.get("value").cloned().unwrap_or(Value::Null);
    // is_link_leaf guarantees a '.' and a resolvable first-segment link.
    let (link, rest) = field.split_once('.').unwrap_or((field, "id"));
    let target = links
        .get(table)
        .and_then(|m| m.get(link))
        .cloned()
        .unwrap_or_default();
    json!({
        "type": "in_subquery",
        "left": link,
        "proj": "id",
        "table": target,
        "where": { "type": "eq", "field": rest, "value": value },
    })
}

fn wrap_conditions(input_op: Value, predicate: Value) -> Value {
    let mut joins = Vec::new();
    let mut filters = Vec::new();

    // 1. Normalize & Partition
    if let Some(obj) = predicate.as_object() {
        if obj.get("type").and_then(|s| s.as_str()) == Some("and") {
            if let Some(list) = obj.get("predicates").and_then(|v| v.as_array()) {
                for p in list {
                    if p.get("type").and_then(|s| s.as_str()) == Some("__JOIN_CANDIDATE__") {
                        joins.push(p.clone());
                    } else {
                        filters.push(p.clone());
                    }
                }
            }
        } else if obj.get("type").and_then(|s| s.as_str()) == Some("__JOIN_CANDIDATE__") {
            joins.push(predicate.clone());
        } else {
            filters.push(predicate.clone());
        }
    } else {
        filters.push(predicate.clone());
    }

    let mut current_op = input_op;

    // 2. Apply Joins (Bottom-Up)
    for join_pred in joins {
        let left_field = join_pred
            .get("left")
            .and_then(|v| v.as_str())
            .unwrap_or("id");
        let right_full = join_pred
            .get("right")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown");

        // Assume right_full is "table.field"
        let parts: Vec<&str> = right_full.split('.').collect();
        let (r_table, r_col) = if parts.len() > 1 {
            (parts[0], parts[1])
        } else {
            (right_full, "id")
        };

        current_op = json!({
            "op": "join",
            "left": current_op,
            "right": { "op": "scan", "table": r_table },
            "on": { "left_field": left_field, "right_field": r_col }
        });
    }

    // 3. Apply Filters
    if !filters.is_empty() {
        let final_pred = if filters.len() == 1 {
            filters[0].clone()
        } else {
            json!({ "type": "and", "predicates": filters })
        };

        current_op = json!({
            "op": "filter",
            "predicate": final_pred,
            "input": current_op
        });
    }

    current_op
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::operator::plan::OperatorPlan as Operator;
    use crate::operator::plan::Projection;

    /// `$access INSIDE [...]` is what every account-scoped permission became
    /// when impersonation shipped. It used to fail to parse, and a permission
    /// that does not parse makes the SSP reject the view registration outright,
    /// which takes every live query on that table down for every client.
    #[test]
    fn param_membership_lowers_to_an_or_of_equalities() {
        let sql = "SELECT * FROM renderer_device \
                   WHERE $access INSIDE ['account', '_00_impersonate'] AND owner = $auth.id";
        let plan = convert_surql_to_dbsp(sql).expect("membership permission should parse");
        let operator: Operator =
            serde_json::from_value(plan).expect("should deserialize to an operator");
        // A flat Filter over a Scan is what `permission_inject` accepts; an OR
        // of `parameq` leaves keeps it flat.
        match operator {
            Operator::Filter { input, .. } => {
                assert!(matches!(*input, Operator::Scan { .. }), "expected a flat filter over a scan");
            }
            other => panic!("expected Filter, got {other:?}"),
        }
    }

    /// The spelling SurrealDB canonicalises to, and the one the impersonation
    /// migration wrote, must both parse.
    #[test]
    fn param_membership_accepts_both_in_and_inside() {
        for op in ["IN", "INSIDE"] {
            let sql = format!(
                "SELECT * FROM presence WHERE $access {op} ['account', '_00_impersonate'] AND owner = $auth.id"
            );
            assert!(
                convert_surql_to_dbsp(&sql).is_ok(),
                "`$access {op} [...]` should parse"
            );
        }
    }

    /// A one-member list is just an equality; it must not grow an OR wrapper.
    #[test]
    fn param_membership_of_one_is_a_plain_equality() {
        let sql = "SELECT * FROM presence WHERE $access INSIDE ['account']";
        let plan = convert_surql_to_dbsp(sql).expect("single-member membership should parse");
        let predicate = plan
            .get("predicate")
            .expect("expected a filter predicate")
            .clone();
        assert_eq!(predicate["type"], "parameq", "got {predicate}");
        assert_eq!(predicate["param"], "access");
        assert_eq!(predicate["value"], "account");
    }

    /// The real `stream_presence` permission: membership AND an owner check,
    /// OR two record-link subqueries. This is the one that took the livestream
    /// board off the web.
    #[test]
    fn stream_presence_permission_parses() {
        let sql = "SELECT * FROM stream_presence WHERE \
                   $access INSIDE ['account', '_00_impersonate'] AND owner = $auth.id \
                   OR owner INSIDE (SELECT VALUE owner FROM broadcast WHERE share_visibility = 'public')";
        assert!(
            convert_surql_to_dbsp(sql).is_ok(),
            "the stream_presence select permission should parse"
        );
    }

    #[test]
    fn test_parse_failing_subquery() {
        let sql = "SELECT *, (SELECT * FROM user WHERE id=$parent.author LIMIT 1)[0] AS author FROM thread ORDER BY created_at desc LIMIT 10;";
        let result = convert_surql_to_dbsp(sql);
        assert!(
            result.is_ok(),
            "Failed to parse subquery: {:?}",
            result.err()
        );
    }

    #[test]
    fn test_subquery_deserializes_to_operator() {
        let sql = "SELECT *, (SELECT * FROM user WHERE id=$parent.author LIMIT 1)[0] AS author FROM thread ORDER BY created_at desc LIMIT 10;";
        let result = convert_surql_to_dbsp(sql).expect("Failed to parse SQL");

        // Try to deserialize the result into an Operator
        let operator: Result<Operator, _> = serde_json::from_value(result);
        assert!(
            operator.is_ok(),
            "Failed to deserialize to Operator: {:?}",
            operator.err()
        );

        // Verify the structure
        let op = operator.unwrap();
        match op {
            Operator::Limit { input, .. } => {
                match *input {
                    Operator::Project { projections, .. } => {
                        assert!(projections.len() > 0, "Expected projections");
                        // Check that we have a subquery projection
                        let has_subquery = projections
                            .iter()
                            .any(|p| matches!(p, Projection::Subquery { .. }));
                        assert!(has_subquery, "Expected at least one subquery projection");
                    }

                    _ => panic!("Expected Project operator inside Limit"),
                }
            }
            _ => panic!("Expected Limit operator at top level"),
        }
    }

    #[test]
    fn limit_start_parses_with_3key_order_and_id() {
        // The solid-app query after adding the `id` tiebreaker. START must still
        // parse to 50 (a 3-key ORDER BY must not swallow the START clause).
        let sql = "SELECT * FROM game WHERE database = $db ORDER BY sort_index asc, date desc, id asc LIMIT 50 START 50;";
        let result = convert_surql_to_dbsp(sql).expect("parse");
        let op: Operator = serde_json::from_value(result).expect("deser");
        match op {
            Operator::Limit { limit, start, order_by, .. } => {
                assert_eq!(limit, 50, "limit");
                assert_eq!(start, 50, "START offset dropped with 3-key ORDER BY");
                assert_eq!(order_by.as_ref().map(|o| o.len()), Some(3), "3 order keys");
            }
            _ => panic!("expected Limit"),
        }
    }

    #[test]
    fn limit_start_parses_into_plan_offset() {
        // The solid-app game-list page query. The `START 30` offset must reach
        // the plan's Limit.start — otherwise every page window collapses to the
        // first rows and infinite scroll can never advance past page 0.
        let sql = "SELECT * FROM game WHERE database = $db ORDER BY sort_index ASC, date DESC LIMIT 30 START 30;";
        let result = convert_surql_to_dbsp(sql).expect("Failed to parse SQL");
        let operator: Operator = serde_json::from_value(result).expect("Failed to deserialize");
        match operator {
            Operator::Limit { limit, start, .. } => {
                assert_eq!(limit, 30);
                assert_eq!(start, 30, "START offset was dropped during conversion");
            }
            _ => panic!("Expected Limit operator at top level"),
        }
    }

    #[test]
    fn test_subquery_extracts_parent_key() {
        let sql = "SELECT *, (SELECT * FROM comment WHERE thread=$parent.id) AS comments FROM thread";
        let result = convert_surql_to_dbsp(sql).expect("Failed to parse SQL");

        let operator: Operator = serde_json::from_value(result).expect("Failed to deserialize");
        match operator {
            Operator::Project { projections, .. } => {
                let subquery = projections.iter().find(|p| matches!(p, Projection::Subquery { .. }));
                assert!(subquery.is_some(), "Expected subquery projection");
                if let Projection::Subquery { alias, parent_key, .. } = subquery.unwrap() {
                    assert_eq!(alias, "comments");
                    assert!(parent_key.is_some(), "Expected parent_key to be extracted");
                    let pk = parent_key.as_ref().unwrap();
                    assert_eq!(pk.child_field, "thread");
                    assert_eq!(pk.parent_field, "id");
                }
            }
            _ => panic!("Expected Project operator at top level"),
        }
    }

    #[test]
    fn test_subquery_extracts_reverse_parent_key() {
        // WHERE id = $parent.author — reverse direction
        let sql = "SELECT *, (SELECT * FROM user WHERE id=$parent.author LIMIT 1)[0] AS author FROM thread";
        let result = convert_surql_to_dbsp(sql).expect("Failed to parse SQL");

        let operator: Operator = serde_json::from_value(result).expect("Failed to deserialize");
        match operator {
            Operator::Project { projections, .. } => {
                let subquery = projections.iter().find(|p| matches!(p, Projection::Subquery { .. }));
                assert!(subquery.is_some(), "Expected subquery projection");
                if let Projection::Subquery { parent_key, .. } = subquery.unwrap() {
                    assert!(parent_key.is_some(), "Expected parent_key");
                    let pk = parent_key.as_ref().unwrap();
                    assert_eq!(pk.child_field, "id");
                    assert_eq!(pk.parent_field, "author");
                }
            }
            _ => panic!("Expected Project operator at top level"),
        }
    }

    // Regression for the production "comments vanish" bug: a reverse
    // one-to-many subquery that is BOTH windowed (ORDER BY + LIMIT) AND carries
    // a nested subquery (comment -> author) must still extract its parent_key.
    // If parent_key comes back None, `compute_current_subquery_set` skips the
    // subquery entirely (circuit.rs) and no `_00_list_ref` edges are written,
    // so comments never sync to clients. Mirrors the real threads query.
    #[test]
    fn test_windowed_reverse_subquery_with_nested_child_extracts_parent_key() {
        let sql = "SELECT *, \
             (SELECT * FROM user WHERE id=$parent.author LIMIT 1)[0] AS author, \
             (SELECT *, (SELECT * FROM user WHERE id=$parent.author LIMIT 1)[0] AS author \
              FROM comment WHERE thread=$parent.id ORDER BY created_at desc LIMIT 10) AS comments \
             FROM thread";
        let result = convert_surql_to_dbsp(sql).expect("Failed to parse SQL");
        let operator: Operator = serde_json::from_value(result).expect("Failed to deserialize");

        let Operator::Project { projections, .. } = operator else {
            panic!("Expected Project operator at top level");
        };
        let comments = projections
            .iter()
            .find_map(|p| match p {
                Projection::Subquery { alias, parent_key, .. } if alias == "comments" => {
                    Some(parent_key.clone())
                }
                _ => None,
            })
            .expect("Expected a `comments` subquery projection");

        let pk = comments.expect(
            "`comments` subquery must have a parent_key — without it the SSP emits no \
             _00_list_ref edges and comments never reach clients (production bug)",
        );
        // reverse one-to-many: child comment.thread = parent thread.id
        assert_eq!(pk.child_field, "thread", "comments correlate on comment.thread");
        assert_eq!(pk.parent_field, "id", "…against the parent thread's id");
    }

    fn link_map(pairs: &[(&str, &str, &str)]) -> LinkMap {
        let mut m = LinkMap::new();
        for (table, field, target) in pairs {
            m.entry(table.to_string())
                .or_default()
                .insert(field.to_string(), target.to_string());
        }
        m
    }

    #[test]
    fn link_traversal_without_map_stays_flat_filter() {
        // No link map → the record-link traversal stays a flat Filter (the
        // buggy shape: the Filter can't dereference `assigned_to` across rows).
        let sql = "SELECT * FROM job WHERE assigned_to.owner.id = $auth.id";
        let plan = convert_surql_to_dbsp(sql).expect("parse");
        assert_eq!(plan.get("op").and_then(|v| v.as_str()), Some("filter"));
        assert_eq!(
            plan.pointer("/input/op").and_then(|v| v.as_str()),
            Some("scan"),
            "flat Filter over Scan"
        );
    }

    #[test]
    fn link_traversal_with_map_lowers_to_semijoin() {
        // With `job.assigned_to -> connection` the traversal lowers to
        // SemiJoin(Scan{job}, Filter(Scan{connection}, owner.id = $auth.id),
        //         on: assigned_to = id).
        let sql = "SELECT * FROM job WHERE assigned_to.owner.id = $auth.id";
        let links = link_map(&[("job", "assigned_to", "connection")]);
        let plan = convert_surql_to_dbsp_with_links(sql, &links).expect("parse");

        assert_eq!(plan.get("op").and_then(|v| v.as_str()), Some("semijoin"));
        assert_eq!(plan.pointer("/left/op").and_then(|v| v.as_str()), Some("scan"));
        assert_eq!(plan.pointer("/left/table").and_then(|v| v.as_str()), Some("job"));
        assert_eq!(plan.pointer("/on/left_field").and_then(|v| v.as_str()), Some("assigned_to"));
        assert_eq!(plan.pointer("/on/right_field").and_then(|v| v.as_str()), Some("id"));
        // right = Filter(Scan{connection}, owner.id = $auth.id)
        assert_eq!(plan.pointer("/right/op").and_then(|v| v.as_str()), Some("filter"));
        assert_eq!(plan.pointer("/right/input/table").and_then(|v| v.as_str()), Some("connection"));
    }

    #[test]
    fn link_traversal_preserves_sibling_conjuncts() {
        // The real permission: `$access = "account" AND assigned_to.owner.id =
        // $auth.id`. The flat conjunct stays a Filter on the job scan; the
        // traversal becomes the SemiJoin on top.
        let sql = "SELECT * FROM job WHERE $access = \"account\" AND assigned_to.owner.id = $auth.id";
        let links = link_map(&[("job", "assigned_to", "connection")]);
        let plan = convert_surql_to_dbsp_with_links(sql, &links).expect("parse");

        assert_eq!(plan.get("op").and_then(|v| v.as_str()), Some("semijoin"));
        // left = Filter(Scan{job}, $access = "account")
        assert_eq!(plan.pointer("/left/op").and_then(|v| v.as_str()), Some("filter"));
        assert_eq!(plan.pointer("/left/input/table").and_then(|v| v.as_str()), Some("job"));
        assert_eq!(plan.pointer("/left/predicate/type").and_then(|v| v.as_str()), Some("parameq"));
    }

    #[test]
    fn single_hop_dot_id_is_not_treated_as_link_traversal() {
        // `owner.id = $auth.id` is single-hop: resolve_field reads the id off the
        // record-ref string directly, so it must stay a flat Filter (no join),
        // even when a link map is present.
        let sql = "SELECT * FROM connection WHERE owner.id = $auth.id";
        let links = link_map(&[("connection", "owner", "user")]);
        let plan = convert_surql_to_dbsp_with_links(sql, &links).expect("parse");
        assert_eq!(plan.get("op").and_then(|v| v.as_str()), Some("filter"));
        assert_eq!(plan.pointer("/input/op").and_then(|v| v.as_str()), Some("scan"));
    }

    #[test]
    fn test_subquery_without_parent_ref_has_no_parent_key() {
        // No $parent reference → no parent_key
        let sql = "SELECT *, (SELECT * FROM comment WHERE active=true) AS comments FROM thread";
        let result = convert_surql_to_dbsp(sql).expect("Failed to parse SQL");

        let operator: Operator = serde_json::from_value(result).expect("Failed to deserialize");
        match operator {
            Operator::Project { projections, .. } => {
                let subquery = projections.iter().find(|p| matches!(p, Projection::Subquery { .. }));
                assert!(subquery.is_some());
                if let Projection::Subquery { parent_key, .. } = subquery.unwrap() {
                    assert!(parent_key.is_none(), "Expected no parent_key when no $parent ref");
                }
            }
            _ => panic!("Expected Project operator at top level"),
        }
    }
}
