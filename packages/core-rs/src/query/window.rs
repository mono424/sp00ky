//! Windowed (`LIMIT n START m`, m > 0) queries.
//!
//! PORT: `packages/spooky_core/lib/src/query/window_query.dart`.
//!
//! Divergence from the TypeScript core, which rewrites the surql into
//! `SELECT <projection> FROM $__win <ORDER BY>` and re-runs it against the
//! local SurrealDB: sqlite cannot run SurrealQL, so the window's id-set is read
//! by id and the parsed `ORDER BY` re-applied with [`crate::utils::sort_rows`].
//! The `SELECT` projection is ignored (full documents are returned), as on the
//! non-windowed read path.

use std::sync::LazyLock;

use regex::Regex;

use crate::utils::OrderByTerm;

/// How to materialize a windowed query's rows.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WindowMaterialization {
    /// The query's top-level `ORDER BY` terms, empty when it has none (then
    /// the id-set's own order is kept).
    pub order_by: Vec<OrderByTerm>,
}

/// Detect a windowed query and extract what materializing it needs (TS
/// `buildWindowMaterialization`).
///
/// `None` for non-offset queries (`START` absent or 0): only those keep the
/// normal id-set path.
///
/// Why windowed queries need this at all: rows are materialized from the
/// circuit's `local_array`, but the local store is shared and, with sparse
/// windowing, holds only some pages. Re-deriving page 2 from whatever is
/// resident locally returns the wrong rows (or none, the "page 2 returns 0
/// rows" bug), so the window is taken from the server's `_00_list_ref`.
pub fn build_window_materialization(surql: &str) -> Option<WindowMaterialization> {
    let clauses = scan_top_level_clauses(surql);
    if clauses.start_value.is_none_or(|v| v == 0) {
        return None;
    }
    clauses.from_index?;
    Some(WindowMaterialization {
        order_by: parse_order_by(surql, &clauses),
    })
}

static ORDER_BY_PREFIX: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)^ORDER\s+BY\s+").unwrap());

/// Slice out the top-level `ORDER BY` clause and parse it into field and
/// direction pairs. It ends at whichever of `LIMIT` / `START` / `;` /
/// end-of-string comes next.
fn parse_order_by(surql: &str, clauses: &TopLevelClauses) -> Vec<OrderByTerm> {
    let Some(start) = clauses.order_by_index else {
        return Vec::new();
    };
    let end = [
        clauses.limit_index,
        clauses.start_index,
        clauses.semicolon_index,
        Some(surql.len()),
    ]
    .into_iter()
    .flatten()
    .filter(|&n| n > start)
    .min()
    .unwrap_or(surql.len());
    let clause = &surql[start..end];
    let body = ORDER_BY_PREFIX.replace(clause, "");
    let mut terms = Vec::new();
    for raw in body.split(',') {
        let mut parts = raw.split_whitespace();
        let Some(field) = parts.next() else { continue };
        // SurrealQL allows collation modifiers (COLLATE / NUMERIC) between the
        // field and the direction; the direction is whichever DESC appears.
        let desc = parts.any(|p| p.eq_ignore_ascii_case("DESC"));
        terms.push((
            field.to_string(),
            if desc { "desc" } else { "asc" }.to_string(),
        ));
    }
    terms
}

#[derive(Default)]
struct TopLevelClauses {
    from_index: Option<usize>,
    order_by_index: Option<usize>,
    limit_index: Option<usize>,
    start_index: Option<usize>,
    start_value: Option<u64>,
    semicolon_index: Option<usize>,
}

fn is_word_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_'
}

/// One pass tracking paren depth and single-quoted strings, so clauses inside
/// subqueries (which live in parens) are ignored: only the outermost clause
/// keywords are recorded (TS `scanTopLevelClauses`). Byte-wise: every keyword
/// is ASCII, so every recorded index is a char boundary.
fn scan_top_level_clauses(sql: &str) -> TopLevelClauses {
    let bytes = sql.as_bytes();
    let mut out = TopLevelClauses::default();
    let mut depth = 0i32;
    let mut in_str = false;
    let mut i = 0;
    while i < bytes.len() {
        let ch = bytes[i];
        if in_str {
            if ch == b'\\' {
                i += 1; // skip the escaped char
            } else if ch == b'\'' {
                in_str = false;
            }
            i += 1;
            continue;
        }
        match ch {
            b'\'' => in_str = true,
            b'(' => depth += 1,
            b')' => depth -= 1,
            _ if depth != 0 => {}
            b';' if out.semicolon_index.is_none() => out.semicolon_index = Some(i),
            _ if i > 0 && is_word_byte(bytes[i - 1]) => {}
            _ if out.from_index.is_none() => {
                if match_keyword(bytes, i, "FROM") {
                    out.from_index = Some(i);
                }
            }
            // FROM comes before the trailing clauses; only record them once seen.
            _ => {
                if out.order_by_index.is_none() && match_keyword(bytes, i, "ORDER BY") {
                    out.order_by_index = Some(i);
                } else if out.limit_index.is_none() && match_keyword(bytes, i, "LIMIT") {
                    out.limit_index = Some(i);
                } else if out.start_index.is_none() && match_keyword(bytes, i, "START") {
                    out.start_index = Some(i);
                    out.start_value = read_number_after(bytes, i + "START".len());
                }
            }
        }
        i += 1;
    }
    out
}

/// Case-insensitive keyword match where internal whitespace (`ORDER BY`)
/// matches any run of whitespace, and the keyword ends on a non-word char.
fn match_keyword(sql: &[u8], i: usize, keyword: &str) -> bool {
    let words: Vec<&str> = keyword.split(' ').collect();
    let mut pos = i;
    for (n, word) in words.iter().enumerate() {
        let end = pos + word.len();
        if end > sql.len() || !sql[pos..end].eq_ignore_ascii_case(word.as_bytes()) {
            return false;
        }
        pos = end;
        if n + 1 < words.len() {
            let ws_start = pos;
            while pos < sql.len() && sql[pos].is_ascii_whitespace() {
                pos += 1;
            }
            if pos == ws_start {
                return false; // required whitespace
            }
        }
    }
    pos >= sql.len() || !is_word_byte(sql[pos])
}

fn read_number_after(sql: &[u8], from: usize) -> Option<u64> {
    let mut pos = from;
    while pos < sql.len() && sql[pos].is_ascii_whitespace() {
        pos += 1;
    }
    let digits = sql[pos..].iter().take_while(|b| b.is_ascii_digit()).count();
    if digits == 0 {
        return None;
    }
    std::str::from_utf8(&sql[pos..pos + digits])
        .ok()?
        .parse()
        .ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn order(surql: &str) -> Vec<(String, String)> {
        build_window_materialization(surql)
            .expect("a window")
            .order_by
    }

    fn terms(t: &[(&str, &str)]) -> Vec<(String, String)> {
        t.iter()
            .map(|(f, d)| (f.to_string(), d.to_string()))
            .collect()
    }

    #[test]
    fn detects_an_offset_window_and_keeps_its_order_by() {
        assert_eq!(
            order("SELECT * FROM game WHERE database = $database ORDER BY sort_index asc, date desc LIMIT 30 START 30;"),
            terms(&[("sort_index", "asc"), ("date", "desc")])
        );
    }

    #[test]
    fn offset_free_queries_are_not_windows() {
        assert!(build_window_materialization(
            "SELECT * FROM game WHERE database = $database ORDER BY date desc LIMIT 30 START 0;"
        )
        .is_none());
        assert!(build_window_materialization(
            "SELECT * FROM game WHERE database = $database LIMIT 30;"
        )
        .is_none());
        assert!(build_window_materialization("SELECT * FROM game;").is_none());
        assert!(
            build_window_materialization("RETURN 1 START 5").is_none(),
            "no FROM clause"
        );
    }

    #[test]
    fn order_by_shapes() {
        assert!(order("SELECT * FROM game LIMIT 30 START 30;").is_empty());
        assert_eq!(
            order("SELECT * FROM game ORDER BY date LIMIT 10 START 10"),
            terms(&[("date", "asc")])
        );
        assert_eq!(
            order("SELECT * FROM game START 5 ORDER BY date desc;"),
            terms(&[("date", "desc")])
        );
        assert_eq!(
            order("select * from game order by date DESC limit 10 start 20"),
            terms(&[("date", "desc")])
        );
        assert_eq!(
            order("SELECT * FROM game ORDER BY title COLLATE DESC LIMIT 5 START 5"),
            terms(&[("title", "desc")])
        );
    }

    #[test]
    fn ignores_clauses_inside_subqueries() {
        assert_eq!(
            order(
                "SELECT *, (SELECT * FROM comment WHERE game = $parent.id ORDER BY created_at desc LIMIT 5) AS comments \
                 FROM game ORDER BY sort_index asc LIMIT 30 START 90;"
            ),
            terms(&[("sort_index", "asc")]),
            "the subquery ORDER BY must not leak into the window order"
        );
    }

    #[test]
    fn string_literals_are_opaque() {
        assert_eq!(
            order("SELECT * FROM game WHERE note = 'LIMIT 30 START 30' ORDER BY date desc LIMIT 30 START 30;"),
            terms(&[("date", "desc")])
        );
        assert!(build_window_materialization(
            "SELECT * FROM game WHERE note = 'START 30' LIMIT 30;"
        )
        .is_none());
        assert!(build_window_materialization(
            "SELECT * FROM game WHERE note = 'it\\'s START 3' LIMIT 3;"
        )
        .is_none());
    }

    #[test]
    fn non_ascii_text_is_safe() {
        assert_eq!(
            order("SELECT * FROM gäme WHERE n = 'é' ORDER BY ü LIMIT 3 START 3"),
            terms(&[("ü", "asc")])
        );
    }
}
