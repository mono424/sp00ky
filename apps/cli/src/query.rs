//! `spky query` — run ad-hoc SurrealQL against the configured database.
//!
//! Two modes:
//!   * One-shot: `spky query "<surrealql>"` runs the statement(s) and prints
//!     the result (an aligned table by default, raw JSON with `--json`).
//!   * REPL: `spky query` (no positional argument) opens an interactive prompt
//!     where multiple statements can be run one after another, much like the
//!     `psql` REPL. Statements are submitted when the buffer ends with `;`.
//!
//! Connection resolution mirrors the other DB commands (`flag`, `jobs`,
//! `migrate`): it defaults to the local SurrealDB and targets the production
//! cloud deployment when `--cloud` is passed.

use anyhow::{anyhow, Result};
use serde_json::{json, Value};
use std::path::PathBuf;

use crate::surreal_client::{SurrealClient, SurrealResponse};
use crate::{ConnectionArgs, ResolvedConnection};

const RED: &str = "\x1b[31m";
const DIM: &str = "\x1b[2m";
const BOLD: &str = "\x1b[1m";
const RESET: &str = "\x1b[0m";

pub fn run(
    sql: Option<String>,
    json: bool,
    conn: ConnectionArgs,
    config: Option<PathBuf>,
) -> Result<()> {
    let is_cloud = conn.cloud;
    let resolved = resolve(conn, config)?;
    let client = resolved.client();

    match sql {
        Some(sql) => run_once(&client, &sql, json),
        None => run_repl(&client, &resolved, is_cloud, json),
    }
}

// =============================================================
// Connection
// =============================================================

/// Resolve the connection parameters. With `--cloud`, this resolves the
/// deployment's SurrealDB URL + login from Sp00ky Cloud (same lookup as
/// `spky project credentials`); otherwise it falls back to local resolution
/// from `sp00ky.yml`, honoring explicit `--namespace`/`--database` overrides.
fn resolve(conn: ConnectionArgs, config: Option<PathBuf>) -> Result<ResolvedConnection> {
    conn.resolve(&config)
}

// =============================================================
// One-shot mode
// =============================================================

fn run_once(client: &SurrealClient, sql: &str, json: bool) -> Result<()> {
    // Transport / HTTP / parse failures propagate (printed by main); per-
    // statement ERR responses come back in the Vec so we can render them.
    let responses = client.query(sql)?;
    let had_err = render_responses(&responses, json);
    if had_err {
        std::process::exit(1);
    }
    Ok(())
}

// =============================================================
// REPL mode
// =============================================================

fn run_repl(
    client: &SurrealClient,
    resolved: &ResolvedConnection,
    is_cloud: bool,
    json: bool,
) -> Result<()> {
    // Without an interactive terminal (e.g. `echo "..." | spky query`), read
    // statements from stdin instead of driving the interactive prompt.
    if !std::io::IsTerminal::is_terminal(&std::io::stdin()) {
        return run_stdin(client, json);
    }

    print_banner(resolved, is_cloud);

    let mut buffer = String::new();
    loop {
        let prompt = if buffer.is_empty() {
            "surreal>"
        } else {
            "   ...>"
        };

        let line = match inquire::Text::new(prompt).prompt() {
            Ok(line) => line,
            // Esc / Ctrl-C / Ctrl-D — leave the REPL cleanly.
            Err(inquire::InquireError::OperationCanceled)
            | Err(inquire::InquireError::OperationInterrupted) => {
                println!();
                break;
            }
            Err(e) => return Err(anyhow!("Input error: {}", e)),
        };

        // Meta-commands are only recognized at the start of a fresh statement.
        if buffer.is_empty() {
            let trimmed = line.trim();
            if trimmed.is_empty() {
                continue;
            }
            if is_quit_command(trimmed) {
                break;
            }
            if is_help_command(trimmed) {
                print_repl_help();
                continue;
            }
        }

        if !buffer.is_empty() {
            buffer.push('\n');
        }
        buffer.push_str(&line);

        if statement_complete(&buffer) {
            let sql = std::mem::take(&mut buffer);
            run_chunk(client, &sql, json);
        }
    }

    Ok(())
}

/// Non-interactive variant of the REPL: read `;`-terminated statements from
/// stdin and run each, so `spky query` works in a pipe. Exits non-zero if any
/// statement errored, which makes it usable in scripts.
fn run_stdin(client: &SurrealClient, json: bool) -> Result<()> {
    use std::io::BufRead;

    let stdin = std::io::stdin();
    let mut buffer = String::new();
    let mut had_err = false;

    for line in stdin.lock().lines() {
        let line = line?;
        if buffer.is_empty() {
            let trimmed = line.trim();
            if trimmed.is_empty() {
                continue;
            }
            if is_quit_command(trimmed) {
                break;
            }
        }
        if !buffer.is_empty() {
            buffer.push('\n');
        }
        buffer.push_str(&line);

        if statement_complete(&buffer) {
            let sql = std::mem::take(&mut buffer);
            had_err |= run_chunk(client, &sql, json);
        }
    }

    // Run any trailing statement that wasn't terminated with `;`.
    if !buffer.trim().is_empty() {
        had_err |= run_chunk(client, &buffer, json);
    }

    if had_err {
        std::process::exit(1);
    }
    Ok(())
}

/// Run one chunk of SurrealQL and render it, reporting whether it errored
/// without ever aborting the surrounding loop (transport errors included).
fn run_chunk(client: &SurrealClient, sql: &str, json: bool) -> bool {
    match client.query(sql) {
        Ok(responses) => render_responses(&responses, json),
        Err(e) => {
            eprintln!("{}{}{}", RED, e, RESET);
            true
        }
    }
}

fn print_banner(resolved: &ResolvedConnection, is_cloud: bool) {
    println!("{}spky query{} — interactive SurrealQL", BOLD, RESET);
    println!(
        "{}Connected to {} (ns={}, db={}){}",
        DIM, resolved.url, resolved.namespace, resolved.database, RESET
    );
    if is_cloud {
        println!(
            "{}{}⚠ PRODUCTION{} — queries run against the live cloud deployment.",
            BOLD, RED, RESET
        );
    }
    println!(
        "{}Type \\q to quit, \\? for help. End statements with ;{}",
        DIM, RESET
    );
}

fn print_repl_help() {
    println!("Commands:");
    println!("  \\q, quit, exit   Quit the REPL");
    println!("  \\?, help         Show this help");
    println!("  <surrealql>;     Run statement(s); end with ; (multi-line supported)");
}

/// True when a fresh-statement line is a request to quit the REPL.
fn is_quit_command(line: &str) -> bool {
    matches!(line.trim().trim_end_matches(';'), "\\q" | "quit" | "exit")
}

/// True when a fresh-statement line asks for REPL help.
fn is_help_command(line: &str) -> bool {
    matches!(line.trim(), "\\?" | "help")
}

/// A buffered statement is ready to execute once it ends with `;` (psql-style).
fn statement_complete(buffer: &str) -> bool {
    buffer.trim_end().ends_with(';')
}

// =============================================================
// Rendering
// =============================================================

/// Render every statement's response. Returns true if any statement errored.
fn render_responses(responses: &[SurrealResponse], json: bool) -> bool {
    let had_err = responses.iter().any(|r| r.status == "ERR");

    if json {
        // Single statement: emit just the data so it pipes cleanly into `jq`.
        // Multiple statements: emit an array of {status, result} objects.
        if responses.len() == 1 {
            let r = &responses[0];
            let payload = if r.status == "ERR" {
                json!({ "status": r.status, "result": r.result })
            } else {
                r.result.clone().unwrap_or(Value::Null)
            };
            let text =
                serde_json::to_string_pretty(&payload).unwrap_or_else(|_| payload.to_string());
            if r.status == "ERR" {
                eprintln!("{}", text);
            } else {
                println!("{}", text);
            }
        } else {
            let arr: Vec<Value> = responses
                .iter()
                .map(|r| json!({ "status": r.status, "result": r.result }))
                .collect();
            let value = Value::Array(arr);
            println!(
                "{}",
                serde_json::to_string_pretty(&value).unwrap_or_else(|_| value.to_string())
            );
        }
        return had_err;
    }

    let multi = responses.len() > 1;
    for (i, r) in responses.iter().enumerate() {
        if multi {
            println!("{}-- statement {}{}", DIM, i + 1, RESET);
        }
        if r.status == "ERR" {
            let msg = r
                .result
                .as_ref()
                .and_then(|v| v.as_str())
                .unwrap_or("Unknown error");
            eprintln!("{}ERR{} {}", RED, RESET, msg);
        } else {
            match &r.result {
                Some(v) => print_table_or_value(v),
                None => println!("{}(no result){}", DIM, RESET),
            }
        }
    }
    had_err
}

/// Print a result value as a table when it is row-shaped, otherwise as JSON.
fn print_table_or_value(value: &Value) {
    match value {
        Value::Array(arr) if arr.is_empty() => {
            println!("{}(0 rows){}", DIM, RESET);
        }
        // Array of objects → aligned table.
        Value::Array(arr) if arr.iter().all(Value::is_object) => {
            print!("{}", render_table(arr));
            println!("{}{}{}", DIM, row_count(arr.len()), RESET);
        }
        // Array of scalars (or mixed) → one value per line.
        Value::Array(arr) => {
            for v in arr {
                println!("{}", cell_to_string(v));
            }
            println!("{}{}{}", DIM, row_count(arr.len()), RESET);
        }
        // Scalar or single object (e.g. INFO FOR DB) → pretty JSON.
        other => {
            println!(
                "{}",
                serde_json::to_string_pretty(other).unwrap_or_else(|_| other.to_string())
            );
        }
    }
}

fn row_count(n: usize) -> String {
    format!("({} row{})", n, if n == 1 { "" } else { "s" })
}

/// Widest a table cell gets, in chars. Longer values are cut with `…` and the
/// table says so; `--json` prints them in full.
const MAX_CELL_CHARS: usize = 80;

/// Render an array of JSON objects as an aligned ASCII table. Columns are taken
/// in first-seen order across all rows; missing cells are blank and nested
/// objects/arrays are shown as compact JSON. Every cell stays on one line and
/// within `MAX_CELL_CHARS`.
fn render_table(rows: &[Value]) -> String {
    let mut columns: Vec<String> = Vec::new();
    for row in rows {
        if let Some(obj) = row.as_object() {
            for key in obj.keys() {
                if !columns.iter().any(|c| c == key) {
                    columns.push(key.clone());
                }
            }
        }
    }
    if columns.is_empty() {
        return String::new();
    }

    let mut widths: Vec<usize> = columns.iter().map(|c| c.chars().count()).collect();
    let mut cells: Vec<Vec<String>> = Vec::with_capacity(rows.len());
    let mut cut = 0;
    for row in rows {
        let obj = row.as_object();
        let mut line = Vec::with_capacity(columns.len());
        for (i, col) in columns.iter().enumerate() {
            let (cell, was_cut) = obj
                .and_then(|o| o.get(col))
                .map(|v| fit_cell(&cell_to_string(v)))
                .unwrap_or_default();
            cut += usize::from(was_cut);
            let width = cell.chars().count();
            if width > widths[i] {
                widths[i] = width;
            }
            line.push(cell);
        }
        cells.push(line);
    }

    let mut out = String::new();

    let header: Vec<String> = columns
        .iter()
        .enumerate()
        .map(|(i, c)| pad(c, widths[i]))
        .collect();
    out.push_str(&format!(
        "{}{}{}\n",
        BOLD,
        header.join("  ").trim_end(),
        RESET
    ));

    let sep: Vec<String> = widths.iter().map(|w| "-".repeat(*w)).collect();
    out.push_str(sep.join("  ").trim_end());
    out.push('\n');

    for line in &cells {
        let formatted: Vec<String> = line
            .iter()
            .enumerate()
            .map(|(i, c)| pad(c, widths[i]))
            .collect();
        out.push_str(formatted.join("  ").trim_end());
        out.push('\n');
    }

    if cut > 0 {
        out.push_str(&format!(
            "{}({} long value{} cut at {} chars, use --json to see them in full){}\n",
            DIM,
            cut,
            if cut == 1 { "" } else { "s" },
            MAX_CELL_CHARS,
            RESET
        ));
    }

    out
}

/// Left-align `s` in a column `width` chars wide. Pads by hand because a
/// `{:<width$}` width above `u16::MAX` panics with "Formatting argument out
/// of range".
fn pad(s: &str, width: usize) -> String {
    let fill = width.saturating_sub(s.chars().count());
    format!("{}{}", s, " ".repeat(fill))
}

/// Fit a cell on one table line: control characters are escaped (a raw
/// newline would split the row) and anything past `MAX_CELL_CHARS` is cut
/// with `…`. Returns whether the value was cut.
fn fit_cell(raw: &str) -> (String, bool) {
    let mut line = String::new();
    let mut len = 0;
    for c in raw.chars() {
        if len > MAX_CELL_CHARS {
            break;
        }
        if c.is_control() {
            for e in c.escape_default() {
                line.push(e);
                len += 1;
            }
        } else {
            line.push(c);
            len += 1;
        }
    }
    if len <= MAX_CELL_CHARS {
        return (line, false);
    }
    let mut cut: String = line.chars().take(MAX_CELL_CHARS - 1).collect();
    cut.push('…');
    (cut, true)
}

/// Stringify a single cell value: strings as-is, scalars via Display, and
/// nested objects/arrays as compact JSON.
fn cell_to_string(v: &Value) -> String {
    match v {
        Value::Null => String::new(),
        Value::String(s) => s.clone(),
        Value::Bool(b) => b.to_string(),
        Value::Number(n) => n.to_string(),
        other => serde_json::to_string(other).unwrap_or_else(|_| other.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    // ---- statement / meta-command parsing ------------------------------

    #[test]
    fn statement_is_complete_only_when_ending_in_semicolon() {
        assert!(statement_complete("SELECT * FROM user;"));
        assert!(statement_complete("SELECT * FROM user;   \n"));
        assert!(!statement_complete("SELECT * FROM user"));
        assert!(!statement_complete("SELECT * FROM user WHERE name ="));
    }

    #[test]
    fn quit_commands_are_recognized() {
        for q in ["\\q", "quit", "exit", "quit;", "  exit  "] {
            assert!(is_quit_command(q), "{q:?} should quit");
        }
        for not_q in ["select", "\\?", "help", "quitter"] {
            assert!(!is_quit_command(not_q), "{not_q:?} should not quit");
        }
    }

    #[test]
    fn help_commands_are_recognized() {
        assert!(is_help_command("\\?"));
        assert!(is_help_command("help"));
        assert!(!is_help_command("\\q"));
        assert!(!is_help_command("SELECT 1;"));
    }

    // ---- table rendering ----------------------------------------------

    #[test]
    fn render_table_aligns_uniform_objects() {
        let rows = vec![
            json!({ "id": "user:1", "name": "alice" }),
            json!({ "id": "user:22", "name": "bob" }),
        ];
        let out = render_table(&rows);
        // Header + values all present.
        assert!(out.contains("id"));
        assert!(out.contains("name"));
        assert!(out.contains("user:1"));
        assert!(out.contains("user:22"));
        assert!(out.contains("alice"));
        assert!(out.contains("bob"));
        // Header, separator, and one line per row.
        assert_eq!(out.lines().count(), 4);
        // Separator row is made of dashes.
        assert!(out.lines().nth(1).unwrap().contains("--"));
    }

    #[test]
    fn render_table_handles_missing_keys_and_nested_values() {
        let rows = vec![
            json!({ "id": "a", "tags": ["x", "y"] }),
            // Missing "tags", extra "extra", nested object cell.
            json!({ "id": "b", "extra": { "k": 1 } }),
        ];
        let out = render_table(&rows);
        // Union of all keys becomes columns.
        assert!(out.contains("id"));
        assert!(out.contains("tags"));
        assert!(out.contains("extra"));
        // Nested values render as compact JSON without panicking.
        assert!(out.contains("[\"x\",\"y\"]"));
        assert!(out.contains("{\"k\":1}"));
        // 2 data rows + header + separator.
        assert_eq!(out.lines().count(), 4);
    }

    #[test]
    fn render_table_survives_ragged_rows_with_huge_values() {
        // Shape of an `UPDATE puzzle ...` result: optional fields come back
        // missing (NONE) or null, so key sets differ per row, and `content`
        // is far wider than the 65535 limit Rust puts on a format width.
        let huge = format!("{}\nsecond line", "x".repeat(70_000));
        let rows: Vec<Value> = (0..70)
            .map(|i| {
                let mut row = json!({ "id": format!("puzzle:{i}"), "content": huge, "version": 8 });
                let obj = row.as_object_mut().unwrap();
                if i % 2 == 0 {
                    obj.insert("importing".into(), json!(false));
                }
                if i % 3 == 0 {
                    obj.insert("rating".into(), Value::Null);
                }
                if i % 5 == 0 {
                    obj.insert("tags".into(), json!(["endgame", "pin"]));
                }
                row
            })
            .collect();

        let out = render_table(&rows);
        let lines: Vec<&str> = out.lines().collect();

        // Header, separator, one line per row (the newline in `content` does
        // not split a row), then the note about cut values.
        assert_eq!(lines.len(), 2 + rows.len() + 1);
        for col in ["id", "content", "version", "importing", "rating", "tags"] {
            assert!(lines[0].contains(col), "header misses {col}");
        }
        assert!(out.contains("puzzle:69"));
        assert!(!out.contains(&huge));
        assert!(lines[2].contains('…'));
        assert!(lines[2].contains("puzzle:0"));
        assert!(lines.last().unwrap().contains("--json"));
        // Every line stays far below the format width limit.
        assert!(lines.iter().all(|l| l.chars().count() < 1_000));
    }

    #[test]
    fn fit_cell_escapes_and_cuts_at_the_limit() {
        let exact = "a".repeat(MAX_CELL_CHARS);
        assert_eq!(fit_cell(&exact), (exact.clone(), false));

        let (cut, was_cut) = fit_cell(&"a".repeat(MAX_CELL_CHARS + 1));
        assert!(was_cut);
        assert_eq!(cut.chars().count(), MAX_CELL_CHARS);
        assert!(cut.ends_with('…'));

        assert_eq!(fit_cell("a\nb\tc"), ("a\\nb\\tc".to_string(), false));
        // Padding past u16::MAX is fine without a format width argument.
        assert_eq!(pad("x", 70_000).len(), 70_000);
    }

    #[test]
    fn cell_to_string_formats_scalars_and_json() {
        assert_eq!(cell_to_string(&json!("hi")), "hi");
        assert_eq!(cell_to_string(&json!(42)), "42");
        assert_eq!(cell_to_string(&json!(true)), "true");
        assert_eq!(cell_to_string(&Value::Null), "");
        assert_eq!(cell_to_string(&json!({ "a": 1 })), "{\"a\":1}");
    }

    #[test]
    fn row_count_pluralizes() {
        assert_eq!(row_count(1), "(1 row)");
        assert_eq!(row_count(0), "(0 rows)");
        assert_eq!(row_count(3), "(3 rows)");
    }
}
