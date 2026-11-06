//! The data plane of the admin API (`/admin/api/data`): browse and edit the
//! project's own tables, its users and the files of its buckets, and run
//! SurrealQL against it.
//!
//! One implementation for every host of that API, like [`crate::admin`]: a host
//! authenticates the request, refuses a read-only session anything but GET and
//! hands the rest to [`dispatch`], which owns the routing, so the scheduler and
//! the SSP node cannot drift.
//!
//! The handle behind this runs as root, so the rules that keep it an operator's
//! tool and not a hole live here:
//!
//! - Names reach a statement only as validated identifiers, and every value
//!   travels as a binding. Record ids are bound and cast (`<record>$id`), and
//!   must name the table in the route, so `tables/user/row?id=_00_admin:x`
//!   cannot reach another table.
//! - The browse routes never show a credential: a field named like one, and the
//!   field a `DEFINE ACCESS` compares a password against, read as
//!   `{"$redacted": true}`, and cannot be written, filtered or sorted on from a
//!   grid. Filtering is a way to read: `password > 'a'` is a binary search. A
//!   read-only session can reach those routes, so this is its boundary, not a
//!   courtesy. `POST query` shows what the statement returns, and is refused to
//!   read-only sessions for that reason.
//! - `_00_*` tables are read-only here: the sync engine owns them. The roster
//!   moves through its own user routes; anything else takes a statement. The
//!   tables of push keys and device tokens are not browsed at all.
//! - `INFO FOR DB` carries every access method's signing keys. Only the parts
//!   named below ever leave this file.
//! - A folder's listing can come from the disk instead (see [`BucketDirs`]), and
//!   that reads names and sizes only: every file operation still goes through
//!   the database, so its rules and its one writer stay as they are.
//! - Feature flags change through their own routes, in the shape `spky flag`
//!   writes and evaluated by the project's own `fn::feature::*`, so the console,
//!   the CLI and the DevTools panel cannot assign different variants.
//! - Reads carry a `TIMEOUT` and a `LIMIT`. Nothing here counts or sorts a table
//!   unasked, a scan of a large user table being a real load on the project's
//!   database: the Database page's count is a button, and the Users list is sorted
//!   when asked or when an index makes it free. The one exception is the Users
//!   page's total, read once when the page opens, under its own timeout.

use std::collections::{BTreeSet, HashMap, HashSet};

use serde_json::{json, Map, Value};

use crate::admin::{AdminError, AdminResult};
use crate::db::{rows, ScheduleDb, ScheduleDbError};
use crate::sql::is_plain_identifier;
use crate::MaybeSendSync;

/// Cap on `?limit=`.
pub const MAX_ROWS_PAGE: usize = 500;
pub const DEFAULT_ROWS_PAGE: usize = 50;

/// Rows one statement of `POST query` brings back to the browser.
pub const MAX_QUERY_ROWS: usize = 1000;
/// The `LIMIT` a bare `SELECT` in `POST query` gets, one past what is shown so
/// the answer can say there was more.
const QUERY_LIMIT: usize = MAX_QUERY_ROWS + 1;
/// Serialized size of one `POST query` answer.
const MAX_QUERY_BYTES: usize = 24 * 1024 * 1024;
const MAX_SQL_BYTES: usize = 256 * 1024;

/// A value longer than this is cut down in a listing; the row route has more
/// room, and `$trimmed` says how much is missing.
const LIST_CELL_BYTES: usize = 4 * 1024;
const ROW_CELL_BYTES: usize = 512 * 1024;
const PREVIEW_CHARS: usize = 300;

/// How long a browse statement may run on the database.
const READ_TIMEOUT: &str = "15s";
const COUNT_TIMEOUT: &str = "10s";
const SQL_TIMEOUT: &str = "30s";

/// Bytes a file may have and still be read through this API: the database, the
/// host and the browser each hold it in memory (as base64, a third larger).
pub const MAX_FILE_READ_BYTES: u64 = 32 * 1024 * 1024;
/// What SurrealDB's own `/rpc` limit leaves for a file, by default: 4 MiB of
/// body (`SURREAL_HTTP_MAX_RPC_BODY_SIZE`), which base64 turns into this.
pub const DEFAULT_UPLOAD_BYTES: u64 = 3 * 1024 * 1024 - 4 * 1024;
/// A host's limit on one request's body: a database with a raised limit can take
/// more than the default, this much at most.
pub const MAX_BODY_BYTES: usize = (MAX_FILE_READ_BYTES as usize) * 4 / 3 + 4096;
const MAX_FILE_PAGE: usize = 500;
const DEFAULT_FILE_PAGE: usize = 100;
/// Round trips one folder listing may spend skipping over folders.
const MAX_LIST_ROUNDS: usize = 12;
const MAX_KEY_BYTES: usize = 1024;
const MAX_BATCH: usize = 500;
const MAX_FILTERS: usize = 16;

/// Whether a value of `SPKY_ADMIN_DATA` turns this surface off.
pub fn is_off(value: Option<&str>) -> bool {
    matches!(value.map(|v| v.trim().to_ascii_lowercase()).as_deref(), Some("0" | "false" | "off" | "no"))
}

/// One folder of a `file:` bucket as the disk has it.
#[derive(Debug, Default, Clone, PartialEq)]
pub struct DirPage {
    /// Subfolders, each with how many entries it holds, up to [`DIR_COUNT_CAP`].
    pub folders: Vec<(String, u64)>,
    pub files: Vec<DirFile>,
    /// Where the next page starts, when there is one.
    pub next: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DirFile {
    pub name: String,
    pub size: u64,
    pub updated: Option<String>,
}

/// Where a folder's entry count stops. The page shows it as "999+".
pub const DIR_COUNT_CAP: u64 = 1000;

/// A host that can read a `file:` bucket's directory itself.
///
/// `file::list` answers with the files of one directory and leaves out the
/// directories in it, on every SurrealDB version probed (3.1.5, 4.0.0-beta.1), so
/// through the database a subfolder can only be found by already knowing its path.
/// A host that mounts the bucket volume read-only can list them. One that cannot
/// passes `None` and the page says a folder must be opened by its path.
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
pub trait BucketDirs: MaybeSendSync {
    /// Whether this host can read a bucket stored at `location`.
    fn covers(&self, location: &str) -> bool;
    /// One page of `prefix` (empty, or `a/b/`, already validated) under `location`,
    /// in name order, starting after the `start` it returned earlier.
    async fn list(&self, location: &str, prefix: &str, start: Option<&str>, limit: usize) -> Result<DirPage, String>;
}

/// One request, already authenticated and past the scope rule.
pub struct Request<'a> {
    pub method: &'a str,
    /// The path under `/admin/api/data`, decoded and split.
    pub path: &'a [&'a str],
    pub query: &'a [(String, String)],
    pub body: &'a [u8],
    /// The session's subject, for the audit line and to stop an operator
    /// removing themselves.
    pub actor: &'a str,
    /// See [`BucketDirs`].
    pub dirs: Option<&'a dyn BucketDirs>,
}

/// Answer one data request. Every change is logged with who made it.
pub async fn dispatch(db: &dyn ScheduleDb, req: Request<'_>) -> AdminResult<Value> {
    let out = route(db, &req).await;
    if req.method != "GET" && out.is_ok() {
        let target = param(&req, "id").or_else(|| param(&req, "key")).unwrap_or("");
        tracing::info!(
            by = %req.actor,
            method = req.method,
            route = %req.path.join("/"),
            target,
            "Data changed from the admin API"
        );
    }
    out
}

async fn route(db: &dyn ScheduleDb, req: &Request<'_>) -> AdminResult<Value> {
    match (req.method, req.path) {
        ("GET", ["tables"]) => list_tables(db).await,
        ("GET", ["tables", table]) => table_detail(db, table).await,
        ("GET", ["tables", table, "rows"]) => list_rows(db, table, req).await,
        ("POST", ["tables", table, "rows"]) => create_row(db, table, req).await,
        ("POST", ["tables", table, "rows", "delete"]) => delete_rows(db, table, req).await,
        ("GET", ["tables", table, "count"]) => count_rows(db, table, req).await,
        ("GET", ["tables", table, "row"]) => get_row(db, table, req).await,
        ("PUT", ["tables", table, "row"]) => update_row(db, table, req).await,
        ("DELETE", ["tables", table, "row"]) => delete_row(db, table, req).await,
        ("POST", ["query"]) => run_query(db, req).await,

        ("GET", ["users"]) => list_users(db, req).await,
        ("POST", ["users"]) => create_user(db, req).await,
        ("GET", ["users", "stats"]) => user_stats(db, req).await,
        ("GET", ["users", "user"]) => user_detail(db, req).await,
        ("DELETE", ["users", "user"]) => delete_user(db, req).await,
        ("PUT", ["users", "user", "password"]) => reset_password(db, req).await,
        ("PUT", ["users", "user", "admin"]) => set_admin(db, req).await,

        ("GET", ["flags"]) => list_flags(db).await,
        ("POST", ["flags"]) => create_flag(db, req).await,
        ("GET", ["flags", "flag"]) => flag_detail(db, req).await,
        ("PUT", ["flags", "flag"]) => update_flag(db, req).await,
        ("DELETE", ["flags", "flag"]) => delete_flag(db, req).await,
        ("POST", ["flags", "flag", "evaluate"]) => evaluate_flag(db, req).await,
        ("PUT", ["flags", "flag", "user"]) => pin_user(db, req).await,

        ("GET", ["buckets"]) => list_buckets(db, req.dirs).await,
        ("GET", ["buckets", bucket, "files"]) => list_files(db, bucket, req).await,
        ("GET", ["buckets", bucket, "file"]) => read_file(db, bucket, req).await,
        ("PUT", ["buckets", bucket, "file"]) => write_file(db, bucket, req).await,
        ("DELETE", ["buckets", bucket, "file"]) => delete_file(db, bucket, req).await,
        ("POST", ["buckets", bucket, "rename"]) => rename_file(db, bucket, req).await,
        ("POST", ["buckets", bucket, "delete"]) => delete_files(db, bucket, req).await,

        _ => Err(AdminError::new(404, "No such data route")),
    }
}

// =============================================================
// Small pieces
// =============================================================

fn param<'a>(req: &'a Request<'_>, key: &str) -> Option<&'a str> {
    req.query.iter().find(|(k, v)| k == key && !v.is_empty()).map(|(_, v)| v.as_str())
}

fn number_param(req: &Request<'_>, key: &str) -> Option<usize> {
    param(req, key).and_then(|s| s.parse().ok())
}

fn body_object(req: &Request<'_>) -> AdminResult<Map<String, Value>> {
    match serde_json::from_slice(req.body) {
        Ok(Value::Object(map)) => Ok(map),
        _ => Err(AdminError::new(400, "A JSON object body is required")),
    }
}

fn db_error(e: ScheduleDbError) -> AdminError {
    let message = match &e {
        ScheduleDbError::Auth(m) | ScheduleDbError::Transport(m) | ScheduleDbError::Query(m) => m.clone(),
    };
    let lower = message.to_ascii_lowercase();
    if lower.contains("length limit exceeded") || lower.contains("payload too large") {
        return AdminError::new(
            413,
            "The database refused a request this large. SurrealDB takes 4 MiB per request unless \
             SURREAL_HTTP_MAX_RPC_BODY_SIZE says otherwise, which leaves about 3 MB for a file.",
        );
    }
    if lower.contains("exceeded the timeout") {
        return AdminError::new(504, format!("{message}. Narrow it with a filter or a limit."));
    }
    // SDKs raise a statement that does not parse from the request, not from its
    // result, so it can arrive as a transport failure.
    let parse = lower.contains("parse error");
    match e {
        // The database's own sentence: a failed ASSERT, a cast, a parse error.
        ScheduleDbError::Query(_) => AdminError::new(400, message),
        _ if parse => AdminError::new(400, message),
        _ => AdminError::new(502, format!("Database error: {message}")),
    }
}

async fn run(db: &dyn ScheduleDb, surql: &str, binds: &[(&str, Value)]) -> AdminResult<Vec<Value>> {
    db.query(surql, binds).await.map_err(db_error)
}

async fn first_rows(db: &dyn ScheduleDb, surql: &str, binds: &[(&str, Value)]) -> AdminResult<Vec<Value>> {
    Ok(rows(run(db, surql, binds).await?))
}

/// Push keys and device tokens. Not one field of them is worth a page, and an
/// endpoint or a p256dh key does not look like the credential it is.
const UNBROWSED_TABLES: &[&str] = &["_00_push_credential", "_00_push_subscription"];

fn checked_table(name: &str) -> AdminResult<&str> {
    if UNBROWSED_TABLES.contains(&name) {
        return Err(AdminError::new(403, format!("{name} holds push keys and device tokens and is not browsed here")));
    }
    if is_plain_identifier(name) {
        Ok(name)
    } else {
        Err(AdminError::new(400, format!("'{name}' is not a plain table name; reach it from the SQL editor")))
    }
}

/// Whether the sync engine owns this table.
fn is_internal(table: &str) -> bool {
    table.starts_with("_00_")
}

fn writable_table(name: &str) -> AdminResult<&str> {
    let table = checked_table(name)?;
    if is_internal(table) {
        return Err(AdminError::new(403, format!("{table} belongs to the sync engine and is read-only here; use the SQL editor if you mean it")));
    }
    Ok(table)
}

/// `id` as given, once it names a record of `table`.
fn checked_id(table: &str, id: Option<&str>) -> AdminResult<String> {
    let id = id.ok_or_else(|| AdminError::new(400, "A record id is required"))?;
    match id.strip_prefix(table).and_then(|rest| rest.strip_prefix(':')) {
        Some(key) if !key.is_empty() => Ok(id.to_string()),
        _ => Err(AdminError::new(400, format!("'{id}' is not a record of {table}"))),
    }
}

/// A field a grid may filter or sort on. Top level only: `author.password` would
/// follow the record link into another table, past that table's credentials.
fn checked_filter_field<'a>(table: &str, field: &'a str, known: &HashSet<(String, String)>) -> AdminResult<&'a str> {
    if !is_plain_identifier(field) {
        return Err(AdminError::new(400, format!("'{field}' is not a field of {table}")));
    }
    if is_credential(table, field, known) {
        return Err(AdminError::new(400, format!("{field} holds a credential and cannot be filtered or sorted on")));
    }
    Ok(field)
}

// =============================================================
// Credentials
// =============================================================

/// Anywhere in a field's name, case and separators aside: `apiKey`, `api-key`.
const CREDENTIAL_FRAGMENTS: &[&str] = &[
    "password", "passwd", "passphrase", "secret", "credential", "token", "privatekey", "privkey",
    "accesskey", "apikey", "authorization",
];
/// As a whole word of the name only: `pass`, not `compass`.
const CREDENTIAL_WORDS: &[&str] = &["pass", "pw", "pwd", "jwt", "salt", "bearer"];

/// A name's words: `apiKey`, `api_key` and `api-key` are all `api`, `key`.
fn name_words(field: &str) -> Vec<String> {
    let mut words = vec![String::new()];
    let mut previous_lower = false;
    for c in field.chars() {
        if !c.is_ascii_alphanumeric() {
            words.push(String::new());
            previous_lower = false;
            continue;
        }
        if c.is_ascii_uppercase() && previous_lower {
            words.push(String::new());
        }
        previous_lower = c.is_ascii_lowercase();
        if let Some(word) = words.last_mut() {
            word.push(c.to_ascii_lowercase());
        }
    }
    words.retain(|w| !w.is_empty());
    words
}

/// Whether a field's name says it holds a credential.
pub fn is_credential_name(field: &str) -> bool {
    let words = name_words(field);
    let folded = words.concat();
    CREDENTIAL_FRAGMENTS.iter().any(|fragment| folded.contains(fragment))
        || words.iter().any(|word| CREDENTIAL_WORDS.contains(&word.as_str()))
}

/// The fields `SIGNIN` clauses compare a password against, by table: a hash in a
/// field called `pw` is still a hash.
fn access_credentials(info: &Value) -> HashSet<(String, String)> {
    user_sources(info)
        .into_iter()
        .filter_map(|source| Some((source.table, source.password?)))
        .collect()
}

fn is_credential(table: &str, field: &str, known: &HashSet<(String, String)>) -> bool {
    is_credential_name(field) || known.contains(&(table.to_string(), field.to_string()))
}

/// Mask credentials and cut long values down to `cell` bytes, in place. Under a
/// credential's name everything goes, however it is shaped; only a number under
/// `token` stays, because `tokens_used` is a count.
fn clean_value(table: &str, field: &str, value: &mut Value, known: &HashSet<(String, String)>, cell: usize) {
    if is_credential(table, field, known) {
        let keep = match value {
            Value::Number(_) => field.to_ascii_lowercase().contains("token"),
            Value::String(_) | Value::Array(_) | Value::Object(_) => false,
            _ => true,
        };
        if !keep {
            *value = json!({ "$redacted": true });
        }
        return;
    }
    if let Value::Object(map) = value {
        for (key, nested) in map.iter_mut() {
            clean_value(table, key, nested, known, cell);
        }
    } else if let Value::Array(items) = value {
        for nested in items.iter_mut() {
            clean_value(table, field, nested, known, cell);
        }
    }
    let size = match value {
        Value::String(s) => s.len(),
        Value::Array(_) | Value::Object(_) => value.to_string().len(),
        _ => 0,
    };
    if size > cell {
        let text = match &*value {
            Value::String(s) => s.clone(),
            other => other.to_string(),
        };
        *value = json!({
            "$trimmed": true,
            "bytes": size,
            "preview": text.chars().take(PREVIEW_CHARS).collect::<String>(),
        });
    }
}

/// Whether a value carries one of the masks above, at any depth. Written back, the
/// mask would replace what it stands for.
fn holds_mask(value: &Value) -> bool {
    match value {
        Value::Object(map) => map.contains_key("$redacted") || map.contains_key("$trimmed") || map.values().any(holds_mask),
        Value::Array(items) => items.iter().any(holds_mask),
        _ => false,
    }
}

fn clean_row(table: &str, mut row: Value, known: &HashSet<(String, String)>, cell: usize) -> Value {
    if let Value::Object(map) = &mut row {
        for (field, value) in map.iter_mut() {
            clean_value(table, field, value, known, cell);
        }
    }
    row
}

// =============================================================
// Schema
// =============================================================

async fn db_info(db: &dyn ScheduleDb) -> AdminResult<Value> {
    Ok(run(db, "INFO FOR DB STRUCTURE;", &[]).await?.into_iter().next().unwrap_or(Value::Null))
}

fn table_entry(info: &Value) -> impl Iterator<Item = &Value> {
    info.get("tables").and_then(Value::as_array).into_iter().flatten()
}

fn table_summary(entry: &Value) -> Value {
    let name = entry.get("name").and_then(Value::as_str).unwrap_or_default();
    let kind = entry.pointer("/kind/kind").and_then(Value::as_str).unwrap_or("ANY");
    json!({
        "name": name,
        "kind": kind.to_ascii_lowercase(),
        "in": entry.pointer("/kind/in"),
        "out": entry.pointer("/kind/out"),
        "schemafull": entry.get("schemafull").and_then(Value::as_bool).unwrap_or(false),
        "view": entry.get("view").is_some_and(|v| !v.is_null()),
        "comment": entry.get("comment"),
        "internal": is_internal(name),
    })
}

/// `GET tables`. Names and shapes only; counting them would scan each one.
async fn list_tables(db: &dyn ScheduleDb) -> AdminResult<Value> {
    let info = db_info(db).await?;
    let mut tables: Vec<Value> = table_entry(&info)
        .filter(|t| !t["name"].as_str().is_some_and(|name| UNBROWSED_TABLES.contains(&name)))
        .map(table_summary)
        .collect();
    tables.sort_by(|a, b| a["name"].as_str().cmp(&b["name"].as_str()));
    Ok(json!({ "tables": tables }))
}

/// The defined type of each field of a table, nested ones included.
fn field_kinds(structure: &Value) -> HashMap<String, String> {
    structure
        .get("fields")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|f| Some((f.get("name")?.as_str()?.to_string(), f.get("kind")?.as_str()?.to_string())))
        .collect()
}

async fn table_structure(db: &dyn ScheduleDb, table: &str) -> AdminResult<Value> {
    Ok(run(db, &format!("INFO FOR TABLE {table} STRUCTURE;"), &[])
        .await?
        .into_iter()
        .next()
        .unwrap_or(Value::Null))
}

/// `GET tables/:table`: columns, indexes, events and what can be done here.
async fn table_detail(db: &dyn ScheduleDb, table: &str) -> AdminResult<Value> {
    let table = checked_table(table)?;
    let info = db_info(db).await?;
    let Some(entry) = table_entry(&info).find(|t| t["name"] == table) else {
        return Err(AdminError::new(404, format!("No table named '{table}'")));
    };
    let known = access_credentials(&info);
    let structure = table_structure(db, table).await?;

    let fields: Vec<Value> = structure
        .get("fields")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .map(|f| {
            let name = f.get("name").and_then(Value::as_str).unwrap_or_default();
            let kind = f.get("kind").and_then(Value::as_str).unwrap_or("any");
            let credential = is_credential(table, name, &known);
            // A definition can hold a literal: a credential's says nothing here.
            let shown = |key: &str| if credential { &Value::Null } else { f.get(key).unwrap_or(&Value::Null) };
            json!({
                "name": name,
                "kind": kind,
                "optional": is_optional_kind(kind),
                "nested": name.contains('.') || name.contains('*'),
                "readonly": f.get("readonly").and_then(Value::as_bool).unwrap_or(false),
                "flexible": f.get("flexible").and_then(Value::as_bool).unwrap_or(false),
                "default": shown("default"),
                "value": shown("value"),
                "assert": shown("assert"),
                "comment": f.get("comment"),
                "credential": credential,
                "required": !is_optional_kind(kind)
                    && f.get("default").is_none_or(Value::is_null)
                    && f.get("value").is_none_or(Value::is_null)
                    && f.get("computed").is_none_or(Value::is_null)
                    && !name.contains('.') && !name.contains('*') && kind != "any",
            })
        })
        .collect();
    let mut summary = table_summary(entry);
    summary["fields"] = json!(fields);
    summary["indexes"] = structure.get("indexes").cloned().unwrap_or(json!([]));
    // Not `then`: an event's body can hold a bearer it posts with.
    summary["events"] = json!(structure
        .get("events")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .map(|e| json!({ "name": e.get("name"), "when": e.get("when") }))
        .collect::<Vec<_>>());
    summary["read_only"] = json!(is_internal(table));
    Ok(summary)
}

/// Split a type on a top-level separator: `none | array<int | string>` has two
/// parts, not three.
fn split_kind(kind: &str, separator: char) -> Vec<&str> {
    let mut parts = Vec::new();
    let (mut depth, mut start) = (0i32, 0usize);
    for (i, c) in kind.char_indices() {
        match c {
            '<' | '(' | '{' | '[' => depth += 1,
            '>' | ')' | '}' | ']' => depth -= 1,
            c if c == separator && depth == 0 => {
                parts.push(kind[start..i].trim());
                start = i + c.len_utf8();
            }
            _ => {}
        }
    }
    parts.push(kind[start..].trim());
    parts
}

fn is_optional_kind(kind: &str) -> bool {
    kind.starts_with("option<") || split_kind(kind, '|').iter().any(|p| p.eq_ignore_ascii_case("none"))
}

/// The type a value must be cast to before it lands in a field of this kind, or
/// `None` when a binding is right as it is. A bound string does not coerce into a
/// `datetime` or a `record` (see [`crate::sql`]), so a date from a form needs the
/// cast. `kind` comes from the database's own definition, never from a request.
fn cast_kind(kind: &str) -> Option<String> {
    let kind = kind.trim();
    let inner = kind.strip_prefix("option<").and_then(|k| k.strip_suffix('>')).unwrap_or(kind);
    let parts: Vec<&str> = split_kind(inner, '|')
        .into_iter()
        .filter(|p| !p.eq_ignore_ascii_case("none") && !p.eq_ignore_ascii_case("null"))
        .collect();
    let [only] = parts.as_slice() else { return None };
    if only.eq_ignore_ascii_case("any") || !only.starts_with(|c: char| c.is_ascii_alphabetic()) {
        return None;
    }
    Some((*only).to_string())
}

// =============================================================
// Rows
// =============================================================

/// `field = expression` pairs and the bindings behind them, for a form's values.
/// `null` means "remove this value": a bound null is refused by an `option<T>`
/// field, which takes T or NONE (see [`crate::sql`]).
fn assignments(
    table: &str,
    set: &Map<String, Value>,
    unset: &[String],
    kinds: &HashMap<String, String>,
    known: &HashSet<(String, String)>,
    allow_credentials: bool,
) -> AdminResult<(Vec<(String, String)>, Vec<(String, Value)>)> {
    let mut parts = Vec::with_capacity(set.len() + unset.len());
    let mut binds = Vec::with_capacity(set.len());
    let names = set.keys().map(String::as_str).chain(unset.iter().map(String::as_str));
    for field in names {
        if !is_plain_identifier(field) {
            return Err(AdminError::new(400, format!("'{field}' is not a plain field name")));
        }
        if !allow_credentials && is_credential(table, field, known) {
            return Err(AdminError::new(
                400,
                format!("{field} holds a credential and cannot be edited here; a user's password has its own action on the Users page"),
            ));
        }
    }
    if let Some((field, _)) = set.iter().find(|(_, value)| holds_mask(value)) {
        return Err(AdminError::new(
            400,
            format!("{field} holds a hidden or shortened value, which saving this would overwrite; change it from the SQL editor"),
        ));
    }
    for (i, (field, value)) in set.iter().enumerate() {
        if value.is_null() {
            parts.push((field.clone(), "NONE".to_string()));
            continue;
        }
        let name = format!("v{i}");
        let expr = match kinds.get(field).and_then(|k| cast_kind(k)) {
            Some(kind) => format!("<{kind}>${name}"),
            None if matches!(field.as_str(), "in" | "out") => format!("<record>${name}"),
            None => format!("${name}"),
        };
        parts.push((field.clone(), expr));
        binds.push((name, value.clone()));
    }
    for field in unset {
        parts.push((field.clone(), "NONE".to_string()));
    }
    Ok((parts, binds))
}

fn set_clause(parts: &[(String, String)]) -> String {
    parts.iter().map(|(f, e)| format!("{f} = {e}")).collect::<Vec<_>>().join(", ")
}

fn as_binds(binds: &[(String, Value)]) -> Vec<(&str, Value)> {
    binds.iter().map(|(name, value)| (name.as_str(), value.clone())).collect()
}

const FILTER_OPS: &[(&str, &str)] =
    &[("eq", "="), ("ne", "!="), ("gt", ">"), ("gte", ">="), ("lt", "<"), ("lte", "<=")];

/// What a typed filter value means when the column has no defined type.
fn loose_value(text: &str) -> Value {
    match text {
        "true" => Value::Bool(true),
        "false" => Value::Bool(false),
        _ => serde_json::from_str::<serde_json::Number>(text)
            .map(Value::Number)
            .unwrap_or_else(|_| Value::String(text.to_string())),
    }
}

/// The `WHERE` clause for `?filter=[{field, op, value}, ...]`, ANDed.
fn where_clause(
    table: &str,
    filter: Option<&str>,
    kinds: &HashMap<String, String>,
    known: &HashSet<(String, String)>,
) -> AdminResult<(String, Vec<(String, Value)>)> {
    let Some(filter) = filter else { return Ok((String::new(), Vec::new())) };
    let parsed: Vec<Value> = serde_json::from_str(filter).map_err(|_| AdminError::new(400, "filter must be a JSON array"))?;
    if parsed.len() > MAX_FILTERS {
        return Err(AdminError::new(400, format!("At most {MAX_FILTERS} filters at a time")));
    }
    let mut clauses = Vec::with_capacity(parsed.len());
    let mut binds = Vec::new();
    for (i, f) in parsed.iter().enumerate() {
        let field = f.get("field").and_then(Value::as_str).unwrap_or_default();
        let op = f.get("op").and_then(Value::as_str).unwrap_or("eq");
        let field = checked_filter_field(table, field, known)?;
        let text = match f.get("value") {
            Some(Value::String(s)) => s.clone(),
            Some(Value::Null) | None => String::new(),
            Some(other) => other.to_string(),
        };
        let name = format!("f{i}");
        match op {
            "is_none" => clauses.push(format!("{field} = NONE")),
            "not_none" => clauses.push(format!("{field} != NONE")),
            "contains" | "starts_with" => {
                let function = if op == "contains" { "string::contains" } else { "string::starts_with" };
                // Single values only: an object or array cast to text would search
                // whatever is nested in it, masked or not.
                clauses.push(format!(
                    "(!(type::is_object({field}) OR type::is_array({field}) OR type::is_set({field})) \
                     AND {function}(string::lowercase(<string>({field} OR '')), string::lowercase(${name})))"
                ));
                binds.push((name, Value::String(text)));
            }
            _ => {
                let Some((_, sql_op)) = FILTER_OPS.iter().find(|(key, _)| *key == op) else {
                    return Err(AdminError::new(400, format!("'{op}' is not a filter")));
                };
                let cast = if field == "id" { Some("record".to_string()) } else { kinds.get(field).and_then(|k| cast_kind(k)) };
                match cast.filter(|k| !k.starts_with("array") && !k.starts_with("set") && !k.starts_with("object")) {
                    Some(kind) => {
                        clauses.push(format!("{field} {sql_op} <{kind}>${name}"));
                        binds.push((name, Value::String(text)));
                    }
                    None => {
                        clauses.push(format!("{field} {sql_op} ${name}"));
                        binds.push((name, loose_value(&text)));
                    }
                }
            }
        }
    }
    if clauses.is_empty() {
        return Ok((String::new(), binds));
    }
    Ok((format!(" WHERE {}", clauses.join(" AND ")), binds))
}

/// The types filters are cast by, only when there is a filter to cast.
async fn filter_kinds(db: &dyn ScheduleDb, table: &str, filter: Option<&str>) -> AdminResult<HashMap<String, String>> {
    match filter {
        Some(_) => Ok(field_kinds(&table_structure(db, table).await?)),
        None => Ok(HashMap::new()),
    }
}

fn order_clause(table: &str, req: &Request<'_>, known: &HashSet<(String, String)>) -> AdminResult<String> {
    let Some(sort) = param(req, "sort") else { return Ok(String::new()) };
    let sort = checked_filter_field(table, sort, known)?;
    let direction = if param(req, "dir") == Some("desc") { "DESC" } else { "ASC" };
    Ok(format!(" ORDER BY {sort} {direction}"))
}

/// `GET tables/:table/rows?limit&start&sort&dir&filter`.
async fn list_rows(db: &dyn ScheduleDb, table: &str, req: &Request<'_>) -> AdminResult<Value> {
    let table = checked_table(table)?;
    let limit = number_param(req, "limit").unwrap_or(DEFAULT_ROWS_PAGE).clamp(1, MAX_ROWS_PAGE);
    let start = number_param(req, "start").unwrap_or(0);
    let filter = param(req, "filter");
    let known = access_credentials(&db_info(db).await?);
    let kinds = filter_kinds(db, table, filter).await?;
    let (clause, binds) = where_clause(table, filter, &kinds, &known)?;
    let order = order_clause(table, req, &known)?;
    let surql = format!(
        "SELECT * FROM {table}{clause}{order} LIMIT {} START {start} TIMEOUT {READ_TIMEOUT};",
        limit + 1
    );
    let mut found = first_rows(db, &surql, &as_binds(&binds)).await?;
    let has_more = found.len() > limit;
    found.truncate(limit);
    let cleaned: Vec<Value> = found.into_iter().map(|row| clean_row(table, row, &known, LIST_CELL_BYTES)).collect();
    Ok(json!({ "rows": cleaned, "has_more": has_more, "limit": limit, "start": start }))
}

/// `GET tables/:table/count?filter`: asked for, because it is a scan. Running
/// past its timeout is an answer the page shows, not a failure.
async fn count_rows(db: &dyn ScheduleDb, table: &str, req: &Request<'_>) -> AdminResult<Value> {
    let table = checked_table(table)?;
    let filter = param(req, "filter");
    let known = access_credentials(&db_info(db).await?);
    let kinds = filter_kinds(db, table, filter).await?;
    let (clause, binds) = where_clause(table, filter, &kinds, &known)?;
    let surql = format!("SELECT count() FROM {table}{clause} GROUP ALL TIMEOUT {COUNT_TIMEOUT};");
    match first_rows(db, &surql, &as_binds(&binds)).await {
        Ok(found) => Ok(json!({ "count": found.first().and_then(|r| r["count"].as_u64()).unwrap_or(0) })),
        Err(e) if e.status == 504 => Ok(json!({ "count": null, "timed_out": true })),
        Err(e) => Err(e),
    }
}

/// `GET tables/:table/row?id`: one whole row, for the editor.
async fn get_row(db: &dyn ScheduleDb, table: &str, req: &Request<'_>) -> AdminResult<Value> {
    let table = checked_table(table)?;
    let id = checked_id(table, param(req, "id"))?;
    let known = access_credentials(&db_info(db).await?);
    let found = first_rows(db, "SELECT * FROM <record>$id;", &[("id", json!(id))]).await?;
    let row = found.into_iter().next().ok_or_else(|| AdminError::new(404, format!("{id} does not exist")))?;
    Ok(json!({ "row": clean_row(table, row, &known, ROW_CELL_BYTES) }))
}

/// `POST tables/:table/rows` with `{id?, set}`. A relation row takes its ends
/// from `set.in` and `set.out`.
async fn create_row(db: &dyn ScheduleDb, table: &str, req: &Request<'_>) -> AdminResult<Value> {
    let table = writable_table(table)?;
    let body = body_object(req)?;
    let empty = Map::new();
    let set = body.get("set").and_then(Value::as_object).unwrap_or(&empty);
    let info = db_info(db).await?;
    let known = access_credentials(&info);
    let Some(entry) = table_entry(&info).find(|t| t["name"] == table) else {
        return Err(AdminError::new(404, format!("No table named '{table}'")));
    };
    let relation = entry.pointer("/kind/kind").and_then(Value::as_str) == Some("RELATION");
    let kinds = field_kinds(&table_structure(db, table).await?);
    let (parts, binds) = assignments(table, set, &[], &kinds, &known, false)?;
    let mut bound = as_binds(&binds);
    let key = body.get("id").and_then(Value::as_str).map(str::trim).filter(|k| !k.is_empty());

    let surql = if relation {
        if !set.contains_key("in") || !set.contains_key("out") {
            return Err(AdminError::new(400, "A relation row needs both ends: in and out"));
        }
        let content = parts.iter().map(|(f, e)| format!("{f}: {e}")).collect::<Vec<_>>().join(", ");
        format!("INSERT RELATION INTO {table} {{ {content} }} RETURN AFTER;")
    } else {
        let target = match key {
            Some(key) => {
                bound.push(("key", json!(key)));
                bound.push(("tb", json!(table)));
                "type::record($tb, $key)".to_string()
            }
            None => table.to_string(),
        };
        if parts.is_empty() {
            format!("CREATE {target} RETURN AFTER;")
        } else {
            format!("CREATE {target} SET {} RETURN AFTER;", set_clause(&parts))
        }
    };
    let created = first_rows(db, &surql, &bound).await?;
    Ok(json!({ "row": created.into_iter().next().map(|r| clean_row(table, r, &known, ROW_CELL_BYTES)) }))
}

/// `PUT tables/:table/row?id` with `{set, unset}`. Touches only what it names,
/// so a form never writes back a value it was shown cut down.
async fn update_row(db: &dyn ScheduleDb, table: &str, req: &Request<'_>) -> AdminResult<Value> {
    let table = writable_table(table)?;
    let id = checked_id(table, param(req, "id"))?;
    let body = body_object(req)?;
    let empty = Map::new();
    let set = body.get("set").and_then(Value::as_object).unwrap_or(&empty);
    let unset: Vec<String> = body
        .get("unset")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|v| v.as_str().map(str::to_string))
        .collect();
    if set.is_empty() && unset.is_empty() {
        return Err(AdminError::new(400, "Nothing to change"));
    }
    if set.contains_key("id") || unset.iter().any(|f| f == "id") {
        return Err(AdminError::new(400, "A record's id cannot be changed"));
    }
    let known = access_credentials(&db_info(db).await?);
    let kinds = field_kinds(&table_structure(db, table).await?);
    let (parts, binds) = assignments(table, set, &unset, &kinds, &known, false)?;
    let mut bound = as_binds(&binds);
    bound.push(("id", json!(id)));
    let surql = format!("UPDATE <record>$id SET {} RETURN AFTER;", set_clause(&parts));
    let updated = first_rows(db, &surql, &bound).await?;
    let row = updated.into_iter().next().ok_or_else(|| AdminError::new(404, format!("{id} does not exist")))?;
    Ok(json!({ "row": clean_row(table, row, &known, ROW_CELL_BYTES) }))
}

/// `DELETE tables/:table/row?id`.
async fn delete_row(db: &dyn ScheduleDb, table: &str, req: &Request<'_>) -> AdminResult<Value> {
    let table = writable_table(table)?;
    let id = checked_id(table, param(req, "id"))?;
    let gone = first_rows(db, "DELETE <record>$id RETURN BEFORE;", &[("id", json!(id))]).await?;
    Ok(json!({ "deleted": gone.len() }))
}

/// `POST tables/:table/rows/delete` with `{ids}`.
async fn delete_rows(db: &dyn ScheduleDb, table: &str, req: &Request<'_>) -> AdminResult<Value> {
    let table = writable_table(table)?;
    let body = body_object(req)?;
    let ids: Vec<String> = body
        .get("ids")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .map(|v| checked_id(table, v.as_str()))
        .collect::<AdminResult<_>>()?;
    if ids.is_empty() || ids.len() > MAX_BATCH {
        return Err(AdminError::new(400, format!("Between 1 and {MAX_BATCH} ids at a time")));
    }
    let results = run(
        db,
        "LET $records = $ids.map(|$i| <record>$i); DELETE $records RETURN BEFORE;",
        &[("ids", json!(ids))],
    )
    .await?;
    let deleted = results.last().and_then(Value::as_array).map_or(0, Vec::len);
    Ok(json!({ "deleted": deleted }))
}

// =============================================================
// The SQL editor
// =============================================================

/// Where a lone `SELECT` needs clauses added: past its `FROM`, at the top level
/// of the statement (not in a string, a comment or a subquery).
#[derive(Debug, Default)]
struct SelectShape {
    has_limit: bool,
    has_timeout: bool,
    /// Byte offset of the first clause that must come after `LIMIT`.
    after_limit: Option<usize>,
    /// Byte offset of the first clause that must come after `TIMEOUT`.
    after_timeout: Option<usize>,
    /// One past the last byte of code, so a trailing comment stays a comment.
    end: usize,
}

/// Read a statement's top-level words. `None` when it is not one `SELECT`.
fn select_shape(sql: &str) -> Option<SelectShape> {
    let bytes = sql.as_bytes();
    let mut shape = SelectShape::default();
    let (mut i, mut depth) = (0usize, 0i32);
    let mut words = 0usize;
    let mut past_from = false;
    while i < bytes.len() {
        let c = bytes[i];
        match c {
            b'\'' | b'"' | b'`' => {
                let quote = c;
                i += 1;
                while i < bytes.len() && bytes[i] != quote {
                    i += if bytes[i] == b'\\' { 2 } else { 1 };
                }
                i += 1;
                shape.end = i.min(bytes.len());
            }
            b'-' if bytes.get(i + 1) == Some(&b'-') => {
                i = sql[i..].find('\n').map_or(bytes.len(), |n| i + n);
            }
            b'/' if bytes.get(i + 1) == Some(&b'/') => {
                i = sql[i..].find('\n').map_or(bytes.len(), |n| i + n);
            }
            b'#' => {
                i = sql[i..].find('\n').map_or(bytes.len(), |n| i + n);
            }
            b'/' if bytes.get(i + 1) == Some(&b'*') => {
                i = sql[i + 2..].find("*/").map_or(bytes.len(), |n| i + 2 + n + 2);
            }
            b'(' | b'[' | b'{' => {
                depth += 1;
                i += 1;
                shape.end = i;
            }
            b')' | b']' | b'}' => {
                depth -= 1;
                i += 1;
                shape.end = i;
            }
            b';' => {
                // One statement only: anything but whitespace or comments after
                // it is a script, and a script keeps its own words.
                if depth == 0 && !only_trivia(&sql[i + 1..]) {
                    return None;
                }
                i += 1;
            }
            c if c.is_ascii_alphabetic() || c == b'_' => {
                let start = i;
                while i < bytes.len() && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_') {
                    i += 1;
                }
                let word = sql[start..i].to_ascii_uppercase();
                shape.end = i;
                if words == 0 && word != "SELECT" {
                    return None;
                }
                words += 1;
                if depth != 0 {
                    continue;
                }
                // `WHERE limit = 5` names a field: a clause is followed by its
                // value, not by an operator.
                let next = sql[i..].trim_start().chars().next().unwrap_or(';');
                let takes_number = next.is_ascii_digit() || next == '$';
                let takes_word = next.is_ascii_alphanumeric() || next == '_' || next == '$';
                match word.as_str() {
                    "FROM" => past_from = true,
                    "LIMIT" if past_from && takes_number => shape.has_limit = true,
                    "TIMEOUT" if past_from && takes_number => {
                        shape.has_timeout = true;
                        shape.after_limit.get_or_insert(start);
                    }
                    "START" if past_from && takes_number => {
                        shape.after_limit.get_or_insert(start);
                    }
                    "FETCH" | "VERSION" if past_from && takes_word => {
                        shape.after_limit.get_or_insert(start);
                    }
                    "TEMPFILES" | "PARALLEL" | "EXPLAIN" if past_from => {
                        shape.after_limit.get_or_insert(start);
                        shape.after_timeout.get_or_insert(start);
                    }
                    _ => {}
                }
            }
            c if c.is_ascii_whitespace() => i += 1,
            _ => {
                i += 1;
                shape.end = i;
            }
        }
    }
    (past_from && words > 0).then_some(shape)
}

fn only_trivia(rest: &str) -> bool {
    rest.lines().all(|line| {
        let line = line.trim();
        line.is_empty() || line.starts_with("--") || line.starts_with("//") || line.starts_with('#')
    })
}

/// A lone `SELECT` with a `LIMIT` and a `TIMEOUT` it did not have: a forgotten
/// limit on a large table would otherwise be loaded whole into the database, this
/// host and the browser. `None` when the statement is left as written. Returns the
/// statement and the limit it was given, if it was given one.
fn guarded_select(sql: &str, limit: usize) -> Option<(String, Option<usize>)> {
    let shape = select_shape(sql)?;
    if shape.has_limit && shape.has_timeout {
        return None;
    }
    let mut inserts: Vec<(usize, String)> = Vec::new();
    if !shape.has_limit {
        inserts.push((shape.after_limit.unwrap_or(shape.end), format!(" LIMIT {limit} ")));
    }
    if !shape.has_timeout {
        inserts.push((shape.after_timeout.unwrap_or(shape.end), format!(" TIMEOUT {SQL_TIMEOUT} ")));
    }
    let mut out = sql.to_string();
    // Back to front keeps the earlier offsets true. LIMIT never lands after
    // TIMEOUT: at one offset it goes in last, which puts it first.
    for (at, text) in inserts.into_iter().rev() {
        out.insert_str(at, &text);
    }
    Some((out, (!shape.has_limit).then_some(limit)))
}

/// A statement that never ran. Narrow on purpose: this re-runs a script, and a
/// script's own `THROW 'parse error'` has already done whatever came before it.
fn is_parse_error(e: &AdminError) -> bool {
    let m = e.message.to_ascii_lowercase();
    e.status == 400 && m.contains("parse error:") && !m.contains("an error occurred")
}

/// `POST query` with `{sql}`. As root, statements as written. One failure fails
/// the answer, and what ran before it has happened: SurrealDB commits statement
/// by statement outside a `BEGIN`.
async fn run_query(db: &dyn ScheduleDb, req: &Request<'_>) -> AdminResult<Value> {
    let body = body_object(req)?;
    let sql = body.get("sql").and_then(Value::as_str).map(str::trim).unwrap_or_default();
    if sql.is_empty() {
        return Err(AdminError::new(400, "There is nothing to run"));
    }
    if sql.len() > MAX_SQL_BYTES {
        return Err(AdminError::new(413, "That script is too long to run from here"));
    }
    // What was run, not what it said: a statement can hold a password or a user's
    // data, and the log is read by more people than the data is.
    let verb = sql.split(|c: char| !c.is_ascii_alphabetic()).next().unwrap_or_default();
    tracing::info!(by = %req.actor, verb, bytes = sql.len(), "SQL statement from the admin API");
    let guarded = guarded_select(sql, QUERY_LIMIT);
    let (results, limit_added) = match &guarded {
        Some((statement, added)) => match run(db, statement, &[]).await {
            Ok(results) => (results, *added),
            // The rewrite misread something (a table called `start`, say); a
            // parse error means nothing ran, so the original is safe to try.
            Err(e) if is_parse_error(&e) => (run(db, sql, &[]).await?, None),
            Err(e) => return Err(e),
        },
        None => (run(db, sql, &[]).await?, None),
    };

    let mut used = 0usize;
    let mut answers = Vec::with_capacity(results.len());
    for result in results {
        let size = result.to_string().len();
        if used + size > MAX_QUERY_BYTES {
            let total: f64 = (used + size) as f64 / (1024.0 * 1024.0);
            return Err(AdminError::new(413, format!("The result is {total:.0} MB, more than this editor shows. Select fewer rows or fields.")));
        }
        used += size;
        answers.push(match result {
            Value::Array(mut items) => {
                let total = items.len();
                items.truncate(MAX_QUERY_ROWS);
                json!({ "rows": items, "total": total, "truncated": total > MAX_QUERY_ROWS })
            }
            value => json!({ "value": value }),
        });
    }
    Ok(json!({ "results": answers, "limit_added": limit_added.map(|_| MAX_QUERY_ROWS) }))
}

// =============================================================
// Users
// =============================================================

/// Where an app keeps its users, read off a record access method's `SIGNIN`:
/// `SELECT * FROM user WHERE username = $username AND
/// crypto::argon2::compare(password, $password)` names all of it.
#[derive(Debug, Clone, PartialEq)]
struct UserSource {
    access: String,
    table: String,
    identifier: Option<String>,
    password: Option<String>,
    algorithm: Option<String>,
}

/// The table after `FROM`, past an `ONLY`. Not a word followed by `::`, which is
/// a function: `FROM type::table('user')` names no table this can read.
fn table_after_from(text: &str) -> Option<&str> {
    let at = text.to_ascii_uppercase().find(" FROM ")?;
    let mut rest = text[at + " FROM ".len()..].trim_start();
    if let Some(after) = rest.strip_prefix("ONLY ").or_else(|| rest.strip_prefix("only ")) {
        rest = after.trim_start();
    }
    let end = rest.find(|c: char| !(c.is_ascii_alphanumeric() || c == '_')).unwrap_or(rest.len());
    let word = &rest[..end];
    (!word.is_empty() && !rest[end..].starts_with("::")).then_some(word)
}

fn parse_signin(access: &str, signin: &str) -> Option<UserSource> {
    let table = table_after_from(signin)?;
    if !is_plain_identifier(table) {
        return None;
    }
    // crypto::<algorithm>::compare(<field>, $<password>), the field possibly on a
    // variable the block read it into: `$user.pw`.
    let (mut password, mut algorithm) = (None, None);
    if let Some(at) = signin.find("crypto::") {
        let rest = &signin[at + "crypto::".len()..];
        if let Some((name, args)) = rest.split_once("::compare(") {
            if is_plain_identifier(name) {
                let field = args.split(',').next().unwrap_or("").trim();
                let field = field.rsplit('.').next().unwrap_or(field);
                if is_plain_identifier(field) {
                    password = Some(field.to_string());
                    algorithm = Some(name.to_string());
                }
            }
        }
    }
    // The first `<field> = $<param>`.
    let after_where = signin.to_ascii_uppercase().find(" WHERE ").map_or(signin, |at| &signin[at + 7..]);
    let identifier = after_where.split_once(" = $").and_then(|(before, _)| {
        let name = before.rsplit(|c: char| !(c.is_ascii_alphanumeric() || c == '_')).next()?;
        is_plain_identifier(name).then(|| name.to_string())
    });
    Some(UserSource { access: access.to_string(), table: table.to_string(), identifier, password, algorithm })
}

/// The app's user tables, one per record access method it defines itself.
fn user_sources(info: &Value) -> Vec<UserSource> {
    info.get("accesses")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|access| {
            let name = access.get("name")?.as_str()?;
            if name.starts_with("_00_") || access.pointer("/kind/kind")?.as_str()? != "RECORD" {
                return None;
            }
            parse_signin(name, access.pointer("/kind/signin")?.as_str()?)
        })
        .collect()
}

const IDENTIFIER_FIELDS: &[&str] = &["username", "email", "login", "handle", "name"];
const SEARCH_FIELDS: &[&str] = &["username", "email", "name", "display_name", "displayName", "full_name", "handle", "login", "phone"];
const CREATED_FIELDS: &[&str] = &["created_at", "createdAt", "created", "signed_up_at", "joined_at"];

/// What the user pages work from: the chosen source, every source, the table's
/// field types. `None` for a project with no users to manage.
struct UserContext {
    source: UserSource,
    sources: Vec<String>,
    kinds: HashMap<String, String>,
    known: HashSet<(String, String)>,
    /// The first column of each of the table's indexes: what sorts for free.
    indexed: HashSet<String>,
}

impl UserContext {
    fn search_fields(&self) -> Vec<&str> {
        let table = self.source.table.as_str();
        let mut out: Vec<&str> = SEARCH_FIELDS.iter().copied().filter(|f| self.kinds.contains_key(*f)).collect();
        if let Some(identifier) = self.source.identifier.as_deref() {
            if !out.contains(&identifier) {
                out.push(identifier);
            }
        }
        out.retain(|f| !is_credential(table, f, &self.known));
        out
    }

    fn created(&self) -> Option<&'static str> {
        CREATED_FIELDS.iter().copied().find(|f| self.kinds.contains_key(*f))
    }

    fn describe(&self) -> Value {
        json!({
            "access": self.source.access,
            "table": self.source.table,
            "identifier": self.source.identifier,
            "created_field": self.created(),
            "created_indexed": self.created().is_some_and(|f| self.indexed.contains(f)),
            "search_fields": self.search_fields(),
            // Whether this API knows how to set a password that signs in.
            "can_set_password": self.source.password.is_some() && self.source.algorithm.is_some(),
            "sources": self.sources,
        })
    }
}

async fn user_context(db: &dyn ScheduleDb, req: &Request<'_>) -> AdminResult<Option<UserContext>> {
    let info = db_info(db).await?;
    let known = access_credentials(&info);
    // An access method can name a table that is not one: `FROM thing`, a view.
    let exists = |table: &str| table_entry(&info).any(|t| t["name"] == table);
    let mut sources: Vec<UserSource> = user_sources(&info).into_iter().filter(|s| exists(&s.table)).collect();
    let names: Vec<String> = sources.iter().map(|s| s.access.clone()).collect();
    let wanted = param(req, "source");
    let picked = match wanted {
        Some(name) => sources.iter().position(|s| s.access == name),
        None => (!sources.is_empty()).then_some(0),
    };
    let source = match picked {
        Some(i) => sources.swap_remove(i),
        // An access method this could not read: a `user` table is still users.
        None if exists("user") => UserSource {
            access: String::new(),
            table: "user".to_string(),
            identifier: None,
            password: None,
            algorithm: None,
        },
        None => return Ok(None),
    };
    let structure = table_structure(db, &source.table).await?;
    let kinds = field_kinds(&structure);
    let indexed: HashSet<String> = structure
        .get("indexes")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|ix| ix.pointer("/cols/0")?.as_str().map(str::to_string))
        .collect();
    let mut source = source;
    if source.identifier.as_deref().is_none_or(|f| !kinds.contains_key(f)) {
        source.identifier = IDENTIFIER_FIELDS.iter().find(|f| kinds.contains_key(**f)).map(|f| f.to_string());
    }
    Ok(Some(UserContext { source, sources: names, kinds, known, indexed }))
}

async fn required_context(db: &dyn ScheduleDb, req: &Request<'_>) -> AdminResult<UserContext> {
    user_context(db, req)
        .await?
        .ok_or_else(|| AdminError::new(404, "This project defines no record access or user table"))
}

/// Who is live right now, and on how many tabs and views. Best effort: a project
/// without the table, or a slow answer, leaves the page without the figures.
async fn presence_of(db: &dyn ScheduleDb, ids: Option<&[String]>) -> Option<HashMap<String, (usize, usize)>> {
    let surql = match ids {
        Some(_) => "SELECT auth_id, clientId AS client_id FROM _00_query \
                    WHERE auth_id IN $ids AND lastActiveAt + ttl > time::now() LIMIT 20000 TIMEOUT 4s;",
        None => "SELECT auth_id, clientId AS client_id FROM _00_query \
                 WHERE lastActiveAt + ttl > time::now() LIMIT 20000 TIMEOUT 4s;",
    };
    let binds = ids.map(|ids| ("ids", json!(ids)));
    let bound: Vec<(&str, Value)> = binds.into_iter().collect();
    let found = first_rows(db, surql, &bound).await.ok()?;
    let mut grouped: HashMap<String, (usize, BTreeSet<String>)> = HashMap::new();
    for row in found {
        let Some(auth) = row.get("auth_id").and_then(Value::as_str).filter(|a| !a.is_empty() && *a != "anon") else {
            continue;
        };
        let entry = grouped.entry(auth.to_string()).or_default();
        entry.0 += 1;
        if let Some(client) = row.get("client_id").and_then(Value::as_str) {
            entry.1.insert(client.to_string());
        }
    }
    Some(grouped.into_iter().map(|(auth, (views, clients))| (auth, (views, clients.len()))).collect())
}

async fn admin_ids(db: &dyn ScheduleDb) -> Option<HashSet<String>> {
    let found = first_rows(db, "SELECT VALUE <string>user FROM _00_admin LIMIT 2000 TIMEOUT 4s;", &[]).await.ok()?;
    Some(found.into_iter().filter_map(|v| v.as_str().map(str::to_string)).collect())
}

/// `GET users?q&filter&limit&start&sort&dir&source`.
async fn list_users(db: &dyn ScheduleDb, req: &Request<'_>) -> AdminResult<Value> {
    let Some(cx) = user_context(db, req).await? else {
        return Ok(json!({ "source": null, "users": [], "has_more": false }));
    };
    let table = cx.source.table.as_str();
    let limit = number_param(req, "limit").unwrap_or(DEFAULT_ROWS_PAGE).clamp(1, MAX_ROWS_PAGE);
    let start = number_param(req, "start").unwrap_or(0);
    let filter = param(req, "filter");
    let admins = admin_ids(db).await;

    let mut binds: Vec<(&str, Value)> = Vec::new();
    let clause = match param(req, "q").map(|q| q.trim().to_lowercase()).filter(|q| !q.is_empty()) {
        Some(q) => {
            binds.push(("q", json!(q)));
            let mut tests = vec!["string::contains(string::lowercase(<string>id), $q)".to_string()];
            tests.extend(
                cx.search_fields()
                    .iter()
                    .map(|f| format!("string::contains(string::lowercase(<string>({f} OR '')), $q)")),
            );
            format!(" WHERE {}", tests.join(" OR "))
        }
        None => String::new(),
    };
    // Newest first when an index makes that free. Without one it means sorting the
    // whole table on every open, so it waits to be asked for.
    let order = match param(req, "sort") {
        Some(_) => order_clause(table, req, &cx.known)?,
        None => cx
            .created()
            .filter(|f| cx.indexed.contains(*f))
            .map(|f| format!(" ORDER BY {f} DESC"))
            .unwrap_or_default(),
    };

    // A short list of known ids reads those rows; it does not scan for them.
    let narrowed: Option<Vec<String>> = match filter {
        Some("admins") => Some(admins.iter().flatten().cloned().collect()),
        Some("online") => Some(presence_of(db, None).await.unwrap_or_default().into_keys().collect()),
        _ => None,
    };
    let tail = format!("{clause}{order} LIMIT {} START {start} TIMEOUT {READ_TIMEOUT};", limit + 1);
    let mut found = match narrowed {
        Some(ids) => {
            let prefix = format!("{table}:");
            let ids: Vec<String> = ids.into_iter().filter(|id| id.starts_with(&prefix)).collect();
            if ids.is_empty() {
                Vec::new()
            } else {
                binds.push(("ids", json!(ids)));
                let results = run(db, &format!("LET $records = $ids.map(|$i| <record>$i); SELECT * FROM $records{tail}"), &binds).await?;
                results.into_iter().last().and_then(|v| v.as_array().cloned()).unwrap_or_default()
            }
        }
        None => first_rows(db, &format!("SELECT * FROM {table}{tail}"), &binds).await?,
    };
    let has_more = found.len() > limit;
    found.truncate(limit);

    let page: Vec<String> = found.iter().filter_map(|r| r["id"].as_str().map(str::to_string)).collect();
    let presence = presence_of(db, Some(&page)).await;
    let users: Vec<Value> = found
        .into_iter()
        .map(|row| {
            let id = row["id"].as_str().unwrap_or_default().to_string();
            let live = presence.as_ref().map(|p| p.get(&id).copied().unwrap_or((0, 0)));
            json!({
                "row": clean_row(table, row, &cx.known, LIST_CELL_BYTES),
                "is_admin": admins.as_ref().map(|a| a.contains(&id)),
                "views": live.map(|l| l.0),
                "sessions": live.map(|l| l.1),
            })
        })
        .collect();
    Ok(json!({ "source": cx.describe(), "users": users, "has_more": has_more, "limit": limit, "start": start }))
}

/// `GET users/stats?source`: three reads, each allowed to come back empty.
async fn user_stats(db: &dyn ScheduleDb, req: &Request<'_>) -> AdminResult<Value> {
    let Some(cx) = user_context(db, req).await? else { return Ok(json!({ "total": null, "admins": null, "online": null })) };
    let table = cx.source.table.as_str();
    let total = first_rows(db, &format!("SELECT count() FROM {table} GROUP ALL TIMEOUT {COUNT_TIMEOUT};"), &[])
        .await
        .ok()
        .map(|found| found.first().and_then(|r| r["count"].as_u64()).unwrap_or(0));
    let prefix = format!("{table}:");
    let admins = admin_ids(db).await.map(|ids| ids.iter().filter(|id| id.starts_with(&prefix)).count());
    let online = presence_of(db, None).await.map(|live| live.keys().filter(|id| id.starts_with(&prefix)).count());
    Ok(json!({ "total": total, "admins": admins, "online": online }))
}

/// `GET users/user?id`: the row, the roster entry, live sessions, feature flags.
async fn user_detail(db: &dyn ScheduleDb, req: &Request<'_>) -> AdminResult<Value> {
    let cx = required_context(db, req).await?;
    let table = cx.source.table.as_str();
    let id = checked_id(table, param(req, "id"))?;
    let bound = [("id", json!(id))];
    let found = first_rows(db, "SELECT * FROM <record>$id;", &bound).await?;
    let row = found.into_iter().next().ok_or_else(|| AdminError::new(404, format!("{id} does not exist")))?;

    let admin = first_rows(db, "SELECT note, added_at FROM _00_admin WHERE user = <record>$id LIMIT 1;", &bound)
        .await
        .ok()
        .map(|found| found.into_iter().next());
    let sessions = first_rows(
        db,
        "SELECT clientId AS client_id, count() AS views FROM _00_query \
         WHERE auth_id = $id AND lastActiveAt + ttl > time::now() GROUP BY client_id TIMEOUT 4s;",
        &bound,
    )
    .await
    .ok();
    let flags = first_rows(
        db,
        "SELECT key, variant FROM _00_user_feature WHERE user = <record>$id ORDER BY key LIMIT 200 TIMEOUT 4s;",
        &bound,
    )
    .await
    .ok();
    let features = user_features(db, &id, flags.as_deref().unwrap_or_default()).await;
    Ok(json!({
        "source": cx.describe(),
        "row": clean_row(table, row, &cx.known, ROW_CELL_BYTES),
        "is_admin": admin.as_ref().map(Option::is_some),
        "admin": admin.flatten(),
        "sessions": sessions,
        "flags": flags,
        "features": features,
        // The evaluator materializes rows for `user` records only.
        "features_assignable": table == FLAG_USER_TABLE,
    }))
}

/// Every defined flag with what this user gets and the variant they are pinned
/// to, if any. `None` when flags cannot be read, an older schema among others.
async fn user_features(db: &dyn ScheduleDb, id: &str, assigned: &[Value]) -> Option<Vec<Value>> {
    let uid = canonical_id(db, id).await.ok()?;
    let flags = first_rows(
        db,
        "SELECT key, description, variants, default_variant, enabled, rules FROM _00_feature_flag \
         ORDER BY key LIMIT 500 TIMEOUT 4s;",
        &[],
    )
    .await
    .ok()?;
    let variant_of = |key: &str| assigned.iter().find(|a| a["key"] == key).map(|a| a["variant"].clone());
    Some(
        flags
            .into_iter()
            .filter_map(|flag| {
                let key = flag["key"].as_str()?.to_string();
                let rules = rules_of(&flag);
                Some(json!({
                    "key": key,
                    "description": flag.get("description"),
                    "variants": flag.get("variants"),
                    "default_variant": flag.get("default_variant"),
                    "enabled": flag.get("enabled").and_then(Value::as_bool).unwrap_or(true),
                    "variant": variant_of(&key),
                    "pinned": pinned_variant(&rules, &uid),
                }))
            })
            .collect(),
    )
}

/// Whether a request's id is the signed-in user's own record, however it is
/// spelled: `user:alice`, `` user:`alice` `` and `user:⟨alice⟩` are one record.
async fn is_self(db: &dyn ScheduleDb, table: &str, id: &str, actor: &str) -> AdminResult<bool> {
    if !actor.starts_with(&format!("{table}:")) {
        return Ok(false);
    }
    let found = run(db, "RETURN <record>$id = <record>$actor;", &[("id", json!(id)), ("actor", json!(actor))]).await?;
    Ok(found.first() == Some(&Value::Bool(true)))
}

/// A cleanup statement on an internal table a project may not have.
fn is_missing_table(e: &AdminError) -> bool {
    let m = e.message.to_ascii_lowercase();
    m.contains("table") && m.contains("does not exist")
}

fn checked_password(body: &Map<String, Value>) -> AdminResult<&str> {
    let password = body.get("password").and_then(Value::as_str).unwrap_or_default();
    if password.len() < 8 || password.len() > 1024 {
        return Err(AdminError::new(400, "A password of at least 8 characters is required"));
    }
    Ok(password)
}

fn hash_expression(source: &UserSource) -> AdminResult<(&str, String)> {
    match (source.password.as_deref(), source.algorithm.as_deref()) {
        (Some(field), Some(algorithm)) => Ok((field, format!("crypto::{algorithm}::generate($password)"))),
        _ => Err(AdminError::new(
            501,
            "This project's SIGNIN clause does not show how passwords are checked, so one cannot be set from here",
        )),
    }
}

/// `POST users?source` with `{identifier, password, fields?}`, written as the
/// app's `SIGNIN` expects it. The app's `SIGNUP` block does not run: required
/// fields the form did not send are the database's to refuse.
async fn create_user(db: &dyn ScheduleDb, req: &Request<'_>) -> AdminResult<Value> {
    let cx = required_context(db, req).await?;
    let table = cx.source.table.as_str();
    let body = body_object(req)?;
    let password = checked_password(&body)?;
    let (password_field, hash) = hash_expression(&cx.source)?;
    let identifier_field = cx.source.identifier.as_deref().ok_or_else(|| AdminError::new(501, "This user table has no identifying field to set"))?;
    let identifier = body.get("identifier").and_then(Value::as_str).map(str::trim).unwrap_or_default();
    if identifier.is_empty() {
        return Err(AdminError::new(400, format!("{identifier_field} is required")));
    }

    let empty = Map::new();
    let mut set = body.get("fields").and_then(Value::as_object).unwrap_or(&empty).clone();
    set.insert(identifier_field.to_string(), json!(identifier));
    let (mut parts, binds) = assignments(table, &set, &[], &cx.kinds, &cx.known, false)?;
    parts.push((password_field.to_string(), hash));
    let mut bound = as_binds(&binds);
    bound.push(("password", json!(password)));
    let created = first_rows(db, &format!("CREATE {table} SET {} RETURN AFTER;", set_clause(&parts)), &bound).await?;
    Ok(json!({ "row": created.into_iter().next().map(|r| clean_row(table, r, &cx.known, ROW_CELL_BYTES)) }))
}

/// `PUT users/user/password?id` with `{password}`. Sessions already open keep
/// working until their token runs out: SurrealDB has nothing to revoke them with.
async fn reset_password(db: &dyn ScheduleDb, req: &Request<'_>) -> AdminResult<Value> {
    let cx = required_context(db, req).await?;
    let id = checked_id(&cx.source.table, param(req, "id"))?;
    let body = body_object(req)?;
    let password = checked_password(&body)?;
    let (field, hash) = hash_expression(&cx.source)?;
    let updated = first_rows(
        db,
        &format!("UPDATE <record>$id SET {field} = {hash} RETURN VALUE id;"),
        &[("id", json!(id)), ("password", json!(password))],
    )
    .await?;
    if updated.is_empty() {
        return Err(AdminError::new(404, format!("{id} does not exist")));
    }
    Ok(json!({ "id": id, "password_set": true }))
}

/// `PUT users/user/admin?id` with `{admin, note?}`: what `spky admin add` and
/// `remove` do.
async fn set_admin(db: &dyn ScheduleDb, req: &Request<'_>) -> AdminResult<Value> {
    let cx = required_context(db, req).await?;
    let id = checked_id(&cx.source.table, param(req, "id"))?;
    let body = body_object(req)?;
    let admin = body.get("admin").and_then(Value::as_bool).ok_or_else(|| AdminError::new(400, "admin must be true or false"))?;
    if !admin {
        if is_self(db, &cx.source.table, &id, req.actor).await? {
            return Err(AdminError::new(400, "You cannot remove yourself from the roster"));
        }
        first_rows(db, "DELETE _00_admin WHERE user = <record>$id;", &[("id", json!(id))]).await?;
        return Ok(json!({ "id": id, "is_admin": false }));
    }
    let exists = first_rows(db, "SELECT VALUE id FROM <record>$id;", &[("id", json!(id))]).await?;
    if exists.is_empty() {
        return Err(AdminError::new(404, format!("{id} does not exist")));
    }
    // Asked again with no note: the one it has stays.
    match body.get("note").and_then(Value::as_str).filter(|n| !n.is_empty()) {
        Some(note) => {
            let binds = [("id", json!(id)), ("note", json!(note))];
            first_rows(db, "UPSERT _00_admin SET user = <record>$id, note = $note WHERE user = <record>$id;", &binds).await?;
        }
        None => {
            first_rows(db, "UPSERT _00_admin SET user = <record>$id WHERE user = <record>$id;", &[("id", json!(id))]).await?;
        }
    }
    Ok(json!({ "id": id, "is_admin": true }))
}

/// `DELETE users/user?id`: what only exists because of the row, then the row. In
/// that order: a roster entry left behind would make whoever gets this id next an
/// admin. Rows of the app that point at the user stay; the app's own events decide
/// those.
async fn delete_user(db: &dyn ScheduleDb, req: &Request<'_>) -> AdminResult<Value> {
    let cx = required_context(db, req).await?;
    let id = checked_id(&cx.source.table, param(req, "id"))?;
    if is_self(db, &cx.source.table, &id, req.actor).await? {
        return Err(AdminError::new(400, "You cannot delete your own account from here"));
    }
    let bound = [("id", json!(id))];
    if first_rows(db, "SELECT VALUE id FROM <record>$id;", &bound).await?.is_empty() {
        return Err(AdminError::new(404, format!("{id} does not exist")));
    }
    for surql in ["DELETE _00_admin WHERE user = <record>$id;", "DELETE _00_user_feature WHERE user = <record>$id;"] {
        match first_rows(db, surql, &bound).await {
            Ok(_) => {}
            Err(e) if is_missing_table(&e) => {}
            Err(e) => return Err(e),
        }
    }
    let gone = first_rows(db, "DELETE <record>$id RETURN BEFORE;", &bound).await?;
    Ok(json!({ "deleted": gone.len() }))
}

// =============================================================
// Feature flags
// =============================================================
//
// A flag is a row of `_00_feature_flag`; what each user gets is a row of
// `_00_user_feature`, the table clients sync. The rules are the ones `spky flag`
// writes: an allowlist per variant (priority 10) and a rollout per variant
// (priority 50). Every assignment is made by the project's `fn::feature::assign`,
// the evaluator the CLI's and the scheduler's are pinned against, so whichever
// tool changed a flag, a user lands on the same variant.
//
// A change to a flag's rules re-evaluates every user, which posts each changed
// row to the SSP inside its transaction: on Cloudflare that is most of a second
// per user. So the routes that change rules answer at once with `evaluate`, and
// the console walks `flags/flag/evaluate` page by page, a bounded request each,
// with a progress bar. Pinning one user re-evaluates that user only.

/// The table `fn::feature::*` and the scheduler's sweep evaluate flags for.
const FLAG_USER_TABLE: &str = "user";
const MAX_FLAG_KEY_BYTES: usize = 100;
const MAX_VARIANT_BYTES: usize = 64;
const MAX_VARIANTS: usize = 16;
/// The priorities `spky flag` writes: an allowlist beats a rollout.
const PRIORITY_ALLOWLIST: i64 = 10;
const PRIORITY_ROLLOUT: i64 = 50;
/// Users one `flags/flag/evaluate` request walks, by default and at most.
const DEFAULT_EVALUATE_PAGE: usize = 100;
const MAX_EVALUATE_PAGE: usize = 500;
/// Pinned users a flag's page lists by name.
const MAX_PINS_SHOWN: usize = 500;
/// Read-modify-write attempts on a flag's rules before giving up.
const FLAG_WRITE_ATTEMPTS: usize = 4;
/// Read with every write: a write applies only while it still matches.
const FLAG_SELECT: &str = "SELECT key, description, variants, default_variant, enabled, rules, payloads, created_at, updated_at, \
     <string>(updated_at ?? '') AS stamp FROM _00_feature_flag WHERE key = $key LIMIT 1;";

/// A flag key as clients name it in code: `beta`, `checkout.v2`, `ui/new-nav`.
fn checked_flag_key(key: Option<&str>) -> AdminResult<&str> {
    let key = key.map(str::trim).unwrap_or_default();
    let ok = !key.is_empty()
        && key.len() <= MAX_FLAG_KEY_BYTES
        && key.starts_with(|c: char| c.is_ascii_alphanumeric())
        && key.chars().all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '-' | '.' | ':' | '/'));
    if ok {
        Ok(key)
    } else {
        Err(AdminError::new(400, "A flag key is letters, digits and _ - . : /, starting with a letter or digit, at most 100 characters"))
    }
}

fn checked_variants(value: &Value) -> AdminResult<Vec<String>> {
    let list = value.as_array().ok_or_else(|| AdminError::new(400, "variants must be a list of names"))?;
    let mut out: Vec<String> = Vec::with_capacity(list.len());
    for v in list {
        let name = v.as_str().map(str::trim).unwrap_or_default();
        if name.is_empty() || name.len() > MAX_VARIANT_BYTES || name.contains(',') {
            return Err(AdminError::new(400, "A variant is a name of 1 to 64 characters, without commas"));
        }
        if out.iter().any(|seen| seen == name) {
            return Err(AdminError::new(400, format!("Variant '{name}' is listed twice")));
        }
        out.push(name.to_string());
    }
    if out.len() < 2 || out.len() > MAX_VARIANTS {
        return Err(AdminError::new(400, "A flag has between 2 and 16 variants"));
    }
    Ok(out)
}

fn string_list(value: Option<&Value>) -> Vec<String> {
    value.and_then(Value::as_array).into_iter().flatten().filter_map(|v| v.as_str().map(str::to_string)).collect()
}

fn rules_of(flag: &Value) -> Vec<Value> {
    flag.get("rules").and_then(Value::as_array).cloned().unwrap_or_default()
}

fn rule_kind(rule: &Value) -> &str {
    rule.get("kind").and_then(Value::as_str).unwrap_or_default()
}

/// An allowlist's members as strings. `rules[].users` holds strings, as `spky
/// flag` writes them; a hand-written record reads back as its id string too.
fn rule_users(rule: &Value) -> Vec<String> {
    string_list(rule.get("users"))
}

/// The variant `uid` is allowlisted into. A user on two lists gets the first by
/// priority, then by position, as the evaluator ranks them.
fn pinned_variant(rules: &[Value], uid: &str) -> Option<String> {
    let mut lists: Vec<(i64, usize, &Value)> = rules
        .iter()
        .enumerate()
        .filter(|(_, r)| rule_kind(r) == "allowlist" && rule_users(r).iter().any(|u| u == uid))
        .map(|(i, r)| (r.get("priority").and_then(Value::as_i64).unwrap_or(100), i, r))
        .collect();
    lists.sort_by_key(|(priority, i, _)| (*priority, *i));
    lists.first().and_then(|(_, _, r)| r.get("variant")?.as_str().map(str::to_string))
}

/// `rules` with `uid` off every allowlist, then on `variant`'s when given. A list
/// emptied by this goes, as `spky flag unset` leaves it.
fn pin_rules(rules: &[Value], uid: &str, variant: Option<&str>) -> Vec<Value> {
    let mut out: Vec<Value> = Vec::with_capacity(rules.len() + 1);
    for rule in rules {
        if rule_kind(rule) != "allowlist" {
            out.push(rule.clone());
            continue;
        }
        let users = rule_users(rule);
        if !users.iter().any(|u| u == uid) {
            out.push(rule.clone());
            continue;
        }
        let rest: Vec<String> = users.into_iter().filter(|u| u != uid).collect();
        if !rest.is_empty() {
            let mut kept = rule.clone();
            kept["users"] = json!(rest);
            out.push(kept);
        }
    }
    let Some(variant) = variant else { return out };
    match out.iter_mut().find(|r| rule_kind(r) == "allowlist" && r["variant"] == variant) {
        Some(rule) => {
            let mut users = rule_users(rule);
            users.push(uid.to_string());
            rule["users"] = json!(users);
        }
        None => out.push(json!({ "kind": "allowlist", "variant": variant, "users": [uid], "priority": PRIORITY_ALLOWLIST })),
    }
    out
}

/// `rules` with its rollouts replaced by `rollouts`, `(variant, percent)` in the
/// order given. Each rollout takes the users hashed below its percent who are
/// not taken already, so two rollouts of 20 and 30 split 20 and 10.
fn rollout_rules(rules: &[Value], rollouts: &[(String, i64)]) -> Vec<Value> {
    let mut out: Vec<Value> = rules.iter().filter(|r| rule_kind(r) != "rollout").cloned().collect();
    out.extend(
        rollouts
            .iter()
            .filter(|(_, percent)| *percent > 0)
            .map(|(variant, percent)| json!({ "kind": "rollout", "variant": variant, "percent": percent, "priority": PRIORITY_ROLLOUT })),
    );
    out
}

fn checked_rollouts(value: &Value, variants: &[String]) -> AdminResult<Vec<(String, i64)>> {
    let list = value.as_array().ok_or_else(|| AdminError::new(400, "rollouts must be a list of {variant, percent}"))?;
    let mut out: Vec<(String, i64)> = Vec::with_capacity(list.len());
    for item in list {
        let variant = item.get("variant").and_then(Value::as_str).unwrap_or_default();
        if !variants.iter().any(|v| v == variant) {
            return Err(AdminError::new(400, format!("'{variant}' is not a variant of this flag")));
        }
        if out.iter().any(|(seen, _)| seen == variant) {
            return Err(AdminError::new(400, format!("'{variant}' has two rollouts")));
        }
        let percent = item.get("percent").and_then(Value::as_i64).filter(|p| (0..=100).contains(p));
        let percent = percent.ok_or_else(|| AdminError::new(400, "A rollout's percent is a whole number from 0 to 100"))?;
        out.push((variant.to_string(), percent));
    }
    Ok(out)
}

/// What a page shows of a flag. Allowlists are counted, not listed: one can hold
/// every user of the app.
fn flag_summary(flag: &Value) -> Value {
    let rules = rules_of(flag);
    let pinned: BTreeSet<String> = rules.iter().filter(|r| rule_kind(r) == "allowlist").flat_map(rule_users).collect();
    let rollouts: Vec<Value> = rules
        .iter()
        .filter(|r| rule_kind(r) == "rollout")
        .map(|r| json!({ "variant": r.get("variant"), "percent": r.get("percent").and_then(Value::as_i64).unwrap_or(0) }))
        .collect();
    json!({
        "key": flag.get("key"),
        "description": flag.get("description"),
        "variants": flag.get("variants"),
        "default_variant": flag.get("default_variant"),
        "enabled": flag.get("enabled").and_then(Value::as_bool).unwrap_or(true),
        "rollouts": rollouts,
        "pinned": pinned.len(),
        // Rules no route here writes: kept as they are, counted so a page can say so.
        "other_rules": rules.iter().filter(|r| !matches!(rule_kind(r), "allowlist" | "rollout")).count(),
        "created_at": flag.get("created_at"),
        "updated_at": flag.get("updated_at"),
    })
}

/// The database's own spelling of a record id: what `fn::feature::*` compares
/// allowlists against. `user:alice` and `` user:`alice` `` are one record.
async fn canonical_id(db: &dyn ScheduleDb, id: &str) -> AdminResult<String> {
    let out = run(db, "RETURN <string><record>$id;", &[("id", json!(id))]).await?;
    out.into_iter().next().and_then(|v| v.as_str().map(str::to_string)).ok_or_else(|| AdminError::new(400, format!("'{id}' is not a record id")))
}

async fn load_flag(db: &dyn ScheduleDb, key: &str) -> AdminResult<Value> {
    match first_rows(db, FLAG_SELECT, &[("key", json!(key))]).await {
        Ok(found) => found.into_iter().next().ok_or_else(|| AdminError::new(404, format!("No flag '{key}'"))),
        Err(e) if is_missing_table(&e) => Err(AdminError::new(404, "This project's schema has no feature flags yet; deploy it with a current spky")),
        Err(e) => Err(e),
    }
}

/// An error from `fn::feature::*` that only means the project's functions are
/// older than this API.
fn evaluator_error(e: AdminError) -> AdminError {
    let m = e.message.to_ascii_lowercase();
    if m.contains("fn::feature::") && (m.contains("does not exist") || m.contains("not found")) {
        return AdminError::new(
            409,
            "This project's feature flag functions predate the console. Redeploy it (spky deploy) to update them, then try again.",
        );
    }
    e
}

/// Read the flag, let `change` decide the new fields from it, write them while
/// the flag is still the one read. `change` answers `None` to write nothing.
/// Another writer in between, an admin in the DevTools or `spky flag`, makes the
/// write miss, and it is decided again from what they left.
async fn write_flag<F>(db: &dyn ScheduleDb, key: &str, mut change: F) -> AdminResult<Value>
where
    F: FnMut(&Value) -> AdminResult<Option<Vec<(&'static str, Value)>>>,
{
    for _ in 0..FLAG_WRITE_ATTEMPTS {
        let flag = load_flag(db, key).await?;
        let Some(set) = change(&flag)? else { return Ok(flag) };
        let stamp = flag.get("stamp").cloned().unwrap_or(Value::Null);
        let mut binds: Vec<(&str, Value)> = vec![("key", json!(key)), ("stamp", stamp)];
        let mut parts: Vec<String> = Vec::with_capacity(set.len());
        for (field, value) in set {
            // An absent description is NONE; a bound null would fail option<string>.
            if value.is_null() {
                parts.push(format!("{field} = NONE"));
            } else {
                parts.push(format!("{field} = ${field}"));
                binds.push((field, value));
            }
        }
        let surql = format!(
            "UPDATE _00_feature_flag SET {} WHERE key = $key AND <string>(updated_at ?? '') = $stamp RETURN VALUE key;",
            parts.join(", ")
        );
        match first_rows(db, &surql, &binds).await {
            Ok(written) if !written.is_empty() => return load_flag(db, key).await,
            Ok(_) => continue,
            Err(e) if e.message.to_ascii_lowercase().contains("conflict") => continue,
            Err(e) => return Err(e),
        }
    }
    Err(AdminError::new(409, "The flag kept changing while this was saved. Try again."))
}

/// Whether the project's schema has flags, and the functions that evaluate them.
fn flag_schema(info: &Value) -> (bool, bool) {
    let installed = table_entry(info).any(|t| t["name"] == "_00_feature_flag");
    let evaluator = info
        .get("functions")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .any(|f| f["name"].as_str().is_some_and(|n| n.trim_start_matches("fn::") == "feature::assign"));
    (installed, evaluator)
}

/// `GET flags`.
async fn list_flags(db: &dyn ScheduleDb) -> AdminResult<Value> {
    let info = db_info(db).await?;
    let (installed, evaluator) = flag_schema(&info);
    let users_table = table_entry(&info).any(|t| t["name"] == FLAG_USER_TABLE);
    let flags = if installed {
        first_rows(
            db,
            &format!(
                "SELECT key, description, variants, default_variant, enabled, rules, created_at, updated_at \
                 FROM _00_feature_flag ORDER BY key LIMIT 500 TIMEOUT {READ_TIMEOUT};"
            ),
            &[],
        )
        .await?
    } else {
        Vec::new()
    };
    Ok(json!({
        "installed": installed,
        "evaluator": evaluator,
        "users_table": users_table,
        "flags": flags.iter().map(flag_summary).collect::<Vec<_>>(),
    }))
}

/// `GET flags/flag?key`: the flag, how many users have each variant, and who is
/// pinned, by name where the users page would show one.
async fn flag_detail(db: &dyn ScheduleDb, req: &Request<'_>) -> AdminResult<Value> {
    let key = checked_flag_key(param(req, "key"))?;
    let flag = load_flag(db, key).await?;
    let rules = rules_of(&flag);

    // Best effort: the index leads with the user, so this reads the table.
    let counts = first_rows(
        db,
        "SELECT variant, count() AS n FROM _00_user_feature WHERE key = $key GROUP BY variant TIMEOUT 4s;",
        &[("key", json!(key))],
    )
    .await
    .ok()
    .map(|found| {
        found
            .into_iter()
            .filter_map(|r| Some((r["variant"].as_str()?.to_string(), json!(r["n"].as_u64().unwrap_or(0)))))
            .collect::<Map<String, Value>>()
    });

    let mut pins: Vec<(String, String)> = Vec::new();
    for rule in rules.iter().filter(|r| rule_kind(r) == "allowlist") {
        let variant = rule.get("variant").and_then(Value::as_str).unwrap_or_default();
        for user in rule_users(rule) {
            if !pins.iter().any(|(u, _)| *u == user) {
                pins.push((user, variant.to_string()));
            }
        }
    }
    let total = pins.len();
    pins.truncate(MAX_PINS_SHOWN);
    let names = pin_names(db, req, &pins).await;
    let pins: Vec<Value> = pins
        .into_iter()
        .map(|(id, variant)| {
            let found = names.as_ref().map(|n| n.get(&id).cloned());
            json!({
                "id": id,
                "variant": variant,
                "name": found.clone().flatten().flatten(),
                // Known only when the names were read: a deleted user stays on a list.
                "exists": found.map(|f| f.is_some()),
            })
        })
        .collect();

    let mut out = flag_summary(&flag);
    out["payloads"] = flag.get("payloads").cloned().unwrap_or(Value::Null);
    Ok(json!({ "flag": out, "counts": counts, "pins": pins, "pins_total": total }))
}

/// `id -> identifier` for the pinned users that still exist, `None` when they
/// could not be read.
async fn pin_names(db: &dyn ScheduleDb, req: &Request<'_>, pins: &[(String, String)]) -> Option<HashMap<String, Option<String>>> {
    if pins.is_empty() {
        return Some(HashMap::new());
    }
    let cx = user_context(db, req).await.ok().flatten();
    let field = cx
        .as_ref()
        .filter(|cx| cx.source.table == FLAG_USER_TABLE)
        .and_then(|cx| cx.source.identifier.clone().filter(|f| !is_credential(FLAG_USER_TABLE, f, &cx.known)));
    let projection = field.as_deref().unwrap_or("NONE");
    let ids: Vec<&str> = pins.iter().map(|(id, _)| id.as_str()).filter(|id| id.starts_with("user:")).collect();
    let results = run(
        db,
        &format!("LET $records = $ids.map(|$i| <record>$i); SELECT <string>id AS id, {projection} AS name FROM $records TIMEOUT 4s;"),
        &[("ids", json!(ids))],
    )
    .await
    .ok()?;
    let found = results.into_iter().last().and_then(|v| v.as_array().cloned()).unwrap_or_default();
    Some(
        found
            .into_iter()
            .filter_map(|r| Some((r["id"].as_str()?.to_string(), r["name"].as_str().map(str::to_string))))
            .collect(),
    )
}

/// `POST flags` with `{key, description?, variants?, default_variant?}`, as `spky
/// flag create` makes one: on, no rules, everyone on the default. Answers
/// `evaluate: true`: no user has a row for it yet.
async fn create_flag(db: &dyn ScheduleDb, req: &Request<'_>) -> AdminResult<Value> {
    let body = body_object(req)?;
    let key = checked_flag_key(body.get("key").and_then(Value::as_str))?;
    let variants = match body.get("variants") {
        Some(v) if !v.is_null() => checked_variants(v)?,
        _ => vec!["off".to_string(), "on".to_string()],
    };
    let default = match body.get("default_variant").and_then(Value::as_str).filter(|d| !d.is_empty()) {
        Some(d) if variants.iter().any(|v| v == d) => d.to_string(),
        Some(d) => return Err(AdminError::new(400, format!("The default '{d}' is not one of the variants"))),
        None => variants[0].clone(),
    };
    let description = body.get("description").and_then(Value::as_str).map(str::trim).filter(|d| !d.is_empty());

    match first_rows(db, "SELECT VALUE key FROM _00_feature_flag WHERE key = $key LIMIT 1;", &[("key", json!(key))]).await {
        Ok(found) if !found.is_empty() => return Err(AdminError::new(409, format!("A flag '{key}' exists already"))),
        Ok(_) => {}
        Err(e) if is_missing_table(&e) => return Err(AdminError::new(409, "This project's schema has no feature flags yet; deploy it with a current spky")),
        Err(e) => return Err(e),
    }
    let mut binds = vec![("key", json!(key)), ("variants", json!(variants)), ("default", json!(default))];
    let description_sql = match description {
        Some(d) => {
            binds.push(("description", json!(d)));
            "$description"
        }
        None => "NONE",
    };
    first_rows(
        db,
        &format!(
            "CREATE _00_feature_flag SET key = $key, variants = $variants, default_variant = $default, \
             description = {description_sql}, rules = [], enabled = true RETURN VALUE key;"
        ),
        &binds,
    )
    .await?;
    let flag = load_flag(db, key).await?;
    Ok(json!({ "flag": flag_summary(&flag), "evaluate": true }))
}

/// `PUT flags/flag?key` with any of `{enabled, description, default_variant,
/// variants, rollouts: [{variant, percent}]}`. `rollouts` replaces every rollout
/// rule; allowlists move through `flags/flag/user`. `evaluate` says whether
/// assignments can have moved.
async fn update_flag(db: &dyn ScheduleDb, req: &Request<'_>) -> AdminResult<Value> {
    let key = checked_flag_key(param(req, "key"))?;
    let body = body_object(req)?;
    let enabled = match body.get("enabled") {
        None => None,
        Some(v) => Some(v.as_bool().ok_or_else(|| AdminError::new(400, "enabled must be true or false"))?),
    };
    let variants = match body.get("variants") {
        None => None,
        Some(v) => Some(checked_variants(v)?),
    };
    let description = match body.get("description") {
        None => None,
        Some(Value::Null) => Some(None),
        Some(Value::String(d)) => Some(Some(d.trim().to_string()).filter(|d| !d.is_empty())),
        Some(_) => return Err(AdminError::new(400, "description must be text or null")),
    };
    let default = match body.get("default_variant") {
        None => None,
        Some(v) => Some(v.as_str().filter(|d| !d.is_empty()).ok_or_else(|| AdminError::new(400, "default_variant must be a variant's name"))?.to_string()),
    };
    let rollouts = body.get("rollouts");
    let evaluate = enabled.is_some() || default.is_some() || rollouts.is_some();

    let flag = write_flag(db, key, |flag| {
        let current = string_list(flag.get("variants"));
        let names = variants.clone().unwrap_or(current);
        let default_now = default.clone().or_else(|| flag["default_variant"].as_str().map(str::to_string)).unwrap_or_default();
        if !names.contains(&default_now) {
            return Err(AdminError::new(400, format!("The default '{default_now}' is not one of the variants")));
        }
        let mut rules = rules_of(flag);
        if let Some(list) = rollouts {
            rules = rollout_rules(&rules, &checked_rollouts(list, &names)?);
        }
        // A rule naming a variant that is gone would hand out a variant clients
        // were never told about.
        if let Some(missing) = rules.iter().filter_map(|r| r.get("variant")?.as_str()).find(|v| !names.iter().any(|n| n == v)) {
            return Err(AdminError::new(400, format!("A rule still hands out '{missing}'; remove it before the variant")));
        }
        let mut set: Vec<(&'static str, Value)> = Vec::new();
        if let Some(on) = enabled {
            set.push(("enabled", json!(on)));
        }
        if let Some(d) = &description {
            set.push(("description", d.as_ref().map_or(Value::Null, |d| json!(d))));
        }
        if let Some(d) = &default {
            set.push(("default_variant", json!(d)));
        }
        if variants.is_some() {
            set.push(("variants", json!(names)));
        }
        if rollouts.is_some() {
            set.push(("rules", Value::Array(rules)));
        }
        Ok((!set.is_empty()).then_some(set))
    })
    .await?;
    Ok(json!({ "flag": flag_summary(&flag), "evaluate": evaluate }))
}

/// `DELETE flags/flag?key`: the flag and every assignment of it, as `spky flag
/// delete` does. A client still asking for it gets its own fallback.
async fn delete_flag(db: &dyn ScheduleDb, req: &Request<'_>) -> AdminResult<Value> {
    let key = checked_flag_key(param(req, "key"))?;
    load_flag(db, key).await?;
    let bound = [("key", json!(key))];
    run(db, "DELETE _00_feature_flag WHERE key = $key; DELETE _00_user_feature WHERE key = $key;", &bound).await?;
    Ok(json!({ "deleted": key }))
}

/// `POST flags/flag/evaluate?key&after&limit`: re-evaluate one page of users,
/// in id order from after `after`. Answers `next`, the `after` of the next page,
/// null once every user has been walked, and on the first page the total to
/// walk. An assignment that does not move is rewritten byte for byte, so it costs
/// no SSP round trip (see `fn::feature::assign`).
async fn evaluate_flag(db: &dyn ScheduleDb, req: &Request<'_>) -> AdminResult<Value> {
    let key = checked_flag_key(param(req, "key"))?;
    let limit = number_param(req, "limit").unwrap_or(DEFAULT_EVALUATE_PAGE).clamp(1, MAX_EVALUATE_PAGE);
    let after = param(req, "after");
    let mut binds = vec![("key", json!(key))];
    let range = match after {
        Some(id) => {
            checked_id(FLAG_USER_TABLE, Some(id))?;
            binds.push(("after", json!(id)));
            " WHERE id > <record>$after"
        }
        None => "",
    };
    let surql = format!(
        "LET $flag = (SELECT * FROM ONLY _00_feature_flag WHERE key = $key LIMIT 1);
         LET $ids = (SELECT VALUE id FROM {FLAG_USER_TABLE}{range} ORDER BY id LIMIT {limit});
         IF $flag != NONE {{
             LET $ranked = fn::feature::rules($flag);
             FOR $u IN $ids {{ fn::feature::assign($flag, $ranked, $u); }};
         }};
         RETURN {{ found: $flag != NONE, ids: array::len($ids), last: <string>(array::last($ids) ?? '') }};"
    );
    let out = run(db, &surql, &binds).await.map_err(evaluator_error)?;
    let page = out.into_iter().last().unwrap_or(Value::Null);
    if page["found"] != true {
        return Err(AdminError::new(404, format!("No flag '{key}'")));
    }
    let walked = page["ids"].as_u64().unwrap_or(0) as usize;
    let next = page["last"].as_str().filter(|_| walked == limit).filter(|l| !l.is_empty());
    let total = match after {
        None => first_rows(db, &format!("SELECT count() FROM {FLAG_USER_TABLE} GROUP ALL TIMEOUT {COUNT_TIMEOUT};"), &[])
            .await
            .ok()
            .map(|found| found.first().and_then(|r| r["count"].as_u64()).unwrap_or(0)),
        Some(_) => None,
    };
    Ok(json!({ "evaluated": walked, "next": next, "total": total }))
}

/// `PUT flags/flag/user?key&id` with `{variant}`: pin the user to that variant,
/// or with `{variant: null}` unpin them, back to the rollouts and the default.
/// Then re-evaluate that one user: nobody else's variant can have moved.
async fn pin_user(db: &dyn ScheduleDb, req: &Request<'_>) -> AdminResult<Value> {
    let key = checked_flag_key(param(req, "key"))?;
    let id = checked_id(FLAG_USER_TABLE, param(req, "id"))
        .map_err(|_| AdminError::new(400, "Flags are evaluated for records of the user table only"))?;
    let body = body_object(req)?;
    let variant = match body.get("variant") {
        Some(Value::String(v)) => Some(v.as_str()),
        Some(Value::Null) => None,
        _ => return Err(AdminError::new(400, "variant must be a variant's name, or null to unpin")),
    };
    // A deleted user can stay on a list; they can be taken off it, not put on.
    let exists = !first_rows(db, "SELECT VALUE id FROM <record>$id;", &[("id", json!(id))]).await?.is_empty();
    if !exists && variant.is_some() {
        return Err(AdminError::new(404, format!("{id} does not exist")));
    }
    let uid = canonical_id(db, &id).await?;
    write_flag(db, key, |flag| {
        if let Some(v) = variant {
            if !string_list(flag.get("variants")).iter().any(|n| n == v) {
                return Err(AdminError::new(400, format!("'{v}' is not a variant of {key}")));
            }
        }
        let rules = rules_of(flag);
        let next = pin_rules(&rules, &uid, variant);
        Ok((next != rules).then(|| vec![("rules", Value::Array(next))]))
    })
    .await?;
    if !exists {
        // Evaluating them would write an assignment for a user who is gone.
        return Ok(json!({ "key": key, "id": uid, "pinned": Value::Null, "variant": Value::Null }));
    }
    let out = run(
        db,
        "LET $flag = (SELECT * FROM ONLY _00_feature_flag WHERE key = $key LIMIT 1);
         RETURN fn::feature::assign($flag, fn::feature::rules($flag), <record>$id);",
        &[("key", json!(key)), ("id", json!(id))],
    )
    .await
    .map_err(evaluator_error)?;
    Ok(json!({ "key": key, "id": uid, "pinned": variant, "variant": out.into_iter().last() }))
}

// =============================================================
// Buckets
// =============================================================

/// `sp00ky:maxSize=<bytes>` in a bucket's comment: the limit the client enforces.
fn max_size_of(comment: Option<&str>) -> Option<u64> {
    let text = comment?.split("sp00ky:maxSize=").nth(1)?;
    let digits: String = text.chars().take_while(char::is_ascii_digit).collect();
    digits.parse().ok()
}

/// `BACKEND "r2"` buckets are an empty memory bucket in the database plus this
/// function (see `apps/cli/src/bucket.rs`); their files live next to the SSP.
fn is_r2(info: &Value, bucket: &str) -> bool {
    let function = format!("_00_bucket::{bucket}");
    info.get("functions")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .any(|f| f.get("name").and_then(Value::as_str) == Some(function.as_str()))
}

fn describe_bucket(entry: &Value, info: &Value, dirs: Option<&dyn BucketDirs>) -> Value {
    let name = entry.get("name").and_then(Value::as_str).unwrap_or_default();
    let raw = entry.get("backend").and_then(Value::as_str).unwrap_or("global");
    let r2 = is_r2(info, name);
    let kind = if r2 {
        "r2"
    } else if raw.starts_with("file:") {
        "file"
    } else {
        raw
    };
    let comment = entry.get("comment").and_then(Value::as_str);
    let location = raw.strip_prefix("file:");
    let on_disk = !r2 && location.is_some_and(|l| dirs.is_some_and(|d| d.covers(l)));
    json!({
        "name": name,
        "backend": kind,
        "location": location,
        "readonly": entry.get("readonly").and_then(Value::as_bool).unwrap_or(false),
        "max_bytes": max_size_of(comment),
        // Its database listing would be empty and a file put there would vanish
        // on restart; the files are in the SSP's store.
        "browsable": !r2,
        // SurrealDB's file store lists one directory and leaves its
        // subdirectories out, so without the disk a folder there is only found
        // by its path.
        "lists_folders": kind != "file" || on_disk,
    })
}

/// `GET buckets`.
async fn list_buckets(db: &dyn ScheduleDb, dirs: Option<&dyn BucketDirs>) -> AdminResult<Value> {
    let info = db_info(db).await?;
    let mut buckets: Vec<Value> = info
        .get("buckets")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .map(|b| describe_bucket(b, &info, dirs))
        .collect();
    buckets.sort_by(|a, b| a["name"].as_str().cmp(&b["name"].as_str()));
    Ok(json!({
        "buckets": buckets,
        "max_read_bytes": MAX_FILE_READ_BYTES,
        "default_upload_bytes": DEFAULT_UPLOAD_BYTES,
    }))
}

fn checked_bucket(name: &str) -> AdminResult<&str> {
    if is_plain_identifier(name) {
        Ok(name)
    } else {
        Err(AdminError::new(400, format!("'{name}' is not a bucket name")))
    }
}

/// A key without its leading slash, once it cannot escape the bucket.
fn checked_key(key: Option<&str>) -> AdminResult<String> {
    let key = key.unwrap_or_default().trim_start_matches('/');
    if key.is_empty() || key.ends_with('/') {
        return Err(AdminError::new(400, "A file key is required"));
    }
    if key.len() > MAX_KEY_BYTES || key.contains('\0') || key.split('/').any(|s| s.is_empty() || s == "." || s == "..") {
        return Err(AdminError::new(400, format!("'{key}' is not a usable file key")));
    }
    Ok(key.to_string())
}

/// A folder as a listing prefix: `a/b/`, or empty for the bucket's root.
fn checked_prefix(prefix: Option<&str>) -> AdminResult<String> {
    let trimmed = prefix.unwrap_or_default().trim_matches('/');
    if trimmed.is_empty() {
        return Ok(String::new());
    }
    Ok(format!("{}/", checked_key(Some(trimmed))?))
}

/// Past nearly every key under a folder: `a/b/` + the highest character a binding
/// can carry everywhere. The SSP node inlines its bindings as text, and SurrealQL
/// refuses `char::MAX` there. A name sorting past this one (an emoji) is met again
/// and read as what it is.
fn past_folder(prefix: &str, folder: &str) -> String {
    format!("{prefix}{folder}/\u{FFFD}")
}

/// `GET buckets/:bucket/files?prefix&start&limit`: one folder's files and
/// subfolders. Buckets have no folders, only keys, and `file::list` has no
/// delimiter, so a page is read in key order and each folder found is stepped
/// over with `start`. A folder of a million files costs one round trip.
async fn list_files(db: &dyn ScheduleDb, bucket: &str, req: &Request<'_>) -> AdminResult<Value> {
    let bucket = checked_bucket(bucket)?;
    let prefix = checked_prefix(param(req, "prefix"))?;
    let limit = number_param(req, "limit").unwrap_or(DEFAULT_FILE_PAGE).clamp(1, MAX_FILE_PAGE);
    if let Some(dirs) = req.dirs {
        let info = db_info(db).await?;
        let location = info
            .get("buckets")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .find(|b| b["name"] == bucket)
            .and_then(|b| b.get("backend")?.as_str()?.strip_prefix("file:"))
            .filter(|l| dirs.covers(l))
            .map(str::to_string);
        if let Some(location) = location {
            return list_dir(dirs, &location, &prefix, param(req, "start"), limit).await;
        }
    }
    let mut cursor = param(req, "start").map(str::to_string);
    let expected = format!("/{prefix}");

    let mut folders: BTreeSet<String> = BTreeSet::new();
    let mut files: Vec<Value> = Vec::new();
    let mut more = false;
    'rounds: for round in 0..MAX_LIST_ROUNDS {
        let mut options = Map::new();
        options.insert("limit".into(), json!(limit + 1));
        if !prefix.is_empty() {
            options.insert("prefix".into(), json!(prefix));
        }
        if let Some(start) = &cursor {
            options.insert("start".into(), json!(start));
        }
        let page = first_rows(
            db,
            "RETURN file::list($bucket, $options).map(|$f| { key: file::key($f.file), size: $f.size, updated: <string>$f.updated });",
            &[("bucket", json!(bucket)), ("options", Value::Object(options))],
        )
        .await?;
        let short = page.len() <= limit;
        let mut stepped_over_folder = false;
        for entry in page {
            let key = entry["key"].as_str().unwrap_or_default().to_string();
            let Some(rest) = key.strip_prefix(expected.as_str()) else {
                cursor = Some(key);
                continue;
            };
            // `start` is exclusive, so the cursor only moves past what this
            // page really returns.
            if files.len() + folders.len() >= limit {
                more = true;
                break 'rounds;
            }
            cursor = Some(key.clone());
            match rest.split_once('/') {
                Some((folder, _)) => {
                    if folders.insert(folder.to_string()) {
                        cursor = Some(past_folder(&prefix, folder));
                        stepped_over_folder = true;
                        break;
                    }
                }
                None => files.push(json!({
                    "key": key.trim_start_matches('/'),
                    "name": rest,
                    "size": entry["size"],
                    "updated": entry["updated"],
                })),
            }
        }
        if !stepped_over_folder && short {
            cursor = None;
            break;
        }
        if round + 1 == MAX_LIST_ROUNDS {
            more = true;
        }
    }
    Ok(json!({
        "prefix": prefix,
        "folders": folders,
        "files": files,
        "next": if more { cursor } else { None },
    }))
}

/// A folder off the disk, in the shape the database's listing answers with.
async fn list_dir(dirs: &dyn BucketDirs, location: &str, prefix: &str, start: Option<&str>, limit: usize) -> AdminResult<Value> {
    // `start` and `next` are keys, here as in the database's listing, so a page
    // still follows on after the host gains or loses the volume.
    let start = start.and_then(|s| s.strip_prefix(prefix));
    let page = dirs
        .list(location, prefix, start, limit)
        .await
        .map_err(|e| AdminError::new(502, format!("Could not read this folder from the bucket volume: {e}")))?;
    let counts: Map<String, Value> = page.folders.iter().map(|(name, n)| (name.clone(), json!(n))).collect();
    let files: Vec<Value> = page
        .files
        .iter()
        .map(|f| json!({ "key": format!("{prefix}{}", f.name), "name": f.name, "size": f.size, "updated": f.updated }))
        .collect();
    Ok(json!({
        "prefix": prefix,
        "folders": page.folders.iter().map(|(name, _)| name).collect::<Vec<_>>(),
        "counts": counts,
        "files": files,
        "next": page.next.as_ref().map(|name| format!("{prefix}{name}")),
    }))
}

/// `GET buckets/:bucket/file?key`: metadata and base64 content. One wire shape on
/// every host (the SSP node's responses are JSON or text), and the page turns it
/// into a Blob itself, so a file never renders on this API's own origin.
async fn read_file(db: &dyn ScheduleDb, bucket: &str, req: &Request<'_>) -> AdminResult<Value> {
    let bucket = checked_bucket(bucket)?;
    let key = checked_key(param(req, "key"))?;
    let binds = [("bucket", json!(bucket)), ("key", json!(key))];
    let head = run(db, "RETURN file::head(type::file($bucket, $key)).{ size, updated: <string>updated };", &binds)
        .await?
        .into_iter()
        .next()
        .filter(|v| v.get("size").is_some_and(|s| !s.is_null()))
        .ok_or_else(|| AdminError::new(404, format!("{key} does not exist in {bucket}")))?;
    let size = head["size"].as_u64().unwrap_or(0);
    let mut out = json!({ "key": key, "size": size, "updated": head["updated"] });
    if param(req, "meta").is_some() {
        return Ok(out);
    }
    if size > MAX_FILE_READ_BYTES {
        return Err(AdminError::new(
            413,
            format!("{key} is {} MB; this API reads files up to {} MB", size / (1024 * 1024), MAX_FILE_READ_BYTES / (1024 * 1024)),
        ));
    }
    let data = run(db, "RETURN encoding::base64::encode(type::file($bucket, $key).get());", &binds).await?;
    out["data"] = data.into_iter().next().unwrap_or(Value::Null);
    Ok(out)
}

/// A bucket's `BACKEND "r2"` files are not in the database; refuse a write that
/// would land in the empty memory bucket standing in for them.
async fn writable_bucket<'a>(db: &dyn ScheduleDb, bucket: &'a str) -> AdminResult<&'a str> {
    let bucket = checked_bucket(bucket)?;
    let info = db_info(db).await?;
    let entry = info
        .get("buckets")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .find(|b| b["name"] == bucket)
        .ok_or_else(|| AdminError::new(404, format!("No bucket named '{bucket}'")))?;
    if is_r2(&info, bucket) {
        return Err(AdminError::new(501, format!("{bucket} keeps its files in R2 next to the SSP, not in the database")));
    }
    if entry.get("readonly").and_then(Value::as_bool) == Some(true) {
        return Err(AdminError::new(403, format!("{bucket} is defined READONLY")));
    }
    Ok(bucket)
}

/// `*_if_not_exists` leave a key in use alone without saying so, which would
/// read as success: the 409 comes from asking first. They are still what writes,
/// so a key taken between the two is left alone too.
async fn refuse_if_taken(db: &dyn ScheduleDb, bucket: &str, key: &str) -> AdminResult<()> {
    let found = run(
        db,
        "RETURN file::exists(type::file($bucket, $key));",
        &[("bucket", json!(bucket)), ("key", json!(key))],
    )
    .await?;
    if found.first() == Some(&Value::Bool(true)) {
        return Err(AdminError::new(409, format!("{key} already exists")));
    }
    Ok(())
}

/// `PUT buckets/:bucket/file?key&overwrite` with `{data}` in base64. Without
/// `overwrite` a key already in use answers 409.
async fn write_file(db: &dyn ScheduleDb, bucket: &str, req: &Request<'_>) -> AdminResult<Value> {
    let bucket = writable_bucket(db, bucket).await?;
    let key = checked_key(param(req, "key"))?;
    let body = body_object(req)?;
    let data = body.get("data").and_then(Value::as_str).ok_or_else(|| AdminError::new(400, "data (base64) is required"))?;
    let overwrite = param(req, "overwrite").is_some();
    if !overwrite {
        refuse_if_taken(db, bucket, &key).await?;
    }
    let function = if overwrite { "file::put" } else { "file::put_if_not_exists" };
    let surql = format!("RETURN {function}(type::file($bucket, $key), encoding::base64::decode($data));");
    run(db, &surql, &[("bucket", json!(bucket)), ("key", json!(key)), ("data", json!(data))]).await?;
    Ok(json!({ "key": key }))
}

/// `DELETE buckets/:bucket/file?key`.
async fn delete_file(db: &dyn ScheduleDb, bucket: &str, req: &Request<'_>) -> AdminResult<Value> {
    let bucket = writable_bucket(db, bucket).await?;
    let key = checked_key(param(req, "key"))?;
    run(db, "RETURN file::delete(type::file($bucket, $key));", &[("bucket", json!(bucket)), ("key", json!(key))]).await?;
    Ok(json!({ "deleted": 1 }))
}

/// `POST buckets/:bucket/delete` with `{keys}`, one statement each.
async fn delete_files(db: &dyn ScheduleDb, bucket: &str, req: &Request<'_>) -> AdminResult<Value> {
    let bucket = writable_bucket(db, bucket).await?;
    let body = body_object(req)?;
    let keys: Vec<String> = body
        .get("keys")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .map(|v| checked_key(v.as_str()))
        .collect::<AdminResult<_>>()?;
    if keys.is_empty() || keys.len() > MAX_BATCH {
        return Err(AdminError::new(400, format!("Between 1 and {MAX_BATCH} keys at a time")));
    }
    run(
        db,
        "RETURN $keys.map(|$k| file::delete(type::file($bucket, $k)));",
        &[("bucket", json!(bucket)), ("keys", json!(keys))],
    )
    .await?;
    Ok(json!({ "deleted": keys.len() }))
}

/// `POST buckets/:bucket/rename` with `{key, target}`. A key in use answers 409.
async fn rename_file(db: &dyn ScheduleDb, bucket: &str, req: &Request<'_>) -> AdminResult<Value> {
    let bucket = writable_bucket(db, bucket).await?;
    let body = body_object(req)?;
    let key = checked_key(body.get("key").and_then(Value::as_str))?;
    let target = checked_key(body.get("target").and_then(Value::as_str))?;
    refuse_if_taken(db, bucket, &target).await?;
    let binds = [("bucket", json!(bucket)), ("key", json!(key)), ("target", json!(target))];
    run(db, "RETURN file::rename_if_not_exists(type::file($bucket, $key), $target);", &binds).await?;
    Ok(json!({ "key": target }))
}

#[cfg(test)]
#[path = "admin_data_tests.rs"]
mod tests;
