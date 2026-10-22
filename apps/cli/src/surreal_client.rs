use anyhow::{bail, Context, Result};
use base64::Engine;
use serde::Deserialize;
use std::time::Duration;

/// Fail fast if the host accepts the socket but never completes the TCP/TLS
/// handshake (e.g. staging mid-restart).
const CONNECT_TIMEOUT: Duration = Duration::from_secs(15);
/// Default overall per-request deadline. Bounded so a SurrealDB / reverse-proxy
/// that accepts the connection but stalls the response can never hang the client
/// forever (the failure that stranded a deploy at `migrating` with `finalize`
/// never called). Overridable for unusually long migrations (large backfills).
const DEFAULT_REQUEST_TIMEOUT_SECS: u64 = 120;

/// Overall deadline for a single SurrealDB HTTP call, overridable via
/// `SPKY_DB_HTTP_TIMEOUT_SECS` (0 or unparseable falls back to the default).
fn request_timeout() -> Duration {
    let secs = std::env::var("SPKY_DB_HTTP_TIMEOUT_SECS")
        .ok()
        .and_then(|s| s.trim().parse::<u64>().ok())
        .filter(|n| *n > 0)
        .unwrap_or(DEFAULT_REQUEST_TIMEOUT_SECS);
    Duration::from_secs(secs)
}

#[derive(Debug, Deserialize)]
pub struct SurrealResponse {
    pub status: String,
    pub result: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct AppliedMigration {
    pub version: String,
    pub name: String,
    pub applied_at: String,
    pub checksum: String,
}

/// Trait abstracting the SurrealDB operations needed by the migration system.
/// This enables testing with a mock implementation.
pub trait MigrationDB {
    fn ping(&self) -> Result<()>;
    fn ensure_ns_db(&self) -> Result<()>;
    fn ensure_migration_table(&self) -> Result<()>;
    fn execute(&self, query: &str) -> Result<Vec<SurrealResponse>>;
    fn get_applied_migrations(&self) -> Result<Vec<AppliedMigration>>;
    fn record_migration(&self, version: &str, name: &str, checksum: &str) -> Result<()>;
    fn update_migration_checksum(&self, version: &str, new_checksum: &str) -> Result<()>;
}

pub struct SurrealClient {
    url: String,
    namespace: String,
    database: String,
    auth_header: String,
    /// Namespace the Basic-auth user is defined on, sent as `surreal-auth-ns`.
    /// `None` for a root user. SurrealDB only finds a namespace-level user (the
    /// free plan's `owner` on the shared DB) with this header; without it the
    /// sign-in fails with "There was a problem with authentication".
    auth_namespace: Option<String>,
}

/// Normalize a SurrealDB endpoint to an HTTP(S) URL for the `/sql` transport.
/// External endpoints (e.g. SurrealDB Cloud) are often given as `wss://…`/`ws://…`
/// for the RPC socket, but this client speaks HTTP — ureq rejects the `wss`/`ws`
/// scheme ("Unknown Scheme"). The host is identical, so swap the scheme.
/// Backtick-quote a SurrealDB identifier (namespace/database name) so names with
/// characters that aren't valid in a bare ident — most notably a hyphen, which
/// the parser would otherwise read as subtraction (`sp00ky-demo` → `sp00ky - demo`,
/// failing with "Cannot perform subtraction") — are treated as a single literal
/// name. Any backtick inside the name is escaped by doubling.
fn quote_ident(name: &str) -> String {
    format!("`{}`", name.replace('`', "``"))
}

fn to_http_url(url: &str) -> String {
    let trimmed = url.trim_end_matches('/');
    if let Some(rest) = trimmed.strip_prefix("wss://") {
        format!("https://{rest}")
    } else if let Some(rest) = trimmed.strip_prefix("ws://") {
        format!("http://{rest}")
    } else {
        trimmed.to_string()
    }
}

impl SurrealClient {
    pub fn new(url: &str, namespace: &str, database: &str, username: &str, password: &str) -> Self {
        let credentials = format!("{}:{}", username, password);
        let auth_header = format!(
            "Basic {}",
            base64::engine::general_purpose::STANDARD.encode(credentials)
        );
        Self {
            url: to_http_url(url),
            namespace: namespace.to_string(),
            database: database.to_string(),
            auth_header,
            auth_namespace: None,
        }
    }

    /// Sign in as a user defined on `auth_namespace` instead of a root user.
    /// `None` keeps root-level auth.
    pub fn with_auth_namespace(mut self, auth_namespace: Option<&str>) -> Self {
        self.auth_namespace = auth_namespace.map(str::to_string);
        self
    }

    /// Create a client without authentication (for unauthenticated SurrealDB instances).
    pub fn new_unauthenticated(url: &str, namespace: &str, database: &str) -> Self {
        Self {
            url: to_http_url(url),
            namespace: namespace.to_string(),
            database: database.to_string(),
            auth_header: String::new(),
            auth_namespace: None,
        }
    }
}

/// Send a raw SQL query to SurrealDB via HTTP, returning every parsed response
/// as-is (including those with `status == "ERR"`).
///
/// This is the transport-only half: it extracts the response body from HTTP
/// errors (so we see the actual SurrealDB message) but does NOT inspect the
/// per-statement status. Callers that want fail-fast behavior should use
/// `send_raw_sql`; callers that want to render every statement's outcome
/// themselves (e.g. the `query` REPL) use this directly.
fn send_raw_sql_unchecked(
    url: &str,
    auth_header: &str,
    auth_ns_header: Option<&str>,
    ns_header: Option<&str>,
    db_header: Option<&str>,
    query: &str,
) -> Result<Vec<SurrealResponse>> {
    // Bound every call: a connect timeout plus an overall request deadline so a
    // stalled response surfaces as an error instead of blocking forever.
    let agent = ureq::builder().timeout_connect(CONNECT_TIMEOUT).build();
    let mut req = agent
        .post(url)
        .timeout(request_timeout())
        .set("Accept", "application/json");

    if !auth_header.is_empty() {
        req = req.set("Authorization", auth_header);
    }
    if let Some(auth_ns) = auth_ns_header {
        req = req.set("surreal-auth-ns", auth_ns);
    }

    if let Some(ns) = ns_header {
        req = req.set("surreal-ns", ns);
    }
    if let Some(db) = db_header {
        req = req.set("surreal-db", db);
    }

    let resp = match req.send_string(query) {
        Ok(resp) => resp,
        Err(ureq::Error::Status(code, response)) => {
            let body = response.into_string().unwrap_or_default();
            bail!("SurrealDB returned HTTP {}: {}", code, body);
        }
        Err(ureq::Error::Transport(t)) => {
            // Timeouts land here too (io::ErrorKind::TimedOut). Spell it out so a
            // stall reads as "unreachable/timed out" rather than a generic error,
            // and point at the override knob.
            bail!(
                "Failed to reach SurrealDB at {} (connect/response timed out after {:?}; \
                 set SPKY_DB_HTTP_TIMEOUT_SECS to raise the limit for long migrations): {}",
                url,
                request_timeout(),
                t
            );
        }
    };

    let body: Vec<SurrealResponse> = resp
        .into_json()
        .context("Failed to parse SurrealDB response")?;

    Ok(body)
}

/// Send a raw SQL query to SurrealDB via HTTP, returning parsed responses.
///
/// This is the shared helper for all ureq call sites. It extracts the response
/// body from HTTP errors (so we see the actual SurrealDB message) and checks
/// for ERR status in the parsed response.
fn send_raw_sql(
    url: &str,
    auth_header: &str,
    auth_ns_header: Option<&str>,
    ns_header: Option<&str>,
    db_header: Option<&str>,
    query: &str,
) -> Result<Vec<SurrealResponse>> {
    let body = send_raw_sql_unchecked(
        url,
        auth_header,
        auth_ns_header,
        ns_header,
        db_header,
        query,
    )?;

    for r in &body {
        if r.status == "ERR" {
            let msg = r
                .result
                .as_ref()
                .and_then(|v| v.as_str())
                .unwrap_or("Unknown error");
            bail!("SurrealDB error: {}", msg);
        }
    }

    Ok(body)
}

impl SurrealClient {
    /// Returns the result of INFO FOR DB as a JSON value.
    pub fn info_for_db(&self) -> Result<serde_json::Value> {
        let responses = self
            .execute_query("INFO FOR DB;")
            .context("Failed to execute INFO FOR DB")?;
        let result = responses
            .into_iter()
            .next()
            .and_then(|r| r.result)
            .unwrap_or(serde_json::Value::Null);
        Ok(result)
    }

    /// Returns the result of INFO FOR TABLE <name> as a JSON value.
    pub fn info_for_table(&self, table: &str) -> Result<serde_json::Value> {
        let query = format!("INFO FOR TABLE {};", table);
        let responses = self
            .execute_query(&query)
            .context(format!("Failed to execute INFO FOR TABLE {}", table))?;
        let result = responses
            .into_iter()
            .next()
            .and_then(|r| r.result)
            .unwrap_or(serde_json::Value::Null);
        Ok(result)
    }

    /// Run an arbitrary SurrealQL batch and return EVERY statement's response,
    /// including those with `status == "ERR"`. Unlike `execute`, this does not
    /// abort on the first failing statement, so a caller (e.g. `spky query`)
    /// can render the outcome of each statement in the batch.
    pub fn query(&self, query: &str) -> Result<Vec<SurrealResponse>> {
        send_raw_sql_unchecked(
            &format!("{}/sql", self.url),
            &self.auth_header,
            self.auth_namespace.as_deref(),
            Some(&self.namespace),
            Some(&self.database),
            query,
        )
    }

    /// Internal execute that returns parsed responses (shared by trait impl and info methods).
    fn execute_query(&self, query: &str) -> Result<Vec<SurrealResponse>> {
        send_raw_sql(
            &format!("{}/sql", self.url),
            &self.auth_header,
            self.auth_namespace.as_deref(),
            Some(&self.namespace),
            Some(&self.database),
            query,
        )
    }

    /// Drop and recreate the database (used to recover from stale migration state in dev).
    pub fn reset_database(&self) -> Result<()> {
        let query = format!(
            "USE NS {}; REMOVE DATABASE {};",
            quote_ident(&self.namespace),
            quote_ident(&self.database)
        );
        send_raw_sql(
            &format!("{}/sql", self.url),
            &self.auth_header,
            self.auth_namespace.as_deref(),
            None,
            None,
            &query,
        )
        .context("Failed to remove database")?;

        self.ensure_ns_db()
    }

    /// Ensure namespace and database exist (usable outside the MigrationDB trait).
    pub fn ensure_ns_db(&self) -> Result<()> {
        let query = self.ensure_ns_db_query();
        send_raw_sql(
            &format!("{}/sql", self.url),
            &self.auth_header,
            self.auth_namespace.as_deref(),
            None,
            None,
            &query,
        )
        .context("Failed to create namespace/database")?;

        Ok(())
    }

    /// The statements behind `ensure_ns_db`. A namespace-level user cannot
    /// `DEFINE NAMESPACE` ("IAM error: Not enough permissions"), even with IF NOT
    /// EXISTS; its namespace exists by definition, so only the database is made.
    fn ensure_ns_db_query(&self) -> String {
        let use_db = format!(
            "USE NS {}; DEFINE DATABASE IF NOT EXISTS {};",
            quote_ident(&self.namespace),
            quote_ident(&self.database)
        );
        if self.auth_namespace.is_some() {
            use_db
        } else {
            format!(
                "DEFINE NAMESPACE IF NOT EXISTS {}; {}",
                quote_ident(&self.namespace),
                use_db
            )
        }
    }
}

impl MigrationDB for SurrealClient {
    fn ensure_ns_db(&self) -> Result<()> {
        // Delegate to the inherent method to avoid duplication.
        SurrealClient::ensure_ns_db(self)
    }

    fn execute(&self, query: &str) -> Result<Vec<SurrealResponse>> {
        self.execute_query(query)
    }

    fn ping(&self) -> Result<()> {
        self.execute("INFO FOR DB;")
            .context("Failed to ping SurrealDB")?;
        Ok(())
    }

    fn ensure_migration_table(&self) -> Result<()> {
        let schema = include_str!("migration_tables.surql");
        self.execute(schema)
            .context("Failed to create migration tracking table")?;
        Ok(())
    }

    fn get_applied_migrations(&self) -> Result<Vec<AppliedMigration>> {
        let responses = match self.execute(
            "SELECT version, name, applied_at, checksum FROM _00_migrations ORDER BY version ASC;",
        ) {
            Ok(r) => r,
            Err(e) => {
                // SurrealDB 3.x returns error for non-existent tables — treat as empty
                let msg = e.to_string();
                if msg.contains("does not exist") || msg.contains("NotFound") {
                    return Ok(vec![]);
                }
                return Err(e).context("Failed to query applied migrations");
            }
        };

        let result = responses
            .into_iter()
            .next()
            .and_then(|r| r.result)
            .unwrap_or(serde_json::Value::Array(vec![]));

        let migrations: Vec<AppliedMigration> =
            serde_json::from_value(result).context("Failed to deserialize applied migrations")?;

        Ok(migrations)
    }

    fn record_migration(&self, version: &str, name: &str, checksum: &str) -> Result<()> {
        let query = format!(
            "CREATE _00_migrations SET version = '{}', name = '{}', checksum = '{}';",
            version, name, checksum
        );
        self.execute(&query).context("Failed to record migration")?;
        Ok(())
    }

    fn update_migration_checksum(&self, version: &str, new_checksum: &str) -> Result<()> {
        let query = format!(
            "UPDATE _00_migrations SET checksum = '{}' WHERE version = '{}';",
            new_checksum, version
        );
        self.execute(&query)
            .context("Failed to update migration checksum")?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_to_http_url_normalizes_ws_schemes() {
        assert_eq!(to_http_url("wss://db.example.com"), "https://db.example.com");
        assert_eq!(to_http_url("ws://localhost:8000"), "http://localhost:8000");
        assert_eq!(
            to_http_url("https://db.example.com/"),
            "https://db.example.com"
        );
        assert_eq!(to_http_url("http://localhost:8000"), "http://localhost:8000");
        // Scheme conversion also strips the trailing slash.
        assert_eq!(to_http_url("wss://db.example.com/"), "https://db.example.com");
    }

    #[test]
    fn test_new_constructs_basic_auth_header() {
        let client = SurrealClient::new(
            "http://localhost:8000",
            "test_ns",
            "test_db",
            "root",
            "root",
        );
        // base64("root:root") = "cm9vdDpyb290"
        assert_eq!(client.auth_header, "Basic cm9vdDpyb290");
    }

    #[test]
    fn test_new_encodes_special_chars_in_credentials() {
        let client = SurrealClient::new("http://localhost:8000", "ns", "db", "admin", "p@ss:w0rd!");
        let expected = format!(
            "Basic {}",
            base64::engine::general_purpose::STANDARD.encode("admin:p@ss:w0rd!")
        );
        assert_eq!(client.auth_header, expected);
    }

    #[test]
    fn test_new_trims_trailing_slash_from_url() {
        let client = SurrealClient::new("http://localhost:8000/", "ns", "db", "root", "root");
        assert_eq!(client.url, "http://localhost:8000");
    }

    #[test]
    fn test_new_trims_multiple_trailing_slashes() {
        let client = SurrealClient::new("http://localhost:8000///", "ns", "db", "root", "root");
        assert_eq!(client.url, "http://localhost:8000");
    }

    #[test]
    fn test_new_preserves_url_without_trailing_slash() {
        let client = SurrealClient::new("http://localhost:8000", "ns", "db", "root", "root");
        assert_eq!(client.url, "http://localhost:8000");
    }

    #[test]
    fn test_new_stores_namespace_and_database() {
        let client = SurrealClient::new(
            "http://localhost:8000",
            "my_namespace",
            "my_database",
            "root",
            "root",
        );
        assert_eq!(client.namespace, "my_namespace");
        assert_eq!(client.database, "my_database");
    }

    #[test]
    fn test_root_client_defines_namespace_and_database() {
        let client = SurrealClient::new("http://localhost:8000", "my-ns", "main", "root", "root");
        assert_eq!(client.auth_namespace, None);
        assert_eq!(
            client.ensure_ns_db_query(),
            "DEFINE NAMESPACE IF NOT EXISTS `my-ns`; USE NS `my-ns`; DEFINE DATABASE IF NOT EXISTS `main`;"
        );
    }

    #[test]
    fn test_namespace_client_skips_define_namespace() {
        let client =
            SurrealClient::new("http://localhost:8000", "talktobolo", "main", "owner", "pw")
                .with_auth_namespace(Some("talktobolo"));
        assert_eq!(client.auth_namespace.as_deref(), Some("talktobolo"));
        assert_eq!(
            client.ensure_ns_db_query(),
            "USE NS `talktobolo`; DEFINE DATABASE IF NOT EXISTS `main`;"
        );
    }

    #[test]
    fn test_surreal_response_deserialize_ok() {
        let json = r#"{"status":"OK","result":[{"id":"test:1"}]}"#;
        let resp: SurrealResponse = serde_json::from_str(json).unwrap();
        assert_eq!(resp.status, "OK");
        assert!(resp.result.is_some());
    }

    #[test]
    fn test_surreal_response_deserialize_err() {
        let json = r#"{"status":"ERR","result":"Some error message"}"#;
        let resp: SurrealResponse = serde_json::from_str(json).unwrap();
        assert_eq!(resp.status, "ERR");
        assert_eq!(resp.result.unwrap().as_str().unwrap(), "Some error message");
    }

    #[test]
    fn test_surreal_response_deserialize_null_result() {
        let json = r#"{"status":"OK","result":null}"#;
        let resp: SurrealResponse = serde_json::from_str(json).unwrap();
        assert_eq!(resp.status, "OK");
        assert!(resp.result.is_none());
    }

    #[test]
    fn test_applied_migration_deserialize() {
        let json = r#"{
            "version": "20240101120000",
            "name": "initial_schema",
            "applied_at": "2024-01-01T12:05:00Z",
            "checksum": "abc123"
        }"#;
        let m: AppliedMigration = serde_json::from_str(json).unwrap();
        assert_eq!(m.version, "20240101120000");
        assert_eq!(m.name, "initial_schema");
        assert_eq!(m.applied_at, "2024-01-01T12:05:00Z");
        assert_eq!(m.checksum, "abc123");
    }

    #[test]
    fn test_applied_migration_deserialize_from_array() {
        let json = r#"[
            {"version":"20240101120000","name":"first","applied_at":"2024-01-01T12:00:00Z","checksum":"aaa"},
            {"version":"20240102120000","name":"second","applied_at":"2024-01-02T12:00:00Z","checksum":"bbb"}
        ]"#;
        let migrations: Vec<AppliedMigration> = serde_json::from_str(json).unwrap();
        assert_eq!(migrations.len(), 2);
        assert_eq!(migrations[0].version, "20240101120000");
        assert_eq!(migrations[1].version, "20240102120000");
    }
}
