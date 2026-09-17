use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};

pub mod impersonation;
pub mod snapshot_hash;

/// Storage mode for the SSP's per-query reference tables (`_00_query`,
/// `_00_list_ref`). Shared by the CLI (reads from `sp00ky.yml`), the SSP
/// server (runtime routing), and downstream codegen so all three agree
/// on the table-name convention.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum RefMode {
    /// One shared `_00_list_ref` for every user. Subject to a
    /// SurrealDB v3 LIVE-permission gap that drops cross-session
    /// INSERT notifications; kept for the eventual upstream fix.
    Single,
    /// Per-user `_00_list_ref_user_<id>` table, created lazily by the
    /// SSP on first registration. Its permission rule is hardcoded
    /// against the owning user's record id, sidestepping the
    /// LIVE-permission gap. `_00_query` stays global in both modes.
    Dedicated,
}

impl Default for RefMode {
    fn default() -> Self {
        RefMode::Dedicated
    }
}

impl RefMode {
    /// Stable lowercase token used in env vars and codegen output.
    pub fn as_str(&self) -> &'static str {
        match self {
            RefMode::Single => "single",
            RefMode::Dedicated => "dedicated",
        }
    }

    pub fn parse_str(s: &str) -> Option<Self> {
        match s.to_ascii_lowercase().as_str() {
            "single" => Some(RefMode::Single),
            "dedicated" => Some(RefMode::Dedicated),
            _ => None,
        }
    }
}

/// Sentinel `auth_id` used for unauthenticated clients when anonymous live
/// queries are enabled (`anonymousLiveQueries: true`). It carries no `user:`
/// prefix, so it can never collide with a real user id: every authenticated
/// caller arrives as `"user:<id>"`. Both the SSP (when it sees an empty
/// `auth_id`) and the client (while signed out) substitute this value so they
/// agree on the `_00_list_ref_anon` table name.
pub const ANON_AUTH_ID: &str = "anon";

/// Marker baked into the `COMMENT` of a `DEFINE TABLE` when the schema marks a
/// table `-- @nosync`. The CLI appends `COMMENT 'sp00ky:nosync'` to the server
/// schema; the scheduler and SSP detect it in the `DEFINE TABLE` string
/// returned by `INFO FOR DB` and exclude the table from snapshots and from the
/// in-memory circuit. The table stays a normal table in the main DB, so it is
/// still backed up. Shared here so the CLI (writer) and both runtime services
/// (readers) agree on a single token.
pub const NOSYNC_TABLE_COMMENT: &str = "sp00ky:nosync";

/// True when a `DEFINE TABLE ...` string (as returned by `INFO FOR DB`) carries
/// the `@nosync` marker and must be excluded from sync.
pub fn define_str_is_nosync(define_table: &str) -> bool {
    define_table.contains(NOSYNC_TABLE_COMMENT)
}

/// Marker baked into the `COMMENT` of a `DEFINE FIELD` whose VALUE must never
/// enter the sync machinery — the field-level counterpart of
/// [`NOSYNC_TABLE_COMMENT`]. The CLI emits it for all three exclusion classes
/// (`-- @nosync`, `-- @crdt` and `-- @opaque` on a field), because the runtime
/// treatment is identical in every case: the value is never held in the
/// scheduler replica, never held in an SSP circuit row, and therefore never
/// part of an integrity hash.
///
/// This marker is what makes those exclusions *symmetric*. The sp00ky ingest
/// events already omit these fields from their payload, but until the marker
/// existed nothing told the replica clone or the SSP bootstrap `SELECT *` to
/// skip them. The result was permanent drift: the replica applies an update
/// with `UPDATE … MERGE` (so a field absent from the payload keeps its cloned
/// value forever) while the circuit replaces the whole row (so the field
/// disappears). The two sides then hash differently for the rest of the
/// deployment. Readers turn this marker into an `OMIT` list, so the value never
/// reaches either side and the hashes agree by construction.
pub const OPAQUE_FIELD_COMMENT: &str = "sp00ky:opaque";

/// True when a `DEFINE FIELD ...` string (as returned by `INFO FOR TABLE`)
/// carries the opaque marker and must be projected out of every row scan.
pub fn define_str_is_opaque(define_field: &str) -> bool {
    define_field.contains(OPAQUE_FIELD_COMMENT)
}

/// Field names to `OMIT` from row scans of `table`, read out of an
/// `INFO FOR TABLE` response's `fields` map.
///
/// `id` and any `_00_*` name can never be omitted, whatever the schema says:
/// `id` is the row key every consumer joins on, and `_00_rv` carries the
/// record version that drives catch-up. A marker on either is a schema bug, so
/// it is ignored rather than honored.
///
/// Nested paths (`meta.secret`) are returned verbatim. SurrealDB's `OMIT`
/// accepts an idiom, so `OMIT meta.secret` removes just that leaf. A name that
/// does not exist on the table is harmless — `OMIT` on an absent field is a
/// no-op, not an error.
pub fn opaque_fields_from_info(info_for_table: &serde_json::Value) -> BTreeSet<String> {
    let Some(fields) = info_for_table.get("fields").and_then(|f| f.as_object()) else {
        return BTreeSet::new();
    };
    fields
        .iter()
        .filter(|(name, _)| name.as_str() != "id" && !name.starts_with("_00_"))
        .filter(|(_, define)| define.as_str().is_some_and(define_str_is_opaque))
        .map(|(name, _)| name.clone())
        .collect()
}

/// Root column names of a table from its `INFO FOR TABLE` output: the head
/// segment of every declared field (`meta.secret` → `meta`, `tags[*]` →
/// `tags`), minus the `_00_*` bookkeeping fields. This is what a builder
/// `where` may reference when the query allowlist admits a caller-supplied
/// predicate (`ssp::allowlist::validate_builder_where`).
pub fn columns_from_info(info_for_table: &serde_json::Value) -> BTreeSet<String> {
    let Some(fields) = info_for_table.get("fields").and_then(|f| f.as_object()) else {
        return BTreeSet::new();
    };
    fields
        .keys()
        .map(|name| {
            name.split(|c| c == '.' || c == '[')
                .next()
                .unwrap_or(name)
                .to_string()
        })
        .filter(|root| !root.is_empty() && !root.starts_with("_00_"))
        .collect()
}

/// Render an `OMIT` clause for a `SELECT` over `fields`, e.g. `" OMIT a, b"`.
/// Empty when there is nothing to omit, so callers can splice it into a query
/// string unconditionally. Shared so every producer (scheduler replica, SSP
/// bootstrap, `spky verify`) emits a byte-identical projection — a difference
/// between them is exactly the drift this marker exists to prevent.
pub fn omit_clause(fields: &BTreeSet<String>) -> String {
    if fields.is_empty() {
        return String::new();
    }
    format!(
        " OMIT {}",
        fields.iter().cloned().collect::<Vec<_>>().join(", ")
    )
}

/// `_00_*` meta tables that DO participate in sync: server-written rows the
/// client subscribes to live (feature-flag assignments, app-release
/// announcements). Every other `_00_*` table stays runtime-internal and is
/// excluded from snapshots, bootstrap loads and integrity hashes.
pub const SYNCED_META_TABLES: &[&str] = &["_00_user_feature", "_00_app_release"];

/// True when `table` must be excluded from sync machinery (snapshot clone,
/// circuit bootstrap, integrity hashing): any `_00_*` table except the
/// explicitly synced meta tables above. Shared so the scheduler and the SSP
/// agree on the exact same table set (a mismatch breaks integrity hashes).
pub fn table_excluded_from_sync(table: &str) -> bool {
    table.starts_with("_00_") && !SYNCED_META_TABLES.contains(&table)
}

/// Sanitize a user record id (e.g. `"user:abc123"`) into the segment
/// that goes into a dedicated table name (e.g. `"abc123"`). Returns
/// `None` if the id is empty, missing the `user:` prefix, or contains
/// characters that aren't valid in a SurrealDB table identifier. The
/// SSP and the client mirror this function so both sides land on the
/// exact same table name.
pub fn sanitize_user_id(auth_id: &str) -> Option<String> {
    let raw = auth_id.strip_prefix("user:").unwrap_or(auth_id);
    if raw.is_empty() {
        return None;
    }
    if raw
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '_')
    {
        Some(raw.to_string())
    } else {
        None
    }
}

/// Returns the `_00_list_ref` table name for a given mode + user.
/// The `_00_query` registration table stays global in both modes so
/// the client can compute its record id without knowing the user;
/// only `_00_list_ref` splits per user because that's where the
/// SurrealDB LIVE permission gap lives.
pub fn list_ref_table_for(mode: RefMode, auth_id: &str) -> String {
    // Anonymous clients (flag-enabled) share one dedicated, world-readable
    // table in both modes — checked before the mode match so it never lands on
    // the per-user or the auth-gated global table. The bare `"anon"` sentinel
    // never collides with a real user (those arrive as `"user:<id>"`).
    if auth_id == ANON_AUTH_ID {
        return "_00_list_ref_anon".to_string();
    }
    match mode {
        RefMode::Single => "_00_list_ref".to_string(),
        RefMode::Dedicated => match sanitize_user_id(auth_id) {
            Some(uid) => format!("_00_list_ref_user_{}", uid),
            None => "_00_list_ref".to_string(),
        },
    }
}

// --- Ingest API (snake_case wire format) ---

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IngestRequest {
    pub table: String,
    pub op: String,
    pub id: String,
    pub record: Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub job_assignee: Option<String>,
}

// --- View API (camelCase wire format via serde rename) ---

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ViewRegisterRequest {
    pub id: String,
    pub surql: String,
    pub client_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub params: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ttl: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_active_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub format: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ViewUnregisterRequest {
    pub id: String,
}

// --- SSP Management API (snake_case wire format) ---

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SspRegistration {
    pub ssp_id: String,
    pub url: String,
    pub version: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub env: Option<std::collections::HashMap<String, String>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SspRegistrationResponse {
    pub snapshot_seq: u64,
    /// Per-table content hashes (blake3, hex with `b3:` prefix) at
    /// `snapshot_seq`. The SSP must produce the same hashes after loading
    /// its circuit store; mismatch ⇒ retry-then-fatal so the supervisor
    /// re-registers from a fresh frozen snapshot.
    #[serde(default)]
    pub table_hashes: BTreeMap<String, String>,
}

/// Sent by an SSP whose post-bootstrap integrity check disagreed with the
/// hashes handed out at registration, carrying the hashes it actually computed
/// over its freshly loaded circuit.
///
/// The scheduler's `table_hashes` are a *cache* maintained incrementally as
/// events drain; the replica content is the authority. So a disagreement is
/// resolved by rehashing the disputed tables **from replica content** rather
/// than by assuming the SSP is wrong — the SSP loaded its rows from that same
/// replica, so a stale cache would otherwise crash-loop it forever.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SspBootstrapVerifyRequest {
    pub ssp_id: String,
    /// The SSP's own per-table hashes, in the same `b3:`-prefixed form.
    #[serde(default)]
    pub table_hashes: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SspBootstrapVerifyResponse {
    /// Hashes recomputed from replica content for the disputed tables, so the
    /// SSP can log (and re-diff) against corrected values.
    #[serde(default)]
    pub table_hashes: BTreeMap<String, String>,
    /// Tables that still disagree after the content rehash — a genuine
    /// divergence. Empty means the scheduler's cache was simply stale and has
    /// now been repaired; the SSP may go Ready.
    #[serde(default)]
    pub diverging: Vec<String>,
    /// Breaker escalation: proceed to Ready despite `diverging`, because
    /// withholding this SSP any longer costs more (no ready SSP ⇒ no sync)
    /// than admitting a marginally divergent circuit.
    #[serde(default)]
    pub admit: bool,
    /// The scheduler re-cloned its replica from upstream; the SSP's circuit is
    /// stale by construction and it must bootstrap again.
    #[serde(default)]
    pub recloned: bool,
}

/// How far along an SSP is in loading its circuit from the scheduler's frozen
/// snapshot.
///
/// This rides its own fire-and-forget message rather than the heartbeat,
/// because the heartbeat loop deliberately stays silent until the SSP reaches
/// `Ready` (see `apps/ssp/src/lib.rs`) — which is exactly the window this
/// describes. Every field is `serde(default)`-friendly, so an older SSP that
/// never sends it and a newer one that does both work against either
/// scheduler; a dashboard with no progress falls back to rendering the phase
/// and its elapsed time.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct BootstrapProgress {
    /// Tables fully loaded so far.
    #[serde(default)]
    pub tables_done: usize,
    /// Tables the bootstrap intends to load in total. 0 ⇒ not yet known.
    #[serde(default)]
    pub tables_total: usize,
    /// Rows loaded across all tables so far.
    #[serde(default)]
    pub rows_loaded: u64,
    /// The table currently being loaded, if any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub current_table: Option<String>,
}

/// `POST /ssp/bootstrap-progress` — advisory, best-effort, and never retried.
/// Losing one costs a slightly stale progress bar and nothing else, so the
/// scheduler answers it without touching any bootstrap decision.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SspBootstrapProgressRequest {
    pub ssp_id: String,
    #[serde(flatten)]
    pub progress: BootstrapProgress,
}

/// Body of the 409 the scheduler answers a heartbeat with when it wants the
/// SSP gone. The SSP's response to a 409 is to exit and let its supervisor
/// relaunch it, so this is the one message that can ride along on the way out.
///
/// `clean` asks the SSP to drop its on-disk circuit snapshot first, so the
/// relaunch is a cold rebuild rather than a snapshot restore plus catch-up.
/// Older SSPs read the body as free text and simply restart; older schedulers
/// send free text, which parses to `clean: false`. Both directions degrade to
/// a plain restart, never to a refused one.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ResyncDirective {
    pub reason: String,
    #[serde(default)]
    pub clean: bool,
}

impl ResyncDirective {
    /// Parse a heartbeat 409 body, tolerating the pre-directive plain-text
    /// form so an SSP never treats an old scheduler's reply as garbage.
    pub fn parse(body: &str) -> Self {
        serde_json::from_str(body).unwrap_or_else(|_| ResyncDirective {
            reason: body.trim().to_string(),
            clean: false,
        })
    }
}

/// Bounded, sanitized publication backlog details. No source rows or bindings.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PublicationViewMetrics {
    pub query_id: String,
    pub pending_operations: u64,
    pub pending_bytes: u64,
    pub oldest_age_ms: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PublicationConnectionMetrics {
    pub generation: u64,
    pub last_reconnect_duration_ms: Option<u64>,
    pub reconnect_failures: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PublicationMetrics {
    pub pending_batches: u64,
    pub pending_operations: u64,
    pub pending_bytes: u64,
    pub oldest_age_ms: u64,
    pub parked_batches: u64,
    pub last_success_at_ms: Option<u64>,
    pub overload_total: u64,
    #[serde(default)]
    pub connection: Option<PublicationConnectionMetrics>,
    /// At most eight views, ranked by backlog by the publisher.
    #[serde(default)]
    pub worst_views: Vec<PublicationViewMetrics>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SspHeartbeat {
    pub ssp_id: String,
    pub timestamp: u64,
    pub views: usize,
    pub cpu_usage: Option<f64>,
    pub memory_usage: Option<f64>,
    pub version: String,
    /// Absent on older SSPs. Older schedulers ignore this additional field.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub publication: Option<PublicationMetrics>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn heartbeat_publication_is_backward_compatible() {
        let legacy = serde_json::json!({
            "ssp_id": "ssp-0", "timestamp": 123, "views": 2,
            "cpu_usage": null, "memory_usage": null, "version": "old"
        });
        let heartbeat: SspHeartbeat = serde_json::from_value(legacy.clone()).unwrap();
        assert!(heartbeat.publication.is_none());
        assert_eq!(serde_json::to_value(heartbeat).unwrap(), legacy);

        let mut current = legacy;
        current["publication"] = serde_json::json!({
            "pending_batches": 2, "pending_operations": 15, "pending_bytes": 512,
            "oldest_age_ms": 500, "parked_batches": 1,
            "last_success_at_ms": null, "overload_total": 0
        });
        let heartbeat: SspHeartbeat = serde_json::from_value(current).unwrap();
        let publication = heartbeat.publication.unwrap();
        assert_eq!(publication.pending_operations, 15);
        assert!(publication.connection.is_none());
        assert!(publication.worst_views.is_empty());
    }

    #[test]
    fn anon_routes_to_dedicated_anon_table_in_both_modes() {
        assert_eq!(list_ref_table_for(RefMode::Dedicated, ANON_AUTH_ID), "_00_list_ref_anon");
        assert_eq!(list_ref_table_for(RefMode::Single, ANON_AUTH_ID), "_00_list_ref_anon");
    }

    #[test]
    fn authenticated_user_is_unaffected_by_anon_sentinel() {
        assert_eq!(
            list_ref_table_for(RefMode::Dedicated, "user:abc"),
            "_00_list_ref_user_abc"
        );
        assert_eq!(list_ref_table_for(RefMode::Single, "user:abc"), "_00_list_ref");
        // A user whose id sanitizes to "anon" still carries the `user:` prefix,
        // so it never matches the bare sentinel.
        assert_eq!(
            list_ref_table_for(RefMode::Dedicated, "user:anon"),
            "_00_list_ref_user_anon"
        );
    }

    #[test]
    fn empty_auth_id_keeps_legacy_global_fallback() {
        assert_eq!(list_ref_table_for(RefMode::Dedicated, ""), "_00_list_ref");
        assert_eq!(list_ref_table_for(RefMode::Single, ""), "_00_list_ref");
    }

    /// Shape taken verbatim from a real `INFO FOR TABLE` on SurrealDB 3.1.2 —
    /// note that the engine renders `COMMENT` before `PERMISSIONS`.
    fn info_fixture() -> Value {
        serde_json::json!({
            "events": {},
            "fields": {
                "blob": "DEFINE FIELD blob ON user TYPE none | bytes COMMENT 'sp00ky:opaque' PERMISSIONS FULL",
                "email": "DEFINE FIELD email ON user TYPE string PERMISSIONS FULL",
                "meta.secret": "DEFINE FIELD meta.secret ON user TYPE string COMMENT 'sp00ky:opaque' PERMISSIONS FULL",
                "secret_token": "DEFINE FIELD secret_token ON user TYPE string COMMENT 'audit trail sp00ky:opaque' PERMISSIONS FULL"
            },
            "indexes": {},
            "lives": {},
            "tables": {}
        })
    }

    #[test]
    fn opaque_fields_read_marker_from_field_defines() {
        let got = opaque_fields_from_info(&info_fixture());
        // `secret_token` proves a merged comment (marker appended to existing
        // text) is still detected.
        assert_eq!(
            got.iter().cloned().collect::<Vec<_>>(),
            vec!["blob", "meta.secret", "secret_token"]
        );
    }

    #[test]
    fn opaque_fields_never_include_id_or_reserved_names() {
        // A marker on `id` or `_00_rv` is a schema bug: omitting either breaks
        // every consumer (row key, catch-up version). Ignore rather than honor.
        let info = serde_json::json!({
            "fields": {
                "id": "DEFINE FIELD id ON user TYPE record COMMENT 'sp00ky:opaque'",
                "_00_rv": "DEFINE FIELD _00_rv ON user TYPE int COMMENT 'sp00ky:opaque'",
                "blob": "DEFINE FIELD blob ON user TYPE bytes COMMENT 'sp00ky:opaque'"
            }
        });
        assert_eq!(
            opaque_fields_from_info(&info).iter().cloned().collect::<Vec<_>>(),
            vec!["blob"]
        );
    }

    #[test]
    fn opaque_fields_tolerate_missing_or_malformed_info() {
        assert!(opaque_fields_from_info(&Value::Null).is_empty());
        assert!(opaque_fields_from_info(&serde_json::json!({})).is_empty());
        assert!(opaque_fields_from_info(&serde_json::json!({ "fields": [] })).is_empty());
    }

    #[test]
    fn omit_clause_is_empty_when_nothing_to_omit() {
        assert_eq!(omit_clause(&BTreeSet::new()), "");
    }

    #[test]
    fn omit_clause_is_sorted_and_comma_separated() {
        // Sorted output is not cosmetic: every producer must emit a
        // byte-identical projection or their hashes drift.
        let fields: BTreeSet<String> =
            ["secret_token", "blob"].iter().map(|s| s.to_string()).collect();
        assert_eq!(omit_clause(&fields), " OMIT blob, secret_token");
    }
}

/// Include durable row versions when scanning upstream application tables.
/// Call only when INFO FOR DB contains `_00_version`; replicas already carry
/// `_00_rv`, and bare integration databases may have no version table.
pub fn with_durable_row_versions(page_query: &str) -> String {
    // Bind the outer id before planning the lookup. A correlated $parent.id
    // predicate makes SurrealDB scan the entire ledger for every source row.
    page_query.replacen("SELECT *", "SELECT *, { LET $spky_row_id = id; RETURN (SELECT VALUE version FROM _00_version WHERE record_id = $spky_row_id LIMIT 1)[0] ?? _00_rv ?? 1; } AS _00_rv", 1)
}
