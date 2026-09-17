/// All node configuration, constructor-injected. The core reads the process
/// environment ZERO times — the VM shell builds this from env vars
/// (`load_config()` in `apps/ssp`), the CF shell from Worker bindings.
///
/// Field set = the historical `apps/ssp` `Config` plus the values that were
/// previously scattered `std::env::var` reads inside handlers/tasks
/// (`auth_secret`, `bootstrap_page_size`, `crdt_cache_size`,
/// `health_check_interval_secs`). Cluster-mode fields (`scheduler_url`,
/// `heartbeat_interval_ms`, `advertise_addr`, `register_max_wait_secs`) are
/// VM-shell concerns — the portable core only consults `scheduler_url` as the
/// standalone-mode flag (`None` = standalone).
pub struct NodeConfig {
    /// Address the HTTP server binds (shell concern; kept for drop-in compat).
    pub listen_addr: String,
    /// SurrealDB connection string (self-hosted container, SurrealDB Cloud,
    /// anything reachable). Canonical env: `SPKY_DB_URL`.
    pub db_addr: String,
    pub db_user: String,
    pub db_pass: String,
    pub db_ns: String,
    pub db_db: String,
    /// `Some` = cluster mode behind a scheduler; `None` = standalone (the
    /// mode that ports to Cloudflare).
    pub scheduler_url: Option<String>,
    pub ssp_id: String,
    pub heartbeat_interval_ms: u64,
    pub advertise_addr: Option<String>,
    pub ttl_cleanup_interval_secs: u64,
    /// Total wall-clock budget (seconds) for retrying scheduler registration
    /// before the process exits to let the supervisor restart it. Must comfortably
    /// exceed a cold `--clean` re-clone window (scheduler returns 503 while
    /// `Cloning`). Env: `SPKY_SSP_REGISTER_MAX_WAIT_SECS`, default 180.
    pub register_max_wait_secs: u64,
    /// Storage layout for `_00_query` / `_00_list_ref`. See
    /// `ssp_protocol::RefMode`. Defaults to `Dedicated` so cross-session
    /// LIVE delivery doesn't depend on the SurrealDB v3 LIVE-permission
    /// path; flip to `Single` only when running against a SurrealDB
    /// version that delivers cross-session LIVE notifications correctly
    /// through permission rules.
    pub ref_mode: ssp_protocol::RefMode,
    /// Query allowlist enforcement (`ssp::allowlist::Mode`): `off` never
    /// consults it, `warn` logs + counts a miss and admits it, `enforce`
    /// refuses a miss with 403 `not_allowlisted`. Env:
    /// `SPKY_SSP_QUERY_ALLOWLIST`, default `off`.
    pub query_allowlist: ssp::allowlist::Mode,
    /// Enable realtime sync for unauthenticated (anonymous) clients. When
    /// `true`, anonymous query registrations (empty `auth_id`) are routed to a
    /// dedicated `_00_list_ref_anon` table that anyone can SELECT, so a
    /// logged-out client's `_00_list_ref` poll can read its window.
    pub anonymous_live_queries: bool,
    /// Sign admin impersonation tokens on `POST /impersonate/mint`
    /// (`ssp_protocol::impersonation`). Set by the CLI when `impersonation`
    /// is enabled in sp00ky.yml. Env: `SPKY_IMPERSONATION`, default off.
    pub impersonation: bool,
    /// Idle-to-active debounce (ms) for the bounded query-edge publisher.
    /// Queued work drains without per-item delays. `0` starts immediately.
    pub query_update_throttle_ms: u64,
    /// How often (ms) the per-view metrics (`rowCount`, `updateCount`, the
    /// materialization percentiles on `_00_query`) are flushed. The ingest path
    /// only notes them in memory. Env: `SPKY_SSP_VIEW_METRICS_FLUSH_MS`,
    /// default 2000.
    pub view_metrics_flush_ms: u64,
    /// Share ONE operator graph between registrations that compute the same
    /// thing, instead of building an identical DAG per registered query id.
    /// Env: `SPKY_SSP_MERGE_VIEWS`, default `false`.
    ///
    /// Off by default on purpose. Merging is decided by
    /// `ssp::merge_key::compute`, and the failure mode of a wrong key is not a
    /// crash but two identities sharing a row set, so it is enabled per tenant
    /// after `graph_count` vs `view_count` has been observed on real traffic.
    pub merge_views: bool,
    /// Bearer secret for the authenticated route group. Empty accepts any
    /// bearer (dev only). Env: `SPKY_AUTH_SECRET`.
    pub auth_secret: String,
    /// Rows pulled per bootstrap page (keyset pagination).
    /// Env: `SPKY_SSP_BOOTSTRAP_PAGE_SIZE`, default 200.
    pub bootstrap_page_size: usize,
    /// In-memory CRDT cache capacity. Env: `SPKY_CRDT_CACHE_SIZE`, default 1024.
    pub crdt_cache_size: usize,
    /// Backend health poll cadence (standalone maintenance plane).
    /// Env: `SPKY_HEALTH_CHECK_INTERVAL_SECS`, default 15.
    pub health_check_interval_secs: u64,
    /// Circuit-checkpoint cadence for ephemeral hosts (a `CircuitStore`-backed
    /// snapshot every N seconds, bounding data loss on ungraceful eviction).
    /// `None` disables periodic checkpointing — the VM leaves it `None` (its
    /// store is noop; the process holds the circuit).
    pub checkpoint_interval_secs: Option<u64>,
    /// Max age of a restored circuit snapshot before `bootstrap()` discards it
    /// and rebuilds from the DB instead. Guards against trusting stale state.
    pub max_snapshot_age_secs: u64,
}
