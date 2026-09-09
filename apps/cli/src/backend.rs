use anyhow::{bail, Context, Result};
use openapiv3::OpenAPI;
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

// ── App Type ────────────────────────────────────────────────────────────────

/// Discriminator for app type — must be specified explicitly.
#[derive(Debug, Deserialize, Serialize, Clone, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum AppType {
    Backend,
    Frontend,
    /// Runs a prebuilt docker image (the `image`/`ports`/`args` fields), e.g. a
    /// local LiveKit SFU. No spec/method; honors `scope`.
    Docker,
}

/// A published port for a `type: docker` app. Accepts a bare port number/string
/// (`7880` / `"7880"` → published as `7880:7880`), a `host:container` map
/// (`"3000:8080"`), or any of those with a protocol suffix (`"7882/udp"` →
/// `7882:7882/udp`).
#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(untagged)]
pub enum PortSpec {
    Num(u32),
    Str(String),
}

impl PortSpec {
    /// The `-p` value to pass to `docker run`. A spec without a `:` is expanded
    /// to `host:container` using the same port on both sides (protocol suffix
    /// preserved): `7882/udp` → `7882:7882/udp`.
    pub fn publish(&self) -> String {
        let s = match self {
            PortSpec::Num(n) => n.to_string(),
            PortSpec::Str(s) => s.clone(),
        };
        if s.contains(':') {
            return s;
        }
        match s.split_once('/') {
            Some((port, proto)) => format!("{port}:{port}/{proto}"),
            None => format!("{s}:{s}"),
        }
    }

    /// The host port, for the dev port pre-check. None if it can't be parsed.
    pub fn host_port(&self) -> Option<u16> {
        let publish = self.publish();
        let host = publish.split(':').next().unwrap_or("");
        host.split('/').next().unwrap_or("").parse().ok()
    }
}

/// Where an app runs. `all` (default) runs in `spky dev` AND deploys to cloud;
/// `devOnly` is a local-only process started by `spky dev` and never deployed
/// (and skips backend spec/method/deploy validation); `cloudOnly` deploys but is
/// skipped by `spky dev`.
#[derive(Debug, Deserialize, Serialize, Clone, Copy, PartialEq, Default)]
#[serde(rename_all = "camelCase")]
pub enum AppScope {
    #[default]
    All,
    DevOnly,
    CloudOnly,
}

fn is_default_scope(s: &AppScope) -> bool {
    *s == AppScope::All
}

/// Deployment mode.
#[derive(Debug, Deserialize, Serialize, Clone, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum DeployMode {
    Singlenode,
    Cluster,
    Surrealism,
}

impl Default for DeployMode {
    fn default() -> Self {
        Self::Singlenode
    }
}

impl std::fmt::Display for DeployMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DeployMode::Singlenode => write!(f, "singlenode"),
            DeployMode::Cluster => write!(f, "cluster"),
            DeployMode::Surrealism => write!(f, "surrealism"),
        }
    }
}

/// Authentication type for backend services.
#[derive(Debug, Deserialize, Serialize, Clone, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum AuthType {
    Token,
}

/// Backend trigger method type.
#[derive(Debug, Deserialize, Serialize, Clone, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum MethodType {
    Outbox,
}

/// Client type generation format.
#[derive(Debug, Deserialize, Serialize, Clone, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum ClientFormat {
    Typescript,
    Dart,
}

/// Whether a service is hosted on Sp00ky Cloud or externally.
#[derive(Debug, Deserialize, Serialize, Clone, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum HostingMode {
    Cloud,
    External,
}

impl Default for HostingMode {
    fn default() -> Self {
        Self::Cloud
    }
}

/// A single secret-bearing scalar that is either a literal value or a reference
/// to a key in the encrypted vault. Mirrors the literal-vs-`vault:` distinction
/// of `EnvSource`, but for one value (used by SurrealDB credentials). Resolved
/// against the tenant's decrypted vault at deploy time.
///
/// YAML shapes:
///   password: root                    # literal
///   password: { vault: DB_PASSWORD }  # pulled from the vault
#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(untagged)]
pub enum SecretValue {
    Literal(String),
    Vault {
        #[serde(rename = "vault")]
        vault: String,
    },
}

impl SecretValue {
    /// The vault key this value references, or `None` if it is a literal.
    pub fn vault_key(&self) -> Option<&str> {
        match self {
            SecretValue::Vault { vault } => Some(vault.as_str()),
            SecretValue::Literal(_) => None,
        }
    }

    /// Literal value, or empty string for an unresolved vault reference. For
    /// local contexts (dev/mcp/verify) where vault resolution does not apply —
    /// vault-backed credentials only take effect on external cloud deploys.
    pub fn literal_or_default(&self) -> String {
        match self {
            SecretValue::Literal(s) => s.clone(),
            SecretValue::Vault { .. } => String::new(),
        }
    }
}

pub const DEFAULT_SCHEMA_PATH: &str = "src/schema.surql";
pub const DEFAULT_MIGRATIONS_DIR: &str = "migrations";
pub const DEFAULT_BUCKETS_DIR: &str = "src/buckets";
pub const DEFAULT_CONFIG_PATH: &str = "sp00ky.yml";
/// Per-service config file placed in a backend service's own directory and
/// pulled into the root manifest via `apps.<name>.path: ./svc`. Holds a bare
/// single-app config (an `AppConfig` body).
pub const APP_INCLUDE_FILENAME: &str = "sp00ky.app.yml";
pub const YAML_SCHEMA_COMMENT: &str =
    "# yaml-language-server: $schema=https://sp00ky.cloud/schema/sp00ky.schema.json";

/// SurrealDB config: either a plain version string (backwards compat)
/// or an object with version, namespace, and database.
#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(untagged)]
pub enum SurrealDbConfig {
    /// Just the image version, e.g. "v3.1.0-beta.3"
    Version(String),
    /// Full config with optional fields
    Full {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        version: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        namespace: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        database: Option<String>,
        /// "cloud" (default) or "external". Credentials below (endpoint/
        /// username/password) are only valid when this is "external".
        #[serde(default, skip_serializing_if = "Option::is_none")]
        hosting: Option<HostingMode>,
        /// Required when hosting is "external" — the SurrealDB endpoint URL.
        /// Literal or `{ vault: KEY }`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        endpoint: Option<SecretValue>,
        /// External DB auth username. Literal or `{ vault: KEY }`. Default "root".
        #[serde(default, skip_serializing_if = "Option::is_none")]
        username: Option<SecretValue>,
        /// External DB auth password. Literal or `{ vault: KEY }`. Default "root".
        #[serde(default, skip_serializing_if = "Option::is_none")]
        password: Option<SecretValue>,
    },
}

/// Resolved SurrealDB config with all defaults applied.
#[derive(Debug, Clone)]
pub struct ResolvedSurrealDb {
    pub version: String,
    pub namespace: String,
    pub database: String,
    pub hosting: HostingMode,
    /// Only `Some` when `hosting == External`. Unresolved — a literal or a
    /// `{ vault: KEY }` reference resolved against the vault at deploy time.
    pub endpoint: Option<SecretValue>,
    /// Unresolved username/password (literal or `{ vault: KEY }`); default "root".
    pub username: SecretValue,
    pub password: SecretValue,
}

impl ResolvedSurrealDb {
    pub fn from_config(config: &Option<SurrealDbConfig>) -> Self {
        match config {
            Some(SurrealDbConfig::Version(v)) => Self {
                version: v.clone(),
                namespace: "main".to_string(),
                database: "main".to_string(),
                hosting: HostingMode::Cloud,
                endpoint: None,
                username: SecretValue::Literal("root".to_string()),
                password: SecretValue::Literal("root".to_string()),
            },
            Some(SurrealDbConfig::Full {
                version,
                namespace,
                database,
                hosting,
                endpoint,
                username,
                password,
            }) => Self {
                version: version
                    .clone()
                    .unwrap_or_else(|| DEFAULT_SURREALDB_VERSION.to_string()),
                namespace: namespace.clone().unwrap_or_else(|| "main".to_string()),
                database: database.clone().unwrap_or_else(|| "main".to_string()),
                hosting: hosting.clone().unwrap_or_default(),
                endpoint: endpoint.clone(),
                username: username
                    .clone()
                    .unwrap_or_else(|| SecretValue::Literal("root".to_string())),
                password: password
                    .clone()
                    .unwrap_or_else(|| SecretValue::Literal("root".to_string())),
            },
            None => Self {
                version: DEFAULT_SURREALDB_VERSION.to_string(),
                namespace: "main".to_string(),
                database: "main".to_string(),
                hosting: HostingMode::Cloud,
                endpoint: None,
                username: SecretValue::Literal("root".to_string()),
                password: SecretValue::Literal("root".to_string()),
            },
        }
    }

    pub fn validate(&self) -> Result<()> {
        if self.hosting == HostingMode::External && self.endpoint.is_none() {
            bail!("SurrealDB hosting is 'external' but no endpoint URL was provided");
        }
        Ok(())
    }

    /// Literal username for local (dev/mcp/verify) use. See
    /// [`SecretValue::literal_or_default`].
    pub fn username_literal(&self) -> String {
        self.username.literal_or_default()
    }

    /// Literal password for local (dev/mcp/verify) use.
    pub fn password_literal(&self) -> String {
        self.password.literal_or_default()
    }

    /// Literal endpoint for local (dev/mcp/verify) use, if set.
    pub fn endpoint_literal(&self) -> Option<String> {
        self.endpoint.as_ref().map(|e| e.literal_or_default())
    }
}

impl SurrealDbConfig {
    /// Validate credential placement on the RAW config (before defaults are
    /// applied), so we can tell whether the user explicitly set a credential.
    /// endpoint/username/password are only meaningful for external hosting; the
    /// managed ("cloud") DB generates its own root password.
    pub fn validate_raw(&self) -> Result<()> {
        if let SurrealDbConfig::Full {
            hosting,
            endpoint,
            username,
            password,
            ..
        } = self
        {
            let is_external = matches!(hosting, Some(HostingMode::External));
            if !is_external
                && (endpoint.is_some() || username.is_some() || password.is_some())
            {
                bail!("SurrealDB credentials (endpoint/username/password) are only valid with hosting: external");
            }
        }
        Ok(())
    }
}

/// Schema config: either a directory string (sub-paths derived by convention)
/// or an object with explicit overrides.
#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(untagged)]
pub enum SchemaConfig {
    Dir(String),
    Explicit {
        schema: Option<String>,
        migrations: Option<String>,
        #[serde(rename = "bucketsDir")]
        buckets_dir: Option<String>,
    },
}

/// Resolved schema paths with all defaults applied.
#[derive(Debug, Clone)]
pub struct ResolvedSchema {
    pub schema: PathBuf,
    pub migrations: PathBuf,
    pub buckets_dir: PathBuf,
}

impl ResolvedSchema {
    pub fn from_config(config: &Option<SchemaConfig>) -> Self {
        match config {
            Some(SchemaConfig::Dir(dir)) => Self {
                schema: PathBuf::from(dir).join(DEFAULT_SCHEMA_PATH),
                migrations: PathBuf::from(dir).join(DEFAULT_MIGRATIONS_DIR),
                buckets_dir: PathBuf::from(dir).join(DEFAULT_BUCKETS_DIR),
            },
            Some(SchemaConfig::Explicit {
                schema,
                migrations,
                buckets_dir,
            }) => Self {
                schema: PathBuf::from(schema.as_deref().unwrap_or(DEFAULT_SCHEMA_PATH)),
                migrations: PathBuf::from(migrations.as_deref().unwrap_or(DEFAULT_MIGRATIONS_DIR)),
                buckets_dir: PathBuf::from(buckets_dir.as_deref().unwrap_or(DEFAULT_BUCKETS_DIR)),
            },
            None => Self {
                schema: PathBuf::from(DEFAULT_SCHEMA_PATH),
                migrations: PathBuf::from(DEFAULT_MIGRATIONS_DIR),
                buckets_dir: PathBuf::from(DEFAULT_BUCKETS_DIR),
            },
        }
    }
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct ClientTypeConfig {
    pub format: ClientFormat,
    pub output: String,
    /// For `format: dart`: directory (relative to sp00ky.yml) to run the Dart
    /// generator from — must be a Dart package that depends on `spooky_core`
    /// (that's where `dart run spooky_core:spooky_gen` resolves). Ignored for
    /// TypeScript. Defaults to the sp00ky.yml directory.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workdir: Option<String>,
}

#[derive(Debug, Deserialize, Serialize, Default)]
pub struct Sp00kyConfig {
    /// Cloud project slug (used by the cloud commands: deploy, status, env, ...)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub slug: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mode: Option<DeployMode>,
    /// SurrealDB config: version string or object with version/namespace/database.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub surrealdb: Option<SurrealDbConfig>,
    /// Sp00ky service versions — either a string (sets both ssp & scheduler)
    /// or an object `{ ssp: "...", scheduler: "..." }` for individual control.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version: Option<VersionConfig>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema: Option<SchemaConfig>,
    /// Application definitions (backends and frontends). Each key is the app name.
    #[serde(default)]
    pub apps: BTreeMap<String, AppConfig>,
    /// Server-side cron/interval schedules. Each key is the schedule name.
    /// A value may be a path to a YAML file instead of an inline definition.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub schedules: BTreeMap<String, crate::schedule_config::ScheduleConfig>,
    /// Server-side workflow DAGs. Same file-linking rule as `schedules`.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub workflows: BTreeMap<String, crate::schedule_config::WorkflowConfig>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub buckets: Vec<String>,
    #[serde(default, rename = "clientTypes", skip_serializing_if = "Vec::is_empty")]
    pub client_types: Vec<ClientTypeConfig>,
    /// Deployment configuration (SSP count, scaling options)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deployment: Option<DeploymentConfig>,
    /// How long finished jobs and run history are kept. Written to
    /// `_00_retention:default` at deploy time and read by the schedule engine's
    /// prune pass.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retention: Option<RetentionConfig>,
    /// Override the Sp00ky Cloud API endpoint (e.g. for staging).
    #[serde(default, rename = "cloudApi", skip_serializing_if = "Option::is_none")]
    pub cloud_api: Option<String>,
    /// Migration engine to use: "legacy" (default) or "surrealkit".
    #[serde(
        default,
        rename = "migrationEngine",
        skip_serializing_if = "Option::is_none"
    )]
    pub migration_engine: Option<String>,
    /// SurrealKit-specific configuration (only used when migrationEngine = "surrealkit").
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub surrealkit: Option<SurrealKitConfig>,
    /// Optional shell command run against the freshly-provisioned SurrealDB
    /// during a git-linked deploy's "migrating" window, before the app VMs
    /// start (e.g. `spky migrate prod`). When unset, automated (push-to-deploy)
    /// builds do NOT apply DB migrations — run `spky migrate prod` yourself, or
    /// set this command. Consumed by the cloud builder; see spooky-cloud
    /// `internal/linking/builder.go`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub migrate: Option<String>,
    /// `RUST_LOG` directive applied to the scheduler and SSP containers.
    /// Either a plain string (`trace`, `info`, `info,ssp=debug`, …) or a
    /// per-environment map `{ dev, cloud }`. Unset → defaults to `info`.
    #[serde(default, rename = "logLevel", skip_serializing_if = "Option::is_none")]
    pub log_level: Option<LogLevelConfig>,
    /// Storage mode for the internal `_00_list_ref` ref table.
    /// `single` (legacy): one shared `_00_list_ref` table. `dedicated`
    /// (default): per-user `_00_list_ref_user_<id>` tables created
    /// lazily by the SSP on first registration. The `_00_query`
    /// registration table is global in both modes; only `_00_list_ref`
    /// splits per user. Dedicated mode works around a SurrealDB v3
    /// LIVE-permission gap where cross-session INSERTs to a
    /// permission-gated table never fire LIVE notifications for the
    /// non-inserting subscriber.
    #[serde(default, rename = "refMode", skip_serializing_if = "Option::is_none")]
    pub ref_mode: Option<RefMode>,
    /// Enable realtime sync for unauthenticated (anonymous) clients. When
    /// `true`, the SSP materializes anonymous query registrations into a
    /// dedicated `_00_list_ref_anon` table (readable by anyone) and the
    /// client starts its `_00_list_ref` poll while signed out, so a logged-out
    /// visitor gets live updates over world-readable tables. Defaults to
    /// `false`: anonymous clients can read one-shot but never sync live.
    #[serde(
        default,
        rename = "anonymousLiveQueries",
        skip_serializing_if = "Option::is_none"
    )]
    pub anonymous_live_queries: Option<bool>,
}

// `RefMode` lives in `ssp-protocol` so the CLI, the SSP server, and any
// other crate that needs to derive table names share a single source of
// truth. Re-export here so existing import paths in the CLI keep working.
pub use ssp_protocol::RefMode;

/// Container mount point for the persistent bucket-storage volume. Buckets
/// using a file backend resolve to `file:{BUCKET_VOLUME_PATH}/<name>`.
pub const BUCKET_VOLUME_PATH: &str = "/buckets";

/// The SurrealDB `file:` backend value for a persistently-stored bucket,
/// rooted at the mounted volume with a per-bucket subdir.
pub fn bucket_backend_path(name: &str) -> String {
    format!("file:{BUCKET_VOLUME_PATH}/{name}")
}

/// Extract `(bucket_name, backend)` pairs from bucket SurrealQL. Mirrors the
/// `DEFINE BUCKET` name grammar used by `parser::extract_buckets`.
pub fn detect_bucket_backends(content: &str) -> Vec<(String, String)> {
    let re = Regex::new(
        r#"(?i)DEFINE\s+BUCKET\s+(?:OVERWRITE\s+|IF\s+NOT\s+EXISTS\s+)?(\w+)\s+BACKEND\s+"([^"]*)""#,
    )
    .expect("valid bucket backend regex");
    re.captures_iter(content)
        .map(|c| (c[1].to_string(), c[2].to_string()))
        .collect()
}

/// `retention:` in sp00ky.yml.
///
/// Asymmetric by outcome, because the two are read for different reasons: a
/// successful job is inspected once if at all, while a failure is the thing you
/// went looking for. Anything unset falls back to [`RetentionConfig::DEFAULTS`].
#[derive(Debug, Deserialize, Serialize, Clone, Default)]
#[serde(deny_unknown_fields)]
pub struct RetentionConfig {
    /// Outbox jobs in `success` (`6h`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub success: Option<String>,
    /// Outbox jobs in `failed` (`14d`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failed: Option<String>,
    /// Run history in `success` / `skipped` / `replaced` (`24h`).
    #[serde(default, rename = "runSuccess", skip_serializing_if = "Option::is_none")]
    pub run_success: Option<String>,
    /// Run history in `failed` / `killed` (`30d`).
    #[serde(default, rename = "runFailed", skip_serializing_if = "Option::is_none")]
    pub run_failed: Option<String>,
    /// Hard cap on rows in a trimmable (successful) status, per table. Unset or `0`
    /// disables it. The windows above already bound volume at rate x window; this is
    /// the valve for a fan-out so wide that even a short window holds too much.
    /// Failures are never trimmed by it.
    #[serde(default, rename = "maxRows", skip_serializing_if = "Option::is_none")]
    pub max_rows: Option<i64>,
    /// Wall-clock ceiling on ONE run, for schedules that set no `deadline:` of
    /// their own (`1h`). `0` disables the reaper entirely, which is the behaviour
    /// every deployment had before this existed and a poor thing to choose.
    ///
    /// It lives under `retention:` because it is the same kind of knob: a
    /// project-wide policy the engine reads out of the database, so it can be
    /// retuned with one UPDATE and no redeploy. A schedule with genuinely
    /// long-running work sets its own `deadline:` instead of raising this, since
    /// raising this also raises how long one wedged fan-out key can suppress the
    /// rest of its schedule under `concurrency: skip`.
    #[serde(default, rename = "runDeadline", skip_serializing_if = "Option::is_none")]
    pub run_deadline: Option<String>,
    /// Default history mode for every schedule. `failures-only` means a successful
    /// execution leaves nothing behind but a rollup counter. A schedule's own
    /// `history:` overrides this in either direction.
    ///
    /// Resolved at DEPLOY time into each `_00_schedule.history_mode` rather than read
    /// by the engine, so there is exactly one field to read at runtime and no staleness
    /// window — at the cost of needing a redeploy to change, unlike the windows above
    /// which are one `UPDATE` away.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mode: Option<crate::schedule_config::HistoryMode>,
}

/// Resolved retention windows in seconds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResolvedRetention {
    pub success_secs: i64,
    pub failed_secs: i64,
    pub run_success_secs: i64,
    pub run_failed_secs: i64,
    /// `0` disables the cap.
    pub max_rows: i64,
    /// `0` disables the run reaper.
    pub run_deadline_secs: i64,
}

impl RetentionConfig {
    /// `(job success, job failed, run success, run failed)` in seconds.
    ///
    /// The run-failure window matches what the engine used to hardcode, so
    /// upgrading can never drop failure history a project already had. Job rows get
    /// a much shorter success window because nothing reads them once the job is
    /// done, and they are the ones that pile up: a minutely fan-out over 500 rows
    /// writes 500 of them a minute.
    pub const DEFAULTS: ResolvedRetention = ResolvedRetention {
        success_secs: 6 * 60 * 60,
        failed_secs: 14 * 24 * 60 * 60,
        run_success_secs: 24 * 60 * 60,
        run_failed_secs: 30 * 24 * 60 * 60,
        // Off by default: the age windows are the mechanism, and a cap that
        // surprises someone by deleting inside their retention window is worse than
        // no cap at all.
        max_rows: 0,
        // On by default, and generous: this is a backstop against a run that is
        // STUCK, not a service-level objective. Before it existed a run that stopped
        // making progress stayed `running` forever and held its fan-out key with it.
        run_deadline_secs: 60 * 60,
    };
}

impl Sp00kyConfig {
    /// Project-wide default history mode, or `None` when unset.
    pub fn default_history_mode(&self) -> Option<crate::schedule_config::HistoryMode> {
        self.retention.as_ref().and_then(|r| r.mode)
    }

    /// Retention windows in seconds, with every unset value defaulted.
    pub fn resolved_retention(&self) -> Result<ResolvedRetention> {
        let d = RetentionConfig::DEFAULTS;
        let Some(cfg) = self.retention.as_ref() else { return Ok(d) };
        let secs = |raw: &Option<String>, default: i64| -> Result<i64> {
            match raw {
                Some(raw) => Ok(crate::schedule_config::parse_duration_ms(raw)? / 1000),
                None => Ok(default),
            }
        };
        Ok(ResolvedRetention {
            success_secs: secs(&cfg.success, d.success_secs)?,
            failed_secs: secs(&cfg.failed, d.failed_secs)?,
            run_success_secs: secs(&cfg.run_success, d.run_success_secs)?,
            run_failed_secs: secs(&cfg.run_failed, d.run_failed_secs)?,
            max_rows: cfg.max_rows.unwrap_or(d.max_rows).max(0),
            // `secs` already falls back to the default when unset; an explicit `0`
            // is a deliberate opt-out and must survive.
            run_deadline_secs: secs(&cfg.run_deadline, d.run_deadline_secs)?.max(0),
        })
    }

    /// GB allocated for persistent bucket storage, or `None` when disabled
    /// (no `deployment.storage` block, or `sizeGB` unset/zero). When `Some`,
    /// buckets use a file backend and a storage volume is provisioned.
    pub fn bucket_storage_gb(&self) -> Option<u32> {
        self.deployment
            .as_ref()
            .and_then(|d| d.storage.as_ref())
            .and_then(|s| s.size_gb)
            .filter(|gb| *gb > 0)
    }

    /// Resolved ref mode, falling back to the default when unset in YAML.
    pub fn resolved_ref_mode(&self) -> RefMode {
        self.ref_mode.unwrap_or_default()
    }

    /// Whether anonymous (unauthenticated) live queries are enabled.
    /// Defaults to `false` when unset in YAML.
    pub fn resolved_anonymous_live_queries(&self) -> bool {
        self.anonymous_live_queries.unwrap_or(false)
    }
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct SurrealKitConfig {
    /// Path to the surrealkit binary. Defaults to "surrealkit" (found via PATH).
    pub binary: Option<String>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct DeploymentConfig {
    /// Number of SSP instances to provision (overrides plan default)
    #[serde(default, rename = "sspCount", skip_serializing_if = "Option::is_none")]
    pub ssp_count: Option<u32>,
    /// Backup configuration
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub backup: Option<BackupConfig>,
    /// Persistent bucket storage configuration. When set with a positive
    /// `sizeGB`, buckets use a file backend on a mounted volume instead of
    /// memory, and the size provisions an extra disk (dev docker mount + cloud).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub storage: Option<StorageConfig>,
    /// Extra environment variables for the INFRA containers, keyed by role
    /// (`ssp`, `scheduler`, `surrealdb`). Backends get their env from the
    /// vault (`spky env set`); this is the equivalent for the containers the
    /// control plane owns, which until now took a fixed, closed env list.
    ///
    /// ```yaml
    /// deployment:
    ///   env:
    ///     ssp:
    ///       SPKY_SSP_MERGE_VIEWS: "true"
    /// ```
    ///
    /// Values are NOT secrets: they are stored unencrypted in the project's
    /// cloud config, unlike `spky env set`. Keys the control plane manages
    /// itself (`SPKY_DB_*`, `SPKY_AUTH_SECRET`, `SPKY_SSP_ID`, …) are refused
    /// server-side rather than silently overridden.
    ///
    /// Omitted → the control plane keeps whatever was set on the last deploy;
    /// an explicit empty map clears it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub env: Option<BTreeMap<String, BTreeMap<String, String>>>,
}

/// Persistent storage allocation for bucket files. `sizeGB` is a
/// provisioning size only — it sizes the host/cloud disk mounted at
/// [`BUCKET_VOLUME_PATH`]; SurrealDB's `file:` backend does not enforce a
/// byte quota (per-file limits stay enforced by each bucket's PERMISSIONS).
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct StorageConfig {
    /// GB to allocate for persistent bucket files. A positive value enables
    /// the file backend for buckets and mounts the storage volume.
    #[serde(default, rename = "sizeGB", skip_serializing_if = "Option::is_none")]
    pub size_gb: Option<u32>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct BackupConfig {
    /// Enable automated backups
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub enabled: Option<bool>,
    /// Cron schedule (e.g., "0 2 * * *" for 2am daily)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schedule: Option<String>,
    /// Number of backups to retain
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retention: Option<u32>,
    /// External S3-compatible bucket URL (skip MinIO if set)
    #[serde(default, rename = "bucketUrl", skip_serializing_if = "Option::is_none")]
    pub bucket_url: Option<String>,
    /// Path to env file with BACKUP_ACCESS_KEY and BACKUP_SECRET_KEY
    #[serde(
        default,
        rename = "credentialsEnvFile",
        skip_serializing_if = "Option::is_none"
    )]
    pub credentials_env_file: Option<String>,
}

// ── Unified Env Config ──────────────────────────────────────────────────────

/// A single environment variable source: "vault", a dotenv file path, or an inline map.
#[derive(Debug, Clone)]
pub enum EnvSource {
    /// "vault" (all vars) or a dotenv file path
    Str(String),
    /// Inline key-value map, e.g. `{ DB_URL: "localhost", PORT: 3000 }`
    Map(BTreeMap<String, serde_yaml::Value>),
    /// Vault with a whitelist of variable names, e.g. `{ vault: [DB_URL, API_KEY] }`
    Vault(Vec<String>),
    /// Per-environment split INSIDE a source list, e.g.
    /// `env: [ { dev: {...}, cloud: {...} }, { cloud: { vault: [...] } } ]`.
    /// Without this variant such a map fell into `Map` and was stringified
    /// into literal `dev=...` / `cloud=...` env entries, which is how a CLI
    /// deploy shipped a backend with no real env (and no vault secrets).
    PerEnvironment {
        dev: Option<Box<EnvEntry>>,
        cloud: Option<Box<EnvEntry>>,
    },
}

impl Serialize for EnvSource {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        match self {
            EnvSource::Str(s) => serializer.serialize_str(s),
            EnvSource::Map(m) => m.serialize(serializer),
            EnvSource::Vault(keys) => {
                use serde::ser::SerializeMap;
                let mut map = serializer.serialize_map(Some(1))?;
                map.serialize_entry("vault", keys)?;
                map.end()
            }
            EnvSource::PerEnvironment { dev, cloud } => {
                use serde::ser::SerializeMap;
                let mut map = serializer.serialize_map(None)?;
                if let Some(d) = dev {
                    map.serialize_entry("dev", d)?;
                }
                if let Some(c) = cloud {
                    map.serialize_entry("cloud", c)?;
                }
                map.end()
            }
        }
    }
}

impl<'de> Deserialize<'de> for EnvSource {
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        let value = serde_yaml::Value::deserialize(deserializer)?;
        match &value {
            serde_yaml::Value::String(s) => Ok(EnvSource::Str(s.clone())),
            serde_yaml::Value::Mapping(m) => {
                // Check for vault whitelist: { vault: [KEY1, KEY2, ...] }
                let vault_key = serde_yaml::Value::String("vault".into());
                if m.len() == 1 {
                    if let Some(val) = m.get(&vault_key) {
                        if let serde_yaml::Value::Sequence(seq) = val {
                            let keys: Vec<String> = seq
                                .iter()
                                .filter_map(|v| v.as_str().map(|s| s.to_string()))
                                .collect();
                            return Ok(EnvSource::Vault(keys));
                        }
                    }
                }
                // Per-environment split ({ dev, cloud } only keys) — the same
                // rule as EnvConfig's top-level detection, applied to list
                // items so `env: [ {dev:…, cloud:…}, {cloud: {vault: […]}} ]`
                // resolves per environment instead of degrading to Map.
                let keys: Vec<&str> = m.keys().filter_map(|k| k.as_str()).collect();
                let is_per_env =
                    !keys.is_empty() && keys.iter().all(|k| *k == "dev" || *k == "cloud");
                if is_per_env {
                    let side =
                        |name: &str| -> std::result::Result<Option<Box<EnvEntry>>, D::Error> {
                            let key = serde_yaml::Value::String(name.into());
                            m.get(&key)
                                .map(|v| {
                                    serde_yaml::from_value::<EnvEntry>(v.clone())
                                        .map(Box::new)
                                        .map_err(serde::de::Error::custom)
                                })
                                .transpose()
                        };
                    return Ok(EnvSource::PerEnvironment {
                        dev: side("dev")?,
                        cloud: side("cloud")?,
                    });
                }
                // Otherwise it's an inline key-value map
                let map = m
                    .iter()
                    .filter_map(|(k, v)| k.as_str().map(|s| (s.to_string(), v.clone())))
                    .collect();
                Ok(EnvSource::Map(map))
            }
            _ => Err(serde::de::Error::custom(
                "env source must be a string or a map",
            )),
        }
    }
}

/// An env entry used inside `PerEnvironment`: a single source or a list of sources.
#[derive(Debug, Clone)]
pub enum EnvEntry {
    Source(EnvSource),
    List(Vec<EnvSource>),
}

impl Serialize for EnvEntry {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        match self {
            EnvEntry::Source(s) => s.serialize(serializer),
            EnvEntry::List(l) => l.serialize(serializer),
        }
    }
}

impl<'de> Deserialize<'de> for EnvEntry {
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        let value = serde_yaml::Value::deserialize(deserializer)?;
        match &value {
            serde_yaml::Value::Sequence(_) => {
                let sources: Vec<EnvSource> =
                    serde_yaml::from_value(value).map_err(serde::de::Error::custom)?;
                Ok(EnvEntry::List(sources))
            }
            _ => {
                let source: EnvSource =
                    serde_yaml::from_value(value).map_err(serde::de::Error::custom)?;
                Ok(EnvEntry::Source(source))
            }
        }
    }
}

/// Environment variable configuration.
///
/// Supports:
/// - `"vault"` or `"path/to/file"` — single string source
/// - `{ KEY: "val" }` — inline key-value map
/// - `{ dev: <entry>, cloud: <entry> }` — per-environment split
/// - `["vault", ".env", { KEY: "val" }]` — array of sources, merged in order
#[derive(Debug, Clone)]
pub enum EnvConfig {
    Source(EnvSource),
    PerEnvironment {
        dev: Option<EnvEntry>,
        cloud: Option<EnvEntry>,
    },
    List(Vec<EnvSource>),
}

impl Serialize for EnvConfig {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        match self {
            EnvConfig::Source(s) => s.serialize(serializer),
            EnvConfig::PerEnvironment { dev, cloud } => {
                use serde::ser::SerializeMap;
                let mut map = serializer.serialize_map(None)?;
                if let Some(d) = dev {
                    map.serialize_entry("dev", d)?;
                }
                if let Some(c) = cloud {
                    map.serialize_entry("cloud", c)?;
                }
                map.end()
            }
            EnvConfig::List(l) => l.serialize(serializer),
        }
    }
}

impl<'de> Deserialize<'de> for EnvConfig {
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        let value = serde_yaml::Value::deserialize(deserializer)?;
        match &value {
            serde_yaml::Value::String(s) => Ok(EnvConfig::Source(EnvSource::Str(s.clone()))),
            serde_yaml::Value::Sequence(_) => {
                let sources: Vec<EnvSource> =
                    serde_yaml::from_value(value).map_err(serde::de::Error::custom)?;
                Ok(EnvConfig::List(sources))
            }
            serde_yaml::Value::Mapping(m) => {
                // If the object has ONLY "dev" and/or "cloud" keys → PerEnvironment
                let keys: Vec<&str> = m.keys().filter_map(|k| k.as_str()).collect();
                let is_per_env =
                    !keys.is_empty() && keys.iter().all(|k| *k == "dev" || *k == "cloud");

                if is_per_env {
                    let dev_key = serde_yaml::Value::String("dev".into());
                    let cloud_key = serde_yaml::Value::String("cloud".into());
                    let dev = m
                        .get(&dev_key)
                        .map(|v| serde_yaml::from_value::<EnvEntry>(v.clone()))
                        .transpose()
                        .map_err(serde::de::Error::custom)?;
                    let cloud = m
                        .get(&cloud_key)
                        .map(|v| serde_yaml::from_value::<EnvEntry>(v.clone()))
                        .transpose()
                        .map_err(serde::de::Error::custom)?;
                    Ok(EnvConfig::PerEnvironment { dev, cloud })
                } else {
                    // Delegate to EnvSource which handles vault whitelist + inline maps
                    let source: EnvSource =
                        serde_yaml::from_value(value).map_err(serde::de::Error::custom)?;
                    Ok(EnvConfig::Source(source))
                }
            }
            _ => Err(serde::de::Error::custom(
                "env config must be a string, map, or array",
            )),
        }
    }
}

impl Sp00kyConfig {
    pub fn resolved_schema(&self) -> ResolvedSchema {
        ResolvedSchema::from_config(&self.schema)
    }

    pub fn resolved_surrealdb(&self) -> ResolvedSurrealDb {
        ResolvedSurrealDb::from_config(&self.surrealdb)
    }

    /// Iterate over backend apps only.
    pub fn backends(&self) -> impl Iterator<Item = (&str, &AppConfig)> {
        self.apps
            .iter()
            .filter(|(_, app)| app.app_type == AppType::Backend)
            .map(|(name, app)| (name.as_str(), app))
    }

    /// Return the first frontend app, if any.
    pub fn frontend(&self) -> Option<(&str, &AppConfig)> {
        self.apps
            .iter()
            .find(|(_, app)| app.app_type == AppType::Frontend)
            .map(|(name, app)| (name.as_str(), app))
    }

    /// Iterate over docker apps (prebuilt-image apps) only.
    pub fn docker_apps(&self) -> impl Iterator<Item = (&str, &AppConfig)> {
        self.apps
            .iter()
            .filter(|(_, app)| app.app_type == AppType::Docker)
            .map(|(name, app)| (name.as_str(), app))
    }

    /// Resolve the surrealkit binary path (if migration engine is "surrealkit").
    pub fn resolved_surrealkit_binary(&self) -> Option<String> {
        if self.migration_engine.as_deref() == Some("surrealkit") {
            Some(
                self.surrealkit
                    .as_ref()
                    .and_then(|c| c.binary.clone())
                    .unwrap_or_else(|| "surrealkit".to_string()),
            )
        } else {
            None
        }
    }

    /// Validate the `dependsOn` graph among `type: docker` apps: every referenced
    /// app must exist and be a docker app, no self-dependency, and no cycle.
    fn validate_docker_depends_on(&self) -> Result<()> {
        use std::collections::{BTreeMap, BTreeSet, VecDeque};
        let docker: BTreeSet<&str> = self.docker_apps().map(|(n, _)| n).collect();
        // in-degree = number of deps each node waits on; edges = dep → dependents.
        let mut indegree: BTreeMap<&str, usize> = docker.iter().map(|n| (*n, 0usize)).collect();
        let mut edges: BTreeMap<&str, Vec<&str>> = BTreeMap::new();
        for (name, app) in self.docker_apps() {
            for dep in &app.depends_on {
                if dep.as_str() == name {
                    bail!("docker app '{}' cannot dependsOn itself", name);
                }
                if !docker.contains(dep.as_str()) {
                    bail!(
                        "docker app '{}' dependsOn unknown docker app '{}'",
                        name,
                        dep
                    );
                }
                edges.entry(dep.as_str()).or_default().push(name);
                *indegree.get_mut(name).unwrap() += 1;
            }
        }
        // Kahn: peel zero-indegree nodes; anything left is part of a cycle.
        let mut q: VecDeque<&str> = indegree
            .iter()
            .filter(|(_, d)| **d == 0)
            .map(|(n, _)| *n)
            .collect();
        let mut processed = 0usize;
        while let Some(n) = q.pop_front() {
            processed += 1;
            for &m in edges.get(n).map(|v| v.as_slice()).unwrap_or(&[]) {
                let d = indegree.get_mut(m).unwrap();
                *d -= 1;
                if *d == 0 {
                    q.push_back(m);
                }
            }
        }
        if processed != docker.len() {
            let in_cycle: Vec<&str> = indegree
                .iter()
                .filter(|(_, d)| **d > 0)
                .map(|(n, _)| *n)
                .collect();
            bail!("dependsOn cycle among docker apps: {}", in_cycle.join(", "));
        }
        Ok(())
    }

    /// Validate hosting configuration for SurrealDB and all apps.
    pub fn validate(&self) -> Result<()> {
        if let Some(cfg) = &self.surrealdb {
            cfg.validate_raw()?;
        }
        self.resolved_surrealdb().validate()?;
        for (name, app) in &self.apps {
            app.validate(name)?;
        }
        self.validate_docker_depends_on()?;
        crate::schedule_config::validate_all(&self.schedules, &self.workflows)?;
        // logLevel: walk every directive string and confirm it parses.
        if let Some(cfg) = &self.log_level {
            match cfg {
                LogLevelConfig::Single(s) => validate_rust_log(s)?,
                LogLevelConfig::PerEnvironment { dev, cloud } => {
                    if let Some(s) = dev {
                        validate_rust_log(s)?;
                    }
                    if let Some(s) = cloud {
                        validate_rust_log(s)?;
                    }
                }
            }
        }
        Ok(())
    }

    /// Resolved `RUST_LOG` value for the given environment. Falls back to
    /// `info` when `logLevel` is unset or has no entry for the requested env,
    /// preserving today's behavior for projects that don't opt in.
    pub fn resolved_log_level(&self, env: DeployEnv) -> String {
        self.log_level
            .as_ref()
            .and_then(|c| c.resolved(env))
            .unwrap_or_else(|| "info".to_string())
    }
}

/// Crates whose logs a bare `logLevel` applies to under `spky dev`.
const DEV_LOG_TARGETS: &[&str] = &[
    "ssp",
    "ssp_server",
    "scheduler",
    "maintenance",
    "schedule_core",
    "ssp_protocol",
];

/// Dev only: widen a bare level (`info`, `debug`, `trace`) into per-crate
/// directives so hyper/h2/tokio/surrealdb stay at `warn` instead of flooding
/// the terminal. A bare level is a *global* `EnvFilter` directive, which is why
/// `logLevel: debug` used to turn `spky dev` into a firehose.
///
/// - `warn` / `error` / `off` are already quiet: returned unchanged.
/// - Anything containing `,` or `=` is a hand-written directive list: passed
///   through untouched so power users keep full control.
pub fn scoped_rust_log(level: &str) -> String {
    let level = level.trim();
    if level.contains(',') || level.contains('=') {
        return level.to_string();
    }
    match level {
        "trace" | "debug" | "info" => {
            let mut parts: Vec<String> = DEV_LOG_TARGETS
                .iter()
                .map(|t| format!("{}={}", t, level))
                .collect();
            parts.push("warn".to_string());
            parts.join(",")
        }
        _ => level.to_string(),
    }
}

#[cfg(test)]
mod scoped_rust_log_tests {
    use super::scoped_rust_log;

    #[test]
    fn bare_levels_are_scoped() {
        let s = scoped_rust_log("info");
        assert!(s.starts_with("ssp=info,"));
        assert!(s.contains("scheduler=info"));
        assert!(s.ends_with(",warn"));
        assert!(scoped_rust_log("debug").contains("ssp_server=debug"));
        assert!(scoped_rust_log(" trace ").contains("schedule_core=trace"));
    }

    #[test]
    fn quiet_levels_pass_through() {
        assert_eq!(scoped_rust_log("warn"), "warn");
        assert_eq!(scoped_rust_log("error"), "error");
        assert_eq!(scoped_rust_log("off"), "off");
    }

    #[test]
    fn directives_pass_through() {
        assert_eq!(scoped_rust_log("ssp=debug,warn"), "ssp=debug,warn");
        assert_eq!(scoped_rust_log("a=b"), "a=b");
    }
}

/// Per-component pin: either a Docker tag, or a path to a host binary.
/// A bare string parses as `Tag`, the object form `{ path: "..." }` parses
/// as `Path`. Path-mode opts that one component into "host process" launch;
/// see `RuntimeSource` and `dev::run_direct_mode`.
#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(untagged, deny_unknown_fields)]
pub enum ComponentVersion {
    /// Bare string => Docker tag (e.g. "canary", "v0.1.0").
    Tag(String),
    /// `{ path: "../../target/debug/ssp-server" }` => host process.
    Path { path: PathBuf },
}

/// Inner shape: a string (applies to both ssp & scheduler) or `{ssp, scheduler}`.
/// Used both as the flat `version` value and as the per-env entry inside `VersionConfig::PerEnvironment`.
///
/// `All` is intentionally tag-only — a single value applied to both components
/// can't be a path, since the SSP and scheduler are different binaries. Use
/// `Individual` for path-mode.
#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(untagged, deny_unknown_fields)]
pub enum VersionSpec {
    All(String),
    Individual {
        #[serde(default)]
        ssp: Option<ComponentVersion>,
        #[serde(default)]
        scheduler: Option<ComponentVersion>,
    },
}

/// Either a single spec applied everywhere, or one spec per environment.
/// Custom (de)serialize disambiguates `{ssp, scheduler}` from `{dev, cloud}` by key inspection,
/// mirroring `EnvConfig`.
#[derive(Debug, Clone)]
pub enum VersionConfig {
    Single(VersionSpec),
    PerEnvironment {
        dev: Option<VersionSpec>,
        cloud: Option<VersionSpec>,
    },
}

impl Serialize for VersionConfig {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        match self {
            VersionConfig::Single(s) => s.serialize(serializer),
            VersionConfig::PerEnvironment { dev, cloud } => {
                use serde::ser::SerializeMap;
                let mut map = serializer.serialize_map(None)?;
                if let Some(d) = dev {
                    map.serialize_entry("dev", d)?;
                }
                if let Some(c) = cloud {
                    map.serialize_entry("cloud", c)?;
                }
                map.end()
            }
        }
    }
}

impl<'de> Deserialize<'de> for VersionConfig {
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        let value = serde_yaml::Value::deserialize(deserializer)?;
        match &value {
            serde_yaml::Value::String(_) => {
                let spec: VersionSpec =
                    serde_yaml::from_value(value).map_err(serde::de::Error::custom)?;
                Ok(VersionConfig::Single(spec))
            }
            serde_yaml::Value::Mapping(m) => {
                // If the object has ONLY "dev" and/or "cloud" keys → PerEnvironment.
                let keys: Vec<&str> = m.keys().filter_map(|k| k.as_str()).collect();
                let is_per_env =
                    !keys.is_empty() && keys.iter().all(|k| *k == "dev" || *k == "cloud");

                if is_per_env {
                    let dev_key = serde_yaml::Value::String("dev".into());
                    let cloud_key = serde_yaml::Value::String("cloud".into());
                    let dev = m
                        .get(&dev_key)
                        .map(|v| serde_yaml::from_value::<VersionSpec>(v.clone()))
                        .transpose()
                        .map_err(serde::de::Error::custom)?;
                    let cloud = m
                        .get(&cloud_key)
                        .map(|v| serde_yaml::from_value::<VersionSpec>(v.clone()))
                        .transpose()
                        .map_err(serde::de::Error::custom)?;
                    Ok(VersionConfig::PerEnvironment { dev, cloud })
                } else {
                    // Empty map or other keys → fall through to VersionSpec::Individual,
                    // whose `deny_unknown_fields` will reject typos.
                    let spec: VersionSpec =
                        serde_yaml::from_value(value).map_err(serde::de::Error::custom)?;
                    Ok(VersionConfig::Single(spec))
                }
            }
            _ => Err(serde::de::Error::custom("version must be a string or map")),
        }
    }
}

/// `RUST_LOG` directive shape — same dual-form idiom as `VersionConfig`.
/// A plain string applies in every environment; a `{ dev, cloud }` map sets
/// per-environment levels. `LogLevelConfig::resolved(env)` collapses both
/// shapes to an `Option<String>`.
#[derive(Debug, Clone)]
pub enum LogLevelConfig {
    Single(String),
    PerEnvironment {
        dev: Option<String>,
        cloud: Option<String>,
    },
}

impl LogLevelConfig {
    /// Resolve the level for a given environment. Returns `None` when the
    /// per-env map has no entry for `env` so the caller can apply a default.
    pub fn resolved(&self, env: DeployEnv) -> Option<String> {
        match self {
            LogLevelConfig::Single(s) => Some(s.clone()),
            LogLevelConfig::PerEnvironment { dev, cloud } => match env {
                DeployEnv::Dev => dev.clone(),
                DeployEnv::Cloud => cloud.clone(),
            },
        }
    }
}

impl Serialize for LogLevelConfig {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        match self {
            LogLevelConfig::Single(s) => serializer.serialize_str(s),
            LogLevelConfig::PerEnvironment { dev, cloud } => {
                use serde::ser::SerializeMap;
                let mut map = serializer.serialize_map(None)?;
                if let Some(d) = dev {
                    map.serialize_entry("dev", d)?;
                }
                if let Some(c) = cloud {
                    map.serialize_entry("cloud", c)?;
                }
                map.end()
            }
        }
    }
}

impl<'de> Deserialize<'de> for LogLevelConfig {
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        let value = serde_yaml::Value::deserialize(deserializer)?;
        match &value {
            serde_yaml::Value::String(s) => Ok(LogLevelConfig::Single(s.clone())),
            serde_yaml::Value::Mapping(m) => {
                let keys: Vec<&str> = m.keys().filter_map(|k| k.as_str()).collect();
                let known = keys.iter().all(|k| *k == "dev" || *k == "cloud");
                if !known || keys.is_empty() {
                    return Err(serde::de::Error::custom(
                        "logLevel map must contain only `dev` and/or `cloud` keys",
                    ));
                }
                let dev = m
                    .get(serde_yaml::Value::String("dev".into()))
                    .map(|v| {
                        v.as_str().map(String::from).ok_or_else(|| {
                            serde::de::Error::custom("logLevel.dev must be a string")
                        })
                    })
                    .transpose()?;
                let cloud = m
                    .get(serde_yaml::Value::String("cloud".into()))
                    .map(|v| {
                        v.as_str().map(String::from).ok_or_else(|| {
                            serde::de::Error::custom("logLevel.cloud must be a string")
                        })
                    })
                    .transpose()?;
                Ok(LogLevelConfig::PerEnvironment { dev, cloud })
            }
            _ => Err(serde::de::Error::custom(
                "logLevel must be a string or { dev, cloud } map",
            )),
        }
    }
}

/// Validate a `RUST_LOG` directive string. Accepts either a bare level
/// (`trace|debug|info|warn|error|off`) or a comma-separated list of
/// `target=level` directives matching the `tracing-subscriber` grammar.
fn validate_rust_log(s: &str) -> Result<()> {
    if s.trim().is_empty() {
        anyhow::bail!("logLevel value cannot be empty");
    }
    let valid_level =
        |lv: &str| matches!(lv, "trace" | "debug" | "info" | "warn" | "error" | "off");
    for token in s.split(',') {
        let token = token.trim();
        if token.is_empty() {
            anyhow::bail!("logLevel `{}` has empty directive", s);
        }
        if let Some((_target, level)) = token.split_once('=') {
            if !valid_level(level.trim()) {
                anyhow::bail!(
                    "logLevel `{}` — directive `{}` has invalid level (use trace|debug|info|warn|error|off)",
                    s, token
                );
            }
        } else if !valid_level(token) {
            anyhow::bail!(
                "logLevel `{}` — `{}` is not a valid level (use trace|debug|info|warn|error|off, or `target=level`)",
                s, token
            );
        }
    }
    Ok(())
}

/// Which environment a `from_config` call is resolving versions for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)] // `Cloud` is reserved; cloud.rs does not yet read versions.
pub enum DeployEnv {
    Dev,
    Cloud,
}

const DEFAULT_SURREALDB_VERSION: &str = "v3.1.0-beta.3";
const DEFAULT_SSP_VERSION: &str = "canary";
const DEFAULT_SCHEDULER_VERSION: &str = "canary";

/// How a single component (SSP or scheduler) should be launched in dev mode.
/// `Image` keeps the existing `docker run` path; `LocalBinary` spawns the
/// binary directly on the host. The `dev` runtime branches on this and
/// reshapes networking env vars accordingly (see `RuntimeUrls`).
#[derive(Debug, Clone)]
pub enum RuntimeSource {
    Image(String),        // tag, e.g. "canary"
    LocalBinary(PathBuf), // absolute path to a host binary
}

impl RuntimeSource {
    pub fn is_local(&self) -> bool {
        matches!(self, RuntimeSource::LocalBinary(_))
    }
}

/// Resolved version config with all defaults applied.
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct ResolvedVersions {
    pub surrealdb: String,
    pub ssp: RuntimeSource,
    pub scheduler: RuntimeSource,
}

impl Default for ResolvedVersions {
    fn default() -> Self {
        Self {
            surrealdb: DEFAULT_SURREALDB_VERSION.to_string(),
            ssp: RuntimeSource::Image(DEFAULT_SSP_VERSION.to_string()),
            scheduler: RuntimeSource::Image(DEFAULT_SCHEDULER_VERSION.to_string()),
        }
    }
}

/// Resolve a `ComponentVersion` (or absent value with default tag) into a
/// `RuntimeSource`. Relative paths are anchored at `project_dir` so users
/// can write `../../target/debug/ssp-server` from inside `example/` without
/// caring about the cwd at invocation time.
fn resolve_component(
    cv: Option<&ComponentVersion>,
    default_tag: &str,
    project_dir: &Path,
) -> RuntimeSource {
    match cv {
        None => RuntimeSource::Image(default_tag.to_string()),
        Some(ComponentVersion::Tag(t)) => RuntimeSource::Image(t.clone()),
        Some(ComponentVersion::Path { path }) => {
            let resolved = if path.is_absolute() {
                path.clone()
            } else {
                project_dir.join(path)
            };
            RuntimeSource::LocalBinary(resolved)
        }
    }
}

impl ResolvedVersions {
    /// Resolve versions for the given deploy environment:
    /// - `surrealdb`: from top-level `surrealdb` field, else default
    /// - `version: "tag"` → sets both ssp & scheduler to "tag" for all envs
    /// - `version: { ssp, scheduler }` → individual control, applies to all envs
    /// - `version: { dev: ..., cloud: ... }` → per-env override; missing key for the
    ///   requested env falls back to defaults (same as no `version` field).
    ///
    /// Anchors any relative path entries against the current working dir.
    /// Use `from_config_with_dir` to pin them to the project dir explicitly.
    #[allow(dead_code)] // Public API surface; tests prefer from_config_with_dir.
    pub fn from_config(config: &Sp00kyConfig, env: DeployEnv) -> Self {
        Self::from_config_with_dir(config, env, Path::new("."))
    }

    /// As `from_config`, but anchors any relative `path:` entries against
    /// `project_dir`. Use this from the dev runtime so `../../target/...`
    /// in `example/sp00ky.yml` resolves regardless of where the user invoked
    /// `spooky dev`.
    pub fn from_config_with_dir(config: &Sp00kyConfig, env: DeployEnv, project_dir: &Path) -> Self {
        let surrealdb = config.resolved_surrealdb().version;

        let spec: Option<&VersionSpec> = match &config.version {
            None => None,
            Some(VersionConfig::Single(s)) => Some(s),
            Some(VersionConfig::PerEnvironment { dev, cloud }) => match env {
                DeployEnv::Dev => dev.as_ref(),
                DeployEnv::Cloud => cloud.as_ref(),
            },
        };

        let (ssp, scheduler) = match spec {
            Some(VersionSpec::All(v)) => (
                RuntimeSource::Image(v.clone()),
                RuntimeSource::Image(v.clone()),
            ),
            Some(VersionSpec::Individual { ssp, scheduler }) => (
                resolve_component(ssp.as_ref(), DEFAULT_SSP_VERSION, project_dir),
                resolve_component(scheduler.as_ref(), DEFAULT_SCHEDULER_VERSION, project_dir),
            ),
            None => (
                RuntimeSource::Image(DEFAULT_SSP_VERSION.to_string()),
                RuntimeSource::Image(DEFAULT_SCHEDULER_VERSION.to_string()),
            ),
        };

        Self {
            surrealdb,
            ssp,
            scheduler,
        }
    }

    pub fn surrealdb_image(&self) -> String {
        format!("surrealdb/surrealdb:{}", self.surrealdb)
    }

    /// Image reference for the SSP, if it is launched from Docker.
    /// Returns `None` for `LocalBinary`.
    pub fn ssp_image(&self) -> Option<String> {
        match &self.ssp {
            RuntimeSource::Image(t) => Some(format!("mono424/spooky-ssp:{}", t)),
            RuntimeSource::LocalBinary(_) => None,
        }
    }

    /// Image reference for the scheduler, if it is launched from Docker.
    /// Returns `None` for `LocalBinary`.
    #[allow(dead_code)]
    pub fn scheduler_image(&self) -> Option<String> {
        match &self.scheduler {
            RuntimeSource::Image(t) => Some(format!("mono424/spooky-scheduler:{}", t)),
            RuntimeSource::LocalBinary(_) => None,
        }
    }

    /// Path to the SSP host binary, if configured. `None` for `Image`.
    #[allow(dead_code)]
    pub fn ssp_local_binary(&self) -> Option<&Path> {
        match &self.ssp {
            RuntimeSource::LocalBinary(p) => Some(p),
            RuntimeSource::Image(_) => None,
        }
    }

    /// Path to the scheduler host binary, if configured. `None` for `Image`.
    #[allow(dead_code)]
    pub fn scheduler_local_binary(&self) -> Option<&Path> {
        match &self.scheduler {
            RuntimeSource::LocalBinary(p) => Some(p),
            RuntimeSource::Image(_) => None,
        }
    }
}

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(untagged)]
pub enum BackendDevConfig {
    /// Raw shell command string, e.g. "node server.js"
    Command(String),
    /// Typed object form with type discriminator
    Typed(BackendDevTypedConfig),
}

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(tag = "type")]
pub enum BackendDevTypedConfig {
    #[serde(rename = "npm")]
    Npm {
        script: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        workdir: Option<String>,
        /// Port the dev server listens on, so `spky dev` can print its URL
        /// up front. Optional: without it the CLI reads the URL the server
        /// announces (e.g. vite's `Local: http://…`).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        port: Option<u16>,
    },
    #[serde(rename = "docker")]
    Docker {
        file: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        workdir: Option<String>,
        /// Single published port (host:container). Kept for back-compat; for
        /// more than one use `ports`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        port: Option<String>,
        /// Additional published ports (host:container), e.g. a REST + a gRPC
        /// port. Merged with `port`. Lets a backend that listens on two ports
        /// (like a relay with WS + gRPC) run through the native docker method.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        ports: Option<Vec<String>>,
        /// docker `--platform` for build + run, e.g. `linux/arm64` to run the
        /// image's toolchain natively on Apple Silicon instead of emulating the
        /// deploy (amd64) arch under QEMU.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        platform: Option<String>,
    },
    #[serde(rename = "uv")]
    Uv {
        script: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        workdir: Option<String>,
        /// See `Npm::port`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        port: Option<u16>,
    },
}

/// Unified deploy configuration for all app types.
/// Port defaults depend on app type (8080 for backends, 3000 for frontends).
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct AppDeployConfig {
    /// Dockerfile path (relative to sp00ky.yml)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dockerfile: Option<String>,
    /// Build context directory (relative to sp00ky.yml, defaults to project root)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context: Option<String>,
    /// Port the service listens on (no default — resolved by app type)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub port: Option<u16>,
    /// Additional published ports for apps requiring multiple ports.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ports: Option<Vec<String>>,
    /// Second container port carrying gRPC, exposed by the cloud via a
    /// dedicated h2c Traefik router at `<slug>-<name>-grpc.<domain>` (backend
    /// only). Keep in sync with spooky-cloud's linking builder schema.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub grpc_port: Option<u16>,
    /// Resource allocation for the VM
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resources: Option<BackendDeployResources>,
    /// Expose publicly via {slug}-{name}.fn.spky.cloud (backend only)
    #[serde(default)]
    pub expose: bool,
    /// Health check path for the scheduler to ping, e.g. "/health" (backend only)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub healthcheck: Option<String>,
    /// HTTP request timeout in seconds (backend only, default: 10)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout: Option<u32>,
    /// Whether the frontend can override the timeout per-job (backend only)
    #[serde(
        default,
        rename = "timeoutOverridable",
        skip_serializing_if = "Option::is_none"
    )]
    pub timeout_overridable: Option<bool>,
    /// Command override for the container (replaces ENTRYPOINT/CMD in image)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cmd: Option<String>,
    /// Build-time secrets/args passed to `docker build --build-arg` (any app
    /// type). Same shape as `env` (inline map, `vault:` whitelist, dotenv path).
    /// Build-time only: never injected into the container's runtime env. Keep in
    /// sync with spooky-cloud's linking builder (`deploy.build_args`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub build_args: Option<EnvConfig>,
    /// Static-frontend hosting (free/Cloudflare plan): build the SPA and ship the
    /// output dir to Cloudflare Workers Static Assets instead of a Docker image.
    #[serde(rename = "static", default, skip_serializing_if = "Option::is_none")]
    pub static_site: Option<StaticDeployConfig>,
}

/// Static SPA hosting config (`deploy.static`) for the free/Cloudflare plan.
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct StaticDeployConfig {
    /// Build command run in the app dir before upload (e.g. "npm run build").
    /// Optional: omit if the output dir already holds a built SPA.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub build: Option<String>,
    /// Directory (relative to the app dir) holding the built static site.
    #[serde(default = "default_static_dir")]
    pub dir: String,
}

fn default_static_dir() -> String {
    "dist".to_string()
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct BackendDeployResources {
    /// Number of vCPUs (default: 1)
    #[serde(default = "default_vcpus")]
    pub vcpus: u32,
    /// Memory in MB (default: 512)
    #[serde(default = "default_memory")]
    pub memory: u32,
    /// Disk in GB (default: 5)
    #[serde(default = "default_disk")]
    pub disk: u32,
}

fn default_vcpus() -> u32 {
    1
}
fn default_memory() -> u32 {
    512
}
fn default_disk() -> u32 {
    5
}

impl BackendDeployResources {
    pub fn validate(&self) -> Result<()> {
        if self.vcpus < 1 {
            bail!("resources.vcpus must be >= 1, got {}", self.vcpus);
        }
        if self.memory < 128 {
            bail!("resources.memory must be >= 128 MB, got {}", self.memory);
        }
        if self.disk < 1 {
            bail!("resources.disk must be >= 1 GB, got {}", self.disk);
        }
        Ok(())
    }
}

/// Unified application configuration — works for both backend and frontend apps.
#[derive(Debug, Deserialize, Serialize)]
pub struct AppConfig {
    /// App type: "backend" or "frontend" (required).
    #[serde(rename = "type")]
    pub app_type: AppType,

    /// Where this app runs: "all" (default), "devOnly", or "cloudOnly".
    #[serde(default, skip_serializing_if = "is_default_scope")]
    pub scope: AppScope,

    // ── Backend-specific fields ─────────────────────────────────────────
    /// "cloud" (default) or "external" — whether this backend is deployed to
    /// Sp00ky Cloud or self-hosted at `baseUrl`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hosting: Option<HostingMode>,
    /// Path to the OpenAPI specification file (required for backends).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub spec: Option<String>,
    /// Base URL for the backend service (required when hosting is "external").
    #[serde(default, rename = "baseUrl", skip_serializing_if = "Option::is_none")]
    pub base_url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auth: Option<AuthConfig>,
    /// Trigger method (required for backends).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub method: Option<BackendMethod>,

    // ── Docker-app fields (type: docker) ────────────────────────────────
    /// Prebuilt image to run (required for `type: docker`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub image: Option<String>,
    /// Published ports, e.g. `[7880, "7882/udp", "3000:8080"]`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub ports: Vec<PortSpec>,
    /// Args appended after the image (the container command), e.g. `["--dev"]`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub args: Vec<String>,
    /// Bind/volume mounts for `type: docker` apps, e.g.
    /// `["/var/run/docker.sock:/var/run/docker.sock", "${PROJECT_DIR}/../..:/src", "wp_gomod:/go"]`.
    /// `${PROJECT_DIR}` (abs dir of sp00ky.yml) is expanded in the host portion.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub volumes: Vec<String>,
    /// Working directory inside the container (`docker run -w`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workdir: Option<String>,
    /// Other docker apps that must be ready before this one starts (dev ordering).
    #[serde(default, rename = "dependsOn", skip_serializing_if = "Vec::is_empty")]
    pub depends_on: Vec<String>,
    /// Optional readiness probe: an HTTP path polled on the app's first published
    /// host port (e.g. `/health`) until 200. Lets a dependency signal readiness so
    /// `dependsOn` waits for actual up, not just "container started".
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub healthcheck: Option<String>,

    // ── Shared fields ───────────────────────────────────────────────────
    /// Dev server configuration (npm, docker, uv, or raw command).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dev: Option<BackendDevConfig>,
    /// Deployment configuration (dockerfile, port, resources, etc.)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deploy: Option<AppDeployConfig>,
    /// Environment variable configuration.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub env: Option<EnvConfig>,
}

impl AppConfig {
    pub fn resolved_hosting(&self) -> HostingMode {
        self.hosting.clone().unwrap_or_default()
    }

    /// Resolve the deploy port, falling back to type-specific defaults.
    pub fn deploy_port(&self) -> u16 {
        self.deploy
            .as_ref()
            .and_then(|d| d.port)
            // For a docker app, fall back to the first published port's host side.
            .or_else(|| {
                if self.app_type == AppType::Docker {
                    self.ports.first().and_then(|p| p.host_port())
                } else {
                    None
                }
            })
            .unwrap_or(match self.app_type {
                AppType::Backend => 8080,
                AppType::Frontend => 3000,
                AppType::Docker => 8080,
            })
    }

    /// True if `spky dev` should start this app (scope all or devOnly).
    pub fn runs_in_dev(&self) -> bool {
        self.scope != AppScope::CloudOnly
    }

    /// True if this app is deployed to the cloud (scope all or cloudOnly).
    pub fn deploys(&self) -> bool {
        self.scope != AppScope::DevOnly
    }

    pub fn validate(&self, name: &str) -> Result<()> {
        // A docker app always needs an image (regardless of scope).
        if self.app_type == AppType::Docker && self.image.is_none() {
            bail!("Docker app '{}' is missing required field 'image'", name);
        }
        // devOnly apps are local-only processes — they carry no spec/method/deploy,
        // so skip the backend deploy-shape requirements below. They still validate
        // `deploy` resources if a deploy block happens to be present (checked at end).
        if self.scope == AppScope::DevOnly {
            return self.validate_deploy_resources(name);
        }
        match self.app_type {
            AppType::Backend => {
                if self.spec.is_none() {
                    // bail!("Backend app '{}' is missing required field 'spec'", name);
                }
                if self.method.is_none() {
                    // bail!("Backend app '{}' is missing required field 'method'", name);
                }
                if self.resolved_hosting() == HostingMode::External && self.base_url.is_none() {
                    bail!(
                        "Backend app '{}' has hosting 'external' but no baseUrl was provided",
                        name
                    );
                }
            }
            AppType::Frontend => {
                if let Some(ref deploy) = self.deploy {
                    if deploy.dockerfile.is_none() {
                        bail!(
                            "Frontend app '{}' has 'deploy' but is missing 'dockerfile'",
                            name
                        );
                    }
                }
            }
            // image presence already checked above; nothing else required.
            AppType::Docker => {}
        }
        self.validate_deploy_resources(name)
    }

    fn validate_deploy_resources(&self, name: &str) -> Result<()> {
        if let Some(ref deploy) = self.deploy {
            if let Some(ref resources) = deploy.resources {
                resources
                    .validate()
                    .context(format!("Invalid resources for app '{}'", name))?;
            }
        }
        Ok(())
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub struct AuthConfig {
    #[serde(rename = "type")]
    pub auth_type: AuthType,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub token: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct BackendMethod {
    #[serde(rename = "type")]
    pub method_type: MethodType,
    pub schema: String,
    pub table: Option<String>,
    /// How many of this outbox table's jobs may be `processing` at once.
    /// Omitted => 1, which is what the runner did before this knob existed.
    /// Everything above the bound waits as a `pending` row and is admitted in
    /// `created_at` order — the outbox itself is the queue.
    ///
    /// Lives on `method:` rather than `deploy:` because it is a property of the
    /// outbox, not of the container: `deploy:` is serialized wholesale into the
    /// cloud deploy manifest, and this value never leaves the database.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub concurrency: Option<u32>,
}

#[derive(Debug, Serialize, Clone)]
pub struct BackendRouteArg {
    #[serde(rename = "type")]
    pub arg_type: String,
    pub required: bool,
}

#[derive(Debug, Serialize, Clone)]
pub struct BackendRoute {
    pub args: BTreeMap<String, BackendRouteArg>,
}

#[derive(Debug, Serialize, Clone)]
pub struct BackendDefinition {
    pub routes: BTreeMap<String, BackendRoute>,
    pub outbox_table: Option<String>,
}

/// Load and parse a Sp00kyConfig from the given path.
/// Returns a default config if the file doesn't exist or can't be parsed.
pub fn load_config(path: &Path) -> Sp00kyConfig {
    if !path.exists() {
        return default_config();
    }

    let content = match fs::read_to_string(path) {
        Ok(c) => c,
        Err(e) => {
            eprintln!(
                "\x1b[33mwarning\x1b[0m: could not read {}: {} — using defaults",
                path.display(),
                e
            );
            return default_config();
        }
    };

    let base_dir = path.parent().unwrap_or(Path::new("."));
    match parse_config_with_includes(&content, base_dir) {
        Ok(c) => c,
        Err(e) => {
            // Loud, not silent: a malformed manifest otherwise degrades quietly to
            // an empty config (no slug, no cloudApi → commands hit the wrong/prod
            // API and fail with confusing DNS errors). Surface the exact parse
            // error and location so the user can fix the manifest.
            // `{:#}` (not `{}`) so anyhow prints the whole source chain. With
            // `{}` the only line shown was the outermost context ("failed to
            // interpret sp00ky manifest"), which names neither the offending
            // key nor its line — useless for actually fixing the manifest.
            eprintln!(
                "\x1b[31merror\x1b[0m: failed to parse {}:\n  {:#}\n\
                 Falling back to default configuration — fix the manifest, then re-run.",
                path.display(),
                e
            );
            default_config()
        }
    }
}

/// Parse manifest text into a `Sp00kyConfig`, first resolving any
/// `apps.<name>.path` service includes at the raw-YAML level so that the typed
/// `AppConfig` never sees a reference-only entry. This is the single canonical
/// parse used by both `load_config` and `BackendProcessor::process`.
pub fn parse_config_with_includes(content: &str, base_dir: &Path) -> Result<Sp00kyConfig> {
    let mut root: serde_yaml::Value =
        serde_yaml::from_str(content).context("failed to parse sp00ky manifest")?;
    resolve_app_includes(&mut root, base_dir)?;
    resolve_linked_definitions(&mut root, base_dir)?;
    serde_yaml::from_value(root).context("failed to interpret sp00ky manifest")
}

/// Sections whose entries may be linked to an external YAML file instead of
/// being written inline. Resolved here, at the `Value` stage, for the same
/// reason app includes are: by the time serde sees the tree it must already be
/// the real thing.
const LINKABLE_SECTIONS: &[&str] = &["schedules", "workflows"];

/// Pull `schedules:` / `workflows:` definitions in from files.
///
/// Two forms, both relative to the manifest that names them:
///
/// ```yaml
/// schedules: ./schedules.yml            # the whole map lives in that file
/// workflows:
///   monthly-report: ./workflows/monthly-report.yml   # one definition per file
///   inline-one: { steps: { … } }                     # mixing is fine
/// ```
///
/// Deliberately one level deep: a linked file holding more links would make the
/// effective config depend on a traversal order nobody can see. If that is ever
/// wanted, it should be an explicit feature rather than a fallout of recursion.
fn resolve_linked_definitions(root: &mut serde_yaml::Value, base_dir: &Path) -> Result<()> {
    for section in LINKABLE_SECTIONS {
        let key = serde_yaml::Value::String((*section).to_string());
        let Some(value) = root.get(&key).cloned() else { continue };

        // Whole-section link: `schedules: ./schedules.yml`.
        if let serde_yaml::Value::String(path) = &value {
            let loaded = load_linked_yaml(base_dir, path, section)?;
            if !matches!(loaded, serde_yaml::Value::Mapping(_)) {
                bail!(
                    "{} file '{}' must contain a mapping of names to definitions",
                    section,
                    path
                );
            }
            if let serde_yaml::Value::Mapping(root_map) = root {
                root_map.insert(key, loaded);
            }
            continue;
        }

        // Per-entry links: one file per definition.
        let serde_yaml::Value::Mapping(entries) = value else { continue };
        let mut resolved = serde_yaml::Mapping::new();
        for (name, entry) in entries {
            match &entry {
                serde_yaml::Value::String(path) => {
                    let loaded = load_linked_yaml(base_dir, path, section)?;
                    if !matches!(loaded, serde_yaml::Value::Mapping(_)) {
                        bail!(
                            "{} '{}' points at '{}', which must contain a single definition mapping",
                            section,
                            value_key_str(&name),
                            path
                        );
                    }
                    resolved.insert(name, loaded);
                }
                _ => {
                    resolved.insert(name, entry);
                }
            }
        }
        if let serde_yaml::Value::Mapping(root_map) = root {
            root_map.insert(key, serde_yaml::Value::Mapping(resolved));
        }
    }
    Ok(())
}

fn load_linked_yaml(base_dir: &Path, rel: &str, section: &str) -> Result<serde_yaml::Value> {
    let path = base_dir.join(rel);
    let content = fs::read_to_string(&path).with_context(|| {
        format!("{} references '{}' but {} could not be read", section, rel, path.display())
    })?;
    serde_yaml::from_str(&content)
        .with_context(|| format!("failed to parse {} file {}", section, path.display()))
}

/// Keys inside an `AppConfig` (and its nested `deploy`/`dev`/`method` objects)
/// whose values are file paths resolved relative to the ROOT manifest dir.
/// When an app is pulled in from `apps.<name>.path: ./svc`, these are rebased
/// by prefixing the service dir so they still resolve against the root dir.
///
/// MUST track `AppConfig`'s path-bearing fields (see `AppConfig`,
/// `AppDeployConfig`, `BackendMethod`, `BackendDevTypedConfig`). Non-path
/// fields (e.g. `baseUrl` is a URL, `deploy.static.build` is a shell command,
/// docker `volumes` use the absolute `${PROJECT_DIR}`) are intentionally
/// excluded.
fn rebase_app_paths(app: &mut serde_yaml::Value, service_rel: &str) {
    // Top-level path fields.
    for key in ["spec"] {
        rebase_string_at(app, key, service_rel);
    }
    // Nested one-level path fields, keyed by parent object.
    // NOTE: `dev.file` is deliberately absent — for the docker dev method it is
    // resolved relative to `dev.workdir` (the CLI runs `docker build -f <file>
    // .` from workdir), not relative to the manifest dir, so prefixing the
    // service dir onto it would double-prefix. Only manifest-dir-relative paths
    // belong here.
    let nested: &[(&str, &[&str])] = &[
        ("method", &["schema"]),
        ("dev", &["workdir"]),
        ("deploy", &["dockerfile", "context"]),
    ];
    for (parent, keys) in nested {
        if let Some(obj) = app.get_mut(parent) {
            for key in *keys {
                rebase_string_at(obj, key, service_rel);
            }
        }
    }
    // deploy.static.dir
    if let Some(static_obj) = app.get_mut("deploy").and_then(|d| d.get_mut("static")) {
        rebase_string_at(static_obj, "dir", service_rel);
    }
    // `schedules` / `workflows` file links: a whole-section string, or a string
    // per entry. Both are relative to the manifest that names them, so a
    // per-service include's links need the service dir prefixed.
    for section in LINKABLE_SECTIONS {
        match app.get_mut(*section) {
            Some(serde_yaml::Value::String(s)) => {
                if is_rebasable_path(s) {
                    *s = joined_rel(service_rel, s);
                }
            }
            Some(serde_yaml::Value::Mapping(entries)) => {
                for (_, value) in entries.iter_mut() {
                    if let serde_yaml::Value::String(s) = value {
                        if is_rebasable_path(s) {
                            *s = joined_rel(service_rel, s);
                        }
                    }
                }
            }
            _ => {}
        }
    }
    // `env` may be a bare dotenv path string, or a `{ dev, cloud }` map whose
    // string entries are dotenv paths. Vault/inline-map forms are left alone.
    match app.get_mut("env") {
        Some(serde_yaml::Value::String(s)) => {
            if is_rebasable_path(s) {
                *s = joined_rel(service_rel, s);
            }
        }
        Some(env @ serde_yaml::Value::Mapping(_)) => {
            for key in ["dev", "cloud"] {
                rebase_string_at(env, key, service_rel);
            }
        }
        _ => {}
    }
}

/// If `parent[key]` is a rebasable relative path string, prefix it with the
/// service dir. No-op when the key is absent or not a plain string.
fn rebase_string_at(parent: &mut serde_yaml::Value, key: &str, service_rel: &str) {
    if let Some(serde_yaml::Value::String(s)) = parent.get_mut(key) {
        if is_rebasable_path(s) {
            *s = joined_rel(service_rel, s);
        }
    }
}

/// A value is rebasable when it looks like a relative filesystem path: not
/// absolute, not a URL, and not a `${PROJECT_DIR}`-anchored docker mount.
fn is_rebasable_path(s: &str) -> bool {
    !s.is_empty()
        && !s.starts_with('/')
        && !s.contains("://")
        && !s.starts_with('$')
}

/// Join a service-relative dir with a path written relative to that service,
/// yielding a path relative to the root manifest dir (forward slashes).
fn joined_rel(service_rel: &str, value: &str) -> String {
    let service_rel = service_rel.strip_prefix("./").unwrap_or(service_rel);
    let value = value.strip_prefix("./").unwrap_or(value);
    Path::new(service_rel)
        .join(value)
        .to_string_lossy()
        .replace('\\', "/")
}

/// Recursively merge `overlay` into `base`: for two mappings, each overlay key
/// is merged into the matching base key (recursing when both are mappings);
/// any other overlay value replaces the base value outright. Implements the
/// "main overrides sub" precedence.
fn merge_value(base: &mut serde_yaml::Value, overlay: serde_yaml::Value) {
    match (base, overlay) {
        (serde_yaml::Value::Mapping(base_map), serde_yaml::Value::Mapping(overlay_map)) => {
            for (k, v) in overlay_map {
                match base_map.get_mut(&k) {
                    Some(existing) => merge_value(existing, v),
                    None => {
                        base_map.insert(k, v);
                    }
                }
            }
        }
        (base, overlay) => *base = overlay,
    }
}

/// Resolve `apps.<name>.path` service includes in place. For every app entry
/// that carries a `path`, load `<path>/sp00ky.app.yml` (a bare `AppConfig`),
/// rebase its relative paths against the root manifest dir, then overlay the
/// remaining root-entry keys on top (main overrides sub). Entries without a
/// `path` are left untouched.
fn resolve_app_includes(root: &mut serde_yaml::Value, base_dir: &Path) -> Result<()> {
    let apps = match root.get_mut("apps") {
        Some(serde_yaml::Value::Mapping(apps)) => apps,
        _ => return Ok(()),
    };

    // Collect names to resolve first to avoid borrow juggling while mutating.
    let names: Vec<serde_yaml::Value> = apps
        .iter()
        .filter(|(_, v)| v.get("path").and_then(|p| p.as_str()).is_some())
        .map(|(k, _)| k.clone())
        .collect();

    for name in names {
        let entry = apps.get(&name).cloned().unwrap_or(serde_yaml::Value::Null);
        let service_rel = entry
            .get("path")
            .and_then(|p| p.as_str())
            .expect("filtered for path")
            .to_string();

        let include_path = base_dir.join(&service_rel).join(APP_INCLUDE_FILENAME);
        let sub_content = fs::read_to_string(&include_path).with_context(|| {
            format!(
                "app '{}' references path '{}' but {} could not be read",
                value_key_str(&name),
                service_rel,
                include_path.display()
            )
        })?;
        let mut sub: serde_yaml::Value = serde_yaml::from_str(&sub_content).with_context(|| {
            format!("failed to parse app include {}", include_path.display())
        })?;
        if !matches!(sub, serde_yaml::Value::Mapping(_)) {
            bail!(
                "app include {} must be a mapping (a bare single-app config)",
                include_path.display()
            );
        }

        rebase_app_paths(&mut sub, &service_rel);

        // Overlay the root entry (minus `path`) on top of the sub-file base.
        let mut overrides = entry;
        if let serde_yaml::Value::Mapping(map) = &mut overrides {
            map.remove(serde_yaml::Value::String("path".to_string()));
        }
        merge_value(&mut sub, overrides);

        apps.insert(name, sub);
    }

    Ok(())
}

/// Best-effort rendering of a mapping key for error messages.
fn value_key_str(v: &serde_yaml::Value) -> String {
    v.as_str().map(String::from).unwrap_or_else(|| format!("{:?}", v))
}

fn default_config() -> Sp00kyConfig {
    // Singlenode is the historical default for the dev orchestrator;
    // everything else matches `Sp00kyConfig::default()` so adding a
    // new field doesn't require updating this function.
    Sp00kyConfig {
        mode: Some(DeployMode::Singlenode),
        ..Default::default()
    }
}

pub struct BackendProcessor {
    pub schema_appends: String,
    pub backend_definitions: BTreeMap<String, BackendDefinition>,
    pub bucket_schema: String,
}

impl BackendProcessor {
    pub fn new() -> Self {
        Self {
            schema_appends: String::new(),
            backend_definitions: BTreeMap::new(),
            bucket_schema: String::new(),
        }
    }

    pub fn process(&mut self, config_path: &Path) -> Result<()> {
        let config_str = fs::read_to_string(config_path)
            .context(format!("Failed to read sp00ky config: {:?}", config_path))?;

        let base_dir = config_path.parent().unwrap_or(Path::new("."));

        let config: Sp00kyConfig = parse_config_with_includes(&config_str, base_dir)
            .context("Failed to parse sp00ky config")?;

        for (name, app) in &config.apps {
            if app.app_type != AppType::Backend {
                continue;
            }
            // devOnly backends are local-only host processes (e.g. the LiveKit SFU
            // run via `dev: "livekit-server …"`); they carry no spec/method/schema
            // to apply, matching AppConfig::validate's devOnly early-return. Without
            // this skip, process_backend bails "missing 'method' field".
            if app.scope == AppScope::DevOnly {
                continue;
            }
            // A backend with no `method` is a direct-HTTP service (e.g. the
            // stream-relay: clients fetch it directly, it is not outbox-driven).
            // It is still deployed - the deploy manifest treats `method` as
            // optional (see cloud.rs) - but it carries no outbox schema / client
            // routes to apply, so skip schema processing. Mirrors
            // AppConfig::validate, which does not require a method.
            if app.method.is_none() {
                crate::ui::detail(format!(
                    "skipping backend '{}' schema (no method; direct-HTTP/deploy-only backend)",
                    name
                ));
                continue;
            }
            self.process_backend(name, app, base_dir)?;
        }

        // The `.surql` file is the source of truth for a bucket's backend — we
        // never rewrite it here. We only warn when the authored backend is
        // inconsistent with the storage setting so the mismatch is visible.
        let storage_enabled = config.bucket_storage_gb().is_some();
        for path_str in &config.buckets {
            let bucket_path = base_dir.join(path_str);
            let bucket_content = fs::read_to_string(&bucket_path)
                .context(format!("Failed to read bucket file: {:?}", bucket_path))?;
            for (bucket_name, backend) in detect_bucket_backends(&bucket_content) {
                if storage_enabled && backend.eq_ignore_ascii_case("memory") {
                    crate::ui::warn(format!(
                        "Bucket '{}' uses the memory backend; files won't persist. \
                         Run `spky bucket backend {} persistent` to store them on disk.",
                        bucket_name, bucket_name
                    ));
                } else if !storage_enabled && backend.starts_with("file:") {
                    crate::ui::warn(format!(
                        "Bucket '{}' uses a file backend but deployment.storage.sizeGB \
                         is unset; no volume is mounted and files land on ephemeral storage.",
                        bucket_name
                    ));
                }
            }
            self.bucket_schema.push('\n');
            self.bucket_schema.push_str(&bucket_content);
            crate::ui::detail(format!("loaded bucket schema from {:?}", bucket_path));
        }

        Ok(())
    }

    fn process_backend(
        &mut self,
        backend_name: &str,
        app_config: &AppConfig,
        base_dir: &Path,
    ) -> Result<()> {
        crate::ui::detail(format!("processing backend config: {}", backend_name));

        let method = app_config.method.as_ref().context(format!(
            "Backend '{}' is missing 'method' field",
            backend_name
        ))?;
        let spec = app_config.spec.as_ref().context(format!(
            "Backend '{}' is missing 'spec' field",
            backend_name
        ))?;

        // 1. Append Schema - resolve path relative to sp00ky.yml
        let schema_path = base_dir.join(&method.schema);
        let schema_content = fs::read_to_string(&schema_path)
            .context(format!("Failed to read backend schema: {:?}", schema_path))?;

        self.schema_appends.push('\n');
        self.schema_appends
            .push_str(&format!("-- Backend Schema: {}\n", backend_name));
        self.schema_appends.push_str(&schema_content);
        crate::ui::detail(format!("appended schema from {:?}", schema_path));

        // 2. Parse OpenAPI Spec - resolve path relative to sp00ky.yml
        let spec_path = base_dir.join(spec);
        let spec_content = fs::read_to_string(&spec_path)
            .context(format!("Failed to read openapi spec: {:?}", spec_path))?;

        let openapi: OpenAPI =
            serde_yaml::from_str(&spec_content).context("Failed to parse openapi spec")?;

        let mut backend_def = BackendDefinition {
            routes: BTreeMap::new(),
            outbox_table: method.table.clone(),
        };

        for (path, item) in openapi.paths {
            // We only care about paths that have item content
            let item = match item.as_item() {
                Some(i) => i,
                None => continue,
            };

            // Check for POST method for arguments (as mostly implied by the context of "args")
            // Or should we support all methods? The prompt example shows:
            // backends: { "api": { "/pathA": { args: [...] } } }
            // Let's assume we want to capture arguments from the request body or parameters.

            // For now, let's look at POST operations as they are most likely for RPC-style calls
            if let Some(op) = &item.post {
                let mut args = BTreeMap::new();

                // Extract arguments from Request Body (application/json)
                if let Some(req_body) = &op.request_body {
                    if let Some(req_body_item) = req_body.as_item() {
                        if let Some(content) = req_body_item.content.get("application/json") {
                            if let Some(schema) = &content.schema {
                                if let Some(schema_item) = schema.as_item() {
                                    if let openapiv3::SchemaKind::Type(openapiv3::Type::Object(
                                        obj_type,
                                    )) = &schema_item.schema_kind
                                    {
                                        for (prop_name, prop_schema_ref) in &obj_type.properties {
                                            if let Some(prop_schema_box) = prop_schema_ref.as_item()
                                            {
                                                let prop_schema = &**prop_schema_box;
                                                let arg_type = match &prop_schema.schema_kind {
                                                    openapiv3::SchemaKind::Type(
                                                        openapiv3::Type::String(_),
                                                    ) => "string",
                                                    openapiv3::SchemaKind::Type(
                                                        openapiv3::Type::Number(_),
                                                    ) => "number",
                                                    openapiv3::SchemaKind::Type(
                                                        openapiv3::Type::Integer(_),
                                                    ) => "number",
                                                    openapiv3::SchemaKind::Type(
                                                        openapiv3::Type::Boolean(_),
                                                    ) => "boolean",
                                                    _ => "any", // Fallback
                                                };

                                                let required =
                                                    obj_type.required.contains(prop_name);

                                                args.insert(
                                                    prop_name.clone(),
                                                    BackendRouteArg {
                                                        arg_type: arg_type.to_string(),
                                                        required,
                                                    },
                                                );
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }

                // Also check parameters (query/path) ??
                // The requirements example showed `args: [...]` which usually implies input parameters.
                // Given the context of "spookify" example with "id", it was in the body.
                // I will stick to body properties for now as it matches the sp00ky RPC style.

                backend_def
                    .routes
                    .insert(path.clone(), BackendRoute { args });
            }
        }

        self.backend_definitions
            .insert(backend_name.to_string(), backend_def);
        crate::ui::detail(format!("parsed OpenAPI spec from {:?}", spec_path));

        Ok(())
    }
}

#[cfg(test)]
mod version_tests {
    use super::*;

    fn parse(yaml: &str) -> Sp00kyConfig {
        serde_yaml::from_str(yaml).expect("yaml parse")
    }

    fn try_parse(yaml: &str) -> std::result::Result<Sp00kyConfig, serde_yaml::Error> {
        serde_yaml::from_str(yaml)
    }

    /// Helper for tests that only care about the tag form. Panics if either
    /// side resolved to a `LocalBinary` — those tests must call
    /// `resolve_full` and pattern-match on the `RuntimeSource`.
    fn resolve(yaml: &str, env: DeployEnv) -> (String, String) {
        let r = resolve_full(yaml, env);
        let unwrap_tag = |rs: RuntimeSource| match rs {
            RuntimeSource::Image(t) => t,
            RuntimeSource::LocalBinary(p) => panic!("expected Image, got LocalBinary({:?})", p),
        };
        (unwrap_tag(r.ssp), unwrap_tag(r.scheduler))
    }

    fn resolve_full(yaml: &str, env: DeployEnv) -> ResolvedVersions {
        let cfg = parse(yaml);
        ResolvedVersions::from_config(&cfg, env)
    }

    /// Guards against `Sp00kyConfig::default()` diverging from
    /// `serde_yaml::from_str("{}")`. They must produce identical
    /// serializations so future fields can't accidentally make the
    /// hand-written `default_config()` and the YAML defaults disagree.
    /// Compared via serialized JSON because `Sp00kyConfig` doesn't
    /// derive `PartialEq` (and adding it would cascade across every
    /// child config type).
    #[test]
    fn default_matches_empty_yaml() {
        let from_default = Sp00kyConfig::default();
        let from_yaml: Sp00kyConfig = serde_yaml::from_str("{}").expect("empty yaml parses");

        let default_json = serde_json::to_string(&from_default).expect("serialize default");
        let yaml_json = serde_json::to_string(&from_yaml).expect("serialize from-yaml");
        assert_eq!(
            default_json, yaml_json,
            "Sp00kyConfig::default() must match serde-deserialize of empty YAML"
        );
    }

    #[test]
    fn flat_string_applies_to_both_envs() {
        assert_eq!(
            resolve("version: canary\n", DeployEnv::Dev),
            ("canary".into(), "canary".into())
        );
        assert_eq!(
            resolve("version: canary\n", DeployEnv::Cloud),
            ("canary".into(), "canary".into())
        );
    }

    #[test]
    fn per_service_object_applies_to_both_envs() {
        let yaml = "version: { ssp: a, scheduler: b }\n";
        assert_eq!(resolve(yaml, DeployEnv::Dev), ("a".into(), "b".into()));
        assert_eq!(resolve(yaml, DeployEnv::Cloud), ("a".into(), "b".into()));
    }

    #[test]
    fn per_service_partial_fills_with_defaults() {
        let yaml = "version: { ssp: a }\n";
        assert_eq!(resolve(yaml, DeployEnv::Dev), ("a".into(), "canary".into()));
    }

    #[test]
    fn per_env_strings() {
        let yaml = "version:\n  dev: dev\n  cloud: canary\n";
        assert_eq!(resolve(yaml, DeployEnv::Dev), ("dev".into(), "dev".into()));
        assert_eq!(
            resolve(yaml, DeployEnv::Cloud),
            ("canary".into(), "canary".into())
        );
    }

    #[test]
    fn per_env_with_per_service() {
        let yaml = "version:\n  dev: { ssp: x, scheduler: y }\n  cloud: canary\n";
        assert_eq!(resolve(yaml, DeployEnv::Dev), ("x".into(), "y".into()));
        assert_eq!(
            resolve(yaml, DeployEnv::Cloud),
            ("canary".into(), "canary".into())
        );
    }

    #[test]
    fn per_env_missing_dev_falls_back_to_defaults() {
        let yaml = "version:\n  cloud: canary\n";
        assert_eq!(
            resolve(yaml, DeployEnv::Dev),
            ("canary".into(), "canary".into())
        );
        assert_eq!(
            resolve(yaml, DeployEnv::Cloud),
            ("canary".into(), "canary".into())
        );
    }

    #[test]
    fn no_version_uses_defaults() {
        assert_eq!(
            resolve("apps: {}\n", DeployEnv::Dev),
            ("canary".into(), "canary".into())
        );
    }

    #[test]
    fn unknown_key_in_per_service_errors() {
        // `bogus` isn't a valid VersionSpec::Individual field; deny_unknown_fields rejects it.
        let yaml = "version: { ssp: x, bogus: y }\n";
        assert!(try_parse(yaml).is_err());
    }

    #[test]
    fn mixed_keys_error() {
        // Not a subset of {dev, cloud}, falls through to VersionSpec::Individual which rejects `dev`.
        let yaml = "version: { ssp: x, dev: y }\n";
        assert!(try_parse(yaml).is_err());
    }

    #[test]
    fn round_trip_serialize_flat_string() {
        let cfg = parse("version: canary\n");
        let out = serde_yaml::to_string(&cfg.version).unwrap();
        assert_eq!(out.trim(), "canary");
    }

    #[test]
    fn round_trip_serialize_per_env() {
        let cfg = parse("version:\n  dev: dev\n  cloud: canary\n");
        let out = serde_yaml::to_string(&cfg.version).unwrap();
        // Round-trips back to a parseable PerEnvironment structure.
        let reparsed: VersionConfig = serde_yaml::from_str(&out).unwrap();
        match reparsed {
            VersionConfig::PerEnvironment { dev, cloud } => {
                assert!(matches!(dev, Some(VersionSpec::All(ref s)) if s == "dev"));
                assert!(matches!(cloud, Some(VersionSpec::All(ref s)) if s == "canary"));
            }
            _ => panic!("expected PerEnvironment, got {:?}", reparsed),
        }
    }

    #[test]
    fn per_service_path_resolves_to_local_binary() {
        let yaml = "version:\n  ssp: { path: ./target/debug/ssp-server }\n  scheduler: { path: /abs/path/scheduler }\n";
        let cfg = parse(yaml);
        let r = ResolvedVersions::from_config_with_dir(&cfg, DeployEnv::Dev, Path::new("/proj"));

        match &r.ssp {
            RuntimeSource::LocalBinary(p) => {
                // Relative path is anchored at project_dir.
                assert_eq!(p, &PathBuf::from("/proj/./target/debug/ssp-server"));
            }
            other => panic!("expected LocalBinary, got {:?}", other),
        }
        match &r.scheduler {
            RuntimeSource::LocalBinary(p) => {
                // Absolute path is preserved verbatim.
                assert_eq!(p, &PathBuf::from("/abs/path/scheduler"));
            }
            other => panic!("expected LocalBinary, got {:?}", other),
        }
    }

    #[test]
    fn per_service_mixed_tag_and_path() {
        let yaml = "version:\n  ssp: { path: ./target/debug/ssp-server }\n  scheduler: canary\n";
        let r = resolve_full(yaml, DeployEnv::Dev);
        assert!(matches!(r.ssp, RuntimeSource::LocalBinary(_)));
        match &r.scheduler {
            RuntimeSource::Image(t) => assert_eq!(t, "canary"),
            other => panic!("expected Image(canary), got {:?}", other),
        }
    }

    #[test]
    fn per_env_with_path_in_dev() {
        let yaml = "version:\n  dev: { ssp: { path: ./target/debug/ssp-server }, scheduler: { path: ./target/debug/scheduler } }\n  cloud: canary\n";
        let cfg = parse(yaml);
        let dev = ResolvedVersions::from_config_with_dir(&cfg, DeployEnv::Dev, Path::new("/proj"));
        let cloud =
            ResolvedVersions::from_config_with_dir(&cfg, DeployEnv::Cloud, Path::new("/proj"));

        assert!(matches!(dev.ssp, RuntimeSource::LocalBinary(_)));
        assert!(matches!(dev.scheduler, RuntimeSource::LocalBinary(_)));
        assert!(matches!(cloud.ssp, RuntimeSource::Image(ref t) if t == "canary"));
        assert!(matches!(cloud.scheduler, RuntimeSource::Image(ref t) if t == "canary"));
    }

    #[test]
    fn helpers_split_image_and_path() {
        let yaml = "version:\n  ssp: { path: ./bin/ssp }\n  scheduler: canary\n";
        let cfg = parse(yaml);
        let r = ResolvedVersions::from_config_with_dir(&cfg, DeployEnv::Dev, Path::new("/proj"));

        assert!(r.ssp_image().is_none());
        assert_eq!(r.ssp_local_binary().unwrap(), Path::new("/proj/./bin/ssp"));

        assert_eq!(
            r.scheduler_image().as_deref(),
            Some("mono424/spooky-scheduler:canary")
        );
        assert!(r.scheduler_local_binary().is_none());
    }
}

#[cfg(test)]
mod process_tests {
    use super::*;
    use tempfile::TempDir;

    fn write_config(yaml: &str) -> (TempDir, std::path::PathBuf) {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("sp00ky.yml");
        std::fs::write(&path, yaml).unwrap();
        (dir, path)
    }

    /// A `type: backend` + `scope: devOnly` app (the native LiveKit SFU pattern:
    /// only a `dev:` command, no spec/method) must be SKIPPED by process(), not
    /// rejected. Regression for "Backend 'livekit' is missing 'method' field"
    /// that broke `spky dev`/migrate/generate once LiveKit moved off `type: docker`.
    #[test]
    fn devonly_backend_is_skipped_by_process() {
        let yaml = "\
slug: t
surrealdb:
  namespace: main
  database: main
apps:
  livekit:
    type: backend
    scope: devOnly
    dev: \"livekit-server --dev --bind 0.0.0.0\"
";
        let (_dir, path) = write_config(yaml);
        let mut p = BackendProcessor::new();
        let r = p.process(&path);
        assert!(
            r.is_ok(),
            "devOnly backend should be skipped, got: {:?}",
            r.err()
        );
        // Nothing should have been appended for the skipped app.
        assert!(
            !p.schema_appends.contains("livekit"),
            "skipped devOnly backend must not contribute schema"
        );
    }

    /// A `type: backend` with no `method` is a direct-HTTP / deploy-only service
    /// (e.g. the stream-relay: clients fetch it directly, it is not outbox-driven).
    /// process() must SKIP it, not error, since it carries no outbox schema to
    /// apply; the deploy path still ships it. Previously this was asserted to
    /// error; direct-HTTP backends are now supported.
    #[test]
    fn backend_without_method_is_skipped_as_deploy_only() {
        let yaml = "\
slug: t
surrealdb:
  namespace: main
  database: main
apps:
  relaylike:
    type: backend
    spec: ./relay-openapi.yml
    deploy:
      dockerfile: ./Dockerfile
      context: .
      port: 3670
";
        let (_dir, path) = write_config(yaml);
        let mut p = BackendProcessor::new();
        let r = p.process(&path);
        assert!(
            r.is_ok(),
            "method-less (direct-HTTP) backend should be skipped, got: {:?}",
            r.err()
        );
        assert!(
            !p.schema_appends.contains("relaylike"),
            "skipped direct-HTTP backend must not contribute schema"
        );
    }
}

#[cfg(test)]
mod include_tests {
    use super::*;
    use tempfile::TempDir;

    /// Serialize an app back to YAML value so two AppConfigs can be compared
    /// structurally (AppConfig doesn't derive PartialEq).
    fn app_value(config: &Sp00kyConfig, name: &str) -> serde_yaml::Value {
        serde_yaml::to_value(config.apps.get(name).expect("app present")).unwrap()
    }

    /// A `{ path }` reference must resolve to the exact same AppConfig as the
    /// equivalent inline block, with every relative path rebased to the root.
    #[test]
    fn path_include_matches_inline_and_rebases() {
        let dir = TempDir::new().unwrap();
        std::fs::create_dir(dir.path().join("api")).unwrap();
        std::fs::write(
            dir.path().join("api/sp00ky.app.yml"),
            "\
type: backend
spec: ./openapi.yml
method:
  type: outbox
  table: job
  schema: ./src/outbox/api.surql
deploy:
  dockerfile: ./Dockerfile
  context: ../
  port: 3660
env:
  dev: ./.env.local
",
        )
        .unwrap();

        let split = "\
slug: t
apps:
  api:
    path: ./api
";
        let inline = "\
slug: t
apps:
  api:
    type: backend
    spec: api/openapi.yml
    method:
      type: outbox
      table: job
      schema: api/src/outbox/api.surql
    deploy:
      dockerfile: api/Dockerfile
      context: api/../
      port: 3660
    env:
      dev: api/.env.local
";
        let split_cfg = parse_config_with_includes(split, dir.path()).unwrap();
        let inline_cfg = parse_config_with_includes(inline, dir.path()).unwrap();

        assert_eq!(
            app_value(&split_cfg, "api"),
            app_value(&inline_cfg, "api"),
            "split config must resolve identically to the inline form"
        );

        // Spot-check that every relative path was rebased onto the service dir.
        let api = split_cfg.apps.get("api").unwrap();
        assert_eq!(api.spec.as_deref(), Some("api/openapi.yml"));
        assert_eq!(
            api.method.as_ref().unwrap().schema,
            "api/src/outbox/api.surql"
        );
        let deploy = api.deploy.as_ref().unwrap();
        assert_eq!(deploy.dockerfile.as_deref(), Some("api/Dockerfile"));
    }

    /// The docker dev method's `file` is workdir-relative and must NOT be
    /// rebased, while `workdir` (manifest-relative) must be. Deploy dockerfile
    /// is manifest-relative and IS rebased. Regression for a double-prefix bug.
    #[test]
    fn docker_dev_file_not_rebased_but_workdir_is() {
        let dir = TempDir::new().unwrap();
        std::fs::create_dir_all(dir.path().join("svc")).unwrap();
        std::fs::write(
            dir.path().join("svc/sp00ky.app.yml"),
            "\
type: backend
scope: devOnly
dev:
  type: docker
  file: svc/Dockerfile
  workdir: ../..
deploy:
  dockerfile: ./Dockerfile
  context: ../..
  port: 3663
",
        )
        .unwrap();

        let cfg = parse_config_with_includes("apps:\n  svc:\n    path: ./svc\n", dir.path())
            .unwrap();
        let app = cfg.apps.get("svc").unwrap();
        match app.dev.as_ref().unwrap() {
            BackendDevConfig::Typed(BackendDevTypedConfig::Docker { file, workdir, .. }) => {
                // `file` untouched (still relative to workdir).
                assert_eq!(file, "svc/Dockerfile");
                // `workdir` ../.. from service dir `svc` → repo root marker.
                assert_eq!(workdir.as_deref(), Some("svc/../.."));
            }
            other => panic!("expected docker dev, got {other:?}"),
        }
        // Deploy dockerfile rebased onto the service dir.
        assert_eq!(
            app.deploy.as_ref().unwrap().dockerfile.as_deref(),
            Some("svc/Dockerfile")
        );
    }

    /// A field set alongside `path` in the root file overrides the sub-file.
    #[test]
    fn root_entry_overrides_sub_file() {
        let dir = TempDir::new().unwrap();
        std::fs::create_dir(dir.path().join("svc")).unwrap();
        std::fs::write(
            dir.path().join("svc/sp00ky.app.yml"),
            "type: backend\nscope: all\nspec: ./openapi.yml\nmethod:\n  type: outbox\n  table: job\n  schema: ./s.surql\n",
        )
        .unwrap();

        let yaml = "\
apps:
  svc:
    path: ./svc
    scope: cloudOnly
";
        let cfg = parse_config_with_includes(yaml, dir.path()).unwrap();
        assert_eq!(cfg.apps.get("svc").unwrap().scope, AppScope::CloudOnly);
    }

    /// A missing sub-file surfaces a clear error rather than a silent default.
    #[test]
    fn missing_include_errors() {
        let dir = TempDir::new().unwrap();
        let yaml = "apps:\n  api:\n    path: ./nope\n";
        let err = parse_config_with_includes(yaml, dir.path()).unwrap_err();
        assert!(
            err.to_string().contains("api") && err.to_string().contains("nope"),
            "error should name the app and path: {err}"
        );
    }

    /// A config with no `path:` entries parses exactly as before (regression).
    #[test]
    fn no_path_entries_is_unchanged() {
        let dir = TempDir::new().unwrap();
        let yaml = "\
slug: t
apps:
  web:
    type: frontend
  api:
    type: backend
    spec: ./api/openapi.yml
    method:
      type: outbox
      table: job
      schema: ./schema.surql
";
        let via_includes = parse_config_with_includes(yaml, dir.path()).unwrap();
        let direct: Sp00kyConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(
            serde_yaml::to_value(&via_includes).unwrap(),
            serde_yaml::to_value(&direct).unwrap()
        );
    }
}

#[cfg(test)]
mod storage_tests {
    use super::*;

    #[test]
    fn backend_path_is_per_bucket_under_volume() {
        assert_eq!(bucket_backend_path("avatars"), "file:/buckets/avatars");
    }

    #[test]
    fn detect_backends_parses_name_and_value() {
        let surql = r#"
DEFINE BUCKET IF NOT EXISTS avatars BACKEND "memory"
  PERMISSIONS WHERE $action NOT IN ['put'];
DEFINE BUCKET docs BACKEND "file:/buckets/docs";
"#;
        let found = detect_bucket_backends(surql);
        assert_eq!(
            found,
            vec![
                ("avatars".to_string(), "memory".to_string()),
                ("docs".to_string(), "file:/buckets/docs".to_string()),
            ]
        );
    }

    #[test]
    fn storage_gb_enabled_only_when_positive() {
        let enabled: Sp00kyConfig = serde_yaml::from_str(
            "deployment:\n  storage:\n    sizeGB: 10\n",
        )
        .unwrap();
        assert_eq!(enabled.bucket_storage_gb(), Some(10));

        let zero: Sp00kyConfig =
            serde_yaml::from_str("deployment:\n  storage:\n    sizeGB: 0\n").unwrap();
        assert_eq!(zero.bucket_storage_gb(), None);

        let unset: Sp00kyConfig =
            serde_yaml::from_str("deployment:\n  sspCount: 2\n").unwrap();
        assert_eq!(unset.bucket_storage_gb(), None);

        let no_deploy: Sp00kyConfig = serde_yaml::from_str("slug: t\n").unwrap();
        assert_eq!(no_deploy.bucket_storage_gb(), None);
    }
}

#[cfg(test)]
mod env_source_tests {
    use super::*;

    /// The exact shape whitepawn's relay uses: a source LIST whose first item
    /// is a { dev, cloud } split and whose second scopes a vault whitelist to
    /// cloud. Before EnvSource::PerEnvironment existed, both items degraded to
    /// Map and a CLI deploy shipped literal `dev=...` / `cloud=vault:...` env
    /// entries (no real vars, no secrets) — the 2026-07-02 staging outage.
    const SCOPED_LIST_YAML: &str = r#"
- dev:
    PORT: "3670"
    RELAY_PUBLIC_URL: "ws://localhost:3670"
  cloud:
    PORT: "3670"
    RELAY_PUBLIC_URL: "wss://relay.example.com"
- cloud:
    vault:
      - SPKY_JWT_PUBLIC_KEY
      - LIVEKIT_SECRET
"#;

    #[test]
    fn scoped_list_items_parse_per_environment() {
        let cfg: EnvConfig = serde_yaml::from_str(SCOPED_LIST_YAML).expect("parse");
        let EnvConfig::List(sources) = cfg else {
            panic!("expected List");
        };
        assert_eq!(sources.len(), 2);
        match &sources[0] {
            EnvSource::PerEnvironment { dev, cloud } => {
                assert!(dev.is_some() && cloud.is_some());
            }
            other => panic!("first item should be PerEnvironment, got {other:?}"),
        }
        match &sources[1] {
            EnvSource::PerEnvironment { dev, cloud } => {
                assert!(dev.is_none());
                match cloud.as_deref() {
                    Some(EnvEntry::Source(EnvSource::Vault(keys))) => {
                        assert_eq!(keys, &["SPKY_JWT_PUBLIC_KEY", "LIVEKIT_SECRET"]);
                    }
                    other => panic!("cloud side should be a vault whitelist, got {other:?}"),
                }
            }
            other => panic!("second item should be PerEnvironment, got {other:?}"),
        }
    }

    #[test]
    fn plain_inline_map_is_untouched() {
        let cfg: EnvConfig =
            serde_yaml::from_str("- PORT: \"1234\"\n  MODE: \"x\"\n").expect("parse");
        let EnvConfig::List(sources) = cfg else {
            panic!("expected List");
        };
        match &sources[0] {
            EnvSource::Map(m) => assert_eq!(m.len(), 2),
            other => panic!("expected Map, got {other:?}"),
        }
    }

    #[test]
    fn deploy_config_parses_grpc_port() {
        let d: AppDeployConfig = serde_yaml::from_str(
            "dockerfile: Dockerfile\nport: 3670\ngrpc_port: 3671\nexpose: true\n",
        )
        .expect("parse");
        assert_eq!(d.grpc_port, Some(3671));
        // Absent stays None (and is skipped on serialize).
        let d2: AppDeployConfig =
            serde_yaml::from_str("dockerfile: Dockerfile\nport: 8080\n").expect("parse");
        assert_eq!(d2.grpc_port, None);
    }
}

#[cfg(test)]
mod surrealdb_tests {
    use super::*;

    fn cfg(yaml: &str) -> Sp00kyConfig {
        serde_yaml::from_str(yaml).expect("yaml parse")
    }

    #[test]
    fn cloud_with_password_is_rejected() {
        let c = cfg("surrealdb:\n  hosting: cloud\n  password: secret\n");
        let err = c.validate().expect_err("cloud + password must error");
        assert!(err.to_string().contains("only valid with hosting: external"));
    }

    #[test]
    fn cloud_with_endpoint_is_rejected() {
        // hosting omitted defaults to cloud.
        let c = cfg("surrealdb:\n  endpoint: https://db.example.com\n");
        assert!(c.validate().is_err());
    }

    #[test]
    fn external_without_endpoint_is_rejected() {
        let c = cfg("surrealdb:\n  hosting: external\n");
        let err = c.validate().expect_err("external needs endpoint");
        assert!(err.to_string().contains("no endpoint"));
    }

    #[test]
    fn external_with_literal_creds_validates() {
        let c = cfg(
            "surrealdb:\n  hosting: external\n  endpoint: https://db.example.com\n  username: root\n  password: hunter2\n",
        );
        c.validate().expect("external + literal creds should validate");
        let r = c.resolved_surrealdb();
        assert_eq!(r.hosting, HostingMode::External);
        assert_eq!(r.username.literal_or_default(), "root");
        assert_eq!(r.password.literal_or_default(), "hunter2");
        assert!(r.username.vault_key().is_none());
    }

    #[test]
    fn external_with_vault_password_parses_as_reference() {
        let c = cfg(
            "surrealdb:\n  hosting: external\n  endpoint: { vault: DB_ENDPOINT }\n  password: { vault: DB_PASSWORD }\n",
        );
        c.validate().expect("external + vault creds should validate");
        let r = c.resolved_surrealdb();
        assert_eq!(r.password.vault_key(), Some("DB_PASSWORD"));
        assert_eq!(
            r.endpoint.as_ref().and_then(|e| e.vault_key()),
            Some("DB_ENDPOINT")
        );
        // Literal accessors are empty for an unresolved vault ref.
        assert_eq!(r.password.literal_or_default(), "");
    }

    #[test]
    fn defaults_are_root_literals() {
        let c = cfg("surrealdb:\n  namespace: main\n");
        let r = c.resolved_surrealdb();
        assert_eq!(r.username.literal_or_default(), "root");
        assert_eq!(r.password.literal_or_default(), "root");
        assert_eq!(r.hosting, HostingMode::Cloud);
    }
}

#[cfg(test)]
mod retention_config_tests {
    use super::*;

    fn config(yaml: &str) -> Sp00kyConfig {
        serde_yaml::from_str(yaml).expect("parses")
    }

    /// With no `retention:` block a project gets the built-in windows. The
    /// run-failure window matches what the engine used to hardcode, so upgrading can
    /// never silently drop failure history a project already had.
    #[test]
    fn an_absent_block_resolves_to_the_defaults() {
        let r = config("slug: demo\n").resolved_retention().expect("resolves");
        assert_eq!(r, RetentionConfig::DEFAULTS);
        assert_eq!(r.run_failed_secs, 30 * 24 * 60 * 60);
    }

    /// Successes are kept far more briefly than failures — the asymmetry is the
    /// point, so it is pinned rather than left to a comment.
    #[test]
    fn the_defaults_keep_failures_far_longer_than_successes() {
        let d = RetentionConfig::DEFAULTS;
        assert!(d.failed_secs > d.success_secs * 10);
        assert!(d.run_failed_secs > d.run_success_secs * 10);
    }

    #[test]
    fn durations_resolve_to_seconds_and_partial_blocks_keep_the_rest() {
        let r = config("retention:\n  success: 30m\n  runFailed: 7d\n")
            .resolved_retention()
            .expect("resolves");
        assert_eq!(r.success_secs, 1800);
        assert_eq!(r.run_failed_secs, 7 * 24 * 60 * 60);
        // Untouched keys keep their defaults.
        assert_eq!(r.failed_secs, RetentionConfig::DEFAULTS.failed_secs);
        assert_eq!(r.run_success_secs, RetentionConfig::DEFAULTS.run_success_secs);
    }

    /// An unparseable or zero duration must fail the deploy. Resolving it to 0 would
    /// mean "prune everything immediately", which is the worst possible fallback.
    #[test]
    fn a_bad_duration_fails_rather_than_defaulting_to_zero() {
        assert!(config("retention:\n  success: soon\n").resolved_retention().is_err());
        assert!(config("retention:\n  success: 0s\n").resolved_retention().is_err());
    }

    /// The cap is OFF by default. A cap that deletes inside someone's retention
    /// window without them asking is worse than no cap at all.
    #[test]
    fn the_row_cap_is_disabled_unless_asked_for() {
        assert_eq!(RetentionConfig::DEFAULTS.max_rows, 0);
        let r = config("retention:\n  success: 1h\n").resolved_retention().expect("resolves");
        assert_eq!(r.max_rows, 0, "setting an unrelated window must not enable the cap");
    }

    #[test]
    fn the_row_cap_is_read_from_config_and_never_negative() {
        let r = config("retention:\n  maxRows: 50000\n").resolved_retention().expect("resolves");
        assert_eq!(r.max_rows, 50_000);
        // A negative cap would underflow into "keep nothing" downstream.
        let r = config("retention:\n  maxRows: -5\n").resolved_retention().expect("resolves");
        assert_eq!(r.max_rows, 0);
    }

    /// A typo must be rejected, not ignored — a silently dropped retention setting is
    /// discovered by running out of disk.
    #[test]
    fn an_unknown_retention_key_is_rejected() {
        let err = serde_yaml::from_str::<Sp00kyConfig>("retention:\n  sucess: 1h\n")
            .expect_err("unknown key must not parse");
        assert!(err.to_string().contains("sucess"), "got: {err}");
    }
}

#[cfg(test)]
mod infra_env_tests {
    use super::*;

    /// `deployment.env` must survive the round trip from sp00ky.yml into the
    /// deploy payload. It is the only way to set a runtime knob on the infra
    /// containers, whose environment is otherwise a closed list assembled by
    /// the control plane.
    #[test]
    fn deployment_env_parses_per_role() {
        let cfg: Sp00kyConfig = serde_yaml::from_str(
            "name: demo\ndeployment:\n  env:\n    ssp:\n      SPKY_SSP_MERGE_VIEWS: \"true\"\n",
        )
        .expect("yaml parse");

        let env = cfg.deployment.as_ref().and_then(|d| d.env.as_ref()).expect("env present");
        assert_eq!(env["ssp"]["SPKY_SSP_MERGE_VIEWS"], "true");
    }

    /// Absent means "keep whatever the control plane already has", which is
    /// why the field is an Option and the payload omits it rather than
    /// sending an empty map that would clear the previous setting.
    #[test]
    fn deployment_env_is_absent_when_unset() {
        let cfg: Sp00kyConfig =
            serde_yaml::from_str("name: demo\ndeployment:\n  sspCount: 2\n").expect("yaml parse");
        assert!(cfg.deployment.as_ref().and_then(|d| d.env.as_ref()).is_none());
    }
}
