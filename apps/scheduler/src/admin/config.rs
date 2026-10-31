//! Admin-plane configuration, read from the environment.
//!
//! Everything here is opt-out rather than opt-in: the dashboard is part of what
//! a scheduler is, so it comes up by default. What is *not* on by default is
//! the break-glass password — an unset `SPKY_ADMIN_PASSWORD` disables that
//! login path entirely rather than falling back to some default secret.

/// Default port for the admin listener.
///
/// Deliberately NOT the ingest port. `/ingest`, `/proxy/query` (arbitrary
/// SurrealQL against the replica), `/ssp/*` and `PUT /backends` are all
/// unauthenticated by design, on the assumption that 9667 is reachable only
/// from inside the deployment's private network. The dashboard is the first
/// thing here meant to be reached by a human browser, so it gets its own
/// listener that can be published without publishing any of that.
pub const DEFAULT_ADMIN_PORT: u16 = 9668;

/// Where the built dashboard lives in the scheduler image.
pub const DEFAULT_ADMIN_DIR: &str = "/usr/share/spooky/dashboard";

#[derive(Debug, Clone)]
pub struct AdminConfig {
    pub enabled: bool,
    pub host: String,
    pub port: u16,
    /// Directory holding the built dashboard (`index.html` + assets).
    pub dir: std::path::PathBuf,
    /// Break-glass password. `None` disables password-only login.
    pub password: Option<String>,
    /// Pins the SurrealDB record-access method used for admin signin. When
    /// `None`, the accesses are discovered from `INFO FOR DB`.
    pub access: Option<String>,
    /// How long a signed-in session lasts.
    pub session_ttl: std::time::Duration,
    /// How often the presence sampler re-reads `_00_query`. One sampler feeds
    /// every dashboard, so this is the total cost of the Overview charts and
    /// the Views tab no matter how many people have them open.
    pub presence_interval: std::time::Duration,
    /// A registered view whose materialization p99 reaches this is counted as
    /// slow. Only a default — `GET /views?slow_ms=` overrides it per request.
    pub presence_slow_ms: f64,
    /// A registered view holding at least this many rows is counted as large
    /// and flagged on the Views tab. Every row of an unwindowed live view is a
    /// `_00_list_ref` edge republished on each cold registration, and a few
    /// thousand of those in one transaction is what stalled a tenant's
    /// SurrealDB 3.0.5. Env `SPKY_ADMIN_LARGE_VIEW_ROWS`, default 1000.
    pub presence_large_view_rows: u64,
    /// A registered view whose `updateCount` climbs at least this many times a
    /// second between two presence samples is counted as hot: flagged on the
    /// Views tab and the Overview, and opens a `hot_views` incident. One view
    /// per user changing ~125 times a second is what a bulk PGN import into an
    /// open game list produced, and it filled the SSP's publication queue
    /// (2026-10-08). Env `SPKY_ADMIN_HOT_VIEW_UPDATES_PER_SEC`, default 5.
    pub presence_hot_view_rate: f64,
    /// Ceiling on rows any one presence/views query may pull back, so a tenant
    /// with a runaway number of registrations cannot make the sampler the
    /// expensive thing on the box.
    pub presence_max_rows: usize,
    /// How often the job sampler re-reads the outbox tables while nobody has
    /// the Jobs page open. Every job aggregate is a scan of a user table, so
    /// this is the standing cost of the Jobs tile and the sidebar count on an
    /// idle cluster. Zero switches the sampler off entirely.
    pub job_interval: std::time::Duration,
    /// The cadence while at least one dashboard holds `/jobs/stream` open.
    /// Fast enough to watch a backlog drain, and paid only while someone is
    /// actually watching it.
    pub job_live_interval: std::time::Duration,
    /// Browser origins allowed to call the API and open `/v2/live`, from the
    /// comma-separated `SPKY_ADMIN_ALLOWED_ORIGINS`. Empty (unset) or `*`
    /// allows any origin, which is safe because the bearer token, never a
    /// cookie, is what authenticates; the list is defence in depth. This
    /// host's own origin is always allowed for the socket.
    pub allowed_origins: Vec<String>,
    /// Whether a Sp00ky Cloud console user may sign in with a grant
    /// (`SPKY_ADMIN_CLOUD_LOGIN`, default on). Only takes effect on a
    /// cloud-linked scheduler: the grant's audience is the linked project.
    pub cloud_login: bool,
    /// The control plane's grant keys, inline (`SPKY_ADMIN_CLOUD_JWKS`, a
    /// JWKS document). Without it, or for a key it lacks, the keys are
    /// fetched from `{SPKY_CLOUD_API_URL}/.well-known/jwks.json`.
    pub cloud_jwks: Option<String>,
    /// Expected grant issuer (`SPKY_ADMIN_CLOUD_ISSUER`); defaults to
    /// `SPKY_CLOUD_API_URL`.
    pub cloud_issuer: Option<String>,
    /// How long a session opened with a grant lasts
    /// (`SPKY_ADMIN_CLOUD_SESSION_TTL_SECS`, default 900). Short on purpose:
    /// the console renews it from a fresh grant, so a member removed from a
    /// workspace loses access within this window.
    pub cloud_session_ttl: std::time::Duration,
}

impl AdminConfig {
    pub fn from_env() -> Self {
        let enabled = !matches!(
            std::env::var("SPKY_ADMIN_ENABLED").as_deref(),
            Ok("0") | Ok("false") | Ok("off")
        );

        let port = std::env::var("SPKY_ADMIN_PORT")
            .ok()
            .and_then(|s| s.parse::<u16>().ok())
            .filter(|p| *p > 0)
            .unwrap_or(DEFAULT_ADMIN_PORT);

        let session_ttl_secs = std::env::var("SPKY_ADMIN_SESSION_TTL_SECS")
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .filter(|n| *n > 0)
            .unwrap_or(8 * 60 * 60);

        // Ticks under a second would put a sampler on the hot path for no gain:
        // the underlying signal only moves when a client registers, unregisters
        // or heartbeats a view.
        let presence_interval_secs = std::env::var("SPKY_ADMIN_PRESENCE_INTERVAL_SECS")
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .filter(|n| *n > 0)
            .unwrap_or(15);

        let presence_slow_ms = std::env::var("SPKY_ADMIN_SLOW_VIEW_MS")
            .ok()
            .and_then(|s| s.parse::<f64>().ok())
            .filter(|n| n.is_finite() && *n > 0.0)
            .unwrap_or(250.0);

        let presence_large_view_rows = std::env::var("SPKY_ADMIN_LARGE_VIEW_ROWS")
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .filter(|n| *n > 0)
            .unwrap_or(1000);

        let presence_hot_view_rate = std::env::var("SPKY_ADMIN_HOT_VIEW_UPDATES_PER_SEC")
            .ok()
            .and_then(|s| s.parse::<f64>().ok())
            .filter(|n| n.is_finite() && *n > 0.0)
            .unwrap_or(5.0);

        let presence_max_rows = std::env::var("SPKY_ADMIN_PRESENCE_MAX_ROWS")
            .ok()
            .and_then(|s| s.parse::<usize>().ok())
            .filter(|n| *n > 0)
            .unwrap_or(20_000);

        // Zero is meaningful here, unlike the presence interval: it disables the
        // sampler for a deployment that would rather pay nothing for job
        // aggregates until someone opens the page.
        let job_interval_secs = std::env::var("SPKY_ADMIN_JOB_INTERVAL_SECS")
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(15);

        let job_live_interval_secs = std::env::var("SPKY_ADMIN_JOB_LIVE_INTERVAL_SECS")
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .filter(|n| *n > 0)
            .unwrap_or(2);

        Self {
            enabled,
            host: std::env::var("SPKY_ADMIN_HOST").unwrap_or_else(|_| "0.0.0.0".to_string()),
            port,
            dir: std::env::var("SPKY_ADMIN_DIR")
                .unwrap_or_else(|_| DEFAULT_ADMIN_DIR.to_string())
                .into(),
            // An empty value is treated as unset: a deployment that templates
            // this var but leaves it blank must not end up with a password of
            // "" that anyone can guess.
            password: std::env::var("SPKY_ADMIN_PASSWORD")
                .ok()
                .filter(|s| !s.is_empty()),
            access: std::env::var("SPKY_ADMIN_ACCESS")
                .ok()
                .filter(|s| !s.is_empty()),
            session_ttl: std::time::Duration::from_secs(session_ttl_secs),
            presence_interval: std::time::Duration::from_secs(presence_interval_secs),
            presence_slow_ms,
            presence_large_view_rows,
            presence_hot_view_rate,
            presence_max_rows,
            job_interval: std::time::Duration::from_secs(job_interval_secs),
            // Never slower than the idle cadence: a "live" tick that ticks less
            // often than the resting one would make opening the page make it
            // worse.
            job_live_interval: std::time::Duration::from_secs(
                job_live_interval_secs.min(job_interval_secs.max(1)),
            ),
            allowed_origins: std::env::var("SPKY_ADMIN_ALLOWED_ORIGINS")
                .unwrap_or_default()
                .split(',')
                .map(|o| o.trim().trim_end_matches('/').to_string())
                .filter(|o| !o.is_empty())
                .collect(),
            cloud_login: !matches!(
                std::env::var("SPKY_ADMIN_CLOUD_LOGIN").as_deref(),
                Ok("0") | Ok("false") | Ok("off")
            ),
            cloud_jwks: std::env::var("SPKY_ADMIN_CLOUD_JWKS").ok().filter(|s| !s.trim().is_empty()),
            cloud_issuer: std::env::var("SPKY_ADMIN_CLOUD_ISSUER").ok().filter(|s| !s.trim().is_empty()),
            cloud_session_ttl: std::time::Duration::from_secs(
                std::env::var("SPKY_ADMIN_CLOUD_SESSION_TTL_SECS")
                    .ok()
                    .and_then(|s| s.parse::<u64>().ok())
                    .filter(|n| *n > 0)
                    .unwrap_or(900),
            ),
        }
    }

    pub fn bind_addr(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }
}
