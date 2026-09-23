use anyhow::{bail, Context, Result};
use std::io::{BufRead, BufReader};
use std::path::Path;
use std::process::Command;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use crate::backend::{
    self, BackendDevConfig, BackendDevTypedConfig, DeployEnv, DeployMode, HostingMode,
    ResolvedSurrealDb, ResolvedVersions, RuntimeSource, Sp00kyConfig, DEFAULT_CONFIG_PATH,
};
use crate::migrate;
use crate::port_check;
use crate::schema_builder::{self, SchemaBuilderConfig};
use crate::schema_diff;
use crate::schema_extract;
use crate::surreal_client::{MigrationDB, SurrealClient};
use crate::ui::{self, LineSink, StreamKind};

const NETWORK_NAME: &str = "sp00ky-dev-net";
const SURREAL_CONTAINER: &str = "sp00ky-dev-surrealdb";
const SSP_CONTAINER: &str = "sp00ky-dev-ssp";
const SCHEDULER_CONTAINER: &str = "sp00ky-dev-scheduler";

pub(crate) const SURREAL_PORT: u16 = 8666;
pub(crate) const SSP_PORT: u16 = 8667;
pub(crate) const SCHEDULER_PORT: u16 = 9667;
const HEALTH_MAX_RETRIES: u32 = 30;
const HEALTH_RETRY_INTERVAL: Duration = Duration::from_secs(2);

const INFRA_SERVICES_SINGLENODE: &[&str] = &["surrealdb", "aspire-dashboard"];
const INFRA_SERVICES_CLUSTER: &[&str] = &["surrealdb"];
const INFRA_SERVICES_SURREALISM: &[&str] = &["surrealdb"];

/// Returns the SurrealDB URL for the given config: either the external endpoint
/// or `http://localhost:{port}` for locally hosted instances.
pub(crate) fn surreal_connection_url(resolved: &ResolvedSurrealDb, local_port: u16) -> String {
    match &resolved.hosting {
        HostingMode::External => resolved
            .endpoint_literal()
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| format!("http://localhost:{}", local_port)),
        HostingMode::Cloud => format!("http://localhost:{}", local_port),
    }
}

/// Collect every host port `spky dev` will try to bind in the current mode.
/// Mirrors the gates around each `docker run -p` in `run_direct_mode` /
/// `run_compose_mode`: SurrealDB only when locally hosted, scheduler only in
/// cluster mode, plus any user `dev: { type: docker, port: "..." }` entries.
fn collect_dev_ports(
    config: &Sp00kyConfig,
    mode: &DeployMode,
    resolved_surreal: &ResolvedSurrealDb,
) -> Vec<(u16, String)> {
    let mut out: Vec<(u16, String)> = Vec::new();
    out.push((SSP_PORT, "ssp".to_string()));
    if resolved_surreal.hosting != HostingMode::External {
        out.push((SURREAL_PORT, "surrealdb".to_string()));
    }
    if *mode == DeployMode::Cluster {
        out.push((SCHEDULER_PORT, "scheduler".to_string()));
    }

    let mut collect_app = |label: &str, name: &str, dev: &Option<BackendDevConfig>| {
        if let Some(BackendDevConfig::Typed(BackendDevTypedConfig::Docker {
            port, ports, ..
        })) = dev
        {
            for p in merge_docker_ports(port.as_deref(), ports.as_deref()) {
                match port_check::parse_docker_host_port(&p) {
                    Some(host) => out.push((host, format!("{}:{}", label, name))),
                    None => ui::warn(format!(
                        "could not parse docker port spec '{}' for {} '{}', skipping pre-check for it",
                        p, label, name
                    )),
                }
            }
        }
    };

    if let Some((name, fe)) = config.frontend() {
        if fe.runs_in_dev() {
            collect_app("app", name, &fe.dev);
        }
    }
    for (name, app) in config.backends() {
        if !app.runs_in_dev() || app.pool().is_some() {
            continue; // cloudOnly, or a pool backend: nothing of it listens on the host
        }
        collect_app("app", name, &app.dev);
    }
    // Docker apps publish their ports directly (no BackendDevConfig).
    for (name, app) in config.docker_apps() {
        if !app.runs_in_dev() {
            continue;
        }
        for p in &app.ports {
            if let Some(host) = p.host_port() {
                out.push((host, format!("app:{}", name)));
            }
        }
    }

    out
}

// ── Public entry point ──────────────────────────────────────────────────────

pub fn run(
    skip_migrations: bool,
    auto_apply_migrations: bool,
    fix_checksums: bool,
    clean: bool,
    clean_db: bool,
) -> Result<()> {
    let stop = Arc::new(AtomicBool::new(false));
    let stop_clone = stop.clone();
    ctrlc::set_handler(move || {
        stop_clone.store(true, Ordering::SeqCst);
    })
    .context("Failed to set Ctrl+C handler")?;

    // Load config first so the port pre-check below is mode-aware and so we
    // don't wipe local state in `--clean`/`--clean-db` if the pre-check fails.
    let config = backend::load_config(Path::new(DEFAULT_CONFIG_PATH));
    let mode = config.mode.clone().unwrap_or(DeployMode::Singlenode);
    // Anchor any relative `version: { ssp: { path: ... } }` entries against
    // the project directory (where sp00ky.yml lives), not the user's cwd.
    // Otherwise `../../target/debug/ssp-server` from inside `example/`
    // would only work when invoked from that exact dir.
    let project_dir = std::env::current_dir().context("Failed to get current directory")?;
    let versions = ResolvedVersions::from_config_with_dir(&config, DeployEnv::Dev, &project_dir);
    // `version: newest` means the newest build, not the newest one this machine
    // happens to have: re-pull before anything starts. Best effort, so working
    // offline still starts the stack on the image that is already here.
    for image in versions.images_to_refresh(&config) {
        match Command::new("docker").args(["pull", "--quiet", &image]).output() {
            Ok(out) if out.status.success() => {}
            Ok(out) => ui::detail(&format!(
                "could not refresh {image} ({}); using the local copy",
                String::from_utf8_lossy(&out.stderr).trim()
            )),
            Err(e) => ui::detail(&format!("could not refresh {image} ({e}); using the local copy")),
        }
    }
    let resolved = config.resolved_schema();
    let resolved_surreal = config.resolved_surrealdb();
    let migrations_path = resolved.migrations.to_string_lossy().to_string();
    let migrations_path = migrations_path.as_str();
    ui::header(
        "sp00ky dev",
        &[&mode.to_string(), concat!("v", env!("CARGO_PKG_VERSION"))],
    );

    // Pre-flight port check: bail before touching docker or local state if
    // any port we're about to bind is already taken.
    port_check::ensure_ports_free(
        collect_dev_ports(&config, &mode, &resolved_surreal),
        ui::step("Ports free"),
    )?;

    // `--clean-db` implies `--clean`: wiping the DB while keeping SSP/
    // scheduler caches would leave them rebootstrapping into stale state.
    let clean_state = clean || clean_db;

    if clean_state || clean_db {
        let mut subs: Vec<&str> = Vec::new();
        if clean_state {
            subs.extend_from_slice(&["ssp_data", "scheduler_data"]);
        }
        if clean_db {
            // Stop the existing SurrealDB container first so the running
            // process doesn't hold the volume open while we delete it.
            // Best-effort: ignored if the container doesn't exist.
            let _ = docker(&["rm", "-f", SURREAL_CONTAINER]);
            subs.push("surrealdb_data");
            // Persistent bucket files live on a sibling volume; a full DB reset
            // clears them too (a plain `--clean` preserves both).
            subs.push("bucket_data");
        }

        let mut removed: Vec<&str> = Vec::new();
        for sub in subs {
            let path = project_dir.join(".sp00ky").join(sub);
            if path.exists() {
                std::fs::remove_dir_all(&path)
                    .with_context(|| format!("Failed to remove {}", path.display()))?;
                removed.push(sub);
            }
        }

        let flag = if clean_db { "--clean-db" } else { "--clean" };
        if removed.is_empty() {
            ui::info(format!("{}: nothing to wipe under .sp00ky/", flag));
        } else {
            ui::info(format!("{}: wiped .sp00ky/{{{}}}", flag, removed.join(", ")));
        }
        if !clean_db {
            ui::hint("SurrealDB volume preserved (pass --clean-db for a full reset)");
        } else {
            ui::hint("SurrealDB volume wiped: starting from an empty database");
        }
    }

    // Check for schema drift before starting infrastructure
    if !skip_migrations {
        let step = ui::step("Schema drift");
        if let Err(e) = check_schema_drift(&config, step) {
            // Non-fatal: the step line already reads "failed while …"; show the
            // cause as one indented block and move on.
            let msg = format!("{:#}", e);
            for (i, part) in msg.split(": ").enumerate() {
                if i == 0 {
                    ui::hint(part);
                } else {
                    ui::hint(format!("  {}", part));
                }
            }
            ui::hint("Continuing without the drift check. Run `spky migrate create` to check manually.");
        }
    }

    // Check for compose files
    let compose_file = format!("docker-compose.{}.yml", mode.to_string());
    if Path::new(&compose_file).exists() {
        ui::info(format!("using {}", compose_file));
        // Compose mode is driven by the YAML, not by `version.{ssp,scheduler}`.
        // A `path:` entry can't take effect there, so flag it loudly so the
        // user doesn't silently keep hitting the published image.
        if versions.ssp.is_local() || versions.scheduler.is_local() {
            ui::warn(format!(
                "`version: {{ ssp/scheduler: {{ path: ... }} }}` is ignored in compose mode. The {} file controls those services. Either delete it (to use direct Docker mode with the path) or remove the path entry.",
                compose_file
            ));
        }
        run_compose_mode(
            &compose_file,
            &mode,
            &config,
            &resolved_surreal,
            &stop,
            skip_migrations,
            auto_apply_migrations,
            fix_checksums,
            migrations_path,
        )
    } else {
        run_direct_mode(
            &mode,
            &versions,
            &config,
            &resolved_surreal,
            &stop,
            skip_migrations,
            auto_apply_migrations,
            fix_checksums,
            migrations_path,
        )
    }
}

// ── Schema drift detection ──────────────────────────────────────────────────

fn check_schema_drift(config: &Sp00kyConfig, step: ui::Step) -> Result<()> {
    let resolved = config.resolved_schema();
    let schema_path = &resolved.schema;
    let migrations_dir = &resolved.migrations;

    // No schema file → nothing to check
    if !schema_path.exists() {
        step.skip("no schema file");
        return Ok(());
    }

    // Build the desired schema from source files
    let config_path = Path::new(DEFAULT_CONFIG_PATH);
    let builder_config = SchemaBuilderConfig {
        input_path: schema_path.clone(),
        config_path: if config_path.exists() {
            Some(config_path.to_path_buf())
        } else {
            None
        },
        mode: config.mode.clone().unwrap_or(DeployMode::Singlenode),
        endpoint: None,
        secret: None,
        include_functions: false,
    };

    step.set_message("building target schema…");
    let new_schema_sql = schema_builder::build_server_schema(&builder_config)
        .context("Failed to build schema from source files")?;

    // Extract old (from migrations) and new (from source) schemas via ephemeral DB
    step.set_message("comparing migrations against schema…");
    let (old_schema, new_schema) =
        schema_extract::extract_old_and_new_schemas(migrations_dir, &new_schema_sql)
            .context("Failed to extract schemas for drift comparison")?;

    // Diff
    let diff = schema_diff::diff_schemas(&old_schema, &new_schema);

    if diff.is_empty() {
        step.done("in sync");
        return Ok(());
    }

    // Drift detected — show summary
    step.warn(format!(
        "+{} -{} ~{}",
        diff.added.len(),
        diff.removed.len(),
        diff.modified.len(),
    ));
    diff.print_colored();

    // Unattended (no terminal, or --yes): warn and continue (matches
    // apply_migrations below).
    if ui::is_unattended() {
        ui::hint("Unattended: continuing with schema drift. Run `spky migrate create` to generate a migration.");
        return Ok(());
    }

    // Interactive prompt
    let options = vec!["Generate migration", "Continue anyway", "Abort"];
    let choice = ui::suspend(|| {
        inquire::Select::new("Schema drift detected. What would you like to do?", options)
            .prompt()
            .unwrap_or("Abort")
    });

    match choice {
        "Generate migration" => {
            let name = ui::suspend(|| inquire::Text::new("Migration name:").prompt())
                .context("Failed to read migration name")?;

            migrate::create(migrations_dir, &name, None, Some(&builder_config), None)
                .context("Failed to create migration")?;

            ui::info("Migration created. It will be applied in the next step.");
        }
        "Continue anyway" => {
            ui::hint("Continuing with schema drift. Run `spky migrate create` to generate a migration later.");
        }
        _ => bail!("User chose to abort due to schema drift."),
    }

    Ok(())
}

// ── Direct Docker mode ──────────────────────────────────────────────────────

/// Collapses the SSP/scheduler networking matrix into named getters.
///
/// SurrealDB and the rest of the dev infrastructure stay containerized, so
/// each side has to choose between docker-network DNS (`surrealdb:8000`,
/// `scheduler:9667`, `ssp:8667`) and host-side mapped ports
/// (`localhost:{SURREAL,SSP,SCHEDULER}_PORT`). When SSP and scheduler split
/// across host/container, the container side reaches the host via
/// `host.docker.internal`. See the table in the implementation plan.
struct RuntimeUrls {
    ssp_local: bool,
    scheduler_local: bool,
}

impl RuntimeUrls {
    fn new(ssp_local: bool, scheduler_local: bool) -> Self {
        Self {
            ssp_local,
            scheduler_local,
        }
    }

    fn ssp_db_url(&self) -> String {
        if self.ssp_local {
            format!("http://localhost:{}", SURREAL_PORT)
        } else {
            "http://surrealdb:8000".to_string()
        }
    }

    fn ssp_db_ws(&self) -> String {
        if self.ssp_local {
            format!("ws://localhost:{}", SURREAL_PORT)
        } else {
            "ws://surrealdb:8000".to_string()
        }
    }

    fn scheduler_db_url(&self) -> String {
        if self.scheduler_local {
            format!("http://localhost:{}", SURREAL_PORT)
        } else {
            "http://surrealdb:8000".to_string()
        }
    }

    fn scheduler_db_ws(&self) -> String {
        if self.scheduler_local {
            format!("ws://localhost:{}", SURREAL_PORT)
        } else {
            "ws://surrealdb:8000".to_string()
        }
    }

    /// Where the SSP reaches the scheduler (cluster mode only).
    fn ssp_scheduler_url(&self) -> String {
        match (self.ssp_local, self.scheduler_local) {
            // SSP host: localhost works whether the scheduler is host (direct)
            // or in a container (mapped port).
            (true, _) => format!("http://localhost:{}", SCHEDULER_PORT),
            // SSP container, scheduler host: container reaches host.
            (false, true) => format!("http://host.docker.internal:{}", SCHEDULER_PORT),
            // Both in containers: docker DNS.
            (false, false) => format!("http://scheduler:{}", SCHEDULER_PORT),
        }
    }

    /// Address the SSP advertises to the scheduler. The scheduler stores it
    /// and reuses it as the SSP endpoint for ingest forwarding.
    fn ssp_advertise(&self) -> String {
        match (self.ssp_local, self.scheduler_local) {
            // SSP host, scheduler host: localhost loops back.
            (true, true) => format!("localhost:{}", SSP_PORT),
            // SSP host, scheduler container: container side reaches host.
            (true, false) => format!("host.docker.internal:{}", SSP_PORT),
            // SSP container: existing behaviour, advertise the container alias.
            (false, _) => format!("{}:{}", SSP_CONTAINER, SSP_PORT),
        }
    }
}

fn run_direct_mode(
    mode: &DeployMode,
    versions: &ResolvedVersions,
    config: &Sp00kyConfig,
    resolved_surreal: &ResolvedSurrealDb,
    stop: &Arc<AtomicBool>,
    skip_migrations: bool,
    auto_apply_migrations: bool,
    fix_checksums: bool,
    migrations_path: &str,
) -> Result<()> {
    let surreal_image = versions.surrealdb_image();
    // Networking matrix between SSP and scheduler shifts based on whether
    // each runs in a container or on the host. RuntimeUrls collapses that
    // matrix into named getters so the launch blocks below stay readable.
    let urls = RuntimeUrls::new(versions.ssp.is_local(), versions.scheduler.is_local());

    // Clean up any stale resources from a previous run
    remove_pool_machines();
    let _ = docker(&["rm", "-f", SURREAL_CONTAINER]);
    let _ = docker(&["rm", "-f", SSP_CONTAINER]);
    let _ = docker(&["rm", "-f", SCHEDULER_CONTAINER]);
    let _ = docker(&["network", "rm", NETWORK_NAME]);

    let use_local_surreal = resolved_surreal.hosting != HostingMode::External;
    let surreal_url = surreal_connection_url(resolved_surreal, SURREAL_PORT);

    // Phase 1: Create Docker network
    {
        let step = ui::step("Docker network");
        if let Err(e) = docker(&["network", "create", NETWORK_NAME]) {
            step.fail("could not create");
            return Err(e);
        }
        step.done_quiet();
    }

    // Phase 2: Start SurrealDB (skip if using external instance)
    if use_local_surreal {
        let step = ui::step("SurrealDB");
        ensure_image(&surreal_image, Some(&step))?;
        step.set_message("starting container…");
        let surreal_data_dir = std::env::current_dir()
            .context("Failed to get current directory")?
            .join(".sp00ky/surrealdb_data");
        std::fs::create_dir_all(&surreal_data_dir).ok();
        let surreal_data_mount = format!("{}:/data", surreal_data_dir.display());

        let surreal_user_env = format!("SURREAL_USER={}", resolved_surreal.username_literal());
        let surreal_pass_env = format!("SURREAL_PASS={}", resolved_surreal.password_literal());
        let surreal_port_pub = format!("{}:8000", SURREAL_PORT);

        let mut surreal_args: Vec<String> = vec![
            "run".into(),
            "-d".into(),
            "--name".into(),
            SURREAL_CONTAINER.into(),
            "--network".into(),
            NETWORK_NAME.into(),
            "--network-alias".into(),
            "surrealdb".into(),
            "-p".into(),
            surreal_port_pub,
            "-v".into(),
            surreal_data_mount,
        ];

        // Mount the persistent bucket-storage volume when configured. Buckets
        // with a file backend (file:/buckets/<name>) resolve under this mount.
        // SurrealDB also gates file backends behind a folder allowlist —
        // `--allow-all` does NOT cover it, so the mount path must be added to
        // SURREAL_BUCKET_FOLDER_ALLOWLIST or every put fails "File access denied".
        if config.bucket_storage_gb().is_some() {
            let bucket_data_dir = std::env::current_dir()
                .context("Failed to get current directory")?
                .join(".sp00ky/bucket_data");
            std::fs::create_dir_all(&bucket_data_dir).ok();
            surreal_args.push("-v".into());
            surreal_args.push(format!(
                "{}:{}",
                bucket_data_dir.display(),
                crate::backend::BUCKET_VOLUME_PATH
            ));
            surreal_args.push("-e".into());
            surreal_args.push(format!(
                "SURREAL_BUCKET_FOLDER_ALLOWLIST={}",
                crate::backend::BUCKET_VOLUME_PATH
            ));
        }

        surreal_args.extend([
            "-e".into(),
            surreal_user_env,
            "-e".into(),
            surreal_pass_env,
            "-e".into(),
            "SURREAL_LOG=info".into(),
            "-e".into(),
            "SURREAL_CAPS_ALLOW_EXPERIMENTAL=surrealism,files".into(),
            surreal_image.clone(),
            "start".into(),
            "--bind".into(),
            "0.0.0.0:8000".into(),
            "--allow-all".into(),
            "--user".into(),
            resolved_surreal.username_literal(),
            "--pass".into(),
            resolved_surreal.password_literal(),
            "surrealkv:/data".into(),
        ]);

        let surreal_args_ref: Vec<&str> = surreal_args.iter().map(|s| s.as_str()).collect();
        if let Err(e) = docker(&surreal_args_ref) {
            step.fail("docker run failed");
            return Err(e);
        }

        // Phase 3: Wait for health
        wait_for_health(
            &format!("http://localhost:{}/health", SURREAL_PORT),
            HEALTH_MAX_RETRIES,
            HEALTH_RETRY_INTERVAL,
            stop,
            "SurrealDB",
            &step,
        )?;
        step.done("ready");
    } else {
        ui::step("SurrealDB").skip(format!("external{}{}", ui::glyphs().sep, surreal_url));
    }

    if stop.load(Ordering::SeqCst) {
        return cleanup_direct(stop);
    }

    // Phase 3a: Ensure namespace + database exist. Idempotent (the SurrealQL
    // uses IF NOT EXISTS) so it's safe to run on every boot, and required
    // after a `--clean-db` wipe — without it the migration engine's first
    // write hits "Couldn't write to a read only transaction" (SurrealDB's
    // way of saying the target NS/DB is missing) and the internal-schema
    // apply later fails with "database '...' does not exist".
    if use_local_surreal {
        let step = ui::step("Namespace / database");
        let bootstrap_client = SurrealClient::new(
            &surreal_url,
            &resolved_surreal.namespace,
            &resolved_surreal.database,
            &resolved_surreal.username_literal(),
            &resolved_surreal.password_literal(),
        );
        if let Err(e) = bootstrap_client.ensure_ns_db() {
            step.fail("failed");
            return Err(e).context("Failed to bootstrap SurrealDB namespace/database");
        }
        step.done(format!("{}/{}", resolved_surreal.namespace, resolved_surreal.database));
    }

    // Phase 4: Apply migrations
    if skip_migrations {
        ui::step("Migrations").skip("--skip-migrations");
    } else {
        apply_migrations(
            &surreal_url,
            auto_apply_migrations,
            fix_checksums,
            migrations_path,
            resolved_surreal,
        )?;
    }

    if stop.load(Ordering::SeqCst) {
        return cleanup_direct(stop);
    }

    // Phase 4a: Apply internal Sp00ky schema (meta tables + events)
    apply_internal_sp00ky_schema(&surreal_url, mode, versions, resolved_surreal)?;

    if stop.load(Ordering::SeqCst) {
        return cleanup_direct(stop);
    }

    // Phase 4b: Apply remote functions with Docker-internal endpoints
    apply_remote_functions(&surreal_url, mode, versions, resolved_surreal)?;

    if stop.load(Ordering::SeqCst) {
        return cleanup_direct(stop);
    }

    // Resolved RUST_LOG for both scheduler and SSP. `logLevel:` in sp00ky.yml
    // (string or `{ dev, cloud }` map) overrides; default `info` matches the
    // pre-feature behavior so projects that don't opt in see no change.
    let dev_log = backend::scoped_rust_log(&config.resolved_log_level(DeployEnv::Dev));

    // Phase 5 (cluster only): start the scheduler before the SSP so the SSP can
    // register. We keep the launch spec so the supervisor loop can respawn the
    // scheduler if it crashes; `scheduler_guard` is the current lifecycle handle
    // (in docker mode it owns the `docker logs -f` tail, in host mode the spawned
    // process directly). Both kill on Drop.
    let scheduler_spec: Option<SchedulerLaunchSpec>;
    let scheduler_guard: Option<LogTailGuard>;
    if *mode == DeployMode::Cluster {
        // Persist the scheduler replica + WAL to the host so `--clean` can
        // wipe it and so it survives container restarts.
        let scheduler_data_dir = std::env::current_dir()
            .context("Failed to get current directory")?
            .join(".sp00ky/scheduler_data");
        std::fs::create_dir_all(&scheduler_data_dir).ok();

        let kind = match &versions.scheduler {
            RuntimeSource::Image(_) => LaunchKind::Docker {
                image: versions
                    .scheduler_image()
                    .expect("scheduler_image is Some when RuntimeSource::Image"),
            },
            RuntimeSource::LocalBinary(path) => LaunchKind::Host {
                binary: path.clone(),
            },
        };
        let spec = SchedulerLaunchSpec {
            kind,
            data_dir: scheduler_data_dir,
            dev_log: dev_log.clone(),
            db_url: urls.scheduler_db_url(),
            db_ws: urls.scheduler_db_ws(),
            ns: resolved_surreal.namespace.clone(),
            db_name: resolved_surreal.database.clone(),
            db_user: resolved_surreal.username_literal(),
            db_pass: resolved_surreal.password_literal(),
            sync_env: {
                let mut env = dev_infra_env(&config);
                env.extend(pool_dev_env(&config, versions.scheduler.is_local()));
                env
            },
            mount_docker_sock: !config.pools.is_empty(),
        };

        let step = ui::step("Scheduler");
        let guard = start_scheduler(&spec, Some(&step))?;

        // Wait for /health/ready, which only flips to 200 after the scheduler
        // finishes cloning the upstream SurrealDB into its replica. Without this
        // gate, the SSP boots in Phase 6 against an empty replica and computes
        // wrong list_refs. Container liveness is only meaningful in docker mode;
        // a host process streams its own stdio and has no container to inspect.
        let check_container = spec.kind_is_docker();
        step.set_message("cloning replica from SurrealDB…");
        if let Err(e) = wait_for_health_with_container(
            &format!("http://localhost:{}/health/ready", SCHEDULER_PORT),
            HEALTH_MAX_RETRIES,
            HEALTH_RETRY_INTERVAL,
            stop,
            "Scheduler",
            check_container,
            &step,
        ) {
            // Docker mode already printed the container tail; a host process
            // only has what its sink buffered.
            if !check_container {
                guard.dump_ring("scheduler output");
            }
            return Err(e);
        }
        step.done(format!("replica cloned{}", spec.kind.detail()));

        scheduler_spec = Some(spec);
        scheduler_guard = Some(guard);
    } else {
        scheduler_spec = None;
        scheduler_guard = None;
    }

    if stop.load(Ordering::SeqCst) {
        return cleanup_direct(stop);
    }

    // Phase 6: Start SSP. As with the scheduler, keep the launch spec so the
    // supervisor loop can respawn it on crash.
    let data_dir = std::env::current_dir()
        .context("Failed to get current directory")?
        .join(".sp00ky/ssp_data");

    // Ensure data dir exists
    std::fs::create_dir_all(&data_dir).ok();

    // Build SPKY_JOB_CONFIG from backend apps with outbox method (mode-agnostic).
    // A Docker SSP can't reach host backends via 127.0.0.1; rewrite to
    // host.docker.internal (see build_job_config_json).
    let ssp_in_docker = matches!(versions.ssp, RuntimeSource::Image(_));
    let job_config_json = build_job_config_json(config, ssp_in_docker);

    let ssp_kind = match &versions.ssp {
        RuntimeSource::Image(_) => LaunchKind::Docker {
            image: versions
                .ssp_image()
                .expect("ssp_image is Some when RuntimeSource::Image"),
        },
        RuntimeSource::LocalBinary(path) => LaunchKind::Host {
            binary: path.clone(),
        },
    };
    let ssp_spec = SspLaunchSpec {
        kind: ssp_kind,
        data_dir,
        dev_log: dev_log.clone(),
        db_url: urls.ssp_db_url(),
        db_ws: urls.ssp_db_ws(),
        ns: resolved_surreal.namespace.clone(),
        db_name: resolved_surreal.database.clone(),
        db_user: resolved_surreal.username_literal(),
        db_pass: resolved_surreal.password_literal(),
        job_config: job_config_json,
        sync_env: dev_infra_env(&config),
        ref_mode: config.resolved_ref_mode().as_str().to_string(),
        anon_live: if config.resolved_anonymous_live_queries() {
            "1"
        } else {
            "0"
        }
        .to_string(),
        cluster: *mode == DeployMode::Cluster,
        scheduler_url: urls.ssp_scheduler_url(),
        advertise: urls.ssp_advertise(),
    };

    let step = ui::step("SSP");
    let ssp_guard = start_ssp(&ssp_spec, Some(&step))?;
    step.done(format!("started{}", ssp_spec.kind.detail()));

    // Infra is up: let infra/app sinks print (quiet-filtered) from here on.
    ui::done_startup();

    // Tail logs from infra containers (SurrealDB always; SSP is already
    // captured inside `ssp_guard`).
    let surreal_log = spawn_log_tail(SURREAL_CONTAINER, "surrealdb");

    // Start app dev servers (frontend + backends) BEFORE the ready box so it
    // can lead with the frontend's actual URL.
    let project_dir = std::env::current_dir().context("Failed to get current directory")?;
    let apps_step = ui::step("Apps");
    let mut app_dev = spawn_frontend_dev(config, &project_dir, resolved_surreal, mode, &apps_step);
    let backend_devs =
        spawn_backend_dev_commands(config, &project_dir, resolved_surreal, mode, &apps_step);
    let docker_devs = spawn_docker_app_devs(config, &project_dir, resolved_surreal, mode, &apps_step);
    apps_step.done(dev_app_names(config).join(", "));

    let frontend = wait_for_frontend_url(&mut app_dev, frontend_declared_port(config));
    print_ready_banner(config, mode, &surreal_url, use_local_surreal, frontend);

    // Supervisor loop: keep the SSP (and, in cluster mode, the scheduler) alive
    // until Ctrl+C. Both are restart-driven by design: the SSP calls
    // `std::process::exit(...)` on registration / bootstrap / heartbeat failures,
    // every one commented "exit so the supervisor restarts us". In staging/prod
    // the orchestrator is that supervisor; in dev there was none, so a single
    // crash left the stack wedged on "No ready SSP available for query". We are
    // that supervisor now.
    let mut services: Vec<SupervisedService> = Vec::new();
    if let (Some(spec), Some(guard)) = (scheduler_spec, scheduler_guard) {
        let is_docker = spec.kind_is_docker();
        services.push(SupervisedService::new(
            "scheduler",
            guard,
            is_docker,
            SCHEDULER_CONTAINER,
            move || start_scheduler(&spec, None),
        ));
    }
    {
        let is_docker = ssp_spec.kind_is_docker();
        services.push(SupervisedService::new(
            "ssp",
            ssp_guard,
            is_docker,
            SSP_CONTAINER,
            move || start_ssp(&ssp_spec, None),
        ));
    }

    while !stop.load(Ordering::SeqCst) {
        let now = Instant::now();
        for svc in &mut services {
            svc.tick(now);
        }
        thread::sleep(Duration::from_millis(250));
    }

    // Stop backend dev commands, log tailers, the app dev server, and the
    // supervised infra processes/containers.
    drop(docker_devs);
    drop(backend_devs);
    drop(app_dev);
    drop(surreal_log);
    drop(services);

    cleanup_direct(stop)
}

fn cleanup_direct(_stop: &Arc<AtomicBool>) -> Result<()> {
    ui::done_startup();
    ui::println("");
    ui::info("Shutting down…");

    // Remove every container we started this run. They all share the
    // `sp00ky-dev-` prefix: infra (`sp00ky-dev-{surrealdb,ssp,scheduler}`) and
    // app/frontend/backend docker apps (`sp00ky-dev-<name>` /
    // `sp00ky-dev-app-<name>`). The app LogTailGuard only SIGKILLs the
    // `docker run` client, which neither stops the container nor triggers
    // `--rm`, so without this sweep their published ports stay bound after
    // Ctrl+C. Fall back to the known infra names if enumeration fails.
    let removed = match Command::new("docker")
        .args(["ps", "-aq", "--filter", "name=^sp00ky-dev-"])
        .output()
    {
        Ok(out) if out.status.success() => {
            let ids: Vec<String> = String::from_utf8_lossy(&out.stdout)
                .split_whitespace()
                .map(|s| s.to_string())
                .collect();
            for id in &ids {
                let _ = docker(&["rm", "-f", id]);
            }
            !ids.is_empty()
        }
        _ => false,
    };
    if !removed {
        // Enumeration failed (or matched nothing) — remove infra by name.
        let _ = docker(&["rm", "-f", SCHEDULER_CONTAINER]);
        let _ = docker(&["rm", "-f", SSP_CONTAINER]);
        let _ = docker(&["rm", "-f", SURREAL_CONTAINER]);
    }

    // Pool machines hold the network open, so they go before it does.
    remove_pool_machines();

    // Remove network
    let _ = docker(&["network", "rm", NETWORK_NAME]);

    ui::info(format!("Cleaned up. Goodbye! {}", ui::glyphs().ghost));
    Ok(())
}

// ── Compose mode ────────────────────────────────────────────────────────────

fn run_compose_mode(
    compose_file: &str,
    mode: &DeployMode,
    config: &Sp00kyConfig,
    resolved_surreal: &ResolvedSurrealDb,
    stop: &Arc<AtomicBool>,
    skip_migrations: bool,
    auto_apply_migrations: bool,
    fix_checksums: bool,
    migrations_path: &str,
) -> Result<()> {
    let use_local_surreal = resolved_surreal.hosting != HostingMode::External;
    let surreal_url = surreal_connection_url(resolved_surreal, SURREAL_PORT);

    let infra_services: &[&str] = match mode {
        DeployMode::Cluster => INFRA_SERVICES_CLUSTER,
        DeployMode::Surrealism => INFRA_SERVICES_SURREALISM,
        _ => INFRA_SERVICES_SINGLENODE,
    };

    // Phase 1: Start infrastructure (filter out surrealdb if external)
    let infra_services: Vec<&str> = if use_local_surreal {
        infra_services.to_vec()
    } else {
        infra_services
            .iter()
            .copied()
            .filter(|s| *s != "surrealdb")
            .collect()
    };

    if !infra_services.is_empty() {
        let step = ui::step("Infrastructure");
        step.set_message(format!("docker compose up {}…", infra_services.join(" ")));
        let mut args = vec![
            "compose",
            "-f",
            compose_file,
            "up",
            "-d",
            "--remove-orphans",
        ];
        for svc in &infra_services {
            args.push(svc);
        }
        if let Err(e) = docker(&args) {
            step.fail("docker compose up failed");
            return Err(e);
        }
        step.done(infra_services.join(" "));
    } else {
        ui::step("Infrastructure").skip("external SurrealDB, nothing local to start");
    }

    if stop.load(Ordering::SeqCst) {
        return cleanup_compose(compose_file);
    }

    // Phase 2: Wait for SurrealDB health
    if use_local_surreal {
        let step = ui::step("SurrealDB");
        wait_for_health_with_container(
            &format!("http://localhost:{}/health", SURREAL_PORT),
            HEALTH_MAX_RETRIES,
            HEALTH_RETRY_INTERVAL,
            stop,
            "SurrealDB",
            false,
            &step,
        )?;
        step.done("ready");
    } else {
        ui::step("SurrealDB").skip(format!("external{}{}", ui::glyphs().sep, surreal_url));
    }

    if stop.load(Ordering::SeqCst) {
        return cleanup_compose(compose_file);
    }

    // Phase 3: Apply migrations
    if skip_migrations {
        ui::step("Migrations").skip("--skip-migrations");
    } else {
        apply_migrations(
            &surreal_url,
            auto_apply_migrations,
            fix_checksums,
            migrations_path,
            resolved_surreal,
        )?;
    }

    if stop.load(Ordering::SeqCst) {
        return cleanup_compose(compose_file);
    }

    // Phase 3a: Apply internal Sp00ky schema (meta tables + events)
    // Compose mode launches both services in docker per the YAML, so the
    // SurrealDB-side endpoints stay at the docker-DNS aliases. Default
    // versions (Image-variants) selects exactly that.
    let compose_versions = ResolvedVersions::default();
    apply_internal_sp00ky_schema(&surreal_url, mode, &compose_versions, resolved_surreal)?;

    if stop.load(Ordering::SeqCst) {
        return cleanup_compose(compose_file);
    }

    // Phase 3b: Apply remote functions with Docker-internal endpoints
    apply_remote_functions(&surreal_url, mode, &compose_versions, resolved_surreal)?;

    if stop.load(Ordering::SeqCst) {
        return cleanup_compose(compose_file);
    }

    // Phase 4: Start remaining services (foreground)
    ui::done_startup();

    // Start app dev servers (frontend + backends) before the ready box so it
    // can lead with the frontend's actual URL.
    let project_dir = std::env::current_dir().context("Failed to get current directory")?;
    let apps_step = ui::step("Apps");
    let mut app_dev = spawn_frontend_dev(config, &project_dir, resolved_surreal, mode, &apps_step);
    let backend_devs =
        spawn_backend_dev_commands(config, &project_dir, resolved_surreal, mode, &apps_step);
    let docker_devs = spawn_docker_app_devs(config, &project_dir, resolved_surreal, mode, &apps_step);
    apps_step.done(dev_app_names(config).join(", "));

    let frontend = wait_for_frontend_url(&mut app_dev, frontend_declared_port(config));
    print_ready_banner(config, mode, &surreal_url, use_local_surreal, frontend);
    ui::info("docker compose output follows");

    let status = Command::new("docker")
        .args([
            "compose",
            "-f",
            compose_file,
            "up",
            "--build",
            "--remove-orphans",
        ])
        .status()
        .context("Failed to run docker compose up")?;

    drop(docker_devs);
    drop(backend_devs);
    drop(app_dev);

    if !status.success() && !stop.load(Ordering::SeqCst) {
        bail!("docker compose up exited with status {}", status);
    }

    cleanup_compose(compose_file)
}

fn cleanup_compose(compose_file: &str) -> Result<()> {
    ui::done_startup();
    ui::println("");
    ui::info("Stopping compose services…");
    let _ = docker(&["compose", "-f", compose_file, "down", "--remove-orphans"]);
    ui::info(format!("Cleaned up. Goodbye! {}", ui::glyphs().ghost));
    Ok(())
}

// ── Health checking ─────────────────────────────────────────────────────────

fn wait_for_health(
    url: &str,
    max_retries: u32,
    interval: Duration,
    stop: &Arc<AtomicBool>,
    service_name: &str,
    step: &ui::Step,
) -> Result<()> {
    wait_for_health_with_container(url, max_retries, interval, stop, service_name, true, step)
}

/// Poll `url` until it answers 200. Progress is shown on `step` (live message
/// on a TTY, a heartbeat note every 10 attempts otherwise). The caller finishes
/// the step on success; on failure this fails it and bails.
fn wait_for_health_with_container(
    url: &str,
    max_retries: u32,
    interval: Duration,
    stop: &Arc<AtomicBool>,
    service_name: &str,
    check_container: bool,
    step: &ui::Step,
) -> Result<()> {
    // Try to infer container name from service name for liveness checks (direct mode only)
    let container_name = if check_container {
        match service_name {
            "SurrealDB" => Some(SURREAL_CONTAINER),
            "Scheduler" => Some(SCHEDULER_CONTAINER),
            "SSP" => Some(SSP_CONTAINER),
            _ => None,
        }
    } else {
        None
    };

    let started = Instant::now();
    for attempt in 1..=max_retries {
        if stop.load(Ordering::SeqCst) {
            bail!("Interrupted while waiting for {}", service_name);
        }

        // Check if the container is still running (fail fast on crash)
        if let Some(name) = container_name {
            if !is_container_running(name) {
                // Print last logs to help diagnose
                let _ = print_container_logs(name, 20);
                bail!(
                    "{} container '{}' exited unexpectedly. Check logs above.",
                    service_name,
                    name
                );
            }
        }

        match ureq::get(url).timeout(Duration::from_secs(5)).call() {
            Ok(resp) if resp.status() == 200 => return Ok(()),
            _ => {
                let waited = started.elapsed().as_secs();
                step.set_message(format!(
                    "waiting{}attempt {}/{}{}{}s",
                    ui::glyphs().sep,
                    attempt,
                    max_retries,
                    ui::glyphs().sep,
                    waited
                ));
                if attempt % 10 == 0 {
                    step.note(format!(
                        "still waiting (attempt {}/{}, {}s)",
                        attempt, max_retries, waited
                    ));
                }
                thread::sleep(interval);
            }
        }
    }

    // Print logs on timeout to help diagnose
    if let Some(name) = container_name {
        let _ = print_container_logs(name, 30);
    }

    bail!(
        "{} did not become ready after {} attempts",
        service_name,
        max_retries
    );
}

/// Check if a Docker container is currently running
fn is_container_running(name: &str) -> bool {
    Command::new("docker")
        .args(["inspect", "-f", "{{.State.Running}}", name])
        .output()
        .map(|o| String::from_utf8_lossy(&o.stdout).trim() == "true")
        .unwrap_or(false)
}

/// Read the last exit code of a (stopped) Docker container, so the supervisor
/// can tell an intentional re-bootstrap exit (3/4) from a crash. `None` if the
/// inspect fails or the value can't be parsed.
fn container_exit_code(name: &str) -> Option<i32> {
    Command::new("docker")
        .args(["inspect", "-f", "{{.State.ExitCode}}", name])
        .output()
        .ok()
        .and_then(|o| {
            String::from_utf8_lossy(&o.stdout)
                .trim()
                .parse::<i32>()
                .ok()
        })
}

/// Print the last N lines of a container's logs
fn print_container_logs(name: &str, tail: u32) -> Result<()> {
    let output = Command::new("docker")
        .args(["logs", "--tail", &tail.to_string(), name])
        .output()
        .context("Failed to get container logs")?;

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    let lines = stdout
        .lines()
        .chain(stderr.lines())
        .map(|l| l.to_string())
        .collect::<Vec<_>>();
    ui::block(&format!("last {} lines{}{}", tail, ui::glyphs().sep, name), lines.into_iter());
    Ok(())
}

// ── Migration helper ────────────────────────────────────────────────────────

fn apply_migrations(
    surreal_url: &str,
    auto_apply: bool,
    fix_checksums: bool,
    migrations_path: &str,
    resolved_surreal: &ResolvedSurrealDb,
) -> Result<()> {
    use crate::migration::{self, MigrationState};

    let step = ui::step("Migrations");
    let migrations_dir = Path::new(migrations_path);
    if !migrations_dir.exists() {
        step.skip(format!("no {}/ directory", migrations_path));
        return Ok(());
    }

    let sp00ky_config = backend::load_config(Path::new(DEFAULT_CONFIG_PATH));

    // Build engine for user migrations only (internal schema + remote functions
    // are separate phases in the dev flow with stop-checks between them).
    let ctx = migration::MigrationContext {
        environment: migration::MigrationEnvironment::Dev,
        project_dir: std::env::current_dir().context("Failed to get current directory")?,
        migrations_dir: migrations_dir.to_path_buf(),
        url: surreal_url.to_string(),
        namespace: resolved_surreal.namespace.clone(),
        database: resolved_surreal.database.clone(),
        username: "root".to_string(),
        password: "root".to_string(),
        surrealkit_binary: sp00ky_config.resolved_surrealkit_binary(),
        internal_schema: None,
        remote_functions: None,
        // Dev vault secrets for {{KEY}} substitution in user migrations (e.g. the
        // EdDSA signing key for the `account` access). Empty / not logged in ->
        // migrations apply verbatim (see LegacyEngine::apply).
        secrets: Some(crate::cloud::load_vault_envs_for_dev()),
    };

    let engine = migration::create_engine(ctx)?;

    // Fix checksums first if requested
    if fix_checksums {
        step.set_message("fixing checksums…");
        if let Err(e) = engine.fix(true) {
            ui::warn(format!("checksum fix failed: {:#}", e));
        }
    }

    // Check for pending migrations
    step.set_message("checking status…");
    let statuses = match engine.status() {
        Ok(s) => s,
        Err(e) => {
            step.fail("could not check status");
            ui::error(format!("{:#}", e));
            return Ok(());
        }
    };

    // Report drift warnings
    for info in &statuses {
        if info.state == MigrationState::Drift {
            let detail = info.detail.as_deref().unwrap_or("");
            ui::warn(format!("drift on {}_{}: {}", info.id, info.name, detail));
        }
    }

    let pending: Vec<_> = statuses
        .iter()
        .filter(|s| s.state == MigrationState::Pending)
        .collect();

    if pending.is_empty() {
        step.done("up to date");
        return Ok(());
    }

    step.warn(format!("{} pending", pending.len()));
    for m in &pending {
        ui::hint(format!("  {}_{}", m.id, m.name));
    }

    if auto_apply {
        ui::info("auto-applying (--apply-migrations)");
    } else if ui::is_unattended() {
        ui::info("unattended: auto-applying pending migrations");
    } else {
        let options = vec![
            "Apply migrations",
            "Skip migrations (continue without applying)",
            "Quit",
        ];
        let choice = ui::suspend(|| {
            inquire::Select::new(
                &format!(
                    "{} pending migration(s) found. What would you like to do?",
                    pending.len()
                ),
                options,
            )
            .prompt()
            .unwrap_or("Quit")
        });

        match choice {
            "Apply migrations" => {}
            "Skip migrations (continue without applying)" => {
                ui::info("Skipping migrations. Dev server will start without applying pending migrations.");
                return Ok(());
            }
            _ => bail!("User chose to quit."),
        }
    }

    match engine.apply() {
        Ok(_) => Ok(()),
        Err(e) => {
            ui::error(format!("Migration failed: {:#}", e));

            // Reset-and-retry uses SurrealClient directly (dev-only escape hatch)
            let client = SurrealClient::new(
                surreal_url,
                &resolved_surreal.namespace,
                &resolved_surreal.database,
                "root",
                "root",
            );

            if auto_apply || ui::is_unattended() {
                ui::info("auto-resetting database and retrying migrations");
                client.reset_database()?;
                engine.apply().map(|_| ())
            } else {
                let options = vec![
                    "Reset database and retry",
                    "Skip migrations (continue without applying)",
                    "Quit",
                ];
                let choice = ui::suspend(|| {
                    inquire::Select::new("Migration failed. What would you like to do?", options)
                        .prompt()
                        .unwrap_or("Quit")
                });

                match choice {
                    "Reset database and retry" => {
                        ui::info("Resetting database and retrying…");
                        client.reset_database()?;
                        engine.apply().map(|_| ())
                    }
                    "Skip migrations (continue without applying)" => {
                        ui::info("Skipping migrations. Dev server will start without applying pending migrations.");
                        Ok(())
                    }
                    _ => bail!("User chose to quit."),
                }
            }
        }
    }
}

// ── Remote functions helper ─────────────────────────────────────────────────

/// Pick the URL SurrealDB (in its container) should use to call out to
/// the SSP/scheduler. Container DNS aliases when the target also runs in
/// docker; `host.docker.internal` when it's a host process.
fn surreal_to_target_endpoint(mode: &DeployMode, versions: &ResolvedVersions) -> String {
    if *mode == DeployMode::Cluster {
        if versions.scheduler.is_local() {
            format!("http://host.docker.internal:{}", SCHEDULER_PORT)
        } else {
            format!("http://scheduler:{}", SCHEDULER_PORT)
        }
    } else {
        if versions.ssp.is_local() {
            format!("http://host.docker.internal:{}", SSP_PORT)
        } else {
            format!("http://ssp:{}", SSP_PORT)
        }
    }
}

/// Apply the remote functions with Docker-internal endpoints so that
/// SurrealDB (running inside the Docker network) can reach the SSP/scheduler
/// via container names instead of `localhost`.
/// Env the dev scheduler/SSP containers need to match the schema.
fn dev_infra_env(config: &backend::Sp00kyConfig) -> Vec<(String, String)> {
    let mut env = config.sync().infra_env();
    env.extend(config.impersonation().infra_env());
    env
}

fn apply_remote_functions(
    surreal_url: &str,
    mode: &DeployMode,
    versions: &ResolvedVersions,
    resolved_surreal: &ResolvedSurrealDb,
) -> Result<()> {
    let endpoint = surreal_to_target_endpoint(mode, versions);
    let secret = "mysecret";

    let impersonation =
        backend::impersonation_settings_for(Some(Path::new(DEFAULT_CONFIG_PATH)));
    let functions_sql =
        schema_builder::build_remote_functions_schema(mode, &endpoint, secret, &impersonation);

    let client = SurrealClient::new(
        surreal_url,
        &resolved_surreal.namespace,
        &resolved_surreal.database,
        "root",
        "root",
    );

    let step = ui::step("Remote functions");
    if let Err(e) = client.execute(&functions_sql) {
        step.fail("failed");
        return Err(e).context("Failed to apply remote functions");
    }
    step.done(format!("{} {}", ui::glyphs().arrow, endpoint));
    Ok(())
}

// ── Docker helpers ──────────────────────────────────────────────────────────

fn docker(args: &[&str]) -> Result<()> {
    let output = Command::new("docker")
        .args(args)
        .output()
        .with_context(|| format!("Failed to run: docker {}", args.join(" ")))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        bail!("docker {} failed: {}", args.join(" "), stderr.trim());
    }

    Ok(())
}

/// Make sure `image` is present locally before `docker run`, so a cold pull
/// shows up as a live step message (and a log line in non-TTY mode) instead
/// of minutes of silence. `docker()` swallows pull progress otherwise.
fn ensure_image(image: &str, step: Option<&ui::Step>) -> Result<()> {
    let present = Command::new("docker")
        .args(["image", "inspect", image])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false);
    if present {
        return Ok(());
    }
    if let Some(step) = step {
        step.set_message(format!("pulling {} (one-time)…", image));
        step.note(format!("pulling {} (one-time)", image));
    }
    let output = Command::new("docker")
        .args(["pull", image])
        .output()
        .with_context(|| format!("Failed to run: docker pull {}", image))?;
    if !output.status.success() {
        if let Some(step) = step {
            step.set_message(format!("pulling {}", image));
        }
        bail!(
            "docker pull {} failed: {}",
            image,
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    if ui::is_verbose() {
        for line in String::from_utf8_lossy(&output.stdout).lines().rev().take(2) {
            ui::detail(line);
        }
    }
    Ok(())
}

/// Names of the apps `spky dev` is about to launch, for the one-line summary
/// that replaces per-app "Starting: …" chatter in quiet mode.
fn dev_app_names(config: &Sp00kyConfig) -> Vec<String> {
    let mut names: Vec<String> = Vec::new();
    match config.frontend().filter(|(_, fe)| fe.runs_in_dev()) {
        Some((name, _)) => names.push(name.to_string()),
        None => names.push("app (pnpm dev:app)".to_string()),
    }
    for (name, app) in config.backends() {
        if app.runs_in_dev() && app.dev.is_some() {
            names.push(name.to_string());
        }
    }
    for (name, app) in config.docker_apps() {
        if app.runs_in_dev() {
            names.push(format!("{} (docker)", name));
        }
    }
    names
}

/// Host port an app will be reachable on in dev, when the config says so:
/// the first published port of a docker dev block / docker app, else the
/// `deploy.port` the service listens on (npm/uv dev servers bind it too).
/// None for a frontend dev server that picks its own port (vite); that one
/// announces its URL in the `[app]` stream instead.
fn dev_app_port(app: &backend::AppConfig) -> Option<u16> {
    if let Some(p) = dev_block_port(&app.dev) {
        return Some(p);
    }
    if let Some(p) = app.ports.iter().find_map(|p| p.host_port()) {
        return Some(p);
    }
    app.deploy.as_ref().and_then(|d| d.port)
}

/// Host port declared on a `dev:` block: `port:` on npm/uv, the first
/// published host port on docker. None for a string command or when unset.
fn dev_block_port(dev: &Option<BackendDevConfig>) -> Option<u16> {
    match dev {
        Some(BackendDevConfig::Typed(BackendDevTypedConfig::Npm { port, .. }))
        | Some(BackendDevConfig::Typed(BackendDevTypedConfig::Uv { port, .. })) => *port,
        Some(BackendDevConfig::Typed(BackendDevTypedConfig::Docker { port, ports, .. })) => {
            merge_docker_ports(port.as_deref(), ports.as_deref())
                .iter()
                .find_map(|p| port_check::parse_docker_host_port(p))
        }
        _ => None,
    }
}

/// Port declared for the frontend dev server, if any. Without one a vite-style
/// server picks its own port and announces it on its output instead. The
/// frontend's `deploy.port` is the production nginx port, deliberately not used.
fn frontend_declared_port(config: &Sp00kyConfig) -> Option<u16> {
    let (_, fe) = config.frontend().filter(|(_, fe)| fe.runs_in_dev())?;
    dev_block_port(&fe.dev)
}

/// The "ready" box: the app URL bright and first, then infra and backends dimmed.
fn print_ready_banner(
    config: &Sp00kyConfig,
    mode: &DeployMode,
    surreal_url: &str,
    local_surreal: bool,
    frontend: FrontendStatus,
) {
    use ui::BoxRow;
    let mut rows: Vec<BoxRow> = Vec::new();

    let app_name = config
        .frontend()
        .filter(|(_, fe)| fe.runs_in_dev())
        .map(|(n, _)| n.to_string())
        .unwrap_or_else(|| "app".to_string());
    match frontend {
        FrontendStatus::Url(url) => rows.push(BoxRow::kv(app_name, url)),
        FrontendStatus::Pending => rows.push(BoxRow::dim(app_name, "starting… URL follows below")),
        FrontendStatus::Failed => rows.push(BoxRow::dim(app_name, "failed to start (see errors above)")),
    }
    rows.push(BoxRow::Gap);

    // Infra rows are dimmed too: the app URL is the one people open.
    if local_surreal {
        rows.push(BoxRow::dim("SurrealDB", format!("http://localhost:{}", SURREAL_PORT)));
    } else {
        rows.push(BoxRow::dim("SurrealDB", surreal_url));
    }
    rows.push(BoxRow::dim("SSP", format!("http://localhost:{}", SSP_PORT)));
    if *mode == DeployMode::Cluster {
        rows.push(BoxRow::dim("Scheduler", format!("http://localhost:{}", SCHEDULER_PORT)));
    }

    let mut backend_rows: Vec<BoxRow> = Vec::new();
    for (name, app) in config.backends() {
        if !app.runs_in_dev() || app.dev.is_none() {
            continue;
        }
        if let Some(p) = dev_app_port(app) {
            backend_rows.push(BoxRow::dim(name, format!("http://localhost:{}", p)));
        }
    }
    for (name, app) in config.docker_apps() {
        if !app.runs_in_dev() {
            continue;
        }
        if let Some(p) = dev_app_port(app) {
            backend_rows.push(BoxRow::dim(name, format!("http://localhost:{}", p)));
        }
    }
    if !backend_rows.is_empty() {
        rows.push(BoxRow::Gap);
        rows.extend(backend_rows);
    }

    let mut footer: Vec<&str> = vec!["Ctrl+C to stop"];
    if !ui::is_verbose() {
        footer.push("errors only, --verbose for all logs");
    }
    ui::kv_box("Development environment ready", &rows, &footer);
}

/// Spawn a background thread that tails container logs.
/// Returns a guard that kills the child process on drop. Keeps the line sink
/// so a startup failure can dump what the process printed before it died
/// (quiet mode buffers infra output until the stack is ready).
struct LogTailGuard {
    child: Option<std::process::Child>,
    sink: Option<Arc<LineSink>>,
}

impl LogTailGuard {
    fn none() -> Self {
        LogTailGuard { child: None, sink: None }
    }

    /// Render the pre-ready lines buffered by this guard's sink, if any.
    fn dump_ring(&self, title: &str) {
        if let Some(sink) = &self.sink {
            sink.dump_ring(title);
        }
    }

    fn announced_url(&self) -> Option<String> {
        self.sink.as_ref().and_then(|s| s.announced_url())
    }

    fn print_urls(&self, on: bool) {
        if let Some(s) = &self.sink {
            s.print_urls(on);
        }
    }
}

/// What the ready box can say about the frontend dev server.
enum FrontendStatus {
    /// Dev server announced its URL (or it was known from a docker port).
    Url(String),
    /// Still starting when the box went out; its URL line will follow.
    Pending,
    /// Exited before announcing anything: look at the errors above.
    Failed,
}

/// Wait (briefly) for the frontend dev server to announce where it listens,
/// so the ready box can lead with the URL people actually open. Bounded so a
/// slow server never blocks the box; its URL line prints later instead.
fn wait_for_frontend_url(guard: &mut LogTailGuard, known_port: Option<u16>) -> FrontendStatus {
    if let Some(p) = known_port {
        return FrontendStatus::Url(format!("http://localhost:{}", p));
    }
    const MAX_WAIT: Duration = Duration::from_secs(20);
    let step = ui::step("App dev server");
    step.set_message("waiting for it to announce its URL…");
    let started = Instant::now();
    while started.elapsed() < MAX_WAIT {
        if let Some(url) = guard.announced_url() {
            step.done(url.clone());
            return FrontendStatus::Url(url);
        }
        if !guard.poll().alive {
            step.fail("exited before announcing a URL (see errors above)");
            return FrontendStatus::Failed;
        }
        thread::sleep(Duration::from_millis(100));
    }
    step.warn("still starting; its URL will print when it's up");
    guard.print_urls(true);
    FrontendStatus::Pending
}

/// Outcome of a liveness probe: whether the service is up, and — when it is
/// down — the process/container exit code if we could read it. The supervisor
/// uses the code to tell an *intentional* exit (the SSP exits 3/4 to force a
/// clean re-bootstrap) apart from a genuine crash.
struct Liveness {
    alive: bool,
    exit_code: Option<i32>,
}

impl LogTailGuard {
    /// Host-process liveness. While the wrapped child runs, `alive=true`. Once
    /// it has exited (reaping it here), `alive=false` and `exit_code` carries
    /// its status code if the OS reported one. No child (spawn failed) reads as
    /// down with no code. Docker guards wrap the `docker logs -f` tail rather
    /// than the service itself, so their liveness is tracked via the container
    /// (`is_container_running` / `container_exit_code`), not this handle.
    fn poll(&mut self) -> Liveness {
        match self.child {
            Some(ref mut child) => match child.try_wait() {
                Ok(None) => Liveness {
                    alive: true,
                    exit_code: None,
                },
                Ok(Some(status)) => Liveness {
                    alive: false,
                    exit_code: status.code(),
                },
                Err(_) => Liveness {
                    alive: false,
                    exit_code: None,
                },
            },
            None => Liveness {
                alive: false,
                exit_code: None,
            },
        }
    }
}

impl Drop for LogTailGuard {
    fn drop(&mut self) {
        if let Some(ref mut child) = self.child {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

// ── Supervised infra services (SSP + scheduler) ───────────────────────────────

/// How an infra service is launched, resolved once at boot from `version:`.
enum LaunchKind {
    Docker { image: String },
    Host { binary: std::path::PathBuf },
}

impl LaunchKind {
    /// Suffix for the step line: nothing for the published image, the binary
    /// path when running a locally built host process.
    fn detail(&self) -> String {
        match self {
            LaunchKind::Docker { .. } => String::new(),
            LaunchKind::Host { binary } => {
                format!("{}host{}{}", ui::glyphs().sep, ui::glyphs().sep, binary.display())
            }
        }
    }
}

/// Owned launch parameters for the scheduler, kept so the supervisor can respawn
/// it without re-deriving everything from borrowed `run_direct_mode` locals.
struct SchedulerLaunchSpec {
    kind: LaunchKind,
    data_dir: std::path::PathBuf,
    dev_log: String,
    db_url: String,
    db_ws: String,
    ns: String,
    db_name: String,
    db_user: String,
    db_pass: String,
    /// `SPKY_INGEST_TRANSPORT` and friends, from `sync:` in the manifest, plus
    /// the machine pool settings when the manifest declares pools.
    sync_env: Vec<(String, String)>,
    /// A containerized scheduler creates pool machines through the host's docker
    /// socket. Only mounted when the manifest actually declares a pool.
    mount_docker_sock: bool,
}

impl SchedulerLaunchSpec {
    fn kind_is_docker(&self) -> bool {
        matches!(self.kind, LaunchKind::Docker { .. })
    }
}

/// Owned launch parameters for the SSP, mirroring `SchedulerLaunchSpec`.
struct SspLaunchSpec {
    kind: LaunchKind,
    data_dir: std::path::PathBuf,
    dev_log: String,
    db_url: String,
    db_ws: String,
    ns: String,
    db_name: String,
    db_user: String,
    db_pass: String,
    job_config: String,
    ref_mode: String,
    anon_live: String,
    cluster: bool,
    scheduler_url: String,
    advertise: String,
    /// Same `sync:` env as the scheduler's, so a standalone SSP tails too.
    sync_env: Vec<(String, String)>,
}

impl SspLaunchSpec {
    fn kind_is_docker(&self) -> bool {
        matches!(self.kind, LaunchKind::Docker { .. })
    }
}

/// (Re)start the scheduler and return a fresh guard. No `wait_for_health`: a
/// respawn must not block the supervisor loop, and the SSP tolerates a
/// not-yet-ready scheduler via its own registration retry/backoff.
fn start_scheduler(spec: &SchedulerLaunchSpec, step: Option<&ui::Step>) -> Result<LogTailGuard> {
    match &spec.kind {
        LaunchKind::Docker { image } => {
            ensure_image(image, step)?;
            if let Some(s) = step {
                s.set_message("starting container…");
            }
            // Clear any exited container still holding the name from a prior crash.
            let _ = docker(&["rm", "-f", SCHEDULER_CONTAINER]);
            let port_mapping = format!("{}:9667", SCHEDULER_PORT);
            let data_mount = format!("{}:/data", spec.data_dir.display());
            let log_env = format!("RUST_LOG={}", spec.dev_log);
            let db_url = format!("SPKY_DB_URL={}", spec.db_url);
            let db_ws = format!("SPKY_DB_WS={}", spec.db_ws);
            let ns = format!("SPKY_DB_NS={}", spec.ns);
            let db_name = format!("SPKY_DB_NAME={}", spec.db_name);
            let db_user = format!("SPKY_DB_USER={}", spec.db_user);
            let db_pass = format!("SPKY_DB_PASS={}", spec.db_pass);
            let sync_env: Vec<String> = spec.sync_env.iter().map(|(k, v)| format!("{k}={v}")).collect();
            let mut args: Vec<&str> = vec![
                "run",
                "-d",
                "--name",
                SCHEDULER_CONTAINER,
                "--network",
                NETWORK_NAME,
                "--network-alias",
                "scheduler",
                "-p",
                &port_mapping,
                "-v",
                &data_mount,
                "-e",
                &log_env,
                "-e",
                &db_url,
                "-e",
                &db_ws,
                "-e",
                &ns,
                "-e",
                &db_name,
                "-e",
                &db_user,
                "-e",
                &db_pass,
                "-e",
                "SPKY_AUTH_SECRET=mysecret",
                // Default 300s makes records take 5 minutes to land in the
                // replica/SSP, unusable in dev.
                "-e",
                "SPKY_SNAPSHOT_UPDATE_INTERVAL_SECS=2",
                "-e",
                "SPKY_LOG_FORMAT=compact",
            ];
            for e in &sync_env {
                args.push("-e");
                args.push(e);
            }
            if spec.mount_docker_sock {
                args.push("-v");
                args.push("/var/run/docker.sock:/var/run/docker.sock");
            }
            args.push(image.as_str());
            docker(&args)?;
            Ok(spawn_log_tail(SCHEDULER_CONTAINER, "scheduler"))
        }
        LaunchKind::Host { binary } => {
            if !binary.exists() {
                bail!(
                    "Scheduler binary not found at {}.\n  Hint: run `cargo build -p scheduler` (or set version.dev.scheduler back to a Docker tag).",
                    binary.display()
                );
            }
            if let Some(s) = step {
                s.set_message("starting host process…");
            }
            let sink = LineSink::new("scheduler", ui::style().infra("scheduler"), StreamKind::Infra);
            let mut cmd = Command::new(binary);
            // The scheduler defaults `replica_db_path: ./data/replica` and
            // `wal_path: ./data/event_wal.log`, both relative to cwd. Run from
            // `.sp00ky/scheduler_data` so the host paths land where `--clean`
            // already wipes.
            cmd.current_dir(&spec.data_dir);
            cmd.env("RUST_LOG", &spec.dev_log)
                .env("SPKY_DB_URL", &spec.db_url)
                .env("SPKY_DB_WS", &spec.db_ws)
                .env("SPKY_DB_NS", &spec.ns)
                .env("SPKY_DB_NAME", &spec.db_name)
                .env("SPKY_DB_USER", &spec.db_user)
                .env("SPKY_DB_PASS", &spec.db_pass)
                .env("SPKY_AUTH_SECRET", "mysecret")
                .env("SPKY_SNAPSHOT_UPDATE_INTERVAL_SECS", "2")
                .env("SPKY_LOG_FORMAT", "compact");
            for (k, v) in &spec.sync_env {
                cmd.env(k, v);
            }
            Ok(spawn_prefixed(&mut cmd, sink))
        }
    }
}

/// (Re)start the SSP and return a fresh guard. See `start_scheduler` for why
/// there is no health wait here.
fn start_ssp(spec: &SspLaunchSpec, step: Option<&ui::Step>) -> Result<LogTailGuard> {
    match &spec.kind {
        LaunchKind::Docker { image } => {
            ensure_image(image, step)?;
            if let Some(s) = step {
                s.set_message("starting container…");
            }
            let _ = docker(&["rm", "-f", SSP_CONTAINER]);
            let port_mapping = format!("{}:8667", SSP_PORT);
            let data_mount = format!("{}:/data", spec.data_dir.display());
            let log_env = format!("RUST_LOG={}", spec.dev_log);
            let db_url = format!("SPKY_DB_URL={}", spec.db_url);
            let db_ws = format!("SPKY_DB_WS={}", spec.db_ws);
            let ns = format!("SPKY_DB_NS={}", spec.ns);
            let db_name = format!("SPKY_DB_NAME={}", spec.db_name);
            let db_user = format!("SPKY_DB_USER={}", spec.db_user);
            let db_pass = format!("SPKY_DB_PASS={}", spec.db_pass);
            let job_config = format!("SPKY_JOB_CONFIG={}", spec.job_config);
            let ref_mode = format!("SPKY_SSP_REF_MODE={}", spec.ref_mode);
            let anon_live = format!("SPKY_SSP_ANON_LIVE_QUERIES={}", spec.anon_live);
            let scheduler_url = format!("SPKY_SCHEDULER_URL={}", spec.scheduler_url);
            let advertise = format!("SPKY_SSP_ADVERTISE_ADDR={}", spec.advertise);

            let mut args: Vec<String> = vec![
                "run".into(),
                "-d".into(),
                "--name".into(),
                SSP_CONTAINER.into(),
                "--network".into(),
                NETWORK_NAME.into(),
                "--network-alias".into(),
                "ssp".into(),
                "-p".into(),
                port_mapping,
                "-e".into(),
                log_env,
                "-e".into(),
                db_url,
                "-e".into(),
                db_ws,
                "-e".into(),
                ns,
                "-e".into(),
                db_name,
                "-e".into(),
                db_user,
                "-e".into(),
                db_pass,
                "-e".into(),
                "SPKY_AUTH_SECRET=mysecret".into(),
                "-e".into(),
                job_config,
                "-e".into(),
                ref_mode,
                "-e".into(),
                anon_live,
                "-e".into(),
                "SPKY_LOG_FORMAT=compact".into(),
            ];
            if spec.cluster {
                args.push("-e".into());
                args.push(scheduler_url);
                args.push("-e".into());
                args.push("SPKY_SSP_ID=ssp-1".into());
                args.push("-e".into());
                args.push(advertise);
            }
            for (k, v) in &spec.sync_env {
                args.push("-e".into());
                args.push(format!("{k}={v}"));
            }
            args.push("-v".into());
            args.push(data_mount);
            args.push(image.clone());

            let argv: Vec<&str> = args.iter().map(|s| s.as_str()).collect();
            docker(&argv)?;
            Ok(spawn_log_tail(SSP_CONTAINER, "ssp"))
        }
        LaunchKind::Host { binary } => {
            if !binary.exists() {
                bail!(
                    "SSP binary not found at {}.\n  Hint: run `cargo build -p ssp-server` (or set version.dev.ssp back to a Docker tag).",
                    binary.display()
                );
            }
            if let Some(s) = step {
                s.set_message("starting host process…");
            }
            let sink = LineSink::new("ssp", ui::style().infra("ssp"), StreamKind::Infra);
            let mut cmd = Command::new(binary);
            cmd.current_dir(&spec.data_dir);
            cmd.env("RUST_LOG", &spec.dev_log)
                .env("SPKY_DB_URL", &spec.db_url)
                .env("SPKY_DB_WS", &spec.db_ws)
                .env("SPKY_DB_NS", &spec.ns)
                .env("SPKY_DB_NAME", &spec.db_name)
                .env("SPKY_DB_USER", &spec.db_user)
                .env("SPKY_DB_PASS", &spec.db_pass)
                .env("SPKY_AUTH_SECRET", "mysecret")
                .env("SPKY_JOB_CONFIG", &spec.job_config)
                .env("SPKY_SSP_REF_MODE", &spec.ref_mode)
                .env("SPKY_SSP_ANON_LIVE_QUERIES", &spec.anon_live)
                .env("SPKY_LOG_FORMAT", "compact")
                // The container Dockerfile binds 0.0.0.0:8667; on host we need
                // the same port reachable from frontend dev servers and
                // (optionally) the docker-side scheduler.
                .env("SPKY_SSP_LISTEN_ADDR", format!("0.0.0.0:{}", SSP_PORT));
            if spec.cluster {
                cmd.env("SPKY_SCHEDULER_URL", &spec.scheduler_url)
                    .env("SPKY_SSP_ID", "ssp-1")
                    .env("SPKY_SSP_ADVERTISE_ADDR", &spec.advertise);
            }
            for (k, v) in &spec.sync_env {
                cmd.env(k, v);
            }
            Ok(spawn_prefixed(&mut cmd, sink))
        }
    }
}

// Restart policy tuning for supervised infra services.
const RESTART_BACKOFF_INITIAL: Duration = Duration::from_secs(1);
const RESTART_BACKOFF_MAX: Duration = Duration::from_secs(30);
/// Continuous uptime after which a service is considered recovered: its backoff
/// and consecutive-restart counter reset.
const RESTART_RESET_AFTER: Duration = Duration::from_secs(60);
/// Give up after this many restarts in a row without the service ever staying up
/// for `RESTART_RESET_AFTER` (i.e. a genuine crash loop, not transient flapping).
const RESTART_MAX_CONSECUTIVE: usize = 5;
/// Exit codes the SSP uses for an *intentional* exit it expects to recover from
/// by restarting: 3 = scheduler dropped us, 4 = integrity-check resync. These
/// must not count toward `RESTART_MAX_CONSECUTIVE` — a correctly-resyncing SSP
/// would otherwise be killed by its own supervisor. See `apps/ssp/src/lib.rs`.
const INTENTIONAL_EXIT_CODES: &[i32] = &[3, 4];
/// After this many intentional restarts in a row (without ever staying up for
/// `RESTART_RESET_AFTER`), emit a distinct WARN so a real resync *storm* is
/// visible — but keep restarting (never hard-stop on an intentional exit).
const INTENTIONAL_RESTART_WARN_EVERY: usize = 5;

#[derive(Debug, PartialEq, Eq)]
enum SupervisorAction {
    /// Healthy, or backing off / already given up: do nothing this tick.
    None,
    /// Crashed but inside the backoff window: wait before retrying.
    Wait,
    /// Crashed and ready to (re)start now.
    Restart,
    /// Exited *on purpose* (e.g. the SSP exits 3/4 to force a clean
    /// re-bootstrap) and is ready to restart now. Does NOT count toward the
    /// crash-loop cap — a healthy resync must not look like a crash.
    RestartIntentional,
    /// Crash-looped past the cap: stop restarting, warn once.
    GaveUp,
}

/// Pure restart bookkeeping, split out so the timing logic is unit-testable with
/// injected `Instant`s (no sleeps, no processes).
struct RestartPolicy {
    backoff: Duration,
    last_restart: Option<Instant>,
    last_up: Instant,
    was_down: bool,
    consecutive_restarts: usize,
    /// Consecutive *intentional* restarts (exit 3/4) without a stable recovery.
    /// Tracked only to warn on a resync storm; never trips `gave_up`.
    intentional_restarts: usize,
    gave_up: bool,
}

impl RestartPolicy {
    fn new(now: Instant) -> Self {
        Self {
            backoff: RESTART_BACKOFF_INITIAL,
            last_restart: None,
            last_up: now,
            was_down: false,
            consecutive_restarts: 0,
            intentional_restarts: 0,
            gave_up: false,
        }
    }

    /// Decide what to do given the service's liveness at `now`. `exit_code` is
    /// the process/container exit status when `!alive` (None if unknown or
    /// alive); an intentional re-bootstrap code restarts without crash-counting.
    fn tick(&mut self, now: Instant, alive: bool, exit_code: Option<i32>) -> SupervisorAction {
        if self.gave_up {
            return SupervisorAction::None;
        }

        if alive {
            if self.was_down {
                // Just recovered from a restart.
                self.was_down = false;
                self.last_up = now;
            }
            // Stable long enough → forget past flapping.
            if now.duration_since(self.last_up) > RESTART_RESET_AFTER {
                self.backoff = RESTART_BACKOFF_INITIAL;
                self.consecutive_restarts = 0;
                self.intentional_restarts = 0;
                self.last_restart = None;
            }
            return SupervisorAction::None;
        }

        // Not alive.
        self.was_down = true;

        // Backoff gate: wait `backoff` since the last restart before retrying,
        // so a flapping service doesn't get hammered.
        if let Some(lr) = self.last_restart {
            if now.duration_since(lr) < self.backoff {
                return SupervisorAction::Wait;
            }
        }

        // Intentional exit (SSP forces a clean re-bootstrap): always restart,
        // never count toward the crash cap. A small fixed backoff still applies
        // (via `last_restart`, leaving `backoff` at its current value) so a tight
        // resync loop can't spin the CPU, but it can never `GaveUp`.
        if exit_code
            .map(|c| INTENTIONAL_EXIT_CODES.contains(&c))
            .unwrap_or(false)
        {
            self.intentional_restarts += 1;
            self.last_restart = Some(now);
            return SupervisorAction::RestartIntentional;
        }

        if self.consecutive_restarts >= RESTART_MAX_CONSECUTIVE {
            self.gave_up = true;
            return SupervisorAction::GaveUp;
        }

        self.consecutive_restarts += 1;
        self.last_restart = Some(now);
        self.backoff = (self.backoff * 2).min(RESTART_BACKOFF_MAX);
        SupervisorAction::Restart
    }
}

/// A dev infra service the supervisor keeps alive: holds the current process
/// handle, a liveness probe, and a respawn closure.
struct SupervisedService {
    label: &'static str,
    /// Container name when docker-launched (for crash diagnostics), else None.
    container: Option<&'static str>,
    guard: LogTailGuard,
    is_alive: Box<dyn FnMut(&mut LogTailGuard) -> Liveness>,
    respawn: Box<dyn FnMut() -> Result<LogTailGuard>>,
    policy: RestartPolicy,
}

impl SupervisedService {
    fn new(
        label: &'static str,
        guard: LogTailGuard,
        is_docker: bool,
        container: &'static str,
        respawn: impl FnMut() -> Result<LogTailGuard> + 'static,
    ) -> Self {
        let is_alive: Box<dyn FnMut(&mut LogTailGuard) -> Liveness> = if is_docker {
            // Docker: the guard tails logs; real liveness is the container.
            Box::new(move |_guard| {
                if is_container_running(container) {
                    Liveness {
                        alive: true,
                        exit_code: None,
                    }
                } else {
                    Liveness {
                        alive: false,
                        exit_code: container_exit_code(container),
                    }
                }
            })
        } else {
            // Host process: the guard owns the child directly.
            Box::new(|guard: &mut LogTailGuard| guard.poll())
        };
        Self {
            label,
            container: if is_docker { Some(container) } else { None },
            guard,
            is_alive,
            respawn: Box::new(respawn),
            policy: RestartPolicy::new(Instant::now()),
        }
    }

    /// One supervision step: probe liveness and act on the policy's decision.
    fn tick(&mut self, now: Instant) {
        if self.policy.gave_up {
            return;
        }
        let Liveness { alive, exit_code } = (self.is_alive)(&mut self.guard);
        match self.policy.tick(now, alive, exit_code) {
            SupervisorAction::None | SupervisorAction::Wait => {}
            SupervisorAction::GaveUp => {
                ui::error(format!(
                    "FATAL: {} crashed {} times in a row without recovering, not restarting it. Fix the cause and rerun `spky dev` (Ctrl+C to stop).",
                    self.label, RESTART_MAX_CONSECUTIVE
                ));
            }
            SupervisorAction::Restart => {
                ui::warn(format!(
                    "{} exited (code {:?}), restarting (attempt {} of {})…",
                    self.label, exit_code, self.policy.consecutive_restarts, RESTART_MAX_CONSECUTIVE
                ));
                // In quiet mode the crash output was filtered; show the tail
                // so the user sees *why* without rerunning with --verbose.
                // Once per crash streak, not on every backoff retry.
                if !ui::is_verbose() && self.policy.consecutive_restarts == 1 {
                    if let Some(c) = self.container {
                        let _ = print_container_logs(c, 20);
                    }
                }
                self.do_respawn();
            }
            SupervisorAction::RestartIntentional => {
                // A storm of intentional re-bootstraps points at an upstream
                // cause (e.g. a real integrity divergence); surface it loudly
                // but keep the service alive.
                if self.policy.intentional_restarts % INTENTIONAL_RESTART_WARN_EVERY == 0 {
                    ui::error(format!(
                        "{} has re-bootstrapped {} times without staying up (exit code {:?}). It will keep restarting; check the scheduler integrity check.",
                        self.label, self.policy.intentional_restarts, exit_code
                    ));
                } else {
                    ui::warn(format!(
                        "{} re-bootstrapping (intentional exit {:?}), restarting…",
                        self.label, exit_code
                    ));
                }
                self.do_respawn();
            }
        }
    }

    /// Respawn the service, replacing the guard. Dropping the stale guard kills
    /// the dead `docker logs -f` tail or reaps the exited host child.
    fn do_respawn(&mut self) {
        match (self.respawn)() {
            Ok(guard) => self.guard = guard,
            Err(e) => ui::error(format!("restart of {} failed: {:#}", self.label, e)),
        }
    }
}

#[cfg(test)]
mod docker_port_tests {
    use super::*;

    #[test]
    fn merges_single_port_and_array() {
        // A relay listening on WS + gRPC publishes both host ports.
        let merged = merge_docker_ports(
            Some("3670:3670"),
            Some(&["3671:3671".to_string()]),
        );
        assert_eq!(merged, vec!["3670:3670".to_string(), "3671:3671".to_string()]);
    }

    #[test]
    fn handles_only_ports_array_or_only_single() {
        assert_eq!(
            merge_docker_ports(None, Some(&["3670:3670".into(), "3671:3671".into()])),
            vec!["3670:3670".to_string(), "3671:3671".to_string()]
        );
        assert_eq!(merge_docker_ports(Some("80:80"), None), vec!["80:80".to_string()]);
        assert!(merge_docker_ports(None, None).is_empty());
    }
}

#[cfg(test)]
mod supervisor_tests {
    use super::*;

    fn at(base: Instant, secs: u64) -> Instant {
        base + Duration::from_secs(secs)
    }

    #[test]
    fn transient_crash_restarts_then_resets_after_stable_uptime() {
        let base = Instant::now();
        let mut p = RestartPolicy::new(base);

        // Crash at t=10 → immediate restart, backoff doubles to 2s.
        assert_eq!(
            p.tick(at(base, 10), false, Some(1)),
            SupervisorAction::Restart
        );
        assert_eq!(p.consecutive_restarts, 1);
        assert_eq!(p.backoff, Duration::from_secs(2));

        // Comes back up.
        assert_eq!(p.tick(at(base, 11), true, None), SupervisorAction::None);

        // Still up past the reset window → backoff + counter cleared.
        assert_eq!(
            p.tick(at(base, 11 + 61), true, None),
            SupervisorAction::None
        );
        assert_eq!(p.backoff, RESTART_BACKOFF_INITIAL);
        assert_eq!(p.consecutive_restarts, 0);
    }

    #[test]
    fn backoff_gate_blocks_rapid_retries() {
        let base = Instant::now();
        let mut p = RestartPolicy::new(base);

        // First crash → restart immediately, backoff now 2s.
        assert_eq!(p.tick(base, false, Some(1)), SupervisorAction::Restart);
        assert_eq!(p.backoff, Duration::from_secs(2));

        // 1s later, still down and within the 2s gate → wait.
        assert_eq!(p.tick(at(base, 1), false, Some(1)), SupervisorAction::Wait);

        // 2s later, gate elapsed → restart again, backoff now 4s.
        assert_eq!(
            p.tick(at(base, 2), false, Some(1)),
            SupervisorAction::Restart
        );
        assert_eq!(p.backoff, Duration::from_secs(4));
    }

    #[test]
    fn gives_up_after_max_consecutive() {
        let base = Instant::now();
        let mut p = RestartPolicy::new(base);

        // Drive a permanently-down service, advancing time so each backoff gate
        // elapses, and confirm it gives up after exactly the cap.
        let mut t = 0u64;
        let mut restarts = 0;
        loop {
            assert!(t < 10_000, "should give up rather than loop forever");
            match p.tick(at(base, t), false, Some(1)) {
                SupervisorAction::Restart => restarts += 1,
                SupervisorAction::GaveUp => break,
                _ => {}
            }
            t += 1;
        }
        assert_eq!(restarts, RESTART_MAX_CONSECUTIVE);
        assert!(p.gave_up);

        // Once given up, it stays given up and issues no further restarts.
        assert_eq!(
            p.tick(at(base, t + 1), false, Some(1)),
            SupervisorAction::None
        );
    }

    #[test]
    fn intentional_rebootstrap_never_gives_up() {
        // The SSP exits 4 (integrity resync) over and over. This is a healthy
        // re-bootstrap, NOT a crash — the supervisor must keep restarting it and
        // never reach GaveUp, no matter how many times in a row.
        let base = Instant::now();
        let mut p = RestartPolicy::new(base);

        let mut t = 0u64;
        let mut restarts = 0;
        while restarts < RESTART_MAX_CONSECUTIVE * 4 {
            match p.tick(at(base, t), false, Some(4)) {
                SupervisorAction::RestartIntentional => restarts += 1,
                SupervisorAction::Restart => panic!("intentional exit must not crash-count"),
                SupervisorAction::GaveUp => panic!("must never give up on an intentional exit"),
                _ => {}
            }
            t += 1;
        }
        assert!(!p.gave_up);
        assert_eq!(
            p.consecutive_restarts, 0,
            "intentional exits don't touch the crash counter"
        );
        assert_eq!(p.intentional_restarts, restarts);
    }

    #[test]
    fn exit_code_3_is_also_intentional() {
        // 3 = scheduler dropped us; same intentional treatment as 4.
        let base = Instant::now();
        let mut p = RestartPolicy::new(base);
        assert_eq!(
            p.tick(base, false, Some(3)),
            SupervisorAction::RestartIntentional
        );
        assert_eq!(p.consecutive_restarts, 0);
    }

    #[test]
    fn unknown_exit_code_counts_as_crash() {
        // A down service with no readable exit code is treated as a crash (the
        // conservative default), so a genuine crash loop still surfaces.
        let base = Instant::now();
        let mut p = RestartPolicy::new(base);
        assert_eq!(p.tick(base, false, None), SupervisorAction::Restart);
        assert_eq!(p.consecutive_restarts, 1);
    }

    #[test]
    fn intentional_restarts_reset_after_stable_uptime() {
        let base = Instant::now();
        let mut p = RestartPolicy::new(base);

        assert_eq!(
            p.tick(at(base, 1), false, Some(4)),
            SupervisorAction::RestartIntentional
        );
        assert_eq!(p.intentional_restarts, 1);
        // Comes up and stays up past the reset window → counter cleared.
        assert_eq!(p.tick(at(base, 2), true, None), SupervisorAction::None);
        assert_eq!(p.tick(at(base, 2 + 61), true, None), SupervisorAction::None);
        assert_eq!(p.intentional_restarts, 0);
    }
}

/// Build SPKY_JOB_CONFIG JSON from backend apps with outbox methods.
///
/// `baseUrl` in sp00ky.yml is written for the co-located cloud runtime, where
/// SSP and the backend share a network namespace and `127.0.0.1:<port>` reaches
/// the backend. In dev, when the SSP runs as a Docker container, `127.0.0.1`
/// is the SSP container's own loopback, so a host-process backend (`dev: npm` /
/// `uv`) or a sibling `dev: docker` backend (published to the host) is
/// unreachable there. Rewrite the loopback host to `host.docker.internal` so
/// the containerized SSP reaches the host. A host-binary SSP keeps `127.0.0.1`.
fn build_job_config_json(config: &Sp00kyConfig, ssp_in_docker: bool) -> String {
    let mut entries = Vec::new();
    for (name, app) in config.backends() {
        if !app.runs_in_dev() {
            continue; // cloudOnly backend isn't running in dev; don't route jobs to it
        }
        // A pool backend's jobs run on pool machines, driven by the scheduler's
        // pool engine. Routing its table to the SSP runner as well would have two
        // executors claiming the same rows. A dedicated machine backend is an
        // ordinary backend here: in dev it runs on the host like any other, and
        // in the cloud the SSPs POST to it through its forwarder.
        if app.pool().is_some() {
            continue;
        }
        let method = match &app.method {
            Some(m) => m,
            None => continue,
        };
        let base_url = match &app.base_url {
            Some(u) if ssp_in_docker => u
                .replace("127.0.0.1", "host.docker.internal")
                .replace("localhost", "host.docker.internal"),
            Some(u) => u.clone(),
            None => continue,
        };
        let table = match &method.table {
            Some(t) => t.clone(),
            None => continue,
        };
        let auth_token = app.auth.as_ref().and_then(|a| a.token.clone());
        let timeout = app.deploy.as_ref().and_then(|d| d.timeout);
        let timeout_overridable = app
            .deploy
            .as_ref()
            .and_then(|d| d.timeout_overridable)
            .unwrap_or(false);

        entries.push(serde_json::json!({
            "name": name,
            "table": table,
            "base_url": base_url,
            "auth_token": auth_token,
            "timeout": timeout,
            "timeout_overridable": timeout_overridable,
        }));
    }
    serde_json::to_string(&entries).unwrap_or_else(|_| "[]".to_string())
}

/// Scheduler environment for machine pools under `spky dev`.
///
/// Every pool runs on the `docker` provider in dev, whatever the manifest says
/// (`pool_sync` writes the rows that way too): machines are containers on the dev
/// network. They reach the scheduler by its network alias when it is a container,
/// and through the host gateway when it is a locally built host process.
///
/// `SPKY_DEV_AGENT_IMAGE` overrides the agent image, for working on the agent.
fn pool_dev_env(config: &Sp00kyConfig, scheduler_on_host: bool) -> Vec<(String, String)> {
    if config.pools.is_empty() {
        return vec![("SPKY_POOL_ENABLED".into(), "false".into())];
    }
    let public_url = if scheduler_on_host {
        "http://host.docker.internal:9669"
    } else {
        "http://scheduler:9669"
    };
    let mut env = vec![
        ("SPKY_POOL_DOCKER".to_string(), "1".to_string()),
        ("SPKY_POOL_DOCKER_NETWORK".to_string(), NETWORK_NAME.to_string()),
        ("SPKY_POOL_PUBLIC_URL".to_string(), public_url.to_string()),
    ];
    if let Ok(image) = std::env::var("SPKY_DEV_AGENT_IMAGE") {
        if !image.trim().is_empty() {
            env.push(("SPKY_POOL_AGENT_IMAGE".to_string(), image));
        }
    }
    env
}

/// Build the image pool machines run for `name`, tagged the way `pool_sync`
/// names it in the pool row. Needs `deploy.dockerfile`: there is no other
/// description of how to run this backend inside a container.
fn build_pool_image(name: &str, app: &backend::AppConfig, project_dir: &Path) {
    let Some(dockerfile) = app.deploy.as_ref().and_then(|d| d.dockerfile.as_deref()) else {
        ui::warn(format!(
            "'{name}' runs on a pool but has no `deploy.dockerfile`, so there is no image to run it from"
        ));
        return;
    };
    let context = app.deploy.as_ref().and_then(|d| d.context.as_deref()).unwrap_or(".");
    let tag = crate::pool_sync::dev_image_tag(name);
    let built = Command::new("docker")
        .args(["build", "-q", "-f", dockerfile, "-t", &tag, context])
        .current_dir(project_dir)
        .output();
    match built {
        Ok(out) if out.status.success() => {}
        Ok(out) => ui::warn(format!(
            "docker build for pool backend '{name}' failed:\n{}",
            String::from_utf8_lossy(&out.stderr).trim()
        )),
        Err(e) => ui::warn(format!("could not run docker build for pool backend '{name}': {e}")),
    }
}

/// Remove every pool machine container. They are created by the scheduler, so
/// the CLI only knows them by the label the docker provider stamps on them.
fn remove_pool_machines() {
    let Ok(out) = Command::new("docker").args(["ps", "-aq", "--filter", "label=spky.pool"]).output()
    else {
        return;
    };
    let ids: Vec<String> =
        String::from_utf8_lossy(&out.stdout).split_whitespace().map(str::to_string).collect();
    if ids.is_empty() {
        return;
    }
    let mut args = vec!["rm".to_string(), "-f".to_string()];
    args.extend(ids);
    let _ = Command::new("docker").args(&args).output();
}

/// Build the auto-injected SPKY_* environment variables for dev mode.
fn build_spky_dev_vars(
    resolved_surreal: &ResolvedSurrealDb,
    mode: &DeployMode,
) -> Vec<(String, String)> {
    let mut vars = vec![
        ("SPKY_ENV".into(), "dev".into()),
        (
            "SPKY_DB_URL".into(),
            format!("http://localhost:{}", SURREAL_PORT),
        ),
        (
            "SPKY_DB_WS".into(),
            format!("ws://localhost:{}", SURREAL_PORT),
        ),
        ("SPKY_DB_NS".into(), resolved_surreal.namespace.clone()),
        ("SPKY_DB_NAME".into(), resolved_surreal.database.clone()),
        ("SPKY_DB_USER".into(), resolved_surreal.username_literal()),
        ("SPKY_DB_PASS".into(), resolved_surreal.password_literal()),
        ("SPKY_SSP_ADDR".into(), format!("localhost:{}", SSP_PORT)),
    ];
    if *mode == DeployMode::Cluster {
        vars.push((
            "SPKY_SCHEDULER_URL".into(),
            format!("http://localhost:{}", SCHEDULER_PORT),
        ));
    }
    vars
}

/// Merge SPKY auto-injected vars with user-provided vars. User vars take precedence.
fn merge_spky_with_user_env(
    spky_vars: &[(String, String)],
    user_vars: Vec<(String, String)>,
) -> Vec<(String, String)> {
    let mut merged = std::collections::BTreeMap::new();
    // SPKY vars first (base)
    for (k, v) in spky_vars {
        merged.insert(k.clone(), v.clone());
    }
    // User vars override
    for (k, v) in user_vars {
        merged.insert(k, v);
    }
    merged.into_iter().collect()
}

/// Warn if a frontend app uses vault without a whitelist.
fn warn_frontend_vault_no_whitelist(name: &str, env: &Option<backend::EnvConfig>) {
    if let Some(backend::EnvConfig::Source(backend::EnvSource::Str(s))) = env {
        if s == "vault" {
            ui::warn(format!("Frontend app '{}' uses vault without a whitelist. Consider using vault: [KEY1, KEY2] to avoid exposing all secrets to the frontend.", name));
        }
    }
}

/// Sink for the frontend app's dev server output (`[app]`).
fn app_sink() -> Arc<LineSink> {
    LineSink::new("app", ui::style().app.clone(), StreamKind::App)
}

fn spawn_pnpm_dev_app(script: &str, envs: Vec<(String, String)>) -> LogTailGuard {
    let sink = app_sink();
    sink.push_verbose(&format!("Starting: pnpm {}", script));
    spawn_prefixed(Command::new("pnpm").args([script]).envs(envs), sink)
}

/// Start the frontend app dev server from the apps config.
fn spawn_frontend_dev(
    config: &Sp00kyConfig,
    project_dir: &Path,
    resolved_surreal: &ResolvedSurrealDb,
    mode: &DeployMode,
    step: &ui::Step,
) -> LogTailGuard {
    if let Some((name, frontend)) = config.frontend().filter(|(_, fe)| fe.runs_in_dev()) {
        step.set_message(format!("starting {}…", name));
        warn_frontend_vault_no_whitelist(name, &frontend.env);
        let spky_vars = build_spky_dev_vars(resolved_surreal, mode);
        let user_envs = resolve_env_for_dev(&frontend.env, project_dir);
        let envs = merge_spky_with_user_env(&spky_vars, user_envs);
        // Use the same dev config dispatch as backends
        if let Some(ref dev_config) = frontend.dev {
            let sink = app_sink();
            match dev_config {
                BackendDevConfig::Command(cmd) => {
                    sink.push_verbose(&format!("Starting: {}", cmd));
                    return spawn_prefixed(
                        Command::new("sh")
                            .args(["-c", cmd.as_str()])
                            .current_dir(project_dir)
                            .envs(envs),
                        sink,
                    );
                }
                BackendDevConfig::Typed(BackendDevTypedConfig::Npm { script, workdir, .. }) => {
                    let cwd = resolve_workdir(project_dir, workdir.as_deref());
                    sink.push_verbose(&format!("Starting: pnpm run {}", script));
                    return spawn_prefixed(
                        Command::new("pnpm")
                            .args(["run", script])
                            .current_dir(cwd)
                            .envs(envs),
                        sink,
                    );
                }
                BackendDevConfig::Typed(BackendDevTypedConfig::Docker {
                    file,
                    workdir,
                    port,
                    ports,
                    platform,
                }) => {
                    let cwd = resolve_workdir(project_dir, workdir.as_deref());
                    let all_ports = merge_docker_ports(port.as_deref(), ports.as_deref());
                    sink.push_verbose(&format!("Building: docker build -f {}", file));
                    return spawn_docker_dev(
                        file,
                        &all_ports,
                        platform.as_deref(),
                        &envs,
                        &cwd,
                        "frontend",
                        sink,
                        step,
                    );
                }
                BackendDevConfig::Typed(BackendDevTypedConfig::Uv { script, workdir, .. }) => {
                    let cwd = resolve_workdir(project_dir, workdir.as_deref());
                    sink.push_verbose(&format!("Starting: uv run {}", script));
                    return spawn_prefixed(
                        Command::new("uv")
                            .args(["run", script])
                            .current_dir(cwd)
                            .envs(envs),
                        sink,
                    );
                }
            }
        }
        // Fallback: no dev config — try pnpm dev:app
        return spawn_pnpm_dev_app("dev:app", envs);
    }
    // No frontend app defined — try default pnpm dev:app
    step.set_message("starting app (pnpm dev:app)…");
    spawn_pnpm_dev_app("dev:app", Vec::new())
}

/// Apply the internal Sp00ky schema (meta tables + per-table events) so that
/// record versioning and DBSP ingest work after migrations are applied.
fn apply_internal_sp00ky_schema(
    surreal_url: &str,
    mode: &DeployMode,
    versions: &ResolvedVersions,
    resolved_surreal: &ResolvedSurrealDb,
) -> Result<()> {
    let config = backend::load_config(Path::new(DEFAULT_CONFIG_PATH));
    let resolved = config.resolved_schema();

    if !resolved.schema.exists() {
        ui::step("Internal schema").skip(format!("no schema file at {}", resolved.schema.display()));
        return Ok(());
    }

    let endpoint = surreal_to_target_endpoint(mode, versions);
    let secret = "mysecret";

    let config_path = Path::new(DEFAULT_CONFIG_PATH);
    let config_path_ref = if config_path.exists() {
        Some(config_path)
    } else {
        None
    };

    let client = SurrealClient::new(
        surreal_url,
        &resolved_surreal.namespace,
        &resolved_surreal.database,
        "root",
        "root",
    );

    migrate::apply_internal_schema(
        &client,
        &resolved.schema,
        config_path_ref,
        mode,
        Some(&endpoint),
        Some(secret),
    )?;

    // Machine pools: after the internal schema (the tables must exist), with the
    // dev image tags and dev environment.
    if !config.pools.is_empty() {
        let project_dir = config_path.parent().unwrap_or(Path::new("."));
        crate::pool_sync::sync_and_report(
            &client,
            &config,
            &crate::pool_sync::PoolEnv::Dev { project_dir },
        );
    }

    // Publish the query allowlist so a `warn`/`enforce` dev SSP has something to
    // compare against. Version "dev": one row, overwritten on every start.
    let config_dir = config_path.parent().unwrap_or(Path::new("."));
    crate::cloud::write_query_allowlist_rows(
        surreal_url,
        "root",
        &resolved_surreal.namespace,
        &resolved_surreal.database,
        &config,
        config_dir,
        "dev",
    )
}

// ── Backend dev command helpers ──────────────────────────────────────────────

fn spawn_backend_dev_commands(
    config: &Sp00kyConfig,
    project_dir: &Path,
    resolved_surreal: &ResolvedSurrealDb,
    mode: &DeployMode,
    step: &ui::Step,
) -> Vec<LogTailGuard> {
    let spky_vars = build_spky_dev_vars(resolved_surreal, mode);
    let mut guards = Vec::new();
    let mut color_idx = 0;
    for (name, app) in config.backends() {
        if !app.runs_in_dev() {
            continue; // cloudOnly: deployed but not started by `spky dev`
        }
        // A pool backend never runs as one always-on dev process: the scheduler
        // starts it on pool machines (containers, in dev), on demand. What dev
        // owes it is the image those machines run.
        if app.pool().is_some() {
            step.set_message(format!("building pool image for {}…", name));
            build_pool_image(name, app, project_dir);
            continue;
        }
        let dev_config = match &app.dev {
            Some(cfg) => cfg,
            None => continue,
        };
        step.set_message(format!("starting {}…", name));
        let sink = LineSink::new(
            &format!("app.{}.dev", name),
            ui::style().app_cycle(color_idx),
            StreamKind::App,
        );
        color_idx += 1;
        let user_envs = resolve_env_for_dev(&app.env, project_dir);
        let envs = merge_spky_with_user_env(&spky_vars, user_envs);
        match dev_config {
            BackendDevConfig::Command(cmd) => {
                sink.push_verbose(&format!("Starting: {}", cmd));
                guards.push(spawn_prefixed(
                    Command::new("sh")
                        .args(["-c", cmd])
                        .current_dir(project_dir)
                        .envs(envs),
                    sink,
                ));
            }
            BackendDevConfig::Typed(BackendDevTypedConfig::Npm { script, workdir, .. }) => {
                let cwd = resolve_workdir(project_dir, workdir.as_deref());
                sink.push_verbose(&format!("Starting: pnpm run {}", script));
                guards.push(spawn_prefixed(
                    Command::new("pnpm")
                        .args(["run", script])
                        .current_dir(cwd)
                        .envs(envs),
                    sink,
                ));
            }
            BackendDevConfig::Typed(BackendDevTypedConfig::Docker {
                file,
                workdir,
                port,
                ports,
                platform,
            }) => {
                let cwd = resolve_workdir(project_dir, workdir.as_deref());
                let all_ports = merge_docker_ports(port.as_deref(), ports.as_deref());
                sink.push_verbose(&format!("Building: docker build -f {}", file));
                guards.push(spawn_docker_dev(
                    file,
                    &all_ports,
                    platform.as_deref(),
                    &envs,
                    &cwd,
                    name,
                    sink,
                    step,
                ));
            }
            BackendDevConfig::Typed(BackendDevTypedConfig::Uv { script, workdir, .. }) => {
                let cwd = resolve_workdir(project_dir, workdir.as_deref());
                sink.push_verbose(&format!("Starting: uv run {}", script));
                guards.push(spawn_prefixed(
                    Command::new("uv")
                        .args(["run", script])
                        .current_dir(cwd)
                        .envs(envs),
                    sink,
                ));
            }
        }
    }
    guards
}

/// Start each `type: docker` app (scope all or devOnly) by running its prebuilt
/// image in the foreground: `docker run --rm --name sp00ky-dev-<key> --network
/// <net> [-p <publish>]… [-e K=V]… <image> <args…>`. The returned LogTailGuard
/// SIGKILLs the `docker run` client on Ctrl-C, but that neither stops the
/// container nor triggers `--rm`; teardown of these containers happens in
/// `cleanup_direct`, which sweeps every `sp00ky-dev-*` container.
fn spawn_docker_app_devs(
    config: &Sp00kyConfig,
    project_dir: &Path,
    resolved_surreal: &ResolvedSurrealDb,
    mode: &DeployMode,
    step: &ui::Step,
) -> Vec<LogTailGuard> {
    use std::collections::BTreeMap;
    let spky_vars = build_spky_dev_vars(resolved_surreal, mode);
    let mut guards = Vec::new();
    let mut color_idx = 0;

    // Dev-runnable docker apps, started in `dependsOn` order (validated acyclic).
    let apps: BTreeMap<&str, &backend::AppConfig> = config
        .docker_apps()
        .filter(|(_, a)| a.runs_in_dev())
        .collect();

    for name in topo_order_docker(&apps) {
        let app = apps[name];
        let image = match &app.image {
            Some(i) => i,
            None => continue, // validation already requires it; defensive
        };

        // Gate on each dependency being ready before we start this app.
        for dep in &app.depends_on {
            if let Some(dep_app) = apps.get(dep.as_str()) {
                step.set_message(format!("waiting for {} (needed by {})…", dep, name));
                wait_for_ready(dep, dep_app);
            }
        }
        step.set_message(format!("starting {} (docker)…", name));

        let sink = LineSink::new(
            &format!("app.{}.docker", name),
            ui::style().app_cycle(color_idx),
            StreamKind::App,
        );
        color_idx += 1;
        let container = format!("sp00ky-dev-{}", name);

        // Clear any stale container from a previous (hard-killed) run.
        let _ = docker(&["rm", "-f", &container]);

        let user_envs = resolve_env_for_dev(&app.env, project_dir);
        let envs = merge_spky_with_user_env(&spky_vars, user_envs);

        // Build the `docker run` argv (owned strings; passed as -e so they reach
        // the container, not the docker process).
        let mut args: Vec<String> = vec![
            "run".into(),
            "--rm".into(),
            "--name".into(),
            container.clone(),
            "--network".into(),
            NETWORK_NAME.into(),
            "--network-alias".into(),
            name.to_string(),
        ];
        for p in &app.ports {
            args.push("-p".into());
            args.push(p.publish());
        }
        for vol in &app.volumes {
            args.push("-v".into());
            args.push(expand_project_dir(vol, project_dir));
        }
        if let Some(wd) = &app.workdir {
            args.push("-w".into());
            args.push(wd.clone()); // container path — no expansion
        }
        for (k, v) in &envs {
            args.push("-e".into());
            args.push(format!("{}={}", k, expand_project_dir(v, project_dir)));
        }
        args.push(image.clone());
        for a in &app.args {
            args.push(a.clone());
        }

        sink.push_verbose(&format!("Starting: docker run {} {}", image, app.args.join(" ")));
        guards.push(spawn_prefixed(Command::new("docker").args(&args), sink));
    }
    guards
}

/// Expand `${PROJECT_DIR}` (abs dir of sp00ky.yml) in a docker volume/env value.
fn expand_project_dir(s: &str, project_dir: &Path) -> String {
    s.replace("${PROJECT_DIR}", &project_dir.display().to_string())
}

/// Order dev docker apps so every app comes after its `dependsOn` deps (Kahn).
/// The graph is validated acyclic at config load, so all nodes are emitted.
fn topo_order_docker<'a>(
    apps: &std::collections::BTreeMap<&'a str, &'a backend::AppConfig>,
) -> Vec<&'a str> {
    use std::collections::{BTreeMap, VecDeque};
    let mut indegree: BTreeMap<&str, usize> = apps.keys().map(|n| (*n, 0usize)).collect();
    let mut edges: BTreeMap<&str, Vec<&str>> = BTreeMap::new();
    for (name, app) in apps {
        for dep in &app.depends_on {
            if apps.contains_key(dep.as_str()) {
                edges.entry(dep.as_str()).or_default().push(name);
                *indegree.get_mut(*name).unwrap() += 1;
            }
        }
    }
    let mut q: VecDeque<&str> = indegree
        .iter()
        .filter(|(_, d)| **d == 0)
        .map(|(n, _)| *n)
        .collect();
    let mut order = Vec::new();
    while let Some(n) = q.pop_front() {
        order.push(n);
        for &m in edges.get(n).map(|v| v.as_slice()).unwrap_or(&[]) {
            let d = indegree.get_mut(m).unwrap();
            *d -= 1;
            if *d == 0 {
                q.push_back(m);
            }
        }
    }
    // Defensive (shouldn't happen — validated acyclic): append any stragglers.
    for n in apps.keys() {
        if !order.contains(n) {
            order.push(*n);
        }
    }
    order
}

/// Block until a dependency app is ready, or a ~60s timeout (then proceed —
/// dependents self-heal on reconnect). Ready = healthcheck HTTP 200 (probed on
/// the app's first published host port) if set, else the container is running.
fn wait_for_ready(name: &str, app: &backend::AppConfig) {
    let container = format!("sp00ky-dev-{}", name);
    for _ in 0..60 {
        let ready = if let Some(path) = &app.healthcheck {
            match app.ports.first().and_then(|p| p.host_port()) {
                Some(port) => {
                    let url = format!("http://localhost:{}{}", port, path);
                    Command::new("curl")
                        .args(["-fsS", "-m", "2", "-o", "/dev/null", &url])
                        .status()
                        .map(|s| s.success())
                        .unwrap_or(false)
                }
                None => true, // healthcheck declared but no port to probe — don't block
            }
        } else {
            Command::new("docker")
                .args(["inspect", "-f", "{{.State.Running}}", &container])
                .output()
                .map(|o| String::from_utf8_lossy(&o.stdout).trim() == "true")
                .unwrap_or(false)
        };
        if ready {
            return;
        }
        std::thread::sleep(std::time::Duration::from_secs(1));
    }
    ui::warn(format!(
        "dependency '{}' not ready after timeout; starting dependents anyway",
        name
    ));
}

fn resolve_workdir(project_dir: &Path, workdir: Option<&str>) -> std::path::PathBuf {
    match workdir {
        Some(dir) => project_dir.join(dir),
        None => project_dir.to_path_buf(),
    }
}

/// Parse a dotenv-style file into key-value pairs.
/// Resolves the path relative to `base_dir`. Skips blank lines and `#` comments.
pub fn load_dotenv_file(path: &Path) -> Vec<(String, String)> {
    let content = match std::fs::read_to_string(path) {
        Ok(c) => c,
        Err(e) => {
            ui::warn(format!("Could not read env-file {:?}: {}", path, e));
            return Vec::new();
        }
    };
    content
        .lines()
        .filter(|l| {
            let trimmed = l.trim();
            !trimmed.is_empty() && !trimmed.starts_with('#')
        })
        .filter_map(|l| {
            let (key, value) = l.split_once('=')?;
            Some((key.trim().to_string(), value.trim().to_string()))
        })
        .collect()
}

/// Load all vault env vars for dev via the Cloud API directly.
fn load_dev_vault_envs() -> Vec<(String, String)> {
    crate::cloud::load_vault_envs_for_dev()
}

/// Resolve an EnvSource into key-value pairs for dev mode.
fn resolve_env_source(source: &backend::EnvSource, project_dir: &Path) -> Vec<(String, String)> {
    match source {
        backend::EnvSource::Str(s) if s == "vault" => load_dev_vault_envs(),
        backend::EnvSource::Vault(whitelist) => {
            let all = load_dev_vault_envs();
            all.into_iter()
                .filter(|(k, _)| whitelist.iter().any(|w| w == k))
                .collect()
        }
        backend::EnvSource::Str(file_path) => {
            let path = project_dir.join(file_path);
            let envs = load_dotenv_file(&path);
            if !envs.is_empty() {
                ui::detail(format!("loaded env-file: {}", path.display()));
            }
            envs
        }
        backend::EnvSource::Map(map) => map
            .iter()
            .map(|(k, v)| {
                let val = match v {
                    serde_yaml::Value::String(s) => s.clone(),
                    serde_yaml::Value::Number(n) => n.to_string(),
                    serde_yaml::Value::Bool(b) => b.to_string(),
                    other => serde_yaml::to_string(other)
                        .unwrap_or_default()
                        .trim()
                        .to_string(),
                };
                (k.clone(), val)
            })
            .collect(),
        // Scoped list item: dev mode resolves the dev side only.
        backend::EnvSource::PerEnvironment { dev, .. } => match dev {
            Some(entry) => resolve_env_entry(entry, project_dir),
            None => Vec::new(),
        },
    }
}

/// Resolve an EnvEntry (single source or list) into key-value pairs.
fn resolve_env_entry(entry: &backend::EnvEntry, project_dir: &Path) -> Vec<(String, String)> {
    match entry {
        backend::EnvEntry::Source(source) => resolve_env_source(source, project_dir),
        backend::EnvEntry::List(sources) => {
            let mut merged = std::collections::BTreeMap::new();
            for source in sources {
                for (k, v) in resolve_env_source(source, project_dir) {
                    merged.insert(k, v);
                }
            }
            merged.into_iter().collect()
        }
    }
}

/// Resolve the full EnvConfig for dev mode, merging sources in order.
pub fn resolve_env_for_dev(
    env: &Option<backend::EnvConfig>,
    project_dir: &Path,
) -> Vec<(String, String)> {
    let env = match env {
        Some(e) => e,
        None => return Vec::new(),
    };
    match env {
        backend::EnvConfig::Source(source) => resolve_env_source(source, project_dir),
        backend::EnvConfig::List(sources) => {
            let mut merged = std::collections::BTreeMap::new();
            for source in sources {
                for (k, v) in resolve_env_source(source, project_dir) {
                    merged.insert(k, v);
                }
            }
            merged.into_iter().collect()
        }
        backend::EnvConfig::PerEnvironment { dev, .. } => match dev {
            Some(entry) => resolve_env_entry(entry, project_dir),
            None => Vec::new(),
        },
    }
}

/// Spawn a command with its stdout/stderr routed line-by-line through `sink`,
/// which prefixes, filters (infra streams) and prints without tearing the
/// spinner region.
fn spawn_prefixed(cmd: &mut Command, sink: Arc<LineSink>) -> LogTailGuard {
    let child = cmd
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn();

    match child {
        Ok(mut c) => {
            if let Some(stdout) = c.stdout.take() {
                let sink = sink.clone();
                thread::spawn(move || {
                    let reader = BufReader::new(stdout);
                    for line in reader.lines() {
                        match line {
                            Ok(l) => sink.push(&l, false),
                            Err(_) => break,
                        }
                    }
                });
            }
            if let Some(stderr) = c.stderr.take() {
                let sink = sink.clone();
                thread::spawn(move || {
                    let reader = BufReader::new(stderr);
                    for line in reader.lines() {
                        match line {
                            Ok(l) => sink.push(&l, true),
                            Err(_) => break,
                        }
                    }
                });
            }
            LogTailGuard {
                child: Some(c),
                sink: Some(sink),
            }
        }
        Err(e) => {
            ui::warn(format!("Could not start process: {}", e));
            LogTailGuard::none()
        }
    }
}

/// Merge the single `port` field and the `ports` array into one host:container
/// list, so a backend that listens on more than one port can be published.
fn merge_docker_ports(port: Option<&str>, ports: Option<&[String]>) -> Vec<String> {
    let mut out = Vec::new();
    if let Some(p) = port {
        out.push(p.to_string());
    }
    if let Some(ps) = ports {
        out.extend(ps.iter().cloned());
    }
    out
}

#[allow(clippy::too_many_arguments)]
fn spawn_docker_dev(
    file: &str,
    ports: &[String],
    platform: Option<&str>,
    envs: &[(String, String)],
    cwd: &Path,
    name: &str,
    sink: Arc<LineSink>,
    step: &ui::Step,
) -> LogTailGuard {
    let tag = format!("sp00ky-dev-{}", name);
    let container_name = format!("sp00ky-dev-app-{}", name);
    step.set_message(format!("building {} (docker build)…", name));

    // Build the image (blocking, with prefixed output). `--platform` (when set)
    // builds for the host arch so the image's toolchain runs natively instead
    // of under QEMU emulation of the deploy arch.
    let mut build_args: Vec<String> = vec!["build".into()];
    if let Some(p) = platform {
        build_args.push("--platform".into());
        build_args.push(p.to_string());
    }
    build_args.extend(["-f".into(), file.to_string(), "-t".into(), tag.clone(), ".".into()]);
    let build_result = Command::new("docker")
        .args(&build_args)
        .current_dir(cwd)
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .output();

    match build_result {
        Ok(output) => {
            for line in String::from_utf8_lossy(&output.stdout).lines() {
                sink.push(line, false);
            }
            for line in String::from_utf8_lossy(&output.stderr).lines() {
                sink.push(line, true);
            }
            if !output.status.success() {
                ui::warn(format!("docker build for '{}' exited with {}", name, output.status));
                return LogTailGuard::none();
            }
        }
        Err(e) => {
            ui::warn(format!("Could not run docker build for '{}': {}", name, e));
            return LogTailGuard::none();
        }
    }

    // Remove any stale container with the same name
    let _ = Command::new("docker")
        .args(["rm", "-f", &container_name])
        .output();

    // Run the container
    let mut args = vec![
        "run".to_string(),
        "--rm".to_string(),
        "--name".to_string(),
        container_name,
        "--network".to_string(),
        NETWORK_NAME.to_string(),
    ];

    if let Some(p) = platform {
        args.push("--platform".to_string());
        args.push(p.to_string());
    }

    // Publish every requested host:container port (a backend may listen on
    // more than one, e.g. REST + gRPC).
    for p in ports {
        args.push("-p".to_string());
        args.push(p.clone());
    }

    // Pass resolved env vars as -e flags
    for (k, v) in envs {
        args.push("-e".to_string());
        args.push(format!("{}={}", k, v));
    }

    args.push(tag);

    spawn_prefixed(Command::new("docker").args(&args).current_dir(cwd), sink)
}

/// Follow a container's logs through an infra sink. `--tail 0`: no backlog
/// dump on attach; the sink filters to WARN+ unless `--verbose`.
fn spawn_log_tail(container: &str, label: &str) -> LogTailGuard {
    let sink = LineSink::new(label, ui::style().infra(label), StreamKind::Infra);
    spawn_prefixed(
        Command::new("docker").args(["logs", "-f", "--tail", "0", container]),
        sink,
    )
}


#[cfg(test)]
mod job_config_tests {
    use super::*;

    fn cfg(run_on: &str) -> Sp00kyConfig {
        serde_yaml::from_str(&format!(
            "
pools:
  render: {{ min: 1 }}
machines:
  api-box: {{}}
apps:
  api:
    type: backend
    runOn: {run_on}
    baseUrl: http://127.0.0.1:8080
    deploy: {{ port: 8080, healthcheck: /health, cmd: /api }}
    method: {{ type: outbox, table: job, schema: ./x.surql }}
"
        ))
        .unwrap()
    }

    /// A dedicated machine backend is an ordinary backend to the dev SSP: its
    /// jobs are dispatched to its base URL. A pool backend's are not (the
    /// scheduler's pool engine runs them).
    #[test]
    fn a_machine_backend_stays_in_the_dev_job_config() {
        let on_machine = build_job_config_json(&cfg("{ machine: api-box }"), true);
        assert!(
            on_machine.contains("host.docker.internal:8080"),
            "{on_machine}"
        );
        let on_pool = build_job_config_json(&cfg("{ pool: render }"), true);
        assert_eq!(on_pool, "[]");
    }
}
