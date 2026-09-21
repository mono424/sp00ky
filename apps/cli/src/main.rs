mod add_api;
mod admin;
mod agents;
mod annotations;
mod backend;
mod bucket;
mod cloud;
mod codegen;
mod create;
mod dev;
mod doctor;
mod flag;
mod jobs;
mod json_schema;
mod logs_browser;
mod mcp;
mod mcp_cloud;
mod migrate;
mod migration;
mod modules;
mod package_manager;
mod parser;
mod pool_config;
mod pool_sync;
mod port_check;
mod query;
mod scaffold;
mod dag_render;
mod schedules;
mod workflows;
mod schedule_config;
mod schedule_sync;
mod schema_builder;
mod schema_diff;
mod schema_extract;
mod sp00ky;
mod stats_tui;
mod surreal_client;
mod ui;
mod verify;

use anyhow::{Context, Result};
use backend::{BackendProcessor, DeployMode, Sp00kyConfig, DEFAULT_CONFIG_PATH};
use clap::{Args as ClapArgs, Parser as ClapParser, Subcommand};
use codegen::{CodeGenerator, OutputFormat};
use create::create_project;
use json_schema::JsonSchemaGenerator;
use parser::SchemaParser;
use std::fs;
use std::path::{Path, PathBuf};

#[derive(ClapParser, Debug)]
#[command(name = "spky")]
#[command(about = "Develop, generate types for, and deploy realtime SurrealDB apps", long_about = None)]
#[command(version, disable_version_flag = true)]
struct Args {
    #[arg(long = "version", short = 'v', action = clap::ArgAction::Version)]
    version: (),

    /// Path to the project directory (defaults to the current directory)
    #[arg(long, global = true)]
    path: Option<PathBuf>,

    /// Answer confirmation prompts without asking, so the command never waits on
    /// a keypress. Consent questions ("apply to PRODUCTION?") are answered yes;
    /// preference questions ("set up billing now?") take their default. Works on
    /// every subcommand, and `SPKY_YES=1` does the same from the environment.
    ///
    /// Does NOT cover the two irreversible operations - destroying a project and
    /// resetting its database - which take `--confirm <project>` instead.
    ///
    /// This is what an agent or a CI job should pass: a pseudo-terminal makes a
    /// prompt render and then wait forever for input nothing can send.
    #[arg(long = "yes", short = 'y', alias = "y", global = true)]
    yes: bool,

    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Print version information
    Version,
    /// Scaffold a new Sp00ky project in the current directory
    Init,
    /// Database migration management
    Migrate {
        #[command(subcommand)]
        action: MigrateCommands,
    },
    /// Bucket management
    Bucket {
        #[command(subcommand)]
        action: BucketCommands,
    },
    /// API backend management
    Api {
        #[command(subcommand)]
        action: ApiCommands,
    },
    /// Start a local development environment
    Dev {
        /// Skip migration check entirely
        #[arg(long)]
        skip_migrations: bool,
        /// Auto-apply pending migrations without prompting
        #[arg(long)]
        apply_migrations: bool,
        /// Update stored checksums for modified-but-applied migration files before applying
        #[arg(long)]
        fix_checksums: bool,
        /// Wipe SSP and scheduler persistent state before starting so they
        /// re-bootstrap from the upstream SurrealDB. Does NOT touch the
        /// SurrealDB volume — user data is preserved.
        #[arg(long)]
        clean: bool,
        /// Wipe the SurrealDB volume too — start with a completely empty
        /// database. Implies `--clean` since the SSP/scheduler caches would
        /// otherwise rebootstrap inconsistent state. Pair with this for a
        /// full reset; use `--clean` alone if you only need to recover from
        /// SSP/scheduler corruption.
        #[arg(long)]
        clean_db: bool,
        /// Show every log line after startup: SurrealDB / SSP / scheduler
        /// (compact-formatted) and your apps' dev servers. By default only
        /// errors and crash traces are shown.
        #[arg(long, short = 'v', env = "SPKY_VERBOSE")]
        verbose: bool,
    },
    /// Verify the SSP/scheduler snapshot matches the upstream SurrealDB
    Verify {
        /// On mismatch, POST /admin/ssp/resync-all to force every SSP to
        /// re-bootstrap from the scheduler's frozen snapshot.
        #[arg(long)]
        fix: bool,
    },
    /// Diagnose project health: config validity, schema/codegen freshness,
    /// migration state. Designed as the agent feedback loop after a schema
    /// edit. Use `--json` for an LLM-readable structured contract.
    Doctor {
        /// Emit results as JSON instead of human-readable text.
        #[arg(long)]
        json: bool,
    },
    /// Render a cookbook recipe (live-list, optimistic-mutation, crdt-text-field).
    /// Pass `list` to print the recipe index. Recipes are parameterized by your
    /// table (and field, for CRDT recipes).
    Recipe {
        /// Recipe name. Use `spky recipe list` to see all available recipes.
        recipe: String,
        /// Target table name. Substituted into the template (e.g., `thread`).
        #[arg(long)]
        table: Option<String>,
        /// Target field name (required by recipes that touch a single column).
        #[arg(long)]
        field: Option<String>,
        /// Write the rendered snippet to this path instead of stdout.
        #[arg(long)]
        out: Option<PathBuf>,
    },
    /// Manage AGENTS.md — the LLM-readable project guide.
    Agents {
        #[command(subcommand)]
        action: AgentsCommands,
    },
    /// Generate client types from sp00ky.yml, or a single .surql file via --input/--output
    #[command(visible_alias = "gen")]
    Generate {
        #[command(flatten)]
        args: GenerateArgs,
    },
    /// Validate sp00ky.yml configuration
    Lint {
        /// Path to sp00ky.yml config file
        #[arg(short, long)]
        config: Option<PathBuf>,
    },
    /// MCP server tooling: local dev server, cloud tokens, and editor setup
    Mcp {
        #[command(subcommand)]
        action: Option<McpCommands>,
    },
    // ── Deploy & operate (act on the current project) ──────────────────────
    /// Deploy the current project to Sp00ky Cloud
    Deploy {
        /// Also upgrade SSP and scheduler to latest version
        #[arg(long)]
        upgrade: bool,
        /// Wipe the scheduler's persistent volume before redeploy. Use
        /// after scheduler state corruption. Does NOT touch the user's
        /// SurrealDB data.
        #[arg(long)]
        clean: bool,
        /// Deploy only a subset of apps (comma-separated names from sp00ky.yml,
        /// e.g. `--only api,web`). Builds/uploads just those apps; every other
        /// running service is left untouched. Omit to deploy everything.
        #[arg(long, value_delimiter = ',')]
        only: Vec<String>,
        /// Force re-applying the internal schema + remote functions even when
        /// unchanged (bypasses the schema-hash skip). Use for drift recovery.
        #[arg(long)]
        force_schema: bool,
        /// Mark this deploy's frontend release rows with cache_bust = true:
        /// clients reloading onto the new version clear service-worker caches
        /// first. Use when a previous deploy is wedged behind stale caches.
        #[arg(long)]
        cache_bust: bool,
        /// Mark this deploy's frontend release rows with mandatory = true:
        /// clients reload onto the new version immediately instead of being
        /// offered a notification. Broken-build kill-switch.
        #[arg(long)]
        mandatory: bool,
    },
    /// Announce an app release to running clients without deploying: updates
    /// the app's `_00_app_release` row (same write `spky deploy` does for
    /// frontends automatically). Clients using the SDK's app-release hook see
    /// the new version live and offer/force a reload.
    Release {
        /// App name from sp00ky.yml (e.g. `web`)
        app: String,
        /// Version to announce (X.Y.Z). Defaults to the frontend's
        /// package.json version.
        #[arg(long)]
        version: Option<String>,
        /// Ask clients to clear service-worker caches when reloading onto
        /// this version.
        #[arg(long)]
        cache_bust: bool,
        /// Force clients to reload/update immediately instead of offering a
        /// notification.
        #[arg(long)]
        mandatory: bool,
    },
    /// Show deployment status
    Status,
    /// Live resource graphs (CPU, memory, disk & network) for the cloud deployment
    Stats {
        /// History window to backfill, e.g. 15m, 1h, 24h (default 15m)
        #[arg(long)]
        window: Option<String>,
        /// Only show specific services (comma-separated roles or app names,
        /// e.g. --filter backend,surrealdb or --filter api)
        #[arg(long, value_delimiter = ',')]
        filter: Option<Vec<String>>,
    },
    /// Tail or browse logs from the cloud deployment
    Logs {
        /// Filter by service(s): surrealdb, scheduler, ssp, backend, frontend.
        /// Supports blueprints: "spooky" = ssp+scheduler.
        /// Comma-separated: --filter spooky,surrealdb
        #[arg(long)]
        filter: Option<String>,

        /// Split view: "h" (horizontal/stacked) or "v" (vertical/side-by-side)
        #[arg(long)]
        split: Option<String>,

        /// Replay history starting at this point, then keep tailing live.
        /// Accepts a duration (e.g. `2h`, `3d`, `1h30m`) or an RFC-3339
        /// timestamp. Matches `kubectl logs --since` — the stream stays open
        /// unless `--until` bounds it.
        #[arg(long)]
        since: Option<String>,

        /// End of the log window. Same format as `--since`. Setting this
        /// turns the command into a one-shot: the stream closes when the
        /// window is exhausted instead of continuing to tail.
        #[arg(long)]
        until: Option<String>,

        /// Server-side regex match on the log message. Works for both
        /// historical and live-tail modes (when the backend supports it).
        #[arg(long)]
        grep: Option<String>,

        /// Open an interactive TUI browser instead of streaming to stdout.
        /// Supports scroll, search, service + time filters, and follow mode.
        #[arg(short, long)]
        interactive: bool,

        /// Start in follow mode (TUI only): auto-scroll on new entries as
        /// they arrive. Without this the TUI opens paused at the newest
        /// entry; toggle live with `f` inside the TUI.
        #[arg(short = 'F', long)]
        follow: bool,

        /// (Deprecated) Filter by single service
        #[arg(long, hide = true)]
        service: Option<String>,
    },
    /// Restart deployment containers. With no target: the scheduler and SSPs,
    /// leaving backends and frontends untouched.
    ///
    /// Targets are roles (`db`/`surrealdb`, `scheduler`, `ssp`, `frontend`) or
    /// the name of a single app from sp00ky.yml:
    ///
    ///   spky restart               # scheduler + SSPs
    ///   spky restart db            # SurrealDB only
    ///   spky restart gameanalysis  # one backend
    ///   spky restart ssp scheduler # several roles
    #[command(verbatim_doc_comment)]
    Restart {
        /// Roles and/or app names to restart. Omit for scheduler + SSPs.
        #[arg(value_name = "TARGET")]
        targets: Vec<String>,
        /// Restart every backend app.
        #[arg(long)]
        all_backends: bool,
        /// Also wipe the scheduler's persistent volume. Use after
        /// scheduler state corruption. Does NOT touch SurrealDB data.
        #[arg(long)]
        clean: bool,
        /// Pull the latest scheduler/SSP base images before restarting.
        #[arg(long)]
        upgrade: bool,
        /// Also restart the SurrealDB container. This is a process restart,
        /// not a wipe: data on the volume is preserved, but the whole
        /// deployment is briefly unavailable while SurrealDB comes back up.
        /// Equivalent to adding `db` to the targets.
        #[arg(long, visible_alias = "db")]
        surreal: bool,
    },
    /// Push the current schema to a free (Cloudflare) project's SSP node.
    /// Builds the server schema locally and sends it to the control plane,
    /// which applies it to the project's database and reloads the node. For
    /// paid plans, schema is applied during `deploy` instead.
    Push,
    /// Scale a deployment component (e.g. `spky scale ssp 3`)
    Scale {
        #[command(subcommand)]
        action: ScaleCommands,
    },
    /// Authenticate with Sp00ky Cloud (opens browser)
    Login,
    /// Clear stored Sp00ky Cloud credentials
    Logout,

    // ── Resources ───────────────────────────────────────────────────────────
    /// Manage encrypted environment variables and the vault that protects them
    Env {
        #[command(subcommand)]
        action: EnvCommands,
    },
    /// Connect custom domains to your project
    Domain {
        #[command(subcommand)]
        action: CloudDomainCommands,
    },
    /// Post a status notice to the project's public uptime page
    #[command(args_conflicts_with_subcommands = true)]
    Notice {
        /// The notice message to publish (e.g. "We are investigating elevated error rates").
        /// A message that is literally "list" or "remove" is parsed as the subcommand instead.
        message: Option<String>,
        /// Notice type: investigating | identified | resolved | maintenance | update
        #[arg(long = "type", value_name = "TYPE", default_value = "update")]
        notice_type: String,
        /// How long the notice stays visible, e.g. 30m, 2h, 7d
        #[arg(long, default_value = "24h")]
        timeout: String,
        #[command(subcommand)]
        action: Option<NoticeCommands>,
    },
    /// Manage database backups
    Backup {
        #[command(subcommand)]
        action: CloudBackupCommands,
    },
    /// Link a GitHub repository for automated deployments
    Link {
        #[command(subcommand)]
        action: CloudLinkCommands,
    },
    /// Manage feature flags for the configured project
    Flag {
        #[command(subcommand)]
        action: FlagCommands,
    },
    /// Manage who may edit feature flags from the DevTools panel
    Admin {
        #[command(subcommand)]
        action: AdminCommands,
    },
    /// Inspect and control server-side schedules (`schedules:` in sp00ky.yml)
    Schedules {
        #[command(flatten)]
        conn: ConnectionArgs,
        /// Path to sp00ky.yml config file
        #[arg(long)]
        config: Option<PathBuf>,
        #[command(subcommand)]
        action: SchedulesCommands,
    },
    /// Inspect workflow DAG runs, with a live view
    Workflows {
        #[command(flatten)]
        conn: ConnectionArgs,
        /// Path to sp00ky.yml config file
        #[arg(long)]
        config: Option<PathBuf>,
        #[command(subcommand)]
        action: WorkflowsCommands,
    },
    /// Overview and control of background (outbox) jobs. Run without a
    /// subcommand to open the interactive dashboard.
    Jobs {
        #[command(flatten)]
        conn: ConnectionArgs,
        /// Path to sp00ky.yml config file
        #[arg(long)]
        config: Option<PathBuf>,
        #[command(subcommand)]
        action: Option<JobsCommands>,
    },
    /// Run SurrealQL against the database. With a positional query it runs once
    /// and prints the result; without one it opens an interactive REPL. Use
    /// `--cloud` to target the production deployment.
    Query {
        /// SurrealQL to execute. Omit to enter interactive (REPL) mode.
        query: Option<String>,
        /// Print raw pretty JSON instead of a table.
        #[arg(long)]
        json: bool,
        #[command(flatten)]
        conn: ConnectionArgs,
        /// Path to sp00ky.yml config file
        #[arg(long)]
        config: Option<PathBuf>,
    },

    // ── Account ─────────────────────────────────────────────────────────────
    /// Manage cloud projects (create, list, credentials, destroy)
    Project {
        #[command(subcommand)]
        action: ProjectCommands,
    },
    /// Manage team members and invitations
    Team {
        #[command(subcommand)]
        action: CloudTeamCommands,
    },
    /// Billing management (run without a subcommand to open the billing portal)
    Billing {
        #[command(subcommand)]
        action: Option<CloudBillingCommands>,
    },
    /// Manage API tokens for CI/CD authentication
    Token {
        #[command(subcommand)]
        action: CloudKeyCommands,
    },

    // ── Removed commands (kept only to print a migration hint) ───────────────
    #[command(hide = true, name = "cloud")]
    Cloud {
        #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
        rest: Vec<String>,
    },
    #[command(hide = true, name = "create")]
    CreateDeprecated,
    #[command(hide = true, name = "setup")]
    SetupDeprecated,
    #[command(hide = true, name = "scaffold")]
    ScaffoldDeprecated {
        #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
        rest: Vec<String>,
    },
}

#[derive(Subcommand, Debug)]
enum AgentsCommands {
    /// Generate AGENTS.md in the project root, parameterized by the parsed schema.
    Init {
        /// Overwrite an existing AGENTS.md without prompting.
        #[arg(long)]
        force: bool,
        /// Write to this path instead of `<project>/AGENTS.md`.
        #[arg(long)]
        out: Option<PathBuf>,
    },
}

/// `spky admin` — the sp00ky operator roster (`_00_admin`).
///
/// Root-only by construction: the table denies create/update/delete to every
/// record token, so this command is the only way in or out. An admin may read
/// flag definitions and flip flags from the DevTools panel; creating and
/// deleting flags stays with `spky flag`.
#[derive(Subcommand, Debug)]
enum AdminCommands {
    /// List the current admins
    #[command(visible_alias = "ls")]
    List {
        #[command(flatten)]
        conn: ConnectionArgs,
        /// Path to sp00ky.yml config file
        #[arg(long)]
        config: Option<PathBuf>,
    },
    /// Grant a user admin rights over feature flags
    Add {
        /// Username or `user:xxx` record id
        user: String,
        /// Optional note (e.g. why, or who asked)
        #[arg(long)]
        note: Option<String>,
        #[command(flatten)]
        conn: ConnectionArgs,
        /// Path to sp00ky.yml config file
        #[arg(long)]
        config: Option<PathBuf>,
    },
    /// Revoke a user's admin rights
    Remove {
        /// Username or `user:xxx` record id
        user: String,
        #[command(flatten)]
        conn: ConnectionArgs,
        /// Path to sp00ky.yml config file
        #[arg(long)]
        config: Option<PathBuf>,
    },
}

#[derive(Subcommand, Debug)]
enum FlagCommands {
    /// List all feature flag definitions
    List {
        #[command(flatten)]
        conn: ConnectionArgs,
        /// Path to sp00ky.yml config file
        #[arg(long)]
        config: Option<PathBuf>,
    },
    /// Create a new feature flag definition (default: variants `off`, `on`)
    Create {
        /// Flag key (lowercase, alphanumeric + dashes)
        key: String,
        /// Comma-separated variant list (default: `off,on`)
        #[arg(long)]
        variants: Option<String>,
        /// Default variant (default: `off`)
        #[arg(long)]
        default: Option<String>,
        /// Human-readable description
        #[arg(long)]
        description: Option<String>,
        #[command(flatten)]
        conn: ConnectionArgs,
        /// Path to sp00ky.yml config file
        #[arg(long)]
        config: Option<PathBuf>,
    },
    /// Delete a feature flag and all its user assignments
    Delete {
        /// Flag key
        key: String,
        #[command(flatten)]
        conn: ConnectionArgs,
        /// Path to sp00ky.yml config file
        #[arg(long)]
        config: Option<PathBuf>,
    },
    /// Show a flag's configuration, rules, and assignment count
    Get {
        /// Flag key
        key: String,
        #[command(flatten)]
        conn: ConnectionArgs,
        /// Path to sp00ky.yml config file
        #[arg(long)]
        config: Option<PathBuf>,
    },
    /// Globally enable a flag (sets `enabled = true`, re-evaluates assignments)
    Enable {
        /// Flag key
        key: String,
        #[command(flatten)]
        conn: ConnectionArgs,
        /// Path to sp00ky.yml config file
        #[arg(long)]
        config: Option<PathBuf>,
    },
    /// Globally disable a flag (sets `enabled = false`, re-evaluates assignments)
    Disable {
        /// Flag key
        key: String,
        #[command(flatten)]
        conn: ConnectionArgs,
        /// Path to sp00ky.yml config file
        #[arg(long)]
        config: Option<PathBuf>,
    },
    /// Add a targeting rule. Pass exactly one of --for-user / --rollout / --sql.
    Set {
        /// Flag key
        key: String,
        /// Variant the rule should resolve to (must be one of the flag's variants)
        #[arg(long)]
        variant: String,
        /// Username (or user record id) to allowlist
        #[arg(long = "for-user")]
        for_user: Option<String>,
        /// Percentage rollout: integer 0..=100
        #[arg(long)]
        rollout: Option<u32>,
        /// SurrealQL SELECT yielding user ids to allowlist; previews matches and
        /// the total count before applying (e.g. "SELECT id FROM user WHERE age > 18")
        #[arg(long)]
        sql: Option<String>,
        #[command(flatten)]
        conn: ConnectionArgs,
        /// Path to sp00ky.yml config file
        #[arg(long)]
        config: Option<PathBuf>,
    },
    /// Remove a user from a flag's allowlist
    Unset {
        /// Flag key
        key: String,
        /// Username (or user record id) to remove from the allowlist
        #[arg(long = "for-user")]
        for_user: String,
        #[command(flatten)]
        conn: ConnectionArgs,
        /// Path to sp00ky.yml config file
        #[arg(long)]
        config: Option<PathBuf>,
    },
    /// Dry-run an evaluation for a single user without writing anything
    Eval {
        /// Flag key
        key: String,
        /// Username (or user record id) to evaluate as
        #[arg(long = "as-user")]
        as_user: String,
        #[command(flatten)]
        conn: ConnectionArgs,
        /// Path to sp00ky.yml config file
        #[arg(long)]
        config: Option<PathBuf>,
    },
}

#[derive(Subcommand, Debug)]
enum SchedulesCommands {
    /// List deployed schedules with their cadence, state, and last outcome
    #[command(visible_alias = "ls")]
    List {
        /// Emit JSON instead of a table
        #[arg(long)]
        json: bool,
    },
    /// Show one schedule's definition, clock, and recent runs
    Get {
        /// Schedule name, as written in sp00ky.yml
        name: String,
        #[arg(long)]
        json: bool,
    },
    /// Stop future fires. Survives redeploys; runs already in flight finish
    Pause {
        name: String,
    },
    /// Resume a paused schedule, planning the next fire from now
    Resume {
        name: String,
    },
    /// Fire once now, without shifting the cadence
    Trigger {
        name: String,
    },
    /// Let a quarantined forEach key fire again
    Release {
        name: String,
        /// The key, exactly as `spky schedules get` reports it
        key: String,
    },
    /// Run history, newest first
    Runs {
        /// Limit to one schedule
        name: Option<String>,
        /// Filter by status (running, success, failed, skipped, replaced, killed)
        #[arg(long)]
        status: Option<String>,
        #[arg(long, default_value_t = 30)]
        limit: usize,
        #[arg(long)]
        json: bool,
    },
    /// Re-push definitions from sp00ky.yml without a full deploy
    Sync,
}

#[derive(Subcommand, Debug)]
enum WorkflowsCommands {
    /// List deployed workflows
    #[command(visible_alias = "ls")]
    List {
        #[arg(long)]
        json: bool,
    },
    /// Render a run's DAG (defaults to the newest run of a named workflow)
    Show {
        /// Workflow name or run id
        target: String,
        /// Force plain-ASCII output (the default when piped)
        #[arg(long)]
        ascii: bool,
        #[arg(long)]
        json: bool,
    },
    /// Live DAG view, updating as steps advance
    Watch {
        /// Workflow name or run id
        target: String,
    },
    /// Run history, newest first
    Runs {
        /// Limit to one workflow
        name: Option<String>,
        /// Filter by status (running, success, failed, killed)
        #[arg(long)]
        status: Option<String>,
        #[arg(long, default_value_t = 30)]
        limit: usize,
        #[arg(long)]
        json: bool,
    },
    /// Stop a run: in-flight steps are killed, the rest are skipped
    Kill {
        /// Workflow name or run id
        run: String,
    },
}

#[derive(Subcommand, Debug)]
enum JobsCommands {
    /// List jobs as a static table (scriptable; supports --json)
    #[command(visible_alias = "ls")]
    List {
        /// Filter by status (pending, processing, success, failed)
        #[arg(long)]
        status: Option<String>,
        /// Filter to a single job table
        #[arg(long)]
        table: Option<String>,
        /// Maximum rows to fetch per job table
        #[arg(long, default_value_t = 200)]
        limit: usize,
        /// Emit JSON instead of a table
        #[arg(long)]
        json: bool,
    },
    /// Show full detail (payload + error history) for one job
    Get {
        /// Job id, e.g. `job:abc123`
        id: String,
        /// Emit JSON instead of formatted fields
        #[arg(long)]
        json: bool,
    },
    /// Stop a job: cancel it if in-flight, or drop it if still queued
    Kill {
        /// Job id, e.g. `job:abc123`
        id: String,
    },
    /// Re-run a terminal (failed/success) job
    Retry {
        /// Job id, e.g. `job:abc123`
        id: String,
    },
    /// Delete terminal jobs (success/failed) from all job tables; use --all to also clear pending
    Clear {
        /// Also clear pending (queued) jobs. Running (processing) jobs are never deleted — use `jobs kill` for those
        #[arg(short = 'A', long)]
        all: bool,
    },
}

#[derive(ClapArgs, Debug)]
struct GenerateArgs {
    /// Path to the input .surql schema file (single-file mode)
    #[arg(short, long)]
    input: Option<PathBuf>,
    /// Path to the output file; the extension picks the format (.json, .ts, .dart)
    #[arg(short, long)]
    output: Option<PathBuf>,
    /// Path to the sp00ky.yml config file (config-driven mode; the default)
    #[arg(short, long)]
    config: Option<PathBuf>,
    /// Output format (json, typescript, dart, surql); inferred from --output if omitted
    #[arg(short, long)]
    format: Option<String>,
    /// Also generate TypeScript and Dart alongside JSON Schema (single-file mode)
    #[arg(short, long, default_value_t = false)]
    all: bool,
    /// Disable the generated file comment header (enabled by default)
    #[arg(long = "no-header", default_value_t = false)]
    no_header: bool,
    /// Append another .surql file to the input (single-file mode)
    #[arg(long)]
    append: Option<PathBuf>,
    /// Directory containing Surrealism modules to compile and bundle
    #[arg(long, default_value = "../../packages/surrealism-modules")]
    modules_dir: PathBuf,
    /// Generation mode: "singlenode", "cluster", or "surrealism"
    #[arg(long, default_value = "singlenode")]
    mode: String,
    /// SSP/Scheduler endpoint URL (singlenode/cluster modes)
    #[arg(long)]
    endpoint: Option<String>,
    /// SSP/Scheduler auth secret (singlenode/cluster modes)
    #[arg(long)]
    secret: Option<String>,
}

#[derive(Subcommand, Debug)]
enum ScaleCommands {
    /// Scale the number of SSP instances (e.g. `spky scale ssp 3`)
    Ssp {
        /// Desired number of SSP instances
        count: u32,
    },
}

#[derive(Subcommand, Debug)]
enum ProjectCommands {
    /// Create a new cloud project
    Create {
        /// Project slug (lowercase, alphanumeric + hyphens)
        #[arg(long)]
        slug: Option<String>,
        /// Plan: starter, pro, business
        #[arg(long, default_value = "starter")]
        plan: String,
    },
    /// List cloud projects
    #[command(visible_alias = "ls")]
    List,
    /// Print SurrealDB root credentials for the current project
    Credentials {
        /// Print only the password (no URL / username label) — handy for piping
        /// into other tools (e.g. `export SURREAL_PASS=$(spky project credentials --raw)`).
        #[arg(long)]
        raw: bool,
    },
    /// Destroy the cloud project and all its VMs
    Destroy {
        /// The project's slug, typed out. Required wherever there is no terminal
        /// to ask on: this cannot be undone, so `--yes` deliberately does not
        /// cover it.
        #[arg(long, value_name = "PROJECT")]
        confirm: Option<String>,
    },
}

#[derive(Subcommand, Debug)]
pub enum EnvCommands {
    /// Set an environment variable (prompts for the value, or reads it from a file)
    Set {
        /// Variable name (e.g. DATABASE_URL)
        name: Option<String>,
        /// Read the value from a file instead of the interactive prompt. Useful
        /// for multi-line values like PEM keys. The variable name is required.
        #[arg(long, value_name = "PATH")]
        file: Option<String>,
        /// With --file: set the Development value only (default: both).
        #[arg(long)]
        dev: bool,
        /// With --file: set the Production value only (default: both).
        #[arg(long)]
        prod: bool,
    },
    /// List all environment variable names
    #[command(visible_alias = "ls")]
    List,
    /// Delete an environment variable
    #[command(visible_alias = "delete")]
    Rm {
        /// Variable name to delete
        name: String,
    },
    /// Load and export environment variables to stdout
    Pull {
        /// Load production values instead of development
        #[arg(long)]
        prod: bool,
    },
    /// Import environment variables from a .env file
    Import {
        /// Path to the .env file (defaults to .env in the current directory)
        #[arg(default_value = ".env")]
        file: String,
    },
    /// Initialize or unlock the encryption vault (set a vault passphrase)
    Unlock,
    /// Change your vault passphrase
    Passphrase,
    /// Enable, disable, or check CI/CD vault access for push deploys.
    ///
    /// When enabled, `git push` deploys can read production secrets from the
    /// vault (referenced in sp00ky.yml via `env: { cloud: { vault: [...] } }`).
    /// Requires your vault passphrase. Off by default.
    ShareCi {
        /// Revoke CI/CD vault access
        #[arg(long)]
        disable: bool,
        /// Show whether CI/CD vault access is enabled
        #[arg(long)]
        status: bool,
    },
    /// Manage vault passphrase resets (for a forgotten passphrase)
    Reset {
        #[command(subcommand)]
        action: EnvResetCommands,
    },
}

#[derive(Subcommand, Debug)]
pub enum EnvResetCommands {
    /// Request a vault passphrase reset (if you forgot your passphrase)
    Request,
    /// Approve a pending vault reset request (admin only)
    Approve {
        /// Email of the member whose reset to approve
        email: String,
    },
    /// Complete your vault passphrase reset (set a new passphrase after admin approval)
    Complete,
    /// List pending vault reset requests (admin only)
    List,
}

#[derive(Subcommand, Debug)]
pub enum CloudDomainCommands {
    /// Connect a custom domain (e.g. app.example.com) to an app
    Add {
        /// The custom domain to connect
        domain: String,
        /// Which app to serve (frontend or exposed backend name); defaults to the primary frontend
        #[arg(long)]
        app: Option<String>,
        /// Attach the domain to the project's public status page instead of an app (Pro)
        #[arg(long)]
        status: bool,
    },
    /// List connected custom domains and their status
    List,
    /// Disconnect a custom domain
    Remove {
        /// The custom domain to disconnect
        domain: String,
    },
}

#[derive(Subcommand, Debug)]
pub enum NoticeCommands {
    /// List active notices
    List,
    /// Remove a notice by id
    Remove {
        /// Notice id (from `spky notice list`)
        id: String,
    },
}

#[derive(Subcommand, Debug)]
pub enum CloudLinkCommands {
    /// Connect a GitHub repo for automated deployments
    #[command(name = "connect")]
    Setup,
    /// Show link configuration and recent runs
    Status,
    /// Change link settings (branch, auto-deploy, manifest path)
    Settings {
        /// Branch to deploy from
        #[arg(long)]
        branch: Option<String>,
        /// Enable or disable auto-deploy on push
        #[arg(long)]
        auto_deploy: Option<bool>,
        /// Path to sp00ky.yml within the repo (e.g. packages/schema/sp00ky.yml)
        #[arg(long)]
        config_path: Option<String>,
    },
    /// Disconnect the GitHub repo
    #[command(name = "disconnect")]
    Unlink,
    /// Manually trigger a deployment from the linked repo
    Trigger,
    /// List recent build runs
    Runs,
}

#[derive(Subcommand, Debug)]
pub enum CloudBackupCommands {
    /// List backups
    List,
    /// Create a manual backup
    Create {
        /// Optional backup name
        #[arg(long)]
        name: Option<String>,
    },
    /// Restore database from a backup
    Restore {
        /// Backup ID, full name, or unique id prefix
        backup_id: String,
    },
    /// Delete a backup
    Delete {
        /// Backup ID, full name, or unique id prefix
        backup_id: String,
    },
    /// Configure automatic backups
    Configure {
        /// Enable or disable backups
        #[arg(long)]
        enabled: Option<bool>,
        /// Cron schedule (e.g. "0 2 * * *")
        #[arg(long)]
        schedule: Option<String>,
        /// Number of backups to retain
        #[arg(long)]
        retention: Option<u32>,
    },
    /// Reset the database (drop all data, re-run migrations)
    Reset {
        /// Skip the backup before reset
        #[arg(long)]
        no_backup: bool,
        /// The project's slug, typed out. Required wherever there is no terminal
        /// to ask on: this deletes every record, so `--yes` deliberately does
        /// not cover it.
        #[arg(long, value_name = "PROJECT")]
        confirm: Option<String>,
    },
}

#[derive(Subcommand, Debug)]
enum CloudBillingCommands {
    /// Show current usage
    Usage,
    /// Change plan or billing interval
    #[command(name = "plan")]
    ChangePlan,
}

#[derive(Subcommand, Debug)]
pub enum CloudTeamCommands {
    /// List all team members
    List,
    /// Invite a user by their GitHub email
    Invite {
        /// GitHub email address of the user to invite
        email: String,
    },
    /// List pending invitations
    #[command(name = "invites")]
    Invitations,
    /// Revoke a pending invitation
    Revoke {
        /// Email address of the invitation to revoke
        email: String,
    },
    /// Remove a team member
    Remove {
        /// Email address of the member to remove
        email: String,
    },
    /// Rename the tenant
    Rename {
        /// New tenant name
        name: Option<String>,
    },
}

#[derive(Subcommand, Debug)]
pub enum CloudVaultCommands {
    /// Request a vault passphrase reset (if you forgot your passphrase)
    RequestReset,
    /// Approve a pending vault reset request (admin only)
    ApproveReset {
        /// Email of the member whose reset to approve
        email: String,
    },
    /// Complete your vault passphrase reset (set a new passphrase after admin approval)
    CompleteReset,
    /// List pending vault reset requests (admin only)
    ListResets,
}

#[derive(Subcommand, Debug)]
enum CloudKeyCommands {
    /// Create a new API key
    Create,
    /// List all API keys
    List,
    /// Revoke an API key
    Revoke {
        /// Key ID to revoke
        id: String,
    },
}

#[derive(Subcommand, Debug)]
enum McpCommands {
    /// Run the local SurrealDB devtools MCP server (default for `spky mcp`)
    Serve,
    /// Generate a cloud MCP token with scopes, then print setup instructions
    Token {
        /// Token name/label (defaults to "spooky-mcp")
        #[arg(long)]
        name: Option<String>,
        /// Comma-separated scopes (skips the interactive picker), e.g. "mcp:read,deployments:write"
        #[arg(long)]
        scopes: Option<String>,
        /// Shortcut for read-only access (mcp:read)
        #[arg(long)]
        read_only: bool,
        /// Register the server with an editor after creating the token
        #[arg(long)]
        install: bool,
        /// Editor to register with: claude | cursor | vscode
        #[arg(long)]
        client: Option<String>,
    },
    /// List your cloud MCP tokens
    Tokens,
    /// Revoke a cloud MCP token by ID
    Revoke {
        /// Token ID to revoke
        id: String,
    },
    /// Register the MCP server with an editor using an existing token
    Install {
        /// MCP token (mcp_live_…); prompted for if omitted
        #[arg(long)]
        token: Option<String>,
        /// Editor to register with: claude | cursor | vscode
        #[arg(long)]
        client: Option<String>,
    },
}

#[derive(Subcommand, Debug)]
enum BucketCommands {
    /// Add a new storage bucket
    Add {
        /// Bucket name (snake_case, e.g. user_avatars)
        #[arg(long)]
        name: Option<String>,

        /// Preset type: avatars, images, documents, video, audio, custom
        #[arg(long)]
        preset: Option<String>,

        /// Max file size (e.g. 5mb, 500kb, 1gb)
        #[arg(long)]
        max_size: Option<String>,

        /// Allowed file extensions, comma-separated (e.g. jpg,png,gif)
        #[arg(long)]
        extensions: Option<String>,

        /// Storage backend (default: persistent file backend when
        /// deployment.storage.sizeGB is set, otherwise "memory")
        #[arg(long)]
        backend: Option<String>,

        /// Enable per-user path isolation
        #[arg(long)]
        path_prefix_auth: Option<bool>,

        /// Path to sp00ky.yml config file
        #[arg(long)]
        config: Option<PathBuf>,

        /// Directory for bucket .surql files
        #[arg(long)]
        buckets_dir: Option<PathBuf>,
    },
    /// Switch a bucket's storage backend (memory | persistent)
    Backend {
        /// Bucket name (snake_case)
        name: String,

        /// Target backend: memory | persistent (prompted if omitted)
        #[arg(value_parser = ["memory", "persistent"])]
        target: Option<String>,

        /// Path to sp00ky.yml config file
        #[arg(long)]
        config: Option<PathBuf>,

        /// Directory for bucket .surql files
        #[arg(long)]
        buckets_dir: Option<PathBuf>,
    },
}

#[derive(Subcommand, Debug)]
enum ApiCommands {
    /// Add an API backend
    Add {
        /// Path to OpenAPI spec file
        #[arg(long)]
        spec: Option<String>,

        /// Backend name (key in sp00ky.yml)
        #[arg(long)]
        name: Option<String>,

        /// API base URL
        #[arg(long)]
        base_url: Option<String>,

        /// Auth type (e.g. "token")
        #[arg(long)]
        auth_type: Option<String>,

        /// Auth token
        #[arg(long)]
        auth_token: Option<String>,

        /// Outbox table name
        #[arg(long)]
        table: Option<String>,

        /// Path for generated .surql schema file
        #[arg(long)]
        schema_path: Option<String>,

        /// Path to sp00ky.yml config file
        #[arg(long)]
        config: Option<PathBuf>,
    },
}

#[derive(Subcommand, Debug)]
enum MigrateCommands {
    /// Create a new migration (auto-generates diff from schema changes)
    Create {
        /// Name for the migration (e.g. "add_user_avatar")
        name: String,
        /// Path to .surql schema file to pre-populate the migration (legacy mode)
        #[arg(long)]
        schema: Option<PathBuf>,
        /// Migrations directory
        #[arg(long)]
        migrations_dir: Option<PathBuf>,
        /// Path to the input .surql schema file (for auto-diff)
        #[arg(long)]
        input: Option<PathBuf>,
        /// Path to sp00ky.yml config file
        #[arg(long)]
        config: Option<PathBuf>,
        /// Generation mode: singlenode, cluster, surrealism
        #[arg(long, default_value = "singlenode")]
        mode: String,
        /// SSP/Scheduler endpoint URL
        #[arg(long)]
        endpoint: Option<String>,
        /// SSP/Scheduler auth secret
        #[arg(long)]
        secret: Option<String>,
        /// SurrealDB URL for live DB schema extraction (skips ephemeral DB)
        #[arg(long)]
        url: Option<String>,
        /// SurrealDB namespace (used with --url)
        #[arg(long, default_value = "main")]
        namespace: String,
        /// SurrealDB database (used with --url)
        #[arg(long, default_value = "main")]
        database: String,
        /// SurrealDB username (used with --url)
        #[arg(long, default_value = "root")]
        username: String,
        /// SurrealDB password (used with --url)
        #[arg(long, default_value = "root")]
        password: String,
        /// Skip auto-diff and create an empty migration template
        #[arg(long)]
        empty: bool,
    },
    /// Apply all pending migrations
    Apply {
        #[command(flatten)]
        conn: ConnectionArgs,
        /// Migrations directory
        #[arg(long)]
        migrations_dir: Option<PathBuf>,
        /// Path to sp00ky.yml config file
        #[arg(long)]
        config: Option<PathBuf>,
        /// Generation mode: singlenode, cluster, surrealism
        #[arg(long, default_value = "singlenode")]
        mode: String,
        /// SSP/Scheduler endpoint URL
        #[arg(long)]
        endpoint: Option<String>,
        /// SSP/Scheduler auth secret
        #[arg(long)]
        secret: Option<String>,
        /// Force re-applying the internal schema + remote functions even when
        /// unchanged (bypasses the schema-hash skip). Use for drift recovery.
        #[arg(long)]
        force_schema: bool,
    },
    /// Apply pending migrations to the project's Sp00ky Cloud deployment (no deploy).
    ///
    /// Resolves the SurrealDB URL + root password from your login (`spky login`)
    /// and the `slug` in sp00ky.yml, then applies any pending migrations plus the
    /// internal Sp00ky schema. The deploy mode (singlenode/cluster/surrealism),
    /// namespace, and database are read from sp00ky.yml — no connection flags
    /// needed. Use this to migrate prod on demand, decoupled from a full deploy.
    Prod {
        /// Path to sp00ky.yml config file
        #[arg(long)]
        config: Option<PathBuf>,
        /// Migrations directory (defaults to the one in sp00ky.yml)
        #[arg(long)]
        migrations_dir: Option<PathBuf>,
        /// SSP/Scheduler endpoint URL
        #[arg(long)]
        endpoint: Option<String>,
        /// SSP/Scheduler auth secret
        #[arg(long)]
        secret: Option<String>,
        /// Force re-applying the internal schema + remote functions even when
        /// unchanged (bypasses the schema-hash skip). Use for drift recovery.
        #[arg(long)]
        force_schema: bool,
    },
    /// Show migration status
    Status {
        #[command(flatten)]
        conn: ConnectionArgs,
        /// Migrations directory
        #[arg(long)]
        migrations_dir: Option<PathBuf>,
    },
    /// Fix schema drift and/or checksum mismatches
    Fix {
        #[command(flatten)]
        conn: ConnectionArgs,
        /// Migrations directory
        #[arg(long)]
        migrations_dir: Option<PathBuf>,
        /// Update stored checksums for modified-but-applied migration files
        #[arg(long)]
        fix_checksums: bool,
    },
}

#[derive(ClapArgs, Debug)]
struct ConnectionArgs {
    /// SurrealDB URL. When omitted, resolved from sp00ky.yml: the locally-hosted
    /// dev endpoint (e.g. http://localhost:8666) or an external endpoint.
    #[arg(long, env = "SURREAL_URL")]
    url: Option<String>,
    /// SurrealDB namespace
    #[arg(long, env = "SURREAL_NS", default_value = "main")]
    namespace: String,
    /// SurrealDB database
    #[arg(long, env = "SURREAL_DB", default_value = "main")]
    database: String,
    /// SurrealDB username
    #[arg(long, env = "SURREAL_USER", default_value = "root")]
    username: String,
    /// SurrealDB password
    #[arg(long, env = "SURREAL_PASS", default_value = "root")]
    password: String,
    /// Target the project's Sp00ky Cloud deployment. Resolves the SurrealDB URL
    /// and root password automatically from your login (`spky login`) and the
    /// `slug` in sp00ky.yml, so you don't pass --url/--password.
    #[arg(long)]
    cloud: bool,
}

/// Fully resolved SurrealDB connection parameters.
pub(crate) struct ResolvedConnection {
    pub(crate) url: String,
    pub(crate) namespace: String,
    pub(crate) database: String,
    pub(crate) username: String,
    pub(crate) password: String,
}

impl ConnectionArgs {
    /// When `--cloud` is set, resolve the deployment's SurrealDB endpoint and
    /// root password from Sp00ky Cloud (the same lookup as `spky project
    /// credentials`). `namespace`/`database` come from explicit flags when
    /// given, otherwise from sp00ky.yml. Returns `Ok(None)` when `--cloud` was
    /// not passed, so callers fall back to their existing local resolution.
    pub(crate) fn cloud_connection(
        &self,
        config: &Option<PathBuf>,
    ) -> Result<Option<ResolvedConnection>> {
        if !self.cloud {
            return Ok(None);
        }
        let config_file = config
            .clone()
            .unwrap_or_else(|| PathBuf::from(DEFAULT_CONFIG_PATH));
        let resolved = backend::load_config(&config_file).resolved_surrealdb();
        let namespace = if self.namespace == "main" {
            resolved.namespace.clone()
        } else {
            self.namespace.clone()
        };
        let database = if self.database == "main" {
            resolved.database.clone()
        } else {
            self.database.clone()
        };
        // External DB: Sp00ky doesn't host it, so there's no deployment URL —
        // resolve the endpoint + credentials (incl. vault refs) from the manifest.
        if resolved.hosting == backend::HostingMode::External {
            let ext = cloud::resolve_external_surreal(&config_file)?;
            return Ok(Some(ResolvedConnection {
                url: ext.url,
                namespace,
                database,
                username: ext.username,
                password: ext.password,
            }));
        }
        let cloud = cloud::resolve_cloud_surreal(&config_file)?;
        Ok(Some(ResolvedConnection {
            url: cloud.url,
            namespace,
            database,
            username: "root".to_string(),
            password: cloud.password,
        }))
    }

    /// Fully resolve the SurrealDB connection. `--cloud` takes precedence;
    /// otherwise builds a local connection from sp00ky.yml. The URL comes from
    /// `--url`/`SURREAL_URL` when given, else the locally-hosted dev endpoint
    /// derived from the config (e.g. http://localhost:8666) or its external
    /// endpoint. namespace/database fall back to the config's resolved values
    /// unless overridden; username/password default to root/root.
    pub(crate) fn resolve(&self, config: &Option<PathBuf>) -> Result<ResolvedConnection> {
        if let Some(c) = self.cloud_connection(config)? {
            return Ok(c);
        }
        let config_file = config
            .clone()
            .unwrap_or_else(|| PathBuf::from(DEFAULT_CONFIG_PATH));
        let resolved = backend::load_config(&config_file).resolved_surrealdb();
        let namespace = if self.namespace == "main" {
            resolved.namespace.clone()
        } else {
            self.namespace.clone()
        };
        let database = if self.database == "main" {
            resolved.database.clone()
        } else {
            self.database.clone()
        };
        // External DB with no explicit --url: connect to the manifest's endpoint
        // using its credentials (resolving `{ vault: KEY }` refs). The external
        // endpoint is the same whether or not `--cloud` is passed.
        if resolved.hosting == backend::HostingMode::External && self.url.is_none() {
            let ext = cloud::resolve_external_surreal(&config_file)?;
            return Ok(ResolvedConnection {
                url: ext.url,
                namespace,
                database,
                username: ext.username,
                password: ext.password,
            });
        }
        let url = self
            .url
            .clone()
            .unwrap_or_else(|| dev::surreal_connection_url(&resolved, dev::SURREAL_PORT));
        Ok(ResolvedConnection {
            url,
            namespace,
            database,
            username: self.username.clone(),
            password: self.password.clone(),
        })
    }
}

/// Filter schema content to remove field definitions with FOR select WHERE false
/// and make all fields (except 'id') nullable by wrapping their types in option<>
fn filter_schema_for_client(content: &str, parser: &SchemaParser) -> Result<String> {
    let field_annotations = annotations::extract_field_annotations(content);
    let lines: Vec<&str> = content.lines().collect();
    let mut result = Vec::new();
    let mut modified_lines: Vec<String> = Vec::new(); // Store owned strings
    let mut i = 0;

    // Tables marked `-- @nosync` are server-only: drop them (and every DEFINE
    // that targets them) from the client schema entirely.
    let nosync: std::collections::HashSet<&str> = parser
        .tables
        .iter()
        .filter(|(_, t)| t.no_sync)
        .map(|(n, _)| n.as_str())
        .collect();

    // Dropping a statement must also drop the annotation comment attached to it.
    // Left behind, the orphan re-attaches to whatever statement follows and marks
    // THAT one instead — a stray `-- @nosync` above a `DEFINE TABLE` deletes the
    // whole table from the generated types. It also leaks the removed field's
    // name into the shipped client schema.
    //
    // Pops the whole contiguous comment block, but only when it contains an
    // annotation. That covers `-- @nosync` / `-- prose` / `DEFINE FIELD`, where
    // popping annotation lines alone would stop at the prose and strand the
    // marker. A comment block with no annotation is left alone, so ordinary
    // documentation above a permission-stripped field survives.
    let pop_attached_annotations = |result: &mut Vec<String>| {
        let block_start = result
            .iter()
            .rposition(|l| !l.trim().starts_with("--"))
            .map(|i| i + 1)
            .unwrap_or(0);
        let has_annotation = result[block_start..]
            .iter()
            .any(|l| annotations::is_annotation_comment(l));
        if has_annotation {
            result.truncate(block_start);
        }
    };

    while i < lines.len() {
        let line = lines[i];
        let trimmed = line.trim();

        // Drop any DEFINE TABLE/FIELD/INDEX/EVENT targeting a @nosync table.
        if !nosync.is_empty() {
            if let Some(target) = define_target_table(trimmed) {
                if nosync.contains(target.as_str()) {
                    println!(
                        "  → Removing @nosync table target '{}' from client schema",
                        target
                    );
                    pop_attached_annotations(&mut result);
                    while i < lines.len() {
                        if let Some(idx) = lines[i].find(';') {
                            let after_semicolon = &lines[i][idx + 1..];
                            if !after_semicolon.trim().is_empty() {
                                result.push(after_semicolon.to_string());
                            }
                            i += 1;
                            break;
                        }
                        i += 1;
                    }
                    continue;
                }
            }
        }

        // Check if this line starts a DEFINE FIELD
        if trimmed.starts_with("DEFINE FIELD") {
            // Extract table and field name
            if let Some((table_name, field_name)) = extract_table_and_field_name(trimmed) {
                // Check if this field should be stripped
                if let Some(table) = parser.tables.get(&table_name) {
                    if let Some(field) = table.fields.get(&field_name) {
                        if field.should_strip {
                            // Skip this entire field definition (until semicolon)
                            println!(
                                "  → Removing field '{}' from table '{}' in client schema",
                                field_name, table_name
                            );
                            pop_attached_annotations(&mut result);
                            while i < lines.len() {
                                if let Some(idx) = lines[i].find(';') {
                                    // Check if there is content after the semicolon
                                    let after_semicolon = &lines[i][idx + 1..];
                                    if !after_semicolon.trim().is_empty() {
                                        result.push(after_semicolon.to_string());
                                    }
                                    i += 1;
                                    break;
                                }
                                i += 1;
                            }
                            continue;
                        }
                    }
                }
            }

            // Make all fields (except 'id') nullable by wrapping TYPE in option<>.
            // Then, for `@crdt @cursor` fields, override the resulting TYPE
            // with `option<object> FLEXIBLE` so the row carries `{ state,
            // cursors }`. The override runs AFTER the nullable wrap so a
            // user-declared `string` becomes `option<object> FLEXIBLE`, not
            // `option<string>`.
            if let Some((table_name, field_name)) = extract_table_and_field_name(trimmed) {
                if field_name != "id" {
                    // Gather the full statement (a DEFINE FIELD may span several
                    // lines, e.g. a trailing `VALUE time::now();`) so the VALUE
                    // strip below sees the whole clause, not just the first line.
                    let mut stmt = String::new();
                    let mut trailing = String::new();
                    while i < lines.len() {
                        if let Some(idx) = lines[i].find(';') {
                            stmt.push_str(&lines[i][..idx]);
                            trailing = lines[i][idx + 1..].to_string();
                            i += 1;
                            break;
                        }
                        stmt.push_str(lines[i]);
                        stmt.push(' ');
                        i += 1;
                    }
                    let stmt = stmt.split_whitespace().collect::<Vec<_>>().join(" ");

                    let mut modified_line = make_field_nullable(&stmt);
                    if let Some(anns) =
                        field_annotations.get(&(table_name.clone(), field_name.clone()))
                    {
                        if let Some(rewritten) =
                            annotations::rewrite_crdt_cursor_type(&modified_line, anns)
                        {
                            modified_line = rewritten;
                        }
                    }
                    // Strip server write-time `VALUE <expr>` clauses. The local
                    // cache stores values synced from the server verbatim; the
                    // cache write is an UPSERT, so a surviving `VALUE time::now()`
                    // (or `VALUE $auth.id`, …) would RE-EVALUATE on every write and
                    // overwrite the synced value — e.g. every thread/comment ends
                    // up with `created_at` = the moment it was cached, so they all
                    // show the same date.
                    modified_line = strip_value_clause(&modified_line);
                    modified_line.push(';');

                    modified_lines.push(modified_line.clone());
                    result.push(modified_line);
                    if !trailing.trim().is_empty() {
                        result.push(trailing);
                    }
                    continue;
                }
            }
        }

        result.push(line.to_string());
        i += 1;
    }

    Ok(result.join("\n"))
}

/// Remove a top-level `VALUE <expr>` clause from a single-line DEFINE FIELD
/// statement (semicolon already stripped by the caller). `VALUE` is a SERVER
/// write-time computation (`time::now()`, `$auth.id`, …) that the client cache
/// must never re-evaluate — a cache UPSERT would otherwise overwrite the synced
/// value. The clause spans ` VALUE ` up to the next top-level clause keyword
/// (ASSERT / PERMISSIONS / COMMENT) or the end of the statement.
fn strip_value_clause(stmt: &str) -> String {
    let upper = stmt.to_ascii_uppercase();
    let Some(vpos) = upper.find(" VALUE ") else {
        return stmt.to_string();
    };
    let after = vpos + " VALUE ".len();
    let rest_upper = &upper[after..];
    let mut end = stmt.len();
    for kw in [" ASSERT ", " PERMISSIONS ", " COMMENT "] {
        if let Some(p) = rest_upper.find(kw) {
            end = end.min(after + p);
        }
    }
    let mut out = stmt[..vpos].trim_end().to_string();
    let tail = stmt[end..].trim_start();
    if !tail.is_empty() {
        out.push(' ');
        out.push_str(tail);
    }
    out
}

/// Make a DEFINE FIELD line nullable by wrapping its TYPE in option<>
/// Example: "DEFINE FIELD username ON TABLE user TYPE string"
///       -> "DEFINE FIELD username ON TABLE user TYPE option<string>"
fn make_field_nullable(line: &str) -> String {
    // Find "TYPE " in the line
    if let Some(type_pos) = line.find("TYPE ") {
        let before_type = &line[..type_pos + 5]; // Include "TYPE "
        let after_type = &line[type_pos + 5..];

        // Extract the type (everything until the next keyword or end of line).
        // Common keywords after TYPE: ASSERT, VALUE, PERMISSIONS, DEFAULT,
        // READONLY. We take the EARLIEST keyword that appears in the string,
        // not the first one in this list — otherwise a clause order like
        // `TYPE bool DEFAULT false PERMISSIONS ...` would stop at PERMISSIONS
        // and pull `DEFAULT false` into the type, yielding the invalid
        // `option<bool DEFAULT false>`.
        let type_end = [
            " ASSERT ",
            " VALUE ",
            " PERMISSIONS ",
            " DEFAULT ",
            " READONLY ",
        ]
        .iter()
        .filter_map(|kw| after_type.find(kw))
        .chain(after_type.find(';'))
        .min()
        .unwrap_or(after_type.len());

        let type_str = after_type[..type_end].trim();
        let rest = &after_type[type_end..];

        // Check if already wrapped in option<> or if type is 'any' (can't be wrapped)
        if type_str.starts_with("option<")
            || type_str.starts_with("OPTION<")
            || type_str.eq_ignore_ascii_case("any")
        {
            // Already nullable or is 'any' type, return as-is
            line.to_string()
        } else {
            // Wrap the type in option<>
            format!("{}option<{}>{}", before_type, type_str, rest)
        }
    } else {
        // No TYPE found, return as-is
        line.to_string()
    }
}

/// Extract table and field name from a DEFINE FIELD line
/// Example: "DEFINE FIELD password ON TABLE user TYPE string"
/// Returns: Some(("user", "password"))
fn extract_table_and_field_name(line: &str) -> Option<(String, String)> {
    let parts: Vec<&str> = line.split_whitespace().collect();

    // Look for pattern: DEFINE FIELD <name> ON TABLE <table>
    let mut field_name = None;
    let mut table_name = None;

    for i in 0..parts.len() {
        if parts[i] == "FIELD" && i + 1 < parts.len() {
            field_name = Some(parts[i + 1].to_string());
        }
        if parts[i] == "TABLE" && i + 1 < parts.len() {
            table_name = Some(parts[i + 1].to_string());
        }
    }

    if let (Some(table), Some(field)) = (table_name, field_name) {
        Some((table, field))
    } else {
        None
    }
}

/// Return the table a DEFINE statement targets, for the forms whose membership
/// in a table is what matters when stripping a `@nosync` table from the client
/// schema: `DEFINE TABLE <name>`, and `DEFINE FIELD|INDEX|EVENT ... ON [TABLE] <name>`.
/// Returns None for any other statement (or a non-DEFINE line).
fn define_target_table(line: &str) -> Option<String> {
    let parts: Vec<&str> = line.split_whitespace().collect();
    if parts.len() < 3 || !parts[0].eq_ignore_ascii_case("DEFINE") {
        return None;
    }
    let kind = parts[1].to_ascii_uppercase();
    match kind.as_str() {
        "TABLE" => {
            // Skip optional OVERWRITE / IF NOT EXISTS qualifiers.
            let mut idx = 2;
            while idx < parts.len() {
                let up = parts[idx].to_ascii_uppercase();
                if up == "OVERWRITE" || up == "IF" || up == "NOT" || up == "EXISTS" {
                    idx += 1;
                } else {
                    break;
                }
            }
            parts.get(idx).map(|s| s.trim_end_matches(';').to_string())
        }
        "FIELD" | "INDEX" | "EVENT" => {
            // ... ON [TABLE] <name>
            for i in 0..parts.len() {
                if parts[i].eq_ignore_ascii_case("ON") {
                    let mut j = i + 1;
                    if j < parts.len() && parts[j].eq_ignore_ascii_case("TABLE") {
                        j += 1;
                    }
                    return parts.get(j).map(|s| s.trim_end_matches(';').to_string());
                }
            }
            None
        }
        _ => None,
    }
}

fn handle_migrate(action: MigrateCommands) -> Result<()> {
    match action {
        MigrateCommands::Create {
            name,
            schema,
            migrations_dir,
            input,
            config,
            mode,
            endpoint,
            secret,
            url,
            namespace,
            database,
            username,
            password,
            empty,
        } => {
            // Load config to resolve paths
            let config_file = config
                .clone()
                .unwrap_or_else(|| PathBuf::from(DEFAULT_CONFIG_PATH));
            let sp00ky_config = backend::load_config(&config_file);
            let resolved = sp00ky_config.resolved_schema();
            let resolved_surreal = sp00ky_config.resolved_surrealdb();

            let resolved_input = input.unwrap_or(resolved.schema);
            let resolved_migrations = migrations_dir.unwrap_or(resolved.migrations);

            // Use config ns/db as defaults when CLI flags are at their default "main"
            let namespace = if namespace == "main" {
                resolved_surreal.namespace
            } else {
                namespace
            };
            let database = if database == "main" {
                resolved_surreal.database
            } else {
                database
            };

            let deploy_mode = match mode.as_str() {
                "cluster" => DeployMode::Cluster,
                "surrealism" => DeployMode::Surrealism,
                _ => DeployMode::Singlenode,
            };

            if empty {
                // Legacy: empty template or schema dump
                migrate::create(&resolved_migrations, &name, schema.as_deref(), None, None)
            } else {
                // Auto-diff mode
                let builder_config = schema_builder::SchemaBuilderConfig {
                    input_path: resolved_input,
                    config_path: Some(config_file),
                    mode: deploy_mode,
                    endpoint,
                    secret,
                    include_functions: false,
                };

                let conn = url.as_ref().map(|u| {
                    (
                        u.as_str(),
                        namespace.as_str(),
                        database.as_str(),
                        username.as_str(),
                        password.as_str(),
                    )
                });

                migrate::create(
                    &resolved_migrations,
                    &name,
                    schema.as_deref(),
                    Some(&builder_config),
                    conn,
                )
            }
        }
        MigrateCommands::Apply {
            conn,
            migrations_dir,
            config,
            mode,
            endpoint,
            secret,
            force_schema,
        } => {
            if force_schema {
                std::env::set_var("SPKY_FORCE_SCHEMA", "1");
            }
            let config_file = config.unwrap_or_else(|| PathBuf::from(DEFAULT_CONFIG_PATH));
            let sp00ky_config = backend::load_config(&config_file);
            let resolved = sp00ky_config.resolved_schema();
            let resolved_migrations = migrations_dir.unwrap_or(resolved.migrations);

            let conn_resolved = conn.resolve(&Some(config_file.clone()))?;

            let deploy_mode = match mode.as_str() {
                "cluster" => DeployMode::Cluster,
                "surrealism" => DeployMode::Surrealism,
                _ => DeployMode::Singlenode,
            };

            let config_path_opt = if config_file.exists() {
                Some(config_file.clone())
            } else {
                None
            };

            let ctx = migration::MigrationContext {
                environment: migration::MigrationEnvironment::Production,
                project_dir: std::env::current_dir()?,
                migrations_dir: resolved_migrations,
                url: conn_resolved.url,
                namespace: conn_resolved.namespace,
                database: conn_resolved.database,
                username: conn_resolved.username,
                password: conn_resolved.password,
                surrealkit_binary: sp00ky_config.resolved_surrealkit_binary(),
                internal_schema: Some(migration::InternalSchemaConfig {
                    schema_path: resolved.schema,
                    config_path: config_path_opt,
                    deploy_mode,
                    endpoint: endpoint.clone(),
                    secret: secret.clone(),
                }),
                remote_functions: None,
                secrets: None,
            };

            let engine = migration::create_engine(ctx)?;
            engine.apply()?;
            Ok(())
        }
        MigrateCommands::Prod {
            config,
            migrations_dir,
            endpoint,
            secret,
            force_schema,
        } => {
            if force_schema {
                std::env::set_var("SPKY_FORCE_SCHEMA", "1");
            }
            let config_file = config.unwrap_or_else(|| PathBuf::from(DEFAULT_CONFIG_PATH));
            let sp00ky_config = backend::load_config(&config_file);
            let resolved = sp00ky_config.resolved_schema();
            let resolved_migrations = migrations_dir.unwrap_or(resolved.migrations);

            // Always target the cloud deployment: resolve the SurrealDB URL + root
            // password from the login + `slug` in sp00ky.yml (same lookup as
            // `migrate apply --cloud` and `project credentials`).
            let cloud = cloud::resolve_cloud_surreal(&config_file)?;
            let resolved_surreal = sp00ky_config.resolved_surrealdb();
            // Deploy mode comes from sp00ky.yml so the internal schema matches the
            // deployment topology (e.g. cluster). `migrate apply --cloud` instead
            // defaults its --mode flag to singlenode, which would push the wrong
            // internal schema to a cluster deployment.
            let deploy_mode = sp00ky_config.mode.clone().unwrap_or_default();

            println!(
                "Target: {} (ns={}, db={}, mode={})",
                cloud.url, resolved_surreal.namespace, resolved_surreal.database, deploy_mode
            );

            // `consent`, not a bare prompt: this gate used to be skipped whenever
            // stdin was not a terminal, so the one caller that most needs to be
            // asked - a script or an agent - applied to production unasked.
            if !ui::consent("Apply pending migrations to this PRODUCTION database?")? {
                println!("Aborted.");
                return Ok(());
            }

            let config_path_opt = if config_file.exists() {
                Some(config_file.clone())
            } else {
                None
            };

            let ctx = migration::MigrationContext {
                environment: migration::MigrationEnvironment::Production,
                project_dir: std::env::current_dir()?,
                migrations_dir: resolved_migrations,
                url: cloud.url,
                namespace: resolved_surreal.namespace,
                database: resolved_surreal.database,
                username: "root".to_string(),
                password: cloud.password,
                surrealkit_binary: sp00ky_config.resolved_surrealkit_binary(),
                internal_schema: Some(migration::InternalSchemaConfig {
                    schema_path: resolved.schema,
                    config_path: config_path_opt,
                    deploy_mode,
                    endpoint: endpoint.clone(),
                    secret: secret.clone(),
                }),
                remote_functions: None,
                // Inject vault secrets so `{{KEY}}` placeholders in user migrations
                // (e.g. DEFINE ACCESS ... WITH JWT KEY '{{JWT_PUBLIC_KEY}}') resolve
                // to real values on prod. `Some(_)` forces the checked apply path:
                // an unresolved placeholder fails loudly rather than writing a
                // literal `{{...}}`. (Previously `None` → verbatim → broke auth.)
                secrets: Some(cloud::load_vault_secrets_for_prod()),
            };

            let engine = migration::create_engine(ctx)?;
            engine.apply()?;
            Ok(())
        }
        MigrateCommands::Status {
            conn,
            migrations_dir,
        } => {
            let sp00ky_config = backend::load_config(Path::new(DEFAULT_CONFIG_PATH));
            let resolved_migrations =
                migrations_dir.unwrap_or(sp00ky_config.resolved_schema().migrations);

            let conn_resolved = conn.resolve(&None)?;

            let ctx = migration::MigrationContext {
                environment: migration::MigrationEnvironment::Production,
                project_dir: std::env::current_dir()?,
                migrations_dir: resolved_migrations,
                url: conn_resolved.url,
                namespace: conn_resolved.namespace,
                database: conn_resolved.database,
                username: conn_resolved.username,
                password: conn_resolved.password,
                surrealkit_binary: sp00ky_config.resolved_surrealkit_binary(),
                internal_schema: None,
                remote_functions: None,
                secrets: None,
            };

            let engine = migration::create_engine(ctx)?;
            let statuses = engine.status()?;
            print_migration_status(&statuses);
            Ok(())
        }
        MigrateCommands::Fix {
            conn,
            migrations_dir,
            fix_checksums,
        } => {
            let sp00ky_config = backend::load_config(Path::new(DEFAULT_CONFIG_PATH));
            let resolved_migrations =
                migrations_dir.unwrap_or(sp00ky_config.resolved_schema().migrations);

            let conn_resolved = conn.resolve(&None)?;

            let ctx = migration::MigrationContext {
                environment: migration::MigrationEnvironment::Production,
                project_dir: std::env::current_dir()?,
                migrations_dir: resolved_migrations,
                url: conn_resolved.url,
                namespace: conn_resolved.namespace,
                database: conn_resolved.database,
                username: conn_resolved.username,
                password: conn_resolved.password,
                surrealkit_binary: sp00ky_config.resolved_surrealkit_binary(),
                internal_schema: None,
                remote_functions: None,
                secrets: None,
            };

            let engine = migration::create_engine(ctx)?;
            engine.fix(fix_checksums)
        }
    }
}

fn print_migration_status(statuses: &[migration::MigrationInfo]) {
    const GREEN: &str = "\x1b[32m";
    const YELLOW: &str = "\x1b[33m";
    const RED: &str = "\x1b[31m";
    const DIM: &str = "\x1b[2m";
    const RESET: &str = "\x1b[0m";

    if statuses.is_empty() {
        println!("No migrations found.");
        return;
    }

    println!("Migration Status:\n");

    for info in statuses {
        match info.state {
            migration::MigrationState::Applied => {
                let at = info.applied_at.as_deref().unwrap_or("");
                println!(
                    "  {GREEN}[applied]{RESET}  {}_{:<40} {DIM}(applied {}){RESET}",
                    info.id, info.name, at
                );
            }
            migration::MigrationState::Pending => {
                println!("  {YELLOW}[pending]{RESET}  {}_{}", info.id, info.name);
            }
            migration::MigrationState::Drift => {
                let detail = info.detail.as_deref().unwrap_or("");
                println!(
                    "  {RED}[DRIFT]    {}_{:<40} ({}){RESET}",
                    info.id, info.name, detail
                );
            }
        }
    }

    println!();
}

fn handle_api(action: ApiCommands) -> Result<()> {
    match action {
        ApiCommands::Add {
            spec,
            name,
            base_url,
            auth_type,
            auth_token,
            table,
            schema_path,
            config,
        } => {
            let resolved_config = config.unwrap_or_else(|| PathBuf::from(DEFAULT_CONFIG_PATH));
            add_api::add_api(
                spec,
                name,
                base_url,
                auth_type,
                auth_token,
                table,
                schema_path,
                resolved_config,
            )
        }
    }
}

fn handle_bucket(action: BucketCommands) -> Result<()> {
    match action {
        BucketCommands::Add {
            name,
            preset,
            max_size,
            extensions,
            backend,
            path_prefix_auth,
            config,
            buckets_dir,
        } => {
            let resolved_config = config.unwrap_or_else(|| PathBuf::from(DEFAULT_CONFIG_PATH));
            let sp00ky_config = backend::load_config(&resolved_config);
            let resolved_buckets =
                buckets_dir.unwrap_or(sp00ky_config.resolved_schema().buckets_dir);
            let storage_enabled = sp00ky_config.bucket_storage_gb().is_some();

            bucket::add(
                name,
                preset,
                max_size,
                extensions,
                backend,
                storage_enabled,
                path_prefix_auth,
                resolved_config,
                resolved_buckets,
            )
        }
        BucketCommands::Backend {
            name,
            target,
            config,
            buckets_dir,
        } => {
            let resolved_config = config.unwrap_or_else(|| PathBuf::from(DEFAULT_CONFIG_PATH));
            let sp00ky_config = backend::load_config(&resolved_config);
            let resolved_buckets =
                buckets_dir.unwrap_or(sp00ky_config.resolved_schema().buckets_dir);

            bucket::set_backend(name, target, resolved_buckets)
        }
    }
}

fn run_codegen(
    input_path: &Path,
    append_paths: &[PathBuf],
    output_path: &Path,
    output_format: OutputFormat,
    config_path: Option<&Path>,
    backend_processor: &BackendProcessor,
    no_header: bool,
    mode: &DeployMode,
    endpoint: Option<&str>,
    secret: Option<&str>,
    modules_dir: &Path,
    generate_all: bool,
) -> Result<()> {
    // Read the input file
    let mut content = fs::read_to_string(input_path)
        .context(format!("Failed to read input file: {:?}", input_path))?;

    // Append backend schemas to content
    if !backend_processor.schema_appends.is_empty() {
        content.push('\n');
        content.push_str(&backend_processor.schema_appends);
    }

    // Append embedded meta tables
    let meta_tables = include_str!("meta_tables.surql");
    let meta_tables_remote_raw = include_str!("meta_tables_remote.surql");
    let meta_tables_client = include_str!("meta_tables_client.surql");

    content.push('\n');
    content.push_str(meta_tables);
    println!("  + Appended base meta_tables.surql");

    if matches!(output_format, OutputFormat::Surql) {
        // Pre-parse the user schema (without meta tables yet) so we can
        // derive the per-parent CRDT permission rule and substitute the
        // {{CRDT_UPDATE_RULE}} placeholder before appending.
        let mut pre_parser = SchemaParser::new();
        pre_parser
            .parse_file(&content)
            .context("Failed to pre-parse user schema for CRDT permission derivation")?;
        let meta_tables_remote = schema_builder::substitute_crdt_update_rule(
            meta_tables_remote_raw,
            &content,
            &pre_parser,
        );
        content.push('\n');
        content.push_str(&meta_tables_remote);
        println!("  + Appended meta_tables_remote.surql");
    } else {
        content.push('\n');
        content.push_str(meta_tables_client);
        println!("  + Appended meta_tables_client.surql");
    }

    // Append extra files if specified
    for append_path in append_paths {
        let append_content = fs::read_to_string(append_path)
            .context(format!("Failed to read append file: {:?}", append_path))?;
        content.push('\n');
        content.push_str(&append_content);
        println!("  + Appended schema from {:?}", append_path);
    }

    // Extract field annotations from raw content (before surrealdb-core strips comments)
    let field_annotations = annotations::extract_field_annotations(&content);

    // Parse the schema
    let mut parser = SchemaParser::new();
    parser
        .parse_file(&content)
        .context("Failed to parse SurrealDB schema")?;

    // Merge annotations into parsed field definitions
    for ((table_name, field_name), anns) in &field_annotations {
        if let Some(table_schema) = parser.tables.get_mut(table_name) {
            if let Some(field_def) = table_schema.fields.get_mut(field_name) {
                field_def.annotations = anns.clone();
            } else {
                eprintln!(
                    "  ⚠ Annotation on unknown field: {}.{}",
                    table_name, field_name
                );
            }
        }
    }

    // Validate @crdt fields' TYPE matches their on-disk shape:
    //
    //   `@crdt`             → TYPE bytes  (raw LoroDoc snapshot)
    //   `@crdt` + `@cursor` → TYPE object (`{ state: bytes, cursors: { … } }`)
    //
    // The schema-builder rewrites `@crdt @cursor` to `option<object>
    // FLEXIBLE` regardless of source TYPE, but we still want the source
    // TYPE to reflect intent — otherwise readers see e.g. `TYPE bytes`
    // for a field that's actually stored as an object. Fail loudly so
    // mistakes surface at codegen time, not via a confusing runtime
    // shape mismatch later.
    let mut crdt_errors: Vec<String> = Vec::new();
    for (table_name, table_schema) in &parser.tables {
        for (field_name, field_def) in &table_schema.fields {
            let has_crdt = annotations::has_annotation(&field_def.annotations, "crdt");
            if !has_crdt {
                continue;
            }
            let has_cursor = annotations::has_annotation(&field_def.annotations, "cursor");

            // Unwrap option<inner> once so authors can choose `option<bytes>`
            // / `option<object>` if they want explicit nullability — same
            // effective shape since the schema-builder wraps non-option
            // fields in option<> anyway when emitting the client schema.
            let inner: &crate::parser::FieldType = match &field_def.field_type {
                crate::parser::FieldType::Option(inner) => inner,
                other => other,
            };

            if has_cursor {
                let is_object = matches!(inner, crate::parser::FieldType::Object);
                if !is_object {
                    crdt_errors.push(format!(
                        "  - `{table}.{field}` is annotated `@crdt @cursor` but its TYPE is \
                         `{ty:?}`. Cursored CRDT fields carry `{{ state, cursors }}`, so the \
                         source TYPE must be `object` (the schema-builder rewrites it to \
                         `option<object> FLEXIBLE` on emit). Change the DEFINE FIELD line to \
                         `TYPE object`.",
                        table = table_name,
                        field = field_name,
                        ty = field_def.field_type,
                    ));
                }
            } else {
                let is_bytes = matches!(inner, crate::parser::FieldType::Bytes);
                if !is_bytes {
                    crdt_errors.push(format!(
                        "  - `{table}.{field}` is annotated `@crdt` but its TYPE is `{ty:?}`. \
                         CRDT fields must be `TYPE bytes` so the LoroDoc snapshot can be stored \
                         verbatim. Change the DEFINE FIELD line to `TYPE bytes`.",
                        table = table_name,
                        field = field_name,
                        ty = field_def.field_type,
                    ));
                }
            }
        }
    }
    if !crdt_errors.is_empty() {
        anyhow::bail!(
            "Schema validation failed for @crdt fields:\n{}",
            crdt_errors.join("\n")
        );
    }

    // Extract buckets from separate bucket files (if any)
    if !backend_processor.bucket_schema.is_empty() {
        parser.extract_buckets(&backend_processor.bucket_schema);
    }

    // Filter the raw schema content to remove fields with FOR select WHERE false
    let mut filtered_schema_content = filter_schema_for_client(&content, &parser)?;

    // Append _00_rv field to every table for local cache setup (client-side only).
    //
    // The `WHERE true` field-level permission is intentional: SurrealDB applies
    // the table-level PERMISSIONS first, so a user who can't SELECT from `thread`
    // never reaches `thread._00_rv` regardless of what's written here. The field
    // permission only matters once the row is already accessible — and in that
    // case the sync runtime needs to read/write the version unconditionally.
    println!("  + Injecting _00_rv field for local cache schema");
    for (table_name, table_schema) in &parser.tables {
        // @nosync tables are not in the client schema, so don't add the
        // local-cache version field for them.
        if table_schema.no_sync {
            continue;
        }
        filtered_schema_content.push_str(&format!(
            "\nDEFINE FIELD _00_rv ON TABLE {} TYPE int DEFAULT 0 PERMISSIONS FOR select, create, update WHERE true;",
            table_name
        ));
    }

    // Choose which content to use based on format
    let raw_schema_content = if matches!(output_format, OutputFormat::Surql) {
        let builder_config = schema_builder::SchemaBuilderConfig {
            input_path: input_path.to_path_buf(),
            config_path: config_path.map(|p| p.to_path_buf()),
            mode: mode.clone(),
            endpoint: endpoint.map(|s| s.to_string()),
            secret: secret.map(|s| s.to_string()),
            include_functions: true,
        };
        let c = schema_builder::build_server_schema(&builder_config)?;
        println!("  + Built server schema via schema_builder");
        c
    } else {
        filtered_schema_content.clone()
    };

    println!(
        "Successfully parsed {} table(s) from {:?}",
        parser.tables.len(),
        input_path
    );

    for (table_name, table_schema) in &parser.tables {
        println!(
            "  - {}: {} field(s), schemafull: {}",
            table_name,
            table_schema.fields.len(),
            table_schema.schemafull
        );
    }

    // Generate JSON Schema
    let generator = JsonSchemaGenerator::new();
    let json_schema = generator.generate(&parser);

    let json_schema_string =
        serde_json::to_string_pretty(&json_schema).context("Failed to serialize JSON Schema")?;

    fn ensure_directory_exists(path: &std::path::Path) -> Result<()> {
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() && !parent.exists() {
                fs::create_dir_all(parent)
                    .context(format!("Failed to create directory: {:?}", parent))?;
            }
        }
        Ok(())
    }

    if generate_all {
        println!("\nGenerating all formats...");

        let json_path = output_path.with_extension("json");
        ensure_directory_exists(&json_path)?;
        fs::write(&json_path, &json_schema_string)
            .context(format!("Failed to write JSON Schema file: {:?}", json_path))?;
        println!("  ✓ JSON Schema: {:?}", json_path);

        let ts_gen = CodeGenerator::new_with_header(OutputFormat::Typescript, !no_header);
        let ts_code = ts_gen
            .generate_with_schema(
                &json_schema_string,
                "Database",
                Some(&raw_schema_content),
                None,
                Some(&backend_processor.backend_definitions),
            )
            .context("Failed to generate TypeScript code")?;
        let ts_path = output_path.with_extension("ts");
        ensure_directory_exists(&ts_path)?;
        fs::write(&ts_path, ts_code)
            .context(format!("Failed to write TypeScript file: {:?}", ts_path))?;
        println!("  ✓ TypeScript: {:?}", ts_path);

        let dart_gen = CodeGenerator::new_with_header(OutputFormat::Dart, !no_header);
        let dart_code = dart_gen
            .generate_with_schema(
                &json_schema_string,
                "Database",
                Some(&raw_schema_content),
                None,
                Some(&backend_processor.backend_definitions),
            )
            .context("Failed to generate Dart code")?;
        let dart_path = output_path.with_extension("dart");
        ensure_directory_exists(&dart_path)?;
        fs::write(&dart_path, dart_code)
            .context(format!("Failed to write Dart file: {:?}", dart_path))?;
        println!("  ✓ Dart: {:?}", dart_path);

        println!("\nSuccessfully generated all formats!");
    } else {
        let is_client = !matches!(output_format, OutputFormat::Surql);
        let sp00ky_events = sp00ky::generate_sp00ky_events(
            &parser.tables,
            &content,
            is_client,
            mode,
            endpoint,
            secret,
            crate::backend::sync_settings_for(config_path).transport(),
        );

        let include_modules = *mode == DeployMode::Surrealism;
        let generator = CodeGenerator::new(output_format, !no_header, include_modules)
            .with_query_allowlist(crate::backend::sync_settings_for(config_path).query_allowlist())
            .with_impersonation(crate::backend::impersonation_settings_for(config_path).enabled());
        let output_content = generator
            .generate_with_schema(
                &json_schema_string,
                "Schema",
                Some(&raw_schema_content),
                Some(&sp00ky_events),
                Some(&backend_processor.backend_definitions),
            )
            .context("Failed to generate output code")?;

        ensure_directory_exists(output_path)?;
        fs::write(output_path, output_content)
            .context(format!("Failed to write output file: {:?}", output_path))?;

        let format_name = match output_format {
            OutputFormat::JsonSchema => "JSON Schema",
            OutputFormat::Typescript => "TypeScript",
            OutputFormat::Dart => "Dart",
            OutputFormat::Surql => "sql",
        };

        if matches!(output_format, OutputFormat::Surql) && *mode == DeployMode::Surrealism {
            if let Some(output_dir) = output_path.parent() {
                println!("\nProcessing Surrealism Modules...");
                if let Err(e) = modules::compile_modules(modules_dir, output_dir) {
                    eprintln!("Warning: Failed to compile modules: {}", e);
                }
            }
        }

        println!(
            "\nSuccessfully generated {} at {:?}",
            format_name, output_path
        );
    }

    Ok(())
}

fn handle_lint(config_path: &Path) -> Result<()> {
    use anyhow::bail;
    use backend::{
        AppType, BackendDevConfig, BackendDevTypedConfig, EnvConfig, EnvEntry, EnvSource,
    };

    let mut errors: Vec<String> = Vec::new();
    let mut warnings: Vec<String> = Vec::new();

    // 1. Check config file exists
    if !config_path.exists() {
        bail!("Config file not found: {}", config_path.display());
    }
    let base_dir = config_path.parent().unwrap_or(Path::new("."));

    // 2. Parse YAML
    let content = fs::read_to_string(config_path)
        .context(format!("Failed to read {}", config_path.display()))?;
    let config: Sp00kyConfig = backend::parse_config_with_includes(&content, base_dir)
        .context(format!("Failed to parse {}", config_path.display()))?;
    println!("  Parsed {} successfully.", config_path.display());

    // 3. Structural validation
    if let Err(e) = config.validate() {
        errors.push(format!("{}", e));
    }

    // 4. Check referenced files exist
    let check_file = |path: &str, label: &str, errs: &mut Vec<String>| {
        let resolved = base_dir.join(path);
        if !resolved.exists() {
            errs.push(format!("{} not found: {}", label, resolved.display()));
        }
    };

    // Schema paths
    let schema = config.resolved_schema();
    if !base_dir.join(&schema.schema).exists() {
        warnings.push(format!(
            "Schema file not found: {}",
            base_dir.join(&schema.schema).display()
        ));
    }

    // Bucket files
    for bucket_path in &config.buckets {
        check_file(bucket_path, "Bucket file", &mut errors);
    }

    // Per-app checks
    for (name, app) in &config.apps {
        let prefix = format!("apps.{}", name);

        // Spec file (backends)
        if let Some(spec) = &app.spec {
            check_file(spec, &format!("{}.spec", prefix), &mut errors);
        }

        // Method schema file (backends)
        if let Some(method) = &app.method {
            check_file(
                &method.schema,
                &format!("{}.method.schema", prefix),
                &mut errors,
            );
        }

        // Deploy dockerfile
        if let Some(deploy) = &app.deploy {
            if let Some(dockerfile) = &deploy.dockerfile {
                check_file(
                    dockerfile,
                    &format!("{}.deploy.dockerfile", prefix),
                    &mut errors,
                );
            }
            if let Some(context) = &deploy.context {
                let resolved = base_dir.join(context);
                if !resolved.is_dir() {
                    errors.push(format!(
                        "{}.deploy.context directory not found: {}",
                        prefix,
                        resolved.display()
                    ));
                }
            }
        }

        // Dev workdir
        if let Some(ref dev) = app.dev {
            match dev {
                BackendDevConfig::Typed(BackendDevTypedConfig::Npm {
                    workdir: Some(w), ..
                })
                | BackendDevConfig::Typed(BackendDevTypedConfig::Docker {
                    workdir: Some(w), ..
                })
                | BackendDevConfig::Typed(BackendDevTypedConfig::Uv {
                    workdir: Some(w), ..
                }) => {
                    let resolved = base_dir.join(w);
                    if !resolved.is_dir() {
                        warnings.push(format!(
                            "{}.dev.workdir directory not found: {}",
                            prefix,
                            resolved.display()
                        ));
                    }
                }
                _ => {}
            }
        }

        // Env file references
        fn check_env_file_sources(
            source: &EnvSource,
            base_dir: &Path,
            prefix: &str,
            warnings: &mut Vec<String>,
        ) {
            match source {
                EnvSource::Str(s) if s != "vault" => {
                    let resolved = base_dir.join(s);
                    if !resolved.exists() {
                        warnings.push(format!(
                            "{}.env file not found: {}",
                            prefix,
                            resolved.display()
                        ));
                    }
                }
                _ => {}
            }
        }
        fn check_env_entry(
            entry: &EnvEntry,
            base_dir: &Path,
            prefix: &str,
            warnings: &mut Vec<String>,
        ) {
            match entry {
                EnvEntry::Source(s) => check_env_file_sources(s, base_dir, prefix, warnings),
                EnvEntry::List(sources) => {
                    for s in sources {
                        check_env_file_sources(s, base_dir, prefix, warnings);
                    }
                }
            }
        }
        if let Some(ref env) = app.env {
            match env {
                EnvConfig::Source(s) => check_env_file_sources(s, base_dir, &prefix, &mut warnings),
                EnvConfig::List(sources) => {
                    for s in sources {
                        check_env_file_sources(s, base_dir, &prefix, &mut warnings);
                    }
                }
                EnvConfig::PerEnvironment { dev, cloud } => {
                    if let Some(e) = dev {
                        check_env_entry(e, base_dir, &prefix, &mut warnings);
                    }
                    if let Some(e) = cloud {
                        check_env_entry(e, base_dir, &prefix, &mut warnings);
                    }
                }
            }
        }

        // Frontend-specific: warn if no deploy config
        if app.app_type == AppType::Frontend && app.deploy.is_none() {
            warnings.push(format!(
                "{}: frontend app has no deploy configuration",
                prefix
            ));
        }
    }

    // 4b. Schedules and workflows: `config.validate()` above already covered the
    // structural rules (cadence, cron syntax, DAG acyclicity). What needs the
    // backend route map — does that backend exist, does it have an outbox table,
    // does that route exist on it — happens here, and is the same resolution the
    // deploy-time sync performs, so anything that lints can actually be written.
    if !config.schedules.is_empty() || !config.workflows.is_empty() {
        let mut processor = backend::BackendProcessor::new();
        match processor.process(config_path) {
            Ok(()) => match schedule_sync::resolve_rows(&config, &processor, base_dir) {
                Ok(rows) => {
                    println!(
                        "  Validated {} schedule/workflow definition(s).",
                        rows.len()
                    );
                    for note in schedule_sync::dev_only_targets(&config) {
                        warnings.push(format!("{note} — it will not run in a cloud deployment"));
                    }
                }
                Err(e) => errors.push(format!("{e:#}")),
            },
            Err(e) => warnings.push(format!(
                "could not resolve backends to validate schedules: {e:#}"
            )),
        }
    }

    // 5. Print results
    if !warnings.is_empty() {
        println!();
        for w in &warnings {
            println!("  \x1b[33mwarning\x1b[0m: {}", w);
        }
    }

    if !errors.is_empty() {
        println!();
        for e in &errors {
            println!("  \x1b[31merror\x1b[0m: {}", e);
        }
        println!();
        bail!(
            "Lint failed with {} error(s) and {} warning(s).",
            errors.len(),
            warnings.len()
        );
    }

    if warnings.is_empty() {
        println!("  \x1b[32mAll checks passed.\x1b[0m");
    } else {
        println!();
        println!("  \x1b[32mNo errors.\x1b[0m {} warning(s).", warnings.len());
    }

    Ok(())
}

fn handle_generate(config_path: &Path) -> Result<()> {
    let config_str = fs::read_to_string(config_path)
        .context(format!("Failed to read config file: {:?}", config_path))?;
    let base_dir = config_path.parent().unwrap_or(Path::new("."));
    let config: Sp00kyConfig = backend::parse_config_with_includes(&config_str, base_dir)
        .context("Failed to parse sp00ky config")?;

    if config.client_types.is_empty() {
        anyhow::bail!(
            "No clientTypes entries found in {:?}. Add at least one entry to generate.",
            config_path
        );
    }

    let base_dir = config_path.parent().unwrap_or(Path::new("."));
    let resolved = config.resolved_schema();

    // Process backends once
    let mut backend_processor = BackendProcessor::new();
    backend_processor.process(config_path)?;

    for (i, ct) in config.client_types.iter().enumerate() {
        println!(
            "\n[{}/{}] Generating {:?} → {}",
            i + 1,
            config.client_types.len(),
            ct.format,
            ct.output
        );

        let input_path = base_dir.join(&resolved.schema);
        let append_paths: Vec<PathBuf> = Vec::new();
        let output_path = base_dir.join(&ct.output);

        // Dart uses spooky_core's richer generator (typed client / Patch classes /
        // spookySchema + surqlSchema), NOT the CLI's quicktype output. It's a Dart
        // tool that resolves `spooky_core` from the enclosing Dart package, so it
        // runs from the package dir — auto-derived from `output` (nearest ancestor
        // with a pubspec.yaml), or the optional `workdir` override.
        if ct.format == backend::ClientFormat::Dart {
            let workdir = ct.workdir.as_deref().map(|w| base_dir.join(w));
            run_spooky_gen_dart(&input_path, &output_path, workdir.as_deref())?;
            continue;
        }

        let output_format = match ct.format {
            backend::ClientFormat::Typescript => OutputFormat::Typescript,
            backend::ClientFormat::Dart => OutputFormat::Dart,
        };

        run_codegen(
            &input_path,
            &append_paths,
            &output_path,
            output_format,
            Some(config_path),
            &backend_processor,
            false,
            &DeployMode::Singlenode,
            None,
            None,
            Path::new("../../packages/surrealism-modules"),
            false,
        )?;

        // Query allowlist: run the app's query module against the schema just
        // generated and write the JSON that deploy/release/dev publish.
        if let Some((queries, out)) = ct.allowlist_paths(base_dir) {
            let app = ct
                .app
                .clone()
                .or_else(|| config.frontend().map(|(n, _)| n.to_string()))
                .unwrap_or_else(|| "app".to_string());
            println!("    Query allowlist: {} → {}", queries.display(), out.display());
            codegen::run_query_allowlist(&queries, &output_path, &out, &app)?;
        }
    }

    println!("\nAll clientTypes generated successfully.");
    Ok(())
}

/// Walk up from `start`'s directory to the nearest ancestor containing a
/// `pubspec.yaml` (the enclosing Dart package). `exists()` resolves any embedded
/// `..` against the filesystem, so a relative-joined path works fine.
fn nearest_dart_package(start: &Path) -> Option<PathBuf> {
    let mut dir = start.parent();
    while let Some(d) = dir {
        if d.join("pubspec.yaml").exists() {
            return Some(d.to_path_buf());
        }
        dir = d.parent();
    }
    None
}

/// Generate Dart types with spooky_core's `spooky_gen` (the rich typed client),
/// run via `dart run spooky_core:spooky_gen <schema> -o <output>`. `spooky_gen`
/// resolves `spooky_core` from the enclosing Dart package, so it must run from a
/// package dir — derived from `output`'s nearest ancestor with a `pubspec.yaml`,
/// or the explicit `workdir` override. Paths are absolute so they resolve
/// regardless of the command's working directory.
fn run_spooky_gen_dart(input: &Path, output: &Path, workdir: Option<&Path>) -> Result<()> {
    let cwd = std::env::current_dir().context("Failed to get current directory")?;
    let abs = |p: &Path| {
        if p.is_absolute() {
            p.to_path_buf()
        } else {
            cwd.join(p)
        }
    };
    let input_abs = abs(input);
    let output_abs = abs(output);

    if let Some(parent) = output_abs.parent() {
        fs::create_dir_all(parent).ok();
    }

    // Resolve the Dart package dir to run from: explicit override, else walk up
    // from the output file to the nearest pubspec.yaml.
    let workdir_abs = match workdir {
        Some(w) => abs(w),
        None => nearest_dart_package(&output_abs).with_context(|| {
            format!(
                "Could not find a pubspec.yaml above '{}' for the Dart generator; \
             set `workdir` on this clientTypes entry to a Dart package that depends on spooky_core",
                output.display()
            )
        })?,
    };

    let status = std::process::Command::new("dart")
        .args([
            "run",
            "spooky_core:spooky_gen",
            &input_abs.to_string_lossy(),
            "-o",
            &output_abs.to_string_lossy(),
        ])
        .current_dir(&workdir_abs)
        .status()
        .context(
            "Failed to run `dart run spooky_core:spooky_gen` — is the Dart SDK installed \
             and is `workdir` a Dart package that depends on spooky_core?",
        )?;
    if !status.success() {
        anyhow::bail!("spooky_gen failed for {}", output.display());
    }

    // Best-effort format (matches the flutter_app Makefile); ignore failures.
    let _ = std::process::Command::new("dart")
        .args(["format", &output_abs.to_string_lossy()])
        .current_dir(&workdir_abs)
        .status();
    Ok(())
}

/// Drive `spky generate`. With no explicit `--input`/`--output` this runs in
/// config-driven mode (every `clientTypes` entry in sp00ky.yml); with them it
/// generates a single file (the former implicit root-codegen path).
fn run_generate(gen: GenerateArgs) -> Result<()> {
    if gen.input.is_none() && gen.output.is_none() {
        let resolved_config = gen
            .config
            .clone()
            .unwrap_or_else(|| PathBuf::from(DEFAULT_CONFIG_PATH));
        return handle_generate(&resolved_config);
    }

    let cli_mode = match gen.mode.as_str() {
        "cluster" => DeployMode::Cluster,
        "surrealism" => DeployMode::Surrealism,
        _ => DeployMode::Singlenode,
    };
    if cli_mode == DeployMode::Surrealism {
        eprintln!("Warning: Surrealism mode is not supported yet.");
        std::process::exit(1);
    }

    let input_path = gen.input.as_ref().context(
        "--input is required for single-file generation (omit it to generate every sp00ky.yml `clientTypes` entry)",
    )?;
    let output_path = gen
        .output
        .as_ref()
        .context("--output is required for single-file generation")?;

    let output_format = if let Some(format_str) = &gen.format {
        match format_str.to_lowercase().as_str() {
            "json" => OutputFormat::JsonSchema,
            "typescript" | "ts" => OutputFormat::Typescript,
            "dart" => OutputFormat::Dart,
            "surql" => OutputFormat::Surql,
            _ => anyhow::bail!(
                "Unknown format: {}. Supported formats: json, typescript, dart, surql",
                format_str
            ),
        }
    } else {
        OutputFormat::from_extension(output_path.to_str().unwrap_or(""))
            .unwrap_or(OutputFormat::JsonSchema)
    };

    let mut backend_processor = BackendProcessor::new();
    if let Some(config_path) = &gen.config {
        println!("Loading sp00ky config from {:?}", config_path);
        backend_processor.process(config_path)?;
    }

    let append_paths: Vec<PathBuf> = gen.append.iter().cloned().collect();

    run_codegen(
        input_path,
        &append_paths,
        output_path,
        output_format,
        gen.config.as_deref(),
        &backend_processor,
        gen.no_header,
        &cli_mode,
        gen.endpoint.as_deref(),
        gen.secret.as_deref(),
        &gen.modules_dir,
        gen.all,
    )
}

/// Print a "command moved" hint for a removed command and exit non-zero.
fn moved(old: &str, new: &str) -> ! {
    eprintln!("`spky {old}` has moved.");
    eprintln!("  use: spky {new}");
    eprintln!();
    eprintln!("Run `spky --help` for the new command layout.");
    std::process::exit(2);
}

/// Map an old `spky cloud <...>` invocation to its new top-level form and exit.
fn moved_cloud_hint(rest: &[String]) -> ! {
    let joined = rest.join(" ");
    let sub = rest.first().map(String::as_str).unwrap_or("");
    let tail = rest.get(1..).map(|s| s.join(" ")).unwrap_or_default();
    let trim = |s: String| s.trim().to_string();
    let new = match sub {
        "login" | "logout" | "deploy" | "status" | "logs" | "restart" | "domain" | "backup"
        | "team" => joined.clone(),
        "scale" => "scale ssp <N>".to_string(),
        "create" => "project create".to_string(),
        "list" => "project list".to_string(),
        "credentials" => "project credentials".to_string(),
        "destroy" => "project destroy".to_string(),
        "keys" => trim(format!("token {tail}")),
        "billing" => trim(format!("billing {}", tail.replace("change-plan", "plan"))),
        "link" => trim(format!(
            "link {}",
            tail.replace("setup", "connect")
                .replace("unlink", "disconnect")
        )),
        "env" => trim(format!(
            "env {}",
            tail.replace("init", "unlock")
                .replace("load", "pull")
                .replace("delete", "rm")
                .replace("change-passphrase", "passphrase")
                .replace("ci-access", "share-ci")
        )),
        "vault" => trim(format!(
            "env reset {}",
            tail.replace("request-reset", "request")
                .replace("approve-reset", "approve")
                .replace("complete-reset", "complete")
                .replace("list-resets", "list")
        )),
        "" => "deploy (also: env, domain, project, team, billing, link, token)".to_string(),
        _ => joined.clone(),
    };
    eprintln!("`spky cloud {joined}` has moved. The `cloud` namespace was removed.");
    eprintln!("  use: spky {new}");
    eprintln!();
    eprintln!("Run `spky --help` for the full command layout.");
    std::process::exit(2);
}

fn main() -> Result<()> {
    let args = Args::parse();
    ui::set_assume_yes(args.yes);

    if let Some(ref project_path) = args.path {
        std::env::set_current_dir(project_path).context(format!(
            "Failed to set project directory: {:?}",
            project_path
        ))?;
    }

    match args.command {
        // ── Develop ──────────────────────────────────────────────────────
        Some(Commands::Init) => create_project(),
        Some(Commands::Dev {
            skip_migrations,
            apply_migrations,
            fix_checksums,
            clean,
            clean_db,
            verbose,
        }) => {
            ui::init(ui::Options { verbose });
            dev::run(
                skip_migrations,
                apply_migrations,
                fix_checksums,
                clean,
                clean_db,
            )
        }
        Some(Commands::Generate { args: gen }) => run_generate(gen),
        Some(Commands::Migrate { action }) => handle_migrate(action),
        Some(Commands::Bucket { action }) => handle_bucket(action),
        Some(Commands::Api { action }) => handle_api(action),
        Some(Commands::Recipe {
            recipe,
            table,
            field,
            out,
        }) => scaffold::run(&recipe, table.as_deref(), field.as_deref(), out.as_deref()),
        Some(Commands::Lint { config }) => {
            let resolved_config = config.unwrap_or_else(|| PathBuf::from(DEFAULT_CONFIG_PATH));
            handle_lint(&resolved_config)
        }
        Some(Commands::Doctor { json }) => {
            let project_dir = args.path.as_deref().unwrap_or_else(|| Path::new("."));
            doctor::run(json, project_dir, false)
        }
        Some(Commands::Verify { fix }) => verify::run(fix),
        Some(Commands::Agents { action }) => {
            let project_dir = args.path.as_deref().unwrap_or_else(|| Path::new("."));
            match action {
                AgentsCommands::Init { force, out } => {
                    agents::init(force, out.as_deref(), project_dir)
                }
            }
        }
        Some(Commands::Mcp { action }) => match action {
            None | Some(McpCommands::Serve) => mcp::run(),
            Some(McpCommands::Token {
                name,
                scopes,
                read_only,
                install,
                client,
            }) => mcp_cloud::token(name, scopes, read_only, install, client, ui::assume_yes()),
            Some(McpCommands::Tokens) => mcp_cloud::list_tokens(),
            Some(McpCommands::Revoke { id }) => mcp_cloud::revoke(id),
            Some(McpCommands::Install { token, client }) => mcp_cloud::install(token, client),
        },

        // ── Deploy & operate (current project) ─────────────────────────────
        Some(Commands::Deploy {
            upgrade,
            clean,
            only,
            force_schema,
            cache_bust,
            mandatory,
        }) => {
            if force_schema {
                std::env::set_var("SPKY_FORCE_SCHEMA", "1");
            }
            cloud::deploy(upgrade, clean, only, cache_bust, mandatory)
        }
        Some(Commands::Release {
            app,
            version,
            cache_bust,
            mandatory,
        }) => cloud::release(app, version, cache_bust, mandatory),
        Some(Commands::Status) => cloud::status(),
        Some(Commands::Stats { window, filter }) => cloud::stats(window, filter),
        Some(Commands::Logs {
            filter,
            split,
            since,
            until,
            grep,
            interactive,
            follow,
            service,
        }) => cloud::logs(cloud::LogsArgs {
            filter,
            split,
            since,
            until,
            grep,
            interactive,
            follow,
            service,
        }),
        Some(Commands::Restart {
            targets,
            all_backends,
            clean,
            upgrade,
            surreal,
        }) => cloud::restart(targets, all_backends, clean, upgrade, surreal),
        Some(Commands::Push) => cloud::push(),
        Some(Commands::Scale { action }) => match action {
            ScaleCommands::Ssp { count } => cloud::scale(count),
        },
        Some(Commands::Login) => cloud::login(),
        Some(Commands::Logout) => cloud::logout(),

        // ── Resources ──────────────────────────────────────────────────────
        Some(Commands::Env { action }) => cloud::env_group(action),
        Some(Commands::Domain { action }) => cloud::domain(action),
        Some(Commands::Notice { message, notice_type, timeout, action }) => {
            cloud::notice(message, notice_type, timeout, action)
        }
        Some(Commands::Backup { action }) => cloud::backup(action),
        Some(Commands::Link { action }) => cloud::link(action),
        Some(Commands::Flag { action }) => flag::run(action),
        Some(Commands::Admin { action }) => admin::run(action),
        Some(Commands::Jobs {
            conn,
            config,
            action,
        }) => jobs::run(conn, config, action),
        Some(Commands::Schedules {
            conn,
            config,
            action,
        }) => schedules::run(conn, config, action),
        Some(Commands::Workflows {
            conn,
            config,
            action,
        }) => workflows::run(conn, config, action),
        Some(Commands::Query {
            query,
            json,
            conn,
            config,
        }) => query::run(query, json, conn, config),

        // ── Account ─────────────────────────────────────────────────────────
        Some(Commands::Project { action }) => match action {
            ProjectCommands::Create { slug, plan } => cloud::create(slug, plan),
            ProjectCommands::List => cloud::list(),
            ProjectCommands::Credentials { raw } => cloud::credentials(raw),
            ProjectCommands::Destroy { confirm } => cloud::destroy(confirm),
        },
        Some(Commands::Team { action }) => cloud::team(action),
        Some(Commands::Billing { action }) => cloud::billing(action),
        Some(Commands::Token { action }) => cloud::keys(action),

        Some(Commands::Version) => {
            println!("spky {}", env!("CARGO_PKG_VERSION"));
            Ok(())
        }

        // ── Removed commands: print a migration hint ────────────────────────
        Some(Commands::Cloud { rest }) => moved_cloud_hint(&rest),
        Some(Commands::CreateDeprecated) => moved("create", "init"),
        Some(Commands::SetupDeprecated) => moved("setup", "init"),
        Some(Commands::ScaffoldDeprecated { rest }) => {
            let tail = rest.join(" ");
            let new = if tail.is_empty() {
                "recipe <name>".to_string()
            } else {
                format!("recipe {tail}")
            };
            moved("scaffold", &new)
        }

        None => {
            // No subcommand: print help instead of the old implicit codegen.
            use clap::CommandFactory;
            Args::command().print_help().ok();
            println!();
            Ok(())
        }
    }
}

#[cfg(test)]
mod migration_tests {
    use crate::migration::sp00ky_engine::Sp00kyEngine;
    use crate::migration::{
        self, MigrationEngine, MigrationEnvironment, MigrationInfo, MigrationState,
    };
    use tempfile::TempDir;

    // ── Mock Engine ────────────────────────────────────────────────────

    struct MockEngine {
        fail_apply: bool,
    }

    impl MockEngine {
        fn new() -> Self {
            Self { fail_apply: false }
        }
        fn failing() -> Self {
            Self { fail_apply: true }
        }
    }

    impl MigrationEngine for MockEngine {
        fn apply(&self) -> anyhow::Result<()> {
            if self.fail_apply {
                anyhow::bail!("mock apply failure");
            }
            Ok(())
        }

        fn status(&self) -> anyhow::Result<Vec<MigrationInfo>> {
            Ok(vec![MigrationInfo {
                id: "20240101".into(),
                name: "test_migration".into(),
                state: MigrationState::Pending,
                applied_at: None,
                detail: None,
            }])
        }

        fn fix(&self, _fix_checksums: bool) -> anyhow::Result<()> {
            Ok(())
        }
    }

    fn wrap_mock(mock: MockEngine) -> Sp00kyEngine {
        Sp00kyEngine::new(
            Box::new(mock),
            "http://localhost:8000".into(),
            "ns".into(),
            "db".into(),
            "root".into(),
            "root".into(),
            None,
            None,
        )
    }

    // ── Factory tests ──────────────────────────────────────────────────

    fn make_ctx(tmp: &TempDir, surrealkit_binary: Option<String>) -> migration::MigrationContext {
        migration::MigrationContext {
            environment: MigrationEnvironment::Production,
            project_dir: tmp.path().to_path_buf(),
            migrations_dir: tmp.path().join("migrations"),
            url: "http://localhost:8000".into(),
            namespace: "test_ns".into(),
            database: "test_db".into(),
            username: "root".into(),
            password: "root".into(),
            surrealkit_binary,
            internal_schema: None,
            remote_functions: None,
            secrets: None,
        }
    }

    #[test]
    fn test_factory_selects_legacy_when_no_surrealkit() {
        let tmp = TempDir::new().unwrap();
        let engine = migration::create_engine(make_ctx(&tmp, None));
        assert!(engine.is_ok());
    }

    #[test]
    fn test_factory_fails_with_missing_surrealkit_binary() {
        let tmp = TempDir::new().unwrap();
        match migration::create_engine(make_ctx(&tmp, Some("nonexistent_xyz_12345".into()))) {
            Err(e) => assert!(e.to_string().contains("not found")),
            Ok(_) => panic!("should fail with missing binary"),
        }
    }

    // ── Decorator tests ────────────────────────────────────────────────

    #[test]
    fn test_decorator_apply_delegates_to_inner() {
        let engine = wrap_mock(MockEngine::new());
        engine.apply().unwrap();
    }

    #[test]
    fn test_decorator_apply_propagates_inner_error() {
        let engine = wrap_mock(MockEngine::failing());
        let err = engine.apply().unwrap_err();
        assert!(err.to_string().contains("mock apply failure"));
    }

    #[test]
    fn test_decorator_status_delegates_to_inner() {
        let engine = wrap_mock(MockEngine::new());
        let statuses = engine.status().unwrap();
        assert_eq!(statuses.len(), 1);
        assert_eq!(statuses[0].id, "20240101");
        assert_eq!(statuses[0].state, MigrationState::Pending);
    }

    #[test]
    fn test_decorator_fix_delegates() {
        wrap_mock(MockEngine::new()).fix(true).unwrap();
    }

    // ── SurrealKit adapter tests ───────────────────────────────────────

    #[test]
    fn test_surrealkit_fails_when_binary_not_found() {
        match crate::migration::surrealkit::SurrealKitEngine::new(
            "nonexistent_xyz_12345".into(),
            MigrationEnvironment::Production,
            std::path::PathBuf::from("."),
            "http://localhost:8000".into(),
            "ns".into(),
            "db".into(),
            "root".into(),
            "root".into(),
        ) {
            Err(e) => assert!(e.to_string().contains("not found")),
            Ok(_) => panic!("should fail with missing binary"),
        }
    }

    #[test]
    fn test_surrealkit_succeeds_with_valid_binary() {
        let result = crate::migration::surrealkit::SurrealKitEngine::new(
            "echo".into(),
            MigrationEnvironment::Dev,
            std::path::PathBuf::from("."),
            "http://localhost:8000".into(),
            "ns".into(),
            "db".into(),
            "root".into(),
            "root".into(),
        );
        assert!(result.is_ok());
    }

    // ── Type tests ─────────────────────────────────────────────────────

    #[test]
    fn test_migration_state_equality() {
        assert_eq!(MigrationState::Applied, MigrationState::Applied);
        assert_ne!(MigrationState::Applied, MigrationState::Pending);
        assert_ne!(MigrationState::Pending, MigrationState::Drift);
    }

    // ── Dev flow integration tests ─────────────────────────────────────
    //
    // These simulate the exact logic from dev.rs::apply_migrations():
    //   1. create engine
    //   2. optionally fix checksums
    //   3. call status(), filter pending
    //   4. call apply()
    //   5. on failure: retry after reset

    /// Mock engine with configurable status results for flow testing.
    struct FlowMockEngine {
        statuses: Vec<MigrationInfo>,
        apply_result: std::cell::RefCell<Vec<anyhow::Result<()>>>,
        fix_called: std::cell::RefCell<bool>,
    }

    impl FlowMockEngine {
        fn with_pending(count: usize) -> Self {
            let statuses = (0..count)
                .map(|i| MigrationInfo {
                    id: format!("2024010{}120000", i + 1),
                    name: format!("migration_{}", i + 1),
                    state: MigrationState::Pending,
                    applied_at: None,
                    detail: None,
                })
                .collect();
            Self {
                statuses,
                apply_result: std::cell::RefCell::new(vec![Ok(())]),
                fix_called: std::cell::RefCell::new(false),
            }
        }

        fn all_applied() -> Self {
            Self {
                statuses: vec![MigrationInfo {
                    id: "20240101120000".into(),
                    name: "initial".into(),
                    state: MigrationState::Applied,
                    applied_at: Some("2024-01-01T12:00:00Z".into()),
                    detail: None,
                }],
                apply_result: std::cell::RefCell::new(vec![]),
                fix_called: std::cell::RefCell::new(false),
            }
        }

        fn with_drift() -> Self {
            Self {
                statuses: vec![
                    MigrationInfo {
                        id: "20240101120000".into(),
                        name: "initial".into(),
                        state: MigrationState::Drift,
                        applied_at: Some("2024-01-01T12:00:00Z".into()),
                        detail: Some("checksum mismatch".into()),
                    },
                    MigrationInfo {
                        id: "20240102120000".into(),
                        name: "second".into(),
                        state: MigrationState::Pending,
                        applied_at: None,
                        detail: None,
                    },
                ],
                apply_result: std::cell::RefCell::new(vec![Ok(())]),
                fix_called: std::cell::RefCell::new(false),
            }
        }

        fn failing_then_succeeding() -> Self {
            Self {
                statuses: vec![MigrationInfo {
                    id: "20240101120000".into(),
                    name: "initial".into(),
                    state: MigrationState::Pending,
                    applied_at: None,
                    detail: None,
                }],
                apply_result: std::cell::RefCell::new(vec![
                    Err(anyhow::anyhow!("migration failed: table already exists")),
                    Ok(()),
                ]),
                fix_called: std::cell::RefCell::new(false),
            }
        }
    }

    impl MigrationEngine for FlowMockEngine {
        fn apply(&self) -> anyhow::Result<()> {
            let mut results = self.apply_result.borrow_mut();
            if results.is_empty() {
                Ok(())
            } else {
                results.remove(0)
            }
        }

        fn status(&self) -> anyhow::Result<Vec<MigrationInfo>> {
            Ok(self.statuses.clone())
        }

        fn fix(&self, _fix_checksums: bool) -> anyhow::Result<()> {
            *self.fix_called.borrow_mut() = true;
            Ok(())
        }
    }

    /// Simulates the dev.rs apply_migrations flow with a mock engine.
    fn simulate_dev_flow(
        engine: &dyn MigrationEngine,
        auto_apply: bool,
        fix_checksums: bool,
    ) -> anyhow::Result<String> {
        let mut output = String::new();

        // Step 1: Fix checksums if requested (mirrors dev.rs line 692)
        if fix_checksums {
            if let Err(e) = engine.fix(true) {
                output.push_str(&format!("checksum fix failed: {:#}\n", e));
            }
        }

        // Step 2: Get status (mirrors dev.rs line 699)
        let statuses = engine.status()?;

        // Step 3: Report drift (mirrors dev.rs line 708)
        for info in &statuses {
            if info.state == MigrationState::Drift {
                output.push_str(&format!("DRIFT: {}_{}\n", info.id, info.name));
            }
        }

        // Step 4: Filter pending (mirrors dev.rs line 715)
        let pending: Vec<_> = statuses
            .iter()
            .filter(|s| s.state == MigrationState::Pending)
            .collect();

        if pending.is_empty() {
            output.push_str("no_pending\n");
            return Ok(output);
        }

        output.push_str(&format!("pending:{}\n", pending.len()));

        // Step 5: Apply (mirrors dev.rs line 754)
        // In non-TTY/auto mode, always apply
        if auto_apply {
            match engine.apply() {
                Ok(()) => output.push_str("applied\n"),
                Err(e) => {
                    output.push_str(&format!("apply_failed:{}\n", e));
                    // Retry after reset (mirrors dev.rs line 768)
                    match engine.apply() {
                        Ok(()) => output.push_str("retry_applied\n"),
                        Err(e2) => output.push_str(&format!("retry_failed:{}\n", e2)),
                    }
                }
            }
        }

        Ok(output)
    }

    /// Simulates the cloud.rs deploy migration flow with a mock engine.
    fn simulate_cloud_flow(engine: &dyn MigrationEngine) -> String {
        // Cloud flow is simple: just apply (mirrors cloud.rs line 1719 / 2917)
        match engine.apply() {
            Ok(_) => "migrations_complete".into(),
            Err(e) => format!("migration_warning:{}", e),
        }
    }

    #[test]
    fn test_dev_flow_no_pending_skips_apply() {
        let engine = FlowMockEngine::all_applied();
        let output = simulate_dev_flow(&engine, true, false).unwrap();
        assert!(output.contains("no_pending"));
        assert!(!output.contains("applied"));
    }

    #[test]
    fn test_dev_flow_pending_migrations_applied() {
        let engine = FlowMockEngine::with_pending(3);
        let output = simulate_dev_flow(&engine, true, false).unwrap();
        assert!(output.contains("pending:3"));
        assert!(output.contains("applied"));
    }

    #[test]
    fn test_dev_flow_drift_reported_then_pending_applied() {
        let engine = FlowMockEngine::with_drift();
        let output = simulate_dev_flow(&engine, true, false).unwrap();
        assert!(output.contains("DRIFT: 20240101120000_initial"));
        assert!(output.contains("pending:1"));
        assert!(output.contains("applied"));
    }

    #[test]
    fn test_dev_flow_fix_checksums_called_when_requested() {
        let engine = FlowMockEngine::with_pending(1);
        let _ = simulate_dev_flow(&engine, true, true).unwrap();
        assert!(*engine.fix_called.borrow());
    }

    #[test]
    fn test_dev_flow_fix_checksums_not_called_by_default() {
        let engine = FlowMockEngine::with_pending(1);
        let _ = simulate_dev_flow(&engine, true, false).unwrap();
        assert!(!*engine.fix_called.borrow());
    }

    #[test]
    fn test_dev_flow_apply_failure_triggers_retry() {
        let engine = FlowMockEngine::failing_then_succeeding();
        let output = simulate_dev_flow(&engine, true, false).unwrap();
        assert!(output.contains("apply_failed:"));
        assert!(output.contains("retry_applied"));
    }

    #[test]
    fn test_cloud_flow_success() {
        let engine = FlowMockEngine::with_pending(2);
        let output = simulate_cloud_flow(&engine);
        assert_eq!(output, "migrations_complete");
    }

    #[test]
    fn test_cloud_flow_failure_returns_warning() {
        let engine = FlowMockEngine::failing_then_succeeding();
        let output = simulate_cloud_flow(&engine);
        assert!(output.starts_with("migration_warning:"));
        assert!(output.contains("table already exists"));
    }

    #[test]
    fn test_cloud_flow_with_all_applied() {
        let engine = FlowMockEngine::all_applied();
        let output = simulate_cloud_flow(&engine);
        assert_eq!(output, "migrations_complete");
    }
}

#[cfg(test)]
mod make_field_nullable_tests {
    use super::make_field_nullable;

    #[test]
    fn wraps_plain_type() {
        let line = "DEFINE FIELD username ON TABLE user TYPE string";
        assert_eq!(
            make_field_nullable(line),
            "DEFINE FIELD username ON TABLE user TYPE option<string>"
        );
    }

    #[test]
    fn keeps_default_clause_outside_option() {
        let line = "DEFINE FIELD active ON TABLE user TYPE bool DEFAULT false";
        assert_eq!(
            make_field_nullable(line),
            "DEFINE FIELD active ON TABLE user TYPE option<bool> DEFAULT false"
        );
    }

    // Regression: clause order `DEFAULT ... PERMISSIONS ...` must not pull the
    // DEFAULT into the type, which produced the invalid `option<bool DEFAULT
    // false>` and a SurrealDB parse error at client DB init.
    #[test]
    fn default_before_permissions_does_not_leak_into_type() {
        let line = "DEFINE FIELD active ON TABLE user TYPE bool DEFAULT false PERMISSIONS FOR update WHERE $access = \"account\"";
        assert_eq!(
            make_field_nullable(line),
            "DEFINE FIELD active ON TABLE user TYPE option<bool> DEFAULT false PERMISSIONS FOR update WHERE $access = \"account\""
        );
    }

    #[test]
    fn permissions_only_stays_outside_option() {
        let line = "DEFINE FIELD active ON TABLE user TYPE bool PERMISSIONS FOR update WHERE $access = \"account\"";
        assert_eq!(
            make_field_nullable(line),
            "DEFINE FIELD active ON TABLE user TYPE option<bool> PERMISSIONS FOR update WHERE $access = \"account\""
        );
    }

    #[test]
    fn trailing_semicolon_is_preserved() {
        let line = "DEFINE FIELD active ON TABLE user TYPE bool DEFAULT false;";
        assert_eq!(
            make_field_nullable(line),
            "DEFINE FIELD active ON TABLE user TYPE option<bool> DEFAULT false;"
        );
    }

    #[test]
    fn already_optional_is_left_alone() {
        let line = "DEFINE FIELD bio ON TABLE user TYPE option<string> DEFAULT NONE";
        assert_eq!(make_field_nullable(line), line);
    }

    #[test]
    fn any_type_is_not_wrapped() {
        let line = "DEFINE FIELD payload ON TABLE event TYPE any";
        assert_eq!(make_field_nullable(line), line);
    }
}

#[cfg(test)]
mod client_schema_value_strip_tests {
    use super::{filter_schema_for_client, strip_value_clause};
    use crate::parser::SchemaParser;

    #[test]
    fn strips_value_at_end_of_statement() {
        let s = "DEFINE FIELD created_at ON TABLE comment TYPE option<datetime> VALUE time::now()";
        assert_eq!(
            strip_value_clause(s),
            "DEFINE FIELD created_at ON TABLE comment TYPE option<datetime>"
        );
    }

    #[test]
    fn strips_value_but_keeps_following_assert_and_permissions() {
        let s = "DEFINE FIELD content ON TABLE comment TYPE option<string> VALUE string::trim($value) ASSERT $value != NONE PERMISSIONS FULL";
        assert_eq!(
            strip_value_clause(s),
            "DEFINE FIELD content ON TABLE comment TYPE option<string> ASSERT $value != NONE PERMISSIONS FULL"
        );
    }

    #[test]
    fn leaves_statements_without_value_untouched() {
        let s = "DEFINE FIELD title ON TABLE thread TYPE option<string> PERMISSIONS FULL";
        assert_eq!(strip_value_clause(s), s);
    }

    // The production bug: a `VALUE time::now()` on the CLIENT schema re-evaluates
    // on every cache UPSERT, so every synced row's `created_at` becomes the sync
    // time (all threads/comments show the same date). The client-schema filter
    // must drop the VALUE clause — even when it sits on its own line.
    #[test]
    fn filter_drops_multiline_value_timenow_from_client_schema() {
        let src = "DEFINE TABLE comment SCHEMAFULL PERMISSIONS FULL;\n\
                   DEFINE FIELD created_at ON TABLE comment TYPE datetime\n    \
                   VALUE time::now();\n\
                   DEFINE FIELD content ON TABLE comment TYPE string;\n";
        let mut parser = SchemaParser::new();
        parser.parse_file(src).unwrap();
        let out = filter_schema_for_client(src, &parser).unwrap();

        assert!(
            !out.to_uppercase().contains("VALUE TIME::NOW")
                && !out.to_uppercase().contains("VALUE  TIME"),
            "client schema must not carry `VALUE time::now()`; got:\n{out}"
        );
        // The field itself must survive (nullable-wrapped), just without VALUE.
        assert!(
            out.contains("DEFINE FIELD created_at ON TABLE comment TYPE option<datetime>"),
            "created_at field must remain (sans VALUE); got:\n{out}"
        );
    }

    fn client_schema(src: &str) -> String {
        let mut parser = SchemaParser::new();
        parser.parse_file(src).unwrap();
        filter_schema_for_client(src, &parser).unwrap()
    }

    #[test]
    fn nosync_field_and_its_annotation_are_both_removed() {
        // The orphan matters as much as the field: a stray `-- @nosync` in the
        // client schema re-attaches to the next statement. The Dart parser marks
        // the following DEFINE TABLE server-only and drops that whole table from
        // generated types.
        let out = client_schema(
            "DEFINE TABLE user SCHEMAFULL PERMISSIONS FULL;\n\
             DEFINE FIELD email ON TABLE user TYPE string;\n\
             -- @nosync\n\
             DEFINE FIELD secret_token ON TABLE user TYPE string;\n\
             DEFINE TABLE game SCHEMAFULL PERMISSIONS FULL;\n",
        );
        assert!(!out.contains("secret_token"), "field must be gone:\n{out}");
        assert!(!out.contains("@nosync"), "orphan comment must be gone:\n{out}");
        assert!(out.contains("DEFINE TABLE game"), "game must survive:\n{out}");
        assert!(out.contains("email"), "email must survive:\n{out}");
    }

    #[test]
    fn a_whole_annotation_comment_block_is_removed_with_the_field() {
        // `-- @nosync` / prose / DEFINE FIELD: popping only annotation lines
        // would stop at the prose and strand the marker.
        let out = client_schema(
            "DEFINE TABLE user SCHEMAFULL PERMISSIONS FULL;\n\
             -- @nosync\n\
             -- the API token, server-side bookkeeping only\n\
             DEFINE FIELD secret_token ON TABLE user TYPE string;\n\
             DEFINE TABLE game SCHEMAFULL PERMISSIONS FULL;\n",
        );
        assert!(!out.contains("@nosync"), "got:\n{out}");
        assert!(!out.contains("API token"), "got:\n{out}");
        assert!(out.contains("DEFINE TABLE game"), "got:\n{out}");
    }

    #[test]
    fn plain_prose_above_a_permission_stripped_field_is_kept() {
        // Only annotation blocks are popped, so ordinary documentation attached
        // to a `FOR select WHERE false` field is not collateral damage.
        let out = client_schema(
            "DEFINE TABLE user SCHEMAFULL PERMISSIONS FULL;\n\
             -- internal bookkeeping flag\n\
             DEFINE FIELD flag ON TABLE user TYPE bool PERMISSIONS FOR select WHERE false;\n",
        );
        assert!(!out.contains("DEFINE FIELD flag"), "got:\n{out}");
        assert!(out.contains("internal bookkeeping flag"), "got:\n{out}");
    }

    #[test]
    fn opaque_field_stays_in_the_client_schema() {
        // The defining difference from @nosync: @opaque is synced, so the client
        // needs the local column to store it in.
        let out = client_schema(
            "DEFINE TABLE user SCHEMAFULL PERMISSIONS FULL;\n\
             -- @opaque\n\
             DEFINE FIELD blob ON TABLE user TYPE bytes;\n",
        );
        assert!(
            out.contains("DEFINE FIELD blob ON TABLE user TYPE option<bytes>"),
            "@opaque field must remain, nullable-wrapped:\n{out}"
        );
    }

    #[test]
    fn nosync_table_removal_also_drops_its_annotation() {
        let out = client_schema(
            "DEFINE TABLE user SCHEMAFULL PERMISSIONS FULL;\n\
             -- @nosync\n\
             DEFINE TABLE audit_log SCHEMALESS;\n\
             DEFINE FIELD action ON TABLE audit_log TYPE string;\n\
             DEFINE TABLE game SCHEMAFULL PERMISSIONS FULL;\n",
        );
        assert!(!out.contains("audit_log"), "got:\n{out}");
        assert!(!out.contains("@nosync"), "orphan comment must be gone:\n{out}");
        assert!(out.contains("DEFINE TABLE game"), "got:\n{out}");
    }
}
