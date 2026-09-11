use anyhow::{bail, Context, Result};
use inquire::{Confirm, Text};
use regex::Regex;
use std::fs;
use std::path::{Path, PathBuf};

use crate::backend::{
    AppConfig, AppType, AuthConfig, AuthType, BackendMethod, MethodType, YAML_SCHEMA_COMMENT,
};

// ── Outbox schema template ──────────────────────────────────────────────────

fn outbox_template(table_name: &str) -> String {
    format!(
        r#"-- ##################################################################
-- API OUTBOX TABLE
-- ##################################################################

DEFINE TABLE {table} SCHEMAFULL
PERMISSIONS
  FOR select, create, update, delete WHERE true;

-- The domain record this job belongs to, used by row-level permissions
-- (`assigned_to.author.id = $auth.id`). OPTIONAL on purpose: a server-side
-- schedule (`schedules:` in sp00ky.yml) spawns system-initiated jobs that belong
-- to no single record, so the scheduler creates rows without it. Requiring it
-- makes every scheduled fire fail with "Expected `record` but found `NONE`".
DEFINE FIELD assigned_to ON TABLE {table} TYPE option<record>
PERMISSIONS
  FOR create, select WHERE true
  FOR update WHERE false;

DEFINE FIELD path ON TABLE {table} TYPE string
PERMISSIONS
  FOR create, select WHERE true
  FOR update WHERE false;

DEFINE FIELD payload ON TABLE {table} TYPE any
PERMISSIONS
  FOR create, select WHERE true
  FOR update WHERE false;

DEFINE FIELD retries ON TABLE {table} TYPE int DEFAULT ALWAYS 0
PERMISSIONS
  FOR create, select WHERE true
  FOR update WHERE false;

DEFINE FIELD max_retries ON TABLE {table} TYPE int DEFAULT ALWAYS 3;

DEFINE FIELD retry_strategy ON TABLE {table} TYPE string DEFAULT ALWAYS "linear"
ASSERT $value IN ["linear", "exponential"]
PERMISSIONS
  FOR create, select WHERE true
  FOR update WHERE false;

DEFINE FIELD status ON TABLE {table} TYPE string DEFAULT ALWAYS "pending"
ASSERT $value IN ["pending", "processing", "success", "failed"]
PERMISSIONS
  FOR create, select WHERE true
  FOR update WHERE false;

DEFINE FIELD errors ON TABLE {table} TYPE array<object> DEFAULT ALWAYS []
PERMISSIONS
  FOR create WHERE true
  FOR select, update WHERE false;

-- The element must be FLEXIBLE or a SCHEMAFULL table rejects the runner's
-- `{{ code, reason }}` entries as unknown fields (`errors[0].code`).
DEFINE FIELD errors[*] ON TABLE {table} TYPE object FLEXIBLE;

-- Set on create, and thereafter only when a writer sets it explicitly. Every
-- platform write does (`UPDATE ... SET status = ..., updated_at = time::now()`),
-- and the recovery sweeps' staleness clocks read it -- so it must NOT be `VALUE
-- time::now()`: the SSP's assignee stamp deliberately leaves this field alone so
-- claiming a job does not reset how long it has looked stuck.
DEFINE FIELD updated_at ON TABLE {table} TYPE datetime
DEFAULT ALWAYS time::now()
PERMISSIONS
  FOR create, select WHERE true
  FOR update WHERE false;

-- `DEFAULT`, deliberately NOT `VALUE`. A `VALUE time::now()` field is recomputed
-- on every UPDATE, so `created_at` would silently mean "last modified": the
-- delay window (`created_at + delay`, the job-due clause both recovery sweeps
-- share) would slide forward on any write to a pending row, and `spky jobs`
-- would report ages from the last write instead of from creation.
DEFINE FIELD created_at ON TABLE {table} TYPE datetime
DEFAULT time::now()
PERMISSIONS
  FOR create, select WHERE true
  FOR update WHERE false;

-- Platform claim marker: the sp00ky SSP stamps the owning SSP instance
-- (UPDATE ... SET assignee = "ssp-0") when it picks up or recovers a job.
-- Written by the platform as root, never by clients. Deploys also inject it
-- with IF NOT EXISTS as a safety net, but keep it here so the schema file
-- documents the full shape of the table.
DEFINE FIELD assignee ON TABLE {table} TYPE option<string>
PERMISSIONS
  FOR select WHERE true
  FOR create, update WHERE false;

-- Lease expiry for the attempt currently holding this row, and the fencing token
-- that attempt must present to write its outcome. Platform-written (root).
--
-- `assignee` above says WHO claimed the row; these say UNTIL WHEN, which is the
-- part that makes a stuck job recoverable. Reclaim keys off `lease_until` alone --
-- never off whether the assignee still looks alive -- because "the owner is up"
-- and "the work is progressing" are different questions, and a process that
-- restarts under a stable SPKY_SSP_ID answers the first one yes while abandoning
-- every row it held.
--
-- `lease_epoch` is bumped on every claim AND on every reclaim, and each of the
-- runner's writes is guarded on the epoch it claimed under. So when a reclaimed
-- job is re-run and the ORIGINAL attempt later wakes up, its write matches no row
-- instead of overwriting the new one. Without it, reclaiming would trade a stuck
-- job for a corrupted one.
--
-- Both `option<T>`: DEFAULT only fills a CREATE while every UPDATE re-validates
-- the whole row, so a non-option field would break every write to a row created
-- before the field existed. Readers treat NONE as "no lease" / epoch 0.
DEFINE FIELD lease_until ON TABLE {table} TYPE option<datetime>
PERMISSIONS
  FOR select WHERE true
  FOR create, update WHERE false;

DEFINE FIELD lease_epoch ON TABLE {table} TYPE option<int>
PERMISSIONS
  FOR select WHERE true
  FOR create, update WHERE false;

-- The backend's response body on success, stored by the job runner. Readable so
-- you can inspect job output (`spky jobs get`), and it is what a scheduled
-- workflow hands to the steps that depend on this one. Platform-written.
DEFINE FIELD result ON TABLE {table} TYPE any
PERMISSIONS
  FOR select WHERE true
  FOR create, update WHERE false;

-- Per-job HTTP timeout in SECONDS, overriding the backend default. Set by
-- `db.run(..., {{ timeout }})` and by a schedule's `timeout:` (the CLI converts
-- that duration to seconds at deploy time). Seconds, not milliseconds -- this
-- comment used to say ms and every consumer has always read it as seconds
-- (`BackendInfo::effective_timeout` -> `Duration::from_secs`).
--
-- The unit is load-bearing now that it also sizes the reclaim lease below: a
-- value written as milliseconds would mint a lease thousands of times too long
-- and make the row unreclaimable for months.
--
-- The field must exist: the scheduler puts it in the job content whenever the
-- schedule declares one, and a SCHEMAFULL table without it rejects the whole
-- CREATE with "Found field 'timeout', but no such field exists".
DEFINE FIELD timeout ON TABLE {table} TYPE option<int>
PERMISSIONS
  FOR create, select WHERE true
  FOR update WHERE false;

-- One-shot delay in ms: the job becomes due at `created_at + delay`
-- (`db.run(..., {{ delay }})`). Recurring work is no longer a field on this
-- table — declare it in sp00ky.yml under `schedules:` and the server-side
-- scheduler creates a fresh row here each cycle.
DEFINE FIELD delay ON TABLE {table} TYPE option<int>
PERMISSIONS
  FOR create, select WHERE true
  FOR update WHERE false;

-- ── Indexes ────────────────────────────────────────────────────────────────
--
-- `(status, created_at)` is the shape the dispatcher's drain asks for on EVERY
-- job completion (`SELECT ... WHERE status = 'pending' ORDER BY created_at`),
-- and the shape the retention prune and the admin plane's queue counts ask for
-- too. Without it all three are full scans of a table that, on a busy project,
-- is the largest one there is.
--
-- `updated_at` carries the other two: the prune ages terminal rows by their last
-- write, and the Jobs page lists by it.
--
-- `IF NOT EXISTS`, never `OVERWRITE`: an `OVERWRITE` here would rebuild both
-- indexes on every single `spky deploy`.
DEFINE INDEX IF NOT EXISTS idx_{table}_dispatch ON TABLE {table} COLUMNS status, created_at;
DEFINE INDEX IF NOT EXISTS idx_{table}_activity ON TABLE {table} COLUMNS updated_at;
"#,
        table = table_name
    )
}

/// The scaffold's DDL, for `schedule_sync`'s test that deploy defines the same
/// indexes this does. Two places write outbox index definitions and they have to
/// agree; this is what lets a test say so.
#[cfg(test)]
pub fn outbox_template_for_test(table_name: &str) -> String {
    outbox_template(table_name)
}

// ── Validation ──────────────────────────────────────────────────────────────

fn validate_identifier(name: &str) -> Result<()> {
    let re = Regex::new(r"^[a-z][a-z0-9_]*$")?;
    if !re.is_match(name) {
        bail!(
            "Not a valid SurrealDB identifier: '{}'. Must be lowercase letters, digits, and underscores, starting with a letter.",
            name
        );
    }
    Ok(())
}

// ── Public entry point ──────────────────────────────────────────────────────

pub fn add_api(
    spec: Option<String>,
    name: Option<String>,
    base_url: Option<String>,
    auth_type: Option<String>,
    auth_token: Option<String>,
    table: Option<String>,
    schema_path: Option<String>,
    config: PathBuf,
) -> Result<()> {
    // Step 1: Locate sp00ky.yml
    let config_path = if config.exists() {
        config.clone()
    } else {
        let cwd_config = PathBuf::from("sp00ky.yml");
        if cwd_config.exists() {
            cwd_config
        } else {
            let input = Text::new("Path to sp00ky.yml:")
                .with_default("sp00ky.yml")
                .with_help_message("Will be created if it doesn't exist")
                .prompt()?;
            PathBuf::from(input)
        }
    };

    // Step 2: Load or create config. We work at the raw-YAML level (not the
    // typed `Sp00kyConfig`) so a project that pulls apps in via
    // `apps.<name>.path` — whose reference-only entries would not deserialize
    // into a strict `AppConfig` — round-trips untouched and is never inlined.
    let mut root: serde_yaml::Value = if config_path.exists() {
        let content = fs::read_to_string(&config_path)
            .context(format!("Failed to read config: {:?}", config_path))?;
        serde_yaml::from_str(&content).context("Failed to parse sp00ky.yml")?
    } else {
        serde_yaml::Value::Mapping(serde_yaml::Mapping::new())
    };
    if !matches!(root, serde_yaml::Value::Mapping(_)) {
        root = serde_yaml::Value::Mapping(serde_yaml::Mapping::new());
    }

    // Step 3: OpenAPI spec path
    let spec_path_str = if let Some(s) = spec {
        s
    } else {
        Text::new("Path to OpenAPI spec:")
            .with_help_message("Relative to sp00ky.yml (e.g. ../api/openapi.yml)")
            .prompt()?
    };

    // Resolve relative to config directory for validation
    let config_dir = config_path.parent().unwrap_or(Path::new("."));
    let resolved_spec = config_dir.join(&spec_path_str);

    // Sanity check: spec file exists
    let spec_content = fs::read_to_string(&resolved_spec)
        .context(format!("Failed to read OpenAPI spec: {:?}", resolved_spec))?;

    // Sanity check: spec parses as YAML
    let openapi: openapiv3::OpenAPI =
        serde_yaml::from_str(&spec_content).context("OpenAPI spec is not valid YAML/JSON")?;

    // Sanity check: spec has at least one path
    if openapi.paths.paths.is_empty() {
        bail!("OpenAPI spec has no endpoints defined");
    }

    // Step 4: Backend name
    let backend_name = if let Some(n) = name {
        n
    } else {
        Text::new("Backend name:")
            .with_default("api")
            .with_help_message("Used as the key in sp00ky.yml apps section")
            .prompt()?
    };

    // Sanity check: no duplicate
    if root
        .get("apps")
        .and_then(|a| a.get(backend_name.as_str()))
        .is_some()
    {
        bail!("App '{}' already exists in sp00ky.yml", backend_name);
    }

    // Step 5: Base URL
    let base_url_val = if let Some(u) = base_url {
        u
    } else {
        Text::new("Base URL:")
            .with_default("http://localhost:3000")
            .with_help_message("The API server base URL")
            .prompt()?
    };

    // Step 6: Auth
    let auth_config = if let Some(_at) = auth_type {
        Some(AuthConfig {
            auth_type: AuthType::Token,
            token: auth_token,
        })
    } else {
        let needs_auth = Confirm::new("Does this API require authentication?")
            .with_default(false)
            .prompt()?;

        if needs_auth {
            let token = Text::new("Auth token:")
                .with_help_message("Bearer token for API authentication")
                .prompt()?;
            Some(AuthConfig {
                auth_type: AuthType::Token,
                token: if token.is_empty() { None } else { Some(token) },
            })
        } else {
            None
        }
    };

    // Step 7: Outbox table name
    let table_name = if let Some(t) = table {
        validate_identifier(&t)?;
        t
    } else {
        let input = Text::new("Outbox table name:")
            .with_default("job")
            .with_help_message("SurrealDB table for the outbox queue")
            .prompt()?;
        validate_identifier(&input)?;
        input
    };

    // Step 8: Schema output path
    let default_schema_path = format!("./src/outbox/{}.surql", backend_name);
    let schema_output_str = if let Some(sp) = schema_path {
        sp
    } else {
        Text::new("Schema output path:")
            .with_default(&default_schema_path)
            .with_help_message("Where to write the outbox .surql file (relative to sp00ky.yml)")
            .prompt()?
    };

    let resolved_schema_output = config_dir.join(&schema_output_str);

    // Sanity check: schema file doesn't already exist (or confirm overwrite)
    if resolved_schema_output.exists() {
        let overwrite = Confirm::new(&format!(
            "{} already exists. Overwrite?",
            resolved_schema_output.display()
        ))
        .with_default(false)
        .prompt()?;

        if !overwrite {
            println!("  Aborted.");
            return Ok(());
        }
    }

    // ── Actions ─────────────────────────────────────────────────────────────

    // 1. Generate and write outbox schema
    let surql_content = outbox_template(&table_name);

    if let Some(parent) = resolved_schema_output.parent() {
        fs::create_dir_all(parent).context(format!("Failed to create directory: {:?}", parent))?;
    }

    fs::write(&resolved_schema_output, &surql_content).context(format!(
        "Failed to write schema: {:?}",
        resolved_schema_output
    ))?;

    // 2. Update sp00ky.yml
    let new_app = AppConfig {
        app_type: AppType::Backend,
        scope: Default::default(),
        hosting: None,
        spec: Some(spec_path_str.clone()),
        base_url: Some(base_url_val.clone()),
        auth: auth_config,
        method: Some(BackendMethod {
            method_type: MethodType::Outbox,
            schema: schema_output_str.clone(),
            table: Some(table_name.clone()),
            // Scaffolded apps take the default (serial). Opting into
            // parallelism is a deliberate act, not a starter-template value.
            concurrency: None,
        }),
        image: None,
        ports: Vec::new(),
        args: Vec::new(),
        volumes: Vec::new(),
        workdir: None,
        depends_on: Vec::new(),
        healthcheck: None,
        dev: None,
        deploy: None,
        env: None,
    };

    // Insert the new app as a YAML node under `apps`, creating the map if the
    // file had none. Existing entries (including `path:` references) are left
    // byte-for-byte as authored.
    let new_app_value =
        serde_yaml::to_value(&new_app).context("Failed to serialize new app to YAML")?;
    if let serde_yaml::Value::Mapping(root_map) = &mut root {
        let apps_key = serde_yaml::Value::String("apps".to_string());
        let apps = root_map
            .entry(apps_key)
            .or_insert_with(|| serde_yaml::Value::Mapping(serde_yaml::Mapping::new()));
        if let serde_yaml::Value::Mapping(apps_map) = apps {
            apps_map.insert(
                serde_yaml::Value::String(backend_name.clone()),
                new_app_value,
            );
        } else {
            bail!("`apps` in sp00ky.yml is not a mapping");
        }
    }

    let yaml_output =
        serde_yaml::to_string(&root).context("Failed to serialize config to YAML")?;
    let yaml_output = format!("{}\n{}", YAML_SCHEMA_COMMENT, yaml_output);

    fs::write(&config_path, &yaml_output)
        .context(format!("Failed to write config: {:?}", config_path))?;

    // 3. Print summary
    println!();
    println!("  API Backend Added");
    println!("  ─────────────────────────────────");
    println!("  Name:        {}", backend_name);
    println!("  Spec:        {}", spec_path_str);
    println!("  Base URL:    {}", base_url_val);
    println!("  Table:       {}", table_name);
    println!("  Schema:      {}", schema_output_str);
    println!("  Config:      {} (updated)", config_path.display());
    println!();
    println!("  Run `sp00ky` to regenerate types with the new backend.");
    println!();

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Slice out one `DEFINE FIELD <name> ON TABLE <t> ...;` statement.
    fn field_def(ddl: &str, name: &str) -> String {
        let at = ddl
            .find(&format!("DEFINE FIELD {name} ON TABLE"))
            .unwrap_or_else(|| panic!("no `{name}` field in the outbox template"));
        let rest = &ddl[at..];
        rest[..rest.find(';').expect("field statement ends")].to_string()
    }

    /// `created_at` must never be a `VALUE` field.
    ///
    /// A `VALUE time::now()` field is recomputed on EVERY update, so `created_at`
    /// would silently mean "last modified". Two things read it and would be wrong:
    /// the job-due clause both recovery sweeps share (`created_at + delay`, so a
    /// delayed job's window slides forward on any write to the pending row) and
    /// `spky jobs`' age / oldest-pending columns.
    ///
    /// Nothing errors when this is wrong, which is exactly why it is pinned here.
    #[test]
    fn created_at_is_set_once_not_recomputed_on_every_update() {
        let created = field_def(&outbox_template("job"), "created_at");
        assert!(
            !created.contains("VALUE"),
            "created_at must not be a VALUE field, or it means \"last modified\": {created}"
        );
        assert!(
            created.contains("DEFAULT time::now()"),
            "created_at should default on create: {created}"
        );
    }

    /// `updated_at` must stay explicit-write-only — `DEFAULT ALWAYS`, never `VALUE`.
    ///
    /// Every platform transition sets it by hand, and the recovery sweeps use it as
    /// the staleness clock (`updated_at < time::now() - 30s`). The SSP's assignee
    /// stamp deliberately does NOT touch it so that claiming a job cannot reset how
    /// long that job has looked stuck. Making this `VALUE time::now()` would bump it
    /// on the assignee stamp and stop recovery from ever seeing a wedged job.
    #[test]
    fn updated_at_is_not_bumped_by_writes_that_leave_it_alone() {
        let updated = field_def(&outbox_template("job"), "updated_at");
        assert!(
            !updated.contains("VALUE"),
            "updated_at must not be a VALUE field, or the assignee stamp resets the \
             recovery staleness clock: {updated}"
        );
        assert!(updated.contains("DEFAULT ALWAYS time::now()"), "got: {updated}");
    }

    /// The statuses the template's ASSERT allows are exactly the four the runner
    /// writes. A status the runner writes but the ASSERT rejects is a silently
    /// dropped write on a SCHEMAFULL table.
    #[test]
    fn the_status_assert_covers_every_status_the_runner_writes() {
        let status = field_def(&outbox_template("job"), "status");
        for s in ["pending", "processing", "success", "failed"] {
            assert!(status.contains(s), "status ASSERT is missing '{s}': {status}");
        }
    }

    /// `errors[*]` has to be FLEXIBLE or the runner's `{ code, reason }` append is
    /// rejected by a SCHEMAFULL table — and the append is fire-and-forget, so the
    /// job still completes and the error history is just silently empty.
    #[test]
    fn error_entries_are_flexible_objects() {
        let ddl = outbox_template("job");
        assert!(ddl.contains("DEFINE FIELD errors[*] ON TABLE job TYPE object FLEXIBLE"));
    }

    /// The template documents the full shape of the table, so it must carry the lease
    /// fields even though every deploy also injects them. A new project generated from
    /// this template should not be relying on the healing path.
    ///
    /// `option<T>` rather than a bare type, for the reason `_00_workflow_run.retry_count`
    /// spells out: DEFAULT only fills a CREATE while every UPDATE re-validates the whole
    /// row, so a non-option field breaks every write to a row created before it existed.
    #[test]
    fn the_template_carries_the_lease_fields() {
        let ddl = outbox_template("job");
        assert!(ddl.contains("DEFINE FIELD lease_until ON TABLE job TYPE option<datetime>"));
        assert!(ddl.contains("DEFINE FIELD lease_epoch ON TABLE job TYPE option<int>"));
    }

    /// `timeout` is SECONDS. The comment used to say milliseconds while every consumer
    /// read it through `Duration::from_secs`, which was survivable while the value only
    /// bounded an HTTP request. It now also sizes the reclaim lease, so a value written
    /// in the wrong unit is the difference between a row recovering in a minute and
    /// staying unreclaimable for months.
    #[test]
    fn the_timeout_field_documents_its_unit_as_seconds() {
        let ddl = outbox_template("job");
        assert!(
            ddl.contains("Per-job HTTP timeout in SECONDS"),
            "the unit must be stated, and stated correctly"
        );
        assert!(!ddl.contains("timeout in ms"), "the old wrong unit must not come back");
    }
}
