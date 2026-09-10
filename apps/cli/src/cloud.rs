use std::fs;
use std::io::IsTerminal;
use std::path::PathBuf;
use std::thread;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use argon2::{Algorithm, Argon2, Params, Version};
use rand::RngCore;
use serde::{Deserialize, Serialize};

use crate::backend::{self, BackendDevConfig, BackendDevTypedConfig, DeployEnv, HostingMode};
use crate::surreal_client::MigrationDB;
use crate::{
    CloudBackupCommands, CloudBillingCommands, CloudDomainCommands, CloudKeyCommands,
    CloudLinkCommands, CloudTeamCommands, CloudVaultCommands, EnvCommands, EnvResetCommands,
    NoticeCommands,
};

/// Load vault environment variables for dev mode via the Cloud API.
/// Returns key-value pairs. Returns empty vec (with warning) if not logged in or vault not initialized.
pub fn load_vault_envs_for_dev() -> Vec<(String, String)> {
    load_vault_secrets(false)
}

/// Load PRODUCTION vault secrets as `(KEY, VALUE)` pairs for `{{KEY}}` placeholder
/// substitution in migrations at apply time (`spky migrate prod` + cloud deploy).
/// Same resolution as dev (login creds + project slug + vault passphrase) but the
/// production environment. Returns empty (with a warning) on any failure; the
/// apply layer then fails loudly on any unresolved placeholder, so an empty result
/// never silently writes a literal `{{...}}` (e.g. a broken JWT signing key).
pub fn load_vault_secrets_for_prod() -> Vec<(String, String)> {
    load_vault_secrets(true)
}

/// Shared vault loader: resolve login + project (sp00ky.yml slug) + vault
/// passphrase, then load the vault variables for the given environment. Returns
/// empty (with a warning) if not logged in, the project can't be resolved, or the
/// vault is unavailable.
fn load_vault_secrets(prod: bool) -> Vec<(String, String)> {
    let creds = match load_credentials() {
        Some(c) => c,
        None => {
            eprintln!("  Warning: Not logged in to Sp00ky Cloud. Skipping vault env vars. Run `spky cloud login` first.");
            return Vec::new();
        }
    };
    let mut client = CloudClient::new(&creds);

    // Resolve project
    let pid = match resolve_project_id_quiet(&mut client) {
        Some(pid) => pid,
        None => {
            eprintln!("  Warning: Could not resolve cloud project. Skipping vault env vars.");
            return Vec::new();
        }
    };

    let derived_key = match get_derived_key(&mut client) {
        Ok(dk) => dk,
        Err(e) => {
            eprintln!(
                "  Warning: Could not load vault passphrase: {}. Skipping vault env vars.",
                e
            );
            return Vec::new();
        }
    };

    let environment = if prod { "production" } else { "development" };
    let resp = match client.post(
        &format!("/v1/projects/{}/envs/load", pid),
        &serde_json::json!({ "derived_key": derived_key, "environment": environment }),
    ) {
        Ok(r) => r,
        Err(e) => {
            eprintln!("  Warning: Could not load vault env vars: {}. Skipping.", e);
            return Vec::new();
        }
    };

    let data: serde_json::Value = match resp.into_json() {
        Ok(d) => d,
        Err(_) => return Vec::new(),
    };

    let mut result = Vec::new();
    if let Some(vars) = data["variables"].as_object() {
        for (key, value) in vars {
            if let Some(val) = value.as_str() {
                result.push((key.clone(), val.to_string()));
            }
        }
    }
    result
}

/// Quietly resolve the project ID from sp00ky.yml slug, without prompts.
fn resolve_project_id_quiet(client: &mut CloudClient) -> Option<String> {
    let config_path = std::env::current_dir().ok()?.join("sp00ky.yml");
    if !config_path.exists() {
        return None;
    }
    let config = backend::load_config(&config_path);
    let slug = config.slug?;
    let project = fetch_project(client, &slug).ok()??;
    Some(project_id(&project))
}

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

const DEFAULT_API_URL: &str = "https://api.sp00ky.cloud";
const CREDENTIALS_FILE: &str = "credentials.json";
const SP00KY_DIR: &str = ".sp00ky";

pub(crate) fn api_base_url() -> String {
    // Priority: env var > sp00ky.yml cloudApi > default
    if let Ok(url) = std::env::var("SP00KY_CLOUD_API") {
        return url;
    }
    if let Ok(cwd) = std::env::current_dir() {
        let config = backend::load_config(&cwd.join("sp00ky.yml"));
        if let Some(url) = config.cloud_api {
            return url;
        }
    }
    DEFAULT_API_URL.to_string()
}

/// Derive the SurrealDB WebSocket endpoint for a project.
/// api-stg.sp00ky.cloud → wss://{slug}-db.stg.spky.cloud/rpc
/// api.sp00ky.cloud → wss://{slug}-db.spky.cloud/rpc
fn derive_db_ws_endpoint(api_url: &str, slug: &str) -> Option<String> {
    let host = api_url
        .strip_prefix("https://")
        .or(api_url.strip_prefix("http://"))?
        .split('/')
        .next()?;

    if let Some(stage) = host
        .strip_prefix("api-")
        .and_then(|h| h.strip_suffix(".sp00ky.cloud"))
    {
        // Staging: api-stg.sp00ky.cloud → {slug}-db.stg.spky.cloud
        Some(format!("wss://{}-db.{}.spky.cloud/rpc", slug, stage))
    } else if host.ends_with(".sp00ky.cloud") {
        // Production: api.sp00ky.cloud → {slug}-db.spky.cloud
        Some(format!("wss://{}-db.spky.cloud/rpc", slug))
    } else {
        None
    }
}

/// Derive the upload URL for large files (bypasses Cloudflare proxy).
/// Converts "https://api-stg.sp00ky.cloud" → "https://upload-stg.spky.cloud"
/// Falls back to the API URL if no upload domain can be derived.
fn upload_base_url(api_url: &str) -> String {
    if let Ok(url) = std::env::var("SP00KY_CLOUD_UPLOAD") {
        return url;
    }
    // Try to derive: api-{stage}.sp00ky.cloud → upload-{stage}.spky.cloud
    if let Some(host) = api_url
        .strip_prefix("https://")
        .or(api_url.strip_prefix("http://"))
    {
        let host = host.split('/').next().unwrap_or(host);
        if let Some(stage) = host
            .strip_prefix("api-")
            .and_then(|h| h.strip_suffix(".sp00ky.cloud"))
        {
            return format!("https://upload.{}.spky.cloud", stage);
        }
        if host == "api.sp00ky.cloud" {
            return "https://upload.spky.cloud".to_string();
        }
    }
    api_url.to_string()
}

fn credentials_path() -> PathBuf {
    dirs::home_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join(SP00KY_DIR)
        .join(CREDENTIALS_FILE)
}

// ---------------------------------------------------------------------------
// Credentials
// ---------------------------------------------------------------------------

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct Credentials {
    access_token: String,
    refresh_token: String,
}

fn load_credentials() -> Option<Credentials> {
    // API key from environment takes precedence (for CI/CD)
    if let Ok(api_key) = std::env::var("SPOOKY_API_KEY") {
        if api_key.starts_with("spk_live_") {
            return Some(Credentials {
                access_token: api_key,
                refresh_token: String::new(),
            });
        }
    }
    let path = credentials_path();
    let content = fs::read_to_string(&path).ok()?;
    serde_json::from_str(&content).ok()
}

fn save_credentials(creds: &Credentials) -> Result<()> {
    let path = credentials_path();
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).context("Failed to create ~/.sp00ky directory")?;
    }
    let json = serde_json::to_string_pretty(creds)?;
    fs::write(&path, json).context("Failed to write credentials")?;
    Ok(())
}

fn clear_credentials() -> Result<()> {
    let path = credentials_path();
    if path.exists() {
        fs::remove_file(&path).context("Failed to remove credentials file")?;
    }
    Ok(())
}

pub(crate) fn require_credentials() -> Result<Credentials> {
    load_credentials()
        .ok_or_else(|| anyhow::anyhow!("Not logged in. Run `spky login` first."))
}

// ---------------------------------------------------------------------------
// Cloud HTTP Client
// ---------------------------------------------------------------------------

pub(crate) struct CloudClient {
    base_url: String,
    token: String,
    refresh_token: String,
    is_api_key: bool,
}

impl CloudClient {
    pub(crate) fn new(creds: &Credentials) -> Self {
        let is_api_key = creds.access_token.starts_with("spk_live_");
        Self {
            base_url: api_base_url(),
            token: creds.access_token.clone(),
            refresh_token: creds.refresh_token.clone(),
            is_api_key,
        }
    }

    fn auth_header(&self) -> String {
        if self.is_api_key {
            format!("ApiKey {}", self.token)
        } else {
            format!("Bearer {}", self.token)
        }
    }

    fn try_refresh(&mut self) -> Result<()> {
        let url = format!("{}/v1/auth/refresh", self.base_url);
        let body = serde_json::json!({ "refresh_token": self.refresh_token });
        let resp = ureq::post(&url)
            .set("Accept", "application/json")
            .send_json(body)
            .map_err(|e| anyhow::anyhow!("Token refresh failed: {}", e))?;
        let tokens: serde_json::Value = resp
            .into_json()
            .context("Failed to parse refresh response")?;
        let access = tokens["access_token"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("Missing access_token in refresh response"))?;
        let refresh = tokens["refresh_token"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("Missing refresh_token in refresh response"))?;
        self.token = access.to_string();
        self.refresh_token = refresh.to_string();
        save_credentials(&Credentials {
            access_token: self.token.clone(),
            refresh_token: self.refresh_token.clone(),
        })?;
        Ok(())
    }

    fn format_api_error(code: u16, resp: ureq::Response) -> String {
        let body = resp.into_string().unwrap_or_default();
        if body.contains("<!DOCTYPE") || body.contains("<html") {
            // HTML error page (e.g. Cloudflare 502/503)
            format!(
                "API unavailable (HTTP {}). The server may be restarting — try again in a moment.",
                code
            )
        } else if let Ok(json) = serde_json::from_str::<serde_json::Value>(&body) {
            let error_code = json["code"].as_str().unwrap_or("");
            let error_msg = json["error"].as_str().unwrap_or(&body);
            match error_code {
                "project_not_active" => {
                    "Project billing is not set up. Run `spky billing` or `spky deploy` to get started.".to_string()
                }
                "plan_limit" => {
                    format!("{}. Run `spky billing` to manage your plan.", error_msg)
                }
                "slug_taken" => {
                    "A project with this slug already exists — choose a different slug.".to_string()
                }
                _ if !error_msg.is_empty() => {
                    format!("API error (HTTP {}): {}", code, error_msg)
                }
                _ => format!("API error (HTTP {}): {}", code, body),
            }
        } else {
            format!("API error (HTTP {}): {}", code, body)
        }
    }

    pub(crate) fn get(&mut self, path: &str) -> Result<ureq::Response> {
        let url = format!("{}{}", self.base_url, path);
        match ureq::get(&url)
            .set("Authorization", &self.auth_header())
            .set("Accept", "application/json")
            .timeout(Duration::from_secs(60))
            .call()
        {
            Ok(resp) => Ok(resp),
            Err(ureq::Error::Status(401, _)) if !self.is_api_key => {
                self.try_refresh().map_err(|_| {
                    anyhow::anyhow!("Session expired. Run `spky login` to re-authenticate.")
                })?;
                match ureq::get(&url)
                    .set("Authorization", &self.auth_header())
                    .set("Accept", "application/json")
                    .timeout(Duration::from_secs(60))
                    .call()
                {
                    Ok(resp) => Ok(resp),
                    Err(ureq::Error::Status(401, _)) => {
                        bail!("Session expired. Run `spky login` to re-authenticate.")
                    }
                    Err(ureq::Error::Status(code, resp)) => {
                        bail!("{}", Self::format_api_error(code, resp))
                    }
                    Err(ureq::Error::Transport(t)) => {
                        bail!("Failed to connect to Sp00ky Cloud API: {}", t)
                    }
                }
            }
            Err(ureq::Error::Status(code, resp)) => {
                bail!("{}", Self::format_api_error(code, resp))
            }
            Err(ureq::Error::Transport(t)) => {
                bail!("Failed to connect to Sp00ky Cloud API: {}", t)
            }
        }
    }

    pub(crate) fn post(&mut self, path: &str, body: &serde_json::Value) -> Result<ureq::Response> {
        let url = format!("{}{}", self.base_url, path);
        match ureq::post(&url)
            .set("Authorization", &self.auth_header())
            .set("Accept", "application/json")
            .timeout(Duration::from_secs(60))
            .send_json(body.clone())
        {
            Ok(resp) => Ok(resp),
            Err(ureq::Error::Status(401, _)) if !self.is_api_key => {
                self.try_refresh().map_err(|_| {
                    anyhow::anyhow!("Session expired. Run `spky login` to re-authenticate.")
                })?;
                match ureq::post(&url)
                    .set("Authorization", &self.auth_header())
                    .set("Accept", "application/json")
                    .timeout(Duration::from_secs(60))
                    .send_json(body.clone())
                {
                    Ok(resp) => Ok(resp),
                    Err(ureq::Error::Status(401, _)) => {
                        bail!("Session expired. Run `spky login` to re-authenticate.")
                    }
                    Err(ureq::Error::Status(code, resp)) => {
                        bail!("{}", Self::format_api_error(code, resp))
                    }
                    Err(ureq::Error::Transport(t)) => {
                        bail!("Failed to connect to Sp00ky Cloud API: {}", t)
                    }
                }
            }
            Err(ureq::Error::Status(code, resp)) => {
                bail!("{}", Self::format_api_error(code, resp))
            }
            Err(ureq::Error::Transport(t)) => {
                bail!("Failed to connect to Sp00ky Cloud API: {}", t)
            }
        }
    }

    pub(crate) fn delete(&mut self, path: &str) -> Result<ureq::Response> {
        let url = format!("{}{}", self.base_url, path);
        match ureq::delete(&url)
            .set("Authorization", &self.auth_header())
            .set("Accept", "application/json")
            .call()
        {
            Ok(resp) => Ok(resp),
            Err(ureq::Error::Status(401, _)) if !self.is_api_key => {
                self.try_refresh().map_err(|_| {
                    anyhow::anyhow!("Session expired. Run `spky login` to re-authenticate.")
                })?;
                match ureq::delete(&url)
                    .set("Authorization", &self.auth_header())
                    .set("Accept", "application/json")
                    .call()
                {
                    Ok(resp) => Ok(resp),
                    Err(ureq::Error::Status(401, _)) => {
                        bail!("Session expired. Run `spky login` to re-authenticate.")
                    }
                    Err(ureq::Error::Status(code, resp)) => {
                        bail!("{}", Self::format_api_error(code, resp))
                    }
                    Err(ureq::Error::Transport(t)) => {
                        bail!("Failed to connect to Sp00ky Cloud API: {}", t)
                    }
                }
            }
            Err(ureq::Error::Status(code, resp)) => {
                bail!("{}", Self::format_api_error(code, resp))
            }
            Err(ureq::Error::Transport(t)) => {
                bail!("Failed to connect to Sp00ky Cloud API: {}", t)
            }
        }
    }
    fn put(&mut self, path: &str, body: &serde_json::Value) -> Result<ureq::Response> {
        let url = format!("{}{}", self.base_url, path);
        match ureq::request("PUT", &url)
            .set("Authorization", &self.auth_header())
            .set("Accept", "application/json")
            .send_json(body.clone())
        {
            Ok(resp) => Ok(resp),
            Err(ureq::Error::Status(401, _)) if !self.is_api_key => {
                self.try_refresh().map_err(|_| {
                    anyhow::anyhow!("Session expired. Run `spky login` to re-authenticate.")
                })?;
                match ureq::request("PUT", &url)
                    .set("Authorization", &self.auth_header())
                    .set("Accept", "application/json")
                    .send_json(body.clone())
                {
                    Ok(resp) => Ok(resp),
                    Err(ureq::Error::Status(401, _)) => {
                        bail!("Session expired. Run `spky login` to re-authenticate.")
                    }
                    Err(ureq::Error::Status(code, resp)) => {
                        bail!("{}", Self::format_api_error(code, resp))
                    }
                    Err(ureq::Error::Transport(t)) => {
                        bail!("Failed to connect to Sp00ky Cloud API: {}", t)
                    }
                }
            }
            Err(ureq::Error::Status(code, resp)) => {
                bail!("{}", Self::format_api_error(code, resp))
            }
            Err(ureq::Error::Transport(t)) => {
                bail!("Failed to connect to Sp00ky Cloud API: {}", t)
            }
        }
    }

    fn patch(&mut self, path: &str, body: &serde_json::Value) -> Result<ureq::Response> {
        let url = format!("{}{}", self.base_url, path);
        match ureq::request("PATCH", &url)
            .set("Authorization", &self.auth_header())
            .set("Accept", "application/json")
            .send_json(body.clone())
        {
            Ok(resp) => Ok(resp),
            Err(ureq::Error::Status(401, _)) if !self.is_api_key => {
                self.try_refresh().map_err(|_| {
                    anyhow::anyhow!("Session expired. Run `spky login` to re-authenticate.")
                })?;
                match ureq::request("PATCH", &url)
                    .set("Authorization", &self.auth_header())
                    .set("Accept", "application/json")
                    .send_json(body.clone())
                {
                    Ok(resp) => Ok(resp),
                    Err(ureq::Error::Status(401, _)) => {
                        bail!("Session expired. Run `spky login` to re-authenticate.")
                    }
                    Err(ureq::Error::Status(code, resp)) => {
                        bail!("{}", Self::format_api_error(code, resp))
                    }
                    Err(ureq::Error::Transport(t)) => {
                        bail!("Failed to connect to Sp00ky Cloud API: {}", t)
                    }
                }
            }
            Err(ureq::Error::Status(code, resp)) => {
                bail!("{}", Self::format_api_error(code, resp))
            }
            Err(ureq::Error::Transport(t)) => {
                bail!("Failed to connect to Sp00ky Cloud API: {}", t)
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Project slug resolution
// ---------------------------------------------------------------------------

fn resolve_project_slug() -> Result<String> {
    // 1. Environment variable
    if let Ok(slug) = std::env::var("SP00KY_CLOUD_PROJECT") {
        return Ok(slug);
    }

    // 2. Read slug from sp00ky.yml
    if let Ok(cwd) = std::env::current_dir() {
        let config_path = cwd.join("sp00ky.yml");
        if config_path.exists() {
            let config = backend::load_config(&config_path);
            if let Some(slug) = config.slug {
                return Ok(slug);
            }
        }
    }

    // 3. Prompt
    let slug = inquire::Text::new("Project slug:")
        .with_help_message("The slug you used when creating the cloud project")
        .prompt()
        .context("Failed to read project slug")?;

    Ok(slug)
}

// ---------------------------------------------------------------------------
// Flow helpers — composable building blocks for guided CLI flows
// ---------------------------------------------------------------------------

fn is_interactive() -> bool {
    std::io::stdin().is_terminal()
}

/// Fetch a project by slug. Returns None if not found.
fn fetch_project(client: &mut CloudClient, slug: &str) -> Result<Option<serde_json::Value>> {
    let projects = fetch_project_list(client)?;
    Ok(projects
        .into_iter()
        .find(|p| p["slug"].as_str() == Some(slug)))
}

/// Extract the project UUID from a project JSON value.
/// Falls back to slug if id is missing (shouldn't happen).
fn project_id(project: &serde_json::Value) -> String {
    project["id"]
        .as_str()
        .unwrap_or(project["slug"].as_str().unwrap_or("unknown"))
        .to_string()
}

/// Resolve the project slug and look up its UUID.
/// Used by standalone commands (status, logs, scale, destroy).
fn resolve_project_id(client: &mut CloudClient) -> Result<(String, String)> {
    let slug = resolve_project_slug()?;
    let project = fetch_project(client, &slug)?
        .ok_or_else(|| anyhow::anyhow!("Project '{}' not found.", slug))?;
    let pid = project_id(&project);
    Ok((slug, pid))
}

/// List all projects for the authenticated user.
fn fetch_project_list(client: &mut CloudClient) -> Result<Vec<serde_json::Value>> {
    let resp = client.get("/v1/projects")?;
    let projects: Vec<serde_json::Value> = resp.into_json().context("Failed to parse projects")?;
    Ok(projects)
}

/// Ensure the user is logged in. In interactive mode, offers to log in inline.
fn ensure_login() -> Result<Credentials> {
    if let Some(creds) = load_credentials() {
        return Ok(creds);
    }

    if !is_interactive() {
        bail!("Not logged in. Run `spky login` first.");
    }

    println!("You're not logged in to Sp00ky Cloud.");
    let do_login = inquire::Confirm::new("Log in now?")
        .with_default(true)
        .prompt()
        .context("Failed to read confirmation")?;

    if !do_login {
        bail!("Login required. Run `spky login` to authenticate.");
    }

    login()?;

    load_credentials().ok_or_else(|| anyhow::anyhow!("Login succeeded but credentials not found."))
}

/// Ensure a cloud project exists and return (slug, project_json).
/// In interactive mode, guides the user through project selection or creation.
fn ensure_project(client: &mut CloudClient) -> Result<(String, serde_json::Value)> {
    // 1. Try configured slug (env var or sp00ky.yml)
    if let Ok(slug) = std::env::var("SP00KY_CLOUD_PROJECT") {
        if let Some(project) = fetch_project(client, &slug)? {
            return Ok((slug, project));
        }
        bail!("Project '{}' not found (from SP00KY_CLOUD_PROJECT).", slug);
    }

    if let Ok(cwd) = std::env::current_dir() {
        let config_path = cwd.join("sp00ky.yml");
        if config_path.exists() {
            let config = backend::load_config(&config_path);
            if let Some(slug) = config.slug {
                if let Some(project) = fetch_project(client, &slug)? {
                    return Ok((slug, project));
                }
                // Slug is configured but project doesn't exist — fall through to create
                if !is_interactive() {
                    bail!(
                        "Project '{}' not found. Run `spky project create --slug {}` first.",
                        slug,
                        slug
                    );
                }
                println!(
                    "Project '{}' (from sp00ky.yml) not found in Sp00ky Cloud.",
                    slug
                );
                let do_create = inquire::Confirm::new(&format!("Create project '{}'?", slug))
                    .with_default(true)
                    .prompt()
                    .context("Failed to read confirmation")?;
                if do_create {
                    return create_project_inline(client, Some(slug));
                }
                bail!("Project required. Run `spky project create` to create one.");
            }
        }
    }

    // 2. No slug configured — check existing projects
    if !is_interactive() {
        bail!("No project configured. Set SP00KY_CLOUD_PROJECT or add `slug` to sp00ky.yml.");
    }

    let projects = fetch_project_list(client)?;
    let active_projects: Vec<&serde_json::Value> = projects
        .iter()
        .filter(|p| p["status"].as_str() != Some("destroyed"))
        .collect();

    if active_projects.is_empty() {
        println!("No cloud projects found.");
        let do_create = inquire::Confirm::new("Create a new project?")
            .with_default(true)
            .prompt()
            .context("Failed to read confirmation")?;
        if do_create {
            return create_project_inline(client, None);
        }
        bail!("Project required. Run `spky project create` to create one.");
    }

    if active_projects.len() == 1 {
        let slug = active_projects[0]["slug"]
            .as_str()
            .context("Missing slug in project")?
            .to_string();
        let status = active_projects[0]["status"].as_str().unwrap_or("unknown");
        println!("Found project '{}' ({}).", slug, status);
        return Ok((slug, active_projects[0].clone()));
    }

    // Multiple projects — let user pick
    let slugs: Vec<String> = active_projects
        .iter()
        .map(|p| {
            format!(
                "{} ({})",
                p["slug"].as_str().unwrap_or("?"),
                p["status"].as_str().unwrap_or("?")
            )
        })
        .collect();

    let selection = inquire::Select::new("Select a project:", slugs)
        .prompt()
        .context("Failed to read selection")?;

    // Extract slug from "slug (status)" format
    let slug = selection.split(' ').next().unwrap_or("").to_string();
    let project = fetch_project(client, &slug)?.context("Selected project not found")?;
    Ok((slug, project))
}

/// Create a project inline and return (slug, project_json).
/// Retries with a different slug if the chosen one is already taken.
fn create_project_inline(
    client: &mut CloudClient,
    slug: Option<String>,
) -> Result<(String, serde_json::Value)> {
    let mut slug = match slug {
        Some(s) => s,
        None => inquire::Text::new("Project slug:")
            .with_help_message("Lowercase letters, numbers, and hyphens (e.g. my-app)")
            .prompt()
            .context("Failed to read slug")?,
    };

    let plan = "starter".to_string();

    loop {
        match client.post(
            "/v1/projects",
            &serde_json::json!({ "slug": slug, "plan": plan }),
        ) {
            Ok(resp) => {
                let project: serde_json::Value =
                    resp.into_json().context("Failed to parse response")?;

                println!();
                println!("  Project created!");
                println!("  Slug:   {}", project["slug"].as_str().unwrap_or(&slug));
                println!("  Plan:   {}", project["plan"].as_str().unwrap_or(&plan));
                println!(
                    "  Status: {}",
                    project["status"].as_str().unwrap_or("pending_payment")
                );

                return Ok((slug, project));
            }
            Err(err) => {
                let msg = err.to_string();
                // Catch slug conflicts: 409 "slug already exists" (new backend)
                // or 500 "failed to create project" (old backend — likely unique constraint)
                let is_slug_conflict = msg.contains("already exists")
                    || msg.contains("slug already")
                    || msg.contains("failed to create project");
                if is_slug_conflict {
                    if !is_interactive() {
                        bail!(
                            "Slug '{}' is likely already taken. Choose a different slug.",
                            slug
                        );
                    }
                    println!(
                        "  Slug '{}' is likely already taken. Try a different one.",
                        slug
                    );
                    slug = inquire::Text::new("Project slug:")
                        .with_help_message("Lowercase letters, numbers, and hyphens (e.g. my-app)")
                        .prompt()
                        .context("Failed to read slug")?;
                    continue;
                }
                return Err(err);
            }
        }
    }
}

/// Ensure the project has active billing. Offers to set up billing inline if interactive.
fn ensure_billing_active(
    client: &mut CloudClient,
    slug: &str,
    project: &serde_json::Value,
) -> Result<()> {
    let status = project["status"].as_str().unwrap_or("unknown");

    match status {
        "active" => Ok(()),
        "pending_payment" => {
            if !is_interactive() {
                bail!(
                    "Project '{}' requires billing setup. Run `spky billing` first.",
                    slug
                );
            }

            println!();
            println!("  Billing is not set up for '{}'.", slug);
            let do_billing = inquire::Confirm::new("Set up billing now?")
                .with_default(true)
                .prompt()
                .context("Failed to read confirmation")?;

            if !do_billing {
                bail!("Billing required before deploying. Run `spky billing` when ready.");
            }

            wait_for_billing(client, slug)
        }
        "suspended" => {
            bail!(
                "Project '{}' is suspended due to a billing issue. Run `spky billing` to resolve.",
                slug
            );
        }
        "destroyed" => {
            bail!("Project '{}' has been destroyed.", slug);
        }
        _ => {
            bail!("Project '{}' has unexpected status: {}", slug, status);
        }
    }
}

/// Open Stripe checkout and poll until payment completes.
fn wait_for_billing(client: &mut CloudClient, slug: &str) -> Result<()> {
    // Plan selection
    let plan = if is_interactive() {
        let plans = vec!["Starter", "Pro"];
        let selection = inquire::Select::new("Select a plan:", plans)
            .prompt()
            .context("Failed to select plan")?;
        match selection {
            "Pro" => "pro",
            _ => "starter",
        }
    } else {
        "starter"
    };

    // Billing interval selection
    let interval = if is_interactive() {
        let intervals = vec!["Monthly", "Yearly"];
        let selection = inquire::Select::new("Billing interval:", intervals)
            .prompt()
            .context("Failed to select billing interval")?;
        match selection {
            "Yearly" => "yearly",
            _ => "monthly",
        }
    } else {
        "monthly"
    };

    // Start checkout with plan and interval
    let resp = client.post(
        "/v1/billing/checkout",
        &serde_json::json!({
            "project_id": slug,
            "plan": plan,
            "billing_interval": interval,
        }),
    )?;
    let data: serde_json::Value = resp
        .into_json()
        .context("Failed to parse checkout response")?;
    let url = data["url"]
        .as_str()
        .context("No checkout URL in response")?;

    println!("  Opening Stripe checkout...");
    let _ = open::that(url);
    println!("  Waiting for payment to complete... (press Ctrl+C to cancel)");

    // Poll project status every 3 seconds, timeout after 10 minutes
    let max_attempts = 200;
    for _ in 0..max_attempts {
        thread::sleep(Duration::from_secs(3));

        if let Some(project) = fetch_project(client, slug)? {
            let status = project["status"].as_str().unwrap_or("unknown");
            if status == "active" {
                println!("  Payment confirmed! Project '{}' is now active.", slug);
                return Ok(());
            }
        }
        print!(".");
    }

    bail!(
        "Timed out waiting for payment. If you completed checkout, it may take a moment to process.\n  Run `spky deploy` to try again."
    );
}

/// Poll deployment until SSP VMs match the target count.
fn poll_scale_completion(client: &mut CloudClient, pid: &str, target_ssp: u32) -> Result<()> {
    println!("  Waiting for scaling to complete...");

    let max_attempts = 100; // ~5 minutes at 3s intervals
    for _ in 0..max_attempts {
        thread::sleep(Duration::from_secs(3));

        let resp = client.get(&format!("/v1/projects/{}/deployment", pid));
        if let Ok(resp) = resp {
            let data: serde_json::Value = resp.into_json().unwrap_or_default();
            if let Some(vms) = data.get("vms").and_then(|v| v.as_array()) {
                let running_ssp = vms
                    .iter()
                    .filter(|vm| {
                        vm["role"].as_str() == Some("ssp")
                            && vm["status"].as_str() == Some("running")
                    })
                    .count() as u32;

                if running_ssp >= target_ssp {
                    println!(
                        "  Scaling complete! {} SSP instance(s) running.",
                        running_ssp
                    );
                    print_deployment_details(&data);
                    return Ok(());
                }
                print!("  SSP instances: {}/{}...\r", running_ssp, target_ssp);
            }
        }
    }

    println!();
    println!("  Scaling is still in progress. Run `spky status` to check.");
    Ok(())
}

// ---------------------------------------------------------------------------
// Command dispatch
// ---------------------------------------------------------------------------

/// Unified `env` group dispatch: the environment variables AND the vault that
/// encrypts them live under one command surface. Reuses the existing leaf
/// handlers unchanged; the `reset` subgroup maps onto the existing `vault`
/// passphrase-reset flows.
pub fn env_group(action: EnvCommands) -> Result<()> {
    match action {
        EnvCommands::Set {
            name,
            file,
            dev,
            prod,
        } => env_set(name, file, dev, prod),
        EnvCommands::List => env_list(),
        EnvCommands::Rm { name } => env_delete(name),
        EnvCommands::Pull { prod } => env_load(prod),
        EnvCommands::Import { file } => env_import(file),
        EnvCommands::Unlock => env_init(),
        EnvCommands::Passphrase => env_change_passphrase(),
        EnvCommands::ShareCi { disable, status } => env_ci_access(disable, status),
        EnvCommands::Reset { action } => vault(match action {
            EnvResetCommands::Request => CloudVaultCommands::RequestReset,
            EnvResetCommands::Approve { email } => CloudVaultCommands::ApproveReset { email },
            EnvResetCommands::Complete => CloudVaultCommands::CompleteReset,
            EnvResetCommands::List => CloudVaultCommands::ListResets,
        }),
    }
}

// ---------------------------------------------------------------------------
// Commands
// ---------------------------------------------------------------------------

pub fn login() -> Result<()> {
    let base_url = api_base_url();
    let url = format!("{}/v1/auth/login", base_url);

    println!("Initiating login...");

    let resp: serde_json::Value = match ureq::post(&url)
        .set("Accept", "application/json")
        .send_json(serde_json::json!({}))
    {
        Ok(resp) => resp.into_json().context("Failed to parse login response")?,
        Err(ureq::Error::Status(code, resp)) => {
            bail!("{}", CloudClient::format_api_error(code, resp));
        }
        Err(ureq::Error::Transport(t)) => {
            bail!("Failed to connect to Sp00ky Cloud API: {}", t);
        }
    };

    let device_code = resp["device_code"]
        .as_str()
        .context("Missing device_code in response")?;
    let user_code = resp["user_code"]
        .as_str()
        .context("Missing user_code in response")?;
    let verification_url = resp["verification_url"]
        .as_str()
        .context("Missing verification_url in response")?;
    let interval = resp["interval"].as_u64().unwrap_or(5);

    let verification_url_with_code = format!("{}?code={}", verification_url, user_code);

    println!();
    println!("  Your verification code is: {}", user_code);
    println!();
    println!("  Opening browser to: {}", verification_url_with_code);
    println!("  (If the browser doesn't open, visit the URL manually)");
    println!();

    // Open browser
    let _ = open::that(&verification_url_with_code);

    // Poll for completion
    let poll_url = format!("{}/v1/auth/login/poll", base_url);

    loop {
        thread::sleep(Duration::from_secs(interval));

        let poll_resp = match ureq::post(&poll_url)
            .set("Accept", "application/json")
            .send_json(serde_json::json!({ "device_code": device_code }))
        {
            Ok(resp) => {
                // 202 means authorization pending (ureq treats 2xx as Ok)
                if resp.status() == 202 {
                    print!(".");
                    continue;
                }
                resp
            }
            Err(ureq::Error::Status(410, _)) => {
                bail!("Device code expired. Please try again.");
            }
            Err(ureq::Error::Status(code, resp)) => {
                let body = resp.into_string().unwrap_or_default();
                bail!("Poll failed (HTTP {}): {}", code, body);
            }
            Err(ureq::Error::Transport(t)) => {
                bail!("Connection error during polling: {}", t);
            }
        };

        let tokens: serde_json::Value = poll_resp
            .into_json()
            .context("Failed to parse token response")?;

        let access_token = tokens["access_token"]
            .as_str()
            .context("Missing access_token")?;
        let refresh_token = tokens["refresh_token"]
            .as_str()
            .context("Missing refresh_token")?;

        save_credentials(&Credentials {
            access_token: access_token.to_string(),
            refresh_token: refresh_token.to_string(),
        })?;

        println!();
        println!("Logged in successfully.");
        return Ok(());
    }
}

pub fn logout() -> Result<()> {
    clear_credentials()?;
    println!("Logged out.");
    Ok(())
}

pub fn list() -> Result<()> {
    let creds = require_credentials()?;
    let mut client = CloudClient::new(&creds);

    let projects = fetch_project_list(&mut client)?;

    if projects.is_empty() {
        println!("No projects found.");
        return Ok(());
    }

    println!(
        "  {:<20} {:<12} {:<18} {}",
        "SLUG", "PLAN", "STATUS", "CREATED"
    );
    println!("  {}", "-".repeat(65));
    for p in &projects {
        println!(
            "  {:<20} {:<12} {:<18} {}",
            p["slug"].as_str().unwrap_or("-"),
            p["plan"].as_str().unwrap_or("-"),
            p["status"].as_str().unwrap_or("-"),
            p["created_at"]
                .as_str()
                .unwrap_or("-")
                .get(..10)
                .unwrap_or("-"),
        );
    }

    Ok(())
}

pub fn create(slug: Option<String>, plan: String) -> Result<()> {
    let creds = require_credentials()?;
    let mut client = CloudClient::new(&creds);

    let slug = match slug {
        Some(s) => s,
        None => inquire::Text::new("Project slug:")
            .with_help_message("Lowercase letters, numbers, and hyphens (e.g. my-app)")
            .prompt()
            .context("Failed to read slug")?,
    };

    let resp = client.post(
        "/v1/projects",
        &serde_json::json!({ "slug": slug, "plan": plan }),
    )?;

    let project: serde_json::Value = resp.into_json().context("Failed to parse response")?;

    println!();
    println!("  Project created!");
    println!("  Slug:   {}", project["slug"].as_str().unwrap_or(&slug));
    println!("  Plan:   {}", project["plan"].as_str().unwrap_or(&plan));
    println!(
        "  Status: {}",
        project["status"].as_str().unwrap_or("pending_payment")
    );

    if is_interactive() {
        println!();
        let setup_billing = inquire::Confirm::new("Set up billing now?")
            .with_default(true)
            .prompt()
            .context("Failed to read confirmation")?;
        if setup_billing {
            wait_for_billing(&mut client, &slug)?;
            println!();
            println!("  Project is active! Run `spky deploy` to deploy.");
        } else {
            println!();
            println!("  Next: Run `spky billing` to set up payment,");
            println!("        then `spky deploy` to deploy.");
        }
    } else {
        println!();
        println!("  Next: Run `spky billing` to set up payment,");
        println!("        then `spky deploy` to deploy.");
    }

    Ok(())
}

/// Load vault env vars for deploy via the API. Returns "KEY=VAL" strings.
fn load_vault_envs_for_deploy(client: &mut CloudClient, pid: &str, prod: bool) -> Vec<String> {
    let derived_key = match get_derived_key(client) {
        Ok(dk) => dk,
        Err(e) => {
            eprintln!(
                "  Warning: Could not load vault passphrase: {}. Skipping vault env vars.",
                e
            );
            return Vec::new();
        }
    };
    let environment = if prod { "production" } else { "development" };
    let resp = match client.post(
        &format!("/v1/projects/{}/envs/load", pid),
        &serde_json::json!({ "derived_key": derived_key, "environment": environment }),
    ) {
        Ok(r) => r,
        Err(e) => {
            eprintln!("  Warning: Could not load vault env vars: {}. Skipping.", e);
            return Vec::new();
        }
    };
    let data: serde_json::Value = match resp.into_json() {
        Ok(d) => d,
        Err(_) => return Vec::new(),
    };
    let mut result = Vec::new();
    if let Some(vars) = data["variables"].as_object() {
        for (key, value) in vars {
            if let Some(val) = value.as_str() {
                result.push(format!("{}={}", key, val));
            }
        }
    }
    result
}

/// Load the decrypted vault vars for an environment into a key→value map.
fn load_vault_map_for_deploy(
    client: &mut CloudClient,
    pid: &str,
    prod: bool,
) -> std::collections::HashMap<String, String> {
    load_vault_envs_for_deploy(client, pid, prod)
        .into_iter()
        .filter_map(|e| e.split_once('=').map(|(k, v)| (k.to_string(), v.to_string())))
        .collect()
}

/// Resolve a `SecretValue` (literal or `{ vault: KEY }`) against a loaded vault
/// map. Errors when a referenced vault key is absent.
fn resolve_secret_value(
    value: &backend::SecretValue,
    vault: &std::collections::HashMap<String, String>,
) -> Result<String> {
    match value {
        backend::SecretValue::Literal(s) => Ok(s.clone()),
        backend::SecretValue::Vault { vault: key } => vault.get(key).cloned().ok_or_else(|| {
            anyhow::anyhow!(
                "SurrealDB credential references vault key '{key}', which is not present in the vault (set it with `spky env set {key} ...`, and enable CI access if deploying on push)"
            )
        }),
    }
}

/// Resolve a single EnvSource into "KEY=VAL" strings for cloud deploy.
fn resolve_deploy_env_source(
    source: &backend::EnvSource,
    config_dir: &std::path::Path,
    client: &mut CloudClient,
    pid: &str,
) -> Vec<String> {
    match source {
        backend::EnvSource::Str(s) if s == "vault" => load_vault_envs_for_deploy(client, pid, true),
        backend::EnvSource::Vault(whitelist) => {
            let all = load_vault_envs_for_deploy(client, pid, true);
            all.into_iter()
                .filter(|entry| {
                    entry
                        .split_once('=')
                        .map(|(k, _)| whitelist.iter().any(|w| w == k))
                        .unwrap_or(false)
                })
                .collect()
        }
        backend::EnvSource::Str(file_path) => {
            let path = config_dir.join(file_path);
            let envs = crate::dev::load_dotenv_file(&path);
            if !envs.is_empty() {
                println!("  Loaded env-file: {}", path.display());
            }
            envs.into_iter()
                .map(|(k, v)| format!("{}={}", k, v))
                .collect()
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
                format!("{}={}", k, val)
            })
            .collect(),
        // Scoped list item ({ dev, cloud }): a deploy resolves the cloud side.
        backend::EnvSource::PerEnvironment { cloud, .. } => match cloud {
            Some(entry) => resolve_deploy_env_entry(entry, config_dir, client, pid),
            None => Vec::new(),
        },
    }
}

/// Resolve an EnvEntry (single source or list) for deploy.
fn resolve_deploy_env_entry(
    entry: &backend::EnvEntry,
    config_dir: &std::path::Path,
    client: &mut CloudClient,
    pid: &str,
) -> Vec<String> {
    match entry {
        backend::EnvEntry::Source(source) => {
            resolve_deploy_env_source(source, config_dir, client, pid)
        }
        backend::EnvEntry::List(sources) => {
            let mut merged = std::collections::BTreeMap::new();
            for source in sources {
                for entry in resolve_deploy_env_source(source, config_dir, client, pid) {
                    if let Some((k, v)) = entry.split_once('=') {
                        merged.insert(k.to_string(), v.to_string());
                    }
                }
            }
            merged
                .into_iter()
                .map(|(k, v)| format!("{}={}", k, v))
                .collect()
        }
    }
}

/// Split the host out of an `http://host:port` (or `host:port`) endpoint.
fn endpoint_host(endpoint: &str) -> &str {
    endpoint
        .split_once("://")
        .map(|(_, rest)| rest)
        .unwrap_or(endpoint)
        .split([':', '/'])
        .next()
        .unwrap_or("")
}

/// A host is a *stable* address we can safely bake into the DB schema if it's
/// not empty and not an ephemeral Docker bridge IP. Non-IP hostnames (DNS
/// aliases like `scheduler`) and Firecracker `10.x` addresses are stable;
/// any other dotted-quad IPv4 (e.g. a `172.x` Docker bridge IP) is not.
fn is_stable_endpoint_host(host: &str) -> bool {
    if host.is_empty() {
        return false;
    }
    let is_ipv4 = host.split('.').count() == 4 && host.split('.').all(|o| o.parse::<u8>().is_ok());
    !is_ipv4 || host.starts_with("10.")
}

/// The stable per-deployment DNS alias for the remote-function entry point in
/// Docker mode. Mirrors `roleAliasFor` in spooky-cloud: the scheduler (cluster)
/// is reachable as `scheduler:9667`, the single SSP (singlenode) as `ssp-0:8667`.
fn docker_alias_endpoint(mode: &backend::DeployMode) -> String {
    if *mode == backend::DeployMode::Cluster {
        "http://scheduler:9667".to_string()
    } else {
        "http://ssp-0:8667".to_string()
    }
}

/// Derive the **stable** internal endpoint that SurrealDB's remote functions
/// (`fn::query::register` → `/view/register`, the `/spooky` API → `/info/text`,
/// `fn::job::kill` / `fn::job::retry`) call via `http::get` / `http::post`.
///
/// This value is baked into the database schema at deploy time (as the
/// `$sp00ky_endpoint` param and inlined into each function body), so it MUST NOT
/// be an ephemeral container IP. On Docker the scheduler/SSP get a fresh bridge
/// IP on every redeploy; a baked IP goes stale the moment a container is
/// recreated, and SurrealDB then hangs on a dead address — SurrealQL `http::`
/// calls have no timeout — which stalls every live-query registration and wedges
/// the app on "Loading database…". (Firecracker is unaffected: its scheduler has
/// a deterministic, stable `.20` address.)
///
/// Resolution order:
///   1. A canonical endpoint supplied by the control plane (`fn_endpoint`, or
///      `scheduler_url`), as long as its host is stable.
///   2. Firecracker (internal IPs in the `10.x` range): reuse the scheduler's
///      own stable IP, or derive the deterministic `.20` from the subnet.
///   3. Docker (any other IP family): the per-deployment DNS alias, which
///      Docker's embedded DNS resolves to the live container regardless of IP
///      churn — exactly what backends already use (`SPKY_SCHEDULER_URL`).
fn derive_fn_endpoint(
    mode: &backend::DeployMode,
    deployment_status: &serde_json::Value,
) -> Option<String> {
    // 1. Trust a canonical endpoint from the control plane when its host is
    //    stable; if the server still handed us a raw Docker IP, fall through to
    //    derive the alias ourselves so an old control plane can't reintroduce
    //    the stale-IP bug.
    for key in ["fn_endpoint", "scheduler_url"] {
        if let Some(ep) = deployment_status[key].as_str() {
            if is_stable_endpoint_host(endpoint_host(ep)) {
                return Some(ep.to_string());
            }
        }
    }

    let vms = deployment_status["vms"].as_array()?;
    let ip_for = |role: &str| {
        vms.iter()
            .find(|v| v["role"].as_str() == Some(role))
            .and_then(|v| v["internal_ip"].as_str())
    };
    let sched_ip = ip_for("scheduler");
    let any_ip = sched_ip
        .or_else(|| ip_for("surrealdb"))
        .or_else(|| ip_for("ssp"));

    let subnet_of = |ip: &str| -> Option<String> {
        let p: Vec<&str> = ip.split('.').collect();
        (p.len() == 4).then(|| format!("{}.{}.{}", p[0], p[1], p[2]))
    };

    // 2. Firecracker — stable deterministic addressing.
    if any_ip.map(|ip| ip.starts_with("10.")).unwrap_or(false) {
        if *mode == backend::DeployMode::Cluster {
            if let Some(ip) = sched_ip {
                return Some(format!("http://{}:9667", ip));
            }
            return Some(format!("http://{}.20:9667", subnet_of(any_ip?)?));
        }
        // singlenode: the single SSP at its deterministic suffix.
        return Some(format!("http://{}.10:8667", subnet_of(any_ip?)?));
    }

    // 3. Docker — never a raw IP; use the stable per-deployment DNS alias.
    //    (Reached whenever we have VM rows but no Firecracker `10.x` IP.)
    any_ip?;
    Some(docker_alias_endpoint(mode))
}

/// Probe SurrealDB's `/spooky` custom API — the same `http::get`-to-scheduler
/// path that `fn::query::register` exercises — to confirm the data plane
/// actually responds. A timeout here means SurrealDB can't reach the scheduler
/// (e.g. a stale baked endpoint), which is exactly what wedges the app on
/// "Loading database…". Retries briefly to let a just-provisioned scheduler
/// finish coming up. Returns `Ok` on the first `200`.
fn verify_data_plane(db_url: &str, ns: &str, db: &str) -> Result<(), String> {
    let url = format!("{}/api/{}/{}/spooky", db_url.trim_end_matches('/'), ns, db);
    let mut last = "no response".to_string();
    for attempt in 0..5 {
        if attempt > 0 {
            thread::sleep(Duration::from_secs(2));
        }
        match ureq::get(&url).timeout(Duration::from_secs(4)).call() {
            Ok(resp) if resp.status() == 200 => return Ok(()),
            Ok(resp) => last = format!("HTTP {}", resp.status()),
            Err(e) => last = e.to_string(),
        }
    }
    Err(last)
}

/// Apply remote functions + internal sp00ky schema against the main SurrealDB.
///
/// Runs after user migrations on both deploy AND restore. Without this step,
/// the `_00_metadata`, `_00_query`, and per-table sp00ky events are missing,
/// and the scheduler/SSP can't bootstrap against the DB.
///
/// Idempotent — all DEFINE statements use OVERWRITE.
#[allow(clippy::too_many_arguments)]
/// What a schema-apply pass could NOT do.
///
/// Empty means everything landed. Anything else has to reach the operator and
/// the exit code: a deploy that prints success while the internal schema was
/// skipped is exactly how a broken tenant looks healthy. The meta tables and
/// per-table events the scheduler and SSP bootstrap against are simply absent,
/// and the first symptom is a client that hangs or renders empty hours later,
/// with nothing in the deploy log to connect it to.
#[derive(Default)]
pub(crate) struct SchemaApplyFailures {
    steps: Vec<String>,
}

impl SchemaApplyFailures {
    fn record(&mut self, step: &str, err: impl std::fmt::Debug) {
        self.steps.push(format!("{step} failed: {err:?}"));
    }

    fn skipped(&mut self, why: &str) {
        self.steps.push(why.to_string());
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.steps.is_empty()
    }

    /// Print a block an operator cannot scroll past, naming the consequence.
    pub(crate) fn report(&self, what: &str) {
        if self.is_empty() {
            return;
        }
        println!();
        println!("  ✖ {what}: the internal Sp00ky schema did NOT fully apply.");
        for step in &self.steps {
            println!("      - {step}");
        }
        println!("    The tenant is now running an out-of-date internal schema.");
        println!("    Re-run `spky deploy --force-schema` once the cause is fixed.");
    }
}

fn apply_remote_fns_and_internal_schema(
    db_url: &str,
    db_password: &str,
    deployment_status: &serde_json::Value,
    config: &backend::Sp00kyConfig,
    config_path: &std::path::Path,
    client: &mut CloudClient,
    pid: &str,
) -> SchemaApplyFailures {
    let mut failures = SchemaApplyFailures::default();
    let resolved = config.resolved_surrealdb();

    let surreal_client = if db_password.is_empty() {
        crate::surreal_client::SurrealClient::new_unauthenticated(
            db_url,
            &resolved.namespace,
            &resolved.database,
        )
    } else {
        crate::surreal_client::SurrealClient::new(
            db_url,
            &resolved.namespace,
            &resolved.database,
            "root",
            db_password,
        )
    };

    let mode = config.mode.clone().unwrap_or_default();

    // Bake a STABLE internal endpoint into the DB schema — never an ephemeral
    // container IP, which goes stale on the next Docker redeploy and hangs every
    // SurrealQL `http::` call. See `derive_fn_endpoint`.
    let Some(fn_endpoint) = derive_fn_endpoint(&mode, deployment_status) else {
        println!("  ▸ Warning: could not derive scheduler endpoint; skipping remote functions + internal schema.");
        failures.skipped("could not derive the scheduler endpoint, so nothing was applied");
        return failures;
    };
    let auth_secret = deployment_status["sp00ky_auth_secret"]
        .as_str()
        .map(|s| s.to_string())
        .unwrap_or_else(|| {
            let config_dir = config_path.parent().unwrap_or(std::path::Path::new("."));
            for (_name, app_config) in config.backends() {
                let all_env = resolve_env_for_deploy(&app_config.env, config_dir, client, pid);
                for entry in &all_env {
                    if let Some(val) = entry.strip_prefix("SP00KY_AUTH_SECRET=") {
                        return val.to_string();
                    }
                }
            }
            String::new()
        });

    let functions_sql =
        crate::schema_builder::build_remote_functions_schema(&mode, &fn_endpoint, &auth_secret);
    if let Err(e) = crate::migrate::apply_remote_functions_if_changed(&surreal_client, &functions_sql)
    {
        println!("  ▸ Warning: failed to apply remote functions: {:?}", e);
        failures.record("remote functions", &e);
    }

    let int_schema = config.resolved_schema();
    let int_config_dir = config_path.parent().unwrap_or(std::path::Path::new("."));
    let schema_input_path = int_config_dir.join(&int_schema.schema);
    if schema_input_path.exists() {
        match crate::migrate::apply_internal_schema(
            &surreal_client,
            &schema_input_path,
            Some(config_path),
            &mode,
            Some(&fn_endpoint),
            Some(&auth_secret),
        ) {
            Ok(()) => {}
            Err(e) => {
                println!("  ▸ Warning: failed to apply internal schema: {:?}", e);
                failures.record("internal schema", &e);
            }
        }
    }
    failures
}

/// Version of a frontend app, read from the `version` field of the
/// package.json in its app dir (deploy.context, falling back to the dev
/// workdir). None when no versioned package.json is found — the release row
/// is then skipped with a warning rather than announcing a bogus version.
pub(crate) fn frontend_package_version(
    config_dir: &std::path::Path,
    app: &backend::AppConfig,
) -> Option<String> {
    let mut dirs: Vec<String> = Vec::new();
    if let Some(deploy) = &app.deploy {
        if let Some(ctx) = &deploy.context {
            dirs.push(ctx.clone());
        }
    }
    if let Some(backend::BackendDevConfig::Typed(t)) = &app.dev {
        let wd = match t {
            backend::BackendDevTypedConfig::Npm { workdir, .. } => workdir,
            backend::BackendDevTypedConfig::Docker { workdir, .. } => workdir,
            backend::BackendDevTypedConfig::Uv { workdir, .. } => workdir,
        };
        if let Some(wd) = wd {
            dirs.push(wd.clone());
        }
    }
    for dir in dirs {
        let pkg_path = config_dir.join(&dir).join("package.json");
        let Ok(raw) = fs::read_to_string(&pkg_path) else {
            continue;
        };
        let Ok(pkg) = serde_json::from_str::<serde_json::Value>(&raw) else {
            continue;
        };
        if let Some(v) = pkg.get("version").and_then(|v| v.as_str()) {
            if !v.is_empty() {
                return Some(v.to_string());
            }
        }
    }
    None
}

/// UPSERT the `_00_app_release:<app>` row announcing `version` as the current
/// release of frontend `app`. Root-only write; synced live to clients via the
/// `_00_app_release` ingest-notify events (sp00ky.rs), where SDK hooks like
/// `useAppRelease` compare it against the running build and offer/force a
/// reload. Best-effort like `record_schema_hash`: a failure only means clients
/// aren't nudged, never a failed deploy.
pub(crate) fn write_app_release_row(
    db_url: &str,
    db_password: &str,
    namespace: &str,
    database: &str,
    app_name: &str,
    version: &str,
    cache_bust: bool,
    mandatory: bool,
) {
    let surreal_client = if db_password.is_empty() {
        crate::surreal_client::SurrealClient::new_unauthenticated(db_url, namespace, database)
    } else {
        crate::surreal_client::SurrealClient::new(db_url, namespace, database, "root", db_password)
    };
    // App names come from sp00ky.yml keys (identifier-safe) and the version
    // from a parsed package.json; sanitize anyway since this lands in a query.
    let safe_app: String = app_name
        .chars()
        .filter(|c| c.is_ascii_alphanumeric() || *c == '_')
        .collect();
    let safe_version: String = version
        .chars()
        .filter(|c| c.is_ascii_alphanumeric() || *c == '.' || *c == '-')
        .collect();
    // UPSERT, deliberately not DELETE + CREATE. The old form was chosen when the
    // SSP circuit skipped `_00_*` tables at bootstrap (an UPDATE to a row the
    // circuit never saw carried no weight). The circuit has loaded
    // `_00_app_release` at bootstrap for a long time now, and the DELETE was
    // doing real damage: SurrealDB cascades a record delete over every graph
    // edge pointing at it, i.e. every client's `_00_list_ref_*` membership edge
    // to the release row, in the same transaction as the scheduler ingest
    // callbacks, while the SSP is writing edges on those very tables. On
    // 2026-09-06 that pattern coincided with a SurrealDB 3.0.5 wedge (accepts
    // TCP, answers nothing, 0% CPU) that the control plane had to heal by
    // recreating the database, and the delete-then-create pair also left the
    // scheduler's replica without the row, which triggered a 15-minute full
    // re-clone on its next start. One UPSERT is one UPDATE (or CREATE) event.
    let q = format!(
        "UPSERT _00_app_release:{app} SET app = '{app}', \
         version = '{version}', cache_bust = {cache_bust}, mandatory = {mandatory}, \
         released_at = time::now();",
        app = safe_app,
        version = safe_version,
        cache_bust = cache_bust,
        mandatory = mandatory,
    );
    match surreal_client.query(&q) {
        Ok(_) => println!(
            "  ▸ Release row updated: {} -> v{}{}{}",
            safe_app,
            safe_version,
            if cache_bust { " [cache-bust]" } else { "" },
            if mandatory { " [mandatory]" } else { "" },
        ),
        Err(e) => println!(
            "  ▸ Warning: failed to update release row for '{}' ({e:?}); clients will not be notified.",
            safe_app
        ),
    }
}

/// `spky release <app>`: manually announce a release by updating the app's
/// `_00_app_release` row on the cloud deployment — the same write `spky
/// deploy` performs automatically for frontends, for when you need to (re)set
/// the flags or announce a version without deploying (e.g. after a git-linked
/// deploy that should have been cache-busting, or a store rollout of a mobile
/// app the CLI never deploys).
pub fn release(
    app: String,
    version: Option<String>,
    cache_bust: bool,
    mandatory: bool,
) -> Result<()> {
    let config_path = std::env::current_dir()?.join("sp00ky.yml");
    let config = backend::load_config(&config_path);
    let config_dir = config_path.parent().unwrap_or(std::path::Path::new("."));

    let version = match version {
        Some(v) => v,
        None => {
            let frontend = config
                .frontend()
                .filter(|(n, _)| *n == app)
                .map(|(_, a)| a)
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "'{}' is not a frontend in sp00ky.yml — pass --version <v> explicitly.",
                        app
                    )
                })?;
            frontend_package_version(config_dir, frontend).ok_or_else(|| {
                anyhow::anyhow!(
                    "No versioned package.json found for frontend '{}' — pass --version <v>.",
                    app
                )
            })?
        }
    };

    let cloud = resolve_cloud_surreal(&config_path)?;
    let resolved = config.resolved_surrealdb();
    write_app_release_row(
        &cloud.url,
        &cloud.password,
        &resolved.namespace,
        &resolved.database,
        &app,
        &version,
        cache_bust,
        mandatory,
    );
    Ok(())
}

/// Resolve the full EnvConfig for cloud deploy, returning "KEY=VAL" strings.
fn resolve_env_for_deploy(
    env: &Option<backend::EnvConfig>,
    config_dir: &std::path::Path,
    client: &mut CloudClient,
    pid: &str,
) -> Vec<String> {
    let env = match env {
        Some(e) => e,
        None => return Vec::new(),
    };
    match env {
        backend::EnvConfig::Source(source) => {
            resolve_deploy_env_source(source, config_dir, client, pid)
        }
        backend::EnvConfig::List(sources) => {
            let mut merged = std::collections::BTreeMap::new();
            for source in sources {
                for entry in resolve_deploy_env_source(source, config_dir, client, pid) {
                    if let Some((k, v)) = entry.split_once('=') {
                        merged.insert(k.to_string(), v.to_string());
                    }
                }
            }
            merged
                .into_iter()
                .map(|(k, v)| format!("{}={}", k, v))
                .collect()
        }
        backend::EnvConfig::PerEnvironment { cloud, .. } => match cloud {
            Some(entry) => resolve_deploy_env_entry(entry, config_dir, client, pid),
            None => Vec::new(),
        },
    }
}

/// Build a backend manifest JSON value.
fn build_backend_manifest(
    name: &str,
    slug: &str,
    image_id: &Option<String>,
    port: u16,
    deploy: &backend::AppDeployConfig,
    merged_env: &[String],
    cmd: &Option<String>,
    working_dir: &Option<String>,
    app_config: &backend::AppConfig,
) -> serde_json::Value {
    let resources = deploy
        .resources
        .clone()
        .unwrap_or(backend::BackendDeployResources {
            vcpus: 1,
            memory: 512,
            disk: 5,
        });
    let mut manifest = serde_json::json!({
        "name": name,
        "image": format!("{}/{}", slug, name),
        "image_hash": image_id,
        "port": port,
        "ports": deploy.ports,
        "expose": deploy.expose,
        "grpc_port": deploy.grpc_port,
        "resources": {
            "vcpus": resources.vcpus,
            "memory_mb": resources.memory,
            "disk_gb": resources.disk,
        },
        "env": merged_env,
        "cmd": cmd,
        "working_dir": working_dir,
        "healthcheck": deploy.healthcheck,
        "timeout": deploy.timeout,
        "timeout_overridable": deploy.timeout_overridable,
    });
    if let Some(ref method) = app_config.method {
        if let Some(ref table) = method.table {
            manifest["method"] = serde_json::json!({
                "type": method.method_type,
                "table": table,
            });
        }
    }
    manifest
}

/// Build auto-injected SPKY_* vars for cloud deploy manifests.
/// These are base vars — the orchestrator adds IP-specific ones (SPKY_DB_URL, SPKY_DB_PASS, etc.)
fn build_spky_cloud_vars(config: &backend::Sp00kyConfig) -> Vec<String> {
    let resolved = config.resolved_surrealdb();
    vec![
        "SPKY_ENV=cloud".into(),
        format!("SPKY_DB_NS={}", resolved.namespace),
        format!("SPKY_DB_NAME={}", resolved.database),
    ]
}

/// Warn if a frontend app uses vault without a whitelist.
fn warn_frontend_vault_no_whitelist_deploy(name: &str, env: &Option<backend::EnvConfig>) {
    if let Some(backend::EnvConfig::Source(backend::EnvSource::Str(s))) = env {
        if s == "vault" {
            eprintln!("  \x1b[33mwarning\x1b[0m: Frontend app '{}' uses vault without a whitelist. Consider using vault: [KEY1, KEY2] to avoid exposing all secrets to the frontend.", name);
        }
    }
}

/// Build a frontend manifest JSON value.
fn build_frontend_manifest(
    name: &str,
    slug: &str,
    image_id: &Option<String>,
    port: u16,
    resources: &backend::BackendDeployResources,
    merged_env: &[String],
    cmd: &Option<String>,
    working_dir: &Option<String>,
) -> serde_json::Value {
    serde_json::json!({
        // The frontend's name from sp00ky.yml (e.g. "web"). The control plane
        // exposes the app at `{slug}-{name}.{domain}`; without it the server
        // falls back to "app", silently moving the endpoint to `{slug}-app`.
        "name": name,
        "image": format!("{}/frontend", slug),
        "image_hash": image_id,
        "port": port,
        "resources": {
            "vcpus": resources.vcpus,
            "memory_mb": resources.memory,
            "disk_gb": resources.disk,
        },
        "env": merged_env,
        "cmd": cmd,
        "working_dir": working_dir,
    })
}

/// Free/Cloudflare frontend: build the SPA and upload its static output dir to
/// the control plane (`PUT /frontend-assets`), which publishes it via Cloudflare
/// Workers Static Assets. No Docker image — the SPA talks directly to SurrealDB
/// via the baked `VITE_DB_ENDPOINT`.
fn deploy_static_frontend_cloudflare(
    client: &CloudClient,
    pid: &str,
    slug: &str,
    frontend_name: &str,
    frontend_app: &backend::AppConfig,
    config_dir: &std::path::Path,
    db_endpoint: Option<&str>,
    db_ns: Option<&str>,
    db_db: Option<&str>,
) -> Result<()> {
    let deploy = frontend_app
        .deploy
        .as_ref()
        .context("Frontend app is missing 'deploy' configuration")?;
    let static_cfg = deploy.static_site.as_ref().context(
        "Free-plan frontend needs a 'deploy.static' block ({ build, dir }). \
         Add it under the web app in sp00ky.yml.",
    )?;

    let app_dir = match &deploy.context {
        Some(ctx) => config_dir.join(ctx),
        None => config_dir.to_path_buf(),
    };

    if let Some(build) = &static_cfg.build {
        println!("  Building static frontend ('{}'): {}", frontend_name, build);
        let mut cmd = std::process::Command::new("sh");
        cmd.arg("-c").arg(build).current_dir(&app_dir);
        if let Some(ep) = db_endpoint {
            cmd.env("VITE_DB_ENDPOINT", ep);
            println!("  DB endpoint: {}", ep);
        }
        if let Some(ns) = db_ns {
            cmd.env("VITE_DB_NS", ns);
        }
        if let Some(db) = db_db {
            cmd.env("VITE_DB_DB", db);
        }
        if db_ns.is_some() || db_db.is_some() {
            println!("  DB ns/db: {}/{}", db_ns.unwrap_or("-"), db_db.unwrap_or("-"));
        }
        let status = cmd
            .stdout(std::process::Stdio::inherit())
            .stderr(std::process::Stdio::inherit())
            .status()
            .context("Failed to run static frontend build")?;
        if !status.success() {
            bail!("Static frontend build failed");
        }
    }

    let dist_dir = app_dir.join(&static_cfg.dir);
    if !dist_dir.is_dir() {
        bail!(
            "Static output dir not found: {} (set deploy.static.dir)",
            dist_dir.display()
        );
    }

    // gzip'd tar of the built dir contents (files at the archive root).
    let tmp_tar = std::env::temp_dir().join(format!("sp00ky-{}-web.tar.gz", slug));
    let _ = fs::remove_file(&tmp_tar);
    let tar_status = std::process::Command::new("tar")
        .arg("-czf")
        .arg(&tmp_tar)
        .arg("-C")
        .arg(&dist_dir)
        .arg(".")
        .status()
        .context("Failed to tar static frontend")?;
    if !tar_status.success() {
        bail!("Failed to archive static frontend dir");
    }

    let data = fs::read(&tmp_tar).context("Failed to read static frontend archive")?;
    let total = data.len();
    println!(
        "  Uploading static frontend ({:.1}KB)...",
        total as f64 / 1024.0
    );
    let url = format!(
        "{}/v1/projects/{}/frontend-assets",
        upload_base_url(&client.base_url),
        pid
    );
    let progress = ProgressReader::new(&data, total, "  Uploading frontend");
    match ureq::put(&url)
        .set("Authorization", &format!("Bearer {}", client.token))
        .set("Content-Type", "application/gzip")
        .set("Content-Length", &total.to_string())
        .send(progress)
    {
        Ok(_) => println!(),
        Err(ureq::Error::Status(code, resp)) => {
            println!();
            let body = resp.into_string().unwrap_or_default();
            bail!("Frontend asset upload failed (HTTP {}): {}", code, body);
        }
        Err(ureq::Error::Transport(t)) => {
            println!();
            bail!("Frontend asset upload failed: {}", t);
        }
    }
    let _ = fs::remove_file(&tmp_tar);
    println!("  Static frontend uploaded — will publish on Cloudflare.");
    Ok(())
}

pub fn deploy(
    upgrade: bool,
    clean: bool,
    only: Vec<String>,
    cache_bust: bool,
    mandatory: bool,
) -> Result<()> {
    // Guided flow: ensure login → project → billing before expensive work
    let creds = ensure_login()?;
    let mut client = CloudClient::new(&creds);
    let (slug, project) = ensure_project(&mut client)?;
    let pid = project_id(&project);
    let mut platform = project
        .get("platform")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    // The free plan always runs on Cloudflare — derive it if the API omitted the
    // platform (older responses), so the static-frontend path is taken.
    if platform.is_empty() && project.get("plan").and_then(|v| v.as_str()) == Some("free") {
        platform = "cloudflare".to_string();
    }
    ensure_billing_active(&mut client, &slug, &project)?;

    println!();
    println!("Deploying project '{}'...", slug);

    // Collected rather than printed-and-forgotten: this decides the exit code,
    // so a deploy cannot report success while the internal schema was skipped.
    let mut schema_failures = SchemaApplyFailures::default();

    // Load sp00ky.yml and find deployable apps
    let config_path = std::env::current_dir()?.join("sp00ky.yml");
    let config = backend::load_config(&config_path);
    config.validate()?;

    // --only: restrict this deploy to a subset of apps. The selected apps are
    // built/uploaded and sent with `partial: true`; the control plane merges them
    // over the previous deployment so every other running service is left alone.
    // Validate names up-front against the deployable apps in sp00ky.yml.
    let only_filter: Option<std::collections::HashSet<String>> = if only.is_empty() {
        None
    } else {
        let mut valid: std::collections::HashSet<String> = std::collections::HashSet::new();
        for (n, _) in config.backends() {
            valid.insert(n.to_string());
        }
        for (n, _) in config.docker_apps() {
            valid.insert(n.to_string());
        }
        if let Some((n, _)) = config.frontend() {
            valid.insert(n.to_string());
        }
        let unknown: Vec<String> = only
            .iter()
            .filter(|n| !valid.contains(n.as_str()))
            .cloned()
            .collect();
        if !unknown.is_empty() {
            let mut names: Vec<String> = valid.into_iter().collect();
            names.sort();
            bail!(
                "--only: unknown app(s): {}. Valid apps: {}",
                unknown.join(", "),
                names.join(", ")
            );
        }
        Some(only.iter().cloned().collect())
    };
    let is_partial = only_filter.is_some();

    let config_dir = config_path.parent().unwrap_or(std::path::Path::new("."));
    let mut backend_manifests: Vec<serde_json::Value> = Vec::new();
    let mut external_backends: Vec<serde_json::Value> = Vec::new();

    for (name, app_config) in config.backends() {
        // devOnly apps are local-only (started by `spky dev`); never deployed.
        if !app_config.deploys() {
            println!("  Skipping devOnly backend '{}' (local dev only)", name);
            continue;
        }
        // External backends are self-hosted — skip cloud deployment
        if app_config.resolved_hosting() == HostingMode::External {
            println!(
                "  Skipping external backend '{}' (self-hosted at {})",
                name,
                app_config.base_url.as_deref().unwrap_or("?")
            );
            external_backends.push(serde_json::json!({
                "name": name,
                "base_url": app_config.base_url,
            }));
            continue;
        }
        // --only: skip building backends not in the selected subset.
        if let Some(ref f) = only_filter {
            if !f.contains(name) {
                println!("  Skipping backend '{}' (not in --only)", name);
                continue;
            }
        }

        let deploy = match &app_config.deploy {
            Some(d) => d,
            None => continue,
        };

        // Resolve Dockerfile path
        let dockerfile = deploy
            .dockerfile
            .clone()
            .unwrap_or_else(|| match &app_config.dev {
                Some(BackendDevConfig::Typed(BackendDevTypedConfig::Docker { file, .. })) => {
                    file.clone()
                }
                _ => "Dockerfile".to_string(),
            });

        let dockerfile_path = config_dir.join(&dockerfile);
        if !dockerfile_path.exists() {
            bail!(
                "Dockerfile not found for backend '{}': {}",
                name,
                dockerfile_path.display()
            );
        }

        let context_dir = match &deploy.context {
            Some(ctx) => config_dir.join(ctx),
            None => config_dir.to_path_buf(),
        };
        let image_tag = format!("sp00ky-backend-{}:deploy", name);
        let port = app_config.deploy_port();

        // Build Docker image
        println!(
            "  Building image for backend '{}' (dockerfile={}, context={})...",
            name,
            dockerfile_path.display(),
            context_dir.display()
        );
        let mut docker_build_args = vec![
            "build".to_string(),
            "--platform".to_string(),
            "linux/amd64".to_string(),
            "-t".to_string(),
            image_tag.clone(),
            "-f".to_string(),
            dockerfile_path.to_string_lossy().to_string(),
        ];
        // Build-time args (deploy.build_args): resolved from the same env shapes
        // as runtime env (incl. vault), passed only to `docker build`. Never added
        // to the container's runtime env. Mirrors the git-linked builder.
        for kv in resolve_env_for_deploy(&deploy.build_args, config_dir, &mut client, &pid) {
            docker_build_args.push("--build-arg".to_string());
            docker_build_args.push(kv);
        }
        docker_build_args.push(context_dir.to_string_lossy().to_string());
        let build_status = std::process::Command::new("docker")
            .args(&docker_build_args)
            .stdout(std::process::Stdio::inherit())
            .stderr(std::process::Stdio::inherit())
            .status()
            .context(format!("Failed to run docker build for backend '{}'", name))?;

        if !build_status.success() {
            bail!("Docker build failed for backend '{}'", name);
        }

        // Resolve env vars:
        //   image Config.Env (seeds PATH, LANG, venv, NODE_ENV, etc. —
        //     stripped by `docker export` so we have to carry them over
        //     in the manifest)
        //   → SPKY_* base
        //   → user config (wins on conflicts)
        let spky_vars = build_spky_cloud_vars(&config);
        let user_env = resolve_env_for_deploy(&app_config.env, config_dir, &mut client, &pid);
        let image_env = get_docker_env(&image_tag);
        let mut env_map = std::collections::BTreeMap::new();
        for entry in &image_env {
            if let Some((k, v)) = entry.split_once('=') {
                env_map.insert(k.to_string(), v.to_string());
            }
        }
        for entry in &spky_vars {
            if let Some((k, v)) = entry.split_once('=') {
                env_map.insert(k.to_string(), v.to_string());
            }
        }
        for entry in &user_env {
            if let Some((k, v)) = entry.split_once('=') {
                env_map.insert(k.to_string(), v.to_string());
            }
        }
        let merged_env: Vec<String> = env_map
            .into_iter()
            .map(|(k, v)| format!("{}={}", k, v))
            .collect();
        let working_dir = get_docker_workdir(&image_tag);

        // Check if image changed since last deploy (skip export+upload if unchanged)
        let image_id = get_docker_image_id(&image_tag);
        if let Some(ref id) = image_id {
            if let Some(remote_hash) = get_remote_image_hash(&client, &pid, name) {
                if remote_hash == *id {
                    println!("  Image for backend '{}' unchanged, skipping upload.", name);
                    let cmd = match get_docker_cmd(&image_tag) {
                        Some(c) => Some(c),
                        None => deploy.cmd.clone(),
                    };
                    backend_manifests.push(build_backend_manifest(
                        name,
                        &slug,
                        &image_id,
                        port,
                        deploy,
                        &merged_env,
                        &cmd,
                        &working_dir,
                        app_config,
                    ));
                    println!("  Backend '{}' ready for deployment.", name);
                    continue;
                }
            }
        }

        // Export image as flat rootfs tar.gz (not layered docker save)
        let tmp_tar = std::env::temp_dir().join(format!("sp00ky-{}-{}.tar.gz", slug, name));
        println!("  Exporting image for backend '{}'...", name);
        let container_name = format!("sp00ky-export-{}-{}", slug, name);
        let _ = std::process::Command::new("docker")
            .args(["rm", "-f", &container_name])
            .output();
        let create_out = std::process::Command::new("docker")
            .args(["create", "--name", &container_name, &image_tag])
            .output()
            .context(format!("Failed to create container for backend '{}'", name))?;
        if !create_out.status.success() {
            bail!("Failed to create container for backend '{}' export", name);
        }
        let export_status = std::process::Command::new("sh")
            .args([
                "-c",
                &format!(
                    "docker export {} | gzip > {}",
                    container_name,
                    tmp_tar.to_string_lossy()
                ),
            ])
            .status()
            .context("Failed to export backend container")?;
        let _ = std::process::Command::new("docker")
            .args(["rm", "-f", &container_name])
            .output();
        if !export_status.success() {
            bail!("Failed to export docker image for backend '{}'", name);
        }

        // Upload image tar to API with progress
        let image_data = fs::read(&tmp_tar)
            .context(format!("Failed to read image tar for backend '{}'", name))?;
        let total = image_data.len();
        println!(
            "  Uploading image for backend '{}' ({:.1}MB)...",
            name,
            total as f64 / 1_048_576.0
        );

        let upload_url = format!(
            "{}/v1/projects/{}/images/{}",
            upload_base_url(&client.base_url),
            pid,
            name
        );
        let progress = ProgressReader::new(&image_data, total, &format!("  Uploading '{}'", name));
        let hash_header = image_id.as_deref().unwrap_or("");
        match ureq::put(&upload_url)
            .set("Authorization", &format!("Bearer {}", client.token))
            .set("Content-Type", "application/octet-stream")
            .set("Content-Length", &total.to_string())
            .set("X-Image-Hash", hash_header)
            .send(progress)
        {
            Ok(_) => {
                println!();
            }
            Err(ureq::Error::Status(code, resp)) => {
                println!();
                let body = resp.into_string().unwrap_or_default();
                let err = format!("HTTP {}: {}", code, body);
                if !upload_landed_despite_error(&client, &pid, name, image_id.as_deref(), &err) {
                    bail!("Image upload failed for '{}' ({})", name, err);
                }
            }
            Err(ureq::Error::Transport(t)) => {
                println!();
                let err = t.to_string();
                if !upload_landed_despite_error(&client, &pid, name, image_id.as_deref(), &err) {
                    bail!("Image upload failed for '{}': {}", name, err);
                }
            }
        }

        // Clean up temp file
        let _ = fs::remove_file(&tmp_tar);

        // Build manifest
        let cmd = match get_docker_cmd(&image_tag) {
            Some(c) => Some(c),
            None => deploy.cmd.clone(),
        };
        backend_manifests.push(build_backend_manifest(
            name,
            &slug,
            &image_id,
            port,
            deploy,
            &merged_env,
            &cmd,
            &working_dir,
            app_config,
        ));

        println!("  Backend '{}' ready for deployment.", name);
    }

    // Docker apps (type: docker) — run a prebuilt image. devOnly ones are local
    // only; the rest deploy by pulling the image and reusing the same flat-rootfs
    // export/upload/manifest path as backends (the pipeline operates on a local
    // image tag, so a pulled image just skips the build step).
    for (name, app_config) in config.docker_apps() {
        if !app_config.deploys() {
            println!("  Skipping devOnly docker app '{}' (local dev only)", name);
            continue;
        }
        // --only: skip docker apps not in the selected subset.
        if let Some(ref f) = only_filter {
            if !f.contains(name) {
                println!("  Skipping docker app '{}' (not in --only)", name);
                continue;
            }
        }
        let image_ref = app_config
            .image
            .clone()
            .context(format!("Docker app '{}' is missing 'image'", name))?;
        let port = app_config.deploy_port();

        println!(
            "  Pulling image for docker app '{}' ({})...",
            name, image_ref
        );
        let pull_status = std::process::Command::new("docker")
            .args(["pull", "--platform", "linux/amd64", &image_ref])
            .stdout(std::process::Stdio::inherit())
            .stderr(std::process::Stdio::inherit())
            .status()
            .context(format!("Failed to docker pull for docker app '{}'", name))?;
        if !pull_status.success() {
            bail!("docker pull failed for docker app '{}'", name);
        }
        let image_tag = image_ref.clone();

        // Env: image Config.Env → SPKY_* base → user config (wins).
        let spky_vars = build_spky_cloud_vars(&config);
        let user_env = resolve_env_for_deploy(&app_config.env, config_dir, &mut client, &pid);
        let image_env = get_docker_env(&image_tag);
        let mut env_map = std::collections::BTreeMap::new();
        for entry in image_env
            .iter()
            .chain(spky_vars.iter())
            .chain(user_env.iter())
        {
            if let Some((k, v)) = entry.split_once('=') {
                env_map.insert(k.to_string(), v.to_string());
            }
        }
        let merged_env: Vec<String> = env_map
            .into_iter()
            .map(|(k, v)| format!("{}={}", k, v))
            .collect();
        let working_dir = get_docker_workdir(&image_tag);
        let image_id = get_docker_image_id(&image_tag);

        if let Some(ref id) = image_id {
            if let Some(remote_hash) = get_remote_image_hash(&client, &pid, name) {
                if remote_hash == *id {
                    println!(
                        "  Image for docker app '{}' unchanged, skipping upload.",
                        name
                    );
                    let cmd = if app_config.args.is_empty() {
                        match get_docker_cmd(&image_tag) {
                            Some(c) => Some(c),
                            None => app_config.deploy.as_ref().and_then(|d| d.cmd.clone()),
                        }
                    } else {
                        Some(app_config.args.join(" "))
                    };
                    let deploy = app_config
                        .deploy
                        .clone()
                        .unwrap_or(backend::AppDeployConfig {
                            dockerfile: None,
                            context: None,
                            port: Some(port),
                            ports: None,
                            grpc_port: None,
                            resources: None,
                            expose: false,
                            healthcheck: None,
                            timeout: None,
                            timeout_overridable: None,
                            cmd: None,
                            build_args: None,
                            static_site: None,
                        });
                    backend_manifests.push(build_backend_manifest(
                        name,
                        &slug,
                        &image_id,
                        port,
                        &deploy,
                        &merged_env,
                        &cmd,
                        &working_dir,
                        app_config,
                    ));
                    println!("  Docker app '{}' ready for deployment.", name);
                    continue;
                }
            }
        }

        // Export flat rootfs and upload (mirrors the backend path).
        let tmp_tar = std::env::temp_dir().join(format!("sp00ky-{}-{}.tar.gz", slug, name));
        println!("  Exporting image for docker app '{}'...", name);
        let container_name = format!("sp00ky-export-{}-{}", slug, name);
        let _ = std::process::Command::new("docker")
            .args(["rm", "-f", &container_name])
            .output();
        let create_out = std::process::Command::new("docker")
            .args(["create", "--name", &container_name, &image_tag])
            .output()
            .context(format!(
                "Failed to create container for docker app '{}'",
                name
            ))?;
        if !create_out.status.success() {
            bail!(
                "Failed to create container for docker app '{}' export",
                name
            );
        }
        let export_status = std::process::Command::new("sh")
            .args([
                "-c",
                &format!(
                    "docker export {} | gzip > {}",
                    container_name,
                    tmp_tar.to_string_lossy()
                ),
            ])
            .status()
            .context("Failed to export docker-app container")?;
        let _ = std::process::Command::new("docker")
            .args(["rm", "-f", &container_name])
            .output();
        if !export_status.success() {
            bail!("Failed to export image for docker app '{}'", name);
        }

        let image_data = fs::read(&tmp_tar).context(format!(
            "Failed to read image tar for docker app '{}'",
            name
        ))?;
        let total = image_data.len();
        println!(
            "  Uploading image for docker app '{}' ({:.1}MB)...",
            name,
            total as f64 / 1_048_576.0
        );
        let upload_url = format!(
            "{}/v1/projects/{}/images/{}",
            upload_base_url(&client.base_url),
            pid,
            name
        );
        let progress = ProgressReader::new(&image_data, total, &format!("  Uploading '{}'", name));
        match ureq::put(&upload_url)
            .set("Authorization", &format!("Bearer {}", client.token))
            .set("Content-Type", "application/octet-stream")
            .set("Content-Length", &total.to_string())
            .set("X-Image-Hash", image_id.as_deref().unwrap_or(""))
            .send(progress)
        {
            Ok(_) => {
                println!();
            }
            Err(ureq::Error::Status(code, resp)) => {
                println!();
                let err = format!("HTTP {}: {}", code, resp.into_string().unwrap_or_default());
                if !upload_landed_despite_error(&client, &pid, name, image_id.as_deref(), &err) {
                    bail!("Image upload failed for '{}' ({})", name, err);
                }
            }
            Err(ureq::Error::Transport(t)) => {
                println!();
                let err = t.to_string();
                if !upload_landed_despite_error(&client, &pid, name, image_id.as_deref(), &err) {
                    bail!("Image upload failed for '{}': {}", name, err);
                }
            }
        }
        let _ = fs::remove_file(&tmp_tar);

        // Manifest: image cmd, or the configured args as the container command.
        let cmd = if app_config.args.is_empty() {
            match get_docker_cmd(&image_tag) {
                Some(c) => Some(c),
                None => app_config.deploy.as_ref().and_then(|d| d.cmd.clone()),
            }
        } else {
            Some(app_config.args.join(" "))
        };
        let deploy = app_config
            .deploy
            .clone()
            .unwrap_or(backend::AppDeployConfig {
                dockerfile: None,
                context: None,
                port: Some(port),
                ports: None,
                grpc_port: None,
                resources: None,
                expose: false,
                healthcheck: None,
                timeout: None,
                timeout_overridable: None,
                cmd: None,
                build_args: None,
                static_site: None,
            });
        backend_manifests.push(build_backend_manifest(
            name,
            &slug,
            &image_id,
            port,
            &deploy,
            &merged_env,
            &cmd,
            &working_dir,
            app_config,
        ));
        println!("  Docker app '{}' ready for deployment.", name);
    }

    // Build SurrealDB manifest
    let resolved_surreal = config.resolved_surrealdb();
    let mut surrealdb_manifest = match resolved_surreal.hosting {
        HostingMode::Cloud => serde_json::json!({
            "hosting": "cloud",
            "namespace": resolved_surreal.namespace,
            "database": resolved_surreal.database,
        }),
        HostingMode::External => {
            use base64::Engine as _;
            // Resolve credentials — each is a literal or a `{ vault: KEY }`
            // reference — against the decrypted prod vault. Load the vault only
            // when at least one field actually references it.
            let needs_vault = resolved_surreal.username.vault_key().is_some()
                || resolved_surreal.password.vault_key().is_some()
                || resolved_surreal
                    .endpoint
                    .as_ref()
                    .and_then(|e| e.vault_key())
                    .is_some();
            let vault = if needs_vault {
                load_vault_map_for_deploy(&mut client, &pid, true)
            } else {
                std::collections::HashMap::new()
            };
            let endpoint = match &resolved_surreal.endpoint {
                Some(v) => Some(resolve_secret_value(v, &vault)?),
                None => None,
            };
            let username = resolve_secret_value(&resolved_surreal.username, &vault)?;
            let password = resolve_secret_value(&resolved_surreal.password, &vault)?;
            // Free/Cloudflare: the SSP Worker talks to the DB over HTTP-RPC and
            // needs a ready Authorization header value. Send `auth` = Basic
            // creds; the control plane stores it as surrealdb_auth.
            let auth = format!(
                "Basic {}",
                base64::engine::general_purpose::STANDARD
                    .encode(format!("{username}:{password}"))
            );
            serde_json::json!({
                "hosting": "external",
                "endpoint": endpoint,
                "namespace": resolved_surreal.namespace,
                "database": resolved_surreal.database,
                "auth": auth,
            })
        }
    };
    // Attach persistent bucket-storage provisioning when configured. The
    // spooky-cloud control plane (Go repo ~/dev/spooky-cloud) must read this,
    // provision a `sizeGB` volume on the SurrealDB server, and mount it at
    // `bucketPath` so `file:/buckets/<name>` bucket backends resolve to durable
    // storage. It must ALSO, on that SurrealDB server:
    //   - enable the `files` experimental cap
    //     (SURREAL_CAPS_ALLOW_EXPERIMENTAL=…,files), and
    //   - allowlist the mount path via SURREAL_BUCKET_FOLDER_ALLOWLIST=<bucketPath>
    //     — verified required: `--allow-all` alone yields "File access denied"
    //     and every bucket `put` fails without it.
    // Omitted when unset → byte-compatible with existing deploys.
    if let Some(gb) = config.bucket_storage_gb() {
        if let Some(obj) = surrealdb_manifest.as_object_mut() {
            obj.insert(
                "storage".to_string(),
                serde_json::json!({
                    "sizeGB": gb,
                    "bucketPath": crate::backend::BUCKET_VOLUME_PATH,
                }),
            );
        }
    }

    // Build and upload frontend if configured
    let mut frontend_manifest: Option<serde_json::Value> = None;
    let frontend_selected = config
        .frontend()
        .map(|(n, _)| only_filter.as_ref().map_or(true, |f| f.contains(n)))
        .unwrap_or(false);
    if !frontend_selected {
        if let Some((n, _)) = config.frontend() {
            if only_filter.is_some() {
                println!("  Skipping frontend '{}' (not in --only)", n);
            }
        }
    }
    if let Some((frontend_name, frontend_app)) = config.frontend().filter(|_| frontend_selected) {
        if platform == "cloudflare" {
        // Free plan: static SPA on Cloudflare Workers Static Assets. Bake the
        // external SurrealDB endpoint the SPA connects to directly. No manifest —
        // the control plane's free deploy path publishes the uploaded assets.
        let db_endpoint = surrealdb_manifest
            .get("endpoint")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let db_ns = surrealdb_manifest
            .get("namespace")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let db_db = surrealdb_manifest
            .get("database")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        deploy_static_frontend_cloudflare(
            &client,
            &pid,
            &slug,
            frontend_name,
            frontend_app,
            config_dir,
            db_endpoint.as_deref(),
            db_ns.as_deref(),
            db_db.as_deref(),
        )?;
        } else {
        let frontend_deploy = frontend_app
            .deploy
            .as_ref()
            .context("Frontend app is missing 'deploy' configuration")?;
        let dockerfile = frontend_deploy
            .dockerfile
            .as_deref()
            .context("Frontend app deploy is missing 'dockerfile'")?;
        let dockerfile_path = config_dir.join(dockerfile);

        if !dockerfile_path.exists() {
            bail!(
                "Frontend Dockerfile not found: {}",
                dockerfile_path.display()
            );
        }

        let context_dir = match &frontend_deploy.context {
            Some(ctx) => config_dir.join(ctx),
            None => config_dir.to_path_buf(),
        };
        let image_tag = format!("sp00ky-frontend-{}:deploy", slug);
        let port = frontend_app.deploy_port();

        // Auto-compute the SurrealDB WebSocket endpoint for the frontend
        let db_ws_endpoint = derive_db_ws_endpoint(&client.base_url, &slug);

        // Resolve env vars: (merged_env is built after the docker build so
        // the image's Config.Env can be seeded first — see below.)
        warn_frontend_vault_no_whitelist_deploy(frontend_name, &frontend_app.env);
        let spky_vars = build_spky_cloud_vars(&config);
        let user_env = resolve_env_for_deploy(&frontend_app.env, config_dir, &mut client, &pid);

        println!("  Building frontend image ('{}')...", frontend_name);
        let mut build_args = vec![
            "build".to_string(),
            "--no-cache".to_string(),
            "--platform".to_string(),
            "linux/amd64".to_string(),
            "-t".to_string(),
            image_tag.clone(),
            "-f".to_string(),
            dockerfile_path.to_string_lossy().to_string(),
        ];
        if let Some(ref endpoint) = db_ws_endpoint {
            build_args.push("--build-arg".to_string());
            build_args.push(format!("VITE_DB_ENDPOINT={}", endpoint));
            println!("  DB endpoint: {}", endpoint);
        }
        // Frontend env (sp00ky.yml env.vault whitelist, e.g. VITE_GOOGLE_CLIENT_ID,
        // VITE_TWITCH_CLIENT_ID) must reach the build as --build-arg: a static SPA
        // bakes import.meta.env at `vite build` time, so runtime container env is
        // useless. Mirror the git-linked builder (spooky-cloud internal/linking).
        // VITE_DB_ENDPOINT is auto-derived above; skip a vault duplicate so it wins.
        for kv in &user_env {
            if let Some((k, _)) = kv.split_once('=') {
                if k == "VITE_DB_ENDPOINT" {
                    continue;
                }
                build_args.push("--build-arg".to_string());
                build_args.push(kv.clone());
            }
        }
        // Explicit deploy.build_args (build-time only, resolved incl. vault). Applied
        // after env so they win on key clash. Never enters runtime env.
        for kv in resolve_env_for_deploy(&frontend_deploy.build_args, config_dir, &mut client, &pid) {
            build_args.push("--build-arg".to_string());
            build_args.push(kv);
        }
        build_args.push(context_dir.to_string_lossy().to_string());

        let build_status = std::process::Command::new("docker")
            .args(&build_args)
            .stdout(std::process::Stdio::inherit())
            .stderr(std::process::Stdio::inherit())
            .status()
            .context("Failed to run docker build for frontend")?;

        if !build_status.success() {
            bail!("Docker build failed for frontend");
        }

        // Now that the image is built, merge the env in priority order:
        // image Config.Env → SPKY → user (user wins on conflicts).
        let image_env = get_docker_env(&image_tag);
        let mut env_map = std::collections::BTreeMap::new();
        for entry in &image_env {
            if let Some((k, v)) = entry.split_once('=') {
                env_map.insert(k.to_string(), v.to_string());
            }
        }
        for entry in &spky_vars {
            if let Some((k, v)) = entry.split_once('=') {
                env_map.insert(k.to_string(), v.to_string());
            }
        }
        for entry in &user_env {
            if let Some((k, v)) = entry.split_once('=') {
                env_map.insert(k.to_string(), v.to_string());
            }
        }
        let merged_env: Vec<String> = env_map
            .into_iter()
            .map(|(k, v)| format!("{}={}", k, v))
            .collect();
        let working_dir = get_docker_workdir(&image_tag);

        // Check if frontend image changed since last deploy
        let frontend_image_id = get_docker_image_id(&image_tag);
        let frontend_unchanged = frontend_image_id
            .as_ref()
            .and_then(|id| get_remote_image_hash(&client, &pid, "frontend").map(|h| h == *id))
            .unwrap_or(false);

        let resources =
            frontend_deploy
                .resources
                .clone()
                .unwrap_or(backend::BackendDeployResources {
                    vcpus: 1,
                    memory: 512,
                    disk: 5,
                });

        if frontend_unchanged {
            println!("  Frontend image unchanged, skipping upload.");
            let cmd = match get_docker_cmd(&image_tag) {
                Some(c) => Some(c),
                None => frontend_deploy.cmd.clone(),
            };
            frontend_manifest = Some(build_frontend_manifest(
                frontend_name,
                &slug,
                &frontend_image_id,
                port,
                &resources,
                &merged_env,
                &cmd,
                &working_dir,
            ));
        } else {
            let tmp_tar = std::env::temp_dir().join(format!("sp00ky-{}-frontend.tar.gz", slug));
            println!("  Exporting frontend image...");
            let container_name = format!("sp00ky-export-{}", slug);
            let _ = std::process::Command::new("docker")
                .args(["rm", "-f", &container_name])
                .output();
            let create_status = std::process::Command::new("docker")
                .args(["create", "--name", &container_name, &image_tag])
                .output()
                .context("Failed to create container for export")?;
            if !create_status.status.success() {
                bail!("Failed to create container for frontend export");
            }
            let export_status = std::process::Command::new("sh")
                .args([
                    "-c",
                    &format!(
                        "docker export {} | gzip > {}",
                        container_name,
                        tmp_tar.to_string_lossy()
                    ),
                ])
                .status()
                .context("Failed to export frontend container")?;
            let _ = std::process::Command::new("docker")
                .args(["rm", "-f", &container_name])
                .output();
            if !export_status.success() {
                bail!("Failed to export frontend container");
            }

            let image_data = fs::read(&tmp_tar).context("Failed to read frontend image tar")?;
            let total = image_data.len();
            println!(
                "  Uploading frontend image ({:.1}MB)...",
                total as f64 / 1_048_576.0
            );
            let upload_url = format!(
                "{}/v1/projects/{}/images/frontend",
                upload_base_url(&client.base_url),
                pid
            );
            let progress = ProgressReader::new(&image_data, total, "  Uploading frontend");
            let hash_header = frontend_image_id.as_deref().unwrap_or("");
            match ureq::put(&upload_url)
                .set("Authorization", &format!("Bearer {}", client.token))
                .set("Content-Type", "application/octet-stream")
                .set("Content-Length", &total.to_string())
                .set("X-Image-Hash", hash_header)
                .send(progress)
            {
                Ok(_) => {
                    println!();
                }
                Err(ureq::Error::Status(code, resp)) => {
                    println!();
                    let body = resp.into_string().unwrap_or_default();
                    let err = format!("HTTP {}: {}", code, body);
                    if !upload_landed_despite_error(&client, &pid, "frontend", frontend_image_id.as_deref(), &err) {
                        bail!("Frontend image upload failed ({})", err);
                    }
                }
                Err(ureq::Error::Transport(t)) => {
                    println!();
                    let err = t.to_string();
                    if !upload_landed_despite_error(&client, &pid, "frontend", frontend_image_id.as_deref(), &err) {
                        bail!("Frontend image upload failed: {}", err);
                    }
                }
            }
            let _ = fs::remove_file(&tmp_tar);

            let cmd = match get_docker_cmd(&image_tag) {
                Some(c) => Some(c),
                None => frontend_deploy.cmd.clone(),
            };
            frontend_manifest = Some(build_frontend_manifest(
                frontend_name,
                &slug,
                &frontend_image_id,
                port,
                &resources,
                &merged_env,
                &cmd,
                &working_dir,
            ));

            println!("  Frontend ready for deployment.");
        } // end else (frontend changed)
        } // end else (non-cloudflare docker frontend)
    }

    let ssp_count = config.deployment.as_ref().and_then(|d| d.ssp_count);

    // Cloud-side `RUST_LOG` for the scheduler + SSP containers. Only sent
    // when sp00ky.yml has `logLevel:` (or its `cloud:` sub-key) set —
    // otherwise omit so the cloud control plane keeps the previous setting
    // (or its `info` default). Persists across deploys via
    // `projects.config.log_level` server-side.
    let cloud_log_level = config
        .log_level
        .as_ref()
        .and_then(|c| c.resolved(DeployEnv::Cloud));

    // Free (Cloudflare): assemble SPKY_JOB_CONFIG from outbox backends so the
    // SSP Worker can dispatch jobs to the (self-hosted) backends. base_url must
    // be a public URL the Worker can reach; backends without one are skipped.
    let mut ssp_job_config = serde_json::Value::Null;
    if platform == "cloudflare" {
        let mut entries: Vec<serde_json::Value> = Vec::new();
        for (name, app) in config.backends() {
            let Some(m) = &app.method else { continue };
            if !matches!(m.method_type, crate::backend::MethodType::Outbox) {
                continue;
            }
            if let (Some(table), Some(base_url)) = (m.table.as_ref(), app.base_url.as_ref()) {
                entries.push(serde_json::json!({
                    "name": name,
                    "base_url": base_url,
                    "table": table,
                }));
            }
        }
        ssp_job_config = serde_json::json!(entries);
    }

    // Per-role env for the infra containers (`deployment.env` in sp00ky.yml).
    // Omitted when unset so the control plane keeps the previous setting, the
    // same contract as `log_level` above.
    let infra_env = config
        .deployment
        .as_ref()
        .and_then(|d| d.env.as_ref())
        .map(|e| serde_json::json!(e));

    let deploy_body = serde_json::json!({
        "infra_env": infra_env,
        "surrealdb": surrealdb_manifest,
        "backends": backend_manifests,
        "external_backends": external_backends,
        "frontend": frontend_manifest,
        "ssp_count": ssp_count,
        "log_level": cloud_log_level,
        "upgrade_infra": upgrade,
        "clean": clean,
        "ssp_job_config": ssp_job_config,
        // Free plan: the app's refMode must reach the SSP Worker so its ref mode
        // matches the client's — a mismatch wedges cross-session sync.
        "ref_mode": config.resolved_ref_mode().as_str(),
        // Same contract as `ref_mode`, and the same failure when it is missing.
        // The SSP decides which `_00_list_ref*` table an anonymous
        // registration writes to; the client decides which one it reads. This
        // key reached only `spky dev`, so a cloud tenant that set
        // `anonymousLiveQueries: true` had its client reading
        // `_00_list_ref_anon` while the SSP kept writing the global
        // `_00_list_ref` — signed-out sessions got no rows and no realtime, with
        // nothing logged on either side.
        "anonymous_live_queries": config.resolved_anonymous_live_queries(),
        // Partial deploy: control plane merges these apps over the previous
        // deployment instead of treating them as the complete desired set.
        "partial": is_partial,
    });

    println!("PAYLOAD: {}", deploy_body);
    let resp = client.post(&format!("/v1/projects/{}/deploy", pid), &deploy_body)?;

    let deployment: serde_json::Value = resp.into_json().context("Failed to parse response")?;
    let version = deployment["version"].as_u64().unwrap_or(0);

    println!(
        "  Deployment v{} created. Waiting for provisioning...",
        version
    );
    println!();

    // Phase 1: Stream events until infra is ready (migrating state)
    let phase1_raw = stream_deployment_events(
        &client,
        &pid,
        &["migrating", "running", "failed", "destroyed"],
    )?;
    let (phase1_status, phase1_error) = if let Some((s, e)) = phase1_raw.split_once('|') {
        (s.to_string(), Some(e.to_string()))
    } else {
        (phase1_raw, None)
    };

    match phase1_status.as_str() {
        "migrating" => {
            println!();
            println!("  ▸ Infrastructure ready. Running migrations...");

            // Get the SurrealDB URL from deployment status
            if let Ok(status_resp) = client.get(&format!("/v1/projects/{}/deployment", pid)) {
                if let Ok(status_data) = status_resp.into_json::<serde_json::Value>() {
                    if let Some(db_url) = status_data["urls"]["surrealdb"].as_str() {
                        // Wait for SurrealDB to be reachable via public URL. A
                        // freshly re-provisioned DB VM's public URL routing/TLS
                        // can lag well past a minute, so wait on a deadline
                        // (default 5 min, SPKY_DB_READY_TIMEOUT_SECS) rather than
                        // a fixed 30 attempts — otherwise real pending migrations
                        // get silently skipped on a slow-to-warm VM. Any HTTP
                        // response (even an auth error) means reachable; only a
                        // connect/timeout error keeps us waiting.
                        print!("  ▸ Connecting to SurrealDB");
                        let _ = std::io::Write::flush(&mut std::io::stdout());
                        let mut db_ready = false;
                        let deadline = std::time::Instant::now() + db_ready_timeout();
                        while std::time::Instant::now() < deadline {
                            // Bounded per-probe timeout: without it a half-open
                            // connection blocks this check forever.
                            let check = ureq::post(&format!("{}/sql", db_url))
                                .set("Accept", "application/json")
                                .timeout(Duration::from_secs(8))
                                .send_string("INFO FOR KV;");
                            if check.is_ok() {
                                db_ready = true;
                                break;
                            }
                            print!(".");
                            let _ = std::io::Write::flush(&mut std::io::stdout());
                            thread::sleep(Duration::from_secs(2));
                        }
                        println!();

                        if !db_ready {
                            println!(
                                "  ▸ WARNING: SurrealDB not reachable at {} after {}s — \
                                 pending migrations were NOT applied. Run `spky migrate prod` \
                                 once the database is up (raise the wait with \
                                 SPKY_DB_READY_TIMEOUT_SECS).",
                                db_url,
                                db_ready_timeout().as_secs()
                            );
                        } else {
                            let resolved = config.resolved_surrealdb();
                            // Use the auto-generated password from the deployment status
                            let db_password =
                                status_data["surrealdb_password"].as_str().unwrap_or("");
                            let schema = config.resolved_schema();
                            let config_dir =
                                config_path.parent().unwrap_or(std::path::Path::new("."));
                            let migrations_dir = config_dir.join(&schema.migrations);

                            // Apply user migrations via migration engine
                            if migrations_dir.exists() {
                                let ctx = crate::migration::MigrationContext {
                                    environment: crate::migration::MigrationEnvironment::Production,
                                    project_dir: config_dir.to_path_buf(),
                                    migrations_dir: migrations_dir.clone(),
                                    url: db_url.to_string(),
                                    namespace: resolved.namespace.clone(),
                                    database: resolved.database.clone(),
                                    username: "root".to_string(),
                                    password: db_password.to_string(),
                                    surrealkit_binary: config.resolved_surrealkit_binary(),
                                    internal_schema: None,
                                    remote_functions: None,
                                    secrets: Some(load_vault_secrets_for_prod()),
                                };
                                match crate::migration::create_engine(ctx).and_then(|e| e.apply()) {
                                    Ok(_) => println!("  ▸ Migrations complete."),
                                    Err(e) => println!("  ▸ Migration warning: {:?}", e),
                                }
                            } else {
                                println!("  ▸ No migrations directory found, skipping.");
                            }

                            schema_failures = apply_remote_fns_and_internal_schema(
                                db_url,
                                db_password,
                                &status_data,
                                &config,
                                config_path.as_path(),
                                &mut client,
                                &pid,
                            );
                        }
                    }
                }
            }

            // Phase 2: Finalize deployment (triggers SSP + scheduler + app VM provisioning)
            println!("  ▸ Deploying applications...");
            client.post(
                &format!("/v1/projects/{}/deploy/finalize", pid),
                &serde_json::json!({}),
            )?;

            // Stream events until fully running
            let phase2_raw =
                stream_deployment_events(&client, &pid, &["running", "failed", "destroyed"])?;
            let (phase2_status, phase2_error) = if let Some((s, e)) = phase2_raw.split_once('|') {
                (s.to_string(), Some(e.to_string()))
            } else {
                (phase2_raw, None)
            };
            match phase2_status.as_str() {
                "running" => {
                    println!();
                    println!("  Deployment is running!");

                    if let Ok(status_resp) = client.get(&format!("/v1/projects/{}/deployment", pid))
                    {
                        if let Ok(status) = status_resp.into_json::<serde_json::Value>() {
                            print_deployment_details(&status);

                            // Data-plane gate: the Phase-1 function apply ran
                            // before the scheduler existed, so confirm now that
                            // the scheduler is up that SurrealDB can actually
                            // reach it via the baked endpoint. Re-apply once if
                            // not (defense-in-depth against a stale endpoint),
                            // then surface a clear, actionable warning.
                            if let Some(db_url) = status["urls"]["surrealdb"].as_str() {
                                let resolved = config.resolved_surrealdb();
                                if verify_data_plane(
                                    db_url,
                                    &resolved.namespace,
                                    &resolved.database,
                                )
                                .is_ok()
                                {
                                    println!("  ▸ Data plane healthy (/spooky responds).");
                                } else {
                                    println!("  ▸ Data plane not responding; re-applying remote functions...");
                                    let db_password =
                                        status["surrealdb_password"].as_str().unwrap_or("");
                                    let reapply = apply_remote_fns_and_internal_schema(
                                        db_url,
                                        db_password,
                                        &status,
                                        &config,
                                        config_path.as_path(),
                                        &mut client,
                                        &pid,
                                    );
                                    if !reapply.is_empty() {
                                        schema_failures = reapply;
                                    }
                                    if let Err(e) = verify_data_plane(
                                        db_url,
                                        &resolved.namespace,
                                        &resolved.database,
                                    ) {
                                        println!("  ▸ WARNING: SurrealDB '/spooky' still not responding ({e}).");
                                        println!("    The app may hang on \"Loading database…\". Verify the scheduler is running:");
                                        println!(
                                            "      {}/api/{}/{}/spooky",
                                            db_url.trim_end_matches('/'),
                                            resolved.namespace,
                                            resolved.database
                                        );
                                    } else {
                                        println!("  ▸ Data plane healthy after re-apply.");
                                    }
                                }

                                // Announce the deployed frontend to running
                                // clients: UPSERT its `_00_app_release` row with
                                // the version from the app's package.json. SDK
                                // hooks (useAppRelease) compare against the
                                // running build and offer a reload — mandatory
                                // reloads immediately, cache_bust clears SW
                                // caches on that reload.
                                if frontend_selected {
                                    if let Some((frontend_name, frontend_app)) = config.frontend() {
                                        let db_password = status["surrealdb_password"]
                                            .as_str()
                                            .unwrap_or("");
                                        match frontend_package_version(config_dir, frontend_app) {
                                            Some(version) => write_app_release_row(
                                                db_url,
                                                db_password,
                                                &resolved.namespace,
                                                &resolved.database,
                                                frontend_name,
                                                &version,
                                                cache_bust,
                                                mandatory,
                                            ),
                                            None => println!(
                                                "  ▸ Warning: no versioned package.json found for frontend '{}'; release row not updated.",
                                                frontend_name
                                            ),
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                "failed" => {
                    let detail = phase2_error.as_deref().unwrap_or("unknown error");
                    bail!("Deployment failed: {}", detail);
                }
                _ => bail!("Deployment ended unexpectedly (status: {})", phase2_status),
            }
        }
        "running" => {
            // No migration phase (legacy or no infra VMs)
            println!();
            println!("  Deployment is running!");
            if let Ok(status_resp) = client.get(&format!("/v1/projects/{}/deployment", pid)) {
                if let Ok(status) = status_resp.into_json::<serde_json::Value>() {
                    print_deployment_details(&status);
                }
            }
        }
        "failed" => {
            let detail = phase1_error.as_deref().unwrap_or("unknown error");
            bail!("Deployment failed: {}", detail);
        }
        _ => bail!("Deployment ended unexpectedly (status: {})", phase1_status),
    }

    // Free (Cloudflare) projects have no CLI migrate phase — the schema is
    // applied out-of-band via the control-plane proxy. Push it automatically
    // now so a single `spky deploy` also lands the schema + reloads the node.
    if platform == "cloudflare" {
        println!();
        println!("Applying schema to the SSP node...");
        if let Err(e) = push_schema_inner(&mut client, &slug, &pid) {
            println!("  ▸ Warning: schema push failed: {} (run `spky push` to retry)", e);
            schema_failures.record("SSP node schema push", &e);
        }
    }

    // A schema that did not apply is a failed deploy, however healthy the
    // containers look. Reporting success here is what let a tenant run for
    // weeks on a stale internal schema, with the first symptom showing up in
    // the app instead of in this log.
    if !schema_failures.is_empty() {
        schema_failures.report("Deploy");
        bail!("deploy completed but the internal schema did not apply");
    }

    Ok(())
}

/// Name the case where the deployed components do not all run the same release.
///
/// The table already prints every version, but reading skew off three columns is
/// a thing nobody does until something is broken. That is how a tenant ran an
/// SSP many releases behind its client for weeks: the server-side fix for a
/// permission that admitted no rows was published, the client picked it up, and
/// the SSP that actually evaluates permissions never moved — so analytics stayed
/// empty and every version reading in the deploy looked plausible.
///
/// Compares only what the components report about themselves; a component that
/// did not answer is listed as unknown rather than assumed to match.
/// The roles whose release must move together for the sync path to agree.
const CORE_ROLES: [&str; 2] = ["ssp", "scheduler"];

/// Outcome of the skew check, separated from printing so it can be tested.
#[derive(Debug, PartialEq)]
pub(crate) struct VersionSkew {
    /// (role, reported version) for every core role that answered.
    pub reported: Vec<(String, String)>,
    /// Core roles that did not report a version at all.
    pub unknown: Vec<String>,
    /// True when the reported versions and the CLI's own are not all equal.
    pub skewed: bool,
}

pub(crate) fn detect_version_skew(
    component_versions: &std::collections::HashMap<String, (String, String)>,
    cli_version: &str,
) -> VersionSkew {
    let mut reported: Vec<(String, String)> = Vec::new();
    let mut unknown: Vec<String> = Vec::new();
    for role in CORE_ROLES {
        match component_versions.get(role).map(|(v, _)| v.as_str()) {
            Some(v) if v != "-" && !v.is_empty() => {
                reported.push((role.to_string(), v.to_string()))
            }
            _ => unknown.push(role.to_string()),
        }
    }

    // Only compare what actually answered: an unreported role is called out
    // separately rather than counted as agreeing.
    let skewed = reported.iter().any(|(_, v)| v != cli_version);

    VersionSkew { reported, unknown, skewed }
}

fn report_version_skew(
    component_versions: &std::collections::HashMap<String, (String, String)>,
) {
    let cli_version = env!("CARGO_PKG_VERSION");
    let skew = detect_version_skew(component_versions, cli_version);

    if skew.skewed {
        println!();
        println!("  \x1b[33m▲ Version skew:\x1b[0m components are not on the same release.");
        println!("      {:<26} {}", "cli (this deploy's target)", cli_version);
        for (role, v) in &skew.reported {
            let marker = if v == cli_version { "" } else { "  ← does not match" };
            println!("      {role:<26} {v}{marker}");
        }
        println!("    A server-side fix is only live once ssp + scheduler move too.");
        println!("    Run `spky deploy --upgrade` to bring them to {cli_version}.");
    }

    if !skew.unknown.is_empty() {
        println!();
        println!(
            "  \x1b[33m▲ Could not read the running version for: {}\x1b[0m",
            skew.unknown.join(", ")
        );
        println!("    Unknown is not the same as up to date — treat skew as possible.");
    }
}

#[cfg(test)]
mod version_skew_tests {
    use super::*;
    use std::collections::HashMap;

    fn versions(pairs: &[(&str, &str)]) -> HashMap<String, (String, String)> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), (v.to_string(), "running".to_string())))
            .collect()
    }

    #[test]
    fn no_skew_when_everything_matches() {
        let v = versions(&[("ssp", "1.2.3"), ("scheduler", "1.2.3")]);
        let skew = detect_version_skew(&v, "1.2.3");
        assert!(!skew.skewed);
        assert!(skew.unknown.is_empty());
    }

    #[test]
    fn flags_an_ssp_left_behind() {
        // The whitepawn case: client moved, the SSP that evaluates permissions
        // did not, so a server-side fix was never actually live.
        let v = versions(&[("ssp", "0.0.1-canary.168"), ("scheduler", "0.0.1-canary.168")]);
        let skew = detect_version_skew(&v, "0.0.1-canary.178");
        assert!(skew.skewed);
        assert_eq!(skew.reported.len(), 2);
    }

    #[test]
    fn flags_a_partial_upgrade() {
        let v = versions(&[("ssp", "1.2.3"), ("scheduler", "1.2.2")]);
        let skew = detect_version_skew(&v, "1.2.3");
        assert!(skew.skewed);
    }

    #[test]
    fn unknown_is_reported_but_is_not_skew_on_its_own() {
        let v = versions(&[("ssp", "1.2.3")]);
        let skew = detect_version_skew(&v, "1.2.3");
        assert!(!skew.skewed);
        assert_eq!(skew.unknown, vec!["scheduler".to_string()]);
    }

    #[test]
    fn a_placeholder_version_counts_as_unknown() {
        // The control plane reports "-" for a component it cannot read; that
        // must not be compared as if it were a real release.
        let v = versions(&[("ssp", "-"), ("scheduler", "")]);
        let skew = detect_version_skew(&v, "1.2.3");
        assert!(!skew.skewed);
        assert_eq!(skew.unknown.len(), 2);
    }
}

/// Get the Docker image ID (SHA digest) for a given tag.
fn get_docker_image_id(tag: &str) -> Option<String> {
    let output = std::process::Command::new("docker")
        .args(["inspect", "--format", "{{.Id}}", tag])
        .output()
        .ok()?;
    if output.status.success() {
        Some(String::from_utf8_lossy(&output.stdout).trim().to_string())
    } else {
        None
    }
}

/// Extract the CMD from a Docker image as a shell command string.
/// Return the image's Config.WorkingDir (empty string if unset) — needed
/// by the Docker runtime because `docker import` strips image metadata,
/// so we have to re-apply the WORKDIR at container-create time.
fn get_docker_workdir(tag: &str) -> Option<String> {
    let out = std::process::Command::new("docker")
        .args(["inspect", "--format", "{{.Config.WorkingDir}}", tag])
        .output()
        .ok()?;
    if !out.status.success() {
        return None;
    }
    let s = String::from_utf8_lossy(&out.stdout).trim().to_string();
    if s.is_empty() {
        None
    } else {
        Some(s)
    }
}

/// Return the image's Config.Env as individual KEY=VAL strings — needed
/// because `docker import` drops them and the user's backend often
/// depends on PATH, NODE_ENV, LANG, virtualenv paths, etc. inherited
/// from the base image.
fn get_docker_env(tag: &str) -> Vec<String> {
    let out = match std::process::Command::new("docker")
        .args(["inspect", "--format", "{{json .Config.Env}}", tag])
        .output()
    {
        Ok(o) if o.status.success() => o,
        _ => return Vec::new(),
    };
    let raw = String::from_utf8_lossy(&out.stdout).trim().to_string();
    serde_json::from_str::<Vec<String>>(&raw).unwrap_or_default()
}

fn get_docker_cmd(tag: &str) -> Option<String> {
    // Combine Entrypoint and Cmd the way Docker itself would when running
    // the container. Many base images (nginx, python, node) rely on
    // ENTRYPOINT + CMD — capturing only Cmd drops the actual startup
    // binary and the deployed container fails with "no command specified".
    let inspect = |field: &str| -> Option<Vec<String>> {
        let out = std::process::Command::new("docker")
            .args([
                "inspect",
                "--format",
                &format!("{{{{json .Config.{}}}}}", field),
                tag,
            ])
            .output()
            .ok()?;
        if !out.status.success() {
            return None;
        }
        let raw = String::from_utf8_lossy(&out.stdout).trim().to_string();
        serde_json::from_str::<Vec<String>>(&raw).ok()
    };

    let entrypoint = inspect("Entrypoint").unwrap_or_default();
    let cmd = inspect("Cmd").unwrap_or_default();
    let mut parts: Vec<String> = Vec::new();
    parts.extend(entrypoint);
    parts.extend(cmd);
    if parts.is_empty() {
        return None;
    }
    // Shell-quote arguments that contain spaces or special chars
    // e.g. ["nginx", "-g", "daemon off;"] → "nginx -g 'daemon off;'"
    let quoted: Vec<String> = parts
        .iter()
        .map(|p| {
            if p.contains(' ') || p.contains(';') || p.contains('\'') || p.contains('"') {
                format!("'{}'", p.replace('\'', "'\\''"))
            } else {
                p.clone()
            }
        })
        .collect();
    Some(quoted.join(" "))
}

/// Decide whether a failed image upload actually landed.
///
/// The upload goes through the edge proxy, which gives up on a request that
/// has produced no response bytes for 100 s. A large image on a slow uplink
/// takes longer than that to *send*, so the proxy answers 502 (or drops the
/// socket: "Broken pipe") while the origin is still reading the body, stores
/// it, and logs a 200 nobody saw. The control plane writes the image's `hash`
/// file only after the body was read to completion, so a remote hash equal to
/// the one we just sent is proof the upload succeeded, and the deploy can go
/// on instead of failing after a full rebuild and a five-minute upload.
fn upload_landed_despite_error(
    client: &CloudClient,
    pid: &str,
    image_name: &str,
    local_hash: Option<&str>,
    error: &str,
) -> bool {
    let Some(local) = local_hash.filter(|h| !h.is_empty()) else {
        return false;
    };
    match get_remote_image_hash(client, pid, image_name) {
        Some(remote) if remote == local => {
            println!(
                "  ▸ Upload of '{}' reported an error ({}), but the server holds this exact image; continuing.",
                image_name, error
            );
            true
        }
        _ => false,
    }
}

/// Get the remote image hash from the API.
fn get_remote_image_hash(client: &CloudClient, pid: &str, image_name: &str) -> Option<String> {
    let url = format!(
        "{}/v1/projects/{}/images/{}/hash",
        client.base_url, pid, image_name
    );
    let resp = ureq::get(&url)
        .set("Authorization", &format!("Bearer {}", client.token))
        .call()
        .ok()?;
    let data: serde_json::Value = resp.into_json().ok()?;
    let hash = data["hash"].as_str()?.to_string();
    if hash.is_empty() {
        None
    } else {
        Some(hash)
    }
}

/// A Read wrapper that prints upload progress percentage.
struct ProgressReader<'a> {
    data: &'a [u8],
    pos: usize,
    total: usize,
    label: String,
    last_pct: u8,
}

impl<'a> ProgressReader<'a> {
    fn new(data: &'a [u8], total: usize, label: &str) -> Self {
        Self {
            data,
            pos: 0,
            total,
            label: label.to_string(),
            last_pct: 0,
        }
    }
}

impl<'a> std::io::Read for ProgressReader<'a> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let remaining = &self.data[self.pos..];
        let n = std::cmp::min(buf.len(), remaining.len());
        buf[..n].copy_from_slice(&remaining[..n]);
        self.pos += n;
        let pct = if self.total > 0 {
            (self.pos * 100 / self.total) as u8
        } else {
            100
        };
        if pct != self.last_pct {
            self.last_pct = pct;
            print!("\r  {} {}%", self.label, pct);
            use std::io::Write;
            std::io::stdout().flush().ok();
        }
        Ok(n)
    }
}

// ---------------------------------------------------------------------------
// Deployment status display — animated inline table
// ---------------------------------------------------------------------------

const SPINNER_FRAMES: &[&str] = &["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"];

/// Shared state between the SSE reader and the render thread.
struct DeployState {
    phase: String,
    vms: Vec<(String, String, String)>, // (role, ip, status)
    done: bool,
}

fn vm_icon(status: &str, spinner_frame: usize) -> String {
    match status {
        "running" => " ●".to_string(),
        "starting" => format!(" {}", SPINNER_FRAMES[spinner_frame % SPINNER_FRAMES.len()]),
        "failed" => " ✗".to_string(),
        "stopped" => " ○".to_string(),
        _ => " ·".to_string(), // pending
    }
}

fn vm_style(status: &str) -> &str {
    match status {
        "running" => "\x1b[32m",  // green
        "starting" => "\x1b[33m", // yellow
        "failed" => "\x1b[31m",   // red
        "stopped" => "\x1b[90m",  // dim
        _ => "\x1b[90m",          // dim / pending
    }
}

/// Render the full VM table in-place by moving the cursor up and overwriting.
fn render_vm_table(
    vms: &[(String, String, String)],
    phase: &str,
    spinner_frame: usize,
    first_render: bool,
) {
    use std::io::Write;
    let mut out = std::io::stdout();

    // Layout: blank + phase + blank + header + vms + blank
    let total_lines = 4 + vms.len();

    if !first_render {
        write!(out, "\x1b[{}A", total_lines).ok();
    }

    // Phase header with padding
    let phase_label = match phase {
        "provisioning" => "Provisioning infrastructure",
        "deploying_apps" => "Deploying applications",
        "migrating" => "Running migrations",
        "running" => "Deployment complete",
        "failed" => "Deployment failed",
        _ => phase,
    };
    write!(out, "\r\x1b[2K\n").ok(); // top padding

    // Phase line with its own spinner for in-progress phases
    match phase {
        "running" => {
            write!(
                out,
                "\r\x1b[2K   \x1b[32m●\x1b[0m \x1b[1m{}\x1b[0m\n",
                phase_label
            )
            .ok();
        }
        "failed" => {
            write!(
                out,
                "\r\x1b[2K   \x1b[31m✗\x1b[0m \x1b[1m{}\x1b[0m\n",
                phase_label
            )
            .ok();
        }
        _ => {
            let f = SPINNER_FRAMES[spinner_frame % SPINNER_FRAMES.len()];
            write!(
                out,
                "\r\x1b[2K   \x1b[36m{}\x1b[0m \x1b[1m{}\x1b[0m\n",
                f, phase_label
            )
            .ok();
        }
    }

    write!(out, "\r\x1b[2K\n").ok(); // padding after phase

    // Header
    write!(
        out,
        "\r\x1b[2K   \x1b[90m  {:<14} {:<18} {}\x1b[0m\n",
        "SERVICE", "IP", "STATUS"
    )
    .ok();

    // VM rows
    for (role, ip, status) in vms {
        let icon = vm_icon(status, spinner_frame);
        let style = vm_style(status);
        write!(
            out,
            "\r\x1b[2K   {}{}\x1b[0m {}{:<14}\x1b[0m \x1b[90m{:<18}\x1b[0m {}{}\x1b[0m\n",
            style, icon, style, role, ip, style, status,
        )
        .ok();
    }

    write!(out, "\r\x1b[2K").ok(); // bottom padding line (no newline, cursor stays)

    out.flush().ok();
}

/// Stream SSE deployment events and return the final status when the stream closes.
/// Uses an animated inline table with spinner for in-progress services.
/// Per-read timeout for the deployment SSE stream. A silently dead connection
/// (dropped by a NAT/LB without a FIN) otherwise blocks the reader FOREVER —
/// the classic "deploy stuck on migrations" hang: the deployment finishes
/// server-side while the CLI sits in a blocking read. On timeout we fall back
/// to polling the deployment status endpoint instead of failing the deploy.
fn events_read_timeout() -> Duration {
    let secs = std::env::var("SPKY_DEPLOY_EVENTS_READ_TIMEOUT_SECS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(120);
    Duration::from_secs(secs)
}

/// How long to wait for a freshly-provisioned SurrealDB VM's public URL to
/// become reachable before giving up and skipping migrations (default 5 min).
fn db_ready_timeout() -> Duration {
    let secs = std::env::var("SPKY_DB_READY_TIMEOUT_SECS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(300);
    Duration::from_secs(secs)
}

/// Overall deadline for the status-poll fallback (default 30 min).
fn deploy_poll_deadline() -> Duration {
    let secs = std::env::var("SPKY_DEPLOY_POLL_TIMEOUT_SECS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(1800);
    Duration::from_secs(secs)
}

/// Poll the deployment status endpoint until it reaches one of
/// `terminal_statuses`. Fallback path for when the SSE stream dies or goes
/// silent; bounded by `deploy_poll_deadline()`.
fn poll_deployment_status(
    client: &CloudClient,
    pid: &str,
    terminal_statuses: &[&str],
) -> Result<String> {
    let url = format!("{}/v1/projects/{}/deployment", client.base_url, pid);
    let deadline = std::time::Instant::now() + deploy_poll_deadline();
    loop {
        if let Ok(resp) = ureq::get(&url)
            .set("Authorization", &format!("Bearer {}", client.token))
            .set("Accept", "application/json")
            .timeout(Duration::from_secs(30))
            .call()
        {
            if let Ok(data) = resp.into_json::<serde_json::Value>() {
                let dep = data.get("deployment").unwrap_or(&data);
                let status = dep["status"].as_str().unwrap_or("");
                if terminal_statuses.contains(&status) {
                    let error = dep["error"].as_str().unwrap_or("");
                    return Ok(if error.is_empty() {
                        status.to_string()
                    } else {
                        format!("{}|{}", status, error)
                    });
                }
            }
        }
        if std::time::Instant::now() >= deadline {
            bail!(
                "Timed out waiting for the deployment to reach one of {:?} \
                 (raise with SPKY_DEPLOY_POLL_TIMEOUT_SECS)",
                terminal_statuses
            );
        }
        thread::sleep(Duration::from_secs(5));
    }
}

fn stream_deployment_events(
    client: &CloudClient,
    pid: &str,
    terminal_statuses: &[&str],
) -> Result<String> {
    use std::sync::{Arc, Mutex};

    let events_url = format!("{}/v1/projects/{}/deployment/events", client.base_url, pid);
    // Bounded reads: without timeout_read a half-dead SSE connection blocks
    // `reader.lines()` indefinitely (no error, no EOF). See events_read_timeout.
    let agent = ureq::builder()
        .timeout_connect(Duration::from_secs(15))
        .timeout_read(events_read_timeout())
        .build();
    let sse_result = agent
        .get(&events_url)
        .set("Authorization", &format!("Bearer {}", client.token))
        .set("Accept", "text/event-stream")
        .call();

    match sse_result {
        Ok(sse_resp) => {
            let reader = std::io::BufReader::new(sse_resp.into_reader());
            use std::io::BufRead;

            let is_tty = std::io::stdout().is_terminal();

            let state = Arc::new(Mutex::new(DeployState {
                phase: String::from("provisioning"),
                vms: Vec::new(),
                done: false,
            }));

            // Spawn a render thread that redraws every 80ms for spinner animation
            let render_state = Arc::clone(&state);
            let render_handle = if is_tty {
                Some(thread::spawn(move || {
                    let mut frame: usize = 0;
                    let mut rendered = false;

                    loop {
                        thread::sleep(Duration::from_millis(80));

                        let s = render_state.lock().unwrap();
                        if s.done {
                            // Final render
                            render_vm_table(&s.vms, &s.phase, frame, !rendered);
                            break;
                        }
                        if !s.vms.is_empty() {
                            render_vm_table(&s.vms, &s.phase, frame, !rendered);
                            rendered = true;
                        }
                        drop(s);

                        frame += 1;
                    }
                }))
            } else {
                None
            };

            let mut final_status = String::new();
            let mut deploy_error = String::new();
            let mut stream_died = false;

            for line in reader.lines() {
                let line = match line {
                    Ok(l) => l,
                    Err(_) => {
                        // Read error or per-read timeout — the stream died
                        // without the server closing it cleanly.
                        stream_died = true;
                        break;
                    }
                };
                let data = match line.strip_prefix("data: ") {
                    Some(d) => d,
                    None => continue,
                };
                let event: serde_json::Value = match serde_json::from_str(data) {
                    Ok(v) => v,
                    Err(_) => continue,
                };

                let event_type = event["type"].as_str().unwrap_or("");
                match event_type {
                    "vm" => {
                        let role = event["role"].as_str().unwrap_or("?").to_string();
                        let ip = event["ip"].as_str().unwrap_or("?").to_string();
                        let status = event["status"].as_str().unwrap_or("?").to_string();

                        if is_tty {
                            let mut s = state.lock().unwrap();
                            if let Some(vm) = s.vms.iter_mut().find(|v| v.0 == role && v.1 == ip) {
                                vm.2 = status;
                            } else {
                                s.vms.push((role, ip, status));
                            }
                        } else {
                            let icon = match status.as_str() {
                                "running" => "●",
                                "starting" => "◐",
                                "failed" => "✗",
                                "stopped" => "○",
                                _ => "·",
                            };
                            println!("  {} {:14} {:18} {}", icon, role, ip, status);
                        }
                    }
                    "deployment" => {
                        let status = event["status"].as_str().unwrap_or("unknown");
                        final_status = status.to_string();
                        if let Some(err) = event["error"].as_str() {
                            if !err.is_empty() {
                                deploy_error = err.to_string();
                            }
                        }

                        if is_tty {
                            let mut s = state.lock().unwrap();
                            s.phase = status.to_string();
                        } else {
                            match status {
                                "provisioning" => println!("  ▸ Provisioning VMs..."),
                                "deploying_apps" => println!("  ▸ Deploying applications..."),
                                "migrating" | "running" | "failed" | "destroyed" => {}
                                _ => println!("  ▸ {}", status),
                            }
                        }

                        // Return as soon as the phase we're waiting on is
                        // reached. Waiting for the server to close the stream
                        // instead (the old behavior) hangs the CLI forever
                        // when the control plane keeps the connection open
                        // after the terminal event.
                        if terminal_statuses.contains(&status) {
                            break;
                        }
                    }
                    _ => {}
                }
            }

            // Signal render thread to stop and do final render
            if is_tty {
                {
                    let mut s = state.lock().unwrap();
                    s.done = true;
                }
                if let Some(handle) = render_handle {
                    handle.join().ok();
                }
                println!(); // final newline after table
            }

            // Stream ended without a terminal status (died mid-flight, or the
            // server closed early): the deployment is still progressing
            // server-side, so poll the status endpoint instead of hanging or
            // failing.
            if stream_died || !terminal_statuses.contains(&final_status.as_str()) {
                println!("  ▸ Event stream interrupted; polling deployment status...");
                return poll_deployment_status(client, pid, terminal_statuses);
            }

            if deploy_error.is_empty() {
                Ok(final_status)
            } else {
                Ok(format!("{}|{}", final_status, deploy_error))
            }
        }
        Err(ureq::Error::Status(code, resp)) => {
            let body = resp.into_string().unwrap_or_default();
            bail!("Failed to stream events (HTTP {}): {}", code, body);
        }
        Err(ureq::Error::Transport(_)) => {
            // Couldn't even open the stream — fall straight back to polling.
            println!("  ▸ Event stream unavailable; polling deployment status...");
            poll_deployment_status(client, pid, terminal_statuses)
        }
    }
}

pub fn status() -> Result<()> {
    let creds = require_credentials()?;
    let mut client = CloudClient::new(&creds);
    let (_, pid) = resolve_project_id(&mut client)?;

    let resp = client.get(&format!("/v1/projects/{}/deployment", pid))?;
    let data: serde_json::Value = resp.into_json().context("Failed to parse response")?;

    print_deployment_details(&data);
    Ok(())
}

/// SurrealDB connection details for a cloud-hosted project.
pub struct CloudSurreal {
    /// HTTP(S) endpoint of the deployment's SurrealDB.
    pub url: String,
    /// Root password issued for the deployment.
    pub password: String,
}

/// Resolve the SurrealDB HTTP endpoint and root password for the project
/// configured in `sp00ky.yml` (or `SP00KY_CLOUD_PROJECT`) from Sp00ky Cloud,
/// the same lookup `spky project credentials` performs. This is what backs the
/// `--cloud` flag on the DB-connecting commands (`jobs`, `flag`, `migrate`):
/// it requires a configured project slug, valid credentials, and a deployed
/// project, erroring clearly otherwise rather than silently using local defaults.
pub fn resolve_cloud_surreal(config_path: &std::path::Path) -> Result<CloudSurreal> {
    let slug = std::env::var("SP00KY_CLOUD_PROJECT")
        .ok()
        .or_else(|| backend::load_config(config_path).slug)
        .ok_or_else(|| {
            anyhow::anyhow!(
                "No cloud project configured for --cloud. Add a `slug:` to \
                 sp00ky.yml or set SP00KY_CLOUD_PROJECT."
            )
        })?;

    let creds = load_credentials().ok_or_else(|| {
        anyhow::anyhow!("Not logged in to Sp00ky Cloud. Run `spky login` first to use --cloud.")
    })?;
    let mut client = CloudClient::new(&creds);

    let project = fetch_project(&mut client, &slug)?
        .ok_or_else(|| anyhow::anyhow!("Cloud project '{}' not found.", slug))?;
    let pid = project_id(&project);

    let resp = client.get(&format!("/v1/projects/{}/deployment", pid))?;
    let data: serde_json::Value = resp
        .into_json()
        .context("Failed to parse deployment response")?;

    let url = data["urls"]["surrealdb"]
        .as_str()
        .filter(|s| !s.is_empty())
        .ok_or_else(|| {
            anyhow::anyhow!(
                "No SurrealDB URL for project '{}'. Has it been deployed?",
                slug
            )
        })?
        .to_string();
    let password = data["surrealdb_password"]
        .as_str()
        .or_else(|| data["db_password"].as_str())
        .filter(|s| !s.is_empty())
        .ok_or_else(|| {
            anyhow::anyhow!(
                "No SurrealDB password for project '{}'. Has it been deployed?",
                slug
            )
        })?
        .to_string();

    Ok(CloudSurreal { url, password })
}

/// A fully-resolved external SurrealDB connection (endpoint + credentials).
pub struct ExternalSurreal {
    pub url: String,
    pub username: String,
    pub password: String,
}

/// Resolve an external ("bring-your-own") SurrealDB connection from the manifest,
/// decrypting any `{ vault: KEY }` credential references via the tenant vault.
///
/// For external hosting Sp00ky doesn't run the DB, so there's no deployment URL
/// to look up (that's only for managed/cloud DBs) — the endpoint and credentials
/// come straight from `sp00ky.yml`. The vault is only contacted when a credential
/// actually references it, so literal creds resolve offline.
pub fn resolve_external_surreal(config_path: &std::path::Path) -> Result<ExternalSurreal> {
    let config = backend::load_config(config_path);
    let resolved = config.resolved_surrealdb();

    let needs_vault = resolved.username.vault_key().is_some()
        || resolved.password.vault_key().is_some()
        || resolved
            .endpoint
            .as_ref()
            .and_then(|e| e.vault_key())
            .is_some();

    let vault = if needs_vault {
        let slug = std::env::var("SP00KY_CLOUD_PROJECT")
            .ok()
            .or(config.slug.clone())
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "SurrealDB credentials reference the vault, but no project is configured. \
                     Add a `slug:` to sp00ky.yml or set SP00KY_CLOUD_PROJECT."
                )
            })?;
        let creds = load_credentials().ok_or_else(|| {
            anyhow::anyhow!(
                "SurrealDB credentials reference the vault — run `spky login` first to resolve them."
            )
        })?;
        let mut client = CloudClient::new(&creds);
        let project = fetch_project(&mut client, &slug)?
            .ok_or_else(|| anyhow::anyhow!("Cloud project '{}' not found.", slug))?;
        let pid = project_id(&project);
        load_vault_map_for_deploy(&mut client, &pid, true)
    } else {
        std::collections::HashMap::new()
    };

    let url = match &resolved.endpoint {
        Some(e) => resolve_secret_value(e, &vault)?,
        None => bail!("SurrealDB hosting is 'external' but no endpoint URL was provided"),
    };
    let username = resolve_secret_value(&resolved.username, &vault)?;
    let password = resolve_secret_value(&resolved.password, &vault)?;
    Ok(ExternalSurreal {
        url,
        username,
        password,
    })
}

pub fn credentials(raw: bool) -> Result<()> {
    let creds = require_credentials()?;
    let mut client = CloudClient::new(&creds);
    let (_, pid) = resolve_project_id(&mut client)?;

    let resp = client.get(&format!("/v1/projects/{}/deployment", pid))?;
    let data: serde_json::Value = resp.into_json().context("Failed to parse response")?;

    let password = data["surrealdb_password"]
        .as_str()
        .or_else(|| data["db_password"].as_str())
        .filter(|s| !s.is_empty())
        .ok_or_else(|| {
            anyhow::anyhow!(
                "No SurrealDB password available for this project. \
                 Has the project been deployed?"
            )
        })?;

    if raw {
        println!("{}", password);
        return Ok(());
    }

    let url = data["urls"]["surrealdb"].as_str().unwrap_or("(unknown)");
    println!("  SurrealDB URL:      {}", url);
    println!("  Username:           root");
    println!("  Password:           {}", password);
    Ok(())
}

// ---------------------------------------------------------------------------
// Log streaming
// ---------------------------------------------------------------------------

/// Known service names and their display colors.
pub fn service_color(service: &str) -> crossterm::style::Color {
    use crossterm::style::Color;
    match service {
        "surrealdb" => Color::Cyan,
        "scheduler" => Color::Yellow,
        "ssp" => Color::Green,
        "backend" => Color::Magenta,
        "frontend" => Color::Blue,
        _ => Color::White,
    }
}

/// Expand blueprint names and comma-separated service lists into a set of service names.
fn resolve_filters(filter: Option<&str>, service: Option<&str>) -> Option<Vec<String>> {
    // --filter takes precedence over deprecated --service
    let raw = filter.or(service)?;
    let mut services = Vec::new();
    for token in raw.split(',') {
        let token = token.trim().to_lowercase();
        match token.as_str() {
            "spooky" => {
                services.push("ssp".to_string());
                services.push("scheduler".to_string());
            }
            other if !other.is_empty() => {
                services.push(other.to_string());
            }
            _ => {}
        }
    }
    if services.is_empty() {
        None
    } else {
        Some(services)
    }
}

/// Check if a service name matches any of the filter patterns.
/// Supports exact match and simple glob patterns with `*` and `?`.
fn matches_filter(service: &str, filters: &[String]) -> bool {
    fn glob_match(pattern: &str, text: &str) -> bool {
        let mut star_p = None;
        let mut star_t = None;
        let pattern: Vec<char> = pattern.chars().collect();
        let text: Vec<char> = text.chars().collect();
        let (mut pi, mut ti) = (0, 0);
        while ti < text.len() {
            if pi < pattern.len() && (pattern[pi] == '?' || pattern[pi] == text[ti]) {
                pi += 1;
                ti += 1;
            } else if pi < pattern.len() && pattern[pi] == '*' {
                star_p = Some(pi);
                star_t = Some(ti);
                pi += 1;
            } else if let Some(sp) = star_p {
                pi = sp + 1;
                let st = star_t.unwrap() + 1;
                star_t = Some(st);
                ti = st;
            } else {
                return false;
            }
        }
        while pi < pattern.len() && pattern[pi] == '*' {
            pi += 1;
        }
        pi == pattern.len()
    }

    filters.iter().any(|f| {
        if f.contains('*') || f.contains('?') {
            glob_match(f, service)
        } else {
            f == service
        }
    })
}

/// Strip ANSI escape sequences from a string so ratatui's cell-width
/// tracking stays in sync with what the terminal actually renders.
fn strip_ansi(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut chars = s.chars();
    while let Some(c) = chars.next() {
        if c == '\x1b' {
            // Skip ESC [ ... (final byte in 0x40–0x7E)
            if let Some(next) = chars.next() {
                if next == '[' {
                    for c2 in chars.by_ref() {
                        if c2.is_ascii() && (0x40..=0x7E).contains(&(c2 as u8)) {
                            break;
                        }
                    }
                }
                // else: lone ESC + char, just skip both
            }
        } else {
            out.push(c);
        }
    }
    out
}

/// A single log line as streamed by the backend's SSE endpoint.
///
/// The wire format is JSON: `{"service": "...", "timestamp": "...", "message": "..."}`.
/// Older deployments emitted plain `[service] message` lines; `parse_log_line`
/// keeps a fallback path so those keep working while we roll out the richer format.
///
/// `timestamp` uses a flexible deserializer that accepts:
///   - RFC-3339 strings (`"2026-04-22T10:23:45.123Z"`)
///   - Unix seconds (i64)
///   - Unix milliseconds (i64, detected when the value exceeds 10¹⁰)
///   - Float seconds (f64)
/// so we don't silently drop the timestamp when a deployment uses a
/// non-standard encoding.
#[derive(Debug, Clone, serde::Deserialize)]
pub struct LogEntry {
    pub service: String,
    #[serde(default, deserialize_with = "deserialize_flexible_timestamp")]
    pub timestamp: Option<chrono::DateTime<chrono::Utc>>,
    pub message: String,
    // Captured off the wire for the upcoming TUI (colored severity column).
    // Not yet read by the stdout printer, so silence the dead-code lint.
    #[allow(dead_code)]
    #[serde(default)]
    pub level: Option<String>,
}

impl LogEntry {
    pub fn ts(&self) -> Option<chrono::DateTime<chrono::Utc>> {
        self.timestamp
    }
}

fn deserialize_flexible_timestamp<'de, D>(
    d: D,
) -> std::result::Result<Option<chrono::DateTime<chrono::Utc>>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::Deserialize;
    let v = Option::<serde_json::Value>::deserialize(d)?;
    Ok(match v {
        Some(serde_json::Value::String(s)) => chrono::DateTime::parse_from_rfc3339(&s)
            .ok()
            .map(|t| t.with_timezone(&chrono::Utc)),
        Some(serde_json::Value::Number(n)) => {
            if let Some(i) = n.as_i64() {
                if i.abs() > 10_000_000_000 {
                    chrono::DateTime::from_timestamp_millis(i)
                } else {
                    chrono::DateTime::from_timestamp(i, 0)
                }
            } else if let Some(f) = n.as_f64() {
                let secs = f.trunc() as i64;
                let nanos = (f.fract() * 1_000_000_000.0) as u32;
                chrono::DateTime::from_timestamp(secs, nanos)
            } else {
                None
            }
        }
        _ => None,
    })
}

/// Parse a SSE `data:` payload. Prefers JSON (the current backend format);
/// falls back to the legacy `[service] message` plain-text form.
pub fn parse_log_line(data: &str) -> Option<LogEntry> {
    let data = data.trim();
    if data.is_empty() {
        return None;
    }
    // Drop SSE control sentinels (`done`, `[done]`, `close`, …) up front so
    // they don't slip through as fake log entries — these are stream markers
    // some server implementations emit and they'd otherwise render as noisy
    // lines in the TUI / stdout.
    if is_stream_sentinel(data) {
        return None;
    }
    if data.starts_with('{') {
        if let Ok(mut entry) = serde_json::from_str::<LogEntry>(data) {
            entry.message = strip_ansi(&entry.message);
            return Some(entry);
        }
    }
    if data.starts_with('[') {
        if let Some(end) = data.find(']') {
            let service = data[1..end].to_string();
            let message = strip_ansi(data[end + 1..].trim_start());
            return Some(LogEntry {
                service,
                timestamp: None,
                message,
                level: None,
            });
        }
    }
    Some(LogEntry {
        service: "unknown".to_string(),
        timestamp: None,
        message: strip_ansi(data),
        level: None,
    })
}

/// SSE control words that the server sometimes emits alongside real log
/// entries. We drop them so they don't render as fake log lines.
fn is_stream_sentinel(s: &str) -> bool {
    matches!(
        s.to_ascii_lowercase().as_str(),
        "done" | "close" | "closed" | "eof" | "end" | "[done]" | "[end]" | "--end--"
    )
}

enum LogEvent {
    Entry(LogEntry),
    Eof,
    Error(String),
}

/// Spawn a background thread that reads the SSE stream and sends parsed log events.
fn spawn_sse_reader(
    url: String,
    auth_header: String,
    tx: std::sync::mpsc::Sender<LogEvent>,
) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        let resp = ureq::get(&url)
            .set("Authorization", &auth_header)
            .set("Accept", "text/event-stream")
            .call();

        match resp {
            Ok(resp) => {
                // Use a small buffer so lines are yielded promptly over the SSE
                // stream, rather than waiting for the default 8KB to fill.
                let reader = std::io::BufReader::with_capacity(512, resp.into_reader());
                use std::io::BufRead;
                for line in reader.lines() {
                    match line {
                        Ok(line) => {
                            if let Some(data) = line
                                .strip_prefix("data: ")
                                .or_else(|| line.strip_prefix("data:"))
                            {
                                if let Some(entry) = parse_log_line(data) {
                                    if tx.send(LogEvent::Entry(entry)).is_err() {
                                        return;
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            let _ = tx.send(LogEvent::Error(format!("Read error: {}", e)));
                            return;
                        }
                    }
                }
                let _ = tx.send(LogEvent::Eof);
            }
            Err(ureq::Error::Status(code, resp)) => {
                let body = resp.into_string().unwrap_or_default();
                let _ = tx.send(LogEvent::Error(format!("HTTP {}: {}", code, body)));
            }
            Err(ureq::Error::Transport(t)) => {
                let _ = tx.send(LogEvent::Error(format!("Connection error: {}", t)));
            }
        }
    })
}

/// Non-split mode: colored streaming output to stdout.
fn logs_stream(
    rx: std::sync::mpsc::Receiver<LogEvent>,
    filters: &Option<Vec<String>>,
    show_timestamps: bool,
) -> Result<()> {
    use crossterm::style::{Color, ResetColor, SetForegroundColor};
    use std::io::Write;

    let stdout = std::io::stdout();
    let mut out = stdout.lock();

    loop {
        match rx.recv() {
            Ok(LogEvent::Entry(entry)) => {
                if let Some(ref f) = filters {
                    if !matches_filter(&entry.service, f) {
                        continue;
                    }
                }
                if show_timestamps {
                    if let Some(ts) = entry.ts() {
                        let _ = crossterm::execute!(out, SetForegroundColor(Color::DarkGrey));
                        let _ = write!(out, "{} ", ts.format("%Y-%m-%dT%H:%M:%S%.3fZ"));
                        let _ = crossterm::execute!(out, ResetColor);
                    }
                }
                let color = service_color(&entry.service);
                let _ = crossterm::execute!(out, SetForegroundColor(color));
                let _ = write!(out, "[{}]", entry.service);
                let _ = crossterm::execute!(out, SetForegroundColor(Color::Reset));
                let _ = crossterm::execute!(out, ResetColor);
                let _ = writeln!(out, " {}", entry.message);
                let _ = out.flush();
            }
            Ok(LogEvent::Eof) => break,
            Ok(LogEvent::Error(e)) => bail!("{}", e),
            Err(_) => break,
        }
    }
    Ok(())
}

/// Split mode: ratatui TUI with one panel per service.
fn logs_split(
    rx: std::sync::mpsc::Receiver<LogEvent>,
    filters: &Option<Vec<String>>,
    direction: ratatui::layout::Direction,
) -> Result<()> {
    use crossterm::event::{self, Event, KeyCode, KeyModifiers};
    use crossterm::terminal::{EnterAlternateScreen, LeaveAlternateScreen};
    use ratatui::layout::{Constraint, Layout};
    use ratatui::style::{Color, Style};
    use ratatui::text::{Line, Span};
    use ratatui::widgets::{Block, Borders, Paragraph};
    use std::collections::{HashMap, VecDeque};

    const MAX_LINES: usize = 1000;
    const MIN_PANEL_HEIGHT: u16 = 4; // border top + 2 content lines + border bottom

    // If an explicit filter is set, pre-create panels for those services.
    // Otherwise we dynamically add panels as data arrives.
    let explicit_filter = filters.is_some();
    let has_glob = filters
        .as_ref()
        .is_some_and(|f| f.iter().any(|p| p.contains('*') || p.contains('?')));
    let mut panel_order: Vec<String> = if !has_glob {
        if let Some(ref f) = filters {
            let mut v: Vec<String> = f.iter().cloned().collect();
            v.sort();
            v
        } else {
            Vec::new()
        }
    } else {
        Vec::new()
    };

    let mut panels: HashMap<String, VecDeque<String>> = HashMap::new();
    for svc in &panel_order {
        panels.insert(svc.clone(), VecDeque::new());
    }

    // Install panic hook to restore terminal
    let original_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        let _ = crossterm::terminal::disable_raw_mode();
        let _ = crossterm::execute!(std::io::stderr(), LeaveAlternateScreen);
        original_hook(info);
    }));

    // Flush any pending stdout before taking over the terminal
    use std::io::Write;
    std::io::stdout().flush()?;

    crossterm::terminal::enable_raw_mode()?;
    let mut stdout = std::io::stdout();
    crossterm::execute!(
        stdout,
        EnterAlternateScreen,
        crossterm::terminal::Clear(crossterm::terminal::ClearType::All),
        crossterm::cursor::Hide,
        crossterm::cursor::MoveTo(0, 0)
    )?;
    let backend = ratatui::backend::CrosstermBackend::new(stdout);
    let mut terminal = ratatui::Terminal::new(backend)?;
    terminal.clear()?;

    let mut stream_ended = false;

    loop {
        // Drain all pending log events (non-blocking)
        if !stream_ended {
            loop {
                match rx.try_recv() {
                    Ok(LogEvent::Entry(entry)) => {
                        let LogEntry {
                            service, message, ..
                        } = entry;
                        // Dynamically add panels for new services (when no explicit filter)
                        if !panels.contains_key(&service) {
                            if explicit_filter {
                                if let Some(ref f) = filters {
                                    if !matches_filter(&service, f) {
                                        continue; // skip services not matching the filter
                                    }
                                }
                                // Glob-matched service: dynamically add panel
                            }
                            panel_order.push(service.clone());
                            panels.insert(service.clone(), VecDeque::new());
                        }
                        if let Some(buf) = panels.get_mut(&service) {
                            buf.push_back(message);
                            if buf.len() > MAX_LINES {
                                buf.pop_front();
                            }
                        }
                    }
                    Ok(LogEvent::Eof) => {
                        for buf in panels.values_mut() {
                            buf.push_back("--- Stream ended ---".to_string());
                        }
                        stream_ended = true;
                        break;
                    }
                    Ok(LogEvent::Error(e)) => {
                        for buf in panels.values_mut() {
                            buf.push_back(format!("--- Error: {} ---", e));
                        }
                        stream_ended = true;
                        break;
                    }
                    Err(std::sync::mpsc::TryRecvError::Empty) => break,
                    Err(std::sync::mpsc::TryRecvError::Disconnected) => {
                        stream_ended = true;
                        break;
                    }
                }
            }
        }

        // Render
        let stream_status = stream_ended;
        let visible_panels: Vec<String> = panel_order.clone();
        terminal.draw(|frame| {
            if visible_panels.is_empty() {
                let msg = Paragraph::new(Line::from(Span::styled(
                    " Waiting for logs...",
                    Style::default().fg(Color::DarkGray),
                )));
                frame.render_widget(msg, frame.area());
                return;
            }

            // Reserve 1 line at the bottom for the status bar
            let outer = Layout::default()
                .direction(ratatui::layout::Direction::Vertical)
                .constraints([Constraint::Min(0), Constraint::Length(1)])
                .split(frame.area());

            let panel_area = outer[0];
            let status_area = outer[1];

            // Cap visible panels to what fits (each panel needs at least MIN_PANEL_HEIGHT)
            let max_panels = (panel_area.height / MIN_PANEL_HEIGHT) as usize;
            let show: &[String] = if visible_panels.len() > max_panels && max_panels > 0 {
                &visible_panels[..max_panels]
            } else {
                &visible_panels
            };
            let n = show.len() as u32;

            let constraints: Vec<Constraint> = (0..n).map(|_| Constraint::Ratio(1, n)).collect();
            let chunks = Layout::default()
                .direction(direction.clone())
                .constraints(constraints)
                .split(panel_area);

            for (i, svc) in show.iter().enumerate() {
                let color: Color = match service_color(svc) {
                    crossterm::style::Color::Cyan => Color::Cyan,
                    crossterm::style::Color::Yellow => Color::Yellow,
                    crossterm::style::Color::Green => Color::Green,
                    crossterm::style::Color::Magenta => Color::Magenta,
                    crossterm::style::Color::Blue => Color::Blue,
                    _ => Color::White,
                };

                let area = chunks[i];
                if area.height < 3 {
                    continue; // skip panels too small to render
                }
                let inner_height = area.height.saturating_sub(2) as usize;
                let buf = &panels[svc];
                let start = buf.len().saturating_sub(inner_height);
                let lines: Vec<Line> = buf
                    .iter()
                    .skip(start)
                    .map(|l| Line::from(Span::raw(l.as_str())))
                    .collect();

                let block = Block::default()
                    .borders(Borders::ALL)
                    .border_style(Style::default().fg(color))
                    .title(format!(" {} ", svc))
                    .title_style(Style::default().fg(color));

                let paragraph = Paragraph::new(lines).block(block);

                frame.render_widget(paragraph, area);
            }

            // Status bar
            let status_text = if stream_status {
                " Stream disconnected — press q to exit"
            } else {
                " Streaming logs — press q to exit"
            };
            let status_color = if stream_status {
                Color::Red
            } else {
                Color::DarkGray
            };
            let status = Paragraph::new(Line::from(Span::styled(
                status_text,
                Style::default().fg(status_color),
            )));
            frame.render_widget(status, status_area);
        })?;

        // Handle input (250ms poll for ~4fps refresh)
        if event::poll(Duration::from_millis(250))? {
            if let Event::Key(key) = event::read()? {
                match key.code {
                    KeyCode::Char('q') => break,
                    KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => break,
                    _ => {}
                }
            }
        }
    }

    crossterm::terminal::disable_raw_mode()?;
    crossterm::execute!(
        terminal.backend_mut(),
        LeaveAlternateScreen,
        crossterm::cursor::Show
    )?;
    Ok(())
}

/// Bundle for `logs()` so adding new flags doesn't keep widening the
/// function signature.
pub(crate) struct LogsArgs {
    pub filter: Option<String>,
    pub split: Option<String>,
    pub since: Option<String>,
    pub until: Option<String>,
    pub grep: Option<String>,
    pub interactive: bool,
    pub follow: bool,
    pub service: Option<String>,
}

/// Parse `--since` / `--until`: either the literal `now`, a humantime
/// duration ("2h", "1d12h") relative to now, or an RFC-3339 timestamp.
/// Returns UTC.
fn parse_time_anchor(input: &str) -> Result<chrono::DateTime<chrono::Utc>> {
    let trimmed = input.trim();
    if trimmed.eq_ignore_ascii_case("now") {
        return Ok(chrono::Utc::now());
    }
    if let Ok(ts) = chrono::DateTime::parse_from_rfc3339(trimmed) {
        return Ok(ts.with_timezone(&chrono::Utc));
    }
    let dur = humantime::parse_duration(trimmed)
        .with_context(|| format!("Invalid duration or RFC-3339 timestamp: {:?}", input))?;
    let chrono_dur = chrono::Duration::from_std(dur)
        .with_context(|| format!("Duration too large: {:?}", input))?;
    Ok(chrono::Utc::now() - chrono_dur)
}

/// `spky stats` — live resource graphs for the current project's deployment.
pub fn stats(window: Option<String>, filter: Option<Vec<String>>) -> Result<()> {
    let creds = require_credentials()?;
    let mut client = CloudClient::new(&creds);
    let (slug, pid) = resolve_project_id(&mut client)?;

    crate::stats_tui::run(crate::stats_tui::StatsArgs {
        base_url: client.base_url.clone(),
        auth_header: client.auth_header(),
        pid,
        slug,
        window: window.unwrap_or_else(|| "15m".to_string()),
        filter,
    })
}

pub fn logs(args: LogsArgs) -> Result<()> {
    let LogsArgs {
        filter,
        split,
        since,
        until,
        grep,
        interactive,
        follow,
        service,
    } = args;

    if interactive && split.is_some() {
        bail!("`--interactive` and `--split` can't be combined; pick one TUI.");
    }

    let creds = require_credentials()?;
    let mut client = CloudClient::new(&creds);
    let (slug, pid) = resolve_project_id(&mut client)?;

    let filters = resolve_filters(filter.as_deref(), service.as_deref());

    // Resolve time window up-front so user sees errors before connecting.
    let since_ts = since.as_deref().map(parse_time_anchor).transpose()?;
    let until_ts = until.as_deref().map(parse_time_anchor).transpose()?;

    let has_history_window = since_ts.is_some() || until_ts.is_some() || grep.is_some();

    if interactive {
        // Hand everything over to the TUI. It owns its own fetchers
        // (initial window, scroll-back pagination, follow stream) so we
        // don't build a URL here.
        return crate::logs_browser::launch(
            client.base_url.clone(),
            client.auth_header(),
            pid,
            filters,
            since_ts,
            until_ts,
            grep,
            follow,
        );
    }

    // Build the query string. We only ask the server to close the stream
    // (`tail=false`) when the user has set a BOUNDED window via `--until`;
    // everything else — including a bare `--since 1h` — replays history and
    // then keeps tailing live (the server's default behaviour). This matches
    // `kubectl logs --since` / `journalctl --since -f` expectations: `--since`
    // is "also give me this much past", not "exit after this window".
    let mut query: Vec<(String, String)> = Vec::new();
    if let Some(ts) = since_ts {
        query.push(("since".to_string(), ts.to_rfc3339()));
    }
    if let Some(ts) = until_ts {
        query.push(("until".to_string(), ts.to_rfc3339()));
    }
    if let Some(ref g) = grep {
        query.push(("q".to_string(), g.clone()));
    }
    let bounded = until_ts.is_some();
    if bounded {
        query.push(("tail".to_string(), "false".to_string()));
    }

    let path = format!("/v1/projects/{}/logs", pid);
    let url = if query.is_empty() {
        format!("{}{}", client.base_url, path)
    } else {
        let qs: Vec<String> = query
            .iter()
            .map(|(k, v)| format!("{}={}", urlencode(k), urlencode(v)))
            .collect();
        format!("{}{}?{}", client.base_url, path, qs.join("&"))
    };
    let auth_header = client.auth_header();

    let is_split = split.is_some();

    if !is_split {
        let mode = if bounded {
            "Fetching bounded log window"
        } else if has_history_window {
            "Replaying history + streaming live"
        } else {
            "Streaming live logs"
        };
        println!("{} for '{}' (Ctrl+C to stop)...", mode, slug);
        if let Some(ref f) = filters {
            let mut names: Vec<&str> = f.iter().map(|s| s.as_str()).collect();
            names.sort();
            println!("Filtering services: {}", names.join(", "));
        }
        if let Some(ts) = since_ts {
            println!("Since: {}", ts.to_rfc3339());
        }
        if let Some(ts) = until_ts {
            println!("Until: {}", ts.to_rfc3339());
        }
        if let Some(ref g) = grep {
            println!("Grep: /{}/", g);
        }
        println!();
    }

    let (tx, rx) = std::sync::mpsc::channel();
    let _reader = spawn_sse_reader(url, auth_header, tx);

    // Show timestamps whenever we're replaying history (user cares about "when");
    // live-only tail keeps the existing compact `[service] message` format.
    let show_timestamps = has_history_window;

    match split.as_deref() {
        Some("h") => logs_split(rx, &filters, ratatui::layout::Direction::Vertical)?,
        Some("v") => logs_split(rx, &filters, ratatui::layout::Direction::Horizontal)?,
        Some(other) => bail!(
            "Invalid split mode '{}'. Use 'h' (horizontal) or 'v' (vertical).",
            other
        ),
        None => logs_stream(rx, &filters, show_timestamps)?,
    }

    Ok(())
}

/// Percent-encode a query-string value. We only need a minimal, dependency-free
/// implementation — regex-y strings in `--grep` can contain `+`, `&`, spaces,
/// etc., which would otherwise corrupt the URL.
pub fn urlencode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.as_bytes() {
        match *b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(*b as char);
            }
            _ => out.push_str(&format!("%{:02X}", b)),
        }
    }
    out
}

pub fn scale(ssp: u32) -> Result<()> {
    let creds = require_credentials()?;
    let mut client = CloudClient::new(&creds);
    let (_, pid) = resolve_project_id(&mut client)?;

    let resp = client.post(
        &format!("/v1/projects/{}/scale", pid),
        &serde_json::json!({ "ssp_count": ssp }),
    )?;

    let result: serde_json::Value = resp.into_json().context("Failed to parse response")?;
    let count = result["ssp_count"].as_u64().unwrap_or(ssp as u64);

    println!("Scaling to {} SSP instance(s)...", count);
    poll_scale_completion(&mut client, &pid, ssp)?;
    Ok(())
}

/// Push the local schema to a free (Cloudflare) project's SSP node. Builds the
/// full server schema (user tables + change-push events + meta tables) WITHOUT
/// the remote-function/`$sp00ky_*` param DEFINEs — the control plane owns those
/// for free projects — and POSTs it to the schema proxy, which applies it to the
/// project's database with server-side creds and reloads the node.
pub fn push() -> Result<()> {
    let creds = require_credentials()?;
    let mut client = CloudClient::new(&creds);
    let (slug, pid) = resolve_project_id(&mut client)?;
    push_schema_inner(&mut client, &slug, &pid)
}

/// Build the local server schema and POST it to the schema proxy. Shared by the
/// standalone `spky push` and the free-project tail of `spky deploy`.
pub(crate) fn push_schema_inner(client: &mut CloudClient, slug: &str, pid: &str) -> Result<()> {
    let config_path = std::env::current_dir()?.join("sp00ky.yml");
    let config = backend::load_config(&config_path);
    config.validate()?;

    let config_dir = config_path.parent().unwrap_or(std::path::Path::new("."));
    let schema_rel = config.resolved_schema().schema;
    let input_path = config_dir.join(&schema_rel);
    if !input_path.exists() {
        bail!("schema file not found: {}", input_path.display());
    }

    let builder_config = crate::schema_builder::SchemaBuilderConfig {
        input_path,
        config_path: Some(config_path.clone()),
        mode: config.mode.clone().unwrap_or_default(),
        endpoint: None,
        secret: None,
        include_functions: false,
    };
    let mut sql = crate::schema_builder::build_server_schema(&builder_config)
        .context("failed to build schema")?;

    // Append the per-table ingest/versioning events. `build_server_schema`
    // omits them (the VM path applies them separately via
    // `apply_internal_schema`), but the free/Cloudflare node has no such step —
    // without these events SurrealDB never POSTs mutations to the SSP's
    // `/ingest`, so no incremental updates reach the node and realtime is dead.
    let events = crate::schema_builder::build_server_events(&builder_config)
        .context("failed to build sp00ky ingest events")?;
    sql.push('\n');
    sql.push_str(&events);

    println!("Pushing schema for '{}' ({} bytes)...", slug, sql.len());
    client
        .post(
            &format!("/v1/projects/{}/schema", pid),
            &serde_json::json!({ "sql": sql }),
        )
        .context("schema push failed (is this a free/cloudflare project?)")?;
    println!("  ▸ Schema queued — the SSP node will reload to pick it up.");
    Ok(())
}

/// Role targets a user may name, and the role they map to. `db` is the alias
/// worth having: nobody thinks of the database as "surrealdb" when it is down.
const RESTART_ROLE_ALIASES: &[(&str, &str)] = &[
    ("db", "surrealdb"),
    ("database", "surrealdb"),
    ("surrealdb", "surrealdb"),
    ("surreal", "surrealdb"),
    ("scheduler", "scheduler"),
    ("ssp", "ssp"),
    ("ssps", "ssp"),
    ("backend", "backend"),
    ("backends", "backend"),
    ("frontend", "frontend"),
    ("frontends", "frontend"),
];

/// App names running in the deployment, mapped to the role that hosts them.
///
/// Read from `vms[].metadata.app_name` — the very column the control plane
/// filters on — so a name that resolves here is a name the server can match.
/// Anything else is rejected locally rather than queued as a restart that
/// would stop nothing and still report success.
fn deployed_apps(data: &serde_json::Value) -> std::collections::BTreeMap<String, String> {
    let mut apps = std::collections::BTreeMap::new();
    if let Some(vms) = data.get("vms").and_then(|v| v.as_array()) {
        for vm in vms {
            // Only live rows: a stopped VM from a superseded deployment is not
            // something a restart can act on.
            if !matches!(
                vm.get("status").and_then(|v| v.as_str()),
                Some("running") | Some("starting") | Some("pending")
            ) {
                continue;
            }
            let name = vm
                .get("metadata")
                .and_then(|m| m.get("app_name"))
                .and_then(|v| v.as_str())
                .unwrap_or("");
            let role = vm.get("role").and_then(|v| v.as_str()).unwrap_or("");
            if !name.is_empty() && !role.is_empty() {
                apps.insert(name.to_string(), role.to_string());
            }
        }
    }
    apps
}

/// Whether the control plane can act on per-role / per-app restart targets.
///
/// Probed from `/health`'s `features` list rather than inferred from the
/// restart response, because by the time a response comes back the restart is
/// already queued — and an older control plane would have queued the wrong one.
fn cloud_supports_restart_targets(client: &mut CloudClient) -> bool {
    let Ok(resp) = client.get("/health") else {
        // Unreachable /health is not evidence of an old control plane, and
        // failing closed here would block restarts during exactly the kind of
        // incident they exist for. The POST below surfaces a real outage.
        return true;
    };
    let Ok(body) = resp.into_json::<serde_json::Value>() else {
        return true;
    };
    match body.get("features").and_then(|f| f.as_array()) {
        Some(features) => features
            .iter()
            .any(|f| f.as_str() == Some("restart_targets")),
        // No `features` key at all ⇒ a build older than capability reporting.
        None => false,
    }
}

pub fn restart(
    targets: Vec<String>,
    all_backends: bool,
    yes: bool,
    clean: bool,
    upgrade: bool,
    surreal: bool,
) -> Result<()> {
    let creds = require_credentials()?;
    let mut client = CloudClient::new(&creds);
    let (slug, pid) = resolve_project_id(&mut client)?;

    let mut roles: std::collections::BTreeSet<String> = Default::default();
    let mut apps: std::collections::BTreeSet<String> = Default::default();
    if surreal {
        roles.insert("surrealdb".to_string());
    }
    if all_backends {
        roles.insert("backend".to_string());
    }

    if !targets.is_empty() {
        // Only fetch when there is something to resolve, so the default
        // `spky restart` keeps working even if /deployment is unhappy.
        let data: serde_json::Value = client
            .get(&format!("/v1/projects/{}/deployment", pid))?
            .into_json()
            .context("Failed to parse deployment response")?;
        let known = deployed_apps(&data);

        for target in &targets {
            let key = target.to_ascii_lowercase();
            // Roles win over app names. The two namespaces only collide if an
            // app is literally called "scheduler" or "ssp", and resolving that
            // to the infra role is both the safer reading and the stable one.
            if let Some((_, role)) = RESTART_ROLE_ALIASES.iter().find(|(a, _)| *a == key) {
                roles.insert((*role).to_string());
                continue;
            }
            match known.get(&key) {
                // Only the app, never its role: roles and apps are additive on
                // the server, so adding `backend` here would restart every
                // backend instead of the one that was named.
                Some(_role) => {
                    apps.insert(key);
                }
                None => {
                    let mut valid: Vec<String> =
                        RESTART_ROLE_ALIASES.iter().map(|(a, _)| a.to_string()).collect();
                    valid.dedup();
                    anyhow::bail!(
                        "Unknown restart target '{}' for '{}'.\n  roles: {}\n  apps:  {}",
                        target,
                        slug,
                        valid.join(", "),
                        if known.is_empty() {
                            "(none running)".to_string()
                        } else {
                            known.keys().cloned().collect::<Vec<_>>().join(", ")
                        }
                    );
                }
            }
        }
    }

    // Check the control plane understands targeting BEFORE queueing anything.
    // An older build accepts `roles`/`apps`, ignores them, and queues the
    // historical scheduler+SSP restart — so detecting this from the response
    // would mean the wrong restart is already in flight.
    let targeted = !roles.is_empty() || !apps.is_empty();
    if targeted && !cloud_supports_restart_targets(&mut client) {
        anyhow::bail!(
            "This Sp00ky Cloud control plane does not support targeted restarts yet, \
             and would restart the scheduler + SSPs instead of: {}\n  \
             Upgrade the control plane, or use `spky restart` (scheduler + SSPs) \
             / `spky restart --surreal` explicitly.",
            {
                let mut parts: Vec<String> = roles.iter().cloned().collect();
                parts.extend(apps.iter().cloned());
                parts.join(" + ")
            }
        );
    }

    // Restarting SurrealDB takes the whole deployment offline for a few
    // seconds, so confirm before pulling the trigger. A single backend does
    // not warrant a prompt — that is the whole point of naming one.
    if roles.contains("surrealdb") && !yes {
        let confirmed = inquire::Confirm::new(&format!(
            "Restart SurrealDB for '{}'? The deployment will be briefly unavailable (data is preserved).",
            slug
        ))
        .with_default(false)
        .prompt()
        .context("Failed to read confirmation")?;

        if !confirmed {
            println!("Cancelled.");
            return Ok(());
        }
    }

    // Roles and apps are additive, so the summary is simply both lists. Empty
    // means neither was named, which the control plane resolves to the
    // historical scheduler+SSP set.
    let mut parts: Vec<String> = roles.iter().cloned().collect();
    parts.extend(apps.iter().cloned());
    let summary = if parts.is_empty() {
        "scheduler + SSPs".to_string()
    } else {
        parts.join(" + ")
    };
    let mut flags = Vec::new();
    if clean {
        flags.push("wipe scheduler volume");
    }
    if upgrade {
        flags.push("pull latest images");
    }
    let suffix = if flags.is_empty() {
        String::new()
    } else {
        format!(" ({})", flags.join(", "))
    };
    println!("Restarting {} for '{}'{}...", summary, slug, suffix);

    let mut body = serde_json::json!({ "clean": clean, "upgrade": upgrade, "surreal": surreal });
    // Omit both when empty so the request stays byte-identical to what older
    // CLIs sent, and the control plane's historical default applies.
    if !roles.is_empty() {
        body["roles"] = serde_json::json!(roles.iter().cloned().collect::<Vec<_>>());
    }
    if !apps.is_empty() {
        body["apps"] = serde_json::json!(apps.iter().cloned().collect::<Vec<_>>());
    }

    let resp = client.post(&format!("/v1/projects/{}/restart", pid), &body)?;
    let _: serde_json::Value = resp.into_json().context("Failed to parse response")?;

    // The API only queues the request; the worker stops and recreates the
    // containers itself so there is a single writer of container lifecycle.
    println!("  Restart queued. Worker will recreate the containers shortly.");
    Ok(())
}

pub fn destroy() -> Result<()> {
    let creds = require_credentials()?;
    let mut client = CloudClient::new(&creds);
    let (slug, pid) = resolve_project_id(&mut client)?;

    let confirmed = inquire::Confirm::new(&format!(
        "Are you sure you want to destroy project '{}'? This cannot be undone.",
        slug
    ))
    .with_default(false)
    .prompt()
    .context("Failed to read confirmation")?;

    if !confirmed {
        println!("Cancelled.");
        return Ok(());
    }

    client.delete(&format!("/v1/projects/{}", pid))?;

    println!("  Destroying project '{}'...", slug);

    // Poll until the project status is "destroyed" or it disappears
    let max_attempts = 60; // ~3 minutes at 3s intervals
    for _ in 0..max_attempts {
        thread::sleep(Duration::from_secs(3));

        match fetch_project(&mut client, &slug)? {
            None => {
                // Project no longer listed — fully removed
                println!("  Project '{}' destroyed.", slug);
                return Ok(());
            }
            Some(project) => {
                let status = project["status"].as_str().unwrap_or("unknown");
                if status == "destroyed" {
                    println!("  Project '{}' destroyed.", slug);
                    return Ok(());
                }
                // Still tearing down, keep polling
            }
        }
    }

    println!("  Destroy is still in progress. Run `spky status` to check.");
    Ok(())
}

/// Accept either a backup id (UUID) or a user-supplied `name`/slug and return
/// the resolved id. UUIDs pass through untouched to avoid a round-trip on the
/// common case. For anything else we fetch `/backups` and match on name, id
/// prefix (first 8 chars shown by `backup list`), or exact id.
fn resolve_backup_id(client: &mut CloudClient, pid: &str, input: &str) -> Result<String> {
    if is_uuid(input) {
        return Ok(input.to_string());
    }

    let resp = client.get(&format!("/v1/projects/{}/backups", pid))?;
    let backups: Vec<serde_json::Value> = resp
        .into_json()
        .context("Failed to list backups while resolving backup name")?;

    let mut by_name: Vec<&serde_json::Value> = backups
        .iter()
        .filter(|b| b["name"].as_str() == Some(input))
        .collect();
    if by_name.len() == 1 {
        return Ok(by_name.remove(0)["id"]
            .as_str()
            .unwrap_or(input)
            .to_string());
    }
    if by_name.len() > 1 {
        bail!(
            "Backup name '{}' is ambiguous — {} backups share this name. Pass the full backup id instead.",
            input,
            by_name.len()
        );
    }

    let mut by_prefix: Vec<&serde_json::Value> = backups
        .iter()
        .filter(|b| {
            b["id"]
                .as_str()
                .map(|id| id.starts_with(input))
                .unwrap_or(false)
        })
        .collect();
    if by_prefix.len() == 1 {
        return Ok(by_prefix.remove(0)["id"]
            .as_str()
            .unwrap_or(input)
            .to_string());
    }
    if by_prefix.len() > 1 {
        bail!(
            "Backup id prefix '{}' is ambiguous — matches {} backups.",
            input,
            by_prefix.len()
        );
    }

    bail!(
        "No backup found matching '{}'. Run `spky backup list` to see available backups.",
        input
    )
}

fn is_uuid(s: &str) -> bool {
    // UUIDs are 36 chars, 8-4-4-4-12 hex with dashes.
    if s.len() != 36 {
        return false;
    }
    let bytes = s.as_bytes();
    for (i, &b) in bytes.iter().enumerate() {
        let expect_dash = matches!(i, 8 | 13 | 18 | 23);
        if expect_dash {
            if b != b'-' {
                return false;
            }
        } else if !b.is_ascii_hexdigit() {
            return false;
        }
    }
    true
}

pub fn backup(action: CloudBackupCommands) -> Result<()> {
    let creds = require_credentials()?;
    let mut client = CloudClient::new(&creds);
    let (_slug, pid) = resolve_project_id(&mut client)?;

    match action {
        CloudBackupCommands::List => {
            let resp = client.get(&format!("/v1/projects/{}/backups", pid))?;
            let backups: Vec<serde_json::Value> = resp.into_json()?;
            if backups.is_empty() {
                println!("  No backups found.");
                return Ok(());
            }
            println!(
                "  {:<36}  {:<12}  {:<10}  {}",
                "ID", "STATUS", "SIZE", "CREATED"
            );
            println!("  {}", "-".repeat(80));
            for b in &backups {
                let id = b["id"].as_str().unwrap_or("?");
                let status = b["status"].as_str().unwrap_or("?");
                let size = b["size_bytes"].as_i64().unwrap_or(0);
                let created = b["created_at"].as_str().unwrap_or("?");
                let name = b["name"].as_str().unwrap_or("");
                let size_str = if size > 1_000_000 {
                    format!("{:.1} MB", size as f64 / 1_000_000.0)
                } else if size > 0 {
                    format!("{:.1} KB", size as f64 / 1_000.0)
                } else {
                    "—".to_string()
                };
                let label = if name.is_empty() {
                    id.to_string()
                } else {
                    format!("{} ({})", name, &id[..8])
                };
                println!(
                    "  {:<36}  {:<12}  {:<10}  {}",
                    label,
                    status,
                    size_str,
                    &created[..19]
                );
            }
        }
        CloudBackupCommands::Create { name } => {
            println!("  Creating backup...");
            let body = serde_json::json!({ "name": name });
            let resp = client.post(&format!("/v1/projects/{}/backups", pid), &body)?;
            let result: serde_json::Value = resp.into_json()?;
            println!("  Backup created: {}", result["id"].as_str().unwrap_or("?"));
            println!(
                "  Status: {}",
                result["status"].as_str().unwrap_or("pending")
            );
        }
        CloudBackupCommands::Restore { backup_id } => {
            let backup_id = resolve_backup_id(&mut client, &pid, &backup_id)?;
            println!("  Restoring from backup {}...", backup_id);
            let resp = client.post(
                &format!("/v1/projects/{}/backups/{}/restore", pid, backup_id),
                &serde_json::json!({}),
            )?;
            let result: serde_json::Value = resp.into_json()?;
            let restore_id = result["id"]
                .as_str()
                .or_else(|| result["restore_id"].as_str())
                .unwrap_or(&backup_id)
                .to_string();
            println!("  Restore {} queued. Waiting for completion...", restore_id);

            // Poll until restore completes. Same cadence as backup polling in
            // the Reset flow (5s × 60 = up to 5 minutes).
            let mut last_status = String::new();
            let mut final_status: Option<String> = None;
            let mut final_error: Option<String> = None;
            for _ in 0..60 {
                thread::sleep(Duration::from_secs(5));
                let status_resp = client.get(&format!(
                    "/v1/projects/{}/backups/{}/restore",
                    pid, backup_id
                ))?;
                let row: serde_json::Value = status_resp.into_json()?;
                let status = row["status"].as_str().unwrap_or("pending").to_string();
                match status.as_str() {
                    "completed" => {
                        if !last_status.is_empty() {
                            println!();
                        }
                        println!("  Restore completed.");
                        final_status = Some(status);
                        break;
                    }
                    "failed" => {
                        if !last_status.is_empty() {
                            println!();
                        }
                        final_error = Some(row["error"].as_str().unwrap_or("unknown").to_string());
                        final_status = Some(status);
                        break;
                    }
                    _ => {
                        if status != last_status {
                            if !last_status.is_empty() {
                                println!();
                            }
                            print!("  [{}]", status);
                            last_status = status;
                        } else {
                            print!(".");
                        }
                        let _ = std::io::Write::flush(&mut std::io::stdout());
                    }
                }
            }

            match final_status.as_deref() {
                Some("completed") => {}
                Some("failed") => {
                    let err = final_error.unwrap_or_else(|| "unknown".to_string());
                    bail!("Restore failed: {}", err);
                }
                _ => bail!("Restore did not complete within 5 minutes. Check status with `spky backup list`."),
            }

            // Re-run migrations against the restored database.
            println!("  Running migrations...");
            let config_path = std::env::current_dir()?.join("sp00ky.yml");
            let config = backend::load_config(&config_path);

            let deployment = client.get(&format!("/v1/projects/{}/deployment", pid))?;
            let deployment: serde_json::Value = deployment.into_json()?;

            let db_url = deployment["urls"]["surrealdb"]
                .as_str()
                .map(|s| s.to_string())
                .or_else(|| {
                    deployment["surrealdb_ip"]
                        .as_str()
                        .map(|ip| format!("http://{}:8000", ip))
                });
            let db_password = deployment["surrealdb_password"]
                .as_str()
                .or_else(|| deployment["db_password"].as_str())
                .unwrap_or("")
                .to_string();

            if let Some(db_url) = db_url {
                let resolved = config.resolved_surrealdb();
                let schema = config.resolved_schema();
                let config_dir = config_path.parent().unwrap_or(std::path::Path::new("."));
                let migrations_dir = config_dir.join(&schema.migrations);

                if migrations_dir.exists() {
                    let ctx = crate::migration::MigrationContext {
                        environment: crate::migration::MigrationEnvironment::Production,
                        project_dir: config_dir.to_path_buf(),
                        migrations_dir: migrations_dir.clone(),
                        url: db_url.clone(),
                        namespace: resolved.namespace.clone(),
                        database: resolved.database.clone(),
                        username: "root".to_string(),
                        password: db_password.clone(),
                        surrealkit_binary: config.resolved_surrealkit_binary(),
                        internal_schema: None,
                        remote_functions: None,
                        secrets: Some(load_vault_secrets_for_prod()),
                    };
                    match crate::migration::create_engine(ctx).and_then(|e| e.apply()) {
                        Ok(_) => println!("  Migrations complete."),
                        Err(e) => println!("  Migration warning: {:?}", e),
                    }
                } else {
                    println!("  No migrations directory found, skipping.");
                }

                // Re-apply sp00ky-internal schema (meta tables + per-table events).
                // The dump only contains user-table data; without this, the restored
                // DB is missing _00_metadata / _00_query / mutation + delete events
                // and the scheduler/SSP can't bootstrap against it.
                let restore_schema = apply_remote_fns_and_internal_schema(
                    &db_url,
                    &db_password,
                    &deployment,
                    &config,
                    config_path.as_path(),
                    &mut client,
                    &pid,
                );
                if !restore_schema.is_empty() {
                    // The dump carries user-table data only. Without the meta
                    // tables and per-table events the scheduler and SSP cannot
                    // bootstrap against the restored DB at all, so this is not a
                    // completed restore.
                    restore_schema.report("Restore");
                    bail!("restore finished but the internal schema did not apply");
                }
            } else {
                println!("  Warning: could not resolve SurrealDB URL from deployment; skipping migrations.");
            }

            println!();
            println!("  Restore complete.");
        }
        CloudBackupCommands::Delete { backup_id } => {
            let backup_id = resolve_backup_id(&mut client, &pid, &backup_id)?;
            let resp = client.delete(&format!("/v1/projects/{}/backups/{}", pid, backup_id))?;
            if resp.status() == 200 {
                println!("  Backup {} deleted.", backup_id);
            } else {
                bail!("Failed to delete backup");
            }
        }
        CloudBackupCommands::Configure {
            enabled,
            schedule,
            retention,
        } => {
            let body = serde_json::json!({
                "enabled": enabled,
                "schedule": schedule,
                "retention": retention,
            });
            client.post(&format!("/v1/projects/{}/backups/configure", pid), &body)?;
            println!("  Backup configuration updated.");
        }
        CloudBackupCommands::Reset { no_backup } => {
            println!();
            println!("  ╔══════════════════════════════════════════════════════╗");
            println!("  ║  ⚠️  WARNING: DATABASE RESET                        ║");
            println!("  ║                                                      ║");
            println!("  ║  This will PERMANENTLY DELETE all data in the        ║");
            println!("  ║  database and recreate it from scratch.              ║");
            println!("  ║                                                      ║");
            println!("  ║  All user accounts, records, and application         ║");
            println!("  ║  data will be lost. Migrations will be re-applied.   ║");
            println!("  ╚══════════════════════════════════════════════════════╝");
            println!();

            let confirmed =
                inquire::Confirm::new("Are you absolutely sure you want to reset the database?")
                    .with_default(false)
                    .prompt()
                    .context("Failed to read confirmation")?;

            if !confirmed {
                println!("  Cancelled.");
                return Ok(());
            }

            // Offer to create a backup first
            if !no_backup {
                let backup_first = inquire::Confirm::new("Create a backup before resetting?")
                    .with_default(true)
                    .prompt()
                    .context("Failed to read confirmation")?;

                if backup_first {
                    println!("  Creating backup before reset...");
                    let body = serde_json::json!({ "name": "pre-reset-backup" });
                    let resp = client.post(&format!("/v1/projects/{}/backups", pid), &body)?;
                    let result: serde_json::Value = resp.into_json()?;
                    let backup_id = result["id"].as_str().unwrap_or("?").to_string();
                    println!("  Backup {} created. Waiting for completion...", backup_id);

                    // Poll until backup completes
                    for _ in 0..60 {
                        thread::sleep(Duration::from_secs(5));
                        let status_resp = client.get(&format!("/v1/projects/{}/backups", pid))?;
                        let backups: Vec<serde_json::Value> = status_resp.into_json()?;
                        if let Some(b) = backups
                            .iter()
                            .find(|b| b["id"].as_str() == Some(&backup_id))
                        {
                            match b["status"].as_str() {
                                Some("completed") => {
                                    println!("  Backup completed.");
                                    break;
                                }
                                Some("failed") => {
                                    let err = b["error"].as_str().unwrap_or("unknown");
                                    bail!("Backup failed: {}. Aborting reset.", err);
                                }
                                _ => print!("."),
                            }
                        }
                    }
                    println!();
                }
            }

            // Reset the database
            println!("  Resetting database...");
            let resp = client.post(
                &format!("/v1/projects/{}/backups/reset", pid),
                &serde_json::json!({}),
            )?;
            let result: serde_json::Value = resp.into_json()?;
            println!(
                "  Database reset: {}",
                result["status"].as_str().unwrap_or("done")
            );

            // Re-run migrations
            println!("  Running migrations...");
            let config_path = std::env::current_dir()?.join("sp00ky.yml");
            let config = backend::load_config(&config_path);

            if let Some(db_url) = result["surrealdb_ip"].as_str() {
                let resolved = config.resolved_surrealdb();
                let db_password = result
                    .get("db_password")
                    .and_then(|v| v.as_str())
                    .unwrap_or("");

                // Connect directly to SurrealDB internal IP for migrations
                let surreal_url = format!("http://{}:8000", db_url);

                let schema = config.resolved_schema();
                let config_dir = config_path.parent().unwrap_or(std::path::Path::new("."));
                let migrations_dir = config_dir.join(&schema.migrations);

                if migrations_dir.exists() {
                    let ctx = crate::migration::MigrationContext {
                        environment: crate::migration::MigrationEnvironment::Production,
                        project_dir: config_dir.to_path_buf(),
                        migrations_dir: migrations_dir.clone(),
                        url: surreal_url.clone(),
                        namespace: resolved.namespace.clone(),
                        database: resolved.database.clone(),
                        username: "root".to_string(),
                        password: db_password.to_string(),
                        surrealkit_binary: config.resolved_surrealkit_binary(),
                        internal_schema: None,
                        remote_functions: None,
                        secrets: Some(load_vault_secrets_for_prod()),
                    };
                    match crate::migration::create_engine(ctx).and_then(|e| e.apply()) {
                        Ok(_) => println!("  Migrations complete."),
                        Err(e) => println!("  Migration warning: {:?}", e),
                    }
                }

                // Apply remote functions
                if let Some(sched_url) = result["scheduler_url"].as_str() {
                    let surreal_client = if db_password.is_empty() {
                        crate::surreal_client::SurrealClient::new_unauthenticated(
                            &surreal_url,
                            &resolved.namespace,
                            &resolved.database,
                        )
                    } else {
                        crate::surreal_client::SurrealClient::new(
                            &surreal_url,
                            &resolved.namespace,
                            &resolved.database,
                            "root",
                            db_password,
                        )
                    };
                    let mode = config.mode.clone().unwrap_or_default();
                    // Never bake an ephemeral Docker IP — fall back to the stable
                    // DNS alias if the server handed us a raw bridge IP.
                    let fn_endpoint = if is_stable_endpoint_host(endpoint_host(sched_url)) {
                        sched_url.to_string()
                    } else {
                        docker_alias_endpoint(&mode)
                    };
                    let functions_sql = crate::schema_builder::build_remote_functions_schema(
                        &mode,
                        &fn_endpoint,
                        "",
                    );
                    // Raw execute (not apply_remote_functions_if_changed) on
                    // purpose: `/backups/reset` wipes the DB and this flow does
                    // NOT re-apply the internal schema (internal_schema: None
                    // above), so _00_schema_state doesn't exist here — there's no
                    // stored hash to consult or keep in sync, and the next full
                    // `spky deploy` re-applies (and re-hashes) everything anyway.
                    match surreal_client.execute(&functions_sql) {
                        Ok(_) => println!("  Remote functions applied."),
                        Err(e) => println!("  Warning: remote functions: {:?}", e),
                    }
                }
            }

            println!();
            println!("  Database reset complete.");
        }
    }
    Ok(())
}

pub fn billing(action: Option<CloudBillingCommands>) -> Result<()> {
    let creds = require_credentials()?;
    let mut client = CloudClient::new(&creds);

    match action {
        Some(CloudBillingCommands::Usage) => {
            let resp = client.get("/v1/billing/usage")?;
            let usage: Vec<serde_json::Value> =
                resp.into_json().context("Failed to parse usage")?;

            if usage.is_empty() {
                println!("No usage data for this billing period.");
                return Ok(());
            }

            println!("{:<20} {:<20} {:>10}", "PROJECT", "METRIC", "TOTAL");
            println!("{}", "-".repeat(52));
            for entry in &usage {
                println!(
                    "{:<20} {:<20} {:>10.2}",
                    entry["project"].as_str().unwrap_or("-"),
                    entry["metric"].as_str().unwrap_or("-"),
                    entry["total"].as_f64().unwrap_or(0.0),
                );
            }
            Ok(())
        }
        Some(CloudBillingCommands::ChangePlan) => {
            let slug = resolve_project_slug()?;

            // Get current project info
            let project = fetch_project(&mut client, &slug)?
                .context(format!("Project '{}' not found", slug))?;
            let current_plan = project["plan"].as_str().unwrap_or("starter");
            let current_interval = project["billing_interval"].as_str().unwrap_or("monthly");

            println!("  Current plan: {} ({})", current_plan, current_interval);
            println!();

            // Plan selection
            let plans = vec!["Starter", "Pro", "Free (Cloudflare)"];
            let default_plan = if current_plan == "pro" { 1 } else { 0 };
            let plan_selection = inquire::Select::new("Select a plan:", plans)
                .with_starting_cursor(default_plan)
                .prompt()
                .context("Failed to select plan")?;
            let new_plan = match plan_selection {
                "Pro" => "pro",
                "Free (Cloudflare)" => "free",
                _ => "starter",
            };

            // Free: no billing interval. Confirm the (destructive) downgrade —
            // this cancels the subscription and tears down the paid deployment.
            if new_plan == "free" {
                if current_plan == "free" {
                    println!("  Already on the free plan.");
                    return Ok(());
                }
                let confirm = inquire::Confirm::new(
                    "Downgrade to Free? This cancels your subscription and tears down \
                     the current deployment. You then run `spky deploy` to publish on Cloudflare.",
                )
                .with_default(false)
                .prompt()
                .context("Failed to read confirmation")?;
                if !confirm {
                    println!("  Cancelled.");
                    return Ok(());
                }
                let resp = client.post(
                    "/v1/billing/change-plan",
                    &serde_json::json!({ "project_id": slug, "plan": "free" }),
                )?;
                let data: serde_json::Value =
                    resp.into_json().context("Failed to parse response")?;
                let status = data["status"].as_str().unwrap_or("unknown");
                if status == "downgraded" {
                    println!("  Switched to the free plan. Old deployment torn down.");
                    println!("  Run `spky deploy` to publish on Cloudflare.");
                } else if status == "no_change" {
                    println!("  Already on the free plan.");
                } else {
                    println!("  Response: {}", data);
                }
                return Ok(());
            }

            // Billing interval selection
            let intervals = vec!["Monthly", "Yearly"];
            let default_interval = if current_interval == "yearly" { 1 } else { 0 };
            let interval_selection = inquire::Select::new("Billing interval:", intervals)
                .with_starting_cursor(default_interval)
                .prompt()
                .context("Failed to select billing interval")?;
            let new_interval = match interval_selection {
                "Yearly" => "yearly",
                _ => "monthly",
            };

            if new_plan == current_plan && new_interval == current_interval {
                println!(
                    "  No changes needed — already on {} ({}).",
                    new_plan, new_interval
                );
                return Ok(());
            }

            let confirm = inquire::Confirm::new(&format!(
                "Switch to {} ({})? Stripe will handle proration.",
                new_plan, new_interval,
            ))
            .with_default(true)
            .prompt()
            .context("Failed to read confirmation")?;

            if !confirm {
                println!("  Cancelled.");
                return Ok(());
            }

            let resp = client.post(
                "/v1/billing/change-plan",
                &serde_json::json!({
                    "project_id": slug,
                    "plan": new_plan,
                    "billing_interval": new_interval,
                }),
            )?;
            let data: serde_json::Value = resp.into_json().context("Failed to parse response")?;
            let status = data["status"].as_str().unwrap_or("unknown");

            if status == "updated" {
                println!("  Plan updated to {} ({}).", new_plan, new_interval);
            } else if status == "no_change" {
                println!("  No changes needed.");
            } else {
                println!("  Response: {}", data);
            }
            Ok(())
        }
        None => {
            let slug = resolve_project_slug()?;

            // Try portal first, fall back to checkout if no billing account yet
            match client.post("/v1/billing/portal", &serde_json::json!({})) {
                Ok(resp) => {
                    let data: serde_json::Value =
                        resp.into_json().context("Failed to parse response")?;
                    let url = data["url"]
                        .as_str()
                        .context("No billing portal URL in response")?;
                    println!("Opening billing portal...");
                    open::that(url).context("Failed to open browser")?;
                }
                Err(_) => {
                    // No billing account — start checkout and wait for payment
                    println!(
                        "No billing account found. Starting checkout for '{}'...",
                        slug
                    );
                    wait_for_billing(&mut client, &slug)?;
                }
            }
            Ok(())
        }
    }
}

// ---------------------------------------------------------------------------
// Team management
// ---------------------------------------------------------------------------

pub fn team(action: CloudTeamCommands) -> Result<()> {
    let creds = require_credentials()?;
    let mut client = CloudClient::new(&creds);

    match action {
        CloudTeamCommands::List => {
            let resp = client.get("/v1/tenants")?;
            let tenants: Vec<serde_json::Value> =
                resp.into_json().context("Failed to parse tenants")?;

            // Find the active tenant (personal or first one)
            let tenant = tenants.first().context("No tenant found")?;
            let tenant_id = tenant["id"].as_str().context("No tenant ID")?;

            let resp = client.get(&format!("/v1/tenants/{}/members", tenant_id))?;
            let members: Vec<serde_json::Value> =
                resp.into_json().context("Failed to parse members")?;

            if members.is_empty() {
                println!("No team members found.");
                return Ok(());
            }

            println!("{:<30} {:<10} {}", "EMAIL", "ROLE", "JOINED");
            println!("{}", "-".repeat(60));
            for member in &members {
                let email = member["email"]
                    .as_str()
                    .or_else(|| member["github_login"].as_str())
                    .unwrap_or("-");
                let role = member["role"].as_str().unwrap_or("-");
                let created = member["created_at"].as_str().unwrap_or("-");
                // Trim the timestamp to just the date portion
                let date = created.split('T').next().unwrap_or(created);
                println!("{:<30} {:<10} {}", email, role, date);
            }
            Ok(())
        }

        CloudTeamCommands::Invite { email } => {
            let resp = client.get("/v1/tenants")?;
            let tenants: Vec<serde_json::Value> =
                resp.into_json().context("Failed to parse tenants")?;
            let tenant = tenants.first().context("No tenant found")?;
            let tenant_id = tenant["id"].as_str().context("No tenant ID")?;

            // Check if vault exists and prompt for passphrase to transfer vault key
            let vault_resp = client.get("/v1/vault/status")?;
            let vault_status: serde_json::Value = vault_resp.into_json().unwrap_or_default();
            let vault_initialized = vault_status["initialized"].as_bool().unwrap_or(false);

            let mut body = serde_json::json!({
                "email": email,
                "role": "member",
            });

            if vault_initialized && is_interactive() {
                let dk = get_derived_key(&mut client)?;
                body["derived_key"] = serde_json::Value::String(dk);
            }

            client.post(&format!("/v1/tenants/{}/invitations", tenant_id), &body)?;

            println!("  Invitation sent to {}.", email);
            println!("  They will receive an email with a link to accept.");
            Ok(())
        }

        CloudTeamCommands::Invitations => {
            let resp = client.get("/v1/tenants")?;
            let tenants: Vec<serde_json::Value> =
                resp.into_json().context("Failed to parse tenants")?;
            let tenant = tenants.first().context("No tenant found")?;
            let tenant_id = tenant["id"].as_str().context("No tenant ID")?;

            let resp = client.get(&format!("/v1/tenants/{}/invitations", tenant_id))?;
            let invitations: Vec<serde_json::Value> =
                resp.into_json().context("Failed to parse invitations")?;

            if invitations.is_empty() {
                println!("No pending invitations.");
                return Ok(());
            }

            println!(
                "{:<30} {:<10} {:<12} {}",
                "EMAIL", "ROLE", "STATUS", "EXPIRES"
            );
            println!("{}", "-".repeat(70));
            for inv in &invitations {
                let email = inv["email"].as_str().unwrap_or("-");
                let role = inv["role"].as_str().unwrap_or("-");
                let status = inv["status"].as_str().unwrap_or("-");
                let expires = inv["expires_at"].as_str().unwrap_or("-");
                let date = expires.split('T').next().unwrap_or(expires);
                println!("{:<30} {:<10} {:<12} {}", email, role, status, date);
            }
            Ok(())
        }

        CloudTeamCommands::Revoke { email } => {
            let resp = client.get("/v1/tenants")?;
            let tenants: Vec<serde_json::Value> =
                resp.into_json().context("Failed to parse tenants")?;
            let tenant = tenants.first().context("No tenant found")?;
            let tenant_id = tenant["id"].as_str().context("No tenant ID")?;

            // Find the invitation by email
            let resp = client.get(&format!("/v1/tenants/{}/invitations", tenant_id))?;
            let invitations: Vec<serde_json::Value> =
                resp.into_json().context("Failed to parse invitations")?;

            let inv = invitations
                .iter()
                .find(|i| i["email"].as_str() == Some(&email))
                .context(format!("No pending invitation found for '{}'", email))?;

            let inv_id = inv["id"].as_str().context("No invitation ID")?;

            client.delete(&format!("/v1/tenants/{}/invitations/{}", tenant_id, inv_id))?;
            println!("  Invitation for {} has been revoked.", email);
            Ok(())
        }

        CloudTeamCommands::Remove { email } => {
            let resp = client.get("/v1/tenants")?;
            let tenants: Vec<serde_json::Value> =
                resp.into_json().context("Failed to parse tenants")?;
            let tenant = tenants.first().context("No tenant found")?;
            let tenant_id = tenant["id"].as_str().context("No tenant ID")?;

            // Find the member by email
            let resp = client.get(&format!("/v1/tenants/{}/members", tenant_id))?;
            let members: Vec<serde_json::Value> =
                resp.into_json().context("Failed to parse members")?;

            let member = members
                .iter()
                .find(|m| {
                    m["email"].as_str() == Some(&email)
                        || m["github_login"].as_str() == Some(&email)
                })
                .context(format!("No member found with email '{}'", email))?;

            let member_id = member["id"].as_str().context("No member ID")?;

            if is_interactive() {
                let confirm = inquire::Confirm::new(&format!(
                    "Remove {} from the team? This revokes all access immediately.",
                    email,
                ))
                .with_default(false)
                .prompt()
                .context("Failed to read confirmation")?;

                if !confirm {
                    println!("  Cancelled.");
                    return Ok(());
                }
            }

            client.delete(&format!("/v1/tenants/{}/members/{}", tenant_id, member_id))?;
            println!("  {} has been removed from the team.", email);
            Ok(())
        }

        CloudTeamCommands::Rename { name } => {
            let resp = client.get("/v1/tenants")?;
            let tenants: Vec<serde_json::Value> =
                resp.into_json().context("Failed to parse tenants")?;
            let tenant = tenants.first().context("No tenant found")?;
            let tenant_id = tenant["id"].as_str().context("No tenant ID")?;
            let current_name = tenant["name"].as_str().unwrap_or("-");

            let new_name = match name {
                Some(n) => n,
                None => {
                    println!("  Current name: {}", current_name);
                    inquire::Text::new("New tenant name:")
                        .prompt()
                        .context("Failed to read name")?
                }
            };

            let resp = client.patch(
                &format!("/v1/tenants/{}", tenant_id),
                &serde_json::json!({ "name": new_name }),
            )?;
            let updated: serde_json::Value =
                resp.into_json().context("Failed to parse response")?;
            println!(
                "  Tenant renamed to '{}'.",
                updated["name"].as_str().unwrap_or(&new_name)
            );
            Ok(())
        }
    }
}

// ---------------------------------------------------------------------------
// Vault passphrase reset
// ---------------------------------------------------------------------------

fn vault(action: CloudVaultCommands) -> Result<()> {
    let creds = require_credentials()?;
    let mut client = CloudClient::new(&creds);

    // Get tenant ID (same pattern as team())
    let resp = client.get("/v1/tenants")?;
    let tenants: Vec<serde_json::Value> = resp.into_json().context("Failed to parse tenants")?;
    let tenant = tenants.first().context("No tenant found")?;
    let tenant_id = tenant["id"].as_str().context("No tenant ID")?;

    match action {
        CloudVaultCommands::RequestReset => {
            client.post(
                &format!("/v1/tenants/{}/vault-resets", tenant_id),
                &serde_json::json!({}),
            )?;
            println!("  Reset request submitted.");
            println!("  Your team admin(s) have been notified by email.");
            Ok(())
        }

        CloudVaultCommands::ApproveReset { email } => {
            // List pending resets and find the one for this email
            let resp = client.get(&format!("/v1/tenants/{}/vault-resets", tenant_id))?;
            let resets: Vec<serde_json::Value> =
                resp.into_json().context("Failed to parse reset requests")?;

            let reset = resets
                .iter()
                .find(|r| {
                    r["email"].as_str() == Some(&email) && r["status"].as_str() == Some("pending")
                })
                .context(format!("No pending reset request found for '{}'", email))?;

            let reset_id = reset["id"].as_str().context("No reset ID")?;

            let dk = get_derived_key(&mut client)?;

            client.post(
                &format!(
                    "/v1/tenants/{}/vault-resets/{}/approve",
                    tenant_id, reset_id
                ),
                &serde_json::json!({ "derived_key": dk }),
            )?;

            println!("  Reset approved for {}.", email);
            println!("  They have been notified by email and can now set a new passphrase.");
            Ok(())
        }

        CloudVaultCommands::CompleteReset => {
            // Check if we have an approved reset
            let resp = client.get(&format!("/v1/tenants/{}/vault-resets/mine", tenant_id))?;
            let reset: serde_json::Value =
                resp.into_json().context("Failed to parse reset status")?;

            let status = reset["status"].as_str().unwrap_or("");
            if status == "pending" {
                println!("  Your reset request is still pending admin approval.");
                println!(
                    "  Ask a team admin to run: spky env reset approve <your-email>"
                );
                return Ok(());
            }
            if status != "approved" {
                bail!("No approved reset request found. Request one first with: spky env reset request");
            }

            // Prompt for new passphrase
            let passphrase = inquire::Password::new("New vault passphrase:")
                .with_display_mode(inquire::PasswordDisplayMode::Masked)
                .prompt()
                .context("Failed to read passphrase")?;

            if passphrase.is_empty() {
                bail!("Passphrase cannot be empty.");
            }

            let confirm = inquire::Password::new("Confirm passphrase:")
                .with_display_mode(inquire::PasswordDisplayMode::Masked)
                .without_confirmation()
                .prompt()
                .context("Failed to read confirmation")?;

            if passphrase != confirm {
                bail!("Passphrases do not match.");
            }

            let salt_hex = generate_salt();
            let salt = hex::decode(&salt_hex).context("Invalid salt")?;
            println!("Deriving encryption key...");
            let dk = derive_key(&passphrase, &salt)?;

            client.post(
                &format!("/v1/tenants/{}/vault-resets/complete", tenant_id),
                &serde_json::json!({ "derived_key": dk, "salt": salt_hex }),
            )?;

            // Update cached derived key if it exists
            if load_cached_derived_key().is_some() {
                save_cached_derived_key(&dk)?;
                println!("  Cached derived key updated.");
            } else if is_interactive() {
                let cache = inquire::Confirm::new("Cache derived key locally in ~/.sp00ky/?")
                    .with_default(true)
                    .prompt()
                    .unwrap_or(false);
                if cache {
                    save_cached_derived_key(&dk)?;
                }
            }

            println!("  Vault passphrase has been reset successfully.");
            Ok(())
        }

        CloudVaultCommands::ListResets => {
            let resp = client.get(&format!("/v1/tenants/{}/vault-resets", tenant_id))?;
            let resets: Vec<serde_json::Value> =
                resp.into_json().context("Failed to parse reset requests")?;

            if resets.is_empty() {
                println!("No pending vault reset requests.");
                return Ok(());
            }

            println!("{:<30} {:<12} {}", "EMAIL", "STATUS", "REQUESTED");
            println!("{}", "-".repeat(60));
            for reset in &resets {
                let email = reset["email"].as_str().unwrap_or("-");
                let status = reset["status"].as_str().unwrap_or("-");
                let created = reset["created_at"].as_str().unwrap_or("-");
                let date = created.split('T').next().unwrap_or(created);
                println!("{:<30} {:<12} {}", email, status, date);
            }
            Ok(())
        }
    }
}

pub fn keys(action: CloudKeyCommands) -> Result<()> {
    let creds = require_credentials()?;
    let mut client = CloudClient::new(&creds);

    match action {
        CloudKeyCommands::Create => {
            let resp = client.post("/v1/auth/keys", &serde_json::json!({}))?;
            let data: serde_json::Value = resp.into_json()?;
            let key = data["key"].as_str().unwrap_or("");
            let id = data["id"].as_str().unwrap_or("");
            println!("API key created:");
            println!("  Key: {}", key);
            println!("  ID:  {}", id);
            println!();
            println!("  Save this key — it won't be shown again.");
            println!("  Use it in CI/CD: export SPOOKY_API_KEY={}", key);
        }
        CloudKeyCommands::List => {
            let resp = client.get("/v1/auth/keys")?;
            let data: Vec<serde_json::Value> = resp.into_json()?;
            if data.is_empty() {
                println!("No API keys found. Create one with `spky token create`.");
            } else {
                println!("{:<38} {:<24} {}", "ID", "PREFIX", "CREATED");
                for key in &data {
                    println!(
                        "{:<38} {:<24} {}",
                        key["id"].as_str().unwrap_or("-"),
                        key["prefix"].as_str().unwrap_or("-"),
                        key["created_at"].as_str().unwrap_or("-"),
                    );
                }
            }
        }
        CloudKeyCommands::Revoke { id } => {
            client.delete(&format!("/v1/auth/keys/{}", id))?;
            println!("API key {} revoked.", id);
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn print_deployment_details(data: &serde_json::Value) {
    if let Some(dep) = data.get("deployment") {
        println!(
            "  Deployment v{} — {}",
            dep["version"].as_u64().unwrap_or(0),
            dep["status"].as_str().unwrap_or("unknown")
        );
        if let Some(err) = dep["error"].as_str() {
            println!("  Error: {}", err);
        }
    }

    // Collect component versions from /api/spooky (keyed by entity name)
    let mut component_versions: std::collections::HashMap<String, (String, String)> =
        std::collections::HashMap::new();
    // The scheduler's e2e sync heartbeat, if it reports one.
    let mut heartbeat: Option<serde_json::Value> = None;
    if let Some(db_url) = data
        .get("urls")
        .and_then(|u| u.get("surrealdb"))
        .and_then(|v| v.as_str())
    {
        let config_path = std::env::current_dir()
            .unwrap_or_default()
            .join("sp00ky.yml");
        let config = backend::load_config(&config_path);
        let resolved = config.resolved_surrealdb();
        let spooky_url = format!(
            "{}/api/{}/{}/spooky",
            db_url, resolved.namespace, resolved.database
        );
        if let Ok(resp) = ureq::get(&spooky_url)
            .timeout(std::time::Duration::from_secs(5))
            .call()
        {
            if let Ok(entities) = resp.into_json::<Vec<serde_json::Value>>() {
                for entity in &entities {
                    let name = entity["entity"].as_str().unwrap_or("").to_string();
                    let ver = entity["version"].as_str().unwrap_or("-").to_string();
                    let st = entity["status"].as_str().unwrap_or("-").to_string();
                    if name == "scheduler" {
                        heartbeat = entity.get("heartbeat").cloned();
                    }
                    if !name.is_empty() {
                        component_versions.insert(name, (ver, st));
                    }
                }
            }
        }
    }

    // Collect URL endpoints keyed by role
    let urls: std::collections::HashMap<String, String> = data
        .get("urls")
        .and_then(|v| v.as_object())
        .map(|m| {
            m.iter()
                .map(|(k, v)| (k.clone(), v.as_str().unwrap_or("-").to_string()))
                .collect()
        })
        .unwrap_or_default();

    // Print unified table
    if let Some(vms) = data.get("vms").and_then(|v| v.as_array()) {
        if !vms.is_empty() {
            println!();
            println!(
                "  {:<14} {:<18} {:<22} {:<12} {}",
                "ROLE", "IP", "VERSION", "STATUS", "E2E"
            );
            println!("  {}", "─".repeat(78));

            for vm in vms {
                let role = vm["role"].as_str().unwrap_or("-");
                let ip = vm["internal_ip"].as_str().unwrap_or("-");
                let vm_status = vm["status"].as_str().unwrap_or("-");

                // Look up version from component data, fall back to image_hash
                let name = vm["name"].as_str().unwrap_or(role);
                let component_ver = component_versions
                    .get(role)
                    .or_else(|| component_versions.get(name))
                    .map(|(v, _)| v.as_str())
                    .filter(|v| *v != "-");
                let version = if let Some(v) = component_ver {
                    v.to_string()
                } else {
                    let raw_hash = vm["image_hash"]
                        .as_str()
                        .or_else(|| {
                            vm.get("metadata")
                                .and_then(|m| m.get("image_hash"))
                                .and_then(|v| v.as_str())
                        })
                        .unwrap_or("-");
                    if raw_hash.len() > 12 && raw_hash != "-" {
                        let h = raw_hash.trim_start_matches("sha256:");
                        format!("{}…", &h[..12.min(h.len())])
                    } else {
                        raw_hash.to_string()
                    }
                };

                // The container being up says nothing about the service being
                // able to do its job. Prefer what the component reports about
                // itself, and for the scheduler let a failing end-to-end
                // heartbeat override both: a probe that cannot round-trip
                // through the sync pipeline means writes are not reaching
                // clients, however healthy the process looks.
                let reported = component_versions
                    .get(role)
                    .or_else(|| component_versions.get(name))
                    .map(|(_, st)| st.as_str())
                    .filter(|st| *st != "-");
                let hb = if role == "scheduler" { heartbeat.as_ref() } else { None };
                let hb_enabled = hb.and_then(|h| h["enabled"].as_bool()).unwrap_or(false);
                let hb_stale = hb.and_then(|h| h["stale"].as_bool()).unwrap_or(false);
                let hb_fails = hb.and_then(|h| h["consecutive_failures"].as_u64()).unwrap_or(0);
                let hb_ms = hb.and_then(|h| h["last_e2e_ms"].as_u64());
                // Blocked: nothing failed, but there is nothing to measure —
                // no ready SSPs means no client is receiving changes, so the
                // last latency describes a stack that is not serving anyone.
                let hb_blocked = hb.and_then(|h| h["blocked"].as_bool()).unwrap_or(false);
                let hb_block_reason = hb.and_then(|h| h["blocked_reason"].as_str());

                let (status_text, health) = if vm_status != "running" {
                    (vm_status.to_string(), vm_status)
                } else if hb_enabled && hb_blocked {
                    (
                        format!("degraded ({})", hb_block_reason.unwrap_or("e2e blocked")),
                        "degraded",
                    )
                } else if hb_enabled && hb_stale {
                    // No successful probe inside the grace window: the pipeline
                    // is not carrying changes, so the scheduler is not serving
                    // its purpose even though it answers HTTP.
                    ("degraded (e2e)".to_string(), "degraded")
                } else if hb_enabled && hb_fails > 0 {
                    (format!("degraded ({hb_fails} failed)"), "degraded")
                } else {
                    (reported.unwrap_or(vm_status).to_string(), vm_status)
                };

                let icon = match health {
                    "running" => "\x1b[32m●\x1b[0m",
                    "starting" => "\x1b[33m◐\x1b[0m",
                    "degraded" => "\x1b[33m▲\x1b[0m",
                    "failed" => "\x1b[31m✗\x1b[0m",
                    "stopped" => "\x1b[90m○\x1b[0m",
                    _ => "\x1b[90m·\x1b[0m",
                };

                // Latency column: only the scheduler measures one. `timeout`
                // rather than a stale number, so a frozen reading can never
                // read as a healthy one.
                let e2e = if !hb_enabled {
                    "-".to_string()
                } else if hb_blocked {
                    "\x1b[33mblocked\x1b[0m".to_string()
                } else if hb_stale {
                    "\x1b[31mtimeout\x1b[0m".to_string()
                } else {
                    match hb_ms {
                        Some(ms) => format!("{ms}ms"),
                        None => "-".to_string(),
                    }
                };

                println!(
                    "  {} {:<13} {:<18} {:<22} {:<12} {}",
                    icon, role, ip, version, status_text, e2e,
                );
            }

            report_version_skew(&component_versions);
        }
    }

    // Print public endpoints (skip internal ssp/scheduler URLs)
    let public_urls: Vec<_> = urls
        .iter()
        .filter(|(name, url)| {
            url.starts_with("http") && !matches!(name.as_str(), "ssp" | "scheduler")
        })
        .collect();
    if !public_urls.is_empty() {
        println!();
        println!("  \x1b[90mEndpoints:\x1b[0m");
        for (name, url) in &public_urls {
            println!("    \x1b[90m{}\x1b[0m {}", name, url);
        }
    }

    // Print spooky status endpoint
    if let Some(db_url) = urls.get("surrealdb") {
        if db_url.starts_with("http") {
            let config_path = std::env::current_dir()
                .unwrap_or_default()
                .join("sp00ky.yml");
            let config = backend::load_config(&config_path);
            let resolved = config.resolved_surrealdb();
            println!(
                "    \x1b[90mstatus\x1b[0m {}/api/{}/{}/spooky",
                db_url, resolved.namespace, resolved.database
            );
        }
    }
}

// ---------------------------------------------------------------------------
// Link (GitHub auto-deploy)
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Custom domains
// ---------------------------------------------------------------------------

pub fn domain(action: CloudDomainCommands) -> Result<()> {
    match action {
        CloudDomainCommands::Add { domain, app, status } => domain_add(domain, app, status),
        CloudDomainCommands::List => domain_list(),
        CloudDomainCommands::Remove { domain } => domain_remove(domain),
    }
}

fn domain_add(domain: String, app: Option<String>, status: bool) -> Result<()> {
    let creds = ensure_login()?;
    let mut client = CloudClient::new(&creds);
    let (_slug, pid) = resolve_project_id(&mut client)?;

    // A status domain attaches to the project's status page, not an app, so the
    // `app` flag is ignored server-side for kind=status.
    let kind = if status { "status" } else { "app" };
    let resp = client.post(
        &format!("/v1/projects/{}/domains", pid),
        &serde_json::json!({ "domain": domain, "app": app.unwrap_or_default(), "kind": kind }),
    )?;
    let data: serde_json::Value = resp.into_json()?;

    println!(
        "Connecting {} → {} ({})",
        data["domain"].as_str().unwrap_or(&domain),
        data["target_name"].as_str().unwrap_or("app"),
        data["target_kind"].as_str().unwrap_or("frontend"),
    );
    println!();
    println!("Add these DNS records at your registrar:");
    if let Some(records) = data["records"].as_array() {
        for rec in records {
            println!(
                "  {:<6} {}  →  {}",
                rec["type"].as_str().unwrap_or("?"),
                rec["name"].as_str().unwrap_or("?"),
                rec["value"].as_str().unwrap_or("?"),
            );
        }
    }
    println!();
    println!(
        "Status: {} (checking automatically — run `spky domain list` to track)",
        data["status"].as_str().unwrap_or("pending_dns")
    );
    Ok(())
}

fn domain_list() -> Result<()> {
    let creds = require_credentials()?;
    let mut client = CloudClient::new(&creds);
    let (_slug, pid) = resolve_project_id(&mut client)?;

    let resp = client.get(&format!("/v1/projects/{}/domains", pid))?;
    let data: Vec<serde_json::Value> = resp.into_json()?;
    if data.is_empty() {
        println!("No custom domains. Connect one with `spky domain add <domain>`.");
        return Ok(());
    }

    println!("{:<34} {:<12} {:<8} {}", "DOMAIN", "STATUS", "→", "TARGET");
    for d in &data {
        let status = d["status"].as_str().unwrap_or("-");
        let dot = match status {
            "active" => "\x1b[32m●\x1b[0m", // green
            "failed" => "\x1b[31m●\x1b[0m", // red
            _ => "\x1b[33m◐\x1b[0m",        // yellow (pending/verifying)
        };
        println!(
            "{:<34} {} {:<10} {} {}",
            d["domain"].as_str().unwrap_or("-"),
            dot,
            status,
            "→",
            d["target_name"].as_str().unwrap_or("-"),
        );
        if let Some(err) = d["verification_error"].as_str() {
            if !err.is_empty() {
                println!("  └ {}", err);
            }
        }
        // Until active, show the DNS records the user still needs to create.
        if status != "active" {
            if let Some(records) = d["validation_records"].as_array() {
                if !records.is_empty() {
                    println!("  Add these DNS records at your registrar:");
                    for rec in records {
                        println!(
                            "    {:<6} {}  →  {}",
                            rec["type"].as_str().unwrap_or("?"),
                            rec["name"].as_str().unwrap_or("?"),
                            rec["value"].as_str().unwrap_or("?"),
                        );
                    }
                }
            }
        }
    }
    Ok(())
}

fn domain_remove(domain: String) -> Result<()> {
    let creds = ensure_login()?;
    let mut client = CloudClient::new(&creds);
    let (_slug, pid) = resolve_project_id(&mut client)?;

    client.delete(&format!("/v1/projects/{}/domains/{}", pid, domain))?;
    println!("Disconnected {}.", domain);
    Ok(())
}

const NOTICE_TYPES: &[&str] = &[
    "investigating",
    "identified",
    "resolved",
    "maintenance",
    "update",
];

pub fn notice(
    message: Option<String>,
    notice_type: String,
    timeout: String,
    action: Option<NoticeCommands>,
) -> Result<()> {
    match action {
        Some(NoticeCommands::List) => notice_list(),
        Some(NoticeCommands::Remove { id }) => notice_remove(id),
        None => {
            let msg = message.ok_or_else(|| {
                anyhow::anyhow!(
                    "Provide a message, e.g. `spky notice \"We are investigating...\"` \
                     (or `spky notice list` / `spky notice remove <id>`)."
                )
            })?;
            notice_post(msg, notice_type, timeout)
        }
    }
}

fn notice_post(message: String, notice_type: String, timeout: String) -> Result<()> {
    let t = notice_type.to_lowercase();
    if !NOTICE_TYPES.contains(&t.as_str()) {
        anyhow::bail!(
            "Invalid --type '{}'. Valid types: {}",
            notice_type,
            NOTICE_TYPES.join(", ")
        );
    }
    let dur = humantime::parse_duration(timeout.trim())
        .map_err(|_| anyhow::anyhow!("Invalid --timeout '{}'. Use durations like 30m, 2h, 7d.", timeout))?;

    let creds = ensure_login()?;
    let mut client = CloudClient::new(&creds);
    let (slug, pid) = resolve_project_id(&mut client)?;

    let resp = client.post(
        &format!("/v1/projects/{}/notices", pid),
        &serde_json::json!({ "type": t, "body": message, "expires_in_seconds": dur.as_secs() }),
    )?;
    let data: serde_json::Value = resp.into_json()?;

    println!("Posted {} notice.", t);
    println!("  id:      {}", data["id"].as_str().unwrap_or("?"));
    println!("  expires: {}", data["expires_at"].as_str().unwrap_or("-"));
    println!("  page:    {}/v1/uptime/{}", api_base_url(), slug);
    Ok(())
}

fn notice_list() -> Result<()> {
    let creds = require_credentials()?;
    let mut client = CloudClient::new(&creds);
    let (_slug, pid) = resolve_project_id(&mut client)?;

    let resp = client.get(&format!("/v1/projects/{}/notices", pid))?;
    let data: Vec<serde_json::Value> = resp.into_json()?;
    if data.is_empty() {
        println!("No active notices. Post one with `spky notice \"<message>\"`.");
        return Ok(());
    }

    println!("{:<38} {:<15} {:<18} {}", "ID", "TYPE", "EXPIRES", "MESSAGE");
    for n in &data {
        let t = n["type"].as_str().unwrap_or("-");
        let dot = match t {
            "resolved" => "\x1b[32m●\x1b[0m",                      // green
            "investigating" | "identified" => "\x1b[33m●\x1b[0m", // yellow
            "maintenance" => "\x1b[34m●\x1b[0m",                   // blue
            _ => "\x1b[90m●\x1b[0m",                               // grey (update)
        };
        let expires = n["expires_at"]
            .as_str()
            .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
            .map(|dt| {
                dt.with_timezone(&chrono::Local)
                    .format("%Y-%m-%d %H:%M")
                    .to_string()
            })
            .unwrap_or_else(|| "-".into());
        println!(
            "{:<38} {} {:<13} {:<18} {}",
            n["id"].as_str().unwrap_or("-"),
            dot,
            t,
            expires,
            n["body"].as_str().unwrap_or("-"),
        );
    }
    Ok(())
}

fn notice_remove(id: String) -> Result<()> {
    let creds = ensure_login()?;
    let mut client = CloudClient::new(&creds);
    let (_slug, pid) = resolve_project_id(&mut client)?;

    client.delete(&format!("/v1/projects/{}/notices/{}", pid, id))?;
    println!("Removed notice {}.", id);
    Ok(())
}

pub fn link(action: CloudLinkCommands) -> Result<()> {
    match action {
        CloudLinkCommands::Setup => link_setup(),
        CloudLinkCommands::Status => link_status(),
        CloudLinkCommands::Settings {
            branch,
            auto_deploy,
            config_path,
        } => link_settings(branch, auto_deploy, config_path),
        CloudLinkCommands::Unlink => link_unlink(),
        CloudLinkCommands::Trigger => link_trigger(),
        CloudLinkCommands::Runs => link_runs(),
    }
}

/// Detect the current git branch (e.g. "develop"). Returns None outside a repo
/// or in detached-HEAD state.
fn detect_git_branch() -> Option<String> {
    let out = std::process::Command::new("git")
        .args(["rev-parse", "--abbrev-ref", "HEAD"])
        .output()
        .ok()?;
    if !out.status.success() {
        return None;
    }
    let b = String::from_utf8_lossy(&out.stdout).trim().to_string();
    if b.is_empty() || b == "HEAD" {
        None
    } else {
        Some(b)
    }
}

/// Git repo top-level directory, falling back to the current directory.
fn git_repo_root() -> PathBuf {
    if let Ok(out) = std::process::Command::new("git")
        .args(["rev-parse", "--show-toplevel"])
        .output()
    {
        if out.status.success() {
            let p = String::from_utf8_lossy(&out.stdout).trim().to_string();
            if !p.is_empty() {
                return PathBuf::from(p);
            }
        }
    }
    std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."))
}

/// Scan the repo for sp00ky.yml/.yaml manifests, returning paths relative to
/// `root` (forward-slashed). Skips common noise directories.
fn find_manifest_candidates(root: &std::path::Path) -> Vec<String> {
    fn walk(dir: &std::path::Path, root: &std::path::Path, out: &mut Vec<String>, depth: usize) {
        if depth > 6 {
            return;
        }
        let entries = match fs::read_dir(dir) {
            Ok(e) => e,
            Err(_) => return,
        };
        for entry in entries.flatten() {
            let path = entry.path();
            let name = entry.file_name().to_string_lossy().to_string();
            if path.is_dir() {
                if matches!(
                    name.as_str(),
                    ".git"
                        | "node_modules"
                        | "target"
                        | "dist"
                        | "build"
                        | ".next"
                        | "vendor"
                        | ".sp00ky"
                ) {
                    continue;
                }
                walk(&path, root, out, depth + 1);
            } else if name == "sp00ky.yml" || name == "sp00ky.yaml" {
                if let Ok(rel) = path.strip_prefix(root) {
                    out.push(rel.to_string_lossy().replace('\\', "/"));
                }
            }
        }
    }
    let mut out = Vec::new();
    walk(root, root, &mut out, 0);
    out.sort();
    out
}

/// Autodetect branch + manifest path for `link setup`. Auto-accepts detected
/// values; only prompts when ambiguous (multiple manifests) or detection fails.
fn autodetect_link_settings() -> (String, String) {
    let root = git_repo_root();

    let branch = match detect_git_branch() {
        Some(b) => b,
        None => {
            if is_interactive() {
                inquire::Text::new("Branch to deploy from:")
                    .with_default("main")
                    .prompt()
                    .unwrap_or_else(|_| "main".to_string())
            } else {
                "main".to_string()
            }
        }
    };

    let candidates = find_manifest_candidates(&root);
    let config_path = match candidates.len() {
        1 => candidates[0].clone(),
        0 => {
            if is_interactive() {
                inquire::Text::new("Path to sp00ky.yml:")
                    .with_default("sp00ky.yml")
                    .prompt()
                    .unwrap_or_else(|_| "sp00ky.yml".to_string())
            } else {
                "sp00ky.yml".to_string()
            }
        }
        _ => {
            if is_interactive() {
                inquire::Select::new("Multiple sp00ky.yml found — pick one:", candidates.clone())
                    .prompt()
                    .unwrap_or_else(|_| candidates[0].clone())
            } else {
                candidates[0].clone()
            }
        }
    };

    (branch, config_path)
}

fn link_setup() -> Result<()> {
    let creds = ensure_login()?;
    let mut client = CloudClient::new(&creds);
    let (_slug, pid) = resolve_project_id(&mut client)?;

    // Initiate setup
    let resp = client.post(
        &format!("/v1/projects/{}/link/setup", pid),
        &serde_json::json!({}),
    )?;
    let data: serde_json::Value = resp.into_json()?;
    let install_url = data["install_url"]
        .as_str()
        .context("Missing install_url in response")?;

    println!("Opening GitHub to install the Sp00ky Cloud app...");
    println!("  {}", install_url);
    let _ = open::that(install_url);

    // Poll until linked
    println!();
    println!("Waiting for GitHub App installation...");

    let mut attempts = 0;
    let max_attempts = 120; // 6 minutes at 3s intervals

    loop {
        thread::sleep(Duration::from_secs(3));
        attempts += 1;

        if attempts > max_attempts {
            bail!("Timed out waiting for GitHub App installation. Try again with `spky link connect`.");
        }

        let resp = client.get(&format!("/v1/projects/{}/link", pid))?;
        let data: serde_json::Value = resp.into_json()?;
        let status = data["status"].as_str().unwrap_or("");

        match status {
            "linked" => {
                let repo_owner = data["repo_owner"].as_str().unwrap_or("").to_string();
                let repo_name = data["repo_name"].as_str().unwrap_or("").to_string();

                // Repo was auto-linked with defaults — apply the detected branch
                // and manifest path.
                let (branch, config_path) = autodetect_link_settings();
                client.patch(
                    &format!("/v1/projects/{}/link", pid),
                    &serde_json::json!({ "branch": branch, "config_path": config_path }),
                )?;

                println!(
                    "Linked to {}/{} (branch '{}', manifest '{}')",
                    repo_owner, repo_name, branch, config_path
                );
                println!(
                    "  Auto-deploy: {}",
                    if data["auto_deploy"].as_bool().unwrap_or(true) {
                        "enabled"
                    } else {
                        "disabled"
                    }
                );

                // Offer to deploy now
                if is_interactive() {
                    let deploy_now = inquire::Confirm::new("Deploy now?")
                        .with_default(true)
                        .prompt()
                        .unwrap_or(false);

                    if deploy_now {
                        let resp = client.post(
                            &format!("/v1/projects/{}/link/trigger", pid),
                            &serde_json::json!({}),
                        )?;
                        let run: serde_json::Value = resp.into_json()?;
                        println!(
                            "Build triggered (run: {})",
                            run["run_id"].as_str().unwrap_or("?")
                        );
                        println!("  Commit: {}", run["commit_sha"].as_str().unwrap_or("?"));
                    }
                }
                return Ok(());
            }
            "pending_repo_selection" => {
                // Installation complete but multiple repos — user needs to pick one
                if let Some(repos) = data["repos"].as_array() {
                    if repos.is_empty() {
                        continue;
                    }

                    let repo_names: Vec<String> = repos
                        .iter()
                        .map(|r| r["full_name"].as_str().unwrap_or("?").to_string())
                        .collect();

                    println!();
                    let selection = inquire::Select::new("Select repository:", repo_names)
                        .prompt()
                        .context("Failed to select repository")?;

                    // Parse owner/name
                    let parts: Vec<&str> = selection.split('/').collect();
                    if parts.len() != 2 {
                        bail!("Invalid repository format");
                    }

                    let (branch, config_path) = autodetect_link_settings();
                    client.patch(
                        &format!("/v1/projects/{}/link", pid),
                        &serde_json::json!({
                            "repo_owner": parts[0],
                            "repo_name": parts[1],
                            "branch": branch,
                            "config_path": config_path,
                        }),
                    )?;

                    println!(
                        "Linked to {} (branch '{}', manifest '{}')",
                        selection, branch, config_path
                    );

                    // Offer to deploy
                    if is_interactive() {
                        let deploy_now = inquire::Confirm::new("Deploy now?")
                            .with_default(true)
                            .prompt()
                            .unwrap_or(false);

                        if deploy_now {
                            let resp = client.post(
                                &format!("/v1/projects/{}/link/trigger", pid),
                                &serde_json::json!({}),
                            )?;
                            let run: serde_json::Value = resp.into_json()?;
                            println!(
                                "Build triggered (run: {})",
                                run["run_id"].as_str().unwrap_or("?")
                            );
                        }
                    }
                    return Ok(());
                }
            }
            "not_linked" => {
                // Still waiting
                if attempts % 10 == 0 {
                    print!(".");
                }
            }
            _ => {}
        }
    }
}

fn link_status() -> Result<()> {
    let creds = require_credentials()?;
    let mut client = CloudClient::new(&creds);
    let (slug, pid) = resolve_project_id(&mut client)?;

    let resp = client.get(&format!("/v1/projects/{}/link", pid))?;
    let data: serde_json::Value = resp.into_json()?;
    let status = data["status"].as_str().unwrap_or("unknown");

    match status {
        "linked" => {
            println!("Project: {}", slug);
            println!(
                "Repository: {}/{}",
                data["repo_owner"].as_str().unwrap_or("?"),
                data["repo_name"].as_str().unwrap_or("?")
            );
            println!("Branch: {}", data["branch"].as_str().unwrap_or("?"));
            println!(
                "Auto-deploy: {}",
                if data["auto_deploy"].as_bool().unwrap_or(true) {
                    "enabled"
                } else {
                    "disabled"
                }
            );

            if let Some(runs) = data["runs"].as_array() {
                if !runs.is_empty() {
                    println!();
                    println!("Recent runs:");
                    println!(
                        "  {:<10} {:<12} {:<20} {}",
                        "STATUS", "COMMIT", "TIME", "MESSAGE"
                    );
                    println!("  {}", "-".repeat(60));
                    for run in runs {
                        let sha = run["commit_sha"].as_str().unwrap_or("?");
                        let short_sha = if sha.len() > 8 { &sha[..8] } else { sha };
                        println!(
                            "  {:<10} {:<12} {:<20} {}",
                            run["status"].as_str().unwrap_or("?"),
                            short_sha,
                            run["created_at"]
                                .as_str()
                                .unwrap_or("?")
                                .get(..19)
                                .unwrap_or("?"),
                            run["commit_message"]
                                .as_str()
                                .unwrap_or("")
                                .lines()
                                .next()
                                .unwrap_or(""),
                        );
                    }
                }
            }
        }
        "not_linked" => {
            println!("Project '{}' is not linked to a GitHub repository.", slug);
            println!("Run `spky link connect` to set up automated deployments.");
        }
        _ => {
            println!("Link status: {}", status);
        }
    }

    Ok(())
}

fn link_settings(
    branch: Option<String>,
    auto_deploy: Option<bool>,
    config_path: Option<String>,
) -> Result<()> {
    let creds = require_credentials()?;
    let mut client = CloudClient::new(&creds);
    let (_slug, pid) = resolve_project_id(&mut client)?;

    // If no flags provided, use interactive prompts
    let (branch, auto_deploy, config_path) =
        if branch.is_none() && auto_deploy.is_none() && config_path.is_none() && is_interactive() {
            // Get current settings first
            let resp = client.get(&format!("/v1/projects/{}/link", pid))?;
            let data: serde_json::Value = resp.into_json()?;
            if data["status"].as_str() != Some("linked") {
                bail!("Project is not linked. Run `spky link connect` first.");
            }

            let current_branch = data["branch"].as_str().unwrap_or("main").to_string();
            let current_auto = data["auto_deploy"].as_bool().unwrap_or(true);
            let current_config = data["config_path"]
                .as_str()
                .unwrap_or("sp00ky.yml")
                .to_string();

            let new_branch = inquire::Text::new("Branch:")
                .with_default(&current_branch)
                .prompt()
                .context("Failed to read branch")?;

            let new_config = inquire::Text::new("Path to sp00ky.yml:")
                .with_default(&current_config)
                .prompt()
                .context("Failed to read manifest path")?;

            let new_auto = inquire::Confirm::new("Auto-deploy on push?")
                .with_default(current_auto)
                .prompt()
                .context("Failed to read auto-deploy setting")?;

            (Some(new_branch), Some(new_auto), Some(new_config))
        } else {
            (branch, auto_deploy, config_path)
        };

    let mut body = serde_json::Map::new();
    if let Some(b) = &branch {
        body.insert("branch".to_string(), serde_json::Value::String(b.clone()));
    }
    if let Some(a) = auto_deploy {
        body.insert("auto_deploy".to_string(), serde_json::Value::Bool(a));
    }
    if let Some(c) = &config_path {
        body.insert(
            "config_path".to_string(),
            serde_json::Value::String(c.clone()),
        );
    }

    client.patch(
        &format!("/v1/projects/{}/link", pid),
        &serde_json::Value::Object(body),
    )?;

    println!("Settings updated.");
    if let Some(b) = &branch {
        println!("  Branch: {}", b);
    }
    if let Some(c) = &config_path {
        println!("  Manifest: {}", c);
    }
    if let Some(a) = auto_deploy {
        println!("  Auto-deploy: {}", if a { "enabled" } else { "disabled" });
    }

    Ok(())
}

fn link_unlink() -> Result<()> {
    let creds = require_credentials()?;
    let mut client = CloudClient::new(&creds);
    let (slug, pid) = resolve_project_id(&mut client)?;

    if is_interactive() {
        let confirm = inquire::Confirm::new(&format!(
            "Unlink project '{}'? This will stop automated deployments.",
            slug
        ))
        .with_default(false)
        .prompt()
        .context("Failed to read confirmation")?;

        if !confirm {
            println!("Cancelled.");
            return Ok(());
        }
    }

    client.delete(&format!("/v1/projects/{}/link", pid))?;
    println!("Project '{}' unlinked from GitHub.", slug);

    Ok(())
}

fn link_trigger() -> Result<()> {
    let creds = require_credentials()?;
    let mut client = CloudClient::new(&creds);
    let (slug, pid) = resolve_project_id(&mut client)?;

    let resp = client.post(
        &format!("/v1/projects/{}/link/trigger", pid),
        &serde_json::json!({}),
    )?;
    let data: serde_json::Value = resp.into_json()?;

    println!("Build triggered for '{}'", slug);
    println!("  Run ID: {}", data["run_id"].as_str().unwrap_or("?"));
    println!("  Commit: {}", data["commit_sha"].as_str().unwrap_or("?"));
    println!("  Status: {}", data["status"].as_str().unwrap_or("pending"));

    Ok(())
}

fn link_runs() -> Result<()> {
    let creds = require_credentials()?;
    let mut client = CloudClient::new(&creds);
    let (_slug, pid) = resolve_project_id(&mut client)?;

    let resp = client.get(&format!("/v1/projects/{}/link/runs", pid))?;
    let runs: Vec<serde_json::Value> = resp.into_json()?;

    if runs.is_empty() {
        println!(
            "No build runs found. Push to the linked branch or run `spky link trigger`."
        );
        return Ok(());
    }

    println!(
        "{:<10} {:<12} {:<20} {}",
        "STATUS", "COMMIT", "TIME", "MESSAGE"
    );
    println!("{}", "-".repeat(70));

    for run in &runs {
        let sha = run["commit_sha"].as_str().unwrap_or("?");
        let short_sha = if sha.len() > 8 { &sha[..8] } else { sha };
        let time = run["created_at"].as_str().unwrap_or("?");
        let short_time = time.get(..19).unwrap_or(time);
        let msg = run["commit_message"]
            .as_str()
            .unwrap_or("")
            .lines()
            .next()
            .unwrap_or("");
        let msg_truncated = if msg.len() > 40 { &msg[..37] } else { msg };

        println!(
            "{:<10} {:<12} {:<20} {}",
            run["status"].as_str().unwrap_or("?"),
            short_sha,
            short_time,
            msg_truncated,
        );
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Environment Variables
// ---------------------------------------------------------------------------

const VAULT_DERIVED_KEY_FILE: &str = "vault-derived-key";
const VAULT_PASSPHRASE_FILE_LEGACY: &str = "vault-passphrase";

fn vault_derived_key_path() -> PathBuf {
    dirs::home_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join(SP00KY_DIR)
        .join(VAULT_DERIVED_KEY_FILE)
}

fn vault_passphrase_path_legacy() -> PathBuf {
    dirs::home_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join(SP00KY_DIR)
        .join(VAULT_PASSPHRASE_FILE_LEGACY)
}

fn load_cached_derived_key() -> Option<String> {
    fs::read_to_string(vault_derived_key_path())
        .ok()
        .map(|s| s.trim().to_string())
}

fn save_cached_derived_key(hex_key: &str) -> Result<()> {
    let path = vault_derived_key_path();
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(&path, hex_key)?;
    Ok(())
}

fn prompt_passphrase(label: &str) -> Result<String> {
    inquire::Password::new(label)
        .with_display_mode(inquire::PasswordDisplayMode::Masked)
        .without_confirmation()
        .prompt()
        .context("Failed to read passphrase")
}

fn derive_key(passphrase: &str, salt: &[u8]) -> Result<String> {
    let params = Params::new(65536, 1, 4, Some(32))
        .map_err(|e| anyhow::anyhow!("Invalid Argon2 params: {}", e))?;
    let argon2 = Argon2::new(Algorithm::Argon2id, Version::V0x13, params);
    let mut output = [0u8; 32];
    argon2
        .hash_password_into(passphrase.as_bytes(), salt, &mut output)
        .map_err(|e| anyhow::anyhow!("Argon2 derivation failed: {}", e))?;
    Ok(hex::encode(output))
}

fn generate_salt() -> String {
    let mut salt = [0u8; 16];
    rand::thread_rng().fill_bytes(&mut salt);
    hex::encode(salt)
}

fn fetch_key_params(client: &mut CloudClient) -> Result<String> {
    let resp = client.get("/v1/vault/key-params")?;
    let data: serde_json::Value = resp.into_json()?;
    let salt_hex = data["salt"]
        .as_str()
        .context("No salt in key-params response")?;
    Ok(salt_hex.to_string())
}

/// Get the vault derived key: cached file > migrate legacy > prompt + derive.
fn get_derived_key(client: &mut CloudClient) -> Result<String> {
    // 1. Check for cached derived key
    if let Some(dk) = load_cached_derived_key() {
        return Ok(dk);
    }

    // 2. Auto-migrate legacy passphrase file
    let legacy_path = vault_passphrase_path_legacy();
    if let Ok(passphrase) = fs::read_to_string(&legacy_path) {
        let passphrase = passphrase.trim().to_string();
        if !passphrase.is_empty() {
            println!("Migrating cached passphrase to derived key...");
            let salt_hex = fetch_key_params(client)?;
            let salt = hex::decode(&salt_hex).context("Invalid salt hex")?;
            let dk = derive_key(&passphrase, &salt)?;
            save_cached_derived_key(&dk)?;
            let _ = fs::remove_file(&legacy_path);
            println!("Done. Cached derived key saved to ~/.sp00ky/vault-derived-key");
            return Ok(dk);
        }
    }

    // 3. Prompt for passphrase and derive
    let pp = prompt_passphrase("Vault passphrase:")?;
    println!("Deriving key (this may take a moment)...");
    let salt_hex = fetch_key_params(client)?;
    let salt = hex::decode(&salt_hex).context("Invalid salt hex")?;
    let dk = derive_key(&pp, &salt)?;

    if is_interactive() {
        let cache = inquire::Confirm::new("Cache derived key locally in ~/.sp00ky/?")
            .with_default(true)
            .with_help_message("Saves to ~/.sp00ky/vault-derived-key so you don't need to enter your passphrase every time")
            .prompt()
            .unwrap_or(false);
        if cache {
            save_cached_derived_key(&dk)?;
        }
    }
    Ok(dk)
}

fn ensure_vault(client: &mut CloudClient) -> Result<()> {
    let resp = client.get("/v1/vault/status")?;
    let data: serde_json::Value = resp.into_json()?;
    if data["initialized"].as_bool() == Some(true) {
        return Ok(());
    }

    if !is_interactive() {
        bail!("Vault not initialized. Run `spky env unlock` first.");
    }

    println!("Your encryption vault is not set up yet.");
    println!("Choose a passphrase to protect your environment variable secrets.");
    println!();

    let passphrase = inquire::Password::new("New vault passphrase:")
        .with_display_mode(inquire::PasswordDisplayMode::Masked)
        .prompt()
        .context("Failed to read passphrase")?;

    if passphrase.is_empty() {
        bail!("Passphrase cannot be empty.");
    }

    let confirm = inquire::Password::new("Confirm passphrase:")
        .with_display_mode(inquire::PasswordDisplayMode::Masked)
        .without_confirmation()
        .prompt()
        .context("Failed to read confirmation")?;

    if passphrase != confirm {
        bail!("Passphrases do not match.");
    }

    let salt_hex = generate_salt();
    let salt = hex::decode(&salt_hex).context("Invalid salt")?;
    println!("Deriving encryption key (this may take a moment)...");
    let dk = derive_key(&passphrase, &salt)?;
    client.post(
        "/v1/vault/init",
        &serde_json::json!({ "derived_key": dk, "salt": salt_hex }),
    )?;
    println!("Vault initialized.");

    let cache = inquire::Confirm::new("Cache derived key locally in ~/.sp00ky/?")
        .with_default(true)
        .prompt()
        .unwrap_or(false);
    if cache {
        save_cached_derived_key(&dk)?;
    }

    Ok(())
}

fn env_init() -> Result<()> {
    let creds = ensure_login()?;
    let mut client = CloudClient::new(&creds);

    let resp = client.get("/v1/vault/status")?;
    let data: serde_json::Value = resp.into_json()?;
    if data["initialized"].as_bool() == Some(true) {
        println!("Vault is already initialized.");
        return Ok(());
    }

    println!("Choose a passphrase to protect your environment variable secrets.");
    println!("This is separate from your GitHub login.");
    println!();

    let passphrase = inquire::Password::new("New vault passphrase:")
        .with_display_mode(inquire::PasswordDisplayMode::Masked)
        .prompt()
        .context("Failed to read passphrase")?;

    if passphrase.is_empty() {
        bail!("Passphrase cannot be empty.");
    }

    let confirm = inquire::Password::new("Confirm passphrase:")
        .with_display_mode(inquire::PasswordDisplayMode::Masked)
        .without_confirmation()
        .prompt()
        .context("Failed to read confirmation")?;

    if passphrase != confirm {
        bail!("Passphrases do not match.");
    }

    let salt_hex = generate_salt();
    let salt = hex::decode(&salt_hex).context("Invalid salt")?;
    println!("Deriving encryption key (this may take a moment)...");
    let dk = derive_key(&passphrase, &salt)?;
    client.post(
        "/v1/vault/init",
        &serde_json::json!({ "derived_key": dk, "salt": salt_hex }),
    )?;
    println!("Vault initialized successfully.");

    let cache = inquire::Confirm::new("Cache derived key locally in ~/.sp00ky/?")
        .with_default(true)
        .with_help_message("So you don't need to enter it every time you load env variables")
        .prompt()
        .unwrap_or(false);
    if cache {
        save_cached_derived_key(&dk)?;
        println!("Derived key cached at ~/.sp00ky/vault-derived-key");
    }

    Ok(())
}

fn env_set(
    name_arg: Option<String>,
    file: Option<String>,
    dev_only: bool,
    prod_only: bool,
) -> Result<()> {
    let creds = ensure_login()?;
    let mut client = CloudClient::new(&creds);

    ensure_vault(&mut client)?;

    let (slug, project) = ensure_project(&mut client)?;
    let pid = project_id(&project);

    let name = match name_arg {
        Some(n) => n.to_uppercase(),
        None => {
            if file.is_some() {
                bail!("Provide the variable name with --file, e.g. `spky cloud env set JWT_PRIVATE_KEY --file key.pem`.");
            }
            inquire::Text::new("Variable name:")
                .with_help_message("e.g. DATABASE_URL, STRIPE_KEY")
                .prompt()
                .context("Failed to read variable name")?
                .to_uppercase()
        }
    };

    if name.is_empty() {
        bail!("Variable name cannot be empty.");
    }

    // --file: read the value from a file (multi-line safe, no prompt). Target
    // environment is chosen by --dev / --prod (default: both with the same value).
    if let Some(path) = file {
        let raw = std::fs::read_to_string(&path)
            .with_context(|| format!("Could not read value file: {}", path))?;
        let val = raw.trim_end_matches(['\n', '\r']).to_string();
        if val.is_empty() {
            bail!("Value file {} is empty.", path);
        }
        let (dev_value, prod_value, both) = match (dev_only, prod_only) {
            (true, false) => (Some(val), None, false),
            (false, true) => (None, Some(val), false),
            _ => (Some(val), None, true), // both flags, or neither → both, same value
        };
        let mut body = serde_json::json!({ "both": both });
        if let Some(ref v) = dev_value {
            body["dev_value"] = serde_json::Value::String(v.clone());
        }
        if let Some(ref v) = prod_value {
            body["prod_value"] = serde_json::Value::String(v.clone());
        }
        client.put(&format!("/v1/projects/{}/envs/{}", pid, name), &body)?;
        let scope = if both {
            "development + production"
        } else if dev_only {
            "development"
        } else {
            "production"
        };
        println!(
            "Set {} ({}) for project '{}' from {}.",
            name, scope, slug, path
        );
        return Ok(());
    }

    // Ask which environments
    let env_choice = inquire::Select::new(
        "Which environment(s)?",
        vec![
            "Both (same value)",
            "Development only",
            "Production only",
            "Both (different values)",
        ],
    )
    .prompt()
    .context("Failed to read environment choice")?;

    let (dev_value, prod_value, both) = match env_choice {
        "Both (same value)" => {
            let val = inquire::Password::new(&format!("Value for {}:", name))
                .with_display_mode(inquire::PasswordDisplayMode::Masked)
                .without_confirmation()
                .prompt()
                .context("Failed to read value")?;
            (Some(val), None, true)
        }
        "Development only" => {
            let val = inquire::Password::new(&format!("Development value for {}:", name))
                .with_display_mode(inquire::PasswordDisplayMode::Masked)
                .without_confirmation()
                .prompt()
                .context("Failed to read value")?;
            (Some(val), None, false)
        }
        "Production only" => {
            let val = inquire::Password::new(&format!("Production value for {}:", name))
                .with_display_mode(inquire::PasswordDisplayMode::Masked)
                .without_confirmation()
                .prompt()
                .context("Failed to read value")?;
            (None, Some(val), false)
        }
        "Both (different values)" => {
            let dev = inquire::Password::new(&format!("Development value for {}:", name))
                .with_display_mode(inquire::PasswordDisplayMode::Masked)
                .without_confirmation()
                .prompt()
                .context("Failed to read dev value")?;
            let prod = inquire::Password::new(&format!("Production value for {}:", name))
                .with_display_mode(inquire::PasswordDisplayMode::Masked)
                .without_confirmation()
                .prompt()
                .context("Failed to read prod value")?;
            (Some(dev), Some(prod), false)
        }
        _ => unreachable!(),
    };

    let mut body = serde_json::json!({ "both": both });
    if let Some(ref v) = dev_value {
        body["dev_value"] = serde_json::Value::String(v.clone());
    }
    if let Some(ref v) = prod_value {
        body["prod_value"] = serde_json::Value::String(v.clone());
    }

    client.put(&format!("/v1/projects/{}/envs/{}", pid, name), &body)?;
    println!("Set {} for project '{}'.", name, slug);

    Ok(())
}

fn parse_env_file(path: &str) -> Result<Vec<(String, String)>> {
    let content = std::fs::read_to_string(path)
        .with_context(|| format!("Could not read env file: {}", path))?;
    let pairs: Vec<(String, String)> = content
        .lines()
        .filter(|l| {
            let trimmed = l.trim();
            !trimmed.is_empty() && !trimmed.starts_with('#')
        })
        .filter_map(|l| {
            let (key, value) = l.split_once('=')?;
            let key = key.trim().to_string();
            let mut value = value.trim().to_string();
            // Strip surrounding quotes
            if (value.starts_with('"') && value.ends_with('"'))
                || (value.starts_with('\'') && value.ends_with('\''))
            {
                value = value[1..value.len() - 1].to_string();
            }
            Some((key, value))
        })
        .collect();
    Ok(pairs)
}

fn env_import(file: String) -> Result<()> {
    let pairs = parse_env_file(&file)?;
    if pairs.is_empty() {
        bail!("No KEY=VALUE pairs found in '{}'.", file);
    }

    println!("Found {} variable(s) in '{}':", pairs.len(), file);
    for (key, _) in &pairs {
        println!("  {}", key);
    }
    println!();

    let creds = ensure_login()?;
    let mut client = CloudClient::new(&creds);
    ensure_vault(&mut client)?;
    let (slug, project) = ensure_project(&mut client)?;
    let pid = project_id(&project);

    let env_choice = inquire::Select::new(
        "Import these variables to which environment(s)?",
        vec!["Both (same value)", "Development only", "Production only"],
    )
    .prompt()
    .context("Failed to read environment choice")?;

    let confirm = inquire::Confirm::new(&format!(
        "Import {} variables to project '{}'?",
        pairs.len(),
        slug
    ))
    .with_default(true)
    .prompt()
    .unwrap_or(false);

    if !confirm {
        println!("Import cancelled.");
        return Ok(());
    }

    let total = pairs.len();
    let mut success = 0;
    let mut failed = 0;

    for (i, (key, value)) in pairs.iter().enumerate() {
        let name = key.to_uppercase();
        let body = match env_choice {
            "Both (same value)" => {
                serde_json::json!({ "both": true, "dev_value": value })
            }
            "Development only" => {
                serde_json::json!({ "both": false, "dev_value": value })
            }
            "Production only" => {
                serde_json::json!({ "both": false, "prod_value": value })
            }
            _ => unreachable!(),
        };

        print!("[{}/{}] Setting {}... ", i + 1, total, name);
        match client.put(&format!("/v1/projects/{}/envs/{}", pid, name), &body) {
            Ok(_) => {
                println!("ok");
                success += 1;
            }
            Err(e) => {
                println!("FAILED: {}", e);
                failed += 1;
            }
        }
    }

    println!();
    println!("Import complete: {} succeeded, {} failed.", success, failed);
    if failed > 0 {
        bail!("{} variable(s) failed to import.", failed);
    }

    Ok(())
}

fn env_list() -> Result<()> {
    let creds = ensure_login()?;
    let mut client = CloudClient::new(&creds);
    let (_slug, project) = ensure_project(&mut client)?;
    let pid = project_id(&project);

    let resp = client.get(&format!("/v1/projects/{}/envs", pid))?;
    let data: Vec<serde_json::Value> = resp.into_json()?;

    if data.is_empty() {
        println!("No environment variables set. Use `spky env set` to add one.");
        return Ok(());
    }

    println!("{:<30} {:<6} {:<6} {}", "NAME", "DEV", "PROD", "UPDATED");
    println!("{}", "-".repeat(65));
    for var in &data {
        let dev = if var["has_dev"].as_bool() == Some(true) {
            "yes"
        } else {
            "-"
        };
        let prod = if var["has_prod"].as_bool() == Some(true) {
            "yes"
        } else {
            "-"
        };
        let updated = var["updated_at"].as_str().unwrap_or("-");
        let short_time = updated.get(..19).unwrap_or(updated);
        println!(
            "{:<30} {:<6} {:<6} {}",
            var["name"].as_str().unwrap_or("-"),
            dev,
            prod,
            short_time,
        );
    }

    Ok(())
}

fn env_load(prod: bool) -> Result<()> {
    let creds = ensure_login()?;
    let mut client = CloudClient::new(&creds);
    let (_slug, project) = ensure_project(&mut client)?;
    let pid = project_id(&project);

    let derived_key = get_derived_key(&mut client)?;
    let environment = if prod { "production" } else { "development" };

    let resp = client.post(
        &format!("/v1/projects/{}/envs/load", pid),
        &serde_json::json!({
            "derived_key": derived_key,
            "environment": environment,
        }),
    )?;
    let data: serde_json::Value = resp.into_json()?;

    if let Some(vars) = data["variables"].as_object() {
        if vars.is_empty() {
            eprintln!("No {} variables found.", environment);
            return Ok(());
        }
        for (key, value) in vars {
            if let Some(val) = value.as_str() {
                println!("{}={}", key, val);
            }
        }
    }

    Ok(())
}

fn env_delete(name: String) -> Result<()> {
    let creds = ensure_login()?;
    let mut client = CloudClient::new(&creds);
    let (slug, project) = ensure_project(&mut client)?;
    let pid = project_id(&project);

    client.delete(&format!(
        "/v1/projects/{}/envs/{}",
        pid,
        name.to_uppercase()
    ))?;
    println!("Deleted {} from project '{}'.", name.to_uppercase(), slug);

    Ok(())
}

// env_ci_access manages CI/CD vault access: it lets `git push` deploys read
// production secrets from the vault. Enabling unwraps the vault key with the
// passphrase and hands the server a transit-encrypted copy for the build
// pipeline (the server can't otherwise decrypt the end-to-end-encrypted vault).
fn env_ci_access(disable: bool, status: bool) -> Result<()> {
    let creds = ensure_login()?;
    let mut client = CloudClient::new(&creds);

    if status {
        let resp = client.get("/v1/vault/ci-access")?;
        let data: serde_json::Value = resp.into_json()?;
        if data["enabled"].as_bool() == Some(true) {
            println!("CI/CD vault access is ENABLED — push deploys can read production secrets.");
        } else {
            println!(
                "CI/CD vault access is DISABLED. Enable it with `spky env share-ci`."
            );
        }
        return Ok(());
    }

    if disable {
        client.delete("/v1/vault/ci-access")?;
        println!("CI/CD vault access disabled. Push deploys will no longer inject vault secrets.");
        return Ok(());
    }

    // Enable: derive the key from the passphrase, then grant the server a
    // transit-encrypted copy of the vault key for the build pipeline.
    let derived_key = get_derived_key(&mut client)?;
    client.post(
        "/v1/vault/ci-access",
        &serde_json::json!({ "derived_key": derived_key }),
    )?;
    println!("CI/CD vault access enabled. Push deploys will now inject production secrets from the vault.");
    println!("Reference them in sp00ky.yml, e.g. `env: {{ cloud: {{ vault: [KEY1, KEY2] }} }}`.");

    Ok(())
}

fn env_change_passphrase() -> Result<()> {
    let creds = ensure_login()?;
    let mut client = CloudClient::new(&creds);

    let current = prompt_passphrase("Current passphrase:")?;
    let salt_hex = fetch_key_params(&mut client)?;
    let current_dk = derive_key(&current, &hex::decode(&salt_hex)?)?;

    let new_pp = inquire::Password::new("New passphrase:")
        .with_display_mode(inquire::PasswordDisplayMode::Masked)
        .prompt()
        .context("Failed to read new passphrase")?;

    if new_pp.is_empty() {
        bail!("Passphrase cannot be empty.");
    }

    let confirm = inquire::Password::new("Confirm new passphrase:")
        .with_display_mode(inquire::PasswordDisplayMode::Masked)
        .without_confirmation()
        .prompt()
        .context("Failed to read confirmation")?;

    if new_pp != confirm {
        bail!("Passphrases do not match.");
    }

    let new_salt_hex = generate_salt();
    let new_dk = derive_key(&new_pp, &hex::decode(&new_salt_hex)?)?;

    client.post(
        "/v1/vault/change-passphrase",
        &serde_json::json!({
            "current_derived_key": current_dk,
            "new_derived_key": new_dk,
            "new_salt": new_salt_hex,
        }),
    )?;

    // Update cached derived key
    if vault_derived_key_path().exists() {
        save_cached_derived_key(&new_dk)?;
        println!("Passphrase changed. Local cache updated.");
    } else {
        println!("Passphrase changed.");
    }

    Ok(())
}

#[cfg(test)]
mod app_release_tests {
    use super::*;

    fn frontend_app(yaml: &str) -> backend::AppConfig {
        serde_yaml::from_str(yaml).expect("valid app config yaml")
    }

    #[test]
    fn version_read_from_deploy_context_package_json() {
        let dir = tempfile::tempdir().unwrap();
        let app_dir = dir.path().join("webapp");
        std::fs::create_dir_all(&app_dir).unwrap();
        std::fs::write(app_dir.join("package.json"), r#"{"version":"1.2.3"}"#).unwrap();
        let app = frontend_app("type: frontend\ndeploy:\n  context: ./webapp\n");
        assert_eq!(
            frontend_package_version(dir.path(), &app),
            Some("1.2.3".to_string())
        );
    }

    #[test]
    fn version_falls_back_to_dev_workdir() {
        let dir = tempfile::tempdir().unwrap();
        let app_dir = dir.path().join("devapp");
        std::fs::create_dir_all(&app_dir).unwrap();
        std::fs::write(app_dir.join("package.json"), r#"{"version":"0.9.0"}"#).unwrap();
        let app =
            frontend_app("type: frontend\ndev:\n  type: npm\n  script: dev\n  workdir: ./devapp\n");
        assert_eq!(
            frontend_package_version(dir.path(), &app),
            Some("0.9.0".to_string())
        );
    }

    #[test]
    fn missing_or_unversioned_package_json_is_none() {
        let dir = tempfile::tempdir().unwrap();
        let app = frontend_app("type: frontend\ndeploy:\n  context: ./nope\n");
        assert_eq!(frontend_package_version(dir.path(), &app), None);
        let app_dir = dir.path().join("empty");
        std::fs::create_dir_all(&app_dir).unwrap();
        std::fs::write(app_dir.join("package.json"), r#"{"name":"x"}"#).unwrap();
        let app = frontend_app("type: frontend\ndeploy:\n  context: ./empty\n");
        assert_eq!(frontend_package_version(dir.path(), &app), None);
    }
}

#[cfg(test)]
mod secret_value_tests {
    use super::*;

    #[test]
    fn literal_resolves_to_itself() {
        let vault = std::collections::HashMap::new();
        let v = backend::SecretValue::Literal("hunter2".to_string());
        assert_eq!(resolve_secret_value(&v, &vault).unwrap(), "hunter2");
    }

    #[test]
    fn vault_reference_resolves_from_map() {
        let mut vault = std::collections::HashMap::new();
        vault.insert("DB_PASSWORD".to_string(), "s3cr3t".to_string());
        let v = backend::SecretValue::Vault {
            vault: "DB_PASSWORD".to_string(),
        };
        assert_eq!(resolve_secret_value(&v, &vault).unwrap(), "s3cr3t");
    }

    #[test]
    fn missing_vault_key_errors() {
        let vault = std::collections::HashMap::new();
        let v = backend::SecretValue::Vault {
            vault: "MISSING".to_string(),
        };
        let err = resolve_secret_value(&v, &vault).unwrap_err();
        assert!(err.to_string().contains("MISSING"));
    }
}

#[cfg(test)]
mod logs_tests {
    use super::*;

    #[test]
    fn parse_json_log_entry() {
        let data = r#"{"service":"ssp","timestamp":"2026-04-22T10:23:45Z","message":"hello"}"#;
        let entry = parse_log_line(data).expect("should parse");
        assert_eq!(entry.service, "ssp");
        assert_eq!(entry.message, "hello");
        assert!(entry.ts().is_some());
    }

    #[test]
    fn parse_json_log_entry_unix_seconds() {
        let data = r#"{"service":"ssp","timestamp":1745324625,"message":"hi"}"#;
        let entry = parse_log_line(data).expect("should parse");
        assert!(entry.ts().is_some(), "numeric timestamp should decode");
    }

    #[test]
    fn parse_json_log_entry_unix_millis() {
        let data = r#"{"service":"ssp","timestamp":1745324625000,"message":"hi"}"#;
        let entry = parse_log_line(data).expect("should parse");
        // Large integer → treated as milliseconds.
        assert!(entry.ts().is_some(), "millisecond timestamp should decode");
    }

    #[test]
    fn parse_json_log_entry_missing_timestamp_is_none() {
        let data = r#"{"service":"ssp","message":"hi"}"#;
        let entry = parse_log_line(data).expect("should parse");
        assert!(entry.ts().is_none());
    }

    #[test]
    fn parse_json_log_entry_strips_ansi_from_message() {
        let data = r#"{"service":"ssp","timestamp":"2026-04-22T10:23:45Z","message":"\u001b[31mred\u001b[0m"}"#;
        let entry = parse_log_line(data).expect("should parse");
        assert_eq!(entry.message, "red");
    }

    #[test]
    fn parse_legacy_plain_text_line() {
        let entry = parse_log_line("[scheduler] booting").expect("should parse");
        assert_eq!(entry.service, "scheduler");
        assert_eq!(entry.message, "booting");
        assert!(entry.ts().is_none());
    }

    #[test]
    fn parse_unknown_service_falls_through() {
        let entry = parse_log_line("raw line with no prefix").expect("should parse");
        assert_eq!(entry.service, "unknown");
        assert_eq!(entry.message, "raw line with no prefix");
    }

    #[test]
    fn parse_empty_returns_none() {
        assert!(parse_log_line("").is_none());
        assert!(parse_log_line("   ").is_none());
    }

    #[test]
    fn parse_drops_stream_sentinels() {
        // These are SSE control markers some server implementations emit —
        // they should not surface as "unknown" log entries in the UI.
        assert!(parse_log_line("done").is_none());
        assert!(parse_log_line("Done").is_none());
        assert!(parse_log_line("[done]").is_none());
        assert!(parse_log_line("close").is_none());
        assert!(parse_log_line("eof").is_none());
        assert!(parse_log_line("  end  ").is_none());
    }

    #[test]
    fn parse_time_anchor_accepts_duration() {
        let before = chrono::Utc::now();
        let two_hours_ago = parse_time_anchor("2h").unwrap();
        let after = chrono::Utc::now();
        assert!(
            two_hours_ago <= before - chrono::Duration::hours(2) + chrono::Duration::seconds(1)
        );
        assert!(two_hours_ago >= after - chrono::Duration::hours(2) - chrono::Duration::seconds(1));
    }

    #[test]
    fn parse_time_anchor_accepts_rfc3339() {
        let ts = parse_time_anchor("2026-04-21T10:00:00Z").unwrap();
        assert_eq!(ts.to_rfc3339(), "2026-04-21T10:00:00+00:00");
    }

    #[test]
    fn parse_time_anchor_rejects_garbage() {
        assert!(parse_time_anchor("not-a-date").is_err());
    }

    #[test]
    fn parse_time_anchor_accepts_now_literal() {
        let before = chrono::Utc::now();
        let now = parse_time_anchor("now").unwrap();
        let after = chrono::Utc::now();
        assert!(now >= before && now <= after);
        // Case-insensitive + whitespace-tolerant.
        assert!(parse_time_anchor("NOW").is_ok());
        assert!(parse_time_anchor("  Now ").is_ok());
    }

    #[test]
    fn urlencode_preserves_safe_chars() {
        assert_eq!(urlencode("abcXYZ-_.~0123"), "abcXYZ-_.~0123");
    }

    #[test]
    fn urlencode_escapes_regex_metacharacters() {
        assert_eq!(urlencode("a+b&c d/e"), "a%2Bb%26c%20d%2Fe");
    }
}

#[cfg(test)]
mod fn_endpoint_tests {
    use super::*;
    use crate::backend::DeployMode;
    use serde_json::json;

    fn vms(role_ips: &[(&str, &str)]) -> serde_json::Value {
        json!({
            "vms": role_ips
                .iter()
                .map(|(r, ip)| json!({ "role": r, "internal_ip": ip }))
                .collect::<Vec<_>>()
        })
    }

    // The whole point of the fix: a Docker deployment must NEVER bake a raw
    // ephemeral container IP — it has to be the stable DNS alias.
    #[test]
    fn docker_cluster_uses_scheduler_alias_never_ip() {
        let status = vms(&[("scheduler", "172.21.0.3"), ("surrealdb", "172.21.0.2")]);
        let ep = derive_fn_endpoint(&DeployMode::Cluster, &status).unwrap();
        assert_eq!(ep, "http://scheduler:9667");
        assert!(
            !ep.contains("172."),
            "must not bake a Docker bridge IP: {ep}"
        );
    }

    #[test]
    fn docker_singlenode_uses_ssp_alias() {
        // Singlenode has no scheduler; only surrealdb + ssp on a Docker subnet.
        let status = vms(&[("surrealdb", "172.21.0.2"), ("ssp", "172.21.0.4")]);
        let ep = derive_fn_endpoint(&DeployMode::Singlenode, &status).unwrap();
        assert_eq!(ep, "http://ssp-0:8667");
        assert!(!ep.contains("172."));
    }

    #[test]
    fn firecracker_cluster_keeps_stable_scheduler_ip() {
        let status = vms(&[("scheduler", "10.100.5.20"), ("surrealdb", "10.100.5.10")]);
        let ep = derive_fn_endpoint(&DeployMode::Cluster, &status).unwrap();
        assert_eq!(ep, "http://10.100.5.20:9667");
    }

    #[test]
    fn firecracker_cluster_derives_dot20_when_scheduler_ip_missing() {
        // On a redeploy the scheduler may not be listed yet; derive .20 from the
        // surrealdb subnet rather than guessing an ephemeral address.
        let status = vms(&[("surrealdb", "10.100.7.10")]);
        let ep = derive_fn_endpoint(&DeployMode::Cluster, &status).unwrap();
        assert_eq!(ep, "http://10.100.7.20:9667");
    }

    #[test]
    fn firecracker_singlenode_uses_ssp_port() {
        let status = vms(&[("surrealdb", "10.100.9.10")]);
        let ep = derive_fn_endpoint(&DeployMode::Singlenode, &status).unwrap();
        assert_eq!(ep, "http://10.100.9.10:8667");
    }

    #[test]
    fn prefers_canonical_endpoint_when_host_is_stable() {
        let mut status = vms(&[("scheduler", "172.21.0.3")]);
        status["fn_endpoint"] = json!("http://scheduler:9667");
        assert_eq!(
            derive_fn_endpoint(&DeployMode::Cluster, &status).unwrap(),
            "http://scheduler:9667"
        );
    }

    #[test]
    fn rejects_canonical_endpoint_that_is_a_raw_docker_ip() {
        // An older control plane might still hand us a raw IP — ignore it and
        // derive the alias so the stale-IP bug can't be reintroduced.
        let mut status = vms(&[("scheduler", "172.21.0.3")]);
        status["scheduler_url"] = json!("http://172.21.0.3:9667");
        let ep = derive_fn_endpoint(&DeployMode::Cluster, &status).unwrap();
        assert_eq!(ep, "http://scheduler:9667");
        assert!(!ep.contains("172."));
    }

    #[test]
    fn no_vms_yields_none() {
        assert!(derive_fn_endpoint(&DeployMode::Cluster, &json!({ "vms": [] })).is_none());
        assert!(derive_fn_endpoint(&DeployMode::Cluster, &json!({})).is_none());
    }

    #[test]
    fn host_parsing_and_stability_predicate() {
        assert_eq!(endpoint_host("http://scheduler:9667"), "scheduler");
        assert_eq!(endpoint_host("http://172.21.0.3:9667/info"), "172.21.0.3");
        assert_eq!(endpoint_host("http://:9667"), "");
        assert!(is_stable_endpoint_host("scheduler"));
        assert!(is_stable_endpoint_host("10.100.5.20")); // Firecracker
        assert!(!is_stable_endpoint_host("172.21.0.3")); // Docker bridge IP
        assert!(!is_stable_endpoint_host("")); // empty host
    }
}
