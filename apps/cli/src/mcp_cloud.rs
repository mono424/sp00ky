//! Cloud MCP integration: generate dedicated `mcp_live_` tokens for the
//! Sp00ky Cloud MCP server, print setup instructions, and register the server
//! with Claude Code / Cursor / VS Code.
//!
//! The MCP server itself lives in spooky-cloud at `<api>/v1/mcp`. A token is
//! created via the JWT-protected `/v1/mcp/tokens` endpoint, so the user must be
//! logged in (`spky login`) first.

use anyhow::{bail, Context, Result};
use std::process::Command;

use crate::cloud;

const BOLD: &str = "\x1b[1m";
const DIM: &str = "\x1b[2m";
const GREEN: &str = "\x1b[32m";
const YELLOW: &str = "\x1b[33m";
const RESET: &str = "\x1b[0m";

const SERVER_NAME: &str = "spooky-cloud";

/// Every individual scope, used for the "Custom…" interactive picker. Mirrors
/// the taxonomy enforced server-side in spooky-cloud/internal/mcp/scopes.go.
const ALL_SCOPES: &[&str] = &[
    "projects:read",
    "projects:write",
    "deployments:read",
    "deployments:write",
    "logs:read",
    "env:read",
    "env:write",
    "vault:read",
    "vault:write",
    "backups:read",
    "backups:write",
    "domains:read",
    "domains:write",
    "links:read",
    "links:write",
    "tenants:read",
    "tenants:write",
    "billing:read",
    "billing:write",
    "secrets:reveal",
];

/// The MCP endpoint URL derived from the configured API base.
fn endpoint_url() -> String {
    format!("{}/v1/mcp", cloud::api_base_url())
}

// ---------------------------------------------------------------------------
// `spky mcp token`
// ---------------------------------------------------------------------------

#[allow(clippy::too_many_arguments)]
pub fn token(
    name: Option<String>,
    scopes: Option<String>,
    read_only: bool,
    install_flag: bool,
    client: Option<String>,
    yes: bool,
) -> Result<()> {
    let scopes = resolve_scopes(scopes, read_only, yes)?;
    let name = name.unwrap_or_else(|| "spooky-mcp".to_string());

    let creds = cloud::require_credentials()?;
    let mut http = cloud::CloudClient::new(&creds);

    let resp = http
        .post(
            "/v1/mcp/tokens",
            &serde_json::json!({ "name": name, "scopes": scopes }),
        )
        .context("Failed to create MCP token. Are you logged in? Run `spky login`.")?;
    let data: serde_json::Value = resp.into_json().context("Failed to parse token response")?;
    let key = data["key"].as_str().unwrap_or("").to_string();
    let id = data["id"].as_str().unwrap_or("");

    if key.is_empty() {
        bail!("Server did not return a token");
    }

    let url = endpoint_url();

    println!();
    println!("{BOLD}{GREEN}✓ MCP token created{RESET}");
    println!("  {DIM}Name:{RESET}   {name}");
    println!("  {DIM}ID:{RESET}     {id}");
    println!("  {DIM}Scopes:{RESET} {}", scopes.join(", "));
    println!();
    println!("  {BOLD}Token (shown once — copy it now):{RESET}");
    println!("  {GREEN}{key}{RESET}");
    println!();

    // Decide whether to auto-install or just print the tutorial.
    let should_install = install_flag
        || client.is_some()
        || (!yes
            && crate::ui::prefer("Add this MCP server to an editor now?", true).unwrap_or(false));

    if should_install {
        install_to(client, &key, &url)?;
    } else {
        print_tutorial(&key, &url);
    }

    Ok(())
}

/// Resolve the scope set from flags or an interactive prompt.
fn resolve_scopes(scopes: Option<String>, read_only: bool, yes: bool) -> Result<Vec<String>> {
    if read_only {
        return Ok(vec!["mcp:read".to_string()]);
    }
    if let Some(s) = scopes {
        let parsed: Vec<String> = s
            .split(',')
            .map(|p| p.trim().to_string())
            .filter(|p| !p.is_empty())
            .collect();
        if parsed.is_empty() {
            bail!("--scopes was empty");
        }
        return Ok(parsed);
    }
    if yes {
        return Ok(vec!["mcp:read".to_string()]);
    }

    // Interactive: presets, with an escape hatch to pick individual scopes.
    let presets = vec![
        "Read-only (recommended) — view projects, deployments, logs",
        "Read + deploy — reads plus deploy/restart/scale",
        "Full access — all reads and writes (no secret values)",
        "Full access + reveal secrets — also expose env values & vault",
        "Custom… — pick individual scopes",
    ];
    let choice = inquire::Select::new("What should this MCP token be allowed to do?", presets)
        .prompt()
        .context("scope selection cancelled")?;

    let scopes = match choice {
        s if s.starts_with("Read-only") => vec!["mcp:read".to_string()],
        s if s.starts_with("Read + deploy") => {
            vec!["mcp:read".to_string(), "deployments:write".to_string()]
        }
        s if s.starts_with("Full access + reveal") => {
            vec!["mcp:full".to_string(), "secrets:reveal".to_string()]
        }
        s if s.starts_with("Full access") => vec!["mcp:full".to_string()],
        _ => {
            let picked = inquire::MultiSelect::new(
                "Select scopes:",
                ALL_SCOPES.iter().map(|s| s.to_string()).collect(),
            )
            .prompt()
            .context("scope selection cancelled")?;
            if picked.is_empty() {
                bail!("No scopes selected");
            }
            picked
        }
    };
    Ok(scopes)
}

// ---------------------------------------------------------------------------
// `spky mcp tokens` / `spky mcp revoke`
// ---------------------------------------------------------------------------

pub fn list_tokens() -> Result<()> {
    let creds = cloud::require_credentials()?;
    let mut http = cloud::CloudClient::new(&creds);
    let resp = http.get("/v1/mcp/tokens")?;
    let data: Vec<serde_json::Value> = resp.into_json().context("Failed to parse token list")?;

    if data.is_empty() {
        println!("No MCP tokens yet. Create one with `spky mcp token`.");
        return Ok(());
    }

    println!("{:<38} {:<16} {:<20} {}", "ID", "NAME", "PREFIX", "SCOPES");
    for t in &data {
        let scopes = t["scopes"]
            .as_array()
            .map(|a| {
                a.iter()
                    .filter_map(|s| s.as_str())
                    .collect::<Vec<_>>()
                    .join(",")
            })
            .unwrap_or_default();
        println!(
            "{:<38} {:<16} {:<20} {}",
            t["id"].as_str().unwrap_or("-"),
            t["name"].as_str().unwrap_or("-"),
            t["prefix"].as_str().unwrap_or("-"),
            scopes,
        );
    }
    Ok(())
}

pub fn revoke(id: String) -> Result<()> {
    let creds = cloud::require_credentials()?;
    let mut http = cloud::CloudClient::new(&creds);
    http.delete(&format!("/v1/mcp/tokens/{}", id))?;
    println!("{GREEN}✓{RESET} MCP token {id} revoked.");
    Ok(())
}

// ---------------------------------------------------------------------------
// `spky mcp install`
// ---------------------------------------------------------------------------

pub fn install(token: Option<String>, client: Option<String>) -> Result<()> {
    let token = match token {
        Some(t) => t,
        None => inquire::Text::new("MCP token (mcp_live_…):")
            .prompt()
            .context("token required")?,
    };
    if !token.starts_with("mcp_live_") {
        eprintln!("{YELLOW}warning:{RESET} token does not look like an mcp_live_ token");
    }
    install_to(client, &token, &endpoint_url())
}

/// Register the server with the requested editor (default: Claude Code if
/// available), and always print copy-paste config for the others.
fn install_to(client: Option<String>, token: &str, url: &str) -> Result<()> {
    let target = client.as_deref().unwrap_or("claude");

    match target {
        "claude" => {
            if claude_available() {
                add_to_claude(token, url)?;
            } else {
                println!("{YELLOW}Claude Code CLI (`claude`) not found on PATH.{RESET}");
                println!("Once installed, run:");
                println!("  {}", claude_add_command(token, url));
            }
            // Still show the others so any editor can be configured.
            print_cursor_snippet(token, url);
            print_vscode_snippet(token, url);
        }
        "cursor" => print_cursor_snippet(token, url),
        "vscode" | "code" => print_vscode_snippet(token, url),
        other => bail!("unknown editor {other:?} (expected: claude, cursor, vscode)"),
    }
    Ok(())
}

fn claude_available() -> bool {
    Command::new("claude")
        .arg("--version")
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

fn claude_add_command(token: &str, url: &str) -> String {
    format!(
        "claude mcp add --transport http {SERVER_NAME} {url} --header \"Authorization: Bearer {token}\" --scope user"
    )
}

fn add_to_claude(token: &str, url: &str) -> Result<()> {
    let status = Command::new("claude")
        .args([
            "mcp",
            "add",
            "--transport",
            "http",
            SERVER_NAME,
            url,
            "--header",
            &format!("Authorization: Bearer {token}"),
            "--scope",
            "user",
        ])
        .status()
        .context("Failed to run `claude mcp add`")?;
    if status.success() {
        println!("{GREEN}✓{RESET} Added '{SERVER_NAME}' to Claude Code (user scope).");
        println!("  {DIM}Verify with:{RESET} claude mcp list");
    } else {
        println!("{YELLOW}`claude mcp add` exited with an error.{RESET} Run it manually:");
        println!("  {}", claude_add_command(token, url));
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Tutorial output
// ---------------------------------------------------------------------------

fn print_tutorial(token: &str, url: &str) {
    println!("{BOLD}Add the Sp00ky Cloud MCP server to your editor:{RESET}");
    println!();
    println!("{BOLD}Claude Code{RESET}");
    println!("  {}", claude_add_command(token, url));
    println!();
    print_cursor_snippet(token, url);
    print_vscode_snippet(token, url);
}

fn print_cursor_snippet(token: &str, url: &str) {
    println!("{BOLD}Cursor{RESET} {DIM}(~/.cursor/mcp.json or .cursor/mcp.json){RESET}");
    println!(
        r#"  {{
    "mcpServers": {{
      "{SERVER_NAME}": {{
        "url": "{url}",
        "headers": {{ "Authorization": "Bearer {token}" }}
      }}
    }}
  }}"#
    );
    println!();
}

fn print_vscode_snippet(token: &str, url: &str) {
    println!("{BOLD}VS Code{RESET} {DIM}(.vscode/mcp.json){RESET}");
    println!(
        r#"  {{
    "servers": {{
      "{SERVER_NAME}": {{
        "type": "http",
        "url": "{url}",
        "headers": {{ "Authorization": "Bearer {token}" }}
      }}
    }}
  }}"#
    );
    println!();
}
