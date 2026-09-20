use anyhow::{Context, Result};
use crossterm::{
    cursor::{Hide, MoveTo, Show},
    execute,
    style::{Attribute, Color, Print, ResetColor, SetAttribute, SetForegroundColor},
    terminal::{self, Clear, ClearType},
};
use inquire::Select;
use inquire::Text;
use std::fs;
use std::io::{self, IsTerminal, Write};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::thread;
use std::time::{Duration, SystemTime};

use crate::doctor;
use crate::package_manager::{self, PackageManager};

// ---------------------------------------------------------------------------
// Version
// ---------------------------------------------------------------------------

const VERSION: &str = "0.0.1-canary.17";

// ---------------------------------------------------------------------------
// Ghost art (matches AsciiGhost.tsx)
// ---------------------------------------------------------------------------

const RAW_GHOST: [&str; 12] = [
    "   \u{2584}\u{2584}\u{2588}\u{2588}\u{2588}\u{2588}\u{2588}\u{2588}\u{2588}\u{2588}\u{2584}\u{2584}   ",
    " \u{2584}\u{2588}\u{2588}\u{2588}\u{2588}\u{2588}\u{2588}\u{2588}\u{2588}\u{2588}\u{2588}\u{2588}\u{2588}\u{2588}\u{2588}\u{2584} ",
    " \u{2588}\u{2588}\u{2588}\u{2588}\u{2588}\u{2588}\u{2588}\u{2588}\u{2588}\u{2588}\u{2588}\u{2588}\u{2588}\u{2588}\u{2588}\u{2588} ",
    " \u{2588}\u{2588}\u{2588}\u{2588}  \u{2588}\u{2588}\u{2588}\u{2588}  \u{2588}\u{2588}\u{2588}\u{2588} ",
    " \u{2588}\u{2588}\u{2588}\u{2588}\u{2588}\u{2588}\u{2588}\u{2588}\u{2588}\u{2588}\u{2588}\u{2588}\u{2588}\u{2588}\u{2588}\u{2588} ",
    " \u{2588}\u{2588}\u{2588}\u{2588}\u{2588}\u{2588}\u{2580}  \u{2580}\u{2588}\u{2588}\u{2588}\u{2588}\u{2588}\u{2588} ",
    " \u{2588}\u{2588}\u{2588}\u{2588}\u{2588}\u{2588}    \u{2588}\u{2588}\u{2588}\u{2588}\u{2588}\u{2588} ",
    " \u{2588}\u{2588}\u{2588}\u{2588}\u{2588}\u{2588}\u{2584}  \u{2584}\u{2588}\u{2588}\u{2588}\u{2588}\u{2588}\u{2588} ",
    " \u{2588}\u{2588}\u{2588}\u{2588}\u{2588}\u{2588}\u{2588}\u{2588}\u{2588}\u{2588}\u{2588}\u{2588}\u{2588}\u{2588}\u{2588}\u{2588} ",
    " \u{2588}\u{2588}\u{2588}\u{2588}\u{2588}\u{2588}\u{2588}\u{2588}\u{2588}\u{2588}\u{2588}\u{2588}\u{2588}\u{2588}\u{2588}\u{2588} ",
    " \u{2588}\u{2588}\u{2584} \u{2580}\u{2588}\u{2584}\u{2580} \u{2588}\u{2580} \u{2584}\u{2588}\u{2584}\u{2588} ",
    "  \u{2580}   \u{2588}   \u{2588}   \u{2580} \u{2580} ",
];

const EYES_CLOSED: &str = " \u{2588}\u{2588}\u{2588}\u{2588}\u{2580}\u{2580}\u{2588}\u{2588}\u{2588}\u{2588}\u{2580}\u{2580}\u{2588}\u{2588}\u{2588}\u{2588} ";

// ---------------------------------------------------------------------------
// Templates
// ---------------------------------------------------------------------------

mod templates {
    // Schema templates
    pub const SCHEMA_EMPTY: &str = include_str!("templates/schema/empty.surql");
    pub const SCHEMA_MINIMAL: &str = include_str!("templates/schema/minimal.surql");
    pub const SCHEMA_EXAMPLE: &str = include_str!("templates/schema/example.surql");

    // App templates
    pub const APP_PACKAGE_JSON: &str = include_str!("templates/app/package.json.tmpl");
    pub const APP_VITE_CONFIG: &str = include_str!("templates/app/vite.config.ts.tmpl");
    pub const APP_TSCONFIG: &str = include_str!("templates/app/tsconfig.json.tmpl");
    pub const APP_TAILWIND_CONFIG: &str = include_str!("templates/app/tailwind.config.js.tmpl");
    pub const APP_POSTCSS_CONFIG: &str = include_str!("templates/app/postcss.config.js.tmpl");
    pub const APP_INDEX_HTML: &str = include_str!("templates/app/index.html.tmpl");
    pub const APP_MAIN_TSX: &str = include_str!("templates/app/src/main.tsx.tmpl");
    pub const APP_GLOBAL_CSS: &str = include_str!("templates/app/src/global.css.tmpl");
    pub const APP_DB_TS: &str = include_str!("templates/app/src/db.ts.tmpl");
    pub const APP_AUTH_TSX: &str = include_str!("templates/app/src/auth.tsx.tmpl");
    pub const APP_TSX: &str = include_str!("templates/app/src/App.tsx.tmpl");
    pub const APP_NOAUTH_TSX: &str = include_str!("templates/app/src/App.noauth.tsx.tmpl");
    pub const APP_SCHEMA_GEN: &str = include_str!("templates/app/src/schema.gen.ts.tmpl");

    // Shared templates
    pub const GITIGNORE: &str = include_str!("templates/shared/gitignore.tmpl");
    pub const ROOT_PACKAGE_JSON: &str = include_str!("templates/shared/root-package.json.tmpl");
    pub const PNPM_WORKSPACE: &str = include_str!("templates/shared/pnpm-workspace.yaml.tmpl");
    pub const SCHEMA_PACKAGE_JSON: &str = include_str!("templates/shared/schema-package.json.tmpl");
    pub const SP00KY_YML: &str = include_str!("templates/shared/sp00ky.yml.tmpl");

    // AI setup templates
    pub const CLAUDE_MD_FULL: &str = include_str!("templates/shared/claude-md-full.tmpl");
    pub const CLAUDE_MD_SCHEMA: &str = include_str!("templates/shared/claude-md-schema.tmpl");
    pub const CLAUDE_SETTINGS_JSON: &str =
        include_str!("templates/shared/claude-settings.json.tmpl");
}

// ---------------------------------------------------------------------------
// Template rendering
// ---------------------------------------------------------------------------

fn render(template: &str, vars: &[(&str, &str)]) -> String {
    let mut result = template.to_string();
    for (key, value) in vars {
        let placeholder = format!("{{{{{}}}}}", key);
        result = result.replace(&placeholder, value);
    }
    result
}

// ---------------------------------------------------------------------------
// Splash screen animation
// ---------------------------------------------------------------------------

fn shift_line(line: &str, dir: f64) -> String {
    let trimmed = line.trim();
    if dir < -0.7 {
        format!("{}  ", trimmed) // Left
    } else if dir > 0.7 {
        format!("  {}", trimmed) // Right
    } else {
        format!(" {} ", trimmed) // Center
    }
}

fn show_splash() -> Result<()> {
    let mut stdout = io::stdout();

    // Skip animation if not a TTY
    if !stdout.is_terminal() {
        return Ok(());
    }

    let (term_width, term_height) = terminal::size().unwrap_or((80, 24));

    let title = "Sp00ky";
    let version_line = format!("v{}", VERSION);
    let intro_1 = "Real-time sync for SurrealDB.";
    let intro_2 = "Offline-first. Schema-driven. No backend code.";

    // Total block: 12 ghost + 1 blank + 1 title + 1 version + 1 blank + 2 intro + 1 blank + 1 separator = 20
    let block_height: u16 = 20;
    let start_y = if term_height > block_height + 2 {
        (term_height - block_height - 2) / 2
    } else {
        0
    };

    execute!(stdout, Hide, Clear(ClearType::All))?;

    let ghost_width = 20;
    let nanos_seed = || -> u64 {
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .subsec_nanos() as u64
    };

    let mut phase: f64 = 0.0;
    let mut blink = false;
    let mut blink_frames = 0u32;

    // Helper to center text
    let center_x = |text_len: usize| -> u16 {
        if (term_width as usize) > text_len {
            ((term_width as usize - text_len) / 2) as u16
        } else {
            0
        }
    };

    for _ in 0..25 {
        phase += 0.5;

        // Random blink: ~5% chance per frame
        if !blink && (nanos_seed() % 20 == 0) {
            blink = true;
            blink_frames = 2;
        }
        if blink {
            if blink_frames == 0 {
                blink = false;
            } else {
                blink_frames -= 1;
            }
        }

        let mut y = start_y;

        // Draw ghost
        for (i, row) in RAW_GHOST.iter().enumerate() {
            let wave_value = ((i as f64) * 0.6 + phase).sin();
            let content = if blink && i == 3 { EYES_CLOSED } else { row };
            let shifted = shift_line(content, wave_value);
            execute!(
                stdout,
                MoveTo(center_x(ghost_width), y),
                SetForegroundColor(Color::White),
                SetAttribute(Attribute::Bold),
                Print(&shifted),
                SetAttribute(Attribute::Reset),
            )?;
            y += 1;
        }

        y += 1; // blank

        // Title
        execute!(
            stdout,
            MoveTo(center_x(title.len()), y),
            SetForegroundColor(Color::White),
            SetAttribute(Attribute::Bold),
            Print(title),
            SetAttribute(Attribute::Reset),
        )?;
        y += 1;

        // Version
        execute!(
            stdout,
            MoveTo(center_x(version_line.len()), y),
            SetForegroundColor(Color::DarkGrey),
            Print(&version_line),
            ResetColor,
        )?;
        y += 2;

        // Intro lines
        execute!(
            stdout,
            MoveTo(center_x(intro_1.len()), y),
            SetForegroundColor(Color::White),
            Print(intro_1),
            ResetColor,
        )?;
        y += 1;

        execute!(
            stdout,
            MoveTo(center_x(intro_2.len()), y),
            SetForegroundColor(Color::White),
            Print(intro_2),
            ResetColor,
        )?;

        stdout.flush()?;
        thread::sleep(Duration::from_millis(100));
    }

    // After animation: move cursor below the content, show it, print separator
    let end_y = start_y + block_height - 1;

    // Draw a dim separator line
    let sep = "\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}";
    execute!(
        stdout,
        MoveTo(center_x(40), end_y),
        SetForegroundColor(Color::DarkGrey),
        Print(sep),
        ResetColor,
    )?;

    // Position cursor on the line after the separator for the prompt
    execute!(stdout, MoveTo(0, end_y + 2), Show)?;
    stdout.flush()?;

    Ok(())
}

// ---------------------------------------------------------------------------
// Scaffold options — non-interactive entry point for tests + create_project()
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SchemaTemplate {
    Empty,
    Minimal,
    Example,
}

impl SchemaTemplate {
    fn label(self) -> &'static str {
        match self {
            Self::Empty => "Empty",
            Self::Minimal => "Minimal (User + Auth)",
            Self::Example => "Example (User + Threads + Comments)",
        }
    }
    fn has_auth(self) -> bool {
        !matches!(self, Self::Empty)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CodegenFormat {
    TypeScript,
    Dart,
    Skip,
}

impl CodegenFormat {
    fn label(self) -> &'static str {
        match self {
            Self::TypeScript => "TypeScript",
            Self::Dart => "Dart",
            Self::Skip => "Skip",
        }
    }
}

#[derive(Debug, Clone)]
pub struct ScaffoldOptions {
    pub project_name: String,
    pub is_schema_only: bool,
    pub schema_template: SchemaTemplate,
    pub codegen_format: CodegenFormat,
    pub pm: PackageManager,
}

/// Write the project tree to `root_path`. Pure file-writing — no prompts,
/// no shell-out. Callable from tests.
pub fn scaffold(root_path: &Path, opts: &ScaffoldOptions) -> Result<()> {
    let ScaffoldOptions {
        project_name,
        is_schema_only,
        schema_template,
        codegen_format,
        pm,
    } = opts.clone();

    fs::create_dir_all(root_path)?;

    let schema_path = if is_schema_only {
        root_path.to_path_buf()
    } else {
        root_path.join("packages/schema")
    };

    write_schema_package(
        &schema_path,
        &project_name,
        schema_template.label(),
        codegen_format.label(),
        is_schema_only,
    )?;

    if is_schema_only {
        write_file(root_path.join(".gitignore"), templates::GITIGNORE)?;
    } else {
        write_file(root_path.join(".gitignore"), templates::GITIGNORE)?;

        let client_types_section = match codegen_format {
            CodegenFormat::Skip => String::new(),
            CodegenFormat::TypeScript => {
                "\nclientTypes:\n  - format: typescript\n    output: apps/app/src/schema.gen.ts"
                    .to_string()
            }
            CodegenFormat::Dart => {
                "\nclientTypes:\n  - format: dart\n    output: apps/app/src/schema.gen.dart"
                    .to_string()
            }
        };
        write_file(
            root_path.join("sp00ky.yml"),
            &render(
                templates::SP00KY_YML,
                &[
                    ("SCHEMA_DIR", "packages/schema"),
                    ("CLIENT_TYPES_SECTION", &client_types_section),
                ],
            ),
        )?;

        if pm == PackageManager::Pnpm {
            write_file(
                root_path.join("pnpm-workspace.yaml"),
                templates::PNPM_WORKSPACE,
            )?;
        }

        let app_pkg = format!("@{}/app", project_name);
        let app_dev_cmd = pm.run_filter(&app_pkg, "dev");
        let app_build_cmd = pm.run_filter(&app_pkg, "build");
        let workspaces_block = workspaces_block(pm);
        let overrides_block = overrides_block(pm, VERSION);

        write_file(
            root_path.join("package.json"),
            &render(
                templates::ROOT_PACKAGE_JSON,
                &[
                    ("PROJECT_NAME", &project_name),
                    ("VERSION", VERSION),
                    ("APP_DEV_CMD", &app_dev_cmd),
                    ("APP_BUILD_CMD", &app_build_cmd),
                    ("WORKSPACES_BLOCK", &workspaces_block),
                    ("OVERRIDES_BLOCK", &overrides_block),
                ],
            ),
        )?;

        write_app_package(root_path, &project_name, schema_template.has_auth())?;
    }

    write_file(
        root_path.join("CLAUDE.md"),
        &render(
            if is_schema_only {
                templates::CLAUDE_MD_SCHEMA
            } else {
                templates::CLAUDE_MD_FULL
            },
            &[("PROJECT_NAME", &project_name)],
        ),
    )?;
    write_file(
        root_path.join(".claude/settings.local.json"),
        templates::CLAUDE_SETTINGS_JSON,
    )?;

    Ok(())
}

// ---------------------------------------------------------------------------
// Public entry point
// ---------------------------------------------------------------------------

pub fn create_project() -> Result<()> {
    show_splash()?;

    // 1. Project name
    let project_name = Text::new("What is the name of your project?").prompt()?;

    // 2. Project kind
    let project_kind = Select::new(
        "What kind of project?",
        vec!["Full project (Schema + App)", "Schema only"],
    )
    .prompt()?;
    let is_schema_only = project_kind == "Schema only";

    // 3. Schema template
    let schema_type = Select::new(
        "Which schema template?",
        vec![
            "Minimal (User + Auth)",
            "Empty",
            "Example (User + Threads + Comments)",
        ],
    )
    .prompt()?;

    // 4. Codegen format
    let codegen_format = if is_schema_only {
        let fmt = Select::new(
            "Which output format for code generation?",
            vec!["TypeScript", "Dart", "Skip"],
        )
        .prompt()?;
        match fmt {
            "Dart" => CodegenFormat::Dart,
            "Skip" => CodegenFormat::Skip,
            _ => CodegenFormat::TypeScript,
        }
    } else {
        CodegenFormat::TypeScript // full project always uses TypeScript
    };

    let schema_template = match schema_type {
        "Empty" => SchemaTemplate::Empty,
        "Minimal (User + Auth)" => SchemaTemplate::Minimal,
        "Example (User + Threads + Comments)" => SchemaTemplate::Example,
        _ => SchemaTemplate::Empty,
    };
    // 5. Package manager (full projects only — schema-only has no node deps)
    let pm = if is_schema_only {
        PackageManager::Npm
    } else {
        package_manager::detect_preferred()
    };

    println!(
        "\n  Creating {} project \x1b[1m{}\x1b[0m with {} schema...",
        if is_schema_only {
            "schema-only"
        } else {
            "full"
        },
        project_name,
        schema_type
    );

    // --- Create root directory ---
    let root_path = Path::new(&project_name);
    if root_path.exists() {
        if !crate::ui::consent("Directory already exists. Overwrite?")?
        {
            println!("Aborting.");
            return Ok(());
        }
        fs::remove_dir_all(root_path)?;
    }

    // --- Scaffold the project tree (non-interactive, also used by tests) ---
    scaffold(
        root_path,
        &ScaffoldOptions {
            project_name: project_name.clone(),
            is_schema_only,
            schema_template,
            codegen_format,
            pm,
        },
    )?;

    println!(
        "\n  \x1b[32m\u{2713}\x1b[0m Project \x1b[1m{}\x1b[0m created",
        project_name
    );

    // --- Optional: git init ---
    let do_git = crate::ui::prefer("Initialize git repository?", true).unwrap_or(false);

    if do_git {
        let git_result = Command::new("git")
            .args(["init"])
            .current_dir(root_path)
            .output();

        match git_result {
            Ok(output) if output.status.success() => {
                // Initial commit
                let _ = Command::new("git")
                    .args(["add", "-A"])
                    .current_dir(root_path)
                    .output();
                let _ = Command::new("git")
                    .args(["commit", "-m", "Initial commit from sp00ky create"])
                    .current_dir(root_path)
                    .output();
                println!("  \x1b[32m\u{2713}\x1b[0m Git repository initialized");
            }
            _ => {
                println!("  \x1b[33m!\x1b[0m Could not initialize git (is git installed?)");
            }
        }
    }

    // --- Optional: install dependencies ---
    let pm_cmd = pm.cmd();
    let do_install = crate::ui::prefer(&format!("Install dependencies with {}?", pm_cmd), true).unwrap_or(false);

    let mut installed_ok = false;
    if do_install {
        if !package_manager::is_on_path(pm_cmd) {
            println!(
                "  \x1b[31m\u{2717}\x1b[0m {} is not on PATH. Skipping install.",
                pm_cmd
            );
        } else {
            println!("\n  Installing dependencies with {}...", pm_cmd);
            let install_result = Command::new(pm_cmd)
                .args(["install"])
                .current_dir(root_path)
                .status();

            match install_result {
                Ok(status) if status.success() => {
                    installed_ok = true;
                    println!("  \x1b[32m\u{2713}\x1b[0m Dependencies installed");

                    // Run sp00ky generate and migration:create for full projects.
                    // Use the current binary directly instead of the npm-installed one,
                    // so we always get the latest config-loading behavior.
                    if !is_schema_only {
                        let current_exe =
                            std::env::current_exe().unwrap_or_else(|_| PathBuf::from("sp00ky"));

                        println!("\n  Running sp00ky generate...");
                        match Command::new(&current_exe)
                            .args(["generate"])
                            .current_dir(root_path)
                            .status()
                        {
                            Ok(s) if s.success() => {
                                println!("  \x1b[32m\u{2713}\x1b[0m Schema types generated");
                            }
                            _ => {
                                println!("  \x1b[33m!\x1b[0m Could not run sp00ky generate");
                            }
                        }

                        // Docker pre-flight: skip migrate create if the daemon
                        // isn't reachable, rather than failing inside the
                        // ephemeral-DB startup. Print a clear next-step.
                        if !package_manager::docker_available() {
                            println!(
                                "  \x1b[33m!\x1b[0m Docker not running — skipping initial migration."
                            );
                            println!(
                                "      Install/start Docker, then run: \x1b[1m{} run migrate:create initial\x1b[0m",
                                pm_cmd
                            );
                        } else {
                            println!("  Creating initial migration...");
                            match Command::new(&current_exe)
                                .args(["migrate", "create", "initial"])
                                .current_dir(root_path)
                                .status()
                            {
                                Ok(s) if s.success() => {
                                    println!("  \x1b[32m\u{2713}\x1b[0m Initial migration created");
                                }
                                _ => {
                                    println!(
                                        "  \x1b[33m!\x1b[0m Could not create initial migration"
                                    );
                                }
                            }
                        }
                    }
                }
                _ => {
                    println!(
                        "  \x1b[33m!\x1b[0m Could not install dependencies (is {} installed?)",
                        pm_cmd
                    );
                }
            }
        }
    }

    // --- Diagnostics: surface docker/config issues without aborting ---
    if !is_schema_only && installed_ok {
        println!("\n  Running diagnostics...");
        let _ = doctor::run(false, root_path, /*treat_warn_as_ok=*/ true);
    }

    // --- Next steps ---
    println!("\n  \x1b[2mNext steps:\x1b[0m");

    println!("  \x1b[1mcd {}\x1b[0m", project_name);

    if !do_install || !installed_ok {
        println!("  \x1b[1m{} install\x1b[0m", pm_cmd);
        if !is_schema_only {
            println!(
                "  \x1b[1m{} run generate\x1b[0m   \x1b[2m# generate schema types\x1b[0m",
                pm_cmd
            );
            println!(
                "  \x1b[1m{} run migrate:create initial\x1b[0m   \x1b[2m# create initial migration (needs Docker)\x1b[0m",
                pm_cmd
            );
        }
    }

    if is_schema_only {
        println!("  \x1b[1m{} run dev\x1b[0m", pm_cmd);
    } else {
        println!(
            "  \x1b[1m{} run dev\x1b[0m   \x1b[2m# start Sp00ky dev server + app\x1b[0m",
            pm_cmd
        );
    }

    if !is_schema_only {
        println!(
            "\n  \x1b[2mAd-hoc CLI: \x1b[0m\x1b[1m{}\x1b[0m\x1b[2m  (or `{} run doctor`)\x1b[0m",
            pm.exec("spky --help"),
            pm_cmd
        );
    }

    println!(
        "\n  \x1b[2mAI: CLAUDE.md and .claude/ configured. Install the Sp00ky DevTools\x1b[0m"
    );
    println!("  \x1b[2mbrowser extension for live MCP debugging access.\x1b[0m");

    println!();
    Ok(())
}

// ---------------------------------------------------------------------------
// Package-manager-aware JSON fragments for the root package.json template.
// Each fragment is leading-comma so the template stays valid JSON.
// ---------------------------------------------------------------------------

fn workspaces_block(pm: PackageManager) -> String {
    match pm {
        // pnpm uses pnpm-workspace.yaml; nothing to inject here.
        PackageManager::Pnpm => String::new(),
        PackageManager::Npm => ",\n  \"workspaces\": [\"apps/*\", \"packages/*\"]".to_string(),
    }
}

fn overrides_block(pm: PackageManager, version: &str) -> String {
    let entries = format!(
        "\"@spooky-sync/query-builder\": \"{v}\",\n      \"@spooky-sync/core\": \"{v}\",\n      \"@spooky-sync/ssp-wasm\": \"{v}\"",
        v = version
    );
    match pm {
        PackageManager::Pnpm => format!(
            ",\n  \"pnpm\": {{\n    \"overrides\": {{\n      {}\n    }}\n  }}",
            entries
        ),
        // npm overrides aren't transitive the way pnpm.overrides are, but the
        // top-level `overrides` field is the closest equivalent and pins our
        // own packages consistently across direct deps.
        PackageManager::Npm => format!(
            ",\n  \"overrides\": {{\n    {}\n  }}",
            entries.replace("\n      ", "\n    ")
        ),
    }
}

// ---------------------------------------------------------------------------
// Schema package creation
// ---------------------------------------------------------------------------

fn write_schema_package(
    schema_path: &Path,
    project_name: &str,
    schema_type: &str,
    codegen_format: &str,
    is_schema_only: bool,
) -> Result<()> {
    fs::create_dir_all(schema_path.join("src/buckets"))?;
    fs::create_dir_all(schema_path.join("src/outbox"))?;
    fs::create_dir_all(schema_path.join("migrations"))?;
    fs::create_dir_all(schema_path.join(".sp00ky"))?;

    // Schema content
    let schema_content = match schema_type {
        "Empty" => templates::SCHEMA_EMPTY,
        "Minimal (User + Auth)" => templates::SCHEMA_MINIMAL,
        "Example (User + Threads + Comments)" => templates::SCHEMA_EXAMPLE,
        _ => templates::SCHEMA_EMPTY,
    };
    write_file(schema_path.join("src/schema.surql"), schema_content)?;

    // Package name
    let pkg_name = if is_schema_only {
        project_name.to_string()
    } else {
        format!("@{}/schema", project_name)
    };

    // Scripts for schema package
    let scripts = if is_schema_only {
        // Schema-only: all scripts live here
        let mut s = String::from(
            "\n    \"dev\": \"sp00ky dev\",\n    \"migrate:create\": \"sp00ky migrate create\",\n    \"migrate:apply\": \"sp00ky migrate apply --url http://localhost:8666\",\n    \"migrate:status\": \"sp00ky migrate status --url http://localhost:8666\""
        );
        if codegen_format != "Skip" {
            s.push_str(",\n    \"build\": \"sp00ky generate\"");
        }
        s
    } else {
        // Full project: no sp00ky scripts in schema package (they live at root)
        String::new()
    };

    // devDependencies for schema package
    let dev_dependencies = if is_schema_only {
        // Schema-only: CLI dep lives here
        format!(
            ",\n  \"devDependencies\": {{\n    \"@spooky-sync/cli\": \"{}\"\n  }}",
            VERSION
        )
    } else {
        // Full project: CLI dep is at root
        String::new()
    };

    write_file(
        schema_path.join("package.json"),
        &render(
            templates::SCHEMA_PACKAGE_JSON,
            &[
                ("PACKAGE_NAME", &pkg_name),
                ("SCRIPTS", &scripts),
                ("DEV_DEPENDENCIES", &dev_dependencies),
            ],
        ),
    )?;

    // sp00ky.yml — only for schema-only projects (full projects write it at root)
    if is_schema_only {
        let client_types_section = match codegen_format {
            "Skip" => String::new(),
            _ => {
                let fmt = codegen_format.to_lowercase();
                let ext = match codegen_format {
                    "TypeScript" => "ts",
                    "Dart" => "dart",
                    _ => "ts",
                };
                let output = format!("./schema.gen.{}", ext);
                format!(
                    "\nclientTypes:\n  - format: {}\n    output: {}",
                    fmt, output
                )
            }
        };

        write_file(
            schema_path.join("sp00ky.yml"),
            &render(
                templates::SP00KY_YML,
                &[
                    ("SCHEMA_DIR", "."),
                    ("CLIENT_TYPES_SECTION", &client_types_section),
                ],
            ),
        )?;
    }

    if !is_schema_only {
        write_file(schema_path.join(".gitignore"), templates::GITIGNORE)?;
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// App (SolidJS) package creation
// ---------------------------------------------------------------------------

fn write_app_package(root_path: &Path, project_name: &str, has_auth: bool) -> Result<()> {
    let app_path = root_path.join("apps/app");
    fs::create_dir_all(app_path.join("src"))?;

    let vars: &[(&str, &str)] = &[("PROJECT_NAME", project_name), ("VERSION", VERSION)];

    // Config files
    write_file(
        app_path.join("package.json"),
        &render(templates::APP_PACKAGE_JSON, vars),
    )?;
    write_file(app_path.join("vite.config.ts"), templates::APP_VITE_CONFIG)?;
    write_file(app_path.join("tsconfig.json"), templates::APP_TSCONFIG)?;
    write_file(
        app_path.join("tailwind.config.js"),
        templates::APP_TAILWIND_CONFIG,
    )?;
    write_file(
        app_path.join("postcss.config.js"),
        templates::APP_POSTCSS_CONFIG,
    )?;
    write_file(
        app_path.join("index.html"),
        &render(templates::APP_INDEX_HTML, vars),
    )?;

    // Source files
    write_file(app_path.join("src/main.tsx"), templates::APP_MAIN_TSX)?;
    write_file(app_path.join("src/global.css"), templates::APP_GLOBAL_CSS)?;
    write_file(app_path.join("src/db.ts"), templates::APP_DB_TS)?;
    write_file(
        app_path.join("src/schema.gen.ts"),
        templates::APP_SCHEMA_GEN,
    )?;

    // Auth-conditional files
    if has_auth {
        write_file(
            app_path.join("src/App.tsx"),
            &render(templates::APP_TSX, vars),
        )?;
        write_file(app_path.join("src/auth.tsx"), templates::APP_AUTH_TSX)?;
    } else {
        write_file(
            app_path.join("src/App.tsx"),
            &render(templates::APP_NOAUTH_TSX, vars),
        )?;
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn write_file<P: AsRef<Path>>(path: P, content: &str) -> Result<()> {
    if let Some(parent) = path.as_ref().parent() {
        if !parent.exists() {
            fs::create_dir_all(parent)
                .with_context(|| format!("Failed to create directory {:?}", parent))?;
        }
    }
    fs::write(&path, content)
        .with_context(|| format!("Failed to write file {:?}", path.as_ref()))?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Bootstrap tests
// ---------------------------------------------------------------------------
//
// Two layers of coverage:
//   1. Per-config sanity: each generated config (root package.json for npm and
//      pnpm, app package.json, schema package.json, sp00ky.yml in both schema
//      placement modes) renders into its expected format and parses with the
//      canonical project parser.
//   2. One full e2e scaffold: invoke `scaffold()` against a tempdir for a
//      realistic option set and walk the resulting tree, asserting every
//      produced config parses and key fields are populated.
#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::Sp00kyConfig;
    use serde_json::Value as JsonValue;
    use std::path::Path;
    use tempfile::TempDir;

    // --- Helpers ---

    fn render_root_pkg(pm: PackageManager, project: &str) -> String {
        let app_pkg = format!("@{}/app", project);
        render(
            templates::ROOT_PACKAGE_JSON,
            &[
                ("PROJECT_NAME", project),
                ("VERSION", VERSION),
                ("APP_DEV_CMD", &pm.run_filter(&app_pkg, "dev")),
                ("APP_BUILD_CMD", &pm.run_filter(&app_pkg, "build")),
                ("WORKSPACES_BLOCK", &workspaces_block(pm)),
                ("OVERRIDES_BLOCK", &overrides_block(pm, VERSION)),
            ],
        )
    }

    fn parse_json(label: &str, raw: &str) -> JsonValue {
        serde_json::from_str(raw)
            .unwrap_or_else(|e| panic!("{} did not parse as JSON: {}\n---\n{}", label, e, raw))
    }

    fn parse_sp00ky_yml(raw: &str) -> Sp00kyConfig {
        serde_yaml::from_str::<Sp00kyConfig>(raw)
            .unwrap_or_else(|e| panic!("sp00ky.yml did not parse: {}\n---\n{}", e, raw))
    }

    fn assert_no_unrendered_placeholders(label: &str, raw: &str) {
        assert!(
            !raw.contains("{{"),
            "{} still has unrendered template placeholders:\n{}",
            label,
            raw
        );
    }

    fn read(root: &Path, rel: &str) -> String {
        fs::read_to_string(root.join(rel))
            .unwrap_or_else(|e| panic!("missing scaffolded file {}: {}", rel, e))
    }

    // --- Per-config sanity: root package.json renders for both PMs ---

    #[test]
    fn root_package_json_pnpm_is_valid_json_with_pnpm_overrides() {
        let raw = render_root_pkg(PackageManager::Pnpm, "myproj");
        assert_no_unrendered_placeholders("root pkg (pnpm)", &raw);
        let v = parse_json("root pkg (pnpm)", &raw);

        assert_eq!(v["name"], "myproj");
        assert_eq!(v["private"], true);
        assert_eq!(v["dependencies"]["@spooky-sync/cli"], VERSION);
        // pnpm-shaped overrides
        assert!(
            v["pnpm"]["overrides"]["@spooky-sync/core"].is_string(),
            "pnpm.overrides should hold sp00ky package pins"
        );
        // Should not emit npm-specific shapes
        assert!(
            v.get("workspaces").is_none(),
            "pnpm variant uses pnpm-workspace.yaml"
        );
        assert!(
            v.get("overrides").is_none(),
            "pnpm variant should not have a top-level overrides field"
        );
        // Scripts should target pnpm
        let dev_app = v["scripts"]["dev:app"].as_str().unwrap();
        assert!(dev_app.starts_with("pnpm --filter"), "got {}", dev_app);
        assert_eq!(v["scripts"]["doctor"], "spky doctor");
    }

    #[test]
    fn root_package_json_npm_is_valid_json_with_workspaces_and_overrides() {
        let raw = render_root_pkg(PackageManager::Npm, "myproj");
        assert_no_unrendered_placeholders("root pkg (npm)", &raw);
        let v = parse_json("root pkg (npm)", &raw);

        assert_eq!(v["name"], "myproj");
        // npm-shaped workspaces
        let ws = v["workspaces"].as_array().expect("workspaces array");
        let labels: Vec<&str> = ws.iter().map(|w| w.as_str().unwrap()).collect();
        assert!(labels.contains(&"apps/*") && labels.contains(&"packages/*"));
        // Top-level overrides (closest npm equivalent to pnpm.overrides)
        assert!(v["overrides"]["@spooky-sync/core"].is_string());
        // Should not emit pnpm-specific shapes
        assert!(
            v.get("pnpm").is_none(),
            "npm variant should not emit a pnpm key"
        );
        // Scripts should target npm
        let dev_app = v["scripts"]["dev:app"].as_str().unwrap();
        assert!(dev_app.starts_with("npm run -w"), "got {}", dev_app);
        // CLI is a runtime dependency so npx-scaffolded users get spky on PATH via node_modules/.bin
        assert_eq!(v["dependencies"]["@spooky-sync/cli"], VERSION);
        assert!(
            v.get("devDependencies").is_none()
                || v["devDependencies"].get("@spooky-sync/cli").is_none(),
            "@spooky-sync/cli must live in dependencies, not devDependencies"
        );
    }

    // --- Per-config sanity: sp00ky.yml renders ---

    #[test]
    fn sp00ky_yml_full_project_render_parses_with_canonical_schema() {
        let client_types =
            "\nclientTypes:\n  - format: typescript\n    output: apps/app/src/schema.gen.ts";
        let raw = render(
            templates::SP00KY_YML,
            &[
                ("SCHEMA_DIR", "packages/schema"),
                ("CLIENT_TYPES_SECTION", client_types),
            ],
        );
        assert_no_unrendered_placeholders("sp00ky.yml (full)", &raw);

        let cfg = parse_sp00ky_yml(&raw);
        assert_eq!(
            cfg.client_types.len(),
            1,
            "clientTypes should contain one entry"
        );
        // Mode is optional in the parser but the template emits singlenode by default.
        assert!(raw.contains("mode: singlenode"));
        // Optional examples must remain commented so they don't pollute the active config.
        assert!(
            raw.contains("# slug:"),
            "slug example should stay commented"
        );
        assert!(raw.contains("# surrealdb:"));
    }

    #[test]
    fn sp00ky_yml_schema_only_render_parses_with_canonical_schema() {
        // Schema-only writes the yml with SCHEMA_DIR="." and no clientTypes section
        let raw = render(
            templates::SP00KY_YML,
            &[("SCHEMA_DIR", "."), ("CLIENT_TYPES_SECTION", "")],
        );
        assert_no_unrendered_placeholders("sp00ky.yml (schema-only)", &raw);
        let _ = parse_sp00ky_yml(&raw);
    }

    #[test]
    fn sp00ky_yml_skip_codegen_render_parses() {
        // codegen=Skip: scaffold passes empty CLIENT_TYPES_SECTION
        let raw = render(
            templates::SP00KY_YML,
            &[
                ("SCHEMA_DIR", "packages/schema"),
                ("CLIENT_TYPES_SECTION", ""),
            ],
        );
        let cfg = parse_sp00ky_yml(&raw);
        assert!(cfg.client_types.is_empty());
    }

    // --- Full e2e: scaffold a real project tree to a tempdir ---

    #[test]
    fn scaffold_full_npm_project_e2e() {
        let tmp = TempDir::new().expect("tempdir");
        let root = tmp.path().join("e2e-app");
        let opts = ScaffoldOptions {
            project_name: "e2e-app".to_string(),
            is_schema_only: false,
            schema_template: SchemaTemplate::Minimal,
            codegen_format: CodegenFormat::TypeScript,
            pm: PackageManager::Npm,
        };

        scaffold(&root, &opts).expect("scaffold succeeds");

        // 1. Expected files exist
        for rel in [
            "sp00ky.yml",
            "package.json",
            ".gitignore",
            "CLAUDE.md",
            ".claude/settings.local.json",
            "packages/schema/package.json",
            "packages/schema/src/schema.surql",
            "packages/schema/.gitignore",
            "apps/app/package.json",
            "apps/app/vite.config.ts",
            "apps/app/tsconfig.json",
            "apps/app/index.html",
            "apps/app/src/main.tsx",
            "apps/app/src/App.tsx",
            "apps/app/src/auth.tsx", // Minimal template includes auth
            "apps/app/src/db.ts",
            "apps/app/src/schema.gen.ts",
        ] {
            assert!(
                root.join(rel).exists(),
                "expected scaffolded file missing: {}",
                rel
            );
        }
        // npm variant must NOT write pnpm-workspace.yaml
        assert!(
            !root.join("pnpm-workspace.yaml").exists(),
            "npm scaffold should not emit pnpm-workspace.yaml"
        );

        // 2. sp00ky.yml parses with canonical schema and reflects scaffold opts
        let yml = read(&root, "sp00ky.yml");
        assert_no_unrendered_placeholders("scaffolded sp00ky.yml", &yml);
        let cfg = parse_sp00ky_yml(&yml);
        assert_eq!(cfg.client_types.len(), 1);
        assert_eq!(cfg.client_types[0].output, "apps/app/src/schema.gen.ts");

        // 3. Root package.json parses, has expected fields, names use scaffold project name
        let root_pkg = read(&root, "package.json");
        assert_no_unrendered_placeholders("scaffolded root package.json", &root_pkg);
        let root_v = parse_json("root pkg", &root_pkg);
        assert_eq!(root_v["name"], "e2e-app");
        assert_eq!(root_v["dependencies"]["@spooky-sync/cli"], VERSION);
        let dev_app = root_v["scripts"]["dev:app"].as_str().unwrap();
        assert!(
            dev_app.contains("@e2e-app/app"),
            "dev:app should reference scoped app package, got {}",
            dev_app
        );
        assert!(dev_app.starts_with("npm run -w"));

        // 4. App package.json parses
        let app_pkg = read(&root, "apps/app/package.json");
        assert_no_unrendered_placeholders("app package.json", &app_pkg);
        let app_v = parse_json("app pkg", &app_pkg);
        assert_eq!(app_v["name"], "@e2e-app/app");

        // 5. Schema package.json parses
        let schema_pkg = read(&root, "packages/schema/package.json");
        assert_no_unrendered_placeholders("schema package.json", &schema_pkg);
        let schema_v = parse_json("schema pkg", &schema_pkg);
        assert_eq!(schema_v["name"], "@e2e-app/schema");

        // 6. Claude settings is valid JSON
        let claude = read(&root, ".claude/settings.local.json");
        let _ = parse_json("claude settings", &claude);

        // 7. Schema source is non-empty (Minimal template carries auth tables)
        let surql = read(&root, "packages/schema/src/schema.surql");
        assert!(!surql.trim().is_empty(), "schema.surql should have content");
    }

    #[test]
    fn scaffold_full_pnpm_project_emits_pnpm_workspace_yaml() {
        let tmp = TempDir::new().expect("tempdir");
        let root = tmp.path().join("pnpm-app");
        let opts = ScaffoldOptions {
            project_name: "pnpm-app".to_string(),
            is_schema_only: false,
            schema_template: SchemaTemplate::Empty,
            codegen_format: CodegenFormat::TypeScript,
            pm: PackageManager::Pnpm,
        };
        scaffold(&root, &opts).expect("scaffold succeeds");

        assert!(
            root.join("pnpm-workspace.yaml").exists(),
            "pnpm scaffold must emit pnpm-workspace.yaml"
        );
        let root_v = parse_json("root pkg (pnpm e2e)", &read(&root, "package.json"));
        // pnpm variant: no inline workspaces array; pnpm.overrides present
        assert!(root_v.get("workspaces").is_none());
        assert!(root_v["pnpm"]["overrides"]["@spooky-sync/core"].is_string());
        // Empty schema template: no auth.tsx
        assert!(!root.join("apps/app/src/auth.tsx").exists());
    }

    #[test]
    fn scaffold_schema_only_project_writes_yml_at_root() {
        let tmp = TempDir::new().expect("tempdir");
        let root = tmp.path().join("schema-only");
        let opts = ScaffoldOptions {
            project_name: "schema-only".to_string(),
            is_schema_only: true,
            schema_template: SchemaTemplate::Minimal,
            codegen_format: CodegenFormat::Dart,
            pm: PackageManager::Npm,
        };
        scaffold(&root, &opts).expect("scaffold succeeds");

        // Schema-only puts package.json + sp00ky.yml at the project root,
        // not under packages/schema/, and does not scaffold an app.
        assert!(root.join("sp00ky.yml").exists());
        assert!(root.join("package.json").exists());
        assert!(root.join("src/schema.surql").exists());
        assert!(!root.join("apps/app/package.json").exists());

        let cfg = parse_sp00ky_yml(&read(&root, "sp00ky.yml"));
        assert_eq!(cfg.client_types.len(), 1);
        assert_eq!(
            cfg.client_types[0].output, "./schema.gen.dart",
            "Dart codegen should write schema.gen.dart at the project root"
        );
    }
}
