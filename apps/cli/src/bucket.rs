use anyhow::{bail, Context, Result};
use inquire::{Confirm, Select, Text};
use regex::Regex;
use std::fs;
use std::path::{Path, PathBuf};

use crate::backend::YAML_SCHEMA_COMMENT;

// ── Presets ──────────────────────────────────────────────────────────────────

struct Preset {
    label: &'static str,
    description: &'static str,
    extensions: &'static [&'static str],
    default_size_human: &'static str,
    path_prefix_auth: bool,
    default_name: &'static str,
}

const PRESETS: &[Preset] = &[
    Preset {
        label: "Avatars",
        description: "Profile pictures / user avatars",
        extensions: &["jpg", "jpeg", "png", "gif", "webp"],

        default_size_human: "2mb",
        path_prefix_auth: true,
        default_name: "avatars",
    },
    Preset {
        label: "Images",
        description: "General image uploads",
        extensions: &["jpg", "jpeg", "png", "gif", "webp", "svg"],

        default_size_human: "10mb",
        path_prefix_auth: false,
        default_name: "images",
    },
    Preset {
        label: "Documents",
        description: "PDFs, office docs, spreadsheets",
        extensions: &["pdf", "doc", "docx", "txt", "csv", "xlsx"],

        default_size_human: "25mb",
        path_prefix_auth: false,
        default_name: "documents",
    },
    Preset {
        label: "Video",
        description: "Video file uploads",
        extensions: &["mp4", "webm", "mov", "avi"],

        default_size_human: "100mb",
        path_prefix_auth: false,
        default_name: "videos",
    },
    Preset {
        label: "Audio",
        description: "Music and audio files",
        extensions: &["mp3", "wav", "ogg", "flac", "aac"],

        default_size_human: "50mb",
        path_prefix_auth: false,
        default_name: "audio",
    },
    Preset {
        label: "Custom",
        description: "Configure everything manually",
        extensions: &[],

        default_size_human: "5mb",
        path_prefix_auth: false,
        default_name: "",
    },
];

const DANGEROUS_EXTENSIONS: &[&str] = &[
    "exe", "sh", "bat", "cmd", "ps1", "msi", "com", "scr", "vbs", "js", "wsh", "wsf", "jar", "py",
    "rb", "pl", "php",
];

// ── Size parsing ─────────────────────────────────────────────────────────────

fn parse_size(input: &str) -> Result<u64> {
    let input = input.trim().to_lowercase();
    let re = Regex::new(r"^(\d+(?:\.\d+)?)\s*(kb|mb|gb|b|bytes?)?$")?;

    if let Some(caps) = re.captures(&input) {
        let num: f64 = caps[1].parse()?;
        let unit = caps.get(2).map(|m| m.as_str()).unwrap_or("b");

        let bytes = match unit {
            "kb" => num * 1024.0,
            "mb" => num * 1024.0 * 1024.0,
            "gb" => num * 1024.0 * 1024.0 * 1024.0,
            _ => num,
        };

        Ok(bytes as u64)
    } else {
        // Try parsing as raw bytes
        if let Ok(n) = input.parse::<u64>() {
            return Ok(n);
        }
        bail!(
            "Invalid size format: '{}'. Use formats like 5mb, 500kb, 1gb, or raw bytes.",
            input
        );
    }
}

fn format_size(bytes: u64) -> String {
    if bytes >= 1024 * 1024 * 1024 {
        format!("{} GB", bytes / (1024 * 1024 * 1024))
    } else if bytes >= 1024 * 1024 {
        format!("{} MB", bytes / (1024 * 1024))
    } else if bytes >= 1024 {
        format!("{} KB", bytes / 1024)
    } else {
        format!("{} bytes", bytes)
    }
}

// ── Name validation ──────────────────────────────────────────────────────────

fn validate_bucket_name(name: &str) -> Result<()> {
    if name.len() < 2 || name.len() > 64 {
        bail!(
            "Bucket name must be between 2 and 64 characters, got {}.",
            name.len()
        );
    }

    let re = Regex::new(r"^[a-z][a-z0-9_]*$")?;
    if !re.is_match(name) {
        bail!(
            "Bucket name must be snake_case (lowercase letters, digits, underscores) and start with a letter. Got: '{}'",
            name
        );
    }

    Ok(())
}

// ── Extension processing ─────────────────────────────────────────────────────

fn process_extensions(input: &str) -> Vec<String> {
    input
        .split(',')
        .map(|s| s.trim().to_lowercase().trim_start_matches('.').to_string())
        .filter(|s| !s.is_empty())
        .collect::<Vec<_>>()
        .into_iter()
        .fold(Vec::new(), |mut acc, ext| {
            if !acc.contains(&ext) {
                acc.push(ext);
            }
            acc
        })
}

fn check_dangerous_extensions(extensions: &[String]) -> Vec<String> {
    extensions
        .iter()
        .filter(|ext| DANGEROUS_EXTENSIONS.contains(&ext.as_str()))
        .cloned()
        .collect()
}

// ── Duplicate detection ──────────────────────────────────────────────────────

fn check_duplicate_bucket(name: &str, buckets_dir: &Path, config_path: &Path) -> Result<()> {
    // Check if .surql file already exists
    let surql_path = buckets_dir.join(format!("{}.surql", name));
    if surql_path.exists() {
        bail!("Bucket file already exists: {}", surql_path.display());
    }

    // Scan existing bucket files for DEFINE BUCKET with same name
    if buckets_dir.exists() {
        for entry in fs::read_dir(buckets_dir).context("Failed to read buckets directory")? {
            let entry = entry?;
            let path = entry.path();
            if path.extension().and_then(|e| e.to_str()) == Some("surql") {
                let content =
                    fs::read_to_string(&path).context(format!("Failed to read {:?}", path))?;
                let pattern = format!("DEFINE BUCKET IF NOT EXISTS {}", name);
                if content.contains(&pattern) {
                    bail!(
                        "A bucket named '{}' is already defined in {}",
                        name,
                        path.display()
                    );
                }
            }
        }
    }

    // Check sp00ky.yml for duplicate path reference
    if config_path.exists() {
        let content = fs::read_to_string(config_path).context("Failed to read sp00ky.yml")?;
        let expected_entry = format!("{}.surql", name);
        if content.contains(&expected_entry) {
            bail!(
                "sp00ky.yml already references a bucket file containing '{}'",
                name
            );
        }
    }

    Ok(())
}

// ── SURQL generation ─────────────────────────────────────────────────────────

/// The bucket's `.surql`, as scaffolded by `spky bucket create`.
///
/// The PERMISSIONS clause gates `put` on the file EXTENSION only. It cannot gate
/// on size: SurrealDB evaluates the clause before the upload exists, binding
/// only `$action`, `$file` and `$target`, so the only size available to it is
/// `file::head($file).size`, the size of whatever is ALREADY at that key. On
/// SurrealDB 3.0.5 per-file actions were not enforced at all and the mistake was
/// invisible; on 3.1.x the clause runs, and a `size <=` predicate then does the
/// opposite of what it reads like: a first upload to a fresh key passes at any
/// size (`file::head` is NONE), and re-uploading over a file that is already
/// over the limit is DENIED however small the new content is.
///
/// The limit therefore lives in a `COMMENT` annotation, which `parser.rs` reads
/// into the generated client schema so `useFileUpload` enforces it before the
/// bytes ever leave the browser. A server-side size cap needs a bucket-level
/// engine feature that does not exist yet; do not put one back in here.
fn generate_surql(name: &str, backend: &str, max_size: u64, extensions: &[String]) -> String {
    let ext_conditions: Vec<String> = extensions
        .iter()
        .map(|ext| format!("      string::ends_with(file::key($file), '.{}')", ext))
        .collect();

    let ext_block = ext_conditions.join("\n      OR ");

    format!(
        r#"-- Uploads are limited to {} by the CLIENT (the `sp00ky:maxSize` comment
-- below is read by codegen into the bucket's client config). The engine cannot
-- check an upload's size in a PERMISSIONS clause (it evaluates the clause
-- before the file exists), so do not add a `file::head($file).size` predicate:
-- it reads the size of the file already at that key, which lets any first
-- upload through and blocks legitimate overwrites.
DEFINE BUCKET IF NOT EXISTS {} BACKEND "{}"
  PERMISSIONS WHERE
    $action NOT IN ['put']
    OR (
{}
    )
  COMMENT 'sp00ky:maxSize={}';
"#,
        format_size(max_size),
        name,
        backend,
        ext_block,
        max_size
    )
}

// ── sp00ky.yml update ────────────────────────────────────────────────────────

fn update_sp00ky_yml(config_path: &Path, relative_surql_path: &str) -> Result<()> {
    let entry = format!("  - {}", relative_surql_path);

    if !config_path.exists() {
        let content = format!("{}\nbuckets:\n{}\n", YAML_SCHEMA_COMMENT, entry);
        fs::write(config_path, content).context(format!("Failed to create {:?}", config_path))?;
        return Ok(());
    }

    let content = fs::read_to_string(config_path).context("Failed to read sp00ky.yml")?;

    // Check for duplicate
    if content.contains(relative_surql_path) {
        bail!(
            "sp00ky.yml already contains entry for {}",
            relative_surql_path
        );
    }

    if content.contains("buckets:") {
        // Find the buckets block and append after the last `  - ` line in that block
        let lines: Vec<&str> = content.lines().collect();
        let mut result = Vec::new();
        let mut in_buckets_block = false;
        let mut inserted = false;
        let mut last_bucket_entry_idx = None;

        // First pass: find the last entry in the buckets block
        for (i, line) in lines.iter().enumerate() {
            if line.trim() == "buckets:" || line.starts_with("buckets:") {
                in_buckets_block = true;
                continue;
            }
            if in_buckets_block {
                let trimmed = line.trim();
                if trimmed.starts_with("- ") {
                    last_bucket_entry_idx = Some(i);
                } else if !trimmed.is_empty() && !trimmed.starts_with('#') {
                    // Hit a new top-level key, end of buckets block
                    in_buckets_block = false;
                }
            }
        }

        // Second pass: insert after last bucket entry
        for (i, line) in lines.iter().enumerate() {
            result.push(line.to_string());
            if Some(i) == last_bucket_entry_idx && !inserted {
                result.push(entry.clone());
                inserted = true;
            }
        }

        // If buckets: existed but had no entries (empty list)
        if !inserted {
            let mut final_result = Vec::new();
            for line in &result {
                final_result.push(line.clone());
                if (line.trim() == "buckets:" || line.starts_with("buckets:")) && !inserted {
                    final_result.push(entry.clone());
                    inserted = true;
                }
            }
            result = final_result;
        }

        let new_content = result.join("\n");
        // Preserve trailing newline if original had one
        let new_content = if content.ends_with('\n') && !new_content.ends_with('\n') {
            format!("{}\n", new_content)
        } else {
            new_content
        };

        fs::write(config_path, new_content).context("Failed to write updated sp00ky.yml")?;
    } else {
        // No buckets key — append section to end
        let mut new_content = content.clone();
        if !new_content.ends_with('\n') {
            new_content.push('\n');
        }
        new_content.push_str(&format!("buckets:\n{}\n", entry));
        fs::write(config_path, new_content).context("Failed to write updated sp00ky.yml")?;
    }

    Ok(())
}

// ── Public entry point ───────────────────────────────────────────────────────

pub fn add(
    name: Option<String>,
    preset: Option<String>,
    max_size: Option<String>,
    extensions: Option<String>,
    backend: Option<String>,
    storage_enabled: bool,
    path_prefix_auth: Option<bool>,
    config: PathBuf,
    buckets_dir: PathBuf,
) -> Result<()> {
    // Step 1: Preset selection
    let selected_preset = if let Some(preset_name) = &preset {
        let preset_name_lower = preset_name.to_lowercase();
        PRESETS
            .iter()
            .find(|p| p.label.to_lowercase() == preset_name_lower)
            .context(format!(
                "Unknown preset: '{}'. Available: avatars, images, documents, video, audio, custom",
                preset_name
            ))?
    } else {
        let options: Vec<String> = PRESETS
            .iter()
            .map(|p| format!("{:<12} {}", p.label, p.description))
            .collect();

        let selection = Select::new("Choose a preset:", options).prompt()?;

        let idx = PRESETS
            .iter()
            .position(|p| selection.starts_with(p.label))
            .unwrap_or(PRESETS.len() - 1);

        &PRESETS[idx]
    };

    let is_custom = selected_preset.label == "Custom";

    // Step 2: Bucket name
    let bucket_name = if let Some(n) = name {
        validate_bucket_name(&n)?;
        n
    } else {
        let default = if !selected_preset.default_name.is_empty() {
            selected_preset.default_name.to_string()
        } else {
            String::new()
        };

        let mut prompt = Text::new("Bucket name (snake_case):");
        if !default.is_empty() {
            prompt = prompt.with_default(&default);
        }
        prompt =
            prompt.with_help_message("Lowercase letters, digits, underscores. e.g. user_avatars");

        let input = prompt.prompt()?;
        validate_bucket_name(&input)?;
        input
    };

    // Check for duplicates
    check_duplicate_bucket(&bucket_name, &buckets_dir, &config)?;

    // Resolve the backend: an explicit `--backend` wins; otherwise default to a
    // persistent file path when storage is enabled, else memory.
    let backend = backend.unwrap_or_else(|| {
        if storage_enabled {
            crate::backend::bucket_backend_path(&bucket_name)
        } else {
            "memory".to_string()
        }
    });

    // Step 3: Max file size
    let size_bytes = if let Some(size_str) = max_size {
        parse_size(&size_str)?
    } else {
        let default = selected_preset.default_size_human.to_string();
        let input = Text::new("Max file size:")
            .with_default(&default)
            .with_help_message("e.g. 5mb, 500kb, 1gb, or raw bytes")
            .prompt()?;
        parse_size(&input)?
    };

    // Size sanity checks
    if size_bytes > 10 * 1024 * 1024 * 1024 {
        bail!("Max file size cannot exceed 10 GB.");
    }
    if size_bytes > 1024 * 1024 * 1024 {
        println!(
            "\n  Warning: Max size is over 1 GB ({}). Make sure this is intentional.\n",
            format_size(size_bytes)
        );
    }

    // Step 4: Extensions
    let exts = if let Some(ext_str) = extensions {
        process_extensions(&ext_str)
    } else if is_custom {
        let input = Text::new("Allowed file extensions (comma-separated):")
            .with_help_message("e.g. jpg,png,gif — no dots needed")
            .prompt()?;
        let processed = process_extensions(&input);
        if processed.is_empty() {
            bail!("At least one file extension is required.");
        }
        processed
    } else {
        let default = selected_preset.extensions.join(", ");
        let input = Text::new("Allowed file extensions:")
            .with_default(&default)
            .with_help_message("Comma-separated, no dots. Edit to customize.")
            .prompt()?;
        let processed = process_extensions(&input);
        if processed.is_empty() {
            bail!("At least one file extension is required.");
        }
        processed
    };

    // Warn on dangerous extensions
    let dangerous = check_dangerous_extensions(&exts);
    if !dangerous.is_empty() {
        println!(
            "\n  Warning: Potentially dangerous extensions detected: {}\n  These file types can execute code. Proceed with caution.\n",
            dangerous.join(", ")
        );
    }

    // Step 5: Path prefix auth
    let _use_path_prefix_auth = if let Some(auth) = path_prefix_auth {
        auth
    } else {
        let default = selected_preset.path_prefix_auth;
        Confirm::new("Enable per-user path isolation (path_prefix_auth)?")
            .with_default(default)
            .with_help_message("Isolates uploads by user path prefix for access control")
            .prompt()?
    };

    // Step 6: Confirmation summary
    let relative_buckets_dir = if buckets_dir.starts_with("./") {
        buckets_dir.to_string_lossy().to_string()
    } else {
        format!("./{}", buckets_dir.display())
    };
    let surql_filename = format!("{}.surql", bucket_name);
    let surql_relative = format!("{}/{}", relative_buckets_dir, surql_filename);

    println!();
    println!("  Bucket Summary");
    println!("  ─────────────────────────────────");
    println!("  Name:        {}", bucket_name);
    println!("  Preset:      {}", selected_preset.label);
    println!(
        "  Max size:    {} ({} bytes)",
        format_size(size_bytes),
        size_bytes
    );
    println!("  Extensions:  {}", exts.join(", "));
    println!("  Backend:     {}", backend);
    println!(
        "  Auth:        {}",
        if _use_path_prefix_auth {
            "per-user path isolation"
        } else {
            "none"
        }
    );
    println!("  File:        {}", surql_relative);
    println!("  Config:      {}", config.display());
    println!();

    // Only prompt for confirmation in interactive mode (if we got here via prompts)
    if preset.is_none() {
        let confirmed = Confirm::new("Create this bucket?")
            .with_default(true)
            .prompt()?;
        if !confirmed {
            println!("  Aborted.");
            return Ok(());
        }
    }

    // ── Generate files ───────────────────────────────────────────────────

    // Create buckets directory
    fs::create_dir_all(&buckets_dir).context(format!(
        "Failed to create buckets directory: {:?}",
        buckets_dir
    ))?;

    // Write .surql file
    let surql_content = generate_surql(&bucket_name, &backend, size_bytes, &exts);
    let surql_path = buckets_dir.join(&surql_filename);
    fs::write(&surql_path, &surql_content).context(format!("Failed to write {:?}", surql_path))?;

    // Update sp00ky.yml
    update_sp00ky_yml(&config, &surql_relative)?;

    // ── Output ───────────────────────────────────────────────────────────

    println!();
    println!("  Bucket created!");
    println!();
    println!("    File:    {}", surql_relative);
    println!("    Config:  {} (updated)", config.display());
    println!();
    println!("  Run `sp00ky` to regenerate types with the new bucket.");
    println!();

    Ok(())
}

// ── Backend switching ─────────────────────────────────────────────────────────

/// Build the regex that matches a single bucket's `DEFINE BUCKET … BACKEND "…"`
/// prefix (group 1) so the quoted backend value can be rewritten.
fn bucket_backend_re(name: &str) -> Result<Regex> {
    Regex::new(&format!(
        r#"(?i)(DEFINE\s+BUCKET\s+(?:OVERWRITE\s+|IF\s+NOT\s+EXISTS\s+)?{}\s+BACKEND\s+)"[^"]*""#,
        regex::escape(name)
    ))
    .context("Failed to build bucket backend regex")
}

/// Locate the `.surql` file that defines `name`. Prefers `<name>.surql`, then
/// scans the buckets directory for any file containing the definition.
fn find_bucket_file(name: &str, buckets_dir: &Path) -> Result<PathBuf> {
    if !buckets_dir.exists() {
        bail!("Buckets directory not found: {}", buckets_dir.display());
    }
    let def_re = Regex::new(&format!(
        r"(?i)DEFINE\s+BUCKET\s+(?:OVERWRITE\s+|IF\s+NOT\s+EXISTS\s+)?{}\b",
        regex::escape(name)
    ))?;

    let direct = buckets_dir.join(format!("{}.surql", name));
    if direct.exists() {
        let content = fs::read_to_string(&direct)?;
        if def_re.is_match(&content) {
            return Ok(direct);
        }
    }

    for entry in fs::read_dir(buckets_dir).context("Failed to read buckets directory")? {
        let path = entry?.path();
        if path.extension().and_then(|e| e.to_str()) == Some("surql") {
            let content = fs::read_to_string(&path)?;
            if def_re.is_match(&content) {
                return Ok(path);
            }
        }
    }

    bail!(
        "No bucket named '{}' found in {}",
        name,
        buckets_dir.display()
    );
}

/// Switch an existing bucket's storage backend between `memory` and
/// `persistent` (a `file:` backend on the storage volume) by rewriting the
/// `BACKEND "…"` clause in its authored `.surql` file.
pub fn set_backend(
    name: String,
    target: Option<String>,
    buckets_dir: PathBuf,
) -> Result<()> {
    validate_bucket_name(&name)?;
    let surql_path = find_bucket_file(&name, &buckets_dir)?;

    let target = match target {
        Some(t) => t.to_lowercase(),
        None => Select::new(
            "Backend for this bucket:",
            vec!["persistent".to_string(), "memory".to_string()],
        )
        .with_help_message("persistent = file backend on the storage volume; memory = in-RAM")
        .prompt()?,
    };

    let new_backend = match target.as_str() {
        "memory" => "memory".to_string(),
        "persistent" | "file" => crate::backend::bucket_backend_path(&name),
        other => bail!(
            "Unknown backend '{}'. Use 'memory' or 'persistent'.",
            other
        ),
    };

    let content = fs::read_to_string(&surql_path)
        .context(format!("Failed to read {:?}", surql_path))?;
    let re = bucket_backend_re(&name)?;
    if !re.is_match(&content) {
        bail!(
            "Could not find a `DEFINE BUCKET {} ... BACKEND \"...\"` clause in {}",
            name,
            surql_path.display()
        );
    }
    let new_content = re
        .replace(&content, |c: &regex::Captures| {
            format!(r#"{}"{}""#, &c[1], new_backend)
        })
        .into_owned();
    fs::write(&surql_path, new_content)
        .context(format!("Failed to write {:?}", surql_path))?;

    println!();
    println!("  Bucket '{}' backend set to: {}", name, new_backend);
    println!("  File: {}", surql_path.display());
    println!();
    println!("  Note: this applies on the next fresh schema apply. SurrealDB will");
    println!("  not redefine an already-created bucket (DEFINE BUCKET IF NOT EXISTS),");
    println!("  and existing in-memory files do not migrate to disk.");
    println!();
    println!("  Run `sp00ky` to regenerate, then redeploy or `spky dev --clean-db`.");
    println!();

    Ok(())
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    // ── parse_size ───────────────────────────────────────────────────────

    #[test]
    fn test_parse_size_megabytes() {
        assert_eq!(parse_size("5mb").unwrap(), 5 * 1024 * 1024);
    }

    #[test]
    fn test_parse_size_kilobytes() {
        assert_eq!(parse_size("500kb").unwrap(), 500 * 1024);
    }

    #[test]
    fn test_parse_size_gigabytes() {
        assert_eq!(parse_size("1gb").unwrap(), 1024 * 1024 * 1024);
    }

    #[test]
    fn test_parse_size_raw_bytes() {
        assert_eq!(parse_size("1048576").unwrap(), 1048576);
    }

    #[test]
    fn test_parse_size_with_spaces() {
        assert_eq!(parse_size("  10 mb  ").unwrap(), 10 * 1024 * 1024);
    }

    #[test]
    fn test_parse_size_case_insensitive() {
        assert_eq!(parse_size("5MB").unwrap(), 5 * 1024 * 1024);
    }

    #[test]
    fn test_parse_size_invalid() {
        assert!(parse_size("abc").is_err());
    }

    #[test]
    fn test_parse_size_bytes_suffix() {
        assert_eq!(parse_size("100b").unwrap(), 100);
    }

    // ── validate_bucket_name ─────────────────────────────────────────────

    #[test]
    fn test_valid_name() {
        assert!(validate_bucket_name("user_avatars").is_ok());
    }

    #[test]
    fn test_valid_name_with_digits() {
        assert!(validate_bucket_name("images2").is_ok());
    }

    #[test]
    fn test_name_too_short() {
        assert!(validate_bucket_name("a").is_err());
    }

    #[test]
    fn test_name_starts_with_digit() {
        assert!(validate_bucket_name("1images").is_err());
    }

    #[test]
    fn test_name_uppercase() {
        assert!(validate_bucket_name("UserAvatars").is_err());
    }

    #[test]
    fn test_name_with_hyphens() {
        assert!(validate_bucket_name("user-avatars").is_err());
    }

    // ── process_extensions ───────────────────────────────────────────────

    #[test]
    fn test_process_extensions_basic() {
        let result = process_extensions("jpg,png,gif");
        assert_eq!(result, vec!["jpg", "png", "gif"]);
    }

    #[test]
    fn test_process_extensions_strips_dots() {
        let result = process_extensions(".jpg, .png");
        assert_eq!(result, vec!["jpg", "png"]);
    }

    #[test]
    fn test_process_extensions_deduplicates() {
        let result = process_extensions("jpg, jpg, png");
        assert_eq!(result, vec!["jpg", "png"]);
    }

    #[test]
    fn test_process_extensions_lowercases() {
        let result = process_extensions("JPG, PNG");
        assert_eq!(result, vec!["jpg", "png"]);
    }

    #[test]
    fn test_process_extensions_handles_spaces() {
        let result = process_extensions("  jpg ,  png , gif  ");
        assert_eq!(result, vec!["jpg", "png", "gif"]);
    }

    // ── check_dangerous_extensions ───────────────────────────────────────

    #[test]
    fn test_no_dangerous() {
        let exts = vec!["jpg".to_string(), "png".to_string()];
        assert!(check_dangerous_extensions(&exts).is_empty());
    }

    #[test]
    fn test_finds_dangerous() {
        let exts = vec!["jpg".to_string(), "exe".to_string(), "sh".to_string()];
        let dangerous = check_dangerous_extensions(&exts);
        assert_eq!(dangerous, vec!["exe", "sh"]);
    }

    // ── generate_surql ───────────────────────────────────────────────────

    #[test]
    fn test_generate_surql_basic() {
        let result = generate_surql(
            "avatars",
            "memory",
            5242880,
            &["jpg".to_string(), "png".to_string()],
        );
        assert!(result.contains("DEFINE BUCKET IF NOT EXISTS avatars BACKEND \"memory\""));
        assert!(result.contains("string::ends_with(file::key($file), '.jpg')"));
        assert!(result.contains("string::ends_with(file::key($file), '.png')"));
        // The limit is an annotation the client enforces, never a permission
        // predicate: `file::head($file)` in a PERMISSIONS clause reads the file
        // ALREADY at the key, so it waves through every first upload and blocks
        // overwrites of anything over the limit.
        assert!(result.contains("COMMENT 'sp00ky:maxSize=5242880'"));
        let statement = result.split("DEFINE BUCKET").nth(1).unwrap();
        assert!(!statement.contains("file::head("));
    }

    #[test]
    fn test_generate_surql_single_extension() {
        let result = generate_surql("docs", "memory", 1024, &["pdf".to_string()]);
        assert!(result.contains("string::ends_with(file::key($file), '.pdf')"));
        // With a single extension, there should be no OR between extension checks
        let ext_section = result.split("$action NOT IN ['put']").nth(1).unwrap_or("");
        assert!(!ext_section.contains("\n      OR "));
    }

    #[test]
    fn test_generate_surql_matches_example_format() {
        let result = generate_surql(
            "profile_pictures",
            "memory",
            5242880,
            &[
                "jpg".to_string(),
                "jpeg".to_string(),
                "png".to_string(),
                "gif".to_string(),
            ],
        );
        // Check it matches the structure from profile.surql
        assert!(result.contains("DEFINE BUCKET IF NOT EXISTS profile_pictures BACKEND \"memory\""));
        assert!(result.contains("PERMISSIONS WHERE"));
        assert!(result.contains("$action NOT IN ['put']"));
    }

    // ── update_sp00ky_yml ────────────────────────────────────────────────

    #[test]
    fn test_update_yml_creates_new_file() {
        let dir = TempDir::new().unwrap();
        let config_path = dir.path().join("sp00ky.yml");

        update_sp00ky_yml(&config_path, "./src/buckets/test.surql").unwrap();

        let content = fs::read_to_string(&config_path).unwrap();
        assert!(content.contains("buckets:"));
        assert!(content.contains("  - ./src/buckets/test.surql"));
    }

    #[test]
    fn test_update_yml_appends_to_existing_buckets() {
        let dir = TempDir::new().unwrap();
        let config_path = dir.path().join("sp00ky.yml");

        fs::write(
            &config_path,
            "buckets:\n  - ./src/buckets/existing.surql\napps:\n  api:\n    type: backend\n",
        )
        .unwrap();

        update_sp00ky_yml(&config_path, "./src/buckets/new.surql").unwrap();

        let content = fs::read_to_string(&config_path).unwrap();
        assert!(content.contains("  - ./src/buckets/existing.surql"));
        assert!(content.contains("  - ./src/buckets/new.surql"));
        assert!(content.contains("apps:"));
    }

    #[test]
    fn test_update_yml_adds_buckets_section() {
        let dir = TempDir::new().unwrap();
        let config_path = dir.path().join("sp00ky.yml");

        fs::write(
            &config_path,
            "mode: cluster\napps:\n  api:\n    type: backend\n",
        )
        .unwrap();

        update_sp00ky_yml(&config_path, "./src/buckets/test.surql").unwrap();

        let content = fs::read_to_string(&config_path).unwrap();
        assert!(content.contains("buckets:"));
        assert!(content.contains("  - ./src/buckets/test.surql"));
        assert!(content.contains("mode: cluster"));
    }

    #[test]
    fn test_update_yml_rejects_duplicate() {
        let dir = TempDir::new().unwrap();
        let config_path = dir.path().join("sp00ky.yml");

        fs::write(&config_path, "buckets:\n  - ./src/buckets/test.surql\n").unwrap();

        let result = update_sp00ky_yml(&config_path, "./src/buckets/test.surql");
        assert!(result.is_err());
    }

    // ── format_size ──────────────────────────────────────────────────────

    #[test]
    fn test_format_size_bytes() {
        assert_eq!(format_size(500), "500 bytes");
    }

    #[test]
    fn test_format_size_kb() {
        assert_eq!(format_size(2048), "2 KB");
    }

    #[test]
    fn test_format_size_mb() {
        assert_eq!(format_size(5 * 1024 * 1024), "5 MB");
    }

    #[test]
    fn test_format_size_gb() {
        assert_eq!(format_size(2 * 1024 * 1024 * 1024), "2 GB");
    }

    // ── check_duplicate_bucket ───────────────────────────────────────────

    #[test]
    fn test_duplicate_file_exists() {
        let dir = TempDir::new().unwrap();
        let buckets_dir = dir.path().join("buckets");
        fs::create_dir_all(&buckets_dir).unwrap();
        fs::write(buckets_dir.join("test.surql"), "content").unwrap();
        let config_path = dir.path().join("sp00ky.yml");

        let result = check_duplicate_bucket("test", &buckets_dir, &config_path);
        assert!(result.is_err());
    }

    #[test]
    fn test_duplicate_name_in_existing_file() {
        let dir = TempDir::new().unwrap();
        let buckets_dir = dir.path().join("buckets");
        fs::create_dir_all(&buckets_dir).unwrap();
        fs::write(
            buckets_dir.join("other.surql"),
            "DEFINE BUCKET IF NOT EXISTS test BACKEND \"memory\"",
        )
        .unwrap();
        let config_path = dir.path().join("sp00ky.yml");

        let result = check_duplicate_bucket("test", &buckets_dir, &config_path);
        assert!(result.is_err());
    }

    #[test]
    fn test_no_duplicate() {
        let dir = TempDir::new().unwrap();
        let buckets_dir = dir.path().join("buckets");
        let config_path = dir.path().join("sp00ky.yml");

        let result = check_duplicate_bucket("new_bucket", &buckets_dir, &config_path);
        assert!(result.is_ok());
    }

    // ── set_backend / find_bucket_file ───────────────────────────────────

    #[test]
    fn test_find_bucket_file_by_name() {
        let dir = TempDir::new().unwrap();
        let buckets_dir = dir.path().join("buckets");
        fs::create_dir_all(&buckets_dir).unwrap();
        fs::write(
            buckets_dir.join("avatars.surql"),
            "DEFINE BUCKET IF NOT EXISTS avatars BACKEND \"memory\";",
        )
        .unwrap();

        let found = find_bucket_file("avatars", &buckets_dir).unwrap();
        assert_eq!(found, buckets_dir.join("avatars.surql"));
    }

    #[test]
    fn test_find_bucket_file_missing_errors() {
        let dir = TempDir::new().unwrap();
        let buckets_dir = dir.path().join("buckets");
        fs::create_dir_all(&buckets_dir).unwrap();
        assert!(find_bucket_file("nope", &buckets_dir).is_err());
    }

    #[test]
    fn test_set_backend_memory_to_persistent() {
        let dir = TempDir::new().unwrap();
        let buckets_dir = dir.path().join("buckets");
        fs::create_dir_all(&buckets_dir).unwrap();
        let path = buckets_dir.join("avatars.surql");
        fs::write(
            &path,
            "DEFINE BUCKET IF NOT EXISTS avatars BACKEND \"memory\"\n  PERMISSIONS FULL;\n",
        )
        .unwrap();

        set_backend(
            "avatars".to_string(),
            Some("persistent".to_string()),
            buckets_dir.clone(),
        )
        .unwrap();

        let content = fs::read_to_string(&path).unwrap();
        assert!(content.contains("BACKEND \"file:/buckets/avatars\""));
        assert!(!content.contains("BACKEND \"memory\""));
        // The rest of the statement is untouched.
        assert!(content.contains("PERMISSIONS FULL;"));
    }

    #[test]
    fn test_set_backend_persistent_to_memory() {
        let dir = TempDir::new().unwrap();
        let buckets_dir = dir.path().join("buckets");
        fs::create_dir_all(&buckets_dir).unwrap();
        let path = buckets_dir.join("docs.surql");
        fs::write(
            &path,
            "DEFINE BUCKET IF NOT EXISTS docs BACKEND \"file:/buckets/docs\";\n",
        )
        .unwrap();

        set_backend(
            "docs".to_string(),
            Some("memory".to_string()),
            buckets_dir.clone(),
        )
        .unwrap();

        let content = fs::read_to_string(&path).unwrap();
        assert!(content.contains("BACKEND \"memory\""));
    }

    #[test]
    fn test_set_backend_rejects_unknown_target() {
        let dir = TempDir::new().unwrap();
        let buckets_dir = dir.path().join("buckets");
        fs::create_dir_all(&buckets_dir).unwrap();
        fs::write(
            buckets_dir.join("avatars.surql"),
            "DEFINE BUCKET IF NOT EXISTS avatars BACKEND \"memory\";",
        )
        .unwrap();

        let result = set_backend(
            "avatars".to_string(),
            Some("s3".to_string()),
            buckets_dir,
        );
        assert!(result.is_err());
    }
}
