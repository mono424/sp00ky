//! `spky recipe <recipe>`: emit a parameterized cookbook recipe.
//!
//! Recipes are framework-level patterns that an agent can drop into any
//! sp00ky-backed app. Templates are embedded in the binary via `include_str!`
//! and also shipped as files under `templates/cookbook/` so they're readable
//! from `node_modules/@spooky-sync/cli/templates/cookbook/` without running
//! the binary.
use anyhow::{bail, Result};
use std::fs;
use std::path::Path;

const INDEX_MD: &str = include_str!("../templates/cookbook/INDEX.md");
const LIVE_LIST: &str = include_str!("../templates/cookbook/live-list.tsx");
const OPTIMISTIC_MUTATION: &str = include_str!("../templates/cookbook/optimistic-mutation.tsx");
const CRDT_TEXT_FIELD: &str = include_str!("../templates/cookbook/crdt-text-field.tsx");

/// Render a recipe and either print to stdout or write to `out`.
pub fn run(
    recipe: &str,
    table: Option<&str>,
    field: Option<&str>,
    out: Option<&Path>,
) -> Result<()> {
    if recipe == "list" || recipe == "index" {
        print_or_write(INDEX_MD, out)?;
        return Ok(());
    }

    let template = match recipe {
        "live-list" => LIVE_LIST,
        "optimistic-mutation" => OPTIMISTIC_MUTATION,
        "crdt-text-field" => CRDT_TEXT_FIELD,
        other => bail!(
            "unknown recipe `{}`: run `spky recipe list` to see available recipes",
            other
        ),
    };

    let table =
        table.ok_or_else(|| anyhow::anyhow!("recipe `{}` requires --table <name>", recipe))?;

    if recipe == "crdt-text-field" && field.is_none() {
        bail!("recipe `crdt-text-field` requires --field <name>");
    }

    let rendered = render(template, table, field);
    print_or_write(&rendered, out)?;
    Ok(())
}

fn render(template: &str, table: &str, field: Option<&str>) -> String {
    let mut s = template
        .replace("{{table}}", table)
        .replace("{{TablePascal}}", &pascal_case(table));
    if let Some(f) = field {
        s = s
            .replace("{{field}}", f)
            .replace("{{FieldPascal}}", &pascal_case(f));
    }
    s
}

fn pascal_case(s: &str) -> String {
    s.split(|c: char| c == '_' || c == '-' || c == ' ')
        .filter(|p| !p.is_empty())
        .map(|p| {
            let mut chars = p.chars();
            match chars.next() {
                Some(c) => c.to_uppercase().collect::<String>() + chars.as_str(),
                None => String::new(),
            }
        })
        .collect()
}

fn print_or_write(content: &str, out: Option<&Path>) -> Result<()> {
    if let Some(path) = out {
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                fs::create_dir_all(parent)?;
            }
        }
        fs::write(path, content)?;
        eprintln!("Wrote {}", path.display());
    } else {
        print!("{}", content);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pascal_case_basic() {
        assert_eq!(pascal_case("thread"), "Thread");
        assert_eq!(pascal_case("user_profile"), "UserProfile");
        assert_eq!(pascal_case("thread-invite"), "ThreadInvite");
    }

    #[test]
    fn renders_table_substitution() {
        let out = render("hello {{table}} {{TablePascal}}", "thread", None);
        assert_eq!(out, "hello thread Thread");
    }

    #[test]
    fn renders_field_substitution() {
        let out = render(
            "{{table}}.{{field}} -> {{FieldPascal}}",
            "thread",
            Some("title_text"),
        );
        assert_eq!(out, "thread.title_text -> TitleText");
    }
}
