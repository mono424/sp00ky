//! Package manager detection and docker availability checks for `spky create`.
//!
//! Defaults to npm. If pnpm is on PATH, the user is prompted to opt in.
//! Used by `create` to scaffold the right `package.json` shape and pick the
//! right install/run binaries; used by `doctor` to surface docker as a warning.

use std::process::{Command, Stdio};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PackageManager {
    Npm,
    Pnpm,
}

impl PackageManager {
    pub fn cmd(self) -> &'static str {
        match self {
            Self::Npm => "npm",
            Self::Pnpm => "pnpm",
        }
    }

    /// Run a script inside a specific workspace package.
    pub fn run_filter(self, pkg: &str, script: &str) -> String {
        match self {
            Self::Npm => format!("npm run -w {} {}", pkg, script),
            Self::Pnpm => format!("pnpm --filter {} {}", pkg, script),
        }
    }

    /// Run a top-level script.
    #[allow(dead_code)]
    pub fn run_script(self, script: &str) -> String {
        match self {
            Self::Npm => format!("npm run {}", script),
            Self::Pnpm => format!("pnpm {}", script),
        }
    }

    /// Run an arbitrary binary (typically the locally-installed `spky`).
    pub fn exec(self, cmd: &str) -> String {
        match self {
            Self::Npm => format!("npx {}", cmd),
            Self::Pnpm => format!("pnpm exec {}", cmd),
        }
    }
}

pub fn is_on_path(bin: &str) -> bool {
    Command::new(bin)
        .arg("--version")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

/// Choose a package manager. Default = npm. If pnpm is installed, ask the
/// user whether to use it instead. `inquire` errors fall back to the default.
pub fn detect_preferred() -> PackageManager {
    let pnpm = is_on_path("pnpm");
    let npm = is_on_path("npm");

    if pnpm && !npm {
        return PackageManager::Pnpm;
    }
    if pnpm {
        let use_pnpm = crate::ui::prefer("pnpm detected — use it instead of npm?", true).unwrap_or(true);
        if use_pnpm {
            return PackageManager::Pnpm;
        }
    }
    PackageManager::Npm
}

/// True iff `docker info` exits 0 — i.e. the daemon is reachable.
/// Used as a pre-flight before `migrate create` and as a doctor warning.
pub fn docker_available() -> bool {
    Command::new("docker")
        .arg("info")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}
