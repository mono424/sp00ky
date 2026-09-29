import { execSync } from "node:child_process";
import { readFileSync } from "node:fs";
import { join, resolve } from "node:path";

const ROOT = resolve(import.meta.dirname, "..");
const EXCLUDE = ["packages/ssp-wasm/pkg"];

const CARGO_TOML_PACKAGES = [
  "apps/scheduler/Cargo.toml",
  "apps/ssp/Cargo.toml",
  "apps/cli/Cargo.toml",
  "apps/ssp-portable/Cargo.toml",
  "apps/ssp-cf/Cargo.toml",
  "packages/ssp/Cargo.toml",
  "packages/ssp-protocol/Cargo.toml",
  "packages/ssp-ffi/Cargo.toml",
  "packages/ssp-node/Cargo.toml",
  "packages/push-core/Cargo.toml",
  "packages/maintenance/Cargo.toml",
  "packages/ssp-wasm/Cargo.toml",
];

const CLI_PLATFORM_PACKAGES = [
  "cli-linux-x64",
  "cli-linux-arm64",
  "cli-darwin-x64",
  "cli-darwin-arm64",
  "cli-win32-x64",
];

function readJson(path) {
  return JSON.parse(readFileSync(path, "utf-8"));
}

function getWorkspacePackageDirs() {
  const raw = execSync("pnpm -r list --json --depth -1", {
    cwd: ROOT,
    encoding: "utf-8",
  });
  const packages = JSON.parse(raw);
  return packages
    .map((pkg) => pkg.path)
    .filter((p) => !EXCLUDE.some((ex) => p.endsWith(ex)));
}

// ── main ─────────────────────────────────────────────────────────────

const expected = process.argv[2];
if (!expected) {
  console.error("Usage: node scripts/check-version.mjs <expected-version>");
  process.exit(1);
}

const dirs = getWorkspacePackageDirs();
const mismatches = [];

for (const dir of dirs) {
  const pkgPath = join(dir, "package.json");
  const pkg = readJson(pkgPath);
  const rel = pkgPath.replace(ROOT + "/", "");

  if (pkg.version !== expected) {
    mismatches.push({ file: rel, actual: pkg.version });
  }
}

// Check CLI platform packages
for (const name of CLI_PLATFORM_PACKAGES) {
  const pkgPath = join(ROOT, "apps/cli/npm", name, "package.json");
  const pkg = readJson(pkgPath);
  const rel = pkgPath.replace(ROOT + "/", "");

  if (pkg.version !== expected) {
    mismatches.push({ file: rel, actual: pkg.version });
  }
}

// Check CLI optionalDependencies
const cliPkgPath = join(ROOT, "apps/cli/package.json");
const cliPkg = readJson(cliPkgPath);
if (cliPkg.optionalDependencies) {
  for (const [dep, ver] of Object.entries(cliPkg.optionalDependencies)) {
    if (ver !== expected) {
      mismatches.push({ file: `apps/cli/package.json optionalDependencies.${dep}`, actual: ver });
    }
  }
}

// Check Cargo.toml files
for (const rel of CARGO_TOML_PACKAGES) {
  const cargoPath = join(ROOT, rel);
  const content = readFileSync(cargoPath, "utf-8");
  const match = content.match(/^version\s*=\s*"([^"]+)"/m);

  if (!match) {
    mismatches.push({ file: rel, actual: "not found" });
  } else if (match[1] !== expected) {
    mismatches.push({ file: rel, actual: match[1] });
  }
}

if (mismatches.length > 0) {
  console.error(`Version mismatch! Expected ${expected} but found:\n`);
  for (const m of mismatches) {
    console.error(`  ${m.file}: ${m.actual}`);
  }
  console.error(`\nRun: pnpm version:bump ${expected}`);
  process.exit(1);
}

console.log(`All ${dirs.length} workspace packages + ${CLI_PLATFORM_PACKAGES.length} platform packages + ${CARGO_TOML_PACKAGES.length} Cargo packages are at ${expected}`);
