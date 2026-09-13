---
name: bump-version
description: Bump the canary version across all packages, commit, tag, and push to trigger CI/CD.
user_invocable: true
---

# Bump Version

Bump the canary version number across all `package.json`, `Cargo.toml`, and manifest files, regenerate all Cargo.lock files, verify the SSP + scheduler binaries will report the new version, then commit, tag, and push.

## Steps

1. **Determine current version**: Read the current version from the root `package.json` (e.g. `0.0.1-canary.50`).

2. **Calculate next version**: Increment the canary number by 1 (e.g. `0.0.1-canary.50` → `0.0.1-canary.51`).

3. **Run the bump script**: Run `node scripts/bump-version.mjs NEW_VERSION` from the repo root. This updates all workspace `package.json` files, CLI platform packages, Chrome manifest, and all `Cargo.toml` files in one go.

4. **Regenerate Cargo.lock files**: Run `cargo check` in `apps/cli/`, `apps/scheduler/`, **and** `apps/ssp/`. All three crates have their own `Cargo.lock` — skipping any one of them leaves a stale lock in the commit, which causes `cargo build --locked` to fail in CI and makes the git history lie about the built version.

5. **Verify version is propagated to SSP + scheduler**: The SSP and scheduler binaries embed their version via `env!("CARGO_PKG_VERSION")` at compile time, which reads from the crate's `Cargo.toml` and is pinned in `Cargo.lock`. Both must match `NEW_VERSION` or the Docker images CI produces will ship with the wrong version string on `/version`, `/info`, `/metrics`, and logs.

   Run this check before committing — any mismatch must be fixed (usually re-run step 4) before continuing:

   ```sh
   set -e
   V=NEW_VERSION
   for manifest in apps/ssp/Cargo.toml apps/scheduler/Cargo.toml; do
     grep -q "^version = \"$V\"$" "$manifest" || { echo "BAD: $manifest not at $V"; exit 1; }
   done
   grep -q "^version = \"$V\"$" <(grep -A1 '^name = "ssp"$' apps/ssp/Cargo.lock) \
     || { echo "BAD: apps/ssp/Cargo.lock not at $V — re-run cargo check"; exit 1; }
   grep -q "^version = \"$V\"$" <(grep -A1 '^name = "scheduler"$' apps/scheduler/Cargo.lock) \
     || { echo "BAD: apps/scheduler/Cargo.lock not at $V — re-run cargo check"; exit 1; }
   echo "OK: ssp + scheduler pinned to $V in both Cargo.toml and Cargo.lock"
   ```

6. **Commit**: Stage all changes and commit with message `vNEW_VERSION`.

7. **Tag**: Create tag `sp00ky/vNEW_VERSION` (always `sp00ky/v` prefix — never bare `v`).

8. **Push**: Push both the commit and tag to origin: `git push origin main && git push origin sp00ky/vNEW_VERSION`.

This triggers the `npm-publish.yml` (CLI binaries + npm packages) and `docker-publish.yml` (SSP + scheduler Docker images) GitHub Actions workflows. The Docker workflow builds from the tagged commit, so the verification in step 5 is what guarantees the published images will report the bumped version on their `/version` and `/info` endpoints.
