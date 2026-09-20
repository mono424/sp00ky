---
name: bump-version
description: Bump the canary version across all packages, commit, tag, and push to trigger CI/CD.
user_invocable: true
---

# Bump Version

Bump the canary version number across all `package.json`, `Cargo.toml`, and manifest files, regenerate the root Cargo.lock, verify the SSP + scheduler binaries will report the new version, then commit, tag, and push.

## Steps

1. **Determine current version**: Read the current version from the root `package.json` (e.g. `0.0.1-canary.50`).

2. **Calculate next version**: Increment the canary number by 1 (e.g. `0.0.1-canary.50` → `0.0.1-canary.51`).

3. **Run the bump script**: Run `node scripts/bump-version.mjs NEW_VERSION` from the repo root. This updates all workspace `package.json` files, CLI platform packages, Chrome manifest, and all `Cargo.toml` files in one go.

4. **Regenerate the Cargo.lock**: Run `cargo check` once from the **repo root**. `apps/cli`, `apps/scheduler` and `apps/ssp` are all members of the root workspace, so the root `Cargo.lock` is the only lockfile any of them reads — cargo ignores a `Cargo.lock` inside a workspace member. (There used to be one in each of those directories, frozen at whatever version they held when the crates joined the workspace. They were inert, but this skill verified against them and so failed every release; they have been deleted.)

5. **Verify version is propagated to SSP + scheduler**: The SSP and scheduler binaries embed their version via `env!("CARGO_PKG_VERSION")` at compile time, which reads from the crate's `Cargo.toml` and is pinned in the root `Cargo.lock`. Both must match `NEW_VERSION` or the Docker images CI produces will ship with the wrong version string on `/version`, `/info`, `/metrics`, and logs.

   Note the SSP's binary crate is named `ssp-server` (`apps/ssp`), not `ssp` — `ssp` is the DBSP circuit library in `packages/ssp`.

   Run this check before committing — any mismatch must be fixed (usually re-run step 4) before continuing:

   ```sh
   set -e
   V=NEW_VERSION
   for manifest in apps/ssp/Cargo.toml apps/scheduler/Cargo.toml; do
     grep -q "^version = \"$V\"$" "$manifest" || { echo "BAD: $manifest not at $V"; exit 1; }
   done
   # A plain pipe, not `grep -q ... <(grep ...)`: process substitution
   # false-negatives where `grep` is aliased to ugrep, which reads as a bad lock.
   for crate in ssp-server scheduler; do
     grep -A1 "^name = \"$crate\"\$" Cargo.lock | grep -q "^version = \"$V\"$" \
       || { echo "BAD: root Cargo.lock has $crate at another version — re-run cargo check"; exit 1; }
   done
   echo "OK: ssp-server + scheduler pinned to $V in Cargo.toml and the root Cargo.lock"
   ```

6. **Commit**: Stage all changes and commit with message `vNEW_VERSION`.

7. **Tag**: Create tag `sp00ky/vNEW_VERSION` (always `sp00ky/v` prefix — never bare `v`).

8. **Push**: Push both the commit and tag to origin: `git push origin main && git push origin sp00ky/vNEW_VERSION`.

This triggers the `npm-publish.yml` (CLI binaries + npm packages) and `docker-publish.yml` (SSP + scheduler Docker images) GitHub Actions workflows. The Docker workflow builds from the tagged commit, so the verification in step 5 is what guarantees the published images will report the bumped version on their `/version` and `/info` endpoints.
