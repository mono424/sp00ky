//! Push `pools:` from sp00ky.yml into `_00_pool`.
//!
//! Same contract as `schedule_sync`: deploy owns the SPEC fields and nothing
//! else. `MERGE` keeps the operator's `paused` and the engine's breaker fields;
//! optional spec fields that were removed from the manifest are cleared
//! explicitly, because `MERGE` only ever writes the keys it is given; and a pool
//! that is no longer declared is deleted, which makes the engine tell its
//! machines to shut down (their next poll finds no pool).
//!
//! Called from the two real flows rather than from the shared internal-schema
//! step, because what a machine should run differs per environment: `spky dev`
//! knows a locally built image tag and a resolved dev environment, a cloud
//! deploy leaves both to the control plane.

use std::collections::BTreeMap;
use std::path::Path;

use anyhow::{Context, Result};
use serde_json::Value;

use crate::backend::Sp00kyConfig;
use crate::migrate::checksum_str;
use crate::pool_config::normalize_pool;
use crate::surreal_client::MigrationDB;

/// Where the rows are being written for, and so what a machine should run.
pub enum PoolEnv<'a> {
    /// `spky dev`: images are built locally and tagged per backend; environment
    /// comes from the app's `dev` env sources.
    Dev { project_dir: &'a Path },
    /// A cloud deploy. `manifests` are the backend manifests this deploy is
    /// sending to the control plane: the image reference it will resolve, the
    /// command and working directory (an imported image carries no metadata), and
    /// the environment with vault values already resolved.
    ///
    /// A pool whose backend has no manifest here (an `--only` deploy of something
    /// else, a restore) is left exactly as it is: writing a row without its
    /// command and environment would break the machines of a healthy pool.
    Cloud { manifests: &'a [Value] },
}

/// The tag `spky dev` builds a pool backend's image under.
pub fn dev_image_tag(backend: &str) -> String {
    format!("sp00ky-dev-pool-{backend}")
}

#[derive(Debug, Default, PartialEq, Eq)]
pub struct PoolSyncReport {
    pub created: usize,
    pub updated: usize,
    pub unchanged: usize,
    pub removed: usize,
}

impl PoolSyncReport {
    pub fn is_noop(&self) -> bool {
        self.created + self.updated + self.removed == 0
    }

    pub fn summary(&self) -> String {
        format!(
            "{} created, {} updated, {} unchanged, {} removed",
            self.created, self.updated, self.unchanged, self.removed
        )
    }
}

/// Optional spec fields deploy owns on `_00_pool`: cleared when absent.
const SPEC_OPTIONAL_FIELDS: &[&str] = &["machine_type", "max"];

pub fn record_literal(name: &str) -> String {
    format!("_00_pool:⟨{}⟩", name.replace('⟩', ""))
}

/// `["K=V", ...]` (how a backend manifest carries its environment) to a map.
fn env_map(manifest: &Value) -> BTreeMap<String, String> {
    manifest
        .get("env")
        .and_then(Value::as_array)
        .map(|vars| {
            vars.iter()
                .filter_map(Value::as_str)
                .filter_map(|kv| kv.split_once('='))
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect()
        })
        .unwrap_or_default()
}

/// One `(name, spec)` per pool that can be written in this environment, in the
/// shape the engine reads. Declared pools that are skipped (see
/// [`PoolEnv::Cloud`]) are simply absent; they are still declared, so `sync` does
/// not delete them.
pub fn resolve_rows(config: &Sp00kyConfig, env: &PoolEnv) -> Result<Vec<(String, Value)>> {
    let mut rows = Vec::new();
    for name in config.pools.keys() {
        let backend = config
            .backends()
            .find(|(_, app)| app.pool() == Some(name.as_str()));
        let mut manifest: Option<&Value> = None;
        let (image, vars) = match (env, backend) {
            (PoolEnv::Dev { project_dir }, Some((backend, app))) => (
                Some(dev_image_tag(backend)),
                crate::dev::resolve_env_for_dev(&app.env, project_dir)
                    .into_iter()
                    .collect(),
            ),
            (PoolEnv::Cloud { manifests }, Some((backend, _))) => {
                let found = manifests
                    .iter()
                    .find(|m| m.get("name").and_then(Value::as_str) == Some(backend));
                let Some(found) = found else { continue };
                manifest = Some(found);
                (
                    found
                        .get("image")
                        .and_then(Value::as_str)
                        .map(str::to_string),
                    env_map(found),
                )
            }
            _ => (None, BTreeMap::new()),
        };
        let mut row = normalize_pool(config, name, image.as_deref(), &vars)?;
        if let Some(manifest) = manifest {
            // What `docker import` strips from the image, plus the content hash:
            // a rebuilt image changes the spec hash, which is what rolls machines.
            for (from, to) in [
                ("cmd", "cmd"),
                ("working_dir", "workdir"),
                ("image_hash", "image_hash"),
            ] {
                if let Some(v) = manifest.get(from).filter(|v| !v.is_null()) {
                    row["container"][to] = v.clone();
                }
            }
        }
        // `spky dev` never creates cloud machines, whatever the manifest asks for
        // in production: every pool is containers on the dev network.
        if matches!(env, PoolEnv::Dev { .. }) {
            row["provider"] = Value::String("docker".into());
            if let Some(obj) = row.as_object_mut() {
                obj.remove("machine_type");
                obj.insert("locations".into(), Value::Array(Vec::new()));
            }
        }
        rows.push((name.clone(), row));
    }
    Ok(rows)
}

pub fn sync(
    client: &dyn MigrationDB,
    config: &Sp00kyConfig,
    env: &PoolEnv,
) -> Result<PoolSyncReport> {
    let rows = resolve_rows(config, env)?;
    let stored = read_stored_hashes(client)?;
    let mut report = PoolSyncReport::default();

    for (name, spec) in &rows {
        let hash = checksum_str(&serde_json::to_string(spec)?);
        if stored.get(name).map(String::as_str) == Some(hash.as_str()) {
            report.unchanged += 1;
            continue;
        }
        let id = record_literal(name);
        let cleared: String = SPEC_OPTIONAL_FIELDS
            .iter()
            .filter(|f| spec.get(**f).is_none())
            .map(|f| format!(", {f} = NONE"))
            .collect();
        // `container` is replaced whole (SET), not merged: MERGE is deep, so an
        // env var removed from the manifest would otherwise live on in the row.
        let sql = format!(
            "UPSERT {id} MERGE {patch}; UPDATE {id} SET container = {container}{cleared};",
            patch = merge_patch(spec, &hash)?,
            container = serde_json::to_string(spec.get("container").unwrap_or(&Value::Null))?,
        );
        client
            .execute(&sql)
            .with_context(|| format!("failed to sync pool '{name}'"))?;
        if stored.contains_key(name) {
            report.updated += 1;
        } else {
            report.created += 1;
        }
    }

    // Swept against what is DECLARED, not against what was written: a pool this
    // deploy had to skip is still wanted.
    for name in stored.keys() {
        if !config.pools.contains_key(name) {
            client
                .execute(&format!("DELETE {};", record_literal(name)))
                .with_context(|| format!("failed to remove pool '{name}'"))?;
            report.removed += 1;
        }
    }
    Ok(report)
}

fn merge_patch(spec: &Value, hash: &str) -> Result<String> {
    let mut patch = spec.as_object().cloned().unwrap_or_default();
    patch.remove("container");
    patch.insert("spec_hash".into(), Value::String(hash.to_string()));
    debug_assert!(
        !patch.contains_key("paused")
            && !patch.contains_key("boot_failures")
            && !patch.contains_key("breaker_until"),
        "deploy must never write operator- or engine-owned fields"
    );
    Ok(serde_json::to_string(&Value::Object(patch))?)
}

fn read_stored_hashes(client: &dyn MigrationDB) -> Result<BTreeMap<String, String>> {
    let responses = client
        .execute("SELECT name, spec_hash FROM _00_pool;")
        .context("failed to read existing pools")?;
    let mut out = BTreeMap::new();
    for response in responses {
        let Some(result) = response.result else {
            continue;
        };
        let rows = match result {
            Value::Array(rows) => rows,
            other => vec![other],
        };
        for row in rows {
            let Some(name) = row.get("name").and_then(Value::as_str) else {
                continue;
            };
            let hash = row
                .get("spec_hash")
                .and_then(Value::as_str)
                .unwrap_or_default();
            out.insert(name.to_string(), hash.to_string());
        }
    }
    Ok(out)
}

/// Non-fatal wrapper for the deploy flows: a bad pool must not fail a deploy
/// that otherwise succeeded, but it has to be loud.
pub fn sync_and_report(client: &dyn MigrationDB, config: &Sp00kyConfig, env: &PoolEnv) {
    match sync(client, config, env) {
        Ok(report) if report.is_noop() => {
            if report.unchanged > 0 {
                crate::ui::detail(format!("pools unchanged ({})", report.summary()));
            }
        }
        Ok(report) => crate::ui::step("Pools").done(format!("synced: {}", report.summary())),
        Err(e) => crate::ui::warn(format!("failed to sync pools: {e:#}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config() -> Sp00kyConfig {
        serde_yaml::from_str(
            "pools:\n  gpu-render: { autoscale: true, min: 0, max: 4, buffer: 1 }\napps:\n  renderer:\n    \
             type: backend\n    runOn: { pool: gpu-render }\n    deploy: { port: 8080 }\n    \
             method: { type: outbox, table: render_job, schema: ./r.surql }\n",
        )
        .unwrap()
    }

    #[test]
    fn dev_rows_carry_the_local_image_and_cloud_rows_leave_it_to_the_control_plane() {
        let dev = resolve_rows(
            &config(),
            &PoolEnv::Dev {
                project_dir: Path::new("."),
            },
        )
        .unwrap();
        assert_eq!(dev[0].0, "gpu-render");
        assert_eq!(dev[0].1["container"]["image"], "sp00ky-dev-pool-renderer");

        let manifests = [serde_json::json!({
            "name": "renderer", "image": "whitepawn/renderer", "image_hash": "sha256:abc",
            "cmd": "/renderer --port 8080", "working_dir": "/app",
            "env": ["RELAY_URL=wss://relay", "KEY=a=b"],
        })];
        let cloud = resolve_rows(
            &config(),
            &PoolEnv::Cloud {
                manifests: &manifests,
            },
        )
        .unwrap();
        let container = &cloud[0].1["container"];
        assert_eq!(container["image"], "whitepawn/renderer");
        assert_eq!(
            container["image_hash"], "sha256:abc",
            "a rebuilt image must change the spec hash"
        );
        assert_eq!(
            (container["cmd"].clone(), container["workdir"].clone()),
            ("/renderer --port 8080".into(), "/app".into())
        );
        assert_eq!(
            container["env"],
            serde_json::json!({ "RELAY_URL": "wss://relay", "KEY": "a=b" })
        );

        // No manifest for the pool's backend (an `--only` deploy of something
        // else): the pool is left alone rather than written half-empty.
        assert!(resolve_rows(&config(), &PoolEnv::Cloud { manifests: &[] })
            .unwrap()
            .is_empty());
    }

    #[test]
    fn a_cloud_pool_runs_as_local_containers_under_spky_dev() {
        let cfg: Sp00kyConfig = serde_yaml::from_str(
            "pools:\n  render:\n    provider: hetzner\n    machine: { type: cpx32, locations: [fsn1] }\n    min: 1\n\
             apps:\n  renderer:\n    type: backend\n    runOn: { pool: render }\n    deploy: { port: 8080 }\n    \
             method: { type: outbox, table: render_job, schema: ./r.surql }\n",
        )
        .unwrap();
        let dev = &resolve_rows(
            &cfg,
            &PoolEnv::Dev {
                project_dir: Path::new("."),
            },
        )
        .unwrap()[0]
            .1;
        assert_eq!(dev["provider"], "docker");
        assert!(dev.get("machine_type").is_none());
        let manifests = [serde_json::json!({ "name": "renderer", "image": "x/renderer" })];
        let cloud = &resolve_rows(
            &cfg,
            &PoolEnv::Cloud {
                manifests: &manifests,
            },
        )
        .unwrap()[0]
            .1;
        assert_eq!(
            (cloud["provider"].clone(), cloud["machine_type"].clone()),
            ("hetzner".into(), "cpx32".into())
        );
    }

    #[test]
    fn the_patch_never_touches_operator_or_engine_fields_and_keys_are_quoted() {
        let manifests = [serde_json::json!({ "name": "renderer", "image": "x/renderer" })];
        let rows = resolve_rows(
            &config(),
            &PoolEnv::Cloud {
                manifests: &manifests,
            },
        )
        .unwrap();
        let patch: Value = serde_json::from_str(&merge_patch(&rows[0].1, "h").unwrap()).unwrap();
        for owned_elsewhere in [
            "paused",
            "boot_failures",
            "breaker_until",
            "last_error",
            "container",
        ] {
            assert!(patch.get(owned_elsewhere).is_none(), "{owned_elsewhere}");
        }
        assert_eq!(patch["spec_hash"], "h");
        assert_eq!(record_literal("gpu-render"), "_00_pool:⟨gpu-render⟩");
    }
}
