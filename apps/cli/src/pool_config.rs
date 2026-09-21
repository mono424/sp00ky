//! `pools:` in sp00ky.yml, and `runOn:` on a backend.
//!
//! ```yaml
//! pools:
//!   render:
//!     provider: hetzner            # hetzner | docker (default: docker)
//!     machine: { type: cpx32, locations: [fsn1, nbg1] }
//!     slots: 1                     # concurrent jobs per machine
//!     min: 1                       # baseline that always runs
//!     autoscale: true              # false = fixed size (= min), jobs queue
//!     max: 8                       # required with autoscale
//!     buffer: 2                    # ready machines kept ABOVE what is in use
//!     idleTimeout: 10m
//!     recycle: job                 # job | never
//!     maxJobDuration: 8h
//!     maxLifetime: 24h
//!     lease: 90s
//!     bootTimeout: 5m
//!
//! apps:
//!   renderer:
//!     type: backend
//!     runOn: { pool: render }
//! ```
//!
//! This module owns the YAML shape, its validation, and the flattening into the
//! `_00_pool` row the engine reads (`packages/pool-core`). Like schedules, the
//! engine never reads sp00ky.yml: everything it needs is resolved here, at
//! deploy time, into the row.

use std::collections::BTreeMap;

use anyhow::{bail, Result};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::backend::{AppConfig, Sp00kyConfig};
use crate::schedule_config::parse_duration_ms;

/// Shortest lease deploy accepts. The agent polls six times per lease and gives
/// a job up a third of a lease early, so anything much shorter turns ordinary
/// network jitter into failovers.
const MIN_LEASE_SECS: i64 = 30;

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct PoolConfig {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider: Option<PoolProvider>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub machine: Option<PoolMachineConfig>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub slots: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub min: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub autoscale: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub buffer: Option<u32>,
    #[serde(
        default,
        rename = "idleTimeout",
        skip_serializing_if = "Option::is_none"
    )]
    pub idle_timeout: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub recycle: Option<PoolRecycle>,
    #[serde(
        default,
        rename = "maxJobDuration",
        skip_serializing_if = "Option::is_none"
    )]
    pub max_job_duration: Option<String>,
    #[serde(
        default,
        rename = "maxLifetime",
        skip_serializing_if = "Option::is_none"
    )]
    pub max_lifetime: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lease: Option<String>,
    #[serde(
        default,
        rename = "bootTimeout",
        skip_serializing_if = "Option::is_none"
    )]
    pub boot_timeout: Option<String>,
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum PoolProvider {
    /// Containers on the scheduler's own docker host. What `spky dev` always uses.
    #[default]
    Docker,
    /// One VM per machine, created by the cloud control plane.
    Hetzner,
}

impl PoolProvider {
    pub fn as_str(self) -> &'static str {
        match self {
            PoolProvider::Docker => "docker",
            PoolProvider::Hetzner => "hetzner",
        }
    }
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum PoolRecycle {
    #[default]
    Job,
    Never,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct PoolMachineConfig {
    #[serde(default, rename = "type", skip_serializing_if = "Option::is_none")]
    pub machine_type: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub locations: Vec<String>,
}

/// `runOn:` on a backend: its jobs run on machines from this pool instead of
/// being POSTed to one always-on container by the SSPs.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RunOnConfig {
    pub pool: String,
}

fn secs(label: &str, raw: Option<&str>, default_secs: i64) -> Result<i64> {
    match raw {
        None => Ok(default_secs),
        Some(raw) => {
            let ms = parse_duration_ms(raw).map_err(|e| anyhow::anyhow!("{label}: {e}"))?;
            Ok((ms / 1000).max(1))
        }
    }
}

/// Backends that run on `pool`, by app name.
fn backends_on<'a>(config: &'a Sp00kyConfig, pool: &str) -> Vec<(&'a str, &'a AppConfig)> {
    config
        .backends()
        .filter(|(_, app)| app.run_on.as_ref().is_some_and(|r| r.pool == pool))
        .collect()
}

pub fn validate_all(config: &Sp00kyConfig) -> Result<()> {
    for (name, app) in &config.apps {
        let Some(run_on) = &app.run_on else { continue };
        if !config.pools.contains_key(&run_on.pool) {
            bail!(
                "app '{name}' has runOn.pool '{}', but no such pool is declared under `pools:`",
                run_on.pool
            );
        }
        if app.method.as_ref().and_then(|m| m.table.as_ref()).is_none() {
            bail!("app '{name}' runs on a pool, so it needs an outbox `method` with a `table` (pool machines run outbox jobs)");
        }
        if app.deploy.as_ref().and_then(|d| d.port).is_none() {
            bail!("app '{name}' runs on a pool, so it needs `deploy.port` (the agent on the machine POSTs jobs to it)");
        }
    }

    for (name, pool) in &config.pools {
        crate::schedule_config::validate_name("pool", name)?;
        let label = format!("pool '{name}'");

        let autoscale = pool.autoscale.unwrap_or(false);
        let min = pool.min.unwrap_or(0);
        if pool.slots == Some(0) {
            bail!("{label}: slots must be at least 1");
        }
        if autoscale {
            let Some(max) = pool.max else {
                bail!("{label}: autoscale is on, so `max` is required (the ceiling is what bounds the bill)");
            };
            if max == 0 {
                bail!("{label}: max must be at least 1");
            }
            if min > max {
                bail!("{label}: min ({min}) is greater than max ({max})");
            }
            if pool.buffer.unwrap_or(0) > max {
                bail!(
                    "{label}: buffer ({}) is greater than max ({max}), it could never be filled",
                    pool.buffer.unwrap_or(0)
                );
            }
        } else {
            if pool.max.is_some() {
                bail!("{label}: `max` only applies with `autoscale: true` (a fixed pool's size is `min`)");
            }
            if pool.buffer.unwrap_or(0) > 0 {
                bail!("{label}: `buffer` only applies with `autoscale: true` (a fixed pool's size is `min`)");
            }
            if min == 0 {
                bail!("{label}: a fixed pool (autoscale off) with min 0 can never run a job; set `min`, or turn `autoscale` on");
            }
        }

        let lease = secs(&format!("{label} lease"), pool.lease.as_deref(), 90)?;
        if lease < MIN_LEASE_SECS {
            bail!("{label}: lease must be at least {MIN_LEASE_SECS}s (got {lease}s)");
        }
        secs(
            &format!("{label} idleTimeout"),
            pool.idle_timeout.as_deref(),
            600,
        )?;
        secs(
            &format!("{label} bootTimeout"),
            pool.boot_timeout.as_deref(),
            300,
        )?;
        let job = secs(
            &format!("{label} maxJobDuration"),
            pool.max_job_duration.as_deref(),
            28_800,
        )?;
        let life = secs(
            &format!("{label} maxLifetime"),
            pool.max_lifetime.as_deref(),
            86_400,
        )?;
        if life < job {
            bail!("{label}: maxLifetime is shorter than maxJobDuration, so a job could never run to its limit");
        }

        // v1: one backend per pool. A machine runs exactly one backend image, so a
        // shared pool would need per-backend machines, which is a different feature.
        let users = backends_on(config, name);
        match users.len() {
            0 => bail!("{label} is not used by any app (add `runOn: {{ pool: {name} }}` to a backend, or remove the pool)"),
            1 => {}
            _ => bail!(
                "{label} is used by {} apps ({}); a pool runs exactly one backend",
                users.len(),
                users.iter().map(|(n, _)| *n).collect::<Vec<_>>().join(", ")
            ),
        }
    }
    Ok(())
}

/// What the cloud control plane needs to know about each pool, sent with every
/// deploy: which provider and machine shape to create, and the ceiling it must
/// enforce on its own side. The scheduler states the same numbers when it asks
/// for a machine, but a cap is only a cap if the side holding the cloud
/// credentials checks it too.
pub fn cloud_pool_manifests(config: &Sp00kyConfig) -> Vec<Value> {
    config
        .pools
        .iter()
        .map(|(name, pool)| {
            let autoscale = pool.autoscale.unwrap_or(false);
            let min = pool.min.unwrap_or(0);
            let ceiling = if autoscale {
                pool.max.unwrap_or(min).max(min)
            } else {
                min
            };
            let machine = pool.machine.clone().unwrap_or_default();
            json!({
                "name": name,
                "provider": pool.provider.unwrap_or_default().as_str(),
                "machine_type": machine.machine_type,
                "locations": machine.locations,
                "slots": pool.slots.unwrap_or(1),
                "ceiling": ceiling,
                "backend": backends_on(config, name).first().map(|(n, _)| *n),
            })
        })
        .collect()
}

/// Flatten one pool into the spec fields of its `_00_pool` row. `image` is how
/// the provider should run the backend in this environment (a local tag in dev,
/// whatever the control plane resolved in the cloud); `env` is the backend's
/// resolved environment, which reaches the machine through the agent's
/// authenticated channel rather than through provider metadata.
pub fn normalize_pool(
    config: &Sp00kyConfig,
    name: &str,
    image: Option<&str>,
    env: &BTreeMap<String, String>,
) -> Result<Value> {
    let pool = config
        .pools
        .get(name)
        .ok_or_else(|| anyhow::anyhow!("pool '{name}' is not declared"))?;
    let users = backends_on(config, name);
    let Some((backend, app)) = users.first() else {
        bail!("pool '{name}' is not used by any app");
    };
    let table = app
        .method
        .as_ref()
        .and_then(|m| m.table.clone())
        .ok_or_else(|| anyhow::anyhow!("app '{backend}' has no outbox table"))?;
    let deploy = app.deploy.as_ref();
    let label = format!("pool '{name}'");

    let mut container = json!({
        "port": deploy.and_then(|d| d.port).unwrap_or(8080),
        "env": env,
    });
    if let Some(image) = image {
        container["image"] = json!(image);
    }
    if let Some(path) = deploy.and_then(|d| d.healthcheck.as_ref()) {
        container["healthcheck"] = json!(path);
    }
    if let Some(cmd) = deploy.and_then(|d| d.cmd.as_ref()) {
        container["cmd"] = json!(cmd);
    }

    let autoscale = pool.autoscale.unwrap_or(false);
    let machine = pool.machine.clone().unwrap_or_default();
    let mut row = json!({
        "name": name,
        "provider": pool.provider.unwrap_or_default().as_str(),
        "locations": machine.locations,
        "slots": pool.slots.unwrap_or(1),
        "min": pool.min.unwrap_or(0),
        "autoscale": autoscale,
        "buffer": if autoscale { pool.buffer.unwrap_or(0) } else { 0 },
        "idle_timeout_secs": secs(&label, pool.idle_timeout.as_deref(), 600)?,
        "recycle": match pool.recycle.unwrap_or_default() {
            PoolRecycle::Job => "job",
            PoolRecycle::Never => "never",
        },
        "max_job_duration_secs": secs(&label, pool.max_job_duration.as_deref(), 28_800)?,
        "max_lifetime_secs": secs(&label, pool.max_lifetime.as_deref(), 86_400)?,
        "lease_secs": secs(&label, pool.lease.as_deref(), 90)?,
        "boot_timeout_secs": secs(&label, pool.boot_timeout.as_deref(), 300)?,
        "backend": backend,
        "target_table": table,
        "container": container,
    });
    if let Some(t) = machine.machine_type {
        row["machine_type"] = json!(t);
    }
    if let (true, Some(max)) = (autoscale, pool.max) {
        row["max"] = json!(max);
    }
    Ok(row)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config(yaml: &str) -> Sp00kyConfig {
        serde_yaml::from_str(yaml).expect("yaml parses")
    }

    const APP: &str = "
apps:
  renderer:
    type: backend
    runOn: { pool: render }
    deploy: { port: 8080, healthcheck: /health, cmd: /renderer }
    method: { type: outbox, table: render_job, schema: ./render.surql }
";

    #[test]
    fn the_three_modes_validate() {
        for pool in [
            "{ autoscale: false, min: 2 }",
            "{ autoscale: true, min: 1, max: 8, buffer: 0 }",
            "{ autoscale: true, min: 0, max: 8, buffer: 2, lease: 5m, idleTimeout: 10m }",
        ] {
            let cfg = config(&format!("pools:\n  render: {pool}\n{APP}"));
            validate_all(&cfg).unwrap_or_else(|e| panic!("{pool}: {e}"));
        }
    }

    #[test]
    fn autoscale_demands_a_ceiling_and_a_fixed_pool_refuses_one() {
        let err = |pool: &str| {
            validate_all(&config(&format!("pools:\n  render: {pool}\n{APP}")))
                .unwrap_err()
                .to_string()
        };
        assert!(err("{ autoscale: true, min: 1 }").contains("`max` is required"));
        assert!(err("{ autoscale: true, min: 5, max: 2 }").contains("greater than max"));
        assert!(err("{ autoscale: true, max: 2, buffer: 3 }").contains("could never be filled"));
        assert!(err("{ autoscale: false, min: 1, max: 4 }")
            .contains("only applies with `autoscale: true`"));
        assert!(err("{ autoscale: false, min: 1, buffer: 1 }")
            .contains("only applies with `autoscale: true`"));
        assert!(err("{ min: 0 }").contains("can never run a job"));
        assert!(err("{ min: 1, lease: 5s }").contains("at least 30s"));
        assert!(err("{ min: 1, maxJobDuration: 8h, maxLifetime: 1h }")
            .contains("shorter than maxJobDuration"));
        assert!(err("{ min: 1, slots: 0 }").contains("slots must be at least 1"));
    }

    #[test]
    fn a_pool_and_its_backend_must_find_each_other() {
        let unknown = config(&APP.replace("render }", "missing }"));
        assert!(validate_all(&unknown)
            .unwrap_err()
            .to_string()
            .contains("no such pool"));

        let unused = config("pools:\n  render: { min: 1 }\napps: {}\n");
        assert!(validate_all(&unused)
            .unwrap_err()
            .to_string()
            .contains("not used by any app"));

        let shared = config(&format!(
            "pools:\n  render: {{ min: 1 }}\n{APP}  second:\n    type: backend\n    runOn: {{ pool: render }}\n    \
             deploy: {{ port: 1 }}\n    method: {{ type: outbox, table: other_job, schema: ./o.surql }}\n"
        ));
        assert!(validate_all(&shared)
            .unwrap_err()
            .to_string()
            .contains("exactly one backend"));

        let no_port = config(&format!(
            "pools:\n  render: {{ min: 1 }}\n{}",
            APP.replace("port: 8080, ", "")
        ));
        assert!(validate_all(&no_port)
            .unwrap_err()
            .to_string()
            .contains("deploy.port"));
    }

    #[test]
    fn unknown_keys_are_typos_not_silently_ignored() {
        let bad: Result<Sp00kyConfig, _> =
            serde_yaml::from_str(&format!("pools:\n  render: {{ min: 1, bufer: 2 }}\n{APP}"));
        assert!(bad.is_err());
    }

    #[test]
    fn normalizes_into_the_row_the_engine_reads() {
        let cfg = config(&format!(
            "pools:\n  render:\n    provider: hetzner\n    machine: {{ type: cpx32, locations: [fsn1, nbg1] }}\n    \
             autoscale: true\n    min: 1\n    max: 8\n    buffer: 2\n    lease: 5m\n    recycle: never\n{APP}"
        ));
        validate_all(&cfg).unwrap();
        let env = BTreeMap::from([("RELAY_URL".to_string(), "wss://relay".to_string())]);
        let row = normalize_pool(&cfg, "render", Some("renderer:dev"), &env).unwrap();
        assert_eq!(row["provider"], "hetzner");
        assert_eq!(row["machine_type"], "cpx32");
        assert_eq!(row["locations"], json!(["fsn1", "nbg1"]));
        assert_eq!(
            (
                row["min"].clone(),
                row["max"].clone(),
                row["buffer"].clone()
            ),
            (json!(1), json!(8), json!(2))
        );
        assert_eq!(row["lease_secs"], 300);
        assert_eq!(row["recycle"], "never");
        assert_eq!(row["backend"], "renderer");
        assert_eq!(row["target_table"], "render_job");
        assert_eq!(
            row["container"],
            json!({ "port": 8080, "healthcheck": "/health", "cmd": "/renderer",
                    "image": "renderer:dev", "env": { "RELAY_URL": "wss://relay" } })
        );
    }

    #[test]
    fn the_control_plane_is_told_each_pools_ceiling() {
        let cfg = config(&format!(
            "pools:\n  render:\n    provider: hetzner\n    machine: {{ type: cpx32 }}\n    autoscale: true\n    \
             min: 1\n    max: 8\n{APP}"
        ));
        let sent = cloud_pool_manifests(&cfg);
        assert_eq!(sent.len(), 1);
        assert_eq!(
            (sent[0]["name"].clone(), sent[0]["ceiling"].clone()),
            (json!("render"), json!(8))
        );
        assert_eq!(
            (sent[0]["provider"].clone(), sent[0]["backend"].clone()),
            (json!("hetzner"), json!("renderer"))
        );

        let fixed = config(&format!("pools:\n  render: {{ min: 3 }}\n{APP}"));
        assert_eq!(
            cloud_pool_manifests(&fixed)[0]["ceiling"],
            3,
            "a fixed pool's ceiling is its size"
        );
    }

    #[test]
    fn a_fixed_pool_never_carries_a_max_or_a_buffer() {
        let cfg = config(&format!("pools:\n  render: {{ min: 3 }}\n{APP}"));
        let row = normalize_pool(&cfg, "render", None, &BTreeMap::new()).unwrap();
        assert!(
            row.get("max").is_none(),
            "absent, so the row stores NONE and not NULL"
        );
        assert_eq!(
            (row["autoscale"].clone(), row["buffer"].clone()),
            (json!(false), json!(0))
        );
        assert!(row["container"].get("image").is_none());
    }
}
