//! `docker` machine provider: a pool machine is a container on this docker host.
//!
//! What `spky dev` uses, and what a small deployment can use for light jobs that
//! do not need their own VM. It talks to the Docker Engine API over the local
//! socket, so the scheduler image needs no docker CLI.
//!
//! **How a backend becomes a machine.** The backend's image is run unchanged,
//! with one twist: `spky-agent` is mounted in from a volume and becomes the
//! entrypoint, wrapping the image's own command.
//!
//! ```text
//! entrypoint: /spky/spky-agent
//! cmd:        -- <the image's ENTRYPOINT + CMD, or the pool's `cmd`>
//! ```
//!
//! The agent starts that command as its child, waits for it to be healthy,
//! registers with the scheduler and then pulls jobs for it. Nothing about the
//! backend image has to know it is running in a pool.
//!
//! The agent binary lives in a named volume, filled once from the agent image by
//! asking the agent to copy itself (`--install`): the agent image may be
//! `scratch`, so there is no `cp` to rely on.

use std::collections::HashMap;

use bollard::container::{
    Config, CreateContainerOptions, ListContainersOptions, RemoveContainerOptions,
    StartContainerOptions, WaitContainerOptions,
};
use bollard::errors::Error as DockerError;
use bollard::models::HostConfig;
use bollard::volume::CreateVolumeOptions;
use bollard::Docker;
use futures::StreamExt;
use pool_core::spec::machine_key;
use pool_core::{CreateMachine, MachineProvider, ProviderError, ProviderMachine};
use serde_json::Value;
use tokio::sync::Mutex;

const LABEL_POOL: &str = "spky.pool";
const LABEL_MACHINE: &str = "spky.machine";
const AGENT_MOUNT: &str = "/spky";
const AGENT_BIN: &str = "/spky/spky-agent";

#[derive(Debug, Clone)]
pub struct DockerProviderConfig {
    /// Image that carries the `spky-agent` binary for this platform.
    pub agent_image: String,
    /// Docker network machines join, so they can reach the scheduler by name.
    pub network: Option<String>,
    /// URL of the scheduler's pool listener AS SEEN FROM A MACHINE.
    pub pool_url: String,
}

pub struct DockerProvider {
    docker: Docker,
    cfg: DockerProviderConfig,
    /// Mints the bearer token a machine authenticates its polls with.
    token_for: Box<dyn Fn(&str) -> String + Send + Sync>,
    /// Serializes the one-time agent volume install.
    install: Mutex<()>,
}

fn container_name(machine_id: &str) -> String {
    format!("spky-m-{}", machine_key(machine_id))
}

/// One volume per agent image, so upgrading the agent never reuses a stale binary.
fn agent_volume(agent_image: &str) -> String {
    let safe: String = agent_image
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '-' })
        .collect();
    format!("spky-agent-{safe}")
}

fn is_not_found(e: &DockerError) -> bool {
    matches!(
        e,
        DockerError::DockerResponseServerError {
            status_code: 404,
            ..
        }
    )
}

fn is_conflict(e: &DockerError) -> bool {
    matches!(
        e,
        DockerError::DockerResponseServerError {
            status_code: 409,
            ..
        }
    )
}

fn provider_err(e: DockerError) -> ProviderError {
    match &e {
        DockerError::DockerResponseServerError { status_code, .. } if *status_code >= 500 => {
            ProviderError::Transient(e.to_string())
        }
        DockerError::RequestTimeoutError | DockerError::IOError { .. } => {
            ProviderError::Transient(e.to_string())
        }
        _ => ProviderError::Other(e.to_string()),
    }
}

/// The pool's `cmd` is one string in sp00ky.yml (`deploy.cmd: /renderer --flag`).
fn split_cmd(cmd: &str) -> Vec<String> {
    cmd.split_whitespace().map(str::to_string).collect()
}

impl DockerProvider {
    pub fn connect(
        cfg: DockerProviderConfig,
        token_for: Box<dyn Fn(&str) -> String + Send + Sync>,
    ) -> Result<Self, DockerError> {
        Ok(Self {
            docker: Docker::connect_with_local_defaults()?,
            cfg,
            token_for,
            install: Mutex::new(()),
        })
    }

    /// Make sure the agent binary is in its volume. Idempotent and cheap once done.
    async fn ensure_agent_volume(&self) -> Result<String, ProviderError> {
        let volume = agent_volume(&self.cfg.agent_image);
        let _guard = self.install.lock().await;
        match self.docker.inspect_volume(&volume).await {
            Ok(_) => return Ok(volume),
            Err(e) if is_not_found(&e) => {}
            Err(e) => return Err(provider_err(e)),
        }

        // Fill a scratch volume first and only name it on success would be ideal;
        // docker has no rename, so install and remove the volume again on failure.
        self.docker
            .create_volume(CreateVolumeOptions {
                name: volume.clone(),
                driver: "local".to_string(),
                driver_opts: HashMap::new(),
                labels: HashMap::from([("spky.agent".to_string(), self.cfg.agent_image.clone())]),
            })
            .await
            .map_err(provider_err)?;

        let installer = format!("{volume}-install");
        let _ = self.remove(&installer).await;
        let result = async {
            self.docker
                .create_container(
                    Some(CreateContainerOptions {
                        name: installer.clone(),
                        platform: None,
                    }),
                    Config {
                        image: Some(self.cfg.agent_image.clone()),
                        entrypoint: Some(vec!["/spky-agent".to_string()]),
                        cmd: Some(vec!["--install".to_string(), "/out/spky-agent".to_string()]),
                        host_config: Some(HostConfig {
                            binds: Some(vec![format!("{volume}:/out")]),
                            ..Default::default()
                        }),
                        ..Default::default()
                    },
                )
                .await?;
            self.docker
                .start_container(&installer, None::<StartContainerOptions<String>>)
                .await?;
            let mut wait = self
                .docker
                .wait_container(&installer, None::<WaitContainerOptions<String>>);
            while let Some(status) = wait.next().await {
                status?;
            }
            Ok::<(), DockerError>(())
        }
        .await;
        let _ = self.remove(&installer).await;
        if let Err(e) = result {
            let _ = self.docker.remove_volume(&volume, None).await;
            return Err(ProviderError::Other(format!(
                "could not install the agent from image `{}`: {e}",
                self.cfg.agent_image
            )));
        }
        Ok(volume)
    }

    async fn remove(&self, name: &str) -> Result<(), DockerError> {
        let opts = RemoveContainerOptions {
            force: true,
            v: true,
            ..Default::default()
        };
        match self.docker.remove_container(name, Some(opts)).await {
            Ok(()) => Ok(()),
            Err(e) if is_not_found(&e) => Ok(()),
            Err(e) => Err(e),
        }
    }

    /// The command the agent should run: the pool's own `cmd`, else whatever the
    /// image would have run by itself.
    async fn backend_command(
        &self,
        image: &str,
        container: &Value,
    ) -> Result<Vec<String>, ProviderError> {
        if let Some(cmd) = container
            .get("cmd")
            .and_then(Value::as_str)
            .filter(|c| !c.trim().is_empty())
        {
            return Ok(split_cmd(cmd));
        }
        let inspected = self.docker.inspect_image(image).await.map_err(|e| {
            if is_not_found(&e) {
                ProviderError::Other(format!("image `{image}` is not on this docker host"))
            } else {
                provider_err(e)
            }
        })?;
        let cfg = inspected.config.unwrap_or_default();
        let mut command = cfg.entrypoint.unwrap_or_default();
        command.extend(cfg.cmd.unwrap_or_default());
        if command.is_empty() {
            return Err(ProviderError::Other(format!(
                "image `{image}` has no ENTRYPOINT or CMD; set `deploy.cmd` on the backend"
            )));
        }
        Ok(command)
    }
}

#[async_trait::async_trait]
impl MachineProvider for DockerProvider {
    async fn create(&self, req: &CreateMachine) -> Result<ProviderMachine, ProviderError> {
        let name = container_name(&req.machine_id);
        let existing = |id: Option<String>| ProviderMachine {
            provider_id: id.unwrap_or_else(|| name.clone()),
            machine_id: req.machine_id.clone(),
        };
        // Idempotent on the machine id: a second call finds the first container.
        match self.docker.inspect_container(&name, None).await {
            Ok(found) => return Ok(existing(found.id)),
            Err(e) if is_not_found(&e) => {}
            Err(e) => return Err(provider_err(e)),
        }

        let image = req
            .container
            .get("image")
            .and_then(Value::as_str)
            .ok_or_else(|| {
                ProviderError::Other(format!("pool `{}` has no container image", req.pool))
            })?;
        let command = self.backend_command(image, &req.container).await?;
        let volume = self.ensure_agent_volume().await?;

        let mut cmd = vec!["--".to_string()];
        cmd.extend(command);
        let env = vec![
            format!("SPKY_POOL_URL={}", self.cfg.pool_url),
            format!("SPKY_MACHINE_ID={}", req.machine_id),
            format!("SPKY_MACHINE_TOKEN={}", (self.token_for)(&req.machine_id)),
        ];
        let labels = HashMap::from([
            (LABEL_POOL.to_string(), req.pool.clone()),
            (LABEL_MACHINE.to_string(), req.machine_id.clone()),
        ]);
        let created = self
            .docker
            .create_container(
                Some(CreateContainerOptions {
                    name: name.clone(),
                    platform: None,
                }),
                Config {
                    image: Some(image.to_string()),
                    entrypoint: Some(vec![AGENT_BIN.to_string()]),
                    cmd: Some(cmd),
                    env: Some(env),
                    labels: Some(labels),
                    working_dir: req
                        .container
                        .get("workdir")
                        .and_then(Value::as_str)
                        .map(str::to_string),
                    host_config: Some(HostConfig {
                        binds: Some(vec![format!("{volume}:{AGENT_MOUNT}:ro")]),
                        network_mode: self.cfg.network.clone(),
                        // Lets a machine reach a scheduler running on the host
                        // (spky dev with a local scheduler) on Linux too.
                        extra_hosts: Some(vec!["host.docker.internal:host-gateway".to_string()]),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
            )
            .await;
        let id = match created {
            Ok(created) => created.id,
            // Lost a race with ourselves (a retried create): same container.
            Err(e) if is_conflict(&e) => {
                let found = self
                    .docker
                    .inspect_container(&name, None)
                    .await
                    .map_err(provider_err)?;
                return Ok(existing(found.id));
            }
            Err(e) => return Err(provider_err(e)),
        };
        if let Err(e) = self
            .docker
            .start_container(&name, None::<StartContainerOptions<String>>)
            .await
        {
            let _ = self.remove(&name).await;
            return Err(provider_err(e));
        }
        Ok(ProviderMachine {
            provider_id: id,
            machine_id: req.machine_id.clone(),
        })
    }

    async fn destroy(
        &self,
        machine_id: &str,
        _provider_id: Option<&str>,
    ) -> Result<(), ProviderError> {
        // By name, which is derived from the machine id and so is known even when
        // the row never got as far as recording a provider id.
        self.remove(&container_name(machine_id))
            .await
            .map_err(provider_err)
    }

    async fn list(&self, pool: &str) -> Result<Vec<ProviderMachine>, ProviderError> {
        let filters = HashMap::from([("label".to_string(), vec![format!("{LABEL_POOL}={pool}")])]);
        let found = self
            .docker
            .list_containers(Some(ListContainersOptions {
                all: true,
                filters,
                ..Default::default()
            }))
            .await
            .map_err(provider_err)?;
        Ok(found
            .into_iter()
            .filter_map(|c| {
                let machine_id = c.labels.as_ref()?.get(LABEL_MACHINE)?.clone();
                Some(ProviderMachine {
                    provider_id: c.id.unwrap_or_default(),
                    machine_id,
                })
            })
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_are_stable_docker_safe_and_derived_from_the_machine_id() {
        assert_eq!(container_name("_00_machine:ab12cd"), "spky-m-ab12cd");
        assert_eq!(container_name("_00_machine:⟨ab12cd⟩"), "spky-m-ab12cd");
        assert_eq!(
            agent_volume("mono424/spooky-agent:0.1"),
            "spky-agent-mono424-spooky-agent-0-1"
        );
        assert_ne!(
            agent_volume("agent:1"),
            agent_volume("agent:2"),
            "an upgrade gets a fresh volume"
        );
    }

    #[test]
    fn a_pool_cmd_string_becomes_argv() {
        assert_eq!(
            split_cmd("/renderer --port 8080"),
            vec!["/renderer", "--port", "8080"]
        );
        assert!(split_cmd("   ").is_empty());
    }
}
