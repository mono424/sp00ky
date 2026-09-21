//! Machine provider port: the only way the engine touches infrastructure.
//!
//! Hosts implement it per provider name (`docker` against a local docker host,
//! `hetzner` by asking the cloud control plane). The engine needs three verbs and
//! leans on two promises:
//!
//! - **`create` is idempotent on `machine_id`.** The engine writes the
//!   `_00_machine` row first and creates second, so a crash in between is healed
//!   by simply calling `create` again with the same id. A provider must answer
//!   that second call with the machine it already made, never a second one.
//! - **`list` reports every machine it made for the pool**, tagged with the
//!   `machine_id` it was created under. That is how leaked machines are found.

use schedule_core::MaybeSendSync;
use serde_json::Value;

#[derive(Debug, Clone, PartialEq)]
pub struct CreateMachine {
    /// Full record id (`_00_machine:<key>`). The idempotency key.
    pub machine_id: String,
    pub pool: String,
    pub machine_type: Option<String>,
    pub locations: Vec<String>,
    pub slots: u32,
    /// `{ image, cmd, port, healthcheck, env, workdir }` from the pool spec.
    pub container: Value,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderMachine {
    /// The provider's own handle (container id, server id).
    pub provider_id: String,
    /// The `_00_machine` record id this machine was created for.
    pub machine_id: String,
}

#[derive(Debug, thiserror::Error)]
pub enum ProviderError {
    /// Worth retrying as is: rate limit, timeout, 5xx.
    #[error("provider temporarily unavailable: {0}")]
    Transient(String),
    /// Out of capacity / quota / over the account cap. Retrying soon is pointless.
    #[error("provider refused: {0}")]
    Refused(String),
    #[error("provider error: {0}")]
    Other(String),
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
pub trait MachineProvider: MaybeSendSync {
    async fn create(&self, req: &CreateMachine) -> Result<ProviderMachine, ProviderError>;

    /// Destroy by `machine_id` (always known) and `provider_id` (when the row got
    /// that far). Destroying something already gone is `Ok`.
    async fn destroy(
        &self,
        machine_id: &str,
        provider_id: Option<&str>,
    ) -> Result<(), ProviderError>;

    async fn list(&self, pool: &str) -> Result<Vec<ProviderMachine>, ProviderError>;
}
