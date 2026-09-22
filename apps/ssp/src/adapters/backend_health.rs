use ssp_node::ports::{BackendCounts, BackendHealth, BackendSpec};

/// `ssp_node::BackendHealth` over the `maintenance` crate's live cache +
/// shared-config pair (standalone mode). `PUT /backends` reconciles both.
pub struct MaintenanceBackendHealth {
    pub cache: maintenance::BackendHealthCache,
    pub configs: maintenance::SharedBackendConfigs,
}

#[async_trait::async_trait]
impl BackendHealth for MaintenanceBackendHealth {
    async fn counts(&self) -> BackendCounts {
        let backends = self.cache.read().await;
        let mut c = BackendCounts { total: backends.len(), ..Default::default() };
        for b in backends.iter() {
            match b.status {
                maintenance::BackendStatus::Healthy => c.healthy += 1,
                maintenance::BackendStatus::Unhealthy => c.unhealthy += 1,
                maintenance::BackendStatus::Unreachable => c.unreachable += 1,
                // Pool-backed statuses never occur here: the standalone SSP
                // has no pool sweep to write them.
                maintenance::BackendStatus::Unknown
                | maintenance::BackendStatus::Idle
                | maintenance::BackendStatus::Starting => {}
            }
        }
        c
    }

    async fn update(&self, backends: Vec<BackendSpec>) {
        let new: Vec<maintenance::BackendHealthConfig> = backends
            .into_iter()
            .map(|s| maintenance::BackendHealthConfig {
                name: s.name,
                url: s.url,
                healthcheck: s.healthcheck,
                port: s.port,
                env: s.env,
            })
            .collect();
        maintenance::update_backends(&self.configs, &self.cache, new).await;
    }
}
