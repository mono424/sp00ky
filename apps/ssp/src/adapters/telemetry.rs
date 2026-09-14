use std::sync::Arc;

use ssp_node::Telemetry;

use crate::metrics::Metrics;

/// `ssp_node::Telemetry` over the existing OpenTelemetry instruments.
/// Port names map onto the named instruments in `crate::metrics::Metrics`;
/// unknown names are dropped (debug-logged) rather than silently minted, so
/// the instrument list stays the single source of truth.
pub struct OtelTelemetry {
    metrics: Arc<Metrics>,
}

impl OtelTelemetry {
    pub fn new(metrics: Arc<Metrics>) -> Self {
        Self { metrics }
    }
}

impl Telemetry for OtelTelemetry {
    fn counter(&self, name: &'static str, value: u64) {
        match name {
            "ingest" => {
                self.metrics.ingest_counter.add(value, &[]);
                self.metrics.inc_ingest(value, &[]);
            }
            "edge_operations" => self.metrics.edge_operations.add(value, &[]),
            "edge_publish_failures" => self.metrics.edge_publish_failures.add(value, &[]),
            "ttl_cleanup" => self.metrics.ttl_cleanup_count.add(value, &[]),
            other => tracing::debug!(name = other, "unmapped telemetry counter"),
        }
    }

    fn histogram_ms(&self, name: &'static str, value: f64) {
        match name {
            "edge_lock_wait" => self.metrics.edge_lock_wait.record(value, &[]),
            "edge_lock_hold" => self.metrics.edge_lock_hold.record(value, &[]),
            "edge_publish" => self.metrics.edge_publish.record(value, &[]),
            "edge_transaction" => self.metrics.edge_transaction.record(value, &[]),
            "ingest_duration" => self.metrics.ingest_duration.record(value, &[]),
            other => tracing::debug!(name = other, "unmapped telemetry histogram"),
        }
    }

    fn gauge_add(&self, name: &'static str, delta: i64) {
        match name {
            "view_count" => self.metrics.view_count.add(delta, &[]),
            other => tracing::debug!(name = other, "unmapped telemetry gauge"),
        }
    }
}
