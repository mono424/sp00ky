use opentelemetry::{KeyValue, metrics::MeterProvider};
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::{
    Resource,
    metrics::{MeterProviderBuilder, PeriodicReader, SdkMeterProvider},
};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicU64, Ordering},
};
use std::time::{Duration, Instant};

pub struct Metrics {
    meter: opentelemetry::metrics::Meter,
    pub ingest_counter: opentelemetry::metrics::Counter<u64>,
    pub ingest_duration: opentelemetry::metrics::Histogram<f64>,
    pub view_count: opentelemetry::metrics::UpDownCounter<i64>,
    pub edge_operations: opentelemetry::metrics::Counter<u64>,
    pub edge_publish_failures: opentelemetry::metrics::Counter<u64>,
    pub edge_lock_wait: opentelemetry::metrics::Histogram<f64>,
    pub edge_lock_hold: opentelemetry::metrics::Histogram<f64>,
    pub edge_publish: opentelemetry::metrics::Histogram<f64>,
    pub edge_transaction: opentelemetry::metrics::Histogram<f64>,
    pub ttl_cleanup_count: opentelemetry::metrics::Counter<u64>,

    // Internal tracking for rate calculation
    ingest_total: Arc<AtomicU64>,
}

struct RateState {
    last_count: u64,
    last_tick: Instant,
}

/// Default assumed cgroup memory ceiling, in MB. Mirrors the SSP `Resources`
/// entry in the control plane (`internal/vms/specs.go`); override with
/// `SPKY_SSP_MEMORY_LIMIT_MB` when a deployment carries a per-tenant
/// `infra_resources.ssp.memory_mb` override.
const DEFAULT_MEMORY_LIMIT_MB: u64 = 1024;

/// Resident set size of this process, in bytes.
///
/// Reads the `VmRSS` line of `/proc/self/status`, which is already in kB — the
/// `statm` alternative reports pages and would need a `sysconf(_SC_PAGESIZE)`
/// call (so, a `libc` dependency this crate doesn't otherwise carry) to be
/// correct on aarch64, where the page size isn't always 4K.
///
/// Returns `None` off Linux and on any parse failure, so callers have to treat
/// the metric as best-effort.
pub fn process_rss_bytes() -> Option<u64> {
    #[cfg(target_os = "linux")]
    {
        let status = std::fs::read_to_string("/proc/self/status").ok()?;
        let kb: u64 = status
            .lines()
            .find_map(|line| line.strip_prefix("VmRSS:"))?
            .split_whitespace()
            .next()?
            .parse()
            .ok()?;
        Some(kb.saturating_mul(1024))
    }
    #[cfg(not(target_os = "linux"))]
    {
        None
    }
}

/// Configured memory ceiling in bytes, from `SPKY_SSP_MEMORY_LIMIT_MB`.
pub fn memory_limit_bytes() -> u64 {
    std::env::var("SPKY_SSP_MEMORY_LIMIT_MB")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .filter(|mb| *mb > 0)
        .unwrap_or(DEFAULT_MEMORY_LIMIT_MB)
        * 1024
        * 1024
}

/// RSS as a fraction of the configured ceiling, clamped to `[0.0, 1.0]`.
///
/// This is what the heartbeat reports. The scheduler's `LeastLoad` strategy
/// sums it with `cpu_usage`, so it has to be a comparable 0..1 load figure
/// rather than a raw byte count.
pub fn memory_load_fraction() -> Option<f64> {
    let rss = process_rss_bytes()?;
    Some((rss as f64 / memory_limit_bytes() as f64).clamp(0.0, 1.0))
}

impl Metrics {
    pub fn new(provider: &SdkMeterProvider) -> Self {
        let meter = provider.meter("ssp");

        let ingest_total = Arc::new(AtomicU64::new(0));
        let count_ref = ingest_total.clone();

        // State for rate calculation (protected by Mutex for the callback)
        let rate_state = Arc::new(Mutex::new(RateState {
            last_count: 0,
            last_tick: Instant::now(),
        }));

        // Observable Gauge for Ingestions Per Minute
        let _ingest_rate = meter
            .u64_observable_gauge("ssp_ingest_rate_per_minute")
            .with_description("Ingestion rate per minute (calculated window)")
            .with_callback(move |observer| {
                let current_total = count_ref.load(Ordering::Relaxed);

                if let Ok(mut state) = rate_state.lock() {
                    let now = Instant::now();
                    let elapsed = now.duration_since(state.last_tick).as_secs_f64();
                    // Avoid division by zero or extremely small intervals
                    if elapsed.round() >= 60.0 {
                        let delta = current_total.saturating_sub(state.last_count);
                        let rate_per_sec = delta as f64 / elapsed;
                        let rate_per_min = rate_per_sec * 60.0;
                        observer.observe(rate_per_min.round() as u64, &[]);

                        // Update state
                        state.last_count = current_total;
                        state.last_tick = now;
                    }
                }
            })
            .build();

        // Resident memory. The SSP holds the whole circuit in RAM, so an OOM
        // kill is the dominant failure mode — and it arrives as a SIGKILL with
        // no log line, so without this gauge the only trace is the restart.
        // Observable rather than a counter: RSS is a level, not an event.
        let _memory_rss = meter
            .u64_observable_gauge("ssp_memory_rss_bytes")
            .with_description("Resident set size of the SSP process")
            .with_callback(|observer| {
                if let Some(rss) = process_rss_bytes() {
                    observer.observe(rss, &[]);
                }
            })
            .build();

        Self {
            meter: meter.clone(),
            ingest_counter: meter
                .u64_counter("ssp_ingest_total")
                .with_description("Total number of ingest operations")
                .build(),
            ingest_duration: meter
                .f64_histogram("ssp_ingest_duration_milliseconds")
                .with_description("Ingest operation duration")
                .build(),
            view_count: meter
                .i64_up_down_counter("ssp_views_active")
                .with_description("Number of active views")
                .build(),
            edge_operations: meter
                .u64_counter("ssp_edge_operations_total")
                .with_description("Total edge operations by type")
                .build(),
            edge_publish_failures: meter.u64_counter("ssp_edge_publish_failures_total").build(),
            edge_lock_wait: meter.f64_histogram("ssp_edge_lock_wait_milliseconds").build(),
            edge_lock_hold: meter.f64_histogram("ssp_edge_lock_hold_milliseconds").build(),
            edge_publish: meter.f64_histogram("ssp_edge_publish_milliseconds").build(),
            edge_transaction: meter.f64_histogram("ssp_edge_transaction_milliseconds").build(),
            ttl_cleanup_count: meter
                .u64_counter("ssp_ttl_cleanup_total")
                .with_description("Total queries removed by TTL expiry")
                .build(),
            ingest_total,
        }
    }

    /// Export backlog levels without touching the circuit or database.
    pub fn observe_publication(&self, publisher: ssp_node::edges::EdgePublisher, connection: crate::SharedDb) {
        type Read = fn(&ssp_protocol::PublicationMetrics) -> u64;
        let instruments: [(&str, Read); 6] = [
            ("ssp_edge_pending_batches", |s| s.pending_batches),
            ("ssp_edge_pending_operations", |s| s.pending_operations),
            ("ssp_edge_pending_bytes", |s| s.pending_bytes),
            ("ssp_edge_oldest_age_milliseconds", |s| s.oldest_age_ms),
            ("ssp_edge_parked_batches", |s| s.parked_batches),
            ("ssp_edge_overload_rejections", |s| s.overload_total),
        ];
        for (name, read) in instruments {
            let queue = publisher.clone();
            self.meter.u64_observable_gauge(name)
                .with_callback(move |observer| observer.observe(read(&queue.snapshot()), &[]))
                .build();
        }
        self.meter.u64_observable_gauge("ssp_edge_last_success_epoch_milliseconds")
            .with_callback(move |observer| {
                if let Some(at) = publisher.snapshot().last_success_at_ms { observer.observe(at, &[]); }
            }).build();
        self.meter.u64_observable_gauge("ssp_edge_connection_generation")
            .with_callback(move |observer| observer.observe(connection.reconnect_metrics().0, &[]))
            .build();
    }

    pub fn inc_ingest(&self, count: u64, _: &[KeyValue]) {
        self.ingest_total.fetch_add(count, Ordering::Relaxed);
    }
}

pub fn init_metrics() -> Result<(SdkMeterProvider, Metrics), anyhow::Error> {
    let endpoint = std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT").ok();

    let service_name = std::env::var("OTEL_SERVICE_NAME").unwrap_or_else(|_| "ssp".to_string());

    let resource = Resource::new(vec![KeyValue::new("service.name", service_name)]);

    let mut builder = MeterProviderBuilder::default().with_resource(resource);

    if let Some(endpoint) = endpoint {
        let exporter = opentelemetry_otlp::MetricExporter::builder()
            .with_tonic()
            .with_endpoint(&endpoint)
            .build()?;

        let reader = PeriodicReader::builder(exporter, opentelemetry_sdk::runtime::Tokio)
            .with_interval(Duration::from_secs(15))
            .build();

        builder = builder.with_reader(reader);
    }

    let provider = builder.build();
    let metrics = Metrics::new(&provider);

    Ok((provider, metrics))
}
