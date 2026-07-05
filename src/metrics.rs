use std::net::SocketAddr;

use metrics::{counter, describe_counter, describe_histogram, histogram};
use metrics_exporter_prometheus::{Matcher, PrometheusBuilder};
use tracing::{error, info};

const LATENCY_BUCKETS: &[f64] = &[
    0.000_01,  // 10μs
    0.000_025, // 25μs
    0.000_05,  // 50μs
    0.000_1,   // 100μs
    0.000_25,  // 250μs
    0.000_5,   // 500μs
    0.001,     // 1ms
    0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
];

pub struct Metrics {}

impl Metrics {
    pub fn record_ingested(&self, stream: &str, kind: &str) {
        counter!(
            "ingested_total",
            "stream" => stream.to_string(),
            "type" => kind.to_string(),
        )
        .increment(1);
    }

    pub fn record_lag(&self, stream: &str, seconds: f64) {
        histogram!("lag_seconds", "stream" => stream.to_string()).record(seconds);
    }

    pub fn record_parse_duration(&self, stream: &str, seconds: f64) {
        histogram!("parse_duration_seconds", "stream" => stream.to_string()).record(seconds);
    }

    pub fn record_commit_duration(&self, table: &str, seconds: f64) {
        histogram!("commit_duration_seconds", "table" => table.to_string()).record(seconds);
    }

    pub fn new() -> Self {
        describe_counter!(
            "ingested_total",
            "Total records ingested, by stream and type"
        );
        describe_histogram!(
            "lag_seconds",
            metrics::Unit::Seconds,
            "Time between a record's source timestamp and ingestion"
        );
        describe_histogram!(
            "parse_duration_seconds",
            metrics::Unit::Seconds,
            "Time spent parsing one source line"
        );
        describe_histogram!(
            "commit_duration_seconds",
            metrics::Unit::Seconds,
            "Time spent flushing one batch to ClickHouse, by table"
        );

        Self {}
    }

    pub fn install_prometheus(&self, addr: SocketAddr) -> Result<(), Box<dyn std::error::Error>> {
        match PrometheusBuilder::new()
            .set_buckets_for_metric(
                Matcher::Full("parse_duration_seconds".to_string()),
                LATENCY_BUCKETS,
            )
            .unwrap()
            .set_buckets_for_metric(Matcher::Full("lag_seconds".to_string()), LATENCY_BUCKETS)
            .unwrap()
            .set_buckets_for_metric(
                Matcher::Full("commit_duration_seconds".to_string()),
                LATENCY_BUCKETS,
            )
            .unwrap()
            .with_http_listener(addr)
            .install()
        {
            Ok(()) => {
                info!("Metrics listening {addr:?}");
                Ok(())
            }
            Err(e) => {
                error!(error = %e, "Failed to install Prometheus");
                Err(e.into())
            }
        }
    }
}
