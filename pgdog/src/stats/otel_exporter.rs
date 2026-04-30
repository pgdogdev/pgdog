//! OTLP push exporter.
//!
//! Periodically collects all metrics and POSTs them as OTLP JSON
//! to the configured endpoint (e.g. Datadog's `/api/v2/otlp/v1/metrics`).

use std::time::Duration;

use tokio::time::sleep;
use tracing::{info, warn};

use super::otel;
use super::{Clients, MirrorStatsMetrics, Pools, QueryCache, TwoPc};
use crate::config::config;

/// Maximum number of metrics per OTLP request to stay under endpoint payload limits.
const BATCH_SIZE: usize = 10;

/// Run the push exporter loop. Exits only if the task is cancelled.
pub async fn run() {
    let client = reqwest::Client::new();
    let otel_config = config().config.otel.clone();
    let interval = Duration::from_millis(otel_config.push_interval);

    let endpoint = match otel_config.endpoint {
        Some(ref url) => url.clone(),
        None => return,
    };

    info!(
        "OTEL exporter pushing metrics to {} every {:.1}s",
        endpoint,
        interval.as_secs_f64(),
    );

    loop {
        sleep(interval).await;

        let clients = Clients::load();
        let pools = Pools::load().into_metrics();
        let mirror = MirrorStatsMetrics::load();
        let query_cache = QueryCache::load().metrics();
        let two_pc = TwoPc::load();

        let mut all: Vec<&super::Metric> = vec![&clients, &two_pc];
        all.extend(pools.iter());
        all.extend(mirror.iter());
        all.extend(query_cache.iter());

        // Send batches in parallel to stay under the 512 KB payload limit.
        let futs: Vec<_> = all
            .chunks(BATCH_SIZE)
            .filter_map(|chunk| {
                let request = otel::build_request(chunk);
                let body = match serde_json::to_vec(&request) {
                    Ok(b) => b,
                    Err(err) => {
                        warn!("otel exporter: failed to serialize metrics: {}", err);
                        return None;
                    }
                };

                let mut req = client
                    .post(&endpoint)
                    .header("Content-Type", "application/json");

                if let Some(ref api_key) = otel_config.datadog_api_key {
                    req = req.header("DD-API-KEY", api_key);
                }

                for (k, v) in &otel_config.headers {
                    req = req.header(k.as_str(), v.as_str());
                }

                Some(req.body(body).send())
            })
            .collect();

        for result in futures::future::join_all(futs).await {
            match result {
                Ok(resp) if !resp.status().is_success() => {
                    let status = resp.status();
                    let body = resp.text().await.unwrap_or_default();
                    warn!("otel exporter: endpoint returned {}: {}", status, body);
                }
                Err(err) => {
                    warn!("otel exporter: failed to push metrics: {}", err);
                }
                _ => {}
            }
        }
    }
}

#[cfg(test)]
mod test {
    use crate::config::{self, ConfigAndUsers};
    use crate::stats::open_metric::{Measurement, MeasurementType};
    use crate::stats::otel;
    use crate::stats::pools::PoolMetric;
    use crate::stats::Metric;

    #[test]
    fn serialized_payload_is_valid_json() {
        config::set(ConfigAndUsers::default()).unwrap();

        let metric = Metric::new(PoolMetric {
            name: "sv_idle".into(),
            measurements: vec![Measurement {
                labels: vec![("user".into(), "test".into())],
                measurement: MeasurementType::Integer(7),
            }],
            help: "Idle servers".into(),
            unit: None,
            metric_type: None,
        });

        let request = otel::build_request(&[&metric]);
        let body = serde_json::to_vec(&request).expect("serialize");

        // Verify the output is valid JSON by parsing it back.
        let _: serde_json::Value = serde_json::from_slice(&body).expect("valid json");
    }
}
