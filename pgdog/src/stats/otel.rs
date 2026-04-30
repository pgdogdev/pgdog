//! OpenTelemetry Protocol (OTLP) JSON renderer.
//!
//! Converts the existing `OpenMetric` trait objects into the OTLP JSON format
//! (`ExportMetricsServiceRequest`) compatible with Datadog's OTLP ingest endpoint.

use std::collections::HashMap;
use std::env;
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

use once_cell::sync::Lazy;
use serde::Serialize;

use crate::util::hostname;

use super::open_metric::{MeasurementType, Metric};

static RESOURCE_ATTRIBUTES: Lazy<Vec<KeyValue>> = Lazy::new(resource_attributes);

/// Identity of a single counter data point for delta tracking.
#[derive(Hash, Eq, PartialEq, Clone)]
struct CounterKey {
    metric: String,
    labels: Vec<(String, String)>,
}

/// Previous cumulative values for delta computation.
static PREV_COUNTERS: Lazy<Mutex<HashMap<CounterKey, f64>>> =
    Lazy::new(|| Mutex::new(HashMap::new()));

fn now_nanos() -> String {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
        .to_string()
}

/// Top-level OTLP metrics export request.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ExportMetricsServiceRequest {
    pub resource_metrics: Vec<ResourceMetrics>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ResourceMetrics {
    pub resource: Resource,
    pub scope_metrics: Vec<ScopeMetrics>,
}

#[derive(Serialize)]
pub struct Resource {
    pub attributes: Vec<KeyValue>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ScopeMetrics {
    pub scope: Scope,
    pub metrics: Vec<OtelMetric>,
}

#[derive(Serialize)]
pub struct Scope {
    pub name: String,
    pub version: String,
}

#[derive(Serialize)]
pub struct OtelMetric {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    pub unit: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gauge: Option<Gauge>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sum: Option<Sum>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Gauge {
    pub data_points: Vec<NumberDataPoint>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Sum {
    /// 1 = DELTA, 2 = CUMULATIVE
    pub aggregation_temporality: u32,
    pub is_monotonic: bool,
    pub data_points: Vec<NumberDataPoint>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct NumberDataPoint {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub start_time_unix_nano: Option<String>,
    pub time_unix_nano: String,
    pub as_double: f64,
    pub attributes: Vec<KeyValue>,
}

#[derive(Serialize, Clone)]
pub struct KeyValue {
    pub key: String,
    pub value: AttributeValue,
}

#[derive(Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct AttributeValue {
    pub string_value: String,
}

/// Build resource attributes from defaults, `OTEL_RESOURCE_ATTRIBUTES`, and `OTEL_SERVICE_NAME`.
///
/// Per the OpenTelemetry spec:
/// - `OTEL_RESOURCE_ATTRIBUTES` is a comma-separated list of `key=value` pairs
/// - `OTEL_SERVICE_NAME` overrides `service.name` from any other source
///
/// Later insertions override earlier ones (HashMap semantics).
fn resource_attributes() -> Vec<KeyValue> {
    let mut attrs: HashMap<String, String> = HashMap::new();

    // Defaults (OTEL semantic conventions).
    attrs.insert("service.name".into(), "pgdog".into());
    attrs.insert(
        "service.instance.id".into(),
        crate::util::instance_id().into(),
    );

    let host = hostname();
    if !host.is_empty() {
        attrs.insert("host.name".into(), host.to_string());
    }

    // OTEL_RESOURCE_ATTRIBUTES: key1=value1,key2=value2
    if let Ok(raw) = env::var("OTEL_RESOURCE_ATTRIBUTES") {
        for pair in raw.split(',') {
            let pair = pair.trim();
            if let Some((k, v)) = pair.split_once('=') {
                attrs.insert(percent_decode(k.trim()), percent_decode(v.trim()));
            }
        }
    }

    // OTEL_SERVICE_NAME takes highest precedence.
    if let Ok(name) = env::var("OTEL_SERVICE_NAME") {
        attrs.insert("service.name".into(), name);
    }

    attrs
        .into_iter()
        .map(|(key, value)| KeyValue {
            key,
            value: AttributeValue {
                string_value: value,
            },
        })
        .collect()
}

/// Decode percent-encoded characters in OTEL_RESOURCE_ATTRIBUTES keys/values.
fn percent_decode(s: &str) -> String {
    let mut result = String::with_capacity(s.len());
    let mut chars = s.bytes();
    while let Some(b) = chars.next() {
        if b == b'%' {
            let hi = chars.next();
            let lo = chars.next();
            if let (Some(hi), Some(lo)) = (hi, lo) {
                let hex = [hi, lo];
                if let Ok(s) = std::str::from_utf8(&hex) {
                    if let Ok(byte) = u8::from_str_radix(s, 16) {
                        result.push(byte as char);
                        continue;
                    }
                }
            }
            // Malformed sequence — pass through as-is.
            result.push('%');
        } else {
            result.push(b as char);
        }
    }
    result
}

fn measurement_to_f64(m: &MeasurementType) -> f64 {
    match m {
        MeasurementType::Float(f) => *f,
        MeasurementType::Integer(i) => *i as f64,
        MeasurementType::Millis(ms) => *ms as f64,
    }
}

/// Build an `ExportMetricsServiceRequest` from a collection of `Metric` objects.
pub fn build_request(metrics: &[&Metric]) -> ExportMetricsServiceRequest {
    let now = now_nanos();
    let config = crate::config::config();
    let prefix = config
        .config
        .general
        .openmetrics_namespace
        .as_deref()
        .map(|s| s.trim_end_matches(['.', '_']))
        .unwrap_or("pgdog");

    let common_attrs = &*RESOURCE_ATTRIBUTES;

    let otel_metrics: Vec<OtelMetric> = metrics
        .iter()
        .map(|metric| {
            let name = format!("{}.{}", prefix, metric.name());
            let is_counter = metric.metric_type() == "counter";

            let data_points: Vec<NumberDataPoint> = metric
                .measurements()
                .iter()
                .filter_map(|m| {
                    let cumulative = measurement_to_f64(&m.measurement);

                    let as_double = if is_counter {
                        let key = CounterKey {
                            metric: name.clone(),
                            labels: m.labels.clone(),
                        };
                        let mut prev = PREV_COUNTERS.lock().expect("counter lock");
                        let delta = cumulative - prev.get(&key).copied().unwrap_or(0.0);
                        prev.insert(key, cumulative);

                        // Skip negative deltas (counter reset).
                        if delta < 0.0 {
                            return None;
                        }
                        delta
                    } else {
                        cumulative
                    };

                    let mut attributes: Vec<KeyValue> = m
                        .labels
                        .iter()
                        .map(|(k, v)| KeyValue {
                            key: k.clone(),
                            value: AttributeValue {
                                string_value: v.clone(),
                            },
                        })
                        .collect();

                    attributes.extend(common_attrs.iter().map(|a| KeyValue {
                        key: a.key.clone(),
                        value: AttributeValue {
                            string_value: a.value.string_value.clone(),
                        },
                    }));

                    Some(NumberDataPoint {
                        start_time_unix_nano: None,
                        time_unix_nano: now.clone(),
                        as_double,
                        attributes,
                    })
                })
                .collect();

            let (gauge, sum) = if is_counter {
                (
                    None,
                    Some(Sum {
                        aggregation_temporality: 1, // DELTA
                        is_monotonic: true,
                        data_points,
                    }),
                )
            } else {
                (Some(Gauge { data_points }), None)
            };

            OtelMetric {
                name,
                description: metric.help(),
                unit: metric.unit().unwrap_or_else(|| "1".into()),
                gauge,
                sum,
            }
        })
        .collect();

    ExportMetricsServiceRequest {
        resource_metrics: vec![ResourceMetrics {
            resource: Resource {
                attributes: RESOURCE_ATTRIBUTES.clone(),
            },
            scope_metrics: vec![ScopeMetrics {
                scope: Scope {
                    name: crate::config::config()
                        .config
                        .general
                        .openmetrics_namespace
                        .as_deref()
                        .map(|s| s.trim_end_matches(['.', '_']).to_string())
                        .unwrap_or_else(|| "pgdog".into()),
                    version: env!("CARGO_PKG_VERSION").into(),
                },
                metrics: otel_metrics,
            }],
        }],
    }
}

#[cfg(test)]
mod test {
    use crate::stats::open_metric::{Measurement, MeasurementType};
    use crate::stats::pools::PoolMetric;

    use super::*;

    #[test]
    fn gauge_metric_produces_gauge_json() {
        let metric = Metric::new(PoolMetric {
            name: "sv_idle".into(),
            measurements: vec![Measurement {
                labels: vec![("user".into(), "alice".into())],
                measurement: MeasurementType::Integer(3),
            }],
            help: "Idle servers".into(),
            unit: None,
            metric_type: None,
        });

        let request = build_request(&[&metric]);
        let json = serde_json::to_string_pretty(&request).expect("serialize");

        assert!(json.contains("\"gauge\""));
        assert!(!json.contains("\"sum\""));
        assert!(json.contains("sv_idle"));
        assert!(json.contains("\"asDouble\": 3.0"));
        assert!(json.contains("\"alice\""));
    }

    #[test]
    fn counter_metric_produces_sum_json() {
        let metric = Metric::new(PoolMetric {
            name: "total_query_count".into(),
            measurements: vec![Measurement {
                labels: vec![],
                measurement: MeasurementType::Integer(42),
            }],
            help: "Total queries".into(),
            unit: None,
            metric_type: Some("counter".into()),
        });

        let request = build_request(&[&metric]);
        let json = serde_json::to_string(&request).expect("serialize");

        assert!(json.contains("\"sum\""));
        assert!(!json.contains("\"gauge\""));
        assert!(json.contains("\"isMonotonic\":true"));
        assert!(json.contains("\"aggregationTemporality\":1"));
    }

    #[test]
    fn namespace_used_as_scope_name() {
        use crate::config::{self, ConfigAndUsers};

        let mut cfg = ConfigAndUsers::default();
        cfg.config.general.openmetrics_namespace = Some("pgdog".into());
        config::set(cfg).expect("set config");

        let metric = Metric::new(PoolMetric {
            name: "clients".into(),
            measurements: vec![Measurement {
                labels: vec![],
                measurement: MeasurementType::Integer(10),
            }],
            help: "Clients".into(),
            unit: None,
            metric_type: None,
        });

        let request = build_request(&[&metric]);
        let scope = &request.resource_metrics[0].scope_metrics[0].scope;
        assert_eq!(scope.name, "pgdog");

        let otel_metric = &request.resource_metrics[0].scope_metrics[0].metrics[0];
        assert_eq!(otel_metric.name, "pgdog.clients");
    }

    #[test]
    fn multiple_data_points_with_labels() {
        let metric = Metric::new(PoolMetric {
            name: "sv_active".into(),
            measurements: vec![
                Measurement {
                    labels: vec![
                        ("user".into(), "alice".into()),
                        ("shard".into(), "0".into()),
                    ],
                    measurement: MeasurementType::Integer(5),
                },
                Measurement {
                    labels: vec![("user".into(), "bob".into()), ("shard".into(), "1".into())],
                    measurement: MeasurementType::Integer(3),
                },
            ],
            help: "Active servers".into(),
            unit: Some("connections".into()),
            metric_type: None,
        });

        let request = build_request(&[&metric]);
        let scope = &request.resource_metrics[0].scope_metrics[0];
        let otel_metric = &scope.metrics[0];

        assert_eq!(otel_metric.unit, "connections");
        assert!(otel_metric.gauge.is_some());

        let points = &otel_metric.gauge.as_ref().expect("gauge").data_points;
        assert_eq!(points.len(), 2);
        assert_eq!(points[0].as_double, 5.0);
        assert_eq!(points[1].as_double, 3.0);
        assert_eq!(points[0].attributes[0].key, "user");
        assert_eq!(points[0].attributes[0].value.string_value, "alice");
    }

    #[test]
    fn millis_measurement_converts_to_double() {
        let metric = Metric::new(PoolMetric {
            name: "total_xact_time".into(),
            measurements: vec![Measurement {
                labels: vec![],
                measurement: MeasurementType::Millis(12345),
            }],
            help: "Transaction time".into(),
            unit: None,
            metric_type: Some("counter".into()),
        });

        let request = build_request(&[&metric]);
        let sum = &request.resource_metrics[0].scope_metrics[0].metrics[0]
            .sum
            .as_ref()
            .expect("sum");
        assert_eq!(sum.data_points[0].as_double, 12345.0);
    }

    #[test]
    fn float_measurement_preserves_value() {
        let metric = Metric::new(PoolMetric {
            name: "maxwait".into(),
            measurements: vec![Measurement {
                labels: vec![],
                measurement: MeasurementType::Float(1.234),
            }],
            help: "Max wait".into(),
            unit: Some("seconds".into()),
            metric_type: None,
        });

        let request = build_request(&[&metric]);
        let gauge = &request.resource_metrics[0].scope_metrics[0].metrics[0]
            .gauge
            .as_ref()
            .expect("gauge");
        assert!((gauge.data_points[0].as_double - 1.234).abs() < f64::EPSILON);
    }

    #[test]
    fn service_name_and_scope() {
        // Ensure no env overrides leak from other tests.
        unsafe {
            std::env::remove_var("OTEL_SERVICE_NAME");
            std::env::remove_var("OTEL_RESOURCE_ATTRIBUTES");
        }

        let metric = Metric::new(PoolMetric {
            name: "test".into(),
            measurements: vec![],
            help: "test".into(),
            unit: None,
            metric_type: None,
        });

        let request = build_request(&[&metric]);
        let resource = &request.resource_metrics[0].resource;

        let svc = resource
            .attributes
            .iter()
            .find(|a| a.key == "service.name")
            .unwrap();
        assert_eq!(svc.value.string_value, "pgdog");

        let inst = resource
            .attributes
            .iter()
            .find(|a| a.key == "service.instance.id")
            .unwrap();
        assert!(!inst.value.string_value.is_empty());

        let scope = &request.resource_metrics[0].scope_metrics[0].scope;
        assert_eq!(scope.name, "pgdog");
    }

    #[test]
    fn otel_env_var_overrides() {
        // Clean slate.
        unsafe {
            std::env::remove_var("OTEL_SERVICE_NAME");
            std::env::remove_var("OTEL_RESOURCE_ATTRIBUTES");

            // 1. OTEL_SERVICE_NAME overrides service.name.
            std::env::set_var("OTEL_SERVICE_NAME", "my-pgdog");
            let attrs = resource_attributes();
            let svc = attrs.iter().find(|a| a.key == "service.name").unwrap();
            assert_eq!(svc.value.string_value, "my-pgdog");
            std::env::remove_var("OTEL_SERVICE_NAME");

            // 2. OTEL_RESOURCE_ATTRIBUTES adds and overrides.
            std::env::set_var("OTEL_RESOURCE_ATTRIBUTES", "env=prod,service.name=custom");
            let attrs = resource_attributes();
            let svc = attrs.iter().find(|a| a.key == "service.name").unwrap();
            assert_eq!(svc.value.string_value, "custom");
            let env_attr = attrs.iter().find(|a| a.key == "env").unwrap();
            assert_eq!(env_attr.value.string_value, "prod");
            std::env::remove_var("OTEL_RESOURCE_ATTRIBUTES");

            // 3. OTEL_SERVICE_NAME takes precedence over OTEL_RESOURCE_ATTRIBUTES.
            std::env::set_var("OTEL_RESOURCE_ATTRIBUTES", "service.name=from-attrs");
            std::env::set_var("OTEL_SERVICE_NAME", "from-svc-name");
            let attrs = resource_attributes();
            let svc = attrs.iter().find(|a| a.key == "service.name").unwrap();
            assert_eq!(svc.value.string_value, "from-svc-name");
            std::env::remove_var("OTEL_RESOURCE_ATTRIBUTES");
            std::env::remove_var("OTEL_SERVICE_NAME");
        }
    }

    #[test]
    fn percent_decode_handles_encoded_chars() {
        assert_eq!(percent_decode("key%3Dname"), "key=name");
        assert_eq!(percent_decode("a%2Cb"), "a,b");
        assert_eq!(percent_decode("plain"), "plain");
    }
}
