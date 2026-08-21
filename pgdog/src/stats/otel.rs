//! OpenTelemetry Protocol (OTLP) JSON renderer.
//!
//! Converts the existing `OpenMetric` trait objects into the OTLP JSON format
//! (`ExportMetricsServiceRequest`) compatible with Datadog's OTLP ingest endpoint.

use std::collections::HashMap;
use std::env;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use once_cell::sync::Lazy;
use parking_lot::Mutex;
use pgdog_config::otel_temporality::OtelTemporalityPreference;
use serde::Serialize;

use crate::util::hostname;

use super::open_metric::{
    HistogramMeasurement, Measurement, MeasurementType, Metric, OpenMetricType,
};

static RESOURCE_ATTRIBUTES: Lazy<Vec<KeyValue>> = Lazy::new(resource_attributes);

#[cfg(test)]
pub(crate) static TEST_LOCK: Lazy<Mutex<()>> = Lazy::new(|| Mutex::new(()));

/// Identity of a single counter data point for delta tracking.
#[derive(Hash, Eq, PartialEq, Clone)]
struct CounterKey {
    metric: String,
    labels: Vec<(String, String)>,
}

/// Per-data-point counter bookkeeping: previous cumulative values (for delta
/// computation) and first-seen timestamps (used as `start_time_unix_nano` so
/// cumulative counters carry a stable collection-start reference).
///
/// None of these maps is evicted, so a pool that goes away keeps its entry for
/// the life of the process. Eviction should cover all of them together: they
/// are keyed identically, and expiring one but not the others would leave the
/// same series half-forgotten.
#[derive(Default)]
struct CounterState {
    prev_values: Mutex<HashMap<CounterKey, f64>>,
    prev_histograms: Mutex<HashMap<CounterKey, HistogramState>>,
    start_times: Mutex<HashMap<CounterKey, String>>,
}

impl CounterState {
    fn start_time(&self, key: &CounterKey, now: &str) -> String {
        self.start_times
            .lock()
            .entry(key.clone())
            .or_insert_with(|| now.to_string())
            .clone()
    }

    /// Delta since the previously recorded cumulative value. Updates the
    /// stored value as a side effect. Returns `None` on a negative delta
    /// (counter reset), which callers should treat as a skipped data point.
    fn delta(&self, key: &CounterKey, cumulative: f64) -> Option<f64> {
        let mut prev = self.prev_values.lock();
        let delta = cumulative - prev.get(key).copied().unwrap_or(0.0);
        prev.insert(key.clone(), cumulative);
        (delta >= 0.0).then_some(delta)
    }

    /// Delta since the previously recorded cumulative distribution. Updates
    /// the stored value as a side effect.
    ///
    /// Deliberately stricter than [`CounterState::delta`] on first sight: a
    /// counter reports its lifetime total as the first delta, which is a
    /// legitimate interval value, whereas a histogram's lifetime bucket
    /// distribution reported as a single interval would skew per-interval
    /// percentiles. So the first export only latches state and returns `None`.
    ///
    /// Also returns `None` after a reset — pool recreated, or bucket layout
    /// changed — so one interval is skipped rather than emitting negative
    /// counts.
    fn histogram_delta(
        &self,
        key: &CounterKey,
        cumulative: HistogramState,
    ) -> Option<HistogramState> {
        let previous = self
            .prev_histograms
            .lock()
            .insert(key.clone(), cumulative.clone())?;

        if cumulative.count < previous.count
            || cumulative.sum < previous.sum
            || cumulative.buckets.len() != previous.buckets.len()
        {
            return None;
        }

        Some(HistogramState {
            buckets: cumulative
                .buckets
                .iter()
                .zip(previous.buckets.iter())
                .map(|(cumulative, previous)| cumulative.saturating_sub(*previous))
                .collect(),
            sum: cumulative.sum - previous.sum,
            count: cumulative.count - previous.count,
        })
    }
}

static COUNTER_STATE: Lazy<CounterState> = Lazy::new(CounterState::default);

/// Bucket counts, sum and observation count for one histogram data point.
///
/// Holds either a cumulative snapshot or a per-interval delta, depending on
/// where it came from. Bucket counts are per-bucket, as OTLP expects, not the
/// cumulative `le` counts the OpenMetrics endpoint renders.
#[derive(Clone, Default)]
struct HistogramState {
    buckets: Vec<u64>,
    sum: f64,
    count: u64,
}

pub fn now_nanos() -> String {
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
    #[serde(skip_serializing_if = "Option::is_none")]
    pub histogram: Option<Histogram>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Gauge {
    pub data_points: Vec<NumberDataPoint>,
}

// little serde trick to let us serialize directly as the integer representation
#[derive(serde_repr::Serialize_repr, Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum AggregationTemporality {
    Delta = 1,
    Cumulative = 2,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Sum {
    pub aggregation_temporality: AggregationTemporality,
    pub is_monotonic: bool,
    pub data_points: Vec<NumberDataPoint>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Histogram {
    // Same OTLP enum as `Sum` carries: the temporality choice is resolved once
    // per export, so sums and histograms in a batch always agree.
    pub aggregation_temporality: AggregationTemporality,
    pub data_points: Vec<HistogramDataPoint>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HistogramDataPoint {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub start_time_unix_nano: Option<String>,
    pub time_unix_nano: String,
    #[serde(serialize_with = "u64_to_string")]
    pub count: u64,
    pub sum: f64,
    /// Per-bucket counts; one longer than `explicit_bounds`.
    #[serde(serialize_with = "u64s_to_strings")]
    pub bucket_counts: Vec<u64>,
    /// Upper bounds, excluding the implicit `+Inf` bucket. Shared with the
    /// source measurement rather than copied per data point; serializes as a
    /// plain JSON array.
    pub explicit_bounds: Arc<[f64]>,
    pub attributes: Vec<KeyValue>,
}

/// OTLP/JSON (protojson) encodes 64-bit integers as decimal strings.
fn u64_to_string<S: serde::Serializer>(value: &u64, serializer: S) -> Result<S::Ok, S::Error> {
    serializer.collect_str(value)
}

/// Same, for a sequence of 64-bit integers.
fn u64s_to_strings<S: serde::Serializer>(values: &[u64], serializer: S) -> Result<S::Ok, S::Error> {
    use serde::ser::SerializeSeq;

    let mut seq = serializer.serialize_seq(Some(values.len()))?;
    for value in values {
        seq.serialize_element(&value.to_string())?;
    }
    seq.end()
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
                if let Ok(s) = std::str::from_utf8(&hex)
                    && let Ok(byte) = u8::from_str_radix(s, 16)
                {
                    result.push(byte as char);
                    continue;
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
        // Histograms are exported as their own data point type, never as a
        // single number.
        MeasurementType::Histogram(_) => 0.0,
    }
}

/// Compute `(as_double, start_time_unix_nano)` for a single measurement.
///
/// Returns `None` to skip this data point (only possible for a Delta counter
/// that just observed a reset — a negative delta).
fn value_for_data_point(
    state: &CounterState,
    metric_name: &str,
    measurement: &Measurement,
    metric_type: OpenMetricType,
    temporality: AggregationTemporality,
    now: &str,
) -> Option<(f64, Option<String>)> {
    let cumulative = measurement_to_f64(&measurement.measurement);

    match metric_type {
        OpenMetricType::Gauge => Some((cumulative, None)),
        OpenMetricType::Counter => {
            let key = CounterKey {
                metric: metric_name.into(),
                labels: measurement.labels.clone(),
            };
            let start = state.start_time(&key, now);
            let value = match temporality {
                AggregationTemporality::Cumulative => cumulative,
                AggregationTemporality::Delta => state.delta(&key, cumulative)?,
            };
            Some((value, Some(start)))
        }
        // A distribution has no scalar value, so it cannot become a
        // `NumberDataPoint`. Callers fork to `histogram_data_point` before
        // reaching here.
        OpenMetricType::Histogram => None,
    }
}

/// Wrap a collection of data points in the correct OTLP container based on
/// the source metric type: `Gauge` for gauges, `Sum` (monotonic) for counters.
fn wrap_data_points(
    metric_type: OpenMetricType,
    temporality: AggregationTemporality,
    data_points: Vec<NumberDataPoint>,
) -> (Option<Gauge>, Option<Sum>) {
    match metric_type {
        OpenMetricType::Gauge => (Some(Gauge { data_points }), None),
        OpenMetricType::Counter => (
            None,
            Some(Sum {
                aggregation_temporality: temporality,
                is_monotonic: true,
                data_points,
            }),
        ),
        // Neither container fits a distribution; the `Histogram` container is
        // built directly by the caller, which never routes one here.
        OpenMetricType::Histogram => (None, None),
    }
}

/// Merge the measurement's own labels with the process-wide resource
/// attributes into the OTLP `attributes` field for a data point.
fn build_attributes(labels: &[(String, String)], common_attrs: &[KeyValue]) -> Vec<KeyValue> {
    let mut attributes: Vec<KeyValue> = labels
        .iter()
        .map(|(k, v)| KeyValue {
            key: k.clone(),
            value: AttributeValue {
                string_value: v.clone(),
            },
        })
        .collect();
    attributes.extend(common_attrs.iter().cloned());
    attributes
}

/// Build one OTLP histogram data point from a cumulative measurement.
///
/// Pool counts accumulate since the pool was created, so the measurement is
/// always cumulative. Cumulative exports pass it straight through; only the
/// Delta path needs per-export bookkeeping, and it may report nothing — see
/// [`CounterState::histogram_delta`].
fn histogram_data_point(
    state: &CounterState,
    metric_name: &str,
    measurement: &Measurement,
    histogram: &HistogramMeasurement,
    temporality: AggregationTemporality,
    now: &str,
    attributes: Vec<KeyValue>,
) -> Option<HistogramDataPoint> {
    // The measurement already carries per-bucket counts, which is what OTLP
    // wants; only the OpenMetrics `le` format needs them accumulated.
    let cumulative = HistogramState {
        buckets: histogram.buckets.clone(),
        sum: histogram.sum,
        count: histogram.count,
    };

    let key = CounterKey {
        metric: metric_name.to_owned(),
        labels: measurement.labels.clone(),
    };

    // Pin the collection-start reference before the Delta path can bail, so a
    // series that starts reporting later still carries its original start.
    let start_time_unix_nano = Some(state.start_time(&key, now));

    let values = match temporality {
        AggregationTemporality::Cumulative => cumulative,
        AggregationTemporality::Delta => state.histogram_delta(&key, cumulative)?,
    };

    Some(HistogramDataPoint {
        start_time_unix_nano,
        time_unix_nano: now.to_owned(),
        count: values.count,
        sum: values.sum,
        bucket_counts: values.buckets,
        explicit_bounds: histogram.bounds.clone(),
        attributes,
    })
}

/// Build an `ExportMetricsServiceRequest` from a collection of `Metric` objects,
/// reading namespace and temporality preference from the global config and
/// using the process-wide counter bookkeeping. `now` is threaded in from the
/// caller so every data point in a batch shares one timestamp.
pub fn build_request(metrics: &[&Metric], now: &str) -> ExportMetricsServiceRequest {
    let config = crate::config::config();
    let otel = &config.config.otel;

    let temporality = match otel.effective_temporality_preference() {
        OtelTemporalityPreference::Cumulative => AggregationTemporality::Cumulative,
        OtelTemporalityPreference::Delta | OtelTemporalityPreference::LowMemory => {
            AggregationTemporality::Delta
        }
    };

    let namespace = otel.namespace.as_deref();

    build_request_with_state(&COUNTER_STATE, temporality, namespace, now, metrics)
}

/// Injectable core of [`build_request`]. Takes counter state, temporality,
/// and namespace explicitly so tests can exercise the stateful counter logic
/// without touching global config or the process-wide static.
fn build_request_with_state(
    state: &CounterState,
    temporality: AggregationTemporality,
    namespace: Option<&str>,
    now: &str,
    metrics: &[&Metric],
) -> ExportMetricsServiceRequest {
    let namespace = namespace.unwrap_or("pgdog").trim_end_matches(['.', '_']);
    let namespace = if namespace.is_empty() {
        "pgdog"
    } else {
        namespace
    };

    let common_attrs = &*RESOURCE_ATTRIBUTES;

    let otel_metrics: Vec<OtelMetric> = metrics
        .iter()
        .map(|metric| {
            let name = format!("{}.{}", namespace, metric.name());
            let metric_type = metric.metric_type();

            // A distribution becomes a `HistogramDataPoint`, which shares no
            // fields with the `NumberDataPoint` the other two shapes produce, so
            // the paths fork before any data point is built.
            let (gauge, sum, histogram) = match metric_type {
                OpenMetricType::Histogram => {
                    let data_points = metric
                        .measurements()
                        .iter()
                        .filter_map(|m| {
                            let MeasurementType::Histogram(ref histogram) = m.measurement else {
                                return None;
                            };

                            histogram_data_point(
                                state,
                                &name,
                                m,
                                histogram,
                                temporality,
                                now,
                                build_attributes(&m.labels, common_attrs),
                            )
                        })
                        .collect();

                    (
                        None,
                        None,
                        Some(Histogram {
                            aggregation_temporality: temporality,
                            data_points,
                        }),
                    )
                }

                OpenMetricType::Gauge | OpenMetricType::Counter => {
                    let data_points: Vec<NumberDataPoint> = metric
                        .measurements()
                        .iter()
                        .filter_map(|m| {
                            let (as_double, start_time_unix_nano) = value_for_data_point(
                                state,
                                &name,
                                m,
                                metric_type,
                                temporality,
                                now,
                            )?;

                            Some(NumberDataPoint {
                                start_time_unix_nano,
                                time_unix_nano: now.to_owned(),
                                as_double,
                                attributes: build_attributes(&m.labels, common_attrs),
                            })
                        })
                        .collect();

                    let (gauge, sum) = wrap_data_points(metric_type, temporality, data_points);
                    (gauge, sum, None)
                }
            };

            OtelMetric {
                name,
                description: metric.help(),
                unit: metric.unit().unwrap_or_else(|| "1".into()),
                gauge,
                sum,
                histogram,
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
                    name: namespace.into(),
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
    use crate::test_utils::*;

    #[test]
    fn gauge_metric_produces_gauge_json() {
        let _test_lock = TEST_LOCK.lock();

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

        let request = build_request(&[&metric], &now_nanos());
        let json = serde_json::to_string_pretty(&request).expect("serialize");

        assert!(json.contains("\"gauge\""));
        assert!(!json.contains("\"sum\""));
        assert!(json.contains("sv_idle"));
        assert!(json.contains("\"asDouble\": 3.0"));
        assert!(json.contains("\"alice\""));
    }

    #[test]
    fn counter_metric_produces_sum_json() {
        let _test_lock = TEST_LOCK.lock();

        use crate::config::{self, ConfigAndUsers};
        let mut cfg = ConfigAndUsers::default();
        cfg.config.otel.temporality_preference = Some(OtelTemporalityPreference::Delta);
        config::set(cfg).expect("set config");

        let metric = Metric::new(PoolMetric {
            name: "total_query_count".into(),
            measurements: vec![Measurement {
                labels: vec![],
                measurement: MeasurementType::Integer(42),
            }],
            help: "Total queries".into(),
            unit: None,
            metric_type: Some(OpenMetricType::Counter),
        });

        let request = build_request(&[&metric], &now_nanos());
        let json = serde_json::to_string(&request).expect("serialize");

        assert!(json.contains("\"sum\""));
        assert!(!json.contains("\"gauge\""));
        assert!(json.contains("\"isMonotonic\":true"));
        assert!(json.contains("\"aggregationTemporality\":1"));
    }

    #[test]
    fn namespace_used_as_scope_name() {
        let _test_lock = TEST_LOCK.lock();

        use crate::config::{self, ConfigAndUsers};

        let mut cfg = ConfigAndUsers::default();
        cfg.config.general.openmetrics_namespace = Some("openmetrics_".into());
        cfg.config.otel.namespace = Some("pgdog_".into());
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

        let request = build_request(&[&metric], &now_nanos());
        let scope = &request.resource_metrics[0].scope_metrics[0].scope;
        assert_eq!(scope.name, "pgdog");

        let otel_metric = &request.resource_metrics[0].scope_metrics[0].metrics[0];
        assert_eq!(otel_metric.name, "pgdog.clients");
    }

    #[test]
    fn multiple_data_points_with_labels() {
        let _test_lock = TEST_LOCK.lock();

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

        let request = build_request(&[&metric], &now_nanos());
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
        let _test_lock = TEST_LOCK.lock();

        let metric = Metric::new(PoolMetric {
            name: "total_xact_time".into(),
            measurements: vec![Measurement {
                labels: vec![],
                measurement: MeasurementType::Millis(12345),
            }],
            help: "Transaction time".into(),
            unit: None,
            metric_type: Some(OpenMetricType::Counter),
        });

        let request = build_request(&[&metric], &now_nanos());
        let sum = &request.resource_metrics[0].scope_metrics[0].metrics[0]
            .sum
            .as_ref()
            .expect("sum");
        assert_eq!(sum.data_points[0].as_double, 12345.0);
    }

    #[test]
    fn float_measurement_preserves_value() {
        let _test_lock = TEST_LOCK.lock();

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

        let request = build_request(&[&metric], &now_nanos());
        let gauge = &request.resource_metrics[0].scope_metrics[0].metrics[0]
            .gauge
            .as_ref()
            .expect("gauge");
        assert!((gauge.data_points[0].as_double - 1.234).abs() < f64::EPSILON);
    }

    #[test]
    fn service_name_and_scope() {
        let _test_lock = TEST_LOCK.lock();

        // Ensure no env overrides leak from other tests.
        let _guard = remove_env_var("OTEL_SERVICE_NAME");
        let _guard = remove_env_var("OTEL_RESOURCE_ATTRIBUTES");

        let metric = Metric::new(PoolMetric {
            name: "test".into(),
            measurements: vec![],
            help: "test".into(),
            unit: None,
            metric_type: None,
        });

        let request = build_request(&[&metric], &now_nanos());
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
        let _test_lock = TEST_LOCK.lock();

        // Clean slate.
        let _guard = remove_env_var("OTEL_SERVICE_NAME");
        let _guard = remove_env_var("OTEL_RESOURCE_ATTRIBUTES");

        // 1. OTEL_SERVICE_NAME overrides service.name.
        {
            let _guard = set_env_var("OTEL_SERVICE_NAME", "my-pgdog");
            let attrs = resource_attributes();
            let svc = attrs.iter().find(|a| a.key == "service.name").unwrap();
            assert_eq!(svc.value.string_value, "my-pgdog");
        }

        // 2. OTEL_RESOURCE_ATTRIBUTES adds and overrides.
        {
            let _guard = set_env_var("OTEL_RESOURCE_ATTRIBUTES", "env=prod,service.name=custom");
            let attrs = resource_attributes();
            let svc = attrs.iter().find(|a| a.key == "service.name").unwrap();
            assert_eq!(svc.value.string_value, "custom");
            let env_attr = attrs.iter().find(|a| a.key == "env").unwrap();
            assert_eq!(env_attr.value.string_value, "prod");
        }

        // 3. OTEL_SERVICE_NAME takes precedence over OTEL_RESOURCE_ATTRIBUTES.
        {
            let _guard = set_env_var("OTEL_RESOURCE_ATTRIBUTES", "service.name=from-attrs");
            let _guard = set_env_var("OTEL_SERVICE_NAME", "from-svc-name");
            let attrs = resource_attributes();
            let svc = attrs.iter().find(|a| a.key == "service.name").unwrap();
            assert_eq!(svc.value.string_value, "from-svc-name");
        }
    }

    fn histogram_metric(sum: f64, count: u64, per_bucket: &[u64]) -> Metric {
        Metric::new(PoolMetric {
            name: "query_time_seconds".into(),
            measurements: vec![Measurement {
                labels: vec![("database".into(), "app".into())],
                measurement: HistogramMeasurement::new(vec![0.001, 0.01], per_bucket, sum, count)
                    .into(),
            }],
            help: "Query times".into(),
            unit: Some("seconds".into()),
            metric_type: Some(OpenMetricType::Histogram),
        })
    }

    fn export_histogram(
        state: &CounterState,
        temporality: AggregationTemporality,
        metric: &Metric,
    ) -> ExportMetricsServiceRequest {
        export_histogram_at(state, temporality, &now_nanos(), metric)
    }

    /// Export with a caller-supplied collection timestamp, for tests that
    /// assert on which `now` a series records.
    fn export_histogram_at(
        state: &CounterState,
        temporality: AggregationTemporality,
        now: &str,
        metric: &Metric,
    ) -> ExportMetricsServiceRequest {
        build_request_with_state(state, temporality, None, now, &[metric])
    }

    fn only_histogram(req: &ExportMetricsServiceRequest) -> &Histogram {
        req.resource_metrics[0].scope_metrics[0].metrics[0]
            .histogram
            .as_ref()
            .expect("histogram")
    }

    #[test]
    fn cumulative_histogram_passes_counts_through() {
        let state = CounterState::default();

        // Cumulative is the default temporality, so this is the common path: the
        // measurement is already cumulative-since-pool-creation and needs no
        // bookkeeping, including on the very first export.
        let metric = histogram_metric(0.5, 3, &[1, 1, 1]);
        let request = export_histogram(&state, AggregationTemporality::Cumulative, &metric);
        let histogram = only_histogram(&request);

        assert_eq!(
            histogram.aggregation_temporality,
            AggregationTemporality::Cumulative
        );
        assert_eq!(histogram.data_points.len(), 1);

        let point = &histogram.data_points[0];
        assert_eq!(point.count, 3);
        assert!((point.sum - 0.5).abs() < f64::EPSILON);
        // OTLP wants per-bucket counts even though the measurement is cumulative.
        assert_eq!(point.bucket_counts, vec![1, 1, 1]);
        assert_eq!(&point.explicit_bounds[..], [0.001, 0.01]);
        // bucketCounts is always one longer than explicitBounds.
        assert_eq!(point.bucket_counts.len(), point.explicit_bounds.len() + 1);
    }

    #[test]
    fn cumulative_histogram_accumulates_across_exports() {
        let state = CounterState::default();

        let first = histogram_metric(0.5, 3, &[1, 1, 1]);
        export_histogram(&state, AggregationTemporality::Cumulative, &first);

        let second = histogram_metric(1.5, 5, &[2, 1, 2]);
        let request = export_histogram(&state, AggregationTemporality::Cumulative, &second);

        // Reported as-is, not diffed.
        let point = &only_histogram(&request).data_points[0];
        assert_eq!(point.count, 5);
        assert_eq!(point.bucket_counts, vec![2, 1, 2]);
    }

    #[test]
    fn delta_histogram_first_export_is_skipped() {
        let state = CounterState::default();

        let metric = histogram_metric(0.5, 3, &[1, 1, 1]);
        let request = export_histogram(&state, AggregationTemporality::Delta, &metric);

        let otel_metric = &request.resource_metrics[0].scope_metrics[0].metrics[0];

        // No prior sample to diff against. Reporting the lifetime distribution as
        // one interval would skew percentiles, so nothing is reported yet.
        assert!(
            otel_metric
                .histogram
                .as_ref()
                .expect("histogram")
                .data_points
                .is_empty()
        );
        assert!(otel_metric.gauge.is_none());
        assert!(otel_metric.sum.is_none());
    }

    #[test]
    fn delta_histogram_exports_delta_against_previous() {
        let state = CounterState::default();

        // Prime the previous state.
        let first = histogram_metric(0.5, 3, &[1, 1, 1]);
        export_histogram(&state, AggregationTemporality::Delta, &first);

        // Two more observations: one in the first bucket, one in +Inf.
        let second = histogram_metric(1.5, 5, &[2, 1, 2]);
        let request = export_histogram(&state, AggregationTemporality::Delta, &second);
        let histogram = only_histogram(&request);

        assert_eq!(
            histogram.aggregation_temporality,
            AggregationTemporality::Delta
        );
        assert_eq!(histogram.data_points.len(), 1);

        let point = &histogram.data_points[0];
        assert_eq!(point.count, 2);
        assert!((point.sum - 1.0).abs() < f64::EPSILON);
        assert_eq!(point.bucket_counts, vec![1, 0, 1]);
        assert_eq!(&point.explicit_bounds[..], [0.001, 0.01]);
        assert_eq!(point.bucket_counts.len(), point.explicit_bounds.len() + 1);
    }

    #[test]
    fn delta_histogram_skips_counter_reset() {
        let state = CounterState::default();

        let first = histogram_metric(1.5, 5, &[2, 1, 2]);
        export_histogram(&state, AggregationTemporality::Delta, &first);

        // Pool recreated: counts went backwards.
        let reset = histogram_metric(0.1, 1, &[1, 0, 0]);
        let request = export_histogram(&state, AggregationTemporality::Delta, &reset);

        assert!(only_histogram(&request).data_points.is_empty());
    }

    #[test]
    fn histogram_start_time_survives_a_skipped_delta() {
        let state = CounterState::default();

        // Fixed timestamps: the property is *which* `now` gets pinned, not
        // whether the wall clock happened to advance between two exports.
        let first_scrape = "1700000000000000000";
        let second_scrape = "1700000015000000000";

        // The first Delta export reports nothing, but must still pin the
        // collection start so the series that follows carries its original one.
        let first = histogram_metric(0.5, 3, &[1, 1, 1]);
        let request =
            export_histogram_at(&state, AggregationTemporality::Delta, first_scrape, &first);
        assert!(only_histogram(&request).data_points.is_empty());

        let second = histogram_metric(1.0, 4, &[2, 1, 1]);
        let request = export_histogram_at(
            &state,
            AggregationTemporality::Delta,
            second_scrape,
            &second,
        );

        // The start time is the *first* scrape's, not the second's: a consumer
        // must see one unbroken series, not one that began 15s in.
        let point = &only_histogram(&request).data_points[0];
        assert_eq!(point.start_time_unix_nano.as_deref(), Some(first_scrape));
        assert_eq!(point.time_unix_nano, second_scrape);
    }

    #[test]
    fn histogram_carries_labels_and_resource_attributes() {
        let state = CounterState::default();

        let metric = histogram_metric(0.5, 3, &[1, 1, 1]);
        let request = export_histogram(&state, AggregationTemporality::Cumulative, &metric);

        let point = &only_histogram(&request).data_points[0];

        assert_eq!(point.attributes[0].key, "database");
        assert_eq!(point.attributes[0].value.string_value, "app");
        assert!(point.attributes.iter().any(|a| a.key == "service.name"));
    }

    #[test]
    fn histogram_serializes_to_valid_otlp_json() {
        let state = CounterState::default();

        let first = histogram_metric(0.5, 3, &[1, 1, 1]);
        export_histogram(&state, AggregationTemporality::Delta, &first);
        let second = histogram_metric(1.0, 4, &[2, 1, 1]);
        let request = export_histogram(&state, AggregationTemporality::Delta, &second);

        let json = serde_json::to_string(&request).expect("serialize");

        assert!(json.contains("\"histogram\""));
        assert!(json.contains("\"bucketCounts\""));
        assert!(json.contains("\"explicitBounds\""));
        assert!(!json.contains("\"gauge\""));
        assert!(!json.contains("\"sum\":null"));

        // OTLP/JSON encodes 64-bit integers as decimal strings.
        assert!(json.contains("\"count\":\"1\""));
        assert!(json.contains("\"bucketCounts\":[\"1\",\"0\",\"0\"]"));

        let _: serde_json::Value = serde_json::from_str(&json).expect("valid json");
    }

    #[test]
    fn percent_decode_handles_encoded_chars() {
        let _test_lock = TEST_LOCK.lock();

        assert_eq!(percent_decode("key%3Dname"), "key=name");
        assert_eq!(percent_decode("a%2Cb"), "a,b");
        assert_eq!(percent_decode("plain"), "plain");
    }

    fn counter(name: &str, labels: Vec<(String, String)>, value: i64) -> Metric {
        Metric::new(PoolMetric {
            name: name.into(),
            measurements: vec![Measurement {
                labels,
                measurement: MeasurementType::Integer(value),
            }],
            help: "".into(),
            unit: None,
            metric_type: Some(OpenMetricType::Counter),
        })
    }

    fn only_data_point(req: &ExportMetricsServiceRequest) -> &NumberDataPoint {
        &req.resource_metrics[0].scope_metrics[0].metrics[0]
            .sum
            .as_ref()
            .expect("sum")
            .data_points[0]
    }

    #[test]
    fn delta_subtracts_previous_cumulative_value() {
        let state = CounterState::default();

        let m1 = counter("total_queries", vec![], 10);
        let r1 = build_request_with_state(
            &state,
            AggregationTemporality::Delta,
            None,
            &now_nanos(),
            &[&m1],
        );
        assert_eq!(only_data_point(&r1).as_double, 10.0);

        let m2 = counter("total_queries", vec![], 25);
        let r2 = build_request_with_state(
            &state,
            AggregationTemporality::Delta,
            None,
            &now_nanos(),
            &[&m2],
        );
        assert_eq!(only_data_point(&r2).as_double, 15.0);
    }

    #[test]
    fn counter_reset_skips_data_point() {
        let state = CounterState::default();

        let m1 = counter("total_queries", vec![], 10);
        let _ = build_request_with_state(
            &state,
            AggregationTemporality::Delta,
            None,
            &now_nanos(),
            &[&m1],
        );

        let m2 = counter("total_queries", vec![], 3);
        let r2 = build_request_with_state(
            &state,
            AggregationTemporality::Delta,
            None,
            &now_nanos(),
            &[&m2],
        );

        let sum = r2.resource_metrics[0].scope_metrics[0].metrics[0]
            .sum
            .as_ref()
            .expect("sum");
        assert!(
            sum.data_points.is_empty(),
            "reset counter should skip the data point, got {:?}",
            sum.data_points
                .iter()
                .map(|d| d.as_double)
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn counter_deltas_are_tracked_per_label_set() {
        let state = CounterState::default();

        let build = |alice_val: i64, bob_val: i64| {
            Metric::new(PoolMetric {
                name: "total_queries".into(),
                measurements: vec![
                    Measurement {
                        labels: vec![("user".into(), "alice".into())],
                        measurement: MeasurementType::Integer(alice_val),
                    },
                    Measurement {
                        labels: vec![("user".into(), "bob".into())],
                        measurement: MeasurementType::Integer(bob_val),
                    },
                ],
                help: "".into(),
                unit: None,
                metric_type: Some(OpenMetricType::Counter),
            })
        };

        let m1 = build(10, 100);
        let _ = build_request_with_state(
            &state,
            AggregationTemporality::Delta,
            None,
            &now_nanos(),
            &[&m1],
        );

        // alice advances by 5, bob stays put.
        let m2 = build(15, 100);
        let r2 = build_request_with_state(
            &state,
            AggregationTemporality::Delta,
            None,
            &now_nanos(),
            &[&m2],
        );

        let points = &r2.resource_metrics[0].scope_metrics[0].metrics[0]
            .sum
            .as_ref()
            .expect("sum")
            .data_points;

        let find_user = |user: &str| {
            points
                .iter()
                .find(|dp| {
                    dp.attributes
                        .iter()
                        .any(|a| a.key == "user" && a.value.string_value == user)
                })
                .unwrap_or_else(|| panic!("data point for user={user}"))
        };

        assert_eq!(find_user("alice").as_double, 5.0);
        assert_eq!(find_user("bob").as_double, 0.0);
    }

    #[test]
    fn counter_start_time_unix_nano_is_pinned_to_first_observation() {
        let state = CounterState::default();

        let m1 = counter("total_queries", vec![], 1);
        let r1 = build_request_with_state(
            &state,
            AggregationTemporality::Cumulative,
            None,
            &now_nanos(),
            &[&m1],
        );
        let dp1 = only_data_point(&r1);
        let first_start = dp1
            .start_time_unix_nano
            .clone()
            .expect("start_time_unix_nano set on counter");
        assert_eq!(
            first_start, dp1.time_unix_nano,
            "on first observation, start_time should equal time"
        );

        // Force `now_nanos()` to advance so we can distinguish "start reused"
        // from "start == current now by coincidence".
        std::thread::sleep(std::time::Duration::from_millis(2));

        let m2 = counter("total_queries", vec![], 2);
        let r2 = build_request_with_state(
            &state,
            AggregationTemporality::Cumulative,
            None,
            &now_nanos(),
            &[&m2],
        );
        let dp2 = only_data_point(&r2);
        let second_start = dp2
            .start_time_unix_nano
            .clone()
            .expect("start_time_unix_nano set on counter");

        assert_eq!(
            second_start, first_start,
            "start_time_unix_nano must be pinned to the first observation"
        );
        assert_ne!(
            dp2.time_unix_nano, second_start,
            "time_unix_nano should advance while start_time_unix_nano stays put"
        );
    }
}
