//! Open metrics.

use std::{ops::Deref, sync::Arc};

use crate::config::config;

pub trait OpenMetric: Send + Sync {
    fn name(&self) -> String;

    /// Metric measurement.
    fn measurements(&self) -> Vec<Measurement>;

    /// Metric unit.
    fn unit(&self) -> Option<String> {
        None
    }

    fn metric_type(&self) -> OpenMetricType {
        OpenMetricType::Gauge
    }

    fn help(&self) -> Option<String> {
        None
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OpenMetricType {
    Gauge,
    Counter,
    /// A distribution. Renders as several series (`_bucket`/`_sum`/`_count`)
    /// rather than one, and carries a `MeasurementType::Histogram`.
    Histogram,
}

impl std::fmt::Display for OpenMetricType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            OpenMetricType::Gauge => "gauge",
            OpenMetricType::Counter => "counter",
            OpenMetricType::Histogram => "histogram",
        };
        f.write_str(s)
    }
}

#[derive(Debug, Clone)]
pub enum MeasurementType {
    Float(f64),
    Integer(i64),
    Millis(u128),
    /// Distribution rendered as an OpenMetrics histogram.
    ///
    /// Boxed: the payload dwarfs the scalar variants, and one `Measurement` per
    /// pool per metric family is built and deep-cloned on every scrape, so the
    /// padding would be paid by every gauge and counter in the process.
    Histogram(Box<HistogramMeasurement>),
}

/// A histogram observation set, in seconds.
///
/// Bucket counts are per-bucket, as the source `pgdog_stats::Histogram`
/// reports them; the OpenMetrics renderer accumulates them on the way out via
/// `cumulative`. `bounds` excludes the implicit `+Inf` bucket, so
/// `buckets.len() == bounds.len() + 1`.
#[derive(Debug, Clone)]
pub struct HistogramMeasurement {
    /// Upper bounds in seconds, ascending. Bounds are process-constant, so one
    /// shared allocation backs every pool's measurement; cloning a measurement
    /// only bumps the refcount.
    pub bounds: Arc<[f64]>,
    /// Per-bucket (non-cumulative) counts, with the `+Inf` bucket last.
    pub buckets: Vec<u64>,
    /// Sum of all observations, in seconds.
    pub sum: f64,
    /// Total number of observations.
    pub count: u64,
}

impl HistogramMeasurement {
    /// Build a measurement from per-bucket (non-cumulative) counts.
    pub fn new(bounds: impl Into<Arc<[f64]>>, per_bucket: &[u64], sum: f64, count: u64) -> Self {
        Self {
            bounds: bounds.into(),
            buckets: per_bucket.to_vec(),
            sum,
            count,
        }
    }
}

/// Running sum of per-bucket counts.
///
/// OpenMetrics `le` buckets are cumulative; OTLP's are not. Cumulative is a
/// property of the `le` wire format, not of the data, so the conversion lives
/// at the OpenMetrics boundary rather than in the measurement.
fn cumulative(per_bucket: &[u64]) -> Vec<u64> {
    let mut buckets = Vec::with_capacity(per_bucket.len());
    let mut running = 0u64;
    for bucket in per_bucket {
        running = running.saturating_add(*bucket);
        buckets.push(running);
    }
    buckets
}

impl From<HistogramMeasurement> for MeasurementType {
    fn from(value: HistogramMeasurement) -> Self {
        Self::Histogram(Box::new(value))
    }
}

impl From<f64> for MeasurementType {
    fn from(value: f64) -> Self {
        Self::Float(value)
    }
}

impl From<i64> for MeasurementType {
    fn from(value: i64) -> Self {
        Self::Integer(value)
    }
}

impl From<u64> for MeasurementType {
    fn from(value: u64) -> Self {
        Self::Integer(value as i64)
    }
}

impl From<usize> for MeasurementType {
    fn from(value: usize) -> Self {
        Self::Integer(value as i64)
    }
}

impl From<u128> for MeasurementType {
    fn from(value: u128) -> Self {
        Self::Millis(value)
    }
}

impl MeasurementType {
    /// Whether a measurement of this shape can be exported under a metric
    /// declared as `metric_type`.
    ///
    /// Histograms render and encode completely differently from scalars, so
    /// the two must agree: a `Histogram` measurement only fits a `Histogram`
    /// metric, and scalar measurements only fit `Gauge` or `Counter`.
    fn matches(&self, metric_type: OpenMetricType) -> bool {
        match metric_type {
            OpenMetricType::Histogram => matches!(self, MeasurementType::Histogram(_)),
            OpenMetricType::Gauge | OpenMetricType::Counter => {
                !matches!(self, MeasurementType::Histogram(_))
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct Measurement {
    pub labels: Vec<(String, String)>,
    pub measurement: MeasurementType,
}

impl Measurement {
    pub fn render(&self, name: &str) -> String {
        let value = match &self.measurement {
            // Histograms render as several lines, so they bypass the scalar format.
            MeasurementType::Histogram(histogram) => return self.render_histogram(name, histogram),
            MeasurementType::Float(f) => format!("{:.3}", f),
            MeasurementType::Integer(i) => i.to_string(),
            MeasurementType::Millis(i) => i.to_string(),
        };

        format!("{}{} {}", name, self.render_labels(&[]), value)
    }

    /// Render the label set, appending `extra` labels after this
    /// measurement's own.
    fn render_labels(&self, extra: &[(&str, String)]) -> String {
        if self.labels.is_empty() && extra.is_empty() {
            return String::new();
        }

        let labels = self
            .labels
            .iter()
            .map(|(name, value)| format!("{}=\"{}\"", name, value))
            .chain(
                extra
                    .iter()
                    .map(|(name, value)| format!("{}=\"{}\"", name, value)),
            )
            .collect::<Vec<_>>();

        format!("{{{}}}", labels.join(","))
    }

    /// Render `_bucket`, `_sum` and `_count` series for a histogram.
    ///
    /// Per the OpenMetrics spec, bucket counts are cumulative and the final
    /// bucket must be `le="+Inf"`.
    fn render_histogram(&self, name: &str, histogram: &HistogramMeasurement) -> String {
        let mut lines = Vec::with_capacity(histogram.buckets.len() + 2);
        let buckets = cumulative(&histogram.buckets);

        // `bounds` excludes +Inf, so zip stops one short and leaves the
        // overflow bucket for the explicit +Inf line below.
        for (bound, count) in histogram.bounds.iter().zip(buckets.iter()) {
            lines.push(format!(
                "{}_bucket{} {}",
                name,
                self.render_labels(&[("le", format_bound(*bound))]),
                count
            ));
        }

        lines.push(format!(
            "{}_bucket{} {}",
            name,
            self.render_labels(&[("le", "+Inf".into())]),
            histogram.count
        ));

        lines.push(format!(
            "{}_sum{} {:.6}",
            name,
            self.render_labels(&[]),
            histogram.sum
        ));
        lines.push(format!(
            "{}_count{} {}",
            name,
            self.render_labels(&[]),
            histogram.count
        ));

        lines.join("\n")
    }
}

/// Format a bucket bound without losing sub-millisecond precision.
///
/// Bounds are seconds, so a default `{}` on `0.0001` would render as
/// `0.0001` but `1e-5` in scientific notation, which Prometheus rejects.
/// Nine decimals preserve `Duration`'s nanosecond resolution, so two
/// distinct bounds can never collapse to the same `le` label — duplicate
/// series fail the entire Prometheus scrape.
fn format_bound(bound: f64) -> String {
    let formatted = format!("{:.9}", bound);
    let trimmed = formatted.trim_end_matches('0').trim_end_matches('.');

    if trimmed.is_empty() {
        "0".into()
    } else {
        trimmed.to_owned()
    }
}

pub struct Metric {
    metric: Box<dyn OpenMetric>,
}

impl Metric {
    pub fn new(metric: impl OpenMetric + 'static) -> Self {
        let metric: Box<dyn OpenMetric> = Box::new(metric);

        // Exporters branch on `metric_type()` and then pattern-match each
        // measurement, so a disagreement between the two fails silently:
        // `# TYPE … gauge` followed by `_bucket` lines, a histogram exported
        // as `0.0` over OTLP, or scalars dropped from a `Histogram` metric.
        debug_assert!(
            metric
                .measurements()
                .iter()
                .all(|m| m.measurement.matches(metric.metric_type())),
            "{:?} is typed {:?} but carries incompatible measurements",
            metric.name(),
            metric.metric_type(),
        );

        Self { metric }
    }
}

impl Deref for Metric {
    type Target = Box<dyn OpenMetric>;

    fn deref(&self) -> &Self::Target {
        &self.metric
    }
}

impl std::fmt::Display for Metric {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let name = self.name();
        let config = config();
        let prefix = config
            .config
            .general
            .openmetrics_namespace
            .as_deref()
            .unwrap_or("");
        writeln!(f, "# TYPE {}{} {}", prefix, name, self.metric_type())?;
        if let Some(unit) = self.unit() {
            writeln!(f, "# UNIT {}{} {}", prefix, name, unit)?;
        }
        if let Some(help) = self.help() {
            writeln!(f, "# HELP {}{} {}", prefix, name, help)?;
        }

        for measurement in self.measurements() {
            // A measurement can render as several lines (histograms), and each
            // one needs the namespace prefix.
            for line in measurement.render(&name).lines() {
                writeln!(f, "{}{}", prefix, line)?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use crate::config::{self, ConfigAndUsers};

    use super::*;

    #[test]
    fn test_prefix() {
        struct TestMetric;

        impl OpenMetric for TestMetric {
            fn name(&self) -> String {
                "test".into()
            }

            fn measurements(&self) -> Vec<Measurement> {
                vec![Measurement {
                    labels: vec![],
                    measurement: MeasurementType::Integer(5),
                }]
            }
        }

        let render = Metric::new(TestMetric {}).to_string();
        assert_eq!(render.lines().last().unwrap(), "test 5");

        let mut cfg = ConfigAndUsers::default();
        cfg.config.general.openmetrics_namespace = Some("pgdog.".into());
        config::set(cfg).unwrap();

        let render = Metric::new(TestMetric {}).to_string();
        assert_eq!(render.lines().next().unwrap(), "# TYPE pgdog.test gauge");
        assert_eq!(render.lines().last().unwrap(), "pgdog.test 5");

        // A histogram renders as several lines, and every one needs the
        // prefix. Asserted here rather than in its own test because the
        // namespace is global state.
        struct TestHistogram;

        impl OpenMetric for TestHistogram {
            fn name(&self) -> String {
                "query_time_seconds".into()
            }

            fn metric_type(&self) -> OpenMetricType {
                OpenMetricType::Histogram
            }

            fn measurements(&self) -> Vec<Measurement> {
                vec![Measurement {
                    labels: vec![],
                    measurement: test_histogram().into(),
                }]
            }
        }

        let render = Metric::new(TestHistogram {}).to_string();
        assert_eq!(
            render.lines().next().unwrap(),
            "# TYPE pgdog.query_time_seconds histogram"
        );
        for line in render.lines().filter(|line| !line.starts_with('#')) {
            assert!(
                line.starts_with("pgdog.query_time_seconds"),
                "missing prefix: {}",
                line
            );
        }
        assert_eq!(
            render
                .lines()
                .filter(|line| line.contains("_bucket"))
                .count(),
            4
        );
    }

    #[test]
    fn measurement_render_formats_labels() {
        let measurement = Measurement {
            labels: vec![
                ("role".into(), "primary".into()),
                ("shard".into(), "0".into()),
            ],
            measurement: MeasurementType::Integer(42),
        };

        let rendered = measurement.render("pool_clients");
        assert_eq!(rendered, "pool_clients{role=\"primary\",shard=\"0\"} 42");
    }

    #[test]
    fn measurement_render_rounds_floats() {
        let measurement = Measurement {
            labels: vec![],
            measurement: MeasurementType::Float(1.23456),
        };

        let rendered = measurement.render("query_latency_seconds");
        assert_eq!(rendered, "query_latency_seconds 1.235");
    }

    fn test_histogram() -> HistogramMeasurement {
        // Per-bucket counts 1/2/0, plus 1 in the +Inf bucket.
        HistogramMeasurement::new(vec![0.001, 0.01, 0.1], &[1, 2, 0, 1], 1.5, 4)
    }

    #[test]
    fn histogram_new_preserves_per_bucket_counts() {
        let histogram = test_histogram();

        // Stored verbatim, matching the source `pgdog_stats::Histogram`.
        // Accumulating is the OpenMetrics renderer's job, not the
        // measurement's — OTLP wants these counts as they are.
        assert_eq!(histogram.buckets, vec![1, 2, 0, 1]);
        assert_eq!(histogram.count, 4);
    }

    #[test]
    fn cumulative_accumulates_per_bucket_counts() {
        assert_eq!(cumulative(&[1, 2, 0, 1]), vec![1, 3, 3, 4]);
        assert_eq!(cumulative(&[]), Vec::<u64>::new());
        assert_eq!(cumulative(&[0, 0, 0]), vec![0, 0, 0]);

        // Never decreases, whatever the inputs.
        let counts = cumulative(&[3, 0, 7, 0, 0, 11]);
        assert!(
            counts.windows(2).all(|pair| pair[0] <= pair[1]),
            "must be monotonic: {:?}",
            counts
        );
    }

    #[test]
    fn cumulative_saturates_instead_of_overflowing() {
        assert_eq!(cumulative(&[u64::MAX, 1]), vec![u64::MAX, u64::MAX]);
    }

    #[test]
    fn last_cumulative_bucket_agrees_with_count() {
        // The `+Inf` line renders `count`, not the last bucket, so the two
        // reach the wire by different routes and must not disagree.
        let histogram = test_histogram();
        let buckets = cumulative(&histogram.buckets);

        assert_eq!(buckets.last().copied(), Some(histogram.count));

        let rendered = Measurement {
            labels: vec![],
            measurement: histogram.into(),
        }
        .render("query_time_seconds");

        let value_of = |suffix: &str| -> u64 {
            rendered
                .lines()
                .find(|line| line.starts_with(&format!("query_time_seconds{}", suffix)))
                .and_then(|line| line.rsplit_once(' '))
                .expect("line")
                .1
                .parse()
                .expect("numeric")
        };

        assert_eq!(value_of("_bucket{le=\"+Inf\"}"), value_of("_count"));
    }

    #[test]
    fn histogram_render_emits_buckets_sum_and_count() {
        let measurement = Measurement {
            labels: vec![("database".into(), "app".into())],
            measurement: test_histogram().into(),
        };

        let rendered = measurement.render("query_time_seconds");
        let lines: Vec<&str> = rendered.lines().collect();

        assert_eq!(
            lines,
            vec![
                r#"query_time_seconds_bucket{database="app",le="0.001"} 1"#,
                r#"query_time_seconds_bucket{database="app",le="0.01"} 3"#,
                r#"query_time_seconds_bucket{database="app",le="0.1"} 3"#,
                r#"query_time_seconds_bucket{database="app",le="+Inf"} 4"#,
                r#"query_time_seconds_sum{database="app"} 1.500000"#,
                r#"query_time_seconds_count{database="app"} 4"#,
            ]
        );
    }

    #[test]
    fn histogram_render_without_labels() {
        let measurement = Measurement {
            labels: vec![],
            measurement: test_histogram().into(),
        };

        let rendered = measurement.render("query_time_seconds");
        let lines: Vec<&str> = rendered.lines().collect();

        assert_eq!(lines[0], r#"query_time_seconds_bucket{le="0.001"} 1"#);
        assert_eq!(lines[3], r#"query_time_seconds_bucket{le="+Inf"} 4"#);
        assert_eq!(lines[4], "query_time_seconds_sum 1.500000");
        assert_eq!(lines[5], "query_time_seconds_count 4");
    }

    #[test]
    fn histogram_bucket_counts_are_monotonic() {
        let measurement = Measurement {
            labels: vec![],
            measurement: test_histogram().into(),
        };

        let counts: Vec<u64> = measurement
            .render("query_time_seconds")
            .lines()
            .filter(|line| line.contains("_bucket"))
            .map(|line| {
                line.rsplit_once(' ')
                    .expect("value")
                    .1
                    .parse()
                    .expect("numeric")
            })
            .collect();

        assert!(
            counts.windows(2).all(|pair| pair[0] <= pair[1]),
            "bucket counts must be cumulative: {:?}",
            counts
        );
        assert_eq!(counts.last(), Some(&4));
    }

    #[test]
    fn bound_formatting_avoids_scientific_notation() {
        assert_eq!(format_bound(0.0001), "0.0001");
        assert_eq!(format_bound(0.001), "0.001");
        assert_eq!(format_bound(1.0), "1");
        assert_eq!(format_bound(30.0), "30");
    }

    #[test]
    fn bound_formatting_distinguishes_sub_microsecond_bounds() {
        use std::time::Duration;

        // Distinct Durations must never collapse to the same le label:
        // duplicates fail the entire Prometheus scrape.
        assert_eq!(
            format_bound(Duration::from_nanos(100).as_secs_f64()),
            "0.0000001"
        );
        assert_ne!(
            format_bound(Duration::from_nanos(100).as_secs_f64()),
            format_bound(Duration::from_nanos(200).as_secs_f64())
        );
        assert_eq!(format_bound(1.1e-6), "0.0000011");
        assert_ne!(format_bound(1.1e-6), format_bound(1.2e-6));
    }

    // The disagreement check lives in a debug_assert!, so these only panic
    // when debug assertions are enabled.
    #[cfg(debug_assertions)]
    #[test]
    #[should_panic(expected = "carries incompatible measurements")]
    fn metric_new_rejects_histogram_measurement_in_gauge_metric() {
        struct BadMetric;

        impl OpenMetric for BadMetric {
            fn name(&self) -> String {
                "bad".into()
            }

            // metric_type() defaults to Gauge.
            fn measurements(&self) -> Vec<Measurement> {
                vec![Measurement {
                    labels: vec![],
                    measurement: test_histogram().into(),
                }]
            }
        }

        let _ = Metric::new(BadMetric);
    }

    #[cfg(debug_assertions)]
    #[test]
    #[should_panic(expected = "carries incompatible measurements")]
    fn metric_new_rejects_scalar_measurement_in_histogram_metric() {
        struct BadMetric;

        impl OpenMetric for BadMetric {
            fn name(&self) -> String {
                "bad".into()
            }

            fn metric_type(&self) -> OpenMetricType {
                OpenMetricType::Histogram
            }

            fn measurements(&self) -> Vec<Measurement> {
                vec![Measurement {
                    labels: vec![],
                    measurement: MeasurementType::Integer(1),
                }]
            }
        }

        let _ = Metric::new(BadMetric);
    }
}
