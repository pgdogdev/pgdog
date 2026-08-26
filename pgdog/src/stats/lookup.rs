//! Sharding key lookup metrics: cache effectiveness, cache size and
//! the cost of misses, per cluster and global.

use std::sync::atomic::Ordering;

use crate::backend::databases::databases;

use super::{Measurement, Metric, OpenMetric};

pub(crate) struct LookupMetrics;

/// One metric series being assembled: per-cluster measurements
/// followed by an unlabeled global rollup, like mirror stats.
struct Series {
    name: &'static str,
    help: &'static str,
    metric_type: &'static str,
    measurements: Vec<Measurement>,
    global: u64,
}

impl Series {
    fn new(name: &'static str, help: &'static str, metric_type: &'static str) -> Self {
        Self {
            name,
            help,
            metric_type,
            measurements: vec![],
            global: 0,
        }
    }

    fn record(&mut self, labels: &[(String, String)], value: u64) {
        self.measurements.push(Measurement {
            labels: labels.to_vec(),
            measurement: value.into(),
        });
        self.global += value;
    }

    fn finish(mut self) -> Metric {
        self.measurements.push(Measurement {
            labels: vec![],
            measurement: self.global.into(),
        });
        Metric::new(LookupMetric {
            name: self.name.into(),
            measurements: self.measurements,
            help: self.help.into(),
            metric_type: self.metric_type.into(),
        })
    }
}

/// Like [`Series`], for time: recorded in microseconds, reported in
/// milliseconds with three decimal places, like the pool metrics.
struct TimeSeries {
    name: &'static str,
    help: &'static str,
    metric_type: &'static str,
    measurements: Vec<Measurement>,
    global: u64,
}

impl TimeSeries {
    fn new(name: &'static str, help: &'static str, metric_type: &'static str) -> Self {
        Self {
            name,
            help,
            metric_type,
            measurements: vec![],
            global: 0,
        }
    }

    fn record(&mut self, labels: &[(String, String)], micros: u64) {
        self.measurements.push(Measurement {
            labels: labels.to_vec(),
            measurement: (micros as f64 / 1_000.0).into(),
        });
        self.global += micros;
    }

    fn finish(mut self) -> Metric {
        self.measurements.push(Measurement {
            labels: vec![],
            measurement: (self.global as f64 / 1_000.0).into(),
        });
        Metric::new(LookupMetric {
            name: self.name.into(),
            measurements: self.measurements,
            help: self.help.into(),
            metric_type: self.metric_type.into(),
        })
    }
}

impl LookupMetrics {
    pub(crate) fn load() -> Vec<Metric> {
        let mut hits = Series::new(
            "sharding_lookup_cache_hits",
            "Sharding key values translated from the lookup cache.",
            "counter",
        );
        let mut misses = Series::new(
            "sharding_lookup_cache_misses",
            "Sharding key values that missed the lookup cache.",
            "counter",
        );
        let mut evictions = Series::new(
            "sharding_lookup_cache_evictions",
            "Lookup cache entries evicted to stay within the memory bound.",
            "counter",
        );
        let mut queries = Series::new(
            "sharding_lookup_queries",
            "Lookup queries run to resolve cache misses.",
            "counter",
        );
        let mut query_time = TimeSeries::new(
            "sharding_lookup_query_time",
            "Total time spent running lookup queries, in milliseconds. \
             Divided by sharding_lookup_queries, the average lookup latency.",
            "counter",
        );
        let mut bytes = Series::new(
            "sharding_lookup_cache_bytes",
            "Approximate memory used by cached translations.",
            "gauge",
        );
        let mut entries = Series::new(
            "sharding_lookup_cache_entries",
            "Number of cached translations.",
            "gauge",
        );

        for (user, cluster) in databases().all() {
            let labels = vec![
                ("user".into(), user.user.clone()),
                ("database".into(), user.database.clone()),
            ];

            let lookup = cluster.stats().lock().lookup.clone();
            hits.record(&labels, lookup.hits.load(Ordering::Relaxed));
            misses.record(&labels, lookup.misses.load(Ordering::Relaxed));
            evictions.record(&labels, lookup.evictions.load(Ordering::Relaxed));
            queries.record(&labels, lookup.lookups.load(Ordering::Relaxed));
            query_time.record(&labels, lookup.lookup_time_us.load(Ordering::Relaxed));

            let (cache_bytes, cache_entries) =
                cluster.sharding_schema().tables.lookup_cache().size();
            bytes.record(&labels, cache_bytes);
            entries.record(&labels, cache_entries);
        }

        vec![
            hits.finish(),
            misses.finish(),
            evictions.finish(),
            queries.finish(),
            query_time.finish(),
            bytes.finish(),
            entries.finish(),
        ]
    }
}

struct LookupMetric {
    name: String,
    measurements: Vec<Measurement>,
    help: String,
    metric_type: String,
}

impl OpenMetric for LookupMetric {
    fn name(&self) -> String {
        self.name.clone()
    }

    fn measurements(&self) -> Vec<Measurement> {
        self.measurements.clone()
    }

    fn help(&self) -> Option<String> {
        Some(self.help.clone())
    }

    fn metric_type(&self) -> String {
        self.metric_type.clone()
    }
}

impl std::fmt::Display for LookupMetrics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for metric in LookupMetrics::load() {
            writeln!(f, "{}", metric)?;
        }
        Ok(())
    }
}
