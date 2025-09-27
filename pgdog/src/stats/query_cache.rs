use crate::{
    frontend::{
        router::parser::{cache::Stats, Cache},
        PreparedStatements,
    },
    stats::memory::MemoryUsage,
};

use super::*;

pub struct QueryCacheMetric {
    name: String,
    help: String,
    value: usize,
    gauge: bool,
}

pub struct QueryCache {
    stats: Stats,
    len: usize,
    prepared_statements: usize,
    prepared_statements_memory: usize,
}

impl QueryCache {
    pub(crate) fn load() -> Self {
        let (prepared_statements, prepared_statements_memory) = {
            let global = PreparedStatements::global();
            let guard = global.read();
            (guard.len(), guard.memory_usage())
        };

        let (stats, len) = Cache::stats();

        QueryCache {
            stats,
            len,
            prepared_statements,
            prepared_statements_memory,
        }
    }

    pub(crate) fn metrics(&self) -> Vec<Metric> {
        vec![
            Metric::new(QueryCacheMetric {
                name: "query_cache_hits".into(),
                help: "Queries already present in the query cache".into(),
                value: self.stats.hits,
                gauge: false,
            }),
            Metric::new(QueryCacheMetric {
                name: "query_cache_misses".into(),
                help: "New queries added to the query cache".into(),
                value: self.stats.misses,
                gauge: false,
            }),
            Metric::new(QueryCacheMetric {
                name: "query_cache_direct".into(),
                help: "Queries sent directly to a single shard".into(),
                value: self.stats.direct,
                gauge: false,
            }),
            Metric::new(QueryCacheMetric {
                name: "query_cache_cross".into(),
                help: "Queries sent to multiple or all shards".into(),
                value: self.stats.multi,
                gauge: false,
            }),
            Metric::new(QueryCacheMetric {
                name: "query_cache_size".into(),
                help: "Number of queries in the cache".into(),
                value: self.len,
                gauge: true,
            }),
            Metric::new(QueryCacheMetric {
                name: "prepared_statements".into(),
                help: "Number of prepared statements in the cache".into(),
                value: self.prepared_statements,
                gauge: true,
            }),
            Metric::new(QueryCacheMetric {
                name: "prepared_statements_memory_used".into(),
                help: "Amount of bytes used for the prepared statements cache".into(),
                value: self.prepared_statements_memory,
                gauge: true,
            }),
        ]
    }
}

impl OpenMetric for QueryCacheMetric {
    fn name(&self) -> String {
        self.name.clone()
    }

    fn metric_type(&self) -> String {
        if self.gauge {
            "gauge".into()
        } else {
            "counter".into()
        }
    }

    fn help(&self) -> Option<String> {
        Some(self.help.clone())
    }

    fn measurements(&self) -> Vec<Measurement> {
        vec![Measurement {
            labels: vec![],
            measurement: MeasurementType::Integer(self.value as i64),
        }]
    }
}

#[cfg(test)]
mod tests {
    use crate::config::{self, ConfigAndUsers};

    use super::*;

    #[test]
    fn query_cache_metric_renders_counter_and_gauge() {
        config::set(ConfigAndUsers::default()).unwrap();

        let counter_metric = QueryCacheMetric {
            name: "query_cache_hits".into(),
            help: "Hits".into(),
            value: 7,
            gauge: false,
        };
        let counter_render = Metric::new(counter_metric).to_string();
        let counter_lines: Vec<&str> = counter_render.lines().collect();
        assert_eq!(counter_lines[0], "# TYPE query_cache_hits counter");
        assert_eq!(counter_lines[2], "query_cache_hits 7");

        let gauge_metric = QueryCacheMetric {
            name: "query_cache_size".into(),
            help: "Size".into(),
            value: 3,
            gauge: true,
        };
        let gauge_render = Metric::new(gauge_metric).to_string();
        let gauge_lines: Vec<&str> = gauge_render.lines().collect();
        assert_eq!(gauge_lines[0], "# TYPE query_cache_size gauge");
        assert_eq!(gauge_lines[2], "query_cache_size 3");
    }

    #[test]
    fn query_cache_metrics_expose_all_counters() {
        let cache = QueryCache {
            stats: Stats {
                hits: 1,
                misses: 2,
                direct: 3,
                multi: 4,
            },
            len: 5,
            prepared_statements: 6,
            prepared_statements_memory: 7,
        };

        let metrics = cache.metrics();
        let metric_names: Vec<String> = metrics.iter().map(|metric| metric.name()).collect();
        assert_eq!(
            metric_names,
            vec![
                "query_cache_hits".to_string(),
                "query_cache_misses".to_string(),
                "query_cache_direct".to_string(),
                "query_cache_cross".to_string(),
                "query_cache_size".to_string(),
                "prepared_statements".to_string(),
                "prepared_statements_memory_used".to_string(),
            ]
        );

        let hits_metric = &metrics[0];
        let hits_value = hits_metric
            .measurements()
            .first()
            .unwrap()
            .measurement
            .clone();
        match hits_value {
            MeasurementType::Integer(value) => assert_eq!(value, 1),
            other => panic!("expected integer measurement, got {:?}", other),
        }

        let memory_metric = metrics.last().unwrap();
        assert_eq!(memory_metric.metric_type(), "gauge");
        let rendered = memory_metric.to_string();
        assert!(rendered.contains("prepared_statements_memory_used 7"));
    }
}
