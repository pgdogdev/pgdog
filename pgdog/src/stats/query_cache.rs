use crate::frontend::router::parser::{cache::Stats, Cache};

use super::*;

pub struct QueryCacheMetric {
    name: String,
    help: String,
    value: usize,
    gauge: bool,
}

pub struct QueryCache {
    stats: Stats,
}

impl QueryCache {
    pub(crate) fn load() -> Self {
        QueryCache {
            stats: Cache::stats(),
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
                value: self.stats.size,
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
