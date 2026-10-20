//! Authentication token cache metrics.

use crate::auth::{AUTH_TOKEN_CACHE, token_cache::TokenCacheStats};

use super::{Measurement, Metric, OpenMetric};

pub(crate) struct TokenCache {
    stats: TokenCacheStats,
}

impl TokenCache {
    pub(crate) fn load() -> Self {
        Self {
            stats: AUTH_TOKEN_CACHE.stats(),
        }
    }

    pub(crate) fn metrics(&self) -> Vec<Metric> {
        vec![
            Metric::new(TokenCacheMetric {
                name: "token_cache_entries",
                help: "Number of entries in the authentication token cache",
                value: self.stats.entries,
                metric_type: "gauge",
            }),
            Metric::new(TokenCacheMetric {
                name: "token_cache_evictions",
                help: "Number of entries evicted from the authentication token cache",
                value: self.stats.evictions,
                metric_type: "counter",
            }),
            Metric::new(TokenCacheMetric {
                name: "token_cache_hits",
                help: "Number of authentication token cache hits",
                value: self.stats.hits,
                metric_type: "counter",
            }),
            Metric::new(TokenCacheMetric {
                name: "token_cache_misses",
                help: "Number of authentication token cache misses",
                value: self.stats.misses,
                metric_type: "counter",
            }),
        ]
    }
}

struct TokenCacheMetric {
    name: &'static str,
    help: &'static str,
    value: u64,
    metric_type: &'static str,
}

impl OpenMetric for TokenCacheMetric {
    fn name(&self) -> String {
        self.name.into()
    }

    fn metric_type(&self) -> String {
        self.metric_type.into()
    }

    fn help(&self) -> Option<String> {
        Some(self.help.into())
    }

    fn measurements(&self) -> Vec<Measurement> {
        vec![Measurement {
            labels: vec![],
            measurement: self.value.into(),
        }]
    }
}
