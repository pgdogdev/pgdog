use std::sync::atomic::{AtomicU64, Ordering};

use super::*;

static HITS: AtomicU64 = AtomicU64::new(0);
static MISSES: AtomicU64 = AtomicU64::new(0);
static STORES: AtomicU64 = AtomicU64::new(0);
static BYTES_SERVED: AtomicU64 = AtomicU64::new(0);
static BYTES_STORED: AtomicU64 = AtomicU64::new(0);
static REDIS_ERRORS: AtomicU64 = AtomicU64::new(0);

pub struct ResultCacheMetric {
    name: String,
    help: String,
    value: u64,
    gauge: bool,
}

pub struct ResultCache;

impl ResultCache {
    pub fn hit(bytes: usize) {
        HITS.fetch_add(1, Ordering::Relaxed);
        BYTES_SERVED.fetch_add(bytes as u64, Ordering::Relaxed);
    }

    pub fn miss() {
        MISSES.fetch_add(1, Ordering::Relaxed);
    }

    pub fn store(bytes: usize) {
        STORES.fetch_add(1, Ordering::Relaxed);
        BYTES_STORED.fetch_add(bytes as u64, Ordering::Relaxed);
    }

    pub fn redis_error() {
        REDIS_ERRORS.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn metrics() -> Vec<Metric> {
        vec![
            Metric::new(ResultCacheMetric {
                name: "result_cache_hits".into(),
                help: "Result cache hits (cached SELECT results served)".into(),
                value: HITS.load(Ordering::Relaxed),
                gauge: false,
            }),
            Metric::new(ResultCacheMetric {
                name: "result_cache_misses".into(),
                help: "Result cache misses (cacheable SELECT not found)".into(),
                value: MISSES.load(Ordering::Relaxed),
                gauge: false,
            }),
            Metric::new(ResultCacheMetric {
                name: "result_cache_stores".into(),
                help: "Result cache stores (cache entries written)".into(),
                value: STORES.load(Ordering::Relaxed),
                gauge: false,
            }),
            Metric::new(ResultCacheMetric {
                name: "result_cache_bytes_served".into(),
                help: "Bytes served from result cache".into(),
                value: BYTES_SERVED.load(Ordering::Relaxed),
                gauge: false,
            }),
            Metric::new(ResultCacheMetric {
                name: "result_cache_bytes_stored".into(),
                help: "Bytes stored into result cache".into(),
                value: BYTES_STORED.load(Ordering::Relaxed),
                gauge: false,
            }),
            Metric::new(ResultCacheMetric {
                name: "result_cache_redis_errors".into(),
                help: "Redis errors encountered by result cache".into(),
                value: REDIS_ERRORS.load(Ordering::Relaxed),
                gauge: false,
            }),
        ]
    }
}

impl OpenMetric for ResultCacheMetric {
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

