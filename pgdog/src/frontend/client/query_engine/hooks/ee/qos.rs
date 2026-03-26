use std::{sync::Arc, time::Instant};

use dashmap::DashMap;
use once_cell::sync::Lazy;
use pgdog_stats::QosMeasurement as QosMeasurementStats;
use regex::Regex;

use super::super::Error;
use crate::frontend::client::query_engine::QueryEngineContext;

type QosMeasurements = Arc<DashMap<Arc<String>, QosMeasurement>>;

static QOS: Lazy<QosMeasurements> = Lazy::new(|| Arc::new(DashMap::new()));
static RE: Lazy<Regex> = Lazy::new(|| Regex::new(r#"\/\*.*?pgdog_qos: *([^;]+);.*?\*\/"#).unwrap());

#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct QosMeasurement {
    pub(crate) total: QosMeasurementStats,
    pub(crate) delta: QosMeasurementStats,
}

#[derive(Debug)]
struct QosGuard {
    unit: Arc<String>,
    started_at: Instant,
}

impl QosGuard {
    fn new(unit: Arc<String>) -> Self {
        Self {
            unit,
            started_at: Instant::now(),
        }
    }
}

impl Drop for QosGuard {
    fn drop(&mut self) {
        let elapsed = self.started_at.elapsed();

        let mut entry = QOS
            .entry(self.unit.clone())
            .or_insert_with(QosMeasurement::default);

        entry.total.count += 1;
        entry.total.time += elapsed;

        entry.delta.count += 1;
        entry.delta.time += elapsed;
    }
}

#[derive(Default, Debug)]
pub struct Qos {
    units: QosMeasurements,
    guard: Option<QosGuard>,
}

impl Qos {
    fn extract_tag(query: &str) -> Option<&str> {
        RE.captures(query).map(|c| c.get(1).unwrap().as_str())
    }

    pub(super) fn after_connected(
        &mut self,
        context: &QueryEngineContext<'_>,
    ) -> Result<(), Error> {
        if let Ok(Some(query)) = context.client_request.query() {
            if let Some(tag) = Self::extract_tag(query.query()) {
                self.guard = Some(QosGuard::new(Arc::new(tag.to_string())));
            }
        }
        Ok(())
    }

    pub(super) fn after_execution(
        &mut self,
        context: &QueryEngineContext<'_>,
    ) -> Result<(), Error> {
        self.guard.take();
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_parse_qos_unit() {
        let query = "SELECT 1 /* pgdog_qos: unit=api.users.create; */";
        assert_eq!(Qos::extract_tag(query), Some("unit=api.users.create"));
    }
}
