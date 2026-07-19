//! Two-phase commit metrics.

use crate::frontend::client::query_engine::two_pc::Manager;

use super::{Measurement, Metric, OpenMetric};

pub struct TwoPc {
    recovered_total: u64,
}

impl TwoPc {
    pub fn load() -> Metric {
        let stats = Manager::get().stats();
        Metric::new(Self {
            recovered_total: stats.recovered_total(),
        })
    }
}

impl OpenMetric for TwoPc {
    fn name(&self) -> String {
        "two_pc_recovered_total".into()
    }

    fn metric_type(&self) -> String {
        "counter".into()
    }

    fn help(&self) -> Option<String> {
        Some(
            "Total number of in-flight 2PC transactions restored from the WAL during recovery."
                .into(),
        )
    }

    fn measurements(&self) -> Vec<Measurement> {
        vec![Measurement {
            labels: vec![],
            measurement: self.recovered_total.into(),
        }]
    }
}
