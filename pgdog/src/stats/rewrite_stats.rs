//! Rewrite stats OpenMetrics.

use crate::backend::databases::databases;

use super::{Measurement, Metric, OpenMetric};

pub struct RewriteStatsMetrics;

impl RewriteStatsMetrics {
    pub fn load() -> Vec<Metric> {
        let mut metrics = vec![];

        let mut parse_measurements = vec![];
        let mut bind_measurements = vec![];
        let mut simple_measurements = vec![];

        let mut global_parse = 0usize;
        let mut global_bind = 0usize;
        let mut global_simple = 0usize;

        for (user, cluster) in databases().all() {
            let stats = cluster.stats();
            let stats = stats.lock();
            let rewrite = stats.rewrite;

            let labels = vec![
                ("user".into(), user.user.clone()),
                ("database".into(), user.database.clone()),
            ];

            parse_measurements.push(Measurement {
                labels: labels.clone(),
                measurement: rewrite.parse.into(),
            });

            bind_measurements.push(Measurement {
                labels: labels.clone(),
                measurement: rewrite.bind.into(),
            });

            simple_measurements.push(Measurement {
                labels,
                measurement: rewrite.simple.into(),
            });

            global_parse += rewrite.parse;
            global_bind += rewrite.bind;
            global_simple += rewrite.simple;
        }

        parse_measurements.push(Measurement {
            labels: vec![],
            measurement: global_parse.into(),
        });

        bind_measurements.push(Measurement {
            labels: vec![],
            measurement: global_bind.into(),
        });

        simple_measurements.push(Measurement {
            labels: vec![],
            measurement: global_simple.into(),
        });

        metrics.push(Metric::new(RewriteStatsMetric {
            name: "rewrite_parse_count".into(),
            measurements: parse_measurements,
            help: "Number of Parse messages rewritten.".into(),
        }));

        metrics.push(Metric::new(RewriteStatsMetric {
            name: "rewrite_bind_count".into(),
            measurements: bind_measurements,
            help: "Number of Bind messages rewritten.".into(),
        }));

        metrics.push(Metric::new(RewriteStatsMetric {
            name: "rewrite_simple_count".into(),
            measurements: simple_measurements,
            help: "Number of simple queries rewritten.".into(),
        }));

        metrics
    }
}

struct RewriteStatsMetric {
    name: String,
    measurements: Vec<Measurement>,
    help: String,
}

impl OpenMetric for RewriteStatsMetric {
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
        "counter".into()
    }
}

impl std::fmt::Display for RewriteStatsMetrics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for metric in RewriteStatsMetrics::load() {
            writeln!(f, "{}", metric)?;
        }
        Ok(())
    }
}
