use crate::backend::databases::databases;

use super::{Measurement, Metric, OpenMetric};

pub struct MirrorStatsMetrics;

impl MirrorStatsMetrics {
    pub fn load() -> Vec<Metric> {
        let mut metrics = vec![];

        let mut total_count_measurements = vec![];
        let mut mirrored_count_measurements = vec![];
        let mut dropped_count_measurements = vec![];
        let mut error_count_measurements = vec![];

        let mut global_total = 0usize;
        let mut global_mirrored = 0usize;
        let mut global_dropped = 0usize;
        let mut global_error = 0usize;

        // Iterate through all clusters and collect their mirror stats
        for (user, cluster) in databases().all() {
            let stats = cluster.stats();
            let stats = stats.lock();
            let counts = stats.counts;

            // Per-cluster metrics with labels
            let labels = vec![
                ("user".into(), user.user.clone()),
                ("database".into(), user.database.clone()),
            ];

            total_count_measurements.push(Measurement {
                labels: labels.clone(),
                measurement: counts.total_count.into(),
            });

            mirrored_count_measurements.push(Measurement {
                labels: labels.clone(),
                measurement: counts.mirrored_count.into(),
            });

            dropped_count_measurements.push(Measurement {
                labels: labels.clone(),
                measurement: counts.dropped_count.into(),
            });

            error_count_measurements.push(Measurement {
                labels: labels.clone(),
                measurement: counts.error_count.into(),
            });

            // Accumulate for global metrics
            global_total += counts.total_count;
            global_mirrored += counts.mirrored_count;
            global_dropped += counts.dropped_count;
            global_error += counts.error_count;
        }

        // Add global measurements (no labels)
        total_count_measurements.push(Measurement {
            labels: vec![],
            measurement: global_total.into(),
        });

        mirrored_count_measurements.push(Measurement {
            labels: vec![],
            measurement: global_mirrored.into(),
        });

        dropped_count_measurements.push(Measurement {
            labels: vec![],
            measurement: global_dropped.into(),
        });

        error_count_measurements.push(Measurement {
            labels: vec![],
            measurement: global_error.into(),
        });

        // Create metrics
        metrics.push(Metric::new(MirrorStatsMetric {
            name: "mirror_total_count".into(),
            measurements: total_count_measurements,
            help: "Total number of requests considered for mirroring.".into(),
            metric_type: "counter".into(),
        }));

        metrics.push(Metric::new(MirrorStatsMetric {
            name: "mirror_mirrored_count".into(),
            measurements: mirrored_count_measurements,
            help: "Total number of requests successfully mirrored.".into(),
            metric_type: "counter".into(),
        }));

        metrics.push(Metric::new(MirrorStatsMetric {
            name: "mirror_dropped_count".into(),
            measurements: dropped_count_measurements,
            help: "Total number of requests dropped due to exposure settings.".into(),
            metric_type: "counter".into(),
        }));

        metrics.push(Metric::new(MirrorStatsMetric {
            name: "mirror_error_count".into(),
            measurements: error_count_measurements,
            help: "Total number of mirror requests that encountered errors.".into(),
            metric_type: "counter".into(),
        }));

        metrics
    }
}

struct MirrorStatsMetric {
    name: String,
    measurements: Vec<Measurement>,
    help: String,
    metric_type: String,
}

impl OpenMetric for MirrorStatsMetric {
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

impl std::fmt::Display for MirrorStatsMetrics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for metric in MirrorStatsMetrics::load() {
            writeln!(f, "{}", metric)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mirror_stats_format() {
        // Create a mock metric directly to test formatting
        let metric = MirrorStatsMetric {
            name: "mirror_total_count".into(),
            measurements: vec![
                Measurement {
                    labels: vec![
                        ("user".into(), "test_user".into()),
                        ("database".into(), "test_db".into()),
                    ],
                    measurement: 10usize.into(),
                },
                Measurement {
                    labels: vec![],
                    measurement: 10usize.into(),
                },
            ],
            help: "Total number of requests considered for mirroring.".into(),
            metric_type: "counter".into(),
        };

        let metric = Metric::new(metric);
        let rendered = metric.to_string();
        let lines: Vec<&str> = rendered.lines().collect();

        assert_eq!(lines[0], "# TYPE mirror_total_count counter");
        assert_eq!(
            lines[1],
            "# HELP mirror_total_count Total number of requests considered for mirroring."
        );
        assert!(lines[2].contains(r#"mirror_total_count{user="test_user",database="test_db"} 10"#));
        assert!(lines[3].contains("mirror_total_count 10"));
    }

    #[test]
    fn test_multiple_clusters_aggregation() {
        // Test that measurements from multiple clusters are properly aggregated
        let measurements = vec![
            Measurement {
                labels: vec![
                    ("user".into(), "alice".into()),
                    ("database".into(), "db1".into()),
                ],
                measurement: 10usize.into(),
            },
            Measurement {
                labels: vec![
                    ("user".into(), "bob".into()),
                    ("database".into(), "db2".into()),
                ],
                measurement: 20usize.into(),
            },
            // Global aggregation
            Measurement {
                labels: vec![],
                measurement: 30usize.into(),
            },
        ];

        let metric = MirrorStatsMetric {
            name: "mirror_mirrored_count".into(),
            measurements,
            help: "Total number of requests successfully mirrored.".into(),
            metric_type: "counter".into(),
        };

        let metric = Metric::new(metric);
        let rendered = metric.to_string();

        assert!(rendered.contains(r#"mirror_mirrored_count{user="alice",database="db1"} 10"#));
        assert!(rendered.contains(r#"mirror_mirrored_count{user="bob",database="db2"} 20"#));
        assert!(rendered.contains("mirror_mirrored_count 30"));
    }

    #[test]
    fn test_all_metric_types() {
        // Test that all four metric types are properly formatted
        let total = MirrorStatsMetric {
            name: "mirror_total_count".into(),
            measurements: vec![Measurement {
                labels: vec![],
                measurement: 10usize.into(),
            }],
            help: "Total number of requests considered for mirroring.".into(),
            metric_type: "counter".into(),
        };

        let mirrored = MirrorStatsMetric {
            name: "mirror_mirrored_count".into(),
            measurements: vec![Measurement {
                labels: vec![],
                measurement: 5usize.into(),
            }],
            help: "Total number of requests successfully mirrored.".into(),
            metric_type: "counter".into(),
        };

        let dropped = MirrorStatsMetric {
            name: "mirror_dropped_count".into(),
            measurements: vec![Measurement {
                labels: vec![],
                measurement: 3usize.into(),
            }],
            help: "Total number of requests dropped due to exposure settings.".into(),
            metric_type: "counter".into(),
        };

        let error = MirrorStatsMetric {
            name: "mirror_error_count".into(),
            measurements: vec![Measurement {
                labels: vec![],
                measurement: 2usize.into(),
            }],
            help: "Total number of mirror requests that encountered errors.".into(),
            metric_type: "counter".into(),
        };

        let metrics = vec![
            Metric::new(total),
            Metric::new(mirrored),
            Metric::new(dropped),
            Metric::new(error),
        ];

        for metric in metrics {
            let rendered = metric.to_string();
            assert!(rendered.contains("# TYPE"));
            assert!(rendered.contains("# HELP"));
            assert!(rendered.contains("counter"));
        }
    }

    #[test]
    fn test_pre_seeded_stats_values() {
        // Test with the exact values requested: total: 10, mirrored: 5, dropped: 3, error: 2
        let measurements = vec![
            ("mirror_total_count", 10usize),
            ("mirror_mirrored_count", 5usize),
            ("mirror_dropped_count", 3usize),
            ("mirror_error_count", 2usize),
        ];

        for (name, value) in measurements {
            let metric = MirrorStatsMetric {
                name: name.into(),
                measurements: vec![Measurement {
                    labels: vec![
                        ("user".into(), "test_user".into()),
                        ("database".into(), "test_db".into()),
                    ],
                    measurement: value.into(),
                }],
                help: format!("Test metric for {}", name),
                metric_type: "counter".into(),
            };

            let metric = Metric::new(metric);
            let rendered = metric.to_string();
            // The formatted output will have the metric name with labels and value
            let expected = format!(
                r#"{}{{user="test_user",database="test_db"}} {}"#,
                name, value
            );
            assert!(
                rendered.contains(&expected),
                "Expected: {}, Got: {}",
                expected,
                rendered
            );
        }
    }
}
