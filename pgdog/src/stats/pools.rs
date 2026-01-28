use crate::backend::{self, databases::databases};
use crate::util::millis;

use super::{Measurement, Metric, OpenMetric};

pub struct PoolMetric {
    pub name: String,
    pub measurements: Vec<Measurement>,
    pub help: String,
    pub unit: Option<String>,
    pub metric_type: Option<String>,
}

impl OpenMetric for PoolMetric {
    fn help(&self) -> Option<String> {
        Some(self.help.clone())
    }

    fn name(&self) -> String {
        self.name.clone()
    }

    fn measurements(&self) -> Vec<Measurement> {
        self.measurements.clone()
    }

    fn unit(&self) -> Option<String> {
        self.unit.clone()
    }

    fn metric_type(&self) -> String {
        if let Some(ref metric_type) = self.metric_type {
            metric_type.clone()
        } else {
            "gauge".into()
        }
    }
}

pub struct Pools {
    metrics: Vec<Metric>,
}

impl Pools {
    pub fn load() -> Pools {
        let mut metrics = vec![];
        let mut cl_waiting = vec![];
        let mut sv_active = vec![];
        let mut sv_idle = vec![];
        let mut maxwait = vec![];
        let mut errors = vec![];
        let mut out_of_sync = vec![];
        let mut total_xact_count = vec![];
        let mut total_xact_2pc_count = vec![];
        let mut avg_xact_count = vec![];
        let mut avg_xact_2pc_count = vec![];
        let mut total_query_count = vec![];
        let mut avg_query_count = vec![];
        let mut total_sent = vec![];
        let mut avg_sent = vec![];
        let mut total_received = vec![];
        let mut avg_received = vec![];
        let mut total_xact_time = vec![];
        let mut avg_xact_time = vec![];
        let mut total_idle_xact_time = vec![];
        let mut avg_idle_xact_time = vec![];
        let mut total_query_time = vec![];
        let mut avg_query_time = vec![];
        let mut total_close = vec![];
        let mut avg_close = vec![];
        let mut total_server_errors = vec![];
        let mut avg_server_errors = vec![];
        let mut total_cleaned = vec![];
        let mut avg_cleaned = vec![];
        let mut total_rollbacks = vec![];
        let mut avg_rollbacks = vec![];
        let mut total_connect_time = vec![];
        let mut avg_connect_time = vec![];
        let mut total_connect_count = vec![];
        let mut avg_connect_count = vec![];
        let mut total_reads = vec![];
        let mut avg_reads = vec![];
        let mut total_writes = vec![];
        let mut avg_writes = vec![];
        let mut total_sv_xact_idle = vec![];

        for (user, cluster) in databases().all() {
            for (shard_num, shard) in cluster.shards().iter().enumerate() {
                for (role, pool) in shard.pools_with_roles() {
                    let state = pool.state();
                    let labels = vec![
                        ("user".into(), user.user.clone()),
                        ("database".into(), user.database.clone()),
                        ("host".into(), pool.addr().host.clone()),
                        ("port".into(), pool.addr().port.to_string()),
                        ("shard".into(), shard_num.to_string()),
                        ("role".into(), role.to_string()),
                    ];

                    cl_waiting.push(Measurement {
                        labels: labels.clone(),
                        measurement: state.waiting.into(),
                    });

                    sv_active.push(Measurement {
                        labels: labels.clone(),
                        measurement: state.checked_out.into(),
                    });

                    sv_idle.push(Measurement {
                        labels: labels.clone(),
                        measurement: state.idle.into(),
                    });

                    maxwait.push(Measurement {
                        labels: labels.clone(),
                        measurement: state.maxwait.as_secs_f64().into(),
                    });

                    errors.push(Measurement {
                        labels: labels.clone(),
                        measurement: state.errors.into(),
                    });

                    out_of_sync.push(Measurement {
                        labels: labels.clone(),
                        measurement: state.out_of_sync.into(),
                    });

                    let stats = state.stats;
                    let totals = stats.counts;
                    let averages = stats.averages;

                    total_xact_count.push(Measurement {
                        labels: labels.clone(),
                        measurement: totals.xact_count.into(),
                    });

                    total_xact_2pc_count.push(Measurement {
                        labels: labels.clone(),
                        measurement: totals.xact_2pc_count.into(),
                    });

                    avg_xact_count.push(Measurement {
                        labels: labels.clone(),
                        measurement: averages.xact_count.into(),
                    });

                    avg_xact_2pc_count.push(Measurement {
                        labels: labels.clone(),
                        measurement: averages.xact_2pc_count.into(),
                    });

                    total_query_count.push(Measurement {
                        labels: labels.clone(),
                        measurement: totals.query_count.into(),
                    });

                    avg_query_count.push(Measurement {
                        labels: labels.clone(),
                        measurement: averages.query_count.into(),
                    });

                    total_received.push(Measurement {
                        labels: labels.clone(),
                        measurement: totals.received.into(),
                    });

                    avg_received.push(Measurement {
                        labels: labels.clone(),
                        measurement: averages.received.into(),
                    });

                    total_sent.push(Measurement {
                        labels: labels.clone(),
                        measurement: totals.sent.into(),
                    });

                    avg_sent.push(Measurement {
                        labels: labels.clone(),
                        measurement: averages.sent.into(),
                    });

                    total_xact_time.push(Measurement {
                        labels: labels.clone(),
                        measurement: millis(totals.xact_time).into(),
                    });

                    avg_xact_time.push(Measurement {
                        labels: labels.clone(),
                        measurement: millis(averages.xact_time).into(),
                    });

                    total_idle_xact_time.push(Measurement {
                        labels: labels.clone(),
                        measurement: millis(totals.idle_xact_time).into(),
                    });

                    avg_idle_xact_time.push(Measurement {
                        labels: labels.clone(),
                        measurement: millis(averages.idle_xact_time).into(),
                    });

                    total_query_time.push(Measurement {
                        labels: labels.clone(),
                        measurement: millis(totals.query_time).into(),
                    });

                    avg_query_time.push(Measurement {
                        labels: labels.clone(),
                        measurement: millis(averages.query_time).into(),
                    });

                    total_close.push(Measurement {
                        labels: labels.clone(),
                        measurement: totals.close.into(),
                    });

                    avg_close.push(Measurement {
                        labels: labels.clone(),
                        measurement: averages.close.into(),
                    });

                    total_server_errors.push(Measurement {
                        labels: labels.clone(),
                        measurement: totals.errors.into(),
                    });

                    avg_server_errors.push(Measurement {
                        labels: labels.clone(),
                        measurement: averages.errors.into(),
                    });

                    total_cleaned.push(Measurement {
                        labels: labels.clone(),
                        measurement: totals.cleaned.into(),
                    });

                    avg_cleaned.push(Measurement {
                        labels: labels.clone(),
                        measurement: averages.cleaned.into(),
                    });

                    total_rollbacks.push(Measurement {
                        labels: labels.clone(),
                        measurement: totals.rollbacks.into(),
                    });

                    avg_rollbacks.push(Measurement {
                        labels: labels.clone(),
                        measurement: averages.rollbacks.into(),
                    });

                    total_connect_time.push(Measurement {
                        labels: labels.clone(),
                        measurement: millis(totals.connect_time).into(),
                    });

                    avg_connect_time.push(Measurement {
                        labels: labels.clone(),
                        measurement: millis(averages.connect_time).into(),
                    });

                    total_connect_count.push(Measurement {
                        labels: labels.clone(),
                        measurement: totals.connect_count.into(),
                    });

                    avg_connect_count.push(Measurement {
                        labels: labels.clone(),
                        measurement: averages.connect_count.into(),
                    });

                    total_reads.push(Measurement {
                        labels: labels.clone(),
                        measurement: totals.reads.into(),
                    });

                    avg_reads.push(Measurement {
                        labels: labels.clone(),
                        measurement: averages.reads.into(),
                    });

                    total_writes.push(Measurement {
                        labels: labels.clone(),
                        measurement: totals.writes.into(),
                    });

                    avg_writes.push(Measurement {
                        labels: labels.clone(),
                        measurement: averages.writes.into(),
                    });

                    total_sv_xact_idle.push(Measurement {
                        labels: labels.clone(),
                        measurement: backend::stats::idle_in_transaction(&pool).into(),
                    });
                }
            }
        }

        metrics.push(Metric::new(PoolMetric {
            name: "cl_waiting".into(),
            measurements: cl_waiting,
            help: "Clients waiting for a connection from a pool.".into(),
            unit: None,
            metric_type: None,
        }));

        metrics.push(Metric::new(PoolMetric {
            name: "sv_active".into(),
            measurements: sv_active,
            help: "Servers currently serving client requests.".into(),
            unit: None,
            metric_type: None,
        }));

        metrics.push(Metric::new(PoolMetric {
            name: "sv_idle".into(),
            measurements: sv_idle,
            help: "Servers available for clients to use.".into(),
            unit: None,
            metric_type: None,
        }));

        metrics.push(Metric::new(PoolMetric {
            name: "maxwait".into(),
            measurements: maxwait,
            help: "How long clients have been waiting for a connection.".into(),
            unit: Some("seconds".into()),
            metric_type: None,
        }));

        metrics.push(Metric::new(PoolMetric {
            name: "errors".into(),
            measurements: errors,
            help: "Errors connections in the pool have experienced.".into(),
            unit: None,
            metric_type: Some("counter".into()),
        }));

        metrics.push(Metric::new(PoolMetric {
            name: "out_of_sync".into(),
            measurements: out_of_sync,
            help: "Connections that have been returned to the pool in a broken state.".into(),
            unit: None,
            metric_type: Some("counter".into()),
        }));

        metrics.push(Metric::new(PoolMetric {
            name: "total_xact_count".into(),
            measurements: total_xact_count,
            help: "Total number of executed transactions.".into(),
            unit: None,
            metric_type: Some("counter".into()),
        }));

        metrics.push(Metric::new(PoolMetric {
            name: "total_xact_2pc_count".into(),
            measurements: total_xact_2pc_count,
            help: "Total number of executed two-phase commit transactions.".into(),
            unit: None,
            metric_type: Some("counter".into()),
        }));

        metrics.push(Metric::new(PoolMetric {
            name: "avg_xact_count".into(),
            measurements: avg_xact_count,
            help: "Average number of executed transactions per statistics period.".into(),
            unit: None,
            metric_type: None,
        }));

        metrics.push(Metric::new(PoolMetric {
            name: "avg_xact_2pc_count".into(),
            measurements: avg_xact_2pc_count,
            help: "Average number of executed two-phase commit transactions per statistics period."
                .into(),
            unit: None,
            metric_type: None,
        }));

        metrics.push(Metric::new(PoolMetric {
            name: "total_query_count".into(),
            measurements: total_query_count,
            help: "Total number of executed queries.".into(),
            unit: None,
            metric_type: Some("counter".into()),
        }));

        metrics.push(Metric::new(PoolMetric {
            name: "avg_query_count".into(),
            measurements: avg_query_count,
            help: "Average number of executed queries per statistics period.".into(),
            unit: None,
            metric_type: None,
        }));

        metrics.push(Metric::new(PoolMetric {
            name: "total_received".into(),
            measurements: total_received,
            help: "Total number of bytes received.".into(),
            unit: None,
            metric_type: Some("counter".into()),
        }));

        metrics.push(Metric::new(PoolMetric {
            name: "avg_received".into(),
            measurements: avg_received,
            help: "Average number of bytes received.".into(),
            unit: None,
            metric_type: Some("counter".into()),
        }));

        metrics.push(Metric::new(PoolMetric {
            name: "total_sent".into(),
            measurements: total_sent,
            help: "Total number of bytes sent.".into(),
            unit: None,
            metric_type: Some("counter".into()),
        }));

        metrics.push(Metric::new(PoolMetric {
            name: "avg_sent".into(),
            measurements: avg_sent,
            help: "Average number of bytes sent.".into(),
            unit: None,
            metric_type: None,
        }));

        metrics.push(Metric::new(PoolMetric {
            name: "total_xact_time".into(),
            measurements: total_xact_time,
            help: "Total time spent executing transactions.".into(),
            unit: None,
            metric_type: Some("counter".into()),
        }));

        metrics.push(Metric::new(PoolMetric {
            name: "avg_xact_time".into(),
            measurements: avg_xact_time,
            help: "Average time spent executing transactions.".into(),
            unit: None,
            metric_type: None,
        }));

        metrics.push(Metric::new(PoolMetric {
            name: "total_idle_xact_time".into(),
            measurements: total_idle_xact_time,
            help: "Total time spent idling inside transactions.".into(),
            unit: None,
            metric_type: Some("counter".into()),
        }));

        metrics.push(Metric::new(PoolMetric {
            name: "avg_idle_xact_time".into(),
            measurements: avg_idle_xact_time,
            help: "Average time spent idling inside transactions.".into(),
            unit: None,
            metric_type: None,
        }));

        metrics.push(Metric::new(PoolMetric {
            name: "total_query_time".into(),
            measurements: total_query_time,
            help: "Total time spent executing queries.".into(),
            unit: None,
            metric_type: Some("counter".into()),
        }));

        metrics.push(Metric::new(PoolMetric {
            name: "avg_query_time".into(),
            measurements: avg_query_time,
            help: "Average time spent executing queries.".into(),
            unit: None,
            metric_type: None,
        }));

        metrics.push(Metric::new(PoolMetric {
            name: "total_prepared_evictions".into(),
            measurements: total_close,
            help: "Total number of prepared statements closed because of cache evictions.".into(),
            unit: None,
            metric_type: Some("counter".into()),
        }));

        metrics.push(Metric::new(PoolMetric {
            name: "avg_prepared_evictions".into(),
            measurements: avg_close,
            help: "Average number of prepared statements closed because of cache evictions.".into(),
            unit: None,
            metric_type: None,
        }));

        metrics.push(Metric::new(PoolMetric {
            name: "total_server_errors".into(),
            measurements: total_server_errors,
            help: "Total number of errors returned by server connections.".into(),
            unit: None,
            metric_type: Some("counter".into()),
        }));

        metrics.push(Metric::new(PoolMetric {
            name: "avg_server_errors".into(),
            measurements: avg_server_errors,
            help: "Average number of errors returned by server connections.".into(),
            unit: None,
            metric_type: None,
        }));

        metrics.push(Metric::new(PoolMetric {
            name: "total_cleaned".into(),
            measurements: total_cleaned,
            help: "Total number of times server connections were cleaned from client parameters."
                .into(),
            unit: None,
            metric_type: Some("counter".into()),
        }));

        metrics.push(Metric::new(PoolMetric {
            name: "avg_cleaned".into(),
            measurements: avg_cleaned,
            help: "Average number of times server connections were cleaned from client parameters."
                .into(),
            unit: None,
            metric_type: None,
        }));

        metrics.push(Metric::new(PoolMetric {
            name: "total_rollbacks".into(),
            measurements: total_rollbacks,
            help:
                "Total number of abandoned transactions that had to be rolled back automatically."
                    .into(),
            unit: None,
            metric_type: Some("counter".into()),
        }));

        metrics.push(Metric::new(PoolMetric {
            name: "avg_rollbacks".into(),
            measurements: avg_rollbacks,
            help:
                "Average number of abandoned transactions that had to be rolled back automatically."
                    .into(),
            unit: None,
            metric_type: None,
        }));

        metrics.push(Metric::new(PoolMetric {
            name: "total_connect_time".into(),
            measurements: total_connect_time,
            help: "Total time spent connecting to servers.".into(),
            unit: None,
            metric_type: Some("counter".into()),
        }));

        metrics.push(Metric::new(PoolMetric {
            name: "avg_connect_time".into(),
            measurements: avg_connect_time,
            help: "Average time spent connecting to servers.".into(),
            unit: None,
            metric_type: None,
        }));

        metrics.push(Metric::new(PoolMetric {
            name: "total_connect_count".into(),
            measurements: total_connect_count,
            help: "Total number of connections established to servers.".into(),
            unit: None,
            metric_type: Some("counter".into()),
        }));

        metrics.push(Metric::new(PoolMetric {
            name: "avg_connect_count".into(),
            measurements: avg_connect_count,
            help: "Average number of connections established to servers.".into(),
            unit: None,
            metric_type: None,
        }));

        metrics.push(Metric::new(PoolMetric {
            name: "total_reads".into(),
            measurements: total_reads,
            help: "Total number of read transactions.".into(),
            unit: None,
            metric_type: Some("counter".into()),
        }));

        metrics.push(Metric::new(PoolMetric {
            name: "avg_reads".into(),
            measurements: avg_reads,
            help: "Average number of read transactions per statistics period.".into(),
            unit: None,
            metric_type: None,
        }));

        metrics.push(Metric::new(PoolMetric {
            name: "total_writes".into(),
            measurements: total_writes,
            help: "Total number of write transactions.".into(),
            unit: None,
            metric_type: Some("counter".into()),
        }));

        metrics.push(Metric::new(PoolMetric {
            name: "avg_writes".into(),
            measurements: avg_writes,
            help: "Average number of write transactions per statistics period.".into(),
            unit: None,
            metric_type: None,
        }));

        metrics.push(Metric::new(PoolMetric {
            name: "sv_idle_xact".into(),
            measurements: total_sv_xact_idle,
            help: "Servers currently idle in transaction.".into(),
            unit: None,
            metric_type: None,
        }));

        Pools { metrics }
    }
}

#[cfg(test)]
mod tests {
    use crate::config::{self, ConfigAndUsers};

    use super::*;

    #[test]
    fn pool_metric_defaults_to_gauge() {
        let metric = PoolMetric {
            name: "cl_waiting".into(),
            measurements: vec![Measurement {
                labels: vec![],
                measurement: 3usize.into(),
            }],
            help: "Waiting clients per pool".into(),
            unit: None,
            metric_type: None,
        };

        assert_eq!(metric.metric_type(), "gauge");
        assert!(metric.unit().is_none());
        assert_eq!(metric.help(), Some("Waiting clients per pool".into()));
    }

    #[test]
    fn pool_metric_renders_labels_and_unit() {
        config::set(ConfigAndUsers::default()).unwrap();

        let metric = PoolMetric {
            name: "sv_active".into(),
            measurements: vec![Measurement {
                labels: vec![
                    ("user".into(), "alice".into()),
                    ("database".into(), "app".into()),
                ],
                measurement: 5usize.into(),
            }],
            help: "Active servers per pool".into(),
            unit: Some("connections".into()),
            metric_type: Some("gauge".into()),
        };

        let rendered = Metric::new(metric).to_string();
        let lines: Vec<&str> = rendered.lines().collect();
        assert_eq!(lines[0], "# TYPE sv_active gauge");
        assert_eq!(lines[1], "# UNIT sv_active connections");
        assert_eq!(lines[2], "# HELP sv_active Active servers per pool");
        assert_eq!(lines[3], "sv_active{user=\"alice\",database=\"app\"} 5");
    }
}

impl std::fmt::Display for Pools {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for pool in &self.metrics {
            writeln!(f, "{}", pool)?
        }

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_pools() {
        let pools = Pools {
            metrics: vec![Metric::new(PoolMetric {
                name: "maxwait".into(),
                measurements: vec![Measurement {
                    measurement: 45.0.into(),
                    labels: vec![
                        ("database".into(), "test_db".into()),
                        ("user".into(), "test_user".into()),
                    ],
                }],
                help: "How long clients wait.".into(),
                unit: Some("seconds".into()),
                metric_type: Some("counter".into()), // Not correct, just testing display.
            })],
        };
        let rendered = pools.to_string();
        let mut lines = rendered.lines();
        assert_eq!(lines.next().unwrap(), "# TYPE maxwait counter");
        assert_eq!(lines.next().unwrap(), "# UNIT maxwait seconds");
        assert_eq!(
            lines.next().unwrap(),
            "# HELP maxwait How long clients wait."
        );
        assert_eq!(
            lines.next().unwrap(),
            r#"maxwait{database="test_db",user="test_user"} 45.000"#
        );
    }
}
