use crate::backend::databases::databases;

use super::{Measurement, Metric, OpenMetric};

struct PoolMetric {
    name: String,
    measurements: Vec<Measurement>,
    help: String,
    unit: Option<String>,
    metric_type: Option<String>,
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
        for (user, cluster) in databases().all() {
            for shard in cluster.shards() {
                for pool in shard.pools() {
                    let state = pool.state();
                    let labels = vec![
                        ("user".into(), user.user.clone()),
                        ("database".into(), user.database.clone()),
                        ("host".into(), pool.addr().host.clone()),
                        ("port".into(), pool.addr().port.to_string()),
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

        Pools { metrics }
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
