//! Clients metrics.

use crate::frontend::comms::comms;

use super::{Measurement, Metric, OpenMetric};

pub struct Clients {
    total: f64,
}

impl Clients {
    pub fn load() -> Metric {
        let total = comms().clients_len();
        Metric::new(Self {
            total: total as f64,
        })
    }
}

impl OpenMetric for Clients {
    fn name(&self) -> String {
        "clients".into()
    }

    fn measurements(&self) -> Vec<Measurement> {
        vec![Measurement {
            labels: vec![],
            measurement: self.total,
        }]
    }

    fn help(&self) -> Option<String> {
        Some("Total number of connected clients.".into())
    }
}

#[cfg(test)]
mod test {
    use crate::stats::Metric;

    use super::*;

    #[test]
    fn test_clients() {
        let clients = Clients { total: 25.0 };
        let metric = Metric::new(clients);
        let metric = metric.to_string();
        let mut lines = metric.lines();
        assert_eq!(lines.next().unwrap(), "# TYPE clients gauge");
        assert_eq!(
            lines.next().unwrap(),
            "# HELP clients Total number of connected clients."
        );
        assert_eq!(lines.next().unwrap(), "clients 25.000");
    }
}
