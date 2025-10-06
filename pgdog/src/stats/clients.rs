//! Clients metrics.

use crate::frontend::comms::comms;

use super::{Measurement, Metric, OpenMetric};

pub struct Clients {
    total: usize,
}

impl Clients {
    pub fn load() -> Metric {
        let total = comms().clients_len();
        Metric::new(Self { total })
    }
}

impl OpenMetric for Clients {
    fn name(&self) -> String {
        "clients".into()
    }

    fn measurements(&self) -> Vec<Measurement> {
        vec![Measurement {
            labels: vec![],
            measurement: self.total.into(),
        }]
    }

    fn help(&self) -> Option<String> {
        Some("Total number of connected clients.".into())
    }
}

#[cfg(test)]
mod test {
    use crate::{
        config::{self, ConfigAndUsers},
        stats::Metric,
    };

    use super::*;

    #[test]
    fn test_clients() {
        let clients = Clients { total: 25 };
        let metric = Metric::new(clients);
        let metric = metric.to_string();
        let mut lines = metric.lines();
        assert_eq!(lines.next().unwrap(), "# TYPE clients gauge");
        assert_eq!(
            lines.next().unwrap(),
            "# HELP clients Total number of connected clients."
        );
        assert_eq!(lines.next().unwrap(), "clients 25");
    }

    #[test]
    fn clients_load_uses_global_state() {
        config::set(ConfigAndUsers::default()).unwrap();

        let metric = Clients::load();
        let rendered = metric.to_string();
        let lines: Vec<&str> = rendered.lines().collect();
        assert_eq!(lines[0], "# TYPE clients gauge");
        assert_eq!(
            lines[1],
            "# HELP clients Total number of connected clients."
        );
        assert_eq!(lines.last().copied(), Some("clients 0"));
    }
}
