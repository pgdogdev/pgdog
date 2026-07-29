//! Clients metrics.

use crate::frontend::comms::comms;

use super::{Measurement, Metric, OpenMetric};

pub struct ClientsLocked {
    total_locked: usize,
}

impl ClientsLocked {
    pub fn load() -> Metric {
        let total_locked = comms().clients_locked_count();
        Metric::new(Self { total_locked })
    }
}

impl OpenMetric for ClientsLocked {
    fn name(&self) -> String {
        "clients_locked".into()
    }

    fn measurements(&self) -> Vec<Measurement> {
        vec![Measurement {
            labels: vec![],
            measurement: self.total_locked.into(),
        }]
    }

    fn help(&self) -> Option<String> {
        Some("Total number of locked clients.".into())
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
    fn test_clients_locked() {
        let clients = ClientsLocked { total_locked: 25 };
        let metric = Metric::new(clients);
        let metric = metric.to_string();
        let mut lines = metric.lines();
        assert_eq!(lines.next().unwrap(), "# TYPE clients_locked gauge");
        assert_eq!(
            lines.next().unwrap(),
            "# HELP clients_locked Total number of locked clients."
        );
        assert_eq!(lines.next().unwrap(), "clients_locked 25");
    }

    #[test]
    fn clients_locked_load_uses_global_state() {
        config::set(ConfigAndUsers::default()).unwrap();

        let metric = ClientsLocked::load();
        let rendered = metric.to_string();
        let lines: Vec<&str> = rendered.lines().collect();
        assert_eq!(lines[0], "# TYPE clients_locked gauge");
        assert_eq!(
            lines[1],
            "# HELP clients_locked Total number of locked clients."
        );
        assert_eq!(lines.last().copied(), Some("clients_locked 0"));
    }
}
