//! Open metrics.

use std::ops::Deref;

use crate::config::config;

pub(crate) trait OpenMetric: Send + Sync {
    fn name(&self) -> String;
    /// Metric measurement.
    fn measurements(&self) -> Vec<Measurement>;
    /// Metric unit.
    fn unit(&self) -> Option<String> {
        None
    }

    fn metric_type(&self) -> String {
        "gauge".into()
    }
    fn help(&self) -> Option<String> {
        None
    }
}

#[derive(Debug, Clone)]
pub(crate) enum MeasurementType {
    Float(f64),
    Integer(i64),
    Millis(u128),
}

impl From<f64> for MeasurementType {
    fn from(value: f64) -> Self {
        Self::Float(value)
    }
}

impl From<i64> for MeasurementType {
    fn from(value: i64) -> Self {
        Self::Integer(value)
    }
}

impl From<usize> for MeasurementType {
    fn from(value: usize) -> Self {
        Self::Integer(value as i64)
    }
}

impl From<u128> for MeasurementType {
    fn from(value: u128) -> Self {
        Self::Millis(value)
    }
}

#[derive(Debug, Clone)]
pub(crate) struct Measurement {
    pub(crate) labels: Vec<(String, String)>,
    pub(crate) measurement: MeasurementType,
}

impl Measurement {
    pub(crate) fn render(&self, name: &str) -> String {
        let labels = if self.labels.is_empty() {
            "".into()
        } else {
            let labels = self
                .labels
                .iter()
                .map(|(name, value)| format!("{}=\"{}\"", name, value))
                .collect::<Vec<_>>();
            format!("{{{}}}", labels.join(","))
        };
        format!(
            "{}{} {}",
            name,
            labels,
            match self.measurement {
                MeasurementType::Float(f) => format!("{:.3}", f),
                MeasurementType::Integer(i) => i.to_string(),
                MeasurementType::Millis(i) => i.to_string(),
            }
        )
    }
}

pub(crate) struct Metric {
    metric: Box<dyn OpenMetric>,
}

impl Metric {
    pub(crate) fn new(metric: impl OpenMetric + 'static) -> Self {
        Self {
            metric: Box::new(metric),
        }
    }
}

impl Deref for Metric {
    type Target = Box<dyn OpenMetric>;

    fn deref(&self) -> &Self::Target {
        &self.metric
    }
}

impl std::fmt::Display for Metric {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let name = self.name();
        let config = config();
        let prefix = config
            .config
            .general
            .openmetrics_namespace
            .as_deref()
            .unwrap_or("");
        writeln!(f, "# TYPE {}{} {}", prefix, name, self.metric_type())?;
        if let Some(unit) = self.unit() {
            writeln!(f, "# UNIT {}{} {}", prefix, name, unit)?;
        }
        if let Some(help) = self.help() {
            writeln!(f, "# HELP {}{} {}", prefix, name, help)?;
        }

        for measurement in self.measurements() {
            writeln!(f, "{}{}", prefix, measurement.render(&name))?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use crate::config::{self, ConfigAndUsers};

    use super::*;

    #[test]
    fn test_prefix() {
        struct TestMetric;

        impl OpenMetric for TestMetric {
            fn name(&self) -> String {
                "test".into()
            }

            fn measurements(&self) -> Vec<Measurement> {
                vec![Measurement {
                    labels: vec![],
                    measurement: MeasurementType::Integer(5),
                }]
            }
        }

        let render = Metric::new(TestMetric {}).to_string();
        assert_eq!(render.lines().last().unwrap(), "test 5");

        let mut cfg = ConfigAndUsers::default();
        cfg.config.general.openmetrics_namespace = Some("pgdog.".into());
        config::set(cfg).unwrap();

        let render = Metric::new(TestMetric {}).to_string();
        assert_eq!(render.lines().next().unwrap(), "# TYPE pgdog.test gauge");
        assert_eq!(render.lines().last().unwrap(), "pgdog.test 5");
    }
}
