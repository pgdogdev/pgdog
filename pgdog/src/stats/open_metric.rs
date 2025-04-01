//! Open metrics.

use std::ops::Deref;

pub trait OpenMetric: Send + Sync {
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
pub struct Measurement {
    pub labels: Vec<(String, String)>,
    pub measurement: f64,
}

impl Measurement {
    pub fn render(&self, name: &str) -> String {
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
        format!("{}{} {:.3}", name, labels, self.measurement)
    }
}

pub struct Metric {
    metric: Box<dyn OpenMetric>,
}

impl Metric {
    pub fn new(metric: impl OpenMetric + 'static) -> Self {
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
        writeln!(f, "# TYPE {} {}", name, self.metric_type())?;
        if let Some(unit) = self.unit() {
            writeln!(f, "# UNIT {} {}", name, unit)?;
        }
        if let Some(help) = self.help() {
            writeln!(f, "# HELP {} {}", name, help)?;
        }

        for measurement in self.measurements() {
            writeln!(f, "{}", measurement.render(&name))?;
        }
        Ok(())
    }
}
