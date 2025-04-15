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
pub enum MeasurementType {
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
pub struct Measurement {
    pub labels: Vec<(String, String)>,
    pub measurement: MeasurementType,
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
