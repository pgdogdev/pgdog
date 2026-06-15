use crate::backend::pub_sub::listener;

use super::{Measurement, Metric, OpenMetric};

pub struct Listeners;

impl Listeners {
    pub fn load() -> Vec<Metric> {
        let mut stats: Vec<_> = listener::stats().into_iter().collect();
        stats.sort_by(|a, b| a.0.cmp(&b.0));

        let mut listeners = vec![];
        let mut received = vec![];
        let mut dropped = vec![];

        for (channel, stats) in stats {
            let labels = vec![("channel".into(), channel)];

            listeners.push(Measurement {
                labels: labels.clone(),
                measurement: stats.listeners.into(),
            });
            received.push(Measurement {
                labels: labels.clone(),
                measurement: stats.recv.into(),
            });
            dropped.push(Measurement {
                labels,
                measurement: stats.dropped.into(),
            });
        }

        vec![
            Metric::new(ListenerMetric {
                name: "pub_sub_listeners".into(),
                measurements: listeners,
                help: "Current number of clients listening on a pub/sub channel.".into(),
                metric_type: "gauge".into(),
            }),
            Metric::new(ListenerMetric {
                name: "pub_sub_listener_received".into(),
                measurements: received,
                help: "Total number of notifications received by pub/sub listeners.".into(),
                metric_type: "counter".into(),
            }),
            Metric::new(ListenerMetric {
                name: "pub_sub_listener_dropped".into(),
                measurements: dropped,
                help: "Total number of notifications dropped by lagging pub/sub listeners.".into(),
                metric_type: "counter".into(),
            }),
        ]
    }
}

struct ListenerMetric {
    name: String,
    measurements: Vec<Measurement>,
    help: String,
    metric_type: String,
}

impl OpenMetric for ListenerMetric {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn listener_metrics_include_expected_series() {
        let metrics = Listeners::load();
        let names: Vec<_> = metrics.iter().map(|metric| metric.name()).collect();

        assert_eq!(
            names,
            [
                "pub_sub_listeners",
                "pub_sub_listener_received",
                "pub_sub_listener_dropped",
            ]
        );
        assert_eq!(metrics[0].metric_type(), "gauge");
        assert_eq!(metrics[1].metric_type(), "counter");
        assert_eq!(metrics[2].metric_type(), "counter");
    }
}
