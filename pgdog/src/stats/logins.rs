//! Client login (startup + auth) statistics.
//!
//! The login path has a finite throughput per instance; when it saturates,
//! clients time out mid-startup and disappear without PgDog producing any
//! error or log line. These metrics make that failure mode observable:
//!
//! - `logins_in_flight`: connections accepted but not yet ReadyForQuery —
//!   the leading indicator of login-path saturation.
//! - `logins`: completed logins.
//! - `logins_abandoned`: clients that started the Postgres handshake but
//!   went away before login completed.
//! - `login_duration`: accept-to-ReadyForQuery latency histogram for
//!   completed logins.
//!
//! Connections that close without sending a single startup byte (TCP health
//! checks from load balancers and probes) are excluded from `logins_abandoned`.

use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::time::Instant;

use super::{Measurement, MeasurementType, Metric, OpenMetric};

/// Histogram bucket upper bounds, in milliseconds.
const BUCKETS_MS: [u64; 14] = [
    1, 2, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000, 30000,
];

/// Login counters shared by all client connections.
pub(crate) struct LoginStats {
    in_flight: AtomicI64,
    completed: AtomicU64,
    abandoned: AtomicU64,
    duration_sum_micros: AtomicU64,
    /// Non-cumulative bucket counts; rendered cumulatively.
    duration_buckets: [AtomicU64; BUCKETS_MS.len()],
    /// Observations above the last bucket bound.
    duration_overflow: AtomicU64,
}

impl LoginStats {
    pub(crate) const fn new() -> Self {
        Self {
            in_flight: AtomicI64::new(0),
            completed: AtomicU64::new(0),
            abandoned: AtomicU64::new(0),
            duration_sum_micros: AtomicU64::new(0),
            duration_buckets: [const { AtomicU64::new(0) }; BUCKETS_MS.len()],
            duration_overflow: AtomicU64::new(0),
        }
    }

    fn record_duration(&self, micros: u64) {
        self.duration_sum_micros
            .fetch_add(micros, Ordering::Relaxed);
        let millis = micros / 1_000;
        match BUCKETS_MS.iter().position(|bound| millis <= *bound) {
            Some(bucket) => &self.duration_buckets[bucket],
            None => &self.duration_overflow,
        }
        .fetch_add(1, Ordering::Relaxed);
    }
}

static LOGINS: LoginStats = LoginStats::new();

/// Global login stats.
pub(crate) fn logins() -> &'static LoginStats {
    &LOGINS
}

/// Tracks one connection through the login path.
///
/// Create it at accept. Call [`LoginTimer::engage`] once the connection sends
/// startup bytes (i.e., it's a Postgres client, not a TCP health check),
/// [`LoginTimer::success`] when the client reaches ReadyForQuery, and
/// [`LoginTimer::disarm`] when PgDog itself ends the login with an explicit
/// response (auth failure, shutdown, pool down) or the connection turns out
/// not to be a login (cancel request). Dropping the timer engaged but
/// unfinished counts the login as abandoned.
pub(crate) struct LoginTimer {
    stats: &'static LoginStats,
    start: Instant,
    engaged: bool,
    finished: bool,
}

impl LoginTimer {
    pub(crate) fn new() -> Self {
        Self::with_stats(logins())
    }

    fn with_stats(stats: &'static LoginStats) -> Self {
        stats.in_flight.fetch_add(1, Ordering::Relaxed);
        Self {
            stats,
            start: Instant::now(),
            engaged: false,
            finished: false,
        }
    }

    /// The connection sent startup bytes: it's a real client, not a probe.
    pub(crate) fn engage(&mut self) {
        self.engaged = true;
    }

    /// Login ended with an explicit response (or wasn't a login at all);
    /// don't count it as abandoned.
    pub(crate) fn disarm(&mut self) {
        self.finished = true;
    }

    /// The client reached ReadyForQuery.
    pub(crate) fn success(&mut self) {
        if self.finished {
            return;
        }
        self.finished = true;
        self.stats.completed.fetch_add(1, Ordering::Relaxed);
        self.stats
            .record_duration(self.start.elapsed().as_micros() as u64);
    }
}

impl Drop for LoginTimer {
    fn drop(&mut self) {
        self.stats.in_flight.fetch_sub(1, Ordering::Relaxed);
        if self.engaged && !self.finished {
            self.stats.abandoned.fetch_add(1, Ordering::Relaxed);
        }
    }
}

/// Login metrics for the OpenMetrics endpoint.
pub(crate) struct Logins;

impl Logins {
    /// Scalar login metrics (gauge + counters).
    pub(crate) fn load() -> Vec<Metric> {
        Self::load_from(logins())
    }

    fn load_from(stats: &LoginStats) -> Vec<Metric> {
        vec![
            Metric::new(LoginMetric {
                name: "logins_in_flight".into(),
                metric_type: "gauge".into(),
                help: "Connections accepted but not yet ReadyForQuery.".into(),
                value: stats.in_flight.load(Ordering::Relaxed).into(),
            }),
            Metric::new(LoginMetric {
                name: "logins".into(),
                metric_type: "counter".into(),
                help: "Total number of completed client logins.".into(),
                value: stats.completed.load(Ordering::Relaxed).into(),
            }),
            Metric::new(LoginMetric {
                name: "logins_abandoned".into(),
                metric_type: "counter".into(),
                help: "Clients that started the handshake but disconnected before login completed."
                    .into(),
                value: stats.abandoned.load(Ordering::Relaxed).into(),
            }),
        ]
    }

    /// Login duration histogram (OpenMetrics text format only).
    pub(crate) fn histogram() -> Metric {
        Self::histogram_from(logins())
    }

    fn histogram_from(stats: &LoginStats) -> Metric {
        let buckets = stats
            .duration_buckets
            .iter()
            .map(|bucket| bucket.load(Ordering::Relaxed))
            .collect();
        Metric::new(LoginDuration {
            buckets,
            overflow: stats.duration_overflow.load(Ordering::Relaxed),
            sum_micros: stats.duration_sum_micros.load(Ordering::Relaxed),
        })
    }
}

struct LoginMetric {
    name: String,
    metric_type: String,
    help: String,
    value: MeasurementType,
}

impl OpenMetric for LoginMetric {
    fn name(&self) -> String {
        self.name.clone()
    }

    fn measurements(&self) -> Vec<Measurement> {
        vec![Measurement {
            labels: vec![],
            measurement: self.value.clone(),
        }]
    }

    fn metric_type(&self) -> String {
        self.metric_type.clone()
    }

    fn help(&self) -> Option<String> {
        Some(self.help.clone())
    }
}

struct LoginDuration {
    buckets: Vec<u64>,
    overflow: u64,
    sum_micros: u64,
}

impl OpenMetric for LoginDuration {
    fn name(&self) -> String {
        "login_duration".into()
    }

    fn metric_type(&self) -> String {
        "histogram".into()
    }

    fn unit(&self) -> Option<String> {
        Some("milliseconds".into())
    }

    fn help(&self) -> Option<String> {
        Some("Accept-to-ReadyForQuery latency of completed client logins.".into())
    }

    fn measurements(&self) -> Vec<Measurement> {
        // Unused: histograms render through `render_measurements`.
        vec![]
    }

    fn render_measurements(
        &self,
        f: &mut std::fmt::Formatter<'_>,
        prefix: &str,
        name: &str,
    ) -> std::fmt::Result {
        let mut cumulative = 0u64;
        for (bound, count) in BUCKETS_MS.iter().zip(&self.buckets) {
            cumulative += count;
            writeln!(f, "{prefix}{name}_bucket{{le=\"{bound}\"}} {cumulative}")?;
        }
        let total = cumulative + self.overflow;
        let sum_ms = self.sum_micros as f64 / 1_000.0;
        writeln!(f, "{prefix}{name}_bucket{{le=\"+Inf\"}} {total}")?;
        writeln!(f, "{prefix}{name}_sum {sum_ms:.3}")?;
        writeln!(f, "{prefix}{name}_count {total}")?;
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;

    fn test_stats() -> &'static LoginStats {
        Box::leak(Box::new(LoginStats::new()))
    }

    #[test]
    fn success_records_completion_and_duration() {
        let stats = test_stats();
        let mut timer = LoginTimer::with_stats(stats);
        assert_eq!(stats.in_flight.load(Ordering::Relaxed), 1);
        timer.engage();
        timer.success();
        drop(timer);

        assert_eq!(stats.in_flight.load(Ordering::Relaxed), 0);
        assert_eq!(stats.completed.load(Ordering::Relaxed), 1);
        assert_eq!(stats.abandoned.load(Ordering::Relaxed), 0);
        let observed: u64 = stats
            .duration_buckets
            .iter()
            .map(|bucket| bucket.load(Ordering::Relaxed))
            .sum::<u64>()
            + stats.duration_overflow.load(Ordering::Relaxed);
        assert_eq!(observed, 1);
    }

    #[test]
    fn engaged_drop_counts_abandoned() {
        let stats = test_stats();
        let mut timer = LoginTimer::with_stats(stats);
        timer.engage();
        drop(timer);

        assert_eq!(stats.in_flight.load(Ordering::Relaxed), 0);
        assert_eq!(stats.abandoned.load(Ordering::Relaxed), 1);
        assert_eq!(stats.completed.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn health_check_drop_is_not_abandoned() {
        let stats = test_stats();
        let timer = LoginTimer::with_stats(stats);
        drop(timer);

        assert_eq!(stats.in_flight.load(Ordering::Relaxed), 0);
        assert_eq!(stats.abandoned.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn disarmed_drop_is_not_abandoned() {
        let stats = test_stats();
        let mut timer = LoginTimer::with_stats(stats);
        timer.engage();
        timer.disarm();
        drop(timer);

        assert_eq!(stats.abandoned.load(Ordering::Relaxed), 0);
        assert_eq!(stats.completed.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn histogram_renders_cumulative_buckets() {
        let stats = test_stats();
        stats.record_duration(1_500); // 1.5ms -> le=2
        stats.record_duration(1_500);
        stats.record_duration(600_000); // 600ms -> le=1000
        stats.record_duration(60_000_000); // 60s -> overflow

        let rendered = Logins::histogram_from(stats).to_string();
        assert!(rendered.contains("# TYPE login_duration histogram"));
        assert!(rendered.contains("login_duration_bucket{le=\"2\"} 2"));
        assert!(rendered.contains("login_duration_bucket{le=\"1000\"} 3"));
        assert!(rendered.contains("login_duration_bucket{le=\"30000\"} 3"));
        assert!(rendered.contains("login_duration_bucket{le=\"+Inf\"} 4"));
        assert!(rendered.contains("login_duration_count 4"));
        assert!(rendered.contains("login_duration_sum 60603.000"));
    }

    #[test]
    fn success_after_disarm_records_nothing() {
        let stats = test_stats();
        let mut timer = LoginTimer::with_stats(stats);
        timer.engage();
        timer.disarm();
        timer.success();
        drop(timer);

        assert_eq!(stats.completed.load(Ordering::Relaxed), 0);
        assert_eq!(stats.abandoned.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn global_stats_flow_end_to_end() {
        let completed_before = logins().completed.load(Ordering::Relaxed);

        let mut timer = LoginTimer::new();
        timer.engage();
        timer.success();
        drop(timer);

        // Other tests share the global stats, so assert deltas only.
        assert!(logins().completed.load(Ordering::Relaxed) > completed_before);

        let names: Vec<_> = Logins::load().iter().map(|metric| metric.name()).collect();
        assert_eq!(names, ["logins_in_flight", "logins", "logins_abandoned"]);

        let histogram = Logins::histogram();
        assert_eq!(histogram.metric_type(), "histogram");
        // Histograms render through render_measurements; the trait method
        // returns nothing.
        assert!(histogram.measurements().is_empty());
        assert!(histogram.to_string().contains("login_duration_count"));
    }

    #[test]
    fn scalar_metrics_have_expected_names_and_types() {
        let stats = test_stats();
        let metrics = Logins::load_from(stats);
        let names: Vec<_> = metrics.iter().map(|metric| metric.name()).collect();
        assert_eq!(names, ["logins_in_flight", "logins", "logins_abandoned"]);
        assert_eq!(metrics[0].metric_type(), "gauge");
        assert_eq!(metrics[1].metric_type(), "counter");
        assert_eq!(metrics[2].metric_type(), "counter");
    }
}
