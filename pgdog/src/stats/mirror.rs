//! Mirror statistics.

use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Instant;

use super::{Measurement, MeasurementType, Metric, OpenMetric};

/// Mirror statistics singleton instance.
static MIRROR_STATS: OnceLock<Arc<MirrorStats>> = OnceLock::new();

/// Mirror error types.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MirrorErrorType {
    Connection,
    Query,
    Timeout,
    BufferFull,
}

/// Per-cluster mirror statistics.
#[derive(Debug, Default)]
pub struct ClusterMirrorStats {
    pub mirrored: AtomicU64,
    pub errors: AtomicU64,
    pub avg_latency_ms: AtomicU64,
}

impl Clone for ClusterMirrorStats {
    fn clone(&self) -> Self {
        Self {
            mirrored: AtomicU64::new(self.mirrored.load(Ordering::Relaxed)),
            errors: AtomicU64::new(self.errors.load(Ordering::Relaxed)),
            avg_latency_ms: AtomicU64::new(self.avg_latency_ms.load(Ordering::Relaxed)),
        }
    }
}

/// Mirror statistics.
#[derive(Debug)]
pub struct MirrorStats {
    // Request counters
    pub requests_total: AtomicU64,
    pub requests_mirrored: AtomicU64,
    pub requests_dropped: AtomicU64,

    // Error counters
    pub errors_connection: AtomicU64,
    pub errors_query: AtomicU64,
    pub errors_timeout: AtomicU64,
    pub errors_buffer_full: AtomicU64,

    // Performance metrics
    pub latency_sum_ms: AtomicU64,
    pub latency_count: AtomicU64,
    pub latency_max_ms: AtomicU64,

    // Health tracking
    pub last_success: RwLock<Instant>,
    pub last_error: RwLock<Option<Instant>>,
    pub consecutive_errors: AtomicU64,

    // Per-cluster stats (key is (database, user) tuple representing a cluster)
    pub cluster_stats: Arc<RwLock<HashMap<(String, String), ClusterMirrorStats>>>,
}

impl Clone for MirrorStats {
    fn clone(&self) -> Self {
        Self {
            requests_total: AtomicU64::new(self.requests_total.load(Ordering::Relaxed)),
            requests_mirrored: AtomicU64::new(self.requests_mirrored.load(Ordering::Relaxed)),
            requests_dropped: AtomicU64::new(self.requests_dropped.load(Ordering::Relaxed)),
            errors_connection: AtomicU64::new(self.errors_connection.load(Ordering::Relaxed)),
            errors_query: AtomicU64::new(self.errors_query.load(Ordering::Relaxed)),
            errors_timeout: AtomicU64::new(self.errors_timeout.load(Ordering::Relaxed)),
            errors_buffer_full: AtomicU64::new(self.errors_buffer_full.load(Ordering::Relaxed)),
            latency_sum_ms: AtomicU64::new(self.latency_sum_ms.load(Ordering::Relaxed)),
            latency_count: AtomicU64::new(self.latency_count.load(Ordering::Relaxed)),
            latency_max_ms: AtomicU64::new(self.latency_max_ms.load(Ordering::Relaxed)),
            last_success: RwLock::new(*self.last_success.read()),
            last_error: RwLock::new(*self.last_error.read()),
            consecutive_errors: AtomicU64::new(self.consecutive_errors.load(Ordering::Relaxed)),
            cluster_stats: Arc::new(RwLock::new(self.cluster_stats.read().clone())),
        }
    }
}

impl Default for MirrorStats {
    fn default() -> Self {
        Self {
            requests_total: AtomicU64::new(0),
            requests_mirrored: AtomicU64::new(0),
            requests_dropped: AtomicU64::new(0),
            errors_connection: AtomicU64::new(0),
            errors_query: AtomicU64::new(0),
            errors_timeout: AtomicU64::new(0),
            errors_buffer_full: AtomicU64::new(0),
            latency_sum_ms: AtomicU64::new(0),
            latency_count: AtomicU64::new(0),
            latency_max_ms: AtomicU64::new(0),
            last_success: RwLock::new(Instant::now()),
            last_error: RwLock::new(None),
            consecutive_errors: AtomicU64::new(0),
            cluster_stats: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

impl MirrorStats {
    /// Get the singleton instance of MirrorStats.
    pub fn instance() -> Arc<MirrorStats> {
        MIRROR_STATS
            .get_or_init(|| Arc::new(MirrorStats::default()))
            .clone()
    }

    /// Increment total requests counter.
    pub fn increment_total(&self) {
        self.requests_total.fetch_add(1, Ordering::Relaxed);
    }

    /// Increment mirrored requests counter.
    pub fn increment_mirrored(&self) {
        self.requests_mirrored.fetch_add(1, Ordering::Relaxed);
    }

    /// Increment dropped requests counter.
    pub fn increment_dropped(&self) {
        self.requests_dropped.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a successful mirror operation.
    pub fn record_success(&self, database: &str, user: &str, latency_ms: u64) {
        // Increment mirrored counter
        self.requests_mirrored.fetch_add(1, Ordering::Relaxed);

        // Update latency metrics
        self.latency_sum_ms.fetch_add(latency_ms, Ordering::Relaxed);
        self.latency_count.fetch_add(1, Ordering::Relaxed);

        // Update max latency using compare_exchange_weak
        let mut current_max = self.latency_max_ms.load(Ordering::Relaxed);
        while latency_ms > current_max {
            match self.latency_max_ms.compare_exchange_weak(
                current_max,
                latency_ms,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(x) => current_max = x,
            }
        }

        // Update cluster-specific stats
        {
            let mut stats = self.cluster_stats.write();
            stats
                .entry((database.to_string(), user.to_string()))
                .or_insert_with(ClusterMirrorStats::default)
                .mirrored
                .fetch_add(1, Ordering::Relaxed);
        }

        // Reset consecutive errors
        self.consecutive_errors.store(0, Ordering::Relaxed);

        // Update last success timestamp
        *self.last_success.write() = Instant::now();
    }

    /// Record an error in mirror operation.
    pub fn record_error(&self, database: &str, user: &str, error_type: MirrorErrorType) {
        self.requests_total.fetch_add(1, Ordering::Relaxed);

        // Increment appropriate error counter
        match error_type {
            MirrorErrorType::Connection => {
                self.errors_connection.fetch_add(1, Ordering::Relaxed);
            }
            MirrorErrorType::Query => {
                self.errors_query.fetch_add(1, Ordering::Relaxed);
            }
            MirrorErrorType::Timeout => {
                self.errors_timeout.fetch_add(1, Ordering::Relaxed);
            }
            MirrorErrorType::BufferFull => {
                self.errors_buffer_full.fetch_add(1, Ordering::Relaxed);
            }
        }

        // Update cluster-specific error count
        {
            let mut stats = self.cluster_stats.write();
            stats
                .entry((database.to_string(), user.to_string()))
                .or_insert_with(ClusterMirrorStats::default)
                .errors
                .fetch_add(1, Ordering::Relaxed);
        }

        // Track consecutive errors
        self.consecutive_errors.fetch_add(1, Ordering::Relaxed);

        // Update last error timestamp
        *self.last_error.write() = Some(Instant::now());
    }

    /// Reset all counters to zero.
    pub fn reset_counters(&self) {
        self.requests_total.store(0, Ordering::Relaxed);
        self.requests_mirrored.store(0, Ordering::Relaxed);
        self.requests_dropped.store(0, Ordering::Relaxed);
        self.errors_connection.store(0, Ordering::Relaxed);
        self.errors_query.store(0, Ordering::Relaxed);
        self.errors_timeout.store(0, Ordering::Relaxed);
        self.errors_buffer_full.store(0, Ordering::Relaxed);
        self.latency_sum_ms.store(0, Ordering::Relaxed);
        self.latency_count.store(0, Ordering::Relaxed);
        self.latency_max_ms.store(0, Ordering::Relaxed);
        self.consecutive_errors.store(0, Ordering::Relaxed);
        self.cluster_stats.write().clear();
    }

    /// Get total error count.
    pub fn total_errors(&self) -> u64 {
        self.errors_connection.load(Ordering::Relaxed)
            + self.errors_query.load(Ordering::Relaxed)
            + self.errors_timeout.load(Ordering::Relaxed)
            + self.errors_buffer_full.load(Ordering::Relaxed)
    }

    /// Calculate error rate.
    pub fn error_rate(&self) -> f64 {
        let total = self.requests_total.load(Ordering::Relaxed);
        if total == 0 {
            return 0.0;
        }
        let errors = self.total_errors();
        errors as f64 / total as f64
    }
}

/// Categorize error from error message string.
pub fn categorize_error(error_msg: &str) -> MirrorErrorType {
    let error_lower = error_msg.to_lowercase();

    // Connection errors
    if error_lower.contains("connection refused")
        || error_lower.contains("connection reset")
        || error_lower.contains("no route to host")
        || error_lower.contains("broken pipe")
        || error_lower.contains("connection closed")
    {
        return MirrorErrorType::Connection;
    }

    // Query errors
    if error_lower.contains("syntax error")
        || error_lower.contains("does not exist")
        || error_lower.contains("permission denied")
        || error_lower.contains("invalid")
        || error_lower.contains("violation")
    {
        return MirrorErrorType::Query;
    }

    // Timeout errors
    if error_lower.contains("timeout")
        || error_lower.contains("timed out")
        || error_lower.contains("deadline exceeded")
    {
        return MirrorErrorType::Timeout;
    }

    // Buffer full errors
    if error_lower.contains("buffer full")
        || error_lower.contains("channel full")
        || error_lower.contains("queue")
        || error_lower.contains("capacity")
    {
        return MirrorErrorType::BufferFull;
    }

    // Default to query error
    MirrorErrorType::Query
}

/// Categorize IO error.
pub fn categorize_io_error(error: &std::io::Error) -> MirrorErrorType {
    use std::io::ErrorKind;

    match error.kind() {
        ErrorKind::ConnectionRefused
        | ErrorKind::ConnectionReset
        | ErrorKind::ConnectionAborted
        | ErrorKind::NotConnected
        | ErrorKind::BrokenPipe => MirrorErrorType::Connection,

        ErrorKind::TimedOut => MirrorErrorType::Timeout,

        ErrorKind::PermissionDenied => MirrorErrorType::Query,

        _ => {
            // Fall back to string categorization
            categorize_error(&error.to_string())
        }
    }
}

/// Individual metric for OpenMetric implementation
struct MirrorMetric {
    name: String,
    help: String,
    metric_type: String,
    unit: Option<String>,
    measurements: Vec<Measurement>,
}

impl OpenMetric for MirrorMetric {
    fn name(&self) -> String {
        self.name.clone()
    }

    fn measurements(&self) -> Vec<Measurement> {
        self.measurements.clone()
    }

    fn help(&self) -> Option<String> {
        Some(self.help.clone())
    }

    fn unit(&self) -> Option<String> {
        self.unit.clone()
    }

    fn metric_type(&self) -> String {
        self.metric_type.clone()
    }
}

impl MirrorStats {
    /// Generate all mirror metrics following Prometheus naming conventions
    pub fn metrics(&self) -> Vec<Metric> {
        let mut metrics = vec![];

        // Request counters
        metrics.push(Metric::new(MirrorMetric {
            name: "mirror_requests_total".into(),
            help: "Total number of requests received for mirroring".into(),
            metric_type: "counter".into(),
            unit: None,
            measurements: vec![Measurement {
                labels: vec![],
                measurement: MeasurementType::Integer(
                    self.requests_total.load(Ordering::Relaxed) as i64
                ),
            }],
        }));

        metrics.push(Metric::new(MirrorMetric {
            name: "mirror_requests_mirrored_total".into(),
            help: "Total number of successfully mirrored requests".into(),
            metric_type: "counter".into(),
            unit: None,
            measurements: vec![Measurement {
                labels: vec![],
                measurement: MeasurementType::Integer(
                    self.requests_mirrored.load(Ordering::Relaxed) as i64,
                ),
            }],
        }));

        metrics.push(Metric::new(MirrorMetric {
            name: "mirror_requests_dropped_total".into(),
            help: "Total number of dropped requests due to buffer overflow".into(),
            metric_type: "counter".into(),
            unit: None,
            measurements: vec![Measurement {
                labels: vec![],
                measurement: MeasurementType::Integer(
                    self.requests_dropped.load(Ordering::Relaxed) as i64,
                ),
            }],
        }));

        // Consolidated error metric with labels
        let mut error_measurements = vec![];
        error_measurements.push(Measurement {
            labels: vec![("error_type".into(), "connection".into())],
            measurement: MeasurementType::Integer(
                self.errors_connection.load(Ordering::Relaxed) as i64
            ),
        });
        error_measurements.push(Measurement {
            labels: vec![("error_type".into(), "query".into())],
            measurement: MeasurementType::Integer(self.errors_query.load(Ordering::Relaxed) as i64),
        });
        error_measurements.push(Measurement {
            labels: vec![("error_type".into(), "timeout".into())],
            measurement: MeasurementType::Integer(
                self.errors_timeout.load(Ordering::Relaxed) as i64
            ),
        });
        error_measurements.push(Measurement {
            labels: vec![("error_type".into(), "buffer_full".into())],
            measurement: MeasurementType::Integer(
                self.errors_buffer_full.load(Ordering::Relaxed) as i64
            ),
        });

        metrics.push(Metric::new(MirrorMetric {
            name: "mirror_errors_total".into(),
            help: "Total number of mirror errors by type".into(),
            metric_type: "counter".into(),
            unit: None,
            measurements: error_measurements,
        }));

        // Latency metrics in seconds
        let count = self.latency_count.load(Ordering::Relaxed);
        if count > 0 {
            let sum_ms = self.latency_sum_ms.load(Ordering::Relaxed);
            let sum_seconds = sum_ms as f64 / 1000.0;
            let avg_seconds = sum_seconds / count as f64;
            let max_ms = self.latency_max_ms.load(Ordering::Relaxed);
            let max_seconds = max_ms as f64 / 1000.0;

            // Average latency
            metrics.push(Metric::new(MirrorMetric {
                name: "mirror_latency_seconds_avg".into(),
                help: "Average mirror operation latency".into(),
                metric_type: "gauge".into(),
                unit: Some("seconds".into()),
                measurements: vec![Measurement {
                    labels: vec![],
                    measurement: MeasurementType::Float(avg_seconds),
                }],
            }));

            // Max latency
            metrics.push(Metric::new(MirrorMetric {
                name: "mirror_latency_seconds_max".into(),
                help: "Maximum mirror operation latency".into(),
                metric_type: "gauge".into(),
                unit: Some("seconds".into()),
                measurements: vec![Measurement {
                    labels: vec![],
                    measurement: MeasurementType::Float(max_seconds),
                }],
            }));

            // Sum for histogram calculation
            metrics.push(Metric::new(MirrorMetric {
                name: "mirror_latency_seconds_sum".into(),
                help: "Sum of all mirror operation latencies".into(),
                metric_type: "counter".into(),
                unit: Some("seconds".into()),
                measurements: vec![Measurement {
                    labels: vec![],
                    measurement: MeasurementType::Float(sum_seconds),
                }],
            }));

            // Count for histogram calculation
            metrics.push(Metric::new(MirrorMetric {
                name: "mirror_latency_seconds_count".into(),
                help: "Count of mirror operations with latency measurements".into(),
                metric_type: "counter".into(),
                unit: None,
                measurements: vec![Measurement {
                    labels: vec![],
                    measurement: MeasurementType::Integer(count as i64),
                }],
            }));
        }

        // Per-database metrics
        {
            let cluster_stats = self.cluster_stats.read();
            let mut db_request_measurements = vec![];
            let mut db_error_measurements = vec![];

            for ((database, user), stats) in cluster_stats.iter() {
                db_request_measurements.push(Measurement {
                    labels: vec![
                        ("database".into(), database.clone()),
                        ("user".into(), user.clone()),
                    ],
                    measurement: MeasurementType::Integer(
                        stats.mirrored.load(Ordering::Relaxed) as i64
                    ),
                });
                db_error_measurements.push(Measurement {
                    labels: vec![
                        ("database".into(), database.clone()),
                        ("user".into(), user.clone()),
                    ],
                    measurement: MeasurementType::Integer(
                        stats.errors.load(Ordering::Relaxed) as i64
                    ),
                });
            }

            if !db_request_measurements.is_empty() {
                metrics.push(Metric::new(MirrorMetric {
                    name: "mirror_database_requests_total".into(),
                    help: "Total mirrored requests per database and user".into(),
                    metric_type: "counter".into(),
                    unit: None,
                    measurements: db_request_measurements,
                }));
            }

            if !db_error_measurements.is_empty() {
                metrics.push(Metric::new(MirrorMetric {
                    name: "mirror_database_errors_total".into(),
                    help: "Total mirror errors per database and user".into(),
                    metric_type: "counter".into(),
                    unit: None,
                    measurements: db_error_measurements,
                }));
            }
        }

        metrics
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::time::Duration;

    #[test]
    fn test_default_initialization() {
        let stats = MirrorStats::default();

        // All counters should start at 0
        assert_eq!(stats.requests_total.load(Ordering::Relaxed), 0);
        assert_eq!(stats.requests_mirrored.load(Ordering::Relaxed), 0);
        assert_eq!(stats.requests_dropped.load(Ordering::Relaxed), 0);
        assert_eq!(stats.errors_connection.load(Ordering::Relaxed), 0);
        assert_eq!(stats.errors_query.load(Ordering::Relaxed), 0);
        assert_eq!(stats.errors_timeout.load(Ordering::Relaxed), 0);
        assert_eq!(stats.errors_buffer_full.load(Ordering::Relaxed), 0);
        assert_eq!(stats.consecutive_errors.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn test_increment_request_counters() {
        let stats = MirrorStats::default();

        stats.increment_total();
        stats.increment_total();
        stats.increment_mirrored();
        stats.increment_dropped();

        assert_eq!(stats.requests_total.load(Ordering::Relaxed), 2);
        assert_eq!(stats.requests_mirrored.load(Ordering::Relaxed), 1);
        assert_eq!(stats.requests_dropped.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn test_record_success_updates_counters() {
        let stats = MirrorStats::default();

        stats.record_success("test_db", "test_user", 100);
        stats.record_success("test_db", "test_user", 200);

        assert_eq!(stats.requests_mirrored.load(Ordering::Relaxed), 2);
        assert_eq!(stats.latency_count.load(Ordering::Relaxed), 2);
        assert_eq!(stats.latency_sum_ms.load(Ordering::Relaxed), 300);
        assert_eq!(stats.latency_max_ms.load(Ordering::Relaxed), 200);
        assert_eq!(stats.consecutive_errors.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn test_record_success_updates_last_success_time() {
        let stats = MirrorStats::default();

        let before = Instant::now();
        std::thread::sleep(Duration::from_millis(10));
        stats.record_success("test_db", "test_user", 50);
        let after = Instant::now();

        let last_success = *stats.last_success.read();
        assert!(last_success > before);
        assert!(last_success < after);
    }

    #[test]
    fn test_max_latency_updates_correctly() {
        let stats = MirrorStats::default();

        stats.record_success("db1", "user1", 100);
        assert_eq!(stats.latency_max_ms.load(Ordering::Relaxed), 100);

        stats.record_success("db2", "user2", 50);
        assert_eq!(stats.latency_max_ms.load(Ordering::Relaxed), 100); // Should not decrease

        stats.record_success("db3", "user3", 200);
        assert_eq!(stats.latency_max_ms.load(Ordering::Relaxed), 200); // Should update to new max
    }

    #[test]
    fn test_database_specific_stats() {
        let stats = MirrorStats::default();

        stats.record_success("db1", "user1", 100);
        stats.record_success("db1", "user1", 150);
        stats.record_success("db2", "user2", 200);

        {
            let cluster_stats = stats.cluster_stats.read();
            let db1_stats = cluster_stats
                .get(&("db1".to_string(), "user1".to_string()))
                .unwrap();
            assert_eq!(db1_stats.mirrored.load(Ordering::Relaxed), 2);
            assert_eq!(db1_stats.errors.load(Ordering::Relaxed), 0);

            let db2_stats = cluster_stats
                .get(&("db2".to_string(), "user2".to_string()))
                .unwrap();
            assert_eq!(db2_stats.mirrored.load(Ordering::Relaxed), 1);
            assert_eq!(db2_stats.errors.load(Ordering::Relaxed), 0);
        }
    }

    #[test]
    fn test_singleton_instance() {
        let instance1 = MirrorStats::instance();
        let instance2 = MirrorStats::instance();

        // Both should be the same instance
        instance1.increment_total();
        assert_eq!(instance2.requests_total.load(Ordering::Relaxed), 1);

        instance2.increment_total();
        assert_eq!(instance1.requests_total.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn test_concurrent_increments() {
        let stats = Arc::new(MirrorStats::default());
        let mut handles = vec![];

        for _ in 0..10 {
            let stats_clone = stats.clone();
            handles.push(std::thread::spawn(move || {
                for _ in 0..100 {
                    stats_clone.increment_total();
                    stats_clone.record_success("test", "user", 10);
                }
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }

        assert_eq!(stats.requests_total.load(Ordering::Relaxed), 1000);
        assert_eq!(stats.requests_mirrored.load(Ordering::Relaxed), 1000);
    }

    #[test]
    fn test_reset_counters() {
        let stats = MirrorStats::default();

        stats.increment_total();
        stats.increment_mirrored();
        stats.record_success("test", "user", 100);

        stats.reset_counters();

        assert_eq!(stats.requests_total.load(Ordering::Relaxed), 0);
        assert_eq!(stats.requests_mirrored.load(Ordering::Relaxed), 0);
        assert_eq!(stats.latency_sum_ms.load(Ordering::Relaxed), 0);
        assert_eq!(stats.latency_count.load(Ordering::Relaxed), 0);
        assert_eq!(stats.latency_max_ms.load(Ordering::Relaxed), 0);
    }

    // Error categorization tests
    #[cfg(test)]
    mod error_tests {
        use super::*;

        #[test]
        fn test_error_type_enum() {
            // Ensure all error types are defined
            let _ = MirrorErrorType::Connection;
            let _ = MirrorErrorType::Query;
            let _ = MirrorErrorType::Timeout;
            let _ = MirrorErrorType::BufferFull;
        }

        #[test]
        fn test_record_error_by_type() {
            let stats = MirrorStats::default();

            stats.record_error("db1", "user1", MirrorErrorType::Connection);
            assert_eq!(stats.errors_connection.load(Ordering::Relaxed), 1);
            assert_eq!(stats.errors_query.load(Ordering::Relaxed), 0);

            stats.record_error("db1", "user1", MirrorErrorType::Query);
            assert_eq!(stats.errors_query.load(Ordering::Relaxed), 1);

            stats.record_error("db1", "user1", MirrorErrorType::Timeout);
            assert_eq!(stats.errors_timeout.load(Ordering::Relaxed), 1);

            stats.record_error("db1", "user1", MirrorErrorType::BufferFull);
            assert_eq!(stats.errors_buffer_full.load(Ordering::Relaxed), 1);
        }

        #[test]
        fn test_consecutive_errors_tracking() {
            let stats = MirrorStats::default();

            stats.record_error("db1", "user1", MirrorErrorType::Connection);
            assert_eq!(stats.consecutive_errors.load(Ordering::Relaxed), 1);

            stats.record_error("db1", "user1", MirrorErrorType::Query);
            assert_eq!(stats.consecutive_errors.load(Ordering::Relaxed), 2);

            stats.record_success("db1", "user1", 100);
            assert_eq!(stats.consecutive_errors.load(Ordering::Relaxed), 0); // Reset on success

            stats.record_error("db1", "user1", MirrorErrorType::Timeout);
            assert_eq!(stats.consecutive_errors.load(Ordering::Relaxed), 1);
        }

        #[test]
        fn test_last_error_timestamp() {
            let stats = MirrorStats::default();

            assert!(stats.last_error.read().is_none());

            let before = Instant::now();
            std::thread::sleep(Duration::from_millis(10));
            stats.record_error("db1", "user1", MirrorErrorType::Connection);
            let after = Instant::now();

            let last_error = stats.last_error.read().unwrap();
            assert!(last_error > before);
            assert!(last_error < after);
        }

        #[test]
        fn test_database_error_stats() {
            let stats = MirrorStats::default();

            stats.record_error("db1", "user1", MirrorErrorType::Connection);
            stats.record_error("db1", "user1", MirrorErrorType::Query);
            stats.record_error("db2", "user2", MirrorErrorType::Timeout);

            {
                let cluster_stats = stats.cluster_stats.read();
                let db1_stats = cluster_stats
                    .get(&("db1".to_string(), "user1".to_string()))
                    .unwrap();
                assert_eq!(db1_stats.errors.load(Ordering::Relaxed), 2);

                let db2_stats = cluster_stats
                    .get(&("db2".to_string(), "user2".to_string()))
                    .unwrap();
                assert_eq!(db2_stats.errors.load(Ordering::Relaxed), 1);
            }
        }

        #[test]
        fn test_categorize_error_from_string() {
            assert_eq!(
                categorize_error("connection refused"),
                MirrorErrorType::Connection
            );
            assert_eq!(
                categorize_error("connection reset by peer"),
                MirrorErrorType::Connection
            );
            assert_eq!(
                categorize_error("no route to host"),
                MirrorErrorType::Connection
            );
            assert_eq!(categorize_error("broken pipe"), MirrorErrorType::Connection);

            assert_eq!(
                categorize_error("syntax error at or near"),
                MirrorErrorType::Query
            );
            assert_eq!(
                categorize_error("column does not exist"),
                MirrorErrorType::Query
            );
            assert_eq!(
                categorize_error("relation does not exist"),
                MirrorErrorType::Query
            );
            assert_eq!(
                categorize_error("permission denied"),
                MirrorErrorType::Query
            );

            assert_eq!(
                categorize_error("operation timed out"),
                MirrorErrorType::Timeout
            );
            assert_eq!(
                categorize_error("timeout expired"),
                MirrorErrorType::Timeout
            );
            assert_eq!(
                categorize_error("deadline exceeded"),
                MirrorErrorType::Timeout
            );

            assert_eq!(categorize_error("buffer full"), MirrorErrorType::BufferFull);
            assert_eq!(
                categorize_error("channel full"),
                MirrorErrorType::BufferFull
            );
            assert_eq!(
                categorize_error("queue capacity exceeded"),
                MirrorErrorType::BufferFull
            );
        }

        #[test]
        fn test_categorize_error_from_error_type() {
            use std::io::{Error, ErrorKind};

            let connection_err = Error::new(ErrorKind::ConnectionRefused, "test");
            assert_eq!(
                categorize_io_error(&connection_err),
                MirrorErrorType::Connection
            );

            let timeout_err = Error::new(ErrorKind::TimedOut, "test");
            assert_eq!(categorize_io_error(&timeout_err), MirrorErrorType::Timeout);

            let broken_pipe = Error::new(ErrorKind::BrokenPipe, "test");
            assert_eq!(
                categorize_io_error(&broken_pipe),
                MirrorErrorType::Connection
            );
        }

        #[test]
        fn test_total_errors_calculation() {
            let stats = MirrorStats::default();

            stats.record_error("db1", "user1", MirrorErrorType::Connection);
            stats.record_error("db1", "user1", MirrorErrorType::Query);
            stats.record_error("db1", "user1", MirrorErrorType::Timeout);
            stats.record_error("db1", "user1", MirrorErrorType::BufferFull);

            assert_eq!(stats.total_errors(), 4);
        }

        #[test]
        fn test_error_rate_calculation() {
            let stats = MirrorStats::default();

            // No requests yet
            assert_eq!(stats.error_rate(), 0.0);

            // Add some successful requests
            for _ in 0..95 {
                stats.increment_total();
                stats.record_success("db1", "user1", 50);
            }

            // Add some errors
            for _ in 0..5 {
                // Note: record_error already increments total internally
                stats.record_error("db1", "user1", MirrorErrorType::Query);
            }

            // We have 95 + 5 = 100 total requests, 5 errors
            assert!((stats.error_rate() - 0.05).abs() < 0.001); // ~5% error rate
        }
    }
}
