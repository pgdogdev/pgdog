//! SHOW MIRROR_STATS.
use crate::stats::MirrorStats;

use super::prelude::*;

pub struct ShowMirrorStats;

#[async_trait]
impl Command for ShowMirrorStats {
    fn name(&self) -> String {
        "SHOW MIRROR_STATS".into()
    }

    fn parse(_: &str) -> Result<Self, Error> {
        Ok(Self)
    }

    async fn execute(&self) -> Result<Vec<Message>, Error> {
        let fields = vec![
            // Overall metrics
            Field::text("metric"),
            Field::numeric("value"),
        ];

        let mut messages = vec![RowDescription::new(&fields).message()?];
        let stats = MirrorStats::instance();

        // Add overall metrics
        let metrics = vec![
            (
                "requests_total",
                stats
                    .requests_total
                    .load(std::sync::atomic::Ordering::Relaxed),
            ),
            (
                "requests_mirrored",
                stats
                    .requests_mirrored
                    .load(std::sync::atomic::Ordering::Relaxed),
            ),
            (
                "requests_dropped",
                stats
                    .requests_dropped
                    .load(std::sync::atomic::Ordering::Relaxed),
            ),
            (
                "errors_connection",
                stats
                    .errors_connection
                    .load(std::sync::atomic::Ordering::Relaxed),
            ),
            (
                "errors_query",
                stats
                    .errors_query
                    .load(std::sync::atomic::Ordering::Relaxed),
            ),
            (
                "errors_timeout",
                stats
                    .errors_timeout
                    .load(std::sync::atomic::Ordering::Relaxed),
            ),
            (
                "errors_buffer_full",
                stats
                    .errors_buffer_full
                    .load(std::sync::atomic::Ordering::Relaxed),
            ),
            (
                "consecutive_errors",
                stats
                    .consecutive_errors
                    .load(std::sync::atomic::Ordering::Relaxed),
            ),
        ];

        for (name, value) in metrics {
            let mut dr = DataRow::new();
            dr.add(name).add(value as i64);
            messages.push(dr.message()?);
        }

        // Add latency metrics
        let latency_count = stats
            .latency_count
            .load(std::sync::atomic::Ordering::Relaxed);
        if latency_count > 0 {
            let latency_sum = stats
                .latency_sum_ms
                .load(std::sync::atomic::Ordering::Relaxed);
            let latency_avg = latency_sum / latency_count;
            let latency_max = stats
                .latency_max_ms
                .load(std::sync::atomic::Ordering::Relaxed);

            let mut dr = DataRow::new();
            dr.add("latency_avg_ms").add(latency_avg as i64);
            messages.push(dr.message()?);

            let mut dr = DataRow::new();
            dr.add("latency_max_ms").add(latency_max as i64);
            messages.push(dr.message()?);
        }

        // Add error rate
        let error_rate = stats.error_rate();
        let mut dr = DataRow::new();
        dr.add("error_rate").add(format!("{:.4}", error_rate));
        messages.push(dr.message()?);

        Ok(messages)
    }
}

pub struct ShowMirrorStatsByDatabase;

#[async_trait]
impl Command for ShowMirrorStatsByDatabase {
    fn name(&self) -> String {
        "SHOW MIRROR_STATS_BY_DATABASE".into()
    }

    fn parse(_: &str) -> Result<Self, Error> {
        Ok(Self)
    }

    async fn execute(&self) -> Result<Vec<Message>, Error> {
        let fields = vec![
            Field::text("database"),
            Field::numeric("mirrored"),
            Field::numeric("errors"),
        ];

        let mut messages = vec![RowDescription::new(&fields).message()?];
        let stats = MirrorStats::instance();

        // Per-database stats
        {
            let db_stats = stats.database_stats.read().unwrap();
            for (database, db_stat) in db_stats.iter() {
                let mut dr = DataRow::new();
                dr.add(database.as_str())
                    .add(db_stat.mirrored.load(std::sync::atomic::Ordering::Relaxed) as i64)
                    .add(db_stat.errors.load(std::sync::atomic::Ordering::Relaxed) as i64);
                messages.push(dr.message()?);
            }
        }

        Ok(messages)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::stats::MirrorStats;

    #[tokio::test]
    async fn test_show_mirror_stats() {
        // Set up some test data
        let stats = MirrorStats::instance();
        stats.increment_total();
        stats.increment_total();
        stats.increment_mirrored();
        stats.record_success("test_db", 100);
        stats.record_error("test_db", crate::stats::MirrorErrorType::Connection);

        // Execute the command
        let cmd = ShowMirrorStats;
        let messages = cmd.execute().await.unwrap();

        // Should have at least the row description and some data rows
        assert!(!messages.is_empty());
        
        // First message should be row description
        assert_eq!(messages[0].code(), 'T'); // RowDescription
        
        // Should have data rows for our metrics
        assert!(messages.len() > 5); // At least row description + several metric rows
        
        // Verify we have data row messages
        for msg in &messages[1..] {
            assert!(msg.code() == 'D'); // DataRow
        }
    }

    #[tokio::test]
    async fn test_show_mirror_stats_by_database() {
        // Set up some test data
        let stats = MirrorStats::instance();
        stats.record_success("db1", 50);
        stats.record_success("db2", 75);
        stats.record_error("db1", crate::stats::MirrorErrorType::Query);

        // Execute the command
        let cmd = ShowMirrorStatsByDatabase;
        let messages = cmd.execute().await.unwrap();

        // Should have row description and data rows
        assert!(!messages.is_empty());
        assert_eq!(messages[0].code(), 'T'); // RowDescription
        
        // Should have at least 2 database entries
        // Count data rows (excluding row description at [0])
        let data_row_count = messages[1..].iter().filter(|m| m.code() == 'D').count();
        assert!(data_row_count >= 2, "Should have at least 2 database entries");
    }

    #[tokio::test]
    async fn test_parse_commands() {
        // Test parsing works
        let cmd1 = ShowMirrorStats::parse("show mirror_stats").unwrap();
        assert_eq!(cmd1.name(), "SHOW MIRROR_STATS");

        let cmd2 = ShowMirrorStatsByDatabase::parse("show mirror_stats_by_database").unwrap();
        assert_eq!(cmd2.name(), "SHOW MIRROR_STATS_BY_DATABASE");
    }
}
