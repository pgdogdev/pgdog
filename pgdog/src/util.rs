//! What's a project without a util module.

use chrono::{DateTime, Local, Utc};
use pgdog_plugin::comp;
use rand::{distributions::Alphanumeric, Rng};
use std::{ops::Deref, time::Duration};

use crate::net::Parameters; // 0.8

pub fn format_time(time: DateTime<Local>) -> String {
    time.format("%Y-%m-%d %H:%M:%S%.3f %Z").to_string()
}

pub fn human_duration_optional(duration: Option<Duration>) -> String {
    if let Some(duration) = duration {
        human_duration(duration)
    } else {
        "default".into()
    }
}

/// Get a human-readable duration for amounts that
/// a human would use.
pub fn human_duration(duration: Duration) -> String {
    let second = 1000;
    let minute = second * 60;
    let hour = minute * 60;
    let day = hour * 24;
    let week = day * 7;
    // Ok that's enough.

    let ms = duration.as_millis();
    let ms_fmt = |ms: u128, unit: u128, name: &str| -> String {
        if ms % unit > 0 {
            format!("{}ms", ms)
        } else {
            format!("{}{}", ms / unit, name)
        }
    };

    if ms < second {
        format!("{}ms", ms)
    } else if ms < minute {
        ms_fmt(ms, second, "s")
    } else if ms < hour {
        ms_fmt(ms, minute, "m")
    } else if ms < day {
        ms_fmt(ms, hour, "h")
    } else if ms < week {
        ms_fmt(ms, day, "d")
    } else {
        ms_fmt(ms, 1, "ms")
    }
}

// 2000-01-01T00:00:00Z
static POSTGRES_EPOCH: i64 = 946684800000000000;

/// Number of microseconds since Postgres epoch.
pub fn postgres_now() -> i64 {
    let start = DateTime::from_timestamp_nanos(POSTGRES_EPOCH).fixed_offset();
    let now = Utc::now().fixed_offset();
    // Panic if overflow.
    (now - start).num_microseconds().unwrap()
}

/// Generate a random string of length n.
pub fn random_string(n: usize) -> String {
    rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(n)
        .map(char::from)
        .collect()
}

/// Escape PostgreSQL identifiers by doubling any embedded quotes.
pub fn escape_identifier(s: &str) -> String {
    s.replace("\"", "\"\"")
}

/// Get PgDog's version string.
pub fn pgdog_version() -> String {
    format!(
        "v{} [main@{}, {}]",
        env!("CARGO_PKG_VERSION"),
        env!("GIT_HASH"),
        comp::rustc_version().deref()
    )
}

/// Get user and database parameters.
pub fn user_database_from_params(params: &Parameters) -> (&str, &str) {
    let user = params.get_default("user", "postgres");
    let database = params.get_default("database", user);

    (user, database)
}

#[cfg(test)]
mod test {

    use super::*;

    #[test]
    fn test_human_duration() {
        assert_eq!(human_duration(Duration::from_millis(500)), "500ms");
        assert_eq!(human_duration(Duration::from_millis(2000)), "2s");
        assert_eq!(human_duration(Duration::from_millis(1000 * 60 * 2)), "2m");
        assert_eq!(human_duration(Duration::from_millis(1000 * 3600)), "1h");
    }

    #[test]
    fn test_postgres_now() {
        let start = DateTime::parse_from_rfc3339("2000-01-01T00:00:00Z")
            .unwrap()
            .fixed_offset();
        assert_eq!(
            DateTime::from_timestamp_nanos(POSTGRES_EPOCH).fixed_offset(),
            start,
        );
        let _now = postgres_now();
    }

    #[test]
    fn test_escape_identifier() {
        assert_eq!(escape_identifier("simple"), "simple");
        assert_eq!(escape_identifier("has\"quote"), "has\"\"quote");
        assert_eq!(escape_identifier("\"starts_with"), "\"\"starts_with");
        assert_eq!(escape_identifier("ends_with\""), "ends_with\"\"");
        assert_eq!(
            escape_identifier("\"multiple\"quotes\""),
            "\"\"multiple\"\"quotes\"\""
        );
    }
}
