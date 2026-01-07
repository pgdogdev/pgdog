//! What's a project without a util module.

use chrono::{DateTime, Local, Utc};
use once_cell::sync::Lazy;
use pgdog_plugin::comp;
use rand::{distr::Alphanumeric, Rng};
use std::{env, num::ParseIntError, ops::Deref, time::Duration};

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
        if !ms.is_multiple_of(unit) {
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
    rand::rng()
        .sample_iter(&Alphanumeric)
        .take(n)
        .map(char::from)
        .collect()
}

// Generate a unique 8-character hex instance ID on first access
static INSTANCE_ID: Lazy<String> = Lazy::new(|| {
    if let Ok(node_id) = env::var("NODE_ID") {
        node_id
    } else {
        let mut rng = rand::rng();
        (0..8)
            .map(|_| {
                let n: u8 = rng.random_range(0..16);
                format!("{:x}", n)
            })
            .collect()
    }
});

/// Get the instance ID for this pgdog instance.
/// This is generated once at startup and persists for the lifetime of the process.
pub fn instance_id() -> &'static str {
    &INSTANCE_ID
}

/// Get an externally assigned, unique, node identifier
/// for this instance of PgDog.
pub fn node_id() -> Result<u64, ParseIntError> {
    // split always returns at least one element.
    instance_id().split("-").last().unwrap().parse()
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

    use std::env::{remove_var, set_var};

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

    #[test]
    fn test_instance_id_format() {
        unsafe {
            remove_var("NODE_ID");
        }
        let id = instance_id();
        assert_eq!(id.len(), 8);
        // All characters should be valid hex digits (0-9, a-f)
        assert!(id.chars().all(|c| c.is_ascii_hexdigit()));
        // All alphabetic characters should be lowercase
        assert!(id
            .chars()
            .filter(|c| c.is_alphabetic())
            .all(|c| c.is_lowercase()));
    }

    #[test]
    fn test_instance_id_consistency() {
        let id1 = instance_id();
        let id2 = instance_id();
        assert_eq!(id1, id2); // Should be the same for lifetime of process
    }

    #[test]
    fn test_node_id_error() {
        unsafe {
            remove_var("NODE_ID");
        }
        assert!(node_id().is_err());
    }

    // These should run in separate processes (if using nextest).
    #[test]
    fn test_node_id_set() {
        unsafe {
            set_var("NODE_ID", "pgdog-1");
        }
        assert_eq!(node_id(), Ok(1));
    }
}
