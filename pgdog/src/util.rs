//! What's a project without a util module.

use chrono::{DateTime, Local, Utc};
use rand::{distributions::Alphanumeric, Rng};
use std::time::Duration; // 0.8

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

/// Number of microseconds since Postgres epoch.
pub fn postgres_now() -> i64 {
    let start = DateTime::parse_from_rfc3339("2000-01-01T00:00:00Z")
        .unwrap()
        .fixed_offset();
    let now = Utc::now().fixed_offset();
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
        let now = postgres_now();
        println!("{}", now);
    }
}
