//! What's a project without a util module.

use rand::{distributions::Alphanumeric, Rng};
use std::time::Duration; // 0.8

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
}
