use std::time::Duration;

use rand::{distributions::Alphanumeric, Rng};

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

/// Generate a random string of length n.
pub fn random_string(n: usize) -> String {
    rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(n)
        .map(char::from)
        .collect()
}
