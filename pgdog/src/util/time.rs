//! Deadlines and jitter.

use chrono::{DateTime, Local, Utc};
use futures::{FutureExt, future::Either};
use pgdog_config::MAX_DURATION;
use rand::Rng;
use std::future::{Future, pending};
use std::panic::Location;
use std::time::{Duration, Instant};
use tokio::time::Interval;
use tracing::warn;

/// Random point in `[base - jitter, base + jitter]`, so things created
/// together don't all expire on the same tick.
///
/// Jitter has millisecond granularity: anything smaller is treated as zero.
pub(crate) fn jitter_duration(base: Duration, jitter: Duration) -> Duration {
    if jitter.is_zero() {
        return base;
    }

    // make sure to clamp the jitter
    let jitter = jitter.as_millis().min(i64::MAX as u128) as i64;
    if jitter == 0 {
        return base;
    }

    let offset = rand::rng().random_range(-jitter..=jitter);
    let magnitude = Duration::from_millis(offset.unsigned_abs());

    if offset >= 0 {
        base.saturating_add(magnitude)
    } else {
        base.saturating_sub(magnitude)
    }
}

/// Calculate the deadline from now
///
/// # Panics
///
/// Panics on a `ttl` big enough to overflow the clock. Config caps it
/// long before that.
pub(crate) fn deadline(ttl: Duration, jitter: Duration) -> Instant {
    Instant::now() + jitter_duration(ttl, jitter)
}

pub(crate) fn format_time(time: DateTime<Local>) -> String {
    time.format("%Y-%m-%d %H:%M:%S%.3f %Z").to_string()
}

/// Convert Duration to milliseconds with 3 decimal places precision.
pub(crate) fn millis(duration: Duration) -> f64 {
    (duration.as_secs_f64() * 1_000_000.0).round() / 1000.0
}

/// Get a human-readable duration for amounts that
/// a human would use.
pub(crate) fn human_duration(duration: Duration) -> String {
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

/// Get a human-readable duration split into days and hh:mm:ss:ms.
/// Example: "2d 03:15:42:100" or "00:05:30:250"
pub(crate) fn human_duration_display(duration: Duration) -> String {
    let total_secs = duration.as_secs();
    let days = total_secs / 86400;
    let hours = (total_secs % 86400) / 3600;
    let minutes = (total_secs % 3600) / 60;
    let seconds = total_secs % 60;
    let millis = duration.subsec_millis();

    if days > 0 {
        format!(
            "{}d {:02}:{:02}:{:02}:{:03}",
            days, hours, minutes, seconds, millis
        )
    } else {
        format!("{:02}:{:02}:{:02}:{:03}", hours, minutes, seconds, millis)
    }
}

// 2000-01-01T00:00:00Z
static POSTGRES_EPOCH: i64 = 946684800000000000;

/// Number of microseconds since Postgres epoch.
pub(crate) fn postgres_now() -> i64 {
    let start = DateTime::from_timestamp_nanos(POSTGRES_EPOCH).fixed_offset();
    let now = Utc::now().fixed_offset();
    // Panic if overflow.
    (now - start).num_microseconds().unwrap()
}

fn armable(duration: Option<Duration>) -> Option<Duration> {
    duration.filter(|duration| *duration < MAX_DURATION)
}

/// [`tokio::time::timeout`] that waits forever instead of arming a timer
/// outside [`MAX_DURATION`].
///
/// Not an `async fn` on purpose: an async wrapper's state machine holds `F`
/// once as its argument and again inside the [`tokio::time::Timeout`] it
/// builds, so every call has to copy the whole future between the two slots.
/// Some of the futures passed here are kilobytes ([`crate::backend::pool::Pool::get`]),
/// and that copy is large enough to show up as `memcpy` in profiles.
pub(crate) fn safe_timeout<F>(
    duration: Duration,
    future: F,
) -> impl Future<Output = Result<F::Output, tokio::time::error::Elapsed>>
where
    F: Future,
{
    match armable(Some(duration)) {
        Some(duration) => Either::Left(tokio::time::timeout(duration, future)),
        None => Either::Right(future.map(Ok)),
    }
}

/// [`tokio::time::sleep`] that waits forever instead of arming a timer
/// outside [`MAX_DURATION`].
pub(crate) async fn safe_sleep(duration: Duration) {
    match armable(Some(duration)) {
        Some(duration) => tokio::time::sleep(duration).await,
        None => pending().await,
    }
}

/// [`tokio::time::interval`] that is disabled instead of arming a timer
/// outside [`MAX_DURATION`], or panicking on a zero period.
#[track_caller]
pub(crate) fn safe_interval(period: Duration) -> SafeInterval {
    match armable(Some(period)).filter(|period| !period.is_zero()) {
        Some(period) => SafeInterval(Some(tokio::time::interval(period))),
        None => {
            warn!(
                "{} is not a usable tick interval, timer disabled [{}]",
                human_duration(period),
                Location::caller()
            );
            SafeInterval(None)
        }
    }
}

/// An [`Interval`] that may not be running at all.
pub(crate) struct SafeInterval(Option<Interval>);

impl SafeInterval {
    /// Wait for the next tick, or forever when the interval is disabled.
    ///
    /// Cancel safe: both branches are.
    pub(crate) async fn tick(&mut self) {
        match self.0.as_mut() {
            Some(interval) => {
                interval.tick().await;
            }
            None => pending().await,
        }
    }

    pub(crate) fn set_missed_tick_behavior(&mut self, behavior: tokio::time::MissedTickBehavior) {
        if let Some(interval) = self.0.as_mut() {
            interval.set_missed_tick_behavior(behavior);
        }
    }
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
    fn test_armable_boundary() {
        let just_under = MAX_DURATION - Duration::from_millis(1);

        assert_eq!(armable(None), None);
        assert_eq!(armable(Some(Duration::ZERO)), Some(Duration::ZERO));
        assert_eq!(armable(Some(just_under)), Some(just_under));
        assert_eq!(armable(Some(MAX_DURATION)), None);
        assert_eq!(armable(Some(Duration::MAX)), None);
    }

    #[tokio::test]
    async fn test_safe_timeout_never_arms_forever() {
        assert_eq!(safe_timeout(Duration::MAX, async { 42 }).await.unwrap(), 42);
        assert_eq!(safe_timeout(MAX_DURATION, async { 42 }).await.unwrap(), 42);
    }

    #[tokio::test(start_paused = true)]
    async fn test_safe_timeout_times_out() {
        let result = safe_timeout(Duration::from_millis(1), std::future::pending::<()>()).await;
        assert!(result.is_err());
    }

    #[tokio::test(start_paused = true)]
    async fn test_safe_sleep() {
        safe_sleep(Duration::from_millis(10)).await;

        for forever in [MAX_DURATION, Duration::MAX] {
            assert!(
                tokio::time::timeout(Duration::from_secs(60), safe_sleep(forever))
                    .await
                    .is_err(),
                "{:?} must never wake up",
                forever
            );
        }
    }

    #[tokio::test(start_paused = true)]
    async fn test_safe_interval_ticks() {
        let mut interval = safe_interval(Duration::from_millis(10));

        // The first tick of a live interval resolves immediately.
        interval.tick().await;

        assert!(
            tokio::time::timeout(Duration::from_millis(50), interval.tick())
                .await
                .is_ok()
        );
    }

    #[tokio::test(start_paused = true)]
    async fn test_safe_interval_disabled_never_ticks() {
        // Zero panics `tokio::time::interval`; the rest would run the wheel hot.
        for period in [Duration::ZERO, MAX_DURATION, Duration::MAX] {
            let mut interval = safe_interval(period);

            assert!(
                tokio::time::timeout(Duration::from_secs(60), interval.tick())
                    .await
                    .is_err(),
                "{:?} must never tick",
                period
            );
        }
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
    fn test_human_duration_display() {
        // Zero duration
        assert_eq!(
            human_duration_display(Duration::from_millis(0)),
            "00:00:00:000"
        );

        // Just milliseconds
        assert_eq!(
            human_duration_display(Duration::from_millis(500)),
            "00:00:00:500"
        );

        // Seconds and milliseconds
        assert_eq!(
            human_duration_display(Duration::from_millis(5500)),
            "00:00:05:500"
        );

        // Minutes, seconds, milliseconds
        assert_eq!(
            human_duration_display(Duration::from_millis(65500)),
            "00:01:05:500"
        );

        // Hours, minutes, seconds, milliseconds
        assert_eq!(
            human_duration_display(Duration::from_millis(3665500)),
            "01:01:05:500"
        );

        // Days
        assert_eq!(
            human_duration_display(
                Duration::from_secs(86400 + 3600 + 60 + 1) + Duration::from_millis(123)
            ),
            "1d 01:01:01:123"
        );

        // Multiple days
        assert_eq!(
            human_duration_display(
                Duration::from_secs(2 * 86400 + 12 * 3600 + 30 * 60 + 45)
                    + Duration::from_millis(999)
            ),
            "2d 12:30:45:999"
        );
    }

    #[test]
    fn test_jitter_duration_zero_returns_base() {
        let base = Duration::from_secs(60);
        assert_eq!(jitter_duration(base, Duration::ZERO), base);
    }

    #[test]
    fn test_jitter_duration_below_a_millisecond_returns_base() {
        let base = Duration::from_secs(60);
        assert_eq!(jitter_duration(base, Duration::from_micros(500)), base);
    }

    #[test]
    fn test_jitter_duration_survives_an_absurd_jitter() {
        let base = Duration::from_secs(60);
        let jitter = Duration::from_millis(u64::MAX);
        assert!(jitter_duration(base, jitter) <= base.saturating_add(jitter));
    }

    #[test]
    fn test_jitter_duration_stays_within_bounds() {
        let base = Duration::from_secs(60);
        let jitter = Duration::from_secs(10);
        let mut below = false;
        let mut above = false;

        for _ in 0..1_000 {
            let sampled = jitter_duration(base, jitter);
            assert!(
                sampled >= base - jitter && sampled <= base + jitter,
                "sampled {sampled:?} outside [{:?}, {:?}]",
                base - jitter,
                base + jitter
            );
            below |= sampled < base;
            above |= sampled > base;
        }

        assert!(below, "never sampled below base");
        assert!(above, "never sampled above base");
    }

    #[test]
    fn test_jitter_duration_saturates_at_zero() {
        let base = Duration::from_millis(10);
        let jitter = Duration::from_millis(100);
        for _ in 0..1_000 {
            let sampled = jitter_duration(base, jitter);
            assert!(sampled <= base + jitter, "sampled {sampled:?} too large");
        }
    }

    #[test]
    fn test_largest_configurable_ttl_fits_in_an_instant() {
        // Largest ttl the config will actually let through.
        let ttl = Duration::from_millis(i64::MAX as u64 - 1);
        assert!(Instant::now().checked_add(ttl).is_some());
    }
}
