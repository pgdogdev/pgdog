//! Deadlines and jitter.

use rand::Rng;
use std::time::{Duration, Instant};

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

#[cfg(test)]
mod test {
    use super::*;

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
