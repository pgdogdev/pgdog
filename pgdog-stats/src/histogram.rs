//! Fixed-bucket histogram for latency distributions.
//!
//! Bucket bounds are process-wide: they are latched once at startup from the
//! configuration and never change while PgDog runs. That keeps [`Histogram`]
//! `Copy` and free of any per-instance bound storage, so histograms can be
//! summed element-wise without checking that their bounds agree.

use std::{
    ops::{Add, AddAssign, Sub},
    sync::OnceLock,
    time::Duration,
};

use pgdog_config::General;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Maximum number of explicit bucket bounds.
///
/// Each bound costs one series per pool on the OpenMetrics endpoint, so the
/// limit keeps cardinality bounded no matter what the configuration asks for.
pub const MAX_BUCKETS: usize = 20;

/// Default bucket bounds, in milliseconds.
///
/// Exponential from 100µs to 30s, which covers everything from an index lookup
/// on a warm buffer cache to a query about to hit `statement_timeout`.
pub const DEFAULT_BOUNDS_MS: [f64; 12] = General::DEFAULT_QUERY_TIME_BUCKETS;

static BOUNDS: OnceLock<Bounds> = OnceLock::new();

/// Latch the process-wide bucket bounds.
///
/// Returns `false` if the bounds were already read or set, in which case the
/// existing bounds are kept. Reconfiguring buckets requires a restart.
pub fn set_bounds(bounds: Bounds) -> bool {
    BOUNDS.set(bounds).is_ok()
}

/// Process-wide bucket bounds, defaulting to [`DEFAULT_BOUNDS_MS`].
pub fn bounds() -> &'static Bounds {
    BOUNDS.get_or_init(Bounds::default)
}

/// Ascending upper bounds of histogram buckets.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Bounds {
    bounds: [Duration; MAX_BUCKETS],
    len: usize,
}

impl Default for Bounds {
    fn default() -> Self {
        Self::defaults()
    }
}

/// How [`Bounds::from_millis_checked`] treated its input.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Normalized {
    /// Bounds were built from the input, discarding this many values as
    /// invalid, duplicate, or past [`MAX_BUCKETS`]. Zero means a clean input.
    Dropped(usize),
    /// Nothing in the input was usable, so [`DEFAULT_BOUNDS_MS`] was used.
    FellBackToDefaults,
}

impl Bounds {
    /// Build bounds from millisecond values.
    ///
    /// Values that aren't finite and positive, or that overflow a
    /// [`Duration`], are dropped; the rest are sorted and deduplicated, and
    /// anything past [`MAX_BUCKETS`] is discarded. An input that leaves
    /// nothing usable falls back to [`DEFAULT_BOUNDS_MS`].
    pub fn from_millis(millis: &[f64]) -> Self {
        Self::from_millis_checked(millis).0
    }

    /// Build bounds, reporting how the input was normalized.
    pub fn from_millis_checked(millis: &[f64]) -> (Self, Normalized) {
        match Self::parse(millis) {
            Some(bounds) => (
                bounds,
                Normalized::Dropped(millis.len().saturating_sub(bounds.len())),
            ),
            // The defaults are valid, so this recovers a usable histogram
            // rather than silently disabling bucketing.
            None => (Self::defaults(), Normalized::FellBackToDefaults),
        }
    }

    /// The built-in bounds, which are always usable.
    fn defaults() -> Self {
        Self::parse(&DEFAULT_BOUNDS_MS).unwrap_or(Self {
            bounds: [Duration::ZERO; MAX_BUCKETS],
            len: 0,
        })
    }

    /// Normalize millisecond bounds, or `None` if none are usable.
    fn parse(millis: &[f64]) -> Option<Self> {
        let mut values = millis
            .iter()
            .copied()
            .filter(|ms| ms.is_finite() && *ms > 0.0)
            // Overflowing values exceed Duration::MAX; drop them like other
            // unusable inputs instead of panicking.
            .filter_map(|ms| Duration::try_from_secs_f64(ms / 1_000.0).ok())
            .collect::<Vec<_>>();

        values.sort_unstable();
        values.dedup();
        values.truncate(MAX_BUCKETS);

        if values.is_empty() {
            return None;
        }

        let mut bounds = [Duration::ZERO; MAX_BUCKETS];
        bounds[..values.len()].copy_from_slice(&values);

        Some(Self {
            bounds,
            len: values.len(),
        })
    }

    /// Upper bounds, ascending.
    pub fn as_slice(&self) -> &[Duration] {
        &self.bounds[..self.len]
    }

    /// Number of explicit bounds, excluding the implicit `+Inf` bucket.
    pub fn len(&self) -> usize {
        self.len
    }

    /// No explicit bounds are configured.
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Upper bounds in seconds, the base unit used when exporting metrics.
    pub fn seconds(&self) -> Vec<f64> {
        self.as_slice().iter().map(Duration::as_secs_f64).collect()
    }

    /// Bucket a sample belongs to. `len()` is the implicit `+Inf` bucket.
    ///
    /// Buckets are inclusive of their upper bound, matching the `le` semantics
    /// of OpenMetrics.
    fn index_of(&self, sample: Duration) -> usize {
        self.as_slice().partition_point(|bound| *bound < sample)
    }
}

/// Cumulative distribution of duration samples.
///
/// Counts are per-bucket rather than cumulative, so merging two histograms is
/// an element-wise add. Samples above the last bound land in the trailing
/// `+Inf` bucket.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, JsonSchema)]
pub struct Histogram {
    /// Observations per bucket. Index [`MAX_BUCKETS`] is the `+Inf` bucket, so
    /// the slot a sample lands in doesn't depend on how many bounds are
    /// configured.
    buckets: [u64; MAX_BUCKETS + 1],
    /// Sum of all observed samples.
    sum: Duration,
    /// Number of observed samples.
    count: u64,
}

impl Histogram {
    /// Record a sample using the process-wide [`bounds`].
    #[inline]
    pub fn observe(&mut self, sample: Duration) {
        self.observe_with(sample, bounds());
    }

    /// Record a sample against explicit bounds.
    #[inline]
    pub fn observe_with(&mut self, sample: Duration, bounds: &Bounds) {
        let index = bounds.index_of(sample);
        // Samples past the last bound go to the dedicated overflow slot.
        let index = if index < bounds.len() {
            index
        } else {
            MAX_BUCKETS
        };

        self.buckets[index] = self.buckets[index].saturating_add(1);
        self.sum = self.sum.saturating_add(sample);
        self.count = self.count.saturating_add(1);
    }

    /// Number of observed samples.
    pub fn count(&self) -> u64 {
        self.count
    }

    /// Sum of all observed samples.
    pub fn sum(&self) -> Duration {
        self.sum
    }

    /// No samples have been observed.
    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// Per-bucket counts for `bounds`, with the `+Inf` bucket last.
    ///
    /// The returned length is always `bounds.len() + 1`.
    pub fn buckets(&self, bounds: &Bounds) -> Vec<u64> {
        let mut counts = Vec::with_capacity(bounds.len() + 1);
        counts.extend_from_slice(&self.buckets[..bounds.len()]);
        counts.push(self.buckets[MAX_BUCKETS]);
        counts
    }
}

impl Add for Histogram {
    type Output = Histogram;

    fn add(self, rhs: Self) -> Self::Output {
        let mut buckets = self.buckets;
        for (bucket, rhs) in buckets.iter_mut().zip(rhs.buckets.iter()) {
            *bucket = bucket.saturating_add(*rhs);
        }

        Self {
            buckets,
            sum: self.sum.saturating_add(rhs.sum),
            count: self.count.saturating_add(rhs.count),
        }
    }
}

impl AddAssign for Histogram {
    fn add_assign(&mut self, rhs: Self) {
        *self = *self + rhs;
    }
}

impl Sub for Histogram {
    type Output = Histogram;

    fn sub(self, rhs: Self) -> Self::Output {
        let mut buckets = self.buckets;
        for (bucket, rhs) in buckets.iter_mut().zip(rhs.buckets.iter()) {
            *bucket = bucket.saturating_sub(*rhs);
        }

        Self {
            buckets,
            sum: self.sum.saturating_sub(rhs.sum),
            count: self.count.saturating_sub(rhs.count),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    fn test_bounds() -> Bounds {
        Bounds::from_millis(&[1.0, 10.0, 100.0])
    }

    #[test]
    fn bounds_from_millis_sorts_and_dedups() {
        let bounds = Bounds::from_millis(&[10.0, 1.0, 10.0, 100.0]);

        assert_eq!(bounds.len(), 3);
        assert_eq!(
            bounds.as_slice(),
            [
                Duration::from_millis(1),
                Duration::from_millis(10),
                Duration::from_millis(100),
            ]
        );
    }

    #[test]
    fn bounds_from_millis_drops_invalid_values() {
        let bounds = Bounds::from_millis(&[f64::NAN, -1.0, 0.0, f64::INFINITY, 5.0]);

        assert_eq!(bounds.as_slice(), [Duration::from_millis(5)]);
    }

    #[test]
    fn bounds_from_millis_falls_back_to_default() {
        let bounds = Bounds::from_millis(&[-1.0, f64::NAN]);

        assert_eq!(bounds.len(), DEFAULT_BOUNDS_MS.len());
        assert_eq!(bounds, Bounds::default());
    }

    #[test]
    fn bounds_from_millis_drops_overflowing_values() {
        // 1e30 ms overflows Duration: must degrade, not panic.
        let bounds = Bounds::from_millis(&[1e30, 5.0]);
        assert_eq!(bounds.as_slice(), [Duration::from_millis(5)]);

        // Nothing usable at all falls back to the defaults.
        let bounds = Bounds::from_millis(&[1e30]);
        assert_eq!(bounds, Bounds::default());
    }

    #[test]
    fn bounds_from_millis_truncates_to_max() {
        let millis = (1..=(MAX_BUCKETS as u64 + 10))
            .map(|ms| ms as f64)
            .collect::<Vec<_>>();
        let bounds = Bounds::from_millis(&millis);

        assert_eq!(bounds.len(), MAX_BUCKETS);
        assert_eq!(bounds.as_slice().last(), Some(&Duration::from_millis(20)));
    }

    #[test]
    fn from_millis_checked_reports_a_clean_input() {
        let (bounds, normalized) = Bounds::from_millis_checked(&[1.0, 10.0, 100.0]);

        assert_eq!(bounds.len(), 3);
        assert_eq!(normalized, Normalized::Dropped(0));
    }

    #[test]
    fn from_millis_checked_counts_invalid_values() {
        let (_, normalized) = Bounds::from_millis_checked(&[f64::NAN, -1.0, 0.0, 5.0]);

        assert_eq!(normalized, Normalized::Dropped(3));
    }

    #[test]
    fn from_millis_checked_counts_duplicates() {
        let (_, normalized) = Bounds::from_millis_checked(&[10.0, 1.0, 10.0, 100.0]);

        assert_eq!(normalized, Normalized::Dropped(1));
    }

    #[test]
    fn from_millis_checked_counts_bounds_past_the_maximum() {
        let millis = (1..=(MAX_BUCKETS as u64 + 10))
            .map(|ms| ms as f64)
            .collect::<Vec<_>>();

        let (bounds, normalized) = Bounds::from_millis_checked(&millis);

        assert_eq!(bounds.len(), MAX_BUCKETS);
        assert_eq!(normalized, Normalized::Dropped(10));
    }

    #[test]
    fn from_millis_checked_reports_the_fallback_separately() {
        // An operator whose whole ladder was rejected needs to hear that the
        // defaults are in use, not that "n bounds were dropped".
        let (bounds, normalized) = Bounds::from_millis_checked(&[-1.0, f64::NAN]);

        assert_eq!(bounds, Bounds::default());
        assert_eq!(normalized, Normalized::FellBackToDefaults);

        let (bounds, normalized) = Bounds::from_millis_checked(&[]);

        assert_eq!(bounds, Bounds::default());
        assert_eq!(normalized, Normalized::FellBackToDefaults);
    }

    #[test]
    fn bounds_are_inclusive_of_upper_bound() {
        let bounds = test_bounds();

        // Exactly on a bound belongs to that bound's bucket, not the next.
        assert_eq!(bounds.index_of(Duration::from_millis(1)), 0);
        assert_eq!(bounds.index_of(Duration::from_micros(999)), 0);
        assert_eq!(bounds.index_of(Duration::from_micros(1001)), 1);
        assert_eq!(bounds.index_of(Duration::from_millis(100)), 2);
        // Past the last bound: the +Inf bucket.
        assert_eq!(bounds.index_of(Duration::from_millis(101)), 3);
    }

    #[test]
    fn observe_counts_sum_and_buckets() {
        let bounds = test_bounds();
        let mut histogram = Histogram::default();

        histogram.observe_with(Duration::from_micros(500), &bounds);
        histogram.observe_with(Duration::from_millis(5), &bounds);
        histogram.observe_with(Duration::from_millis(50), &bounds);
        histogram.observe_with(Duration::from_secs(1), &bounds);

        assert_eq!(histogram.count(), 4);
        assert_eq!(
            histogram.sum(),
            Duration::from_micros(500) + Duration::from_millis(55) + Duration::from_secs(1)
        );
        // One per bucket, including the +Inf overflow.
        assert_eq!(histogram.buckets(&bounds), vec![1, 1, 1, 1]);
    }

    #[test]
    fn buckets_length_matches_bounds() {
        let bounds = test_bounds();
        let histogram = Histogram::default();

        assert_eq!(histogram.buckets(&bounds).len(), bounds.len() + 1);
        assert!(histogram.is_empty());
    }

    #[test]
    fn overflow_bucket_is_independent_of_bound_count() {
        // The +Inf slot is fixed, so a histogram observed under one bound count
        // still reports its overflow correctly.
        let narrow = Bounds::from_millis(&[1.0]);
        let mut histogram = Histogram::default();

        histogram.observe_with(Duration::from_secs(10), &narrow);

        assert_eq!(histogram.buckets(&narrow), vec![0, 1]);
    }

    #[test]
    fn add_merges_element_wise() {
        let bounds = test_bounds();
        let mut a = Histogram::default();
        let mut b = Histogram::default();

        a.observe_with(Duration::from_micros(500), &bounds);
        a.observe_with(Duration::from_millis(5), &bounds);
        b.observe_with(Duration::from_millis(5), &bounds);
        b.observe_with(Duration::from_secs(1), &bounds);

        let merged = a + b;

        assert_eq!(merged.count(), 4);
        assert_eq!(merged.buckets(&bounds), vec![1, 2, 0, 1]);
        assert_eq!(merged.sum(), a.sum() + b.sum());
    }

    #[test]
    fn add_assign_matches_add() {
        let bounds = test_bounds();
        let mut a = Histogram::default();
        a.observe_with(Duration::from_millis(5), &bounds);

        let mut merged = a;
        merged += a;

        assert_eq!(merged.count(), 2);
        assert_eq!(merged.buckets(&bounds), (a + a).buckets(&bounds));
    }

    #[test]
    fn sub_saturates() {
        let bounds = test_bounds();
        let mut a = Histogram::default();
        a.observe_with(Duration::from_millis(5), &bounds);

        let mut b = Histogram::default();
        b.observe_with(Duration::from_millis(5), &bounds);
        b.observe_with(Duration::from_millis(5), &bounds);

        let result = a - b;

        assert_eq!(result.count(), 0);
        assert_eq!(result.sum(), Duration::ZERO);
        assert_eq!(result.buckets(&bounds), vec![0, 0, 0, 0]);
    }

    #[test]
    fn seconds_converts_bounds() {
        let bounds = Bounds::from_millis(&[0.1, 1.0, 1_000.0]);

        assert_eq!(bounds.seconds(), vec![0.0001, 0.001, 1.0]);
    }
}
