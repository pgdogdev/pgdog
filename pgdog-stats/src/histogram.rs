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
///
/// This belongs to the histogram rather than to the configuration, but this
/// crate already depends on `pgdog-config` for the pool types, so owning the
/// constant here would make the dependency circular.
pub const DEFAULT_BOUNDS_MS: [f64; 12] = General::DEFAULT_QUERY_TIME_BUCKETS;

static BOUNDS: OnceLock<LatchedBounds> = OnceLock::new();

/// The process-wide bounds, plus how they got there.
struct LatchedBounds {
    bounds: Bounds,
    /// Set when [`set_bounds`] filled the latch, clear when a [`bounds`] read
    /// fell back to the defaults. The two need different remedies.
    configured: bool,
}

/// What a [`set_bounds`] call did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Latch {
    /// The bounds are now in force for the life of the process.
    Set,
    /// An earlier [`set_bounds`] already latched the enclosed bounds, which
    /// stay in force. Applying different ones requires a restart.
    AlreadySet(Bounds),
    /// A [`bounds`] read latched the defaults before any [`set_bounds`] ran, so
    /// the enclosed defaults stay in force. Unlike [`Latch::AlreadySet`] a
    /// restart doesn't help: the read has to move after configuration load.
    DefaultedByRead(Bounds),
}

/// Latch the process-wide bucket bounds.
///
/// Already-recorded histograms are indexed by position, so re-bucketing at
/// runtime would reinterpret every existing sample. The first value latched
/// wins for the life of the process.
pub fn set_bounds(bounds: Bounds) -> Latch {
    match BOUNDS.set(LatchedBounds {
        bounds,
        configured: true,
    }) {
        Ok(()) => Latch::Set,
        Err(_) => {
            let latched = BOUNDS.get().expect("a failed set means the latch is full");
            if latched.configured {
                Latch::AlreadySet(latched.bounds)
            } else {
                Latch::DefaultedByRead(latched.bounds)
            }
        }
    }
}

/// Process-wide bucket bounds, defaulting to [`DEFAULT_BOUNDS_MS`].
pub fn bounds() -> &'static Bounds {
    &BOUNDS
        .get_or_init(|| LatchedBounds {
            bounds: Bounds::default(),
            configured: false,
        })
        .bounds
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

/// Why a configured ladder can't be used.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum BoundsError {
    /// A value isn't a positive, finite number of milliseconds, or is too
    /// large to be a [`Duration`].
    Invalid(f64),
    /// More bounds than [`MAX_BUCKETS`] allows.
    TooMany(usize),
    /// No bounds at all. A histogram with no explicit bounds files every
    /// sample under `+Inf`, which is worse than having no histogram.
    Empty,
}

impl std::fmt::Display for BoundsError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Invalid(value) => write!(
                f,
                "bound {value} is not a positive, finite number of milliseconds"
            ),
            Self::TooMany(got) => write!(f, "has {got} bounds, at most {MAX_BUCKETS} are allowed"),
            Self::Empty => write!(f, "is empty; remove the setting to use the defaults"),
        }
    }
}

impl std::error::Error for BoundsError {}

impl Bounds {
    /// Build bounds from millisecond values, or say why they can't be used.
    ///
    /// A ladder is taken whole or not at all: every value must be finite,
    /// greater than zero, and small enough for a [`Duration`], and there must
    /// be at least one and no more than [`MAX_BUCKETS`] of them. Dropping the
    /// values that don't qualify would leave an operator running a histogram
    /// whose buckets aren't the ones they wrote, with nothing in the exported
    /// metrics to say which went missing.
    ///
    /// Sorting and deduplication are applied silently, since neither changes
    /// which bounds the operator asked for.
    pub fn try_from_millis(millis: &[f64]) -> Result<Self, BoundsError> {
        if millis.is_empty() {
            return Err(BoundsError::Empty);
        }

        if millis.len() > MAX_BUCKETS {
            return Err(BoundsError::TooMany(millis.len()));
        }

        let mut values = millis
            .iter()
            .copied()
            .map(|ms| {
                if !ms.is_finite() || ms <= 0.0 {
                    return Err(BoundsError::Invalid(ms));
                }
                // Anything past Duration::MAX is unusable for the same reason
                // a negative bound is: it can't name a latency.
                Duration::try_from_secs_f64(ms / 1_000.0).map_err(|_| BoundsError::Invalid(ms))
            })
            .collect::<Result<Vec<_>, _>>()?;

        values.sort_unstable();
        // Deduplicate on the exported value, not on the Duration: past roughly
        // 10^7 seconds an f64 can't resolve nanoseconds, and two bounds that
        // render to the same `le` label fail the whole Prometheus scrape.
        values.dedup_by_key(|value| value.as_secs_f64());

        let mut bounds = [Duration::ZERO; MAX_BUCKETS];
        bounds[..values.len()].copy_from_slice(&values);

        Ok(Self {
            bounds,
            len: values.len(),
        })
    }

    /// The built-in bounds, which are always usable.
    fn defaults() -> Self {
        Self::try_from_millis(&DEFAULT_BOUNDS_MS).expect("DEFAULT_BOUNDS_MS is a valid ladder")
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
///
/// [`Histogram::observe_with`] is what keeps the buckets, the sum and the count
/// agreeing with each other; nothing enforces that across `Deserialize`, which
/// exists only so the stats structs embedding this one can derive it. A decoded
/// value can hold a count that disagrees with its buckets.
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
        // `_bucket` and `_count` are exported from different fields; a sample
        // filed under a bound this call doesn't cover would only show up as a
        // scrape that doesn't add up.
        debug_assert_eq!(counts.iter().sum::<u64>(), self.count);
        counts
    }
}

impl AddAssign for Histogram {
    fn add_assign(&mut self, rhs: Self) {
        for (bucket, rhs) in self.buckets.iter_mut().zip(rhs.buckets.iter()) {
            *bucket = bucket.saturating_add(*rhs);
        }

        self.sum = self.sum.saturating_add(rhs.sum);
        self.count = self.count.saturating_add(rhs.count);
    }
}

impl Add for Histogram {
    type Output = Histogram;

    fn add(mut self, rhs: Self) -> Self::Output {
        self += rhs;
        self
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
        Bounds::try_from_millis(&[1.0, 10.0, 100.0]).expect("valid ladder")
    }

    #[test]
    fn bounds_are_sorted_and_deduplicated() {
        // Neither changes which bounds were asked for, so both are silent.
        let bounds = Bounds::try_from_millis(&[10.0, 1.0, 10.0, 100.0]).expect("valid ladder");

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
    fn the_default_ladder_is_usable() {
        // `Bounds::defaults` panics if this ever stops holding, so state the
        // invariant here rather than degrading at runtime to a bound-less
        // histogram that files every sample under +Inf.
        assert!(Bounds::try_from_millis(&DEFAULT_BOUNDS_MS).is_ok());
        assert_eq!(Bounds::default().len(), DEFAULT_BOUNDS_MS.len());
    }

    #[test]
    fn a_single_bad_bound_rejects_the_whole_ladder() {
        // The point of the error: an operator who typo'd one value gets told
        // so, rather than silently running a ladder missing that bucket.
        for bad in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY, -1.0, 0.0] {
            match Bounds::try_from_millis(&[1.0, bad, 100.0]) {
                // Compared by shape rather than with assert_eq!: NaN is not
                // equal to itself, so a derived PartialEq can never match it.
                Err(BoundsError::Invalid(value)) => assert!(
                    value == bad || (value.is_nan() && bad.is_nan()),
                    "rejected {bad} but reported {value}"
                ),
                other => panic!("{bad} should have been rejected, got {other:?}"),
            }
        }
    }

    #[test]
    fn a_bound_too_large_for_a_duration_is_rejected() {
        // 1e30 ms exceeds Duration::MAX: an error, not a panic and not a drop.
        assert_eq!(
            Bounds::try_from_millis(&[1e30, 5.0]),
            Err(BoundsError::Invalid(1e30))
        );
    }

    #[test]
    fn more_bounds_than_the_maximum_are_rejected() {
        let millis = (1..=(MAX_BUCKETS as u64 + 10))
            .map(|ms| ms as f64)
            .collect::<Vec<_>>();

        assert_eq!(
            Bounds::try_from_millis(&millis),
            Err(BoundsError::TooMany(MAX_BUCKETS + 10))
        );

        // Exactly at the cap is fine.
        let millis = (1..=MAX_BUCKETS as u64)
            .map(|ms| ms as f64)
            .collect::<Vec<_>>();
        assert_eq!(
            Bounds::try_from_millis(&millis).map(|b| b.len()),
            Ok(MAX_BUCKETS)
        );
    }

    #[test]
    fn an_empty_ladder_is_rejected() {
        // Silently substituting the defaults would leave the operator running
        // buckets they explicitly asked not to have.
        assert_eq!(Bounds::try_from_millis(&[]), Err(BoundsError::Empty));
    }

    #[test]
    fn bounds_errors_name_the_offending_value() {
        // These render into the startup error an operator has to act on.
        assert_eq!(
            BoundsError::Invalid(-1.0).to_string(),
            "bound -1 is not a positive, finite number of milliseconds"
        );
        assert_eq!(
            BoundsError::TooMany(25).to_string(),
            format!("has 25 bounds, at most {MAX_BUCKETS} are allowed")
        );
        assert_eq!(
            BoundsError::Empty.to_string(),
            "is empty; remove the setting to use the defaults"
        );
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
        let narrow = Bounds::try_from_millis(&[1.0]).expect("valid ladder");
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
        let bounds = Bounds::try_from_millis(&[0.1, 1.0, 1_000.0]).expect("valid ladder");

        assert_eq!(bounds.seconds(), vec![0.0001, 0.001, 1.0]);
    }
}
