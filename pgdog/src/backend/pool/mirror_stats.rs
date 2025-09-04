//! Mirror stats.

use std::{
    iter::Sum,
    ops::{Add, Div, Sub},
};

#[derive(Debug, Clone, Default, Copy)]
pub(crate) struct Counts {
    pub(crate) total_count: usize,
    pub(crate) mirrored_count: usize,
    pub(crate) dropped_count: usize,
    pub(crate) error_count: usize,
}

impl Sub for Counts {
    type Output = Counts;

    fn sub(self, rhs: Self) -> Self::Output {
        Self {
            total_count: self.total_count.saturating_sub(rhs.total_count),
            mirrored_count: self.mirrored_count.saturating_sub(rhs.mirrored_count),
            dropped_count: self.dropped_count.saturating_sub(rhs.dropped_count),
            error_count: self.error_count.saturating_sub(rhs.error_count),
        }
    }
}

impl Div<usize> for Counts {
    type Output = Counts;

    fn div(self, rhs: usize) -> Self::Output {
        Self {
            total_count: self.total_count.saturating_div(rhs),
            mirrored_count: self.mirrored_count.saturating_div(rhs),
            dropped_count: self.dropped_count.saturating_div(rhs),
            error_count: self.error_count.saturating_div(rhs),
        }
    }
}

impl Add for Counts {
    type Output = Counts;

    fn add(self, rhs: Counts) -> Self::Output {
        Counts {
            total_count: self.total_count + rhs.total_count,
            mirrored_count: self.mirrored_count + rhs.mirrored_count,
            dropped_count: self.dropped_count + rhs.dropped_count,
            error_count: self.error_count + rhs.error_count,
        }
    }
}

impl Sum for Counts {
    fn sum<I: Iterator<Item = Self>>(iter: I) -> Self {
        let mut result = Counts::default();
        for next in iter {
            result = result + next;
        }

        result
    }
}

#[derive(Debug, Clone, Default, Copy)]
pub(crate) struct MirrorStats {
    pub(crate) counts: Counts,
}
