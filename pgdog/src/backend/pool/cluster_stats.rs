//! Mirror stats.

use std::{
    iter::Sum,
    ops::{Add, Div, Sub},
};

use crate::frontend::router::rewrite::stats::RewriteStats;

#[derive(Debug, Clone, Default, Copy)]
pub struct MirrorStats {
    pub total_count: usize,
    pub mirrored_count: usize,
    pub dropped_count: usize,
    pub error_count: usize,
    pub queue_length: usize,
}

impl Sub for MirrorStats {
    type Output = MirrorStats;

    fn sub(self, rhs: Self) -> Self::Output {
        Self {
            total_count: self.total_count.saturating_sub(rhs.total_count),
            mirrored_count: self.mirrored_count.saturating_sub(rhs.mirrored_count),
            dropped_count: self.dropped_count.saturating_sub(rhs.dropped_count),
            error_count: self.error_count.saturating_sub(rhs.error_count),
            queue_length: self.queue_length.saturating_sub(rhs.queue_length),
        }
    }
}

impl Div<usize> for MirrorStats {
    type Output = MirrorStats;

    fn div(self, rhs: usize) -> Self::Output {
        Self {
            total_count: self.total_count.saturating_div(rhs),
            mirrored_count: self.mirrored_count.saturating_div(rhs),
            dropped_count: self.dropped_count.saturating_div(rhs),
            error_count: self.error_count.saturating_div(rhs),
            queue_length: self.queue_length.saturating_div(rhs),
        }
    }
}

impl Add for MirrorStats {
    type Output = MirrorStats;

    fn add(self, rhs: MirrorStats) -> Self::Output {
        MirrorStats {
            total_count: self.total_count + rhs.total_count,
            mirrored_count: self.mirrored_count + rhs.mirrored_count,
            dropped_count: self.dropped_count + rhs.dropped_count,
            error_count: self.error_count + rhs.error_count,
            queue_length: self.queue_length + rhs.queue_length,
        }
    }
}

impl Sum for MirrorStats {
    fn sum<I: Iterator<Item = Self>>(iter: I) -> Self {
        let mut result = MirrorStats::default();
        for next in iter {
            result = result + next;
        }

        result
    }
}

#[derive(Debug, Clone, Default, Copy)]
pub struct ClusterStats {
    pub mirrors: MirrorStats,
    pub rewrite: RewriteStats,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_queue_length_default_is_zero() {
        let stats = ClusterStats::default();
        assert_eq!(
            stats.mirrors.queue_length, 0,
            "queue_length should be 0 by default"
        );
    }

    #[test]
    fn test_queue_length_arithmetic_operations() {
        let counts1 = MirrorStats {
            total_count: 10,
            mirrored_count: 5,
            dropped_count: 3,
            error_count: 2,
            queue_length: 7,
        };

        let counts2 = MirrorStats {
            total_count: 5,
            mirrored_count: 3,
            dropped_count: 1,
            error_count: 1,
            queue_length: 3,
        };

        // Test Add
        let sum = counts1 + counts2;
        assert_eq!(
            sum.queue_length, 10,
            "queue_length should be 10 after addition"
        );

        // Test Sub
        let diff = counts1 - counts2;
        assert_eq!(
            diff.queue_length, 4,
            "queue_length should be 4 after subtraction"
        );

        // Test Div
        let divided = counts1 / 2;
        assert_eq!(
            divided.queue_length, 3,
            "queue_length should be 3 after division"
        );
    }

    #[test]
    fn test_queue_length_saturating_sub() {
        let counts1 = MirrorStats {
            total_count: 10,
            mirrored_count: 5,
            dropped_count: 3,
            error_count: 2,
            queue_length: 3,
        };

        let counts2 = MirrorStats {
            total_count: 5,
            mirrored_count: 3,
            dropped_count: 1,
            error_count: 1,
            queue_length: 5,
        };

        // Test that subtraction doesn't go negative (saturating_sub)
        let diff = counts1 - counts2;
        assert_eq!(
            diff.queue_length, 0,
            "queue_length should saturate at 0, not go negative"
        );
    }
}
