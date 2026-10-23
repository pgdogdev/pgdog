//! Sticky settings for clients that override
//! default routing behavior determined by the query parser.

use rand::{Rng, rng};

#[derive(Debug, Clone, Copy)]
pub(crate) struct Sticky {
    /// Which shard to use for omnisharded queries, making them
    /// stick to only one database.
    pub(crate) omni_index: usize,
}

impl Default for Sticky {
    fn default() -> Self {
        Self::new()
    }
}

impl Sticky {
    /// Create new sticky config.
    pub(crate) fn new() -> Self {
        Self {
            omni_index: rng().random_range(1..usize::MAX),
        }
    }

    #[cfg(test)]
    pub(crate) fn new_test() -> Self {
        Self { omni_index: 1 }
    }
}
