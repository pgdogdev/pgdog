/// Controls which destination shards a subscriber writes to for omni (unsharded) tables.
///
/// Partitions destinations via `dest_shard % n_sources == source_shard` so that each
/// subscriber owns a disjoint subset, preventing cross-subscriber row-lock deadlocks.
#[derive(Debug, Clone, Copy)]
pub struct OmniOwnership {
    source_shard: usize,
    n_sources: usize,
}

impl OmniOwnership {
    pub fn new(source_shard: usize, n_sources: usize) -> Self {
        Self {
            source_shard,
            n_sources,
        }
    }

    /// Returns true if this subscriber should write omni-table DML to `dest_shard`.
    pub fn owns(&self, dest_shard: usize) -> bool {
        if self.n_sources <= 1 {
            return true;
        }
        dest_shard % self.n_sources == self.source_shard
    }
}

impl Default for OmniOwnership {
    fn default() -> Self {
        Self::new(0, 1)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn omni_single_source_owns_all_dests() {
        let p = OmniOwnership::new(0, 1);
        assert!(p.owns(0));
        assert!(p.owns(1));
        assert!(p.owns(2));
        assert!(p.owns(7));
    }

    #[test]
    fn omni_zero_sources_owns_all_dests() {
        let p = OmniOwnership::new(0, 0);
        assert!(p.owns(0));
        assert!(p.owns(1));
        assert!(p.owns(3));
    }

    #[test]
    fn omni_equal_sources_and_dests() {
        // n_sources == n_dests == 3: strict 1:1, each source owns only its own index.
        assert!(OmniOwnership::new(0, 3).owns(0));
        assert!(OmniOwnership::new(1, 3).owns(1));
        assert!(OmniOwnership::new(2, 3).owns(2));

        assert!(!OmniOwnership::new(1, 3).owns(0));
        assert!(!OmniOwnership::new(2, 3).owns(0));
        assert!(!OmniOwnership::new(0, 3).owns(1));
        assert!(!OmniOwnership::new(2, 3).owns(1));
        assert!(!OmniOwnership::new(0, 3).owns(2));
        assert!(!OmniOwnership::new(1, 3).owns(2));
    }

    #[test]
    fn omni_fewer_sources_than_dests() {
        // n_sources=2, n_dests=4: sub-0 owns even dests, sub-1 owns odd dests.
        let p0 = OmniOwnership::new(0, 2);
        assert!(p0.owns(0));
        assert!(p0.owns(2));
        assert!(!p0.owns(1));
        assert!(!p0.owns(3));

        let p1 = OmniOwnership::new(1, 2);
        assert!(p1.owns(1));
        assert!(p1.owns(3));
        assert!(!p1.owns(0));
        assert!(!p1.owns(2));
    }

    #[test]
    fn omni_more_sources_than_dests_all_dests_covered() {
        // n_sources=5, n_dests=3: subs 0-2 each own their matching dest exclusively.
        assert!(OmniOwnership::new(0, 5).owns(0));
        assert!(OmniOwnership::new(1, 5).owns(1));
        assert!(OmniOwnership::new(2, 5).owns(2));

        assert!(!OmniOwnership::new(1, 5).owns(0));
        assert!(!OmniOwnership::new(2, 5).owns(0));
        assert!(!OmniOwnership::new(0, 5).owns(1));
        assert!(!OmniOwnership::new(2, 5).owns(1));
        assert!(!OmniOwnership::new(0, 5).owns(2));
        assert!(!OmniOwnership::new(1, 5).owns(2));
    }

    #[test]
    fn omni_more_sources_than_dests_excess_sources_idle() {
        // n_sources=5, n_dests=3: subs 3 and 4 own no destinations.
        assert!(!OmniOwnership::new(3, 5).owns(0));
        assert!(!OmniOwnership::new(3, 5).owns(1));
        assert!(!OmniOwnership::new(3, 5).owns(2));

        assert!(!OmniOwnership::new(4, 5).owns(0));
        assert!(!OmniOwnership::new(4, 5).owns(1));
        assert!(!OmniOwnership::new(4, 5).owns(2));
    }
}
