//! Check that the source and destination clusters
//! don't have overlapping shards.

use super::super::Error;
use crate::backend::{Cluster, Shard};

pub(super) struct OverlappingShardsCheck<'a> {
    source: &'a Cluster,
}

impl<'a> OverlappingShardsCheck<'a> {
    /// Create check.
    pub(super) fn new(source: &'a Cluster) -> Self {
        Self { source }
    }

    /// Check if the destination shard overlaps with any shards in the source cluster.
    pub(super) fn overlaps(&self, shard: &Shard) -> Result<bool, Error> {
        let address = shard.primary_address()?;

        for source_shard in self.source.shards() {
            if source_shard.primary_address()?.same_database(address) {
                return Ok(true);
            }
        }

        Ok(false)
    }
}
