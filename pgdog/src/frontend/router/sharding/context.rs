use crate::frontend::router::parser::Shard;
use tracing::debug;

use super::{Error, Hasher, Operator, Value};

#[derive(Debug)]
pub(crate) struct Context<'a> {
    pub(super) value: Value<'a>,
    pub(super) operator: Operator<'a>,
    pub(super) hasher: Hasher,
}

impl Context<'_> {
    pub(crate) fn apply(&self) -> Result<Shard, Error> {
        match &self.operator {
            Operator::Shards(shards) => {
                debug!("sharding using hash");
                if let Some(hash) = self.value.hash(self.hasher)? {
                    return Ok(Shard::Direct(hash as usize % shards));
                }
            }

            Operator::Centroids {
                shards,
                probes,
                centroids,
            } => {
                debug!("sharding using k-means");
                if let Some(vector) = self.value.vector()? {
                    return Ok(centroids.shard(&vector, *shards, *probes));
                }
            }

            Operator::Range(ranges) => {
                debug!("sharding using range");
                return ranges.shard(&self.value);
            }

            Operator::List(lists) => {
                debug!("sharding using lists");
                return lists.shard(&self.value);
            }
        }

        Ok(Shard::All)
    }
}
