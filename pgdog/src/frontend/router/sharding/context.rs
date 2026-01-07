use crate::frontend::router::parser::Shard;
use tracing::trace;

use super::{Error, Hasher, Operator, Value};

#[derive(Debug)]
pub struct Context<'a> {
    pub(super) value: Value<'a>,
    pub(super) operator: Operator<'a>,
    pub(super) hasher: Hasher,
}

impl Context<'_> {
    pub fn apply(&self) -> Result<Shard, Error> {
        match &self.operator {
            Operator::Shards(shards) => {
                trace!("sharding using hash");
                if let Some(hash) = self.value.hash(self.hasher)? {
                    return Ok(Shard::Direct(hash as usize % shards));
                }
            }

            Operator::Centroids {
                shards,
                probes,
                centroids,
            } => {
                trace!("sharding using k-means");
                if let Some(vector) = self.value.vector()? {
                    return Ok(centroids.shard(&vector, *shards, *probes).into());
                }
            }

            Operator::Range(ranges) => {
                trace!("sharding using range");
                return ranges.shard(&self.value);
            }

            Operator::List(lists) => {
                trace!("sharding using lists");
                return lists.shard(&self.value);
            }
        }

        Ok(Shard::All)
    }
}
