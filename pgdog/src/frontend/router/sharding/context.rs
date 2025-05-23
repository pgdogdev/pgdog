use crate::frontend::router::parser::Shard;

use super::{Error, Operator, Value};

pub struct Context<'a> {
    value: Value<'a>,
}

impl<'a> Context<'a> {
    pub fn apply(&self, operator: Operator) -> Result<Shard, Error> {
        match operator {
            Operator::Shards(shards) => {
                if let Some(hash) = self.value.hash()? {
                    return Ok(Shard::Direct(hash as usize % shards));
                }
            }

            Operator::Centroids {
                shards,
                probes,
                centroids,
            } => {
                if let Some(vector) = self.value.vector()? {
                    return Ok(centroids.shard(&vector, shards, probes));
                }
            }
        }

        Ok(Shard::All)
    }
}
