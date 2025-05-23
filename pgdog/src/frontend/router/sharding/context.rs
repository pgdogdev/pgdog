use crate::frontend::router::parser::Shard;

use super::{Error, Operator, Value};

#[derive(Debug)]
pub struct Context<'a> {
    pub(super) value: Value<'a>,
    pub(super) operator: Operator<'a>,
}

impl<'a> Context<'a> {
    pub fn apply(&self) -> Result<Shard, Error> {
        match &self.operator {
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
                    return Ok(centroids.shard(&vector, *shards, *probes));
                }
            }
        }

        Ok(Shard::All)
    }
}
