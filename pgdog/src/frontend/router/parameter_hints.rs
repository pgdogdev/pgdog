use pgdog_config::Role;

use super::parser::Error;
use crate::{
    backend::ShardingSchema,
    frontend::router::{
        parser::{Schema, Shard, ShardWithPriority, ShardsWithPriority},
        sharding::{ContextBuilder, SchemaSharder},
    },
    net::{parameter::ParameterValue, Parameters},
};

#[derive(Debug, Clone)]
pub struct ParameterHints<'a> {
    pub search_path: Option<&'a ParameterValue>,
    pub pgdog_shard: Option<&'a ParameterValue>,
    pub pgdog_sharding_key: Option<&'a ParameterValue>,
    pub pgdog_role: Option<&'a ParameterValue>,
}

impl<'a> From<&'a Parameters> for ParameterHints<'a> {
    fn from(value: &'a Parameters) -> Self {
        Self {
            search_path: value.search_path(),
            pgdog_shard: value.get("pgdog.shard"),
            pgdog_role: value.get("pgdog.role"),
            pgdog_sharding_key: value.get("pgdog.sharding_key"),
        }
    }
}

impl ParameterHints<'_> {
    /// Compute shard from parameters.
    pub(crate) fn compute_shard(
        &self,
        shards: &mut ShardsWithPriority,
        sharding_schema: &ShardingSchema,
    ) -> Result<(), Error> {
        if let Some(ParameterValue::Integer(val)) = self.pgdog_shard {
            shards.push(ShardWithPriority::new_set(Shard::Direct(*val as usize)));
        }
        if let Some(ParameterValue::String(val)) = self.pgdog_shard {
            if let Ok(shard) = val.parse() {
                shards.push(ShardWithPriority::new_set(Shard::Direct(shard)));
            }
        }
        if let Some(ParameterValue::String(val)) = self.pgdog_sharding_key {
            let ctx = ContextBuilder::infer_from_from_and_config(val.as_str(), &sharding_schema)?
                .shards(sharding_schema.shards)
                .build()?;
            let shard = ctx.apply()?;
            shards.push(ShardWithPriority::new_set(shard));
        }
        if let Some(search_path) = self.search_path {
            let mut schema_sharder = SchemaSharder::default();

            match search_path {
                ParameterValue::String(search_path) => {
                    let schema = Schema::from(search_path.as_str());
                    schema_sharder.resolve(Some(schema), &sharding_schema.schemas);
                }
                ParameterValue::Tuple(search_paths) => {
                    for schema in search_paths {
                        let schema = Schema::from(schema.as_str());
                        schema_sharder.resolve(Some(schema), &sharding_schema.schemas);
                    }
                }

                _ => (),
            }

            if let Some((shard, schema)) = schema_sharder.get() {
                shards.push(ShardWithPriority::new_search_path(shard.clone(), schema));
            }
        }
        Ok(())
    }

    /// Compute role from parameter value.
    pub(crate) fn compute_role(&self) -> Option<Role> {
        match self.pgdog_role {
            Some(ParameterValue::String(val)) => match val.as_str() {
                "replica" => Some(Role::Replica),
                "primary" => Some(Role::Primary),
                _ => None,
            },

            _ => None,
        }
    }
}
