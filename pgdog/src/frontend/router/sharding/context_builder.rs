//! Context builder for sharding a value.
//!
//! Manages mapping a value (integer, string, etc.)
//! to a shard number, given a sharded mapping in pgdog.toml.
//!
use crate::frontend::router::sharding::mapping::MappingResolver;
use crate::{
    backend::ShardingSchema,
    config::{DataType, Hasher as HasherConfig},
    frontend::router::sharding::ShardedTable,
};

use super::{Centroids, Context, Data, Error, Hasher, Operator, Value};

/// Sharding context builder.
#[derive(Debug)]
pub struct ContextBuilder<'a> {
    data_type: DataType,
    value: Option<Value<'a>>,
    operator: Option<Operator<'a>>,
    centroids: Option<Centroids<'a>>,
    mapping: Option<MappingResolver<'a>>,
    probes: usize,
    hasher: Hasher,
}

impl<'a> ContextBuilder<'a> {
    /// Create new context builder from a sharded table
    /// in the config.
    pub fn new(table: &'a ShardedTable) -> Self {
        Self {
            data_type: table.data_type,
            centroids: if table.centroids.is_empty() {
                None
            } else {
                Some(Centroids::from(&table.centroids))
            },
            probes: table.centroid_probes,
            operator: None,
            value: None,
            hasher: match table.hasher {
                HasherConfig::Sha1 => Hasher::Sha1,
                HasherConfig::Postgres => Hasher::Postgres,
            },
            mapping: MappingResolver::new(&table.mapping),
        }
    }

    /// Infer sharding function from config, iff
    /// only one sharding function is configured.
    pub fn infer_from_from_and_config(
        value: &'a str,
        sharding_schema: &'a ShardingSchema,
    ) -> Result<Self, Error> {
        if let Some(common_mapping) = sharding_schema.tables.common_mapping() {
            let value = Value::new(value, common_mapping.data_type);
            if !value.valid() {
                Err(Error::InvalidValue)
            } else {
                Ok(Self {
                    data_type: common_mapping.data_type,
                    value: Some(value),
                    probes: 0,
                    centroids: None,
                    operator: None,
                    hasher: Hasher::Postgres,
                    mapping: MappingResolver::new(&common_mapping.mapping),
                })
            }
        } else {
            Err(Error::MultipleShardingFunctions)
        }
    }

    /// Guess the data type.
    pub fn from_string(value: &'a str) -> Result<Self, Error> {
        let bigint = Value::new(value, DataType::Bigint);
        let uuid = Value::new(value, DataType::Uuid);
        let varchar = Value::new(value, DataType::Varchar);

        if bigint.valid() {
            Ok(Self {
                data_type: DataType::Bigint,
                value: Some(bigint),
                probes: 0,
                centroids: None,
                operator: None,
                hasher: Hasher::Postgres,
                mapping: None,
            })
        } else if uuid.valid() {
            Ok(Self {
                data_type: DataType::Uuid,
                value: Some(uuid),
                probes: 0,
                centroids: None,
                operator: None,
                hasher: Hasher::Postgres,
                mapping: None,
            })
        } else if varchar.valid() {
            Ok(Self {
                data_type: DataType::Varchar,
                value: Some(varchar),
                probes: 0,
                centroids: None,
                operator: None,
                hasher: Hasher::Postgres,
                mapping: None,
            })
        } else {
            Err(Error::InvalidValue)
        }
    }

    /// Set the number of shards in the configuration.
    pub fn shards(mut self, shards: usize) -> Self {
        if let Some(centroids) = self.centroids.take() {
            self.operator = Some(Operator::Centroids {
                shards,
                probes: self.probes,
                centroids,
            });
        } else if let Some(mapping) = self.mapping.take() {
            self.operator = Some(Operator::Mapping(mapping));
        } else {
            self.operator = Some(Operator::Shards(shards))
        }
        self
    }

    pub fn data(mut self, data: impl Into<Data<'a>>) -> Self {
        self.value = Some(Value::new(data, self.data_type));
        self
    }

    pub fn value(mut self, value: Value<'a>) -> Self {
        self.value = Some(value);
        self
    }

    pub fn build(mut self) -> Result<Context<'a>, Error> {
        let operator = self.operator.take().ok_or(Error::IncompleteContext)?;
        let value = self.value.take().ok_or(Error::IncompleteContext)?;

        Ok(Context {
            operator,
            value,
            hasher: self.hasher,
        })
    }
}

#[cfg(test)]
mod test {
    use crate::frontend::router::sharding::Mapping;
    use pgdog_config::SystemCatalogsBehavior;

    use crate::{
        backend::ShardedTables,
        config::{FlexibleType, ShardedMappingConfig, ShardedMappingList, ShardedMappingRange},
        frontend::router::parser::Shard,
    };

    use super::*;

    #[test]
    fn test_hash() {
        let schema = ShardingSchema {
            shards: 2,
            tables: ShardedTables::new(
                vec![ShardedTable {
                    mapping: None,
                    data_type: DataType::Varchar,
                    ..Default::default()
                }],
                vec![],
                false,
                SystemCatalogsBehavior::default(),
            ),
            ..Default::default()
        };

        let ctx = ContextBuilder::infer_from_from_and_config("test_value", &schema)
            .unwrap()
            .shards(2)
            .build()
            .unwrap();
        let shard = ctx.apply().unwrap();
        assert_eq!(shard, Shard::Direct(1));
    }

    #[test]
    fn test_range() {
        let schema = ShardingSchema {
            shards: 2,
            tables: ShardedTables::new(
                vec![ShardedTable {
                    data_type: DataType::Bigint,
                    mapping: Mapping::new(vec![ShardedMappingConfig::Range(ShardedMappingRange {
                        start: Some(FlexibleType::Integer(1)),
                        end: Some(FlexibleType::Integer(25)),
                        shard: 0,
                    })]),
                    ..Default::default()
                }],
                vec![],
                false,
                SystemCatalogsBehavior::default(),
            ),
            ..Default::default()
        };

        let ctx = ContextBuilder::infer_from_from_and_config("15", &schema)
            .unwrap()
            .shards(2)
            .build()
            .unwrap();
        let shard = ctx.apply().unwrap();
        assert_eq!(shard, Shard::Direct(0));
    }

    #[test]
    fn test_list() {
        let schema = ShardingSchema {
            shards: 2,
            tables: ShardedTables::new(
                vec![ShardedTable {
                    data_type: DataType::Bigint,
                    mapping: Mapping::new(vec![ShardedMappingConfig::List(ShardedMappingList {
                        values: vec![FlexibleType::Integer(1), FlexibleType::Integer(2)],
                        shard: 0,
                    })]),
                    ..Default::default()
                }],
                vec![],
                false,
                SystemCatalogsBehavior::default(),
            ),
            ..Default::default()
        };

        let builder = ContextBuilder::infer_from_from_and_config("1", &schema).unwrap();

        let ctx = builder.shards(2).build().unwrap();
        let shard = ctx.apply().unwrap();
        assert_eq!(shard, Shard::Direct(0));
    }
}
