use crate::{
    backend::ShardingSchema,
    config::{DataType, Hasher as HasherConfig, ShardedTable},
    frontend::router::sharding::Mapping,
};

use super::{Centroids, Context, Data, Error, Hasher, Lists, Operator, Ranges, Value};

#[derive(Debug)]
pub struct ContextBuilder<'a> {
    data_type: DataType,
    value: Option<Value<'a>>,
    operator: Option<Operator<'a>>,
    centroids: Option<Centroids<'a>>,
    ranges: Option<Ranges<'a>>,
    lists: Option<Lists<'a>>,
    probes: usize,
    hasher: Hasher,
    #[allow(dead_code)]
    array: bool,
}

impl<'a> ContextBuilder<'a> {
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
            ranges: Ranges::new(&table.mapping),
            lists: Lists::new(&table.mapping),
            array: false,
        }
    }

    /// Infer sharding function from config.
    pub fn infer_from_from_and_config(
        value: &'a str,
        sharding_schema: &'a ShardingSchema,
    ) -> Result<Self, Error> {
        if let Some(common_mapping) = sharding_schema.tables.common_mapping() {
            let value = Value::new(value, common_mapping.data_type);
            if !value.valid() {
                Err(Error::InvalidValue)
            } else if let Some(ref mapping) = common_mapping.mapping {
                match mapping {
                    Mapping::List(_) => Ok(Self {
                        data_type: common_mapping.data_type,
                        value: Some(value),
                        probes: 0,
                        centroids: None,
                        operator: None,
                        hasher: Hasher::Postgres,
                        array: false,
                        ranges: None,
                        lists: Lists::new(&common_mapping.mapping),
                    }),
                    Mapping::Range(_) => Ok(Self {
                        data_type: common_mapping.data_type,
                        value: Some(value),
                        probes: 0,
                        centroids: None,
                        operator: None,
                        hasher: Hasher::Postgres,
                        array: false,
                        lists: None,
                        ranges: Ranges::new(&common_mapping.mapping),
                    }),
                }
            } else {
                Ok(Self {
                    data_type: common_mapping.data_type,
                    value: Some(value),
                    probes: 0,
                    centroids: None,
                    operator: None,
                    hasher: Hasher::Postgres,
                    array: false,
                    ranges: None,
                    lists: None,
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
                array: false,
                ranges: None,
                lists: None,
            })
        } else if uuid.valid() {
            Ok(Self {
                data_type: DataType::Uuid,
                value: Some(uuid),
                probes: 0,
                centroids: None,
                operator: None,
                hasher: Hasher::Postgres,
                array: false,
                ranges: None,
                lists: None,
            })
        } else if varchar.valid() {
            Ok(Self {
                data_type: DataType::Varchar,
                value: Some(varchar),
                probes: 0,
                centroids: None,
                operator: None,
                hasher: Hasher::Postgres,
                array: false,
                ranges: None,
                lists: None,
            })
        } else {
            Err(Error::InvalidValue)
        }
    }

    pub fn shards(mut self, shards: usize) -> Self {
        if let Some(centroids) = self.centroids.take() {
            self.operator = Some(Operator::Centroids {
                shards,
                probes: self.probes,
                centroids,
            });
        } else if let Some(ranges) = self.ranges.take() {
            self.operator = Some(Operator::Range(ranges));
        } else if let Some(lists) = self.lists.take() {
            self.operator = Some(Operator::List(lists));
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
    use crate::{
        backend::ShardedTables,
        config::{FlexibleType, ShardedMapping, ShardedMappingKind},
        frontend::router::{parser::Shard, sharding::ListShards},
    };

    use super::*;

    #[test]
    fn test_hash() {
        let schema = ShardingSchema {
            shards: 2,
            tables: ShardedTables::new(
                vec![ShardedTable {
                    data_type: DataType::Varchar,
                    mapping: None,
                    ..Default::default()
                }],
                vec![],
            ),
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
                    mapping: Some(Mapping::Range(vec![ShardedMapping {
                        start: Some(FlexibleType::Integer(1)),
                        end: Some(FlexibleType::Integer(25)),
                        shard: 0,
                        kind: ShardedMappingKind::Range,
                        ..Default::default()
                    }])),
                    ..Default::default()
                }],
                vec![],
            ),
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
                    mapping: Some(Mapping::List(ListShards::new(&[ShardedMapping {
                        values: vec![FlexibleType::Integer(1), FlexibleType::Integer(2)]
                            .into_iter()
                            .collect(),
                        shard: 0,
                        kind: ShardedMappingKind::List,
                        ..Default::default()
                    }]))),
                    ..Default::default()
                }],
                vec![],
            ),
        };

        let builder = ContextBuilder::infer_from_from_and_config("1", &schema).unwrap();

        let ctx = builder.shards(2).build().unwrap();
        let shard = ctx.apply().unwrap();
        assert_eq!(shard, Shard::Direct(0));
    }
}
