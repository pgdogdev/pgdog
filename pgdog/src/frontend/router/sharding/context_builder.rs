use crate::config::{DataType, ShardListMap, ShardRangeMap, ShardedTable, ShardingMethod};

use super::{Centroids, Context, Data, Error, Operator, Value};

pub struct ContextBuilder<'a> {
    data_type: DataType,
    value: Option<Value<'a>>,
    operator: Option<Operator<'a>>,
    centroids: Option<Centroids<'a>>,
    probes: usize,
    sharding_method: Option<ShardingMethod>,
    shard_range_map: Option<ShardRangeMap>,
    shard_list_map: Option<ShardListMap>,
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
            // added for list and range sharding
            // todo: add lifetimes to these to avoid cloning
            sharding_method: table.sharding_method.clone(),
            shard_range_map: table.shard_range_map.clone(),
            shard_list_map: table.shard_list_map.clone(),
        }
    }

    /// Guess the data type.
    pub fn from_str(value: &'a str) -> Result<Self, Error> {
        let bigint = Value::new(value, DataType::Bigint);
        let uuid = Value::new(value, DataType::Uuid);

        if bigint.valid() {
            Ok(Self {
                data_type: DataType::Bigint,
                value: Some(bigint),
                probes: 0,
                centroids: None,
                operator: None,
                sharding_method: None,
                shard_range_map: None,
                shard_list_map: None,
            })
        } else if uuid.valid() {
            Ok(Self {
                data_type: DataType::Uuid,
                value: Some(uuid),
                probes: 0,
                centroids: None,
                operator: None,
                sharding_method: None,
                shard_range_map: None,
                shard_list_map: None,
            })
        } else {
            Err(Error::IncompleteContext)
        }
    }

    pub fn shards(mut self, shards: usize) -> Self {
        if let Some(centroids) = self.centroids.take() {
            self.operator = Some(Operator::Centroids {
                shards,
                probes: self.probes,
                centroids,
            })
        } else if let Some(method) = self.sharding_method.take() {
            match method {
                ShardingMethod::Hash => {
                    self.operator = Some(Operator::Shards(shards));
                    return self;
                }
                ShardingMethod::Range => {
                    if self.shard_range_map.is_some() {
                        self.operator =
                            Some(Operator::Ranges(self.shard_range_map.clone().unwrap()))
                    }
                }
                ShardingMethod::List => {
                    if self.shard_list_map.is_some() {
                        self.operator = Some(Operator::Lists(self.shard_list_map.clone().unwrap()))
                    }
                }
            }
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

        Ok(Context { operator, value })
    }
}
