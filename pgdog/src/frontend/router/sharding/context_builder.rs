use crate::config::{DataType, ShardedTable};

use super::{Centroids, Context, Data, Error, Operator, Value};

pub struct ContextBuilder<'a> {
    data_type: DataType,
    value: Option<Value<'a>>,
    operator: Option<Operator<'a>>,
    centroids: Option<Centroids<'a>>,
    probes: usize,
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
            })
        } else if uuid.valid() {
            Ok(Self {
                data_type: DataType::Uuid,
                value: Some(uuid),
                probes: 0,
                centroids: None,
                operator: None,
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
            });
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

        Ok(Context { operator, value })
    }
}
