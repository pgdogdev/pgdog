//! Value extracted from a query.

use pg_query::{
    protobuf::{a_const::Val, *},
    NodeEnum,
};

use crate::{
    backend::ShardingSchema,
    frontend::router::sharding::{shard_int, shard_str},
    net::messages::Bind,
};

/// A value extracted from a query.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Value<'a> {
    String(&'a str),
    Integer(i64),
    Boolean(bool),
    Null,
    Placeholder(i32),
}

impl<'a> Value<'a> {
    /// Extract value from a Bind (F) message and shard on it.
    pub fn shard_placeholder(&self, bind: &'a Bind, schema: &ShardingSchema) -> Option<usize> {
        match self {
            Value::Placeholder(placeholder) => bind
                .parameter(*placeholder as usize - 1)
                .ok()
                .flatten()
                .and_then(|value| value.text().map(|value| shard_str(value, schema)))
                .flatten(),
            _ => self.shard(schema),
        }
    }

    /// Shard the value given the number of shards in the cluster.
    pub fn shard(&self, schema: &ShardingSchema) -> Option<usize> {
        match self {
            Value::String(v) => shard_str(v, schema),
            Value::Integer(v) => Some(shard_int(*v, schema)),
            _ => None,
        }
    }
}

impl<'a> From<&'a AConst> for Value<'a> {
    fn from(value: &'a AConst) -> Self {
        if value.isnull {
            return Value::Null;
        }

        match value.val.as_ref() {
            Some(Val::Sval(s)) => match s.sval.parse::<i64>() {
                Ok(i) => Value::Integer(i),
                Err(_) => Value::String(s.sval.as_str()),
            },
            Some(Val::Boolval(b)) => Value::Boolean(b.boolval),
            Some(Val::Ival(i)) => Value::Integer(i.ival as i64),
            _ => Value::Null,
        }
    }
}

impl<'a> TryFrom<&'a Node> for Value<'a> {
    type Error = ();

    fn try_from(value: &'a Node) -> Result<Self, Self::Error> {
        match &value.node {
            Some(NodeEnum::AConst(a_const)) => Ok(a_const.into()),
            Some(NodeEnum::ParamRef(param_ref)) => Ok(Value::Placeholder(param_ref.number)),
            _ => Err(()),
        }
    }
}
