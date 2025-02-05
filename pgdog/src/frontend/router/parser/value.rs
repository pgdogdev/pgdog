//! Value extracted from a query.

use pg_query::{
    protobuf::{a_const::Val, *},
    NodeEnum,
};

use crate::frontend::router::sharding::{shard_int, shard_str};

/// A value extracted from a query.
pub enum Value<'a> {
    String(&'a str),
    Integer(i64),
    Boolean(bool),
    Null,
}

impl Value<'_> {
    /// Shard the value given the number of shards in the cluster.
    pub fn shard(&self, shards: usize) -> Option<usize> {
        match self {
            Value::String(v) => shard_str(v, shards),
            Value::Integer(v) => Some(shard_int(*v, shards)),
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
            _ => Err(()),
        }
    }
}
