//! Value extracted from a query.

use pg_query::{
    protobuf::{a_const::Val, *},
    NodeEnum,
};

use crate::{
    backend::ShardingSchema,
    frontend::router::sharding::{shard_int, shard_str},
    net::messages::{Bind, Vector},
};

/// A value extracted from a query.
#[derive(Debug, Clone, PartialEq)]
pub enum Value<'a> {
    String(&'a str),
    Integer(i64),
    Boolean(bool),
    Null,
    Placeholder(i32),
    Vector(Vector),
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

    /// Get vector if it's a vector.
    pub fn vector(self) -> Option<Vector> {
        match self {
            Self::Vector(vector) => Some(vector),
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
            Some(Val::Sval(s)) => {
                if s.sval.starts_with('[') && s.sval.ends_with(']') {
                    if let Ok(vector) = Vector::try_from(s.sval.as_str()) {
                        Value::Vector(vector)
                    } else {
                        Value::String(s.sval.as_str())
                    }
                } else {
                    match s.sval.parse::<i64>() {
                        Ok(i) => Value::Integer(i),
                        Err(_) => Value::String(s.sval.as_str()),
                    }
                }
            }
            Some(Val::Boolval(b)) => Value::Boolean(b.boolval),
            Some(Val::Ival(i)) => Value::Integer(i.ival as i64),
            _ => Value::Null,
        }
    }
}

impl<'a> TryFrom<&'a Node> for Value<'a> {
    type Error = ();

    fn try_from(value: &'a Node) -> Result<Self, Self::Error> {
        Value::try_from(&value.node)
    }
}

impl<'a> TryFrom<&'a Option<NodeEnum>> for Value<'a> {
    type Error = ();

    fn try_from(value: &'a Option<NodeEnum>) -> Result<Self, Self::Error> {
        match value {
            Some(NodeEnum::AConst(a_const)) => Ok(a_const.into()),
            Some(NodeEnum::ParamRef(param_ref)) => Ok(Value::Placeholder(param_ref.number)),
            _ => Err(()),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_vector_value() {
        let a_cosnt = AConst {
            val: Some(Val::Sval(String {
                sval: "[1,2,3]".into(),
            })),
            isnull: false,
            location: 0,
        };
        let node = Node {
            node: Some(NodeEnum::AConst(a_cosnt)),
        };
        let vector = Value::try_from(&node).unwrap();
        assert_eq!(vector.vector().unwrap()[0], 1.0.into());
    }
}
