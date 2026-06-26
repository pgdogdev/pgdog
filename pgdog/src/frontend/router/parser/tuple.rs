//! A list of values.

use std::ops::Deref;

use pg_query::{
    NodeEnum,
    protobuf::{Node as PgNode, *},
};
#[cfg(feature = "new_parser")]
use pg_raw_parse::Node;

use super::Value;

/// List of values in a single row.
#[derive(Debug, Clone, PartialEq)]
pub struct Tuple<'a> {
    /// List of values.
    pub values: Vec<Value<'a>>,
}

#[cfg(feature = "new_parser")]
impl<'a> FromIterator<Node<'a>> for Tuple<'a> {
    fn from_iter<T: IntoIterator<Item = Node<'a>>>(iter: T) -> Self {
        // FIXME:
        //
        // This basically makes all values we can't parse NULL.
        // Normally, the result of that is the query is sent to all
        // shards, quietly.
        //
        // I think the right thing here is to throw an error,
        // but more likely it'll be a value we don't actually need for sharding.
        //
        // We should check if its value we actually need and only then
        // throw an error.
        //
        let values = iter
            .into_iter()
            .map(|v| v.try_into().unwrap_or(Value::Null))
            .collect();
        Self { values }
    }
}

impl<'a> TryFrom<&'a List> for Tuple<'a> {
    type Error = ();

    fn try_from(value: &'a List) -> Result<Self, Self::Error> {
        let mut values = vec![];

        for value in &value.items {
            if let Ok(value) = Value::try_from(value) {
                values.push(value);
            } else {
                // FIXME:
                //
                // This basically makes all values we can't parse NULL.
                // Normally, the result of that is the query is sent to all
                // shards, quietly.
                //
                // I think the right thing here is to throw an error,
                // but more likely it'll be a value we don't actually need for sharding.
                //
                // We should check if its value we actually need and only then
                // throw an error.
                //
                values.push(Value::Null);
            }
        }

        Ok(Self { values })
    }
}

impl<'a> TryFrom<&'a PgNode> for Tuple<'a> {
    type Error = ();

    fn try_from(value: &'a PgNode) -> Result<Self, Self::Error> {
        match &value.node {
            Some(NodeEnum::List(list)) => list.try_into(),
            _ => Err(()),
        }
    }
}

impl<'a> Deref for Tuple<'a> {
    type Target = Vec<Value<'a>>;

    fn deref(&self) -> &Self::Target {
        &self.values
    }
}
