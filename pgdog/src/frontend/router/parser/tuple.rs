//! A list of values.

use std::ops::Deref;

use pg_query::{protobuf::*, NodeEnum};

use super::Value;

/// List of values in a single row.
#[derive(Debug, Clone, PartialEq)]
pub struct Tuple<'a> {
    /// List of values.
    pub values: Vec<Value<'a>>,
}

impl<'a> TryFrom<&'a List> for Tuple<'a> {
    type Error = ();

    fn try_from(value: &'a List) -> Result<Self, Self::Error> {
        let mut values = vec![];

        for value in &value.items {
            let value = value.try_into()?;
            values.push(value);
        }

        Ok(Self { values })
    }
}

impl<'a> TryFrom<&'a Node> for Tuple<'a> {
    type Error = ();

    fn try_from(value: &'a Node) -> Result<Self, Self::Error> {
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
