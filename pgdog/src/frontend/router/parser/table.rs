use std::fmt::Display;

use pg_raw_parse::list::{CastNodeList, NodeList};
use pg_raw_parse::{Node, nodes};

use super::{Error, Schema};
use crate::util::escape_identifier;

/// Table name in a query.
#[derive(Debug, Clone, Copy, PartialEq, Default, Hash, Eq)]
pub(crate) struct Table<'a> {
    /// Table name.
    pub(crate) name: &'a str,
    /// Schema name, if specified.
    pub(crate) schema: Option<&'a str>,
    /// Alias.
    pub(crate) alias: Option<&'a str>,
}

impl Display for Table<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(schema) = self.schema {
            write!(
                f,
                "\"{}\".\"{}\"",
                escape_identifier(schema),
                escape_identifier(self.name)
            )
        } else {
            write!(f, "\"{}\"", escape_identifier(self.name))
        }
    }
}

impl<'a> Table<'a> {
    pub(crate) fn name_match(&self, name: &str) -> bool {
        Some(name) == self.alias || name == self.name
    }

    pub(crate) fn schema(&self) -> Option<Schema<'a>> {
        self.schema.map(|s| s.into())
    }
}

impl<'a> TryFrom<pg_raw_parse::Node<'a>> for Table<'a> {
    type Error = ();

    fn try_from(value: pg_raw_parse::Node<'a>) -> Result<Self, Self::Error> {
        match value {
            pg_raw_parse::Node::RangeVar(rv) => Ok(rv.into()),
            _ => Err(()),
        }
    }
}

impl<'a> From<&'a pg_raw_parse::nodes::RangeVar> for Table<'a> {
    fn from(range_var: &'a pg_raw_parse::nodes::RangeVar) -> Self {
        let name = range_var.relname().unwrap_or_default();
        let alias = range_var.alias().and_then(|a| a.aliasname());
        let schema = range_var.schemaname();
        Self {
            name,
            alias,
            schema,
        }
    }
}

impl<'a> TryFrom<&'a CastNodeList<nodes::String>> for Table<'a> {
    type Error = Error;

    fn try_from(value: &'a CastNodeList<nodes::String>) -> Result<Self, Self::Error> {
        let mut list = value.into_iter().rev();
        let name = list
            .next()
            .and_then(nodes::String::sval)
            .ok_or(Error::TableDecode)?;
        match (list.next(), list.next()) {
            (schema, None) => {
                let schema = schema
                    .map(|s| s.sval().ok_or(Error::TableDecode))
                    .transpose()?;
                Ok(Self {
                    schema,
                    name,
                    alias: None,
                })
            }
            (_, Some(_)) => Err(Error::TableDecode),
        }
    }
}

impl<'a> TryFrom<&'a NodeList> for Table<'a> {
    type Error = Error;

    fn try_from(value: &'a NodeList) -> Result<Self, Self::Error> {
        let mut list = value.into_iter().rev();
        let name = match list.next() {
            Some(Node::String(s)) => s.sval(),
            Some(Node::NodeList(l)) if value.len() == 1 => return Self::try_from(l),
            Some(Node::RangeVar(rv)) if value.len() == 1 => return Ok(Self::from(rv)),
            _ => None,
        }
        .ok_or(Error::TableDecode)?;

        match (list.next(), list.next()) {
            (schema, None) => {
                let schema = schema
                    .map(|s| s.as_str().ok_or(Error::TableDecode))
                    .transpose()?;
                Ok(Self {
                    schema,
                    name,
                    alias: None,
                })
            }
            (_, Some(_)) => Err(Error::TableDecode),
        }
    }
}

impl<'a> From<&'a str> for Table<'a> {
    fn from(value: &'a str) -> Self {
        Table {
            name: value,
            ..Default::default()
        }
    }
}
