use std::fmt::Display;

use pg_query::{
    Node, NodeEnum,
    protobuf::{List, RangeVar},
};

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

#[cfg(feature = "new_parser")]
impl<'a> TryFrom<pg_raw_parse::Node<'a>> for Table<'a> {
    type Error = ();

    fn try_from(value: pg_raw_parse::Node<'a>) -> Result<Self, Self::Error> {
        match value {
            pg_raw_parse::Node::RangeVar(rv) => Ok(rv.into()),
            _ => Err(()),
        }
    }
}

impl<'a> TryFrom<&'a Node> for Table<'a> {
    type Error = ();

    fn try_from(value: &'a Node) -> Result<Self, Self::Error> {
        if let Some(NodeEnum::RangeVar(range_var)) = &value.node {
            return Ok(range_var.into());
        }

        Err(())
    }
}

impl<'a> TryFrom<&'a Vec<Node>> for Table<'a> {
    type Error = Error;

    fn try_from(value: &'a Vec<Node>) -> Result<Self, Self::Error> {
        match value.len() {
            1 => {
                let table = value
                    .first()
                    .and_then(|node| {
                        node.node.as_ref().map(|node| match node {
                            NodeEnum::RangeVar(var) => Some(Ok(Table::from(var))),
                            NodeEnum::List(list) => Some(Table::try_from(list)),
                            NodeEnum::String(str) => Some(Ok(Table::from(str.sval.as_str()))),
                            _ => None,
                        })
                    })
                    .flatten()
                    .ok_or(Error::TableDecode)?;
                return table;
            }

            2 => {
                let schema = value.iter().next().unwrap().node.as_ref().and_then(|node| {
                    if let NodeEnum::String(sval) = node {
                        Some(sval.sval.as_str())
                    } else {
                        None
                    }
                });
                let table = value.iter().last().unwrap().node.as_ref().and_then(|node| {
                    if let NodeEnum::String(sval) = node {
                        Some(sval.sval.as_str())
                    } else {
                        None
                    }
                });
                if let Some(schema) = schema
                    && let Some(table) = table
                {
                    return Ok(Table {
                        name: table,
                        schema: Some(schema),
                        alias: None,
                    });
                }
            }

            _ => (),
        }

        Err(Error::TableDecode)
    }
}

#[cfg(feature = "new_parser")]
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

impl<'a> From<&'a RangeVar> for Table<'a> {
    fn from(range_var: &'a RangeVar) -> Self {
        let (name, alias) = if let Some(ref alias) = range_var.alias {
            (range_var.relname.as_str(), Some(alias.aliasname.as_str()))
        } else {
            (range_var.relname.as_str(), None)
        };
        Self {
            name,
            schema: if !range_var.schemaname.is_empty() {
                Some(range_var.schemaname.as_str())
            } else {
                None
            },
            alias,
        }
    }
}

impl<'a> TryFrom<&'a List> for Table<'a> {
    type Error = Error;

    fn try_from(value: &'a List) -> Result<Self, Self::Error> {
        fn str_value(list: &List, pos: usize) -> Option<&str> {
            if let Some(NodeEnum::String(ref schema)) = list.items.get(pos).unwrap().node {
                Some(schema.sval.as_str())
            } else {
                None
            }
        }

        match value.items.len() {
            2 => {
                let schema = str_value(value, 0);
                let name = str_value(value, 1).ok_or(Error::TableDecode)?;
                Ok(Table {
                    schema,
                    name,
                    alias: None,
                })
            }

            1 => {
                let name = str_value(value, 0).ok_or(Error::TableDecode)?;
                Ok(Table {
                    schema: None,
                    name,
                    alias: None,
                })
            }

            _ => Err(Error::TableDecode),
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
