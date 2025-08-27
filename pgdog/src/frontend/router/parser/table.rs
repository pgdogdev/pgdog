use std::fmt::Display;

use pg_query::{protobuf::RangeVar, Node, NodeEnum};

use crate::util::escape_identifier;

/// Table name in a query.
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct Table<'a> {
    /// Table name.
    pub name: &'a str,
    /// Schema name, if specified.
    pub schema: Option<&'a str>,
}

/// Owned version of Table that owns its string data.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct OwnedTable {
    /// Table name.
    pub name: String,
    /// Schema name, if specified.
    pub schema: Option<String>,
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
    /// Convert this borrowed Table to an owned OwnedTable
    pub fn to_owned(&self) -> OwnedTable {
        OwnedTable::from(*self)
    }
}

impl Display for OwnedTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let borrowed = Table::from(self);
        borrowed.fmt(f)
    }
}

impl<'a> From<Table<'a>> for OwnedTable {
    fn from(table: Table<'a>) -> Self {
        Self {
            name: table.name.to_owned(),
            schema: table.schema.map(|s| s.to_owned()),
        }
    }
}

impl<'a> From<&'a OwnedTable> for Table<'a> {
    fn from(owned: &'a OwnedTable) -> Self {
        Self {
            name: &owned.name,
            schema: owned.schema.as_deref(),
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
    type Error = ();

    fn try_from(value: &'a Vec<Node>) -> Result<Self, Self::Error> {
        let name = value
            .first()
            .and_then(|node| {
                node.node.as_ref().map(|node| match node {
                    NodeEnum::RangeVar(var) => Some(if let Some(ref alias) = var.alias {
                        alias.aliasname.as_str()
                    } else {
                        var.relname.as_str()
                    }),
                    _ => None,
                })
            })
            .flatten()
            .ok_or(())?;
        Ok(Self { name, schema: None })
    }
}

impl<'a> From<&'a RangeVar> for Table<'a> {
    fn from(range_var: &'a RangeVar) -> Self {
        let name = if let Some(ref alias) = range_var.alias {
            alias.aliasname.as_str()
        } else {
            range_var.relname.as_str()
        };
        Self {
            name,
            schema: if !range_var.schemaname.is_empty() {
                Some(range_var.schemaname.as_str())
            } else {
                None
            },
        }
    }
}
