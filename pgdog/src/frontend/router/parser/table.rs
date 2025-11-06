use std::fmt::Display;

use pg_query::{
    protobuf::{List, RangeVar},
    Node, NodeEnum,
};

use crate::util::escape_identifier;

/// Table name in a query.
#[derive(Debug, Clone, Copy, PartialEq, Default, Hash, Eq)]
pub struct Table<'a> {
    /// Table name.
    pub name: &'a str,
    /// Schema name, if specified.
    pub schema: Option<&'a str>,
    /// Alias.
    pub alias: Option<&'a str>,
}

/// Owned version of Table that owns its string data.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct OwnedTable {
    /// Table name.
    pub name: String,
    /// Schema name, if specified.
    pub schema: Option<String>,
    /// Alias.
    pub alias: Option<String>,
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

    pub fn name_match(&self, name: &str) -> bool {
        Some(name) == self.alias || name == self.name
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
            alias: table.alias.map(|s| s.to_owned()),
        }
    }
}

impl<'a> From<&'a OwnedTable> for Table<'a> {
    fn from(owned: &'a OwnedTable) -> Self {
        Self {
            name: &owned.name,
            schema: owned.schema.as_deref(),
            alias: owned.alias.as_deref(),
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
        let table = value
            .first()
            .and_then(|node| {
                node.node.as_ref().map(|node| match node {
                    NodeEnum::RangeVar(var) => Some(Ok(Table::from(var))),
                    NodeEnum::List(list) => Some(Table::try_from(list)),
                    _ => None,
                })
            })
            .flatten()
            .ok_or(())?;
        Ok(table?)
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
    type Error = ();
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
                let name = str_value(value, 1).ok_or(())?;
                Ok(Table {
                    schema,
                    name,
                    alias: None,
                })
            }

            1 => {
                let name = str_value(value, 0).ok_or(())?;
                Ok(Table {
                    schema: None,
                    name,
                    alias: None,
                })
            }

            _ => Err(()),
        }
    }
}
