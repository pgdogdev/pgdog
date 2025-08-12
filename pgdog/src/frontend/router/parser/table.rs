use std::fmt::Display;

use pg_query::{protobuf::RangeVar, Node, NodeEnum};

use crate::util::escape_identifier;

/// Table name in a query.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Table<'a> {
    /// Table name.
    pub name: &'a str,
    /// Schema name, if specified.
    pub schema: Option<&'a str>,
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

impl Default for Table<'_> {
    fn default() -> Self {
        Self {
            name: "",
            schema: None,
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
