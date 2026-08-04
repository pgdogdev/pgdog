use pg_raw_parse::{Node, list::NodeList};

use super::*;

/// Handle FROM <table/join> clause.
#[derive(Copy, Clone, Debug)]
pub(crate) struct FromClause<'a> {
    nodes: &'a NodeList,
}

impl<'a> FromClause<'a> {
    /// Create new FROM clause parser.
    #[cfg(test)]
    pub(crate) fn new(nodes: &'a NodeList) -> Self {
        Self { nodes }
    }

    /// Get actual table name from an alias specified in the FROM clause.
    /// If no alias is specified, the table name is returned as-is.
    pub(crate) fn resolve_alias(&self, name: &str) -> Option<&'a str> {
        self.nodes.iter().find_map(|node| Self::resolve(name, node))
    }

    fn resolve(name: &str, node: Node<'a>) -> Option<&'a str> {
        match node {
            Node::JoinExpr(join) => {
                Self::resolve(name, join.larg()).or_else(|| Self::resolve(name, join.rarg()))
            }

            Node::RangeVar(range_var) => {
                let table = Table::from(range_var);
                table.name_match(name).then_some(table.name)
            }

            _ => None,
        }
    }

    /// Get table name if the FROM clause contains only one table.
    pub(crate) fn table_name(&self) -> Option<&'a str> {
        self.nodes.first().and_then(|node| match node {
            Node::RangeVar(r) => Some(Table::from(r).name),
            _ => None,
        })
    }
}
