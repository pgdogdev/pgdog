use pg_query::{Node, NodeEnum};

use super::*;

/// Handle FROM <table/join> clause.
#[derive(Copy, Clone)]
pub struct FromClause<'a> {
    nodes: &'a [Node],
}

impl<'a> FromClause<'a> {
    /// Create new FROM clause parser.
    pub fn new(nodes: &'a [Node]) -> Self {
        Self { nodes }
    }

    /// Get actual table name from an alias specified in the FROM clause.
    /// If no alias is specified, the table name is returned as-is.
    pub fn resolve_alias(&'a self, name: &'a str) -> Option<&'a str> {
        for node in self.nodes {
            if let Some(ref node) = node.node {
                if let Some(name) = Self::resolve(name, node) {
                    return Some(name);
                }
            }
        }

        None
    }

    fn resolve(name: &'a str, node: &'a NodeEnum) -> Option<&'a str> {
        match node {
            NodeEnum::JoinExpr(ref join) => {
                for arg in [&join.larg, &join.rarg].into_iter().flatten() {
                    if let Some(ref node) = arg.node {
                        if let Some(name) = Self::resolve(name, node) {
                            return Some(name);
                        }
                    }
                }
            }

            NodeEnum::RangeVar(ref range_var) => {
                let table = Table::from(range_var);
                if table.name_match(name) {
                    return Some(table.name);
                }
            }

            _ => (),
        }

        None
    }

    /// Get table name if the FROM clause contains only one table.
    pub fn table_name(&'a self) -> Option<&'a str> {
        if let Some(node) = self.nodes.first() {
            if let Some(NodeEnum::RangeVar(ref range_var)) = node.node {
                let table = Table::from(range_var);
                return Some(table.name);
            }
        }

        None
    }
}
