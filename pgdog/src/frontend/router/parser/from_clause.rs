#[cfg(not(feature = "new_parser"))]
use pg_query::{Node, NodeEnum};
#[cfg(feature = "new_parser")]
use pg_raw_parse::{Node, list::NodeList};

use super::*;

/// Handle FROM <table/join> clause.
#[derive(Copy, Clone, Debug)]
pub(crate) struct FromClause<'a> {
    #[cfg(feature = "new_parser")]
    nodes: &'a NodeList,
    #[cfg(not(feature = "new_parser"))]
    nodes: &'a [Node],
}

impl<'a> FromClause<'a> {
    /// Create new FROM clause parser.
    #[cfg(all(feature = "new_parser", test))]
    pub(crate) fn new(nodes: &'a NodeList) -> Self {
        Self { nodes }
    }

    cfg_select! {
        not(feature = "new_parser") => {
            pub(crate) fn new(nodes: &'a [Node]) -> Self {
                Self { nodes }
            }
        }
        _ => {}
    }

    /// Get actual table name from an alias specified in the FROM clause.
    /// If no alias is specified, the table name is returned as-is.
    #[cfg(feature = "new_parser")]
    pub(crate) fn resolve_alias(&self, name: &str) -> Option<&'a str> {
        self.nodes.iter().find_map(|node| Self::resolve(name, node))
    }

    cfg_select! {
        not(feature = "new_parser") => {
            pub(crate) fn resolve_alias(&self, name: &str) -> Option<&'a str> {
                for node in self.nodes {
                    if let Some(ref node) = node.node
                        && let Some(name) = Self::resolve(name, node)
                    {
                        return Some(name);
                    }
                }

                None
            }
        }
        _ => {}
    }

    #[cfg(feature = "new_parser")]
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

    cfg_select! {
        not(feature = "new_parser") => {
            fn resolve(name: &str, node: &'a NodeEnum) -> Option<&'a str> {
                match node {
                    NodeEnum::JoinExpr(join) => {
                        for arg in [&join.larg, &join.rarg].into_iter().flatten() {
                            if let Some(ref node) = arg.node
                                && let Some(name) = Self::resolve(name, node)
                            {
                                return Some(name);
                            }
                        }
                    }

                    NodeEnum::RangeVar(range_var) => {
                        let table = Table::from(range_var);
                        if table.name_match(name) {
                            return Some(table.name);
                        }
                    }

                    _ => (),
                }

                None
            }
        }
        _ => {}
    }

    /// Get table name if the FROM clause contains only one table.
    #[cfg(feature = "new_parser")]
    pub(crate) fn table_name(&self) -> Option<&'a str> {
        self.nodes.first().and_then(|node| match node {
            Node::RangeVar(r) => Some(Table::from(r).name),
            _ => None,
        })
    }

    cfg_select! {
        not(feature = "new_parser") => {
            pub(crate) fn table_name(&self) -> Option<&'a str> {
                if let Some(node) = self.nodes.first()
                    && let Some(NodeEnum::RangeVar(ref range_var)) = node.node
                {
                    let table = Table::from(range_var);
                    return Some(table.name);
                }

                None
            }
        }
        _ => {}
    }
}
