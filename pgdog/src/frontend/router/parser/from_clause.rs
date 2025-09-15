use pg_query::{Node, NodeEnum};

use super::*;

#[derive(Copy, Clone)]
pub struct FromClause<'a> {
    nodes: &'a [Node],
}

impl<'a> FromClause<'a> {
    pub fn new(nodes: &'a [Node]) -> Self {
        Self { nodes }
    }

    pub fn resolve_alias(&'a self, name: &str) -> Option<&'a str> {
        for node in self.nodes {
            if let Some(ref node) = node.node {
                match node {
                    NodeEnum::JoinExpr(ref join) => {
                        for arg in [&join.larg, &join.rarg].into_iter().flatten() {
                            if let Some(ref node) = arg.node {
                                if let NodeEnum::RangeVar(range_var) = node {
                                    let table = Table::from(range_var);
                                    if table.name_match(name) {
                                        return Some(table.name);
                                    }
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
            }
        }

        None
    }

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
