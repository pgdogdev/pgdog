//! Sorting columns extracted from the query.

use std::fmt::Debug;

use pg_raw_parse::{ConstValue, Node, list::CastNodeList, nodes};

use crate::net::messages::Vector;

use super::{Column, StatementParameters, Value};

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum OrderBy {
    Asc(usize),
    Desc(usize),
    AscColumn(String),
    DescColumn(String),
    AscVectorL2Column(String, Vector),
    AscVectorL2(usize, Vector),
}

impl OrderBy {
    /// Extract result sort keys from the final, possibly rewritten ORDER BY.
    pub(crate) fn parse(
        sorts: &CastNodeList<nodes::SortBy>,
        params: Option<StatementParameters<'_>>,
    ) -> Vec<Self> {
        sorts
            .iter()
            .filter_map(|sort| {
                use pg_raw_parse::raw::SortByDir::*;

                let asc = matches!(sort.sortby_dir, SORTBY_DEFAULT | SORTBY_ASC);
                match sort.node() {
                    Node::A_Const(c) if let Some(ConstValue::Integer(i)) = c.val() => {
                        Some(if asc {
                            Self::Asc(i as _)
                        } else {
                            Self::Desc(i as _)
                        })
                    }
                    Node::ColumnRef(column) => {
                        let name = column.fields().iter().next_back()?.as_str()?;
                        Some(if asc {
                            Self::AscColumn(name.into())
                        } else {
                            Self::DescColumn(name.into())
                        })
                    }
                    node => Self::parse_vector(node, params),
                }
            })
            .collect()
    }

    /// Extract a vector distance for shard selection before projection rewriting.
    pub(crate) fn parse_vector(
        node: Node<'_>,
        params: Option<StatementParameters<'_>>,
    ) -> Option<Self> {
        let Node::A_Expr(expr) = node else {
            return None;
        };
        if expr.kind != nodes::A_Expr_Kind::AEXPR_OP
            || expr.name().iter().next().and_then(Node::as_str) != Some("<->")
        {
            return None;
        }

        let mut vector = None;
        let mut column = None;
        for node in [expr.lexpr(), expr.rexpr()] {
            if let Ok(value) = Value::try_from(node) {
                match value {
                    Value::Placeholder(p) => {
                        vector = params?.parameter((p - 1) as _).ok()??.vector();
                    }
                    Value::Vector(value) => vector = Some(value),
                    _ => {}
                }
            } else if let Ok(value) = Column::try_from(node) {
                column = Some(value.name);
            }
        }
        Some(Self::AscVectorL2Column(column?.into(), vector?))
    }

    /// ORDER BY x ASC
    pub(crate) fn asc(&self) -> bool {
        matches!(
            self,
            OrderBy::Asc(_)
                | OrderBy::AscColumn(_)
                | OrderBy::AscVectorL2Column(_, _)
                | OrderBy::AscVectorL2(_, _)
        )
    }

    /// Column index.
    pub(crate) fn index(&self) -> Option<usize> {
        match self {
            OrderBy::Asc(column) => Some(*column - 1),
            OrderBy::Desc(column) => Some(*column - 1),
            OrderBy::AscVectorL2(column, _) => Some(*column - 1),
            _ => None,
        }
    }

    /// ORDER BY clause contains a vector.
    pub(crate) fn vector(&self) -> Option<(&Vector, &String)> {
        match self {
            OrderBy::AscVectorL2Column(name, vector) => Some((vector, name)),
            _ => None,
        }
    }
}
