#[cfg(not(feature = "new_parser"))]
use super::Error;
#[cfg(feature = "new_parser")]
use itertools::*;
#[cfg(not(feature = "new_parser"))]
use pg_query::{
    Node, NodeEnum,
    protobuf::{self, AConst, ColumnRef, Integer, SelectStmt, a_const::Val},
};
#[cfg(feature = "new_parser")]
use pg_raw_parse::{Node, nodes};

#[derive(Debug, PartialEq, Clone)]
pub(crate) enum DistinctColumn {
    Name(String),
    Index(usize),
}

#[derive(Debug, PartialEq, Clone)]
pub(crate) enum DistinctBy {
    Row,
    Columns(Vec<DistinctColumn>),
}

#[derive(Debug, Clone)]
pub(crate) struct Distinct<'a> {
    #[cfg(not(feature = "new_parser"))]
    stmt: &'a SelectStmt,
    #[cfg(feature = "new_parser")]
    stmt: &'a nodes::SelectStmt,
}

impl<'a> Distinct<'a> {
    #[cfg(feature = "new_parser")]
    pub(crate) fn new(stmt: &'a nodes::SelectStmt) -> Self {
        Self { stmt }
    }

    cfg_select! {
        not(feature = "new_parser") => {
            pub(crate) fn new(stmt: &'a SelectStmt) -> Self {
                Self { stmt }
            }
        }
        _ => {}
    }

    #[cfg(feature = "new_parser")]
    pub(crate) fn distinct(&self) -> Option<DistinctBy> {
        match self.stmt.distinct_clause().first() {
            Some(Node::None) => return Some(DistinctBy::Row),
            None => return None,
            _ => (),
        }

        let columns = self
            .stmt
            .distinct_clause()
            .iter()
            .filter_map(|node| match node {
                Node::A_Const(c) => Some(DistinctColumn::Index(
                    c.val()?.numeric_value::<i32>()? as usize - 1,
                )),
                Node::ColumnRef(c) => Some(DistinctColumn::Name(
                    c.fields()
                        .iter()
                        .exactly_one()
                        .ok()?
                        .as_str()
                        .expect("DISTINCT ON (*) is a parse error")
                        .to_owned(),
                )),
                // FIXME: We should return an error to the client name if they
                // sent a form we don't support and the query is routed
                // cross-shard
                _ => None,
            })
            .collect();

        Some(DistinctBy::Columns(columns))
    }

    cfg_select! {
        not(feature = "new_parser") => {
            pub fn distinct(&self) -> Result<Option<DistinctBy>, Error> {
                match self.stmt.distinct_clause.first() {
                    Some(Node { node: None }) => return Ok(Some(DistinctBy::Row)),
                    None => return Ok(None),
                    _ => (),
                }

                let mut columns = vec![];

                for node in &self.stmt.distinct_clause {
                    if let Node { node: Some(node) } = node {
                        match node {
                            NodeEnum::AConst(AConst {
                                val: Some(Val::Ival(Integer { ival })),
                                ..
                            }) => columns.push(DistinctColumn::Index(*ival as usize - 1)),
                            NodeEnum::ColumnRef(ColumnRef { fields, .. }) => {
                                if let Some(Node {
                                    node: Some(NodeEnum::String(protobuf::String { sval })),
                                }) = fields.first()
                                {
                                    columns.push(DistinctColumn::Name(sval.to_string()));
                                }
                            }

                            _ => (),
                        }
                    }
                }

                Ok(Some(DistinctBy::Columns(columns)))
            }
        }
        _ => {}
    }
}
