use itertools::*;
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
    stmt: &'a nodes::SelectStmt,
}

impl<'a> Distinct<'a> {
    pub(crate) fn new(stmt: &'a nodes::SelectStmt) -> Self {
        Self { stmt }
    }

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
}
