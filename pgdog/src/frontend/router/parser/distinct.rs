use pg_raw_parse::{Node, nodes};

/// How a SELECT removes duplicate rows.
#[derive(Debug, PartialEq, Clone, Copy)]
pub(crate) enum DistinctBy {
    /// `DISTINCT`: rows equal in every column.
    Row,
    /// `DISTINCT ON (...)`: rows with the same key. Cross-shard, the
    /// projection rewrite records which result columns hold the key.
    On,
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
            None => None,
            Some(Node::None) => Some(DistinctBy::Row),
            Some(_) => Some(DistinctBy::On),
        }
    }
}
