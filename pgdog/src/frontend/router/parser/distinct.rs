use super::Error;
use pg_query::{
    protobuf::{self, a_const::Val, AConst, ColumnRef, Integer, SelectStmt},
    Node, NodeEnum,
};

#[derive(Debug, PartialEq, Clone)]
pub enum DistinctColumn {
    Name(String),
    Index(usize),
}

#[derive(Debug, PartialEq, Clone)]
pub enum DistinctBy {
    Row,
    Columns(Vec<DistinctColumn>),
}

#[derive(Debug, Clone)]
pub struct Distinct<'a> {
    stmt: &'a SelectStmt,
}

impl<'a> Distinct<'a> {
    pub fn new(stmt: &'a SelectStmt) -> Self {
        Self { stmt }
    }

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
                    NodeEnum::AConst(AConst { val: Some(val), .. }) => match val {
                        Val::Ival(Integer { ival }) => {
                            columns.push(DistinctColumn::Index(*ival as usize - 1))
                        }
                        _ => (),
                    },
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
