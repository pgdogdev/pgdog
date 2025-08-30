use pg_query::{
    protobuf::{a_const::Val, AConst, Integer, ParamRef, SelectStmt},
    Node, NodeEnum,
};

use super::Error;
use crate::net::Bind;

#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct Limit {
    pub limit: Option<usize>,
    pub offset: Option<usize>,
}

#[derive(Debug, Clone)]
pub struct LimitClause<'a> {
    stmt: &'a SelectStmt,
    bind: Option<&'a Bind>,
}

impl<'a> LimitClause<'a> {
    pub(crate) fn new(stmt: &'a SelectStmt, bind: Option<&'a Bind>) -> Self {
        Self { stmt, bind }
    }

    pub(crate) fn limit_offset(&self) -> Result<Limit, Error> {
        let mut limit = Limit::default();
        if let Some(ref limit_count) = self.stmt.limit_count {
            limit.limit = self.decode(limit_count)?;
        }

        if let Some(ref limit_offset) = self.stmt.limit_offset {
            limit.offset = self.decode(limit_offset)?;
        }

        Ok(limit)
    }

    fn decode(&self, node: &Node) -> Result<Option<usize>, Error> {
        match &node.node {
            Some(NodeEnum::AConst(AConst {
                val: Some(Val::Ival(Integer { ival })),
                ..
            })) => Ok(Some(*ival as usize)),

            Some(NodeEnum::ParamRef(ParamRef { number, .. })) => {
                if let Some(bind) = &self.bind {
                    let param = bind
                        .parameter(*number as usize - 1)?
                        .ok_or(Error::MissingParameter(*number as usize))?;

                    Ok(Some(
                        param
                            .bigint()
                            .ok_or(Error::MissingParameter(*number as usize))?
                            as usize,
                    ))
                } else {
                    Ok(None)
                }
            }

            _ => Ok(None),
        }
    }
}
