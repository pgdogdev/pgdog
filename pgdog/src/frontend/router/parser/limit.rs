use pg_raw_parse::{
    Node,
    nodes::{self, SelectStmt},
};

use super::Error;
use crate::net::Bind;

#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub(crate) struct Limit {
    pub(crate) limit: Option<usize>,
    pub(crate) offset: Option<usize>,
}

#[derive(Debug, Clone)]
pub(crate) struct LimitClause<'a> {
    stmt: &'a SelectStmt,
    bind: Option<&'a Bind>,
}

impl<'a> LimitClause<'a> {
    pub(crate) fn new(stmt: &'a SelectStmt, bind: Option<&'a Bind>) -> Self {
        Self { stmt, bind }
    }

    pub(crate) fn limit_offset(&self) -> Result<Limit, Error> {
        Ok(Limit {
            limit: self.decode(self.stmt.limit_count())?,
            offset: self.decode(self.stmt.limit_offset())?,
        })
    }

    fn decode(&self, node: Node<'_>) -> Result<Option<usize>, Error> {
        use pg_raw_parse::ConstValue;

        match node {
            Node::A_Const(c) if let Some(ConstValue::Integer(i)) = c.val() => Ok(Some(i as usize)),
            Node::A_Const(c) if c.val().is_none() => Ok(None),
            Node::ParamRef(nodes::ParamRef { number, .. }) => {
                let Some(bind) = self.bind else {
                    // FIXME: This is a Parse with no Bind. We don't need to
                    // be rewriting the query at all if we have no Bind, and
                    // should be able to treat this as an error condition
                    return Ok(None);
                };
                let param = bind
                    .parameter(*number as usize - 1)?
                    .ok_or(Error::MissingParameter(*number as usize))?;

                if param.is_null() {
                    Ok(None)
                } else {
                    match param.bigint() {
                        Some(param) => Ok(Some(param as usize)),
                        None => Err(Error::ParameterNotInteger(
                            *number as usize,
                            param.text_debug(),
                        )),
                    }
                }
            }
            _ => Ok(None), // FIXME: We should error and not silently treat as NULL
        }
    }
}
