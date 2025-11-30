//! Rewrite input and output types.

use pg_query::protobuf::{ParseResult, RawStmt};

use super::{output::RewriteActionKind, Error, RewriteAction, StepOutput};
use crate::{
    frontend::PreparedStatements,
    net::{Bind, Parse, ProtocolMessage, Query},
};

#[derive(Debug, Clone)]
pub struct Context<'a> {
    // Most requeries won't require a rewrite.
    // This is a clone-free way to check.
    original: &'a ParseResult,
    // If a rewrite was done, the statement is saved here.
    rewrite: Option<ParseResult>,
    /// Original bind message, if any.
    bind: Option<&'a Bind>,
    /// Bind rewritten.
    rewrite_bind: Option<Bind>,
    /// Additional messages to add to the request.
    result: Vec<RewriteAction>,
}

impl<'a> Context<'a> {
    /// Create new input.
    pub fn new(original: &'a ParseResult, bind: Option<&'a Bind>) -> Self {
        Self {
            original,
            bind,
            rewrite: None,
            rewrite_bind: None,
            result: vec![],
        }
    }

    /// Get the Bind message, if set.
    pub fn bind(&'a self) -> Option<&'a Bind> {
        if let Some(ref rewrite_bind) = self.rewrite_bind {
            Some(rewrite_bind)
        } else {
            self.bind
        }
    }

    /// Take the Bind message for modification.
    /// Don't forget to return it.
    #[must_use]
    pub fn bind_take(&mut self) -> Option<Bind> {
        if self.rewrite_bind.is_none() {
            self.rewrite_bind = self.bind.cloned();
        }

        self.rewrite_bind.take()
    }

    pub fn bind_put(&mut self, bind: Option<Bind>) {
        self.rewrite_bind = bind;
    }

    /// Get the original (or modified) statement.
    pub fn stmt(&'a self) -> Result<&'a RawStmt, Error> {
        let stmt = if let Some(ref rewrite) = self.rewrite {
            rewrite
        } else {
            self.original
        };
        let root = stmt.stmts.first().ok_or(Error::EmptyQuery)?;
        Ok(root)
    }

    /// Get the mutable statement we're rewriting.
    pub fn stmt_mut(&mut self) -> Result<&mut RawStmt, Error> {
        let stmt = if let Some(ref mut rewrite) = self.rewrite {
            rewrite
        } else {
            self.rewrite = Some(self.original.clone());
            self.rewrite.as_mut().unwrap()
        };

        stmt.stmts.first_mut().ok_or(Error::EmptyQuery)
    }

    /// New request mutable reference.
    pub fn prepend(&mut self, message: ProtocolMessage) {
        self.result.push(RewriteAction {
            message,
            action: RewriteActionKind::Prepend,
        });
    }

    /// Assemble statement and add it to the global prepared statements cache.
    pub fn build(mut self) -> Result<StepOutput, Error> {
        if self.rewrite.is_none() {
            Ok(StepOutput::NoOp)
        } else {
            let bind = self.rewrite_bind.take();
            let stmt = self.rewrite.take().ok_or(Error::NoRewrite)?.deparse()?;
            let mut result = self.result;

            if let Some(mut bind) = bind {
                let mut parse = Parse::new_anonymous(stmt);
                if bind.anonymous() {
                    result.push(RewriteAction {
                        message: parse.into(),
                        action: RewriteActionKind::Replace,
                    });
                    result.push(RewriteAction {
                        message: bind.into(),
                        action: RewriteActionKind::Replace,
                    });
                } else {
                    let name = PreparedStatements::cache_rewritten(&parse);
                    parse.rename_fast(&name);
                    bind.rename(name);
                    result.push(RewriteAction {
                        message: parse.into(),
                        action: RewriteActionKind::Replace,
                    });
                    result.push(RewriteAction {
                        message: bind.into(),
                        action: RewriteActionKind::Replace,
                    });
                }
            } else {
                result.push(RewriteAction {
                    message: Query::new(stmt).into(),
                    action: RewriteActionKind::Replace,
                });
            }

            Ok(StepOutput::Rewrite(result))
        }
    }
}
