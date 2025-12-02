//! Context passed throughout the rewrite engine.

use pg_query::protobuf::{ParseResult, RawStmt};

use super::{output::RewriteActionKind, stats::RewriteStats, Error, RewriteAction, StepOutput};
use crate::net::{Bind, Parse, ProtocolMessage, Query};

#[derive(Debug, Clone)]
pub struct Context<'a> {
    // Most requeries won't require a rewrite.
    // This is a clone-free way to check.
    original: &'a ParseResult,
    // If an in-place rewrite was done, the statement is saved here.
    rewrite: Option<ParseResult>,
    /// Original bind message, if any.
    bind: Option<&'a Bind>,
    /// Bind rewritten.
    rewrite_bind: Option<Bind>,
    /// Additional messages to add to the request.
    result: Vec<RewriteAction>,
    /// Extended protocol.
    parse: Option<&'a Parse>,
}

impl<'a> Context<'a> {
    /// Create new input.
    pub(super) fn new(
        original: &'a ParseResult,
        bind: Option<&'a Bind>,
        parse: Option<&'a Parse>,
    ) -> Self {
        Self {
            original,
            bind,
            rewrite: None,
            rewrite_bind: None,
            result: vec![],
            parse,
        }
    }

    /// Get Parse reference.
    pub fn parse(&'a self) -> Option<&'a Parse> {
        self.parse
    }

    /// We are rewriting an extended protocol request.
    pub fn extended(&self) -> bool {
        self.parse().is_some() || self.bind().is_some()
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

    /// Put the bind message back.
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

    /// Get protocol version from the original statement.
    pub fn proto_version(&self) -> i32 {
        self.original.version
    }

    /// Get the parse result (original or rewritten).
    pub fn parse_result(&self) -> &ParseResult {
        self.rewrite.as_ref().unwrap_or(self.original)
    }

    /// Prepend new message to rewritten request.
    pub fn prepend(&mut self, message: ProtocolMessage) {
        self.result.push(RewriteAction {
            message,
            action: RewriteActionKind::Prepend,
        });
    }

    /// Assemble rewrite instructions.
    pub fn build(mut self) -> Result<StepOutput, Error> {
        if self.rewrite.is_none() {
            Ok(StepOutput::NoOp)
        } else {
            let mut stats = RewriteStats::default();
            let bind = self.rewrite_bind.take();
            let ast = self.rewrite.take().ok_or(Error::NoRewrite)?;
            let stmt = ast.deparse()?;
            let extended = self.extended();
            let mut parse = self.parse().cloned();

            let mut actions = self.result;

            if extended {
                if let Some(mut parse) = parse.take() {
                    parse.set_query(&stmt);
                    actions.push(RewriteAction {
                        message: parse.into(),
                        action: RewriteActionKind::Replace,
                    });
                    stats.parse += 1;
                }

                if let Some(bind) = bind {
                    actions.push(RewriteAction {
                        message: bind.into(),
                        action: RewriteActionKind::Replace,
                    });
                    stats.bind += 1;
                }
            } else {
                actions.push(RewriteAction {
                    message: Query::new(stmt.clone()).into(),
                    action: RewriteActionKind::Replace,
                });
                stats.simple += 1;
            }

            Ok(StepOutput::RewriteInPlace {
                stmt,
                ast,
                actions,
                stats,
            })
        }
    }
}
