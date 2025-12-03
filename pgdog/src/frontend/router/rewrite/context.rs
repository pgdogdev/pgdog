//! Context passed throughout the rewrite engine.

use pg_query::protobuf::{ParseResult, RawStmt};

use super::{
    output::RewriteActionKind, stats::RewriteStats, Error, RewriteAction, RewritePlan, StepOutput,
};
use crate::net::{Parse, ProtocolMessage, Query};

#[derive(Debug, Clone)]
pub struct Context<'a> {
    // Most requeries won't require a rewrite.
    // This is a clone-free way to check.
    original: &'a ParseResult,
    // If an in-place rewrite was done, the statement is saved here.
    rewrite: Option<ParseResult>,
    /// Additional messages to add to the request.
    result: Vec<RewriteAction>,
    /// Extended protocol.
    parse: Option<&'a Parse>,
    /// Rewrite plan.
    plan: RewritePlan,
}

impl<'a> Context<'a> {
    /// Create new input.
    pub(super) fn new(original: &'a ParseResult, parse: Option<&'a Parse>) -> Self {
        Self {
            original,
            rewrite: None,
            result: vec![],
            parse,
            plan: RewritePlan::default(),
        }
    }

    /// Get Parse reference.
    pub fn parse(&'a self) -> Option<&'a Parse> {
        self.parse
    }

    /// We are rewriting an extended protocol request.
    pub fn extended(&self) -> bool {
        self.parse().is_some()
    }

    /// Get reference to rewrite plan for modification.
    pub fn plan(&mut self) -> &mut RewritePlan {
        &mut self.plan
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
            let ast = self.rewrite.take().ok_or(Error::NoRewrite)?;
            let stmt = ast.deparse()?;
            let mut parse = self.parse().cloned();

            let mut actions = self.result;

            if let Some(mut parse) = parse.take() {
                parse.set_query(&stmt);
                actions.push(RewriteAction {
                    message: parse.into(),
                    action: RewriteActionKind::Replace,
                });
                stats.parse += 1;
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
                plan: self.plan.freeze(),
            })
        }
    }
}
