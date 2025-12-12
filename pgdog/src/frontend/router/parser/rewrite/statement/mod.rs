//! Statement rewriter.
//!

use pg_query::protobuf::ParseResult;

pub mod error;
pub mod plan;
pub mod select;
pub mod unique_id;

pub use error::Error;
pub use plan::RewritePlan;

#[derive(Debug)]
pub struct StatementRewrite<'a> {
    /// SQL statement.
    stmt: &'a mut ParseResult,
    /// The statement was rewritten.
    rewritten: bool,
    /// Statement is using the extended protocol, so
    /// we need to rewrite function calls with parameters
    /// and not actual values.
    extended: bool,
}

impl<'a> StatementRewrite<'a> {
    /// Create new statement rewriter.
    ///
    /// More often than not, it won't do anything.
    ///
    pub fn new(stmt: &'a mut ParseResult, extended: bool) -> Self {
        Self {
            stmt,
            rewritten: false,
            extended,
        }
    }

    /// Maybe rewrite the statement and produce a rewrite plan
    /// we can apply to Bind messages.
    pub fn maybe_rewrite(&'a mut self) -> Result<RewritePlan, Error> {
        let mut plan = RewritePlan::default();

        // TODO: implement rewrite engine.

        Ok(plan)
    }
}
