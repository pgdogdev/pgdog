//! Prepared statement rewriter.
//!
//! Rewrites PREPARE and EXECUTE statements to use globally cached names.

mod execute;
mod prepare;

pub use execute::ExecuteRewrite;
pub use prepare::PrepareRewrite;

use super::{Context, Error, RewriteModule};
use crate::frontend::PreparedStatements;

/// Combined rewriter for PREPARE and EXECUTE statements.
pub struct PreparedRewrite<'a> {
    prepared_statements: &'a mut PreparedStatements,
}

impl<'a> PreparedRewrite<'a> {
    pub fn new(prepared_statements: &'a mut PreparedStatements) -> Self {
        Self {
            prepared_statements,
        }
    }
}

impl RewriteModule for PreparedRewrite<'_> {
    fn rewrite(&mut self, input: &mut Context<'_>) -> Result<(), Error> {
        PrepareRewrite::new(self.prepared_statements).rewrite(input)?;
        ExecuteRewrite::new(self.prepared_statements).rewrite(input)?;
        Ok(())
    }
}
