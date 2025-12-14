pub mod engine;
pub mod plan;

use super::{Error, StatementRewrite};
pub use engine::RewriteEngine;
pub use plan::{AggregateRewritePlan, HelperKind, HelperMapping, RewriteOutput};

impl StatementRewrite<'_> {
    pub(super) fn rewrite_aggregates(&mut self) -> Result<(), Error> {
        Ok(())
    }
}
