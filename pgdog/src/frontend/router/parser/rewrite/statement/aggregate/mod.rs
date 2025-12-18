pub mod engine;
pub mod plan;

use super::{Error, RewritePlan, StatementRewrite};
use crate::frontend::router::parser::aggregate::Aggregate;
use pg_query::NodeEnum;

pub use engine::AggregatesRewrite;
pub use plan::{AggregateRewritePlan, HelperKind, HelperMapping, RewriteOutput};

impl StatementRewrite<'_> {
    /// Add missing COUNT(*) and other helps when using aggregates.
    pub(super) fn rewrite_aggregates(&mut self, plan: &mut RewritePlan) -> Result<(), Error> {
        if self.schema.shards == 1 {
            return Ok(());
        }

        let Some(raw_stmt) = self.stmt.stmts.first() else {
            return Ok(());
        };

        let Some(stmt) = raw_stmt.stmt.as_ref() else {
            return Ok(());
        };

        let Some(NodeEnum::SelectStmt(select)) = stmt.node.as_ref() else {
            return Ok(());
        };

        let aggregate = Aggregate::parse(select);
        if aggregate.is_empty() {
            return Ok(());
        }

        let output = AggregatesRewrite.rewrite_select(self.stmt, &aggregate);
        if output.plan.is_noop() {
            return Ok(());
        }

        plan.aggregates = output.plan;
        self.rewritten = true;
        Ok(())
    }
}
