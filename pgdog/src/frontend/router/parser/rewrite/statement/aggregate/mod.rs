pub mod engine;
pub mod plan;

use super::{Error, RewritePlan, StatementRewrite};
use crate::backend::schema::Schema;
use crate::frontend::router::parser::aggregate::Aggregate;
use pg_query::NodeEnum;
#[cfg(feature = "new_parser")]
use pg_raw_parse::Node;

pub use engine::AggregatesRewrite;
pub use plan::{AggregateRewritePlan, HelperKind, HelperMapping, RewriteOutput};

impl StatementRewrite<'_> {
    /// Add missing COUNT(*) and other helps when using aggregates.
    pub(super) fn rewrite_aggregates(
        &mut self,
        plan: &mut RewritePlan,
        schema: &Schema,
    ) -> Result<(), Error> {
        if self.schema.shards == 1 {
            return Ok(());
        }

        let Some(raw_stmt) = self.stmt.stmts.first_mut() else {
            return Ok(());
        };

        let Some(stmt) = raw_stmt.stmt.as_mut() else {
            return Ok(());
        };

        let Some(NodeEnum::SelectStmt(select_old)) = stmt.node.as_mut() else {
            return Ok(());
        };

        #[cfg(not(feature = "new_parser"))]
        let select = select_old;

        #[cfg(feature = "new_parser")]
        let Some(Node::SelectStmt(select)) = self.new_stmt.stmts().next() else {
            return Ok(());
        };

        let aggregate = Aggregate::parse(select, schema);
        if aggregate.is_empty() {
            return Ok(());
        }

        let output = AggregatesRewrite.rewrite_select(select_old, &aggregate);
        if output.plan.is_noop() {
            return Ok(());
        }

        plan.aggregates = output.plan;
        self.rewritten = true;
        Ok(())
    }
}
