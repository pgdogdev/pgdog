mod engine;
mod plan;

use super::{Error, RewritePlan, StatementRewrite};
use crate::backend::schema::Schema;
use crate::frontend::router::parser::aggregate::Aggregate;
#[cfg(not(feature = "new_parser"))]
use pg_query::NodeEnum;
#[cfg(feature = "new_parser")]
use pg_raw_parse::{make::MemoryToken, nodes::SelectStmtMut};

pub(crate) use engine::AggregatesRewrite;
pub(crate) use plan::{AggregateRewritePlan, HelperKind, HelperMapping, RewriteOutput};

impl StatementRewrite<'_> {
    /// Add missing COUNT(*) and other helps when using aggregates.
    #[cfg(feature = "new_parser")]
    pub(super) fn rewrite_aggregates<'a>(
        &mut self,
        select: &mut SelectStmtMut<'a, '_>,
        mem: MemoryToken<'a>,
        plan: &mut RewritePlan,
        schema: &Schema,
    ) -> Result<(), Error> {
        if self.schema.shards == 1 {
            return Ok(());
        }

        let aggregate = Aggregate::parse(&select, schema);
        if aggregate.is_empty() {
            return Ok(());
        }

        let output = AggregatesRewrite::rewrite_select(select, mem, &aggregate);
        if output.plan.is_noop() {
            return Ok(());
        }

        plan.aggregates = output.plan;
        self.rewritten = true;
        Ok(())
    }

    #[cfg(not(feature = "new_parser"))]
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

        let Some(NodeEnum::SelectStmt(select)) = stmt.node.as_mut() else {
            return Ok(());
        };

        let aggregate = Aggregate::parse(select, schema);
        if aggregate.is_empty() {
            return Ok(());
        }

        let output = AggregatesRewrite.rewrite_select(select, &aggregate);
        if output.plan.is_noop() {
            return Ok(());
        }

        plan.aggregates = output.plan;
        self.rewritten = true;
        Ok(())
    }
}
