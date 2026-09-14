mod engine;

pub(crate) use super::projection::AggregateHelper;
use super::{Error, RewritePlan, StatementRewrite};
use crate::backend::schema::Schema;
use crate::frontend::router::parser::aggregate::Aggregate;
use pg_raw_parse::{make::MemoryToken, nodes::SelectStmtMut};

pub(crate) use engine::AggregatesRewrite;

/// Type of aggregate function added to the result set.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum HelperKind {
    Count,
    Sum,
    SumSquares,
}

impl HelperKind {
    pub(crate) fn alias_suffix(self) -> &'static str {
        match self {
            Self::Count => "count",
            Self::Sum => "sum",
            Self::SumSquares => "sumsq",
        }
    }
}

impl StatementRewrite<'_> {
    /// Add missing COUNT(*) and other helps when using aggregates.
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

        let aggregate = Aggregate::parse(select, schema);
        if aggregate.is_empty() {
            return Ok(());
        }

        let output = AggregatesRewrite::rewrite_select(select, mem, &aggregate);
        if output.plan.is_noop() {
            return Ok(());
        }

        plan.projection = output.plan;
        self.rewritten = true;
        Ok(())
    }
}
