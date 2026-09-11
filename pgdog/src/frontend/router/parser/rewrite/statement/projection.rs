/// Type of aggregate function added to the result set.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum HelperKind {
    /// COUNT(*) or COUNT(name)
    Count,
    /// SUM(column)
    Sum,
    /// SUM(POWER(column, 2))
    SumSquares,
}

impl HelperKind {
    /// Suffix for the aggregate function.
    pub(crate) fn alias_suffix(&self) -> &'static str {
        match self {
            HelperKind::Count => "count",
            HelperKind::Sum => "sum",
            HelperKind::SumSquares => "sumsq",
        }
    }
}

/// Context on the aggregate function column added to the result set.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct AggregateHelper {
    pub(crate) target_column: usize,
    pub(crate) projected_column: usize,
    pub(crate) distinct: bool,
    pub(crate) kind: HelperKind,
    pub(crate) alias: String,
}

/// Column temporarily projected so PgDog can globally order shard results.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct OrderByHelper {
    /// Position of the expression in the ORDER BY clause.
    pub(crate) sort_position: usize,
    /// Position of the temporary expression in the backend result.
    pub(crate) projected_column: usize,
}

/// Plan for temporary columns added to a query's projection.
#[derive(Debug, Clone, Default, PartialEq)]
pub(crate) struct ProjectionRewritePlan {
    aggregate_helpers: Vec<AggregateHelper>,
    order_by_helpers: Vec<OrderByHelper>,
}

impl ProjectionRewritePlan {
    /// Create a no-op projection rewrite plan.
    pub(crate) fn new() -> Self {
        Self {
            aggregate_helpers: Vec::new(),
            order_by_helpers: Vec::new(),
        }
    }

    /// Whether the projection and its result require no changes.
    pub(crate) fn is_noop(&self) -> bool {
        self.aggregate_helpers.is_empty() && self.order_by_helpers.is_empty()
    }

    /// Temporary result columns to remove before forwarding to the client.
    pub(crate) fn drop_columns(&self) -> impl Iterator<Item = usize> + '_ {
        self.aggregate_helpers
            .iter()
            .map(|helper| helper.projected_column)
            .chain(
                self.order_by_helpers
                    .iter()
                    .map(|helper| helper.projected_column),
            )
    }

    pub(crate) fn aggregate_helpers(&self) -> &[AggregateHelper] {
        &self.aggregate_helpers
    }

    pub(crate) fn order_by_helpers(&self) -> &[OrderByHelper] {
        &self.order_by_helpers
    }

    pub(crate) fn add_aggregate_helper(&mut self, helper: AggregateHelper) {
        self.aggregate_helpers.push(helper);
    }

    pub(crate) fn add_order_by_helper(&mut self, helper: OrderByHelper) {
        self.order_by_helpers.push(helper);
    }
}

#[derive(Debug, Default, Clone)]
pub(crate) struct RewriteOutput {
    pub(crate) plan: ProjectionRewritePlan,
}

impl RewriteOutput {
    pub(crate) fn new(plan: ProjectionRewritePlan) -> Self {
        Self { plan }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rewrite_plan_noop() {
        let plan = ProjectionRewritePlan::new();
        assert!(plan.is_noop());
        assert!(plan.drop_columns().count() == 0);
        assert!(plan.aggregate_helpers().is_empty());
    }

    #[test]
    fn rewrite_plan_helpers() {
        let mut plan = ProjectionRewritePlan::new();
        plan.add_aggregate_helper(AggregateHelper {
            target_column: 0,
            projected_column: 1,
            distinct: false,
            kind: HelperKind::Count,
            alias: "__pgdog_count_expr7_col0".into(),
        });
        assert_eq!(plan.aggregate_helpers().len(), 1);
        let helper = &plan.aggregate_helpers()[0];
        assert_eq!(helper.target_column, 0);
        assert_eq!(helper.projected_column, 1);
        assert!(!helper.distinct);
        assert!(matches!(helper.kind, HelperKind::Count));
        assert_eq!(helper.alias, "__pgdog_count_expr7_col0");
    }

    #[test]
    fn rewrite_output_defaults() {
        let output = RewriteOutput::default();
        assert!(output.plan.is_noop());
    }
}
