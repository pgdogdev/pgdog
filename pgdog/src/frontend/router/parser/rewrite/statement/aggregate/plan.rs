/// Type of aggregate function added to the result set.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HelperKind {
    /// COUNT(*) or COUNT(name)
    Count,
    /// SUM(column)
    Sum,
    /// SUM(POWER(column, 2))
    SumSquares,
}

impl HelperKind {
    /// Suffix for the aggregate function.
    pub fn alias_suffix(&self) -> &'static str {
        match self {
            HelperKind::Count => "count",
            HelperKind::Sum => "sum",
            HelperKind::SumSquares => "sumsq",
        }
    }
}

/// Context on the aggregate function column added to the result set.
#[derive(Debug, Clone, PartialEq)]
pub struct HelperMapping {
    pub target_column: usize,
    pub helper_column: usize,
    pub distinct: bool,
    pub kind: HelperKind,
    pub alias: String,
}

/// Plan describing how the proxy rewrites a query and its results.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct AggregateRewritePlan {
    helpers: Vec<HelperMapping>,
}

impl AggregateRewritePlan {
    /// Create new no-op aggregate rewrite plan.
    pub fn new() -> Self {
        Self {
            helpers: Vec::new(),
        }
    }

    /// Is this plan a no-op? Doesn't do anything.
    pub fn is_noop(&self) -> bool {
        self.helpers.is_empty()
    }

    pub fn drop_columns(&self) -> impl Iterator<Item = usize> + '_ {
        self.helpers.iter().map(|h| h.helper_column)
    }

    pub fn helpers(&self) -> &[HelperMapping] {
        &self.helpers
    }

    pub fn add_helper(&mut self, mapping: HelperMapping) {
        self.helpers.push(mapping);
    }
}

#[derive(Debug, Default, Clone)]
pub struct RewriteOutput {
    pub plan: AggregateRewritePlan,
}

impl RewriteOutput {
    pub fn new(plan: AggregateRewritePlan) -> Self {
        Self { plan }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rewrite_plan_noop() {
        let plan = AggregateRewritePlan::new();
        assert!(plan.is_noop());
        assert!(plan.drop_columns().count() == 0);
        assert!(plan.helpers().is_empty());
    }

    #[test]
    fn rewrite_plan_helpers() {
        let mut plan = AggregateRewritePlan::new();
        plan.add_helper(HelperMapping {
            target_column: 0,
            helper_column: 1,
            distinct: false,
            kind: HelperKind::Count,
            alias: "__pgdog_count_expr7_col0".into(),
        });
        assert_eq!(plan.helpers().len(), 1);
        let helper = &plan.helpers()[0];
        assert_eq!(helper.target_column, 0);
        assert_eq!(helper.helper_column, 1);
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
