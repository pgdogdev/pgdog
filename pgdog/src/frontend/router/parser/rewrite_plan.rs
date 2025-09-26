#[derive(Debug, Clone, PartialEq)]
pub struct HelperMapping {
    pub avg_column: usize,
    pub helper_column: usize,
    pub expr_id: usize,
    pub distinct: bool,
}

/// Plan describing how the proxy rewrites a query and its results.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct RewritePlan {
    /// Column indexes (0-based) to drop from the row description/results after execution.
    drop_columns: Vec<usize>,
    helpers: Vec<HelperMapping>,
}

impl RewritePlan {
    pub fn new() -> Self {
        Self {
            drop_columns: Vec::new(),
            helpers: Vec::new(),
        }
    }

    pub fn is_noop(&self) -> bool {
        self.drop_columns.is_empty() && self.helpers.is_empty()
    }

    pub fn drop_columns(&self) -> &[usize] {
        &self.drop_columns
    }

    pub fn add_drop_column(&mut self, column: usize) {
        if !self.drop_columns.contains(&column) {
            self.drop_columns.push(column);
        }
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
    pub sql: String,
    pub plan: RewritePlan,
}

impl RewriteOutput {
    pub fn new(sql: String, plan: RewritePlan) -> Self {
        Self { sql, plan }
    }
}

pub trait QueryRewriter {
    fn rewrite(&self, sql: &str, route: &crate::frontend::router::Route) -> RewriteOutput;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rewrite_plan_noop() {
        let plan = RewritePlan::new();
        assert!(plan.is_noop());
        assert!(plan.drop_columns().is_empty());
        assert!(plan.helpers().is_empty());
    }

    #[test]
    fn rewrite_plan_drop_columns() {
        let mut plan = RewritePlan::new();
        plan.add_drop_column(1);
        plan.add_drop_column(4);
        assert_eq!(plan.drop_columns(), &[1, 4]);
    }

    #[test]
    fn rewrite_plan_helpers() {
        let mut plan = RewritePlan::new();
        plan.add_helper(HelperMapping {
            avg_column: 0,
            helper_column: 1,
            expr_id: 7,
            distinct: false,
        });
        assert_eq!(plan.helpers().len(), 1);
        let helper = &plan.helpers()[0];
        assert_eq!(helper.avg_column, 0);
        assert_eq!(helper.helper_column, 1);
        assert_eq!(helper.expr_id, 7);
        assert!(!helper.distinct);
    }

    #[test]
    fn rewrite_output_defaults() {
        let output = RewriteOutput::default();
        assert!(output.plan.is_noop());
        assert!(output.sql.is_empty());
    }
}
