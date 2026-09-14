use super::aggregate::HelperKind;

/// Aggregate function projected temporarily for cross-shard merging.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct AggregateHelper {
    pub(crate) target_column: usize,
    pub(crate) projected_column: usize,
    pub(crate) distinct: bool,
    pub(crate) kind: HelperKind,
    pub(crate) alias: String,
}

/// Column projected temporarily for cross-shard ordering.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct OrderByHelper {
    pub(crate) sort_position: usize,
    pub(crate) projected_column: usize,
}

/// Temporary result columns required while merging cross-shard results.
#[derive(Debug, Clone, Default, PartialEq)]
pub(crate) struct ProjectionRewritePlan {
    aggregate_helpers: Vec<AggregateHelper>,
    order_by_helpers: Vec<OrderByHelper>,
}

impl ProjectionRewritePlan {
    pub(crate) fn is_noop(&self) -> bool {
        self.aggregate_helpers.is_empty() && self.order_by_helpers.is_empty()
    }

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
    fn projection_plan_tracks_helpers() {
        let mut plan = ProjectionRewritePlan::default();
        plan.add_aggregate_helper(AggregateHelper {
            target_column: 0,
            projected_column: 1,
            distinct: false,
            kind: HelperKind::Count,
            alias: "__pgdog_count_col0".into(),
        });
        plan.add_order_by_helper(OrderByHelper {
            sort_position: 0,
            projected_column: 2,
        });

        assert!(!plan.is_noop());
        assert_eq!(plan.drop_columns().collect::<Vec<_>>(), [1, 2]);
        assert_eq!(plan.aggregate_helpers().len(), 1);
        assert_eq!(plan.order_by_helpers().len(), 1);
    }
}
