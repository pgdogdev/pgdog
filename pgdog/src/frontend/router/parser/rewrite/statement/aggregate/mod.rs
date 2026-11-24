mod engine;
mod sharding_key;

pub(crate) use super::projection::AggregateHelper;

pub(crate) use engine::AggregatesRewrite;
pub(crate) use sharding_key::{disjoint_distinct, groups_stay_on_one_shard};

/// Type of aggregate function added to the result set.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum HelperKind {
    /// `COUNT(*)` or `COUNT(column)`.
    Count,
    /// `SUM(column)`.
    Sum,
    /// `SUM(POWER(column, 2))`.
    SumSquares,
    /// `ARRAY_AGG(DISTINCT column)`.
    DistinctValues,
}

impl HelperKind {
    /// Suffix used in the projected helper's internal alias.
    pub(crate) fn alias_suffix(self) -> &'static str {
        match self {
            Self::Count => "count",
            Self::Sum => "sum",
            Self::SumSquares => "sumsq",
            Self::DistinctValues => "distinct",
        }
    }
}
