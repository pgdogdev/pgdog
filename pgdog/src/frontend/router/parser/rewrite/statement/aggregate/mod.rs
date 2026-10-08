mod engine;

pub(crate) use super::projection::AggregateHelper;

pub(crate) use engine::AggregatesRewrite;

/// Type of aggregate function added to the result set.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum HelperKind {
    /// `COUNT(*)` or `COUNT(column)`.
    Count,
    /// `SUM(column)`.
    Sum,
    /// `SUM(POWER(column, 2))`.
    SumSquares,
}
