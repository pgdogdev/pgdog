mod engine;

pub(crate) use super::projection::AggregateHelper;

pub(crate) use engine::AggregatesRewrite;

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
