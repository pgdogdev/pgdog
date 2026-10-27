//! Cross-shard `HAVING` execution plan.

/// Comparison operators supported by the cross-shard `HAVING` evaluator.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CompareOperator {
    Equal,
    NotEqual,
    Less,
    LessOrEqual,
    Greater,
    GreaterOrEqual,
}

/// A value projected by the cross-shard query under an internal alias.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct HavingValue {
    alias: String,
}

impl HavingValue {
    pub(crate) fn new(alias: String) -> Self {
        Self { alias }
    }

    pub(crate) fn alias(&self) -> &str {
        &self.alias
    }
}

/// Predicate evaluated after partial aggregate results have been merged.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum HavingExpr {
    Compare {
        left: HavingValue,
        operator: CompareOperator,
        right: HavingValue,
    },
}

/// Metadata produced while rewriting a cross-shard `HAVING` clause.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct HavingRewritePlan {
    expression: HavingExpr,
    hidden_aliases: Vec<String>,
}

impl HavingRewritePlan {
    pub(crate) fn new(expression: HavingExpr, hidden_aliases: Vec<String>) -> Self {
        Self {
            expression,
            hidden_aliases,
        }
    }

    pub(crate) fn expression(&self) -> &HavingExpr {
        &self.expression
    }

    pub(crate) fn hidden_aliases(&self) -> &[String] {
        &self.hidden_aliases
    }
}
