//! Cross-shard `HAVING` execution plan.

/// Metadata produced while rewriting a cross-shard `HAVING` clause.
///
/// Shards return every group with each `HAVING` operand projected under a
/// hidden alias. After the merge, PgDog sends those operands to Postgres as
/// arrays and `filter_sql` returns the 1-based positions of the groups that pass.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct HavingRewritePlan {
    filter_sql: String,
    operand_aliases: Vec<String>,
    hidden_aliases: Vec<String>,
}

impl HavingRewritePlan {
    pub(crate) fn new(
        filter_sql: String,
        operand_aliases: Vec<String>,
        hidden_aliases: Vec<String>,
    ) -> Self {
        Self {
            filter_sql,
            operand_aliases,
            hidden_aliases,
        }
    }

    /// Query over the merged operands, with `$1..$n` bound to one array per operand.
    #[allow(dead_code)] // Cross-shard HAVING is planned but not executed yet.
    pub(crate) fn filter_sql(&self) -> &str {
        &self.filter_sql
    }

    /// Hidden columns passed to [`Self::filter_sql`], in parameter order.
    #[allow(dead_code)] // Cross-shard HAVING is planned but not executed yet.
    pub(crate) fn operand_aliases(&self) -> &[String] {
        &self.operand_aliases
    }

    /// Every hidden column, removed before rows reach the client.
    pub(crate) fn hidden_aliases(&self) -> &[String] {
        &self.hidden_aliases
    }
}
