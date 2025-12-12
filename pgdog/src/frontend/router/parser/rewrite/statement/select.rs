use pg_query::Node;

use super::{Error, RewritePlan, StatementRewrite};

impl StatementRewrite<'_> {
    /// Entrypoint for rewriting SELECT queries.
    pub(super) fn rewrite_select(node: &mut Node, plan: &mut RewritePlan) -> Result<(), Error> {
        todo!()
    }
}
