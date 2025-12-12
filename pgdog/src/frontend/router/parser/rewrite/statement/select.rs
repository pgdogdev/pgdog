use pg_query::Node;

use super::{Error, RewritePlan, StatementRewrite};

impl StatementRewrite<'_> {
    /// Entrypoint for rewriting SELECT queries.
    #[allow(dead_code)]
    pub(super) fn rewrite_select(_node: &mut Node, _plan: &mut RewritePlan) -> Result<(), Error> {
        todo!()
    }
}
