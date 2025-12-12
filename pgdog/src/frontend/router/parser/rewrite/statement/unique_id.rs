use pg_query::Node;

use super::{Error, RewritePlan, StatementRewrite};

impl StatementRewrite<'_> {
    /// Replace a function call to pgdog.unique_id() with either
    /// a parameter (e.g., $1::bigint) or a bigint value (e.g., '234'::bigint).
    pub(super) fn replace_unique_id(node: &mut Node, plan: &mut RewritePlan) -> Result<(), Error> {
        todo!()
    }
}
