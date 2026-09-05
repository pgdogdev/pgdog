use pg_raw_parse::{Node, nodes};
use pgdog_config::RewriteMode;

use super::{Error, RewritePlan, StatementRewrite};

impl StatementRewrite<'_> {
    /// Check to see if we should try to use [`QueryPlannerType::MultiInsert`]
    pub(super) fn split_insert(
        &mut self,
        insert: &nodes::InsertStmt,
        plan: &mut RewritePlan,
    ) -> Result<(), Error> {
        // Don't rewrite INSERTs in unsharded databases.
        if self.schema.shards == 1 || self.schema.rewrite.split_inserts != RewriteMode::Rewrite {
            return Ok(());
        }

        if let Node::SelectStmt(select) = insert.select_stmt() {
            let count = select.values_lists().len();
            if count >= 2 {
                plan.insert_split = true;
            }
        }

        Ok(())
    }
}
