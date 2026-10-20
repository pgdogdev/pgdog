//! Plan out subqueries by parsing them and determining what kind they are, as well as
//! getting the relevant SQL for us to send in our multi-step code.
use pg_raw_parse::Node;

use crate::frontend::router::parser::{StatementRewrite, rewrite::statement::RewritePlan};

// Determine if there's a SubLink node in the WHERE clause of a SELECT / UPDATE / DELETE statement.
pub(crate) mod where_clause;

impl<'a> StatementRewrite<'a> {
    /// Handle planning Subqueries to later be executed and re-written by the multi step code.
    pub(crate) fn plan_subquery<'mem>(&mut self, outer_stmt: Node<'mem>, plan: &mut RewritePlan) {
        // Determine if there's a SubLink node in the WHERE clause of a SELECT / UPDATE / DELETE statement.
        if let Some((sub_link, _)) = where_clause::has_sub_link_in_where(outer_stmt) {
            self.handle_subquery_in_where(sub_link, plan);
            // TODO(jkaczman): when adding another type to support here, return early in this branch, so that we don't plan multiple times
        }
    }
}
