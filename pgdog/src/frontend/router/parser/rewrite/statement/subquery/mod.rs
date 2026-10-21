//! Plan out subqueries by parsing them and determining what kind they are, as well as
//! getting the relevant SQL for us to send in our multi-step code.
use pg_raw_parse::Node;

use crate::frontend::router::parser::{
    Shard, StatementParser, StatementRewrite,
    rewrite::statement::{Error, insert::Subquery},
};

// Determine if there's a SELECT FROM in an INSERT.
// E.g., INSERT INTO .. SELECT FROM generate_series(..)
pub(crate) mod insert_select;

// Determine if there's a SubLink node in the WHERE clause of a SELECT / UPDATE / DELETE statement.
pub(crate) mod where_clause;

impl<'a> StatementRewrite<'a> {
    /// Handle planning Subqueries to later be executed and re-written by the multi step code.
    pub(crate) fn plan_subquery<'mem>(
        &mut self,
        outer_stmt: Node<'mem>,
    ) -> Result<Option<Subquery>, Error> {
        let outer_statement_shard: Option<Shard> = {
            let mut parser =
                StatementParser::new_that_ignore_subqueries(outer_stmt, None, self.schema);

            parser.shard().map_err(|_| Error::OuterQuerySharding)?
        };

        // Determine if there's a SubLink node in the WHERE clause of a SELECT / UPDATE / DELETE statement.
        if let Some((sub_link, _)) = where_clause::has_sub_link_in_where(outer_stmt) {
            return Ok(self.handle_subquery_in_where(outer_statement_shard, sub_link));
        }

        // Determine if there's a SELECT FROM in an INSERT.
        // E.g., INSERT INTO .. SELECT FROM generate_series(..)
        if let Some(subquery) = self.handle_subquery_in_insert_select(outer_stmt) {
            return Ok(Some(subquery));
        }

        Ok(None)
    }
}
