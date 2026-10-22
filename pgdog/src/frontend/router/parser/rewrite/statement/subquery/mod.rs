//! Plan out subqueries by parsing them and determining what kind they are, as well as
//! getting the relevant SQL for us to send in our multi-step code.
use std::cell::OnceCell;

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
        let mut subquery_rewrite = SubqueryRewrite::new(outer_stmt, self);

        subquery_rewrite.plan_subquery()
    }
}

/// Handles parsing and planning subquery re-writes.
struct SubqueryRewrite<'a, 'mem> {
    // This is the outer statement's shard w/o factoring in any subselects / etc,
    // so that we can determine if the sharding key is present without recursing into subqueries.
    //
    // Compute outer statement Shard when needed.
    // After computed, store the outer statement's Shard for re-use.
    outer_shard: OnceCell<Option<Shard>>,

    // Store the outer statement's AST.
    outer_stmt: Node<'mem>,

    // Store the calling StatementRewrite, so that we can reference internal fields when calling the `StatementParser`
    statement_rewrite: &'a StatementRewrite<'a>,
}

impl<'a, 'mem> SubqueryRewrite<'a, 'mem> {
    fn new(outer_stmt: Node<'mem>, statement_rewrite: &'a StatementRewrite<'a>) -> Self {
        Self {
            outer_shard: OnceCell::new(),
            outer_stmt,
            statement_rewrite,
        }
    }

    fn plan_subquery(&mut self) -> Result<Option<Subquery>, Error> {
        // Determine if there's a SubLink node in the WHERE clause of a SELECT / UPDATE / DELETE statement.
        if let Some((sub_link, _)) = where_clause::has_sub_link_in_where(self.outer_stmt) {
            return self.handle_subquery_in_where(sub_link);
        }

        // Determine if there's a SELECT FROM in an INSERT.
        // E.g., INSERT INTO .. SELECT FROM generate_series(..)
        if let Some(subquery) = self.handle_subquery_in_insert_select() {
            return Ok(Some(subquery));
        }

        Ok(None)
    }

    /// Returns the outer statement's Shard.
    /// If previously computed, immediately returns.
    /// If not previously computed, uses the `StatementParser` to compute, and then store it.
    pub(crate) fn get_outer_statement_shard(&mut self) -> Option<&Shard> {
        self.outer_shard
            .get_or_init(|| {
                let mut parser = StatementParser::new_that_ignore_subqueries(
                    self.outer_stmt,
                    None,
                    self.statement_rewrite.schema,
                );
                parser.shard().ok().flatten()
            })
            .as_ref()
    }
}
