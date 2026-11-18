//! Rewrites the outer statement with the rows that the subqueries returned (the second half of `evaluate_and_rewrite_outer_statement`)
//!
//! E.g., for SELECT * FROM orders WHERE user_id IN (SELECT id FROM users WHERE status = 'VIP'), we rewrite the statement to
//! - SELECT * FROM orders WHERE user_id IN (SELECT u.c1 FROM unnest($1::pg_catalog._int8) u(c1))
//!
//! with the ids that the subquery returned as $1

use pg_raw_parse::{
    Node, Owned,
    make::{MemoryToken, Unique, owned},
    nodes::RawStmt,
};

use crate::{
    frontend::{
        client::query_engine::multi_step::SubqueryMulti,
        router::parser::rewrite::statement::insert::Subquery,
    },
    net::{Bind, DataRow, RowDescription},
};

impl<'ast> SubqueryMulti<'ast> {
    /// Returns the outer statement (& Bind for that statement) with the rows of all subqueries in place of the subqueries
    pub(super) fn rewrite_outer_statement_with_subquery_response(&self) -> (Owned<RawStmt>, Bind) {
        // TODO(jkaczman): if we're always passing these three, might as well make a new type or
        // hold these in the existing one
        let mut new_bind = self.current_level_bind.clone();

        let new_statement: Owned<RawStmt> = owned(|mem| {
            let mut statement: Unique<'_, Node<'_>> = mem.make_unique(self.top_level_statement);

            // TODO(jkaczman): We'll either have to save the AST of the re-written node that we're
            // constantly rewriting in this outer loop, or we can save it in the struct itself
            // for easy reference.. however that'd mean we need a mutable reference to the struct,
            // could be tricky when we try to parallelize this.
            for (subquery, (data_rows, row_description)) in
                self.subqueries.iter().zip(self.subquery_results.iter())
            {
                statement = self.rewrite_with_subquery_response(
                    mem,
                    statement,
                    subquery,
                    row_description.as_ref(),
                    data_rows.as_slice(),
                    &mut new_bind,
                );
            }

            // We also need to capture Bind parameters from this

            mem.make_raw_stmt(statement)
        });

        (new_statement, new_bind)
    }

    /// Puts the rows of `subquery` (`data_rows`) in place of `subquery` in `stmt`.
    fn rewrite_with_subquery_response<'mem>(
        &self,
        mem: MemoryToken<'mem>,
        stmt: Unique<'mem, Node<'mem>>,
        subquery: &Subquery,
        row_description: Option<&RowDescription>,
        data_rows: &[DataRow],
        new_bind: &mut Bind,
    ) -> Unique<'mem, Node<'mem>> {
        match subquery {
            Subquery::FromClause => {
                self.handle_from_clause(mem, stmt, row_description, data_rows, new_bind)
            }
            Subquery::WhereClause => {
                self.handle_where_clause(mem, stmt, row_description, data_rows, new_bind)
            }
            Subquery::InsertSelect => {
                self.handle_insert_select(mem, stmt, row_description, data_rows, new_bind)
            }
            Subquery::WhereClauseCte(alias) => {
                self.handle_where_clause_cte(alias, mem, stmt, row_description, data_rows, new_bind)
            }
            Subquery::Cte(alias) => {
                self.handle_with_clause(alias, mem, stmt, row_description, data_rows, new_bind)
            }
        }
    }
}
