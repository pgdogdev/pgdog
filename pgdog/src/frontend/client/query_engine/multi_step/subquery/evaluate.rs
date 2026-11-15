//! Builds the SELECT that we send to the shards to evaluate a single `Subquery` (the first half of `evaluate_and_rewrite_outer_statement`)
//!
//! E.g., for SELECT * FROM orders WHERE user_id IN (SELECT id FROM users WHERE status = 'VIP'),
//! the `Subquery` is the SELECT inside IN (..), so we build
//! - SELECT id FROM users WHERE status = 'VIP'

use pg_raw_parse::{
    Node, Owned,
    make::{Unique, owned},
    nodes::{CommonTableExpr, SelectStmt},
};

use crate::{
    frontend::{
        client::query_engine::multi_step::{Error, SubqueryMulti},
        router::parser::rewrite::statement::{
            insert::Subquery,
            subquery::{
                from_clause::parse_from_clause, insert_select::parse_insert_select,
                where_clause::parse_where_clause, with_clause::parse_with_clause,
            },
        },
    },
    net::Bind,
};

impl<'ast> SubqueryMulti<'ast> {
    /// Returns the SELECT that we send to the shards to fetch the rows of `subquery` (+ the Bind for that SELECT)
    /// E.g., for .. WHERE user_id IN (SELECT id FROM users), we return SELECT id FROM users.
    pub(super) fn new_select_to_fetch_rows_of_subquery(
        &self,
        subquery: &Subquery,
        entire_ast: Node<'ast>,
    ) -> Result<(Owned<SelectStmt>, Bind), Error> {
        let subquery_ast: Node<'ast> = self.parse_out_relevant_query_to_send(subquery, entire_ast);

        // We might have to reference earlier statements and perform a re-write. E.g., if we have CTE b referencing CTE a, then we need to include the re-written CTE (w/ unnest) here
        Ok(self.rewrite_subquery_ast_with_earlier_subquery_responses(subquery, subquery_ast))
    }

    /// Returns the SELECT of `subquery` inside `entire_ast`. E.g.,
    /// - for `Subquery::WhereClause` in .. WHERE id IN (SELECT id FROM users), we return SELECT id FROM users
    /// - for `Subquery::Cte("a")` in WITH a AS (SELECT id FROM users) SELECT .., we return SELECT id FROM users
    fn parse_out_relevant_query_to_send(
        &self,
        subquery: &Subquery,
        entire_ast: Node<'ast>,
    ) -> Node<'ast> {
        match &subquery {
            Subquery::FromClause => {
                parse_from_clause(entire_ast)
            }
            Subquery::WhereClause => {
                parse_where_clause(entire_ast)
            }
            Subquery::Cte(alias) => {
                parse_with_clause(entire_ast, alias)
            }
            Subquery::InsertSelect => {
                parse_insert_select(entire_ast)
            }
            Subquery::WhereClauseCte(alias) => {
                parse_where_clause(entire_ast).and_then(|subselect| parse_with_clause(subselect, alias))
            }
        }.expect("We already ran these methods in the parser originally. No reason they shouldn't pass here.")
    }

    /// Adds the rows of the CTEs that we fetched before to `subquery_ast`, allowing `subquery_ast` to read those CTEs. E.g., for
    /// - WITH a AS (SELECT id FROM users), b AS (SELECT id FROM a) SELECT ..
    ///
    /// the SELECT of CTE b reads CTE a. After we fetched CTE a, we return this to evaluate CTE b
    /// - WITH a(id) AS (SELECT u.c1 FROM unnest($1::pg_catalog._int8) u(c1)) SELECT id FROM a
    ///
    /// with the rows of a as $1 in the returned Bind
    fn rewrite_subquery_ast_with_earlier_subquery_responses(
        &self,
        subquery: &Subquery,
        subquery_ast: Node<'ast>,
    ) -> (Owned<SelectStmt>, Bind) {
        let Node::SelectStmt(select) = subquery_ast else {
            unreachable!("We only allow subselects to be subqueries");
        };

        let mut subquery_bind = self.current_level_bind.clone();

        let rewritten_subquery_ast = owned(|mem| {
            let mut subquery_ast = mem.make_unique(select);

            // For INSERT .. SELECT, the WITH clause is attached to the INSERT
            // --> e.g., WITH a AS (..) INSERT INTO orders SELECT .. FROM a
            //
            // However, We send the SELECT without the INSERT (to eval), so to properly
            // eval the SELECT, we copy the WITH clause to the SELECT (as SELECT .. FROM a would fail without a)
            if matches!(subquery, Subquery::InsertSelect)
                && let Node::InsertStmt(insert) = self.top_level_statement
                && let Some(with_clause) = insert.with_clause()
            {
                subquery_ast
                    .as_mut()
                    .set_with_clause(mem.make_unique(with_clause).as_option());
            }

            let mut previously_executed_ctes: Vec<Unique<&CommonTableExpr>> = Vec::new();

            for (i, (rows, row_description)) in self.subquery_results.iter().enumerate() {
                let subquery = self
                    .subqueries
                    .get(i)
                    .expect("Subquery 'i' must have existed to create the matching result");

                match &subquery {
                    Subquery::Cte(alias) | Subquery::WhereClauseCte(alias) => {
                        let row_description = row_description
                            .as_ref()
                            .expect("RowDescription must be Some(..) in order to reach here");

                        // We need to set this within a WITH clause of the subselect,
                        // so that the subselect can reference previously ran CTEs
                        let new_cte_subselect = Self::new_select_with_unnest(
                            &mut subquery_bind,
                            mem,
                            Node::None,
                            rows,
                            row_description,
                        );

                        // Need to form the alias for the CTE to be referenced within the other CTE,
                        // so that it can reference <CTE>.<col>
                        let mut alias_col_names = Vec::new();
                        for i in 0..row_description.len() {
                            alias_col_names.push(
                                mem.make_string(Some(
                                    row_description.fields.get(i).unwrap().name.as_str(),
                                ))
                                .uncast(),
                            );
                        }

                        let new_cte = mem.make_common_table_expr(
                            alias,
                            mem.make_list(alias_col_names.as_slice()),
                            new_cte_subselect.uncast(),
                        );

                        previously_executed_ctes.push(new_cte);
                    }

                    // For now, we'll only consider re-writing if it was previously a CTE.
                    _ => continue,
                }
            }

            if !previously_executed_ctes.is_empty() {
                let new_with_clause =
                    mem.make_with_clause(mem.make_list(previously_executed_ctes.as_slice()), false);

                subquery_ast
                    .as_mut()
                    .set_with_clause(new_with_clause.as_option());
            }

            subquery_ast
        });

        (rewritten_subquery_ast, subquery_bind)
    }
}
