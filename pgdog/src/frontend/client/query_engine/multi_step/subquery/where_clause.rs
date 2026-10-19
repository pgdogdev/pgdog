//! Handles re-writing the WHERE clause of a Select / Update / Delete statement when a subquery is present.

use std::ops::Deref;

use pg_raw_parse::{
    ConstValue, Node, NodeMut, deparse,
    make::{MemoryToken, Unique, owned},
    nodes::SubLink,
    raw::SubLinkType,
    transform,
};
use pgdog_postgres_types::Datum;
use tracing::debug;

use crate::{
    frontend::{
        client::query_engine::multi_step::{Error, SubqueryMulti},
        router::parser::{Error as ParserError, rewrite},
    },
    net::{DataRow, Decoder, RowDescription},
};

impl<'a> SubqueryMulti<'a> {
    pub(crate) fn handle_where_clause(
        &self,
        statement: Node<'a>,
        row_description: Option<&RowDescription>,
        data_rows: &Vec<DataRow>,
    ) -> Result<Option<String>, Error> {
        let Some((sublink, lexpr)) =
            rewrite::statement::subquery::where_clause::has_sub_link_in_where(statement)
        else {
            return Ok(None);
        };
        let new_stmt = owned(|mem| {
            let owned_stmt = mem.make_unique(statement);
            let mut owned_raw_stmt = mem.make_raw_stmt(owned_stmt);
            let mut mutable_raw_stmt = owned_raw_stmt.as_mut();

            // Fetch the where clause.
            let mut where_clause = mem.make_unique(match mutable_raw_stmt.stmt() {
                Node::SelectStmt(stmt) => stmt.where_clause(),
                Node::UpdateStmt(stmt) => stmt.where_clause(),
                Node::DeleteStmt(stmt) => stmt.where_clause(),
                _ => unreachable!("Parser should prevent other statements from reaching here."),
            });

            // Replace the SubLink node present with our own re-written version.
            transform::transform(&mut where_clause, |node| match &*node {
                NodeMut::SubLink(_) => {
                    let sublink_replacement = self.rewrite_sublink_in_where_clause(
                        sublink,
                        data_rows,
                        row_description.as_ref().unwrap(),
                        mem,
                        lexpr,
                    );

                    node.replace(sublink_replacement);

                    // We shouldn't recurse here. Even if we have nested subqueries,
                    // that'll be handled later through routing through the QueryEngine again.
                    None
                }
                _ => Some(node),
            });

            // Put back the modified where clause.
            match mutable_raw_stmt.stmt_mut() {
                NodeMut::SelectStmt(mut stmt) => stmt.set_where_clause(where_clause),
                NodeMut::UpdateStmt(mut stmt) => stmt.set_where_clause(where_clause),
                NodeMut::DeleteStmt(mut stmt) => stmt.set_where_clause(where_clause),
                _ => unreachable!("Parser should prevent other statements from reaching here."),
            };

            owned_raw_stmt
        });

        let new_sql: String = {
            let new_insert_sql = deparse(new_stmt.deref()).map_err(ParserError::from)?;
            let new_insert_sql = new_insert_sql.as_str();

            new_insert_sql.to_string()
        };

        Ok(Some(new_sql))
    }

    fn rewrite_sublink_in_where_clause<'mem>(
        &self,
        sublink: &SubLink,
        data_rows: &Vec<DataRow>,
        row_description: &RowDescription,
        mem: MemoryToken<'mem>,
        lexpr: Node<'_>,
    ) -> Unique<'mem, Node<'_>> {
        let mut decoder = Decoder::default();
        decoder.set_row_description(row_description.clone());

        // Aggregate all DataRows into a list of A_Const nodes; relevant
        // for cases where we have something like IN (1, 2, ...)
        //
        // TODO(jkaczman): it would be better to explicitly state the SubLinkTypes this is relevant for
        // to clearly separate the code (includie this strictly for the relevant branches)
        let mut elems: Vec<Unique<'mem, Node>> = Vec::new();
        for row in data_rows {
            let datum: Datum = row
                .get_column(0, &decoder)
                .expect("get_column never returns an Error")
                .unwrap()
                .value;

            elems.push(Self::convert_datum_to_a_const(datum, mem).uncast());
        }

        if sublink.sub_link_type == SubLinkType::ANY_SUBLINK && data_rows.is_empty() {
            elems.push(mem.make_null().uncast());
        } else if sublink.sub_link_type == SubLinkType::EXISTS_SUBLINK {
            // If there's an EXISTS clause within the WHERE, we can replace the
            // ENTIRE subquery with the boolean result
            return mem
                .make_a_const(ConstValue::Boolean(!data_rows.is_empty()))
                .uncast();
        } else if sublink.sub_link_type == SubLinkType::EXPR_SUBLINK {
            if elems.is_empty() {
                return mem.make_null().uncast();
            } else if elems.len() == 1 {
                return elems.remove(0).uncast();
            }

            // We only expect at MOST a singular element to be returned when we have an "=" expression
            // e.g., SELECT WHERE id = <X>
            //
            // Postgres will actually error if more than one is returned within a subquery.
            // However, it doesn't know that there was a subquery, as we're executing it separately.
            //
            // TODO(jkaczman): This should emit a friendly client-facing error, matching Postgres.
            //       "21000, more than one row returned by a subquery used as an expression"
            unreachable!("Subqueries: EXPR_SUBLINK should only return a singular element");
        } else {
            // TODO(jkaczman): Likely, not all cases are handled here. We should make sure that every single case
            // in the parser is handled with **match** statements.
            debug!(
                "Subqueries: handling other sublink type: {}",
                sublink.sub_link_type
            );
        }

        mem.make_a_expr(
            6,
            mem.make_list(&[mem.make_string(Some("=")).uncast()]),
            mem.make_unique(lexpr).uncast(),
            mem.make_list(elems.as_ref()).uncast(),
        )
        .uncast()
    }
}
