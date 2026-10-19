//! Evaluates sub-queries by sending them separately and injecting into the top-level statement
//!
//! Note: This is still in development. Do not use in production.
//! Please do not work on without discussing with @jkaczman first.
use pg_raw_parse::{
    ConstValue, Node,
    make::{MemoryToken, Unique},
};
use pgdog_postgres_types::Datum;

// These are all internal modules; don't need to be exported out.
mod request;
mod where_clause;

use super::Error;
use crate::frontend::{
    ClientRequest,
    client::query_engine::{QueryEngine, QueryEngineContext},
    router::parser::rewrite::statement::insert::Subquery,
};

/// Evaluates the Subquery and injects the results into the top-level statement.
pub(crate) struct SubqueryMulti<'a> {
    pub(super) subquery: &'a Subquery,
    pub(super) engine: &'a mut QueryEngine,
}

impl<'a> SubqueryMulti<'a> {
    pub(crate) fn new(engine: &'a mut QueryEngine, subquery: &'a Subquery) -> Self {
        Self { subquery, engine }
    }

    /// Evaluates and re-writes a *singular* sub-query.
    pub(crate) async fn execute(
        &mut self,
        context: &mut QueryEngineContext<'_>,
        client_request: &ClientRequest,
    ) -> Result<(), Error> {
        // Send the subquery that we parsed through the QueryEngine, and fetch its response back
        let (data_rows, row_description) = self
            .send_query_string(
                client_request,
                self.subquery.inner_stmt.clone(),
                context,
                // 'is_subquery' will allow us to get the response back
                true,
            )
            .await?
            .expect("send_query_string is only None when `is_subquery` is false");

        // Re-write outer statement with the response
        let new_sql: Option<String> = {
            if let Some(ast) = client_request.ast.as_ref()
                && let Some(stmt) = ast.ast.stmts().next()
            {
                // SELECT / UPDATE / DELETE
                self.handle_where_clause(stmt, row_description, data_rows)?
            } else {
                None
            }
        };

        let Some(new_sql) = new_sql else {
            unreachable!(
                "Subqueries: the parser check for a re-writable subquery should match our logic here."
            )
        };

        // Send the re-written statement with the evaluated sub-select
        self.send_query_string(
            client_request,
            new_sql,
            context,
            // `is_subquery` = false will send the response right back to the client
            false,
        )
        .await?;

        Ok(())
    }

    /// Helper method to turn `Datum` (from DataRow response) into an A_Const node we can inject into the subquery
    fn convert_datum_to_a_const<'mem>(
        datum: Datum,
        mem: MemoryToken<'mem>,
    ) -> Unique<'mem, Node<'mem>> {
        match datum {
            Datum::Integer(num) => mem.make_a_const(ConstValue::Integer(num)).uncast(),
            Datum::Bigint(num) => mem.make_a_const(ConstValue::Integer(num as i32)).uncast(),
            Datum::Text(text) => mem.make_a_const(ConstValue::String(text.as_str())).uncast(),
            Datum::Null => mem.make_null().uncast(),
            _ => panic!("Subqueries: unsupported datum {datum:?}!"),
        }
    }
}
