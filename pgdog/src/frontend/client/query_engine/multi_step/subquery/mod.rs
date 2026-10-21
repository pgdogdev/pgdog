//! Evaluates sub-queries by sending them separately and injecting into the top-level statement
//!
//! Note: This is still in development. Do not use in production.
//! Please do not work on without discussing with @jkaczman first.
use pg_raw_parse::{
    ConstValue, Node,
    make::{MemoryToken, Unique},
};
use pgdog_postgres_types::Datum;
use rust_decimal::prelude::ToPrimitive;

// These are all internal modules; don't need to be exported out.
mod insert_select;
mod request;
mod where_clause;

use super::Error;
use crate::{
    frontend::{
        ClientRequest,
        client::query_engine::{QueryEngine, QueryEngineContext},
        router::parser::rewrite::statement::insert::Subquery,
    },
    net::{Bind, DataRow, RowDescription},
};

/// Evaluates the Subquery and injects the results into the top-level statement.
pub(crate) struct SubqueryMulti<'a> {
    pub(super) subquery: &'a Subquery,
    pub(super) engine: &'a mut QueryEngine,

    // Store the very outer statement's Bind message.
    // As in, if we're 3 levels deep, it'd be the 1st level's Bind.
    //
    // This allows us to find the relevant parameters for each subquery level
    // within the client's Bind message.
    pub(super) top_level_bind: Option<&'a Bind>,
}

impl<'a> SubqueryMulti<'a> {
    pub(crate) fn new(
        engine: &'a mut QueryEngine,
        subquery: &'a Subquery,
        top_level_bind: Option<&'a Bind>,
    ) -> Self {
        Self {
            subquery,
            engine,
            top_level_bind,
        }
    }

    /// Evaluates and re-writes a *singular* sub-query.
    ///
    /// Sends the re-written outer statement, and returns the response directly
    /// to the Client.
    pub(crate) async fn execute(
        &mut self,
        context: &mut QueryEngineContext<'_>,
        client_request: &ClientRequest,
    ) -> Result<(), Error> {
        // Send the inner-subquery and re-write the outer statement with its response.
        let rewritten_outer_statement: String = self
            .evaluate_and_rewrite_outer_statement(context, client_request)
            .await?;

        // Send the re-written statement
        self.send_query_string(
            client_request,
            rewritten_outer_statement,
            context,
            // `is_subquery` = false will send the response right back to the client
            false,
        )
        .await?;

        Ok(())
    }

    /// Evaluates and re-writes a *singular* sub-query
    ///
    /// Returns the SQL of the re-written outer statement.
    pub(crate) async fn evaluate_and_rewrite_outer_statement(
        &mut self,
        context: &mut QueryEngineContext<'_>,
        client_request: &ClientRequest,
    ) -> Result<String, Error> {
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
        let new_sql: Option<String> = self.rewrite_outer_statement_with_subquery_response(
            client_request,
            row_description.as_ref(),
            &data_rows,
        )?;

        let Some(new_sql) = new_sql else {
            unreachable!(
                "Subqueries: the parser check for a re-writable subquery should match our logic here."
            )
        };

        Ok(new_sql)
    }

    fn rewrite_outer_statement_with_subquery_response(
        &self,
        client_request: &ClientRequest,
        row_description: Option<&RowDescription>,
        data_rows: &Vec<DataRow>,
    ) -> Result<Option<String>, Error> {
        if let Some(ast) = client_request.ast.as_ref()
            && let Some(stmt) = ast.ast.stmts().next()
        {
            // SELECT / UPDATE / DELETE with a subquery in a WHERE clause.
            if let Some(sql) = self.handle_where_clause(stmt, row_description, data_rows)? {
                return Ok(Some(sql));
            }

            // Select subquery within an INSERT statement with a SELECT FROM
            // e.g., INSERT INTO .. SELECT FROM generate_series(..)
            if let Some(sql) = self.handle_insert_select(stmt, row_description, data_rows)? {
                return Ok(Some(sql));
            }
        }

        Ok(None)
    }

    /// Helper method to turn `Datum` (from DataRow response) into an A_Const node we can inject into the subquery
    fn convert_datum_to_a_const<'mem>(
        datum: Datum,
        mem: MemoryToken<'mem>,
    ) -> Unique<'mem, Node<'mem>> {
        match datum {
            Datum::Integer(num) => mem.make_a_const(ConstValue::Integer(num)).uncast(),
            Datum::Bigint(num) => match i32::try_from(num) {
                Ok(num) => mem.make_a_const(ConstValue::Integer(num)).uncast(),
                Err(_) => mem
                    .make_a_const(ConstValue::Float(num.to_string().as_str()))
                    .uncast(),
            },
            Datum::Text(text) => mem.make_a_const(ConstValue::String(text.as_str())).uncast(),
            Datum::Numeric(num) => match num.as_decimal() {
                Some(decimal) => match decimal.to_i64().filter(|_| decimal.is_integer()) {
                    Some(whole) => Self::convert_datum_to_a_const(Datum::Bigint(whole), mem),
                    None => mem
                        .make_a_const(ConstValue::Float(decimal.to_string().as_str()))
                        .uncast(),
                },
                None => mem
                    .make_a_const(ConstValue::String(num.to_string().as_str()))
                    .uncast(),
            },
            Datum::Null => mem.make_null().uncast(),
            _ => panic!("Subqueries: unsupported datum {datum:?}!"),
        }
    }
}
