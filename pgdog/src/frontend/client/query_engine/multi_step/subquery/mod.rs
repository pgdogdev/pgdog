//! Evaluates sub-queries by sending them separately and injecting into the top-level statement
//!
//! Note: This is still in development. Do not use in production.
//! Please do not work on without discussing with @jkaczman first.

use pg_raw_parse::{
    Node, Owned, deparse,
    nodes::{RawStmt, SelectStmt},
};

// These are all internal modules; don't need to be exported out.
mod clause;
mod evaluate;
mod request;
mod rewrite;

use super::Error;
use crate::{
    frontend::{
        ClientRequest,
        client::query_engine::{QueryEngine, QueryEngineContext, multi_step::error::SubqueryError},
        router::parser::rewrite::statement::insert::Subquery,
    },
    net::{Bind, DataRow, Format, ProtocolMessage, RowDescription},
};

/// Evaluates the Subquery and injects the results into the top-level statement.
pub(crate) struct SubqueryMulti<'ast> {
    pub(super) subqueries: Vec<Subquery>,

    // Store the very outer statement's Bind message.
    // As in, if we're 3 levels deep, it'd be the 1st level's Bind.
    //
    // This allows us to find the relevant parameters for each subquery level
    // within the client's Bind message.
    pub(super) top_level_bind: Option<&'ast Bind>,

    // Start off with the very outer statement's Bind message.
    // Add our own parameters as we need to inject DataRows into the statement as
    // unnest(..)
    pub(super) current_level_bind: Bind,

    // Store the results here, so that can reference previously executed subqueries (e.g., if we need to
    // use earlier resolved CTEs to resolve a CTE occurring after those)
    pub(super) subquery_results: Vec<(Vec<DataRow>, Option<RowDescription>)>,

    // Was the original statement from the Client sent from simple or extended protocol?
    // This determines how we need to send the response back.
    pub(super) is_extended_protocol: bool,

    // The Ast of the top-level statement for us to reference for subquery planning
    pub(super) top_level_statement: Node<'ast>,
}

impl<'ast> SubqueryMulti<'ast> {
    pub(crate) fn new(subqueries: Vec<Subquery>, client_request: &'ast ClientRequest) -> Self {
        let ast = client_request
            .ast
            .as_ref()
            .expect("The parser had to have been on to reach this code branch");

        let top_level_statement: Node<'ast> = ast
            .ast
            .stmts()
            .next()
            .expect("The parser had to have been on to reach this code branch");

        // TODO(jkaczman): There's likely a different way for us to determine if this is extended protocol or not,
        // not to mention... this is a redundant loop (btwn. is_extended_protocol and top_level_bind)
        let is_extended_protocol = client_request
            .messages
            .iter()
            .find(|message| {
                matches!(
                    message,
                    ProtocolMessage::Parse(_) | ProtocolMessage::Bind(_)
                )
            })
            .is_some();

        let top_level_bind: Option<&Bind> = if is_extended_protocol {
            client_request.messages.iter().find_map(|message| {
                if let ProtocolMessage::Bind(bind) = message {
                    Some(bind)
                } else {
                    None
                }
            })
        } else {
            None
        };

        let current_level_bind = top_level_bind.cloned().unwrap_or(Bind::default());

        Self {
            subqueries,
            top_level_bind,
            current_level_bind,
            subquery_results: Vec::new(),
            is_extended_protocol,
            top_level_statement,
        }
    }

    pub fn new_sub_engine<'b>(
        parent_engine: &'b SubqueryMulti<'ast>,
        new_parent_ast: Node<'ast>,
        subqueries: Vec<Subquery>,
    ) -> Self {
        Self {
            subqueries,
            top_level_bind: parent_engine.top_level_bind,
            current_level_bind: parent_engine.current_level_bind.clone(),
            subquery_results: Vec::new(),
            is_extended_protocol: parent_engine.is_extended_protocol,
            top_level_statement: new_parent_ast,
        }
    }

    /// Evaluates and re-writes a *singular* sub-query.
    ///
    /// Sends the re-written outer statement, and returns the response directly
    /// to the Client.
    pub(crate) async fn execute(
        &mut self,
        engine: &mut QueryEngine,
        context: &mut QueryEngineContext<'_>,
    ) -> Result<(), Error> {
        // Send the inner-subquery and re-write the outer statement with its response.
        let (new_ast, new_bind): (Owned<RawStmt>, Bind) = self
            .evaluate_and_rewrite_outer_statement(engine, context)
            .await?;

        self.current_level_bind = new_bind;

        let rewritten_outer_statement = deparse(new_ast.stmt())?.as_str().to_string();

        // Send the re-written statement
        self.send_query_string(
            engine,
            self.top_level_statement,
            rewritten_outer_statement.to_string(),
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
        engine: &mut QueryEngine,
        context: &mut QueryEngineContext<'_>,
    ) -> Result<(Owned<RawStmt>, Bind), Error> {
        // TODO(jkaczman): we could execute some of these in parallel (in groups) e.g., if CTEs are not referencing earlier ones

        let current_statement: Node<'ast> = self.top_level_statement;

        let subqueries = self.subqueries.clone();
        for subquery in &subqueries {
            // Send the subquery that we parsed through the QueryEngine, and fetch its response back

            let (rewritten_subquery_ast, subquery_bind): (Owned<SelectStmt>, Bind) =
                self.new_select_to_fetch_rows_of_subquery(subquery, current_statement)?;

            let subquery_sql = deparse(&*rewritten_subquery_ast)?.as_str().to_string();

            let current_level_bind = std::mem::replace(&mut self.current_level_bind, subquery_bind);

            let results = self
                .send_query_string(
                    engine,
                    Node::SelectStmt(&rewritten_subquery_ast),
                    subquery_sql.to_string(),
                    context,
                    // 'is_subquery' will allow us to get the response back
                    true,
                )
                .await;

            self.current_level_bind = current_level_bind;

            let results =
                results?.expect("send_query_string is only None when `is_subquery` is false");

            // We send the rows of subqueries as unnest(..) arrays, so we check the column types before rewriting to ensure that all columns have array types.
            // EXISTS is the only exception to this, since we only read whether or not the subquery returned rows (exists just needs 1 row to be replaced w/ TRUE)
            if !self.is_exists_in_where(subquery)
                && let (_, Some(row_description)) = &results
                && row_description
                    .fields
                    .iter()
                    .any(|field| field.data_type().into_array_type().is_none())
            {
                return Err(Error::Subquery(
                    SubqueryError::SubqueryReturningUnsupportedColumn,
                ));
            }

            // INVARIANT: the rows that subqueries return are in binary format
            // `form_client_request` sets the result format of subqueries to binary
            // `new_select_with_unnest` relies on this; it copies the bytes of the values into binary arrays without decoding the values
            if let Some(row_description) = &results.1 {
                assert!(
                    row_description
                        .fields
                        .iter()
                        .all(|field| field.format() == Format::Binary),
                );
            }

            self.subquery_results.push(results);
        }

        if self.subquery_results.is_empty() {
            unreachable!(
                "Subqueries: the parser check for a re-writable subquery should match our logic here."
            )
        }

        // Re-write outer statement with the response
        let (new_ast, new_bind): (Owned<RawStmt>, Bind) =
            self.rewrite_outer_statement_with_subquery_response();

        Ok((new_ast, new_bind))
    }
}
