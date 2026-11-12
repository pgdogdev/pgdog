//! Evaluates sub-queries by sending them separately and injecting into the top-level statement
//!
//! Note: This is still in development. Do not use in production.
//! Please do not work on without discussing with @jkaczman first.

use pg_raw_parse::{
    ConstValue, Node, Owned, deparse,
    make::{MemoryToken, Unique, owned},
    nodes::{CommonTableExpr, RawStmt, SelectStmt},
};
use pgdog_postgres_types::Datum;
use rust_decimal::prelude::ToPrimitive;

// These are all internal modules; don't need to be exported out.
mod from_clause;
mod insert_select;
mod request;
mod where_clause;
mod with_clause;

use super::Error;
use crate::{
    frontend::{
        ClientRequest,
        client::query_engine::{QueryEngine, QueryEngineContext, multi_step::error::SubqueryError},
        router::parser::rewrite::statement::{
            insert::Subquery,
            subquery::{
                from_clause::parse_from_clause, insert_select::parse_insert_select,
                where_clause::parse_where_clause, with_clause::parse_with_clause,
            },
        },
    },
    net::{Bind, DataRow, ProtocolMessage, RowDescription},
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
                self.parse_and_rewrite_subquery_ast(subquery, current_statement)?;

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
            if !matches!(subquery, Subquery::WhereClause)
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

            self.subquery_results.push(results);
        }

        if self.subquery_results.is_empty() {
            unreachable!(
                "Subqueries: the parser check for a re-writable subquery should match our logic here."
            )
        }

        // Re-write outer statement with the response
        let (new_ast, new_bind): (Owned<RawStmt>, Bind) =
            self.rewrite_outer_statement_with_subquery_response()?;

        Ok((new_ast, new_bind))
    }

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

    fn parse_and_rewrite_subquery_ast(
        &self,
        subquery: &Subquery,
        entire_ast: Node<'ast>,
    ) -> Result<(Owned<SelectStmt>, Bind), Error> {
        let subquery_ast: Node<'ast> = self.parse_out_relevant_query_to_send(subquery, entire_ast);

        // We might have to reference earlier statements and perform a re-write. E.g., if we have CTE b referencing CTE a, then we need to include the re-written CTE (w/ unnest) here
        // Only if it's actually relevant, but we need to either (a) figure that out somehow or (b) always include earlier ones

        Ok(self.rewrite_subquery_ast_with_earlier_subquery_responses(subquery, subquery_ast))
    }

    /// This is the relevant part of what we need to send, e.g., the subselect of a specific CTE.
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

    fn rewrite_outer_statement_with_subquery_response(
        &self,
    ) -> Result<(Owned<RawStmt>, Bind), Error> {
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

        Ok((new_statement, new_bind))
    }

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
