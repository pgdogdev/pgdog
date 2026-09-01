use crate::backend::ShardingSchema;
use crate::frontend::ClientRequest;
use crate::frontend::client::query_engine::multi_step::error::{Error, UpdateError};
use crate::frontend::client::query_engine::multi_step::types::{
    ForwardToClient, QueryPlanner, ResponseHistory, StatementRequest, StatementSource, Step,
    StepProtocol, StepRequest,
};
use crate::frontend::client::query_engine::{QueryEngine, QueryEngineContext};
use crate::frontend::router::parser::rewrite::statement::Error as RewriteError;
use crate::frontend::router::parser::{Column, Table, Value};
use crate::frontend::router::sharding::ShardedTable;
use crate::frontend::router::{Ast, Route};
use crate::net::bind::Parameter;
use crate::net::{
    Bind, CommandComplete, DataRow, Message, Parse, Protocol, ReadyForQuery, RowDescription,
};
use indexmap::IndexSet;
use itertools::Itertools;
use pg_raw_parse::make::{owned, try_owned};
use pg_raw_parse::nodes::ResTarget;
use pg_raw_parse::{Node, deparse, nodes};
use pgdog_config::RewriteMode;
use pgdog_postgres_types::Format;
use std::collections::{HashMap, HashSet};
use std::fmt::Debug;

impl QueryPlanner {
    /// Try to create a `QueryPlanner` where we suspect a `ShardingKeyUpdate`
    pub(crate) async fn plan_sharding_key_update(
        request: &ClientRequest,
        engine: &mut QueryEngine,
        context: &mut QueryEngineContext<'_>,
        schema: ShardingSchema,
    ) -> Result<Option<QueryPlanner>, Error> {
        // Get the AST
        let Some(ref ast) = request.ast else {
            debug_assert!(false, "planner dispatched without an AST");
            return Err(UpdateError::PlanMismatch.into());
        };

        // Get the Update Statement node
        let Ok(Node::UpdateStmt(original_update_stmt)) = ast.ast.stmts().exactly_one() else {
            return Ok(None);
        };

        let table = original_update_stmt
            .relation()
            .map(Table::from)
            .expect("UPDATE always has a table");

        // Fetch the `ResTarget` for the original update statement, so that we can formulate
        // the INSERT `Step`
        let Some(new_target) = original_update_stmt.target_list().into_iter().find(|c| {
            Column::try_from(*c).is_ok_and(|mut c| {
                c.qualify(table);
                schema.tables().get_table(c).is_some()
            })
        }) else {
            return Ok(None);
        };

        let Some(new_route) =
            Self::on_same_shard(engine, new_target, original_update_stmt, request, context)?
        else {
            // CHECK: Are we on the same shard? If so, abort and use the original request.
            // This compares two SELECTs (old + new sharding key) through router.query
            return Ok(None);
        };

        if Self::has_destructive_on_delete_reference(engine, original_update_stmt, context)? {
            return Err(UpdateError::ForeignKeyOnDelete.into());
        }

        // Check if we are allowed to do this operation by the config.
        // Propagated as an error so execution stops
        if engine.backend.cluster()?.rewrite().shard_key == RewriteMode::Error {
            return Err(UpdateError::Disabled.into());
        }

        // Do this check at the last possible moment (in case transactions changed in future)
        // TODO: I think this is an approximation of the old execute-time check as this is planning-based;
        //       are we connected yet? Is this suitably tested by an integration test?
        if !context.in_transaction()
            || (engine.backend.connected() && !engine.backend.is_multishard())
        {
            engine.cleanup_backend(context)?;
            return Err(UpdateError::TransactionRequired.into());
        }

        // ****** We've completed all our pre-execution checks. ******

        let steps: Vec<Step> = vec![
            Self::construct_delete_step(engine, request, original_update_stmt, context)?,
            Self::construct_insert_step(request, ast, original_update_stmt, new_route)?,
        ];

        // After completion of all Steps, we return this:
        #[derive(Debug, Clone)]
        struct ShardingKeyUpdateResponse;
        impl ForwardToClient for ShardingKeyUpdateResponse {
            fn forward_to_client(
                &self,
                context: &QueryEngineContext,
                map: ResponseHistory,
            ) -> Vec<Message> {
                // The INSERT step's responses (RETURNING rows and protocol acks)
                // We don't worry about DELETE since it's an internal step.
                let mut messages = match map.get("insert") {
                    Some(insert) => ResponseHistory::compose(&[insert], context.client_request),
                    None => Vec::new(),
                };

                // Only allows update one row at a time
                // We use 0 when the DELETE matched nothing.
                let rows = map
                    .get("delete")
                    .map(|delete| delete.rows.len())
                    .unwrap_or_default();

                messages.push(CommandComplete::new(format!("UPDATE {}", rows)).message());
                messages.push(ReadyForQuery::in_transaction(context.in_transaction()).message());
                messages
            }
        }

        Ok(Some(QueryPlanner {
            steps,
            forward_to_client: Some(Box::new(ShardingKeyUpdateResponse {})),
        }))
    }

    /// Dependent on `construct_delete_step`
    fn construct_insert_step(
        request: &ClientRequest,
        ast: &Ast,
        original_update_stmt: &nodes::UpdateStmt,
        new_route: Route,
    ) -> Result<Step, Error> {
        let mut target_map = HashMap::new();
        let mut inlined = HashSet::new();
        for target in original_update_stmt.target_list() {
            if let Some(name) = target.name() {
                if let Ok(Value::Placeholder(number)) = Value::try_from(target.val()) {
                    target_map.insert(name.to_string(), number);
                } else {
                    inlined.insert(name.to_string());
                }
            }
        }

        /// Builds the INSERT from the row the DELETE step returned
        /// Avoids using schema cache *which can be stale* (in ref. to convos about this),
        /// e.g. after a migration that bypassed pgdog; this is why the RowDescription is used.
        #[derive(Clone, Debug)]
        struct InsertStep {
            /// The original UPDATE
            ast: Ast,
            /// Columns changed via a placeholder
            target_map: HashMap<String, i32>,
            /// Columns changed via an expression that *stays* in the SQL.
            inlined: HashSet<String>,
            params: Option<Bind>,
        }

        impl InsertStep {
            /// If `Ok(None)`: the DELETE matched nothing; there's no row to move.
            fn deleted_row<'a>(
                &self,
                map: &'a ResponseHistory,
            ) -> Result<Option<(&'a RowDescription, &'a DataRow)>, Error> {
                // TODO: Can this be a broad assert?
                debug_assert!(map.get("delete").is_some());
                let delete = map
                    .get("delete")
                    .ok_or(UpdateError::MissingStepResponse("delete"))?;

                let rows = delete.rows.len();
                if rows >= 2 {
                    return Err(UpdateError::TooManyRows(rows).into());
                }

                let Some(data_row) = delete.rows.first() else {
                    return Ok(None);
                };
                let row_description = delete
                    .row_description
                    .as_ref()
                    .ok_or(UpdateError::MissingStepResponse("delete"))?;

                Ok(Some((row_description, data_row)))
            }

            fn update_stmt(&self) -> Result<&nodes::UpdateStmt, Error> {
                match self.ast.ast.stmts().exactly_one() {
                    Ok(Node::UpdateStmt(update)) => Ok(update),
                    _ => Err(UpdateError::NotAnUpdate.into()),
                }
            }
        }

        impl StatementSource for InsertStep {
            fn resolve(&self, map: &ResponseHistory) -> Result<Option<(Parse, Bind)>, Error> {
                let Some((row_description, data_row)) = self.deleted_row(map)? else {
                    // Nothing was deleted; skip the INSERT; emit UPDATE 0
                    return Ok(None);
                };
                let update = self.update_stmt()?;

                let insert = try_owned(|mem| -> Result<_, Error> {
                    let mut columns = Vec::new();
                    let mut values = Vec::new();
                    let mut placeholders = 0;

                    for field in row_description.fields.iter() {
                        let name = field.name.as_str();
                        columns.push(
                            mem.make_res_target(Some(name), mem.empty(), mem.none())
                                .uncast(),
                        );

                        if self.inlined.contains(name) {
                            let value = update
                                .target_list()
                                .iter()
                                .find_map(|rt| {
                                    if rt.name() == Some(name) {
                                        Some(rt.val())
                                    } else {
                                        None
                                    }
                                })
                                .expect("inlined columns come from the target list");
                            values.push(mem.make_unique(value));
                        } else {
                            // $1, $2, $3
                            placeholders += 1;
                            values.push(mem.make_param_ref(placeholders).uncast());
                        }
                    }

                    let mut insert = mem.make_node::<nodes::InsertStmt>();
                    insert
                        .as_mut()
                        .set_relation(mem.make_unique(update.relation()));
                    insert.as_mut().set_cols(mem.make_list(&columns));
                    let mut select = mem.make_node::<nodes::SelectStmt>();
                    select
                        .as_mut()
                        .set_values_lists(mem.make_list(&[mem.make_list(&values)]));
                    insert.as_mut().set_select_stmt(select.uncast());
                    insert
                        .as_mut()
                        .set_returning_clause(mem.make_unique(update.returning_clause()));
                    Ok(mem.make_list(&[mem.make_raw_stmt(insert.uncast())]))
                })?;

                let parse = Parse::new_anonymous(deparse(insert.first().unwrap())?.as_str());

                let mut bind = Bind::new_statement("");
                for (idx, field) in row_description.fields.iter().enumerate() {
                    let name = field.name.as_str();
                    if self.inlined.contains(name) {
                        continue;
                    }

                    if let Some(number) = self.target_map.get(name) {
                        let number = *number;
                        let param = self
                            .params
                            .as_ref()
                            .and_then(|p| p.parameter(number as usize - 1).transpose())
                            .ok_or(RewriteError::MissingParameter(number as u16))??;
                        bind.push_param(param.parameter().clone(), param.format());
                    } else {
                        // This column wasn't changed, get the value from the select.
                        debug_assert!(data_row.get_raw(idx).is_some());
                        let value = data_row
                            .get_raw(idx)
                            .ok_or(RewriteError::MissingColumn(idx))?;

                        if value.is_null() {
                            bind.push_param(Parameter::new_null(), Format::Text);
                        } else {
                            bind.push_param(Parameter::new(value), Format::Text);
                        }
                    }
                }

                Ok(Some((parse, bind)))
            }
        }

        let insert = InsertStep {
            ast: ast.clone(),
            target_map,
            inlined,
            params: request.parameters()?.cloned(),
        };

        Ok(Step {
            save_key: Some("insert"),
            request: StepRequest::Statement(Box::new(StatementRequest {
                source: Box::new(insert),
                protocol: StepProtocol::Extended,
                route: new_route,
                ast: None,
            })),
        })
    }

    /// Static DELETE, RETURNING *
    fn construct_delete_step(
        engine: &QueryEngine,
        client_request: &ClientRequest,
        original_update_stmt: &nodes::UpdateStmt,
        context: &QueryEngineContext<'_>,
    ) -> Result<Step, Error> {
        let mut params = IndexSet::new();
        let delete = owned(|mem| {
            let mut delete = mem.make_node::<nodes::DeleteStmt>();
            delete
                .as_mut()
                .set_relation(mem.make_unique(original_update_stmt.relation()));
            delete
                .as_mut()
                .set_where_clause(mem.make_unique(original_update_stmt.where_clause()));
            delete.as_mut().set_returning_clause(
                mem.make_returning_clause(
                    mem.make_list(&[mem
                        .make_res_target(
                            None,
                            mem.empty(),
                            mem.make_column_ref(
                                mem.make_list(&[mem.make_node::<nodes::A_Star>().uncast()]),
                            )
                            .uncast(),
                        )
                        .uncast()]),
                )
                .as_option(),
            );
            params = QueryPlanner::rewrite_params(delete.as_mut().into());
            mem.make_list(&[mem.make_raw_stmt(delete.uncast())])
        });

        let stmt = deparse(delete.first().unwrap())?.as_str().to_owned();
        let ast = Ast::from_raw_stmts(delete);

        Ok(Step {
            save_key: Some("delete"),
            request: Self::build_request(
                engine,
                context,
                client_request,
                &ast,
                &stmt,
                &params,
                None,
            )?,
        })
    }

    /// If we do a SELECT with the new sharding key as the target,
    /// will it differ from a SELECT with the old sharding key in terms of
    /// which `Shard` it resolves to?
    fn on_same_shard(
        engine: &QueryEngine,
        new_target: &ResTarget,
        original_update_stmt: &nodes::UpdateStmt,
        original_request: &ClientRequest,
        context: &QueryEngineContext,
    ) -> Result<Option<Route>, Error> {
        let select_star = owned(|mem| {
            let mut select_stmt = mem.make_node::<nodes::SelectStmt>();
            select_stmt.as_mut().set_target_list(
                mem.make_list(&[mem.make_res_target(
                    None,
                    mem.empty(),
                    mem.make_column_ref(
                        mem.make_list(&[mem.make_node::<nodes::A_Star>().uncast()]),
                    )
                    .uncast(),
                )]),
            );
            select_stmt.as_mut().set_from_clause(
                mem.make_list(&[mem
                    .make_unique(
                        original_update_stmt
                            .relation()
                            .expect("UPDATE always has a table"),
                    )
                    .uncast()]),
            );
            select_stmt
        });

        let mut params = IndexSet::new();
        let check = owned(|mem| {
            let mut select_stmt = mem.make_unique(&*select_star);
            select_stmt.as_mut().set_where_clause(
                mem.make_a_expr(
                    nodes::A_Expr_Kind::AEXPR_OP,
                    mem.make_list(&[mem.make_string(Some("=")).uncast()]),
                    mem.make_column_ref(
                        mem.make_list(&[mem.make_string(new_target.name()).uncast()]),
                    )
                    .uncast(),
                    mem.make_unique(new_target.val()),
                )
                .uncast(),
            );
            params = Self::rewrite_params(select_stmt.as_mut().into());
            mem.make_list(&[mem.make_raw_stmt(select_stmt.uncast())])
        });

        let stmt = deparse(check.first().unwrap())?;
        let ast = &Ast::from_raw_stmts(check);

        let check_request = Self::build_request(
            engine,
            context,
            original_request,
            ast,
            stmt.as_str(),
            &params,
            None,
        )?;

        let new_route = match check_request {
            StepRequest::Statement(statement) => statement.route,
            StepRequest::Raw(_) => unreachable!("build_request always returns a routed statement"),
        };

        let same_shard = match original_request.route.as_ref() {
            Some(route) => route.shard().eq(new_route.shard()),
            None => {
                let mut original = original_request.clone();
                Self::route(engine, &mut original, context)?;
                original.route().shard().eq(new_route.shard())
            }
        };

        Ok((!same_shard).then_some(new_route))
    }

    /// Returns true if a column is referenced by a foreign key whose ON DELETE
    /// action would be unsafe during a sharding-key row move.
    fn has_destructive_on_delete_reference(
        engine: &QueryEngine,
        from_update: &nodes::UpdateStmt,
        context: &QueryEngineContext<'_>,
    ) -> Result<bool, Error> {
        let cluster = engine.backend.cluster()?;
        let schema = cluster.schema();
        let table = Self::target_table(from_update);

        let Some(relation) = schema.table(table, cluster.user(), context.params.search_path())
        else {
            return Ok(false);
        };
        let Some(sharded_table) = Self::sharded_table(cluster.sharded_tables(), from_update) else {
            return Ok(false);
        };

        Ok(schema.has_destructive_on_delete_reference(
            relation.schema(),
            &relation.name,
            &sharded_table.column,
        ))
    }

    pub(crate) fn sharded_table<'a>(
        sharded_tables: &'a [ShardedTable],
        from_update: &nodes::UpdateStmt,
    ) -> Option<&'a ShardedTable> {
        let table = Self::target_table(from_update);

        sharded_tables.iter().find(|sharded| {
            if let Some(name) = sharded.name.as_ref()
                && !table.name_match(name)
            {
                return false;
            }

            if let Some(schema) = sharded.schema.as_ref()
                && let Some(table_schema) = table.schema
                && table_schema != schema
            {
                return false;
            }

            from_update
                .target_list()
                .iter()
                .any(|rt| rt.name() == Some(&*sharded.column))
        })
    }

    pub(crate) fn target_table(from_update: &nodes::UpdateStmt) -> Table<'_> {
        Table::from(from_update.relation().expect("UPDATE always has table"))
    }
}
