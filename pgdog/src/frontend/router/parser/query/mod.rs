//! Route queries to correct shards.
use std::{collections::HashSet, ops::Deref};

use crate::{
    backend::{databases::databases, ShardingSchema},
    config::Role,
    frontend::router::{
        context::RouterContext,
        parser::{OrderBy, Shard},
        round_robin,
        sharding::{Centroids, ContextBuilder},
    },
    net::{
        messages::{Bind, Vector},
        parameter::ParameterValue,
    },
    plugin::plugins,
};

use super::{
    explain_trace::{ExplainRecorder, ExplainSummary},
    *,
};
mod ddl;
mod delete;
mod explain;
mod fdw_fallback;
mod plugins;
mod schema_sharding;
mod select;
mod set;
mod shared;
mod show;
mod transaction;
mod update;

use multi_tenant::MultiTenantCheck;
use pgdog_plugin::pg_query::{
    protobuf::{a_const::Val, *},
    NodeEnum,
};
use plugins::PluginOutput;

use tracing::{debug, trace};

/// Query parser.
///
/// It's job is to take a Postgres query and figure out:
///
/// 1. Which shard it should go to
/// 2. Is it a read or a write
///
/// It's re-created for each query we process. Struct variables are used
/// to store intermediate state or to store external context for the duration
/// of the parsing.
///
#[derive(Debug, Default)]
pub struct QueryParser {
    // No matter what query is executed, we'll send it to the primary.
    write_override: bool,
    // Plugin read override.
    plugin_output: PluginOutput,
    // Record explain output.
    explain_recorder: Option<ExplainRecorder>,
}

impl QueryParser {
    fn recorder_mut(&mut self) -> Option<&mut ExplainRecorder> {
        self.explain_recorder.as_mut()
    }

    fn ensure_explain_recorder(
        &mut self,
        ast: &pg_query::ParseResult,
        context: &QueryParserContext,
    ) {
        if self.explain_recorder.is_some() || !context.expanded_explain() {
            return;
        }

        if let Some(root) = ast.protobuf.stmts.first() {
            if let Some(node) = root.stmt.as_ref().and_then(|stmt| stmt.node.as_ref()) {
                if matches!(node, NodeEnum::ExplainStmt(_)) {
                    self.explain_recorder = Some(ExplainRecorder::new());
                }
            }
        }
    }

    fn attach_explain(&mut self, command: &mut Command) {
        if let (Some(recorder), Command::Query(route)) = (self.explain_recorder.take(), command) {
            let summary = ExplainSummary {
                shard: route.shard().clone(),
                read: route.is_read(),
            };
            route.set_explain(recorder.finalize(summary));
        }
    }

    /// Parse a query and return a command.
    pub fn parse(&mut self, context: RouterContext) -> Result<Command, Error> {
        let mut context = QueryParserContext::new(context)?;

        let mut command = if context.query().is_ok() {
            self.write_override = context.write_override();

            self.query(&mut context)?
        } else {
            Command::default()
        };

        if let Command::Query(route) = &mut command {
            if route.is_cross_shard() && context.shards == 1 {
                context
                    .shards_calculator
                    .push(ShardWithPriority::new_override_only_one_shard(
                        Shard::Direct(0),
                    ));
                route.set_shard_mut(context.shards_calculator.shard());
            }

            route.set_search_path_driven_mut(context.shards_calculator.is_search_path());

            if let Some(role) = context.router_context.sticky.role {
                match role {
                    Role::Primary => route.set_read(false),
                    _ => route.set_read(true),
                }
            }
        }

        debug!("query router decision: {:#?}", command);

        self.attach_explain(&mut command);

        Ok(command)
    }

    /// Bypass the query parser if we can.
    fn query_parser_bypass(context: &mut QueryParserContext) -> Option<Route> {
        let shard = context.shards_calculator.shard();

        if !shard.is_direct() && context.shards > 1 {
            return None;
        }

        if !shard.is_direct() {
            context
                .shards_calculator
                .push(ShardWithPriority::new_override_parser_disabled(
                    Shard::Direct(0),
                ));
        }

        let shard = context.shards_calculator.shard();

        // Cluster is read-only and only has one shard.
        if context.read_only {
            Some(Route::read(shard))
        }
        // Cluster doesn't have replicas and has only one shard.
        else if context.write_only {
            Some(Route::write(shard))

        // The role is specified in the connection parameter (pgdog.role).
        } else if let Some(role) = context.router_context.parameter_hints.compute_role() {
            Some(match role {
                Role::Replica => Route::read(shard),
                Role::Primary | Role::Auto => Route::write(shard),
            })
        // Default to primary.
        } else {
            Some(Route::write(shard))
        }
    }

    /// Parse a query and return a command that tells us what to do with it.
    ///
    /// # Arguments
    ///
    /// * `context`: Query router context.
    ///
    /// # Return
    ///
    /// Returns a `Command` if successful, error otherwise.
    ///
    fn query(&mut self, context: &mut QueryParserContext) -> Result<Command, Error> {
        let use_parser = context.use_parser();

        debug!(
            "parser is {}",
            if use_parser { "enabled" } else { "disabled" }
        );

        if !use_parser {
            // Try to figure out where we can send the query without
            // parsing SQL.
            if let Some(route) = Self::query_parser_bypass(context) {
                return Ok(Command::Query(route));
            } else {
                return Err(Error::QueryParserRequired);
            }
        }

        let statement = context
            .router_context
            .ast
            .clone()
            .ok_or(Error::EmptyQuery)?;

        self.ensure_explain_recorder(statement.parse_result(), context);

        // Parse hardcoded shard from a query comment.
        if context.router_needed || context.dry_run {
            if let Some(comment_shard) = statement.comment_shard.clone() {
                context
                    .shards_calculator
                    .push(ShardWithPriority::new_comment(comment_shard));
            }

            let role_override = statement.comment_role;
            if let Some(role) = role_override {
                self.write_override = role == Role::Primary;
            }

            if statement.comment_shard.is_some() || role_override.is_some() {
                let shard = context.shards_calculator.shard();

                if let Some(recorder) = self.recorder_mut() {
                    recorder.record_comment_override(shard.deref().clone(), role_override);
                }
            }
        }

        debug!("{}", context.query()?.query());
        trace!("{:#?}", statement);

        if let Some(multi_tenant) = context.multi_tenant() {
            debug!("running multi-tenant check");

            MultiTenantCheck::new(
                context.router_context.cluster.user(),
                multi_tenant,
                context.router_context.cluster.schema(),
                statement.parse_result(),
                context.router_context.parameter_hints.search_path,
            )
            .run()?;
        }

        let stmts = &statement.parse_result().protobuf.stmts;

        // Handle multi-statement SET commands (e.g. "SET x TO 1; SET y TO 2").
        if stmts.len() > 1 {
            if let Some(command) = self.try_multi_set(stmts, context)? {
                return Ok(command);
            }
        }

        //
        // Get the root AST node.
        //
        // We don't expect clients to send multiple queries. If they do
        // only the first one is used for routing.
        //
        let root = stmts.first();

        let root = if let Some(root) = root {
            root.stmt.as_ref().ok_or(Error::EmptyQuery)?
        } else {
            context
                .shards_calculator
                .push(ShardWithPriority::new_rr_empty_query(Shard::Direct(
                    round_robin::next() % context.shards,
                )));
            // Send empty query to any shard.
            return Ok(Command::Query(Route::read(
                context.shards_calculator.shard(),
            )));
        };

        let mut command = match root.node {
            // SET statements -> return immediately.
            Some(NodeEnum::VariableSetStmt(ref stmt)) => return self.set(stmt, context),
            // SHOW statements -> return immediately.
            Some(NodeEnum::VariableShowStmt(ref stmt)) => return self.show(stmt, context),
            // DEALLOCATE statements -> return immediately.
            Some(NodeEnum::DeallocateStmt(_)) => {
                return Ok(Command::Deallocate);
            }
            // SELECT statements.
            Some(NodeEnum::SelectStmt(ref stmt)) => self.select(&statement, stmt, context),
            // COPY statements.
            Some(NodeEnum::CopyStmt(ref stmt)) => Self::copy(stmt, context),
            // INSERT statements.
            Some(NodeEnum::InsertStmt(ref stmt)) => self.insert(stmt, context),
            // UPDATE statements.
            Some(NodeEnum::UpdateStmt(ref stmt)) => self.update(stmt, context),
            // DELETE statements.
            Some(NodeEnum::DeleteStmt(ref stmt)) => self.delete(stmt, context),
            // Transaction control statements,
            // e.g. BEGIN, COMMIT, etc.
            Some(NodeEnum::TransactionStmt(ref stmt)) => match self.transaction(stmt, context)? {
                Command::Query(query) => Ok(Command::Query(query)),
                command => return Ok(command),
            },

            // LISTEN <channel>;
            Some(NodeEnum::ListenStmt(ref stmt)) => {
                let shard = ContextBuilder::from_string(&stmt.conditionname)?
                    .shards(context.shards)
                    .build()?
                    .apply()?;

                return Ok(Command::Listen {
                    shard,
                    channel: stmt.conditionname.clone(),
                });
            }

            Some(NodeEnum::NotifyStmt(ref stmt)) => {
                let shard = ContextBuilder::from_string(&stmt.conditionname)?
                    .shards(context.shards)
                    .build()?
                    .apply()?;

                return Ok(Command::Notify {
                    shard,
                    channel: stmt.conditionname.clone(),
                    payload: stmt.payload.clone(),
                });
            }

            Some(NodeEnum::UnlistenStmt(ref stmt)) => {
                return Ok(Command::Unlisten(stmt.conditionname.clone()));
            }

            Some(NodeEnum::ExplainStmt(ref stmt)) => self.explain(&statement, stmt, context),

            Some(NodeEnum::DiscardStmt { .. }) => {
                return Ok(Command::Discard {
                    extended: !context.query()?.simple(),
                })
            }

            _ => self.ddl(&root.node, context),
        }?;

        // e.g. Parse, Describe, Flush-style flow.
        if !context.router_context.executable {
            if let Command::Query(ref mut query) = command {
                if query.is_cross_shard() && statement.rewrite_plan.insert_split.is_empty() {
                    context
                        .shards_calculator
                        .push(ShardWithPriority::new_rr_not_executable(Shard::Direct(
                            round_robin::next() % context.shards,
                        )));

                    query.set_shard_mut(context.shards_calculator.shard().clone());

                    // Since this query isn't executable and we decided
                    // to route it to any shard, we can early return here.
                    return Ok(command);
                }
            }
        }

        // Run plugins, if any.
        self.plugins(
            context,
            &statement,
            match &command {
                Command::Query(query) => query.is_read(),
                _ => false,
            },
        )?;

        // Set shard on route, if we're ready.
        if let Command::Query(ref mut route) = command {
            let shard = context.shards_calculator.shard();
            if shard.is_direct() {
                route.set_shard_mut(shard);
            }

            // User requested fdw backend. Cool, but never for DDL.
            if context.router_context.parameter_hints.use_fdw_fallback()
                && !route.is_ddl()
                && context.router_context.cluster.fdw_fallback_enabled()
            {
                route.set_fdw_fallback(true);
            }
        }

        // Set plugin-specified route, if available.
        // Plugins override what we calculated above.
        if let Command::Query(ref mut route) = command {
            if let Some(read) = self.plugin_output.read {
                route.set_read(read);
            }

            if let Some(ref shard) = self.plugin_output.shard {
                context
                    .shards_calculator
                    .push(ShardWithPriority::new_plugin(shard.clone()));
                route.set_shard_raw_mut(context.shards_calculator.shard());
            }
        }

        // If we only have one shard, set it.
        //
        // If the query parser couldn't figure it out,
        // there is no point of doing a multi-shard query with only one shard
        // in the set.
        //
        if context.shards == 1 && !context.dry_run {
            if let Command::Query(ref mut route) = command {
                context
                    .shards_calculator
                    .push(ShardWithPriority::new_override_only_one_shard(
                        Shard::Direct(0),
                    ));
                route.set_shard_mut(context.shards_calculator.shard());
            }
        }

        if let Command::Query(ref mut route) = command {
            // Last ditch attempt to route a query to a specific shard.
            //
            // Looking through manual queries to see if we have any
            // with the fingerprint.
            //
            if route.shard().is_all() {
                let databases = databases();
                // Only fingerprint the query if some manual queries are configured.
                // Otherwise, we're wasting time parsing SQL.
                if !databases.manual_queries().is_empty() {
                    let fingerprint = &statement.fingerprint.hex;
                    debug!("fingerprint: {}", fingerprint);
                    let manual_route = databases.manual_query(fingerprint).cloned();

                    // TODO: check routing logic required by config.
                    if manual_route.is_some() {
                        context.shards_calculator.push(ShardWithPriority::new_table(
                            Shard::Direct(round_robin::next() % context.shards),
                        ));
                        route.set_shard_mut(context.shards_calculator.shard().clone());
                    }
                }
            }
        }

        statement.update_stats(command.route());

        if context.dry_run {
            // Record statement in cache with normalized parameters.
            if !statement.cached {
                let query = context.query()?.query();
                Cache::get().record_normalized(
                    query,
                    command.route(),
                    context.sharding_schema.query_parser_engine,
                )?;
            }
            Ok(command.dry_run())
        } else {
            Ok(command)
        }
    }

    /// Handle COPY command.
    fn copy(stmt: &CopyStmt, context: &mut QueryParserContext) -> Result<Command, Error> {
        // Schema-based routing.
        //
        // We do this here as well because COPY <table> TO STDOUT
        // doesn't use the CopyParser (doesn't need to, normally),
        // so we need to handle this case here.
        //
        // The CopyParser itself has handling for schema-based sharding,
        // but that's only used for logical replication during the first
        // phase of data-sync.
        //
        let table = stmt.relation.as_ref().map(Table::from);
        if let Some(table) = table {
            if let Some(schema) = context.sharding_schema.schemas.get(table.schema()) {
                let shard: Shard = schema.shard().into();
                context
                    .shards_calculator
                    .push(ShardWithPriority::new_table(shard));
                if !stmt.is_from {
                    return Ok(Command::Query(Route::read(
                        context.shards_calculator.shard(),
                    )));
                } else {
                    return Ok(Command::Query(Route::write(
                        context.shards_calculator.shard(),
                    )));
                }
            }
        }

        let parser = CopyParser::new(stmt, context.router_context.cluster)?;
        if !stmt.is_from {
            context
                .shards_calculator
                .push(ShardWithPriority::new_table(Shard::All));
            Ok(Command::Query(Route::read(
                context.shards_calculator.shard(),
            )))
        } else {
            Ok(Command::Copy(Box::new(parser)))
        }
    }

    /// Handle INSERT statement.
    ///
    /// # Arguments
    ///
    /// * `stmt`: INSERT statement from pg_query.
    /// * `context`: Query parser context.
    ///
    fn insert(
        &mut self,
        stmt: &InsertStmt,
        context: &mut QueryParserContext,
    ) -> Result<Command, Error> {
        let schema_lookup = SchemaLookupContext {
            db_schema: &context.router_context.schema,
            user: context.router_context.cluster.user(),
            search_path: context.router_context.parameter_hints.search_path,
        };
        let mut parser = StatementParser::from_insert(
            stmt,
            context.router_context.bind,
            &context.sharding_schema,
            self.recorder_mut(),
        )
        .with_schema_lookup(schema_lookup);

        let is_sharded = parser.is_sharded(
            &context.router_context.schema,
            context.router_context.cluster.user(),
            context.router_context.parameter_hints.search_path,
        );

        let shard = parser.shard()?.unwrap_or(Shard::All);

        context.shards_calculator.push(if is_sharded {
            ShardWithPriority::new_table(shard.clone())
        } else {
            ShardWithPriority::new_table_omni(shard)
        });

        let shard = context.shards_calculator.shard();

        if let Some(recorder) = self.recorder_mut() {
            match shard.deref() {
                Shard::Direct(_) => recorder
                    .record_entry(Some(shard.deref().clone()), "INSERT matched sharding key"),
                Shard::Multi(_) => recorder.record_entry(
                    Some(shard.deref().clone()),
                    "INSERT targeted multiple shards",
                ),
                Shard::All => recorder.record_entry(None, "INSERT broadcasted"),
            };
        }

        Ok(Command::Query(Route::write(shard)))
    }
}

#[cfg(test)]
mod test;
