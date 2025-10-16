//! Route queries to correct shards.
use std::collections::HashSet;

use crate::{
    backend::{databases::databases, ShardingSchema},
    config::Role,
    frontend::{
        router::{
            context::RouterContext,
            parser::{rewrite::Rewrite, OrderBy, Shard},
            rewrite::{PlannerContext, PlannerInput, RewritePlanner, RewriteRegistry},
            round_robin,
            sharding::{Centroids, ContextBuilder, Value as ShardingValue},
        },
        BufferedQuery,
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
mod delete;
mod explain;
mod plugins;
mod select;
mod set;
mod shared;
mod show;
mod transaction;
mod update;

use multi_tenant::MultiTenantCheck;
use pgdog_plugin::pg_query::{
    fingerprint,
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
/// 3. Does it need to be re-rewritten to something else, e.g. prepared statement.
///
/// It's re-created for each query we process. Struct variables are used
/// to store intermediate state or to store external context for the duration
/// of the parsing.
///
#[derive(Debug)]
pub struct QueryParser {
    // The statement is executed inside a transaction.
    in_transaction: bool,
    // No matter what query is executed, we'll send it to the primary.
    write_override: bool,
    // Currently calculated shard.
    shard: Shard,
    // Plugin read override.
    plugin_output: PluginOutput,
    explain_recorder: Option<ExplainRecorder>,
    rewrite_registry: RewriteRegistry,
}

impl Default for QueryParser {
    fn default() -> Self {
        Self {
            in_transaction: false,
            write_override: false,
            shard: Shard::All,
            plugin_output: PluginOutput::default(),
            explain_recorder: None,
            rewrite_registry: RewriteRegistry::new(),
        }
    }
}

impl QueryParser {
    pub fn register_rewrite_planner(&mut self, planner: Box<dyn RewritePlanner + Send + Sync>) {
        self.rewrite_registry.add_planner(planner);
    }

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

    /// Indicates we are in a transaction.
    pub fn in_transaction(&self) -> bool {
        self.in_transaction
    }

    /// Parse a query and return a command.
    pub fn parse(&mut self, context: RouterContext) -> Result<Command, Error> {
        let mut qp_context = QueryParserContext::new(context);

        let mut command = if qp_context.query().is_ok() {
            self.in_transaction = qp_context.router_context.in_transaction();
            self.write_override = qp_context.write_override();

            self.query(&mut qp_context)?
        } else {
            Command::default()
        };

        if let Some(plan) = self.rewrite_registry.plan(
            &PlannerContext::new(&qp_context),
            &PlannerInput::Command(&command),
        )? {
            command = Command::PlannedRewrite(plan);
        }

        // If the cluster only has one shard, use direct-to-shard queries.
        match &mut command {
            Command::Query(ref mut query) => {
                if !matches!(query.shard(), Shard::Direct(_)) && qp_context.shards == 1 {
                    query.set_shard_mut(0);
                }
            }
            Command::PlannedRewrite(ref mut plan) => {
                if !matches!(plan.route().shard(), Shard::Direct(_)) && qp_context.shards == 1 {
                    plan.route_mut().set_shard_mut(0);
                }
            }
            _ => {}
        }

        Ok(command)
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
            // Cluster is read-only and only has one shard.
            if context.read_only {
                return Ok(Command::Query(Route::read(Shard::Direct(0))));
            }
            // Cluster doesn't have replicas and has only one shard.
            if context.write_only {
                return Ok(Command::Query(Route::write(Shard::Direct(0))));
            }
        }

        let cache = Cache::get();

        // Get the AST from cache or parse the statement live.
        let statement = match context.query()? {
            // Only prepared statements (or just extended) are cached.
            BufferedQuery::Prepared(query) => {
                cache.parse(query.query(), &context.sharding_schema)?
            }
            // Don't cache simple queries.
            //
            // They contain parameter values, which makes the cache
            // too large to be practical.
            //
            // Make your clients use prepared statements
            // or at least send statements with placeholders using the
            // extended protocol.
            BufferedQuery::Query(query) => {
                cache.parse_uncached(query.query(), &context.sharding_schema)?
            }
        };

        self.ensure_explain_recorder(statement.ast(), context);

        // Parse hardcoded shard from a query comment.
        if context.router_needed || context.dry_run {
            self.shard = statement.shard.clone();
            let role_override = statement.role;
            if let Some(role) = role_override {
                self.write_override = role == Role::Primary;
            }
            if let Some(recorder) = self.recorder_mut() {
                if !matches!(statement.shard, Shard::All) || role_override.is_some() {
                    let role_str = role_override.map(|role| match role {
                        Role::Primary => "primary",
                        Role::Replica => "replica",
                    });
                    recorder.record_comment_override(statement.shard.clone(), role_str);
                }
            }
        }

        debug!("{}", context.query()?.query());
        trace!("{:#?}", statement);

        let rewrite = Rewrite::new(statement.ast());
        if rewrite.needs_rewrite() {
            debug!("rewrite needed");
            return rewrite.rewrite(context.prepared_statements());
        }

        if let Some(multi_tenant) = context.multi_tenant() {
            debug!("running multi-tenant check");

            MultiTenantCheck::new(
                context.router_context.cluster.user(),
                multi_tenant,
                context.router_context.cluster.schema(),
                statement.ast(),
                context.router_context.params,
            )
            .run()?;
        }

        //
        // Get the root AST node.
        //
        // We don't expect clients to send multiple queries. If they do
        // only the first one is used for routing.
        //
        let root = statement.ast().protobuf.stmts.first();

        let root = if let Some(root) = root {
            root.stmt.as_ref().ok_or(Error::EmptyQuery)?
        } else {
            // Send empty query to any shard.
            return Ok(Command::Query(Route::read(Shard::Direct(
                round_robin::next() % context.shards,
            ))));
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
            Some(NodeEnum::SelectStmt(ref stmt)) => self.select(statement.ast(), stmt, context),
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

            Some(NodeEnum::ExplainStmt(ref stmt)) => self.explain(statement.ast(), stmt, context),

            // VACUUM.
            Some(NodeEnum::VacuumRelation(_)) | Some(NodeEnum::VacuumStmt(_)) => {
                Ok(Command::Query(Route::write(None).set_maintenace()))
            }

            Some(NodeEnum::DiscardStmt { .. }) => {
                return Ok(Command::Discard {
                    extended: !context.query()?.simple(),
                })
            }

            // All others are not handled.
            // They are sent to all shards concurrently.
            _ => Ok(Command::Query(Route::write(None))),
        }?;

        // e.g. Parse, Describe, Flush-style flow.
        if !context.router_context.executable {
            if let Command::Query(query) = command {
                return Ok(Command::Query(
                    query.set_shard(round_robin::next() % context.shards),
                ));
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

        // Overwrite shard using shard we got from a comment, if any.
        if let Shard::Direct(shard) = self.shard {
            if let Command::Query(ref mut route) = command {
                route.set_shard_mut(shard);
            }
        }

        // Set plugin-specified route, if available.
        // Plugins override what we calculated above.
        if let Command::Query(ref mut route) = command {
            if let Some(read) = self.plugin_output.read {
                route.set_read_mut(read);
            }

            if let Some(ref shard) = self.plugin_output.shard {
                route.set_shard_raw_mut(shard);
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
                route.set_shard_mut(0);
            }
        }

        if let Command::Query(ref mut route) = command {
            // Last ditch attempt to route a query to a specific shard.
            //
            // Looking through manual queries to see if we have any
            // with the fingerprint.
            //
            if route.shard().all() {
                let databases = databases();
                // Only fingerprint the query if some manual queries are configured.
                // Otherwise, we're wasting time parsing SQL.
                if !databases.manual_queries().is_empty() {
                    let fingerprint =
                        fingerprint(context.query()?.query()).map_err(Error::PgQuery)?;
                    debug!("fingerprint: {}", fingerprint.hex);
                    let manual_route = databases.manual_query(&fingerprint.hex).cloned();

                    // TODO: check routing logic required by config.
                    if manual_route.is_some() {
                        route.set_shard_mut(round_robin::next() % context.shards);
                    }
                }
            }
        }

        debug!("query router decision: {:#?}", command);

        statement.update_stats(command.route());

        if context.dry_run {
            // Record statement in cache with normalized parameters.
            if !statement.cached {
                cache.record_normalized(
                    context.query()?.query(),
                    command.route(),
                    &context.sharding_schema,
                )?;
            }
            Ok(command.dry_run())
        } else {
            Ok(command)
        }
    }

    /// Handle COPY command.
    fn copy(stmt: &CopyStmt, context: &QueryParserContext) -> Result<Command, Error> {
        let parser = CopyParser::new(stmt, context.router_context.cluster)?;
        if let Some(parser) = parser {
            Ok(Command::Copy(Box::new(parser)))
        } else {
            Ok(Command::Query(Route::write(None)))
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
        context: &QueryParserContext,
    ) -> Result<Command, Error> {
        let insert = Insert::new(stmt);
        let shard = insert.shard(&context.sharding_schema, context.router_context.bind)?;
        if let Some(recorder) = self.recorder_mut() {
            match &shard {
                Shard::Direct(_) => {
                    recorder.record_entry(Some(shard.clone()), "INSERT matched sharding key")
                }
                Shard::Multi(_) => {
                    recorder.record_entry(Some(shard.clone()), "INSERT targeted multiple shards")
                }
                Shard::All => recorder.record_entry(None, "INSERT broadcasted"),
            };
        }
        Ok(Command::Query(Route::write(shard)))
    }
}

#[cfg(test)]
mod test;
