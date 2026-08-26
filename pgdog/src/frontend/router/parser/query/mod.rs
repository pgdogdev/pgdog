//! Route queries to correct shards.
use std::{collections::HashSet, ops::Deref};

use crate::{
    backend::ShardingSchema,
    config::Role,
    frontend::router::{
        context::RouterContext,
        parser::{OrderBy, Shard, route::ShardSource},
        round_robin,
        sharding::{self, Centroids, ContextBuilder, ShardOrLookup},
    },
    net::{messages::Vector, parameter::ParameterValue},
    plugin::plugins,
};

#[cfg(test)]
use crate::net::messages::Bind;

use super::{
    explain_trace::{ExplainRecorder, ExplainSummary},
    *,
};
mod ddl;
mod delete;
mod explain;
mod plugins;
mod prepare;
mod select;
mod set;
mod set_config;
mod shared;
mod show;
mod split;
mod transaction;
mod update;

use itertools::*;
use multi_tenant::MultiTenantCheck;
use pg_raw_parse::{Node, nodes};
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

    fn ensure_explain_recorder(&mut self, node: Node<'_>, context: &QueryParserContext) {
        if self.explain_recorder.is_some() || !context.expanded_explain() {
            return;
        }

        if matches!(node, Node::ExplainStmt(_)) {
            self.explain_recorder = Some(ExplainRecorder::new());
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

        match &mut command {
            Command::Query(route) | Command::Set { route, .. } => {
                if route.is_cross_shard() && context.shards == 1 {
                    context
                        .shards_calculator
                        .push(ShardWithPriority::new_override_only_one_shard(
                            Shard::Direct(0),
                        ));
                    route.set_shard(context.shards_calculator.shard());
                }

                // Used in connect.rs to determine if we're routing via search_path.
                // Even if we're only using schema_sharding, what if there's a SELECT from all?
                let is_search_path = context.shards_calculator.is_search_path();
                route.set_search_path_driven(is_search_path);

                // Are we using `sharded_schemas` and NOT using `sharded_tables`?
                // If we're using both: there's no way to know right now if a query
                // that doesn't have SearchPath specified (e.g. using a SET) might mess with an
                // omnisharded configuration for a sharded schema.
                // TODO: We should consider in the future what should happen
                //       if people are using multiple sharding functions.
                route.sharded_schema_only =
                    !context.sharding_schema.schemas.is_empty() && !context.sharded_tables;

                // Note: this is dependent on route.sharded_schema_only being set first.
                let full_shard_coverage = route.requires_full_shard_coverage();

                let manual_routing = matches!(
                    route.shard_with_priority().source(),
                    ShardSource::Comment | ShardSource::Set
                );

                // This means we are serving the request, not just extracting
                // which keys we need to lookup in the routing table.
                let have_lookups = !context.bare_key_lookups.is_empty();

                if full_shard_coverage && (manual_routing || have_lookups) {
                    return Err(Error::OmniWriteWithDirective);
                }

                context
                    .pending_lookups
                    .extend(std::mem::take(&mut context.bare_key_lookups));

                route.set_pending_lookups(std::mem::take(&mut context.pending_lookups));

                if let Some(role) = context.router_context.sticky.role {
                    match role {
                        Role::Primary => route.set_read(false),
                        _ => route.set_read(true),
                    }
                }
            }

            _ => (),
        }

        if let Command::Query(_) = command
            && context.router_context.cluster.cross_shard_disabled()
            && context.shards_calculator.shard().is_all()
        {
            // The user specified a sharding key, which is un-mapped (list-based/range-based).
            // If not stopped, the query would be cross-shard.
            // Rather than give them a generic 'cross shard disabled' error,
            // tell them the sharding key is un-mapped.

            if let Some(sharding_key) = context.router_context.parameter_hints.pgdog_sharding_key
                && let Some(sharding_key_value) = sharding_key.as_str()
            {
                // SET sharding key
                return Err(Error::UnmappedShardKey(sharding_key_value.to_string()));
            } else if let Some(statement) = context.router_context.ast
                && let Some(sharding_key) = &statement.comment_sharding_key
            {
                // Comment directive sharding key
                return Err(Error::UnmappedShardKey(sharding_key.to_string()));
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
        } else if context.prefer_primary {
            // Send queries to primary by default.
            Some(Route::write(shard))
        } else if context.prefer_replica {
            // Send queries to replicas by default.
            Some(Route::read(shard))
        } else {
            // Default to primary.
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
        let parser_enabled = context.router_context.ast.is_some();

        debug!(
            "parser is {}",
            if parser_enabled {
                "enabled"
            } else {
                "disabled"
            }
        );

        if !parser_enabled {
            // Try to figure out where we can send the query without
            // parsing SQL.
            if let Some(route) = Self::query_parser_bypass(context) {
                return Ok(Command::Query(route));
            } else {
                return Err(Error::QueryParserRequired);
            }
        }

        let statement = context.router_context.ast.ok_or(Error::EmptyQuery)?;

        if let Some(stmt) = statement.ast.stmts().next() {
            self.ensure_explain_recorder(stmt, context);
        }

        // Parse hardcoded shard from a query comment.
        if context.router_needed || context.dry_run {
            let mut comment_shard_set = false;
            match &statement.comment_shard {
                Some(ShardOrLookup::Shard(comment_shard)) => {
                    context
                        .shards_calculator
                        .push(ShardWithPriority::new_comment(comment_shard.clone()));
                    comment_shard_set = true;
                }
                // The sharding key in the comment missed the lookup
                // cache when the comment was parsed, which happens
                // before routing. Check again: on the second routing
                // pass the translation has been resolved. The pending
                // lookup is cloned only when it actually has to run.
                Some(ShardOrLookup::Lookup(pending)) => {
                    match sharding::lookup::shard_for_pending(
                        pending,
                        &context.sharding_schema,
                        &context.router_context.resolved_lookups,
                    )? {
                        Some(shard) => {
                            context
                                .shards_calculator
                                .push(ShardWithPriority::new_comment(shard));
                            comment_shard_set = true;
                        }
                        None => context.bare_key_lookups.push(pending.clone()),
                    }
                }
                None => {}
            }

            let role_override = statement.comment_role;
            if let Some(role) = role_override {
                self.write_override = role == Role::Primary;
            }

            if comment_shard_set || role_override.is_some() {
                let shard = context.shards_calculator.shard();

                if let Some(recorder) = self.recorder_mut() {
                    recorder.record_comment_override(shard.deref().clone(), role_override);
                }
            }
        }

        debug!("{}", context.query()?.query());
        trace!("{:#?}", statement);

        let stmts = &statement.ast;

        if let Some(multi_tenant) = context.multi_tenant()
            && let Some(stmt) = stmts.stmts().next()
        {
            debug!("running multi-tenant check");

            MultiTenantCheck::new(
                context.router_context.cluster.user(),
                multi_tenant,
                context.router_context.cluster.schema(),
                stmt,
                context.router_context.parameter_hints.search_path,
            )
            .run()?;
        }

        if let Some(command) = self.check_multi_statement(statement, context)? {
            return Ok(command);
        }

        //
        // Get the root AST node.
        //
        // We don't expect clients to send multiple queries. If they do
        // only the first one is used for routing.
        //
        let root = stmts.first();

        let Some(root) = root else {
            context
                .shards_calculator
                .push(ShardWithPriority::new_rr_empty_query(Shard::Direct(
                    round_robin::next(context.shards),
                )));
            // Send empty query to any shard.
            return Ok(Command::Query(Route::read(
                context.shards_calculator.shard(),
            )));
        };

        let mut command = match root.stmt() {
            Node::VariableSetStmt(stmt) => return self.set(stmt, context),

            Node::SelectStmt(stmt) if let Some(set_config) = extract_set_config(stmt) => {
                return Ok(self.set_config(set_config, context));
            }

            Node::VariableShowStmt(stmt) => {
                return self.show(stmt, context);
            }

            Node::DeallocateStmt(_) => {
                return Ok(Command::Deallocate);
            }

            Node::SelectStmt(stmt) => {
                if context.is_canonicalizing_oids() && references_pg_type(stmt) {
                    // Shard 0 is considered the canonical source for now
                    return Ok(Command::Query(Route::read(
                        ShardWithPriority::new_override_canonical_schema_info(Shard::Direct(0)),
                    )));
                } else {
                    self.select(statement, stmt, context)
                }
            }

            Node::CopyStmt(stmt) => Self::copy(stmt, context),

            Node::InsertStmt(stmt) => self.insert(stmt.into(), context),
            Node::UpdateStmt(stmt) => self.update(stmt.into(), context),
            Node::DeleteStmt(stmt) => self.delete(stmt.into(), context),

            // e.g. BEGIN, COMMIT, etc.
            Node::TransactionStmt(stmt) => self.transaction(stmt, context),

            Node::ListenStmt(stmt) => {
                let channel = stmt
                    .conditionname()
                    .expect("LISTEN always has name")
                    .to_owned();
                let shard = ContextBuilder::from_string(&channel)?
                    .shards(context.shards)
                    .build()?
                    .apply()?;

                return Ok(Command::Listen { shard, channel });
            }

            Node::NotifyStmt(stmt) => {
                let channel = stmt
                    .conditionname()
                    .expect("NOTIFY always has name")
                    .to_owned();
                let shard = ContextBuilder::from_string(&channel)?
                    .shards(context.shards)
                    .build()?
                    .apply()?;

                return Ok(Command::Notify {
                    shard,
                    channel,
                    // FIXME: NOTIFY without payload is not the same as a
                    // payload of an empty string
                    payload: stmt.payload().unwrap_or_default().to_owned(),
                });
            }

            Node::UnlistenStmt(stmt) => {
                // FIXME: UNLISTEN * is sent represented as None
                return Ok(Command::Unlisten(
                    stmt.conditionname().unwrap_or_default().to_owned(),
                ));
            }

            Node::PrepareStmt(stmt) => self.prepare(stmt, context),

            Node::ExecuteStmt(stmt) => self.execute(stmt, context),

            Node::ExplainStmt(stmt) => self.explain(statement, stmt, context),

            Node::DiscardStmt { .. } => {
                return Ok(Command::Discard {
                    extended: !context.query()?.simple(),
                });
            }

            node => self.ddl(node, context),
        }?;

        // e.g. Parse, Describe, Flush-style flow.
        if !context.router_context.executable
            && let Command::Query(ref query) = command
            && query.is_cross_shard()
            && statement.rewrite_plan.insert_split.is_empty()
        {
            context
                .shards_calculator
                .push(ShardWithPriority::new_rr_not_executable(Shard::Direct(
                    round_robin::next(context.shards),
                )));

            // Since this query isn't executable and we decided
            // to route it to any shard, we can early return here.
            return Ok(Command::Query(
                query
                    .clone()
                    .with_shard(context.shards_calculator.shard().clone()),
            ));
        }

        // Run plugins, if any.
        self.plugins(
            context,
            statement,
            match &command {
                Command::Query(query) => query.is_read(),
                _ => false,
            },
        )?;

        // Set shard on route, if we're ready.
        if let Command::Query(ref mut route) = command {
            let shard = context.shards_calculator.shard();
            if shard.is_direct() {
                route.set_shard(shard);
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
                route.set_shard(context.shards_calculator.shard());
            }
        }

        // If we only have one shard, set it.
        //
        // If the query parser couldn't figure it out,
        // there is no point of doing a multi-shard query with only one shard
        // in the set.
        //
        if context.shards == 1
            && !context.dry_run
            && let Command::Query(ref mut route) = command
        {
            context
                .shards_calculator
                .push(ShardWithPriority::new_override_only_one_shard(
                    Shard::Direct(0),
                ));
            route.set_shard(context.shards_calculator.shard());
        }

        statement.update_stats(command.route());

        if context.dry_run {
            // Record statement in cache with normalized parameters.
            if !statement.cached {
                Cache::get().record_normalized(
                    statement.ast.into_iter().next().ok_or(Error::EmptyQuery)?,
                    command.route(),
                )?;
            }
            Ok(command.dry_run())
        } else {
            Ok(command)
        }
    }

    /// Handle COPY command.
    fn copy(stmt: &nodes::CopyStmt, context: &mut QueryParserContext) -> Result<Command, Error> {
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
        let table = stmt.relation().map(Table::from);

        if let Some(table) = table
            && let Some(schema) = context.sharding_schema.schemas.get(table.schema())
        {
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
    /// * `stmt`: INSERT statement.
    /// * `context`: Query parser context.
    ///
    fn insert(
        &mut self,
        stmt: pg_raw_parse::Node<'_>,
        context: &mut QueryParserContext,
    ) -> Result<Command, Error> {
        let schema_lookup = SchemaLookupContext {
            db_schema: &context.router_context.schema,
            user: context.router_context.cluster.user(),
            search_path: context.router_context.parameter_hints.search_path,
        };
        let mut parser = StatementParser::new(
            stmt,
            context.router_context.bind,
            &context.sharding_schema,
            self.recorder_mut(),
        )
        .with_schema_lookup(schema_lookup);
        parser.set_resolved_lookups(&context.router_context.resolved_lookups);

        let is_sharded = parser.is_sharded(
            &context.router_context.schema,
            context.router_context.cluster.user(),
            context.router_context.parameter_hints.search_path,
        );
        let shard = parser.shard()?;
        let omnisharded = !is_sharded && shard.is_none();
        let shard = shard.unwrap_or(Shard::All);
        let pending_lookups = parser.take_pending_lookups();
        context.pending_lookups.extend(pending_lookups);

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

        Ok(Command::Query(
            Route::write(shard).with_omnisharded(omnisharded),
        ))
    }
}

fn extract_set_config(stmt: &nodes::SelectStmt) -> Option<&nodes::FuncCall> {
    static SET_CONFIG: &[&[&str]] = &[&["pg_catalog", "set_config"], &["set_config"]];

    stmt.target_list()
        .iter()
        .exactly_one()
        .ok()
        .and_then(|r| match r.val() {
            Node::FuncCall(f)
                if SET_CONFIG.iter().any(|n| {
                    f.funcname()
                        .iter()
                        .filter_map(Node::as_str)
                        .eq(n.iter().copied())
                }) =>
            {
                Some(f)
            }
            _ => None,
        })
}

fn references_pg_type(stmt: &nodes::SelectStmt) -> bool {
    use pg_raw_parse::walk::{self, Recurse};
    use std::ops::ControlFlow;

    walk::walk_manual(stmt.into(), |node| match node {
        Node::RangeVar(rv) if rv.relname() == Some("pg_type") => ControlFlow::Break(true),
        // atttypid references pg_type.oid
        Node::RangeVar(rv) if rv.relname() == Some("pg_attribute") => ControlFlow::Break(true),
        Node::TypeCast(tc)
            if let Some(tn) = tc.type_name()
                && tn.names().iter().map(|s| s.sval()).eq([Some("regtype")]) =>
        {
            ControlFlow::Break(true)
        }
        Node::FuncCall(fc)
            if fc
                .funcname()
                .iter()
                .map(Node::as_str)
                .eq([Some("to_regtype")]) =>
        {
            ControlFlow::Break(true)
        }
        _ => Recurse::yes(),
    })
    .unwrap_or_default()
}

#[cfg(test)]
mod test;
