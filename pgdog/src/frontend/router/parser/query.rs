//! Route queries to correct shards.
use std::collections::HashSet;

use crate::{
    backend::{databases::databases, ShardingSchema},
    frontend::{
        buffer::BufferedQuery,
        router::{
            context::RouterContext,
            parser::{rewrite::Rewrite, OrderBy, Shard},
            round_robin,
            sharding::{Centroids, ContextBuilder, Value as ShardingValue},
        },
    },
    net::{
        messages::{Bind, Vector},
        parameter::ParameterValue,
    },
};

use super::*;

use multi_tenant::MultiTenantCheck;
use pg_query::{
    fingerprint,
    protobuf::{a_const::Val, *},
    NodeEnum,
};
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
    // The statement is executed inside a tranasction.
    in_transaction: bool,
    // No matter what query is executed, we'll send it to the primary.
    write_override: bool,
    // Currently calculated shard.
    shard: Shard,
}

impl Default for QueryParser {
    fn default() -> Self {
        Self {
            in_transaction: false,
            write_override: false,
            shard: Shard::All,
        }
    }
}

impl QueryParser {
    /// Indicates we are in a transaction.
    pub fn in_transaction(&self) -> bool {
        self.in_transaction
    }

    /// Parse a query and return a command.
    pub fn parse(&mut self, context: RouterContext) -> Result<Command, Error> {
        let mut qp_context = QueryParserContext::new(context);

        let mut command = if qp_context.query().is_ok() {
            self.in_transaction = qp_context.router_context.in_transaction;
            self.write_override = qp_context.write_override();

            self.query(&mut qp_context)?
        } else {
            Command::default()
        };

        // If the cluster only has one shard, use direct-to-shard queries.
        if let Command::Query(ref mut query) = command {
            if !matches!(query.shard(), Shard::Direct(_)) && qp_context.shards == 1 {
                query.set_shard_mut(0);
            }
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

        // Route transaction to primary.

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

        // Parse hardcoded shard from a query comment.
        if context.router_needed {
            if let Some(BufferedQuery::Query(ref query)) = context.router_context.query {
                self.shard = super::comment::shard(query.query(), &context.sharding_schema)?;
            }
        }

        let cache = Cache::get();

        // Get the AST from cache or parse the statement live.
        let statement = match context.query()? {
            // Only prepared statements (or just extended) are cached.
            BufferedQuery::Prepared(query) => cache.parse(query.query()).map_err(Error::PgQuery)?,
            // Don't cache simple queries.
            //
            // They contain parameter values, which makes the cache
            // too large to be practical.
            //
            // Make your clients use prepared statements
            // or at least send statements with placeholders using the
            // extended protocol.
            BufferedQuery::Query(query) => cache
                .parse_uncached(query.query())
                .map_err(Error::PgQuery)?,
        };

        debug!("{}", context.query()?.query());
        trace!("{:#?}", statement.ast());

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
        let root = statement
            .ast()
            .protobuf
            .stmts
            .first()
            .ok_or(Error::EmptyQuery)?
            .stmt
            .as_ref()
            .ok_or(Error::EmptyQuery)?;

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
            Some(NodeEnum::SelectStmt(ref stmt)) => self.select(stmt, context),
            // COPY statements.
            Some(NodeEnum::CopyStmt(ref stmt)) => Self::copy(stmt, context),
            // INSERT statements.
            Some(NodeEnum::InsertStmt(ref stmt)) => Self::insert(stmt, context),
            // UPDATE statements.
            Some(NodeEnum::UpdateStmt(ref stmt)) => Self::update(stmt, context),
            // DELETE statements.
            Some(NodeEnum::DeleteStmt(ref stmt)) => Self::delete(stmt, context),
            // Transaction control statements,
            // e.g. BEGIN, COMMIT, etc.
            Some(NodeEnum::TransactionStmt(ref stmt)) => {
                // Only allow to intercept transaction statements
                // if they are using the simple protocol.
                if context.query()?.simple() {
                    if context.rw_conservative() && !context.read_only {
                        self.write_override = true;
                    }

                    match stmt.kind() {
                        TransactionStmtKind::TransStmtCommit => {
                            return Ok(Command::CommitTransaction)
                        }
                        TransactionStmtKind::TransStmtRollback => {
                            return Ok(Command::RollbackTransaction)
                        }
                        TransactionStmtKind::TransStmtBegin
                        | TransactionStmtKind::TransStmtStart => {
                            self.in_transaction = true;
                            return Ok(Command::StartTransaction(context.query()?.clone()));
                        }
                        _ => Ok(Command::Query(Route::write(None))),
                    }
                } else {
                    Ok(Command::Query(Route::write(None)))
                }
            }

            // LISTEN <channel>;
            Some(NodeEnum::ListenStmt(ref stmt)) => {
                let shard = ContextBuilder::from_str(&stmt.conditionname)?
                    .shards(context.shards)
                    .build()?
                    .apply()?;

                return Ok(Command::Listen {
                    shard,
                    channel: stmt.conditionname.clone(),
                });
            }

            Some(NodeEnum::NotifyStmt(ref stmt)) => {
                let shard = ContextBuilder::from_str(&stmt.conditionname)?
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

            Some(NodeEnum::ExplainStmt(ref stmt)) => self.explain(stmt, context),

            // All others are not handled.
            // They are sent to all shards concurrently.
            _ => Ok(Command::Query(Route::write(None))),
        }?;

        // Overwrite shard using shard we got from a comment, if any.
        if let Shard::Direct(shard) = self.shard {
            if let Command::Query(ref mut route) = command {
                route.set_shard_mut(shard);
            }
        }

        // If we only have one shard, set it.
        //
        // If the query parser couldn't figure it out,
        // there is no point of doing a multi-shard query with only one shard
        // in the set.
        //
        if context.shards == 1 {
            if let Command::Query(ref mut route) = command {
                route.set_shard_mut(0);
            }
        }

        // Last ditch attempt to route a query to a specific shard.
        //
        // Looking through manual queries to see if we have any
        // with the fingerprint.
        //
        if let Command::Query(ref mut route) = command {
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
            Ok(command.dry_run())
        } else {
            Ok(command)
        }
    }

    /// Handle the SET command.
    ///
    /// We allow setting shard/sharding key manually outside
    /// the normal protocol flow. This command is not forwarded to the server.
    ///
    /// All other SETs change the params on the client and are eventually sent to the server
    /// when the client is connected to the server.
    fn set(
        &mut self,
        stmt: &VariableSetStmt,
        context: &QueryParserContext,
    ) -> Result<Command, Error> {
        match stmt.name.as_str() {
            "pgdog.shard" => {
                let node = stmt
                    .args
                    .first()
                    .ok_or(Error::SetShard)?
                    .node
                    .as_ref()
                    .ok_or(Error::SetShard)?;
                if let NodeEnum::AConst(AConst {
                    val: Some(a_const::Val::Ival(Integer { ival })),
                    ..
                }) = node
                {
                    return Ok(Command::Query(
                        Route::write(Some(*ival as usize)).set_read(context.read_only),
                    ));
                }
            }

            "pgdog.sharding_key" => {
                let node = stmt
                    .args
                    .first()
                    .ok_or(Error::SetShard)?
                    .node
                    .as_ref()
                    .ok_or(Error::SetShard)?;

                if let NodeEnum::AConst(AConst {
                    val: Some(Val::Sval(String { sval })),
                    ..
                }) = node
                {
                    let ctx = ContextBuilder::from_str(sval.as_str())?
                        .shards(context.shards)
                        .build()?;
                    let shard = ctx.apply()?;
                    return Ok(Command::Query(
                        Route::write(shard).set_read(context.read_only),
                    ));
                }
            }

            // TODO: Handle SET commands for updating client
            // params without touching the server.
            name => {
                if !self.in_transaction {
                    let mut value = vec![];

                    for node in &stmt.args {
                        if let Some(NodeEnum::AConst(AConst { val: Some(val), .. })) = &node.node {
                            match val {
                                Val::Sval(String { sval }) => {
                                    value.push(sval.to_string());
                                }

                                Val::Ival(Integer { ival }) => {
                                    value.push(ival.to_string());
                                }

                                Val::Fval(Float { fval }) => {
                                    value.push(fval.to_string());
                                }

                                Val::Boolval(Boolean { boolval }) => {
                                    value.push(boolval.to_string());
                                }

                                _ => (),
                            }
                        }
                    }

                    match value.len() {
                        0 => (),
                        1 => {
                            return Ok(Command::Set {
                                name: name.to_string(),
                                value: ParameterValue::String(value.pop().unwrap()),
                            })
                        }
                        _ => {
                            return Ok(Command::Set {
                                name: name.to_string(),
                                value: ParameterValue::Tuple(value),
                            })
                        }
                    }
                }
            }
        }

        Ok(Command::Query(
            Route::write(Shard::All).set_read(context.read_only),
        ))
    }

    /// Handle SHOW command.
    fn show(
        &mut self,
        stmt: &VariableShowStmt,
        context: &QueryParserContext,
    ) -> Result<Command, Error> {
        match stmt.name.as_str() {
            "pgdog.shards" => Ok(Command::Shards(context.shards)),
            _ => Ok(Command::Query(
                Route::write(Shard::All).set_read(context.read_only),
            )),
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

    /// Handle SELECT statement.
    ///
    /// # Arguments
    ///
    /// * `stmt`: SELECT statement from pg_query.
    /// * `context`: Query parser context.
    ///
    fn select(
        &mut self,
        stmt: &SelectStmt,
        context: &QueryParserContext,
    ) -> Result<Command, Error> {
        let cte_writes = Self::cte_writes(stmt);
        let mut writes = Self::functions(stmt)?;

        // Write overwrite because of conservative read/write split.
        if self.write_override {
            writes.writes = true;
        }

        if cte_writes {
            writes.writes = true;
        }

        if matches!(self.shard, Shard::Direct(_)) {
            return Ok(Command::Query(
                Route::read(self.shard.clone()).set_write(writes),
            ));
        }

        // `SELECT NOW()`, `SELECT 1`, etc.
        if stmt.from_clause.is_empty() {
            return Ok(Command::Query(
                Route::read(Some(round_robin::next() % context.shards)).set_write(writes),
            ));
        }

        let order_by = Self::select_sort(&stmt.sort_clause, context.router_context.bind);
        let mut shards = HashSet::new();
        let the_table = Table::try_from(&stmt.from_clause).ok();
        if let Some(where_clause) =
            WhereClause::new(the_table.as_ref().map(|t| t.name), &stmt.where_clause)
        {
            shards = Self::where_clause(
                &context.sharding_schema,
                &where_clause,
                context.router_context.bind,
            )?;
        }

        // Shard by vector in ORDER BY clause.
        for order in &order_by {
            if let Some((vector, column_name)) = order.vector() {
                for table in context.sharding_schema.tables.tables() {
                    if &table.column == column_name
                        && (table.name.is_none()
                            || table.name.as_deref() == the_table.as_ref().map(|t| t.name))
                    {
                        let centroids = Centroids::from(&table.centroids);
                        shards.insert(centroids.shard(
                            vector,
                            context.shards,
                            table.centroid_probes,
                        ));
                    }
                }
            }
        }

        let shard = Self::converge(shards);
        let aggregates = Aggregate::parse(stmt)?;
        let limit = LimitClause::new(stmt, context.router_context.bind).limit_offset()?;
        let distinct = Distinct::new(stmt).distinct()?;

        let mut query = Route::select(shard, order_by, aggregates, limit, distinct);

        let mut omni = false;
        if query.is_all_shards() {
            if let Some(name) = the_table.as_ref().map(|t| t.name) {
                omni = context.sharding_schema.tables.omnishards().contains(name);
            }
        }

        if omni {
            query.set_shard_mut(round_robin::next() % context.shards);
        }

        Ok(Command::Query(query.set_write(writes)))
    }

    /// Handle the `ORDER BY` clause of a `SELECT` statement.
    ///
    /// # Arguments
    ///
    /// * `nodes`: List of pg_query-generated nodes from the ORDER BY clause.
    /// * `params`: Bind parameters, if any.
    ///
    fn select_sort(nodes: &[Node], params: Option<&Bind>) -> Vec<OrderBy> {
        let mut order_by = vec![];
        for clause in nodes {
            if let Some(NodeEnum::SortBy(ref sort_by)) = clause.node {
                let asc = matches!(sort_by.sortby_dir, 0..=2);
                let Some(ref node) = sort_by.node else {
                    continue;
                };
                let Some(ref node) = node.node else {
                    continue;
                };

                match node {
                    NodeEnum::AConst(aconst) => {
                        if let Some(Val::Ival(ref integer)) = aconst.val {
                            order_by.push(if asc {
                                OrderBy::Asc(integer.ival as usize)
                            } else {
                                OrderBy::Desc(integer.ival as usize)
                            });
                        }
                    }

                    NodeEnum::ColumnRef(column_ref) => {
                        // TODO: save the entire column and disambiguate
                        // when reading data with RowDescription as context.
                        let Some(field) = column_ref.fields.last() else {
                            continue;
                        };
                        if let Some(NodeEnum::String(ref string)) = field.node {
                            order_by.push(if asc {
                                OrderBy::AscColumn(string.sval.clone())
                            } else {
                                OrderBy::DescColumn(string.sval.clone())
                            });
                        }
                    }

                    NodeEnum::AExpr(expr) => {
                        if expr.kind() == AExprKind::AexprOp {
                            if let Some(node) = expr.name.first() {
                                if let Some(NodeEnum::String(String { sval })) = &node.node {
                                    match sval.as_str() {
                                        "<->" => {
                                            let mut vector: Option<Vector> = None;
                                            let mut column: Option<std::string::String> = None;

                                            for e in
                                                [&expr.lexpr, &expr.rexpr].iter().copied().flatten()
                                            {
                                                if let Ok(vec) = Value::try_from(&e.node) {
                                                    match vec {
                                                        Value::Placeholder(p) => {
                                                            if let Some(bind) = params {
                                                                if let Ok(Some(param)) =
                                                                    bind.parameter((p - 1) as usize)
                                                                {
                                                                    vector = param.vector();
                                                                }
                                                            }
                                                        }
                                                        Value::Vector(vec) => vector = Some(vec),
                                                        _ => (),
                                                    }
                                                };

                                                if let Ok(col) = Column::try_from(&e.node) {
                                                    column = Some(col.name.to_owned());
                                                }
                                            }

                                            if let Some(vector) = vector {
                                                if let Some(column) = column {
                                                    order_by.push(OrderBy::AscVectorL2Column(
                                                        column, vector,
                                                    ));
                                                }
                                            }
                                        }
                                        _ => continue,
                                    }
                                }
                            }
                        }
                    }

                    _ => continue,
                }
            }
        }

        order_by
    }

    /// Handle Postgres functions that could trigger the SELECT to go to a primary.
    ///
    /// # Arguments
    ///
    /// * `stmt`: SELECT statement from pg_query.
    ///
    fn functions(stmt: &SelectStmt) -> Result<FunctionBehavior, Error> {
        for target in &stmt.target_list {
            if let Ok(func) = Function::try_from(target) {
                return Ok(func.behavior());
            }
        }

        Ok(if stmt.locking_clause.is_empty() {
            FunctionBehavior::default()
        } else {
            FunctionBehavior::writes_only()
        })
    }

    /// Handle INSERT statement.
    ///
    /// # Arguments
    ///
    /// * `stmt`: INSERT statement from pg_query.
    /// * `context`: Query parser context.
    ///
    fn insert(stmt: &InsertStmt, context: &QueryParserContext) -> Result<Command, Error> {
        let insert = Insert::new(stmt);
        let shard = insert.shard(&context.sharding_schema, context.router_context.bind)?;
        Ok(Command::Query(Route::write(shard)))
    }
}

// -------------------------------------------------------------------------------------------------
// ----- QueryParser :: Command :: UPDATE ----------------------------------------------------------

impl QueryParser {
    fn update(stmt: &UpdateStmt, context: &QueryParserContext) -> Result<Command, Error> {
        let table = stmt.relation.as_ref().map(Table::from);

        let where_clause = WhereClause::new(table.map(|t| t.name), &stmt.where_clause);

        if let Some(where_clause) = where_clause {
            let shards = Self::where_clause(
                &context.sharding_schema,
                &where_clause,
                context.router_context.bind,
            )?;
            return Ok(Command::Query(Route::write(Self::converge(shards))));
        }

        Ok(Command::Query(Route::write(Shard::All)))
    }
}

// -------------------------------------------------------------------------------------------------
// ----- QueryParser :: Command :: DELETE ----------------------------------------------------------

impl QueryParser {
    fn delete(stmt: &DeleteStmt, context: &QueryParserContext) -> Result<Command, Error> {
        let table = stmt.relation.as_ref().map(Table::from);
        let where_clause = WhereClause::new(table.map(|t| t.name), &stmt.where_clause);

        if let Some(where_clause) = where_clause {
            let shards = Self::where_clause(
                &context.sharding_schema,
                &where_clause,
                context.router_context.bind,
            )?;
            return Ok(Command::Query(Route::write(Self::converge(shards))));
        }

        Ok(Command::Query(Route::write(None)))
    }
}

// -------------------------------------------------------------------------------------------------
// ----- QueryParser :: Command :: EXPLAIN & ANALYZE -----------------------------------------------

impl QueryParser {
    fn explain(
        &mut self,
        stmt: &ExplainStmt,
        context: &QueryParserContext,
    ) -> Result<Command, Error> {
        let query = stmt.query.as_ref().ok_or(Error::EmptyQuery)?;
        let node = query.node.as_ref().ok_or(Error::EmptyQuery)?;

        match node {
            NodeEnum::SelectStmt(ref stmt) => self.select(stmt, context),
            NodeEnum::InsertStmt(ref stmt) => Self::insert(stmt, context),
            NodeEnum::UpdateStmt(ref stmt) => Self::update(stmt, context),
            NodeEnum::DeleteStmt(ref stmt) => Self::delete(stmt, context),

            _ => {
                // For other statement types, route to all shards
                Ok(Command::Query(Route::write(None)))
            }
        }
    }
}

#[cfg(test)]
mod test_explain {

    use super::*;

    use crate::backend::Cluster;
    use crate::frontend::{Buffer, PreparedStatements, RouterContext};
    use crate::net::messages::{Bind, Parameter, Parse, Query};
    use crate::net::Parameters;

    // Helper function to route a plain SQL statement and return its `Route`.
    fn route(sql: &str) -> Route {
        let buffer = Buffer::from(vec![Query::new(sql).into()]);

        let cluster = Cluster::new_test();
        let mut stmts = PreparedStatements::default();
        let params = Parameters::default();

        let ctx = RouterContext::new(&buffer, &cluster, &mut stmts, &params, false).unwrap();

        match QueryParser::default().parse(ctx).unwrap().clone() {
            Command::Query(route) => route,
            _ => panic!("expected Query command"),
        }
    }

    // Helper function to route a parameterized SQL statement and return its `Route`.
    fn route_parameterized(sql: &str, values: &[&[u8]]) -> Route {
        let parse_msg = Parse::new_anonymous(sql);
        let parameters = values
            .iter()
            .map(|v| Parameter {
                len: v.len() as i32,
                data: v.to_vec(),
            })
            .collect::<Vec<_>>();

        let bind = Bind::new_params("", &parameters);
        let buffer: Buffer = vec![parse_msg.into(), bind.into()].into();

        let cluster = Cluster::new_test();
        let mut stmts = PreparedStatements::default();
        let params = Parameters::default();

        let ctx = RouterContext::new(&buffer, &cluster, &mut stmts, &params, false).unwrap();

        match QueryParser::default().parse(ctx).unwrap().clone() {
            Command::Query(route) => route,
            _ => panic!("expected Query command"),
        }
    }

    #[test]
    #[should_panic(expected = "called `Result::unwrap()`")]
    fn test_explain_empty_query() {
        // explain() returns an EmptyQuery error
        // route() panics on error unwraps.
        let _ = route("EXPLAIN");
    }

    #[test]
    fn test_explain_select_no_tables() {
        let r = route("EXPLAIN SELECT NOW()");
        assert!(matches!(r.shard(), Shard::Direct(_)));
        assert!(r.is_read());
    }

    #[test]
    fn test_explain_select_with_sharding_key() {
        let r = route("EXPLAIN SELECT * FROM sharded WHERE id = 1");
        assert!(matches!(r.shard(), Shard::Direct(_)));
        assert!(r.is_read());

        let r = route_parameterized("EXPLAIN SELECT * FROM sharded WHERE id = $1", &[b"11"]);
        assert!(matches!(r.shard(), Shard::Direct(_)));
        assert!(r.is_read());
    }

    #[test]
    fn test_explain_select_all_shards() {
        let r = route("EXPLAIN SELECT * FROM sharded");
        assert_eq!(r.shard(), &Shard::All);
        assert!(r.is_read());
    }

    #[test]
    fn test_explain_insert() {
        let r = route_parameterized(
            "EXPLAIN INSERT INTO sharded (id, email) VALUES ($1, $2)",
            &[b"11", b"test@test.com"],
        );
        assert!(matches!(r.shard(), Shard::Direct(_)));
        assert!(r.is_write());
    }

    #[test]
    fn test_explain_update() {
        let r = route_parameterized(
            "EXPLAIN UPDATE sharded SET email = $2 WHERE id = $1",
            &[b"11", b"new@test.com"],
        );
        assert!(matches!(r.shard(), Shard::Direct(_)));
        assert!(r.is_write());

        let r = route("EXPLAIN UPDATE sharded SET active = true");
        assert_eq!(r.shard(), &Shard::All);
        assert!(r.is_write());
    }

    #[test]
    fn test_explain_delete() {
        let r = route_parameterized("EXPLAIN DELETE FROM sharded WHERE id = $1", &[b"11"]);
        assert!(matches!(r.shard(), Shard::Direct(_)));
        assert!(r.is_write());

        let r = route("EXPLAIN DELETE FROM sharded");
        assert_eq!(r.shard(), &Shard::All);
        assert!(r.is_write());
    }

    #[test]
    fn test_explain_with_options() {
        let r = route("EXPLAIN (ANALYZE, BUFFERS) SELECT * FROM sharded WHERE id = 1");
        assert!(matches!(r.shard(), Shard::Direct(_)));
        assert!(r.is_read());

        let r = route("EXPLAIN (FORMAT JSON) SELECT * FROM sharded WHERE id = 1");
        assert!(matches!(r.shard(), Shard::Direct(_)));
        assert!(r.is_read());
    }

    #[test]
    fn test_explain_with_comment_override() {
        let r = route("/* pgdog_shard: 5 */ EXPLAIN SELECT * FROM sharded");
        assert_eq!(r.shard(), &Shard::Direct(5));
    }

    #[test]
    fn test_explain_analyze_insert() {
        let r = route("EXPLAIN ANALYZE INSERT INTO sharded (id, email) VALUES (1, 'a@a.com')");
        assert!(matches!(r.shard(), Shard::Direct(_)));
        assert!(r.is_write());

        let r = route_parameterized(
            "EXPLAIN ANALYZE INSERT INTO sharded (id, email) VALUES ($1, $2)",
            &[b"1", b"test@test.com"],
        );
        assert!(matches!(r.shard(), Shard::Direct(_)));
        assert!(r.is_write());
    }

    #[test]
    fn test_explain_analyze_update() {
        let r = route("EXPLAIN ANALYZE UPDATE sharded SET active = true");
        assert_eq!(r.shard(), &Shard::All);
        assert!(r.is_write());

        let r = route_parameterized(
            "EXPLAIN ANALYZE UPDATE sharded SET email = $2",
            &[b"everyone@same.com"],
        );
        assert_eq!(r.shard(), &Shard::All);
        assert!(r.is_write());

        // Test with sharding key
        let r = route_parameterized(
            "EXPLAIN ANALYZE UPDATE sharded SET email = $2 WHERE id = $1",
            &[b"1", b"new@test.com"],
        );
        assert!(matches!(r.shard(), Shard::Direct(_)));
        assert!(r.is_write());
    }

    #[test]
    fn test_explain_analyze_delete() {
        let r = route("EXPLAIN ANALYZE DELETE FROM sharded");
        assert_eq!(r.shard(), &Shard::All);
        assert!(r.is_write());

        // Test with non-sharding key
        let r = route_parameterized(
            "EXPLAIN ANALYZE DELETE FROM sharded WHERE active = $1",
            &[b"false"],
        );
        assert_eq!(r.shard(), &Shard::All);
        assert!(r.is_write());

        // Test with sharding key
        let r = route_parameterized("EXPLAIN ANALYZE DELETE FROM sharded WHERE id = $1", &[b"1"]);
        assert!(matches!(r.shard(), Shard::Direct(_)));
        assert!(r.is_write());
    }
}

// -------------------------------------------------------------------------------------------------
// ----- QueryParser :: Routing Overrides ----------------------------------------------------------

impl QueryParser {
    fn converge(shards: HashSet<Shard>) -> Shard {
        let shard = if shards.len() == 1 {
            shards.iter().next().cloned().unwrap()
        } else {
            let mut multi = vec![];
            let mut all = false;
            for shard in &shards {
                match shard {
                    Shard::All => {
                        all = true;
                        break;
                    }
                    Shard::Direct(v) => multi.push(*v),
                    Shard::Multi(m) => multi.extend(m),
                };
            }
            if all || shards.is_empty() {
                Shard::All
            } else {
                Shard::Multi(multi)
            }
        };

        shard
    }

    fn where_clause(
        sharding_schema: &ShardingSchema,
        where_clause: &WhereClause,
        params: Option<&Bind>,
    ) -> Result<HashSet<Shard>, Error> {
        let mut shards = HashSet::new();
        // Complexity: O(number of sharded tables * number of columns in the query)
        for table in sharding_schema.tables().tables() {
            let table_name = table.name.as_deref();
            let keys = where_clause.keys(table_name, &table.column);
            for key in keys {
                match key {
                    Key::Constant { value, array } => {
                        if array {
                            shards.insert(Shard::All);
                            break;
                        }

                        let ctx = ContextBuilder::new(table)
                            .data(value.as_str())
                            .shards(sharding_schema.shards)
                            .build()?;
                        shards.insert(ctx.apply()?);
                    }

                    Key::Parameter { pos, array } => {
                        // Don't hash individual values yet.
                        // The odds are high this will go to all shards anyway.
                        if array {
                            shards.insert(Shard::All);
                            break;
                        } else if let Some(params) = params {
                            if let Some(param) = params.parameter(pos)? {
                                let value = ShardingValue::from_param(&param, table.data_type)?;
                                let ctx = ContextBuilder::new(table)
                                    .value(value)
                                    .shards(sharding_schema.shards)
                                    .build()?;
                                shards.insert(ctx.apply()?);
                            }
                        }
                    }

                    // Null doesn't help.
                    Key::Null => (),
                }
            }
        }

        Ok(shards)
    }

    fn cte_writes(stmt: &SelectStmt) -> bool {
        if let Some(ref with_clause) = stmt.with_clause {
            for cte in &with_clause.ctes {
                if let Some(ref node) = cte.node {
                    if let NodeEnum::CommonTableExpr(expr) = node {
                        if let Some(ref query) = expr.ctequery {
                            if let Some(ref node) = query.node {
                                match node {
                                    NodeEnum::SelectStmt(stmt) => {
                                        if Self::cte_writes(stmt) {
                                            return true;
                                        }
                                    }

                                    _ => {
                                        return true;
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        false
    }
}

// -------------------------------------------------------------------------------------------------
// ----- QueryParser :: Transaction Control --------------------------------------------------------

// -> TODO

// -------------------------------------------------------------------------------------------------
// ----- QueryParser :: Module Tests ---------------------------------------------------------------

#[cfg(test)]
mod test {

    use crate::net::{
        messages::{parse::Parse, Parameter},
        Close, Format, Sync,
    };

    use super::{super::Shard, *};
    use crate::backend::Cluster;
    use crate::config::ReadWriteStrategy;
    use crate::frontend::{Buffer, PreparedStatements, RouterContext};
    use crate::net::messages::Query;
    use crate::net::Parameters;

    macro_rules! command {
        ($query:expr) => {{
            let query = $query;
            let mut query_parser = QueryParser::default();
            let buffer = Buffer::from(vec![Query::new(query).into()]);
            let cluster = Cluster::new_test();
            let mut stmt = PreparedStatements::default();
            let params = Parameters::default();
            let context = RouterContext::new(&buffer, &cluster, &mut stmt, &params, false).unwrap();
            let command = query_parser.parse(context).unwrap().clone();

            (command, query_parser)
        }};
    }

    macro_rules! query {
        ($query:expr) => {{
            let query = $query;
            let (command, _) = command!(query);

            match command {
                Command::Query(query) => query,

                _ => panic!("should be a query"),
            }
        }};
    }

    macro_rules! parse {
        ($query: expr, $params: expr) => {
            parse!("", $query, $params)
        };

        ($name:expr, $query:expr, $params:expr, $codes:expr) => {{
            let parse = Parse::named($name, $query);
            let params = $params
                .into_iter()
                .map(|p| Parameter {
                    len: p.len() as i32,
                    data: p.to_vec(),
                })
                .collect::<Vec<_>>();
            let bind = Bind::new_params_codes($name, &params, $codes);
            let route = QueryParser::default()
                .parse(
                    RouterContext::new(
                        &Buffer::from(vec![parse.into(), bind.into()]),
                        &Cluster::new_test(),
                        &mut PreparedStatements::default(),
                        &Parameters::default(),
                        false,
                    )
                    .unwrap(),
                )
                .unwrap()
                .clone();

            match route {
                Command::Query(query) => query,

                _ => panic!("should be a query"),
            }
        }};

        ($name:expr, $query:expr, $params: expr) => {
            parse!($name, $query, $params, &[])
        };
    }

    #[test]
    fn test_insert() {
        let route = parse!(
            "INSERT INTO sharded (id, email) VALUES ($1, $2)",
            ["11".as_bytes(), "test@test.com".as_bytes()]
        );
        assert_eq!(route.shard(), &Shard::direct(1));
    }

    #[test]
    fn test_order_by_vector() {
        let route = query!("SELECT * FROM embeddings ORDER BY embedding <-> '[1,2,3]'");
        let order_by = route.order_by().first().unwrap();
        assert!(order_by.asc());
        assert_eq!(
            order_by.vector().unwrap(),
            (
                &Vector::from(&[1.0, 2.0, 3.0][..]),
                &std::string::String::from("embedding")
            ),
        );

        let route = parse!(
            "SELECT * FROM embeddings ORDER BY embedding  <-> $1",
            ["[4.0,5.0,6.0]".as_bytes()]
        );
        let order_by = route.order_by().first().unwrap();
        assert!(order_by.asc());
        assert_eq!(
            order_by.vector().unwrap(),
            (
                &Vector::from(&[4.0, 5.0, 6.0][..]),
                &std::string::String::from("embedding")
            )
        );
    }

    #[test]
    fn test_parse_with_cast() {
        let route = parse!(
            "test",
            r#"SELECT sharded.id, sharded.value
    FROM sharded
    WHERE sharded.id = $1::INTEGER ORDER BY sharded.id"#,
            [[0, 0, 0, 1]],
            &[Format::Binary]
        );
        assert!(route.is_read());
        assert_eq!(route.shard(), &Shard::Direct(0))
    }

    #[test]
    fn test_select_for_update() {
        let route = query!("SELECT * FROM sharded WHERE id = $1 FOR UPDATE");
        assert!(route.is_write());
        assert!(matches!(route.shard(), Shard::All));
        let route = parse!(
            "SELECT * FROM sharded WHERE id = $1 FOR UPDATE",
            ["1".as_bytes()]
        );
        assert!(matches!(route.shard(), Shard::Direct(_)));
        assert!(route.is_write());
    }

    #[test]
    fn test_omni() {
        let q = "SELECT sharded_omni.* FROM sharded_omni WHERE sharded_omni.id = $1";
        let route = query!(q);
        assert!(matches!(route.shard(), Shard::Direct(_)));
        let (_, qp) = command!(q);
        assert!(!qp.in_transaction);
    }

    #[test]
    fn test_set() {
        let route = query!(r#"SET "pgdog.shard" TO 1"#);
        assert_eq!(route.shard(), &Shard::Direct(1));
        let (_, qp) = command!(r#"SET "pgdog.shard" TO 1"#);
        assert!(!qp.in_transaction);

        let route = query!(r#"SET "pgdog.sharding_key" TO '11'"#);
        assert_eq!(route.shard(), &Shard::Direct(1));
        let (_, qp) = command!(r#"SET "pgdog.sharding_key" TO '11'"#);
        assert!(!qp.in_transaction);

        for (command, qp) in [
            command!("SET TimeZone TO 'UTC'"),
            command!("SET TIME ZONE 'UTC'"),
        ] {
            match command {
                Command::Set { name, value } => {
                    assert_eq!(name, "timezone");
                    assert_eq!(value, ParameterValue::from("UTC"));
                }
                _ => panic!("not a set"),
            };
            assert!(!qp.in_transaction);
        }

        let (command, qp) = command!("SET statement_timeout TO 3000");
        match command {
            Command::Set { name, value } => {
                assert_eq!(name, "statement_timeout");
                assert_eq!(value, ParameterValue::from("3000"));
            }
            _ => panic!("not a set"),
        };
        assert!(!qp.in_transaction);

        // TODO: user shouldn't be able to set these.
        // The server will report an error on synchronization.
        let (command, qp) = command!("SET is_superuser TO true");
        match command {
            Command::Set { name, value } => {
                assert_eq!(name, "is_superuser");
                assert_eq!(value, ParameterValue::from("true"));
            }
            _ => panic!("not a set"),
        };
        assert!(!qp.in_transaction);

        let (_, mut qp) = command!("BEGIN");
        assert!(qp.write_override);
        let command = qp
            .parse(
                RouterContext::new(
                    &vec![Query::new(r#"SET statement_timeout TO 3000"#).into()].into(),
                    &Cluster::new_test(),
                    &mut PreparedStatements::default(),
                    &Parameters::default(),
                    true,
                )
                .unwrap(),
            )
            .unwrap();
        match command {
            Command::Query(q) => assert!(q.is_write()),
            _ => panic!("set should trigger binding"),
        }

        let (command, _) = command!("SET search_path TO \"$user\", public, \"APPLES\"");
        match command {
            Command::Set { name, value } => {
                assert_eq!(name, "search_path");
                assert_eq!(
                    value,
                    ParameterValue::Tuple(vec!["$user".into(), "public".into(), "APPLES".into()])
                )
            }
            _ => panic!("search path"),
        }

        let ast = pg_query::parse("SET statement_timeout TO 1").unwrap();
        let mut qp = QueryParser {
            in_transaction: true,
            ..Default::default()
        };

        let root = ast.protobuf.stmts.first().unwrap().stmt.as_ref().unwrap();
        match root.node.as_ref() {
            Some(NodeEnum::VariableSetStmt(stmt)) => {
                for read_only in [true, false] {
                    let route = qp.set(stmt, &ShardingSchema::default(), read_only).unwrap();
                    match route {
                        Command::Query(route) => {
                            assert_eq!(route.is_read(), read_only);
                        }
                        _ => panic!("not a query"),
                    }
                }
            }

            _ => panic!("not a set"),
        }
    }

    #[test]
    fn test_transaction() {
        let (command, mut qp) = command!("BEGIN");
        match command {
            Command::StartTransaction(q) => assert_eq!(q.query(), "BEGIN"),
            _ => panic!("not a query"),
        };

        assert!(qp.in_transaction);
        assert!(qp.write_override);

        let buffer: Buffer = vec![Parse::named("test", "SELECT $1").into()].into();
        let cluster = Cluster::new_test();
        let mut prep_stmts = PreparedStatements::default();
        let params = Parameters::default();

        // qp.in_transaction = true;
        let router_context =
            RouterContext::new(&buffer, &cluster, &mut prep_stmts, &params, true).unwrap();
        let mut context = QueryParserContext::new(router_context);
        let route = qp.query(&mut context).unwrap();
        match route {
            Command::Query(q) => assert!(q.is_write()),
            _ => panic!("not a select"),
        }

        let mut cluster = Cluster::new_test();
        cluster.set_read_write_strategy(ReadWriteStrategy::Aggressive);

        let buffer: Buffer = vec![Query::new("BEGIN").into()].into();
        let mut qp = QueryParser::default();
        let router_context =
            RouterContext::new(&buffer, &cluster, &mut prep_stmts, &params, true).unwrap();
        let mut context = QueryParserContext::new(router_context);
        let command = qp.query(&mut context).unwrap();
        assert!(matches!(
            command,
            Command::StartTransaction(BufferedQuery::Query(_))
        ));
        assert!(qp.in_transaction);

        qp.in_transaction = true;
        let buffer: Buffer = vec![Query::new("SET application_name TO 'test'").into()].into();
        let router_context =
            RouterContext::new(&buffer, &cluster, &mut prep_stmts, &params, true).unwrap();
        let mut context = QueryParserContext::new(router_context);
        let route = qp.query(&mut context).unwrap();

        match route {
            Command::Query(q) => {
                assert!(q.is_write());
                assert!(!cluster.read_only());
            }

            _ => panic!("not a query"),
        }
    }

    #[test]
    fn test_insert_do_update() {
        let route = query!("INSERT INTO foo (id) VALUES ($1::UUID) ON CONFLICT (id) DO UPDATE SET id = excluded.id RETURNING id");
        assert!(route.is_write())
    }

    #[test]
    fn test_begin_extended() {
        let mut qr = QueryParser::default();
        let result = qr
            .parse(
                RouterContext::new(
                    &vec![crate::net::Parse::new_anonymous("BEGIN").into()].into(),
                    &Cluster::new_test(),
                    &mut PreparedStatements::default(),
                    &Parameters::default(),
                    false,
                )
                .unwrap(),
            )
            .unwrap();
        assert!(matches!(result, Command::Query(_)));
    }

    #[test]
    fn test_show_shards() {
        let (cmd, qp) = command!("SHOW pgdog.shards");
        assert!(matches!(cmd, Command::Shards(2)));
        assert!(!qp.in_transaction);
    }

    #[test]
    fn test_write_functions() {
        let route = query!("SELECT pg_advisory_lock($1)");
        assert!(route.is_write());
        assert!(route.lock_session());
    }

    #[test]
    fn test_write_nolock() {
        let route = query!("SELECT nextval('234')");
        assert!(route.is_write());
        assert!(!route.lock_session());
    }

    #[test]
    fn test_cte() {
        let route = query!("WITH s AS (SELECT 1) SELECT 2");
        assert!(route.is_read());

        let route = query!("WITH s AS (SELECT 1), s2 AS (INSERT INTO test VALUES ($1) RETURNING *), s3 AS (SELECT 123) SELECT * FROM s");
        assert!(route.is_write());
    }

    #[test]
    fn test_function_begin() {
        let (cmd, mut qp) = command!("BEGIN");
        assert!(matches!(cmd, Command::StartTransaction(_)));
        assert!(qp.in_transaction);
        let cluster = Cluster::new_test();
        let mut prep_stmts = PreparedStatements::default();
        let params = Parameters::default();
        let buffer: Buffer = vec![Query::new(
            "SELECT
	ROW(t1.*) AS tt1,
	ROW(t2.*) AS tt2
FROM t1
LEFT JOIN t2 ON t1.id = t2.t1_id
WHERE t2.account = (
	SELECT
		account
	FROM
		t2
	WHERE
		t2.id = $1
	)
	",
        )
        .into()]
        .into();
        let router_context =
            RouterContext::new(&buffer, &cluster, &mut prep_stmts, &params, true).unwrap();
        let mut context = QueryParserContext::new(router_context);
        let route = qp.query(&mut context).unwrap();
        match route {
            Command::Query(query) => assert!(query.is_write()),
            _ => panic!("not a select"),
        }
        assert!(qp.in_transaction);
    }

    #[test]
    fn test_comment() {
        let query = "/* pgdog_shard: 1234 */ SELECT 1234";
        let route = query!(query);
        assert_eq!(route.shard(), &Shard::Direct(1234));

        // Comment is ignored.
        let mut qp = QueryParser::default();
        let cluster = Cluster::new_test();
        let mut prep_stmts = PreparedStatements::default();
        let params = Parameters::default();
        let buffer: Buffer =
            vec![Query::new("/* pgdog_shard: 1234 */ SELECT * FROM sharded WHERE id = $1").into()]
                .into();
        let router_context =
            RouterContext::new(&buffer, &cluster, &mut prep_stmts, &params, false).unwrap();
        let mut context = QueryParserContext::new(router_context);
        let command = qp.query(&mut context).unwrap();

        match command {
            Command::Query(query) => assert_eq!(query.shard(), &Shard::Direct(1234)),
            _ => panic!("not a query"),
        }
    }

    #[test]
    fn test_limit_offset() {
        let route = query!("SELECT * FROM users LIMIT 25 OFFSET 5");
        assert_eq!(route.limit().offset, Some(5));
        assert_eq!(route.limit().limit, Some(25));

        let cmd = parse!(
            "SELECT * FROM users LIMIT $1 OFFSET $2",
            &["1".as_bytes(), "25".as_bytes(),]
        );

        assert_eq!(cmd.limit().limit, Some(1));
        assert_eq!(cmd.limit().offset, Some(25));
    }

    #[test]
    fn test_close_direct_one_shard() {
        let cluster = Cluster::new_test_single_shard();
        let mut qp = QueryParser::default();

        let buf: Buffer = vec![Close::named("test").into(), Sync.into()].into();
        let mut pp = PreparedStatements::default();
        let params = Parameters::default();

        let context = RouterContext::new(&buf, &cluster, &mut pp, &params, false).unwrap();

        let cmd = qp.parse(context).unwrap();

        match cmd {
            Command::Query(route) => assert_eq!(route.shard(), &Shard::Direct(0)),
            _ => panic!("not a query"),
        }
    }

    #[test]
    fn test_distinct() {
        let route = query!("SELECT DISTINCT * FROM users");
        let distinct = route.distinct().as_ref().unwrap();
        assert_eq!(distinct, &DistinctBy::Row);

        let route = query!("SELECT DISTINCT ON(1, email) * FROM users");
        let distinct = route.distinct().as_ref().unwrap();
        assert_eq!(
            distinct,
            &DistinctBy::Columns(vec![
                DistinctColumn::Index(0),
                DistinctColumn::Name(std::string::String::from("email"))
            ])
        );
    }

    #[test]
    fn test_any() {
        let route = query!("SELECT * FROM sharded WHERE id = ANY('{1, 2, 3}')");
        assert_eq!(route.shard(), &Shard::All);

        let route = parse!(
            "SELECT * FROM sharded WHERE id = ANY($1)",
            &["{1, 2, 3}".as_bytes()]
        );

        assert_eq!(route.shard(), &Shard::All);
    }
}

// -------------------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------------------
