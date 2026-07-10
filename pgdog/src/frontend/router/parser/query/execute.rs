//! Routing for SQL-level `PREPARE` and `EXECUTE` statements.

#[cfg(not(feature = "new_parser"))]
use pg_query::parse;

use crate::frontend::PreparedStatements;

use super::*;

impl QueryParser {
    /// Route a SQL-level `PREPARE` statement.
    ///
    /// It's broadcast to all shards. The statement behind the name is
    /// recorded, so `EXECUTE` can be routed based on it.
    #[cfg(feature = "new_parser")]
    pub(super) fn prepare_statement(
        stmt: &nodes::PrepareStmt,
        context: &mut QueryParserContext,
    ) -> Result<Command, Error> {
        if let Some(name) = stmt.name()
            && let Ok(query) = pg_raw_parse::deparse(stmt.query())
            && let Some(prepared_statements) =
                context.router_context.prepared_statements.as_deref_mut()
        {
            prepared_statements.insert_simple(&name, query.as_str());
        }

        Self::prepare_route(context)
    }

    /// Route a SQL-level `PREPARE` statement.
    ///
    /// It's broadcast to all shards. The statement behind the name is
    /// recorded, so `EXECUTE` can be routed based on it.
    #[cfg(not(feature = "new_parser"))]
    pub(super) fn prepare_statement(
        stmt: &PrepareStmt,
        context: &mut QueryParserContext,
    ) -> Result<Command, Error> {
        use pgdog_config::QueryParserEngine;

        if let Some(query) = &stmt.query
            && let Ok(query) = match context.sharding_schema.query_parser_engine {
                QueryParserEngine::PgQueryProtobuf => query.deparse(),
                QueryParserEngine::PgQueryRaw => query.deparse_raw(),
            }
            && let Some(prepared_statements) =
                context.router_context.prepared_statements.as_deref_mut()
        {
            prepared_statements.insert_simple(&stmt.name, &query);
        }

        Self::prepare_route(context)
    }

    fn prepare_route(context: &mut QueryParserContext) -> Result<Command, Error> {
        context
            .shards_calculator
            .push(ShardWithPriority::new_table(Shard::All));

        Ok(Command::Query(Route::write(
            context.shards_calculator.shard(),
        )))
    }

    /// Route `EXECUTE <name>` of a server-side prepared statement.
    ///
    /// `PREPARE` is broadcast to all shards, so `EXECUTE` is broadcast as
    /// well. If the statement behind the name only touches omnisharded
    /// tables, mark the route, so results are deduplicated across shards
    /// instead of aggregated, e.g. `UPDATE <rows>` reports the row count
    /// from one shard, not the sum of all of them.
    #[cfg(feature = "new_parser")]
    pub(super) fn execute_prepared(
        stmt: &nodes::ExecuteStmt,
        context: &mut QueryParserContext,
    ) -> Result<Command, Error> {
        Self::execute_route(stmt.name().unwrap_or_default(), context)
    }

    /// Route `EXECUTE <name>` of a server-side prepared statement.
    ///
    /// `PREPARE` is broadcast to all shards, so `EXECUTE` is broadcast as
    /// well. If the statement behind the name only touches omnisharded
    /// tables, mark the route, so results are deduplicated across shards
    /// instead of aggregated, e.g. `UPDATE <rows>` reports the row count
    /// from one shard, not the sum of all of them.
    #[cfg(not(feature = "new_parser"))]
    pub(super) fn execute_prepared(
        stmt: &ExecuteStmt,
        context: &mut QueryParserContext,
    ) -> Result<Command, Error> {
        Self::execute_route(&stmt.name, context)
    }

    fn execute_route(name: &str, context: &mut QueryParserContext) -> Result<Command, Error> {
        let omnisharded = Self::prepared_statement_omnisharded(name, context);

        let shard = if omnisharded {
            ShardWithPriority::new_table_omni(Shard::All)
        } else {
            ShardWithPriority::new_table(Shard::All)
        };
        context.shards_calculator.push(shard);

        Ok(Command::Query(
            Route::write(context.shards_calculator.shard()).with_omnisharded(omnisharded),
        ))
    }

    /// Check if the query behind a prepared statement name only touches
    /// omnisharded tables.
    fn prepared_statement_omnisharded(name: &str, context: &QueryParserContext) -> bool {
        let schema = &context.sharding_schema;
        if schema.tables.omnishards().is_empty() {
            return false;
        }

        // SQL-level PREPARE, recorded when it was routed.
        if let Some(prepared_statements) = context.router_context.prepared_statements.as_deref()
            && let Some(query) = prepared_statements.simple_query(name)
        {
            return Self::statement_omnisharded(query, schema);
        }

        // Globally cached statement, e.g. renamed by the `prepared_statements = "full"`
        // rewrite. Copy the query so the cache isn't locked while we parse it.
        let query = {
            let global = PreparedStatements::global();
            let guard = global.read();
            match guard.query(name) {
                Some(query) => query.to_string(),
                None => return false,
            }
        };

        Self::statement_omnisharded(&query, schema)
    }

    #[cfg(not(feature = "new_parser"))]
    fn statement_omnisharded(query: &str, schema: &ShardingSchema) -> bool {
        let Ok(ast) = parse(query) else {
            return false;
        };
        let Some(root) = ast
            .protobuf
            .stmts
            .first()
            .and_then(|stmt| stmt.stmt.as_ref())
            .and_then(|stmt| stmt.node.as_ref())
        else {
            return false;
        };

        let mut parser = match root {
            NodeEnum::SelectStmt(stmt) => StatementParser::from_select(stmt, None, schema, None),
            NodeEnum::UpdateStmt(stmt) => StatementParser::from_update(stmt, None, schema, None),
            NodeEnum::DeleteStmt(stmt) => StatementParser::from_delete(stmt, None, schema, None),
            NodeEnum::InsertStmt(stmt) => StatementParser::from_insert(stmt, None, schema, None),
            _ => return false,
        };

        parser.is_all_omnisharded()
    }

    #[cfg(feature = "new_parser")]
    fn statement_omnisharded(query: &str, schema: &ShardingSchema) -> bool {
        let Ok(ast) = pg_raw_parse::parse(query) else {
            return false;
        };
        let Some(node) = ast.stmts().next() else {
            return false;
        };

        StatementParser::from_select(node, None, schema, None).is_all_omnisharded()
    }
}
