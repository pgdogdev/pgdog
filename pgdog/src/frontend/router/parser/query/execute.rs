//! Routing for SQL-level `PREPARE` and `EXECUTE` statements.

use tracing::warn;

use crate::frontend::BufferedQuery;
use crate::net::Parse;

use super::*;

impl QueryParser {
    /// Route a SQL-level `PREPARE` statement.
    ///
    /// It's broadcast to all shards. The statement behind the name is
    /// stored in the prepared statements cache, so `EXECUTE` can be
    /// routed based on it.
    pub(super) fn prepare_statement(
        stmt: &nodes::PrepareStmt,
        context: &mut QueryParserContext,
    ) -> Result<Command, Error> {
        if let Some(name) = stmt.name()
            && let Some(prepared_statements) =
                context.router_context.prepared_statements.as_deref_mut()
            // First PREPARE wins: a duplicate fails on the server,
            // which keeps the original statement.
            && prepared_statements.name(name).is_none()
        {
            match pg_raw_parse::deparse(stmt.query()) {
                Ok(query) => {
                    let mut parse = Parse::named(name, query.as_str());
                    prepared_statements.insert(&mut parse);
                }
                Err(err) => {
                    warn!("failed to record PREPARE statement: {}", err);
                }
            }
        }

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
    /// well. If the statement behind the name is a write that only touches
    /// omnisharded tables, mark the route, so results are deduplicated
    /// across shards instead of aggregated, e.g. `UPDATE <rows>` reports
    /// the row count from one shard, not the sum of all of them.
    pub(super) fn execute_prepared(
        stmt: &nodes::ExecuteStmt,
        context: &mut QueryParserContext,
    ) -> Result<Command, Error> {
        let omnisharded = Self::executed_statement_omnisharded(stmt, context);

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

    /// Check if the statement behind an `EXECUTE` name is a write that
    /// only touches omnisharded tables.
    ///
    /// `PREPARE` accepts SELECT, INSERT, UPDATE, DELETE, MERGE and VALUES.
    /// Only writes are flagged: `EXECUTE` always routes as a write, and the
    /// omnisharded flag on a write requires full shard coverage, which
    /// would reject shard directives on read-only statements. MERGE is
    /// left out conservatively; its row counts keep aggregating.
    fn executed_statement_omnisharded(
        stmt: &nodes::ExecuteStmt,
        context: &mut QueryParserContext,
    ) -> bool {
        if context.sharding_schema.tables.omnishards().is_empty() {
            return false;
        }

        let Some(name) = stmt.name() else {
            return false;
        };
        let Some(prepared_statements) = context.router_context.prepared_statements.as_deref_mut()
        else {
            return false;
        };
        let Some(parse) = prepared_statements.parse(name) else {
            return false;
        };

        // The statement cache parses each unique statement once,
        // not on every EXECUTE.
        let ast_context = AstContext {
            sharding_schema: context.sharding_schema.clone(),
            db_schema: context.router_context.schema.clone(),
            user: context.router_context.cluster.user(),
            search_path: context.router_context.parameter_hints.search_path,
        };
        let Ok(ast) = Cache::get().query(
            &BufferedQuery::Prepared(parse),
            &ast_context,
            prepared_statements,
        ) else {
            return false;
        };

        let Some(root) = ast.ast.stmts().next() else {
            return false;
        };
        if !matches!(
            root,
            Node::InsertStmt(_) | Node::UpdateStmt(_) | Node::DeleteStmt(_)
        ) {
            return false;
        }

        StatementParser::new(root, None, &context.sharding_schema, None).is_all_omnisharded()
    }
}
