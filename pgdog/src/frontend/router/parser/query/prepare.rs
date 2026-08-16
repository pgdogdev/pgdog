use pg_raw_parse::nodes::{ExecuteStmt, PrepareStmt};

use crate::{
    frontend::{BufferedQuery, PreparedStatements},
    net::{PREPARE_TEMPLATE_NAME, Query},
};

use super::*;

impl QueryParser {
    // A `PREPARE` statement can be sent to any shard.
    //
    // The only distinction we make here is between reads and writes: `SELECT` queries are sent
    // to a replica, while everything else is prepared on the primary.
    //
    pub(super) fn prepare(
        &self,
        stmt: &PrepareStmt,
        // context: &mut QueryParserContext<'_>,
    ) -> Result<Command, Error> {
        let query = stmt.query();

        let route = match query {
            Node::SelectStmt(_) => Route::read(ShardWithPriority::new_rr_not_executable(
                round_robin::next().into(),
            )),
            _ => Route::write(ShardWithPriority::new_rr_not_executable(
                round_robin::next().into(),
            )),
        };

        Ok(Command::Query(route))
    }

    /// An `EXECUTE` statement.
    pub(super) fn execute(
        &mut self,
        stmt: &ExecuteStmt,
        context: &mut QueryParserContext<'_>,
    ) -> Result<Command, Error> {
        // Extract parameters from `EXECUTE` statement.
        let params = ExecuteParams::new(stmt);
        let stmt_params = StatementParameters::Execute(&params);

        // Create new parser context.
        let mut context = context.clone();
        context.router_context.bind = Some(stmt_params);

        // Get the original query from the prepared
        // statements cache.
        //
        // INVARIANT 1: The prepared statements rewriter places it there.
        // INVARIANT 2: The rewriter renamed the EXECUTE statement to its global name.
        let prepare = PreparedStatements::global()
            .read()
            .prepare(stmt.name().expect("execute to have a name"))
            .ok_or(Error::ExecuteRequiresFull)?;

        // Make sure we never store unique statement names
        // in the cache!
        debug_assert!(prepare.query().contains(PREPARE_TEMPLATE_NAME));

        let query = BufferedQuery::Query(Query::new(prepare.query()));
        let ast = Cache::get().record(&query)?;
        let stmt = match ast.ast.stmts().next() {
            Some(Node::PrepareStmt(stmt)) => Some(stmt.query()),
            stmt => stmt,
        };

        match stmt {
            Some(Node::SelectStmt(stmt)) => self.select(&ast, stmt, &mut context),
            Some(Node::InsertStmt(stmt)) => self.insert(stmt.into(), &mut context),
            Some(Node::UpdateStmt(stmt)) => self.update(stmt.into(), &mut context),
            Some(Node::DeleteStmt(stmt)) => self.delete(stmt.into(), &mut context),
            _ => Ok(Command::Query(Route::write(
                ShardWithPriority::new_default_unset(Shard::All),
            ))),
        }
    }
}
