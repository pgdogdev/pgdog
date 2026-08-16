use pg_raw_parse::nodes::{ExecuteStmt, PrepareStmt};

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

    pub(super) fn execute(
        &self,
        stmt: &ExecuteStmt,
        context: &mut QueryParserContext<'_>,
    ) -> Result<Command, Error> {
        todo!()
    }
}
