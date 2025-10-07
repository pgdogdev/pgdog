use crate::frontend::router::parser::where_clause::TablesSource;

use super::*;

impl QueryParser {
    pub(super) fn update(
        &mut self,
        stmt: &UpdateStmt,
        context: &QueryParserContext,
    ) -> Result<Command, Error> {
        let table = stmt.relation.as_ref().map(Table::from);

        if let Some(table) = table {
            let source = TablesSource::from(table);
            let where_clause = WhereClause::new(&source, &stmt.where_clause);

            if let Some(where_clause) = where_clause {
                let shards = Self::where_clause(
                    &context.sharding_schema,
                    &where_clause,
                    context.router_context.bind,
                    &mut self.explain_recorder,
                )?;
                let shard = Self::converge(shards);
                if let Some(recorder) = self.recorder_mut() {
                    recorder.record_entry(
                        Some(shard.clone()),
                        "UPDATE matched WHERE clause for sharding key",
                    );
                }
                return Ok(Command::Query(Route::write(shard)));
            }
        }

        if let Some(recorder) = self.recorder_mut() {
            recorder.record_entry(None, "UPDATE fell back to broadcast");
        }
        Ok(Command::Query(Route::write(Shard::All)))
    }
}
