use super::StatementParser;
use super::*;

impl QueryParser {
    pub(super) fn delete(
        &mut self,
        stmt: &DeleteStmt,
        context: &QueryParserContext,
    ) -> Result<Command, Error> {
        let shard = StatementParser::from_delete(
            stmt,
            context.router_context.bind,
            &context.sharding_schema,
            self.recorder_mut(),
        )
        .shard()?;

        let shard = match shard {
            Some(shard) => {
                if let Some(recorder) = self.recorder_mut() {
                    recorder.record_entry(
                        Some(shard.clone()),
                        "DELETE matched WHERE clause for sharding key",
                    );
                }
                shard
            }
            None => {
                if let Some(recorder) = self.recorder_mut() {
                    recorder.record_entry(None, "DELETE fell back to broadcast");
                }
                Shard::default()
            }
        };

        Ok(Command::Query(Route::write(shard)))
    }
}
