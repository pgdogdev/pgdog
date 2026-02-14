use super::*;

impl QueryParser {
    pub(super) fn delete(
        &mut self,
        stmt: &DeleteStmt,
        context: &mut QueryParserContext,
    ) -> Result<Command, Error> {
        let mut parser = StatementParser::from_delete(
            stmt,
            context.router_context.bind,
            &context.sharding_schema,
            self.recorder_mut(),
        );

        let is_sharded = parser.is_sharded(
            &context.router_context.schema,
            context.router_context.cluster.user(),
            context.router_context.parameter_hints.search_path,
        );

        let shard = parser.shard()?;

        if let Some(shard) = shard {
            if let Some(recorder) = self.recorder_mut() {
                recorder.record_entry(
                    Some(shard.clone()),
                    "DELETE matched WHERE clause for sharding key",
                );
            }
            context
                .shards_calculator
                .push(ShardWithPriority::new_table(shard));
        } else {
            if let Some(recorder) = self.recorder_mut() {
                recorder.record_entry(None, "DELETE fell back to broadcast");
            }
            if is_sharded {
                context
                    .shards_calculator
                    .push(ShardWithPriority::new_table(Shard::All));
            } else {
                context
                    .shards_calculator
                    .push(ShardWithPriority::new_rr_omni(Shard::All));
            }
        }

        Ok(Command::Query(Route::write(
            context.shards_calculator.shard(),
        )))
    }
}
