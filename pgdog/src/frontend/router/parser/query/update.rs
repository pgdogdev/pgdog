use super::*;

impl QueryParser {
    pub(super) fn update(
        &mut self,
        stmt: pg_raw_parse::Node<'_>,
        context: &mut QueryParserContext,
    ) -> Result<Command, Error> {
        let mut parser = StatementParser::new(
            stmt,
            context.router_context.bind,
            &context.sharding_schema,
            self.recorder_mut(),
        );
        parser.set_resolved_lookups(&context.router_context.resolved_lookups);

        let is_sharded = parser.is_sharded(
            &context.router_context.schema,
            context.router_context.cluster.user(),
            context.router_context.parameter_hints.search_path,
        );
        let shard = parser.shard()?;
        let pending_lookups = parser.take_pending_lookups();
        context.pending_lookups.extend(pending_lookups);
        let omnisharded = !is_sharded && shard.is_none();

        if let Some(shard) = shard {
            if let Some(recorder) = self.recorder_mut() {
                recorder.record_entry(
                    Some(shard.clone()),
                    "UPDATE matched WHERE clause for sharding key",
                );
            }
            context
                .shards_calculator
                .push(ShardWithPriority::new_table(shard));
        } else {
            if let Some(recorder) = self.recorder_mut() {
                recorder.record_entry(None, "UPDATE fell back to broadcast");
            }
            if is_sharded {
                context
                    .shards_calculator
                    .push(ShardWithPriority::new_table(Shard::All));
            } else {
                context
                    .shards_calculator
                    .push(ShardWithPriority::new_table_omni(Shard::All));
            }
        }

        Ok(Command::Query(
            Route::write(context.shards_calculator.shard()).with_omnisharded(omnisharded),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn update_preserves_decimal_values() {
        let parsed = pg_raw_parse::parse(
            "UPDATE transactions SET amount = 50.00, status = 'completed' WHERE id = 1",
        )
        .unwrap();

        let Some(Node::UpdateStmt(update)) = parsed.stmts().next() else {
            panic!("expected update stmt");
        };

        // Check that we can extract assignment values including decimals
        let mut found_decimal = false;
        let mut found_string = false;

        for target in update.target_list() {
            let value = Value::try_from(target.val()).unwrap();
            match value {
                Value::Float(f) => {
                    assert_eq!(f, 50.0);
                    found_decimal = true;
                }
                Value::String(s) => {
                    assert_eq!(s, "completed");
                    found_string = true;
                }
                _ => {}
            }
        }
        assert!(found_decimal, "Should have found decimal value");
        assert!(found_string, "Should have found string value");
    }

    #[test]
    fn update_with_quoted_decimal() {
        let parsed =
            pg_raw_parse::parse("UPDATE transactions SET amount = '50.00' WHERE id = 1").unwrap();

        let Some(Node::UpdateStmt(update)) = parsed.stmts().next() else {
            panic!("expected update stmt");
        };

        // Quoted decimals should be treated as strings
        let mut found_string = false;
        for target in update.target_list() {
            let value = Value::try_from(target.val()).unwrap();
            if let Value::String(s) = value {
                assert_eq!(s, "50.00");
                found_string = true;
            }
        }
        assert!(found_string, "Should have found string value");
    }
}
