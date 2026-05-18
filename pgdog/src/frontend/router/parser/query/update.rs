use super::*;

impl QueryParser {
    pub(super) fn update(
        &mut self,
        stmt: &UpdateStmt,
        context: &mut QueryParserContext,
    ) -> Result<Command, Error> {
        let mut parser = StatementParser::from_update(
            stmt,
            context.router_context.bind,
            &context.sharding_schema,
            self.recorder_mut(),
        );

        let shard = parser.shard()?;
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
            let schema_shard_state = parser.schema_shard_state(
                &context.router_context.schema,
                context.router_context.cluster.user(),
                context.router_context.parameter_hints.search_path,
            );
            let is_sharded = parser.is_sharded(
                &context.router_context.schema,
                context.router_context.cluster.user(),
                context.router_context.parameter_hints.search_path,
            );
            if let SchemaShardState::Resolved { shard, schema } = schema_shard_state {
                if let Some(recorder) = self.recorder_mut() {
                    recorder.record_entry(Some(shard.clone()), "UPDATE matched schema");
                }
                context
                    .shards_calculator
                    .push(ShardWithPriority::new_search_path(shard, &schema));
            } else if matches!(schema_shard_state, SchemaShardState::Ambiguous) || is_sharded {
                if let Some(recorder) = self.recorder_mut() {
                    recorder.record_entry(None, "UPDATE fell back to broadcast");
                }
                context
                    .shards_calculator
                    .push(ShardWithPriority::new_table(Shard::All));
            } else {
                if let Some(recorder) = self.recorder_mut() {
                    recorder.record_entry(None, "UPDATE fell back to omnisharded");
                }
                context
                    .shards_calculator
                    .push(ShardWithPriority::new_table_omni(Shard::All));
            }
        }

        Ok(Command::Query(Route::write(
            context.shards_calculator.shard(),
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn update_preserves_decimal_values() {
        let parsed = pgdog_plugin::pg_query::parse(
            "UPDATE transactions SET amount = 50.00, status = 'completed' WHERE id = 1",
        )
        .expect("parse");

        let stmt = parsed
            .protobuf
            .stmts
            .first()
            .and_then(|node| node.stmt.as_ref())
            .and_then(|node| node.node.as_ref())
            .expect("statement node");

        let update = match stmt {
            NodeEnum::UpdateStmt(update) => update,
            _ => panic!("expected update stmt"),
        };

        // Check that we can extract assignment values including decimals
        let mut found_decimal = false;
        let mut found_string = false;

        for target in &update.target_list {
            if let Some(NodeEnum::ResTarget(res)) = &target.node {
                if let Some(val) = &res.val {
                    let value = Value::try_from(&val.node).unwrap();
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
            }
        }
        assert!(found_decimal, "Should have found decimal value");
        assert!(found_string, "Should have found string value");
    }

    #[test]
    fn update_with_quoted_decimal() {
        let parsed =
            pgdog_plugin::pg_query::parse("UPDATE transactions SET amount = '50.00' WHERE id = 1")
                .expect("parse");

        let stmt = parsed
            .protobuf
            .stmts
            .first()
            .and_then(|node| node.stmt.as_ref())
            .and_then(|node| node.node.as_ref())
            .expect("statement node");

        let update = match stmt {
            NodeEnum::UpdateStmt(update) => update,
            _ => panic!("expected update stmt"),
        };

        // Quoted decimals should be treated as strings
        let mut found_string = false;
        for target in &update.target_list {
            if let Some(NodeEnum::ResTarget(res)) = &target.node {
                if let Some(val) = &res.val {
                    let value = Value::try_from(&val.node).unwrap();
                    if let Value::String(s) = value {
                        assert_eq!(s, "50.00");
                        found_string = true;
                    }
                }
            }
        }
        assert!(found_string, "Should have found string value");
    }
}
