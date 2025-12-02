use std::{collections::HashMap, string::String as StdString};

use crate::{
    config::{RewriteMode, ShardedTable},
    frontend::router::{
        parser::where_clause::TablesSource,
        sharding::{ContextBuilder, Value as ShardingValue},
    },
};
use pg_query::protobuf::ColumnRef;

use super::shared::ConvergeAlgorithm;
use super::*;

impl QueryParser {
    pub(super) fn update(
        &mut self,
        stmt: &UpdateStmt,
        context: &QueryParserContext,
    ) -> Result<Command, Error> {
        let table = stmt.relation.as_ref().map(Table::from);

        if let Some(shard) = self.check_search_path_for_shard(context)? {
            return Ok(Command::Query(Route::write(shard)));
        }

        if let Some(table) = table {
            // Schema-based sharding.
            if let Some(schema) = context.sharding_schema.schemas.get(table.schema()) {
                let shard: Shard = schema.shard().into();

                if let Some(recorder) = self.recorder_mut() {
                    recorder.record_entry(
                        Some(shard.clone()),
                        format!("UPDATE matched schema {}", schema.name()),
                    );
                }

                return Ok(Command::Query(Route::write(shard)));
            }

            let shard_key_columns = Self::detect_shard_key_assignments(stmt, table, context);
            let columns_display =
                (!shard_key_columns.is_empty()).then(|| shard_key_columns.join(", "));
            let mode = context.shard_key_update_mode();

            if let (Some(columns), RewriteMode::Error) = (columns_display.as_ref(), mode) {
                return Err(Error::ShardKeyUpdateViolation {
                    table: table.name.to_owned(),
                    columns: columns.clone(),
                    mode,
                });
            }

            let source = TablesSource::from(table);
            let where_clause = WhereClause::new(&source, &stmt.where_clause);

            if let Some(where_clause) = where_clause {
                let shards = Self::where_clause(
                    &context.sharding_schema,
                    &where_clause,
                    context.router_context.bind,
                    &mut self.explain_recorder,
                )?;
                let shard = Self::converge(&shards, ConvergeAlgorithm::default());
                if let Some(recorder) = self.recorder_mut() {
                    recorder.record_entry(
                        Some(shard.clone()),
                        "UPDATE matched WHERE clause for sharding key",
                    );
                }

                if let (Some(columns), Some(display)) = (
                    (!shard_key_columns.is_empty()).then_some(&shard_key_columns),
                    columns_display.as_deref(),
                ) {
                    if matches!(mode, RewriteMode::Rewrite) {
                        let assignments = Self::collect_assignments(stmt, table, columns, display)?;

                        if assignments.is_empty() {
                            return Ok(Command::Query(Route::write(shard)));
                        }

                        let plan = Self::build_shard_key_rewrite_plan(
                            stmt,
                            table,
                            shard.clone(),
                            context,
                            assignments,
                            columns,
                            display,
                        )?;
                        return Ok(Command::ShardKeyRewrite(Box::new(plan)));
                    }
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

impl QueryParser {
    fn build_shard_key_rewrite_plan(
        stmt: &UpdateStmt,
        table: Table<'_>,
        shard: Shard,
        context: &QueryParserContext,
        assignments: Vec<Assignment>,
        shard_columns: &[StdString],
        columns_display: &str,
    ) -> Result<ShardKeyRewritePlan, Error> {
        let Shard::Direct(old_shard) = shard else {
            return Err(Error::ShardKeyRewriteNotSupported {
                table: table.name.to_owned(),
                columns: columns_display.to_owned(),
            });
        };
        let owned_table = table.to_owned();
        let new_shard =
            Self::compute_new_shard(&assignments, shard_columns, table, context, columns_display)?;

        Ok(ShardKeyRewritePlan::new(
            owned_table,
            Route::write(Shard::Direct(old_shard)),
            new_shard,
            stmt.clone(),
            assignments,
        ))
    }

    fn collect_assignments(
        stmt: &UpdateStmt,
        table: Table<'_>,
        shard_columns: &[StdString],
        columns_display: &str,
    ) -> Result<Vec<Assignment>, Error> {
        let mut assignments = Vec::new();

        for target in &stmt.target_list {
            if let Some(NodeEnum::ResTarget(res)) = target.node.as_ref() {
                let Some(column) = Self::res_target_column(res) else {
                    continue;
                };

                if !shard_columns.iter().any(|candidate| candidate == &column) {
                    continue;
                }

                let value = Self::assignment_value(res).map_err(|_| {
                    Error::ShardKeyRewriteNotSupported {
                        table: table.name.to_owned(),
                        columns: columns_display.to_owned(),
                    }
                })?;

                if let AssignmentValue::Column(reference) = &value {
                    if reference == &column {
                        continue;
                    }
                    return Err(Error::ShardKeyRewriteNotSupported {
                        table: table.name.to_owned(),
                        columns: columns_display.to_owned(),
                    });
                }

                assignments.push(Assignment::new(column, value));
            }
        }

        Ok(assignments)
    }

    fn compute_new_shard(
        assignments: &[Assignment],
        shard_columns: &[StdString],
        table: Table<'_>,
        context: &QueryParserContext,
        columns_display: &str,
    ) -> Result<Option<usize>, Error> {
        let assignment_map: HashMap<&str, &Assignment> = assignments
            .iter()
            .map(|assignment| (assignment.column(), assignment))
            .collect();

        let mut new_shard: Option<usize> = None;

        for column in shard_columns {
            let assignment = assignment_map.get(column.as_str()).ok_or_else(|| {
                Error::ShardKeyRewriteNotSupported {
                    table: table.name.to_owned(),
                    columns: columns_display.to_owned(),
                }
            })?;

            let sharded_table = context
                .sharding_schema
                .tables()
                .tables()
                .iter()
                .find(|candidate| {
                    let name_matches = match candidate.name.as_deref() {
                        Some(name) => name == table.name,
                        None => true,
                    };
                    name_matches && candidate.column == column.as_str()
                })
                .ok_or_else(|| Error::ShardKeyRewriteNotSupported {
                    table: table.name.to_owned(),
                    columns: columns_display.to_owned(),
                })?;

            let shard = Self::assignment_shard(
                assignment.value(),
                sharded_table,
                context,
                table.name,
                columns_display,
            )?;

            let shard_value = match shard {
                Shard::Direct(value) => value,
                _ => {
                    return Err(Error::ShardKeyRewriteNotSupported {
                        table: table.name.to_owned(),
                        columns: columns_display.to_owned(),
                    })
                }
            };

            if let Some(existing) = new_shard {
                if existing != shard_value {
                    return Err(Error::ShardKeyRewriteNotSupported {
                        table: table.name.to_owned(),
                        columns: columns_display.to_owned(),
                    });
                }
            } else {
                new_shard = Some(shard_value);
            }
        }

        Ok(new_shard)
    }

    fn assignment_shard(
        value: &AssignmentValue,
        sharded_table: &ShardedTable,
        context: &QueryParserContext,
        table_name: &str,
        columns_display: &str,
    ) -> Result<Shard, Error> {
        match value {
            AssignmentValue::Integer(int) => {
                let context_builder = ContextBuilder::new(sharded_table)
                    .data(*int)
                    .shards(context.sharding_schema.shards)
                    .build()?;
                Ok(context_builder.apply()?)
            }
            AssignmentValue::Float(_) => {
                // Floats are not supported as sharding keys
                // Return Shard::All to route to all shards (safe but not optimal)
                Ok(Shard::All)
            }
            AssignmentValue::String(text) => {
                let context_builder = ContextBuilder::new(sharded_table)
                    .data(text.as_str())
                    .shards(context.sharding_schema.shards)
                    .build()?;
                Ok(context_builder.apply()?)
            }
            AssignmentValue::Parameter(index) => {
                if *index <= 0 {
                    return Err(Error::MissingParameter(0));
                }
                let param_index = *index as usize;
                let bind = context
                    .router_context
                    .bind
                    .ok_or_else(|| Error::MissingParameter(param_index))?;
                let parameter = bind
                    .parameter(param_index - 1)?
                    .ok_or_else(|| Error::MissingParameter(param_index))?;
                let sharding_value =
                    ShardingValue::from_param(&parameter, sharded_table.data_type)?;
                let context_builder = ContextBuilder::new(sharded_table)
                    .value(sharding_value)
                    .shards(context.sharding_schema.shards)
                    .build()?;
                Ok(context_builder.apply()?)
            }
            AssignmentValue::Null | AssignmentValue::Boolean(_) | AssignmentValue::Column(_) => {
                Err(Error::ShardKeyRewriteNotSupported {
                    table: table_name.to_owned(),
                    columns: columns_display.to_owned(),
                })
            }
        }
    }

    fn assignment_value(res: &ResTarget) -> Result<AssignmentValue, ()> {
        if let Some(val) = &res.val {
            if let Some(NodeEnum::ColumnRef(column_ref)) = val.node.as_ref() {
                if let Some(name) = Self::column_ref_name(column_ref) {
                    return Ok(AssignmentValue::Column(name));
                }
                return Err(());
            }

            if matches!(val.node.as_ref(), Some(NodeEnum::AExpr(_))) {
                return Err(());
            }

            if let Ok(value) = Value::try_from(&val.node) {
                return match value {
                    Value::Integer(i) => Ok(AssignmentValue::Integer(i)),
                    Value::Float(f) => Ok(AssignmentValue::Float(f.to_owned())),
                    Value::String(s) => Ok(AssignmentValue::String(s.to_owned())),
                    Value::Boolean(b) => Ok(AssignmentValue::Boolean(b)),
                    Value::Null => Ok(AssignmentValue::Null),
                    Value::Placeholder(index) => Ok(AssignmentValue::Parameter(index)),
                    _ => Err(()),
                };
            }
        }

        Err(())
    }

    fn column_ref_name(column: &ColumnRef) -> Option<StdString> {
        if column.fields.len() == 1 {
            if let Some(NodeEnum::String(s)) = column.fields[0].node.as_ref() {
                return Some(s.sval.clone());
            }
        } else if column.fields.len() == 2 {
            if let Some(NodeEnum::String(s)) = column.fields[1].node.as_ref() {
                return Some(s.sval.clone());
            }
        }

        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn res_target_column_extracts_simple_assignment() {
        let parsed = pgdog_plugin::pg_query::parse("UPDATE sharded SET id = id + 1 WHERE id = 1")
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

        let target = update
            .target_list
            .first()
            .and_then(|node| node.node.as_ref())
            .and_then(|node| match node {
                NodeEnum::ResTarget(res) => Some(res),
                _ => None,
            })
            .expect("res target");

        let column = QueryParser::res_target_column(target).expect("column");
        assert_eq!(column, "id");
    }

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
                    if let Ok(value) = Value::try_from(&val.node) {
                        match value {
                            Value::Float(f) => {
                                assert_eq!(f, "50.00");
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
                    if let Ok(value) = Value::try_from(&val.node) {
                        match value {
                            Value::String(s) => {
                                assert_eq!(s, "50.00");
                                found_string = true;
                            }
                            _ => {}
                        }
                    }
                }
            }
        }
        assert!(found_string, "Should have found string value");
    }
}

impl QueryParser {
    fn detect_shard_key_assignments(
        stmt: &UpdateStmt,
        table: Table<'_>,
        context: &QueryParserContext,
    ) -> Vec<StdString> {
        let table_name = table.name;
        let mut sharding_columns = Vec::new();

        for sharded_table in context.sharding_schema.tables().tables() {
            match sharded_table.name.as_deref() {
                Some(name) if name == table_name => {
                    sharding_columns.push(sharded_table.column.as_str());
                }
                None => {
                    sharding_columns.push(sharded_table.column.as_str());
                }
                _ => {}
            }
        }

        if sharding_columns.is_empty() {
            return Vec::new();
        }

        let mut assigned: Vec<StdString> = Vec::new();

        for target in &stmt.target_list {
            if let Some(NodeEnum::ResTarget(res)) = target.node.as_ref() {
                if let Some(column) = Self::res_target_column(res) {
                    if sharding_columns.contains(&column.as_str()) {
                        assigned.push(column);
                    }
                }
            }
        }

        assigned.sort();
        assigned.dedup();
        assigned
    }

    fn res_target_column(res: &ResTarget) -> Option<StdString> {
        if !res.name.is_empty() {
            return Some(res.name.clone());
        }

        if res.indirection.len() == 1 {
            if let Some(NodeEnum::String(value)) = res.indirection[0].node.as_ref() {
                return Some(value.sval.clone());
            }
        }

        None
    }
}
