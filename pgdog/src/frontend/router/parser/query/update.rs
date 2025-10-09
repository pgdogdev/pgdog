use crate::{config::ShardKeyUpdateMode, frontend::router::parser::where_clause::TablesSource};
use std::string::String as StdString;

use super::*;

impl QueryParser {
    pub(super) fn update(
        &mut self,
        stmt: &UpdateStmt,
        context: &QueryParserContext,
    ) -> Result<Command, Error> {
        let table = stmt.relation.as_ref().map(Table::from);

        if let Some(table) = table {
            let shard_key_assignments = Self::detect_shard_key_assignments(stmt, table, context);
            if !shard_key_assignments.is_empty() {
                let mode = context.shard_key_update_mode();
                let columns = shard_key_assignments.join(", ");
                match mode {
                    ShardKeyUpdateMode::Ignore => {}
                    ShardKeyUpdateMode::Error => {
                        return Err(Error::ShardKeyUpdateViolation {
                            table: table.name.to_owned(),
                            columns,
                            mode,
                        });
                    }
                    ShardKeyUpdateMode::Rewrite => {
                        return Err(Error::ShardKeyRewriteNotSupported {
                            table: table.name.to_owned(),
                            columns,
                        });
                    }
                }
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
                    if sharding_columns
                        .iter()
                        .any(|candidate| *candidate == column.as_str())
                    {
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
