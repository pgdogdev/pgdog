use crate::backend::replication::logical::status::{resharding_status, State};

use super::prelude::*;

pub struct ShowReshardingStatus;

#[async_trait]
impl Command for ShowReshardingStatus {
    fn name(&self) -> String {
        "SHOW RESHARDING_STATUS".into()
    }

    fn parse(_sql: &str) -> Result<Self, Error> {
        Ok(ShowReshardingStatus)
    }

    async fn execute(&self) -> Result<Vec<Message>, Error> {
        let rd = RowDescription::new(&[
            Field::text("src_database"),
            Field::text("src_user"),
            Field::text("dest_database"),
            Field::text("dest_user"),
            Field::bigint("shard"),
            Field::text("state"),
            Field::text("progress"),
            Field::text("tables"),
        ]);

        let mut messages = vec![rd.message()?];

        let (replication, state) = resharding_status();

        let mut shards: Vec<_> = state.into_iter().collect();
        shards.sort_by_key(|(shard, _)| *shard);

        for (shard, repl_state) in shards {
            let mut row = DataRow::new();

            if let Some(ref repl) = replication {
                row.add(repl.src.database.as_str())
                    .add(repl.src.user.as_str())
                    .add(repl.dest.database.as_str())
                    .add(repl.dest.user.as_str());
            } else {
                row.add("").add("").add("").add("");
            }

            let (state_name, progress) = match &repl_state.state {
                State::SchemaDump => ("schema_dump".to_string(), String::new()),
                State::SchemaRestore { stmt, total } => {
                    ("schema_restore".to_string(), format!("{}/{}", stmt, total))
                }
                State::CopyData { table, total } => {
                    ("copy_data".to_string(), format!("{}/{}", table, total))
                }
                State::CreateSlot => ("create_slot".to_string(), String::new()),
                State::Replication { lag } => ("replication".to_string(), format!("lag={}", lag)),
            };

            let tables: Vec<String> = repl_state
                .tables
                .iter()
                .map(|t| format!("{}.{}", t.table.schema, t.table.name))
                .collect();

            row.add(shard as i64)
                .add(state_name)
                .add(progress)
                .add(tables.join(", "));

            messages.push(row.message()?);
        }

        Ok(messages)
    }
}
