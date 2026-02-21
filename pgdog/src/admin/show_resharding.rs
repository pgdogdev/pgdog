use std::time::SystemTime;

use crate::backend::replication::logical::status::TableCopies;

use super::prelude::*;

pub struct ShowResharding;

#[async_trait]
impl Command for ShowResharding {
    fn name(&self) -> String {
        "SHOW RESHARDING".into()
    }

    fn parse(_sql: &str) -> Result<Self, Error> {
        Ok(ShowResharding)
    }

    async fn execute(&self) -> Result<Vec<Message>, Error> {
        let rd = RowDescription::new(&[
            Field::text("schema"),
            Field::text("table"),
            Field::text("sql"),
            Field::bigint("rows"),
            Field::bigint("bytes"),
            Field::bigint("bytes_per_sec"),
            Field::text("last_write"),
        ]);

        let mut messages = vec![rd.message()?];

        let copies = TableCopies::get();
        let mut entries: Vec<_> = copies
            .iter()
            .map(|entry| (entry.key().clone(), entry.value().clone()))
            .collect();
        entries.sort_by(|a, b| (&a.0.schema, &a.0.table).cmp(&(&b.0.schema, &b.0.table)));

        for (key, state) in entries {
            let mut row = DataRow::new();

            let elapsed = SystemTime::now()
                .duration_since(state.last_update)
                .unwrap_or_default();
            let last_write = format!("{:.1}s ago", elapsed.as_secs_f64());

            row.add(key.schema.as_str())
                .add(key.table.as_str())
                .add(state.sql.as_str())
                .add(state.rows as i64)
                .add(state.bytes as i64)
                .add(state.bytes_per_sec as i64)
                .add(last_write);

            messages.push(row.message()?);
        }

        Ok(messages)
    }
}
