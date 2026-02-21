use std::time::SystemTime;

use crate::backend::replication::logical::status::TableCopies;

use super::prelude::*;

pub struct ShowTableCopies;

#[async_trait]
impl Command for ShowTableCopies {
    fn name(&self) -> String {
        "SHOW TABLE_COPIES".into()
    }

    fn parse(_sql: &str) -> Result<Self, Error> {
        Ok(ShowTableCopies {})
    }

    async fn execute(&self) -> Result<Vec<Message>, Error> {
        let rd = RowDescription::new(&[
            Field::text("schema"),
            Field::text("table"),
            Field::bigint("rows"),
            Field::bigint("bytes"),
            Field::bigint("bytes_per_sec"),
            Field::bigint("elapsed_ms"),
            Field::text("sql"),
        ]);
        let mut messages = vec![rd.message()?];
        let now = SystemTime::now();

        for entry in TableCopies::get().iter() {
            let key = entry.key();
            let state = entry.value();

            let elapsed_ms = now
                .duration_since(state.last_update)
                .unwrap_or_default()
                .as_millis() as i64;

            let mut row = DataRow::new();
            row.add(key.schema.as_str())
                .add(key.table.as_str())
                .add(state.rows as i64)
                .add(state.bytes as i64)
                .add(state.bytes_per_sec as i64)
                .add(elapsed_ms)
                .add(state.sql.as_str());

            messages.push(row.message()?);
        }

        Ok(messages)
    }
}
