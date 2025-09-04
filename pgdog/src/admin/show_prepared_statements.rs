use crate::{frontend::PreparedStatements, stats::memory::MemoryUsage};

use super::prelude::*;

#[derive(Debug, Clone)]
pub(crate) struct ShowPreparedStatements;

#[async_trait]
impl Command for ShowPreparedStatements {
    fn name(&self) -> String {
        "SHOW PREPARED STATEMENTS".into()
    }

    fn parse(_: &str) -> Result<Self, Error> {
        Ok(Self)
    }

    async fn execute(&self) -> Result<Vec<Message>, Error> {
        let statements = PreparedStatements::global().lock().clone();
        let mut messages = vec![RowDescription::new(&[
            Field::text("name"),
            Field::text("statement"),
            Field::numeric("used_by"),
            Field::numeric("memory_used"),
        ])
        .message()?];
        for (key, stmt) in statements.statements() {
            let name_memory = statements
                .names()
                .get(&stmt.name())
                .map(|s| s.memory_usage())
                .unwrap_or(0);
            let mut dr = DataRow::new();
            dr.add(stmt.name())
                .add(key.query()?)
                .add(stmt.used)
                .add(name_memory);
            messages.push(dr.message()?);
        }
        Ok(messages)
    }
}
