use crate::{
    frontend::PreparedStatements,
    net::{data_row::Data, ToDataRowColumn},
    stats::memory::MemoryUsage,
};

use super::prelude::*;

#[derive(Debug, Clone)]
pub struct ShowPreparedStatements;

#[async_trait]
impl Command for ShowPreparedStatements {
    fn name(&self) -> String {
        "SHOW PREPARED STATEMENTS".into()
    }

    fn parse(_: &str) -> Result<Self, Error> {
        Ok(Self)
    }

    async fn execute(&self) -> Result<Vec<Message>, Error> {
        let statements = PreparedStatements::global().read().clone();
        let mut messages = vec![RowDescription::new(&[
            Field::text("name"),
            Field::text("statement"),
            Field::text("rewrite"),
            Field::numeric("used_by"),
            Field::numeric("memory_used"),
        ])
        .message()?];
        for (key, stmt) in statements.statements() {
            let name = stmt.name();
            let rewrite = statements.rewritten_parse(&name).ok_or(Error::Empty)?;
            let rewritten = statements.is_rewritten(&name);
            let name_memory = statements
                .names()
                .get(&stmt.name())
                .map(|s| s.memory_usage())
                .unwrap_or(0);
            let mut dr = DataRow::new();
            dr.add(stmt.name())
                .add(key.query()?)
                .add(if rewritten {
                    rewrite.query().to_data_row_column()
                } else {
                    Data::null()
                })
                .add(stmt.used)
                .add(name_memory);
            messages.push(dr.message()?);
        }
        Ok(messages)
    }
}
