use crate::frontend::PreparedStatements;

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
        let statements = PreparedStatements::global().lock().clone();
        let mut messages =
            vec![RowDescription::new(&[Field::text("name"), Field::text("statement")]).message()?];
        for (name, parse) in statements.names() {
            let mut dr = DataRow::new();
            dr.add(name).add(parse.query());
            messages.push(dr.message()?);
        }
        Ok(messages)
    }
}
