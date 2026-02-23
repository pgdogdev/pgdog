use crate::backend::replication::logical::admin::AsyncTasks;

use super::prelude::*;

pub struct Cutover;

#[async_trait]
impl Command for Cutover {
    fn name(&self) -> String {
        "CUTOVER".into()
    }

    fn parse(sql: &str) -> Result<Self, Error> {
        let parts: Vec<&str> = sql.split_whitespace().collect();

        match parts[..] {
            ["cutover"] => Ok(Cutover),
            _ => Err(Error::Syntax),
        }
    }

    async fn execute(&self) -> Result<Vec<Message>, Error> {
        AsyncTasks::cutover()?;

        let mut dr = DataRow::new();
        dr.add("OK");

        Ok(vec![
            RowDescription::new(&[Field::text("cutover")]).message()?,
            dr.message()?,
        ])
    }
}
