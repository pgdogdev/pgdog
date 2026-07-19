use crate::api::async_task::AsyncTaskId;
use crate::api::tasks_storage;

use super::prelude::*;

pub struct StopTask {
    task_id: AsyncTaskId,
}

#[async_trait]
impl Command for StopTask {
    fn name(&self) -> String {
        "STOP_TASK".into()
    }

    fn parse(sql: &str) -> Result<Self, Error> {
        let parts: Vec<&str> = sql.split_whitespace().collect();

        match parts[..] {
            ["stop_task", id] => {
                let task_id = id.parse().map_err(|_| Error::Syntax)?;
                Ok(StopTask { task_id })
            }
            _ => Err(Error::Syntax),
        }
    }

    async fn execute(&self) -> Result<Vec<Message>, Error> {
        let cancelled = tasks_storage().cancel_task(self.task_id);

        let mut messages = vec![];

        let result = if cancelled.is_some() {
            "OK"
        } else {
            "task not found"
        };

        let mut dr = DataRow::new();
        dr.add(result);

        messages.push(RowDescription::new(&[Field::text("stop_task")]).message()?);
        messages.push(dr.message()?);

        Ok(messages)
    }
}
