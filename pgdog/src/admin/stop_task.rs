use crate::backend::replication::logical::admin::{AsyncTasks, TaskKind};
use crate::net::messages::{ErrorResponse, NoticeResponse};

use super::prelude::*;

pub struct StopTask {
    task_id: u64,
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
        let task_kind = AsyncTasks::remove(self.task_id);

        let mut messages = vec![];

        if task_kind == Some(TaskKind::CopyData) {
            let notice = NoticeResponse::from(ErrorResponse {
                severity: "WARNING".into(),
                code: "01000".into(),
                message: "replication slot was not dropped and requires manual cleanup".into(),
                ..Default::default()
            });
            messages.push(notice.message()?);
        }

        let result = match task_kind {
            Some(_) => "OK",
            None => "task not found",
        };

        let mut dr = DataRow::new();
        dr.add(result);

        messages.push(RowDescription::new(&[Field::text("stop_task")]).message()?);
        messages.push(dr.message()?);

        Ok(messages)
    }
}
