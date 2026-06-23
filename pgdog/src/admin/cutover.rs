use crate::api::async_task::AsyncTaskId;
use crate::api::replication::ReplicationTask;
use crate::backend::replication::logical::Error as ReplicationError;

use super::prelude::*;

pub struct Cutover {
    task_id: Option<AsyncTaskId>,
}

#[async_trait]
impl Command for Cutover {
    fn name(&self) -> String {
        "CUTOVER".into()
    }

    fn parse(sql: &str) -> Result<Self, Error> {
        let parts: Vec<&str> = sql.split_whitespace().collect();

        match parts[..] {
            ["cutover"] => Ok(Cutover { task_id: None }),
            ["cutover", id] => {
                let task_id = id.parse().map_err(|_| Error::Syntax)?;
                Ok(Cutover {
                    task_id: Some(task_id),
                })
            }
            _ => Err(Error::Syntax),
        }
    }

    async fn execute(&self) -> Result<Vec<Message>, Error> {
        // With an id, cut over that task; without, the first running one.
        if !ReplicationTask::trigger_cutover(self.task_id) {
            return Err(ReplicationError::NotReplication.into());
        }

        let mut dr = DataRow::new();
        dr.add("OK");

        Ok(vec![
            RowDescription::new(&[Field::text("cutover")]).message()?,
            dr.message()?,
        ])
    }
}
