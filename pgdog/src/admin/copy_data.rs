//! COPY_DATA command.

use tokio::spawn;
use tracing::info;

use crate::backend::replication::logical::admin::{Task, TaskType};
use crate::backend::replication::orchestrator::Orchestrator;
use crate::backend::replication::AsyncTasks;

use super::prelude::*;

pub struct CopyData {
    pub from_database: String,
    pub to_database: String,
    pub publication: String,
    pub replication_slot: Option<String>,
}

#[async_trait]
impl Command for CopyData {
    fn name(&self) -> String {
        "COPY_DATA".into()
    }

    fn parse(sql: &str) -> Result<Self, Error> {
        let parts = sql.split(" ").collect::<Vec<_>>();

        match parts[..] {
            ["copy_data", from_database, to_database, publication] => Ok(Self {
                from_database: from_database.to_owned(),
                to_database: to_database.to_owned(),
                publication: publication.to_owned(),
                replication_slot: None,
            }),
            ["copy_data", from_database, to_database, publication, replication_slot] => Ok(Self {
                from_database: from_database.to_owned(),
                to_database: to_database.to_owned(),
                publication: publication.to_owned(),
                replication_slot: Some(replication_slot.to_owned()),
            }),
            _ => Err(Error::Syntax),
        }
    }

    async fn execute(&self) -> Result<Vec<Message>, Error> {
        info!(
            r#"copy_data "{}" to "{}", publication="{}""#,
            self.from_database, self.to_database, self.publication
        );

        let mut orchestrator = Orchestrator::new(
            &self.from_database,
            &self.to_database,
            &self.publication,
            self.replication_slot.clone(),
        )?;

        let slot_name = orchestrator.replication_slot().to_owned();

        let task_id = Task::register(TaskType::CopyData(spawn(async move {
            orchestrator.load_schema().await?;
            orchestrator.data_sync().await?;
            AsyncTasks::insert(TaskType::Replication(orchestrator.replicate().await?));

            Ok(())
        })));

        let mut dr = DataRow::new();
        dr.add(task_id.to_string()).add(slot_name);

        Ok(vec![
            RowDescription::new(&[Field::text("task_id"), Field::text("replication_slot")])
                .message()?,
            dr.message()?,
        ])
    }
}
