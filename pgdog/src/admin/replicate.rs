//! REPLICATE command.

use tracing::info;

use crate::api::replication::ReplicationTask;
use crate::api::run_task;
use crate::api::schema_sync::{SchemaSyncPhase, SchemaSyncTask};
use crate::backend::replication::orchestrator::Orchestrator;

use super::prelude::*;

pub(crate) struct Replicate {
    pub(crate) from_database: String,
    pub(crate) to_database: String,
    pub(crate) publication: String,
    pub(crate) replication_slot: Option<String>,
}

#[async_trait]
impl Command for Replicate {
    fn name(&self) -> String {
        "REPLICATE".into()
    }

    fn parse(sql: &str) -> Result<Self, Error> {
        let parts = sql.split(" ").collect::<Vec<_>>();

        match parts[..] {
            ["replicate", from_database, to_database, publication] => Ok(Self {
                from_database: from_database.to_owned(),
                to_database: to_database.to_owned(),
                publication: publication.to_owned(),
                replication_slot: None,
            }),
            [
                "replicate",
                from_database,
                to_database,
                publication,
                replication_slot,
            ] => Ok(Self {
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
            r#"replicate "{}" to "{}", publication="{}""#,
            self.from_database, self.to_database, self.publication
        );

        let orchestrator = Orchestrator::new(
            &self.from_database,
            &self.to_database,
            &self.publication,
            self.replication_slot.clone(),
        )?;

        let schema_sync = SchemaSyncTask::builder()
            .databases(orchestrator.databases())
            .publication(self.publication.clone())
            .phase(SchemaSyncPhase::Cutover)
            .ignore_errors(true)
            .build();

        let waiter = orchestrator.replicate().await?;
        let task_id = run_task(
            ReplicationTask::builder()
                .waiter(waiter)
                .schema_sync(schema_sync)
                .build(),
        )
        .id();

        let mut dr = DataRow::new();
        dr.add(task_id.to_string());

        Ok(vec![
            RowDescription::new(&[Field::text("task_id")]).message(),
            dr.message(),
        ])
    }
}
