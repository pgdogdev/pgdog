//! SCHEMA_SYNC command.

use tracing::info;

use crate::api::schema_sync::{SchemaSyncPhase, SchemaSyncTask};
use crate::api::start;
use crate::backend::replication::orchestrator::Orchestrator;

use super::prelude::*;

pub struct SchemaSync {
    pub from_database: String,
    pub to_database: String,
    pub publication: String,
    pub replication_slot: Option<String>,
    pub phase: SchemaSyncPhase,
}

#[async_trait]
impl Command for SchemaSync {
    fn name(&self) -> String {
        format!("SCHEMA_SYNC {}", self.phase).to_uppercase()
    }

    fn parse(sql: &str) -> Result<Self, Error> {
        let parts = sql.split(" ").collect::<Vec<_>>();

        match parts[..] {
            [
                "schema_sync",
                phase,
                from_database,
                to_database,
                publication,
            ] => Ok(Self {
                from_database: from_database.to_owned(),
                to_database: to_database.to_owned(),
                publication: publication.to_owned(),
                replication_slot: None,
                phase: phase.parse().map_err(|_| Error::Syntax)?,
            }),
            [
                "schema_sync",
                phase,
                from_database,
                to_database,
                publication,
                replication_slot,
            ] => Ok(Self {
                from_database: from_database.to_owned(),
                to_database: to_database.to_owned(),
                publication: publication.to_owned(),
                replication_slot: Some(replication_slot.to_owned()),
                phase: phase.parse().map_err(|_| Error::Syntax)?,
            }),
            _ => Err(Error::Syntax),
        }
    }

    async fn execute(&self) -> Result<Vec<Message>, Error> {
        info!(
            r#"schema_sync {} "{}" to "{}", publication="{}""#,
            self.phase, self.from_database, self.to_database, self.publication
        );

        let orchestrator = Orchestrator::new(
            &self.from_database,
            &self.to_database,
            &self.publication,
            self.replication_slot.clone(),
        )?;

        let task_id = start(
            SchemaSyncTask::builder()
                .orchestrator(orchestrator)
                .phase(self.phase)
                .ignore_errors(true)
                .build(),
        )
        .id();

        let mut dr = DataRow::new();
        dr.add(task_id.to_string());

        Ok(vec![
            RowDescription::new(&[Field::text("task_id")]).message()?,
            dr.message()?,
        ])
    }
}
