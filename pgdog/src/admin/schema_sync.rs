//! SCHEMA_SYNC command.

use tokio::spawn;
use tracing::info;

use crate::backend::replication::logical::admin::{Task, TaskType};
use crate::backend::replication::orchestrator::Orchestrator;

use super::prelude::*;

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum SchemaSyncPhase {
    Pre,
    Post,
}

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
        match self.phase {
            SchemaSyncPhase::Pre => "SCHEMA_SYNC PRE".into(),
            SchemaSyncPhase::Post => "SCHEMA_SYNC POST".into(),
        }
    }

    fn parse(sql: &str) -> Result<Self, Error> {
        let parts = sql.split(" ").collect::<Vec<_>>();

        match parts[..] {
            ["schema_sync", phase, from_database, to_database, publication] => Ok(Self {
                from_database: from_database.to_owned(),
                to_database: to_database.to_owned(),
                publication: publication.to_owned(),
                replication_slot: None,
                phase: parse_phase(phase)?,
            }),
            ["schema_sync", phase, from_database, to_database, publication, replication_slot] => {
                Ok(Self {
                    from_database: from_database.to_owned(),
                    to_database: to_database.to_owned(),
                    publication: publication.to_owned(),
                    replication_slot: Some(replication_slot.to_owned()),
                    phase: parse_phase(phase)?,
                })
            }
            _ => Err(Error::Syntax),
        }
    }

    async fn execute(&self) -> Result<Vec<Message>, Error> {
        let phase_name = match self.phase {
            SchemaSyncPhase::Pre => "pre",
            SchemaSyncPhase::Post => "post",
        };

        info!(
            r#"schema_sync {} "{}" to "{}", publication="{}""#,
            phase_name, self.from_database, self.to_database, self.publication
        );

        let mut orchestrator = Orchestrator::new(
            &self.from_database,
            &self.to_database,
            &self.publication,
            self.replication_slot.clone(),
        )?;

        let phase = self.phase;
        let handle = spawn(async move {
            orchestrator.load_schema().await?;

            match phase {
                SchemaSyncPhase::Pre => orchestrator.schema_sync_pre(true).await,
                SchemaSyncPhase::Post => orchestrator.schema_sync_post(true).await,
            }?;

            Ok(())
        });

        let task_id = Task::register(TaskType::SchemaSync(handle));

        let mut dr = DataRow::new();
        dr.add(task_id.to_string());

        Ok(vec![
            RowDescription::new(&[Field::text("task_id")]).message()?,
            dr.message()?,
        ])
    }
}

fn parse_phase(phase: &str) -> Result<SchemaSyncPhase, Error> {
    match phase {
        "pre" => Ok(SchemaSyncPhase::Pre),
        "post" => Ok(SchemaSyncPhase::Post),
        _ => Err(Error::Syntax),
    }
}
