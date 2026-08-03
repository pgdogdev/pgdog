//! SCHEMA_SYNC command.

use tracing::info;

use crate::api::run_task;
use crate::api::schema_sync::{SchemaSyncPhase, SchemaSyncTask};
use pgdog_stats::Databases;

use super::prelude::*;

pub(crate) struct SchemaSync {
    pub(crate) from_database: String,
    pub(crate) to_database: String,
    pub(crate) publication: String,
    pub(crate) phase: SchemaSyncPhase,
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
                phase: phase.parse().map_err(|_| Error::Syntax)?,
            }),
            // A replication slot may be passed for symmetry with the other
            // migration commands; a schema sync doesn't use one.
            [
                "schema_sync",
                phase,
                from_database,
                to_database,
                publication,
                _replication_slot,
            ] => Ok(Self {
                from_database: from_database.to_owned(),
                to_database: to_database.to_owned(),
                publication: publication.to_owned(),
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

        let task_id = run_task(
            SchemaSyncTask::builder()
                .databases(Databases {
                    source: self.from_database.clone(),
                    destination: self.to_database.clone(),
                })
                .publication(self.publication.clone())
                .phase(self.phase)
                .ignore_errors(true)
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
