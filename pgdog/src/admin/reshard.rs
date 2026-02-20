//! RESHARD command.

use tracing::info;

use crate::backend::replication::orchestrator::Orchestrator;

use super::prelude::*;

pub struct Reshard {
    pub from_database: String,
    pub to_database: String,
    pub publication: String,
    pub replication_slot: Option<String>,
}

#[async_trait]
impl Command for Reshard {
    fn name(&self) -> String {
        "RESHARD".into()
    }

    fn parse(sql: &str) -> Result<Self, Error> {
        let parts = sql.split(" ").collect::<Vec<_>>();

        match parts[..] {
            ["reshard", from_database, to_database, publication] => Ok(Self {
                from_database: from_database.to_owned(),
                to_database: to_database.to_owned(),
                publication: publication.to_owned(),
                replication_slot: None,
            }),
            ["reshard", from_database, to_database, publication, replication_slot] => Ok(Self {
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
            r#"resharding "{}" to "{}", publication="{}""#,
            self.from_database, self.to_database, self.publication
        );
        let mut orchestrator = Orchestrator::new(
            &self.from_database,
            &self.to_database,
            &self.publication,
            self.replication_slot.as_deref(),
        )?;

        orchestrator.replicate_and_cutover().await?;

        Ok(vec![])
    }
}
