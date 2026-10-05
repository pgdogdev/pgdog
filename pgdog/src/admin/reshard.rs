//! RESHARD command.

use tracing::info;

use crate::api::resharding::ReshardTask;
use crate::api::run_task;
use crate::backend::replication::resharding_state::ReshardingState;

use super::prelude::*;

pub(crate) struct Reshard {
    pub(crate) from_database: String,
    pub(crate) to_database: String,
    pub(crate) publication: String,
    pub(crate) replication_slot: Option<String>,
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
            [
                "reshard",
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
            r#"resharding "{}" to "{}", publication="{}""#,
            self.from_database, self.to_database, self.publication
        );
        let state = ReshardingState::builder()
            .source(&self.from_database)
            .destination(&self.to_database)
            .publication(&self.publication)
            .maybe_replication_slot(self.replication_slot.clone())
            .build()?;

        let task_id = run_task(
            ReshardTask::builder()
                .state(state)
                .auto_cutover(true)
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
