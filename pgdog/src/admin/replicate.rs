//! REPLICATE command.

use tracing::info;

use crate::api::replication::ReplicationTask;
use crate::api::run_task;
use crate::api::schema_sync::SchemaSyncTask;
use crate::backend::replication::logical::run::RunMode;
use crate::backend::replication::resharding_state::ReshardingState;
use pgdog_stats::CommandKind;

use super::options::Options;
use super::prelude::*;
use super::reshard::check_abort;

pub(crate) struct Replicate {
    pub(crate) from_database: String,
    pub(crate) to_database: String,
    pub(crate) publication: String,
    pub(crate) replication_slot: Option<String>,
    pub(crate) mode: RunMode,
}

#[async_trait]
impl Command for Replicate {
    fn name(&self) -> String {
        "REPLICATE".into()
    }

    fn parse(sql: &str) -> Result<Self, Error> {
        let (words, mut options) = Options::parse(sql)?;

        let (from_database, to_database, publication, replication_slot) = match words[..] {
            ["replicate", from_database, to_database, publication] => {
                (from_database, to_database, publication, None)
            }
            [
                "replicate",
                from_database,
                to_database,
                publication,
                replication_slot,
            ] => (
                from_database,
                to_database,
                publication,
                Some(replication_slot),
            ),
            _ => return Err(Error::Syntax),
        };
        let mode = options.run_mode()?;
        options.finish()?;

        Ok(Self {
            from_database: from_database.to_owned(),
            to_database: to_database.to_owned(),
            publication: publication.to_owned(),
            replication_slot: replication_slot.map(str::to_owned),
            mode,
        })
    }

    async fn execute(&self) -> Result<Vec<Message>, Error> {
        let mut state = ReshardingState::builder()
            .source(&self.from_database)
            .destination(&self.to_database)
            .publication(&self.publication)
            .maybe_replication_slot(self.replication_slot.clone())
            .mode(self.mode)
            .build()?;
        check_abort!(self, state, CommandKind::Replicate);
        let resumed = state
            .check_resume(CommandKind::Replicate, self.replication_slot.as_deref())
            .await?;
        info!(
            r#"replicate "{}" to "{}", publication="{}""#,
            self.from_database, self.to_database, self.publication
        );

        let schema_sync = SchemaSyncTask::builder()
            .publication_tables_only(false)
            .databases(state.databases())
            .publication(self.publication.clone());

        let task_id = run_task(
            ReplicationTask::builder()
                .state(state)
                .schema_sync(schema_sync)
                .validate(false)
                .maybe_resumed(resumed)
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

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn parses_slot_and_run_mode() {
        for (sql, slot, mode) in [
            (
                "replicate a b pub",
                None,
                RunMode::Run {
                    cleanup_on_cancel: true,
                },
            ),
            (
                "replicate a b pub slot with (cleanup_on_cancel false)",
                Some("slot"),
                RunMode::Run {
                    cleanup_on_cancel: false,
                },
            ),
            ("replicate a b pub (abort)", None, RunMode::Abort),
            ("replicate a b pub (resume)", None, RunMode::Resume),
        ] {
            let cmd = Replicate::parse(sql).unwrap();
            assert_eq!(cmd.publication, "pub", "{sql}");
            assert_eq!(cmd.replication_slot.as_deref(), slot, "{sql}");
            assert_eq!(cmd.mode, mode, "{sql}");
        }
        assert!(matches!(
            Replicate::parse("replicate a b pub (unknown)"),
            Err(Error::InvalidOption(_))
        ));
    }
}
