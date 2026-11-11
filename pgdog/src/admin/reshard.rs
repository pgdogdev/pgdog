//! RESHARD command.

use tracing::info;

use crate::api::resharding::ReshardTask;
use crate::api::run_task;
use crate::backend::replication::resharding_state::ReshardingState;

use super::options::{Options, RunMode};
use super::prelude::*;

pub(crate) struct Reshard {
    pub(crate) from_database: String,
    pub(crate) to_database: String,
    pub(crate) publication: String,
    pub(crate) replication_slot: Option<String>,
    pub(crate) mode: RunMode,
}

#[async_trait]
impl Command for Reshard {
    fn name(&self) -> String {
        "RESHARD".into()
    }

    fn parse(sql: &str) -> Result<Self, Error> {
        let (words, mut options) = Options::parse(sql)?;

        let (from_database, to_database, publication, replication_slot) = match words[..] {
            ["reshard", from_database, to_database, publication] => {
                (from_database, to_database, publication, None)
            }
            [
                "reshard",
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
        match self.mode {
            RunMode::WithCleanup => (),
            RunMode::NoCleanup => return Err(Error::InvalidOption("cleanup_on_cancel".into())),
            RunMode::Abort => return Err(Error::InvalidOption("abort".into())),
        }
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

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn parses_run_mode() {
        for (sql, slot, mode) in [
            ("reshard a b pub", None, RunMode::WithCleanup),
            (
                "reshard a b pub (cleanup_on_cancel)",
                None,
                RunMode::WithCleanup,
            ),
            (
                "reshard a b pub (cleanup_on_cancel true)",
                None,
                RunMode::WithCleanup,
            ),
            (
                "reshard a b pub with (cleanup_on_cancel false)",
                None,
                RunMode::NoCleanup,
            ),
            (
                "reshard a b pub (cleanup_on_cancel off)",
                None,
                RunMode::NoCleanup,
            ),
            (
                "reshard a b pub slot with (cleanup_on_cancel false)",
                Some("slot"),
                RunMode::NoCleanup,
            ),
            ("reshard a b pub (abort)", None, RunMode::Abort),
        ] {
            let cmd = Reshard::parse(sql).unwrap();
            assert_eq!(cmd.publication, "pub", "{sql}");
            assert_eq!(cmd.replication_slot.as_deref(), slot, "{sql}");
            assert_eq!(cmd.mode, mode, "{sql}");
        }

        for sql in [
            "reshard a b pub (cleanup_on_cancel maybe)",
            "reshard a b pub (cleanup_on_cancel false, abort)",
            "reshard a b pub (unknown)",
        ] {
            assert!(
                matches!(Reshard::parse(sql), Err(Error::InvalidOption(_))),
                "{sql}"
            );
        }
    }
}
