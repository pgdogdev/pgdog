//! COPY_DATA command.

use tracing::info;

use crate::api::resharding::ReshardTask;
use crate::api::run_task;
use crate::backend::replication::resharding_state::ReshardingState;

use super::options::Options;
use super::prelude::*;

pub(crate) struct CopyData {
    pub(crate) from_database: String,
    pub(crate) to_database: String,
    pub(crate) publication: String,
    pub(crate) replication_slot: Option<String>,
    /// `tables_only` option: copy only the publication's tables, without cutover.
    pub(crate) tables_only: bool,
}

#[async_trait]
impl Command for CopyData {
    fn name(&self) -> String {
        "COPY_DATA".into()
    }

    fn parse(sql: &str) -> Result<Self, Error> {
        let (words, mut options) = Options::parse(sql)?;

        let (from_database, to_database, publication, slot) = match words[..] {
            ["copy_data", from_database, to_database, publication] => {
                (from_database, to_database, publication, None)
            }
            [
                "copy_data",
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

        let replication_slot = match (slot, options.value("slot")?) {
            (Some(_), Some(_)) => return Err(Error::InvalidOption("slot".into())),
            (slot, option) => slot.or(option).map(str::to_owned),
        };
        let tables_only = options.flag("tables_only")?;
        options.finish()?;

        Ok(Self {
            from_database: from_database.to_owned(),
            to_database: to_database.to_owned(),
            publication: publication.to_owned(),
            replication_slot,
            tables_only,
        })
    }

    async fn execute(&self) -> Result<Vec<Message>, Error> {
        info!(
            r#"copy_data "{}" to "{}", publication="{}", tables_only={}"#,
            self.from_database, self.to_database, self.publication, self.tables_only
        );

        let state = ReshardingState::builder()
            .source(&self.from_database)
            .destination(&self.to_database)
            .publication(&self.publication)
            .maybe_replication_slot(self.replication_slot.clone())
            .build()?;

        let slot_name = state.replication_slot().to_owned();

        let task_id = run_task(
            ReshardTask::builder()
                .state(state)
                .tables_only(self.tables_only)
                .build(),
        )
        .id();

        let mut dr = DataRow::new();
        dr.add(task_id.to_string()).add(slot_name);

        Ok(vec![
            RowDescription::new(&[Field::text("task_id"), Field::text("replication_slot")])
                .message(),
            dr.message(),
        ])
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn parses_slot_and_options() {
        for (sql, slot, tables_only) in [
            ("copy_data a b pub", None, false),
            ("copy_data a b pub slot", Some("slot"), false),
            ("copy_data a b pub (tables_only)", None, true),
            (
                "copy_data a b pub slot with (tables_only)",
                Some("slot"),
                true,
            ),
            (
                "copy_data a b pub (slot 'slot', tables_only)",
                Some("slot"),
                true,
            ),
        ] {
            let cmd = CopyData::parse(sql).unwrap();
            assert_eq!(cmd.publication, "pub", "{sql}");
            assert_eq!(cmd.replication_slot.as_deref(), slot, "{sql}");
            assert_eq!(cmd.tables_only, tables_only, "{sql}");
        }

        for sql in [
            "copy_data a b pub slot (slot 'other')",
            "copy_data a b pub (tables_only 'yes')",
            "copy_data a b pub (unknown)",
        ] {
            assert!(
                matches!(CopyData::parse(sql), Err(Error::InvalidOption(_))),
                "{sql}"
            );
        }
        assert!(matches!(
            CopyData::parse("copy_data a b pub slot extra"),
            Err(Error::Syntax)
        ));
    }
}
