//! COPY_DATA command.

use tracing::info;

use crate::api::resharding::ReshardTask;
use crate::api::run_task;
use crate::backend::replication::logical::table_move::TableMove;
use crate::backend::replication::resharding_state::ReshardingState;

use super::prelude::*;

/// What to copy: the tables of an existing publication, or a single table.
pub(crate) enum CopySource {
    Publication(String),
    Table(String),
}

pub(crate) struct CopyData {
    pub(crate) from_database: String,
    pub(crate) to_database: String,
    pub(crate) source: CopySource,
    pub(crate) replication_slot: Option<String>,
}

#[async_trait]
impl Command for CopyData {
    fn name(&self) -> String {
        "COPY_DATA".into()
    }

    fn parse(sql: &str) -> Result<Self, Error> {
        let parts = sql.split(" ").collect::<Vec<_>>();

        let (from_database, to_database, source, replication_slot) = match parts[..] {
            ["copy_data", from, to, "table", table] => {
                (from, to, CopySource::Table(table.to_owned()), None)
            }
            ["copy_data", from, to, publication] => (
                from,
                to,
                CopySource::Publication(publication.to_owned()),
                None,
            ),
            ["copy_data", from, to, publication, slot] => (
                from,
                to,
                CopySource::Publication(publication.to_owned()),
                Some(slot),
            ),
            _ => return Err(Error::Syntax),
        };

        Ok(Self {
            from_database: from_database.to_owned(),
            to_database: to_database.to_owned(),
            source,
            replication_slot: replication_slot.map(str::to_owned),
        })
    }

    async fn execute(&self) -> Result<Vec<Message>, Error> {
        let (publication, table_move) = match &self.source {
            CopySource::Publication(publication) => {
                info!(
                    r#"copy_data "{}" to "{}", publication="{}""#,
                    self.from_database, self.to_database, publication
                );
                (publication.clone(), None)
            }
            CopySource::Table(table) => {
                info!(
                    r#"copy_data "{}" to "{}", table="{}""#,
                    self.from_database, self.to_database, table
                );
                let table_move = TableMove::new(table.clone());
                (table_move.publication().to_owned(), Some(table_move))
            }
        };

        let state = ReshardingState::builder()
            .source(&self.from_database)
            .destination(&self.to_database)
            .publication(&publication)
            .maybe_replication_slot(self.replication_slot.clone())
            .build()?;

        let slot_name = state.replication_slot().to_owned();

        let task_id = run_task(
            ReshardTask::builder()
                .state(state)
                .maybe_table_move(table_move)
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
    fn parses_publication_and_table_forms() {
        let cmd = CopyData::parse("copy_data a b pub").unwrap();
        assert!(matches!(&cmd.source, CopySource::Publication(p) if p == "pub"));
        assert_eq!(cmd.replication_slot, None);

        let cmd = CopyData::parse("copy_data a b pub slot").unwrap();
        assert!(matches!(&cmd.source, CopySource::Publication(p) if p == "pub"));
        assert_eq!(cmd.replication_slot.as_deref(), Some("slot"));

        let cmd = CopyData::parse("copy_data a b table sales.orders").unwrap();
        assert!(matches!(&cmd.source, CopySource::Table(t) if t == "sales.orders"));
        assert_eq!(cmd.replication_slot, None);
        assert_eq!(cmd.from_database, "a");
        assert_eq!(cmd.to_database, "b");

        assert!(CopyData::parse("copy_data a b").is_err());
    }
}
