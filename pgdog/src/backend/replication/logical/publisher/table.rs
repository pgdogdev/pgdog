//! Table.

use std::time::Duration;

use crate::backend::pool::Address;
use crate::backend::replication::publisher::progress::Progress;
use crate::backend::replication::publisher::Lsn;

use crate::backend::{Cluster, Server};
use crate::net::replication::StatusUpdate;

use super::super::{subscriber::CopySubscriber, Error};
use super::{Copy, PublicationTable, PublicationTableColumn, ReplicaIdentity, ReplicationSlot};

use tracing::info;

#[derive(Debug, Clone)]
pub struct Table {
    /// Name of the table publication.
    pub publication: String,
    /// Table data.
    pub table: PublicationTable,
    /// Table replica identity.
    pub identity: ReplicaIdentity,
    /// Table columns.
    pub columns: Vec<PublicationTableColumn>,
    /// Table data as of this LSN.
    pub lsn: Lsn,
}

impl Table {
    pub async fn load(publication: &str, server: &mut Server) -> Result<Vec<Self>, Error> {
        let tables = PublicationTable::load(publication, server).await?;
        let mut results = vec![];

        for table in tables {
            let identity = ReplicaIdentity::load(&table, server).await?;
            let columns = PublicationTableColumn::load(&identity, server).await?;

            results.push(Self {
                publication: publication.to_owned(),
                table: table.clone(),
                identity,
                columns,
                lsn: Lsn::default(),
            });
        }

        Ok(results)
    }

    /// Upsert record into table.
    pub fn insert(&self, upsert: bool) -> String {
        let names = format!(
            "({})",
            self.columns
                .iter()
                .map(|c| format!("\"{}\"", c.name.as_str()))
                .collect::<Vec<_>>()
                .join(", ")
        );
        let values = format!(
            "VALUES ({})",
            self.columns
                .iter()
                .enumerate()
                .map(|(i, _)| format!("${}", i + 1))
                .collect::<Vec<_>>()
                .join(", ")
        );
        let on_conflict = if upsert {
            format!(
                "ON CONFLICT ({}) DO UPDATE SET {}",
                self.columns
                    .iter()
                    .filter(|c| c.identity)
                    .map(|c| format!("\"{}\"", c.name.as_str()))
                    .collect::<Vec<_>>()
                    .join(", "),
                self.columns
                    .iter()
                    .enumerate()
                    .filter(|(_, c)| !c.identity)
                    .map(|(i, c)| format!("\"{}\" = ${}", c.name, i + 1))
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        } else {
            "".to_string()
        };

        format!(
            "INSERT INTO \"{}\".\"{}\" {} {} {}",
            self.table.schema, self.table.name, names, values, on_conflict
        )
    }

    /// Reload table data inside the transaction.
    pub async fn reload(&mut self, server: &mut Server) -> Result<(), Error> {
        if !server.in_transaction() {
            return Err(Error::TransactionNotStarted);
        }

        self.identity = ReplicaIdentity::load(&self.table, server).await?;
        self.columns = PublicationTableColumn::load(&self.identity, server).await?;

        Ok(())
    }

    pub async fn data_sync(&mut self, source: &Address, dest: &Cluster) -> Result<Lsn, Error> {
        info!(
            "data sync for \"{}\".\"{}\" started [{}]",
            self.table.schema, self.table.name, source
        );

        // Sync data using COPY.
        // Publisher uses COPY [...] TO STDOUT.
        // Subscriber uses COPY [...] FROM STDIN.
        let copy = Copy::new(self);

        // Create new standalone connection for the copy.
        // let mut server = Server::connect(source, ServerOptions::new_replication()).await?;
        let mut copy_sub = CopySubscriber::new(copy.statement(), dest)?;
        copy_sub.connect().await?;

        // Create sync slot.
        let mut slot = ReplicationSlot::data_sync(&self.publication, source);
        slot.connect().await?;
        self.lsn = slot.create_slot().await?;

        // Reload table info just to be sure it's consistent.
        self.reload(slot.server()?).await?;

        // Copy rows over.
        copy.start(slot.server()?).await?;
        copy_sub.start_copy().await?;
        let progress = Progress::new_data_sync(&self.table);

        while let Some(data_row) = copy.data(slot.server()?).await? {
            copy_sub.copy_data(data_row).await?;
            progress.update(copy_sub.bytes_sharded(), slot.lsn().lsn);
        }

        copy_sub.copy_done().await?;
        copy_sub.disconnect().await?;
        progress.done();

        slot.server()?.execute("COMMIT").await?;

        // Close slot.
        slot.start_replication().await?;
        slot.status_update(StatusUpdate::new_reply(self.lsn))
            .await?;
        slot.stop_replication().await?;

        // Drain slot
        while slot.replicate(Duration::MAX).await?.is_some() {}

        // Slot is temporary and will be dropped when the connection closes.

        info!(
            "data sync for \"{}\".\"{}\" finished at lsn {} [{}]",
            self.table.schema, self.table.name, self.lsn, source
        );

        Ok(self.lsn)
    }
}

#[cfg(test)]
mod test {

    use crate::backend::replication::logical::publisher::test::setup_publication;

    use super::*;

    #[tokio::test]
    async fn test_publication() {
        crate::logger();

        let mut publication = setup_publication().await;
        let tables = Table::load("publication_test", &mut publication.server)
            .await
            .unwrap();

        assert_eq!(tables.len(), 2);

        for table in tables {
            let upsert = table.insert(true);
            assert!(pg_query::parse(&upsert).is_ok());
        }

        publication.cleanup().await;
    }
}
