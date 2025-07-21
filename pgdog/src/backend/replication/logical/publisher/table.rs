//! Table.

use crate::backend::pool::Address;
use crate::backend::{Cluster, Server, ServerOptions};
use crate::net::replication::{ReplicationMeta, StatusUpdate};
use crate::net::CopyDone;

use super::super::{subscriber::Subscriber, Error};
use super::{
    Copy, PublicationTable, PublicationTableColumn, ReplicaIdentity, ReplicationData,
    ReplicationSlot,
};

#[derive(Debug, Clone)]
pub struct Table {
    pub(super) publication: String,
    pub(super) table: PublicationTable,
    pub(super) identity: ReplicaIdentity,
    pub(super) columns: Vec<PublicationTableColumn>,
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
            });
        }

        Ok(results)
    }

    /// Upsert record into table.
    pub async fn insert(&self, upsert: bool) -> String {
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
                "ON CONFLICT ({}) DO UPDATE {}",
                self.columns
                    .iter()
                    .filter(|c| c.identity)
                    .map(|c| format!("\"{}\"", c.name.as_str()))
                    .collect::<Vec<_>>()
                    .join(", "),
                self.columns
                    .iter()
                    .enumerate()
                    .filter(|(_, c)| c.identity)
                    .map(|(i, c)| format!("SET \"{}\" = ${}", c.name, i + 1))
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

    pub async fn data_sync(&mut self, source: &Address, dest: &Cluster) -> Result<(), Error> {
        let copy = Copy::new(self);

        let mut server = Server::connect(source, ServerOptions::new_replication()).await?;
        let mut dest = Subscriber::new(copy.statement(), dest)?;

        dest.connect().await?;

        // Start transaction.
        server
            .execute("BEGIN READ ONLY ISOLATION LEVEL REPEATABLE READ")
            .await?;

        // Create sync slot.
        let mut slot = ReplicationSlot::data_sync(&self.publication);
        slot.create_slot(&mut server).await?;

        // Reload table info just to be sure it's consistent.
        self.reload(&mut server).await?;

        // Copy rows over.
        let copy = Copy::new(self);
        copy.start(&mut server).await?;
        dest.start_copy().await?;

        while let Some(data_row) = copy.data(&mut server).await? {
            dest.copy_data(data_row).await?;
        }
        dest.copy_done().await?;

        server.execute("COMMIT").await?;

        // Stream remaining changes.
        slot.start_replication(&mut server).await?;
        while let Some(data) = slot.replicate(&mut server).await? {
            match data {
                ReplicationData::CopyData(data) => {
                    if let Some(meta) = data.replication_meta() {
                        match meta {
                            ReplicationMeta::KeepAlive(ka) => {
                                // TODO: Keep track of the LSN and send the real one.
                                server
                                    .send_one(&StatusUpdate::from(ka).wrapped()?.into())
                                    .await?;
                                server.send_one(&CopyDone.into()).await?;
                                server.flush().await?;
                            }

                            _ => (),
                        }
                    }
                }
                ReplicationData::CopyDone => {
                    // TODO: Verify we flushed everything.
                    // server.send_one(&CopyDone.into()).await?;
                    // server.flush().await?;
                }
            }
        }
        slot.drop_slot(&mut server).await?;

        Ok(())
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

        publication.cleanup().await;
    }
}
