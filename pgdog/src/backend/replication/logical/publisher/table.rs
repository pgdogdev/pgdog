//! Table.

use crate::backend::pool::Address;
use crate::backend::{Server, ServerOptions};
use crate::net::replication::{ReplicationMeta, StatusUpdate};
use crate::net::{CopyDone, ErrorResponse, FromBytes, Protocol, ToBytes};

use super::super::Error;
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

    pub async fn data_sync(&mut self, source: &Address) -> Result<(), Error> {
        let mut server = Server::connect(source, ServerOptions::new_replication()).await?;
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

        while let Some(data_row) = copy.data(&mut server).await? {}
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

    // pub fn new(schema: &str, name: &str, columns: &[String]) -> Self {
    //     Self {
    //         schema: schema.to_owned(),
    //         name: name.to_owned(),
    //         columns: columns.to_vec(),
    //     }
    // }

    pub async fn start_copy_out(&self, source: &mut Server) -> Result<(), Error> {
        if !source.in_transaction() {
            return Err(Error::TransactionNotStarted);
        }

        // let copy_stmt = CopyStatement::new(&self.schema, &self.name, &self.columns).copy_out();

        // let stmt = pg_query::parse(&copy_stmt.to_string())?;
        // let copy = stmt
        //     .protobuf
        //     .stmts
        //     .first()
        //     .ok_or(Error::Copy)?
        //     .stmt
        //     .as_ref()
        //     .ok_or(Error::Copy)?;
        // let mut copy = match copy.node {
        //     Some(NodeEnum::CopyStmt(ref stmt)) => CopyParser::new(stmt, destination)
        //         .map_err(|_| Error::Copy)?
        //         .ok_or(Error::Copy)?,

        //     _ => return Err(Error::Copy),
        // };

        // source
        //     .send(&vec![Query::new(copy_stmt.to_string()).into()].into())
        //     .await?;

        let reply = source.read().await?;
        match reply.code() {
            'E' => {
                return Err(Error::PgError(ErrorResponse::from_bytes(
                    reply.to_bytes()?,
                )?))
            }
            'G' => (),
            c => return Err(Error::OutOfSync(c)),
        }

        // let mut conn = Connection::new(destination.user(), destination.name(), false, &None)?;
        // conn.connect(&Request::default(), &Route::write(None))
        //     .await?;

        // conn.send(&vec![Query::new(copy_in).into()].into()).await?;

        // let mut buffer = vec![];

        // while source.has_more_messages() {
        //     let msg = source.read().await?;
        //     match msg.code() {
        //         'd' => {
        //             buffer.push(CopyData::from_bytes(msg.to_bytes()?)?);
        //             let sharded = copy
        //                 .shard(std::mem::take(&mut buffer))
        //                 .map_err(|_| Error::OutOfSync)?;
        //             conn.send_copy(sharded).await?;
        //         }

        //         'E' => {
        //             ErrorResponse::from_bytes(msg.to_bytes()?)?;
        //             return Err(Error::OutOfSync);
        //         }

        //         _ => return Err(Error::OutOfSync),
        //     }
        // }

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use std::time::Duration;

    use tokio::time::sleep;

    use crate::backend::{
        replication::logical::publisher::test::setup_publication, server::test::test_server,
    };

    use super::*;

    #[tokio::test]
    #[ignore]
    async fn test_publication() {
        crate::logger();
        let mut publication = setup_publication().await;
        let tables = Table::load("publication_test", &mut publication.server)
            .await
            .unwrap();

        let mut server = test_server().await;

        sleep(Duration::from_millis(1_000)).await;

        for mut table in tables {
            table.data_sync(&publication.server.addr()).await.unwrap();
        }
        publication.cleanup().await;
    }
}
