//! Table.
use http_body_util::BodyExt;
use pg_query::NodeEnum;

use crate::backend::pool::{Connection, Request};
use crate::backend::{Cluster, Server};
use crate::frontend::router::parser::CopyParser;
use crate::frontend::router::Route;
use crate::net::{CopyData, ErrorResponse, FromBytes, Protocol, Query, ToBytes};

use super::super::{CopyStatement, Error};
use super::{PublicationTable, PublicationTableColumn, ReplicaIdentity, ReplicationSlot};

#[derive(Debug, Clone)]
pub struct Table {
    publication: String,
    table: PublicationTable,
    identity: ReplicaIdentity,
    columns: Vec<PublicationTableColumn>,
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
    pub async fn upsert(&self) -> String {
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
        let on_conflict = format!(
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
        );

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

    pub async fn data_sync(&mut self, server: &mut Server) -> Result<(), Error> {
        server
            .execute("BEGIN READ ONLY ISOLATION LEVEL REPEATABLE READ")
            .await?;
        self.reload(server).await?;
        let mut slot = ReplicationSlot::data_sync(&self.publication);
        slot.create_slot(server).await?;

        let copy_out = CopyStatement::new(
            &self.table.schema,
            &self.table.name,
            &self
                .columns
                .iter()
                .map(|c| c.name.clone())
                .collect::<Vec<_>>(),
        )
        .copy_out();

        // let copy_in = copy_out.clone().copy_in();

        todo!()
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
            _ => return Err(Error::OutOfSync),
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
