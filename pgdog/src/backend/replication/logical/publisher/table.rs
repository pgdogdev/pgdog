//! Table.
use pg_query::NodeEnum;

use crate::backend::pool::{Connection, Request};
use crate::backend::{Cluster, Server};
use crate::frontend::router::parser::CopyParser;
use crate::frontend::router::Route;
use crate::net::{CopyData, ErrorResponse, FromBytes, Protocol, Query, ToBytes};

use super::super::{CopyStatement, Error};

#[derive(Debug, Clone)]
pub struct Table {
    schema: String,
    name: String,
    columns: Vec<String>,
}

impl Table {
    pub fn new(schema: &str, name: &str, columns: &[String]) -> Self {
        Self {
            schema: schema.to_owned(),
            name: name.to_owned(),
            columns: columns.to_vec(),
        }
    }

    pub async fn start_copy_out(&self, source: &mut Server) -> Result<(), Error> {
        if !source.in_transaction() {
            return Err(Error::TransactionNotStarted);
        }

        let copy_stmt = CopyStatement::new(&self.schema, &self.name, &self.columns).copy_out();

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

        source
            .send(&vec![Query::new(copy_stmt.to_string()).into()].into())
            .await?;

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
