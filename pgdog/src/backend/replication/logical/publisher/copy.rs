use crate::{
    backend::Server,
    net::{CopyData, ErrorResponse, FromBytes, Protocol, Query, ToBytes},
};
use tracing::trace;

use super::{
    super::{CopyStatement, Error},
    Table,
};

#[derive(Debug, Clone)]
pub(crate) struct Copy {
    stmt: CopyStatement,
}

impl Copy {
    pub(crate) fn new(table: &Table) -> Self {
        let stmt = CopyStatement::new(
            &table.table.schema,
            &table.table.name,
            &table
                .columns
                .iter()
                .map(|c| c.name.clone())
                .collect::<Vec<_>>(),
        );

        Self { stmt }
    }

    pub(crate) async fn start(&self, server: &mut Server) -> Result<(), Error> {
        if !server.in_transaction() {
            return Err(Error::TransactionNotStarted);
        }

        server
            .send(&vec![Query::new(self.stmt.copy_out()).into()].into())
            .await?;
        let result = server.read().await?;
        match result.code() {
            'E' => return Err(ErrorResponse::from_bytes(result.to_bytes()?)?.into()),
            'H' => (),
            c => return Err(Error::OutOfSync(c)),
        }
        Ok(())
    }

    pub(crate) async fn data(&self, server: &mut Server) -> Result<Option<CopyData>, Error> {
        loop {
            let msg = server.read().await?;

            match msg.code() {
                'd' => {
                    let data = CopyData::from_bytes(msg.to_bytes()?)?;
                    trace!("[{}] --> {:?}", server.addr().addr().await?, data);
                    return Ok(Some(data));
                }
                'C' => (),
                'c' => (), // CopyDone.
                'Z' => return Ok(None),
                'E' => return Err(ErrorResponse::from_bytes(msg.to_bytes()?)?.into()),
                c => return Err(Error::OutOfSync(c)),
            }
        }
    }

    pub(crate) fn statement(&self) -> &CopyStatement {
        &self.stmt
    }
}
