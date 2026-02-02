use crate::{
    backend::Server,
    net::{CopyData, ErrorResponse, FromBytes, Protocol, Query, ToBytes},
};
use pgdog_config::CopyFormat;
use tracing::{debug, trace};

use super::{
    super::{CopyStatement, Error},
    Table,
};

#[derive(Debug, Clone)]
pub struct Copy {
    stmt: CopyStatement,
}

impl Copy {
    pub fn new(table: &Table, copy_format: CopyFormat) -> Self {
        let stmt = CopyStatement::new(
            &table.table,
            &table
                .columns
                .iter()
                .map(|c| c.name.clone())
                .collect::<Vec<_>>(),
            copy_format,
        );

        Self { stmt }
    }

    pub async fn start(&self, server: &mut Server) -> Result<(), Error> {
        if !server.in_transaction() {
            return Err(Error::TransactionNotStarted);
        }

        let query = Query::new(self.stmt.copy_out());
        debug!("{} [{}]", query.query(), server.addr());

        server.send(&vec![query.into()].into()).await?;
        let result = server.read().await?;
        match result.code() {
            'E' => return Err(ErrorResponse::from_bytes(result.to_bytes()?)?.into()),
            'H' => (),
            c => return Err(Error::OutOfSync(c)),
        }
        Ok(())
    }

    pub async fn data(&self, server: &mut Server) -> Result<Option<CopyData>, Error> {
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

    pub fn statement(&self) -> &CopyStatement {
        &self.stmt
    }
}
