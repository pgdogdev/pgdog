use crate::{
    backend::Server,
    net::{CopyData, FromBytes, Protocol, Query, ToBytes},
};
use tracing::trace;

use super::{
    super::{CopyStatement, Error},
    Table,
};

#[derive(Debug, Clone)]
pub struct Copy {
    stmt: CopyStatement,
}

impl Copy {
    pub fn new(table: &Table) -> Self {
        let stmt = CopyStatement::new(
            &table.table.schema,
            &table.table.name,
            &table
                .columns
                .iter()
                .map(|c| c.name.clone())
                .collect::<Vec<_>>(),
        )
        .copy_out();

        Self { stmt }
    }

    pub async fn start(&self, server: &mut Server) -> Result<(), Error> {
        if !server.in_transaction() {
            return Err(Error::TransactionNotStarted);
        }

        server
            .send(&vec![Query::new(self.stmt.to_string()).into()].into())
            .await?;
        let result = server.read().await?;
        if result.code() != 'H' {
            return Err(Error::OutOfSync(result.code()));
        }

        Ok(())
    }

    pub async fn data(&self, server: &mut Server) -> Result<Option<CopyData>, Error> {
        loop {
            let msg = server.read().await?;

            match msg.code() {
                'd' => {
                    let data = CopyData::from_bytes(msg.to_bytes()?)?;
                    trace!("{:?} [{}]", data, server.addr());
                    return Ok(Some(data));
                }
                'C' => (),
                'c' => (), // CopyDone.
                'Z' => return Ok(None),
                c => return Err(Error::OutOfSync(c)),
            }
        }
    }
}
