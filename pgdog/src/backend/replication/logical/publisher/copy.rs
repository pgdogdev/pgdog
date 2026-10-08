use crate::{
    backend::Server,
    frontend::router::parser::binary::header::HEADER_SIZE,
    net::{CopyData, ErrorResponse, FromBytes, Protocol, Query, ToBytes},
};
use pgdog_config::CopyFormat;
use tokio::sync::mpsc::Sender;
use tracing::debug;

use super::super::{CopyStatement, Error};

/// Send the initial COPY OUT statement (whether serial or parallel)
pub(crate) async fn start(
    server: &mut Server,
    stmt: &CopyStatement,
    index: usize,
) -> Result<(), Error> {
    if !server.in_transaction() {
        return Err(Error::TransactionNotStarted);
    }

    let query = Query::new(stmt.copy_out(index));
    debug!("[PUBLISHER] {} [{}]", query.query(), server.addr());

    server.send(&vec![query.into()].into()).await?;
    let result = server.read().await?;
    match result.code() {
        'E' => return Err(ErrorResponse::from_bytes(result.to_bytes())?.into()),
        'H' => (),
        c => return Err(Error::OutOfSync(c)),
    }
    Ok(())
}

/// Read COPY rows from the server and send them to the subscriber until the COPY ends.
pub(crate) async fn data(
    mut server: Server,
    format: CopyFormat,
    rows: Sender<CopyData>,
) -> Result<(), Error> {
    let mut first = true;

    loop {
        let msg = server.read().await?;

        match msg.code() {
            'd' => {
                let mut data = CopyData::from_bytes(msg.to_bytes())?;

                if format == CopyFormat::Binary {
                    if std::mem::take(&mut first) {
                        // Skip header.
                        let row = data.data().get(HEADER_SIZE..).ok_or(Error::MissingData)?;
                        data = CopyData::new(row);
                    }

                    // Skip EOF.
                    if data.data() == [255, 255] {
                        continue;
                    }
                }

                if rows.send(data).await.is_err() {
                    return Ok(());
                }
            }
            'C' => (),
            'c' => (), // CopyDone.
            'Z' => return Ok(()),
            'E' => return Err(ErrorResponse::from_bytes(msg.to_bytes())?.into()),
            c => return Err(Error::OutOfSync(c)),
        }
    }
}
