//! Run an internal query and collect its result instead of sending it to the client.

use crate::{
    backend::pool::Connection,
    frontend::{ClientRequest, Router},
    net::{DataRow, ErrorResponse, Protocol, RowDescription},
};

use super::Error;

/// Send `request` and return its rows and [`RowDescription`].
///
/// Every reply is read, so the connection stays in sync even when
/// PostgreSQL returns an error, which is returned after the drain.
pub(crate) async fn fetch_rows(
    backend: &mut Connection,
    request: &ClientRequest,
) -> Result<(Vec<DataRow>, Option<RowDescription>), Error> {
    backend
        .handle_client_request(request, &mut Router::default(), false)
        .await?;

    let mut row_description = None;
    let mut data_rows = Vec::new();
    let mut error = None;

    while backend.has_more_messages() {
        let message = backend.read().await?;
        match message.code() {
            'D' => data_rows.push(DataRow::try_from(message)?),
            'T' => row_description = Some(RowDescription::try_from(message)?),
            'E' if error.is_none() => error = Some(ErrorResponse::try_from(message)?),
            _ => (),
        }
    }

    match error {
        Some(error) => Err(error.into()),
        None => Ok((data_rows, row_description)),
    }
}
