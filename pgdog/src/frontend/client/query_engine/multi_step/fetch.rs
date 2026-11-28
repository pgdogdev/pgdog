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
    // TODO(jkaczman): We should return an error if `data_rows` grows past a limit well below 1 GB (e.g., 100 MB) to ensure that we don't run out of memory
    // Currently, we keep all fetched rows in memory, then send the rows to the shards in Bind msg, which Postgres will reject if it's over 1 GB
    //
    // PERF(jkaczman): We could remove the intermediate Vec `data_rows`, and instead copy the rows directly into the unnest(..) arrays as we read them to avoid extra allocations
    // Currently, we allocate a DataRow in `data_rows` per row that we read
    // We then copy all of the rows into the arrays when we do the subquery rewrite in `new_select_with_unnest`
    // Based on some rudimentary benchmarking I did, this would remove roughly 35% of instructions corresponding to the JOIN overhead
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
