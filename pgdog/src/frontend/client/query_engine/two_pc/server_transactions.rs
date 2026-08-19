use crate::{
    backend::{Error, Server},
    net::DataRow,
};

pub(crate) struct TwoPcServerTransaction;

/// Load two phase transactions from the server.
pub(crate) async fn load(server: &mut Server) -> Result<Vec<TwoPcServerTransaction>, Error> {
    let records: Vec<DataRow> = server
        .fetch_all("SELECT gid FROM pg_prepared_xacts")
        .await?;

    Ok(records
        .iter()
        .filter(|record| record.get_text(0).is_some())
        .map(|_| TwoPcServerTransaction)
        .collect())
}
