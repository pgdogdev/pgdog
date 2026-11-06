use crate::api::Error;

pub(in crate::api) async fn cancel_queries(_database: &str) -> Result<(), Error> {
    Ok(())
}
