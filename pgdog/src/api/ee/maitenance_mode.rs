use crate::api::Error;

pub(in crate::api) async fn stop_traffic(_database: Option<String>) -> Result<(), Error> {
    Ok(())
}

pub(in crate::api) async fn resume_traffic(_database: Option<String>) -> Result<(), Error> {
    Ok(())
}
