use super::Error;
use crate::net::BackendKeyData;

pub(crate) async fn broadcast_cancel(_key: &BackendKeyData) -> Result<(), Error> {
    Ok(())
}
