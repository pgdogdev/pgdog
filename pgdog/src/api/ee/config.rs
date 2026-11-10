use pgdog_config::ConfigAndUsers;

use crate::api::Error;

pub(in crate::api) async fn sync_config(_config: &ConfigAndUsers) -> Result<(), Error> {
    Ok(())
}
