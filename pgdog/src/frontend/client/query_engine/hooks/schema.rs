use tracing::debug;

use crate::backend::{Error, databases::reload_from_existing};

pub(crate) fn schema_changed() -> Result<(), Error> {
    debug!("schema change detected, refreshing schema cache");
    reload_from_existing()
}
