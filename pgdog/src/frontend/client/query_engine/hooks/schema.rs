use tracing::debug;

use crate::backend::{databases::reload_from_existing, Error};

pub(crate) fn schema_changed() -> Result<(), Error> {
    debug!("schema change detected, refreshing schema cache");
    reload_from_existing()
}
