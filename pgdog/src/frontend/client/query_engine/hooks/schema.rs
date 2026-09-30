use std::collections::HashSet;
use tracing::debug;

use crate::backend::{Error, databases::reload_schema};

pub(crate) async fn schema_changed(database: &str, shards: &HashSet<usize>) -> Result<(), Error> {
    debug!("schema change detected, refreshing schema cache");
    reload_schema(database, shards)
}
