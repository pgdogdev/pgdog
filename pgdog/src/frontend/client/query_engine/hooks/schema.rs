use std::collections::HashSet;
use tracing::debug;

use crate::backend::{Cluster, Error, databases::reload_schema};

pub(crate) async fn schema_changed(
    cluster: &Cluster,
    shards: &HashSet<usize>,
) -> Result<(), Error> {
    debug!("schema change detected, refreshing schema cache");
    reload_schema(cluster, shards)
}
