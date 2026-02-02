use crate::backend::{Cluster, LoadBalancer, Pool};

use super::Error;
use super::PostgresProcess;

pub(crate) struct FdwLoadBalancer {
    lb: LoadBalancer,
}

impl FdwLoadBalancer {
    pub(crate) fn new(cluster: &Cluster) -> Result<Self, Error> {
        // We check that all shards have identical configs
        // before enabling this feature.
        let pools = PostgresProcess::pools_to_databases(cluster, 0)?;
        let pools = cluster
            .shards()
            .get(0)
            .ok_or(Error::ShardsHostsMismatch)?
            .pools_with_roles();
        todo!()
    }
}
