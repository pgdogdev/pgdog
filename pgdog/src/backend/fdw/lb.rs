use std::sync::Arc;

use pgdog_config::Role;
use tokio::spawn;

use crate::backend::fdw::PostgresLauncher;
use crate::backend::pool::lb::ban::Ban;
use crate::backend::pool::{Guard, Request};
use crate::backend::{Cluster, LoadBalancer, Pool};

use super::Error;
use super::PostgresProcess;

#[derive(Clone, Debug)]
pub(crate) struct FdwLoadBalancer {
    lb: Arc<LoadBalancer>,
}

impl FdwLoadBalancer {
    pub(crate) fn new(cluster: &Cluster) -> Result<Self, Error> {
        let port = PostgresLauncher::get().port();
        let configs = PostgresProcess::connection_pool_configs(port, cluster)?;
        let primary = configs
            .iter()
            .find(|p| p.0 == Role::Primary)
            .map(|p| Pool::new(&p.1));
        let addrs: Vec<_> = configs.iter().map(|c| c.1.clone()).collect();

        let lb = Arc::new(LoadBalancer::new(
            &primary,
            &addrs,
            cluster.lb_strategy(),
            cluster.rw_split(),
        ));

        Ok(Self { lb })
    }

    pub(crate) fn launch(&self) {
        let lb = self.lb.clone();
        spawn(async move {
            let launcher = PostgresLauncher::get();
            launcher.wait_ready().await;
            lb.launch();
        });
    }

    pub(crate) fn primary(&self) -> Option<Pool> {
        self.lb.primary().cloned()
    }

    pub(crate) fn pools_with_roles_and_bans(&self) -> Vec<(Role, Ban, Pool)> {
        self.lb.pools_with_roles_and_bans()
    }

    pub(crate) async fn get(
        &self,
        request: &Request,
    ) -> Result<Guard, crate::backend::pool::Error> {
        self.lb.get(request).await
    }
}
