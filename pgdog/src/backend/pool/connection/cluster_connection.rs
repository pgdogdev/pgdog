use super::Error;
use crate::{
    backend::{
        Cluster, databases,
        pool::{
            Error as PoolError, Guard, Request,
            connection::mirror::{Mirror, MirrorHandler},
        },
        reload_notify,
    },
    config::config,
    frontend::router::{Route, parser::Shard},
};
use pgdog_config::{PoolerMode, User, users::PasswordKind};
use tokio_util::sync::CancellationToken;
use tracing::debug;

#[derive(Default, Debug)]
pub(super) struct ClusterConnection {
    user: String,
    database: String,
    /// Cluster smart pointer.
    /// Swapped at reload time, hence optional.
    cluster: Option<Cluster>,
    /// Cancelled when an admin terminates the cluster (`FORCE_RELOAD`).
    query_cancellation: CancellationToken,
    /// Traffic mirrors.
    mirrors: Vec<MirrorHandler>,
}

impl ClusterConnection {
    /// Create new cluster connection handler.
    pub(super) fn new(user: &str, database: &str) -> Self {
        Self {
            user: user.to_string(),
            database: database.to_string(),
            ..Default::default()
        }
    }

    /// Get cluster reference.
    pub(super) fn cluster(&self) -> Result<&Cluster, Error> {
        self.cluster.as_ref().ok_or(Error::ClusterNotConnected)
    }

    /// Get mirror references.
    pub(super) fn mirrors(&mut self) -> &mut [MirrorHandler] {
        &mut self.mirrors
    }

    /// Get cluster query cancellation token.
    pub(super) fn query_cancellation_token(&self) -> CancellationToken {
        self.query_cancellation.clone()
    }

    /// Perform an atomic and safe reload of the cluster config. This is needed
    /// because the global which holds the clusters is not atomically updated.
    pub(super) async fn safe_reload(&mut self) -> Result<(), Error> {
        if let Some(wait) = reload_notify::ready() {
            wait.await;
        }

        self.reload()
    }

    /// Reload connection state.
    pub(super) fn reload(&mut self) -> Result<(), Error> {
        self.init_passthrough_auth()?;

        let cluster = databases::databases().cluster(self.identity())?;
        self.cluster = Some(cluster);
        self.query_cancellation = self.cluster()?.get_cancellation_token().child_token();

        self.init_mirrors()?;

        Ok(())
    }

    /// Get all connections necessary to serve the route.
    pub(super) async fn get_conns(
        &mut self,
        request: &Request,
        route: &Route,
    ) -> Result<(Vec<Guard>, Vec<usize>), Error> {
        let mut conns = vec![];
        let shards = self.cluster()?.shards().len();

        let indices = (0..shards)
            .filter(|shard_number| {
                if let Shard::Multi(numbers) = route.shard()
                    && !numbers.contains(shard_number)
                {
                    false
                } else {
                    true
                }
            })
            .collect::<Vec<_>>();

        for index in &indices {
            conns.push(self.get_conn(request, *index, route.is_read()).await?);
        }

        // TODO(lev): this doesn't protect against address changes for shards.
        if shards != self.cluster()?.shards().len() {
            return Err(Error::Pool(PoolError::Offline));
        }

        Ok((conns, indices))
    }

    /// Get a connection from the cluster.
    pub(super) async fn get_conn(
        &mut self,
        request: &Request,
        shard: usize,
        is_read: bool,
    ) -> Result<Guard, Error> {
        match self.get_conn_internal(request, shard, is_read).await {
            Ok(conn) => Ok(conn),
            Err(Error::Pool(PoolError::Offline | PoolError::AllReplicasDown)) => {
                debug!(
                    "detected configuration reload, reloading [{}]",
                    self.cluster()?.identifier()
                );

                self.safe_reload().await?;
                self.get_conn_internal(request, shard, is_read).await
            }
            Err(err) => Err(err),
        }
    }

    fn identity(&self) -> (&str, &str) {
        (self.user.as_str(), self.database.as_str())
    }

    async fn get_conn_internal(
        &self,
        request: &Request,
        shard: usize,
        is_read: bool,
    ) -> Result<Guard, Error> {
        let mut server = if is_read {
            self.cluster()?.replica(shard, request).await?
        } else {
            self.cluster()?.primary(shard, request).await?
        };

        if self.is_in_session_mode() {
            server.reset = true;
        }

        Ok(server)
    }

    fn is_in_session_mode(&self) -> bool {
        self.cluster()
            .map(|c| c.pooler_mode() == PoolerMode::Session)
            .unwrap_or(true)
    }

    // Check if we need re-configure passthrough auth using our existing password.
    //
    // This happens on configuration reload (RELOAD/sighup), because we
    // only load databases from the config. RELOAD effectively removes all passthrough
    // connection pools until a client needs to query it and we re-create it.
    //
    // This is atomic and idempotent.
    fn init_passthrough_auth(&self) -> Result<(), Error> {
        let config = config();
        let databases = databases::databases();

        let user = self.identity();

        if config.config.general.passthrough_auth() && databases.passwords(user).is_none() {
            let mut user = User {
                name: user.0.to_string(),
                database: user.1.to_string(),
                ..Default::default()
            };

            for pass in self.cluster()?.passwords() {
                match pass {
                    PasswordKind::Hashed(hashed) => {
                        user.password_hash = Some(hashed.clone());
                    }

                    PasswordKind::Plain(plain) => {
                        user.passwords.push(plain.clone());
                    }

                    // Vault static roles are for client auth only; skip for passthrough.
                    PasswordKind::VaultStaticRole(_) => {}
                }
            }

            databases::add(user)?;
        }

        Ok(())
    }

    fn init_mirrors(&mut self) -> Result<(), Error> {
        let databases = databases::databases();

        self.mirrors = databases
            .mirrors(self.identity())?
            .unwrap_or(&[])
            .iter()
            .map(|dest_cluster| {
                let mirror_config =
                    databases.mirror_config(self.cluster()?.name(), dest_cluster.name());
                Mirror::spawn(self.cluster()?.name(), dest_cluster, mirror_config)
            })
            .collect::<Result<Vec<_>, Error>>()?;

        debug!(
            r#"database "{}" has {} mirrors"#,
            self.cluster()?.name(),
            self.mirrors.len()
        );

        Ok(())
    }
}
