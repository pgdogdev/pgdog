use crate::{
    backend::pool::Connection,
    frontend::{Comms, Router, Stats},
};

use tracing::debug;

use super::{Client, Error};

pub(super) struct Inner {
    /// Client connection to server(s).
    pub(super) backend: Connection,
    /// Query router.
    pub(super) router: Router,
    /// Client stats.
    pub(super) stats: Stats,
    /// Protocol is async.
    pub(super) async_: bool,
    /// Start transactio statement, intercepted by the router.
    pub(super) start_transaction: Option<String>,
    /// Client-wide comms.
    pub(super) comms: Comms,
}

impl Inner {
    pub fn new(client: &Client) -> Result<Self, Error> {
        let user = client.params.get_required("user")?;
        let database = client.params.get_default("database", user);

        let mut backend = Connection::new(user, database, client.admin)?;
        let mut router = Router::new();
        let stats = Stats::new();
        let async_ = false;
        let start_transaction = None;
        let comms = client.comms.clone();

        if client.shard.is_some() {
            if let Some(config) = backend.cluster()?.replication_sharding_config() {
                backend.replication_mode(client.shard, &config)?;
                router.replication_mode();
                debug!("logical replication sharding [{}]", client.addr);
            }
        }

        Ok(Self {
            backend,
            router,
            stats,
            async_,
            start_transaction,
            comms,
        })
    }
}
