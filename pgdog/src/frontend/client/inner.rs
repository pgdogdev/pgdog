use crate::{
    backend::{
        pool::{Connection, Request},
        Error as BackendError,
    },
    frontend::{router::Error as RouterError, Buffer, Command, Comms, Router, Stats},
};

use tracing::debug;

use super::{counter::Counter, Client, Error};

/// Mutable internals used by both client and server message handlers.
///
/// Placed into their own struct so we can easily pass them around
/// without holding a mutable reference to self in client. This is required
/// for the `select!` macro to work.
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
    /// Message counter
    pub(super) counter: Counter,
}

impl Inner {
    pub fn new(client: &Client) -> Result<Self, Error> {
        let user = client.params.get_required("user")?;
        let database = client.params.get_default("database", user);

        let mut backend = Connection::new(user, database, client.admin)?;
        let mut router = Router::new();

        // Configure replication mode.
        if client.shard.is_some() {
            let cluster = backend.cluster()?;
            if let Some(config) = cluster.replication_sharding_config() {
                backend.replication_mode(
                    client.shard.into(),
                    &config,
                    &cluster.sharding_schema(),
                )?;
                router.replication_mode();
                debug!("logical replication sharding [{}]", client.addr);
            }
        }

        Ok(Self {
            backend,
            router,
            stats: Stats::new(),
            async_: false,
            start_transaction: None,
            comms: client.comms.clone(),
            counter: Counter::default(),
        })
    }

    /// Get the query from the buffer and figure out what it wants to do.
    pub(super) fn command(&mut self, buffer: &Buffer) -> Result<Option<&Command>, RouterError> {
        self.backend
            .cluster()
            .ok()
            .map(|cluster| self.router.query(buffer, cluster))
            .transpose()
    }

    /// Client is connected to server(s).
    pub(super) fn connected(&self) -> bool {
        self.backend.connected()
    }

    /// Server(s) are in transaction mode pooling.
    pub(super) fn transaction_mode(&self) -> bool {
        self.backend.transaction_mode()
    }

    /// Disconnect client from server(s).
    pub(super) fn disconnect(&mut self) {
        self.backend.disconnect();
    }

    pub(super) async fn connect(&mut self, request: &Request) -> Result<(), BackendError> {
        // Use currently determined route.
        let route = self.router.route();

        self.comms.stats(self.stats.waiting(request.created_at));

        let result = self.backend.connect(request, &route).await;

        if result.is_ok() {
            self.comms.stats(self.stats.connected());
            if let Ok(addr) = self.backend.addr() {
                let addrs = addr
                    .into_iter()
                    .map(|a| a.to_string())
                    .collect::<Vec<_>>()
                    .join(",");
                debug!(
                    "client paired with {} [{:.4}ms]",
                    addrs,
                    self.stats.wait_time.as_secs_f64() * 1000.0
                );
            }
        } else {
            self.comms.stats(self.stats.error());
        }

        result
    }

    pub(super) fn reset_counter(&mut self) {
        self.counter = Counter::default();
    }
}
