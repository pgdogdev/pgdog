use std::ops::{Deref, DerefMut};

use crate::{
    backend::{
        pool::{Connection, Request},
        Error as BackendError,
    },
    frontend::{
        buffer::BufferedQuery, router::Error as RouterError, Buffer, Command, Comms,
        PreparedStatements, Router, RouterContext, Stats,
    },
    net::Parameters,
    state::State,
};

use tracing::debug;

use super::{Client, Error};

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
    pub(super) is_async: bool,
    /// Start transaction statement, intercepted by the router.
    pub(super) start_transaction: Option<BufferedQuery>,
    /// Client-wide comms.
    pub(super) comms: Comms,
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
            is_async: false,
            start_transaction: None,
            comms: client.comms.clone(),
        })
    }

    /// Get the query from the buffer and figure out what it wants to do.
    pub(super) fn command(
        &mut self,
        buffer: &mut Buffer,
        prepared_statements: &mut PreparedStatements,
        params: &Parameters,
    ) -> Result<Option<&Command>, RouterError> {
        let command = self
            .backend
            .cluster()
            .ok()
            .map(|cluster| {
                // Build router context.
                let context = RouterContext::new(
                    buffer,              // Query and parameters.
                    cluster,             // Cluster configuration.
                    prepared_statements, // Prepared statements.
                    params,              // Client connection parameters.
                )?;
                self.router.query(context)
            })
            .transpose()?;

        if let Some(Command::Rewrite(query)) = command {
            buffer.rewrite(query)?;
        }

        Ok(command)
    }

    /// Reset query router context.
    pub(super) fn reset_router(&mut self) {
        self.router.reset();
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

    pub(super) async fn handle_buffer(
        &mut self,
        buffer: &Buffer,
        streaming: bool,
    ) -> Result<(), Error> {
        self.backend
            .handle_buffer(buffer, &mut self.router, streaming)
            .await?;

        Ok(())
    }

    /// Connect to a backend (or multiple).
    pub(super) async fn connect(&mut self, request: &Request) -> Result<(), BackendError> {
        // Use currently determined route.
        let route = self.router.route();

        self.stats.waiting(request.created_at);

        let result = self.backend.connect(request, &route).await;

        if result.is_ok() {
            self.stats.connected();
            if let Ok(addr) = self.backend.addr() {
                debug!(
                    "client paired with [{}] using route [{}] [{:.4}ms]",
                    addr.into_iter()
                        .map(|a| a.to_string())
                        .collect::<Vec<_>>()
                        .join(","),
                    route,
                    self.stats.wait_time.as_secs_f64() * 1000.0
                );
            }
        } else {
            self.stats.error();
        }

        self.comms.stats(self.stats);

        result
    }

    pub(super) fn done(&mut self, in_transaction: bool) {
        if in_transaction {
            self.stats.state = State::IdleInTransaction;
        } else {
            self.stats.state = State::Idle;
        }
    }

    /// Mutably borrow this,
    /// while ensuring maintenance tasks are performed when
    /// the borrow is finished.
    #[inline(always)]
    pub(super) fn get(&mut self) -> InnerBorrow {
        InnerBorrow { inner: self }
    }
}

/// Makes sure that when Inner reference is dropped,
/// tasks that maintain the global state are performed.
///
/// e.g. updating client stats after every request by the client
/// or response by the server.
pub(super) struct InnerBorrow<'a> {
    inner: &'a mut Inner,
}

impl Deref for InnerBorrow<'_> {
    type Target = Inner;

    fn deref(&self) -> &Self::Target {
        self.inner
    }
}

impl DerefMut for InnerBorrow<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.inner
    }
}

impl Drop for InnerBorrow<'_> {
    fn drop(&mut self) {
        self.comms.stats(self.inner.stats);
    }
}
