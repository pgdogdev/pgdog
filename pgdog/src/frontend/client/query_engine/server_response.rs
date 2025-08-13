//! Handle server response directly inside the engine.

use tokio::time::timeout;

use crate::{
    backend::pool::Connection,
    frontend::{
        client::{
            query_engine::server_message::ServerMessage, timeouts::Timeouts,
            transaction::Transaction,
        },
        Error, Stats,
    },
    net::Protocol,
};

use super::engine_impl::Stream;

pub struct ServerResponseResult {
    pub(super) done: bool,
    pub(super) streaming: bool,
    pub(super) in_transaction: bool,
}

pub struct ServerResponse<'a> {
    // Backend connection.
    backend: &'a mut Connection,
    // Statistics.
    stats: &'a mut Stats,
    // Timeouts
    timeouts: &'a mut Timeouts,
}

impl<'a> ServerResponse<'a> {
    /// Creates a new ServerResponse instance.
    ///
    /// # Arguments
    /// * `backend` - Mutable reference to the backend connection
    /// * `stats` - Mutable reference to query statistics
    /// * `transaction` - Mutable reference to transaction state
    /// * `timeouts` - Mutable reference to timeout configuration
    ///
    pub fn new(
        backend: &'a mut Connection,
        stats: &'a mut Stats,
        timeouts: &'a mut Timeouts,
    ) -> Self {
        Self {
            backend,
            stats,
            timeouts,
        }
    }

    pub async fn handle(
        &'a mut self,
        client_socket: &mut Stream,
    ) -> Result<ServerResponseResult, Error> {
        let query_timeout = self.timeouts.query_timeout(&self.stats.state);
        let mut streaming = false;
        let mut in_transaction = false;

        while self.backend.has_more_messages() {
            let message = timeout(query_timeout, self.backend.read()).await??;
            streaming = message.streaming();

            in_transaction = ServerMessage::new(self.backend, self.stats)
                .handle(message, client_socket)
                .await?;
        }

        Ok(ServerResponseResult {
            done: self.backend.done(),
            streaming,
            in_transaction,
        })
    }
}
