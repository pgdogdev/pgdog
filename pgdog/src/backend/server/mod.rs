//! PostgreSQL server connection.

mod connection;
mod execution;
mod lifecycle;
mod messaging;
mod state;

use std::time::Duration;

use bytes::BytesMut;
use tokio::time::Instant;

pub use super::pool::Address;
pub use super::prepared_statements::HandleResult;

use super::{Error, PreparedStatements, ServerOptions, Stats};
use crate::{
    config::PoolerMode,
    net::{messages::BackendKeyData, parameter::Parameters, Stream},
    state::State,
    stats::memory::MemoryUsage,
};

/// PostgreSQL server connection.
#[derive(Debug)]
pub struct Server {
    pub(crate) addr: Address,
    pub(crate) stream: Option<Stream>,
    pub(crate) id: BackendKeyData,
    pub(crate) params: Parameters,
    pub(crate) startup_options: ServerOptions,
    pub(crate) changed_params: Parameters,
    pub(crate) client_params: Parameters,
    pub(crate) stats: Stats,
    pub(crate) prepared_statements: PreparedStatements,
    pub(crate) dirty: bool,
    pub(crate) streaming: bool,
    pub(crate) schema_changed: bool,
    pub(crate) sync_prepared: bool,
    pub(crate) in_transaction: bool,
    pub(crate) re_synced: bool,
    pub(crate) replication_mode: bool,
    pub(crate) pooler_mode: PoolerMode,
    pub(crate) stream_buffer: BytesMut,
}

impl MemoryUsage for Server {
    #[inline]
    fn memory_usage(&self) -> usize {
        std::mem::size_of::<BackendKeyData>()
            + self.params.memory_usage()
            + self.changed_params.memory_usage()
            + self.client_params.memory_usage()
            + std::mem::size_of::<Stats>()
            + self.prepared_statements.memory_used()
            + 7 * std::mem::size_of::<bool>()
            + std::mem::size_of::<PoolerMode>()
            + self.stream_buffer.capacity()
    }
}

impl Server {
    /// We can disconnect from this server.
    ///
    /// There are no more expected messages from the server connection
    /// and we haven't started an explicit transaction.
    pub fn done(&self) -> bool {
        self.prepared_statements.done() && !self.in_transaction()
    }

    /// Server can execute a query.
    pub fn in_sync(&self) -> bool {
        matches!(
            self.stats.state,
            State::Idle | State::IdleInTransaction | State::TransactionError
        )
    }

    /// Server is done executing all queries and is
    /// not inside a transaction.
    pub fn can_check_in(&self) -> bool {
        self.stats.state == State::Idle
    }

    /// Server hasn't sent all messages yet.
    pub fn has_more_messages(&self) -> bool {
        self.prepared_statements.has_more_messages() || self.streaming
    }

    pub fn copy_mode(&self) -> bool {
        self.prepared_statements.copy_mode()
    }

    /// Server is still inside a transaction.
    #[inline]
    pub fn in_transaction(&self) -> bool {
        self.in_transaction
    }

    /// The server connection permanently failed.
    #[inline]
    pub fn error(&self) -> bool {
        self.stats.state == State::Error
    }

    /// Did the schema change and prepared statements are broken.
    pub fn schema_changed(&self) -> bool {
        self.schema_changed
    }

    /// Prepared statements changed outside of our pipeline,
    /// need to resync from `pg_prepared_statements` view.
    pub fn sync_prepared(&self) -> bool {
        self.sync_prepared
    }

    /// Connection was left with an unfinished query.
    pub fn needs_drain(&self) -> bool {
        !self.in_sync()
    }

    /// Close the connection, don't do any recovery.
    pub fn force_close(&self) -> bool {
        self.stats.state == State::ForceClose
    }

    /// Server parameters.
    #[inline]
    pub fn params(&self) -> &Parameters {
        &self.params
    }

    /// Server connection unique identifier.
    #[inline]
    pub fn id(&self) -> &BackendKeyData {
        &self.id
    }

    /// How old this connection is.
    #[inline]
    pub fn age(&self, instant: Instant) -> Duration {
        instant.duration_since(self.stats.created_at)
    }

    /// How long this connection has been idle.
    #[inline]
    pub fn idle_for(&self, instant: Instant) -> Duration {
        instant.duration_since(self.stats.last_used)
    }

    /// How long has it been since the last connection healthcheck.
    #[inline]
    pub fn healthcheck_age(&self, instant: Instant) -> Duration {
        if let Some(last_healthcheck) = self.stats.last_healthcheck {
            instant.duration_since(last_healthcheck)
        } else {
            Duration::MAX
        }
    }

    /// Get server address.
    #[inline]
    pub fn addr(&self) -> &Address {
        &self.addr
    }

    #[inline]
    pub(crate) fn stream(&mut self) -> &mut Stream {
        self.stream.as_mut().unwrap()
    }

    /// Server needs a cleanup because client changed a session variable
    /// of parameter.
    #[inline]
    pub fn dirty(&self) -> bool {
        self.dirty
    }

    #[inline]
    pub fn mark_dirty(&mut self, dirty: bool) {
        self.dirty = dirty;
    }

    /// Server has been cleaned.
    #[inline]
    pub(super) fn cleaned(&mut self) {
        self.dirty = false;
    }

    /// Server is streaming data.
    #[inline]
    pub fn streaming(&self) -> bool {
        self.streaming
    }

    #[inline]
    pub fn stats(&self) -> &Stats {
        &self.stats
    }

    #[inline]
    pub fn stats_mut(&mut self) -> &mut Stats {
        &mut self.stats
    }

    #[inline]
    pub fn set_pooler_mode(&mut self, pooler_mode: PoolerMode) {
        self.pooler_mode = pooler_mode;
    }

    #[inline]
    pub fn pooler_mode(&self) -> &PoolerMode {
        &self.pooler_mode
    }

    #[inline]
    pub fn replication_mode(&self) -> bool {
        self.replication_mode
    }

    #[inline]
    pub fn prepared_statements_mut(&mut self) -> &mut PreparedStatements {
        &mut self.prepared_statements
    }

    #[inline]
    pub fn prepared_statements(&self) -> &PreparedStatements {
        &self.prepared_statements
    }
}

use crate::net::messages::{Terminate, ToBytes};
use tokio::spawn;

impl Drop for Server {
    fn drop(&mut self) {
        self.stats.disconnect();
        if let Some(mut stream) = self.stream.take() {
            let out_of_sync = if self.done() {
                " ".into()
            } else {
                format!(" {} ", self.stats.state)
            };
            info!("closing{}server connection [{}]", out_of_sync, self.addr,);

            spawn(async move {
                stream.write_all(&Terminate.to_bytes()?).await?;
                stream.flush().await?;
                Ok::<(), Error>(())
            });
        }
    }
}

use tokio::io::AsyncWriteExt;
use tracing::info;

// Used for testing.
#[cfg(test)]
pub mod test {
    use crate::net::*;

    use super::*;

    impl Default for Server {
        fn default() -> Self {
            let id = BackendKeyData::default();
            let addr = Address::default();
            Self {
                stream: None,
                id,
                startup_options: ServerOptions::default(),
                params: Parameters::default(),
                changed_params: Parameters::default(),
                client_params: Parameters::default(),
                stats: Stats::connect(id, &addr, &Parameters::default()),
                prepared_statements: super::PreparedStatements::new(),
                addr,
                dirty: false,
                streaming: false,
                schema_changed: false,
                sync_prepared: false,
                in_transaction: false,
                re_synced: false,
                replication_mode: false,
                pooler_mode: PoolerMode::Transaction,
                stream_buffer: BytesMut::with_capacity(1024),
            }
        }
    }

    impl Server {
        pub fn new_error() -> Server {
            let mut server = Server::default();
            server.stats.state(State::Error);

            server
        }
    }

    pub async fn test_server() -> Server {
        Server::connect(&Address::new_test(), ServerOptions::default())
            .await
            .unwrap()
    }

    pub async fn test_replication_server() -> Server {
        Server::connect(&Address::new_test(), ServerOptions::new_replication())
            .await
            .unwrap()
    }
}
