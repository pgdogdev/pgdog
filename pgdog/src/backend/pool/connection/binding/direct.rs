use std::ops::{Deref, DerefMut};

use tracing::debug;

use crate::{
    backend::Error,
    frontend::{
        client::query_engine::{TwoPcPhase, two_pc::TwoPcTransaction},
        router::CopyRow,
    },
    net::ProtocolMessage,
    state::State,
};

use super::super::Guard;

#[derive(Debug)]
pub(crate) struct BindingDirect {
    server: Guard,
}

impl Deref for BindingDirect {
    type Target = Guard;

    fn deref(&self) -> &Self::Target {
        &self.server
    }
}

impl DerefMut for BindingDirect {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.server
    }
}

impl BindingDirect {
    pub(crate) fn new(server: Guard) -> Self {
        Self { server }
    }

    pub(crate) fn disconnect(self) {
        drop(self);
    }

    pub(crate) fn connected(&self) -> bool {
        true
    }

    pub(crate) fn connected_servers(&self) -> usize {
        1
    }

    pub(crate) async fn send_copy(&mut self, rows: Vec<CopyRow>) -> Result<(), Error> {
        for row in rows {
            self.server
                .send_one(&ProtocolMessage::from(row.message()))
                .await?;
        }

        Ok(())
    }

    pub(in super::super::super::connection) fn state_check(&self, state: State) -> bool {
        debug!(
            "server is in \"{}\" state [{}]",
            self.server.stats().get_state(),
            self.server.addr()
        );
        self.server.stats().get_state() == state
    }

    /// Two-phase commit requires a multi-shard binding.
    pub(crate) async fn two_pc(
        &mut self,
        _transaction: TwoPcTransaction,
        _phase: TwoPcPhase,
        _ignore_missing: bool,
    ) -> Result<(), Error> {
        Err(Error::TwoPcMultiShardOnly)
    }

    pub(crate) fn is_multishard(&self) -> bool {
        false
    }

    /// Get the connected shard number.
    pub(crate) fn direct_shard_number(&self) -> Option<usize> {
        Some(self.server.shard())
    }

    /// Number of connected shards.
    pub(crate) fn shards(&self) -> Result<usize, Error> {
        Ok(1)
    }
}
