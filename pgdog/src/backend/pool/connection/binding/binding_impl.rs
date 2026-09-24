//! Binding between frontend client and a connection on the backend.

use crate::{
    backend::Error,
    frontend::{
        ClientRequest,
        client::query_engine::{TwoPcPhase, two_pc::TwoPcTransaction},
        router::{CopyRow, Route},
    },
    net::{FrontendPid, Message, ProtocolMessage, Query, parameter::Parameters},
    state::State,
};

use super::{AdminBinding, BindingDirect, BindingMulti, not_connected::NotConnectedBinding};

/// The server(s) the client is connected to.
#[derive(Debug)]
pub(crate) enum Binding {
    Direct(BindingDirect),
    Admin(AdminBinding),
    MultiShard(Box<BindingMulti>),
    NotConnected(NotConnectedBinding),
}

impl Default for Binding {
    fn default() -> Self {
        Self::NotConnected(NotConnectedBinding::new())
    }
}

// Dispatch statically, including methods supplied by each binding's Deref/DerefMut.
macro_rules! dispatch {
    ($this:expr, $binding:ident => $call:expr) => {
        match $this {
            Binding::Direct($binding) => $call,
            Binding::Admin($binding) => $call,
            Binding::MultiShard($binding) => $call,
            Binding::NotConnected($binding) => $call,
        }
    };
}

impl Binding {
    /// Release all server connections, keeping admin bindings intact.
    pub(crate) fn disconnect(&mut self) {
        if !matches!(self, Self::Admin(_)) {
            *self = Self::default();
        }
    }

    /// Close connections without allowing them to be reused.
    pub(crate) fn force_close(&mut self) {
        dispatch!(&mut *self, binding => binding.force_close());
        self.disconnect();
    }

    pub(crate) fn connected(&self) -> bool {
        dispatch!(self, binding => binding.connected())
    }

    pub(crate) fn connected_servers(&self) -> usize {
        dispatch!(self, binding => binding.connected_servers())
    }

    pub(crate) async fn read(&mut self) -> Result<Message, Error> {
        dispatch!(self, binding => Ok(binding.read().await?))
    }

    pub(crate) async fn send(&mut self, client_request: &ClientRequest) -> Result<(), Error> {
        dispatch!(self, binding => Ok(binding.send(client_request).await?))
    }

    /// Ignore a response only on the upcoming request's shards.
    pub(crate) async fn send_ignore(
        &mut self,
        message: &ProtocolMessage,
        route: &Route,
    ) -> Result<(), Error> {
        match self {
            Self::Direct(binding) => binding.send_ignore(message).await,
            Self::Admin(binding) => binding.send_ignore(message, route).await,
            Self::MultiShard(binding) => binding.send_ignore(message, route).await,
            Self::NotConnected(binding) => binding.send_ignore(message, route).await,
        }
    }

    pub(crate) async fn send_copy(&mut self, rows: Vec<CopyRow>) -> Result<(), Error> {
        dispatch!(self, binding => binding.send_copy(rows).await)
    }

    pub(in super::super::super::connection) fn done(&self) -> bool {
        dispatch!(self, binding => binding.done())
    }

    pub(crate) fn has_more_messages(&self) -> bool {
        dispatch!(self, binding => binding.has_more_messages())
    }

    pub(crate) fn out_of_sync(&self) -> bool {
        dispatch!(self, binding => binding.out_of_sync())
    }

    pub(in super::super::super::connection) fn state_check(&self, state: State) -> bool {
        dispatch!(self, binding => binding.state_check(state))
    }

    pub(crate) async fn execute(
        &mut self,
        query: impl Into<Query> + Clone,
    ) -> Result<Vec<Message>, Error> {
        let query: Query = query.into();
        dispatch!(self, binding => binding.execute(query).await)
    }

    pub(crate) async fn two_pc(
        &mut self,
        transaction: TwoPcTransaction,
        phase: TwoPcPhase,
        ignore_missing: bool,
    ) -> Result<(), Error> {
        dispatch!(self, binding => binding.two_pc(transaction, phase, ignore_missing).await)
    }

    pub(crate) async fn link_client(
        &mut self,
        id: FrontendPid,
        params: &Parameters,
        transaction_start_stmt: Option<&str>,
    ) -> Result<usize, Error> {
        dispatch!(self, binding => binding.link_client(id, params, transaction_start_stmt).await)
    }

    pub(crate) fn transaction_params_hook(&mut self, rollback: bool) {
        dispatch!(self, binding => binding.transaction_params_hook(rollback));
    }

    pub(crate) fn changed_params(&mut self) -> Parameters {
        match self {
            Self::Direct(binding) => binding.changed_params().clone(),
            Self::Admin(binding) => binding.changed_params(),
            Self::MultiShard(binding) => binding.changed_params(),
            Self::NotConnected(binding) => binding.changed_params(),
        }
    }

    pub(in super::super::super::connection) fn dirty(&mut self) {
        match self {
            Self::Direct(binding) => binding.mark_dirty(true),
            Self::Admin(binding) => binding.dirty(),
            Self::MultiShard(binding) => binding.mark_dirty(),
            Self::NotConnected(binding) => binding.dirty(),
        }
    }

    pub(in super::super::super::connection) fn set_locked(&mut self, locked: bool) {
        dispatch!(self, binding => binding.set_locked(locked));
    }

    pub(in super::super::super::connection) fn is_locked(&self) -> bool {
        dispatch!(self, binding => binding.is_locked())
    }

    pub(crate) fn is_multishard(&self) -> bool {
        dispatch!(self, binding => binding.is_multishard())
    }

    pub(crate) fn direct_shard_number(&self) -> Option<usize> {
        dispatch!(self, binding => binding.direct_shard_number())
    }

    pub(crate) fn in_copy_mode(&self) -> bool {
        dispatch!(self, binding => binding.in_copy_mode())
    }

    pub(crate) fn shards(&self) -> Result<usize, Error> {
        dispatch!(self, binding => binding.shards())
    }
}
