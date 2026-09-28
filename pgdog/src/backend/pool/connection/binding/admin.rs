use std::ops::{Deref, DerefMut};

use crate::{
    admin::server::AdminServer,
    backend::Error,
    frontend::{
        client::query_engine::{TwoPcPhase, two_pc::TwoPcTransaction},
        router::{CopyRow, Route},
    },
    net::{FrontendPid, Message, ProtocolMessage, Query, parameter::Parameters},
    state::State,
};

#[derive(Debug)]
pub(crate) struct AdminBinding {
    admin: AdminServer,
}

impl Deref for AdminBinding {
    type Target = AdminServer;

    fn deref(&self) -> &Self::Target {
        &self.admin
    }
}

impl DerefMut for AdminBinding {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.admin
    }
}

impl AdminBinding {
    pub(crate) fn new() -> Self {
        Self {
            admin: AdminServer::new(),
        }
    }

    pub(crate) fn force_close(&mut self) {}

    pub(crate) fn connected(&self) -> bool {
        true
    }

    pub(crate) fn connected_servers(&self) -> usize {
        1
    }

    pub(crate) async fn send_ignore(
        &mut self,
        _message: &ProtocolMessage,
        _route: &Route,
    ) -> Result<(), Error> {
        Err(Error::NotConnected)
    }

    pub(crate) async fn send_copy(&mut self, _rows: Vec<CopyRow>) -> Result<(), Error> {
        Err(Error::CopyNotConnected)
    }

    pub(crate) fn has_more_messages(&self) -> bool {
        !self.admin.done()
    }

    pub(crate) fn out_of_sync(&self) -> bool {
        false
    }

    pub(in super::super::super::connection) fn state_check(&self, _state: State) -> bool {
        true
    }

    pub(crate) async fn execute(
        &mut self,
        _query: impl Into<Query> + Clone,
    ) -> Result<Vec<Message>, Error> {
        Ok(Vec::new())
    }

    pub(crate) async fn two_pc(
        &mut self,
        _transaction: TwoPcTransaction,
        _phase: TwoPcPhase,
        _ignore_missing: bool,
    ) -> Result<(), Error> {
        Err(Error::TwoPcMultiShardOnly)
    }

    pub(crate) async fn link_client(
        &mut self,
        _id: FrontendPid,
        _params: &Parameters,
        _transaction_start_stmt: Option<&str>,
    ) -> Result<usize, Error> {
        Ok(0)
    }

    pub(crate) fn transaction_params_hook(&mut self, _rollback: bool) {}

    pub(crate) fn changed_params(&mut self) -> Parameters {
        Parameters::default()
    }

    pub(in super::super::super::connection) fn dirty(&mut self) {}

    pub(in super::super::super::connection) fn set_locked(&mut self, _locked: bool) {}

    pub(in super::super::super::connection) fn is_locked(&self) -> bool {
        false
    }

    pub(crate) fn is_multishard(&self) -> bool {
        false
    }

    pub(crate) fn direct_shard_number(&self) -> Option<usize> {
        None
    }

    pub(crate) fn in_copy_mode(&self) -> bool {
        false
    }

    pub(crate) fn shards(&self) -> Result<usize, Error> {
        Ok(1)
    }
}
