use crate::{
    frontend::BufferedQuery,
    net::{FrontendPid, Parameters},
};
use std::ops::{Deref, DerefMut};

use super::*;

#[derive(Debug)]
pub(crate) struct DirectBinding {
    pub(super) server: LinkedServer,
    pub(super) transaction_stmt: Option<BufferedQuery>,
    pub(super) pinned: bool,
}

impl DirectBinding {
    pub(super) fn new(
        server: Guard,
        shard: usize,
        transaction_stmt: Option<BufferedQuery>,
    ) -> Self {
        Self {
            server: LinkedServer {
                server,
                shard,
                linked: false,
            },
            transaction_stmt,
            pinned: false,
        }
    }

    pub(super) async fn link_client(
        &mut self,
        id: FrontendPid,
        params: &Parameters,
    ) -> Result<usize, Error> {
        if self.linked {
            return Ok(0);
        }

        let start_transaction = self.transaction_stmt.as_ref().map(|q| q.query());

        Ok(self
            .server
            .link_client(id, params, start_transaction)
            .await?)
    }
}

impl Deref for DirectBinding {
    type Target = LinkedServer;

    fn deref(&self) -> &Self::Target {
        &self.server
    }
}

impl DerefMut for DirectBinding {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.server
    }
}
