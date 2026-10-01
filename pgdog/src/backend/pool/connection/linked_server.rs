use crate::net::{FrontendPid, Parameters};

use super::Guard;
use std::ops::{Deref, DerefMut};

/// Postgres connection with link state,
/// allowing the calls to [`Self::link_client`] to be idempotent.
#[derive(Debug)]
pub(crate) struct LinkedServer {
    // Postgres connection.
    pub(super) server: Guard,
    // Shard number.
    pub(super) shard: usize,
    // Parameters were sync'ed.
    pub(super) linked: bool,
}

impl LinkedServer {
    /// Link server to client. This is idempotent.
    pub(super) async fn link_client(
        &mut self,
        id: FrontendPid,
        params: &Parameters,
        transaction_stmt: Option<&str>,
    ) -> Result<usize, super::Error> {
        if self.linked {
            return Ok(0);
        }

        let params = self
            .server
            .link_client(id, params, transaction_stmt)
            .await?;

        self.linked = true;

        Ok(params)
    }
}

impl Deref for LinkedServer {
    type Target = Guard;

    fn deref(&self) -> &Self::Target {
        &self.server
    }
}

impl DerefMut for LinkedServer {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.server
    }
}
