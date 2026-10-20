use std::ops::{Deref, DerefMut};

use super::{Error, insert::group_split_requests_by_shard};
use crate::frontend::{ClientRequest, PreparedStatements, router::parser::Shard};

#[derive(Debug, Clone)]
pub(crate) struct InsertSplitRewriteResult {
    pub(super) requests: Vec<ClientRequest>,
}

impl Deref for InsertSplitRewriteResult {
    type Target = Vec<ClientRequest>;

    fn deref(&self) -> &Self::Target {
        &self.requests
    }
}

impl DerefMut for InsertSplitRewriteResult {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.requests
    }
}

impl InsertSplitRewriteResult {
    /// Send one multi-tuple INSERT per shard.
    /// Requests must be routed first.
    pub(crate) fn group_by_shard(
        &mut self,
        request: &ClientRequest,
        prepared_statements: &mut PreparedStatements,
    ) -> Result<(), Error> {
        self.requests =
            group_split_requests_by_shard(&self.requests, request, prepared_statements)?;
        Ok(())
    }

    /// Check that the insert split actually targets
    /// the same shard.
    pub(crate) fn same_shard(&self) -> Option<Shard> {
        if let Some(first) = self
            .requests
            .first()
            .and_then(|request| request.route.as_ref().map(|route| route.shard()))
        {
            if !first.is_direct() {
                return None;
            }
            if self
                .requests
                .iter()
                .skip(1)
                .all(|req| req.route.as_ref().map(|r| r.shard()) == Some(first))
            {
                return Some(first.clone());
            }
        }

        None
    }
}
